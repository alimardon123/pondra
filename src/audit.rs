//! The audit log (`pondra.audit`, ADR-035 §5): who did what, through which door, from where, and
//! what came of it, kept in a table of the lake's own (`pondra$audit`, hidden) for
//! `PONDRA_AUDIT_DAYS` (90). Only a superuser reads it.
//!
//! What is written is chosen by class, as pgaudit does (`PONDRA_AUDIT`, a list): `role` (users,
//! roles, grants, tokens, secrets), `ddl` (the rest of `CREATE`, `ALTER`, `DROP`, `ATTACH`…),
//! `write` (`INSERT`, `UPDATE`, `DELETE`, `MERGE`, `COPY`), `function` (`CALL`, `DO`), `read`
//! (queries), `misc` (`SET`, `BEGIN`…), `all`, or `off`. The default is `role,ddl,function`.
//! Refusals are always written (class `access`, or the statement's): a sign-in that failed, a
//! request its rights or its quota didn't cover.
//!
//! A statement's quoted values are written as `'***'` when it makes a user, a token or a secret.
//! Rows reach the log a moment later, in batches, from a writer on each node: nothing waits for it.
use crate::server::App;
use crate::store::{json, table_key, Lake, TableMeta};
use anyhow::Result;
use datafusion::arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use std::future::Future;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

pub const TABLE: &str = "pondra$audit";

/// One thing done, or refused.
struct Line {
    at: u64,
    user: String,
    door: &'static str,
    from: Option<String>,
    class: &'static str,
    statement: String,
    outcome: &'static str, // ok, failed, refused
    message: Option<String>,
    ms: Option<i64>,
}

fn columns() -> Vec<(String, String)> {
    use datafusion::arrow::datatypes::{DataType, TimeUnit};
    let ts = crate::query::type_name(&DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())));
    let text = |n: &str| (n.to_string(), "Utf8".to_string());
    vec![("at".into(), ts), text("user"), text("door"), text("from"), text("class"), text("statement"), text("outcome"), text("message"), ("ms".into(), "Int64".into()), text("node")]
}

/// Leader: the table, made the first time a node has a line for it (`Ddl::AuditLog`).
pub async fn create_log(lake: &Lake) -> Result<serde_json::Value> {
    if lake.cat.get::<TableMeta>(&table_key(TABLE)).await?.is_none() {
        let days: u64 = std::env::var("PONDRA_AUDIT_DAYS").ok().and_then(|d| d.parse().ok()).unwrap_or(90);
        let meta = TableMeta { columns: columns(), ttl: Some(("at".into(), days * 86400)), tiered: lake.visible(), ..Default::default() };
        lake.cat.commit(vec![(table_key(TABLE), json(&meta))], &[]).await?;
    }
    Ok(serde_json::json!({"table": "pondra.audit"}))
}

/// The classes written (`PONDRA_AUDIT`).
fn classes() -> &'static [String] {
    static C: OnceLock<Vec<String>> = OnceLock::new();
    C.get_or_init(|| {
        let v = std::env::var("PONDRA_AUDIT").unwrap_or_else(|_| "role,ddl,function".into()).to_lowercase();
        v.split(',').map(|c| c.trim().to_string()).filter(|c| !c.is_empty()).collect()
    })
}

fn on() -> bool { !classes().iter().any(|c| c == "off") }

fn written(class: &str) -> bool { on() && classes().iter().any(|c| c == class || c == "all") }

/// A statement's class, by its first words.
pub fn class(sql: &str) -> &'static str {
    let words: Vec<String> = sql.split(|c: char| c.is_whitespace() || c == '(' || c == ';').filter(|w| !w.is_empty()).take(6).map(|w| w.to_uppercase()).collect();
    let first = words.first().map_or("", |w| w.as_str());
    let about = |o: &[&str]| words.iter().skip(1).take(4).any(|w| o.contains(&w.as_str()));
    match first {
        "GRANT" | "REVOKE" => "role",
        "CREATE" | "ALTER" | "DROP" if about(&["USER", "ROLE", "TOKEN", "SECRET"]) => "role",
        "CREATE" | "ALTER" | "DROP" | "TRUNCATE" | "ATTACH" | "DETACH" | "COMMENT" | "UNDROP" | "OPTIMIZE" | "VACUUM" | "CHECKPOINT" => "ddl",
        "INSERT" | "UPDATE" | "DELETE" | "MERGE" | "COPY" | "UPSERT" => "write",
        "CALL" | "DO" => "function",
        _ if sql.to_lowercase().contains("pondra.start(") => "function",
        "SET" | "RESET" | "BEGIN" | "START" | "COMMIT" | "ROLLBACK" | "END" | "DISCARD" | "DEALLOCATE" | "USE" => "misc",
        _ => "read",
    }
}

/// `sql` as the log keeps it: quoted values hidden where they may be a password, a token or a
/// secret's; at most 4 KB.
fn redacted(sql: &str, class: &str) -> String {
    let mut out = String::with_capacity(sql.len().min(4096));
    match class == "role" {
        false => out.push_str(sql),
        true => {
            let (mut quoted, mut chars) = (false, sql.chars().peekable());
            while let Some(c) = chars.next() {
                match (c, quoted) {
                    ('\'', false) => {
                        out.push_str("'***");
                        quoted = true;
                    }
                    ('\'', true) if chars.peek() == Some(&'\'') => drop(chars.next()), // (an escaped quote: still inside)
                    ('\'', true) => {
                        out.push('\'');
                        quoted = false;
                    }
                    (_, true) => {}
                    (c, false) => out.push(c),
                }
            }
        }
    }
    if out.len() > 4096 {
        let mut at = 4096;
        while !out.is_char_boundary(at) {
            at -= 1;
        }
        out.truncate(at);
        out.push('…');
    }
    out
}

tokio::task_local! {
    static INSIDE: (); // (a statement's own statements, a procedure's: the call is what is written)
}

/// An error a door says: an error of its own kind (`code`: Postgres's SQLSTATE).
pub trait Refusal {
    fn refusal(code: &str, message: String) -> Self;
}

impl Refusal for anyhow::Error {
    fn refusal(_: &str, message: String) -> Self { anyhow::anyhow!(message) }
}

impl Refusal for pgwire::error::PgWireError {
    fn refusal(code: &str, message: String) -> Self { pgwire::error::PgWireError::UserError(Box::new(pgwire::error::ErrorInfo::new("ERROR".into(), code.into(), message))) }
}

impl Refusal for tonic::Status {
    fn refusal(_: &str, message: String) -> Self { tonic::Status::resource_exhausted(message) }
}

/// Run one statement a door was sent, within its user's quota (`users::Quota`), writing what
/// came of it when its class is written (and a refusal always).
pub async fn statement<T, E: std::fmt::Display + Refusal>(app: &App, sql: &str, f: impl Future<Output = std::result::Result<T, E>>) -> std::result::Result<T, E> {
    if INSIDE.try_with(|_| ()).is_ok() {
        return f.await;
    }
    let (class, start, quota) = (class(sql), Instant::now(), crate::users::quota());
    let out = INSIDE
        .scope((), async {
            let _turn = quota.turn().await.map_err(|m| E::refusal("53000", m))?; // (insufficient_resources)
            match quota.timeout {
                Some(t) => tokio::time::timeout(t, f).await.unwrap_or_else(|_| Err(E::refusal("57014", format!("quota: {}'s statements run at most {} s (STATEMENT_TIMEOUT); this one was stopped", quota.user, t.as_secs())))),
                None => f.await,
            }
        })
        .await;
    if !on() {
        return out;
    }
    let message = out.as_ref().err().map(|e| e.to_string());
    let refused = message.as_ref().filter(|m| ["permission denied", "needs a", "needs more rights", "sign in", "quota:"].iter().any(|r| m.contains(r)));
    if written(class) || refused.is_some() {
        let outcome = match (&out, &refused) {
            (Ok(_), _) => "ok",
            (_, Some(_)) => "refused",
            _ => "failed",
        };
        log(app, class, redacted(sql, class), outcome, message, Some(start.elapsed().as_millis() as i64));
    }
    out
}

/// A sign-in that failed, or a request its rights didn't cover (at a door, before any statement).
pub fn refused(app: &App, user: &str, door: &'static str, from: Option<std::net::SocketAddr>, what: &str, message: &str) {
    if on() {
        write(app, Line { at: crate::log::now_ms(), user: user.to_string(), door, from: from.map(|a| a.to_string()), class: "access", statement: redacted(what, "role"), outcome: "refused", message: Some(message.to_string()), ms: None });
    }
}

fn log(app: &App, class: &'static str, statement: String, outcome: &'static str, message: Option<String>, ms: Option<i64>) {
    let who = crate::auth::current();
    let user = who.as_ref().map_or(String::new(), |p| p.name.clone());
    let (door, from) = who.as_ref().map_or(("node", None), |p| (p.door, p.from.map(|a| a.to_string())));
    write(app, Line { at: crate::log::now_ms(), user, door, from, class, statement, outcome, message, ms });
}

static WRITER: OnceLock<tokio::sync::mpsc::UnboundedSender<Line>> = OnceLock::new();

fn write(app: &App, line: Line) {
    if app.log.is_none() {
        return; // (a read-only node writes nothing: its own log says it)
    }
    let tx = WRITER.get_or_init(|| {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        crate::panics::spawn(writer(app.clone(), rx));
        tx
    });
    let _ = tx.send(line);
}

/// This node's writer: what came in a moment, appended exactly once (one producer, a seq a batch).
async fn writer(app: App, mut rx: tokio::sync::mpsc::UnboundedReceiver<Line>) {
    let producer = format!("audit-{}", crate::runs::new_id());
    let (mut seq, mut made) = (0, false);
    while let Some(first) = rx.recv().await {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let mut lines = vec![first];
        while let Ok(l) = rx.try_recv() {
            lines.push(l);
        }
        seq += 1;
        for attempt in 0.. {
            match append(&app, &producer, seq, &lines, &mut made).await {
                Ok(()) => break,
                Err(e) if attempt >= 30 => {
                    eprintln!("the audit log lost {} rows: {e:#}", lines.len());
                    break;
                }
                Err(_) => tokio::time::sleep(Duration::from_secs(1)).await,
            }
        }
    }
}

async fn append(app: &App, producer: &str, seq: u64, lines: &[Line], made: &mut bool) -> Result<()> {
    if !*made && app.lake.cat.get::<TableMeta>(&table_key(TABLE)).await?.is_none() {
        crate::write::on_node_as(app, crate::write::Stmt::Ddl(vec![crate::ddl::Ddl::AuditLog]), None, false).await?;
    }
    *made = true;
    let text = |f: &dyn Fn(&Line) -> Option<String>| Arc::new(lines.iter().map(f).collect::<StringArray>()) as ArrayRef;
    let node = app.cluster.addr.clone();
    let arrays = vec![
        Arc::new(lines.iter().map(|l| Some(l.at as i64 * 1000)).collect::<TimestampMicrosecondArray>().with_timezone("UTC")) as ArrayRef,
        text(&|l| Some(l.user.clone())),
        text(&|l| Some(l.door.to_string())),
        text(&|l| l.from.clone()),
        text(&|l| Some(l.class.to_string())),
        text(&|l| Some(l.statement.clone())),
        text(&|l| Some(l.outcome.to_string())),
        text(&|l| l.message.clone()),
        Arc::new(lines.iter().map(|l| l.ms).collect::<Int64Array>()) as ArrayRef,
        text(&|_| Some(node.clone())),
    ];
    let batch = RecordBatch::try_new(crate::query::schema(&columns())?, arrays)?;
    app.log()?.append(TABLE.into(), crate::log::Src { producer: producer.into(), seq, prev: None }, batch).await?;
    Ok(())
}

/// Is `sql` about the audit log, and may the caller read it (a superuser)?
pub fn check(sql: &str) -> Result<bool> {
    if !sql.to_lowercase().contains("pondra.audit") {
        return Ok(false);
    }
    let superuser = crate::auth::current().is_none_or(|p| p.role == crate::auth::Role::Admin && p.access.is_none());
    anyhow::ensure!(superuser, "permission denied: pondra.audit is the superusers'");
    Ok(true)
}

/// `pondra.audit` before its first row: the columns, no rows.
pub fn empty() -> Result<Arc<dyn datafusion::catalog::TableProvider>> {
    Ok(Arc::new(datafusion::datasource::MemTable::try_new(crate::query::read_schema(&columns())?, vec![vec![]])?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classes_and_redaction() {
        assert_eq!(class("CREATE USER ana PASSWORD 'x'"), "role");
        assert_eq!(class("create or replace secret s (type s3, secret 'y')"), "role");
        assert_eq!(class("GRANT SELECT ON t TO ana"), "role");
        assert_eq!(class("CREATE TABLE t (a INT)"), "ddl");
        assert_eq!(class("insert into t values (1)"), "write");
        assert_eq!(class("CALL p(1)"), "function");
        assert_eq!(class("SELECT 1"), "read");
        assert_eq!(redacted("CREATE USER ana PASSWORD 'it''s' SUPERUSER", "role"), "CREATE USER ana PASSWORD '***' SUPERUSER");
        assert_eq!(redacted("INSERT INTO t VALUES ('a')", "write"), "INSERT INTO t VALUES ('a')");
    }
}
