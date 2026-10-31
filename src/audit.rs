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
use crate::app::App;
use crate::store::{json, table_key, Lake, TableMeta};
use anyhow::Result;
use datafusion::arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use futures::{Stream, StreamExt};
use std::future::Future;
use crate::reset::Once;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock, RwLock};
use datafusion::common::instant::Instant;
use std::time::Duration;

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
    let ts = crate::answer::type_name(&DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())));
    let text = |n: &str| (n.to_string(), "Utf8".to_string());
    vec![("at".into(), ts), text("user"), text("door"), text("from"), text("class"), text("statement"), text("outcome"), text("message"), ("ms".into(), "Int64".into()), text("node")]
}

/// Leader: the table, made the first time a node has a line for it (`Ddl::AuditLog`).
pub async fn create_log(lake: &Lake) -> Result<serde_json::Value> {
    if lake.cat.get::<TableMeta>(&table_key(TABLE)).await?.is_none() {
        let days: u64 = crate::env::or("PONDRA_AUDIT_DAYS", 90);
        let meta = TableMeta { columns: columns(), ttl: Some(("at".into(), days * 86400)), tiered: lake.visible(), ..Default::default() };
        lake.cat.commit(vec![(table_key(TABLE), json(&meta))], &[]).await?;
    }
    Ok(serde_json::json!({"table": "pondra.audit"}))
}

/// The classes written (`PONDRA_AUDIT`).
fn classes() -> &'static [String] {
    static C: OnceLock<Vec<String>> = OnceLock::new();
    C.get_or_init(|| {
        let v = crate::env::get("PONDRA_AUDIT").unwrap_or_else(|| "role,ddl,function".into()).to_lowercase();
        v.split(',').map(|c| c.trim().to_string()).filter(|c| !c.is_empty()).collect()
    })
}

fn on() -> bool { !classes().iter().any(|c| c == "off") }

fn written(class: &str) -> bool { on() && classes().iter().any(|c| c == class || c == "all") }

/// A statement without the comments and spaces before its first word (a comment first must not
/// make a `GRANT` a read).
fn lead(mut sql: &str) -> &str {
    loop {
        sql = sql.trim_start();
        sql = match () {
            _ if sql.starts_with("--") => sql.find('\n').map_or("", |i| &sql[i..]),
            _ if sql.starts_with("/*") => sql.find("*/").map_or("", |i| &sql[i + 2..]),
            _ => return sql,
        };
    }
}

/// A statement's class, by its first words.
pub fn class(sql: &str) -> &'static str {
    let sql = lead(sql);
    let words: Vec<String> = sql.split(|c: char| c.is_whitespace() || c == '(' || c == ';').filter(|w| !w.is_empty()).take(6).map(|w| w.to_uppercase()).collect();
    let first = words.first().map_or("", |w| w.as_str());
    let about = |o: &[&str]| words.iter().skip(1).take(4).any(|w| o.contains(&w.as_str()));
    match first {
        "GRANT" | "REVOKE" => "role",
        "CREATE" | "ALTER" | "DROP" if about(&["USER", "ROLE", "TOKEN", "SECRET", "SHARE", "RECIPIENT", "PROVIDER"]) => "role",
        "ATTACH" if sql.to_uppercase().contains("TOKEN") => "role", // (a share's profile: its token is a secret)
        "ALTER" if about(&["DATABASE"]) && sql.to_lowercase().contains("protected") => "role", // (who may change a protected database's projects: ADR-058)
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
pub fn redacted(sql: &str, class: &str) -> String {
    let mut out = String::with_capacity(sql.len().min(4096));
    let words: Vec<String> = lead(sql).split(|c: char| c.is_whitespace() || c == '(').filter(|w| !w.is_empty()).take(5).map(|w| w.to_uppercase()).collect();
    let secret = matches!(words.first().map(String::as_str), Some("CREATE" | "ALTER")) && words.iter().any(|w| w == "SECRET");
    match (class == "role", sql.find('(')) {
        // (a secret's options may be quoted either way, or not at all: every value but its TYPE is
        // kept as '***')
        (true, Some(open)) if secret => {
            let close = sql.rfind(')').filter(|&c| c > open).unwrap_or(sql.len());
            let options: Vec<String> = items(&sql[open + 1..close]).into_iter().map(|o| match o.split_once(char::is_whitespace) {
                Some((name, _)) if !name.eq_ignore_ascii_case("type") => format!("{name} '***'"),
                _ => o.to_string(),
            }).collect();
            out.push_str(&sql[..=open]);
            out.push_str(&options.join(", "));
            out.push_str(&sql[close..]);
        }
        (false, _) => out.push_str(sql),
        (true, _) => {
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
    crate::history::cut(out, 4096)
}

/// A list's items, split at the commas outside quotes, trimmed.
fn items(list: &str) -> Vec<&str> {
    let (mut items, mut start, mut quote) = (vec![], 0, None);
    for (i, c) in list.char_indices() {
        match (c, quote) {
            ('\'' | '"', None) => quote = Some(c),
            (c, Some(q)) if c == q => quote = None,
            (',', None) => {
                items.push(list[start..i].trim());
                start = i + 1;
            }
            _ => {}
        }
    }
    items.push(list[start..].trim());
    items.into_iter().filter(|s| !s.is_empty()).collect()
}

tokio::task_local! {
    static INSIDE: (); // (a statement's own statements, a procedure's: the call is what is written)
}

tokio::task_local! {
    static STOP: Arc<AtomicBool>; // (a statement's stop flag: set when the statement is dropped before its work ends)
}

/// The stop flag of the statement this task runs, for the work it hands on (a MATCH_RECOGNIZE search, which runs after
/// the statement's planning, bound to it). None outside a statement.
pub fn stop() -> Option<Arc<AtomicBool>> { STOP.try_with(|s| s.clone()).ok() }

/// Sets its flag when dropped before it is marked done: the statement's future was dropped (a timeout, a Stop, a closed tab).
struct SetOnDrop {
    flag: Arc<AtomicBool>,
    done: bool,
}

impl SetOnDrop {
    fn new(flag: Arc<AtomicBool>) -> Self { SetOnDrop { flag, done: false } }

    /// The work ended: the drop that follows leaves the flag unset.
    fn mark_done(&mut self) { self.done = true; }
}

impl Drop for SetOnDrop {
    fn drop(&mut self) {
        if !self.done {
            self.flag.store(true, Ordering::Relaxed);
        }
    }
}

/// `f`, with `flag` as its statement's stop flag (`stop`). The flag is set if `f` is dropped before it finishes. A `f` that
/// finishes leaves it unset, because a door may stream the rows the statement made after `statement` returns.
pub(crate) async fn stoppable<T>(flag: Arc<AtomicBool>, f: impl Future<Output = T>) -> T {
    let mut guard = SetOnDrop::new(flag.clone());
    let out = STOP.scope(flag, f).await;
    guard.mark_done();
    out
}

/// `s`, which sets `flag` when it is dropped before it ends. A door whose rows stream after `statement` returns (a
/// plan's pieces, read by the client as they come) is guarded so: a client that goes mid-stream stops the search still
/// running on those rows, as a statement dropped unfinished does (`stoppable`).
pub(crate) fn guarded<S: Stream>(flag: Arc<AtomicBool>, s: S) -> impl Stream<Item = S::Item> {
    futures::stream::unfold((Box::pin(s), SetOnDrop::new(flag)), |(mut s, mut guard)| async move {
        match s.next().await {
            Some(item) => Some((item, (s, guard))),
            None => {
                guard.mark_done(); // (read to its end: the drop that follows leaves the flag unset)
                None
            }
        }
    })
}

/// An error a door says: an error of its own kind (`code`: Postgres's SQLSTATE).
pub trait Refusal {
    fn refusal(code: &str, message: String) -> Self;
}

impl Refusal for anyhow::Error {
    fn refusal(_: &str, message: String) -> Self { anyhow::anyhow!(message) }
}

#[cfg(feature = "doors")]
impl Refusal for pgwire::error::PgWireError {
    fn refusal(code: &str, message: String) -> Self { pgwire::error::PgWireError::UserError(Box::new(pgwire::error::ErrorInfo::new("ERROR".into(), code.into(), message))) }
}

#[cfg(feature = "doors")]
impl Refusal for tonic::Status {
    fn refusal(_: &str, message: String) -> Self { tonic::Status::resource_exhausted(message) }
}

/// A statement dropped before it ended, its client gone (a Stop, a closed tab, a timeout): a row of
/// the history all the same, `stopped`.
struct Stopped<'a>(Option<(&'a App, &'static str, &'a str, Instant)>);

impl Drop for Stopped<'_> {
    fn drop(&mut self) {
        if let Some((app, class, sql, start)) = self.0.take() {
            crate::history::ended(app, class, sql, "stopped", Some("its client stopped waiting".into()), start.elapsed(), Default::default());
        }
    }
}

/// A failure's message that is a refusal of rights or quota (written whatever the classes), not a
/// failure of the statement.
fn refusal(m: &str) -> bool { ["permission denied", "needs a", "needs more rights", "sign in", "quota:"].iter().any(|r| m.contains(r)) }

/// Run one statement a door was sent, within its user's quota (`users::Quota`), writing what
/// came of it when its class is written (and a refusal always). The lake it was sent to is in
/// scope while it runs (`ext::rooted`): every door's statement passes here, so each stores a
/// client's relative LOCATION as the same file of the lake's.
pub async fn statement<T, E: std::fmt::Display + Refusal>(app: &App, sql: &str, f: impl Future<Output = std::result::Result<T, E>>) -> std::result::Result<T, E> {
    if INSIDE.try_with(|_| ()).is_ok() {
        return f.await;
    }
    if app.members.cut_off() {
        return Err(E::refusal("57P03", crate::members::CUT_OFF_SAYS.into())); // (cannot_connect_now: every door's clients retry elsewhere)
    }
    let (class, start, quota) = (class(sql), Instant::now(), crate::users::quota());
    let mut stopped = Stopped(Some((app, class, sql, start)));
    let (out, note) = crate::history::noted(crate::ext::rooted(&app.lake.url, INSIDE.scope((), async {
        let _turn = quota.turn().await.map_err(|m| E::refusal("53000", m))?; // (insufficient_resources)
        let flag = Arc::new(AtomicBool::new(false)); // (set if the work is dropped first: a timeout drops it, inside the timeout)
        match quota.timeout {
            Some(t) => tokio::time::timeout(t, stoppable(flag, f)).await.unwrap_or_else(|_| Err(E::refusal("57014", match quota.by_session {
                true => "canceling statement due to statement timeout".to_string(), // (Postgres's own words: a client matches them)
                false => format!("quota: {}'s statements run at most {} s (STATEMENT_TIMEOUT); this one was stopped", quota.user, t.as_secs()),
            }))),
            None => stoppable(flag, f).await,
        }
    })))
    .await;
    stopped.0 = None;
    let took = start.elapsed();
    let message = out.as_ref().err().map(|e| e.to_string());
    let refused = message.as_ref().filter(|m| refusal(m));
    let outcome = match (&out, &refused) {
        (Ok(_), _) => "ok",
        (_, Some(_)) => "refused",
        _ => "failed",
    };
    if on() && (written(class) || refused.is_some()) {
        log(app, class, redacted(sql, class), outcome, message.clone(), Some(took.as_millis() as i64));
    }
    crate::history::ended(app, class, sql, outcome, message, took, note); // (`pondra.history`)
    out
}

/// Rows a door took that takes rows, not SQL (`/append`, `/insert`): a line of the audit log as the
/// INSERT it stands for, when writes are written (`PONDRA_AUDIT=…,write`), and always when refused.
/// Not a statement, so not in `pondra.history` (an append can come thousands of times a second).
pub fn rows(app: &App, door: &str, table: &str, error: Option<String>, took: std::time::Duration) {
    let refused = error.as_deref().is_some_and(refusal);
    let outcome = match (&error, refused) {
        (None, _) => "ok",
        (_, true) => "refused",
        _ => "failed",
    };
    if on() && (written("write") || refused) {
        log(app, "write", format!("INSERT INTO {table} (rows through {door})"), outcome, error, Some(took.as_millis() as i64));
    }
}

/// A sign-in that failed, or a request its rights didn't cover (at a door, before any statement), from `from`. Its
/// client is the connection's: `refused_by` names another (behind a proxy, `Peer::client`).
pub async fn refused(app: &App, user: &str, door: &'static str, from: Option<std::net::SocketAddr>, what: &str, message: &str) {
    refused_by(app, user, door, from, from.map(|a| a.ip()), what, message).await
}

/// `refused`, counted by `client`. One that names a user of the lake is always written. Anyone may knock with no
/// user, or with a name that is none of its users, so past `KNOCKS` a minute from one client (an IPv6 one's /64)
/// those aren't written one by one: a row says how many weren't, once their minute has ended (at the next refusal).
pub async fn refused_by(app: &App, user: &str, door: &'static str, from: Option<std::net::SocketAddr>, client: Option<std::net::IpAddr>, what: &str, message: &str) {
    if !on() {
        return;
    }
    let known = !user.is_empty() && app.lake.cat.get::<crate::users::User>(&crate::users::user_key(user)).await.ok().flatten().is_some();
    let now = crate::log::now_ms();
    let (written, uncounted) = knocked((!known).then_some(client), now);
    for (range, since, n) in uncounted {
        let since = chrono::DateTime::from_timestamp_millis(since as i64).map(|t| t.to_rfc3339()).unwrap_or_default();
        let message = format!("{n} more refusals naming no user of the lake from {range} in the minute from {since} weren't written one by one");
        write(app, Line { at: now, user: String::new(), door: "node", from: Some(range), class: "access", statement: "refusals".into(), outcome: "refused", message: Some(message), ms: None });
    }
    let message = match written {
        Some(false) => message.to_string(),
        Some(true) => format!("{message} (this client's refusals naming no user of the lake past {KNOCKS} a minute are counted, not written one by one, until its minute ends)"),
        None => return,
    };
    write(app, Line { at: now, user: user.to_string(), door, from: from.map(|a| a.to_string()), class: "access", statement: redacted(what, "role"), outcome: "refused", message: Some(message), ms: None });
}

/// Refusals naming no user of the lake written a minute from one client (security pass 2: 400 anonymous `/sso/…` calls
/// wrote 400 rows; audit 10-31 M6: as many with made-up user names did too).
const KNOCKS: u32 = 60;
const CLIENTS: usize = 10_000; // (the clients counted at once: when full, newcomers count together, and no count starts over)

#[derive(Default)]
struct Knocks {
    by: std::collections::HashMap<std::net::IpAddr, (u64, u32)>, // client's range -> its minute's start, refusals in it
    swept: u64,                                                  // (when the minutes that ended were last counted out)
}

crate::per_node!(static KNOCKED: std::sync::Mutex<Knocks> = std::sync::Mutex::new(Knocks::default()));

/// The range a client's refusals are counted under: its address, an IPv6 one's /64 (what a network hands one machine).
fn range(ip: std::net::IpAddr) -> std::net::IpAddr {
    match ip.to_canonical() {
        std::net::IpAddr::V6(a) => std::net::IpAddr::V6((u128::from(a) & !(u64::MAX as u128)).into()),
        v4 => v4,
    }
}

/// Is a refusal at `now_ms` written: Some(false) yes, Some(true) yes and the last one written this minute, None no. Only
/// those naming no user of the lake are counted (`client` Some: its address, None when the door can't tell, counted under 0.0.0.0).
/// With it, the minutes that ended with refusals not written: (range, its minute's start, how many).
fn knocked(client: Option<Option<std::net::IpAddr>>, now_ms: u64) -> (Option<bool>, Vec<(String, u64, u32)>) {
    let Some(client) = client else { return (Some(false), vec![]) };
    let mut k = KNOCKED.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    let mut ended = vec![];
    let shown = |ip: &std::net::IpAddr| if ip.is_ipv6() { format!("{ip}/64") } else { ip.to_string() };
    if now_ms.saturating_sub(k.swept) >= 60_000 || k.by.len() >= CLIENTS {
        k.swept = now_ms;
        k.by.retain(|ip, (since, n)| {
            let over = now_ms.saturating_sub(*since) >= 60_000;
            if over && *n > KNOCKS {
                ended.push((shown(ip), *since, *n - KNOCKS));
            }
            !over
        });
    }
    let unspecified = std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED);
    let key = client.map_or(unspecified, range);
    let key = if k.by.len() >= CLIENTS && !k.by.contains_key(&key) { unspecified } else { key };
    let (since, n) = k.by.entry(key).or_insert((now_ms, 0));
    if now_ms.saturating_sub(*since) >= 60_000 {
        if *n > KNOCKS {
            ended.push((shown(&key), *since, *n - KNOCKS));
        }
        (*since, *n) = (now_ms, 0);
    }
    *n += 1;
    let written = match *n {
        n if n < KNOCKS => Some(false),
        n if n == KNOCKS => Some(true),
        _ => None,
    };
    (written, ended)
}

/// A request at the sharing door (`sharing.rs`), by a recipient: always written, as refusals are.
pub fn shared(app: &App, recipient: &str, from: Option<std::net::SocketAddr>, what: &str, outcome: &'static str, message: Option<String>) {
    if on() {
        write(app, Line { at: crate::log::now_ms(), user: format!("recipient {recipient}"), door: "sharing", from: from.map(|a| a.to_string()), class: "share", statement: what.to_string(), outcome, message, ms: None });
    }
}

fn log(app: &App, class: &'static str, statement: String, outcome: &'static str, message: Option<String>, ms: Option<i64>) {
    let who = crate::auth::current();
    let user = who.as_ref().map_or(String::new(), |p| p.name.clone());
    let (door, from) = who.as_ref().map_or(("node", None), |p| (p.door, p.from.map(|a| a.to_string())));
    write(app, Line { at: crate::log::now_ms(), user, door, from, class, statement, outcome, message, ms });
}

crate::per_node!(static WRITER: RwLock<Option<tokio::sync::mpsc::UnboundedSender<Line>>> = RwLock::new(None));

fn write(app: &App, line: Line) {
    if app.log.is_none() {
        return; // (a read-only node writes nothing: its own log says it)
    }
    let tx = WRITER.get_or(|| crate::history::writer(app, TABLE, crate::ddl::Ddl::AuditLog, Duration::from_millis(50), batch));
    let _ = tx.send(line);
}

fn batch(app: &App, lines: &[Line]) -> Result<RecordBatch> {
    let text = |f: &dyn Fn(&Line) -> Option<String>| Arc::new(lines.iter().map(f).collect::<StringArray>()) as ArrayRef;
    let node = app.members.me().to_string();
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
    Ok(RecordBatch::try_new(crate::answer::schema(&columns())?, arrays)?)
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
        assert_eq!(class("ALTER DATABASE prod SET (protected = false)"), "role");
        assert_eq!(class("ALTER DATABASE dev REFRESH t"), "ddl");
        assert_eq!(class("CREATE TABLE t (a INT)"), "ddl");
        assert_eq!(class("insert into t values (1)"), "write");
        assert_eq!(class("CALL p(1)"), "function");
        assert_eq!(class("SELECT 1"), "read");
        assert_eq!(redacted("CREATE USER ana PASSWORD 'it''s' SUPERUSER", "role"), "CREATE USER ana PASSWORD '***' SUPERUSER");
        assert_eq!(redacted("INSERT INTO t VALUES ('a')", "write"), "INSERT INTO t VALUES ('a')");
        // (a comment first is still the statement it comments; a secret's values hidden however written)
        assert_eq!(class("/* x */ GRANT ALL ON t TO ana"), "role");
        assert_eq!(class("-- note\nCREATE USER ana PASSWORD 'x'"), "role");
        assert_eq!(redacted("CREATE SECRET s (TYPE s3, KEY_ID \"k\", SECRET xyz)", "role"), "CREATE SECRET s (TYPE s3, KEY_ID '***', SECRET '***')");
        assert_eq!(redacted("/* a */ create temporary secret t (type http, bearer_token 'a,b''c')", "role"), "/* a */ create temporary secret t (type http, bearer_token '***')");
    }

    #[tokio::test]
    async fn a_statements_flag_is_set_only_when_its_work_is_dropped() {
        assert!(stop().is_none(), "no flag outside a statement");
        let done = Arc::new(AtomicBool::new(false));
        let seen = stoppable(done.clone(), async { stop().expect("the statement's flag") }).await;
        assert!(Arc::ptr_eq(&seen, &done), "the work sees its statement's flag");
        assert!(!done.load(Ordering::Relaxed), "a statement whose work finishes leaves its flag unset");
        let early = Arc::new(AtomicBool::new(false));
        let cut = tokio::time::timeout(Duration::from_millis(10), stoppable(early.clone(), std::future::pending::<()>())).await;
        assert!(cut.is_err());
        assert!(early.load(Ordering::Relaxed), "a statement its timeout drops sets its flag");
    }

    #[test]
    fn a_client_has_its_refusals_naming_no_user_written_up_to_sixty_a_minute_and_the_rest_counted() {
        let (ip, other) = (Some("203.0.113.7".parse().unwrap()), Some("203.0.113.8".parse().unwrap()));
        let said: Vec<_> = (0..KNOCKS + 5).map(|_| knocked(Some(ip), 1_000).0).collect();
        assert!(said[..KNOCKS as usize - 1].iter().all(|s| *s == Some(false)), "the first 59 written");
        assert_eq!(said[KNOCKS as usize - 1], Some(true), "the 60th written, saying the rest are counted");
        assert!(said[KNOCKS as usize..].iter().all(Option::is_none), "the rest of the minute not written");
        assert_eq!(knocked(Some(other), 1_000).0, Some(false), "another client counts apart");
        assert_eq!(knocked(None, 1_000).0, Some(false), "one that names a user is always written");
        let (written, counted) = knocked(Some(ip), 61_000);
        assert_eq!(written, Some(false), "a new minute, written again");
        assert_eq!(counted, vec![("203.0.113.7".to_string(), 1_000, 5)], "the minute that ended: a count of the 5 not written");
        let v6 = |s: &str| Some(s.parse().unwrap());
        for _ in 0..KNOCKS {
            knocked(Some(v6("2001:db8:0:1::1")), 1_000);
        }
        assert_eq!(knocked(Some(v6("2001:db8:0:1:ffff::2")), 1_000).0, None, "an IPv6 client's /64 counts as one");
        assert_eq!(knocked(Some(v6("2001:db8:0:2::1")), 1_000).0, Some(false), "the next /64 apart");
    }

    #[tokio::test]
    async fn a_guarded_stream_dropped_after_one_item_sets_its_flag() {
        let flag = Arc::new(AtomicBool::new(false));
        let mut rows = Box::pin(guarded(flag.clone(), futures::stream::iter([1, 2, 3])));
        assert_eq!(rows.next().await, Some(1));
        drop(rows);
        assert!(flag.load(Ordering::Relaxed), "a stream its client drops after one of its three items sets its flag");
    }

    #[tokio::test]
    async fn a_guarded_stream_read_to_its_end_leaves_its_flag_unset() {
        let flag = Arc::new(AtomicBool::new(false));
        let rows: Vec<i32> = guarded(flag.clone(), futures::stream::iter([1, 2, 3])).collect().await;
        assert_eq!(rows, vec![1, 2, 3]);
        assert!(!flag.load(Ordering::Relaxed), "a stream read to its end leaves its flag unset");
    }

    #[tokio::test]
    async fn a_guarded_stream_never_polled_and_dropped_sets_its_flag() {
        let flag = Arc::new(AtomicBool::new(false));
        drop(guarded(flag.clone(), futures::stream::iter([1, 2, 3])));
        assert!(flag.load(Ordering::Relaxed), "a stream dropped unpolled sets its flag: its statement was dropped unfinished");
    }
}
