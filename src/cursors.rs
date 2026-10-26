//! Cursors: `DECLARE name [NO SCROLL] CURSOR [WITH | WITHOUT HOLD] FOR query`, `FETCH`, `MOVE` and
//! `CLOSE`, as Postgres has them. psycopg's named cursors, SQLAlchemy's `stream_results` and pandas'
//! `chunksize` read a big answer a part at a time this way.
//!
//! A cursor belongs to a session: a Postgres connection, which is where these statements are sent. Its
//! query runs when it is declared, as the caller, and its batches are kept with a position: a cursor
//! holds an answer as an answer does, in memory until it is closed or its session ends. Streaming the
//! rows out of the engine as they are made is a later round's work.
//!
//! Cursors move forward only (NO SCROLL, Postgres's default): SCROLL, BINARY and the backward moves
//! are refused by name. A cursor without HOLD lives as long as the transaction it was declared in,
//! as in Postgres: it keeps that transaction's id, and `sweep` drops it once the session's open
//! transaction is no longer that one. A WITH HOLD cursor lives until CLOSE or the session's end.
//!
//! The answers the node's cursors hold are kept within a budget together: `PONDRA_CURSORS_MB`, or by
//! default a quarter of the node's memory up to 1024 MB (`budget`). A DECLARE that would pass it is
//! refused (53200), never answered by dropping a cursor someone is still reading.
use crate::auth::Role;
use crate::codes::coded;
use crate::app::App;
use crate::txn;
use anyhow::Result;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};

/// A cursor statement, read.
pub enum Cmd {
    Declare { name: String, hold: bool, query: String },
    Fetch { name: String, rows: Option<usize> }, // (None: ALL)
    Move { name: String, rows: Option<usize> },
    Close(Option<String>), // (None: ALL)
}

/// What a cursor statement answers: FETCH's rows (one empty batch when there are none, so the
/// columns stay), or the command's tag (`DECLARE CURSOR`, `MOVE 5`, `CLOSE CURSOR`).
pub enum Answer {
    Rows(Vec<RecordBatch>),
    Tag(String),
}

struct Cursor {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    batch: usize,        // the batch the next row is in
    row: usize,          // and its row in that batch
    txn: Option<String>, // the transaction it was declared in (None: WITH HOLD)
    bytes: usize,        // what its answer holds in memory, counted as pages.rs counts an answer
}

crate::per_node!(
    /// Each session's cursors, by name. A session with none has no entry.
    static CURSORS: Mutex<HashMap<String, HashMap<String, Cursor>>> = Mutex::new(HashMap::new()));

const NO_SESSION: &str = "a cursor is a session's: a Postgres connection's, or an HTTP client's (send x-pondra-session: <id>)";

/// The cursor statement `sql` is, if it is one (`Some(Err)`: a form this door doesn't take, or a
/// syntax error). `DECLARE $day …` is a variable's, not a cursor's.
pub fn of(sql: &str) -> Option<Result<Cmd>> {
    let s = crate::text::first_word(sql).trim_end_matches(';').trim_end(); // (past a comment before it, as dbt's)
    let verb = s.split_whitespace().next()?.to_uppercase();
    let words = || s.split_whitespace().skip(1).collect::<Vec<&str>>();
    match verb.as_str() {
        "DECLARE" => declare(s),
        "FETCH" => Some(motion(&words()).map(|(name, rows)| Cmd::Fetch { name, rows })),
        "MOVE" => Some(motion(&words()).map(|(name, rows)| Cmd::Move { name, rows })),
        "CLOSE" => Some(close(&words())),
        _ => None,
    }
}

/// `DECLARE name [options] CURSOR [WITH | WITHOUT HOLD] FOR query`.
fn declare(s: &str) -> Option<Result<Cmd>> {
    static HEAD: LazyLock<regex::Regex> = LazyLock::new(|| regex::Regex::new(r#"(?is)^declare\s+("[^"]+"|\w+)\s+((?:\w+\s+)*?)cursor\b\s*(.*)$"#).expect("a regex"));
    static FOR: LazyLock<regex::Regex> = LazyLock::new(|| regex::Regex::new(r"(?is)^(?:(with|without)\s+hold\s+)?for\s+(.*)$").expect("a regex"));
    let c = HEAD.captures(s)?;
    let options: Vec<String> = c[2].split_whitespace().map(|w| w.to_uppercase()).collect();
    if options.iter().any(|o| o == "BINARY") {
        return Some(Err(unsupported("BINARY cursors: a cursor sends its rows as FETCH asks for them")));
    }
    if options.iter().any(|o| o == "SCROLL") && !options.iter().any(|o| o == "NO") {
        return Some(Err(unsupported("SCROLL cursors: a cursor here moves forward only (NO SCROLL)")));
    }
    let Some(f) = FOR.captures(&c[3]) else { return Some(Err(syntax(&c[3]))) };
    let query = f[2].trim().to_string();
    let first = crate::text::first_word(&query).split_whitespace().next().unwrap_or_default().to_uppercase();
    if !(["SELECT", "WITH", "VALUES", "TABLE"].contains(&first.as_str()) || query.starts_with('(')) {
        return Some(Err(coded("42601", "DECLARE … FOR takes a query: SELECT, WITH, VALUES or TABLE")));
    }
    let hold = f.get(1).is_some_and(|h| h.as_str().eq_ignore_ascii_case("with"));
    Some(Ok(Cmd::Declare { name: ident(&c[1]), hold, query }))
}

/// `[direction] [FROM | IN] name` for FETCH and MOVE: the rows asked for (None: ALL), and the name.
fn motion(words: &[&str]) -> Result<(String, Option<usize>)> {
    let Some((name, head)) = words.split_last() else { return Err(syntax("FETCH")) };
    let head = match head.last() {
        Some(w) if w.eq_ignore_ascii_case("from") || w.eq_ignore_ascii_case("in") => &head[..head.len() - 1],
        _ => head,
    };
    let dir = head.join(" ").to_uppercase();
    let rows = match dir.as_str() {
        "" | "NEXT" | "FORWARD" => Some(1),
        "ALL" | "FORWARD ALL" => None,
        _ => match dir.trim_start_matches("FORWARD ").parse::<usize>() {
            Ok(n) => Some(n),
            Err(_) if backward(&dir) => return Err(coded("55000", format!("cursor \"{}\" can only scan forward: a cursor here is NO SCROLL", ident(name)))),
            Err(_) => return Err(syntax(&words.join(" "))),
        },
    };
    Ok((ident(name), rows))
}

/// Moves that go back: PRIOR, BACKWARD, FIRST, LAST, ABSOLUTE, RELATIVE, or a negative count.
fn backward(dir: &str) -> bool {
    dir.split_whitespace().any(|w| w.starts_with('-') || matches!(w, "PRIOR" | "BACKWARD" | "FIRST" | "LAST" | "ABSOLUTE" | "RELATIVE"))
}

/// `CLOSE name` or `CLOSE ALL`.
fn close(words: &[&str]) -> Result<Cmd> {
    match words {
        [all] if all.eq_ignore_ascii_case("all") => Ok(Cmd::Close(None)),
        [name] => Ok(Cmd::Close(Some(ident(name)))),
        _ => Err(syntax("CLOSE")),
    }
}

/// A cursor's name as Postgres reads it: as written when quoted, in lower case when not.
fn ident(s: &str) -> String {
    match s.strip_prefix('"').and_then(|s| s.strip_suffix('"')) {
        Some(quoted) => quoted.to_string(),
        None => s.to_lowercase(),
    }
}

fn syntax(near: &str) -> anyhow::Error { coded("42601", format!("syntax error at or near \"{near}\"")) }

fn unsupported(what: &str) -> anyhow::Error { coded("0A000", format!("{what}: not supported")) }

fn missing(name: &str) -> anyhow::Error { coded("34000", format!("cursor \"{name}\" does not exist")) }

/// The current session's key. A session with none has the empty key, which no cursor is declared
/// under, so its cursor statements find nothing.
fn session() -> String { crate::temp::current().unwrap_or_default() }

/// Runs a cursor statement in the current session, as `role` (signed in, as a query is).
pub async fn run(app: &App, cmd: Cmd, role: Role) -> Result<Answer> {
    if role < Role::Read {
        return Err(coded("28000", "sign in first"));
    }
    sweep();
    match cmd {
        Cmd::Declare { name, hold, query } => declare_as(app, &name, hold, &query).await,
        Cmd::Fetch { name, rows } => fetch(&name, rows, false),
        Cmd::Move { name, rows } => fetch(&name, rows, true),
        Cmd::Close(name) => close_as(name),
    }
}

/// `DECLARE`: the query runs now, as the caller, and its answer is kept under the name.
async fn declare_as(app: &App, name: &str, hold: bool, query: &str) -> Result<Answer> {
    let session = crate::temp::current().ok_or_else(|| coded("0A000", NO_SESSION))?;
    let in_txn = match hold {
        true => None,
        false => Some(txn::id().ok_or_else(|| coded("25P01", "DECLARE CURSOR can only be used in transaction blocks"))?),
    };
    free(&session, name)?; // (before the query runs: a name taken is answered at once)
    let batches = app.query(query, None).await?;
    let schema = match batches.first() {
        Some(b) => b.schema(),
        None => planned(app, query).await?,
    };
    let schema = crate::pg_types::typed(&schema, &crate::pg_types::columns(&app.lake, query).await); // (its uuids, for the Postgres port)
    let bytes: usize = batches.iter().map(|b| b.get_array_memory_size()).sum();
    let mut all = CURSORS.lock().unwrap(); // (the budget is checked under the lock the insert takes: two DECLAREs can't both pass it)
    let held: usize = all.values().flat_map(|cs| cs.values()).map(|c| c.bytes).sum();
    if held + bytes > budget() {
        return Err(refused(held, bytes));
    }
    match all.entry(session).or_default().entry(name.to_string()) {
        Entry::Occupied(_) => Err(taken(name)),
        Entry::Vacant(v) => {
            v.insert(Cursor { schema, batches, batch: 0, row: 0, txn: in_txn, bytes });
            Ok(Answer::Tag("DECLARE CURSOR".into()))
        }
    }
}

/// The most the node's cursors may hold together, in bytes. `PONDRA_CURSORS_MB` sets it. Unset, it is a
/// quarter of the node's memory (`store::ram`, which knows a container's limit) or 1024 MB, whichever is
/// less, and 1024 MB when the memory is unknown: a node in a 1 GB container mustn't hold 1 GB of cursors.
/// Nothing held is dropped to make room; a client is still reading each cursor.
fn budget() -> usize {
    let default = crate::store::ram().map_or(1 << 30, |ram| (ram / 4).min(1 << 30));
    crate::env::parse::<usize>("PONDRA_CURSORS_MB").map_or(default, |mb| mb << 20)
}

/// The refusal: what the node's cursors hold and what this answer needs, in MB, and the ways out. The
/// budget's default is said too, so a refusal in a small container explains where the limit comes from.
fn refused(held: usize, needs: usize) -> anyhow::Error {
    let mb = |bytes: usize| format!("{:.1}", bytes as f64 / (1 << 20) as f64);
    coded(
        "53200",
        format!(
            "the cursors on this node hold {} MB and this answer needs {} MB, past the budget of {} MB (PONDRA_CURSORS_MB; unset, a quarter of the node's memory, at most 1024 MB): CLOSE some cursors, or read the answer in parts with a WHERE",
            mb(held),
            mb(needs),
            budget() >> 20
        ),
    )
}

/// The columns of a query that returned no rows, from its plan.
async fn planned(app: &App, query: &str) -> Result<SchemaRef> {
    let sql = crate::sys::planned(query)?.into_owned();
    let ctx = crate::query::session(&app.lake, &sql, "").await?;
    let df = crate::engine::sql(&ctx, &sql).await?;
    Ok(Arc::new(crate::query::run_schema(&ctx, df.logical_plan())?.as_arrow().clone()))
}

fn free(session: &str, name: &str) -> Result<()> {
    match CURSORS.lock().unwrap().get(session).is_some_and(|cs| cs.contains_key(name)) {
        true => Err(taken(name)),
        false => Ok(()),
    }
}

fn taken(name: &str) -> anyhow::Error { coded("42P03", format!("cursor \"{name}\" already exists")) }

/// `FETCH` and `MOVE` (`moving`: no rows sent, the count moved): the next rows, and the position past them.
fn fetch(name: &str, rows: Option<usize>, moving: bool) -> Result<Answer> {
    let mut all = CURSORS.lock().unwrap();
    let c = all.get_mut(&session()).and_then(|cs| cs.get_mut(name)).ok_or_else(|| missing(name))?;
    let batches = c.take(rows);
    if moving {
        return Ok(Answer::Tag(format!("MOVE {}", batches.iter().map(|b| b.num_rows()).sum::<usize>())));
    }
    Ok(Answer::Rows(match batches.is_empty() {
        true => vec![RecordBatch::new_empty(c.schema.clone())],
        false => batches.into_iter().map(|b| RecordBatch::try_new(c.schema.clone(), b.columns().to_vec())).collect::<Result<_, _>>()?, // (under the cursor's columns: its uuids marked)
    }))
}

impl Cursor {
    /// The next `n` rows (all of them, for None), as slices of its batches, and moves past them.
    fn take(&mut self, n: Option<usize>) -> Vec<RecordBatch> {
        let mut out = vec![];
        let mut need = n.unwrap_or(usize::MAX);
        while need > 0 && self.batch < self.batches.len() {
            let b = &self.batches[self.batch];
            let take = (b.num_rows() - self.row).min(need);
            if take > 0 {
                out.push(b.slice(self.row, take));
            }
            self.row += take;
            need -= take;
            if self.row == b.num_rows() {
                self.batch += 1;
                self.row = 0;
            }
        }
        out
    }
}

/// `CLOSE name` or `CLOSE ALL`.
fn close_as(name: Option<String>) -> Result<Answer> {
    let mut all = CURSORS.lock().unwrap();
    match name {
        None => {
            all.remove(&session());
            Ok(Answer::Tag("CLOSE CURSOR ALL".into()))
        }
        Some(name) => {
            let closed = all.get_mut(&session()).and_then(|cs| cs.remove(&name));
            closed.map(|_| Answer::Tag("CLOSE CURSOR".into())).ok_or_else(|| missing(&name))
        }
    }
}

/// Drops the current session's cursors without HOLD whose transaction is no longer its open one
/// (a COMMIT or ROLLBACK ended it, or another began). Run before each cursor statement and after a
/// COMMIT or ROLLBACK, so their answers' memory goes at once.
pub fn sweep() {
    let now = txn::id(); // (taken before the lock: txn.rs has its own)
    let s = session();
    let mut all = CURSORS.lock().unwrap();
    if all.get_mut(&s).is_some_and(|cs| {
        cs.retain(|_, c| c.txn.is_none() || c.txn == now);
        cs.is_empty()
    }) {
        all.remove(&s);
    }
}

/// The columns a cursor statement answers, for Describe: FETCH's cursor's (an error if there is no
/// such cursor), and none for the statements that send no rows.
pub fn columns(cmd: &Cmd) -> Result<Option<SchemaRef>> {
    let Cmd::Fetch { name, .. } = cmd else { return Ok(None) };
    sweep();
    let all = CURSORS.lock().unwrap();
    let c = all.get(&session()).and_then(|cs| cs.get(name)).ok_or_else(|| missing(name))?;
    Ok(Some(c.schema.clone()))
}

/// A session ended: its cursors go with it.
pub fn end(session: &str) { CURSORS.lock().unwrap().remove(session); }
