//! Transactions (ADR-036 §5, ADR-067): `BEGIN` … `COMMIT` as one commit, and the writes outside one (`autocommit`).
//!
//! A transaction is a session's (a Postgres connection's, or a client's `x-pondra-session`), on the node it talks to.
//! READ COMMITTED (the default) reads the newest commit this node sees at each statement; REPEATABLE READ reads the
//! commit `BEGIN` saw. Writes are kept here, not sent, and every statement reads with them over it (`overlaid`).
//!
//! Under READ COMMITTED a write locks its rows on the leader (`locks.rs`, §3) until the transaction ends; a write over
//! `LOCK_MAX` rows locks none. An enforced-UNIQUE table locks nothing: its COMMIT's check refuses a row changed since it was
//! read (40001). A MERGE and an INSERT … ON CONFLICT lock their rows as an UPDATE does. `COMMIT` sends the writes to the leader
//! in one request (`commit_here`), which checks them under the lake's lock and writes them as one flush (§6). Outside a
//! transaction, a keyed table's UPDATE or DELETE, and a plain INSERT into a table whose key is checked, is a transaction of its
//! own (`autocommit`, §7), appended through the log under the job's producer.
//!
//! A failed statement fails the transaction: until `ROLLBACK` (or `COMMIT`), every statement is refused with 25P02. DDL
//! runs at once (`ddl_at_once`); savepoints are `Mark`s; a READ ONLY transaction refuses its writes (25006). An idle
//! transaction ends after `PONDRA_TXN_IDLE_SECS` (600): its writes are dropped, its locks let go, and its session's
//! statements are refused (25P03) until a transaction word ends it.
use crate::store::*;
use crate::locks::{lock_wait, passes, Hold, Locks, Place, LOCK_MAX};
use crate::settings::SERIALIZABLE;
use crate::write::Stmt;
use anyhow::{ensure, Context, Result};
use datafusion::arrow::array::{Array, ArrayRef, AsArray, BooleanArray, Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Int64Type, Schema, UInt64Type};
use datafusion::sql::sqlparser::ast::{
    Expr, GroupByExpr, Ident, LockClause, LockType, NonBlock, ObjectName, Query, SelectItem, SetExpr, Statement, TableFactor,
    TransactionAccessMode, TransactionIsolationLevel, TransactionMode, Visit, Visitor,
};
use datafusion::sql::sqlparser::{dialect::GenericDialect, parser::Parser};
use serde::{Deserialize, Serialize};
use serde_json::{json as j, Value};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::Future;
use std::ops::ControlFlow;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};

/// A table's changes in a transaction: rows it adds (new rows, whose `_row_id` is negative until the commit stamps
/// them, and new versions, which keep theirs), and the snapshot's rows it replaces or deletes. Under SQL's names.
#[derive(Default, Clone)]
struct Changes {
    new: Vec<RecordBatch>,
    old: Vec<RecordBatch>,
}

/// A keyed table's kept rows by key, as text (`key_texts`): how many of the transaction's own rows hold each key (`live`, over
/// `Changes::new`), and the keys of the table's rows it replaced or deleted (`gone`, over `Changes::old`). `keep` keeps it as it
/// keeps the rows, and ROLLBACK TO makes it again from the rows it puts back (a savepoint holds rows only, so taking one costs
/// what it did). A plain INSERT's check and a read's overlay ask it rather than go over every row kept: when the check did, a
/// transaction of 2,000 one-row INSERTs took 4.9 ms an INSERT by its end, against 0.9 ms at its start.
#[derive(Default)]
struct ByKey {
    key: Vec<String>, // (SQL's names)
    live: HashMap<String, u32>,
    gone: HashSet<String>,
}

impl ByKey {
    /// The keys of a table's kept rows, worked out from them.
    fn of(key: &[String], c: Option<&Changes>) -> Result<ByKey> {
        let mut by = ByKey { key: key.to_vec(), ..Default::default() };
        if let Some(c) = c {
            c.old.iter().try_for_each(|b| by.taken(b))?;
            c.new.iter().try_for_each(|b| by.added(b))?;
        }
        Ok(by)
    }

    /// The transaction's own rows `b` are kept.
    fn added(&mut self, b: &RecordBatch) -> Result<()> {
        key_texts(b, &self.key)?.into_iter().for_each(|k| *self.live.entry(k).or_default() += 1);
        Ok(())
    }

    /// The transaction's own rows `b` are replaced (by a new version, or deleted).
    fn dropped(&mut self, b: &RecordBatch) -> Result<()> {
        for k in key_texts(b, &self.key)? {
            if let Some(n) = self.live.get_mut(&k) {
                *n -= 1;
                if *n == 0 {
                    self.live.remove(&k);
                }
            }
        }
        Ok(())
    }

    /// The table's rows `b` are replaced or deleted.
    fn taken(&mut self, b: &RecordBatch) -> Result<()> {
        self.gone.extend(key_texts(b, &self.key)?);
        Ok(())
    }

    /// For each key (`key_texts`): Some(true) a row the transaction keeps holds it, Some(false) it took the table's row of it
    /// away, None neither (`constraints::key_check_over`).
    fn kept(&self, keys: &[String]) -> Vec<Option<bool>> {
        keys.iter().map(|k| if self.live.contains_key(k) { Some(true) } else { self.gone.contains(k).then_some(false) }).collect()
    }
}

/// READ COMMITTED or REPEATABLE READ (ADR-067 §1). READ UNCOMMITTED is READ COMMITTED, as in Postgres.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Level {
    Committed,
    Repeatable,
}

impl Level {
    fn name(self) -> &'static str {
        match self {
            Level::Committed => "read committed",
            Level::Repeatable => "repeatable read",
        }
    }
}

/// The keys of rows, as text: a keyed table's key, or an append table's `_row_id`.
pub type Keys = Vec<Vec<String>>;

/// What a transaction read of the rows it changed, per table (ADR-067 §6): the commit each row was read at, and for
/// rows no lock covers, `floor`: the least commit its statements read at. Sent to the leader as it is.
#[derive(Default, Clone, Serialize, Deserialize)]
pub struct Reads {
    pub(crate) floor: Option<u64>,
    #[serde(with = "pairs")]
    pub(crate) keys: HashMap<Vec<String>, u64>,
}

/// A transaction's keys as JSON wants them: a list of (key, commit) pairs, since a key is itself a list.
mod pairs {
    use super::HashMap;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(keys: &HashMap<Vec<String>, u64>, s: S) -> Result<S::Ok, S::Error> {
        let mut pairs: Vec<_> = keys.iter().collect();
        pairs.sort();
        s.collect_seq(pairs)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<HashMap<Vec<String>, u64>, D::Error> {
        Ok(Vec::<(Vec<String>, u64)>::deserialize(d)?.into_iter().collect())
    }
}

/// Open, or ended by the idle timeout (ADR-067 §10): an ended transaction keeps no writes and no locks, only the refusal
/// its session's statements get until a transaction word ends it (kept `ENDED_FOR` at most).
#[derive(Clone, Copy, PartialEq)]
enum State {
    Open,
    Ended(Instant),
}

struct Txn {
    id: String,             // (also the owner of its locks on the leader)
    job: Option<String>,    // what names its commit, when a job does (`alone`): sent again, it is a duplicate
    lake: String,           // the lake's url: where its locks are
    level: Level,
    read_only: bool,
    snapshot: u64,          // the commit a statement reads at (REPEATABLE READ: the one BEGIN saw)
    ran: bool,              // a statement has run: SET TRANSACTION is refused from now on
    leader: Option<String>, // a follower's leader, which keeps its locks
    locked: bool,           // it holds row locks
    state: State,
    tables: BTreeMap<String, Changes>,
    by_key: BTreeMap<String, ByKey>, // (a keyed table's kept rows' keys: `ByKey`)
    reads: BTreeMap<String, Reads>,
    marks: Vec<Mark>,       // its savepoints, oldest first (ADR-067 §9)
    failed: Option<String>,
    used: Instant,          // when a statement last ended (or began): the idle timeout counts from it
    busy: usize,            // statements in progress (`Busy`): a statement that waits for a lock keeps it alive
    next: i64,              // the next new row's temporary id (-1, -2, …)
}

/// A savepoint: the transaction's kept writes and reads as they were when it was taken. ROLLBACK TO puts them back; the
/// row locks taken since stay held until the transaction ends.
#[derive(Clone)]
struct Mark {
    name: String,
    tables: BTreeMap<String, Changes>,
    reads: BTreeMap<String, Reads>,
}

/// Every session's transaction, and whether the upkeep loop runs (`upkeep`): one lock for all of it.
#[derive(Default)]
struct Book {
    txns: HashMap<String, Txn>,
    upkeep: bool,
}

crate::per_node!(static BOOK: Mutex<Book> = Default::default());

tokio::task_local! {
    /// Set by `at_once` around the statements that run at once in an open transaction (DDL, and the two steps of
    /// CREATE TABLE … AS): `statement` lets them through as they are.
    static AT_ONCE: bool;
    /// The transaction a statement began in (`began`): a statement that outlived it is refused (`statement`, `keep`).
    static BEGAN: Option<String>;
}

/// Ended transactions are kept this long, so their sessions get the refusal (25P03) and not a new transaction.
const ENDED_FOR: Duration = Duration::from_secs(3600);

const NO_SESSION: &str = "a transaction is a session's: a Postgres connection's, or the Python or JavaScript client's (over HTTP, send x-pondra-session: <id>; over MCP, the Mcp-Session-Id that initialize gave; over Flight SQL, BeginTransaction)";
/// Separates a key's parts as text: in a lock's name and in the keys the overlay filters on.
pub const KEY_SEP: &str = "\u{1}";
/// A statement that may take row locks (`FOR UPDATE`, `FOR SHARE`, and the weaker `FOR NO KEY UPDATE` and `FOR KEY SHARE`,
/// which take the same locks here): checked before anything is parsed, so a statement without one costs nothing.
static LOCKING: LazyLock<regex::Regex> = LazyLock::new(|| regex::Regex::new(r"(?i)\bfor\s+(?:no\s+key\s+update|key\s+share|update|share)\b").expect("a regex"));
const NESTED_LOCK: &str = "a row lock in a subquery, a union or a write's query isn't taken here: lock one table's rows, in a SELECT of its own";
const ONE_TABLE: &str = "FOR UPDATE over a join, GROUP BY, DISTINCT or WITH isn't taken here: lock one table's rows, in a SELECT of its own";
const MERGE_TABLE: &str = "is a merge table: its rows add up as they're read, so UPDATE and DELETE aren't taken; insert rows that add up to the change";
/// The words a `BEGIN` may take after its first (`control`).
const BEGIN_WORDS: [&str; 14] = ["WORK", "TRANSACTION", "ISOLATION", "LEVEL", "READ", "WRITE", "ONLY", "REPEATABLE", "SERIALIZABLE", "COMMITTED", "UNCOMMITTED", "SNAPSHOT", "DEFERRABLE", "NOT"];

// ---------------------------------------------------------------- the book

/// The open transaction of session `s`, with the book locked for `f` (None: the session has none open).
fn at<R>(s: &str, f: impl FnOnce(&mut Txn) -> R) -> Option<R> {
    BOOK.lock().unwrap().txns.get_mut(s).filter(|t| t.state == State::Open).map(f)
}

/// `at` for the current session.
fn with_txn<R>(f: impl FnOnce(&mut Txn) -> R) -> Option<R> { at(&crate::temp::current()?, f) }

/// Is session `s`'s transaction ended by the idle timeout, with no transaction word since?
fn ended(s: &str) -> bool { BOOK.lock().unwrap().txns.get(s).is_some_and(|t| t.state != State::Open) }

/// The session's open transaction's snapshot, if it has one: every read in it is as of then.
pub fn snapshot() -> Option<u64> { with_txn(|t| t.snapshot) }

/// Is a transaction open in this session, ended by the idle timeout or not? Its statements go through `gate`.
pub fn open() -> bool { crate::temp::current().is_some_and(|s| held(&s)) }

/// Does the session have a transaction in the book (open, or ended)? Keeps its temporary tables.
pub fn held(session: &str) -> bool { BOOK.lock().unwrap().txns.contains_key(session) }

/// The current session's open transaction's id, if it has one (a cursor without HOLD lives as long as it).
pub fn id() -> Option<String> { with_txn(|t| t.id.clone()) }

/// Is a write in the session's open transaction pending on `table` (rows it kept)?
pub fn touched(table: &str) -> bool { with_txn(|t| t.tables.get(table).is_some_and(pending)).unwrap_or(false) }

/// The leader a transaction on this node keeps its locks at (None: this node leads).
fn leader_of(app: &crate::server::App) -> Option<String> { app.seq.is_none().then(|| app.cluster.leader.addr.clone()) }

/// `SHOW` of a transaction's setting (ADR-067 §1): what the open transaction runs at (the session's default outside
/// one), and the session's defaults. None: not one of these.
pub fn shown(name: &str) -> Option<String> {
    let open = with_txn(|t| (t.level, t.read_only));
    Some(match name {
        "transaction_isolation" => open.map_or_else(|| default_level().unwrap_or(Level::Committed), |(l, _)| l).name().into(),
        "default_transaction_isolation" => default_level().unwrap_or(Level::Committed).name().into(),
        "transaction_read_only" => on_off(open.map_or_else(default_read_only, |(_, r)| r)).into(),
        "default_transaction_read_only" => on_off(default_read_only()).into(),
        _ => return None,
    })
}

/// A session ended (its connection closed), or its transaction is over: it is removed, and its locks let go.
pub fn end(session: &str) {
    let gone = BOOK.lock().unwrap().txns.remove(session);
    if let Some(t) = gone {
        release_txn(&t);
    }
}

/// A statement failed in this session's transaction: it is failed now (25P02 for the ones that follow).
pub fn failed(message: &str) {
    with_txn(|t| {
        t.failed.get_or_insert_with(|| message.to_string());
    });
}

/// The checks every statement meets in an open transaction: a failed one refuses it (25P02), and under READ ONLY a write
/// refuses (25006, naming it as `word` does). The words are read only under READ ONLY: a WITH is parsed for them.
fn refused(t: &Txn, word: impl FnOnce() -> Option<String>) -> Result<()> {
    if let Some(why) = &t.failed {
        return Err(aborted(why));
    }
    match if t.read_only { word() } else { None } {
        Some(w) => Err(read_only(&w)),
        None => Ok(()),
    }
}

/// A write's checks where it comes in (`refused`), in the open transaction.
fn writable(word: &str) -> Result<()> {
    with_txn(|t| {
        t.used = Instant::now();
        refused(t, || Some(word.to_string()))
    })
    .unwrap_or(Ok(()))
}

// ---------------------------------------------------------------- transaction words

/// Is a ROLLBACK's first words `[WORK | TRANSACTION] TO`? (`words` holds at most three.)
fn to_point(words: &[String]) -> bool {
    let at = |i: usize| words.get(i).map(String::as_str);
    at(1) == Some("TO") || (matches!(at(1), Some("WORK" | "TRANSACTION")) && at(2) == Some("TO"))
}

/// `BEGIN`, `START TRANSACTION`, `COMMIT`, `END`, `ROLLBACK`, `ABORT`, and the savepoint words: which, if `sql` is one.
/// (`SET TRANSACTION` is `settings.rs`'s.)
pub fn control(sql: &str) -> Option<&'static str> {
    // (past leading comments: dbt sends `/* {"app": "dbt"} */ commit;`)
    let words: Vec<String> = crate::write::first_word(sql).trim_end_matches(';').split_whitespace().take(3).map(|w| w.to_uppercase()).collect();
    let rest_ok = |from: usize| words.get(from..).unwrap_or(&[]).iter().all(|w| BEGIN_WORDS.contains(&w.as_str()) || w.starts_with("ISOLATION"));
    match words.first().map(String::as_str) {
        Some("BEGIN") if rest_ok(1) => Some("BEGIN"),
        Some("START") if words.get(1).map(String::as_str) == Some("TRANSACTION") => Some("BEGIN"),
        Some("COMMIT" | "END") if rest_ok(1) => Some("COMMIT"),
        // ROLLBACK [WORK | TRANSACTION] TO [SAVEPOINT] name: a savepoint's, not the transaction's
        Some("ROLLBACK") if to_point(&words) => Some("ROLLBACK TO"),
        Some("ROLLBACK" | "ABORT") if rest_ok(1) => Some("ROLLBACK"),
        Some("SAVEPOINT") => Some("SAVEPOINT"),
        Some("RELEASE") => Some("RELEASE"),
        _ => None,
    }
}

/// `BEGIN`, `COMMIT` or `ROLLBACK` (`control`), or a savepoint word, for the current session. Its answer: the command's
/// tag (a failed transaction's COMMIT is a ROLLBACK, as in Postgres), and a warning, if any.
pub async fn command(app: &crate::server::App, word: &str, sql: &str) -> Result<(&'static str, Option<String>)> {
    let s = crate::temp::current().context(NO_SESSION)?;
    // (an idle end: a COMMIT must not say its writes landed; a transaction word ends the refusal)
    if ended(&s) {
        if word == "COMMIT" {
            end(&s);
            return Err(idle_commit());
        }
        if !matches!(word, "ROLLBACK" | "BEGIN") {
            return Err(idle_ended());
        }
        end(&s);
        if word == "ROLLBACK" {
            return Ok(("ROLLBACK", None));
        }
    }
    match word {
        "BEGIN" => begin(app, &s, sql),
        "SAVEPOINT" => savepoint(&s, sql),
        "RELEASE" => release_savepoint(&s, sql),
        "ROLLBACK TO" => rollback_to(&s, sql),
        "ROLLBACK" => match BOOK.lock().unwrap().txns.remove(&s) {
            Some(t) => {
                release_txn(&t);
                Ok(("ROLLBACK", None))
            }
            None => Ok(("ROLLBACK", Some("there is no transaction in progress".into()))),
        },
        _ => commit(app, &s).await.map(|(tag, warning, _)| (tag, warning)),
    }
}

/// Runs `f` on the session's open transaction (25P01 outside one), answering `tag`.
fn on_open(s: &str, what: &str, tag: &'static str, f: impl FnOnce(&mut Txn) -> Result<()>) -> Result<(&'static str, Option<String>)> {
    at(s, f).unwrap_or_else(|| Err(crate::codes::coded("25P01", format!("{what} can only be used in transaction blocks")))).map(|()| (tag, None))
}

/// `SAVEPOINT name` (ADR-067 §9): a mark in the transaction's kept writes, which `ROLLBACK TO` returns to.
fn savepoint(s: &str, sql: &str) -> Result<(&'static str, Option<String>)> {
    let name = savepoint_name(sql)?;
    on_open(s, "SAVEPOINT", "SAVEPOINT", |t| {
        refused(t, || None)?;
        t.marks.push(Mark { name, tables: t.tables.clone(), reads: t.reads.clone() });
        Ok(())
    })
}

/// `RELEASE [SAVEPOINT] name`: forgets the savepoint and those taken after it. Their writes stay.
fn release_savepoint(s: &str, sql: &str) -> Result<(&'static str, Option<String>)> {
    let name = savepoint_name(sql)?;
    on_open(s, "RELEASE SAVEPOINT", "RELEASE", |t| {
        refused(t, || None)?;
        let i = mark_of(t, &name)?;
        t.marks.truncate(i);
        Ok(())
    })
}

/// `ROLLBACK [WORK] TO [SAVEPOINT] name`: the kept writes and reads go back to the savepoint, which stays, and a failed
/// state is cleared, so the transaction goes on. Row locks taken since stay held until it ends (ADR-067 §9).
fn rollback_to(s: &str, sql: &str) -> Result<(&'static str, Option<String>)> {
    let name = savepoint_name(sql)?;
    on_open(s, "ROLLBACK TO SAVEPOINT", "ROLLBACK", |t| {
        let i = mark_of(t, &name)?;
        let Mark { tables, reads, .. } = t.marks[i].clone();
        t.by_key = t.by_key.iter().map(|(name, b)| Ok((name.clone(), ByKey::of(&b.key, tables.get(name))?))).collect::<Result<_>>()?;
        (t.tables, t.reads) = (tables, reads);
        t.marks.truncate(i + 1);
        t.failed = None;
        Ok(())
    })
}

/// The newest savepoint of that name (3B001 when there is none). As in Postgres, that error fails the transaction.
fn mark_of(t: &mut Txn, name: &str) -> Result<usize> {
    t.marks.iter().rposition(|m| m.name == name).ok_or_else(|| {
        let why = format!("savepoint \"{name}\" does not exist");
        t.failed.get_or_insert_with(|| why.clone());
        crate::codes::coded("3B001", why)
    })
}

/// The name a savepoint statement gives: an unquoted one lower-cased, as Postgres folds it.
fn savepoint_name(sql: &str) -> Result<String> {
    let ident = match Parser::parse_sql(&GenericDialect {}, sql)?.pop() {
        Some(Statement::Savepoint { name }) | Some(Statement::ReleaseSavepoint { name }) => name,
        Some(Statement::Rollback { savepoint: Some(name), .. }) => name,
        _ => return Err(crate::codes::coded("42601", "a savepoint statement needs a savepoint's name")),
    };
    Ok(match ident.quote_style {
        Some(_) => ident.value,
        None => ident.value.to_lowercase(),
    })
}

/// The error for a statement in a failed transaction (25P02): until its end, only ROLLBACK (or COMMIT) goes on.
fn aborted(why: &str) -> anyhow::Error {
    crate::codes::coded("25P02", format!("current transaction is aborted, commands ignored until end of transaction block (it failed: {why})"))
}

/// How long a transaction may sit idle before the idle timeout ends it (`PONDRA_TXN_IDLE_SECS`, 600).
fn idle_limit() -> Duration {
    Duration::from_secs(crate::env::or("PONDRA_TXN_IDLE_SECS", 600))
}

/// The error for a session whose transaction the idle timeout ended (25P03): its writes were dropped, so its statements
/// are refused until a transaction word, never autocommitted.
fn idle_ended() -> anyhow::Error {
    crate::codes::coded("25P03", "its transaction ended while idle (PONDRA_TXN_IDLE_SECS): its writes were dropped and its row locks let go; ROLLBACK to go on")
}

/// A COMMIT after the idle timeout ended the transaction: nothing was committed, and the answer says so (25P03). Never a
/// success, which would tell the client its writes landed.
fn idle_commit() -> anyhow::Error {
    crate::codes::coded("25P03", format!("the transaction was ended after {} s idle (PONDRA_TXN_IDLE_SECS) and rolled back: nothing was committed", idle_limit().as_secs()))
}

/// `BEGIN [modes]`: a transaction of this session, at the level and access mode its modes and the session's defaults
/// give (ADR-067 §1).
fn begin(app: &crate::server::App, s: &str, sql: &str) -> Result<(&'static str, Option<String>)> {
    let (level, read_only) = plan(&begin_modes(sql)?)?;
    let level = level.map_or_else(default_level, Ok)?;
    let read_only = read_only.unwrap_or_else(default_read_only);
    if !start(app, s, level, read_only, None) {
        return Ok(("BEGIN", Some("there is already a transaction in progress".into())));
    }
    Ok(("BEGIN", None))
}

/// Opens a transaction for session `s` (false: it has one). Its snapshot is taken at its first statement (as Postgres
/// takes it: `gate`), so this is a placeholder until then. Starts the upkeep loop if none runs. Its id is new each time; a
/// `job` names its commit instead (`alone`).
fn start(app: &crate::server::App, s: &str, level: Level, read_only: bool, job: Option<String>) -> bool {
    let mut book = BOOK.lock().unwrap();
    if book.txns.contains_key(s) {
        return false;
    }
    let id = uuid::Uuid::new_v4().simple().to_string(); // (fresh each time: a leader refuses locks to an id that ended)
    let txn = Txn {
        id, job, lake: app.lake.url.clone(), level, read_only, snapshot: app.lake.visible(), ran: false, leader: leader_of(app),
        locked: false, state: State::Open, tables: BTreeMap::new(), by_key: BTreeMap::new(), reads: BTreeMap::new(), marks: vec![], failed: None,
        used: Instant::now(), busy: 0, next: -1,
    };
    book.txns.insert(s.to_string(), txn);
    if !book.upkeep {
        book.upkeep = true;
        crate::panics::spawn(upkeep());
    }
    true
}

/// The modes of a `BEGIN` (or `START TRANSACTION`) statement, as sqlparser reads them.
fn begin_modes(sql: &str) -> Result<Vec<TransactionMode>> {
    Ok(match Parser::parse_sql(&GenericDialect {}, sql)?.pop() {
        Some(Statement::StartTransaction { modes, .. }) => modes,
        _ => vec![],
    })
}

/// The level and access mode the modes name (None: not named). SERIALIZABLE, and the isolation levels that aren't built,
/// are refused by name (0A000); a mode named twice is refused (42601).
fn plan(modes: &[TransactionMode]) -> Result<(Option<Level>, Option<bool>)> {
    let (mut level, mut read_only) = (None, None);
    for m in modes {
        match m {
            TransactionMode::IsolationLevel(l) => {
                if level.is_some() {
                    return Err(crate::codes::coded("42601", "conflicting or redundant isolation levels"));
                }
                level = Some(match l {
                    TransactionIsolationLevel::ReadUncommitted | TransactionIsolationLevel::ReadCommitted => Level::Committed,
                    TransactionIsolationLevel::RepeatableRead => Level::Repeatable,
                    TransactionIsolationLevel::Serializable => return Err(crate::codes::coded("0A000", SERIALIZABLE)),
                    _ => return Err(crate::codes::coded("0A000", "that isolation level isn't built here: READ COMMITTED and REPEATABLE READ are")),
                });
            }
            TransactionMode::AccessMode(a) => {
                if read_only.is_some() {
                    return Err(crate::codes::coded("42601", "conflicting or redundant access modes"));
                }
                read_only = Some(matches!(a, TransactionAccessMode::ReadOnly));
            }
        }
    }
    Ok((level, read_only))
}

/// `SET TRANSACTION` (the open transaction: before its first statement) and `SET SESSION CHARACTERISTICS AS TRANSACTION`
/// (the session's defaults), with sqlparser's modes. Its answer: a warning, if there is one.
pub fn modes(modes: &[TransactionMode], for_session: bool) -> Result<Option<String>> {
    let (level, read_only) = plan(modes)?;
    let s = crate::temp::current().context(NO_SESSION)?;
    if for_session {
        if let Some(l) = level {
            set_default(&s, "default_transaction_isolation", l.name())?;
        }
        if let Some(r) = read_only {
            set_default(&s, "default_transaction_read_only", on_off(r))?;
        }
        return Ok(None);
    }
    match at(&s, |t| {
        if t.ran {
            return Err(crate::codes::coded("25001", "SET TRANSACTION must be called before any query"));
        }
        if let Some(l) = level {
            t.level = l;
        }
        if let Some(r) = read_only {
            t.read_only = r;
        }
        Ok(None)
    }) {
        Some(out) => out,
        None => Ok(Some("SET TRANSACTION can only be used in transaction blocks".into())),
    }
}

fn set_default(s: &str, name: &str, value: &str) -> Result<()> {
    crate::temp::with(s, false, |x| Ok(x.settings.insert(name.to_string(), value.to_string())))?;
    Ok(())
}

fn on_off(b: bool) -> &'static str {
    if b {
        "on"
    } else {
        "off"
    }
}

/// The session's default isolation level (`default_transaction_isolation`; READ COMMITTED, as Postgres).
fn default_level() -> Result<Level> {
    let v = crate::settings::shown("default_transaction_isolation").unwrap_or_default().to_lowercase();
    match v.as_str() {
        "repeatable read" | "repeatable_read" => Ok(Level::Repeatable),
        "serializable" => Err(crate::codes::coded("0A000", SERIALIZABLE)),
        _ => Ok(Level::Committed),
    }
}

/// The session's default access mode (`default_transaction_read_only`).
fn default_read_only() -> bool {
    crate::settings::shown("default_transaction_read_only").is_some_and(|v| matches!(v.to_lowercase().as_str(), "on" | "true" | "1"))
}

// ---------------------------------------------------------------- every statement's gate

/// A statement in progress in a session's transaction (`next`): while it lives, the transaction isn't idle, and when it
/// ends the idle time starts again. It names the transaction it began in, so a statement that outlived its transaction
/// doesn't count against the next one.
pub struct Busy(Option<(String, String)>);

impl Busy {
    /// The transaction this statement began in, if one was open (`began`).
    pub fn id(&self) -> Option<String> { self.0.as_ref().map(|(_, id)| id.clone()) }
}

impl Drop for Busy {
    fn drop(&mut self) {
        let Some((s, id)) = self.0.take() else { return };
        at(&s, |t| {
            if t.id == id {
                t.busy -= 1;
                t.used = Instant::now();
            }
        });
    }
}

/// Before each statement, at each door (ADR-067 §1, §2): the gate (`gate`). The statement runs while the guard it returns
/// lives: a transaction with a statement in progress isn't idle, even while that statement waits for a lock.
pub async fn next(app: &crate::server::App, sql: &str) -> Result<Busy> {
    let busy = Busy(crate::temp::current().and_then(|s| {
        let leader = leader_of(app);
        at(&s, |t| {
            t.busy += 1;
            t.leader = leader;
            t.id.clone()
        })
        .map(|id| (s, id))
    }));
    gate(&app.lake, sql)?;
    Ok(busy)
}

/// Runs `f` as a statement that began in transaction `id` (None: in none): a statement that outlives it is refused
/// (25000), never written into the next one (ADR-067 §1). `routines::one` wraps each statement with it.
pub async fn began<F: Future>(id: Option<String>, f: F) -> F::Output { BEGAN.scope(id, f).await }

/// The transaction the current statement began in (`began`), if it began in one.
fn began_id() -> Option<String> { BEGAN.try_with(|b| b.clone()).ok().flatten() }

/// The error for a statement that outlived its transaction (25000).
fn ended_since() -> anyhow::Error { crate::codes::coded("25000", "the transaction this statement began in has ended") }

/// A statement in a failed transaction is refused (25P02), a write in a READ ONLY one, or outside any with
/// `default_transaction_read_only` on, is refused (25006), and a READ COMMITTED transaction reads the newest commit this
/// node sees. REPEATABLE READ's snapshot is taken at its first statement that isn't a setting, as Postgres takes it (not
/// at BEGIN). Words are read only where they decide something.
pub fn gate(lake: &Lake, sql: &str) -> Result<()> {
    let Some(s) = crate::temp::current() else { return Ok(()) };
    let mut book = BOOK.lock().unwrap();
    let Some(t) = book.txns.get_mut(&s) else {
        drop(book);
        return match default_read_only().then(|| write_word(sql)).flatten() {
            Some(w) => Err(read_only(&w)),
            None => Ok(()),
        };
    };
    if t.state != State::Open {
        return Err(idle_ended());
    }
    t.used = Instant::now();
    refused(t, || write_word(sql))?;
    let setting = is_setting(sql);
    if t.level == Level::Committed || (!t.ran && !setting) {
        t.snapshot = lake.visible();
    }
    if !setting {
        t.ran = true;
    }
    Ok(())
}

/// The error for a write in a read-only transaction (25006), as Postgres words it.
fn read_only(word: &str) -> anyhow::Error {
    crate::codes::coded("25006", format!("cannot execute {word} in a read-only transaction"))
}

/// The words Postgres names a statement by in its errors and notices (25006, the DDL notice), for a parsed statement.
fn word_of(stmt: &Stmt) -> &'static str {
    use crate::ddl::Ddl;
    match stmt {
        Stmt::Create(_) | Stmt::Define(..) => "CREATE TABLE",
        Stmt::TempView(..) | Stmt::TempSecret(..) => "CREATE",
        Stmt::Insert(..) | Stmt::InsertInto(..) => "INSERT",
        Stmt::Update(..) => "UPDATE",
        Stmt::Delete(..) => "DELETE",
        Stmt::Merge(_) => "MERGE",
        Stmt::AddColumn(..) | Stmt::SetOptions(..) => "ALTER TABLE",
        Stmt::CopyTo(..) | Stmt::CopyInto(..) => "COPY",
        Stmt::Ddl(ds) => match ds.first() {
            Some(Ddl::DropTable { .. }) => "DROP TABLE",
            Some(Ddl::DropView { .. }) => "DROP VIEW",
            Some(Ddl::AlterColumn { .. } | Ddl::RenameTable { .. } | Ddl::Swap { .. } | Ddl::Constraint { .. }) => "ALTER TABLE",
            _ => "DDL",
        },
        Stmt::Invalid(_) => "DDL",
    }
}

/// The words Postgres names a write by in 25006: None for a statement that reads. (Its text, before the parse.)
fn write_word(sql: &str) -> Option<String> {
    let words: Vec<String> = crate::write::first_word(sql).trim_end_matches(';').split_whitespace().take(2).map(|w| w.to_uppercase()).collect();
    let first = words.first()?.as_str();
    match first {
        "INSERT" | "UPDATE" | "DELETE" | "MERGE" | "TRUNCATE" | "GRANT" | "REVOKE" | "COMMENT" => Some(first.to_string()),
        "CREATE" | "DROP" | "ALTER" => Some(words.join(" ")),
        "COPY" => (!sql.to_uppercase().contains(" TO ")).then(|| "COPY FROM".to_string()),
        "WITH" => with_write(sql), // (its words don't say what it does: its parse does)
        _ => None,
    }
}

/// The write a `WITH` statement makes (`WITH … INSERT`, `… UPDATE`, `… DELETE`, `… MERGE`), by its parse; None for one
/// that only reads, or doesn't parse (the door reports that).
fn with_write(sql: &str) -> Option<String> {
    // (sqlparser reads `WITH … INSERT` as a query whose body is the write)
    let Statement::Query(q) = Parser::parse_sql(&GenericDialect {}, sql).ok()?.pop()? else { return None };
    let word = match *q.body {
        SetExpr::Insert(_) => "INSERT",
        SetExpr::Update(_) => "UPDATE",
        SetExpr::Delete(_) => "DELETE",
        SetExpr::Merge(_) => "MERGE",
        _ => return None,
    };
    Some(word.to_string())
}

/// Statements that set or show something rather than run a query: they don't make a transaction one that has run.
fn is_setting(sql: &str) -> bool {
    let word = crate::write::first_word(sql).split(|c: char| !c.is_ascii_alphabetic()).next().unwrap_or_default().to_uppercase();
    matches!(word.as_str(), "SET" | "RESET" | "SHOW" | "DISCARD" | "DEALLOCATE" | "PREPARE" | "CLOSE")
}

// ---------------------------------------------------------------- the idle end, and locks let go

fn place(t: &Txn) -> Place { (t.lake.clone(), t.leader.clone(), t.id.clone()) }

/// A transaction's row locks go (on commit, rollback, or the end of its session).
fn release_txn(t: &Txn) {
    if t.locked {
        crate::locks::release_place(place(t));
    }
}

/// The transaction in progress holds row locks now (`locks::passes`): its COMMIT or its end lets them go.
pub fn mark_locked() {
    with_txn(|t| t.locked = true);
}

/// Every transaction's row locks, marked as let go: the node is stopping, and they go to their leaders (`locks::release_all`).
pub fn unlock_all() -> Vec<Place> {
    BOOK.lock().unwrap().txns.values_mut().filter(|t| t.locked).map(|t| { t.locked = false; place(t) }).collect()
}

/// Every third of a lock's lease, and a quarter of the idle limit (at most every 30 s): transactions idle for
/// `PONDRA_TXN_IDLE_SECS` end, ended ones older than `ENDED_FOR` are forgotten, and the locks of the rest are renewed, so a
/// session idle in its transaction keeps them while it lives (ADR-067 §4: a lease is for a node that died, not for an idle
/// session). Once the book is empty, this stops, under the book's lock (`start` starts it again under the same lock).
async fn upkeep() {
    loop {
        let tick = (crate::locks::lease() / 3).min(idle_limit() / 4).min(Duration::from_secs(30)).max(Duration::from_millis(10));
        tokio::time::sleep(tick).await;
        let (gone, holding, done) = {
            let mut book = BOOK.lock().unwrap();
            let idle = idle_limit();
            // (a statement in progress, a wait for a lock included, makes a transaction busy, not idle)
            let mut gone = vec![];
            for t in book.txns.values_mut().filter(|t| t.state == State::Open && t.busy == 0 && t.used.elapsed() >= idle) {
                if t.locked {
                    gone.push(place(t));
                    t.locked = false;
                }
                (t.state, t.tables, t.by_key, t.reads, t.marks) = (State::Ended(Instant::now()), BTreeMap::new(), BTreeMap::new(), BTreeMap::new(), vec![]);
            }
            book.txns.retain(|_, t| !matches!(t.state, State::Ended(at) if at.elapsed() >= ENDED_FOR));
            let holding: Vec<Place> = book.txns.values().filter(|t| t.locked && t.state == State::Open).map(place).collect();
            let done = book.txns.is_empty();
            if done {
                book.upkeep = false;
            }
            (gone, holding, done)
        };
        gone.into_iter().for_each(crate::locks::release_place);
        crate::locks::renew_places(holding);
        if done {
            return;
        }
    }
}

// ---------------------------------------------------------------- DDL and temporary tables: at once

/// Runs `f` with its statements at once though a transaction is open: a CREATE TABLE … AS's two steps (they are DDL's, not
/// kept writes: `write`'s CTAS). ADR-067 §8.
pub async fn at_once<F: Future>(f: F) -> F::Output { AT_ONCE.scope(true, f).await }

/// Is the statement coming in one that runs at once (`at_once`)?
fn once() -> bool { AT_ONCE.try_with(|v| *v).unwrap_or(false) }

/// Statements that change what a lake holds (its catalog, not its rows): DDL. A temporary table is the session's
/// (`temp.rs`), so its CREATE is not one of these.
fn is_ddl(stmt: &Stmt) -> bool {
    match stmt {
        Stmt::Create(c) => !c.temporary,
        Stmt::Ddl(_) | Stmt::Define(..) | Stmt::AddColumn(..) | Stmt::SetOptions(..) => true,
        _ => false,
    }
}

/// What a statement that runs at once in this session's open transaction gets before it runs (ADR-067 §8): None when no
/// transaction is open, or the statement runs at once already (`at_once`). Otherwise the refusals (`writable`, and 0A000
/// under REPEATABLE READ: its snapshot can't see what the DDL makes), and the notice to send once it has run.
fn at_once_notice(word: &str) -> Result<Option<String>> {
    if once() || snapshot().is_none() {
        return Ok(None);
    }
    writable(word)?;
    if with_txn(|t| t.level) == Some(Level::Repeatable) {
        return Err(crate::codes::coded("0A000", "DDL in a REPEATABLE READ transaction: its snapshot can't see what the DDL makes. Run it before BEGIN or after COMMIT, or use READ COMMITTED (the default)"));
    }
    Ok(Some(format!("{word} ran at once: DDL isn't part of a transaction here, and ROLLBACK won't undo it")))
}

/// A temporary table's CREATE in this session's open transaction (`temp.rs`): it runs at once too, with the same checks and
/// notice as DDL (`at_once_notice`).
pub fn temp_at_once() -> Result<Option<String>> { at_once_notice("CREATE TEMP TABLE") }

/// DDL in this session's open transaction runs at once (ADR-067 §8): a commit of its own that ROLLBACK won't undo (the
/// notice says so, once it has run: `on_node_listed`). Refused by name on a table this transaction has written, and under
/// REPEATABLE READ (`at_once_notice`). None: the statement isn't DDL, or no transaction is open.
pub async fn ddl_at_once(app: &crate::server::App, stmt: &Stmt) -> Result<Option<String>> {
    if !is_ddl(stmt) {
        return Ok(None);
    }
    let Some(note) = at_once_notice(word_of(stmt))? else { return Ok(None) };
    let mut names = vec![];
    for table in ddl_tables(stmt) {
        if let (None, name) = crate::ddl::resolve(&app.lake, &table).await? {
            names.push(name); // (an attached lake's table isn't this transaction's)
        }
    }
    if let Some(name) = with_txn(|t| names.iter().find(|n| t.tables.get(*n).is_some_and(pending)).cloned()).flatten() {
        return Err(crate::codes::coded("0A000", format!("{name} has writes pending in this transaction: DDL on it would change the shape they were written against. COMMIT first, then run the DDL")));
    }
    Ok(Some(note))
}

/// Does a table have writes this transaction kept (rows it adds or replaces)?
fn pending(c: &Changes) -> bool { !c.new.is_empty() || !c.old.is_empty() }

/// The tables a DDL statement names: the ones it changes the shape of, drops or renames.
fn ddl_tables(stmt: &Stmt) -> Vec<String> {
    match stmt {
        Stmt::Create(c) => vec![crate::write::object(&c.name)],
        Stmt::Define(t, _) | Stmt::AddColumn(t, ..) | Stmt::SetOptions(t, _) => vec![t.clone()],
        Stmt::Ddl(ds) => ds.iter().flat_map(ddl_names).collect(),
        _ => vec![],
    }
}

fn ddl_names(d: &crate::ddl::Ddl) -> Vec<String> {
    use crate::ddl::Ddl;
    match d {
        Ddl::DropTable { name, .. } | Ddl::DropView { name, .. } | Ddl::RenameTable { name, .. } | Ddl::Undrop { name } | Ddl::Clone { name, .. } => vec![name.clone()],
        Ddl::AlterColumn { table, .. } | Ddl::Constraint { table, .. } => vec![table.clone()],
        Ddl::Object(crate::objects::Op::OrAlterTable { name, .. }) => vec![name.clone()], // (CREATE OR ALTER TABLE changes a table's shape)
        Ddl::Swap { name, with } => vec![name.clone(), with.clone()],
        Ddl::Unless { then, .. } | Ddl::Replacing { then, .. } => ddl_names(then),
        _ => vec![],
    }
}

// ---------------------------------------------------------------- statements in a transaction

/// A write's old rows, new rows, the commit they were read at, and the keys its locks cover (None: none taken).
type Changed = (Vec<RecordBatch>, Vec<RecordBatch>, u64, Option<Keys>);

/// 40001: a row a transaction touched changed since it read it (ADR-067 §6).
fn conflict(table: &str) -> anyhow::Error {
    crate::codes::coded("40001", format!("could not serialize access due to concurrent update of {table}: try the transaction again"))
}

/// An INSERT's rows, worked out as of a read point and, under READ COMMITTED, with the keys they write locked (ADR-067 §3).
/// The query may have read a key another transaction changed while the lock waited (a counter's `n + 1`, an upsert's
/// `ON CONFLICT`): then the rows are worked out again as of the commit the locks were granted at (`passes`).
async fn insert_rows(l: &Locks<'_>, stored: &TableMeta, query: &str, snapshot: u64, files: bool) -> Result<(RecordBatch, u64, Option<Keys>)> {
    let lake = &l.app.lake;
    passes(l, snapshot, |at| async move {
        let rows = crate::write::rows_of(lake, l.meta, query, Some(at), files).await?;
        crate::defaults::check(stored, l.name, &rows)?;
        // (over `LOCK_MAX` rows nothing is locked, `passes`, so their keys' text would be made for nothing)
        let keys = if rows.num_rows() > LOCK_MAX { vec![] } else { write_keys(l.meta, &[], std::slice::from_ref(&rows))? };
        Ok((rows, keys))
    })
    .await
}

/// An UPDATE, DELETE or MERGE that works its rows out from a read point (ADR-067 §3): the rows it would change are locked,
/// and if one of them moved since, the rows are worked out again as of the commit the locks were granted at.
async fn changing(l: &Locks<'_>, stmt: &Stmt, snapshot: u64) -> Result<Changed> {
    let (lake, name, meta) = (&l.app.lake, l.name, l.meta);
    let ((old, new), at, keys) = passes(l, snapshot, |at| async move {
        let (old, new) = crate::change::rows_of(lake, name, meta, stmt, at).await?;
        let mut keys = write_keys(meta, &old, &new)?;
        if let (true, Stmt::Merge(m)) = (meta.key.is_empty(), stmt) {
            // (an INSERT … ON CONFLICT on an append table locks the values of its conflict target it writes: a second one for
            // the same value waits for the first, and then reads its row)
            if let Some(on) = m.on_columns() {
                keys.extend(conflict_keys(meta, on, &new)?);
                keys.sort();
                keys.dedup();
            }
        }
        Ok(((old, new), keys))
    })
    .await?;
    Ok((old, new, at, keys))
}

/// An UPDATE of one key from the serving path (ADR-036 §6), without planning: the key's row as of the read point, its new
/// version by `set_to`. None: SQL works the UPDATE out (`change_one`).
async fn point(l: &Locks<'_>, key: &[String], set_to: &[(String, String)], snapshot: u64) -> Result<Option<Changed>> {
    let (lake, name, meta) = (&l.app.lake, l.name, l.meta);
    let (row, at, keys) = passes(l, snapshot, |at| async move {
        let row = change_one(lake, name, meta, key, Some(at), set_to).await?;
        let keys = if row.is_some() { vec![key.to_vec()] } else { vec![] };
        Ok((row, keys))
    })
    .await?;
    Ok(row.map(|(old, new)| (old, new, at, keys)))
}

/// The rows an UPDATE, DELETE or MERGE changes, with the commit they were read at and the keys they lock: one key's by the
/// serving path's point read when the statement is an UPDATE of one key (`point`), else by planning (`changing`). The new
/// versions meet the table's NOT NULL and DEFAULT (invariant 130).
async fn change(l: &Locks<'_>, stored: &TableMeta, stmt: &Stmt, snapshot: u64) -> Result<Changed> {
    let hit = match stmt {
        Stmt::Update(_, set, Some(cond)) => match point_key(&l.app.lake, l.name, stored, set, cond).await? {
            Some(key) => point(l, &key, set, snapshot).await?,
            None => None,
        },
        _ => None,
    };
    let changed = match hit {
        Some(c) => c,
        None => changing(l, stmt, snapshot).await?,
    };
    if let Some(n) = changed.1.iter().find(|b| b.num_rows() > 0) {
        crate::defaults::check(stored, l.name, n)?;
    }
    Ok(changed)
}

/// The answer of a write: the rows it replaced and added, as SQL's counts say them.
fn tally(old: &[RecordBatch], new: &[RecordBatch]) -> Value {
    let count = |b: &[RecordBatch]| b.iter().map(|b| b.num_rows()).sum::<usize>();
    let inserted: usize = new.iter().map(|b| b.column_by_name(crate::sys::ROW_ID).map_or(b.num_rows(), |c| c.null_count())).sum();
    let (replaced, added) = (count(old), count(new));
    j!({"rows": replaced + inserted, "updated": added - inserted, "deleted": replaced + inserted - added, "inserted": inserted})
}

/// A table's stored meta, which must hold row ids (made by Pondra 0.19 or later).
async fn stored_table(lake: &Lake, name: &str) -> Result<TableMeta> {
    let stored: TableMeta = lake.cat.get(&table_key(name)).await?.with_context(|| format!("no table {name}"))?;
    ensure!(stored.logical().ids, "{name} holds rows from before row ids (made before Pondra 0.19): copy it once and change the copy");
    Ok(stored)
}

/// What a change of `name` may not do, refused by name before anything is worked out (ADR-067 §3): a view's table is changed
/// through the table it follows; a merge table's rows add up as they're read, so UPDATE, DELETE and MERGE aren't taken (an
/// INSERT adds rows, which it takes as they are); an UPDATE doesn't set a key column (the row would move to another key:
/// delete it and insert it with its new key). Called by a transaction's statements, by autocommit and by the leader's
/// changes (`change::rows_of`).
pub async fn refuse_change(lake: &Lake, name: &str, meta: &TableMeta, stmt: &Stmt) -> Result<()> {
    if lake.cat.get::<crate::views::View>(&crate::views::view_key(name)).await?.is_some() {
        return Err(crate::codes::coded("0A000", format!("{name} is a view's: change the table it follows")));
    }
    let adds = matches!(stmt, Stmt::Insert(..) | Stmt::InsertInto(..));
    if !meta.merge.is_empty() && !adds {
        return Err(crate::codes::coded("0A000", format!("{name} {MERGE_TABLE}")));
    }
    if let Stmt::Update(_, set, _) = stmt {
        if let Some((column, _)) = set.iter().find(|(c, _)| meta.key.contains(c)) {
            return Err(crate::codes::coded("0A000", format!("UPDATE of {name}'s key column {column} isn't taken here: delete the row, then insert it with its new key")));
        }
    }
    Ok(())
}

/// A statement's kept writes of a transaction: an INSERT's rows, an UPDATE's or DELETE's changes, all under the transaction's
/// own locks. Outside a transaction (none open) they are None: the statement goes to the leader.
pub async fn statement(app: &crate::server::App, stmt: &Stmt, files: bool) -> Result<Option<Value>> {
    // (every write comes here, whatever its text: a comment or a `WITH` in front of it doesn't get it past READ ONLY)
    // (the snapshot and the id are read together: a statement that outlives its transaction is refused at `keep`, ADR-067 §1)
    let began = began_id();
    let Some((snapshot, id, rc)) = with_txn(|t| (t.snapshot, t.id.clone(), t.level == Level::Committed)) else {
        if began.is_some() {
            return Err(ended_since());
        }
        return if default_read_only() { Err(read_only(word_of(stmt))) } else { Ok(None) };
    };
    if began.is_some_and(|b| b != id) {
        return Err(ended_since());
    }
    writable(word_of(stmt))?;
    if once() {
        return Ok(None); // (DDL, and a CREATE TABLE … AS's steps, run at once: `ddl_at_once`)
    }
    let lake = &app.lake;
    let (table, insert) = match stmt {
        Stmt::Insert(t, q) => (t.clone(), Some(q.clone())),
        Stmt::InsertInto(t, names, q) => (t.clone(), Some(crate::write::whole_rows(lake, t, names, q).await?)),
        Stmt::Update(..) | Stmt::Delete(..) | Stmt::Merge(_) => (stmt.table(), None),
        _ => return Err(crate::codes::coded("0A000", format!("{}: not in a transaction (yet): run it outside one", stmt.table()))),
    };
    let (other, name) = crate::ddl::resolve(lake, &table).await?;
    ensure!(other.is_none(), "{table} is an attached lake's: a transaction writes this lake's tables");
    let stored = stored_table(lake, &name).await?;
    let meta = stored.logical();
    refuse_change(lake, &name, &meta, stmt).await?;
    // (an enforced UNIQUE takes no row locks: its COMMIT's check refuses a row changed since it was read. A MERGE and an
    // INSERT … ON CONFLICT lock their rows as an UPDATE does: a counter's upsert waits for the one holding it, as Postgres's does)
    let locking = rc && !crate::constraints::checked(&stored);
    let taken = AtomicBool::new(false);
    let l = Locks { app, name: &name, meta: &meta, owner: id.clone(), txn: true, locking, committed: rc, wait: lock_wait(), taken: &taken };
    let (old, new, at, keys) = match insert {
        Some(query) => {
            let (rows, at, keys) = insert_rows(&l, &stored, &query, snapshot, files).await?;
            if crate::constraints::key_checked(&stored) {
                // A plain INSERT's keys are checked at the statement, where Postgres checks them, so a SAVEPOINT can roll the
                // refusal back: a key this transaction's own rows hold is refused, one it took away is free (`ByKey`), and the
                // rest are looked up in the table as it is now. COMMIT checks them again (`write_changes`).
                let texts = key_texts(&rows, &meta.key)?;
                let kept = with_txn(|t| t.by_key.get(&name).map(|by| by.kept(&texts))).flatten().unwrap_or_else(|| vec![None; texts.len()]);
                crate::constraints::key_check_over(lake, &name, &stored, &rows, &kept).await?;
            }
            (vec![], vec![rows], at, keys)
        }
        None => change(&l, &stored, stmt, snapshot).await?,
    };
    crate::returning::wrote(&old, &new);
    let out = tally(&old, &new);
    keep(&name, &meta.key, old, new, at, keys, &id)?;
    Ok(Some(out))
}

/// A keyed table's UPDATE or DELETE, or a plain INSERT into a table whose key is checked, outside a transaction (ADR-067 §7):
/// a transaction of its own that is this one statement, READ COMMITTED. Its rows are worked out and their keys locked under an
/// owner of its own (`auto-…`, never a session's transaction), and worked out again if they moved; then its change is appended
/// through the log. Its locks are held while the append is in flight, and go with its answer (`Hold`), or with their lease when no
/// answer came; the table is registered as appending meanwhile, so a writer closing it (`locks::close`) waits for the append. It
/// takes no lake lock and no session state. The producer is the job's (`sql:{job}`), so a retried one is a duplicate. MERGE and
/// tables with an enforced UNIQUE don't come here (`write::on_node_listed`).
pub async fn autocommit(app: &crate::server::App, stmt: &Stmt, job: &str) -> Result<Value> {
    let owner = format!("auto-{}", uuid::Uuid::new_v4().simple());
    let taken = AtomicBool::new(false);
    let mut hold = Hold::new((app.lake.url.clone(), leader_of(app), owner.clone()));
    let out = autocommit_as(app, stmt, job, &owner, &taken, &mut hold).await;
    // (after its append's answer, or its refusal: its locks go only if it took any, and a dropped statement's went on the drop)
    hold.finish(taken.load(Ordering::SeqCst));
    out
}

async fn autocommit_as(app: &crate::server::App, stmt: &Stmt, job: &str, owner: &str, taken: &AtomicBool, hold: &mut Hold) -> Result<Value> {
    if matches!(stmt, Stmt::Insert(..)) {
        return insert_as(app, stmt, job, owner, taken, hold).await;
    }
    let lake = &app.lake;
    let name = stmt.table();
    let stored = stored_table(lake, &name).await?;
    let meta = stored.logical();
    refuse_change(lake, &name, &meta, stmt).await?;
    crate::views::can_follow(lake, &name, false).await?;
    if let Some(v) = crate::views::filling(lake, &name).await? {
        return Err(crate::codes::coded("40001", format!("could not serialize access: materialized view {v} is still being filled from {name}'s rows")));
    }
    let l = Locks { app, name: &name, meta: &meta, owner: owner.to_string(), txn: false, locking: true, committed: true, wait: lock_wait(), taken };
    let (old, new, _, _) = change(&l, &stored, stmt, lake.visible()).await?;
    crate::returning::wrote(&old, &new);
    let out = tally(&old, &new);
    let Some(batch) = crate::change::upserts(&stored, &old, &new)? else {
        return Ok(j!({"rows": 0, "updated": 0, "deleted": 0, "inserted": 0})); // (no row matched)
    };
    pause_for_tests().await;
    let src = crate::log::Src { producer: format!("sql:{job}"), seq: 1, prev: None };
    let log = app.log()?;
    hold.sending(); // (from here its locks go with its answer, not before: the next writer must see this append)
    let ack = match log.append(name.clone(), src, batch).await {
        Ok(ack) => ack,
        Err(e) => {
            if crate::log::unanswered(&e) {
                hold.lost(); // (no answer came: the append may still land, so its locks wait for their lease)
            }
            return Err(e);
        }
    };
    Ok(if ack.duplicate { j!({"duplicate": true}) } else { out })
}

/// A plain INSERT outside a transaction (`autocommit`). Its rows are worked out and their keys locked as an UPDATE's are, so two
/// nodes inserting one new key at once wait for each other, and the second is refused (23505) once the first has committed. A
/// job whose append is in is a duplicate, found before anything is worked out: its rows are in, and they would meet their own
/// keys. An INSERT takes rows back from no one, so none of an UPDATE's or DELETE's checks apply (`refuse_change`,
/// `views::can_follow`, `views::filling`). `key_check` runs under the locks; over `LOCK_MAX` keys no lock is taken, and the check
/// still runs (the documented limit).
async fn insert_as(app: &crate::server::App, stmt: &Stmt, job: &str, owner: &str, taken: &AtomicBool, hold: &mut Hold) -> Result<Value> {
    let lake = &app.lake;
    if crate::write::sent(lake, job).await? {
        return Ok(j!({"duplicate": true}));
    }
    let name = stmt.table();
    let stored = stored_table(lake, &name).await?;
    let meta = stored.logical();
    let sql = crate::write::rows_sql(&meta, stmt)?;
    let files = crate::change::FILES.try_with(|f| *f).unwrap_or(false);
    let l = Locks { app, name: &name, meta: &meta, owner: owner.to_string(), txn: false, locking: true, committed: true, wait: lock_wait(), taken };
    let (batch, _, _) = insert_rows(&l, &stored, &sql, lake.visible(), files).await?;
    // (the lock's grant brought this node up to its leader's commit, so a retry whose append landed while it waited is seen
    // here: the check before the locks can't see it, and the key check would meet the job's own rows)
    if crate::write::sent(lake, job).await? {
        return Ok(j!({"duplicate": true}));
    }
    crate::constraints::key_check(lake, &name, &stored, None, &batch).await?; // (under the locks: the rows the other writer committed are here)
    crate::returning::wrote(&[], std::slice::from_ref(&batch));
    let n = batch.num_rows();
    pause_for_tests().await;
    let src = crate::log::Src { producer: format!("sql:{job}"), seq: 1, prev: None };
    let log = app.log()?;
    hold.sending(); // (from here its locks go with its answer, not before: the next writer must see this append)
    let ack = match log.append(name.clone(), src, batch).await {
        Ok(ack) => ack,
        Err(e) => {
            if crate::log::unanswered(&e) {
                hold.lost(); // (no answer came: the append may still land, so its locks wait for their lease)
            }
            return Err(e);
        }
    };
    Ok(if ack.duplicate { j!({"duplicate": true}) } else { j!({"rows": n}) })
}

/// The test hook `PONDRA_TEST_AUTOCOMMIT_PAUSE_MS` (read once): an autocommitted write waits this long after it works its rows
/// out, with its locks held and its table registered as appending (`tools/harness.py autocommit`'s gate check).
static PAUSE: LazyLock<Option<Duration>> = LazyLock::new(|| crate::env::parse::<u64>("PONDRA_TEST_AUTOCOMMIT_PAUSE_MS").map(Duration::from_millis));

async fn pause_for_tests() {
    if let Some(pause) = *PAUSE {
        tokio::time::sleep(pause).await;
    }
}

/// Keeps a statement's writes in its transaction. A statement that outlived its transaction (the id it captured is not the
/// open one's) is refused (25000), never written into the next one (ADR-067 §1).
fn keep(table: &str, key: &[String], old: Vec<RecordBatch>, new: Vec<RecordBatch>, at: u64, keys: Option<Keys>, id: &str) -> Result<()> {
    let s = crate::temp::current().context(NO_SESSION)?;
    let mut book = BOOK.lock().unwrap();
    let t = book.txns.get_mut(&s).filter(|t| t.state == State::Open).context("no transaction in progress")?;
    if t.id != id {
        return Err(ended_since());
    }
    let c = t.tables.entry(table.to_string()).or_default();
    let mut by = (!key.is_empty()).then(|| t.by_key.entry(table.to_string()).or_insert_with(|| ByKey { key: key.to_vec(), ..Default::default() }));
    let mut mine = HashSet::new(); // (rows of its own it replaces: no `_version`)
    for b in old.iter().filter(|b| b.num_rows() > 0) {
        let ids = b.column_by_name(crate::sys::ROW_ID).context("old rows' ids")?.as_primitive::<Int64Type>();
        let versions = b.column_by_name(crate::sys::VERSION).context("old rows' versions")?;
        let theirs = BooleanArray::from((0..b.num_rows()).map(|i| Some(versions.is_valid(i))).collect::<Vec<_>>());
        mine.extend((0..b.num_rows()).filter(|&i| !versions.is_valid(i)).map(|i| ids.value(i)));
        let taken = datafusion::arrow::compute::filter_record_batch(b, &theirs)?;
        if let Some(by) = by.as_mut() {
            by.taken(&taken)?;
            by.dropped(&datafusion::arrow::compute::filter_record_batch(b, &datafusion::arrow::compute::not(&theirs)?)?)?;
        }
        c.old.push(taken);
    }
    if !mine.is_empty() {
        for b in c.new.iter_mut() {
            let ids = b.column_by_name(crate::sys::ROW_ID).context("new rows' ids")?.as_primitive::<Int64Type>().clone();
            let keep = BooleanArray::from(ids.iter().map(|id| Some(!id.is_some_and(|i| mine.contains(&i)))).collect::<Vec<_>>());
            *b = datafusion::arrow::compute::filter_record_batch(b, &keep)?;
        }
    }
    for b in new.into_iter().filter(|b| b.num_rows() > 0) {
        let given = b.column_by_name(crate::sys::ROW_ID).map(|c| datafusion::arrow::compute::cast(c, &DataType::Int64)).transpose()?;
        let mut ids = Vec::with_capacity(b.num_rows());
        for i in 0..b.num_rows() {
            ids.push(match &given {
                Some(g) if g.is_valid(i) => g.as_primitive::<Int64Type>().value(i),
                _ => {
                    t.next -= 1; // (a temporary id: the commit stamps the real one)
                    t.next + 1
                }
            });
        }
        let kept = set(&b, crate::sys::ROW_ID, Arc::new(Int64Array::from(ids)))?;
        if let Some(by) = by.as_mut() {
            by.added(&kept)?;
        }
        c.new.push(kept);
    }
    note(t.reads.entry(table.to_string()).or_default(), keys, at);
    t.used = Instant::now();
    Ok(())
}

/// Records what a transaction read of a table's rows (`Reads`, ADR-067 §6): each of `keys` at `at`, or for rows no lock
/// covers (`keys` None), the table's floor. A key keeps its earliest point, so a lock taken after an unlocked read doesn't
/// move the point up past it.
fn note(r: &mut Reads, keys: Option<Keys>, at: u64) {
    match keys {
        Some(keys) => {
            for k in keys {
                let earlier = r.keys.get(&k).copied().or(r.floor).map_or(at, |p| p.min(at));
                r.keys.insert(k, earlier);
            }
        }
        None => r.floor = Some(r.floor.map_or(at, |f| f.min(at))),
    }
}

/// Records the rows a locking read locked (`lock_select`) as read at `at`, for the commit's check; the read point moves up
/// to `at` (READ COMMITTED: the statement reads what its locks were granted at).
fn read_keys(table: &str, keys: Keys, at: u64) {
    with_txn(|t| {
        note(t.reads.entry(table.to_string()).or_default(), Some(keys), at);
        if t.level == Level::Committed {
            t.snapshot = t.snapshot.max(at); // (REPEATABLE READ keeps the snapshot BEGIN saw)
        }
        t.used = Instant::now();
    });
}

/// `b` with column `name` set to `column` (added at the end if it has none).
fn set(b: &RecordBatch, name: &str, column: ArrayRef) -> Result<RecordBatch> {
    use datafusion::arrow::datatypes::Field;
    let (mut fields, mut columns) = (b.schema().fields().to_vec(), b.columns().to_vec());
    match b.schema().index_of(name) {
        Ok(i) => columns[i] = column,
        Err(_) => {
            fields.push(Arc::new(Field::new(name, column.data_type().clone(), true)));
            columns.push(column);
        }
    }
    Ok(RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)?)
}

/// New rows' temporary ids taken off (the commit stamps real ones); new versions keep theirs.
fn fresh(new: Vec<RecordBatch>) -> Result<Vec<RecordBatch>> {
    new.into_iter()
        .filter(|b| b.num_rows() > 0)
        .map(|b| {
            let ids = b.column_by_name(crate::sys::ROW_ID).context("ids")?.as_primitive::<Int64Type>().clone();
            set(&b, crate::sys::ROW_ID, Arc::new(ids.iter().map(|i| i.filter(|i| *i >= 0)).collect::<Int64Array>()))
        })
        .collect()
}

/// `base` (`table` as of the snapshot, with its system columns) with the transaction's writes over it: without the rows it
/// replaced (by `_row_id`; a keyed table's, by key), with the rows it added. `sys`: the query names a system column (else
/// they are left out, as `SELECT *` leaves them).
pub fn overlaid(ctx: &datafusion::prelude::SessionContext, table: &str, key: &[String], base: Arc<dyn datafusion::catalog::TableProvider>, sys: bool) -> Result<Arc<dyn datafusion::catalog::TableProvider>> {
    use datafusion::prelude::*;
    let kept = |t: &Txn| t.by_key.get(table).map(|by| by.live.keys().chain(&by.gone).cloned().collect::<HashSet<String>>()).unwrap_or_default();
    let Some((c, keys)) = with_txn(|t| t.tables.get(table).map(|c| (c.clone(), kept(t)))).flatten() else { return Ok(base) };
    let schema = base.schema();
    let mut df = ctx.read_table(base)?;
    let ids: Vec<Expr> = c.old.iter().chain(&c.new).filter_map(|b| b.column_by_name(crate::sys::ROW_ID)).flat_map(|c| c.as_primitive::<Int64Type>().iter().flatten().filter(|i| *i >= 0).collect::<Vec<_>>()).map(lit).collect();
    if !ids.is_empty() {
        df = df.filter(not(col(crate::sys::ROW_ID).in_list(ids, false)))?;
    }
    if !key.is_empty() {
        // (a keyed table's row is its key's: an INSERT of a key there replaces it. One IN list over the key as text, not an
        // OR per key: a transaction that changed 12,000 keys made an expression 12,000 deep, and planning it overflowed the
        // stack and took the node down. A key of several columns joins them with KEY_SEP (\u{1}), which no key's text holds in practice)
        // (the kept rows' keys, `ByKey`: as `key_texts` writes them)
        if !keys.is_empty() {
            let text = match key {
                [one] => cast(col(one), DataType::Utf8),
                _ => datafusion::functions::expr_fn::concat_ws(lit(KEY_SEP), key.iter().map(|k| cast(col(k), DataType::Utf8)).collect()),
            };
            df = df.filter(not(text.in_list(keys.into_iter().map(lit).collect(), false)))?;
        }
    }
    if !c.new.is_empty() {
        let rows = c.new.iter().map(|b| crate::query::conform(b, &schema)).collect::<Result<Vec<_>>>()?;
        df = df.union(ctx.read_table(Arc::new(datafusion::datasource::MemTable::try_new(schema.clone(), vec![rows])?))?)?;
    }
    if !sys {
        let shown: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).filter(|n| !crate::sys::NAMES.contains(&n.as_str())).collect();
        df = df.select_columns(&shown.iter().map(String::as_str).collect::<Vec<_>>())?;
    }
    Ok(df.into_view())
}

/// Each row's key as one text: its columns' (`keys_of`) joined by KEY_SEP. The overlay's IN list, `ByKey` and a plain
/// INSERT's check (`constraints::key_check`) name a key so.
pub(crate) fn key_texts(b: &RecordBatch, key: &[String]) -> Result<Vec<String>> { Ok(keys_of(b, key)?.into_iter().map(|k| k.join(KEY_SEP)).collect()) }

/// Each row's key, as text.
fn keys_of(b: &RecordBatch, key: &[String]) -> Result<Keys> {
    let cols = key.iter().map(|k| Ok(datafusion::arrow::compute::cast(b.column_by_name(k).with_context(|| format!("no key column {k}"))?, &DataType::Utf8)?)).collect::<Result<Vec<ArrayRef>>>()?;
    Ok((0..b.num_rows()).map(|i| cols.iter().map(|c| c.as_string::<i32>().value(i).to_string()).collect()).collect())
}

// ---------------------------------------------------------------- locking reads (SELECT … FOR UPDATE)

/// Counts the queries of a statement that carry a lock clause.
struct Locking(usize);

impl Visitor for Locking {
    type Break = ();
    fn pre_visit_query(&mut self, q: &Query) -> ControlFlow<()> {
        self.0 += usize::from(!q.locks.is_empty());
        ControlFlow::Continue(())
    }
}

/// A locking read in a transaction (ADR-067 §3): a single table's `SELECT … FOR UPDATE` (or `FOR SHARE`, the same locks
/// here) locks the rows it returns. It runs with their keys appended to its columns, so `ORDER BY 1` means what it says.
/// Outside a transaction, and for any other statement, None: a plain read.
pub async fn locked_read(app: &crate::server::App, sql: &str) -> Result<Option<Vec<RecordBatch>>> {
    if !LOCKING.is_match(sql) {
        return Ok(None);
    }
    // (the door's own text: macros expanded, the path written in, Postgres's weaker locks as `FOR UPDATE`)
    let expanded = crate::routines::expand(&app.lake, sql).await?;
    let Ok(stmts) = Parser::parse_sql(&GenericDialect {}, &expanded) else { return Ok(None) }; // (the door reports the error)
    for stmt in &stmts {
        // (refused by name outside a transaction too: SKIP LOCKED isn't a plain read's, whatever the session)
        if let Some((q, lock)) = lock_clause(stmt)? {
            let Some(open) = with_txn(|t| (t.level, t.read_only, t.snapshot)) else { return Ok(None) }; // (outside one, a locking read is a plain read)
            return Ok(Some(lock_select(app, open, q, lock).await?));
        }
    }
    Ok(None)
}

/// The lock clause of a statement's one SELECT, if it has one. Refused by name: SKIP LOCKED (nothing skips a locked row: it
/// is waited for, or refused at once with NOWAIT), and a lock clause in a subquery, a union or a write's query.
fn lock_clause(stmt: &Statement) -> Result<Option<(&Query, &LockClause)>> {
    let mut count = Locking(0);
    let _ = Visit::visit(stmt, &mut count);
    if count.0 == 0 {
        return Ok(None);
    }
    let Statement::Query(q) = stmt else { return Err(crate::codes::coded("0A000", NESTED_LOCK)) };
    if count.0 != 1 || q.locks.len() != 1 {
        return Err(crate::codes::coded("0A000", NESTED_LOCK));
    }
    let lock = &q.locks[0];
    if lock.nonblock == Some(NonBlock::SkipLocked) {
        return Err(crate::codes::coded("0A000", "SKIP LOCKED isn't built here: a locked row is waited for, or refused at once with NOWAIT (55P03)"));
    }
    Ok(Some((&**q, lock)))
}

/// One locking read in the transaction (`locked_read`): its rows are locked (waited for as `lock_timeout` says; at once with
/// NOWAIT) and returned as they are under the locks. The answer's rows are the last run's, without the key columns.
async fn lock_select(app: &crate::server::App, (level, ro, snapshot): (Level, bool, u64), q: &Query, lock: &LockClause) -> Result<Vec<RecordBatch>> {
    let lake = &app.lake;
    if ro {
        let word = if matches!(lock.lock_type, LockType::Share) { "SELECT FOR SHARE" } else { "SELECT FOR UPDATE" };
        return Err(read_only(word));
    }
    let (table, alias) = one_table(q)?;
    if let Some(of) = &lock.of {
        let known = alias.unwrap_or_else(|| table.to_string());
        if !of.to_string().eq_ignore_ascii_case(&known) {
            return Err(crate::codes::coded("0A000", format!("FOR UPDATE OF {of} names a table this SELECT doesn't read")));
        }
    }
    let (other, name) = crate::ddl::resolve(lake, &crate::write::object(&table)).await?;
    if other.is_some() {
        return Err(crate::codes::coded("0A000", "FOR UPDATE on an attached lake's table isn't taken here: lock its rows on the lake that holds it"));
    }
    if lake.cat.get::<crate::views::View>(&crate::views::view_key(&name)).await?.is_some() {
        return Err(crate::codes::coded("0A000", format!("{name} is a view's: lock the table it follows")));
    }
    let Some(stored) = lake.cat.get::<TableMeta>(&table_key(&name)).await? else {
        return Err(crate::codes::coded("0A000", format!("{name} isn't a table: FOR UPDATE locks a table's rows")));
    };
    let meta = stored.logical();
    if !meta.merge.is_empty() {
        return Err(crate::codes::coded("0A000", format!("{name} is a merge table: its rows add up as they're read, so there is no row to lock")));
    }
    ensure!(meta.ids, "{name} holds rows from before row ids (made before Pondra 0.19): copy it once and change the copy");
    crate::auth::may_lock(&name)?; // (the right to UPDATE it: Postgres asks that of FOR UPDATE)
    let append = meta.key.is_empty();
    let cols = if append { vec![crate::sys::ROW_ID.to_string()] } else { meta.key.clone() };
    let (n, table_name) = (cols.len(), name.as_str());
    let text = crate::routines::sql(&Statement::Query(Box::new(with_keys(q, &cols))));
    let id = with_txn(|t| t.id.clone()).context(NO_SESSION)?;
    let nowait = matches!(lock.nonblock, Some(NonBlock::Nowait));
    let wait = if nowait { Some(Duration::ZERO) } else { lock_wait() };
    let taken = AtomicBool::new(false);
    let l = Locks { app, name: table_name, meta: &meta, owner: id, txn: true, locking: true, committed: level == Level::Committed, wait, taken: &taken };
    // (no pass cap: a pass that repeats locks a key it hadn't held, and one that adds none ends the passes (`passes`); a job
    // queue's workers wait their turn as Postgres's FOR UPDATE makes them, rather than failing with 40001 after a few tries)
    let (rows, at, keys) = passes(&l, snapshot, |at| {
        let text = &text;
        async move {
            let (schema, batches) = run_at(lake, text, at).await?;
            let (rows, keys) = split_keys(&schema, &batches, n, append)?;
            if keys.len() > LOCK_MAX {
                return Err(too_many(table_name));
            }
            Ok((rows, keys))
        }
    })
    .await?;
    read_keys(table_name, keys.unwrap_or_default(), at);
    Ok(rows)
}

/// `q` with its table's key columns (or `_row_id`s) appended to its projection, aliased apart: its own columns, and what
/// `ORDER BY 1` means, are as written. Its lock clause goes (`run_at` runs it as a read).
fn with_keys(q: &Query, cols: &[String]) -> Query {
    let mut p = q.clone();
    p.locks.clear();
    if let SetExpr::Select(s) = &mut *p.body {
        for (i, c) in cols.iter().enumerate() {
            s.projection.push(SelectItem::ExprWithAlias { expr: Expr::Identifier(Ident::with_quote('"', c.as_str())), alias: Ident::with_quote('"', format!("pondra_key_{i}")) });
        }
    }
    p
}

/// The query's rows as of commit `at`, as this node sees them (a session's own writes over them), and their columns (an
/// empty answer keeps them: `split_keys`).
async fn run_at(lake: &Lake, text: &str, at: u64) -> Result<(Schema, Vec<RecordBatch>)> {
    let ctx = crate::query::session_at(lake, text, "", Some(at)).await?;
    let df = crate::query::sql(&ctx, &crate::asof::as_of(text)?).await?;
    let rows = crate::engine::collect(df).await?; // (never empty: an answer with no rows keeps its columns as they run)
    Ok((rows[0].schema().as_ref().clone(), rows))
}

/// A run's rows without its last `n` columns (the appended keys), and those rows' keys as text (a temporary `_row_id` is
/// the transaction's own row, nobody's to lock: left out).
fn split_keys(schema: &Schema, batches: &[RecordBatch], n: usize, append: bool) -> Result<(Vec<RecordBatch>, Keys)> {
    let (mut rows, mut keys) = (vec![], vec![]);
    for b in batches {
        let cols = b.num_columns();
        let text = (cols - n..cols).map(|i| datafusion::arrow::compute::cast(b.column(i), &DataType::Utf8)).collect::<Result<Vec<ArrayRef>, _>>()?;
        for i in 0..b.num_rows() {
            let key: Vec<String> = text.iter().map(|c| c.as_string::<i32>().value(i).to_string()).collect();
            if !(append && key[0].starts_with('-')) {
                keys.push(key);
            }
        }
        rows.push(b.project(&(0..cols - n).collect::<Vec<_>>())?);
    }
    if rows.is_empty() {
        // (no rows: the answer still has its columns, as a query's does)
        rows.push(RecordBatch::new_empty(Arc::new(schema.project(&(0..schema.fields().len() - n).collect::<Vec<_>>())?)));
    }
    keys.sort();
    keys.dedup();
    Ok((rows, keys))
}

/// The one table a locking SELECT reads, and its alias: a single table, not joined, grouped, distinct or with a WITH. Anything
/// else is refused by name.
fn one_table(q: &Query) -> Result<(ObjectName, Option<String>)> {
    let SetExpr::Select(s) = &*q.body else { return Err(crate::codes::coded("0A000", ONE_TABLE)) };
    let grouped = match &s.group_by {
        GroupByExpr::All(_) => true,
        GroupByExpr::Expressions(e, _) => !e.is_empty(),
    };
    if q.with.is_some() || s.distinct.is_some() || grouped || s.having.is_some() || s.from.len() != 1 || !s.from[0].joins.is_empty() {
        return Err(crate::codes::coded("0A000", ONE_TABLE));
    }
    match &s.from[0].relation {
        TableFactor::Table { name, alias, args: None, .. } => Ok((name.clone(), alias.as_ref().map(|a| a.name.value.clone()))),
        _ => Err(crate::codes::coded("0A000", ONE_TABLE)),
    }
}

fn too_many(table: &str) -> anyhow::Error {
    crate::codes::coded("0A000", format!("FOR UPDATE over more than {LOCK_MAX} rows of {table} isn't taken here: narrow its WHERE"))
}

// ---------------------------------------------------------------- the commit (the leader)

/// A transaction's writes, for the leader: per table, its new rows and the snapshot's rows it replaced (Arrow IPC, base64),
/// and what it read of the rows it changed (`reads`: per table, its floor and each row's commit).
#[derive(Serialize, Deserialize)]
pub struct Commit {
    pub id: String,
    /// What names its commit when a job does (`alone`), in place of `id`, which owns its locks: a job sent again is a duplicate.
    #[serde(default)]
    pub job: Option<String>,
    #[serde(default)]
    pub reads: BTreeMap<String, Reads>,
    pub tables: Vec<(String, String, String)>,
    /// The rows themselves, table by table as `tables` lists them (new, old), when this node leads: handed over as they
    /// are, with nothing encoded to send or read back (`commit`). Empty when the commit came over HTTP.
    #[serde(skip)]
    pub rows: Vec<(Vec<RecordBatch>, Vec<RecordBatch>)>,
}

/// The batches that have rows, in one schema: what a commit's rows are, sent as IPC or handed over (`commit`).
fn unified(b: &[RecordBatch]) -> Result<Vec<RecordBatch>> {
    let b: Vec<RecordBatch> = b.iter().filter(|b| b.num_rows() > 0).cloned().collect();
    if b.is_empty() {
        return Ok(vec![]);
    }
    // One stream has one schema: an UPDATE's rows carry `_created_at`, an INSERT's after it don't, so every batch takes every
    // column any has (NULL where it has none).
    let mut fields: Vec<datafusion::arrow::datatypes::Field> = vec![];
    for f in b.iter().flat_map(|b| b.schema().fields().iter().cloned().collect::<Vec<_>>()) {
        if !fields.iter().any(|g| g.name() == f.name()) {
            let everywhere = b.iter().all(|b| b.column_by_name(f.name()).is_some());
            fields.push(f.as_ref().clone().with_nullable(f.is_nullable() || !everywhere));
        }
    }
    let schema = Arc::new(Schema::new(fields));
    b.iter().map(|b| crate::query::conform(b, &schema)).collect()
}

/// The rows as base64 Arrow IPC, for the wire. A small commit goes uncompressed: a ZSTD context costs a call more than a
/// small commit's bytes save. `read_ipc` reads both.
fn ipc(b: &[RecordBatch]) -> Result<String> {
    use base64::Engine;
    let b = unified(b)?;
    if b.is_empty() {
        return Ok(String::new());
    }
    let bytes: usize = b.iter().map(|b| b.get_array_memory_size()).sum();
    let encoded = if bytes < 1 << 20 { crate::query::ipc(&b)? } else { crate::log::encode_ipc(&b)? };
    Ok(base64::engine::general_purpose::STANDARD.encode(encoded))
}

fn unipc(s: &str) -> Result<Vec<RecordBatch>> {
    use base64::Engine;
    Ok(if s.is_empty() { vec![] } else { crate::query::read_ipc(&base64::engine::general_purpose::STANDARD.decode(s)?)? })
}

/// Sends the transaction's writes to the leader, here or over HTTP, and ends the transaction: its locks go (`release_txn`)
/// whatever the answer. A failed send lets go too, and the lease covers a send that never arrived.
/// COMMIT: the tag and warning Postgres gives, and what the leader answered (`duplicate`: a job's commit sent again).
async fn commit(app: &crate::server::App, s: &str) -> Result<(&'static str, Option<String>, Value)> {
    let Some(t) = BOOK.lock().unwrap().txns.remove(s) else { return Ok(("COMMIT", Some("there is no transaction in progress".into()), j!({}))) };
    if t.failed.is_some() {
        release_txn(&t);
        return Ok(("ROLLBACK", None, j!({})));
    }
    if t.tables.values().all(|c| c.new.is_empty() && c.old.is_empty()) {
        release_txn(&t); // (it only read)
        return Ok(("COMMIT", None, j!({})));
    }
    let local = app.seq.is_some(); // (this node leads: its rows go to `commit_here` as they are)
    let (mut tables, mut rows) = (vec![], vec![]);
    for (n, c) in &t.tables {
        if local {
            tables.push((n.clone(), String::new(), String::new()));
            rows.push((unified(&fresh(c.new.clone())?)?, unified(&c.old)?));
        } else {
            tables.push((n.clone(), ipc(&fresh(c.new.clone())?)?, ipc(&c.old)?));
        }
    }
    let c = Commit { id: t.id.clone(), job: t.job.clone(), reads: t.reads.clone(), tables, rows };
    let sent = match &app.seq {
        Some(seq) => commit_here(&app.lake, seq, &app.lock, c).await,
        None => crate::write::post(&app.cluster.leader.addr, &crate::write::Request::Txn(c)).await,
    };
    if sent.is_err() {
        release_txn(&t);
    }
    let sent = sent?;
    crate::write::seen_here(app).await; // (the next statement here reads it)
    Ok(("COMMIT", None, sent))
}

/// What a job names a transaction of its own (`alone`): its commit is producer `txn:{this}:{table}`.
fn as_job(job: &str) -> String { format!("job-{job}") }

/// Did a job's transaction of its own (`alone`) commit its changes to `table`? (As this node's catalog has it.)
pub async fn committed(lake: &Lake, job: &str, table: &str) -> Result<bool> {
    Ok(lake.cat.get::<u64>(&crate::store::producer_key(&format!("txn:{}:{table}", as_job(job)))).await?.unwrap_or(0) > 0)
}

/// A write run as a transaction of its own, for its RETURNING (`returning.rs`): BEGIN, the statement and COMMIT, in the caller's
/// session (none open there: its temporary tables and settings are read as the statement's own would be), or one of its own.
/// It is for a change the leader would otherwise work out (`write::changes`): here its rows are worked out where they can be
/// returned, and they still land in one commit. A COMMIT refused with 40001 runs it again, three times at most, since nothing
/// has been answered yet (as Postgres works an UPDATE out again on the newest version of a row it waited for). A job names the
/// transaction's commit, so its commit sent again is a duplicate.
pub async fn alone(app: &crate::server::App, sql: &str, job: Option<String>, files: bool) -> Result<Value> {
    let caller = crate::temp::SESSION.try_with(|s| s.clone()).ok().flatten();
    let s = caller.clone().unwrap_or_else(crate::temp::of_script);
    let job = job.map(|j| as_job(&j));
    let run = async {
        let mut tries = 0;
        loop {
            tries += 1;
            let stmt = crate::write::parse(sql).context("not a write")?;
            start(app, &s, Level::Committed, false, job.clone());
            let out = Box::pin(crate::write::on_node_as(app, stmt, None, files)).await?; // (refused: the session's end rolls it back)
            match commit(app, &s).await {
                Ok((_, _, sent)) => return Ok(if sent["duplicate"] == true { j!({"duplicate": true}) } else { out }),
                Err(e) if crate::codes::of(&e) == "40001" && tries < 3 => continue,
                Err(e) => return Err(e),
            }
        }
    };
    let out = crate::temp::SESSION.scope(Some(s.clone()), run).await;
    match caller {
        Some(_) => end(&s), // (refused before its COMMIT: rolled back, the session kept)
        None => {
            crate::temp::end(&s);
        }
    }
    out
}

/// Leader: checks the transaction against what committed since the points it read at, and writes it as one flush (a
/// retried one, by its id, changes nothing twice). Its locks are let go whatever the answer.
pub async fn commit_here(lake: &Lake, seq: &crate::log::Sequencer, lock: &tokio::sync::Mutex<()>, c: Commit) -> Result<Value> {
    let id = c.id.clone();
    let tables: Vec<String> = c.tables.iter().map(|(name, _, _)| name.clone()).collect();
    let out = {
        let _guard = lock.lock().await;
        // (ADR-067 §7: autocommitted appends to these tables land before this commit reads them, or after it: never between)
        match crate::locks::close(&lake.url, &tables).await {
            Ok(_gate) => write_changes(lake, seq, c).await,
            Err(e) => Err(e),
        }
    };
    crate::locks::release(&lake.url, &id); // (the transaction is over here, and its locks with it: ADR-067 §4)
    out
}

async fn write_changes(lake: &Lake, seq: &crate::log::Sequencer, c: Commit) -> Result<Value> {
    let mut pending = vec![];
    for (i, (name, new, old)) in c.tables.iter().enumerate() {
        // (a commit from this node's own transaction carries its rows; one sent over HTTP carries them as IPC)
        let (new, old) = match c.rows.get(i) {
            Some((n, o)) => (n.clone(), o.clone()),
            None => (unipc(new)?, unipc(old)?),
        };
        let stored: TableMeta = lake.cat.get(&table_key(name)).await?.with_context(|| format!("no table {name} (dropped while the transaction ran)"))?;
        let meta = stored.logical();
        let append = meta.key.is_empty();
        if old.iter().any(|b| b.num_rows() > 0) {
            crate::views::can_follow(lake, name, append).await?;
            if let Some(v) = crate::views::filling(lake, name).await? {
                return Err(crate::codes::coded("40001", format!("could not serialize access: materialized view {v} is still being filled from {name}'s rows")));
            }
        }
        let mut mine: HashSet<Vec<String>> = write_keys(&meta, &old, &new)?.into_iter().collect();
        let reads = c.reads.get(name).cloned().unwrap_or_default();
        // (an append table's conflict targets read by an INSERT … ON CONFLICT are checked here too: a row another writer put under
        // one outside a lock is caught at the commit, as a keyed table's rows are)
        if meta.key.is_empty() {
            mine.extend(reads.keys.keys().filter(|k| is_on(k.as_slice())).cloned());
        }
        conflicts(lake, name, &meta, &reads, &mine).await?;
        let (p, _) = crate::change::appends(lake, seq, name, &stored, old, new, &format!("txn:{}:{name}", c.job.as_ref().unwrap_or(&c.id)), true).await?; // (a plain INSERT's key is new at COMMIT too: `constraints.rs`)
        pending.extend(p);
    }
    if pending.is_empty() {
        return Ok(j!({"committed": true}));
    }
    let outcome = crate::change::submit(lake, seq, &pending).await?;
    Ok(j!({"committed": true, "duplicate": matches!(outcome, crate::log::Outcome::Acks(a) if a.iter().any(|a| a.duplicate))}))
}

/// The keys of the rows a write changes: a keyed table's, as text, from the old and the new rows (in SQL's names); an
/// append table's, the `_row_id`s of the old rows (a new row is nobody's yet, and a temporary id is the transaction's own).
fn write_keys(meta: &TableMeta, old: &[RecordBatch], new: &[RecordBatch]) -> Result<Keys> {
    let mut keys: Keys = vec![];
    if meta.key.is_empty() {
        for ids in old.iter().filter_map(|b| b.column_by_name(crate::sys::ROW_ID)) {
            keys.extend(ids.as_primitive::<Int64Type>().iter().flatten().filter(|i| *i >= 0).map(|i| vec![i.to_string()]));
        }
    } else {
        for b in old.iter().chain(new) {
            keys.extend(keys_of(&meta.to_logical(b).unwrap_or_else(|_| b.clone()), &meta.key)?);
        }
    }
    keys.sort();
    keys.dedup();
    Ok(keys)
}

/// The keys an INSERT … ON CONFLICT takes on an append table (ADR-067 §3, N7): its conflict target's values in the rows it writes,
/// each as `ON`, then each column and its value. A row id's key is one part and a keyed table's is its key's, so an ON key meets
/// neither.
fn conflict_keys(meta: &TableMeta, on: &[String], rows: &[RecordBatch]) -> Result<Keys> {
    let mut keys: Keys = vec![];
    for b in rows {
        for values in keys_of(&meta.to_logical(b).unwrap_or_else(|_| b.clone()), on)? {
            keys.push(on_key(on, &values));
        }
    }
    keys.sort();
    keys.dedup();
    Ok(keys)
}

/// One ON key (`conflict_keys`): `ON`, then each conflict column with its value.
fn on_key(columns: &[String], values: &[String]) -> Vec<String> {
    std::iter::once("ON".to_string()).chain(columns.iter().zip(values).flat_map(|(c, v)| [c.clone(), v.clone()])).collect()
}

/// Is a key an ON key (`on_key`): `ON`, then pairs of a column and its value.
fn is_on(key: &[String]) -> bool {
    key.len() >= 3 && key.len() % 2 == 1 && key[0] == "ON"
}

/// The rows a transaction changed that a commit after the point it read them at changed too: 40001 (ADR-067 §6). A keyed
/// table's row is its key; an append table's is its `_row_id`, changed by a change (the `{t}$deleted` rows) or taken out of
/// files by another engine's commit.
pub(crate) async fn conflicts(lake: &Lake, table: &str, meta: &TableMeta, reads: &Reads, mine: &HashSet<Vec<String>>) -> Result<()> {
    let point = |k: &Vec<String>| reads.keys.get(k).copied().or(reads.floor).unwrap_or(0);
    let Some(from) = mine.iter().map(point).min() else { return Ok(()) };
    if lake.visible() <= from {
        return Ok(());
    }
    let mut changed: Vec<(Vec<String>, u64)> = vec![]; // (the row, the commit that changed it)
    if meta.key.is_empty() {
        for b in crate::query::tail_of(lake, &crate::sys::deleted(table), from, None, true, true, true).await? {
            changed.extend(ids_of(&b, false)?);
        }
        for b in crate::change::taken_out(lake, table, from, None).await? {
            changed.extend(ids_of(&b, true)?);
        }
        // (an INSERT … ON CONFLICT's conflict targets: the log rows that hold one, each as the commit that wrote it)
        let targets: HashSet<Vec<String>> = mine.iter().filter(|k| is_on(k.as_slice())).map(|k| k.iter().skip(1).step_by(2).cloned().collect()).collect();
        for columns in targets {
            for b in crate::query::tail(lake, table, from, None, true).await? {
                for (values, n) in keys_of(&b, &columns)?.into_iter().zip(commits(&b, false)?) {
                    changed.push((on_key(&columns, &values), n));
                }
            }
        }
    } else {
        for b in crate::query::tail(lake, table, from, None, true).await? {
            changed.extend(keys_of(&b, &meta.key)?.into_iter().zip(commits(&b, false)?));
        }
    }
    for (row, n) in changed {
        if mine.contains(&row) && n > point(&row) {
            return Err(conflict(table));
        }
    }
    Ok(())
}

/// Each row's commit: a log row's is its segment (`_ord` above the position, `log::ORD_BITS`), which a file commit's rows
/// carry too, as the commit that recorded them. With `version`, the commit a change taken out of files was made by
/// (`_version`, as `at_commit` gives it).
fn commits(b: &RecordBatch, version: bool) -> Result<Vec<u64>> {
    Ok(if version {
        b.column_by_name(crate::sys::VERSION).context("versions")?.as_primitive::<Int64Type>().iter().map(|v| v.unwrap_or(0) as u64).collect()
    } else {
        b.column_by_name("_ord").context("log positions")?.as_primitive::<UInt64Type>().iter().map(|o| o.unwrap_or(0) >> crate::log::ORD_BITS).collect()
    })
}

/// Each row of an append table's changes, as its `_row_id` and the commit that changed it.
fn ids_of(b: &RecordBatch, version: bool) -> Result<Vec<(Vec<String>, u64)>> {
    let ids = b.column_by_name(crate::sys::ROW_ID).context("row ids")?.as_primitive::<Int64Type>();
    Ok(ids.iter().zip(commits(b, version)?).filter_map(|(id, n)| id.map(|id| (vec![id.to_string()], n))).collect())
}

// ---------------------------------------------------------------- one key, without planning

/// One key's row now (ADR-036 §6): the transaction's own version, the lake's as of the read point, none, or changed since it.
enum Current {
    Row(RecordBatch),
    Gone,
    Changed,
}

/// A row has the table's columns (SQL's names), then `_row_id`, `_created_at` and `_version` (null: the transaction's own).
async fn current(lake: &Lake, table: &str, meta: &TableMeta, key: &[String], snapshot: Option<u64>) -> Result<Current> {
    use datafusion::arrow::datatypes::{Field, TimeUnit};
    let mut fields: Vec<Arc<Field>> = crate::query::schema(&meta.columns)?.fields().to_vec();
    fields.push(Arc::new(Field::new(crate::sys::ROW_ID, DataType::Int64, true)));
    fields.push(Arc::new(Field::new(crate::sys::CREATED, DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), true)));
    fields.push(Arc::new(Field::new(crate::sys::VERSION, DataType::Int64, true)));
    let target = Arc::new(Schema::new(fields));
    if snapshot.is_some() {
        if let Some(c) = with_txn(|t| t.tables.get(table).cloned()).flatten() {
            let want = key.to_vec();
            for b in c.new.iter().rev() {
                if let Some(i) = keys_of(b, &meta.key)?.iter().rposition(|k| *k == want) {
                    return Ok(Current::Row(crate::query::conform(&b.slice(i, 1), &target)?));
                }
            }
            if c.old.iter().any(|b| keys_of(b, &meta.key).is_ok_and(|ks| ks.contains(&want))) {
                return Ok(Current::Gone); // (it deleted it)
            }
        }
    }
    let Some((r, version, created)) = crate::serve::lookup_versioned(lake, table, meta, key).await? else { return Ok(Current::Gone) };
    if snapshot.is_some_and(|s| version > s) {
        return Ok(Current::Changed);
    }
    let r = set(&r, crate::sys::VERSION, Arc::new(Int64Array::from(vec![version as i64])))?;
    let at: ArrayRef = Arc::new(datafusion::arrow::array::TimestampMicrosecondArray::from(vec![created]).with_timezone("UTC"));
    let r = set(&r, crate::sys::CREATED, at)?;
    Ok(Current::Row(crate::query::conform(&r, &target)?))
}

/// `UPDATE t SET … WHERE <key> = <value> [AND …]` of a keyed table, worked out from its one row without planning: its old and
/// new versions, as `change::rows_of` gives them. In a transaction (`snapshot`) the row is its own version or the snapshot's;
/// one changed since refuses it at once (40001: its COMMIT would be). None: not such an UPDATE (SQL works it out).
pub async fn point_change(lake: &Lake, name: &str, stored: &TableMeta, snapshot: Option<u64>, set_to: &[(String, String)], cond: &str) -> Result<Option<(Vec<RecordBatch>, Vec<RecordBatch>)>> {
    let Some(key) = point_key(lake, name, stored, set_to, cond).await? else { return Ok(None) };
    change_one(lake, name, &stored.logical(), &key, snapshot, set_to).await
}

/// The key an UPDATE's `WHERE` names (`<key> = <value> [AND …]`) on a keyed table, as text (`key_text`). None: the UPDATE isn't
/// one key's, sets a key or a column SQL must work out, or names a key the text can't say (a JSON key: the general path).
async fn point_key(lake: &Lake, name: &str, stored: &TableMeta, set_to: &[(String, String)], cond: &str) -> Result<Option<Vec<String>>> {
    let meta = stored.logical();
    if meta.key.is_empty() || !meta.merge.is_empty() || meta.order.is_some() || stored.mapped() || name.contains('.') || name.contains('"')
        || set_to.iter().any(|(c, _)| meta.key.contains(c) || c == "_deleted" || crate::sys::NAMES.contains(&c.as_str()) || !meta.columns.iter().any(|(n, _)| n == c))
    {
        return Ok(None);
    }
    let Some(p) = crate::serve::point(lake, &format!("SELECT * FROM \"{name}\" WHERE {cond}")).await? else { return Ok(None) };
    let Some(key) = p.key.iter().cloned().collect::<Option<Vec<String>>>() else { return Ok(None) };
    Ok(key_text(&meta, &key).ok())
}

/// A key's values named as the locks and the commit's check name them: each value cast to its column's type, then to text
/// as `keys_of` casts a table's rows, so a literal and a row name one key alike (a timestamp's `T`, a decimal's scale).
fn key_text(meta: &TableMeta, key: &[String]) -> Result<Vec<String>> {
    meta.key
        .iter()
        .zip(key)
        .map(|(c, v)| {
            let (_, ty) = meta.columns.iter().find(|(n, _)| n == c).with_context(|| format!("no key column {c}"))?;
            let typed = datafusion::arrow::compute::cast(&datafusion::arrow::array::StringArray::from(vec![v.as_str()]), &crate::query::dtype(ty)?)?;
            let text = datafusion::arrow::compute::cast(&typed, &DataType::Utf8)?;
            Ok(text.as_string::<i32>().value(0).to_string())
        })
        .collect()
}

/// The row of `key` as the read point gives it, with its new version by `set_to`: its old and new versions (none: the row is
/// gone). None: SQL works the UPDATE out (a value only SQL can evaluate).
async fn change_one(lake: &Lake, name: &str, meta: &TableMeta, key: &[String], snapshot: Option<u64>, set_to: &[(String, String)]) -> Result<Option<(Vec<RecordBatch>, Vec<RecordBatch>)>> {
    let row = match current(lake, name, meta, key, snapshot).await? {
        Current::Row(r) => r,
        Current::Gone => return Ok(Some((vec![], vec![]))),
        Current::Changed => return Err(conflict(name)),
    };
    let mut new = row.clone();
    for (c, e) in set_to {
        let Some(v) = crate::defaults::evaluate(&row, e)? else { return Ok(None) };
        let t = new.schema().field_with_name(c)?.data_type().clone();
        new = set(&new, c, crate::query::strict(&v, &t)?)?;
    }
    let keep: Vec<usize> = (0..new.num_columns()).filter(|&i| new.schema().field(i).name() != crate::sys::VERSION).collect();
    Ok(Some((vec![row], vec![new.project(&keep)?])))
}

/// In a transaction, a key lookup (`serve::Point`) answered from the transaction's own version of the row or the snapshot's,
/// without planning. None: not one, or the row changed since the snapshot (SQL reads it as of then).
pub async fn point_read(lake: &Lake, sql: &str) -> Result<Option<Vec<RecordBatch>>> {
    let Some(snapshot) = snapshot() else { return Ok(None) };
    // (a locking read takes its locks first, and is planned over the rows it locked: `lock_reads`)
    if crate::auth::limited().is_some() || crate::temp::mentioned(sql) || LOCKING.is_match(sql) {
        return Ok(None);
    }
    let Some(p) = crate::serve::point(lake, sql.trim().trim_end_matches(';')).await? else { return Ok(None) };
    let Some(key) = p.key.iter().cloned().collect::<Option<Vec<String>>>() else { return Ok(None) };
    let schema = p.schema()?;
    Ok(match current(lake, &p.table, &p.meta, &key, Some(snapshot)).await? {
        Current::Row(r) => Some(vec![crate::query::conform(&r, &schema)?]),
        Current::Gone => Some(vec![RecordBatch::new_empty(schema)]),
        Current::Changed => None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_transactions_keys_are_kept_as_its_rows_are() {
        use datafusion::arrow::array::StringArray;
        let rows = |ids: &[i64], names: &[&str]| RecordBatch::try_from_iter([("id", Arc::new(Int64Array::from(ids.to_vec())) as ArrayRef), ("name", Arc::new(StringArray::from(names.to_vec())) as ArrayRef)]).expect("a batch");
        let key = vec!["id".to_string(), "name".to_string()];
        let mut by = ByKey { key: key.clone(), ..Default::default() };
        by.added(&rows(&[1, 2], &["a", "b"])).expect("added");
        by.taken(&rows(&[3], &["c"])).expect("taken");
        let texts = key_texts(&rows(&[1, 3, 4], &["a", "c", "a"]), &key).expect("texts");
        assert_eq!(by.kept(&texts), vec![Some(true), Some(false), None], "its own row's key, one it took away, neither");
        by.dropped(&rows(&[1], &["a"])).expect("dropped");
        assert_eq!(by.kept(&texts), vec![None, Some(false), None], "a row of its own replaced or deleted frees its key");
        // (made again from the rows, as ROLLBACK TO does, it is the same)
        let c = Changes { new: vec![rows(&[2], &["b"])], old: vec![rows(&[3], &["c"])] };
        let again = ByKey::of(&key, Some(&c)).expect("made again");
        assert_eq!((again.live, again.gone), (by.live, by.gone));
    }

    #[test]
    fn begin_takes_its_modes_and_refuses_what_is_not_built() {
        assert_eq!(control("BEGIN ISOLATION LEVEL REPEATABLE READ"), Some("BEGIN"));
        assert_eq!(control("START TRANSACTION READ ONLY"), Some("BEGIN"));
        assert_eq!(control("SET TRANSACTION ISOLATION LEVEL READ COMMITTED"), None, "SET TRANSACTION is settings.rs's");
        let modes = begin_modes("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY").expect("parses");
        assert_eq!(plan(&modes).expect("plans"), (Some(Level::Repeatable), Some(true)));
        assert_eq!(plan(&begin_modes("BEGIN").expect("parses")).expect("plans"), (None, None));
        let serializable = plan(&begin_modes("BEGIN ISOLATION LEVEL SERIALIZABLE").expect("parses"));
        assert!(serializable.is_err_and(|e| crate::codes::of(&e) == "0A000"));
    }

    #[test]
    fn writes_are_named_and_reads_are_not() {
        assert_eq!(write_word("UPDATE t SET x = 1").as_deref(), Some("UPDATE"));
        assert_eq!(write_word("CREATE TABLE t (a INT)").as_deref(), Some("CREATE TABLE"));
        assert_eq!(write_word("COPY t FROM STDIN").as_deref(), Some("COPY FROM"));
        assert_eq!(write_word("COPY (SELECT 1) TO STDOUT"), None);
        assert_eq!(write_word("SELECT 1"), None);
        // (a comment in front, or a WITH: the write is still named, so READ ONLY refuses it)
        assert_eq!(write_word("/* c */ INSERT INTO t VALUES (1)").as_deref(), Some("INSERT"));
        assert_eq!(write_word("WITH x AS (SELECT 1 AS a) INSERT INTO t SELECT a FROM x").as_deref(), Some("INSERT"));
        assert_eq!(write_word("WITH x AS (SELECT 1 AS a) SELECT a FROM x"), None);
    }

    #[test]
    fn a_comment_before_a_control_word_is_skipped() {
        // (dbt sends `/* {"app": "dbt"} */ commit;`; a `--` comment ends at its newline)
        assert_eq!(control("/* {\"app\": \"dbt\"} */ commit;"), Some("COMMIT"));
        assert_eq!(control("-- c\nBEGIN"), Some("BEGIN"));
        assert_eq!(control("/* c */ START TRANSACTION READ ONLY"), Some("BEGIN"));
        assert_eq!(control("-- c\nROLLBACK"), Some("ROLLBACK"));
        assert_eq!(control("/* c */ SELECT 1"), None);
    }

    #[test]
    fn locking_reads_are_one_table_or_refused_by_name() {
        let lock = |sql: &str| -> Result<Option<(Query, LockClause)>> {
            let stmt = Parser::parse_sql(&GenericDialect {}, sql)?.pop().expect("a statement");
            Ok(lock_clause(&stmt)?.map(|(q, l)| (q.clone(), l.clone())))
        };
        assert!(lock("SELECT * FROM t WHERE a = 1").expect("parses").is_none(), "a plain read takes no lock");
        assert!(lock("SELECT * FROM t WHERE a = 1 FOR UPDATE").expect("parses").is_some());
        assert!(lock("SELECT * FROM t FOR UPDATE NOWAIT").expect("parses").is_some());
        let skip = lock("SELECT * FROM t FOR UPDATE SKIP LOCKED");
        assert!(skip.is_err_and(|e| crate::codes::of(&e) == "0A000"), "SKIP LOCKED is refused by name");
        let nested = lock("SELECT * FROM (SELECT * FROM t FOR UPDATE) s");
        assert!(nested.is_err_and(|e| crate::codes::of(&e) == "0A000"), "a lock in a subquery is refused by name");
        let joined = Parser::parse_sql(&GenericDialect {}, "SELECT * FROM a JOIN b ON a.x = b.x FOR UPDATE").expect("parses").pop().expect("one");
        let (q, _) = lock_clause(&joined).expect("a lock").expect("it locks");
        assert!(one_table(q).is_err_and(|e| crate::codes::of(&e) == "0A000"), "a join is refused, saying to lock one table's rows");
        let one = Parser::parse_sql(&GenericDialect {}, "SELECT k FROM t AS x WHERE a > 1 LIMIT 2 FOR UPDATE").expect("parses").pop().expect("one");
        let (q, _) = lock_clause(&one).expect("a lock").expect("it locks");
        assert_eq!(one_table(q).expect("one table").1.as_deref(), Some("x"), "the alias is what OF names");
    }

    #[test]
    fn an_unlocked_read_keeps_its_floor_for_a_key_locked_later() {
        // (H2: a transaction read rows unlocked at 5, then locked k at 9: k's point is 5, so a change at 7 refuses)
        let mut r = Reads { floor: Some(5), keys: HashMap::new() };
        note(&mut r, Some(vec![vec!["k".to_string()]]), 9);
        assert_eq!(r.keys[&vec!["k".to_string()]], 5);
        let mut fresh = Reads::default();
        note(&mut fresh, Some(vec![vec!["k".to_string()]]), 9);
        assert_eq!(fresh.keys[&vec!["k".to_string()]], 9, "without an unlocked read, the grant is the point");
    }

    #[test]
    fn an_on_key_is_never_a_row_ids() {
        // (an INSERT … ON CONFLICT's key is ON, then a column and its value: a row id's key is one part, and a keyed table's
        // key is its key's, never named ON with its columns)
        let key = on_key(&["id".to_string()], &["5".to_string()]);
        assert_eq!(key, vec!["ON", "id", "5"]);
        assert!(is_on(&key));
        assert!(!is_on(&["5".to_string()]), "a row id's key is one part");
    }
}
