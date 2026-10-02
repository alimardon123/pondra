//! Transactions (ADR-036 §5): `BEGIN` … `COMMIT` as one commit, with snapshot isolation.
//!
//! A transaction is a session's (a Postgres connection's, or a client's `x-pondra-session`), on
//! the node it talks to. `BEGIN` fixes a snapshot: the commit that node sees. Every statement in it
//! reads that snapshot with the transaction's own writes over it (`overlaid`, as temporary tables
//! are registered), so it reads its own writes. Its writes are kept here, not sent: an INSERT's
//! rows, an UPDATE's, DELETE's or MERGE's old and new versions (`change::rows_of`, over the snapshot
//! and the overlay), per table.
//!
//! `COMMIT` sends them to the leader in one request (`commit_here`). Under the lake's lock it
//! checks that no row the transaction changed (an append table's by `_row_id`, a keyed table's by
//! key) was changed by a commit after the snapshot, then writes everything as one flush: one
//! commit, its views following in it. A row changed meanwhile refuses the commit with 40001, as
//! Postgres's REPEATABLE READ does: first committer wins, and the client tries again.
//!
//! A statement that fails inside a transaction fails it: until `ROLLBACK` (or `COMMIT`, which then
//! rolls back), every statement is refused with 25P02, as in Postgres. DDL isn't part of one.
//! An idle transaction ends after `PONDRA_TXN_IDLE_SECS` (600).
use crate::store::*;
use crate::write::Stmt;
use anyhow::{ensure, Context, Result};
use datafusion::arrow::array::{Array, ArrayRef, AsArray, Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Int64Type};
use serde::{Deserialize, Serialize};
use serde_json::{json as j, Value};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};

/// A table's changes in a transaction: rows it adds (new rows, whose `_row_id` is negative until
/// the commit stamps them, and new versions, which keep theirs), and the snapshot's rows it
/// replaces or deletes (with `_version`). Under SQL's names.
#[derive(Default, Clone)]
struct Changes {
    new: Vec<RecordBatch>,
    old: Vec<RecordBatch>,
}

struct Txn {
    id: String,
    snapshot: u64,
    tables: BTreeMap<String, Changes>,
    failed: Option<String>,
    used: Instant,
    next: i64, // the next new row's temporary id (-1, -2, …)
}

static TXNS: LazyLock<Mutex<HashMap<String, Txn>>> = LazyLock::new(Default::default);

const NO_SESSION: &str = "a transaction is a session's: a Postgres connection's, or the Python or JavaScript client's (over HTTP, send x-pondra-session: <id>)";

/// The current session's open transaction's snapshot, if it has one: every read in it is as of then.
pub fn snapshot() -> Option<u64> {
    let s = crate::temp::current()?;
    TXNS.lock().unwrap().get(&s).map(|t| t.snapshot)
}

/// Is a transaction open in this session?
pub fn open() -> bool { snapshot().is_some() }

/// `BEGIN`, `START TRANSACTION`, `COMMIT`, `END`, `ROLLBACK`, `ABORT`: which, if `sql` is one.
pub fn control(sql: &str) -> Option<&'static str> {
    let words: Vec<String> = sql.trim().trim_end_matches(';').split_whitespace().take(3).map(|w| w.to_uppercase()).collect();
    let rest_ok = |from: usize| words[from..].iter().all(|w| ["WORK", "TRANSACTION", "ISOLATION", "LEVEL", "READ", "WRITE", "ONLY", "REPEATABLE", "SERIALIZABLE", "COMMITTED"].contains(&w.as_str()) || w.starts_with("ISOLATION"));
    match words.first().map(String::as_str) {
        Some("BEGIN") if rest_ok(1) => Some("BEGIN"),
        Some("START") if words.get(1).map(String::as_str) == Some("TRANSACTION") => Some("BEGIN"),
        Some("COMMIT" | "END") if rest_ok(1) => Some("COMMIT"),
        Some("ROLLBACK" | "ABORT") if rest_ok(1) && !words.iter().any(|w| w == "TO") => Some("ROLLBACK"),
        Some("SAVEPOINT" | "RELEASE") | Some("ROLLBACK") => Some("SAVEPOINT"), // (ROLLBACK TO …: refused, never ignored)
        _ => None,
    }
}

/// `BEGIN`, `COMMIT` or `ROLLBACK` (`control`), for the current session. Its answer: the command's
/// tag (a failed transaction's COMMIT is a ROLLBACK, as in Postgres), and a warning, if any.
pub async fn command(app: &crate::server::App, word: &str) -> Result<(&'static str, Option<String>)> {
    if word == "SAVEPOINT" {
        failed("a savepoint");
        return Err(crate::codes::coded("0A000", "savepoints aren't supported: a transaction commits or rolls back whole"));
    }
    let s = crate::temp::current().context(NO_SESSION)?;
    match word {
        "BEGIN" => {
            let mut all = TXNS.lock().unwrap();
            if all.contains_key(&s) {
                return Ok(("BEGIN", Some("there is already a transaction in progress".into())));
            }
            let first = all.is_empty();
            all.insert(s, Txn { id: uuid::Uuid::new_v4().simple().to_string(), snapshot: app.lake.visible(), tables: BTreeMap::new(), failed: None, used: Instant::now(), next: -1 });
            if first {
                crate::panics::spawn(reap());
            }
            Ok(("BEGIN", None))
        }
        "ROLLBACK" => Ok(("ROLLBACK", TXNS.lock().unwrap().remove(&s).is_none().then(|| "there is no transaction in progress".into()))),
        _ => {
            let Some(t) = TXNS.lock().unwrap().remove(&s) else { return Ok(("COMMIT", Some("there is no transaction in progress".into()))) };
            if t.failed.is_some() {
                return Ok(("ROLLBACK", None));
            }
            if t.tables.values().all(|c| c.new.is_empty() && c.old.is_empty()) {
                return Ok(("COMMIT", None)); // (it only read)
            }
            let c = Commit { id: t.id, snapshot: t.snapshot, tables: t.tables.into_iter().map(|(n, c)| Ok((n, ipc(&fresh(c.new)?)?, ipc(&c.old)?))).collect::<Result<_>>()? };
            match &app.seq {
                Some(seq) => commit_here(&app.lake, seq, &app.lock, c).await?,
                None => crate::write::post(&app.cluster.leader.addr, &crate::write::Request::Txn(c)).await?,
            };
            crate::write::seen_here(app).await; // (the next statement here reads it)
            Ok(("COMMIT", None))
        }
    }
}

/// A session ended (its connection closed): its transaction, if open, is rolled back.
pub fn end(session: &str) { TXNS.lock().unwrap().remove(session); }

/// Has the session a transaction open here?
pub fn held(session: &str) -> bool { TXNS.lock().unwrap().contains_key(session) }

/// A statement failed in this session's transaction: it is failed now (`refuse`).
pub fn failed(message: &str) {
    let Some(s) = crate::temp::current() else { return };
    if let Some(t) = TXNS.lock().unwrap().get_mut(&s) {
        t.failed.get_or_insert_with(|| message.to_string());
    }
}

/// A failed transaction refuses every statement until it ends (25P02).
pub fn refuse() -> Result<()> {
    let Some(s) = crate::temp::current() else { return Ok(()) };
    let mut all = TXNS.lock().unwrap();
    let Some(t) = all.get_mut(&s) else { return Ok(()) };
    t.used = Instant::now();
    match &t.failed {
        Some(why) => Err(crate::codes::coded("25P02", format!("current transaction is aborted, commands ignored until end of transaction block (it failed: {why})"))),
        None => Ok(()),
    }
}

/// Transactions idle for `PONDRA_TXN_IDLE_SECS` end, rolled back; once there are none, this stops.
async fn reap() {
    let idle = Duration::from_secs(std::env::var("PONDRA_TXN_IDLE_SECS").ok().and_then(|v| v.parse().ok()).unwrap_or(600));
    loop {
        tokio::time::sleep(idle.min(Duration::from_secs(30))).await;
        let mut all = TXNS.lock().unwrap();
        all.retain(|_, t| t.used.elapsed() < idle);
        if all.is_empty() {
            return;
        }
    }
}

// ---------------------------------------------------------------- statements in a transaction

/// A write in the session's open transaction, kept here (None: no transaction open).
pub async fn statement(app: &crate::server::App, stmt: &Stmt, files: bool) -> Result<Option<Value>> {
    let Some(snapshot) = snapshot() else { return Ok(None) };
    refuse()?;
    let lake = &app.lake;
    let (table, insert) = match stmt {
        Stmt::Insert(t, q) => (t.clone(), Some(q.clone())),
        Stmt::InsertInto(t, names, q) => (t.clone(), Some(crate::write::whole_rows(lake, t, names, q).await?)),
        Stmt::Update(..) | Stmt::Delete(..) | Stmt::Merge(_) => (stmt.table(), None),
        Stmt::Ddl(_) | Stmt::Create(_) | Stmt::Define(..) => return Err(crate::codes::coded("0A000", "this statement isn't part of a transaction here: run it before BEGIN or after COMMIT")),
        _ => return Err(crate::codes::coded("0A000", format!("{}: not in a transaction (yet): run it outside one", stmt.table()))),
    };
    let (other, name) = crate::ddl::resolve(lake, &table).await?;
    ensure!(other.is_none(), "{table} is an attached lake's: a transaction writes this lake's tables");
    let stored: TableMeta = lake.cat.get(&table_key(&name)).await?.with_context(|| format!("no table {name}"))?;
    let meta = stored.logical();
    ensure!(lake.cat.get::<crate::views::View>(&crate::views::view_key(&name)).await?.is_none() && meta.merge.is_empty(), "{name} is a view's: change the table it follows");
    ensure!(meta.ids, "{name} holds rows from before row ids (made before Pondra 0.19): copy it once and change the copy");
    if let (Stmt::Update(_, set, Some(cond)), None) = (stmt, &insert) {
        if let Some((old, new)) = point_change(lake, &name, &stored, Some(snapshot), set, cond).await? {
            let n = new.iter().map(|b| b.num_rows()).sum::<usize>();
            keep(&name, old, new)?;
            return Ok(Some(j!({"rows": n, "updated": n, "deleted": 0, "inserted": 0})));
        }
    }
    let out = match insert {
        Some(query) => {
            let ctx = crate::query::session_at(lake, &query, "", Some(snapshot)).await?;
            let ctx = if files { ctx.enable_url_table() } else { ctx };
            let rows = crate::write::rows(&ctx, &meta, &query).await?;
            crate::defaults::check(&stored, &name, &rows)?;
            let n = rows.num_rows();
            keep(&name, vec![], vec![rows])?;
            j!({"rows": n})
        }
        None => {
            let (old, new) = crate::change::rows_of(lake, &name, &meta, stmt, snapshot).await?;
            if let Some(n) = new.iter().find(|b| b.num_rows() > 0) {
                crate::defaults::check(&stored, &name, n)?;
            }
            let count = |b: &[RecordBatch]| b.iter().map(|b| b.num_rows()).sum::<usize>();
            let inserted: usize = new.iter().map(|b| b.column_by_name(crate::sys::ROW_ID).map_or(b.num_rows(), |c| c.null_count())).sum();
            let (replaced, added) = (count(&old), count(&new));
            keep(&name, old, new)?;
            j!({"rows": replaced + inserted, "updated": added - inserted, "deleted": replaced + inserted - added, "inserted": inserted})
        }
    };
    Ok(Some(out))
}

/// A statement's changes of `table` into the transaction: the rows it replaces that the
/// transaction wrote itself are taken out of its new rows (they never were the lake's); the
/// snapshot's are kept as old; its new rows get temporary ids.
fn keep(table: &str, old: Vec<RecordBatch>, new: Vec<RecordBatch>) -> Result<()> {
    let s = crate::temp::current().context(NO_SESSION)?;
    let mut all = TXNS.lock().unwrap();
    let t = all.get_mut(&s).context("no transaction in progress")?;
    let c = t.tables.entry(table.to_string()).or_default();
    let mut mine = HashSet::new(); // (rows of its own it replaces: no `_version`)
    for b in old.iter().filter(|b| b.num_rows() > 0) {
        let (ids, versions) = (b.column_by_name(crate::sys::ROW_ID).context("old rows' ids")?.as_primitive::<Int64Type>(), b.column_by_name(crate::sys::VERSION).context("old rows' versions")?);
        let theirs = datafusion::arrow::array::BooleanArray::from((0..b.num_rows()).map(|i| Some(versions.is_valid(i))).collect::<Vec<_>>());
        mine.extend((0..b.num_rows()).filter(|&i| !versions.is_valid(i)).map(|i| ids.value(i)));
        c.old.push(datafusion::arrow::compute::filter_record_batch(b, &theirs)?);
    }
    if !mine.is_empty() {
        for b in c.new.iter_mut() {
            let ids = b.column_by_name(crate::sys::ROW_ID).context("new rows' ids")?.as_primitive::<Int64Type>().clone();
            let keep = datafusion::arrow::array::BooleanArray::from(ids.iter().map(|id| Some(!id.is_some_and(|i| mine.contains(&i)))).collect::<Vec<_>>());
            *b = datafusion::arrow::compute::filter_record_batch(b, &keep)?;
        }
    }
    for b in new.into_iter().filter(|b| b.num_rows() > 0) {
        let given = b.column_by_name(crate::sys::ROW_ID).map(|c| datafusion::arrow::compute::cast(c, &DataType::Int64)).transpose()?;
        let ids: Int64Array = (0..b.num_rows()).map(|i| Some(match &given {
            Some(g) if g.is_valid(i) => g.as_primitive::<Int64Type>().value(i),
            _ => (t.next, t.next -= 1).0,
        })).collect();
        c.new.push(set(&b, crate::sys::ROW_ID, Arc::new(ids))?);
    }
    t.used = Instant::now();
    Ok(())
}

fn set(b: &RecordBatch, name: &str, column: ArrayRef) -> Result<RecordBatch> {
    use datafusion::arrow::datatypes::{Field, Schema};
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
    new.into_iter().filter(|b| b.num_rows() > 0).map(|b| {
        let ids = b.column_by_name(crate::sys::ROW_ID).context("ids")?.as_primitive::<Int64Type>().clone();
        set(&b, crate::sys::ROW_ID, Arc::new(ids.iter().map(|i| i.filter(|i| *i >= 0)).collect::<Int64Array>()))
    }).collect()
}

/// Has the session's transaction written to `table`?
pub fn touched(table: &str) -> bool {
    let Some(s) = crate::temp::current() else { return false };
    TXNS.lock().unwrap().get(&s).is_some_and(|t| t.tables.get(table).is_some_and(|c| !c.new.is_empty() || !c.old.is_empty()))
}

/// `base` (`table` as of the snapshot, with its system columns) with the transaction's writes over
/// it: without the rows it replaced (by `_row_id`; a keyed table's, by key), with the rows it added.
/// `sys`: the query names a system column (else they are left out, as `SELECT *` leaves them).
pub fn overlaid(ctx: &datafusion::prelude::SessionContext, table: &str, key: &[String], base: Arc<dyn datafusion::catalog::TableProvider>, sys: bool) -> Result<Arc<dyn datafusion::catalog::TableProvider>> {
    use datafusion::prelude::*;
    let Some(s) = crate::temp::current() else { return Ok(base) };
    let c = match TXNS.lock().unwrap().get(&s).and_then(|t| t.tables.get(table)) {
        Some(c) => c.clone(),
        None => return Ok(base),
    };
    let schema = base.schema();
    let mut df = ctx.read_table(base)?;
    let ids: Vec<Expr> = c.old.iter().chain(&c.new).filter_map(|b| b.column_by_name(crate::sys::ROW_ID)).flat_map(|c| c.as_primitive::<Int64Type>().iter().flatten().filter(|i| *i >= 0).collect::<Vec<_>>()).map(lit).collect();
    if !ids.is_empty() {
        df = df.filter(not(col(crate::sys::ROW_ID).in_list(ids, false)))?;
    }
    if !key.is_empty() {
        // (a keyed table's row is its key's: an INSERT of a key there replaces it)
        let keys: Vec<Vec<String>> = c.new.iter().chain(&c.old).flat_map(|b| keys_of(b, key).unwrap_or_default()).collect::<HashSet<_>>().into_iter().collect();
        if !keys.is_empty() {
            let text = |k: &str| cast(col(k), DataType::Utf8);
            let one = |k: &Vec<String>| key.iter().zip(k).map(|(c, v)| text(c).eq(lit(v.clone()))).reduce(Expr::and).unwrap_or(lit(true));
            let any = keys.iter().map(one).reduce(Expr::or).unwrap_or(lit(false));
            df = df.filter(not(any))?;
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

/// Each row's key, as text.
fn keys_of(b: &RecordBatch, key: &[String]) -> Result<Vec<Vec<String>>> {
    let cols = key.iter().map(|k| Ok(datafusion::arrow::compute::cast(b.column_by_name(k).with_context(|| format!("no key column {k}"))?, &DataType::Utf8)?)).collect::<Result<Vec<ArrayRef>>>()?;
    Ok((0..b.num_rows()).map(|i| cols.iter().map(|c| c.as_string::<i32>().value(i).to_string()).collect()).collect())
}

// ---------------------------------------------------------------- the commit (the leader)

/// A transaction's writes, for the leader: per table, its new rows and the snapshot's rows it
/// replaced (Arrow IPC, base64).
#[derive(Serialize, Deserialize)]
pub struct Commit {
    pub id: String,
    pub snapshot: u64,
    pub tables: Vec<(String, String, String)>,
}

fn ipc(b: &[RecordBatch]) -> Result<String> {
    use base64::Engine;
    let b: Vec<RecordBatch> = b.iter().filter(|b| b.num_rows() > 0).cloned().collect();
    Ok(if b.is_empty() { String::new() } else { base64::engine::general_purpose::STANDARD.encode(crate::log::encode_ipc(&b)?) })
}

fn unipc(s: &str) -> Result<Vec<RecordBatch>> {
    use base64::Engine;
    Ok(if s.is_empty() { vec![] } else { crate::query::read_ipc(&base64::engine::general_purpose::STANDARD.decode(s)?)? })
}

/// Leader: check the transaction against what committed since its snapshot, and write it as one
/// flush. A retried one (its id) changes nothing twice.
pub async fn commit_here(lake: &Lake, seq: &crate::log::Sequencer, lock: &tokio::sync::Mutex<()>, c: Commit) -> Result<Value> {
    let _guard = lock.lock().await;
    let mut pending = vec![];
    for (name, new, old) in &c.tables {
        let (new, old) = (unipc(new)?, unipc(old)?);
        let stored: TableMeta = lake.cat.get(&table_key(name)).await?.with_context(|| format!("no table {name} (dropped while the transaction ran)"))?;
        let meta = stored.logical();
        let append = meta.key.is_empty();
        if old.iter().any(|b| b.num_rows() > 0) {
            crate::views::can_follow(lake, name, append).await?;
            if let Some(v) = crate::views::filling(lake, name).await? {
                return Err(crate::codes::coded("40001", format!("could not serialize access: materialized view {v} is still being filled from {name}'s rows")));
            }
        }
        conflicts(lake, name, &meta, c.snapshot, &old, &new).await?;
        let (p, _) = crate::change::appends(lake, seq, name, &stored, old, new, &format!("txn:{}:{name}", c.id)).await?;
        pending.extend(p);
    }
    if pending.is_empty() {
        return Ok(j!({"committed": true}));
    }
    let outcome = crate::change::submit(lake, seq, &pending).await?;
    Ok(j!({"committed": true, "duplicate": matches!(outcome, crate::log::Outcome::Acks(a) if a.iter().any(|a| a.duplicate))}))
}

/// A row the transaction changed that a commit after its snapshot changed too: 40001.
async fn conflicts(lake: &Lake, table: &str, meta: &TableMeta, snapshot: u64, old: &[RecordBatch], new: &[RecordBatch]) -> Result<()> {
    if lake.visible() <= snapshot {
        return Ok(());
    }
    let refused = || crate::codes::coded("40001", format!("could not serialize access due to concurrent update of {table}: try the transaction again"));
    if meta.key.is_empty() {
        let mine: HashSet<i64> = old.iter().filter_map(|b| b.column_by_name(crate::sys::ROW_ID)).flat_map(|c| c.as_primitive::<Int64Type>().iter().flatten().collect::<Vec<_>>()).collect();
        if mine.is_empty() {
            return Ok(()); // (rows added only: nothing of anyone else's to clash with)
        }
        let mut theirs = crate::query::tail_of(lake, &crate::sys::deleted(table), snapshot, None, false, true, false).await?;
        theirs.extend(crate::change::taken_out(lake, table, snapshot, None).await?);
        for b in &theirs {
            if let Some(ids) = b.column_by_name(crate::sys::ROW_ID) {
                if ids.as_primitive::<Int64Type>().iter().flatten().any(|i| mine.contains(&i)) {
                    return Err(refused());
                }
            }
        }
        return Ok(());
    }
    let mine: HashSet<Vec<String>> = old.iter().chain(new).map(|b| keys_of(&meta.to_logical(b).unwrap_or_else(|_| b.clone()), &meta.key)).collect::<Result<Vec<_>>>()?.into_iter().flatten().collect();
    for b in crate::query::tail(lake, table, snapshot, None, false).await? {
        if keys_of(&b, &meta.key)?.iter().any(|k| mine.contains(k)) {
            return Err(refused());
        }
    }
    Ok(())
}

// ---------------------------------------------------------------- one key, without planning

/// One key's row now (ADR-036 §6): the transaction's own version, the lake's (as of the
/// snapshot), none, or the lake's changed since the snapshot. A row has the table's columns
/// (SQL's names), then `_row_id`, `_created_at` and `_version` (null: the transaction's own).
enum Current {
    Row(RecordBatch),
    Gone,
    Changed,
}

async fn current(lake: &Lake, table: &str, meta: &TableMeta, key: &[String], snapshot: Option<u64>) -> Result<Current> {
    use datafusion::arrow::datatypes::{Field, Schema, TimeUnit};
    let mut fields: Vec<Arc<Field>> = crate::query::schema(&meta.columns)?.fields().to_vec();
    fields.push(Arc::new(Field::new(crate::sys::ROW_ID, DataType::Int64, true)));
    fields.push(Arc::new(Field::new(crate::sys::CREATED, DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), true)));
    fields.push(Arc::new(Field::new(crate::sys::VERSION, DataType::Int64, true)));
    let target = Arc::new(Schema::new(fields));
    if snapshot.is_some() {
        let own = crate::temp::current().and_then(|s| TXNS.lock().unwrap().get(&s).and_then(|t| t.tables.get(table).cloned()));
        if let Some(c) = own {
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

/// `UPDATE t SET … WHERE <key> = <value> [AND …]` of a keyed table, worked out from its one row
/// without planning: its old and new versions, as `change::rows_of` gives them. In a transaction
/// (`snapshot`) the row is its own version or the snapshot's; one changed since refuses it at once
/// (40001: its COMMIT would be). None: not such an UPDATE (SQL works it out).
pub async fn point_change(lake: &Lake, name: &str, stored: &TableMeta, snapshot: Option<u64>, set_to: &[(String, String)], cond: &str) -> Result<Option<(Vec<RecordBatch>, Vec<RecordBatch>)>> {
    let meta = stored.logical();
    if meta.key.is_empty() || !meta.merge.is_empty() || meta.order.is_some() || stored.mapped() || name.contains('.') || name.contains('"')
        || set_to.iter().any(|(c, _)| meta.key.contains(c) || c == "_deleted" || crate::sys::NAMES.contains(&c.as_str()) || !meta.columns.iter().any(|(n, _)| n == c)) {
        return Ok(None);
    }
    let Some(p) = crate::serve::point(lake, &format!("SELECT * FROM \"{name}\" WHERE {cond}")).await? else { return Ok(None) };
    let Some(key) = p.key.iter().cloned().collect::<Option<Vec<String>>>() else { return Ok(None) };
    let row = match current(lake, name, &meta, &key, snapshot).await? {
        Current::Row(r) => r,
        Current::Gone => return Ok(Some((vec![], vec![]))),
        Current::Changed => return Err(crate::codes::coded("40001", format!("could not serialize access due to concurrent update of {name}: try the transaction again"))),
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

/// In a transaction, a key lookup (`serve::Point`) answered from the transaction's own version of
/// the row or the snapshot's, without planning. None: not one, or the row changed since the
/// snapshot (SQL reads it as of then).
pub async fn point_read(lake: &Lake, sql: &str) -> Result<Option<Vec<RecordBatch>>> {
    let Some(snapshot) = snapshot() else { return Ok(None) };
    if crate::auth::limited().is_some() || crate::temp::mentioned(sql) {
        return Ok(None);
    }
    let Some(p) = crate::serve::point(lake, sql.trim().trim_end_matches(';')).await? else { return Ok(None) };
    let Some(key) = p.key.iter().cloned().collect::<Option<Vec<String>>>() else { return Ok(None) };
    let schema = p.schema()?;
    Ok(match current(lake, &p.table, &p.meta, &key, Some(snapshot)).await? {
        Current::Row(r) => Some(vec![crate::query::conform(&r.project(&p.names.iter().map(|n| r.schema().index_of(n)).collect::<Result<Vec<_>, _>>()?)?, &schema)?]),
        Current::Gone => Some(vec![RecordBatch::new_empty(schema)]),
        Current::Changed => None,
    })
}

