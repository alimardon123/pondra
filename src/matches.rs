//! `EMIT FINAL` over a `MATCH_RECOGNIZE` (ADR-073 step 8): a view that writes each match once, when nothing that comes later
//! can change it. Its source is a lake table, read as its rows come. A run reads the source's rows in the new span of the
//! watermark, with the rows the last runs left open, and writes the matches that are decided. What is still open stays
//! held: `{view}$held` keeps those rows, and `{view}$resume` keeps each key's counts and the time a live thread dies. Late
//! rows are counted in `pondra$expectations` and never matched. The leader runs it (`views::emit_all`).
use crate::log::{Append, Outcome, Sequencer, Src};
use crate::query::{over_ctx, session, session_at, table_ref};
use crate::store::*;
use crate::text::{ROW_ID, VERSION};
use crate::views::{Options, Refresh, View, EXPECTED};
use anyhow::{bail, ensure, Context, Result};
use datafusion::arrow::array::{Array, ArrayRef, AsArray, BooleanArray, Int64Array, RecordBatch, StructArray};
use datafusion::arrow::compute::{cast, concat_batches, filter_record_batch};
use datafusion::arrow::datatypes::{DataType, Field, Int64Type, Schema, SchemaRef, TimeUnit, TimestampMicrosecondType};
use datafusion::common::ScalarValue;
use datafusion::datasource::MemTable;
use datafusion::prelude::{ident, lit};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};

/// The state of a view made from a MATCH_RECOGNIZE with EMIT FINAL: the id of this view (one made again under the name is
/// another), the time column the watermark follows and its lateness and idle time, P (the most rows a PREV reaches back),
/// the PARTITION BY columns, whether the pattern has WITHIN, and the query in open mode (`inner`) and over the matches
/// (`outer`, None when there is nothing to do over them).
#[derive(Serialize, Deserialize, Clone)]
pub struct Matches {
    pub id: String,
    pub time: String,
    pub lateness_secs: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idle_secs: Option<u64>,
    pub look: u64,
    pub part: Vec<String>,
    pub within: bool,
    pub inner: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outer: Option<String>,
}

fn held_of(name: &str) -> String { format!("{name}$held") }

fn resume_of(name: &str) -> String { format!("{name}$resume") }

/// The producers a matches view's rows and progress are written under (what goes when it is dropped or detached): its
/// matches, its held rows, its resume rows and its late rows. (The state tables are keyed: their deletes are rows of theirs.)
pub fn producers(name: &str) -> Vec<String> {
    ["", "$held", "$resume", "$late"].iter().map(|p| format!("match:{name}{p}")).collect()
}

/// The state tables a matches view keeps besides its own: its held rows and its resume rows.
pub fn tables(name: &str) -> Vec<String> { vec![held_of(name), resume_of(name)] }

/// `CREATE MATERIALIZED VIEW v AS SELECT … MATCH_RECOGNIZE (…) EMIT FINAL`, as `once::create` hands it over: the query
/// without EMIT FINAL. Makes the view, `{v}$held` and `{v}$resume` in one commit.
pub async fn create(lake: &Lake, name: &str, sql: &str, o: Options) -> Result<()> {
    ensure!(o.emit.is_none() && o.join.is_none() && o.history.is_none() && o.sessions.is_none(), "EMIT FINAL keeps each match once: no window, session, join or history options with it");
    ensure!(o.expect.is_empty(), "EMIT FINAL keeps each match once, when it's decided: put expectations on a view of the rows before it");
    ensure!(matches!(o.refresh, None | Some(Refresh::Incremental)) && o.lag_secs.is_none(), "EMIT FINAL keeps each match once, when it's decided, from the rows as they come: not by key, run whole or with a lag");
    let written = format!("{}\nEMIT FINAL", sql.trim_end());
    if let Some(v) = lake.cat.get::<View>(&crate::views::view_key(name)).await? {
        ensure!(v.matches.is_some() && v.query() == written, "view {name} already exists, with other SQL or options");
        return Ok(()); // (asked again as it is: a notebook cell run twice)
    }
    ensure!(lake.cat.get::<TableMeta>(&table_key(name)).await?.is_none(), "table {name} already exists");
    let (held, resume) = (held_of(name), resume_of(name));
    for t in [&held, &resume] {
        ensure!(lake.cat.get::<TableMeta>(&table_key(t)).await?.is_none(), "table {t} already exists");
    }
    crate::ddl::unclaimed(lake, name).await?;

    // The query as the parser reads it, and its MATCH_RECOGNIZE in open mode; the input is a lake table of this lake.
    let ctx = session(lake, sql, "").await?;
    let cols = crate::pattern::open_columns(lake, sql).await?;
    let open = crate::pattern::open(sql, &cols, &ctx.state())?;
    let (other, source) = crate::ddl::resolve(lake, &open.table).await?;
    ensure!(other.is_none(), "a view follows a table of this lake");
    ensure!(source == open.table, "{}: a view follows the table by its name in this lake: write {source}", open.table);
    let src: TableMeta = table_meta(lake, &source).await?.logical(); // (SQL's names)
    if let Some(u) = crate::rerun::unfollow(&src) {
        bail!("{}, so {name} follows its rows as they come: write it from a table of plain rows", u.said(&source));
    }
    let time_type = column_type(&src, &open.time)?;
    ensure!(time_type.starts_with("Timestamp") || time_type.starts_with("Date"), "ORDER BY {} takes a date or a timestamp, as the event time", open.time);
    let part: Vec<(String, String)> = open.part.iter().map(|p| Ok((p.clone(), column_type(&src, p)?))).collect::<Result<_>>()?;
    ensure!(part.iter().all(|(c, _)| !["k", "m", "due", "_deleted"].contains(&c.as_str())), "a PARTITION BY column named k, m, due or _deleted can't be kept beside the view's own counts: rename it in {source}");

    // The view's columns: the plain query's, planned and checked as `rerun::create` plans them.
    let plain = crate::routines::expand(lake, sql).await?;
    let planned = crate::sys::planned(&plain)?.into_owned();
    let pctx = session(lake, &planned, "").await?;
    let plan = crate::engine::sql(&pctx, &planned).await?.logical_plan().clone();
    let (columns, sql_types) = crate::rerun::table_columns(lake, sql, &pctx, &plan).await?;

    // The three tables. `$held` is keyed by its source row's id; `$resume` by the PARTITION BY columns (none: one key).
    let mut held_cols = src.columns.clone();
    held_cols.extend([("__mr_row".to_string(), "Int64".to_string()), ("_deleted".to_string(), "Boolean".to_string())]);
    let held_meta = TableMeta { columns: held_cols, key: vec!["__mr_row".into()], tiered: lake.visible(), ids: true, ..Default::default() };
    let mut resume_cols: Vec<(String, String)> = match part.is_empty() {
        true => vec![("__mr_all".to_string(), "Boolean".to_string())],
        false => part.clone(),
    };
    resume_cols.extend([("k", "Int64"), ("m", "Int64"), ("due", "Int64"), ("_deleted", "Boolean")].map(|(c, t)| (c.to_string(), t.to_string())));
    let resume_key: Vec<String> = match part.is_empty() {
        true => vec!["__mr_all".to_string()],
        false => part.iter().map(|(c, _)| c.clone()).collect(),
    };
    let resume_meta = TableMeta { columns: resume_cols, key: resume_key, tiered: lake.visible(), ids: true, ..Default::default() };
    let view_meta = TableMeta { columns, publish: default_publish(), ids: true, tiered: lake.visible(), sql_types, ..Default::default() };
    let view = View {
        source: source.clone(),
        sql: sql.into(),
        emit: None,
        sessions: None,
        ids: false,
        join: None,
        fill: None,
        expect: vec![],
        once: None,
        written: Some(written),
        rerun: None,
        matches: Some(Matches {
            id: uuid::Uuid::new_v4().to_string(),
            time: open.time.clone(),
            lateness_secs: o.lateness_secs,
            idle_secs: o.idle_secs,
            look: open.look,
            part: open.part.clone(),
            within: open.within,
            inner: open.inner.clone(),
            outer: open.outer.clone(),
        }),
    };
    let mut puts = vec![
        (table_key(name), json(&view_meta)),
        (table_key(&held), json(&held_meta)),
        (table_key(&resume), json(&resume_meta)),
        (crate::views::view_key(name), json(&view)),
    ];
    if lake.cat.get::<TableMeta>(&table_key(EXPECTED)).await?.is_none() {
        puts.push((table_key(EXPECTED), json(&crate::views::expected_table(lake))));
    }
    lake.cat.commit(puts, &[]).await
}

/// A key: a row's values of the PARTITION BY columns, as SQL's NULL-equal comparison takes them (`[true]` with none).
type Key = Vec<ScalarValue>;

/// What each key's resume row says: its look-back rows `k`, its matches `m`, and the time a live thread dies (`due`).
type Resume = HashMap<Key, (i64, i64, Option<i64>)>;

/// A key's marker from the matcher: how many of its last rows stay held, and its new `k`, `m` and `due`.
#[derive(Clone, Copy)]
struct Marker {
    keep: i64,
    k: i64,
    next_m: i64,
    due: Option<i64>,
}

/// The keys by their due time, for a WITHIN view: each key whose live thread dies at a watermark at or after it. Kept in
/// memory per view; loaded from `$resume` when a run first needs it (after a start or a leader change).
#[derive(Default)]
struct Dues {
    loaded: bool,
    by_due: BTreeMap<i64, Vec<Key>>,
    of: HashMap<Key, i64>,
}

impl Dues {
    /// Sets `key`'s due time (None: no live thread), taking its old one out.
    fn set(&mut self, key: &Key, due: Option<i64>) {
        if let Some(old) = self.of.remove(key) {
            let empty = self.by_due.get_mut(&old).is_some_and(|ks| {
                ks.retain(|k| k != key);
                ks.is_empty()
            });
            if empty {
                self.by_due.remove(&old);
            }
        }
        if let Some(d) = due {
            self.of.insert(key.clone(), d);
            self.by_due.entry(d).or_default().push(key.clone());
        }
    }

    /// The keys whose live thread is dead at watermark `w`.
    fn due_by(&self, w: i64) -> Vec<Key> { self.by_due.range(..=w).flat_map(|(_, ks)| ks.iter().cloned()).collect() }

    /// Fills the dues from `$resume`, the first time.
    fn load(&mut self, resume: &Resume) {
        self.loaded = true;
        for (key, (_, _, due)) in resume {
            self.set(key, *due);
        }
    }
}

crate::per_node!(static DUES: Mutex<HashMap<String, Dues>> = Default::default());

/// One run of a matches view (leader only, `views::emit_all`): the watermark's new span, read at the snapshot; the keys it
/// touches, run with their held rows; the matches the matcher decides, and each key's markers; and the writes of every
/// producer in one flush, under the lake's lock. A run that finds nothing new commits nothing.
pub async fn run(lake: &Lake, _log: &crate::log::Log, seq: &Arc<Sequencer>, lock: &Arc<tokio::sync::Mutex<()>>, view: &str, v: &View, m: &Matches) -> Result<()> {
    let producer = format!("match:{view}");
    let late_producer = format!("{producer}$late");
    let w0: u64 = lake.cat.get(&producer_key(&producer)).await?.unwrap_or(0);
    let Some(w) = crate::views::watermark(lake, &v.source, &m.time, m.lateness_secs, m.idle_secs).await? else { return Ok(()) };
    if w <= w0 as i64 {
        return Ok(());
    }
    let at = lake.visible();
    let at0: u64 = lake.cat.get(&producer_key(&late_producer)).await?.unwrap_or(0);
    let (held_name, resume_name) = (held_of(view), resume_of(view));
    let src: TableMeta = table_meta(lake, &v.source).await?.logical();
    let held_meta: Arc<TableMeta> = Arc::new(table_meta(lake, &held_name).await?);
    let resume_meta: Arc<TableMeta> = Arc::new(table_meta(lake, &resume_name).await?);
    let view_meta = table_meta(lake, view).await?;
    let time_type = dtype(&column_type(&src, &m.time)?)?;
    let cols: Vec<(String, DataType)> = src.columns.iter().map(|(c, t)| Ok((c.clone(), dtype(t)?))).collect::<Result<_>>()?;
    let part: Vec<(String, DataType)> = m.part.iter().map(|p| Ok((p.clone(), dtype(&column_type(&src, p)?)?))).collect::<Result<_>>()?;
    let stamp = |us: i64| -> Result<ScalarValue> { Ok(ScalarValue::TimestampMicrosecond(Some(us), None).cast_to(&time_type)?) };
    let t = ident(m.time.as_str());

    // S: the source's rows in (W0, W], as they are at `at`, with their ids and versions.
    let names: Vec<String> = src.columns.iter().map(|(c, _)| c.clone()).collect();
    let read = format!("SELECT {}, {ROW_ID}, {VERSION} FROM {}", crate::objects::list_sql(&names), crate::write::sql_name(&v.source));
    let sctx = session_at(lake, &read, "", Some(at)).await?;
    let table = sctx.table(table_ref(&v.source)).await?;
    let mut bounds = t.clone().lt_eq(lit(stamp(w)?));
    if w0 > 0 {
        bounds = bounds.and(t.clone().gt(lit(stamp(w0 as i64)?)));
    }
    let s_batches: Vec<RecordBatch> = table.clone().filter(bounds)?.collect().await?;

    // Late rows: those that came after the last run's snapshot, with a time at or before its watermark, or none.
    let late: i64 = match w0 > 0 && at > at0 {
        true => {
            let version = ident(VERSION);
            let stale = t.clone().lt_eq(lit(stamp(w0 as i64)?)).or(t.clone().is_null());
            table.filter(version.clone().gt(lit(at0 as i64)).and(version.lt_eq(lit(at as i64))).and(stale))?.count().await? as i64
        }
        false => 0,
    };

    // The state as of `at`: each key's counts and dues, and the held rows.
    let resume = resume_map(&read_state(lake, &resume_name, at).await?, &part)?;
    let held_rows = read_state(lake, &held_name, at).await?;
    let dkey = format!("{}|{view}|{}", lake.url, m.id);
    let mut dues = DUES.lock().unwrap().remove(&dkey).unwrap_or_default();
    if m.within && !dues.loaded {
        dues.load(&resume);
    }
    let s_schema: SchemaRef = Arc::new(Schema::new(fields_of(&cols).chain([Field::new(ROW_ID, DataType::Int64, true)]).collect::<Vec<_>>()));
    let s_all = rows_as(&s_schema, &s_batches)?;
    let s_keys = keys_of(&s_all, &part)?;
    let s_ids = ints(&s_all, ROW_ID)?;
    let held_all = rows_as(&held_schema_of(&cols), &held_rows)?;
    let held_keys = keys_of(&held_all, &part)?;

    // K: the keys S touches, and those whose live thread is dead by W.
    let mut touched: HashSet<Key> = s_keys.iter().cloned().collect();
    if m.within {
        touched.extend(dues.due_by(w));
    }
    if touched.is_empty() && late == 0 {
        if m.within {
            DUES.lock().unwrap().insert(dkey, dues);
        }
        return Ok(()); // (nothing new and nothing late: no run, no commit)
    }
    let keep_held: Vec<bool> = held_keys.iter().map(|k| touched.contains(k)).collect();
    let held_k = filter_record_batch(&held_all, &BooleanArray::from(keep_held.clone()))?;
    let held_keys_k: Vec<Key> = held_keys.iter().zip(&keep_held).filter(|(_, k)| **k).map(|(k, _)| k.clone()).collect();
    let held_ids_k = ints(&held_k, "__mr_row")?;

    // The input: S (new), then the held rows of K (not new), each with its key's counts and W.
    let mut keys_in = s_keys.clone();
    keys_in.extend(held_keys_k);
    let mut ids_in = s_ids.clone();
    ids_in.extend(held_ids_k.iter().copied());
    let is_new: Vec<bool> = (0..keys_in.len()).map(|i| i < s_keys.len()).collect();
    let counts: Vec<(i64, i64)> = keys_in.iter().map(|k| resume.get(k).map_or((0, 0), |r| (r.0, r.1))).collect();
    let in_schema = input_schema(&cols, &time_type);
    let input = input_of(&in_schema, &cols, &s_all, &s_ids, &held_k, &held_ids_k, &counts, &stamp(w)?)?;
    let times_in: Vec<i64> = micros_of(&s_all, &m.time)?.into_iter().chain(micros_of(&held_k, &m.time)?).collect();

    // The matcher in open mode: the matches, and the markers that say what stays held.
    let ictx = over_ctx(lake, &v.source, in_schema.clone(), vec![input.clone()], &m.inner).await?;
    let inner = crate::engine::sql(&ictx, &m.inner).await?;
    let inner_schema: SchemaRef = Arc::new(inner.schema().as_arrow().clone());
    let out: Vec<RecordBatch> = inner.collect().await?;
    let (mut marks, mut matched) = (vec![], vec![]);
    for b in &out {
        let res = b.column_by_name("__mr_resume").context("no __mr_resume")?;
        let mark: BooleanArray = (0..b.num_rows()).map(|i| Some(res.is_valid(i))).collect();
        let rest: BooleanArray = (0..b.num_rows()).map(|i| Some(!res.is_valid(i))).collect();
        marks.push(filter_record_batch(b, &mark)?);
        matched.push(without(&filter_record_batch(b, &rest)?, "__mr_resume")?);
    }
    let markers = markers_of(&marks, &part)?;

    // The view's rows: the matches, through the query's own clause when it has one.
    let match_schema: SchemaRef = Arc::new(Schema::new(inner_schema.fields().iter().filter(|f| f.name() != "__mr_resume").map(|f| f.as_ref().clone()).collect::<Vec<_>>()));
    let all_matches = rows_as(&match_schema, &matched)?;
    let view_schema = crate::answer::schema(&view_meta.columns)?;
    let view_rows = match &m.outer {
        Some(outer) => {
            let octx = session(lake, outer, "__matches").await?;
            octx.register_table("__matches", Arc::new(MemTable::try_new(match_schema.clone(), vec![vec![all_matches]])?))?;
            let rows: Vec<RecordBatch> = crate::engine::sql(&octx, outer).await?.collect().await?;
            rows_as(&view_schema, &rows)?
        }
        None => rows_as(&view_schema, &[all_matches])?,
    };

    // The writes: the matches; the held delta (new rows that stay held, and held rows that no longer do); each key's
    // resume row; and the late count. Every producer moves in one flush.
    let keep: HashMap<Key, usize> = markers.iter().map(|(k, mk)| (k.clone(), mk.keep.max(0) as usize)).collect();
    let (new_mask, gone_mask) = delta(&keys_in, &times_in, &ids_in, &is_new, &keep);
    let new_held = held_rows_of(&filter_record_batch(&input, &BooleanArray::from(new_mask))?, &cols)?;
    let gone_held = held_rows_of(&filter_record_batch(&input, &BooleanArray::from(gone_mask))?, &cols)?;
    let resume_out = resume_batch(&markers, &part, &resume_meta)?;
    let (wv, w0v) = (w as u64, w0);
    let view_meta = Arc::new(view_meta);
    let (view_parts, _) = crate::change::appends(lake, seq, view, &view_meta, vec![], nonempty(view_rows), &producer, false, false).await?; // (new rows: their ids)
    let mut pending = flush_parts(view_parts, view, &view_meta, &producer, wv, w0v)?;
    let (held_parts, _) = crate::change::appends(lake, seq, &held_name, &held_meta, nonempty(gone_held), nonempty(new_held), &format!("{producer}$held"), false, false).await?;
    pending.extend(flush_parts(held_parts, &held_name, &held_meta, &format!("{producer}$held"), wv, w0v)?);
    let (resume_parts, _) = crate::change::appends(lake, seq, &resume_name, &resume_meta, vec![], nonempty(resume_out), &format!("{producer}$resume"), false, false).await?;
    pending.extend(flush_parts(resume_parts, &resume_name, &resume_meta, &format!("{producer}$resume"), wv, w0v)?);
    if at > at0 {
        let batch = match late {
            0 => RecordBatch::new_empty(crate::answer::schema(&crate::views::expected_table(lake).columns)?),
            n => crate::once::late_batch(view, "", n)?,
        };
        pending.push(Append { table: EXPECTED.into(), src: Src { producer: late_producer, seq: at, prev: Some(at0) }, batch, ack: tokio::sync::oneshot::channel().0 });
    }

    // The commit, under the lake's lock: this view still the one this run is for.
    let guard = lock.lock().await;
    let kept = lake.cat.get::<View>(&crate::views::view_key(view)).await?.and_then(|x| x.matches);
    if kept.is_none_or(|x| x.id != m.id) {
        return Ok(());
    }
    let outcome = crate::change::submit(lake, seq, &pending).await?;
    drop(guard);
    match outcome {
        Outcome::Acks(acks) if acks.iter().all(|a| !(a.conflict || a.duplicate || a.refused || a.gone)) => {
            if m.within {
                for (key, mk) in &markers {
                    dues.set(key, mk.due);
                }
                DUES.lock().unwrap().insert(dkey, dues); // (after a commit: the dues are as the writes say)
            }
        }
        Outcome::Acks(acks) if acks.iter().any(|a| a.conflict) => {
            eprintln!("materialized view {view}: another run committed first; the next one reads on from it");
        }
        Outcome::Acks(acks) if acks.iter().any(|a| a.refused || a.gone) => bail!("materialized view {view}: its rows were refused"),
        Outcome::Refused(why) => bail!("materialized view {view}: {why}"),
        _ => {} // (a resent flush: its parts are duplicates; the dues are read again from `$resume` next time)
    }
    Ok(())
}

/// A table's catalog entry.
async fn table_meta(lake: &Lake, name: &str) -> Result<TableMeta> {
    lake.cat.get::<TableMeta>(&table_key(name)).await?.with_context(|| format!("no table {name}"))
}

/// A source column's type, as the table has it.
fn column_type(src: &TableMeta, column: &str) -> Result<String> {
    src.columns.iter().find(|(c, _)| c == column).map(|(_, t)| t.clone()).with_context(|| format!("no column {column}"))
}

fn dtype(t: &str) -> Result<DataType> { crate::answer::dtype(t) }

/// Fields for `cols`, nullable.
fn fields_of(cols: &[(String, DataType)]) -> impl Iterator<Item = Field> + '_ { cols.iter().map(|(c, t)| Field::new(c, t.clone(), true)) }

/// A held row's schema: the source's columns, and `__mr_row`, the source row's id.
fn held_schema_of(cols: &[(String, DataType)]) -> SchemaRef { Arc::new(Schema::new(fields_of(cols).chain([Field::new("__mr_row", DataType::Int64, true)]).collect::<Vec<_>>())) }

/// The rows of a state table, as it is at `at` (a keyed table's versions merged, its delete markers left out).
async fn read_state(lake: &Lake, name: &str, at: u64) -> Result<Vec<RecordBatch>> {
    let meta: Arc<TableMeta> = Arc::new(table_meta(lake, name).await?);
    let ctx = session(lake, "", "").await?;
    let provider = crate::query::table_view_once(lake, &ctx, name, meta.clone(), Some(at)).await?;
    ctx.register_table("__state", crate::query::named(&ctx, provider, &meta, false)?)?;
    Ok(ctx.table("__state").await?.collect().await?)
}

/// `batches` as `schema`'s rows, in one batch (none: an empty one).
fn rows_as(schema: &SchemaRef, batches: &[RecordBatch]) -> Result<RecordBatch> {
    let conformed = batches.iter().filter(|b| b.num_rows() > 0).map(|b| crate::query::conform(b, schema)).collect::<Result<Vec<_>>>()?;
    match conformed.is_empty() {
        true => Ok(RecordBatch::new_empty(schema.clone())),
        false => Ok(concat_batches(schema, &conformed)?),
    }
}

/// A batch as the list a change takes: none when it has no rows.
fn nonempty(b: RecordBatch) -> Vec<RecordBatch> {
    match b.num_rows() {
        0 => vec![],
        _ => vec![b],
    }
}

/// A table's parts of one flush, each under its producer's `seq` and `prev`. A table with no change still sends an empty
/// part (its producer's seq must move with the flush, as `rerun` sends them).
fn flush_parts(parts: Vec<Append>, table: &str, meta: &TableMeta, producer: &str, seq: u64, prev: u64) -> Result<Vec<Append>> {
    let mut parts = match parts.is_empty() {
        true => vec![Append { table: table.to_string(), src: Src { producer: String::new(), seq: 0, prev: None }, batch: RecordBatch::new_empty(crate::answer::schema(&meta.columns)?), ack: tokio::sync::oneshot::channel().0 }],
        false => parts,
    };
    for a in &mut parts {
        a.src = Src { producer: producer.to_string(), seq, prev: Some(prev) };
    }
    Ok(parts)
}

/// A key of each row of `b`: its PARTITION BY columns as SQL compares them (`[true]` with no PARTITION BY).
fn keys_of(b: &RecordBatch, part: &[(String, DataType)]) -> Result<Vec<Key>> {
    let cols = part.iter().map(|(c, t)| Ok(cast(b.column_by_name(c).with_context(|| format!("no column {c}"))?, t)?)).collect::<Result<Vec<ArrayRef>>>()?;
    (0..b.num_rows()).map(|i| key_at(&cols, i)).collect()
}

fn key_at(cols: &[ArrayRef], row: usize) -> Result<Key> {
    match cols.is_empty() {
        true => Ok(vec![ScalarValue::Boolean(Some(true))]),
        false => cols.iter().map(|c| Ok(ScalarValue::try_from_array(c.as_ref(), row)?)).collect(),
    }
}

/// An Int64 column's values (the row ids and counts here are never NULL).
fn ints(b: &RecordBatch, name: &str) -> Result<Vec<i64>> {
    let c = cast(b.column_by_name(name).with_context(|| format!("no column {name}"))?, &DataType::Int64)?;
    Ok(c.as_primitive::<Int64Type>().values().to_vec())
}

/// A time column's values in µs (no NULL time is ever held or new: such rows are late).
fn micros_of(b: &RecordBatch, time: &str) -> Result<Vec<i64>> {
    let c = cast(b.column_by_name(time).with_context(|| format!("no column {time}"))?, &DataType::Timestamp(TimeUnit::Microsecond, None))?;
    Ok(c.as_primitive::<TimestampMicrosecondType>().values().to_vec())
}

/// The resume rows read, by key.
fn resume_map(rows: &[RecordBatch], part: &[(String, DataType)]) -> Result<Resume> {
    let mut out = HashMap::new();
    for b in rows {
        let keys = keys_of(b, part)?;
        let k = cast(b.column_by_name("k").context("no column k")?, &DataType::Int64)?;
        let m = cast(b.column_by_name("m").context("no column m")?, &DataType::Int64)?;
        let due = cast(b.column_by_name("due").context("no column due")?, &DataType::Int64)?;
        let (k, m, due) = (k.as_primitive::<Int64Type>(), m.as_primitive::<Int64Type>(), due.as_primitive::<Int64Type>());
        for (i, key) in keys.into_iter().enumerate() {
            out.insert(key, (k.value(i), m.value(i), due.is_valid(i).then(|| due.value(i))));
        }
    }
    Ok(out)
}

/// The markers of the matcher's rows, by key (each key's last row carries one).
fn markers_of(batches: &[RecordBatch], part: &[(String, DataType)]) -> Result<Vec<(Key, Marker)>> {
    let mut out = vec![];
    for b in batches {
        let keys = keys_of(b, part)?;
        let res = b.column_by_name("__mr_resume").context("no __mr_resume")?;
        let st = res.as_any().downcast_ref::<StructArray>().context("__mr_resume is a struct")?;
        let field = |n: &str| -> Result<Int64Array> { Ok(cast(st.column_by_name(n).with_context(|| format!("no field {n}"))?, &DataType::Int64)?.as_primitive::<Int64Type>().clone()) };
        let (keep, k, next_m, due) = (field("keep")?, field("k")?, field("next_m")?, field("due")?);
        for (i, key) in keys.into_iter().enumerate() {
            out.push((key, Marker { keep: keep.value(i), k: k.value(i), next_m: next_m.value(i), due: due.is_valid(i).then(|| due.value(i)) }));
        }
    }
    Ok(out)
}

/// The run's input's schema: the source's columns, `_row_id`, the key's `__mr_ok` (look-back rows) and `__mr_om` (matches),
/// and `__mr_w` (W, as the time column is typed).
fn input_schema(cols: &[(String, DataType)], time: &DataType) -> SchemaRef {
    let mut fields: Vec<Field> = fields_of(cols).collect();
    fields.extend([Field::new(ROW_ID, DataType::Int64, true), Field::new("__mr_ok", DataType::Int64, true), Field::new("__mr_om", DataType::Int64, true), Field::new("__mr_w", time.clone(), true)]);
    Arc::new(Schema::new(fields))
}

/// The run's input: S's rows (`s`, with their ids), then the held rows of K (`held`, ids `held_ids`), each row with its
/// key's counts (`counts`: `k` and `m`, one per row) and W.
#[allow(clippy::too_many_arguments)]
fn input_of(schema: &SchemaRef, cols: &[(String, DataType)], s: &RecordBatch, s_ids: &[i64], held: &RecordBatch, held_ids: &[i64], counts: &[(i64, i64)], w: &ScalarValue) -> Result<RecordBatch> {
    let n = s_ids.len();
    let (ok, om): (Vec<i64>, Vec<i64>) = counts.iter().copied().unzip();
    let a = input_part(schema, cols, s, s_ids, &ok[..n], &om[..n], w)?;
    let b = input_part(schema, cols, held, held_ids, &ok[n..], &om[n..], w)?;
    Ok(concat_batches(schema, &[a, b])?)
}

/// Some rows as the input's: their columns by name, with their row ids, their counts and W.
fn input_part(schema: &SchemaRef, cols: &[(String, DataType)], rows: &RecordBatch, ids: &[i64], ok: &[i64], om: &[i64], w: &ScalarValue) -> Result<RecordBatch> {
    let mut out: Vec<ArrayRef> = cols.iter().map(|(c, t)| Ok(cast(rows.column_by_name(c).with_context(|| format!("no column {c}"))?, t)?)).collect::<Result<_>>()?;
    out.push(Arc::new(Int64Array::from(ids.to_vec())));
    out.push(Arc::new(Int64Array::from(ok.to_vec())));
    out.push(Arc::new(Int64Array::from(om.to_vec())));
    out.push(w.to_array_of_size(ids.len())?);
    Ok(RecordBatch::try_new(schema.clone(), out)?)
}

/// Input rows as held rows: the source's columns, and the source row's id as `__mr_row`.
fn held_rows_of(b: &RecordBatch, cols: &[(String, DataType)]) -> Result<RecordBatch> {
    let mut arrays: Vec<ArrayRef> = cols.iter().map(|(c, _)| Ok(b.column_by_name(c).with_context(|| format!("no column {c}"))?.clone())).collect::<Result<_>>()?;
    arrays.push(Arc::new(Int64Array::from(ints(b, ROW_ID)?)));
    Ok(RecordBatch::try_new(held_schema_of(cols), arrays)?)
}

/// `$resume`'s rows for the keys with a marker: the key's columns, `k`, `m` (the marker's `next_m`) and `due`.
fn resume_batch(markers: &[(Key, Marker)], part: &[(String, DataType)], meta: &TableMeta) -> Result<RecordBatch> {
    let schema = crate::answer::schema(&meta.columns.iter().filter(|(c, _)| c != "_deleted").cloned().collect::<Vec<_>>())?;
    if markers.is_empty() {
        return Ok(RecordBatch::new_empty(schema));
    }
    let mut arrays: Vec<ArrayRef> = match part.is_empty() {
        true => vec![Arc::new(BooleanArray::from(vec![true; markers.len()]))],
        false => (0..part.len()).map(|j| Ok(ScalarValue::iter_to_array(markers.iter().map(|(k, _)| k[j].clone()))?)).collect::<Result<_>>()?,
    };
    arrays.push(Arc::new(Int64Array::from(markers.iter().map(|(_, mk)| mk.k).collect::<Vec<_>>())));
    arrays.push(Arc::new(Int64Array::from(markers.iter().map(|(_, mk)| mk.next_m).collect::<Vec<_>>())));
    arrays.push(Arc::new(Int64Array::from(markers.iter().map(|(_, mk)| mk.due).collect::<Vec<_>>())));
    Ok(RecordBatch::try_new(schema, arrays)?)
}

/// The held delta of a run, as masks over its input: the new rows that stay held, and the held rows that don't stay. For
/// each key, its last `keep` rows by (time, row id) stay; a key with no marker keeps all of them.
fn delta(keys: &[Key], times: &[i64], ids: &[i64], is_new: &[bool], keep: &HashMap<Key, usize>) -> (Vec<bool>, Vec<bool>) {
    let stay = stay_mask(keys, times, ids, keep);
    ((0..keys.len()).map(|i| is_new[i] && stay[i]).collect(), (0..keys.len()).map(|i| !is_new[i] && !stay[i]).collect())
}

fn stay_mask(keys: &[Key], times: &[i64], ids: &[i64], keep: &HashMap<Key, usize>) -> Vec<bool> {
    let mut groups: HashMap<&Key, Vec<usize>> = HashMap::new();
    for (i, k) in keys.iter().enumerate() {
        groups.entry(k).or_default().push(i);
    }
    let mut stay = vec![false; keys.len()];
    for (k, mut rows) in groups {
        rows.sort_by_key(|&i| (times[i], ids[i]));
        let n = keep.get(k).copied().unwrap_or(rows.len()).min(rows.len());
        for &i in &rows[rows.len() - n..] {
            stay[i] = true;
        }
    }
    stay
}

/// `b` without the column `name`.
fn without(b: &RecordBatch, name: &str) -> Result<RecordBatch> {
    let keep: Vec<usize> = (0..b.num_columns()).filter(|&i| b.schema().field(i).name() != name).collect();
    Ok(b.project(&keep)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    const TICKER: &str = "SELECT * FROM ticks MATCH_RECOGNIZE (PARTITION BY symbol ORDER BY ts MEASURES strt.ts AS start_ts ONE ROW PER MATCH AFTER MATCH SKIP PAST LAST ROW PATTERN (strt down+ up+) DEFINE down AS down.price < PREV(down.price), up AS up.price > PREV(up.price)) AS m";

    async fn lake_with_ticks(dir: &std::path::Path) -> Result<Arc<Lake>> {
        let lake = Lake::open(dir.to_str().unwrap(), true, false).await?;
        crate::write::create_table(&lake, "ticks", r#"{"columns": [["symbol", "Utf8"], ["ts", "Timestamp(Microsecond, None)"], ["price", "Float64"]]}"#).await?;
        Ok(lake)
    }

    fn options() -> Options { Options { once: true, ..Default::default() } }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn create_refuses_what_once_refuses_of_the_options_and_each_kind_of_source() -> Result<()> {
        let dir = std::env::temp_dir().join(format!("pondra-matches-{}", uuid::Uuid::new_v4()));
        let lake = lake_with_ticks(&dir).await?;
        let refused = |o: Options, sql: &str| {
            let (lake, sql) = (lake.clone(), sql.to_string());
            async move { create(&lake, "dips", &sql, o).await.expect_err("a refusal").to_string() }
        };
        let e = refused(Options { refresh: Some(Refresh::ByKey), ..options() }, TICKER).await;
        assert!(e.contains("not by key, run whole or with a lag"), "{e}");
        let e = refused(Options { lag_secs: Some(60), ..options() }, TICKER).await;
        assert!(e.contains("not by key, run whole or with a lag"), "{e}");
        let expect = crate::views::Expect { name: "p".into(), check: "price > 0".into(), on: crate::views::OnViolation::Keep };
        let e = refused(Options { expect: vec![expect], ..options() }, TICKER).await;
        assert!(e.contains("put expectations on a view of the rows before it"), "{e}");
        let history = crate::views::History { key: vec!["symbol".into()], sequence_by: "ts".into(), deletes: false };
        let e = refused(Options { idle_secs: Some(60), history: Some(history), ..options() }, TICKER).await;
        assert!(e.contains("no window, session, join or history options"), "{e}");

        // Each kind of source the rerun views refuse, in their words; then a name that is no table of this lake.
        let plain = TableMeta { columns: vec![("symbol".into(), "Utf8".into()), ("ts".into(), "Timestamp(Microsecond, None)".into()), ("price".into(), "Float64".into())], tiered: lake.visible(), ..Default::default() };
        let keyed = TableMeta { key: vec!["symbol".into()], ..plain.clone() };
        let grouped = TableMeta { merge: [("price".to_string(), "sum".to_string())].into(), ..plain.clone() };
        let history = TableMeta { history: Some(crate::views::History { key: vec!["symbol".into()], sequence_by: "ts".into(), deletes: false }), ..plain.clone() };
        for (name, meta, words) in [("keyed", keyed, "is keyed:"), ("grouped", grouped, "is a GROUP BY view, whose table keeps partial rows"), ("history", history, "is a history view")] {
            lake.cat.commit(vec![(table_key(name), json(&meta))], &[]).await?;
            let e = refused(options(), &TICKER.replace("ticks", name)).await;
            assert!(e.contains(words) && e.contains("write it from a table of plain rows"), "{name}: {e}");
        }
        let e = refused(options(), &TICKER.replace("ticks", "nowhere")).await;
        assert!(e.contains("reads a table directly"), "{e}");
        lake.cat.close().await?;
        let _ = std::fs::remove_dir_all(&dir);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_three_tables_have_their_columns_and_keys() -> Result<()> {
        let dir = std::env::temp_dir().join(format!("pondra-matches-{}", uuid::Uuid::new_v4()));
        let lake = lake_with_ticks(&dir).await?;
        create(&lake, "dips", TICKER, options()).await?;
        let view: View = lake.cat.get(&crate::views::view_key("dips")).await?.context("the view")?;
        let m = view.matches.as_ref().context("a matches view")?;
        assert_eq!((m.time.as_str(), m.part.as_slice(), m.within), ("ts", ["symbol".to_string()].as_slice(), false));
        assert!(!view.inline() && view.once.is_none());
        assert!(view.written.as_deref().is_some_and(|w| w.ends_with("EMIT FINAL")), "the statement as written");
        let out: TableMeta = lake.cat.get(&table_key("dips")).await?.context("the view's table")?;
        assert_eq!(out.columns.iter().map(|(c, _)| c.as_str()).collect::<Vec<_>>(), ["symbol", "start_ts"]);
        let held: TableMeta = lake.cat.get(&table_key("dips$held")).await?.context("held")?;
        assert_eq!(held.key, ["__mr_row"]);
        assert_eq!(held.columns.iter().map(|(c, _)| c.as_str()).collect::<Vec<_>>(), ["symbol", "ts", "price", "__mr_row", "_deleted"]);
        let resume: TableMeta = lake.cat.get(&table_key("dips$resume")).await?.context("resume")?;
        assert_eq!(resume.key, ["symbol"]);
        assert_eq!(resume.columns.iter().map(|(c, _)| c.as_str()).collect::<Vec<_>>(), ["symbol", "k", "m", "due", "_deleted"]);
        assert!(lake.cat.get::<TableMeta>(&table_key(EXPECTED)).await?.is_some());
        assert_eq!(producers("dips"), ["match:dips", "match:dips$held", "match:dips$resume", "match:dips$late"].map(String::from));
        assert_eq!(tables("dips"), ["dips$held", "dips$resume"].map(String::from));

        // No PARTITION BY: one key, held by a column that is always true.
        create(&lake, "all", &TICKER.replace("PARTITION BY symbol ", ""), options()).await?;
        let resume: TableMeta = lake.cat.get(&table_key("all$resume")).await?.context("resume")?;
        assert_eq!(resume.key, ["__mr_all"]);
        assert_eq!(resume.columns.iter().map(|(c, t)| format!("{c}:{t}")).collect::<Vec<_>>(), ["__mr_all:Boolean", "k:Int64", "m:Int64", "due:Int64", "_deleted:Boolean"]);
        lake.cat.close().await?;
        let _ = std::fs::remove_dir_all(&dir);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn update_and_delete_on_the_source_are_refused_while_it_follows_it() -> Result<()> {
        let dir = std::env::temp_dir().join(format!("pondra-matches-{}", uuid::Uuid::new_v4()));
        let lake = lake_with_ticks(&dir).await?;
        create(&lake, "dips", TICKER, options()).await?;
        let not = crate::views::cannot_follow(&lake, "ticks", false).await?;
        assert!(not.iter().any(|w| w.contains("emits windows, sessions or matches once")), "{not:?}");
        lake.cat.close().await?;
        let _ = std::fs::remove_dir_all(&dir);
        Ok(())
    }

    #[test]
    fn the_input_carries_each_keys_counts_and_w() -> Result<()> {
        let cols = vec![("symbol".to_string(), DataType::Utf8), ("price".to_string(), DataType::Float64)];
        let time = DataType::Timestamp(TimeUnit::Microsecond, None);
        let schema = input_schema(&cols, &time);
        let s = RecordBatch::try_from_iter(vec![
            ("symbol", Arc::new(datafusion::arrow::array::StringArray::from(vec!["B"])) as ArrayRef),
            ("price", Arc::new(datafusion::arrow::array::Float64Array::from(vec![2.0])) as ArrayRef),
        ])?;
        // Held rows of key A (its counts: k 1, m 2), and a new key B with one row of its own.
        let held = RecordBatch::try_from_iter(vec![
            ("symbol", Arc::new(datafusion::arrow::array::StringArray::from(vec!["A", "A"])) as ArrayRef),
            ("price", Arc::new(datafusion::arrow::array::Float64Array::from(vec![5.0, 4.0])) as ArrayRef),
        ])?;
        let w = ScalarValue::TimestampMicrosecond(Some(100), None).cast_to(&time)?;
        let all = input_of(&schema, &cols, &s, &[7], &held, &[3, 4], &[(0, 0), (1, 2), (1, 2)], &w)?;
        assert_eq!(all.schema().fields().iter().map(|f| f.name().as_str()).collect::<Vec<_>>(), ["symbol", "price", "_row_id", "__mr_ok", "__mr_om", "__mr_w"]);
        assert_eq!(ints(&all, ROW_ID)?, [7, 3, 4]);
        assert_eq!(ints(&all, "__mr_ok")?, [0, 1, 1]);
        assert_eq!(ints(&all, "__mr_om")?, [0, 2, 2]);
        assert_eq!(micros_of(&all, "__mr_w")?, [100, 100, 100]);
        Ok(())
    }

    #[test]
    fn a_key_keeps_its_last_rows_and_the_rest_leave() {
        let key = |s: &str| vec![ScalarValue::Utf8(Some(s.into()))];
        // A's held rows 3 and 4 (times 1 and 2), its new row 9 (time 3); B's new row 5 (time 1).
        let keys = [key("A"), key("A"), key("A"), key("B")];
        let times = [1, 2, 3, 1];
        let ids = [3, 4, 9, 5];
        let is_new = [false, false, true, true];
        let keep = HashMap::from([(key("A"), 2usize), (key("B"), 1usize)]);
        let (new, gone) = delta(&keys, &times, &ids, &is_new, &keep);
        let pick = |m: &[bool]| (0..4).filter(|&i| m[i]).map(|i| ids[i]).collect::<Vec<i64>>();
        assert_eq!((pick(&new), pick(&gone)), (vec![9, 5], vec![3]), "A keeps its last two rows, B its one");
        // A key with no marker keeps all its rows: nothing leaves.
        let (_, gone) = delta(&keys, &times, &ids, &is_new, &HashMap::new());
        assert!(gone.iter().all(|g| !g));
    }

    #[test]
    fn dues_move_with_their_keys() {
        let key = |s: &str| vec![ScalarValue::Utf8(Some(s.into()))];
        let mut dues = Dues::default();
        dues.set(&key("A"), Some(5));
        dues.set(&key("B"), Some(3));
        assert_eq!(dues.due_by(4), [key("B")]);
        dues.set(&key("B"), Some(9));
        assert!(dues.due_by(4).is_empty());
        assert_eq!(dues.due_by(9).len(), 2);
        dues.set(&key("A"), None);
        assert_eq!(dues.due_by(100), [key("B")]);
        dues.set(&key("B"), None);
        assert!(dues.by_due.is_empty() && dues.of.is_empty());
    }
}
