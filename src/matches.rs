//! `EMIT FINAL` over a `MATCH_RECOGNIZE` (ADR-073 step 8): a view that writes each match once, when nothing that comes later
//! can change it. Its source is a lake table, read as its rows come. A run reads the source's rows in the new span of the
//! watermark, with the rows the last runs left open, and writes the matches that are decided. What is still open stays
//! held: `{view}$held` keeps those rows, and `{view}$resume` keeps each key's counts and the time a live thread dies. Late
//! rows are counted in `pondra$expectations` and never matched. The leader runs it (`views::emit_all`). A run reads about
//! `PONDRA_MATCH_RUN_ROWS` new rows, and a catch-up goes on in steps.
use crate::log::{Append, Outcome, Sequencer, Src};
use crate::query::{over_ctx, session, session_at, table_ref};
use crate::store::*;
use crate::text::{ROW_ID, VERSION};
use crate::views::{Options, View, EXPECTED};
use crate::codes::coded;
use anyhow::{bail, ensure, Context, Result};
use datafusion::arrow::array::{Array, ArrayRef, AsArray, BooleanArray, Int64Array, RecordBatch, StructArray};
use datafusion::arrow::compute::{cast, concat_batches, filter_record_batch, interleave_record_batch};
use datafusion::arrow::datatypes::{DataType, Field, Int64Type, Schema, SchemaRef, TimeUnit, TimestampMicrosecondType};
use datafusion::arrow::row::{RowConverter, SortField};
use datafusion::common::ScalarValue;
use datafusion::dataframe::DataFrame;
use datafusion::datasource::MemTable;
use datafusion::logical_expr::Expr;
use datafusion::prelude::{ident, lit, SessionContext};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
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
    crate::once::final_options(&o, "each match once, when it's decided")?;
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
    let source = crate::views::followed(lake, &open.table).await?;
    takes(source == open.table, format!("{}: a view follows the table by its name in this lake: write {source}", open.table))?;
    let src: TableMeta = table_meta(lake, &source).await?.logical(); // (SQL's names)
    if lake.cat.get::<View>(&crate::views::view_key(&source)).await?.is_some_and(|u| u.rerun.is_some()) {
        return Err(crate::views::kept_by_key(&source, name, "pattern views with EMIT FINAL"));
    }
    if let Some(u) = crate::rerun::unfollow(&src) {
        return Err(coded("0A000", format!("{}, so {name} follows its rows as they come: write it from a table of plain rows", u.said(&source))));
    }
    let time_type = column_type(&src, &open.time)?;
    takes(time_type.starts_with("Timestamp") || time_type.starts_with("Date"), format!("ORDER BY {} takes a date or a timestamp, as the event time", open.time))?;
    let part: Vec<(String, String)> = open.part.iter().map(|p| Ok((p.clone(), column_type(&src, p)?))).collect::<Result<_>>()?;
    takes(part.iter().all(|(c, _)| !["k", "m", "due", "_deleted"].contains(&c.as_str())), format!("a PARTITION BY column named k, m, due or _deleted can't be kept beside the view's own counts: rename it in {source}"))?;

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

/// The keys of a run, each once: a row's key is its index in `all`. Rows are looked up by their PARTITION BY columns' row
/// encoding; `by_key` finds the id of a key given by value (due keys, markers).
struct Keys { part: Vec<(String, DataType)>, conv: Option<RowConverter>, by_row: HashMap<Box<[u8]>, u32>, by_key: HashMap<Key, u32>, all: Vec<Key> }

impl Keys {
    /// The keys of a run over the PARTITION BY columns `part` (none: the one key of the view, id 0).
    fn new(part: &[(String, DataType)]) -> Result<Keys> {
        let conv = match part.is_empty() {
            true => None,
            false => Some(RowConverter::new(part.iter().map(|(_, t)| SortField::new(t.clone())).collect())?),
        };
        let mut keys = Keys { part: part.to_vec(), conv, by_row: HashMap::new(), by_key: HashMap::new(), all: vec![] };
        if part.is_empty() {
            keys.intern(vec![ScalarValue::Boolean(Some(true))])?;
        }
        Ok(keys)
    }

    /// The id of each row's key, a key the run has not seen getting the next id.
    fn ids(&mut self, b: &RecordBatch) -> Result<Vec<u32>> {
        let Some(conv) = &self.conv else { return Ok(vec![0; b.num_rows()]) };
        let cols = part_cols(b, &self.part)?;
        let rows = conv.convert_columns(&cols)?;
        let mut out = Vec::with_capacity(b.num_rows());
        for i in 0..b.num_rows() {
            let row = rows.row(i);
            let id = match self.by_row.get(row.data()) {
                Some(id) => *id,
                None => {
                    let id = self.intern(key_at(&cols, i)?)?;
                    self.by_row.insert(row.data().into(), id);
                    id
                }
            };
            out.push(id);
        }
        Ok(out)
    }

    /// The id of `key`, a new one when the run has not seen it.
    fn intern(&mut self, key: Key) -> Result<u32> {
        if let Some(id) = self.by_key.get(&key) {
            return Ok(*id);
        }
        let id = u32::try_from(self.all.len())?;
        self.by_key.insert(key.clone(), id);
        self.all.push(key);
        Ok(id)
    }

    /// The id of `key`, if the run has it.
    fn find(&self, key: &Key) -> Option<u32> { self.by_key.get(key).copied() }

    /// How many keys the run has.
    fn len(&self) -> usize { self.all.len() }
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

crate::per_node!(
    /// Each view's cut points still ahead (`cut_points`), by `{lake}|{view}|{id}`: a catch-up works them out once and its
    /// steps take them in turn.
    static CUTS: Mutex<HashMap<String, Vec<(i64, bool)>>> = Mutex::new(HashMap::new()));

/// A catch-up's steps run back to back for this long, then go on next round, so the leader's other views emit too.
const STEPS_FOR: std::time::Duration = std::time::Duration::from_secs(5);

/// About the most new rows one run reads (`PONDRA_MATCH_RUN_ROWS`, 1,000,000): past it, the run decides up to an
/// earlier time and the next one goes on from there, so a catch-up's memory stays bounded at any backlog.
fn most_rows() -> u64 { crate::env::or("PONDRA_MATCH_RUN_ROWS", 1_000_000u64).max(1) }

/// The most rows one key's open search may hold (`PONDRA_MATCH_HELD_ROWS`, 1,000,000): a search that is never decided
/// (no WITHIN, and a row that can always go on) would hold every row of its key and fill the leader's memory.
fn most_held() -> u64 { crate::env::or("PONDRA_MATCH_HELD_ROWS", 1_000_000u64).max(1) }

/// One run of a matches view (leader only, `views::emit_all`): steps of about `most_rows()` new rows each, back to back
/// for `STEPS_FOR` while a step has more to do.
pub async fn run(lake: &Lake, _log: &crate::log::Log, seq: &Arc<Sequencer>, lock: &Arc<tokio::sync::Mutex<()>>, view: &str, v: &View, m: &Matches) -> Result<()> {
    let started = datafusion::common::instant::Instant::now(); // (a page's clock too: std's Instant has none there)
    while step(lake, _log, seq, lock, view, v, m).await? && started.elapsed() < STEPS_FOR {}
    Ok(())
}

/// One step of a run: the watermark's new span, cut to about `most_rows()` rows (`next_cut`), read at the snapshot; the keys it
/// touches, run with their held rows; the matches the matcher decides, and each key's markers; and the writes of every
/// producer in one flush, under the lake's lock. A step that finds nothing new commits nothing. Returns whether a next step
/// should follow at once: the time this one decided up to is short of the watermark.
async fn step(lake: &Lake, _log: &crate::log::Log, seq: &Arc<Sequencer>, lock: &Arc<tokio::sync::Mutex<()>>, view: &str, v: &View, m: &Matches) -> Result<bool> {
    let producer = format!("match:{view}");
    let late_producer = format!("{producer}$late");
    let w0: u64 = lake.cat.get(&producer_key(&producer)).await?.unwrap_or(0);
    let Some(w) = crate::views::watermark(lake, &v.source, &m.time, m.lateness_secs, m.idle_secs).await? else { return Ok(false) };
    if w <= w0 as i64 {
        return Ok(false);
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
    let t = ident(m.time.as_str());

    // The dues: each key's live thread's due time, from the whole of `$resume`, once (a leader's first run).
    let dkey = format!("{}|{view}|{}", lake.url, m.id);
    let mut dues = DUES.lock().unwrap().remove(&dkey).unwrap_or_default();
    if m.within && !dues.loaded {
        dues.load(&resume_map(&read_state(lake, &resume_name, at, None).await?, &part)?);
    }

    // S: the source's rows in (W0, W], as they are at `at`, with their ids and versions. The run decides up to the cut.
    let names: Vec<String> = src.columns.iter().map(|(c, _)| c.clone()).collect();
    let read = format!("SELECT {}, {ROW_ID}, {VERSION} FROM {}", crate::objects::list_sql(&names), crate::write::sql_name(&v.source));
    let sctx = session_at(lake, &read, "", Some(at)).await?;
    let table = sctx.table(table_ref(&v.source)).await?;
    let w_mark = w;
    let mut points = CUTS.lock().unwrap().remove(&dkey).unwrap_or_default();
    let w = next_cut(&mut points, &table, &m.time, &time_type, w0, w_mark, most_rows()).await?;
    if !points.is_empty() {
        CUTS.lock().unwrap().insert(dkey.clone(), points);
    }
    let s_batches: Vec<RecordBatch> = table.clone().filter(span(&t, &time_type, w0, w)?)?.collect().await?;

    // Late rows: those that came after the last run's snapshot, with a time at or before the time the last run decided up
    // to, or none.
    let late: i64 = match w0 > 0 && at > at0 {
        true => {
            let version = ident(VERSION);
            let stale = t.clone().lt_eq(lit(stamp(&time_type, w0 as i64)?)).or(t.clone().is_null());
            table.filter(version.clone().gt(lit(at0 as i64)).and(version.lt_eq(lit(at as i64))).and(stale))?.count().await? as i64
        }
        false => 0,
    };

    let s_schema: SchemaRef = Arc::new(Schema::new(fields_of(&cols).chain([Field::new(ROW_ID, DataType::Int64, true)]).collect::<Vec<_>>()));
    let s_list: Vec<RecordBatch> = s_batches.iter().filter(|b| b.num_rows() > 0).map(|b| crate::query::conform(b, &s_schema)).collect::<Result<_>>()?;
    let n_s: usize = s_list.iter().map(|b| b.num_rows()).sum();

    // Each row's key is an id (`Keys`): S's rows (the held rows' keys come with the state read below).
    let mut keys = Keys::new(&part)?;
    let mut s_kid: Vec<u32> = vec![];
    for b in &s_list {
        s_kid.extend(keys.ids(b)?);
    }

    // K: the keys S touches, and those whose live thread is dead by W.
    let due: Vec<Key> = if m.within { dues.due_by(w) } else { vec![] };
    if n_s == 0 && due.is_empty() && late == 0 {
        let short = w < w_mark;
        if short {
            CUTS.lock().unwrap().remove(&dkey); // (a cached point left (W0, W] empty: the next step works its points out again)
        }
        if m.within {
            DUES.lock().unwrap().insert(dkey, dues);
        }
        return Ok(short); // (nothing new and nothing late: no run, no commit; a short step goes on from W0 at once)
    }
    for k in &due {
        keys.intern(k.clone())?;
    }

    // The state of the touched keys only: their counts (`$resume`) and their held rows (`$held`).
    let touched = keys_batch(&keys.all, &part)?;
    let only = (!part.is_empty()).then_some(&touched);
    let resume = resume_map(&read_state(lake, &resume_name, at, only).await?, &part)?;
    let held_all = rows_as(&held_schema_of(&cols), &read_state(lake, &held_name, at, only).await?)?;
    let held_kid = keys.ids(&held_all)?;
    let held_ids = ints(&held_all, "__mr_row")?;

    // The input, in parts: S (new), then the held rows (not new), each with its key's counts and W.
    let mut kid_in = s_kid;
    kid_in.extend(held_kid);
    let mut ids_in: Vec<i64> = vec![];
    for b in &s_list {
        ids_in.extend(ints(b, ROW_ID)?);
    }
    ids_in.extend(held_ids.iter().copied());
    let mut times_in: Vec<i64> = vec![];
    for b in &s_list {
        times_in.extend(micros_of(b, &m.time)?);
    }
    times_in.extend(micros_of(&held_all, &m.time)?);
    let counts: Vec<(i64, i64)> = keys.all.iter().map(|k| resume.get(k).map_or((0, 0), |r| (r.0, r.1))).collect();
    let in_schema = input_schema(&cols, &time_type);
    let parts: Vec<&RecordBatch> = s_list.iter().chain([&held_all]).collect();
    let input = input_of(&in_schema, &cols, &parts, &ids_in, &kid_in, &counts, &stamp(&time_type, w)?)?;

    // The matcher in open mode: the matches, and the markers that say what stays held.
    let ictx = over_ctx(lake, &v.source, in_schema.clone(), input.clone(), &m.inner).await?;
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
    let most = most_held();
    if let Some((key, mk)) = markers.iter().find(|(_, mk)| mk.keep > most as i64) {
        let who = match part.is_empty() {
            true => format!("the view holds {}", mk.keep),
            false => format!("the key {} holds {}", key.iter().map(ToString::to_string).collect::<Vec<_>>().join(", "), mk.keep),
        };
        let fix = match m.within {
            true => "narrow its WITHIN, so a search ends sooner, or raise the setting",
            false => "give its PATTERN a WITHIN, so a search ends, or raise the setting",
        };
        return Err(coded("54000", format!("materialized view {view}: {who} rows of a search still open, more than PONDRA_MATCH_HELD_ROWS ({most}): {fix}")));
    }

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
    let mut keep: Vec<Option<usize>> = vec![None; keys.len()];
    for (key, mk) in &markers {
        if let Some(id) = keys.find(key) {
            keep[id as usize] = Some(mk.keep.max(0) as usize);
        }
    }
    let (new_at, gone_at) = delta(&kid_in, &times_in, &ids_in, n_s, &keep);
    let new_held = held_rows_of(&pick(&input, &in_schema, &new_at)?, &cols)?;
    let gone_held = held_rows_of(&pick(&input, &in_schema, &gone_at)?, &cols)?;
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
            n => crate::once::late_batch(view, &m.id, n)?,
        };
        pending.push(Append { table: EXPECTED.into(), src: Src { producer: late_producer, seq: at, prev: Some(at0) }, batch, ack: tokio::sync::oneshot::channel().0 });
    }

    // The commit, under the lake's lock: this view still the one this run is for.
    let guard = lock.lock().await;
    let kept = lake.cat.get::<View>(&crate::views::view_key(view)).await?.and_then(|x| x.matches);
    if kept.is_none_or(|x| x.id != m.id) {
        return Ok(false);
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
            Ok(w < w_mark) // (a step short of the watermark: the next one reads on from the time it decided up to)
        }
        Outcome::Acks(acks) if acks.iter().any(|a| a.conflict) => {
            eprintln!("materialized view {view}: another run committed first; the next one reads on from it");
            Ok(false)
        }
        Outcome::Acks(acks) if acks.iter().any(|a| a.refused || a.gone) => bail!("materialized view {view}: its rows were refused"),
        Outcome::Refused(why) => bail!("materialized view {view}: {why}"),
        _ => Ok(false), // (a resent flush: its parts are duplicates; the dues are read again from `$resume` next time)
    }
}

/// The exact cut, used within a full slice (and for a span whose slices can't be worked out): the time a run decides up to,
/// W, or, when (W0, W] holds more than `most` rows, 1 µs past its `most`-th row's time (a finer time is in too), and at
/// least 1: a run reads about `most` new rows (all of a time) and always moves on.
async fn cut(table: &DataFrame, t: &Expr, time: &str, time_type: &DataType, w0: u64, w: i64, most: u64) -> Result<i64> {
    let top = table.clone().filter(span(t, time_type, w0, w)?)?.select(vec![t.clone()])?.sort(vec![t.clone().sort(true, false)])?.limit(most as usize - 1, Some(1))?.collect().await?;
    match top.iter().find(|b| b.num_rows() > 0) {
        Some(b) => Ok((micros_of(b, time)?[0] + 1).max(1).min(w)),
        None => Ok(w),
    }
}

/// About how many slices of time a catch-up's pass counts its rows in (`cut_points`).
const SLICES: i64 = 4096;

/// Where a catch-up's steps stop, from one pass over the span (W0, W]: its rows counted in `SLICES` equal slices of time
/// (from W0+1, or at W0 0 the span's first time, to W), walked in order, a cut placed before a slice that would take the
/// rows since the last cut past `most`. A slice holding more than `most` rows alone is marked full: a step that reaches it
/// stops by the exact `cut` within it. Empty when the span holds at most `most` rows (the step takes all of it). Rows
/// that come later into a span already cut only make its step larger. Known limit: times spread very unevenly (one stray
/// old time at W0 0 makes every slice days wide) fill few slices, so most rows land in full slices, and their steps cost
/// what `cut` costs, plus the pass.
async fn cut_points(table: &DataFrame, time: &str, time_type: &DataType, w0: u64, w: i64, most: u64) -> Result<Vec<(i64, bool)>> {
    let t = ident(time);
    // The least whole µs `c` with `t <= stamp(c)`, as `span` compares: a ceiling for a ns column, a cast otherwise.
    let us = match time_type {
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            // (a whole division drops toward zero: one more µs for what is left above it, below 1970 too)
            let ns = datafusion::logical_expr::cast(t.clone(), DataType::Int64);
            ns.clone() / lit(1000_i64) + datafusion::logical_expr::cast((ns % lit(1000_i64)).gt(lit(0_i64)), DataType::Int64)
        }
        _ => datafusion::logical_expr::cast(datafusion::logical_expr::cast(t.clone(), DataType::Timestamp(TimeUnit::Microsecond, None)), DataType::Int64),
    };
    let lo = match w0 > 0 {
        true => w0 as i64 + 1,
        false => {
            let low = table.clone().filter(span(&t, time_type, w0, w)?)?.aggregate(vec![], vec![datafusion::functions_aggregate::expr_fn::min(us.clone()).alias("lo")])?.collect().await?;
            let Some(b) = low.iter().find(|b| b.num_rows() > 0) else { return Ok(vec![]) };
            let least = b.column(0).as_primitive::<Int64Type>();
            if least.is_null(0) {
                return Ok(vec![]);
            }
            least.value(0)
        }
    };
    let Some(range) = w.checked_sub(lo) else { return Ok(vec![(w, true)]) };
    let width = range / SLICES + 1;

    let groups = table.clone().filter(span(&t, time_type, w0, w)?)?.aggregate(
        vec![((us.clone() - lit(lo)) / lit(width)).alias("s")],
        vec![datafusion::functions_aggregate::expr_fn::count(lit(1_i64)).alias("n")],
    )?.collect().await?;
    let mut slices: Vec<(i64, u64)> = vec![];
    for b in &groups {
        slices.extend(ints(b, "s")?.into_iter().zip(ints(b, "n")?.into_iter().map(|n| n as u64)));
    }
    slices.sort_unstable();
    if slices.iter().map(|(_, n)| n).sum::<u64>() <= most {
        return Ok(vec![]);
    }

    let mut points: Vec<(i64, bool)> = vec![];
    let mut acc: u64 = 0;
    for (s, n) in slices {
        let start = lo.saturating_add(s.saturating_mul(width));
        let end = start.saturating_add(width - 1).min(w);
        if n > most {
            if acc > 0 {
                points.push((start - 1, false));
            }
            points.push((end, true));
            acc = 0;
        } else if acc + n > most {
            points.push((start - 1, false));
            acc = n;
        } else {
            acc += n;
        }
    }

    // At least 1 (W0 0 is "the start", as `cut`'s), and one point per time: the first kept, full if any of its equals was.
    let mut out: Vec<(i64, bool)> = vec![];
    for (c, full) in points {
        let c = c.max(1);
        match out.last_mut() {
            Some(last) if last.0 == c => last.1 |= full,
            _ => out.push((c, full)),
        }
    }
    Ok(out)
}

/// The time a step decides up to: the next of `points` (worked out by `cut_points` when none is left past W0), at most W;
/// within a full slice, the exact `cut`; with no point, W. `points` keeps its head until W0 reaches it.
async fn next_cut(points: &mut Vec<(i64, bool)>, table: &DataFrame, time: &str, time_type: &DataType, w0: u64, w: i64, most: u64) -> Result<i64> {
    points.retain(|(c, _)| *c > w0 as i64);
    if points.is_empty() {
        *points = cut_points(table, time, time_type, w0, w, most).await?;
    }
    match points.first() {
        None => Ok(w),
        Some(&(c, full)) => {
            let c = c.min(w); // (W can move back: an idle watermark drops when a new row comes)
            match full {
                true => cut(table, &ident(time), time, time_type, w0, c, most).await,
                false => Ok(c),
            }
        }
    }
}

/// The rows of the span (W0, W] of the time column `t`: at or before W, and after W0 once W0 is past the start.
fn span(t: &Expr, time: &DataType, w0: u64, w: i64) -> Result<Expr> {
    let upto = t.clone().lt_eq(lit(stamp(time, w)?));
    match w0 > 0 {
        true => Ok(upto.and(t.clone().gt(lit(stamp(time, w0 as i64)?)))),
        false => Ok(upto),
    }
}

/// A time in µs, as the time column's type.
fn stamp(time: &DataType, us: i64) -> Result<ScalarValue> { Ok(ScalarValue::TimestampMicrosecond(Some(us), None).cast_to(time)?) }

/// What the view takes: when `ok` doesn't hold, the statement is refused 0A000 with `why`, which says what to write.
fn takes(ok: bool, why: impl Into<String>) -> Result<()> {
    match ok {
        true => Ok(()),
        false => Err(coded("0A000", why)),
    }
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

/// The rows of a state table, as it is at `at` (a keyed table's versions merged, its delete markers left out): all of
/// them, or with `keys`, only the rows of those keys.
async fn read_state(lake: &Lake, name: &str, at: u64, keys: Option<&RecordBatch>) -> Result<Vec<RecordBatch>> {
    let meta: Arc<TableMeta> = Arc::new(table_meta(lake, name).await?);
    let ctx = session(lake, "", "").await?;
    let provider = crate::query::table_view_once(lake, &ctx, name, meta.clone(), Some(at)).await?;
    ctx.register_table("__state", crate::query::named(&ctx, provider, &meta, false)?)?;
    match keys {
        None => Ok(ctx.table("__state").await?.collect().await?),
        Some(keys) => of_keys(&ctx, keys).await,
    }
}

/// The rows of `__state` (registered in `ctx`) whose key is one of `keys`'s rows: a NULL key finds its NULL rows.
async fn of_keys(ctx: &SessionContext, keys: &RecordBatch) -> Result<Vec<RecordBatch>> {
    let on: Vec<String> = keys.schema().fields().iter().map(|f| { let c = crate::ext_names::quoted(f.name()); format!("k.{c} IS NOT DISTINCT FROM s.{c}") }).collect();
    ctx.register_table("__keys", Arc::new(MemTable::try_new(keys.schema(), vec![vec![keys.clone()]])?))?;
    let text = format!("SELECT s.* FROM __keys k RIGHT SEMI JOIN __state s ON {}", on.join(" AND "));
    Ok(crate::engine::sql(ctx, &text).await?.collect().await?)
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
    let cols = part_cols(b, part)?;
    (0..b.num_rows()).map(|i| key_at(&cols, i)).collect()
}

/// The PARTITION BY columns of `b`, cast to their types as keys compare them.
fn part_cols(b: &RecordBatch, part: &[(String, DataType)]) -> Result<Vec<ArrayRef>> {
    part.iter().map(|(c, t)| Ok(cast(b.column_by_name(c).with_context(|| format!("no column {c}"))?, t)?)).collect()
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

/// The run's input, in parts: S's rows, then the held rows of K (`parts`, one batch each), with their ids (`ids`, over all
/// the parts), each row with its key's counts (`counts`: `k` and `m`, one per key id, read through `kid`) and W.
#[allow(clippy::too_many_arguments)]
fn input_of(schema: &SchemaRef, cols: &[(String, DataType)], parts: &[&RecordBatch], ids: &[i64], kid: &[u32], counts: &[(i64, i64)], w: &ScalarValue) -> Result<Vec<RecordBatch>> {
    let mut out = vec![];
    let mut at = 0;
    for rows in parts {
        let n = rows.num_rows();
        let (ok, om): (Vec<i64>, Vec<i64>) = kid[at..at + n].iter().map(|&k| counts[k as usize]).unzip();
        out.push(input_part(schema, cols, rows, &ids[at..at + n], &ok, &om, w)?);
        at += n;
    }
    Ok(out)
}

/// The rows at global positions `at` of `parts`, taken in order, as one batch of `schema` (empty when `at` is empty).
fn pick(parts: &[RecordBatch], schema: &SchemaRef, at: &[usize]) -> Result<RecordBatch> {
    if at.is_empty() {
        return Ok(RecordBatch::new_empty(schema.clone()));
    }
    let mut starts = Vec::with_capacity(parts.len());
    let mut total = 0;
    for p in parts {
        starts.push(total);
        total += p.num_rows();
    }
    // The last part that starts at or before a position holds it (an empty part never does).
    let index: Vec<(usize, usize)> = at.iter().map(|&pos| {
        let part = starts.partition_point(|&s| s <= pos) - 1;
        (part, pos - starts[part])
    }).collect();
    let refs: Vec<&RecordBatch> = parts.iter().collect();
    Ok(interleave_record_batch(&refs, &index)?)
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

/// Input rows as held rows to write: the source's columns, the source row's id as `__mr_row`, and `_deleted` (false: a
/// keyed table's change marks the rows that go).
fn held_rows_of(b: &RecordBatch, cols: &[(String, DataType)]) -> Result<RecordBatch> {
    let mut arrays: Vec<ArrayRef> = cols.iter().map(|(c, _)| Ok(b.column_by_name(c).with_context(|| format!("no column {c}"))?.clone())).collect::<Result<_>>()?;
    arrays.push(Arc::new(Int64Array::from(ints(b, ROW_ID)?)));
    arrays.push(Arc::new(BooleanArray::from(vec![false; b.num_rows()])));
    let mut fields: Vec<Field> = held_schema_of(cols).fields().iter().map(|f| f.as_ref().clone()).collect();
    fields.push(Field::new("_deleted", DataType::Boolean, true));
    Ok(RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)?)
}

/// The key columns of `keys`, one row per key: `part`'s columns, or with no PARTITION BY one `__mr_all` column (the key is
/// `[true]`). No keys: an empty batch of the same columns.
fn keys_batch(keys: &[Key], part: &[(String, DataType)]) -> Result<RecordBatch> {
    let fields: Vec<Field> = match part.is_empty() {
        true => vec![Field::new("__mr_all", DataType::Boolean, true)],
        false => fields_of(part).collect(),
    };
    let schema = Arc::new(Schema::new(fields));
    if keys.is_empty() {
        return Ok(RecordBatch::new_empty(schema));
    }
    let arrays: Vec<ArrayRef> = (0..schema.fields().len()).map(|j| Ok(ScalarValue::iter_to_array(keys.iter().map(|k| k[j].clone()))?)).collect::<Result<_>>()?;
    Ok(RecordBatch::try_new(schema, arrays)?)
}

/// `$resume`'s rows for the keys with a marker: the key's columns, `k`, `m` (the marker's `next_m`) and `due`.
fn resume_batch(markers: &[(Key, Marker)], part: &[(String, DataType)], meta: &TableMeta) -> Result<RecordBatch> {
    let schema = crate::answer::schema(&meta.columns.iter().filter(|(c, _)| c != "_deleted").cloned().collect::<Vec<_>>())?;
    if markers.is_empty() {
        return Ok(RecordBatch::new_empty(schema));
    }
    let keys: Vec<Key> = markers.iter().map(|(k, _)| k.clone()).collect();
    let mut arrays: Vec<ArrayRef> = keys_batch(&keys, part)?.columns().to_vec();
    arrays.push(Arc::new(Int64Array::from(markers.iter().map(|(_, mk)| mk.k).collect::<Vec<_>>())));
    arrays.push(Arc::new(Int64Array::from(markers.iter().map(|(_, mk)| mk.next_m).collect::<Vec<_>>())));
    arrays.push(Arc::new(Int64Array::from(markers.iter().map(|(_, mk)| mk.due).collect::<Vec<_>>())));
    Ok(RecordBatch::try_new(schema, arrays)?)
}

/// The held delta of a run, as positions in its input (ascending): the new rows (the first `n_new`) that stay held, and the
/// held rows that don't stay. For each key id, its last `keep` rows by (time, row id) stay; a key with no marker keeps all.
fn delta(kid: &[u32], times: &[i64], ids: &[i64], n_new: usize, keep: &[Option<usize>]) -> (Vec<usize>, Vec<usize>) {
    let stay = stay_mask(kid, times, ids, keep);
    let n = kid.len();
    ((0..n).filter(|&i| i < n_new && stay[i]).collect(), (0..n).filter(|&i| i >= n_new && !stay[i]).collect())
}

/// Which input rows stay held: per key id, the last `keep` rows by (time, row id), or all of them with no marker.
fn stay_mask(kid: &[u32], times: &[i64], ids: &[i64], keep: &[Option<usize>]) -> Vec<bool> {
    let mut groups: Vec<Vec<usize>> = vec![vec![]; keep.len()];
    for (i, &k) in kid.iter().enumerate() {
        groups[k as usize].push(i);
    }
    let mut stay = vec![false; kid.len()];
    for (k, rows) in groups.iter_mut().enumerate() {
        rows.sort_by_key(|&i| (times[i], ids[i]));
        let n = keep[k].unwrap_or(rows.len()).min(rows.len());
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
    use crate::views::Refresh;

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
            async move {
                let e = create(&lake, "dips", &sql, o).await.expect_err("a refusal");
                assert_eq!(crate::codes::of(&e), "0A000", "{e}");
                e.to_string()
            }
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
    async fn once_and_matches_refuse_the_same_options_with_one_code() -> Result<()> {
        let dir = std::env::temp_dir().join(format!("pondra-matches-{}", uuid::Uuid::new_v4()));
        let lake = lake_with_ticks(&dir).await?;
        let by_minute = "SELECT date_trunc('minute', ts) AS m, count(*) AS n FROM ticks GROUP BY 1";
        let e = crate::once::create(&lake, "per_min", by_minute, Options { lag_secs: Some(60), ..options() }).await.expect_err("a refusal");
        assert_eq!(crate::codes::of(&e), "0A000", "{e}");
        assert!(e.to_string().contains("each group once"), "{e}");
        let e = create(&lake, "dips", TICKER, Options { lag_secs: Some(60), ..options() }).await.expect_err("a refusal");
        assert_eq!(crate::codes::of(&e), "0A000", "{e}");
        assert!(e.to_string().contains("each match once"), "{e}");
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
        // Held rows of key A (its counts: k 1, m 2), and a new key B with one row of its own. Key B is id 0, A is id 1.
        let held = RecordBatch::try_from_iter(vec![
            ("symbol", Arc::new(datafusion::arrow::array::StringArray::from(vec!["A", "A"])) as ArrayRef),
            ("price", Arc::new(datafusion::arrow::array::Float64Array::from(vec![5.0, 4.0])) as ArrayRef),
        ])?;
        let w = ScalarValue::TimestampMicrosecond(Some(100), None).cast_to(&time)?;
        let all = concat_batches(&schema, &input_of(&schema, &cols, &[&s, &held], &[7, 3, 4], &[0, 1, 1], &[(0, 0), (1, 2)], &w)?)?;
        assert_eq!(all.schema().fields().iter().map(|f| f.name().as_str()).collect::<Vec<_>>(), ["symbol", "price", "_row_id", "__mr_ok", "__mr_om", "__mr_w"]);
        assert_eq!(ints(&all, ROW_ID)?, [7, 3, 4]);
        assert_eq!(ints(&all, "__mr_ok")?, [0, 1, 1]);
        assert_eq!(ints(&all, "__mr_om")?, [0, 2, 2]);
        assert_eq!(micros_of(&all, "__mr_w")?, [100, 100, 100]);
        Ok(())
    }

    #[test]
    fn a_key_keeps_its_last_rows_and_the_rest_leave() {
        // The new rows first: A's new row 9 (time 3) and B's new row 5 (time 1). Then A's held rows 3 and 4 (times 1 and 2).
        let kid: [u32; 4] = [0, 1, 0, 0];
        let times: [i64; 4] = [3, 1, 1, 2];
        let ids: [i64; 4] = [9, 5, 3, 4];
        let keep: [Option<usize>; 2] = [Some(2), Some(1)];
        let (new, gone) = delta(&kid, &times, &ids, 2, &keep);
        let at = |p: &[usize]| p.iter().map(|&i| ids[i]).collect::<Vec<i64>>();
        assert_eq!((at(&new), at(&gone)), (vec![9, 5], vec![3]), "A keeps its last two rows, B its one");
        // A key with no marker keeps all its rows: nothing leaves.
        let (_, gone) = delta(&kid, &times, &ids, 2, &[None, None]);
        assert!(gone.is_empty());
    }

    #[test]
    fn keys_are_one_id_each_across_batches() -> Result<()> {
        let part = vec![("symbol".to_string(), DataType::Utf8)];
        let batch = |s: Vec<Option<&str>>| -> Result<RecordBatch> { Ok(RecordBatch::try_from_iter(vec![("symbol", Arc::new(datafusion::arrow::array::StringArray::from(s)) as ArrayRef)])?) };
        let mut keys = Keys::new(&part)?;
        assert_eq!(keys.ids(&batch(vec![Some("A"), None, None])?)?, [0, 1, 1]);
        assert_eq!(keys.ids(&batch(vec![Some("B"), Some("A"), None])?)?, [2, 0, 1]);
        assert_eq!(keys.len(), 3);
        let key = |s: Option<&str>| vec![ScalarValue::Utf8(s.map(String::from))];
        assert_eq!((keys.find(&key(Some("A"))), keys.find(&key(None)), keys.find(&key(Some("C")))), (Some(0), Some(1), None));

        // No PARTITION BY: every row is the one key, id 0.
        let mut all = Keys::new(&[])?;
        assert_eq!(all.ids(&batch(vec![Some("x"), None])?)?, [0, 0]);
        assert_eq!(all.find(&vec![ScalarValue::Boolean(Some(true))]), Some(0));
        Ok(())
    }

    #[test]
    fn keys_batch_has_a_row_per_key() -> Result<()> {
        let part = vec![("symbol".to_string(), DataType::Utf8)];
        let key = |s: Option<&str>| vec![ScalarValue::Utf8(s.map(String::from))];
        let two = keys_batch(&[key(Some("A")), key(None)], &part)?;
        assert_eq!(two.num_rows(), 2);
        let symbol = two.column_by_name("symbol").context("symbol")?.as_string::<i32>();
        assert_eq!((symbol.value(0), symbol.is_null(1)), ("A", true));
        let none = keys_batch(&[], &part)?;
        assert_eq!(none.num_rows(), 0);
        assert_eq!(none.schema(), two.schema());
        // No PARTITION BY: one `__mr_all` column, true for the one key.
        let all = keys_batch(&[vec![ScalarValue::Boolean(Some(true))]], &[])?;
        assert_eq!(all.schema().fields().iter().map(|f| f.name().as_str()).collect::<Vec<_>>(), ["__mr_all"]);
        assert!(all.column(0).as_boolean().value(0));
        Ok(())
    }

    #[tokio::test]
    async fn of_keys_takes_the_rows_of_the_given_keys_only() -> Result<()> {
        use datafusion::arrow::array::StringArray;
        let ctx = datafusion::prelude::SessionContext::new();
        let state = RecordBatch::try_from_iter(vec![
            ("symbol", Arc::new(StringArray::from(vec![Some("A"), None, Some("B")])) as ArrayRef),
            ("price", Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef),
        ])?;
        ctx.register_table("__state", Arc::new(MemTable::try_new(state.schema(), vec![vec![state.clone()]])?))?;
        let keys = RecordBatch::try_from_iter(vec![("symbol", Arc::new(StringArray::from(vec![Some("A"), None])) as ArrayRef)])?;
        let got = rows_as(&state.schema(), &of_keys(&ctx, &keys).await?)?;
        assert_eq!(got.num_rows(), 2, "the 'A' row and the NULL row, not 'B'");
        let symbol = got.column_by_name("symbol").context("symbol")?.as_string::<i32>();
        let price = ints(&got, "price")?;
        let mut pairs: Vec<(Option<String>, i64)> = (0..got.num_rows()).map(|i| (symbol.is_valid(i).then(|| symbol.value(i).to_string()), price[i])).collect();
        pairs.sort();
        assert_eq!(pairs, vec![(None, 2), (Some("A".to_string()), 1)]);
        Ok(())
    }

    #[test]
    fn pick_takes_rows_across_parts() -> Result<()> {
        let part = |v: Vec<i64>| RecordBatch::try_from_iter(vec![("x", Arc::new(Int64Array::from(v)) as ArrayRef)]);
        let parts = vec![part(vec![10, 11])?, part(vec![])?, part(vec![20, 21, 22])?];
        let schema = parts[0].schema();
        let picked = pick(&parts, &schema, &[4, 0, 2])?;
        assert_eq!(ints(&picked, "x")?, [22, 10, 20]);
        assert_eq!(pick(&parts, &schema, &[])?.num_rows(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn cut_is_just_past_the_nth_time() -> Result<()> {
        use datafusion::arrow::array::{TimestampMicrosecondArray, TimestampNanosecondArray};
        let ctx = datafusion::prelude::SessionContext::new();
        let frame = |array: ArrayRef| -> Result<DataFrame> { Ok(ctx.read_batch(RecordBatch::try_from_iter(vec![("ts", array)])?)?) };
        let (micro, nano) = (DataType::Timestamp(TimeUnit::Microsecond, None), DataType::Timestamp(TimeUnit::Nanosecond, None));
        let t = ident("ts");
        // Times 10, 20, 20 and 30 µs, W0 0, W 100: the 2nd time is 20, the 4th 30, and fewer than 9 rows give W.
        let df = frame(Arc::new(TimestampMicrosecondArray::from(vec![10, 20, 20, 30])))?;
        assert_eq!(cut(&df, &t, "ts", &micro, 0, 100, 2).await?, 21);
        assert_eq!(cut(&df, &t, "ts", &micro, 0, 100, 4).await?, 31);
        assert_eq!(cut(&df, &t, "ts", &micro, 0, 100, 9).await?, 100);
        // One row at 1,500 ns in a nanosecond column (1 µs, so 2), and negative times (at least 1).
        let df = frame(Arc::new(TimestampNanosecondArray::from(vec![1500])))?;
        assert_eq!(cut(&df, &t, "ts", &nano, 0, 100, 1).await?, 2);
        let df = frame(Arc::new(TimestampMicrosecondArray::from(vec![-5, -3])))?;
        assert_eq!(cut(&df, &t, "ts", &micro, 0, 100, 1).await?, 1);
        Ok(())
    }

    /// A frame of one time column `ts` of `array`.
    fn frame_of(ctx: &datafusion::prelude::SessionContext, array: ArrayRef) -> Result<DataFrame> {
        Ok(ctx.read_batch(RecordBatch::try_from_iter(vec![("ts", array)])?)?)
    }

    #[tokio::test]
    async fn cut_points_cut_the_span_into_steps_of_about_most_rows() -> Result<()> {
        use datafusion::arrow::array::TimestampMicrosecondArray;
        let ctx = datafusion::prelude::SessionContext::new();
        let micro = DataType::Timestamp(TimeUnit::Microsecond, None);
        // Times 1..=100 µs, one row each: a point every 10 rows, and the last 10 rows (91..=100) are left to W.
        let df = frame_of(&ctx, Arc::new(TimestampMicrosecondArray::from((1..=100).collect::<Vec<i64>>())))?;
        assert_eq!(cut_points(&df, "ts", &micro, 0, 100, 10).await?, [(10, false), (20, false), (30, false), (40, false), (50, false), (60, false), (70, false), (80, false), (90, false)]);
        Ok(())
    }

    #[tokio::test]
    async fn next_cut_takes_the_exact_cut_within_a_full_slice() -> Result<()> {
        use datafusion::arrow::array::TimestampMicrosecondArray;
        let ctx = datafusion::prelude::SessionContext::new();
        let micro = DataType::Timestamp(TimeUnit::Microsecond, None);
        // 50 rows at 7 µs and times 1..=6 and 8..=20: from W0 6, the one slice [7, 31] holds 63 rows, so it is full, and
        // its exact cut is just past the 10th row's time, 7 µs.
        let mut times: Vec<i64> = vec![7; 50];
        times.extend(1..=6);
        times.extend(8..=20);
        let df = frame_of(&ctx, Arc::new(TimestampMicrosecondArray::from(times)))?;
        let mut points = vec![];
        assert_eq!(next_cut(&mut points, &df, "ts", &micro, 6, 100_000, 10).await?, 8);
        assert_eq!(points, [(31, true)]);
        Ok(())
    }

    #[tokio::test]
    async fn cut_points_are_empty_when_the_span_fits_in_one_step() -> Result<()> {
        use datafusion::arrow::array::TimestampMicrosecondArray;
        let ctx = datafusion::prelude::SessionContext::new();
        let micro = DataType::Timestamp(TimeUnit::Microsecond, None);
        let df = frame_of(&ctx, Arc::new(TimestampMicrosecondArray::from(vec![1_i64, 2, 3])))?;
        assert!(cut_points(&df, "ts", &micro, 0, 100, 10).await?.is_empty());
        let mut points = vec![];
        assert_eq!(next_cut(&mut points, &df, "ts", &micro, 0, 100, 10).await?, 100);
        assert!(points.is_empty());
        // A span with no rows at all (the least time of none is NULL): no points either.
        let none = frame_of(&ctx, Arc::new(TimestampMicrosecondArray::from(Vec::<i64>::new())))?;
        assert!(cut_points(&none, "ts", &micro, 0, 100, 10).await?.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn next_cut_walks_its_points_and_makes_no_pass_while_one_is_left() -> Result<()> {
        use datafusion::arrow::array::TimestampMicrosecondArray;
        let ctx = datafusion::prelude::SessionContext::new();
        let micro = DataType::Timestamp(TimeUnit::Microsecond, None);
        // Times 1..=100 from W0 40: a point every 10 rows, 50 first.
        let df = frame_of(&ctx, Arc::new(TimestampMicrosecondArray::from((1..=100).collect::<Vec<i64>>())))?;
        let mut points = vec![];
        assert_eq!(next_cut(&mut points, &df, "ts", &micro, 40, 100, 10).await?, 50);
        assert_eq!(points, [(50, false), (60, false), (70, false), (80, false), (90, false)]);
        // From W0 50 over an empty frame: a pass could find nothing, so the next point is taken as it is.
        let empty = frame_of(&ctx, Arc::new(TimestampMicrosecondArray::from(Vec::<i64>::new())))?;
        assert_eq!(next_cut(&mut points, &empty, "ts", &micro, 50, 100, 10).await?, 60);
        Ok(())
    }

    #[tokio::test]
    async fn cut_points_count_a_nanosecond_time_by_its_ceiling_in_micros() -> Result<()> {
        use datafusion::arrow::array::TimestampNanosecondArray;
        let ctx = datafusion::prelude::SessionContext::new();
        let nano = DataType::Timestamp(TimeUnit::Nanosecond, None);
        let df = frame_of(&ctx, Arc::new(TimestampNanosecondArray::from(vec![11_500_i64, 12_500])))?;
        // The ns count is the raw count in ns: 11,500 ns is 12 µs by the ceiling, so from W0 10 with most 1 the point is 12.
        let raw = df.clone().select(vec![datafusion::logical_expr::cast(ident("ts"), DataType::Int64).alias("raw")])?.collect().await?;
        assert_eq!(ints(&raw[0], "raw")?, [11_500, 12_500]);
        assert_eq!(cut_points(&df, "ts", &nano, 10, 100, 1).await?, [(12, false)]);
        assert_eq!(df.filter(span(&ident("ts"), &nano, 10, 12)?)?.count().await?, 1, "the span (10, 12] holds the 11,500 ns row");
        Ok(())
    }

    #[tokio::test]
    async fn cut_points_clamp_negative_times_to_one() -> Result<()> {
        use datafusion::arrow::array::TimestampMicrosecondArray;
        let ctx = datafusion::prelude::SessionContext::new();
        let micro = DataType::Timestamp(TimeUnit::Microsecond, None);
        let df = frame_of(&ctx, Arc::new(TimestampMicrosecondArray::from(vec![-5_i64, -3])))?;
        assert_eq!(cut_points(&df, "ts", &micro, 0, 100, 1).await?, [(1, false)]);
        Ok(())
    }

    #[tokio::test]
    async fn next_cut_stops_at_w_when_w_is_below_the_head() -> Result<()> {
        use datafusion::arrow::array::TimestampMicrosecondArray;
        let ctx = datafusion::prelude::SessionContext::new();
        let micro = DataType::Timestamp(TimeUnit::Microsecond, None);
        let empty = frame_of(&ctx, Arc::new(TimestampMicrosecondArray::from(Vec::<i64>::new())))?;
        let mut points = vec![(50, false)];
        assert_eq!(next_cut(&mut points, &empty, "ts", &micro, 40, 45, 10).await?, 45);
        assert_eq!(points, [(50, false)]);
        Ok(())
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
