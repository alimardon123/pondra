//! What the lake learns from its runs (ADR-050 §3): a filter on a table that kept far more or far
//! fewer of its rows than the planner expected. A query of `PONDRA_LEARN_MS` (100) or more that ran
//! here notes each such filter, off by 2× or more over 1,000 rows or more, in its history row
//! (`pondra.history`'s `learned`); `pondra.learned` is each one as last seen, and how many runs saw it.
//!
//! The facts are the history's own, so they cost no catalog entries and no requests of their own:
//! written by its writer, off the statement's path, in its quiet commits (invariant 224), and gone
//! with its rows after `PONDRA_HISTORY_DAYS`.
//!
//! A filter is known by its table and its conditions as one text (`about`): each `column op value`,
//! `IN`, `LIKE` and `IS [NOT] NULL`, sorted, joined by AND, the same whether read from the plan the
//! planner made (a logical `Expr`) or the one that ran (a `PhysicalExpr`), so the planner can ask
//! for what it is about to estimate in the words it was learned in.
//!
//! The planner uses them (`share`, from `optimize::size`): a filter on a table a run measured is
//! counted as the share it kept, in place of the estimate, while a query a door asked for is planned
//! (`planning`). Each node keeps a copy, read from the history again (its newer rows) at most every
//! 10 s and only while queries are planned. A spread query's slices carry the facts its coordinator
//! planned with, and every node plans with those alone (`given`, invariant 27). Facts are held to a
//! bar, as the spread guard lets the faster way win (`ran`): the run after a query's 1st, 2nd, 4th,
//! 8th… with facts is planned without them, as warm, and a query whose runs with them are slower than
//! the best of those by a tenth (and 2 ms), twice in a row, has them set aside. A query is known by its words, without
//! its comments and spacing (`key`). `PONDRA_LEARN=off`: the planner uses none.
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{ScalarValue, TableReference};
use datafusion::logical_expr::{utils::split_conjunction, Expr, LogicalPlan};
use datafusion::physical_expr::expressions::{BinaryExpr, CastExpr, Column, InListExpr, IsNotNullExpr, IsNullExpr, LikeExpr, Literal, TryCastExpr};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::ExecutionPlan;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, Mutex, OnceLock, RwLock};

/// A filter that came out far from what the planner expected: of the table's rows, the share it was
/// expected to keep and the share it kept.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct Fact {
    pub object: String,
    pub kind: String,
    pub about: String,
    pub expected: f64,
    pub actual: f64,
}

/// The filters on one table each that a plan which ran found 2× or more off what was expected.
/// A filter whose rows a join, a top-N, a min/max or a limit may have cut as they ran (dynamic
/// filters, an early stop) is passed over: its rows say nothing of the filter.
pub fn found(logical: &LogicalPlan, plan: &Arc<dyn ExecutionPlan>) -> Vec<Fact> {
    let mut tables: HashMap<String, BTreeSet<String>> = HashMap::new();
    let mut counted: HashMap<String, f64> = HashMap::new(); // (each table's rows, as the planner counts them)
    let _ = logical.apply_with_subqueries(|n| {
        if let LogicalPlan::Filter(f) = n {
            if let (LogicalPlan::TableScan(s), Some(about)) = (f.input.as_ref(), about(&f.predicate)) {
                let rows = datafusion::datasource::source_as_provider(&s.source).ok().and_then(|p| p.statistics()).and_then(|st| st.num_rows.get_value().copied());
                counted.insert(name(&s.table_name), rows.unwrap_or(0) as f64);
                tables.entry(about).or_default().insert(name(&s.table_name));
            }
        }
        Ok(TreeNodeRecursion::Continue)
    });
    if tables.is_empty() {
        return vec![];
    }
    let mut seen = HashMap::new();
    running(plan, false, &mut seen);
    let mut facts: Vec<Fact> = seen.into_iter().filter_map(|(about, [expected, actual, of])| {
        let [object] = &tables.get(&about)?.iter().collect::<Vec<_>>()[..] else { return None }; // (one table filtered so: the fact is that table's)
        let off = expected.max(actual) / expected.min(actual).max(1.0);
        // (shares of the whole table as the planner counts it: a scan whose files' ranges pruned some
        // away read fewer rows than the table has, and the planner multiplies a share by all of them)
        let whole = of.max(counted.get(*object).copied().unwrap_or(0.0));
        (of >= 1000.0 && off >= 2.0).then(|| Fact { object: object.to_string(), kind: "filter".into(), about, expected: expected / whole, actual: actual / whole })
    }).collect();
    facts.sort_by(|a, b| (&a.object, &a.about).cmp(&(&b.object, &b.about)));
    facts
}

/// Each filter that ran and wasn't cut: its conditions, and its expected rows, its rows and the
/// rows it read, added up over the plan (a table's files and its log tail are filtered apart).
fn running(p: &Arc<dyn ExecutionPlan>, cut: bool, seen: &mut HashMap<String, [f64; 3]>) {
    if let (false, Some(f)) = (cut, p.downcast_ref::<datafusion::physical_plan::filter::FilterExec>()) {
        if let (Some(about), Some(actual), Some(expected), Some(of)) = (running_about(f.predicate()), p.metrics().and_then(|m| m.output_rows()), rows(p.as_ref()), rows(f.input().as_ref())) {
            let s = seen.entry(about).or_default();
            *s = [s[0] + expected, s[1] + actual as f64, s[2] + of];
        }
    }
    let minmax = || {
        let text = datafusion::physical_plan::displayable(p.as_ref()).one_line().to_string();
        text.contains("min(") || text.contains("max(")
    };
    let cuts = cut || p.fetch().is_some() || (p.name() == "AggregateExec" && minmax());
    for (i, c) in p.children().into_iter().enumerate() {
        running(c, cuts || (p.name() == "HashJoinExec" && i == 1), seen); // (a hash join's probe side gets its dynamic filter)
    }
}

/// The rows the planner expects of `p`.
fn rows(p: &dyn ExecutionPlan) -> Option<f64> {
    use datafusion::physical_plan::statistics::{StatisticsArgs, StatisticsContext};
    let s = StatisticsContext::new().compute(p, &StatisticsArgs::new()).ok()?;
    s.num_rows.get_value().map(|&n| n as f64)
}

/// A table's name as the planner read it, its parts joined (`history` makes it the lake's own name).
fn name(t: &TableReference) -> String {
    [t.catalog(), t.schema(), Some(t.table())].into_iter().flatten().collect::<Vec<_>>().join(".")
}

/// A filter's conditions as one text (see the top), from the plan the planner made; `None` when
/// any of them is something else.
pub fn about(e: &Expr) -> Option<String> {
    fn column(e: &Expr) -> Option<&str> {
        match e {
            Expr::Column(c) => Some(&c.name),
            Expr::Cast(c) => column(&c.expr),
            Expr::TryCast(c) => column(&c.expr),
            _ => None,
        }
    }
    let value = |e: &Expr| match e {
        Expr::Literal(v, _) => Some(sql(v)),
        _ => None,
    };
    let each = split_conjunction(e).into_iter().map(|c| match c {
        Expr::BinaryExpr(b) => match (column(&b.left), value(&b.right), column(&b.right), value(&b.left)) {
            (Some(c), Some(v), ..) => Some(format!("{c} {} {v}", b.op)),
            (.., Some(c), Some(v)) => Some(format!("{c} {} {v}", b.op.swap()?)),
            _ => None,
        },
        Expr::InList(l) => Some(within(column(&l.expr)?, l.negated, l.list.iter().map(value).collect::<Option<_>>()?)),
        Expr::Like(l) if l.escape_char.is_none() => Some(like(column(&l.expr)?, l.negated, l.case_insensitive, value(&l.pattern)?)),
        Expr::IsNull(c) => Some(format!("{} IS NULL", column(c)?)),
        Expr::IsNotNull(c) => Some(format!("{} IS NOT NULL", column(c)?)),
        _ => None,
    });
    joined(each.collect::<Option<_>>()?)
}

/// The same text from the plan that ran.
fn running_about(e: &Arc<dyn PhysicalExpr>) -> Option<String> {
    fn column(e: &Arc<dyn PhysicalExpr>) -> Option<&str> {
        if let Some(c) = e.downcast_ref::<Column>() {
            return Some(c.name());
        }
        column(e.downcast_ref::<CastExpr>().map(|c| c.expr()).or_else(|| e.downcast_ref::<TryCastExpr>().map(|c| c.expr()))?)
    }
    let value = |e: &Arc<dyn PhysicalExpr>| e.downcast_ref::<Literal>().map(|l| sql(l.value()));
    let each = datafusion::physical_expr::split_conjunction(e).into_iter().map(|c| {
        if let Some(b) = c.downcast_ref::<BinaryExpr>() {
            return match (column(b.left()), value(b.right()), column(b.right()), value(b.left())) {
                (Some(c), Some(v), ..) => Some(format!("{c} {} {v}", b.op())),
                (.., Some(c), Some(v)) => Some(format!("{c} {} {v}", b.op().swap()?)),
                _ => None,
            };
        }
        if let Some(l) = c.downcast_ref::<InListExpr>() {
            return Some(within(column(l.expr())?, l.negated(), l.list().iter().map(value).collect::<Option<_>>()?));
        }
        if let Some(l) = c.downcast_ref::<LikeExpr>() {
            return Some(like(column(l.expr())?, l.negated(), l.case_insensitive(), value(l.pattern())?));
        }
        if let Some(n) = c.downcast_ref::<IsNullExpr>() {
            return Some(format!("{} IS NULL", column(n.arg())?));
        }
        c.downcast_ref::<IsNotNullExpr>().and_then(|n| Some(format!("{} IS NOT NULL", column(n.arg())?)))
    });
    joined(each.collect::<Option<_>>()?)
}

fn within(column: &str, negated: bool, values: Vec<String>) -> String {
    format!("{column} {}IN ({})", if negated { "NOT " } else { "" }, values.join(", "))
}

fn like(column: &str, negated: bool, case_insensitive: bool, pattern: String) -> String {
    format!("{column} {}{} {pattern}", if negated { "NOT " } else { "" }, if case_insensitive { "ILIKE" } else { "LIKE" })
}

fn joined(mut each: Vec<String>) -> Option<String> {
    each.sort();
    each.dedup();
    (!each.is_empty()).then(|| each.join(" AND "))
}

/// A value as SQL writes it: text quoted, numbers bare, anything else quoted as it prints.
fn sql(v: &ScalarValue) -> String {
    match v {
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) | ScalarValue::Utf8View(Some(s)) => format!("'{}'", s.replace('\'', "''")),
        v if v.is_null() => "NULL".into(),
        v if v.data_type().is_numeric() => v.to_string(),
        v => format!("'{v}'"),
    }
}

/// `pondra.learned`: each fact as last seen in the history the caller may read, how many runs saw
/// it and when the last did.
pub async fn table(ctx: &datafusion::prelude::SessionContext, history: Arc<dyn datafusion::catalog::TableProvider>) -> anyhow::Result<Arc<dyn datafusion::catalog::TableProvider>> {
    use datafusion::arrow::array::{Array, AsArray, Float64Array, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit, TimestampMicrosecondType};
    use datafusion::prelude::col;
    let rows = ctx.read_table(history)?.filter(col("learned").is_not_null())?.select_columns(&["at", "learned"])?.collect().await?;
    let mut last: BTreeMap<(String, String, String), (i64, f64, f64, i64)> = BTreeMap::new(); // (at, expected, actual, runs)
    for b in &rows {
        let (at, text) = (b.column(0).as_primitive::<TimestampMicrosecondType>(), datafusion::arrow::compute::cast(b.column(1), &DataType::Utf8)?);
        let text = text.as_string::<i32>();
        for i in (0..b.num_rows()).filter(|&i| text.is_valid(i)) {
            for f in serde_json::from_str::<Vec<Fact>>(text.value(i)).unwrap_or_default() {
                let e = last.entry((f.object, f.kind, f.about)).or_insert((i64::MIN, 0.0, 0.0, 0));
                e.3 += 1;
                if at.value(i) >= e.0 {
                    (e.0, e.1, e.2) = (at.value(i), f.expected, f.actual);
                }
            }
        }
    }
    let text = |f: fn(&(String, String, String)) -> &String| Arc::new(last.keys().map(|k| Some(f(k).as_str())).collect::<StringArray>()) as _;
    let ts = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
    let schema = Arc::new(Schema::new(vec![
        Field::new("object", DataType::Utf8, false), Field::new("kind", DataType::Utf8, false), Field::new("about", DataType::Utf8, false),
        Field::new("expected", DataType::Float64, false), Field::new("actual", DataType::Float64, false), Field::new("runs", DataType::Int64, false), Field::new("updated_at", ts, false),
    ]));
    let batch = RecordBatch::try_new(schema.clone(), vec![
        text(|k| &k.0), text(|k| &k.1), text(|k| &k.2),
        Arc::new(last.values().map(|v| v.1).collect::<Float64Array>()), Arc::new(last.values().map(|v| v.2).collect::<Float64Array>()),
        Arc::new(last.values().map(|v| v.3).collect::<Int64Array>()), Arc::new(last.values().map(|v| Some(v.0)).collect::<TimestampMicrosecondArray>().with_timezone("UTC")),
    ])?;
    Ok(Arc::new(datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]])?))
}

/// Facts as a slice carries them (`spmd::Slice::learned`): a table as this lake names it, a filter's
/// conditions (`about`), and the share of the table's rows it keeps.
pub type Given = Vec<(String, String, f64)>;

/// This node's copy of the facts: (table, conditions) → (when last seen, µs; the share kept). Read
/// from the history again, its newer rows only, at most every 10 s and only while queries are planned:
/// nothing runs when nothing asks.
#[derive(Default)]
struct Known {
    lake: std::sync::Weak<crate::store::Lake>,
    name: String, // (the lake's own name: `lake.s.t` is its `s.t`)
    facts: HashMap<(String, String), (i64, f64)>,
    newest: i64, // (the newest history row read, µs)
    read: Option<std::time::Instant>,
    reading: bool,
}

static KNOWN: LazyLock<RwLock<Known>> = LazyLock::new(Default::default);
const KEPT: usize = 20_000; // (facts at most: a few MB)

fn known() -> std::sync::RwLockReadGuard<'static, Known> { KNOWN.read().unwrap_or_else(|e| e.into_inner()) }
fn known_mut() -> std::sync::RwLockWriteGuard<'static, Known> { KNOWN.write().unwrap_or_else(|e| e.into_inner()) }

/// Whether the planner uses facts (`PONDRA_LEARN=off`: never; they are still learned and kept).
fn on() -> bool {
    static ON: LazyLock<bool> = LazyLock::new(|| !matches!(std::env::var("PONDRA_LEARN").as_deref(), Ok("off" | "0" | "false")));
    *ON
}

/// The lake whose history this node's facts are read from: its own, once open (`main.rs`).
pub fn start(lake: &Arc<crate::store::Lake>) {
    let mut k = known_mut();
    (k.lake, k.name) = (Arc::downgrade(lake), crate::ddl::lake_name(lake));
}

/// A table's name as the history keeps it (`ddl::local`): `public.t` is `t`, this lake's `l.s.t` is
/// `s.t`; another lake's keeps its three parts.
fn local(name: &str, lake: &str) -> String {
    match name.split('.').collect::<Vec<_>>()[..] {
        [s, t] => crate::ddl::join(s, t),
        [l, s, t] if l == lake => crate::ddl::join(s, t),
        _ => name.to_string(),
    }
}

/// A fact seen at `at`, kept if it is the newest of its filter's.
fn keep(k: &mut Known, at: i64, f: &Fact) {
    if f.kind != "filter" || !f.actual.is_finite() {
        return;
    }
    let key = (local(&f.object, &k.name), f.about.clone());
    if k.facts.get(&key).is_none_or(|(seen, _)| at >= *seen) {
        k.facts.insert(key, (at, f.actual.clamp(0.0, 1.0)));
    }
}

/// The newest `KEPT` facts only, once a batch is kept.
fn trim(k: &mut Known) {
    if k.facts.len() > KEPT {
        let mut ats: Vec<i64> = k.facts.values().map(|v| v.0).collect();
        ats.sort_unstable();
        let least = ats[ats.len() - KEPT];
        k.facts.retain(|_, v| v.0 >= least);
    }
}

/// What a run here just found (`history::planned`): known here at once, before its history row is
/// written and read back.
pub fn note(facts: &[Fact]) {
    if facts.is_empty() || !on() {
        return;
    }
    let at = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_micros() as i64);
    let mut k = known_mut();
    for f in facts {
        keep(&mut k, at, f);
    }
    trim(&mut k);
}

/// How a query is being planned: with the facts its coordinator sent (`given`) or with this node's
/// own, as its runs so far say (`verdict`, decided at its first fact). `used`: whether any was;
/// `held`: whether some applied and were held back, to measure the bar.
struct Planning {
    given: Option<HashMap<(String, String), f64>>,
    sql: Box<str>,
    verdict: OnceLock<Verdict>,
    used: Arc<AtomicBool>,
    held: AtomicBool,
}

impl Planning {
    /// Whether this query uses this node's facts; the first time a fact applies, it is decided.
    fn uses(&self) -> bool {
        match *self.verdict.get_or_init(|| verdict(&self.sql)) {
            Verdict::Use => true,
            Verdict::Bar => {
                self.held.store(true, Ordering::Relaxed);
                false
            }
            Verdict::Aside => false,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Debug)]
enum Verdict {
    Use,
    /// planned without them this once: the bar its runs with them are held to
    Bar,
    Aside,
}

/// How a query a door asked for was planned.
#[derive(Clone, Copy, PartialEq, Debug)]
pub enum Planned {
    /// no fact applied (or `PONDRA_LEARN=off`, or set aside for it)
    Plain,
    Used,
    /// facts applied and were held back: its bar
    Bar,
}

tokio::task_local! {
    static PLAN: Arc<Planning>;
}

/// `f`, a query a door asked for, planned with this node's facts as its runs so far say (`ran`);
/// and how. A plan made outside it uses none.
pub async fn planning<F: std::future::Future>(sql: &str, f: F) -> (F::Output, Planned) {
    if !on() {
        return (f.await, Planned::Plain);
    }
    let used = Arc::new(AtomicBool::new(false));
    let plan = Arc::new(Planning { given: None, sql: sql.into(), verdict: OnceLock::new(), used: used.clone(), held: AtomicBool::new(false) });
    let out = PLAN.scope(plan.clone(), f).await;
    let planned = match (plan.held.load(Ordering::Relaxed), used.load(Ordering::Relaxed)) {
        (true, _) => Planned::Bar,
        (false, true) => Planned::Used,
        (false, false) => Planned::Plain,
    };
    (out, planned)
}

/// `f`, a spread query's slice, planned with the facts its coordinator planned with and no others, so
/// every node plans alike (invariant 27). On the coordinator, its query hears whether they were used.
pub async fn given<F: std::future::Future>(facts: &Given, f: F) -> F::Output {
    let used = PLAN.try_with(|p| p.used.clone()).unwrap_or_default();
    let given = facts.iter().map(|(t, a, s)| ((t.clone(), a.clone()), *s)).collect();
    let plan = Planning { given: Some(given), sql: "".into(), verdict: OnceLock::from(Verdict::Use), used, held: AtomicBool::new(false) };
    PLAN.scope(Arc::new(plan), f).await
}

/// The facts on `tables` that a spread query's coordinator plans with and sends in each slice: none
/// outside `planning`, or when held back.
pub fn of(tables: &[String]) -> Given {
    let Ok(plan) = PLAN.try_with(Arc::clone) else { return vec![] };
    let k = known();
    let tables: std::collections::HashSet<String> = tables.iter().map(|t| local(t, &k.name)).collect();
    let mut out: Given = match &plan.given {
        Some(g) => g.iter().filter(|((t, _), _)| tables.contains(t)).map(|((t, a), s)| (t.clone(), a.clone(), *s)).collect(),
        None => k.facts.iter().filter(|((t, _), _)| tables.contains(t)).map(|((t, a), (_, s))| (t.clone(), a.clone(), *s)).collect(),
    };
    if out.is_empty() || !plan.uses() {
        return vec![];
    }
    out.sort_by(|a, b| (&a.0, &a.1).cmp(&(&b.0, &b.1)));
    out
}

/// The share of `table`'s rows a filter keeps (`predicate`), where a run measured it: for
/// `optimize::size`, in place of its estimate. Only while a query is planned (`planning`, `given`).
pub fn share(table: &TableReference, predicate: &Expr) -> Option<f64> {
    let plan = PLAN.try_with(Arc::clone).ok()?;
    let about = about(predicate)?;
    let found = match &plan.given {
        Some(g) => g.get(&(local(&name(table), &known().name), about)).copied(),
        None => {
            let found = {
                let k = known();
                k.facts.get(&(local(&name(table), &k.name), about)).map(|f| f.1)
            };
            refresh();
            found.filter(|_| plan.uses())
        }
    };
    if found.is_some() {
        plan.used.store(true, Ordering::Relaxed);
    }
    found
}

/// Read the history's newer facts, in the background, when the copy is 10 s old or was never read.
fn refresh() {
    if known().read.is_some_and(|t| t.elapsed() < std::time::Duration::from_secs(10)) {
        return;
    }
    let (lake, since) = {
        let mut k = known_mut();
        if k.reading {
            return;
        }
        let Some(lake) = k.lake.upgrade() else { return };
        k.reading = true;
        (lake, k.newest)
    };
    /// Done reading, however it ended.
    struct Done;
    impl Drop for Done {
        fn drop(&mut self) {
            let mut k = known_mut();
            (k.reading, k.read) = (false, Some(std::time::Instant::now()));
        }
    }
    let done = Done;
    let Ok(rt) = tokio::runtime::Handle::try_current() else { return };
    rt.spawn(async move {
        let _done = done;
        // (rows reach the history a second or so after their runs, in any order: a minute back again)
        match read(&lake, since.saturating_sub(60_000_000)).await {
            Ok(rows) => {
                let mut k = known_mut();
                for (at, f) in &rows {
                    k.newest = k.newest.max(*at);
                    keep(&mut k, *at, f);
                }
                trim(&mut k);
            }
            Err(e) => eprintln!("reading what runs learned: {e:#}"),
        }
    });
}

/// The facts in the history's rows after `since` (µs), every row: work no door started runs as the node.
async fn read(lake: &crate::store::Lake, since: i64) -> anyhow::Result<Vec<(i64, Fact)>> {
    use datafusion::arrow::array::{Array, AsArray};
    use datafusion::arrow::datatypes::{DataType, TimestampMicrosecondType};
    use datafusion::prelude::{col, lit};
    let ctx = crate::query::session(lake, "SELECT * FROM pondra.history", "").await?;
    let after = lit(ScalarValue::TimestampMicrosecond(Some(since), Some("UTC".into())));
    let rows = ctx.table("pondra.history").await?.filter(col("learned").is_not_null().and(col("at").gt(after)))?.select_columns(&["at", "learned"])?.collect().await?;
    let mut out = vec![];
    for b in &rows {
        let (at, text) = (b.column(0).as_primitive::<TimestampMicrosecondType>(), datafusion::arrow::compute::cast(b.column(1), &DataType::Utf8)?);
        let text = text.as_string::<i32>();
        for i in (0..b.num_rows()).filter(|&i| text.is_valid(i) && at.is_valid(i)) {
            out.extend(serde_json::from_str::<Vec<Fact>>(text.value(i)).unwrap_or_default().into_iter().map(|f| (at.value(i), f)));
        }
    }
    Ok(out)
}

/// A query's runs with facts: the bar they are held to (its best run planned without the facts that
/// applied, measured after its 1st, 2nd, 4th, 8th… run with them, so as warm as they are), whether its
/// next run measures it, and how many runs in a row with them were slower by a tenth (and 2 ms);
/// twice, and they are set aside for it.
#[derive(Default, Clone, Copy)]
struct Runs {
    bar: Option<f64>,
    measure: bool,
    with: u32,
    slower: u8,
    aside: bool,
}

static RUNS: LazyLock<Mutex<lru::LruCache<u64, Runs>>> = LazyLock::new(|| Mutex::new(lru::LruCache::new(std::num::NonZeroUsize::new(4096).unwrap())));

/// A query by its words, without its comments and spacing: asked again with a note in a comment (as
/// tools and dashboards do), it is the same query. Its literals stay: other values filter otherwise.
/// `EXPLAIN` of it is it (it shows the plan its next run gets); and whether it is an `EXPLAIN` that
/// runs nothing.
fn key(sql: &str) -> (u64, bool) {
    use datafusion::sql::sqlparser::{dialect::GenericDialect, tokenizer::{Token, Tokenizer}};
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    let Ok(tokens) = Tokenizer::new(&GenericDialect {}, sql).tokenize() else {
        sql.hash(&mut h);
        return (h.finish(), false);
    };
    let mut words = tokens.iter().filter(|t| !matches!(t, Token::Whitespace(_))).peekable();
    let (mut explain, mut analyze) = (false, false);
    while let Some(Token::Word(w)) = words.peek() {
        match w.value.to_ascii_uppercase().as_str() {
            "EXPLAIN" => explain = true,
            "ANALYZE" if explain => analyze = true,
            "VERBOSE" if explain => {}
            _ => break,
        }
        words.next();
    }
    words.for_each(|t| t.to_string().hash(&mut h));
    (h.finish(), explain && !analyze)
}

/// How a query is planned, as its runs so far say (decided when a fact first applies to it).
fn verdict(sql: &str) -> Verdict {
    let (k, _) = key(sql);
    match RUNS.lock().unwrap_or_else(|e| e.into_inner()).peek(&k) {
        Some(r) if r.aside => Verdict::Aside,
        Some(r) if r.measure => Verdict::Bar,
        _ => Verdict::Use,
    }
}

/// A query a door asked for ran in `took`, planned as `planning` said. Its 1st, 2nd, 4th, 8th… run
/// with facts has the next one measure the bar (a few runs as before, ever fewer); each run with them
/// is held to the best of those.
pub fn ran(sql: &str, planned: Planned, took: std::time::Duration) {
    if planned == Planned::Plain {
        return; // (nothing applied: nothing to hold to anything)
    }
    let took = took.as_secs_f64();
    let (k, explain) = key(sql);
    if explain {
        return; // (planned, not run)
    }
    let mut runs = RUNS.lock().unwrap_or_else(|e| e.into_inner());
    let r = runs.get_or_insert_mut(k, Runs::default);
    if planned == Planned::Bar {
        (r.bar, r.measure) = (Some(r.bar.map_or(took, |b| b.min(took))), false);
        return;
    }
    r.with = r.with.saturating_add(1);
    r.measure |= r.with.is_power_of_two();
    match r.bar {
        None => {}
        Some(bar) if took > bar * 1.1 + 0.002 => {
            r.slower = r.slower.saturating_add(1);
            r.aside = r.slower >= 2;
        }
        Some(_) => r.slower = 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::{col, lit};

    /// A filter reads the same from the planner's side, whichever way round and in whatever order its
    /// conditions are written; anything else has no text.
    #[test]
    fn a_filter_reads_the_same_either_way() {
        let a = about(&col("country").eq(lit("FR")).and(col("city").eq(lit("Paris"))));
        let b = about(&lit("Paris").eq(col("city")).and(col("country").eq(lit("FR"))));
        assert_eq!(a.as_deref(), Some("city = 'Paris' AND country = 'FR'"));
        assert_eq!(a, b);
        assert_eq!(about(&col("n").gt(lit(5)).and(col("s").in_list(vec![lit("a"), lit("b")], false))).as_deref(), Some("n > 5 AND s IN ('a', 'b')"));
        assert_eq!(about(&lit(5).lt(col("n"))).as_deref(), Some("n > 5"));
        assert_eq!(about(&(col("k") % lit(7)).eq(lit(0))), None);
    }

    /// A table's name as the history keeps it, from any of the names a query gives it.
    #[test]
    fn names_as_the_history_keeps_them() {
        assert_eq!(local("customers", "lake"), "customers");
        assert_eq!(local("public.customers", "lake"), "customers");
        assert_eq!(local("sales.orders", "lake"), "sales.orders");
        assert_eq!(local("lake.public.customers", "lake"), "customers");
        assert_eq!(local("other.public.customers", "lake"), "other.public.customers");
    }

    /// Facts are held to a bar measured right after their first run, and set aside for a query they
    /// made slower twice in a row; once is noise. A note in a comment is the same query.
    #[test]
    fn facts_that_made_a_query_slower_twice_are_set_aside() {
        let q = "SELECT 'a query of this test only' -- run 1";
        let again = "SELECT  'a query of this test only'  -- run 2";
        let ms = std::time::Duration::from_millis;
        assert_eq!(verdict(q), Verdict::Use, "nothing known: its facts used");
        ran(q, Planned::Used, ms(300)); // (cold)
        assert_eq!(verdict(again), Verdict::Bar, "the next run measures the bar, whatever its comment");
        ran(again, Planned::Bar, ms(100));
        assert_eq!(verdict(q), Verdict::Use);
        ran(q, Planned::Used, ms(200));
        assert_eq!(verdict(q), Verdict::Bar, "slower once is not enough; its second run with facts has the bar measured again");
        ran(q, Planned::Bar, ms(120));
        ran(q, Planned::Used, ms(90));
        assert_eq!(verdict(q), Verdict::Use, "its third has not");
        ran(q, Planned::Used, ms(200));
        assert_eq!(verdict(q), Verdict::Bar, "a run no slower in between starts the count again; its fourth: measured again");
        ran(q, Planned::Bar, ms(100));
        ran(q, Planned::Used, ms(115));
        assert_eq!(verdict(q), Verdict::Aside, "twice in a row: set aside");
        assert_eq!(verdict("SELECT 'a query of this test only' WHERE 1 = 2"), Verdict::Use, "another query is its own");
        assert_eq!(verdict("EXPLAIN SELECT 'a query of this test only'"), Verdict::Aside, "EXPLAIN shows the plan its runs get");
        let other = "SELECT 'another query of this test'";
        ran(other, Planned::Used, ms(100));
        ran(&format!("EXPLAIN {other}"), Planned::Used, ms(1));
        assert_eq!(verdict(other), Verdict::Bar, "an EXPLAIN runs nothing: the bar is still to measure");
    }

    /// Only a query being planned uses facts, and a slice only those its coordinator sent.
    #[test]
    fn only_a_planned_query_uses_facts_and_a_slice_only_those_sent() {
        let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
        let paris = col("country").eq(lit("FR")).and(col("city").eq(lit("Paris")));
        let t = TableReference::bare("customers");
        assert_eq!(share(&t, &paris), None, "no query being planned");
        let sent: Given = vec![("customers".into(), about(&paris).unwrap(), 0.01)];
        let (got, planned) = rt.block_on(planning("SELECT 'a slice of this test'", given(&sent, async { share(&t, &paris) })));
        assert_eq!((got, planned), (Some(0.01), Planned::Used));
        assert_eq!(rt.block_on(given(&vec![], async { share(&t, &paris) })), None, "none sent: none used");
    }
}
