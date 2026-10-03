//! Inline views: SQL over each flush of new rows, run by the node that received the rows, whose
//! output commits in the SAME catalog write as its input. No lag behind the source, no progress
//! to track, exactly-once for free, and the work spreads over every node that ingests.
//!
//! * A view without GROUP BY appends its rows to its table: filter, reshape, enrich (it may join
//!   any other table, as of each row's time too: `asof.rs`).
//! * A view with GROUP BY is a merge table: each flush adds partial aggregates per key, and reads
//!   combine them (sum, min, max; counts are summed); compaction folds them into one row per key.
//!   So any number of nodes add to the same keys at once, without coordinating.
//!
//! Two kinds of view also emit what is final, once, when the source's event time has moved past
//! it. The watermark is the newest event time the source holds, less the lateness allowed (rows
//! may arrive that much out of order): as Flink's bounded out-of-orderness, from the data itself.
//! Emission is exactly-once: its progress is a producer's seq, committed with what it emits.
//!
//! * A window view (GROUP BY a `date_bin(…)` window column, with `emit`) emits each window to
//!   `{view}_final` once the watermark passes its end. Rows arriving later still update the
//!   view, not what was emitted.
//! * A session view (`sessions`) holds each key's sessions, a session being its rows with no gap
//!   of `gap_secs` between them, each emitted once, when the watermark passes its last row plus
//!   the gap. A row that falls inside a session already emitted is late, and left out.
use crate::query::{first_table, over, session, session_at};
use crate::store::*;
use anyhow::{bail, ensure, Context, Result};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::logical_expr::{Expr, LogicalPlan};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Serialize, Deserialize, Clone)]
pub struct View {
    pub source: String, // the first table in FROM: the stream the view follows
    pub sql: String,
    #[serde(default)]
    pub emit: Option<Emit>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sessions: Option<Sessions>,
    /// Row by row over its source alone: each of its rows carries its source row's `_row_id` and
    /// `_created_at`, so it follows that row's changes (views made from Pondra 0.19 on).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub ids: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub join: Option<Join>,
    /// Filled from the rows its source had when it was made (views made from Pondra 0.21 on).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fill: Option<Fill>,
    /// What each of its rows should meet (ADR-036 §2).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub expect: Vec<Expect>,
}

/// An expectation, as Databricks' pipelines have them: a condition each of a view's rows should
/// meet, checked as they are written. A row breaks it when the condition is false (NULL passes, as
/// with Postgres's CHECK). `CONSTRAINT c CHECK (…)` fails the write that brings such a row;
/// `EXPECT (…)` keeps it and counts it; `ON VIOLATION DROP ROW | FAIL` says which, for either.
/// Counted in `pondra$expectations`, committed with the rows (`pondra.expectations`).
#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
pub struct Expect {
    pub name: String,
    pub check: String,
    pub on: OnViolation,
}

#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Debug)]
#[serde(rename_all = "lowercase")]
pub enum OnViolation {
    Keep,
    Drop,
    Fail,
}

impl OnViolation {
    fn word(self) -> &'static str {
        match self {
            OnViolation::Keep => "keep",
            OnViolation::Drop => "drop",
            OnViolation::Fail => "fail",
        }
    }
}

/// History kept per key (SCD type 2, ADR-036 §8): `WITH (history = 'id', sequence_by = 'ts'
/// [, delete_when = 'op = ''D'''])`. The view keeps every version of each key as it arrives (an
/// append view: writers on any node need no coordination); a read gives each version its
/// `__start_at` (its `sequence_by`) and `__end_at` (the next version's, NULL for the current one),
/// worked out then, so versions that arrive out of order still make the right history. A version
/// `delete_when` holds for ends its key's history and isn't shown (`__delete`).
#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
pub struct History {
    pub key: Vec<String>,
    pub sequence_by: String,
    #[serde(default)]
    pub deletes: bool,
}

/// A history view's table as it is read: its versions, each with `__start_at` and `__end_at`.
pub async fn history_view(ctx: &datafusion::prelude::SessionContext, base: std::sync::Arc<dyn datafusion::catalog::TableProvider>, h: &History, sys: bool) -> Result<std::sync::Arc<dyn datafusion::catalog::TableProvider>> {
    let aux = datafusion::prelude::SessionContext::new_with_state(ctx.state());
    aux.register_table("__versions", base.clone())?;
    let keep: Vec<String> = base.schema().fields().iter().map(|f| f.name().clone()).filter(|n| n != "__delete" && (sys || !crate::sys::NAMES.contains(&n.as_str()))).map(|n| quoted(&n)).collect();
    let key = h.key.iter().map(|k| quoted(k)).collect::<Vec<_>>().join(", ");
    let seq = quoted(&h.sequence_by);
    let shown = if h.deletes { "WHERE NOT coalesce(\"__delete\", false)" } else { "" };
    let sql = format!("SELECT {}, \"__start_at\", \"__end_at\" FROM (SELECT *, {seq} AS \"__start_at\", lead({seq}) OVER (PARTITION BY {key} ORDER BY {seq}) AS \"__end_at\" FROM \"__versions\") AS __h {shown}", keep.join(", "));
    Ok(crate::query::sql(&aux, &sql).await?.into_view())
}

/// Where expectations' failed rows are counted: a merge table, a row per view, expectation and
/// flush that had any (its key), combined as it is read.
pub const EXPECTED: &str = "pondra$expectations";

/// A view's filling (ADR-022). Its rows are its source's rows up to commit `upto`, run through
/// its SQL once (by the leader: `fill_all`, producer `fill:{view}`), and the rows every flush
/// after it derives as it is packed. `upto` is set by the sequencer, in the first commit from
/// which it holds every flush to the view (`inline`), so no row counts twice or never.
#[derive(Serialize, Deserialize, Clone, Default)]
pub struct Fill {
    pub id: String, // (this view, not an earlier one of its name)
    pub upto: Option<u64>,
}

/// A join of two streams (`join = 'streams'`): a row of either table pairs with the other's rows
/// when it arrives, and with those that arrive after it. The leader keeps it up to date right after
/// commits (`join_all`). `time` and `within_secs` bound what a new row is paired against: the
/// other table's rows at most that far from it in time (so only their files are read).
#[derive(Serialize, Deserialize, Clone)]
pub struct Join {
    pub tables: Vec<String>,
    pub time: Vec<String>, // one column both tables have, or one per table
    pub within_secs: Option<u64>,
}

/// Emit-once windows: `window` is the view's window-start column (a key), cut from the source's
/// event-time column `time`; windows are `size_secs` long and take rows up to `lateness_secs` late.
/// Sliding windows (`slide_secs`, a divisor of `size_secs`): a new window every `slide_secs`, each
/// `size_secs` long. The view then keeps `slide_secs`-long panes (its `date_bin` is the slide), and
/// each window emitted combines the panes it covers (sums and counts added, min of mins, max of
/// maxes): a row is added once, not once per window it falls in.
#[derive(Serialize, Deserialize, Clone)]
pub struct Emit {
    pub window: String,
    pub size_secs: u64,
    pub lateness_secs: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time: Option<String>, // (None in views made before round 16: found from the SQL)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub slide_secs: Option<u64>,
}

/// Session windows over the source's event-time column `time`, per value of `keys` (the view's
/// GROUP BY), rows up to `lateness_secs` late.
#[derive(Serialize, Deserialize, Clone)]
pub struct Sessions {
    pub time: String,
    pub gap_secs: u64,
    pub lateness_secs: u64,
    #[serde(default)]
    pub keys: Vec<String>,
}

pub fn view_key(name: &str) -> String { format!("v/{name}") }

impl View {
    /// Does this view follow `table`'s rows (its source, or either side of a stream join)?
    pub fn follows(&self, table: &str) -> bool { self.source == table || self.join.as_ref().is_some_and(|j| j.tables.iter().any(|t| t == table)) }
}

/// `CREATE MATERIALIZED VIEW … WITH (window = 'w', size_secs = 60, lateness_secs = 10)` (and
/// `slide_secs = 10`: sliding), `WITH (session = 'ts', gap_secs = 1800, lateness_secs = 5)`, or
/// `WITH (join = 'streams', time = 'ts', within_secs = 600)`: what `POST /views/{v}?…` takes.
/// A materialized view's options (`options`).
#[derive(Default)]
pub struct Options {
    pub emit: Option<Emit>,
    pub sessions: Option<Sessions>,
    pub join: Option<Join>,
    pub expect: Vec<Expect>,
    pub history: Option<History>,
    pub delete_when: Option<String>,
}

pub fn options(kv: &std::collections::BTreeMap<String, String>) -> Result<Options> {
    const KNOWN: [&str; 13] = ["window", "size_secs", "slide_secs", "lateness_secs", "session", "gap_secs", "join", "time", "within_secs", "expect", "history", "sequence_by", "delete_when"];
    if let Some(k) = kv.keys().find(|k| !KNOWN.contains(&k.as_str())) {
        bail!("{k}: a materialized view's options are {}", KNOWN.join(", "));
    }
    let num = |k: &str, d: u64| kv.get(k).map_or(Ok(d), |v| v.parse::<u64>().map_err(|_| anyhow::anyhow!("{k} is a number of seconds")));
    let lateness_secs = num("lateness_secs", 0)?;
    let slide_secs = kv.get("slide_secs").map(|_| num("slide_secs", 0)).transpose()?;
    let emit = kv.get("window").map(|w| Ok::<_, anyhow::Error>(Emit { window: w.clone(), size_secs: num("size_secs", 60)?, lateness_secs, time: None, slide_secs })).transpose()?;
    if let Some(e) = &emit {
        ensure!(e.slide_secs.is_none_or(|s| s > 0 && s < e.size_secs && e.size_secs % s == 0), "slide_secs: a divisor of size_secs, less than it");
    }
    ensure!(emit.is_some() || slide_secs.is_none(), "slide_secs slides a window: window = '…', size_secs = …");
    let join = match kv.get("join").map(String::as_str) {
        Some("streams") => Some(Join { tables: vec![], time: kv.get("time").map(|t| t.split(',').map(|c| c.trim().to_string()).collect()).unwrap_or_default(), within_secs: kv.get("within_secs").map(|_| num("within_secs", 0)).transpose()? }),
        Some(other) => bail!("join = '{other}': join = 'streams' pairs the rows of two tables as they arrive on either side"),
        None => None,
    };
    ensure!(join.as_ref().is_none_or(|j| j.within_secs.is_none() || !j.time.is_empty()), "within_secs needs time = '…': the column (or one per table) it bounds");
    let sessions = kv.get("session").map(|t| Ok::<_, anyhow::Error>(Sessions { time: t.clone(), gap_secs: num("gap_secs", 1800)?, lateness_secs, keys: vec![] })).transpose()?;
    let expect: Vec<Expect> = match kv.get("expect") {
        Some(e) => serde_json::from_str(e).context("expect: a JSON list of {\"name\", \"check\", \"on\": \"keep\" | \"drop\" | \"fail\"}")?,
        None => vec![],
    };
    let list = |v: &str| v.split(',').map(|c| c.trim().to_string()).filter(|c| !c.is_empty()).collect::<Vec<_>>();
    let history = kv.get("history").map(|k| Ok::<_, anyhow::Error>(History {
        key: list(k),
        sequence_by: kv.get("sequence_by").cloned().context("history = '…' keeps versions in an order: sequence_by = 'a column' (a time, or a number that grows)")?,
        deletes: kv.contains_key("delete_when"),
    })).transpose()?;
    ensure!(history.is_some() || (!kv.contains_key("sequence_by") && !kv.contains_key("delete_when")), "sequence_by and delete_when go with history = 'key columns'");
    Ok(Options { emit, sessions, join, expect, history, delete_when: kv.get("delete_when").cloned() })
}
/// `CREATE MATERIALIZED VIEW v (CONSTRAINT c CHECK (…) [ON VIOLATION DROP ROW | FAIL], …) AS …`:
/// the statement without its expectations, and them (`EXPECT (…)` too, Databricks' word; a column
/// named in the list stays). None: no such list.
pub fn constraints(sql: &str) -> Result<Option<(String, Vec<Expect>)>> {
    use datafusion::sql::sqlparser::{dialect::GenericDialect, tokenizer::{Token, Tokenizer}};
    let head = sql.trim_start().chars().take(64).collect::<String>().to_uppercase();
    if !head.starts_with("CREATE") || !head.contains("MATERIALIZED") {
        return Ok(None); // (every other statement, as fast as before)
    }
    let Ok(tokens) = Tokenizer::new(&GenericDialect {}, sql).tokenize() else { return Ok(None) };
    let solid: Vec<usize> = (0..tokens.len()).filter(|&i| !matches!(tokens[i], Token::Whitespace(_))).collect();
    let word = |i: usize| match solid.get(i).map(|&j| &tokens[j]) {
        Some(Token::Word(w)) if w.quote_style.is_none() => w.value.to_uppercase(),
        _ => String::new(),
    };
    let is = |i: usize, t: Token| solid.get(i).is_some_and(|&j| tokens[j] == t);
    let mut i = 0;
    for w in ["CREATE", "OR", "REPLACE", "MATERIALIZED", "VIEW", "IF", "NOT", "EXISTS"] {
        match (w, word(i) == w) {
            (_, true) => i += 1,
            ("OR" | "REPLACE" | "IF" | "NOT" | "EXISTS", false) => {}
            _ => return Ok(None),
        }
    }
    i += 1; // (the name, in parts)
    while is(i, Token::Period) {
        i += 2;
    }
    if !is(i, Token::LParen) {
        return Ok(None);
    }
    let (open, mut items, mut item, mut depth) = (i, vec![], vec![], 0);
    let close = loop {
        i += 1;
        let Some(&j) = solid.get(i) else { return Ok(None) };
        match &tokens[j] {
            Token::LParen => depth += 1,
            Token::RParen if depth == 0 => break i,
            Token::RParen => depth -= 1,
            Token::Comma if depth == 0 => {
                items.push(std::mem::take(&mut item));
                continue;
            }
            _ => {}
        }
        item.push(i);
    };
    items.push(item);
    let text = |a: usize, b: usize| tokens[solid[a]..=solid[b]].iter().map(|t| t.to_string()).collect::<String>();
    let (mut columns, mut expect) = (vec![], vec![]);
    for it in items.into_iter().filter(|it| !it.is_empty()) {
        let (mut k, mut name) = (it[0], None);
        if word(k) == "CONSTRAINT" {
            name = Some(match solid.get(k + 1).map(|&j| &tokens[j]) {
                Some(Token::Word(w)) if w.quote_style.is_none() => w.value.to_lowercase(),
                Some(Token::Word(w)) => w.value.clone(),
                _ => bail!("CONSTRAINT <name> CHECK (…)"),
            });
            k += 2;
        }
        let on = match word(k).as_str() {
            "CHECK" => OnViolation::Fail,
            "EXPECT" => OnViolation::Keep,
            _ if name.is_none() => {
                columns.push(text(it[0], *it.last().unwrap_or(&it[0])));
                continue;
            }
            _ => bail!("CONSTRAINT {}: CHECK (…) or EXPECT (…), then ON VIOLATION DROP ROW or FAIL if you like", name.unwrap_or_default()),
        };
        ensure!(is(k + 1, Token::LParen), "{} (…): the condition in parentheses", word(k));
        let (start, mut end, mut d) = (k + 1, k + 1, 0);
        while end <= *it.last().unwrap_or(&end) {
            match &tokens[solid[end]] {
                Token::LParen => d += 1,
                Token::RParen if d == 1 => break,
                Token::RParen => d -= 1,
                _ => {}
            }
            end += 1;
        }
        ensure!(end > start + 1 && is(end, Token::RParen), "{} (…): a condition", word(k));
        let rest: Vec<String> = (end + 1..=*it.last().unwrap_or(&end)).map(word).collect();
        let on = match rest.iter().map(String::as_str).collect::<Vec<_>>()[..] {
            [] => on,
            ["ON", "VIOLATION", "DROP", "ROW"] => OnViolation::Drop,
            ["ON", "VIOLATION", "FAIL"] | ["ON", "VIOLATION", "FAIL", "UPDATE"] => OnViolation::Fail,
            _ => bail!("after {} (…): ON VIOLATION DROP ROW, or ON VIOLATION FAIL", word(k)),
        };
        let check = text(start + 1, end - 1).trim().to_string();
        expect.push(Expect { name: name.unwrap_or_else(|| format!("expectation_{}", expect.len() + 1)), check, on });
    }
    if expect.is_empty() {
        return Ok(None);
    }
    let list = if columns.is_empty() { String::new() } else { format!(" ({})", columns.join(", ")) };
    let rest = tokens[solid[close] + 1..].iter().map(|t| t.to_string()).collect::<String>();
    Ok(Some((format!("{}{list}{rest}", text(0, open - 1)), expect)))
}

/// A session view's bound: no session still open starts before this (µs).
fn open_key(name: &str) -> String { format!("w/{name}") }

/// Register view `name` (leader only): its table gets the query's output columns; a GROUP BY
/// query makes it a merge table keyed by the group columns.
pub async fn create(lake: &Lake, name: &str, sql: &str, o: Options) -> Result<()> {
    let Options { mut emit, sessions, join, expect, history, delete_when } = o;
    // (a history view's deletes: a column of its rows saying which versions end their key)
    let wrapped;
    let sql = match &delete_when {
        Some(w) => {
            wrapped = format!("SELECT *, coalesce(({w}), false) AS \"__delete\" FROM ({sql}) AS __history");
            wrapped.as_str()
        }
        None => sql,
    };
    if let Some(v) = lake.cat.get::<View>(&view_key(name)).await? {
        let windows = |e: &Option<Emit>| e.as_ref().map(|e| (e.window.clone(), e.size_secs, e.lateness_secs, e.slide_secs));
        let gaps = |s: &Option<Sessions>| s.as_ref().map(|s| (s.time.clone(), s.gap_secs, s.lateness_secs));
        let joins = |j: &Option<Join>| j.as_ref().map(|j| (j.time.clone(), j.within_secs));
        let same = v.sql == sql && windows(&v.emit) == windows(&emit) && gaps(&v.sessions) == gaps(&sessions) && joins(&v.join) == joins(&join) && v.expect == expect;
        ensure!(same, "view {name} already exists, with other SQL or options");
        return Ok(()); // (asked again, the same: a notebook cell run twice)
    }
    ensure!(lake.cat.get::<TableMeta>(&table_key(name)).await?.is_none(), "table {name} already exists");
    let (other, source) = crate::ddl::resolve(lake, &first_table(sql)?).await?;
    ensure!(other.is_none(), "a view follows a table of this lake");
    let src: TableMeta = lake.cat.get::<TableMeta>(&table_key(&source)).await?.with_context(|| format!("no table {source}"))?.logical(); // (SQL's names: ADR-022)
    // A view of a view: a flow (ADR-036 §1's pipeline). Its rows are derived from the other's in the
    // same flush, so in the same commit (`derive`). A GROUP BY view's table keeps partial rows,
    // combined as they are read: what follows it must combine them too (`merges`: a rollup).
    let upstream = lake.cat.get::<View>(&view_key(&source)).await?;
    let partial = upstream.is_some() && !src.merge.is_empty();
    ensure!(src.history.is_none(), "{source} is a history view, whose __start_at and __end_at are worked out as it is read: make {name} a stored view of it (CREATE VIEW {name} AS SELECT … FROM {source})");
    ensure!(history.is_none() || (sessions.is_none() && join.is_none() && emit.is_none()), "a history view keeps each version as it comes: not with windows, sessions or a stream join");
    ensure!(expect.is_empty() || (sessions.is_none() && join.is_none() && emit.is_none()), "expectations check a view's rows as it writes them: a view that emits windows or sessions, or joins streams, writes them later. Put them on a view before it");
    if let Some(s) = sessions {
        ensure!(emit.is_none() && join.is_none(), "a view emits windows or sessions, or joins streams: one of them");
        ensure!(!partial, "{source} is a GROUP BY view, whose table keeps partial rows: sessions are cut from rows, so make {name} from {source}'s own source");
        return create_sessions(lake, name, sql, source, &src, s).await;
    }
    if let Some(j) = join {
        ensure!(emit.is_none(), "a view emits windows or joins streams, not both");
        ensure!(!partial, "{source} is a GROUP BY view, whose table keeps partial rows: streams are joined row by row, so make {name} from {source}'s own source");
        return create_join(lake, name, sql, source, j).await;
    }
    let planned = crate::asof::rewrite(sql)?;
    let plan = crate::query::sql(&session(lake, &planned, "").await?, &planned).await?.logical_plan().clone();
    let (key, merge) = merges(&plan, partial.then_some((source.as_str(), &src)))?;
    let columns: Vec<(String, String)> = plan.schema().fields().iter().map(|f| (f.name().clone(), crate::query::type_name(f.data_type()))).collect();
    if let Some((c, _)) = columns.iter().find(|(c, _)| crate::sys::NAMES.contains(&c.as_str())) {
        bail!("{c} is a system column of the view's own table: name it something else ({c} AS source{c})");
    }
    // (row by row over a table, or over a view that is: then a row keeps its first source row's id)
    let ids = merge.is_empty() && alone(&plan, false) && upstream.as_ref().is_none_or(|u| u.ids);
    ensure!(expect.is_empty() || merge.is_empty(), "{name} is a GROUP BY view: its table keeps partial rows, so expectations can't check its totals. Put them on the rows before it (a view of {source} without GROUP BY, which {name} then follows)");
    if let Some(h) = &history {
        ensure!(merge.is_empty(), "a history view keeps every version of each key as it comes: no GROUP BY");
        let has = |c: &str| columns.iter().any(|(n, _)| n == c);
        if let Some(c) = h.key.iter().chain([&h.sequence_by]).find(|c| !has(c)) {
            bail!("{c}: history = '{}' and sequence_by = '{}' name columns the view's query gives", h.key.join(", "), h.sequence_by);
        }
        ensure!(!has("__start_at") && !has("__end_at"), "__start_at and __end_at are a history view's own columns: name yours something else");
    }
    let meta = TableMeta { columns, key, merge, publish: default_publish(), ids: true, tiered: lake.visible(), history, ..Default::default() };
    if !expect.is_empty() {
        expectations(lake, name, &source, &planned, &meta, &expect).await?;
    }
    let mut puts = vec![(table_key(name), json(&meta))];
    if !expect.is_empty() && lake.cat.get::<TableMeta>(&table_key(EXPECTED)).await?.is_none() {
        let s = |c: &str| (c.to_string(), "Utf8".to_string());
        let columns = vec![s("view"), s("id"), s("expectation"), s("action"), ("failed".into(), "Int64".into())];
        let key = ["view", "id", "expectation", "action"].map(String::from).to_vec();
        puts.push((table_key(EXPECTED), json(&TableMeta { columns, key, merge: [("failed".to_string(), "sum".to_string())].into(), tiered: lake.visible(), ..Default::default() })));
    }
    if let Some(e) = &mut emit {
        let is_time = meta.columns.iter().any(|(c, t)| *c == e.window && t.starts_with("Timestamp"));
        ensure!(meta.key.contains(&e.window) && is_time, "emit: the window column must be a GROUP BY timestamp (date_bin(…) AS {})", e.window);
        e.time = event_time(&plan, &e.window).filter(|t| timestamp(&src, t));
        ensure!(e.time.is_some(), "emit: the window must be cut from a timestamp column of {source}: date_bin(INTERVAL '1 minute', ts) AS {}", e.window);
        let columns = meta.columns.iter().filter(|(c, _)| c != "_deleted").cloned().collect();
        puts.push((table_key(&format!("{name}_final")), json(&TableMeta { columns, publish: default_publish(), ids: true, tiered: lake.visible(), ..Default::default() })));
    }
    let fill = Some(Fill { id: uuid::Uuid::new_v4().to_string(), upto: None }); // (from the rows already there)
    puts.push((view_key(name), json(&View { source, sql: sql.into(), emit, sessions: None, ids, join: None, fill, expect })));
    lake.cat.commit(puts, &[]).await
}

/// A new view's expectations: each a condition over its columns; and one that fails a write
/// (`FAIL`) refuses the view if rows already there break it (they would fill it).
async fn expectations(lake: &Lake, name: &str, source: &str, planned: &str, meta: &TableMeta, expect: &[Expect]) -> Result<()> {
    let empty = RecordBatch::new_empty(crate::query::schema(&meta.columns)?);
    let mut names = std::collections::HashSet::new();
    for e in expect {
        ensure!(names.insert(e.name.to_lowercase()), "{name} has two expectations named {}", e.name);
        crate::defaults::breaking(&empty, &e.check).with_context(|| format!("expectation {} of {name}: CHECK ({})", e.name, e.check))?;
        if e.on == OnViolation::Fail {
            let sql = format!("SELECT count(*) AS n FROM ({planned}) AS _v WHERE NOT ({})", e.check);
            let rows = crate::query::sql(&session(lake, &sql, "").await?, &sql).await?.collect().await?;
            let n = rows.first().and_then(|b| b.column(0).as_any().downcast_ref::<datafusion::arrow::array::Int64Array>()).map_or(0, |c| c.value(0));
            ensure!(n == 0, "{n} of the rows {source} has now would break expectation {} of {name}, CHECK ({}): fix them first, or make it ON VIOLATION DROP ROW", e.name, e.check);
        }
    }
    Ok(())
}

/// A row an expectation or a table's CHECK refuses (Postgres's `check_violation`, 23514).
#[derive(Debug)]
pub struct Violation(pub String);

impl std::fmt::Display for Violation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { f.write_str(&self.0) }
}

impl std::error::Error for Violation {}

/// Was this error a row refused by a CHECK or an expectation?
pub fn refused(e: &anyhow::Error) -> bool { e.chain().any(|c| c.is::<Violation>()) }

/// A row-by-row view's rows taken back (`{view}$deleted`), less those its expectations never let
/// in: a view that adds them up must not take back what it never had.
fn let_in(v: &View, rows: RecordBatch) -> Result<RecordBatch> {
    let mut keep = vec![true; rows.num_rows()];
    for e in v.expect.iter().filter(|e| e.on != OnViolation::Keep) {
        let bad = crate::defaults::breaking(&rows, &e.check)?;
        keep.iter_mut().zip(bad.values().iter()).for_each(|(k, b)| *k &= !b);
    }
    Ok(match keep.iter().all(|k| *k) {
        true => rows,
        false => datafusion::arrow::compute::filter_record_batch(&rows, &keep.into())?,
    })
}

/// A view's new rows through its expectations: the rows it keeps, and a row of
/// `pondra$expectations` per expectation some broke. A `FAIL` one refuses the write (`filling`:
/// rows that were there when the view was made can't be refused any more: they are dropped).
fn expected(view: &str, v: &View, rows: RecordBatch, filling: bool) -> Result<(RecordBatch, Option<RecordBatch>)> {
    use datafusion::arrow::array::{Int64Array, StringArray};
    if v.expect.is_empty() || rows.num_rows() == 0 {
        return Ok((rows, None));
    }
    let (mut keep, mut failed) = (vec![true; rows.num_rows()], vec![]);
    for e in &v.expect {
        let bad = crate::defaults::breaking(&rows, &e.check)?;
        let n = bad.true_count();
        if n == 0 {
            continue;
        }
        if e.on == OnViolation::Fail && !filling {
            return Err(anyhow::Error::new(Violation(format!("new row for relation \"{view}\" violates check constraint \"{}\": CHECK ({})", e.name, e.check))));
        }
        if e.on != OnViolation::Keep {
            keep.iter_mut().zip(bad.values().iter()).for_each(|(k, b)| *k &= !b);
        }
        failed.push((e, n as i64));
    }
    if failed.is_empty() {
        return Ok((rows, None));
    }
    let text = |f: &dyn Fn(&(&Expect, i64)) -> String| std::sync::Arc::new(failed.iter().map(f).map(Some).collect::<StringArray>()) as datafusion::arrow::array::ArrayRef;
    let id = v.fill.as_ref().map_or("", |f| f.id.as_str());
    let counts = RecordBatch::try_from_iter(vec![
        ("view", text(&|_| view.to_string())),
        ("id", text(&|_| id.to_string())),
        ("expectation", text(&|(e, _)| e.name.clone())),
        ("action", text(&|(e, _)| e.on.word().to_string())),
        ("failed", std::sync::Arc::new(failed.iter().map(|(_, n)| *n).collect::<Int64Array>())),
    ])?;
    let kept = datafusion::arrow::compute::filter_record_batch(&rows, &keep.into())?;
    Ok((kept, Some(counts)))
}

/// A session view: the SQL runs over each closed session's rows, grouped by session too, so its
/// table gets the SQL's columns and `session_start`, `session_end`.
async fn create_sessions(lake: &Lake, name: &str, sql: &str, source: String, src: &TableMeta, mut s: Sessions) -> Result<()> {
    ensure!(timestamp(src, &s.time), "sessions: {} is not a timestamp column of {source}", s.time);
    ensure!(s.gap_secs > 0, "sessions: the gap must be at least a second");
    let with = sessionized(sql)?;
    let ctx = crate::query::over_ctx(lake, &source, extended(src, &s.time)?, vec![], &with).await?;
    let plan = crate::query::sql(&ctx, &with).await?.logical_plan().clone();
    let Some(LogicalPlan::Aggregate(agg)) = top_aggregate(&plan) else { bail!("a session view is SELECT … FROM {source} GROUP BY <its key columns>") };
    for g in &agg.group_expr {
        match g {
            Expr::Column(c) if c.name == "session_start" || c.name == "session_end" => {}
            Expr::Column(c) if src.columns.iter().any(|(n, _)| *n == c.name) => s.keys.push(c.name.clone()),
            other => bail!("a session view groups by columns of {source}, not {other}"),
        }
    }
    ensure!(!s.keys.is_empty(), "a session view groups by a key: GROUP BY user");
    let out = plan.schema();
    ensure!(s.keys.iter().all(|k| out.field_with_unqualified_name(k).is_ok()), "a session view SELECTs its GROUP BY columns, as they are named");
    let columns = out.fields().iter().map(|f| (f.name().clone(), crate::query::type_name(f.data_type()))).collect();
    let meta = TableMeta { columns, publish: default_publish(), ids: true, tiered: lake.visible(), ..Default::default() };
    let view = View { source, sql: sql.into(), emit: None, sessions: Some(s), ids: false, join: None, fill: None, expect: vec![] };
    lake.cat.commit(vec![(view_key(name), json(&view)), (table_key(name), json(&meta))], &[]).await
}

fn timestamp(meta: &TableMeta, column: &str) -> bool { meta.columns.iter().any(|(c, t)| c == column && t.starts_with("Timestamp")) }

/// The source's columns and the two a session view's SQL also sees: `session_start`, `session_end`.
fn extended(src: &TableMeta, time: &str) -> Result<datafusion::arrow::datatypes::SchemaRef> {
    use datafusion::arrow::datatypes::{Field, Schema};
    let s = crate::query::schema(&src.columns)?;
    let t = s.field_with_name(time)?.data_type().clone();
    let mut fields = s.fields().to_vec();
    fields.extend(["session_start", "session_end"].map(|n| std::sync::Arc::new(Field::new(n, t.clone(), true))));
    Ok(std::sync::Arc::new(Schema::new(fields)))
}

/// A session view's SQL grouped by session too: `session_start` and `session_end` join its
/// SELECT and its GROUP BY (where it doesn't name them itself).
fn sessionized(sql: &str) -> Result<String> {
    use datafusion::sql::sqlparser::{ast, dialect::GenericDialect, parser::Parser};
    let mut stmts = Parser::parse_sql(&GenericDialect {}, sql)?;
    let plain = "a session view is one SELECT … GROUP BY <its key columns>";
    let [ast::Statement::Query(q)] = &mut stmts[..] else { bail!(plain) };
    let ast::SetExpr::Select(s) = q.body.as_mut() else { bail!(plain) };
    let ast::GroupByExpr::Expressions(by, _) = &mut s.group_by else { bail!(plain) };
    for c in ["session_start", "session_end"] {
        let e = ast::Expr::Identifier(ast::Ident::new(c));
        if !s.projection.iter().any(|p| p.to_string() == c) {
            s.projection.push(ast::SelectItem::UnnamedExpr(e.clone()));
        }
        if !by.iter().any(|b| b.to_string() == c) {
            by.push(e);
        }
    }
    Ok(stmts[0].to_string())
}

/// The source column a window column is cut from: the one column its GROUP BY expression reads.
fn event_time(plan: &LogicalPlan, window: &str) -> Option<String> {
    let LogicalPlan::Projection(p) = plan else { return None };
    let LogicalPlan::Aggregate(a) = p.input.as_ref() else { return None };
    let i = plan.schema().fields().iter().position(|f| f.name() == window)?;
    let Expr::Column(c) = p.expr[i].clone().unalias_nested().data else { return None };
    let columns = a.group_expr.get(a.schema.index_of_column(&c).ok()?)?.column_refs();
    let [c] = columns.into_iter().collect::<Vec<_>>()[..] else { return None };
    Some(c.name.clone())
}

/// Leader: emit what the watermark has passed, once, for every window and session view (one
/// that fails is tried again next round; the others go on).
pub async fn emit_all(lake: &Lake, log: &crate::log::Log) -> Result<()> {
    for (key, v) in lake.cat.scan::<View>("v/", "v0").await? {
        let done = match (&v.emit, &v.sessions) {
            (Some(e), _) => emit(lake, log, &key[2..], &v, e).await,
            (_, Some(s)) => sessions(lake, log, &key[2..], &v, s).await,
            _ => Ok(()),
        };
        if let Err(e) = done {
            eprintln!("emission of {}: {e:#}", &key[2..]);
        }
    }
    Ok(())
}

/// The newest event time `table` holds in `time`, in µs: what its files' ranges say, then each
/// log row after them, read once (kept in memory per table; it only grows, as a watermark does).
async fn newest(lake: &Lake, table: &str, time: &str) -> Result<Option<i64>> {
    use datafusion::arrow::{array::AsArray, compute::{cast, max}, datatypes::{DataType, TimeUnit, TimestampMicrosecondType}};
    use datafusion::common::ScalarValue;
    use std::sync::{LazyLock, Mutex};
    static SEEN: LazyLock<Mutex<std::collections::HashMap<String, (u64, Option<i64>)>>> = LazyLock::new(Default::default);
    let key = format!("{}|{table}|{time}", lake.url);
    let meta: TableMeta = lake.cat.get(&table_key(table)).await?.with_context(|| format!("no table {table}"))?;
    let stored = meta.stored(time).unwrap_or(time).to_string(); // (files' ranges know it by its stored name: ADR-022)
    let upto = lake.visible();
    let (mut seen, mut newest) = SEEN.lock().unwrap().get(&key).copied().unwrap_or((0, None));
    let us = DataType::Timestamp(TimeUnit::Microsecond, None);
    if seen < meta.tiered {
        // (rows went into files since: their ranges say how new they were)
        let s = crate::query::schema(&meta.columns)?;
        let ranges = crate::manifest::ranges(table, &crate::manifest::list(lake, &meta).await?, &meta.files, &s);
        if let Some((_, hi)) = ranges.get(&stored) {
            if let ScalarValue::TimestampMicrosecond(v, _) = ScalarValue::try_from_string(hi.clone(), s.field_with_name(&stored)?.data_type())?.cast_to(&us)? {
                newest = newest.max(v);
            }
        }
        seen = meta.tiered;
    }
    if upto > seen {
        for b in crate::query::tail(lake, table, seen, Some(upto), false).await? {
            if let Some(c) = b.column_by_name(time) {
                newest = newest.max(max(cast(c, &us)?.as_primitive::<TimestampMicrosecondType>()));
            }
        }
        seen = upto;
    }
    SEEN.lock().unwrap().insert(key, (seen, newest));
    Ok(newest)
}

/// A timestamp column's values in µs.
fn micros(b: &RecordBatch, column: &str) -> Result<datafusion::arrow::array::TimestampMicrosecondArray> {
    use datafusion::arrow::{array::AsArray, compute::cast, datatypes::{DataType, TimeUnit, TimestampMicrosecondType}};
    let c = b.column_by_name(column).with_context(|| format!("no column {column}"))?;
    Ok(cast(c, &DataType::Timestamp(TimeUnit::Microsecond, None))?.as_primitive::<TimestampMicrosecondType>().clone())
}

fn quoted(c: &str) -> String { format!("\"{}\"", c.replace('"', "\"\"")) }

/// The windows of `view` now past the watermark, appended to `{view}_final` with the watermark
/// as the producer's seq (`prev`: the last one), so each window is emitted exactly once.
async fn emit(lake: &Lake, log: &crate::log::Log, view: &str, v: &View, e: &Emit) -> Result<()> {
    let time = match &e.time {
        Some(t) => t.clone(),
        None => event_time(crate::query::sql(&session(lake, &v.sql, "").await?, &crate::asof::rewrite(&v.sql)?).await?.logical_plan(), &e.window).context("a window view whose window has no event-time column")?,
    };
    let Some(newest) = newest(lake, &v.source, &time).await? else { return Ok(()) };
    let upto = newest - ((e.size_secs + e.lateness_secs) * 1_000_000) as i64; // windows starting at or before this have ended
    let (producer, final_table) = (format!("emit:{view}"), format!("{view}_final"));
    let done: u64 = lake.cat.get(&producer_key(&producer)).await?.unwrap_or(0);
    if upto <= done as i64 {
        return Ok(());
    }
    let w = quoted(&e.window);
    let sql = match e.slide_secs {
        None => format!("SELECT * FROM {} WHERE {w} > to_timestamp_micros({done}) AND {w} <= to_timestamp_micros({upto}) ORDER BY {w}", quoted(view)),
        // Each window (starting on a pane) is the panes it covers: every pane, shifted back by
        // each offset a window can start before it, grouped by where that window starts.
        Some(slide) => {
            let meta: TableMeta = lake.cat.get(&table_key(view)).await?.with_context(|| format!("no table {view}"))?;
            let offsets = (0..e.size_secs / slide).map(|i| format!("(INTERVAL '{} seconds')", i * slide)).collect::<Vec<_>>().join(", ");
            let cols = meta.columns.iter().filter(|(c, _)| c != "_deleted").map(|(c, _)| match (c == &e.window, meta.merge.get(c).map(String::as_str)) {
                (true, _) => format!("{w} - __o AS {w}"),
                (_, Some("count" | "sum")) => format!("sum({0}) AS {0}", quoted(c)),
                (_, Some(f)) => format!("{f}({0}) AS {0}", quoted(c)),
                (_, None) => quoted(c),
            });
            let keys = meta.key.iter().map(|k| if k == &e.window { format!("{w} - __o") } else { quoted(k) }).collect::<Vec<_>>().join(", ");
            format!("SELECT {} FROM {} CROSS JOIN (VALUES {offsets}) AS __offsets(__o) WHERE {w} - __o > to_timestamp_micros({done}) AND {w} - __o <= to_timestamp_micros({upto}) GROUP BY {keys} ORDER BY 1",
                    cols.collect::<Vec<_>>().join(", "), quoted(view))
        }
    };
    let rows = crate::query::sql(&session(lake, &sql, "").await?, &sql).await?.collect().await?;
    append(lake, log, &final_table, crate::log::Src { producer, seq: upto as u64, prev: Some(done) }, rows).await
}

/// `rows` into `table` (as its columns), with `src`: output and progress commit together. (A
/// conflict: another leader emitted first; the next round catches up.)
async fn append(lake: &Lake, log: &crate::log::Log, table: &str, src: crate::log::Src, rows: Vec<RecordBatch>) -> Result<()> {
    let meta: TableMeta = lake.cat.get(&table_key(table)).await?.with_context(|| format!("no table {table}"))?;
    let s = crate::query::schema(&meta.columns)?;
    let rows = datafusion::arrow::compute::concat_batches(&s, &rows.iter().map(|b| crate::query::conform(b, &s)).collect::<Result<Vec<_>>>()?)?;
    log.append(table.to_string(), src, rows).await?;
    Ok(())
}

/// The sessions of `view` the watermark has passed: each key's rows cut where a gap of
/// `gap_secs` falls, over the rows that may still be in a session not yet emitted (from the
/// earliest start of those still open), leaving out rows inside a session already emitted. The
/// closed ones run through the view's SQL and are appended to its table, with the watermark as
/// the producer's seq, as windows are.
async fn sessions(lake: &Lake, log: &crate::log::Log, view: &str, v: &View, s: &Sessions) -> Result<()> {
    let Some(newest) = newest(lake, &v.source, &s.time).await? else { return Ok(()) };
    let wm = newest - (s.lateness_secs * 1_000_000) as i64; // (rows at or before it are late)
    let producer = format!("emit:{view}");
    let done: u64 = lake.cat.get(&producer_key(&producer)).await?.unwrap_or(0);
    if wm <= done as i64 {
        return Ok(());
    }
    let from: Option<i64> = lake.cat.get(&open_key(view)).await?;
    let (t, by, at) = (quoted(&s.time), s.keys.iter().map(|k| quoted(k)).collect::<Vec<_>>().join(", "), |us: i64| format!("to_timestamp_micros({us})"));
    let on = s.keys.iter().map(|k| format!("e.{0} IS NOT DISTINCT FROM l.{0}", quoted(k))).collect::<Vec<_>>().join(" AND ");
    let gap = format!("INTERVAL '{} seconds'", s.gap_secs);
    let (recent, since) = from.map_or_else(Default::default, |f| (format!(" WHERE session_end > {}", at(f)), format!(" AND e.{t} >= {}", at(f))));
    let sql = format!(
        "WITH _last AS (SELECT {by}, max(session_end) AS _end FROM {view_}{recent} GROUP BY {by}), \
         _rows AS (SELECT e.* FROM {source} e LEFT JOIN _last l ON {on} WHERE e.{t} <= {wm}{since} AND (l._end IS NULL OR e.{t} >= l._end)), \
         _gaps AS (SELECT *, CASE WHEN lag({t}) OVER (PARTITION BY {by} ORDER BY {t}) > {t} - {gap} THEN 0 ELSE 1 END AS _new FROM _rows), \
         _ids AS (SELECT *, sum(_new) OVER (PARTITION BY {by} ORDER BY {t} ROWS UNBOUNDED PRECEDING) AS _sid FROM _gaps) \
         SELECT * EXCLUDE (_new, _sid), min({t}) OVER (PARTITION BY {by}, _sid) AS session_start, max({t}) OVER (PARTITION BY {by}, _sid) + {gap} AS session_end FROM _ids",
        view_ = quoted(view), source = quoted(&v.source), wm = at(wm),
    );
    let rows = crate::query::sql(&session(lake, &sql, "").await?, &sql).await?.collect().await?;
    let (mut closed, mut open) = (vec![], wm);
    for b in &rows {
        let (start, end) = (micros(b, "session_start")?, micros(b, "session_end")?);
        let keep: datafusion::arrow::array::BooleanArray = end.iter().map(|e| e.map(|e| e > done as i64 && e <= wm)).collect();
        open = start.iter().zip(end.iter()).filter_map(|(s, e)| s.filter(|_| e.is_some_and(|e| e > wm))).fold(open, i64::min);
        closed.push(datafusion::arrow::compute::filter_record_batch(b, &keep)?);
    }
    let src: TableMeta = lake.cat.get::<TableMeta>(&table_key(&v.source)).await?.context("a session view without its source")?.logical();
    let with = sessionized(&v.sql)?;
    let out = crate::query::sql(&crate::query::over_ctx(lake, &v.source, extended(&src, &s.time)?, closed, &with).await?, &with).await?.collect().await?;
    append(lake, log, view, crate::log::Src { producer, seq: wm as u64, prev: Some(done) }, out).await?;
    lake.cat.commit(vec![(open_key(view), json(&open))], &[]).await // (a lower bound: stale is safe, only slower)
}

/// Can what follows `table` take its rows' changes (UPDATE, DELETE, MERGE)? A view over the table
/// alone takes the old rows back: one that adds up (sum and count, with a count to drop the groups
/// a change empties) subtracts them; one that is row by row (`View::ids`) drops their rows. Views
/// that emit windows or sessions once, keep a min or max, or read other tables too (which a change
/// would find as they are now, not as they were), and streaming tasks can't, so a change is
/// refused while they follow the table. (`append`: an append table's; a keyed table's changes are
/// upserts, which its followers already see as new rows.)
pub async fn can_follow(lake: &Lake, table: &str, append: bool) -> Result<()> {
    let not = cannot_follow(lake, table, append).await?;
    ensure!(not.is_empty(), "{table}'s rows can't change while these follow it: {}. Drop them first, or change a copy of the table", not.join("; "));
    Ok(())
}

/// What follows `table` and can't take its rows' changes, and why: its views, what follows them
/// (a flow: a row-by-row view's rows change as its source's do; a GROUP BY view's partial rows
/// taken back arrive as new ones), and its tasks.
async fn cannot_follow(lake: &Lake, table: &str, append: bool) -> Result<Vec<String>> {
    let mut not = vec![];
    for (k, v) in lake.cat.scan::<View>("v/", "v0").await?.into_iter().filter(|(_, v)| v.follows(table)) {
        let name = &k[2..];
        let meta: TableMeta = lake.cat.get(&table_key(name)).await?.context("view without table")?;
        let why = match () {
            _ if v.emit.is_some() || v.sessions.is_some() => Some("emits windows or sessions once"),
            _ if v.join.is_some() => Some("pairs two streams' rows as they arrive"),
            _ if !append => None,
            _ if meta.merge.is_empty() && !v.ids => Some("isn't row by row over the table alone (a join, DISTINCT, …), or was made before Pondra 0.19"),
            _ if meta.merge.is_empty() => None,
            _ if !alone(&plan(lake, &v).await?, true) => Some("reads other tables too"),
            _ if meta.merge.values().any(|f| f != "sum" && f != "count") => Some("keeps a min or max"),
            _ if !meta.merge.values().any(|f| f == "count") => Some("has no count(*) to drop the groups a change empties"),
            _ if meta.columns.iter().any(|(c, t)| meta.merge.contains_key(c) && t.starts_with("UInt")) => Some("adds up an unsigned column (it can't subtract)"),
            _ => None,
        };
        match why {
            Some(w) => not.push(format!("view {name} ({w})")),
            None => not.extend(Box::pin(cannot_follow(lake, name, meta.merge.is_empty())).await?),
        }
    }
    for (k, t) in lake.cat.scan::<crate::tasks::Task>("k/", "k0").await? {
        if t.source == table {
            not.push(format!("task {} (streaming tasks see new rows only)", &k[2..]));
        }
    }
    Ok(not)
}

/// The row-by-row views of `table` (`View::ids`), and theirs, with their tables: a change of its
/// rows changes theirs.
pub async fn row_views(lake: &Lake, table: &str) -> Result<Vec<(String, TableMeta)>> {
    let all = lake.cat.scan::<View>("v/", "v0").await?;
    let (mut out, mut next) = (vec![], vec![table.to_string()]);
    while let Some(t) = next.pop() {
        for (k, _) in all.iter().filter(|(_, v)| v.source == t && v.ids) {
            out.push((k[2..].to_string(), lake.cat.get(&table_key(&k[2..])).await?.context("view without table")?));
            next.push(k[2..].to_string());
        }
    }
    Ok(out)
}

async fn plan(lake: &Lake, v: &View) -> Result<LogicalPlan> {
    let planned = crate::asof::rewrite(&v.sql)?;
    Ok(crate::query::sql(&session(lake, &planned, "").await?, &planned).await?.into_unoptimized_plan())
}

/// Does a view read its source alone, row by row: projections and filters over one table, no
/// subquery (`grouped`: under a GROUP BY too)? Only then can it take a changed row back, since
/// anything else it read would be as it is when the change comes, not as it was.
fn alone(p: &LogicalPlan, grouped: bool) -> bool {
    use datafusion::common::tree_node::TreeNode;
    use datafusion::logical_expr::LogicalPlan::*;
    let node = matches!(p, Projection(_) | Filter(_) | SubqueryAlias(_) | TableScan(_)) || (grouped && matches!(p, Aggregate(_)));
    let subquery = |e: &Expr| e.exists(|e| Ok(matches!(e, Expr::ScalarSubquery(_) | Expr::Exists(_) | Expr::InSubquery(_)))).unwrap_or(true);
    node && !p.expressions().iter().any(subquery) && p.inputs().len() <= 1 && p.inputs().iter().all(|i| alone(i, grouped))
}

/// The rows every view derives from a flush's new rows, per view table. A change's old rows
/// (`{source}$deleted`, `change.rs`) are taken back: subtracted from a view that adds up, and, for
/// a row-by-row view, its rows of them go to `{view}$deleted` (whose rows reads leave out).
/// A view of a view takes the rows that view derives here (ADR-036 §1: a flow, bronze → silver
/// → gold, in one flush, so in one commit): views go after the views they follow (`in_order`).
/// Expectations keep, drop or refuse each view's new rows (`expected`), counted in
/// `pondra$expectations`.
pub async fn derive(lake: &Lake, new: &BTreeMap<String, Vec<RecordBatch>>) -> Result<Vec<(String, RecordBatch)>> {
    let (mut out, mut made) = (vec![], BTreeMap::<String, Vec<RecordBatch>>::new());
    for (target, v) in in_order(lake.cat.scan::<View>("v/", "v0").await?) {
        if v.sessions.is_some() || v.join.is_some() {
            continue; // (sessions are cut as they close; stream joins run once rows commit)
        }
        let gone_from = crate::sys::deleted(&v.source);
        let rows: Vec<&Vec<RecordBatch>> = [new.get(&v.source), made.get(&v.source)].into_iter().flatten().collect();
        let gone: Vec<&Vec<RecordBatch>> = [new.get(&gone_from), made.get(&gone_from)].into_iter().flatten().collect();
        if rows.is_empty() && gone.is_empty() {
            continue;
        }
        let meta: TableMeta = lake.cat.get(&table_key(&target)).await?.context("view without table")?;
        let mut parts = vec![];
        for rows in rows {
            let (kept, failed) = expected(&target, &v, view_rows(lake, &v, &meta, rows, false).await?, false)?;
            parts.push((target.clone(), kept));
            parts.extend(failed.map(|f| (EXPECTED.to_string(), f)));
        }
        for gone in gone {
            match () {
                _ if !meta.merge.is_empty() => parts.push((target.clone(), negated(&view_rows(lake, &v, &meta, gone, false).await?, &meta)?)),
                _ if v.ids => parts.push((crate::sys::deleted(&target), let_in(&v, view_rows(lake, &v, &meta, gone, true).await?)?)),
                _ => {}
            }
        }
        for (t, b) in parts {
            if b.num_rows() > 0 && t != EXPECTED {
                made.entry(t.clone()).or_default().push(b.clone()); // (for the views that follow this one)
            }
            out.push((t, b));
        }
    }
    Ok(out)
}

/// Views, each after the view it follows (by table name, without the `v/`).
fn in_order(views: Vec<(String, View)>) -> Vec<(String, View)> {
    let names: std::collections::HashSet<String> = views.iter().map(|(k, _)| k[2..].to_string()).collect();
    let (mut done, mut out, mut left) = (std::collections::HashSet::new(), vec![], views);
    while !left.is_empty() {
        let (ready, rest): (Vec<_>, Vec<_>) = left.into_iter().partition(|(_, v)| !names.contains(&v.source) || done.contains(&v.source));
        if ready.is_empty() {
            out.extend(rest); // (a loop: none can be made, as a view follows a table that exists)
            break;
        }
        done.extend(ready.iter().map(|(k, _)| k[2..].to_string()));
        out.extend(ready);
        left = rest;
    }
    out.into_iter().map(|(k, v)| (k[2..].to_string(), v)).collect()
}

/// The views whose rows are derived as flushes are packed (`derive`): the sequencer holds every
/// flush to them (`log::commit`). A flush carrying rows of a table such views follow must carry a
/// part for each (empty if it derived none), and none for a view it doesn't know: else its node
/// packs it again, with the views as they are now. So a view made (or dropped) mid-stream never
/// misses a flush's rows or gets them from a flush packed without it. Kept in the leader's
/// memory; `forget` after a view is made or dropped.
#[derive(Default)]
pub struct Inline {
    pub by_source: std::collections::HashMap<String, Vec<String>>, // table -> its views
    pub tables: std::collections::HashSet<String>,                  // the views' tables (and `$deleted`s)
    pub unbounded: Vec<(String, View)>,                             // views whose filling doesn't end yet
}

static INLINE: std::sync::Mutex<BTreeMap<String, std::sync::Arc<Inline>>> = std::sync::Mutex::new(BTreeMap::new());
static BOUNDED: std::sync::Mutex<Vec<String>> = std::sync::Mutex::new(Vec::new()); // (fills this process set `upto` for)

pub fn forget(lake: &Lake) { INLINE.lock().unwrap().remove(&lake.url); }

/// The inline views, as the sequencer holds flushes to them.
pub async fn inline(lake: &Lake) -> Result<std::sync::Arc<Inline>> {
    if let Some(i) = INLINE.lock().unwrap().get(&lake.url) {
        return Ok(i.clone());
    }
    let mut i = Inline::default();
    for (key, v) in lake.cat.scan::<View>("v/", "v0").await? {
        if v.sessions.is_some() || v.join.is_some() {
            continue;
        }
        let t = key[2..].to_string();
        i.by_source.entry(v.source.clone()).or_default().push(t.clone());
        i.tables.extend([crate::sys::deleted(&t), t.clone()]);
        if !v.expect.is_empty() {
            i.tables.insert(EXPECTED.into()); // (their counts)
        }
        if v.fill.as_ref().is_some_and(|f| f.upto.is_none() && !BOUNDED.lock().unwrap().contains(&f.id)) {
            i.unbounded.push((t, v));
        }
    }
    let i = std::sync::Arc::new(i);
    INLINE.lock().unwrap().insert(lake.url.clone(), i.clone());
    Ok(i)
}

/// Sequencer: where the fillings of views made since the last commit end (this commit's puts):
/// every row before `next` is theirs to fill; every flush from this commit on derives their rows.
pub fn bound(lake: &Lake, inline: &Inline, next: u64) -> Vec<(String, Vec<u8>)> {
    let mut puts = vec![];
    for (t, v) in &inline.unbounded {
        let fill = v.fill.clone().map(|f| Fill { upto: Some(next - 1), ..f });
        BOUNDED.lock().unwrap().extend(fill.iter().map(|f| f.id.clone()));
        puts.push((view_key(t), json(&View { fill, ..v.clone() })));
    }
    let mut cache = INLINE.lock().unwrap();
    if !puts.is_empty() && cache.get(&lake.url).is_some_and(|c| std::ptr::eq(c.as_ref(), inline)) {
        // (unless a view was made or dropped meanwhile: then the next commit reads them all again)
        let bounded = Inline { by_source: inline.by_source.clone(), tables: inline.tables.clone(), unbounded: vec![] };
        cache.insert(lake.url.clone(), std::sync::Arc::new(bounded));
    }
    puts
}

/// A view still filling from its source's rows, if one follows `table` (changes of it wait).
pub async fn filling(lake: &Lake, table: &str) -> Result<Option<String>> {
    for (key, v) in lake.cat.scan::<View>("v/", "v0").await? {
        if v.source == table && v.fill.is_some() && lake.cat.get::<u64>(&producer_key(&format!("fill:{}", &key[2..]))).await?.is_none() {
            return Ok(Some(key[2..].to_string()));
        }
    }
    Ok(None)
}

/// Leader: fill the views made since, from their sources' rows up to where their filling ends,
/// once each (producer `fill:{view}`, seq 1). One whose end isn't set yet gets a commit to set it.
/// Under the lake's lock: a view dropped (and made again) meanwhile would get another's rows.
pub async fn fill_all(lake: &Lake, seq: &crate::log::Sequencer, log: &crate::log::Log, lock: &tokio::sync::Mutex<()>) -> Result<()> {
    let mut waiting = vec![];
    for (key, v) in lake.cat.scan::<View>("v/", "v0").await? {
        if v.fill.is_some() && lake.cat.get::<u64>(&producer_key(&format!("fill:{}", &key[2..]))).await?.is_none() {
            waiting.push(key[2..].to_string());
        }
    }
    if waiting.is_empty() {
        return Ok(());
    }
    let _guard = lock.lock().await;
    for name in &waiting {
        let producer = format!("fill:{name}");
        let (Some(v), None) = (lake.cat.get::<View>(&view_key(name)).await?, lake.cat.get::<u64>(&producer_key(&producer)).await?) else { continue }; // (dropped, or filled, meanwhile)
        let Some(fill) = &v.fill else { continue };
        let Some(upto) = fill.upto else {
            seq.number().await?; // (a commit: the sequencer sets `upto` in it)
            continue;
        };
        // The source as it was at `upto`: its rows (and changes) from then, none from after.
        // (with each row's `_version`: a row-by-row view's row keeps its source row's, so a change
        // of that row later takes this one back — `{view}$deleted` names the source's version)
        let sql = format!("SELECT *, \"{}\", \"{}\", \"{v2}\" FROM {} WHERE \"{v2}\" <= {upto}", crate::sys::ROW_ID, crate::sys::CREATED, crate::write::sql_name(&v.source), v2 = crate::sys::VERSION);
        let rows = crate::query::sql(&session_at(lake, &sql, "", Some(upto)).await?, &crate::asof::rewrite(&sql)?).await?.collect().await?;
        let meta: TableMeta = lake.cat.get(&table_key(name)).await?.context("view without table")?;
        let out = match rows.iter().any(|b| b.num_rows() > 0) {
            true => view_rows(lake, &v, &meta, &rows, false).await?,
            false => RecordBatch::new_empty(crate::query::schema(&meta.columns)?),
        };
        let (out, failed) = expected(name, &v, out, true)?;
        if let Some(failed) = failed {
            log.append(EXPECTED.into(), crate::log::Src { producer: format!("{producer}:expect"), seq: 1, prev: None }, failed).await?; // (once, as the filling is)
        }
        log.append(name.to_string(), crate::log::Src { producer, seq: 1, prev: None }, out).await?;
    }
    Ok(())
}

/// A view's rows of `rows`, in its table's columns; a row-by-row view's with their source rows'
/// `_row_id` and `_created_at` after them (and `old`: `_old_version`, as `{view}$deleted` holds;
/// rows that have a `_version`, as a view's filling reads them: that too, which the log keeps).
async fn view_rows(lake: &Lake, v: &View, meta: &TableMeta, rows: &[RecordBatch], old: bool) -> Result<RecordBatch> {
    use crate::query::{cast_as, schema};
    if !v.ids {
        return cast_as(&over(lake, &v.source, rows.to_vec(), &v.sql).await?, &schema(&meta.columns)?);
    }
    let mut ids = vec![(crate::sys::ROW_ID.to_string(), "Int64".to_string()), crate::sys::columns()[2].clone()];
    if old {
        ids.push(("_old_version".into(), "Int64".into()));
    } else if rows.first().is_some_and(|b| b.schema().index_of(crate::sys::VERSION).is_ok()) {
        ids.push((crate::sys::VERSION.into(), "Int64".into()));
    }
    let src: TableMeta = lake.cat.get::<TableMeta>(&table_key(&v.source)).await?.with_context(|| format!("no table {}", v.source))?.logical();
    let sql = crate::asof::rewrite(&v.sql)?;
    let names: Vec<&str> = ids.iter().map(|(c, _)| c.as_str()).collect();
    let (_, batches) = crate::fresh::run(lake, &v.source, schema(&[src.columns, ids.clone()].concat())?, rows, &sql, &names.join(","), |p| carry(p, &names)).await?;
    let target = schema(&[meta.columns.clone(), ids].concat())?;
    let Some(first) = batches.first() else { return Ok(RecordBatch::new_empty(target)) };
    let all = datafusion::arrow::compute::concat_batches(&first.schema(), &batches)?;
    let picked = target.fields().iter().map(|f| all.schema().index_of(f.name())).collect::<Result<Vec<_>, _>>()?;
    cast_as(&all.project(&picked)?, &target)
}

/// `plan` with columns `names` of its table carried through every projection, as they are.
fn carry(plan: LogicalPlan, names: &[&str]) -> Result<LogicalPlan> {
    use datafusion::common::tree_node::{Transformed, TreeNode};
    let carried = plan.transform_up(|p| {
        let LogicalPlan::Projection(p) = p else { return Ok(Transformed::yes(p.recompute_schema()?)) };
        let mut expr = p.expr.clone();
        for n in names {
            if p.schema.field_with_unqualified_name(n).is_err() {
                let (q, f) = p.input.schema().qualified_field_with_unqualified_name(n)?;
                expr.push(Expr::Column(datafusion::common::Column::new(q.cloned(), f.name())));
            }
        }
        Ok(Transformed::yes(LogicalPlan::Projection(datafusion::logical_expr::Projection::try_new(expr, p.input)?)))
    })?;
    Ok(carried.data)
}

/// Partial aggregates taken back: their sums and counts negated.
fn negated(b: &RecordBatch, meta: &TableMeta) -> Result<RecordBatch> {
    let columns = b.schema().fields().iter().zip(b.columns()).map(|(f, c)| match meta.merge.get(f.name()).map(String::as_str) {
        Some("sum" | "count") => Ok(datafusion::arrow::compute::kernels::numeric::neg(c)?),
        _ => Ok(c.clone()),
    }).collect::<Result<Vec<_>>>()?;
    Ok(RecordBatch::try_new(b.schema(), columns)?)
}

/// For a GROUP BY query: its key columns and how each aggregate column merges. Only aggregates
/// that combine from partial results qualify.
fn merges(plan: &LogicalPlan, up: Option<(&str, &TableMeta)>) -> Result<(Vec<String>, BTreeMap<String, String>)> {
    let (mut key, mut merge) = (vec![], BTreeMap::new());
    // (`up`: a GROUP BY view this one follows, whose partial rows only a rollup combines right)
    let rollup = |source: &str, m: &TableMeta| {
        format!("{source} is a GROUP BY view: its table keeps partial rows, a few per key, combined as they are read. A view of it must combine them the same way: GROUP BY some of its keys ({}), a WHERE on those only, and sum() of its sums and counts, min() of its mins, max() of its maxes. Or read it as it is: CREATE VIEW … AS SELECT … FROM {source}", m.key.join(", "))
    };
    let Some(LogicalPlan::Aggregate(agg)) = top_aggregate(plan) else {
        if let Some((source, m)) = up {
            bail!(rollup(source, m));
        }
        return Ok((key, merge)); // no GROUP BY: rows are appended
    };
    if let Some((source, m)) = up {
        // (its keys only, no subquery; below its GROUP BY, filters, then the view's table — whose
        // reading, which combines its rows, the plan shows inlined under its name)
        let keys = |e: &Expr| of_keys(e, m);
        // (DataFusion adds to a GROUP BY the columns its keys determine: read while it holds
        // partial rows, the view's table is grouped by its key, which then determines its sums.
        // Grouping by those too splits nothing; a rollup's own keys are checked below.)
        let input = agg.input.schema();
        let named: Vec<String> = agg.group_expr.iter().filter(|e| keys(e)).map(|e| e.schema_name().to_string()).collect();
        let implied = datafusion::common::get_target_functional_dependencies(input, &named).unwrap_or_default();
        let added = |e: &Expr| matches!(e, Expr::Column(c) if input.index_of_column(c).is_ok_and(|i| implied.contains(&i)));
        let mut ok = agg.group_expr.iter().all(|e| keys(e) || added(e));
        let mut below = agg.input.as_ref();
        loop {
            match below {
                LogicalPlan::Filter(f) => (ok, below) = (ok && keys(&f.predicate), f.input.as_ref()),
                LogicalPlan::SubqueryAlias(_) | LogicalPlan::TableScan(_) => break,
                _ => break ok = false,
            }
        }
        ensure!(ok, rollup(source, m));
    }
    let plain = "an aggregating view must be a plain SELECT … GROUP BY (no HAVING, ORDER BY or LIMIT)";
    let LogicalPlan::Projection(p) = plan else { bail!(plain) };
    ensure!(matches!(p.input.as_ref(), LogicalPlan::Aggregate(_)), plain);
    for (e, f) in p.expr.iter().zip(plan.schema().fields()) {
        let Expr::Column(c) = e.clone().unalias_nested().data else { bail!("column {} must be a group key or one aggregate: {e}", f.name()) };
        let i = agg.schema.index_of_column(&c)?;
        if i < agg.group_expr.len() {
            if let Some((source, m)) = up {
                ensure!(of_keys(&agg.group_expr[i], m), rollup(source, m)); // (a rollup's keys are its source's)
            }
            key.push(f.name().clone());
            continue;
        }
        let Expr::AggregateFunction(a) = agg.aggr_expr[i - agg.group_expr.len()].clone().unalias() else { bail!("unexpected aggregate") };
        ensure!(!a.params.distinct, "DISTINCT aggregates can't be combined from partial results");
        let mut m = match a.func.name() {
            "count" => "count", // (added up like a sum; a group whose count is 0 is gone: a change emptied it)
            "sum" => "sum",
            "min" => "min",
            "max" => "max",
            other => bail!("{other}() can't be combined from partial results: use sum, count, min or max (e.g. avg = sum / count at query time)"),
        };
        if let Some((source, up)) = up {
            let of = match &a.params.args[..] {
                [Expr::Column(c)] if a.params.filter.is_none() => up.merge.get(&c.name).map(String::as_str),
                _ => None,
            };
            m = match (m, of) {
                ("sum", Some("sum")) => "sum",
                ("sum", Some("count")) => "count", // (counts added up are a count: a group they empty is gone)
                ("min", Some("min")) => "min",
                ("max", Some("max")) => "max",
                _ => bail!(rollup(source, up)),
            };
        }
        merge.insert(f.name().clone(), m.to_string());
    }
    ensure!(!key.is_empty(), "an aggregating view needs GROUP BY columns in its SELECT");
    Ok((key, merge))
}

/// Whether `e` reads only `m`'s keys (and no subquery).
fn of_keys(e: &Expr, m: &TableMeta) -> bool {
    use datafusion::common::tree_node::TreeNode;
    e.column_refs().iter().all(|c| m.key.contains(&c.name)) && !e.exists(|e| Ok(matches!(e, Expr::ScalarSubquery(_) | Expr::Exists(_) | Expr::InSubquery(_)))).unwrap_or(true)
}

/// The query's own GROUP BY, if any: the first Aggregate below its top-level projection, sort,
/// limit and filter nodes (aggregates inside joined tables don't count).
fn top_aggregate(plan: &LogicalPlan) -> Option<&LogicalPlan> {
    match plan {
        LogicalPlan::Aggregate(_) => Some(plan),
        LogicalPlan::Projection(_) | LogicalPlan::Sort(_) | LogicalPlan::Limit(_) | LogicalPlan::Filter(_) => top_aggregate(plan.inputs()[0]),
        _ => None,
    }
}

/// A stream join view: its two tables (both of this lake, append tables), its columns from the
/// query, and where it starts (rows written from now on, paired with whatever the other side has).
async fn create_join(lake: &Lake, name: &str, sql: &str, source: String, mut j: Join) -> Result<()> {
    use datafusion::common::tree_node::TreeNode;
    let planned = crate::asof::rewrite(sql)?;
    let plan = crate::query::sql(&session(lake, &planned, "").await?, &planned).await?.into_unoptimized_plan();
    let mut scans = vec![];
    plan.apply(|p| {
        if let LogicalPlan::TableScan(t) = p {
            scans.push(t.table_name.to_string());
        }
        Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
    })?;
    let mut tables = vec![];
    for t in scans {
        let (other, t) = crate::ddl::resolve(lake, &t).await?;
        let meta: Option<TableMeta> = lake.cat.get(&table_key(&t)).await?;
        if other.is_none() && meta.is_some_and(|m| m.key.is_empty()) && !tables.contains(&t) {
            tables.push(t);
        }
    }
    ensure!(tables.len() == 2 && tables[0] == source, "join = 'streams': the query joins two append tables of this lake (it names {tables:?})");
    for (i, t) in tables.iter().enumerate() {
        if let Some(c) = j.time.get(i).or(j.time.first()) {
            let meta: TableMeta = lake.cat.get(&table_key(t)).await?.context("no table")?;
            ensure!(timestamp(&meta, c), "time: {c} isn't a timestamp column of {t}");
        }
    }
    let columns: Vec<(String, String)> = plan.schema().fields().iter().map(|f| (f.name().clone(), crate::query::type_name(f.data_type()))).collect();
    j.tables = tables;
    let now = lake.visible();
    let meta = TableMeta { columns, publish: default_publish(), ids: true, tiered: now, ..Default::default() };
    let view = View { source, sql: sql.into(), emit: None, sessions: None, ids: false, join: Some(j), fill: None, expect: vec![] };
    lake.cat.commit(vec![(table_key(name), json(&meta)), (view_key(name), json(&view)), (producer_key(&format!("join:{name}")), json(&now))], &[]).await
}

/// Leader, right after commits: every stream join view brought up to date (one that fails is
/// tried again next time; the others go on).
pub async fn join_all(lake: &Lake, log: &crate::log::Log) -> Result<()> {
    for (key, v) in lake.cat.scan::<View>("v/", "v0").await? {
        if let Some(j) = &v.join {
            if let Err(e) = join(lake, log, &key[2..], &v, j).await {
                eprintln!("stream join {}: {e:#}", &key[2..]);
            }
        }
    }
    Ok(())
}

/// The pairs commits (done, now] made: the new rows of one table against the other's as of now,
/// and the other's new rows against the first's as of `done` — Δa ⋈ b ∪ a(done) ⋈ Δb, so each pair
/// comes once. `_version` (the commit that wrote a row, `sys.rs`) tells new rows from old. The
/// pairs and the progress (`join:{view}`) commit together: none lost, none twice.
async fn join(lake: &Lake, log: &crate::log::Log, view: &str, v: &View, j: &Join) -> Result<()> {
    use crate::sys::VERSION;
    use datafusion::prelude::{col, lit};
    let producer = format!("join:{view}");
    let done: u64 = lake.cat.get(&producer_key(&producer)).await?.unwrap_or(0);
    let now = lake.visible();
    if now <= done {
        return Ok(());
    }
    let (a, b) = (&j.tables[0], &j.tables[1]);
    let rows = session_at(lake, &format!("SELECT {VERSION} FROM {a}, {b}"), "", Some(now)).await?; // (with their system columns)
    let (new_a, new_b) = (versions(&rows, a, Some(done), now).await?.collect().await?, versions(&rows, b, Some(done), now).await?.collect().await?);
    if new_a.iter().chain(&new_b).all(|b| b.num_rows() == 0) {
        return Ok(()); // (nothing new on either side: no run, no commit)
    }
    // What the new rows of one side pair with: the other's rows up to a commit, and, bounded,
    // those within `within_secs` of the new rows' times.
    let time = |i: usize| j.time.get(i).or(j.time.first()).cloned();
    let bound = |i: usize, fresh: &[RecordBatch]| match (time(i), time(1 - i), j.within_secs) {
        (Some(mine), Some(theirs), Some(w)) => span(fresh, &theirs, w).map(|range| (mine, range)),
        _ => None,
    };
    let (for_a, for_b) = (bound(1, &new_a), bound(0, &new_b));
    let mut other_b = versions(&rows, b, None, now).await?;
    let mut other_a = versions(&rows, a, None, done).await?;
    for (df, bound) in [(&mut other_b, for_a), (&mut other_a, for_b)] {
        if let Some((c, (lo, hi))) = bound {
            let dt = df.schema().field_with_unqualified_name(&c)?.data_type().clone();
            let at = |us: i64| datafusion::common::ScalarValue::TimestampMicrosecond(Some(us), None).cast_to(&dt);
            *df = df.clone().filter(col(c.as_str()).gt_eq(lit(at(lo)?)).and(col(c.as_str()).lt_eq(lit(at(hi)?))))?;
        }
    }
    let mut out = vec![];
    for (fresh, other) in [(0usize, other_b), (1, other_a)] {
        let (fresh_rows, other_i) = if fresh == 0 { (&new_a, 1) } else { (&new_b, 0) };
        if fresh_rows.iter().all(|b| b.num_rows() == 0) {
            continue;
        }
        // The view's SQL over the fresh rows of one table and the other's rows, each with its own
        // columns only (the system columns stay out of the view's `*`).
        let ctx = session_at(lake, &v.sql, "", Some(now)).await?;
        for (i, df) in [(fresh, rows.read_batches(fresh_rows.clone())?), (other_i, other.clone())] {
            let t = &j.tables[i];
            let meta: TableMeta = lake.cat.get(&table_key(t)).await?.with_context(|| format!("no table {t}"))?;
            let df = df.select_columns(&meta.columns.iter().map(|(c, _)| c.as_str()).collect::<Vec<_>>())?;
            ctx.deregister_table(crate::query::table_ref(t))?;
            ctx.register_table(crate::query::table_ref(t), df.into_view())?;
        }
        out.extend(crate::query::sql(&ctx, &crate::asof::rewrite(&v.sql)?).await?.collect().await?);
    }
    let meta: TableMeta = lake.cat.get(&table_key(view)).await?.with_context(|| format!("no table {view}"))?;
    let s = crate::query::schema(&meta.columns)?;
    let out = out.iter().map(|b| crate::query::cast_as(b, &s)).collect::<Result<Vec<_>>>()?;
    append(lake, log, view, crate::log::Src { producer, seq: now, prev: Some(done) }, out).await
}

/// Table `t`'s rows (in `ctx`, with system columns) written by commits (after, upto].
async fn versions(ctx: &datafusion::prelude::SessionContext, t: &str, after: Option<u64>, upto: u64) -> Result<datafusion::prelude::DataFrame> {
    use datafusion::prelude::{col, lit};
    let upto = col(crate::sys::VERSION).lt_eq(lit(upto as i64));
    let cond = match after {
        Some(after) => col(crate::sys::VERSION).gt(lit(after as i64)).and(upto),
        None => upto,
    };
    Ok(ctx.table(crate::query::table_ref(t)).await?.filter(cond)?)
}

/// The time range (µs) `within_secs` around the values of column `c` in `rows` (None: no rows).
fn span(rows: &[RecordBatch], c: &str, within_secs: u64) -> Option<(i64, i64)> {
    use datafusion::arrow::compute::{max, min};
    let (mut lo, mut hi) = (None::<i64>, None::<i64>);
    for b in rows.iter().filter(|b| b.num_rows() > 0) {
        let us = micros(b, c).ok()?;
        lo = lo.min(min(&us)).or(min(&us)).or(lo);
        hi = hi.max(max(&us));
    }
    let w = (within_secs * 1_000_000) as i64;
    Some((lo? - w, hi? + w))
}

/// `pondra.flows` (a row per materialized view: what it follows, its kind, what else it reads,
/// its expectations) and `pondra.expectations` (a row per expectation, with the rows that broke it:
/// `counts`, the rows of `pondra$expectations`). What a user may read only.
pub async fn system(lake: &Lake, counts: Vec<RecordBatch>) -> Result<Vec<(&'static str, std::sync::Arc<dyn datafusion::catalog::TableProvider>)>> {
    use datafusion::arrow::array::{Array, AsArray, Int64Array, StringArray};
    use datafusion::datasource::MemTable;
    let mut views = in_order(lake.cat.scan::<View>("v/", "v0").await?);
    if let Some(a) = crate::auth::limited() {
        views.retain(|(name, _)| a.may("select", name));
    }
    let (mut kinds, mut histories) = (vec![], std::collections::HashSet::new());
    for (name, v) in &views {
        let meta = lake.cat.get::<TableMeta>(&table_key(name)).await?;
        let merged = meta.as_ref().is_some_and(|m| !m.merge.is_empty());
        if meta.as_ref().is_some_and(|m| m.history.is_some()) {
            histories.insert(name.clone());
        }
        kinds.push(match () {
            _ if v.emit.is_some() => "window",
            _ if v.sessions.is_some() => "sessions",
            _ if v.join.is_some() => "stream join",
            _ if merged => "aggregate",
            _ if histories.contains(name) => "history",
            _ => "rows",
        });
    }
    let mut failed: std::collections::HashMap<(String, String), i64> = Default::default();
    for b in &counts {
        let col = |n: &str| b.column_by_name(n).map(|c| datafusion::arrow::compute::cast(c, &datafusion::arrow::datatypes::DataType::Utf8)).transpose();
        let (Some(id), Some(e), Some(n)) = (col("id")?, col("expectation")?, b.column_by_name("failed")) else { continue };
        let n = datafusion::arrow::compute::cast(n, &datafusion::arrow::datatypes::DataType::Int64)?;
        let n = n.as_primitive::<datafusion::arrow::datatypes::Int64Type>();
        for i in (0..b.num_rows()).filter(|&i| n.is_valid(i)) {
            let k = (id.as_string::<i32>().value(i).to_string(), e.as_string::<i32>().value(i).to_string());
            *failed.entry(k).or_default() += n.value(i);
        }
    }
    let s = |f: &dyn Fn(usize, &(String, View)) -> Option<String>| std::sync::Arc::new(views.iter().enumerate().map(|(i, x)| f(i, x)).collect::<StringArray>()) as datafusion::arrow::array::ArrayRef;
    let reads = |v: &View| crate::spmd::tables(&v.sql).map(|t| t.into_iter().filter(|t| *t != v.source).collect::<Vec<_>>().join(", ")).filter(|r| !r.is_empty());
    let flows = RecordBatch::try_from_iter(vec![
        ("name", s(&|_, (n, _)| Some(n.clone()))),
        ("follows", s(&|_, (_, v)| Some(v.join.as_ref().map_or(v.source.clone(), |j| j.tables.join(", "))))),
        ("kind", s(&|i, _| Some(kinds[i].to_string()))),
        ("reads", s(&|_, (_, v)| reads(v))),
        ("expectations", std::sync::Arc::new(views.iter().map(|(_, v)| v.expect.len() as i64).collect::<Int64Array>())),
        ("definition", s(&|_, (_, v)| Some(v.sql.clone()))),
    ])?;
    let all: Vec<(&String, &View, &Expect)> = views.iter().flat_map(|(n, v)| v.expect.iter().map(move |e| (n, v, e))).collect();
    let e = |f: &dyn Fn(&(&String, &View, &Expect)) -> String| std::sync::Arc::new(all.iter().map(|x| Some(f(x))).collect::<StringArray>()) as datafusion::arrow::array::ArrayRef;
    let expectations = RecordBatch::try_from_iter(vec![
        ("view", e(&|(n, _, _)| n.to_string())),
        ("expectation", e(&|(_, _, x)| x.name.clone())),
        ("condition", e(&|(_, _, x)| x.check.clone())),
        ("on_violation", e(&|(_, _, x)| x.on.word().to_string())),
        ("failed_rows", std::sync::Arc::new(all.iter().map(|(_, v, x)| failed.get(&(v.fill.as_ref().map_or(String::new(), |f| f.id.clone()), x.name.clone())).copied().unwrap_or(0)).collect::<Int64Array>())),
    ])?;
    let mem = |b: RecordBatch| -> Result<std::sync::Arc<dyn datafusion::catalog::TableProvider>> { Ok(std::sync::Arc::new(MemTable::try_new(b.schema(), vec![vec![b]])?)) };
    Ok(vec![("flows", mem(flows)?), ("expectations", mem(expectations)?)])
}
