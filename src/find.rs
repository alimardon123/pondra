//! Finding everything (ADR-065 §1): `pondra.parts`, every part inside every object the caller may
//! see, and `pondra.find('words' [, kind => 'k'])`, a search of objects, parts and the concepts in
//! `pondra.kinds` by name, comment and definition. Both read the registry (`objects.rs`), so a kind
//! added there is found as soon as its lister yields it.

use crate::text::{join, PUBLIC};
use crate::objects::{expect_sql, ident, kind, lakes, list, list_sql, name_sql, sql_type, Object, KINDS, PARTS, PATTERNS};
use crate::store::{table_key, Lake, TableMeta};
use anyhow::{bail, ensure, Context, Result};
use datafusion::prelude::SessionContext;
use futures::future::BoxFuture;
use serde_json::{json as j, Value};
use std::collections::{HashMap, HashSet};
use std::ops::Range;
use std::sync::{Arc, LazyLock};

/// One part of an object, as `pondra.parts` lists it: `kind` is the part's (a `PARTS` name),
/// `object` and `object_kind` the object it is inside.
#[derive(Debug)]
pub struct PartRow {
    pub kind: &'static str,
    pub lake: String,
    pub schema: Option<String>,
    pub object: String,
    pub object_kind: &'static str,
    pub name: String,
    pub ty: Option<String>,
    pub comment: Option<String>,
    pub definition: Option<String>,
}

tokio::task_local! {
    /// How many `parts` this one is inside. Planning a view (for its columns) expands its SQL, which
    /// may search the catalog again (`pondra.find`), and so would list the same parts without end.
    static LISTING: usize;
}

/// The parts of `objects`, and only of these: the caller's rights decided which objects were
/// listed, so no other object is read. A view that won't plan has no column rows (no error).
pub async fn parts(lake: &Lake, objects: &[Object]) -> Result<Vec<PartRow>> {
    let depth = LISTING.try_with(|d| *d).unwrap_or(0);
    ensure!(depth < 4, "pondra.parts: views that search the catalog they are in (asked {depth} deep)");
    LISTING.scope(depth + 1, parts_of(lake, objects)).await
}

/// Each object's parts, read from its own lake (`lakes`: this one and those attached that the
/// caller may use). Each lake's routines are listed once.
async fn parts_of(lake: &Lake, objects: &[Object]) -> Result<Vec<PartRow>> {
    let lakes: HashMap<String, Arc<Lake>> = lakes(lake).await.into_iter().collect();
    let here = crate::ddl::lake_name(lake);
    let limit = crate::auth::limited();
    let mut routines = HashMap::new();
    let mut out = vec![];
    // Every view's columns are planned in one session: a session a view took 3 ms each (200 views, 0.6 s).
    let views: Vec<String> = objects.iter().filter(|o| matches!(o.kind, "view" | "external table")).map(|o| reached_as(o, &here)).collect();
    let session = match views.is_empty() {
        true => None,
        false => session_for(lake, &views).await,
    };
    for o in objects {
        let Some(l) = lakes.get(&o.lake) else { continue };
        let partial = match &limit {
            Some(a) if matches!(o.kind, "table" | "view" | "materialized view" | "external table") => partial_columns(a, o, &here),
            _ => None,
        };
        let more = match o.kind {
            "table" | "materialized view" => stored_parts(l, o, partial.as_ref()).await?,
            "view" | "external table" => view_parts(session.as_ref(), l, o, &here, partial.as_ref()).await?,
            "function" | "macro" | "table function" | "procedure" => {
                if !routines.contains_key(&o.lake) {
                    routines.insert(o.lake.clone(), crate::routines::listed(l).await?);
                }
                routine_parts(&routines[&o.lake], o)
            }
            "type" => type_parts(l, o).await?,
            _ => vec![],
        };
        out.extend(more);
    }
    Ok(out)
}

/// A part of `o`, placed inside it.
fn part(o: &Object, kind: &'static str, name: String, ty: Option<String>, comment: Option<String>, definition: Option<String>) -> PartRow {
    PartRow { kind, lake: o.lake.clone(), schema: o.schema.clone(), object: o.name.clone(), object_kind: o.kind, name, ty, comment, definition }
}

/// The columns of `o` that a limited caller may read, when it may read only some of them: None when
/// it may read them all (or the question doesn't arise). A column it may not read is not shown by
/// name either, so a caller granted only some columns gets only those, and none of the parts that
/// name columns (keys, links, checks, expectations). Empty when it may read none.
fn partial_columns(a: &crate::users::Access, o: &Object, here: &str) -> Option<HashSet<String>> {
    let catalog = if o.lake == here { o.local() } else { format!("{}.{}", o.lake, o.local()) };
    match a.columns("select", &catalog) {
        Some(None) => None,
        Some(Some(cols)) => Some(cols),
        None => Some(HashSet::new()),
    }
}

/// A table's or materialized view's parts: its columns (under their SQL names, with their comments),
/// its key, its links and checks, and a materialized view's expectations. `partial`: the columns a
/// limited caller may see (`partial_columns`), and then nothing else.
async fn stored_parts(l: &Lake, o: &Object, partial: Option<&HashSet<String>>) -> Result<Vec<PartRow>> {
    let local = o.local();
    let Some(meta) = l.cat.get::<TableMeta>(&table_key(&local)).await? else { return Ok(vec![]) };
    let d = meta.described();
    let comments = column_comments(l, &local).await?;
    let mut out = vec![];
    for (name, ty) in &d.columns {
        if d.marker(name) {
            continue; // (a keyed table's `_deleted` is the table's own, not a column)
        }
        let stored = meta.stored(name).unwrap_or(name);
        if partial.is_some_and(|cols| !cols.contains(name) && !cols.contains(stored)) {
            continue;
        }
        let ty = crate::types::shown(&d, name, ty);
        let comment = comments.get(stored).cloned();
        out.push(part(o, "column", name.clone(), Some(ty), comment, None));
    }
    if partial.is_some() {
        return Ok(out);
    }
    if !d.key.is_empty() {
        out.push(part(o, "key", format!("{}_pkey", o.name), None, None, Some(format!("PRIMARY KEY ({})", list_sql(&d.key)))));
    }
    for c in &d.constraints {
        let link = c.kind == crate::constraints::Kind::ForeignKey;
        out.push(part(o, if link { "link" } else { "key" }, c.name.clone(), None, None, Some(c.def(&ident))));
    }
    out.extend(d.checks.iter().map(|(n, cond)| part(o, "check", n.clone(), None, None, Some(format!("CHECK ({cond})")))));
    if o.kind == "materialized view" {
        out.extend(expectations(l, &local).await?.iter().map(|e| part(o, "expectation", e.name.clone(), None, None, Some(expect_sql(e)))));
    }
    Ok(out)
}

/// A materialized view's expectations. An EMIT FINAL view's entry is `{name}$open`, which `relations`
/// reads when the plain name has none; this reads the same.
async fn expectations(l: &Lake, local: &str) -> Result<Vec<crate::views::Expect>> {
    let view = match l.cat.get::<crate::views::View>(&crate::views::view_key(local)).await? {
        Some(v) => Some(v),
        None => l.cat.get::<crate::views::View>(&crate::views::view_key(&crate::once::open(local))).await?,
    };
    Ok(view.map(|v| v.expect).unwrap_or_default())
}

/// A table's or view's column comments, by the column's stored name (`cm/column/{table}/{stored}`).
async fn column_comments(l: &Lake, local: &str) -> Result<HashMap<String, String>> {
    let (from, to) = (format!("cm/column/{local}/"), format!("cm/column/{local}0"));
    Ok(l.cat.scan::<String>(&from, &to).await?.into_iter().map(|(k, text)| (k.rsplit('/').next().unwrap_or_default().to_string(), text)).collect())
}

/// A stored view's or external table's columns, as its query names and types them now, with their
/// comments, planned in `session` (every view's, made once) under the name it is reached by there.
async fn view_parts(session: Option<&SessionContext>, l: &Lake, o: &Object, here: &str, partial: Option<&HashSet<String>>) -> Result<Vec<PartRow>> {
    let local = o.local();
    let columns = match session {
        Some(ctx) => columns_in(ctx, &reached_as(o, here)).await.unwrap_or_default(), // (a view that won't plan has no columns to list)
        None => vec![],
    };
    let comments = column_comments(l, &local).await?;
    let stored = l.cat.get::<crate::ddl::StoredView>(&crate::ddl::query_key(&local)).await?;
    let measures: HashMap<String, String> = match &stored {
        Some(v) => crate::measures::listed(&v.sql).into_iter().collect(), // (a view's measures are parts of their own, with their expressions)
        None => HashMap::new(),
    };
    let columns = columns.into_iter().filter(|(name, _)| partial.is_none_or(|cols| cols.contains(name)));
    let mut out: Vec<PartRow> = columns.map(|(name, ty)| {
        let comment = comments.get(&name).cloned();
        match measures.get(&name) {
            Some(e) => part(o, "measure", name, Some(ty), comment, Some(e.clone())),
            None => part(o, "column", name, Some(ty), comment, None),
        }
    }).collect();
    // A view's key facts are listed as a table's are, unless a limited caller may not see the columns they name.
    if partial.is_none() {
        for c in stored.iter().flat_map(|v| &v.facts) {
            let link = c.kind == crate::constraints::Kind::ForeignKey;
            out.push(part(o, if link { "link" } else { "key" }, c.name.clone(), None, None, Some(c.def(&ident))));
        }
    }
    Ok(out)
}

/// The name an object is reached by from this lake: its own name here, `lake.schema.name` in another.
fn reached_as(o: &Object, here: &str) -> String {
    match o.lake == here {
        true => name_sql(&o.local()),
        false => format!("{}.{}", ident(&o.lake), name_sql(&join(o.schema.as_deref().unwrap_or(PUBLIC), &o.name))),
    }
}

/// One session with every view of `views` in it (None if it can't be made). (Boxed, as `columns_in`
/// is: making it expands the views' SQL, which may search the catalog, which comes back here.)
fn session_for<'a>(lake: &'a Lake, views: &'a [String]) -> BoxFuture<'a, Option<SessionContext>> {
    Box::pin(async move { crate::query::session(lake, &format!("SELECT * FROM {}", views.join(", ")), "").await.ok() })
}

/// A relation's columns, as a query in `ctx` names and types them. Planned, not run (`LIMIT 0`).
fn columns_in<'a>(ctx: &'a SessionContext, named: &'a str) -> BoxFuture<'a, Result<Vec<(String, String)>>> {
    Box::pin(async move {
        let df = crate::engine::sql(ctx, &format!("SELECT * FROM {named} LIMIT 0")).await?;
        Ok(crate::query::run_schema(ctx, df.logical_plan())?.fields().iter().map(|f| (f.name().clone(), sql_type(&f.data_type().to_string()))).collect())
    })
}

/// A routine's parameters, each with its type and its default.
fn routine_parts(all: &HashMap<String, crate::expander::Routine>, o: &Object) -> Vec<PartRow> {
    let Some(r) = all.get(&o.local()) else { return vec![] };
    r.params.iter().map(|p| part(o, "parameter", p.name.clone(), p.ty.clone(), None, p.default.as_ref().map(|d| format!("DEFAULT {d}")))).collect()
}

/// An enum type's labels, in the order it allows them.
async fn type_parts(l: &Lake, o: &Object) -> Result<Vec<PartRow>> {
    let Some(t) = l.cat.get::<crate::types::Type>(&crate::types::key(&o.local())).await? else { return Ok(vec![]) };
    Ok(t.labels.iter().map(|label| part(o, "label", label.clone(), None, None, None)).collect())
}

/// What `pondra.find` gives: one object, part or concept (a kind in `pondra.kinds`) a search matched.
/// `rank` is its place in the answer; `matched` is the field that held the words.
#[derive(Debug)]
pub struct Found {
    pub rank: i64,
    pub is: &'static str,
    pub kind: String,
    pub lake: Option<String>,
    pub schema: Option<String>,
    pub object: Option<String>,
    pub name: String,
    pub about: Option<String>,
    pub matched: &'static str,
}

/// A candidate `found` tests: what it would return, and its fields in order of preference (each
/// label with its text, lower-cased).
type Candidate = (Found, Vec<(&'static str, String)>);

/// The search: every word of `text` (split on whitespace, lower-cased) must be in one field. A name
/// that is the whole text comes first; then the candidate whose first field holding the words is
/// earliest; then shorter names first. Concepts are searched only when no kind is asked for.
pub fn found(text: &str, kind: Option<&str>, objects: &[Object], parts: &[PartRow]) -> Vec<Found> {
    let words: Vec<String> = text.to_lowercase().split_whitespace().map(str::to_string).collect();
    let whole = text.trim().to_lowercase();
    let mut all: Vec<Candidate> = match kind {
        None => concepts(),
        Some(_) => vec![],
    };
    all.extend(objects.iter().filter(|o| kind.map_or(true, |k| o.kind == k)).map(object_candidate));
    all.extend(parts.iter().filter(|p| kind.map_or(true, |k| p.kind == k)).map(part_candidate));
    let mut hits: Vec<(usize, Found)> = all.into_iter().filter_map(|(mut f, fields)| {
        let (tier, matched) = match f.name.to_lowercase() == whole {
            true => (0, "name"),
            false => fields.iter().enumerate().find(|(_, (_, field))| words.iter().all(|w| field.contains(w.as_str()))).map(|(i, (label, _))| (i + 1, *label))?,
        };
        f.matched = matched;
        Some((tier, f))
    }).collect();
    hits.sort_by(|(ta, a), (tb, b)| (ta, a.name.len(), &a.lake, &a.schema, &a.object, &a.name, &a.kind).cmp(&(tb, b.name.len(), &b.lake, &b.schema, &b.object, &b.name, &b.kind)));
    hits.into_iter().enumerate().map(|(i, (_, mut f))| {
        f.rank = i as i64 + 1;
        f
    }).collect()
}

/// The searched fields of a candidate, in order, lower-cased; a missing one is left out.
fn fields(list: &[(&'static str, Option<&str>)]) -> Vec<(&'static str, String)> {
    list.iter().filter_map(|&(label, text)| text.map(|t| (label, t.to_lowercase()))).collect()
}

/// Every kind, part and pattern, each as a concept of its own: found by its name, its `also` and its line.
fn concepts() -> Vec<Candidate> {
    let entries = KINDS.iter().map(|k| ("object", k.name, k.also, k.about))
        .chain(PARTS.iter().map(|p| ("part", p.name, p.also, p.about)))
        .chain(PATTERNS.iter().map(|p| ("pattern", p.name, p.also, p.about)));
    entries.map(|(is, name, also, about)| {
        let also = also.join(", ");
        let found = Found { rank: 0, is, kind: name.into(), lake: None, schema: None, object: None, name: name.into(), about: Some(about.into()), matched: "" };
        (found, fields(&[("name", Some(name)), ("also", Some(also.as_str())), ("about", Some(about))]))
    }).collect()
}

fn object_candidate(o: &Object) -> Candidate {
    let found = Found { rank: 0, is: "object", kind: o.kind.into(), lake: Some(o.lake.clone()), schema: o.schema.clone(), object: Some(o.name.clone()), name: o.name.clone(), about: o.comment.clone(), matched: "" };
    (found, fields(&[("name", Some(o.name.as_str())), ("comment", o.comment.as_deref()), ("definition", o.definition.as_deref())]))
}

fn part_candidate(p: &PartRow) -> Candidate {
    let found = Found { rank: 0, is: "part", kind: p.kind.into(), lake: Some(p.lake.clone()), schema: p.schema.clone(), object: Some(p.object.clone()), name: p.name.clone(), about: p.comment.clone(), matched: "" };
    (found, fields(&[("name", Some(p.name.as_str())), ("comment", p.comment.as_deref()), ("definition", p.definition.as_deref())]))
}

/// `pondra.find('words' [, kind => 'k'])` where SQL comes in: each call becomes its rows, as a
/// subquery of VALUES (as `vars::parameters_in` does for `pondra.parameters`). The catalog is read
/// once for all the calls in a statement.
pub async fn functions_in(lake: &Lake, sql: &str) -> Result<String> {
    static VIEW: LazyLock<regex::Regex> = LazyLock::new(|| regex::Regex::new(r"(?is)^\s*create\s+(?:or\s+(?:replace|alter)\s+)?view\b").expect("a regex"));
    if !sql.to_lowercase().contains("pondra.find") || VIEW.is_match(sql) {
        return Ok(sql.to_string()); // (a view keeps its call: it searches the catalog as it is when read)
    }
    let calls = calls(sql)?;
    if calls.is_empty() {
        return Ok(sql.to_string());
    }
    let all = list(lake).await?;
    let pieces = parts(lake, &all).await?;
    let mut out = String::new();
    let mut last = 0;
    for (at, text, kind) in calls {
        out.push_str(&sql[last..at.start]);
        out.push_str(&found_rows(&found(&text, kind.as_deref(), &all, &pieces)));
        last = at.end;
    }
    out.push_str(&sql[last..]);
    Ok(out)
}

/// The `pondra.find(…)` calls in `sql`: where each is, its words, and its kind if it names one. A
/// `pondra.find(` that doesn't read as `pondra.find('words' [, kind => 'k'])` is refused, and so is a
/// call with no words, or with a kind that is none of the registry's.
fn calls(sql: &str) -> Result<Vec<(Range<usize>, String, Option<String>)>> {
    static OPEN: LazyLock<regex::Regex> = LazyLock::new(|| regex::Regex::new(r"(?i)\bpondra\.find\s*\(").expect("a regex"));
    static WHOLE: LazyLock<regex::Regex> = LazyLock::new(|| regex::Regex::new(r"(?i)^pondra\.find\s*\(\s*'((?:[^']|'')*)'\s*(?:,\s*kind\s*=>\s*'((?:[^']|'')*)'\s*)?\)").expect("a regex"));
    let mut out = vec![];
    for open in OPEN.find_iter(sql) {
        // (anchored at the call's start: a call that doesn't read whole is refused, not skipped)
        let Some(c) = WHOLE.captures(&sql[open.start()..]) else {
            bail!("pondra.find('words' [, kind => 'column']): the words to look for, in quotes");
        };
        let text = c[1].replace("''", "'");
        let named = c.get(2).map(|k| k.as_str().replace("''", "'"));
        ensure!(!text.trim().is_empty(), "pondra.find: give the words to look for");
        if let Some(k) = &named {
            ensure!(known_kind(k), "pondra.find: no kind {k} (pondra.kinds lists them)");
        }
        out.push((open.start()..open.start() + c[0].len(), text, named));
    }
    Ok(out)
}

/// Is `name` a kind, a part or a pattern: what `pondra.find`'s kind may name?
fn known_kind(name: &str) -> bool {
    kind(name).is_some() || PARTS.iter().any(|p| p.name == name) || PATTERNS.iter().any(|p| p.name == name)
}

/// Found rows as a subquery of VALUES (one batch). With no rows, one row of NULLs that `WHERE false`
/// takes away: the shape `vars::rows` gives `pondra.parameters`.
fn found_rows(all: &[Found]) -> String {
    let text = |v: Option<&str>| v.map_or("CAST(NULL AS VARCHAR)".to_string(), |t| format!("CAST('{}' AS VARCHAR)", t.replace('\'', "''")));
    let row = |f: &Found| format!("(CAST({} AS BIGINT), {}, {}, {}, {}, {}, {}, {}, {})", f.rank, text(Some(f.is)), text(Some(f.kind.as_str())), text(f.lake.as_deref()),
        text(f.schema.as_deref()), text(f.object.as_deref()), text(Some(f.name.as_str())), text(f.about.as_deref()), text(Some(f.matched)));
    let values = match all.is_empty() {
        true => format!("(CAST(NULL AS BIGINT){})", ", CAST(NULL AS VARCHAR)".repeat(8)),
        false => all.iter().map(row).collect::<Vec<_>>().join(", "),
    };
    let only = if all.is_empty() { " WHERE false" } else { "" };
    format!("(SELECT * FROM (VALUES {values}) AS find(\"rank\", \"is\", kind, lake, schema, object, name, about, matched){only})")
}

/// The catalog's tools for agents (MCP), each a query: `find` now.
pub fn mcp_tools() -> Vec<Value> {
    vec![j!({"name": "find", "annotations": {"readOnlyHint": true},
        "inputSchema": {"type": "object", "properties": {"text": {"type": "string", "description": "words to look for"},
            "kind": {"type": "string", "description": "only this kind: table, view, column, key, link, …"}}, "required": ["text"]},
        "description": "Find tables, views, columns, keys, links, routines and every other object or part by its name, comment or definition, \
            and the concepts Pondra has under other products' names (\"field\" finds column, \"relationship\" finds link). \
            Each row: rank, is (object, part or pattern), kind, lake, schema, object, name, about (its comment), matched (the field that matched)."})]
}

/// Is `name` one of the catalog's tools?
pub fn is_tool(name: &str) -> bool { name == "find" }

/// The SQL a catalog tool runs, from its arguments.
pub fn mcp_sql(name: &str, args: &Value) -> Result<String> {
    match name {
        "find" => find_call(args),
        other => bail!("no catalog tool {other}"),
    }
}

/// `find`'s arguments as its call, with the words and the kind quoted as SQL reads them.
fn find_call(args: &Value) -> Result<String> {
    let quote = |s: &str| format!("'{}'", s.replace('\'', "''"));
    let text = args["text"].as_str().context("find takes text: the words to look for")?;
    let kind = args["kind"].as_str().map(|k| format!(", kind => {}", quote(k))).unwrap_or_default();
    Ok(format!("SELECT * FROM pondra.find({}{kind})", quote(text)))
}

/// `pondra.parts` as a batch: one row per part, its object's place and kind beside it.
pub fn parts_batch(all: &[PartRow]) -> Result<datafusion::arrow::array::RecordBatch> {
    use datafusion::arrow::array::{ArrayRef, RecordBatch, StringArray};
    let p = |f: &dyn Fn(&PartRow) -> Option<String>| Arc::new(all.iter().map(f).collect::<StringArray>()) as ArrayRef;
    Ok(RecordBatch::try_from_iter(vec![
        ("kind", p(&|p| Some(p.kind.to_string()))),
        ("lake", p(&|p| Some(p.lake.clone()))),
        ("schema", p(&|p| p.schema.clone())),
        ("object", p(&|p| Some(p.object.clone()))),
        ("object_kind", p(&|p| Some(p.object_kind.to_string()))),
        ("name", p(&|p| Some(p.name.clone()))),
        ("type", p(&|p| p.ty.clone())),
        ("comment", p(&|p| p.comment.clone())),
        ("definition", p(&|p| p.definition.clone())),
    ])?)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn object(kind: &'static str, name: &str, comment: Option<&str>) -> Object {
        Object { kind, lake: "lake".into(), schema: None, name: name.into(), comment: comment.map(Into::into), definition: None }
    }

    fn column(name: &str, comment: Option<&str>) -> PartRow {
        PartRow { kind: "column", lake: "lake".into(), schema: None, object: "sales".into(), object_kind: "table", name: name.into(), ty: Some("DOUBLE".into()), comment: comment.map(Into::into), definition: None }
    }

    #[test]
    fn find_ranks_the_name_first_then_names_then_comments() {
        let objects = [object("table", "revenue_daily", None), object("table", "revenue", None)];
        let got = found("revenue", None, &objects, &[column("amount", Some("revenue in euros"))]);
        let ranked: Vec<(i64, &str, &str)> = got.iter().map(|f| (f.rank, f.name.as_str(), f.matched)).collect();
        assert_eq!(ranked, [(1, "revenue", "name"), (2, "revenue_daily", "name"), (3, "amount", "comment")]);
    }

    #[test]
    fn find_keeps_one_kind_when_asked() {
        let parts = [column("revenue_total", None), column("amount", Some("revenue in euros"))];
        let got = found("revenue", Some("column"), &[object("table", "revenue", None)], &parts);
        let names: Vec<(&str, &str)> = got.iter().map(|f| (f.is, f.name.as_str())).collect();
        assert_eq!(names, [("part", "revenue_total"), ("part", "amount")]);
    }

    #[test]
    fn find_finds_a_concept_by_its_other_name() {
        let got = found("foreign key", None, &[], &[]);
        assert_eq!(got.len(), 1);
        assert_eq!((got[0].is, got[0].kind.as_str(), got[0].matched), ("part", "link", "also"));
    }

    #[test]
    fn find_wants_every_word() {
        let sales = Object { comment: Some("order total per day".into()), ..object("table", "sales", None) };
        let lines = object("table", "order_lines", None);
        let got = found("order total", None, &[sales, lines], &[]);
        assert_eq!(got.iter().map(|f| f.name.as_str()).collect::<Vec<_>>(), ["sales"]);
    }

    #[test]
    fn find_calls_are_read_whole_or_refused() {
        let sql = "SELECT * FROM pondra.find('it''s') WHERE x";
        let one = calls(sql).unwrap();
        assert_eq!((one.len(), &sql[one[0].0.clone()], one[0].1.as_str(), one[0].2.as_deref()), (1, "pondra.find('it''s')", "it's", None));
        let named = calls("SELECT * FROM pondra.find('margin', kind => 'column')").unwrap();
        assert_eq!((named[0].1.as_str(), named[0].2.as_deref()), ("margin", Some("column")));
        assert!(calls("SELECT * FROM pondra.find(margin)").unwrap_err().to_string().contains("pondra.find('words'"));
        assert!(calls("pondra.find('  ')").unwrap_err().to_string().contains("give the words"));
        assert!(calls("pondra.find('x', kind => 'nope')").unwrap_err().to_string().contains("no kind nope"));
        assert!(calls("SELECT 1").unwrap().is_empty());
    }

    #[test]
    fn find_as_a_tool_is_one_query() {
        let sql = |args: Value| mcp_sql("find", &args).map_err(|e| e.to_string());
        assert_eq!(sql(j!({"text": "it's", "kind": "column"})), Ok("SELECT * FROM pondra.find('it''s', kind => 'column')".to_string()));
        assert!(sql(j!({})).is_err());
        assert!(is_tool("find") && !is_tool("query"));
    }
}
