//! A WITH that writes, as Postgres runs one: `WITH moved AS (DELETE FROM orders WHERE status = 'done' RETURNING *) INSERT
//! INTO archive SELECT * FROM moved`, a write CTE whose rows the statement after it reads by its name, and a WITH before a
//! write, as `WITH src AS (SELECT …) INSERT INTO orders SELECT * FROM src`. The parts run in WITH order, each write once,
//! named or not, and the rows a write returns are bound by its name (`query::SENT`) for the parts after it. All of it is one
//! commit: in the caller's transaction, or one of its own (`txn::as_one`), so a part that fails writes nothing. Every part sees
//! the data as it was before the statement, as Postgres's one snapshot shows it, so a statement where a part reads or writes a
//! table that another write CTE writes is refused (0A000) before anything runs. Each part runs through `routines::one_of`, so
//! each write meets the checks a lone one does.

use crate::app::App;
use crate::returning;
use crate::routines::{self, Outcome, Who};
use crate::text::{ident, object, write_of};
use anyhow::Result;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::sql::sqlparser::ast::{self, FromTable, Query, SetExpr, Statement, TableFactor, TableObject, Visitor, With};
use std::ops::ControlFlow;
use std::sync::{Arc, LazyLock};

/// A write word as a whole word, read before anything is parsed: `updated_at` and `is_deleted` are no write.
static WRITE_WORD: LazyLock<regex::Regex> = LazyLock::new(|| regex::Regex::new(r"(?i)\b(insert|update|delete|merge)\b").expect("a regex"));

const NESTED: &str = "a write CTE inside a subquery or a CTE's own WITH isn't taken here: write it in the top WITH";
const MERGE_CTE: &str = "MERGE as a CTE isn't taken here: write the MERGE as the statement itself";
const RECURSIVE: &str = "a write CTE in WITH RECURSIVE isn't taken here: write it in a WITH that isn't RECURSIVE";
const LOCKS: &str = "FOR UPDATE or FOR SHARE in a WITH with a write isn't taken here: lock the rows in a SELECT of their own";
const NO_TABLE: &str = "a write in a WITH that writes something other than a table isn't taken here";

/// A WITH that writes, taken apart: its steps in order, then the rest (the main statement, with the steps taken out of its WITH).
pub struct WithWrites {
    steps: Vec<Step>,
    /// The main statement as the steps leave it: a query, or a write (`INSERT`, `UPDATE`, `DELETE` or `MERGE`).
    rest: String,
    /// Postgres's command tag for the answer: the main statement's, or `SELECT` for a query.
    tag: &'static str,
    /// What each part reads and writes, the steps' in order and the rest's last, as `clash` checks them.
    parts: Vec<Part>,
    /// The names the WITH gives its CTEs, as SQL reads them.
    names: Vec<String>,
    /// The statement as it was sent: the views it names are looked up in its words.
    sql: String,
}

/// One step, in the order they run: a write CTE, or a plain CTE that runs first.
enum Step {
    /// A write CTE: its statement (with its RETURNING, if any), the name its rows go by, its column list.
    Write { name: String, sql: String, returns: bool, columns: Vec<String> },
    /// A plain CTE run first, as `WITH [RECURSIVE] … SELECT * FROM name`, so a recursive one and its column list survive.
    Rows { name: String, sql: String },
}

/// What one part reads and writes, by table name: each name's last part, lowercased.
struct Part {
    reads: Vec<String>,
    write: Option<Write>,
}

/// The table a part writes, as SQL names it, and the words a refusal names the write by (`DELETE FROM orders`).
struct Write {
    table: String,
    said: String,
}

/// Counts a statement's queries that write, and whether any of them locks rows, as the walk meets them.
#[derive(Default)]
struct Scan {
    writes: usize,
    locks: bool,
}

impl Visitor for Scan {
    type Break = ();

    fn pre_visit_query(&mut self, q: &Query) -> ControlFlow<()> {
        self.writes += usize::from(write_of(q).is_some());
        self.locks |= !q.locks.is_empty();
        ControlFlow::Continue(())
    }
}

/// `sql` as a WITH that writes, or a WITH before a write: None when it is neither. Err: refused (0A000), with the words that
/// say why, before anything runs. Reads no table and no view (`run` checks those with the node): it parses and nothing more.
pub fn of(sql: &str) -> Option<Result<WithWrites>> {
    if crate::text::head(sql) != "with" || !WRITE_WORD.is_match(sql) {
        return None; // (nearly every statement: not parsed here)
    }
    match build(sql) {
        Ok(None) => None,
        Ok(Some(w)) => Some(Ok(w)),
        Err(e) => Some(Err(e)),
    }
}

/// `of`'s work: the statement taken apart, or None when it is neither a WITH that writes nor a WITH before a write.
fn build(sql: &str) -> Result<Option<WithWrites>> {
    let Some(Statement::Query(q)) = crate::pg_types::one(sql) else { return Ok(None) };
    let Some(with) = q.with.clone() else { return Ok(None) };
    let mut scan = Scan::default();
    let _ = crate::walk::visit(&*q, &mut scan);
    let cte_writes = with.cte_tables.iter().filter(|c| write_of(&c.query).is_some()).count();
    let top = cte_writes + usize::from(write_of(&q).is_some());
    if scan.writes > top {
        return Err(crate::codes::coded("0A000", NESTED));
    }
    if top == 0 {
        return Ok(None);
    }
    if with.cte_tables.iter().any(|c| write_of(&c.query) == Some("MERGE")) {
        return Err(crate::codes::coded("0A000", MERGE_CTE));
    }
    if with.recursive && cte_writes > 0 {
        return Err(crate::codes::coded("0A000", RECURSIVE));
    }
    if scan.locks {
        return Err(crate::codes::coded("0A000", LOCKS));
    }
    let quoted = |i: &ast::Ident| i.quote_style.is_some() && i.value != i.value.to_lowercase();
    if let Some(c) = with.cte_tables.iter().find(|c| quoted(&c.alias.name)) {
        return Err(crate::codes::coded("0A000", format!("WITH … CTE \"{}\" is quoted and isn't all lowercase: the session registers names in lowercase, so it would not be found: write the name in lowercase", c.alias.name.value)));
    }
    let body = write_stmt(&q);
    let last_write = with.cte_tables.iter().rposition(|c| write_of(&c.query).is_some());
    // (a write main that isn't `INSERT … SELECT` runs the plain CTEs after the last write first; the other mains keep them)
    let moved = matches!(body, Some(Statement::Insert(ins)) if ins.source.as_ref().is_some_and(|s| !matches!(*s.body, SetExpr::Values(_))));
    let tail_steps = body.is_some() && !moved;
    let (mut steps, mut parts, mut tail, mut names) = (vec![], vec![], vec![], vec![]);
    for (i, c) in with.cte_tables.iter().enumerate() {
        let name = ident(&c.alias.name);
        names.push(name.clone());
        let before_last = last_write.is_some_and(|k| i < k);
        let after_last = last_write.is_none_or(|k| i > k);
        match write_stmt(&c.query) {
            Some(stmt) => {
                let table = target(stmt).ok_or_else(|| crate::codes::coded("0A000", NO_TABLE))?;
                steps.push(Step::Write {
                    name,
                    sql: crate::text::sql(&Statement::Query(c.query.clone())), // (with its own WITH, if it has one)
                    returns: returns(stmt),
                    columns: c.alias.columns.iter().map(|d| ident(&d.name)).collect(),
                });
                parts.push(Part { reads: with_name(relations(&*c.query), &table), write: Some(Write { said: format!("{} {table}", word(stmt)), table }) });
            }
            None if before_last || (after_last && tail_steps) => {
                steps.push(Step::Rows { name, sql: format!("WITH {}{} SELECT * FROM {}", if with.recursive { "RECURSIVE " } else { "" }, c, c.alias.name) });
                parts.push(Part { reads: relations(&*c.query), write: None });
            }
            None if after_last => tail.push(c.clone()),
            None => {}
        }
    }
    let rest_stmt = match body {
        Some(Statement::Insert(ins)) if moved => {
            let mut ins = ins.clone();
            if let (false, Some(src)) = (tail.is_empty(), ins.source.as_mut()) {
                // (the plain CTEs go before any WITH the source has, as Postgres reads them: RECURSIVE kept)
                let own = src.with.take();
                let recursive = with.recursive || own.as_ref().is_some_and(|w| w.recursive);
                let mut cte_tables = tail.clone();
                cte_tables.extend(own.into_iter().flat_map(|w| w.cte_tables));
                src.with = Some(With { with_token: with.with_token.clone(), recursive, cte_tables });
            }
            Statement::Insert(ins)
        }
        Some(s) => s.clone(),
        None => {
            let mut q2 = (*q).clone();
            q2.with = (!tail.is_empty()).then(|| With { with_token: with.with_token.clone(), recursive: with.recursive, cte_tables: tail.clone() });
            Statement::Query(Box::new(q2))
        }
    };
    let rest_part = match target(&rest_stmt) {
        Some(table) => Part { reads: with_name(relations(&rest_stmt), &table), write: Some(Write { said: format!("{} {table}", word(&rest_stmt)), table }) },
        None => Part { reads: relations(&rest_stmt), write: None },
    };
    parts.push(rest_part);
    // (a CTE without RETURNING runs, but nothing may name it: its rows aren't there to read)
    for (i, s) in steps.iter().enumerate() {
        if let Step::Write { name, returns: false, .. } = s {
            if parts.iter().enumerate().any(|(j, p)| j != i && p.reads.contains(name)) {
                return Err(crate::codes::coded("0A000", format!("WITH query \"{name}\" does not have a RETURNING clause")));
            }
        }
    }
    let reads: Vec<Vec<String>> = parts.iter().map(|p| p.reads.clone()).collect();
    clash(&parts, &reads)?;
    let tag = match &rest_stmt {
        Statement::Insert(_) => "INSERT 0",
        Statement::Update(_) => "UPDATE",
        Statement::Delete(_) => "DELETE",
        Statement::Merge(_) => "MERGE",
        _ => "SELECT",
    };
    Ok(Some(WithWrites { steps, rest: crate::text::sql(&rest_stmt), tag, parts, names, sql: sql.to_string() }))
}

/// The check that no write CTE's table is read or written by another part, the main statement included, as the parts read it
/// (`reads`: through views too, when `run` asks). Each part sees the data as it was before the statement, so a part that changed
/// a table could not be read by another part. The main statement's own write is not checked as a writer: the steps before it
/// have run, and it reads its own target as a lone statement does.
fn clash(parts: &[Part], reads: &[Vec<String>]) -> Result<()> {
    for (i, p) in parts.iter().enumerate().take(parts.len().saturating_sub(1)) {
        let Some(w) = &p.write else { continue };
        let table = last(&w.table);
        if reads.iter().enumerate().any(|(j, r)| j != i && r.contains(&table)) {
            return Err(crate::codes::coded("0A000", format!("WITH … {}: another part of the statement reads or writes {}, which would see it as it was before the statement; read or change it in a statement after this one", w.said, w.table)));
        }
    }
    Ok(())
}

impl WithWrites {
    /// Postgres's command tag for the answer (`INSERT 0 3`, `UPDATE 2`, `SELECT 5`): the main statement's.
    pub fn tag(&self) -> &'static str { self.tag }

    /// Does the main statement write, so that its answer is a write's? (Its part names a table.)
    fn rest_writes(&self) -> bool { self.parts.last().is_some_and(|p| p.write.is_some()) }

    /// Each part's text, the steps' in order and the rest's last.
    fn texts(&self) -> Vec<&str> {
        self.steps.iter().map(|s| match s { Step::Write { sql, .. } | Step::Rows { sql, .. } => sql.as_str() }).chain(std::iter::once(self.rest.as_str())).collect()
    }
}

/// Runs a WITH that writes: the checks that need the node, then its steps and rest in one transaction (the caller's, or one of
/// its own: `txn::as_one`). A statement with no step to run first is a lone write, and runs as one.
pub async fn run(app: &App, w: &WithWrites, who: Who, job: Option<String>) -> Result<Outcome> {
    names(app, w).await?;
    if w.steps.is_empty() {
        return Box::pin(routines::one_of(app, &w.rest, who, job)).await; // (a lone write: its own checks, and its job's)
    }
    temps(w)?;
    clash(&w.parts, &reads_with_views(app, w).await?)?;
    if let (Some(j), false) = (&job, crate::txn::open()) {
        for p in &w.parts {
            if let Some(wr) = &p.write {
                let (other, name) = crate::ddl::resolve(&app.lake, &wr.table).await?;
                if other.is_none() && crate::txn::committed(&app.lake, j, &name).await? {
                    return Err(applied(j));
                }
            }
        }
    }
    if crate::txn::open() {
        return steps(app, w, who).await;
    }
    let out = crate::txn::as_one(app, job.clone(), move || steps(app, w, who)).await?;
    out.ok_or_else(|| applied(job.as_deref().unwrap_or_default()))
}

/// The columns the answer will have, worked out without writing (a Postgres Describe). Each plain CTE is planned, and each write
/// with RETURNING answers its columns, under the rows of the parts before it; the rest answers its planned columns (a query), its
/// RETURNING's (a write with one), or none (a write without one).
pub async fn columns(app: &App, w: &WithWrites) -> Result<Option<SchemaRef>> {
    let mut sent = crate::query::SENT.try_with(|t| t.as_ref().clone()).unwrap_or_default();
    for step in &w.steps {
        let scope = Arc::new(sent.clone());
        match step {
            Step::Write { name, sql, returns, columns: cols } => {
                if !*returns {
                    continue;
                }
                let Some(schema) = crate::query::SENT.scope(scope, write_columns(app, sql)).await? else { continue };
                sent.push((name.clone(), renamed(vec![RecordBatch::new_empty(schema)], name, cols)?));
            }
            Step::Rows { name, sql } => {
                let schema = crate::query::SENT.scope(scope, planned(app, sql)).await?;
                sent.push((name.clone(), vec![RecordBatch::new_empty(schema)]));
            }
        }
    }
    let scope = Arc::new(sent);
    crate::query::SENT.scope(scope, async {
        match w.rest_writes() {
            true => write_columns(app, &w.rest).await,
            false => Ok(Some(planned(app, &w.rest).await?)),
        }
    })
    .await
}

/// The statement's steps, in order, then its rest: each part's rows bound by its name (`query::SENT`) for the parts after it. It
/// runs in the transaction `run` gives it, so every part's writes land together, or none do.
async fn steps(app: &App, w: &WithWrites, who: Who) -> Result<Outcome> {
    let mut sent = crate::query::SENT.try_with(|t| t.as_ref().clone()).unwrap_or_default();
    for step in &w.steps {
        let scope = Arc::new(sent.clone());
        match step {
            Step::Write { name, sql, returns, columns } => {
                let out = crate::query::SENT.scope(scope, Box::pin(routines::one_of(app, sql, who, None))).await?;
                if *returns {
                    let rows = match out {
                        Outcome::Rows(rows) => rows,
                        Outcome::Done(_) => vec![],
                    };
                    sent.push((name.clone(), renamed(rows, name, columns)?));
                }
            }
            Step::Rows { name, sql } => {
                let rows = crate::query::SENT.scope(scope, rows_of(app, sql, who)).await?;
                sent.push((name.clone(), rows));
            }
        }
    }
    let scope = Arc::new(sent);
    crate::query::SENT.scope(scope, async {
        match w.rest_writes() {
            true => Box::pin(routines::one_of(app, &w.rest, who, None)).await,
            false => Ok(Outcome::Rows(rows_of(app, &w.rest, who).await?)),
        }
    })
    .await
}

/// A query's rows as `routines::one_of` answers one: with this node's files when its caller may read them (`files`).
async fn rows_of(app: &App, sql: &str, who: Who) -> Result<Vec<RecordBatch>> {
    match who.files {
        true => app.query_as(&crate::sys::planned(sql)?, Some("0"), true).await,
        false => app.query(sql, None).await,
    }
}

/// The rows of a write whose CTE has a column list (`m(a, b) AS (DELETE … RETURNING x, y)`), with those names: as Postgres renames
/// them, the first columns get the names and the rest keep theirs. More names than columns is refused (42601), as Postgres refuses it.
fn renamed(rows: Vec<RecordBatch>, name: &str, names: &[String]) -> Result<Vec<RecordBatch>> {
    if names.is_empty() {
        return Ok(rows);
    }
    rows.into_iter()
        .map(|b| {
            let schema = b.schema();
            if names.len() > schema.fields().len() {
                return Err(crate::codes::coded("42601", format!("WITH query \"{name}\" has {} columns available but {} columns specified", schema.fields().len(), names.len())));
            }
            let fields: Vec<_> = schema.fields().iter().enumerate().map(|(i, f)| match names.get(i) {
                Some(n) => f.as_ref().clone().with_name(n),
                None => f.as_ref().clone(),
            }).collect();
            Ok(RecordBatch::try_new(Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone())), b.columns().to_vec())?)
        })
        .collect()
}

/// The columns a write answers with, worked out without writing: a WITH's own (`columns`), a RETURNING's, and none for a write
/// without one.
async fn write_columns(app: &App, sql: &str) -> Result<Option<SchemaRef>> {
    if let Some(w) = of(sql) {
        return Box::pin(columns(app, &w?)).await;
    }
    match returning::of(sql) {
        Some(r) => Ok(Some(returning::columns(app, &r).await?)),
        None => Ok(None),
    }
}

/// The schema a query answers with, planned and not run (`flight::planned` does the same, and is private there).
async fn planned(app: &App, sql: &str) -> Result<SchemaRef> {
    let ctx = crate::query::session(&app.lake, sql, "").await?;
    let df = crate::engine::sql(&ctx, &crate::sys::planned(sql)?).await?;
    Ok(Arc::new(crate::answer::arrow_schema(crate::query::run_schema(&ctx, df.logical_plan())?.as_arrow())))
}

/// A CTE named like a temporary table or view, or a stored view, would be read in place of the CTE: refused, before anything runs.
async fn names(app: &App, w: &WithWrites) -> Result<()> {
    if w.names.is_empty() {
        return Ok(());
    }
    let (tables, views) = crate::temp::listed();
    let stored = crate::query::stored_views(&app.lake, &w.sql, false).await?;
    for n in &w.names {
        let temp = tables.iter().any(|(t, _)| t == n) || views.iter().any(|(v, _)| v == n);
        if temp || stored.iter().any(|(v, _)| v == n) {
            return Err(crate::codes::coded("0A000", format!("WITH … CTE \"{n}\" is a temporary table or view, or a stored view, and the statement would read it in place of the CTE: name the CTE something else")));
        }
    }
    Ok(())
}

/// A write into a temporary table lands at once, outside the transaction the steps share: refused, before anything runs.
fn temps(w: &WithWrites) -> Result<()> {
    let (tables, _) = crate::temp::listed();
    match w.parts.iter().filter_map(|p| p.write.as_ref()).find(|wr| tables.iter().any(|(t, _)| *t == wr.table)) {
        Some(wr) => Err(crate::codes::coded("0A000", format!("WITH … {}: it writes a temporary table, which would land at once, outside the transaction: write it in a statement of its own", wr.said))),
        None => Ok(()),
    }
}

/// What each part reads, as `clash` checks it: its own names, and those of the views it names (stored or temporary, and the views
/// those name), as the views' text reads them.
async fn reads_with_views(app: &App, w: &WithWrites) -> Result<Vec<Vec<String>>> {
    let mut out = vec![];
    for (p, text) in w.parts.iter().zip(w.texts()) {
        let temps = crate::temp::views(text);
        let mut all = text.to_string();
        for (_, v) in &temps {
            all.push(' ');
            all.push_str(v);
        }
        let stored = crate::query::stored_views(&app.lake, &all, false).await?;
        let mut reads = p.reads.clone();
        for v in temps.iter().map(|(_, v)| v).chain(stored.iter().map(|(_, v)| v)) {
            reads.extend(pg_relations(v));
        }
        out.push(reads);
    }
    Ok(out)
}

/// A job sent again, after its first send committed: refused before anything runs, as `returning::write` refuses one.
fn applied(job: &str) -> anyhow::Error {
    crate::codes::coded("55000", format!("WITH … with job {job} was applied already, by an earlier send of it: its rows went with that answer and aren't kept"))
}

/// The write a query is, as its statement (`INSERT`, `UPDATE`, `DELETE` or `MERGE`), or None for one that only reads.
fn write_stmt(q: &Query) -> Option<&Statement> {
    match &*q.body {
        SetExpr::Insert(s) | SetExpr::Update(s) | SetExpr::Delete(s) | SetExpr::Merge(s) => Some(s),
        _ => None,
    }
}

/// The table a write statement writes, as SQL names it (None for a write into something that isn't a table).
fn target(s: &Statement) -> Option<String> {
    let relation = match s {
        Statement::Insert(i) => {
            return match &i.table {
                TableObject::TableName(n) => Some(object(n)),
                _ => None,
            }
        }
        Statement::Update(u) => &u.table.relation,
        Statement::Delete(d) => {
            let (FromTable::WithFromKeyword(t) | FromTable::WithoutKeyword(t)) = &d.from;
            &t.first()?.relation
        }
        Statement::Merge(m) => &m.table,
        _ => return None,
    };
    match relation {
        TableFactor::Table { name, .. } => Some(object(name)),
        _ => None,
    }
}

/// The words a write statement opens with, as a refusal names it (`DELETE FROM orders`).
fn word(s: &Statement) -> &'static str {
    match s {
        Statement::Insert(_) => "INSERT INTO",
        Statement::Update(_) => "UPDATE",
        Statement::Delete(_) => "DELETE FROM",
        Statement::Merge(_) => "MERGE INTO",
        _ => "",
    }
}

/// Does a write statement answer rows: does it have a RETURNING?
fn returns(s: &Statement) -> bool {
    match s {
        Statement::Insert(i) => i.returning.is_some(),
        Statement::Update(u) => u.returning.is_some(),
        Statement::Delete(d) => d.returning.is_some(),
        _ => false,
    }
}

/// The names a query or statement reads, each by its last part, lowercased: what `walk::visit_relations` finds.
fn relations<V: ast::Visit>(node: &V) -> Vec<String> {
    let mut out = vec![];
    let _ = crate::walk::visit_relations(node, |n: &ast::ObjectName| {
        out.push(last(&object(n)));
        ControlFlow::<()>::Continue(())
    });
    out
}

/// The names a view's text reads (`relations`), read from the text as it is stored; none when the text won't parse.
fn pg_relations(text: &str) -> Vec<String> { crate::pg_types::one(text).map(|s| relations(&s)).unwrap_or_default() }

/// `reads` with `table`'s name added (a write reads its own target, as the statement names it).
fn with_name(mut reads: Vec<String>, table: &str) -> Vec<String> {
    reads.push(last(table));
    reads
}

/// A name's last part, lowercased: how the checks compare table names (`lake.schema.t` and `t` are one table here).
fn last(name: &str) -> String { name.rsplit('.').next().unwrap_or(name).to_lowercase() }

#[cfg(test)]
mod tests {
    use super::*;

    /// The names of the steps, in order.
    fn step_names(w: &WithWrites) -> Vec<&str> {
        w.steps.iter().map(|s| match s { Step::Write { name, .. } | Step::Rows { name, .. } => name.as_str() }).collect()
    }

    /// The code `sql` is refused with, or None when it is taken or is neither a WITH that writes nor one before a write.
    fn refused(sql: &str) -> Option<&'static str> {
        match of(sql)? {
            Err(e) => Some(crate::codes::of(&e)),
            Ok(_) => None,
        }
    }

    #[test]
    fn examples_split_into_steps_and_rest() {
        // (1) a write CTE moves rows into the statement after it
        let moved = of("WITH moved AS (DELETE FROM orders WHERE status = 'done' RETURNING *) INSERT INTO archive SELECT * FROM moved").unwrap().unwrap();
        assert_eq!(step_names(&moved), vec!["moved"]);
        assert!(matches!(&moved.steps[0], Step::Write { sql, returns: true, columns, .. } if sql.starts_with("DELETE FROM orders") && columns.is_empty()));
        assert!(moved.rest.starts_with("INSERT INTO archive") && moved.rest.contains("FROM moved"));
        assert_eq!(moved.tag(), "INSERT 0");
        // (2) two write CTEs, the second reading the first's rows, and a query over the second's
        let chained = of("WITH ins AS (INSERT INTO parent (name) VALUES ('a') RETURNING id), kid AS (INSERT INTO child SELECT id, 'x' FROM ins RETURNING *) SELECT count(*) AS n FROM kid").unwrap().unwrap();
        assert_eq!(step_names(&chained), vec!["ins", "kid"]);
        assert!(chained.rest.starts_with("SELECT") && chained.rest.contains("FROM kid"));
        assert_eq!(chained.tag(), "SELECT");
        // (3) a plain CTE with no write CTE: it moves into the INSERT's source, and no step runs first
        let src = of("WITH src AS (SELECT 20 AS id, 'new' AS status, 1 AS amount) INSERT INTO orders SELECT * FROM src").unwrap().unwrap();
        assert!(step_names(&src).is_empty());
        assert!(src.rest.starts_with("INSERT INTO orders") && src.rest.contains("WITH src AS") && src.rest.contains("SELECT * FROM src"));
        assert_eq!(src.tag(), "INSERT 0");
        // (4) a plain CTE before an UPDATE is a step, as a plain CTE before a write is
        let update = of("WITH c AS (SELECT 1 AS id) UPDATE orders SET status = 'seen' FROM c WHERE orders.id = c.id").unwrap().unwrap();
        assert_eq!(step_names(&update), vec!["c"]);
        assert!(matches!(&update.steps[0], Step::Rows { sql, .. } if sql.starts_with("WITH c AS") && sql.ends_with("SELECT * FROM c")));
        assert!(update.rest.starts_with("UPDATE orders"));
        // a plain CTE before a write keeps its column list
        let listed = of("WITH c(x) AS (SELECT 1) DELETE FROM orders WHERE id IN (SELECT x FROM c) RETURNING *").unwrap().unwrap();
        assert!(matches!(&listed.steps[0], Step::Rows { sql, .. } if sql.contains("c (x) AS")));
    }

    #[test]
    fn a_column_list_renames_and_a_write_without_returning_is_a_step() {
        let w = of("WITH m(k) AS (DELETE FROM archive WHERE id = 20 RETURNING id) SELECT k FROM m").unwrap().unwrap();
        assert!(matches!(&w.steps[0], Step::Write { columns, .. } if columns == &["k".to_string()]));
        let unnamed = of("WITH d AS (DELETE FROM orders WHERE id = 10) SELECT 1 AS one").unwrap().unwrap();
        assert!(matches!(&unnamed.steps[0], Step::Write { returns: false, .. }));
    }

    #[test]
    fn syntactic_refusals_are_0a000() {
        let cases = [
            ("WITH m AS (MERGE INTO orders USING src ON orders.id = src.id WHEN MATCHED THEN UPDATE SET status = src.status) SELECT * FROM m", "MERGE as a CTE"),
            ("WITH RECURSIVE d AS (DELETE FROM orders RETURNING *) SELECT * FROM d", "WITH RECURSIVE"),
            ("WITH c AS (SELECT * FROM (WITH d AS (DELETE FROM orders RETURNING *) SELECT * FROM d) AS x) SELECT * FROM c", "top WITH"),
            ("WITH d AS (WITH e AS (DELETE FROM archive RETURNING *) DELETE FROM orders RETURNING *) SELECT * FROM d", "top WITH"),
            ("WITH d AS (DELETE FROM orders WHERE id = 1) SELECT * FROM d", "does not have a RETURNING clause"),
            ("WITH d AS (DELETE FROM orders RETURNING *) SELECT * FROM d FOR UPDATE", "FOR UPDATE"),
            ("WITH \"Mixed\" AS (DELETE FROM orders RETURNING *) SELECT * FROM \"Mixed\"", "isn't all lowercase"),
            ("WITH d AS (DELETE FROM orders WHERE id = 3 RETURNING *) SELECT * FROM orders", "DELETE FROM orders"),
            ("WITH a AS (DELETE FROM orders RETURNING *), b AS (INSERT INTO orders VALUES (1, 'x', 1) RETURNING *) SELECT * FROM b", "DELETE FROM orders"),
            ("WITH d AS (DELETE FROM orders RETURNING *) INSERT INTO orders SELECT * FROM d", "DELETE FROM orders"),
        ];
        for (sql, words) in cases {
            let e = of(sql).unwrap_or_else(|| panic!("not refused: {sql}")).err().unwrap_or_else(|| panic!("taken: {sql}"));
            assert_eq!(crate::codes::of(&e), "0A000", "{sql}");
            assert!(e.to_string().contains(words), "{sql}: {e}");
        }
        assert_eq!(refused("WITH d AS (DELETE FROM orders WHERE id = 1 RETURNING *) SELECT * FROM d FOR SHARE"), Some("0A000"));
    }

    #[test]
    fn a_write_cte_may_read_its_own_table_and_plain_queries_are_neither() {
        assert_eq!(refused("WITH d AS (DELETE FROM orders WHERE id IN (SELECT id FROM orders) RETURNING *) SELECT * FROM d"), None);
        assert!(of("WITH d AS (DELETE FROM orders WHERE id IN (SELECT id FROM orders) RETURNING *) SELECT * FROM d").unwrap().is_ok());
        assert!(of("WITH c AS (SELECT 1) SELECT * FROM c").is_none());
        assert!(of("SELECT 1").is_none());
        assert!(of("WITH c AS (SELECT updated_at FROM t) SELECT * FROM c").is_none());
    }
}
