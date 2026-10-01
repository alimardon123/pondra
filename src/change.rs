//! UPDATE, DELETE and MERGE: rows that change (ADR-020).
//!
//! An append table's rows change by version. A new version keeps its row's `_row_id` and goes
//! into the table like any new row; the version it replaces goes into `{t}$deleted` (the table's
//! own, never listed), and reads leave out every row whose (`_row_id`, `_version`) is there
//! (`query::Pruned`). A keyed table's change is what it always was, an upsert: new versions, and
//! delete markers, keeping their rows' `_row_id`.
//!
//! The leader carries out a change under the lake's lock, from one snapshot of the lake, in one
//! commit: the new versions, the old ones, and what views derive from them. So a change applies
//! once (its job id), all or nothing, and streams like any other write.
use crate::log::{pack, Append, Outcome, Sequencer, Src};
use crate::store::*;
use crate::sys::{self, ROW_ID};
use anyhow::{bail, ensure, Context, Result};
use datafusion::arrow::array::{ArrayRef, BooleanArray};
use datafusion::arrow::compute::{cast, concat_batches};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::sql::sqlparser::ast;
use serde_json::{json as j, Value};
use std::sync::Arc;

/// `MERGE INTO t [AS a] USING s ON … WHEN …`, as SQL pieces. Postgres's `UPDATE … FROM`,
/// `DELETE … USING` and `INSERT … ON CONFLICT` are carried out as one too (`write::parse`).
pub struct Merge {
    pub sql: String,    // the statement as written
    pub target: String, // as SQL names it (`write::object`)
    alias: String,
    source: String, // the table or subquery, with its alias, as written
    on: String,
    clauses: Vec<Clause>,
    semi: bool, // DELETE … USING: a row matching several source rows is deleted once, not refused
    upsert: Option<Upsert>, // INSERT … ON CONFLICT: its clauses once the table's columns are known
}

/// `INSERT INTO t [(columns)] query ON CONFLICT [(on)] DO NOTHING | DO UPDATE SET … [WHERE …]`.
pub struct Upsert {
    pub columns: Vec<String>,                               // none: the table's, in order
    pub query: String,                                      // the rows
    pub on: Vec<String>,                                    // none: the table's key
    pub update: Option<(Vec<(String, String)>, Option<String>)>, // DO UPDATE's column = expression, WHERE (None: DO NOTHING)
}

struct Clause {
    kind: Kind,
    when: Option<String>,
    action: Action,
}

#[derive(PartialEq, Clone, Copy)]
enum Kind {
    Matched,
    NotMatched,         // a source row no target row matches
    NotMatchedBySource, // a target row no source row matches
}

enum Action {
    Update(Vec<(String, String)>), // column = expression
    Delete,
    Insert(Vec<String>, Vec<String>), // columns (none: all, in order), values
}

impl Merge {
    /// The privileges on its table it needs: what its clauses do (`auth::allows`).
    pub fn privileges(&self) -> Vec<&'static str> {
        let mut all: Vec<&'static str> = self.clauses.iter().map(|c| match c.action {
            Action::Update(_) => "update",
            Action::Delete => "delete",
            Action::Insert(..) => "insert",
        }).collect();
        if let Some(u) = &self.upsert {
            all.push("insert");
            if u.update.is_some() {
                all.push("update");
            }
        }
        all.sort();
        all.dedup();
        all
    }
}

/// A MERGE statement's pieces (None: not one Pondra takes).
/// What a MERGE can't mean, said before it runs: no WHEN clause, a column set or inserted twice,
/// a SET of the source's column, a source named as the target, Oracle's `… WHERE` after an action.
pub fn merge_refused(m: &ast::Merge) -> Option<String> {
    let ast::TableFactor::Table { name, alias, .. } = &m.table else { return None };
    let target = alias.as_ref().map(|a| a.name.value.to_lowercase()).unwrap_or_else(|| name.0.last().map(|p| p.to_string().to_lowercase()).unwrap_or_default());
    let source = match &m.source {
        ast::TableFactor::Table { name, alias, .. } => Some(alias.as_ref().map(|a| a.name.value.to_lowercase()).unwrap_or_else(|| name.0.last().map(|p| p.to_string().to_lowercase()).unwrap_or_default())),
        ast::TableFactor::Derived { alias: Some(a), .. } => Some(a.name.value.to_lowercase()),
        _ => None,
    };
    if m.clauses.is_empty() {
        return Some("MERGE … ON …: and then? WHEN MATCHED THEN UPDATE SET … | DELETE, WHEN NOT MATCHED THEN INSERT …".into());
    }
    if source.as_deref() == Some(target.as_str()) {
        return Some(format!("MERGE: the source is named {target}, as the target is: name one of them otherwise (USING s AS src)"));
    }
    let twice = |names: Vec<String>| names.iter().enumerate().find(|(i, n)| names[..*i].contains(n)).map(|(_, n)| n.clone());
    for c in &m.clauses {
        match &c.action {
            ast::MergeAction::Update(u) => {
                if u.update_predicate.is_some() || u.delete_predicate.is_some() {
                    return Some("MERGE … UPDATE SET … WHERE: put the condition in the clause, WHEN MATCHED AND … THEN UPDATE SET …".into());
                }
                let set: Vec<(Option<String>, String)> = u.assignments.iter().filter_map(|a| match &a.target {
                    ast::AssignmentTarget::ColumnName(n) => {
                        let parts: Vec<String> = n.0.iter().map(|p| p.to_string().trim_matches('"').to_lowercase()).collect();
                        Some((parts.len().checked_sub(2).map(|i| parts[i].clone()), parts.last()?.clone()))
                    }
                    _ => None,
                }).collect();
                if let Some((Some(q), c)) = set.iter().find(|(q, _)| q.as_ref().is_some_and(|q| *q != target)) {
                    return Some(format!("MERGE … UPDATE SET {q}.{c}: only the target's columns are set ({target}.{c}, or {c})"));
                }
                if let Some(c) = twice(set.into_iter().map(|(_, c)| c).collect()) {
                    return Some(format!("MERGE … UPDATE SET {c} = …, {c} = …: one value a column"));
                }
            }
            ast::MergeAction::Insert(i) => {
                if i.insert_predicate.is_some() {
                    return Some("MERGE … INSERT … WHERE: put the condition in the clause, WHEN NOT MATCHED AND … THEN INSERT …".into());
                }
                if let Some(c) = twice(i.columns.iter().map(|n| n.to_string().trim_matches('"').to_lowercase()).collect()) {
                    return Some(format!("MERGE … INSERT ({c}, …, {c}): each column once"));
                }
            }
            ast::MergeAction::Delete { .. } => {}
        }
    }
    None
}

pub fn merge_of(m: &ast::Merge) -> Option<Merge> {
    let ast::TableFactor::Table { name, alias, .. } = &m.table else { return None };
    let target = crate::write::object(name);
    let alias = alias.as_ref().map(|a| crate::write::ident(&a.name)).unwrap_or_else(|| target.rsplit('.').next().unwrap_or(&target).to_string()); // (unquoted: lower case, as SQL reads it)
    let column = |o: &ast::ObjectName| o.0.last().and_then(|p| p.as_ident()).map(|i| if i.quote_style.is_some() { i.value.clone() } else { i.value.to_lowercase() });
    let mut clauses = vec![];
    for c in &m.clauses {
        let kind = match c.clause_kind {
            ast::MergeClauseKind::Matched => Kind::Matched,
            ast::MergeClauseKind::NotMatched | ast::MergeClauseKind::NotMatchedByTarget => Kind::NotMatched,
            ast::MergeClauseKind::NotMatchedBySource => Kind::NotMatchedBySource,
        };
        let action = match &c.action {
            ast::MergeAction::Delete { .. } => Action::Delete,
            ast::MergeAction::Update(u) => Action::Update(u.assignments.iter().map(|a| match &a.target {
                ast::AssignmentTarget::ColumnName(n) => Some((column(n)?, a.value.to_string())),
                _ => None,
            }).collect::<Option<_>>()?),
            ast::MergeAction::Insert(i) => match &i.kind {
                ast::MergeInsertKind::Values(v) if v.rows.len() == 1 => Action::Insert(i.columns.iter().map(column).collect::<Option<_>>()?, v.rows[0].content.iter().map(|e| e.to_string()).collect()),
                _ => return None,
            },
        };
        clauses.push(Clause { kind, when: c.predicate.as_ref().map(|p| p.to_string()), action });
    }
    Some(Merge { sql: m.to_string(), target, alias, source: m.source.to_string(), on: m.on.to_string(), clauses, semi: false, upsert: None })
}

/// `UPDATE t [AS a] SET … FROM s WHERE …` and `DELETE FROM t [AS a] USING s WHERE …` as the MERGE
/// they are (`semi`: a DELETE's, which a row matching two source rows doesn't refuse); `sql`: the
/// statement as written, which the leader parses again.
pub fn merge_from(target: &ast::TableFactor, source: &ast::TableFactor, cond: Option<&ast::Expr>, action: &str, semi: bool, sql: String) -> Option<Merge> {
    use datafusion::sql::sqlparser::{dialect::GenericDialect, parser::Parser};
    let on = cond.map_or("true".to_string(), |c| c.to_string());
    let merge = format!("MERGE INTO {target} USING {source} ON {on} WHEN MATCHED THEN {action}");
    let ast::Statement::Merge(m) = Parser::parse_sql(&GenericDialect {}, &merge).ok()?.pop()? else { return None };
    Some(Merge { sql, semi, ..merge_of(&m)? })
}

/// `INSERT … ON CONFLICT`, to be made a MERGE once the table is known (`Upsert::merge`).
pub fn upsert_of(target: String, upsert: Upsert, sql: String) -> Merge {
    let alias = target.rsplit('.').next().unwrap_or(&target).to_string();
    Merge { sql, target, alias, source: String::new(), on: String::new(), clauses: vec![], semi: false, upsert: Some(upsert) }
}

impl Upsert {
    /// The MERGE this is, for a table of these columns: a row whose `on` columns match one of the
    /// table's updates it (DO UPDATE) or is skipped (DO NOTHING); the others are inserted.
    fn merge(&self, meta: &TableMeta, m: &Merge) -> Result<Merge> {
        let columns: Vec<String> = match self.columns.is_empty() {
            true => meta.columns.iter().map(|(c, _)| c.clone()).filter(|c| c != "_deleted").collect(),
            false => self.columns.clone(),
        };
        let on = if self.on.is_empty() { meta.key.clone() } else { self.on.clone() };
        ensure!(!on.is_empty(), "INSERT … ON CONFLICT: name the columns a conflict is on (ON CONFLICT (id) …), or give {} a PRIMARY KEY", m.target);
        ensure!(on.iter().all(|c| columns.contains(c)), "INSERT … ON CONFLICT ({}): the rows must give those columns", on.join(", "));
        let (a, x) = (q(&m.alias), "excluded");
        let source = format!("({}) AS {x} ({})", self.query, columns.iter().map(|c| q(c)).collect::<Vec<_>>().join(", "));
        let on_sql = on.iter().map(|c| format!("{a}.{} = {x}.{}", q(c), q(c))).collect::<Vec<_>>().join(" AND ");
        let insert = Clause { kind: Kind::NotMatched, when: None, action: Action::Insert(columns.clone(), columns.iter().map(|c| format!("{x}.{}", q(c))).collect()) };
        let mut clauses = vec![];
        if let Some((set, when)) = &self.update {
            clauses.push(Clause { kind: Kind::Matched, when: when.clone(), action: Action::Update(set.clone()) });
        }
        clauses.push(insert);
        Ok(Merge { sql: m.sql.clone(), target: m.target.clone(), alias: m.alias.clone(), source, on: on_sql, clauses, semi: false, upsert: None })
    }
}

fn q(c: &str) -> String { format!("\"{}\"", c.replace('"', "\"\"")) }

tokio::task_local! {
    /// The change's SQL may read files on this machine (`MERGE … USING 'new.csv'` from the shell's
    /// own node: `server::owner`).
    pub static FILES: bool;
}

/// A session for the change's queries, as of `upto`.
async fn session(lake: &Lake, sql: &str, upto: u64) -> Result<datafusion::prelude::SessionContext> {
    let ctx = crate::query::session_at(lake, sql, "", Some(upto)).await?;
    Ok(if FILES.try_with(|f| *f).unwrap_or(false) { ctx.enable_url_table() } else { ctx })
}

/// Leader: carry out an UPDATE or DELETE of an append table, or a MERGE into any table (`sql`),
/// under the lake's lock. A retried `job` changes nothing twice.
pub async fn run(lake: &Lake, seq: &Sequencer, sql: &str, job: &str) -> Result<Value> {
    let stmt = crate::write::parse(sql).context("not an UPDATE, DELETE or MERGE")?;
    let (other, table) = crate::ddl::resolve(lake, &stmt.table()).await?;
    if let Some(other) = other {
        let name = lake.attached.read().unwrap().iter().find(|(_, o)| std::sync::Arc::ptr_eq(o, &other)).map(|(n, _)| n.clone());
        bail!("{} is in attached lake {}: UPDATE, DELETE and MERGE run on that lake's own node for now (pondra {}); INSERT works from here",
              stmt.table(), name.unwrap_or_else(|| crate::ddl::lake_name(&other)), other.url);
    }
    let stored: TableMeta = lake.cat.get(&table_key(&table)).await?.with_context(|| format!("no table {table}"))?;
    let meta = stored.logical(); // (the change's SQL names columns as SQL knows them: ADR-022)
    let view = lake.cat.get::<crate::views::View>(&crate::views::view_key(&table)).await?;
    ensure!(view.is_none() && meta.merge.is_empty(), "{table} is a view's: change the table it follows");
    let append = meta.key.is_empty();
    ensure!(meta.ids || !append, "{table} holds rows from before row ids (made before Pondra 0.19): copy it once (CREATE TABLE t2 AS SELECT * FROM {table}) and change the copy");
    crate::views::can_follow(lake, &table, append).await?;
    if let Some(v) = crate::views::filling(lake, &table).await? {
        bail!("materialized view {v} is still being filled from {table}'s rows: change them once it is (in a moment)"); // (it reads them as they were)
    }
    let upto = lake.visible(); // (one snapshot for every query below: the lock keeps its files)
    let point = match &stmt {
        crate::write::Stmt::Update(_, set, Some(cond)) if !append => crate::txn::point_change(lake, &table, &stored, None, set, cond).await?, // (one key: no planning, ADR-036 §6)
        _ => None,
    };
    let (old, new) = match point {
        Some(p) => p,
        None => rows_of(lake, &table, &meta, &stmt, upto).await?,
    };
    commit(lake, seq, &table, &stored, old, new, job).await
}

/// What an UPDATE, DELETE or MERGE of `table` changes, as of commit `upto`: the old versions of
/// the rows it replaces (with their `_row_id`, `_created_at` and `_version`), and the new versions
/// (keeping their `_row_id` and `_created_at`) and new rows. A temporary table's too (`temp.rs`).
pub async fn rows_of(lake: &Lake, table: &str, meta: &TableMeta, stmt: &crate::write::Stmt, upto: u64) -> Result<(Vec<RecordBatch>, Vec<RecordBatch>)> {
    Ok(match stmt {
        crate::write::Stmt::Update(_, set, cond) => update(lake, table, meta, set, cond, upto).await?,
        crate::write::Stmt::Delete(_, cond) => (select(lake, table, meta, &[], &format!("FROM {} {}", from(table), filter(cond)), upto).await?.0, vec![]),
        crate::write::Stmt::Merge(m) => merge(lake, table, meta, m, upto).await?,
        _ => bail!("not an UPDATE, DELETE or MERGE"),
    })
}

fn from(table: &str) -> String { format!("{} AS {}", crate::write::sql_name(table), q(table.rsplit('.').next().unwrap_or(table))) }

fn filter(cond: &Option<String>) -> String { cond.as_ref().map(|w| format!("WHERE {w}")).unwrap_or_default() }

async fn update(lake: &Lake, table: &str, meta: &TableMeta, set: &[(String, String)], cond: &Option<String>, upto: u64) -> Result<(Vec<RecordBatch>, Vec<RecordBatch>)> {
    ensure!(set.iter().all(|(c, _)| meta.columns.iter().any(|(n, _)| n == c) && c != "_deleted"), "UPDATE sets the table's own columns");
    ensure!(set.iter().all(|(c, _)| !sys::NAMES.contains(&c.as_str())), "system columns can't be set");
    select(lake, table, meta, set, &format!("FROM {} {}", from(table), filter(cond)), upto).await
}

/// The old versions of the rows `rest` (FROM … WHERE …) selects, and, with `set`, their new ones.
async fn select(lake: &Lake, table: &str, meta: &TableMeta, set: &[(String, String)], rest: &str, upto: u64) -> Result<(Vec<RecordBatch>, Vec<RecordBatch>)> {
    select_as(lake, meta, Some(set).filter(|s| !s.is_empty()), None, rest, upto, &q(table.rsplit('.').next().unwrap_or(table))).await
}

/// One query of a change: target rows (old versions: `a`'s columns), and their new versions
/// (`set`) or new rows (`insert`: values for the table's columns).
#[allow(clippy::too_many_arguments)]
async fn select_as(lake: &Lake, meta: &TableMeta, set: Option<&[(String, String)]>, insert: Option<&[String]>, rest: &str, upto: u64, a: &str) -> Result<(Vec<RecordBatch>, Vec<RecordBatch>)> {
    let cols: Vec<&String> = meta.columns.iter().map(|(c, _)| c).collect();
    let mut items = vec![];
    if insert.is_none() {
        items.extend(cols.iter().map(|c| format!("{a}.{} AS {}", q(c), q(&format!("__o_{c}")))));
        items.extend([ROW_ID, sys::CREATED, sys::VERSION].iter().map(|c| format!("{a}.{} AS {}", q(c), q(&format!("__o{c}")))));
    }
    if let Some(set) = set {
        items.extend(cols.iter().map(|c| match set.iter().find(|(s, _)| s == *c) {
            Some((_, e)) => format!("({e}) AS {}", q(&format!("__n_{c}"))),
            None if *c == "_deleted" => format!("false AS {}", q("__n__deleted")), // (a keyed table's row stays)
            None => format!("{a}.{} AS {}", q(c), q(&format!("__n_{c}"))),
        }));
    }
    if let Some(values) = insert {
        items.extend(cols.iter().zip(values).map(|(c, v)| format!("({v}) AS {}", q(&format!("__n_{c}")))));
    }
    let sql = format!("SELECT {} {rest}", items.join(", "));
    let ctx = session(lake, &sql, upto).await?;
    let batches = ctx.sql(&crate::asof::as_of(&sql)?).await?.collect().await?;
    let Some(first) = batches.first() else { return Ok((vec![], vec![])) };
    let all = concat_batches(&first.schema(), &batches)?;
    let old = if insert.is_none() { vec![part(&all, meta, "__o_", true)?] } else { vec![] };
    let new = if set.is_some() || insert.is_some() { vec![part(&all, meta, "__n_", insert.is_none())?] } else { vec![] };
    Ok((old, new))
}

/// The table's columns from the query's `prefix` ones, cast to their types, and (`ids`) the rows'
/// `_row_id`, `_created_at` (and, for old versions, `_version`) after them.
fn part(all: &RecordBatch, meta: &TableMeta, prefix: &str, ids: bool) -> Result<RecordBatch> {
    let target = crate::query::schema(&meta.columns)?;
    let get = |name: &str| all.column_by_name(name).cloned().with_context(|| format!("{name}?"));
    let mut fields: Vec<Arc<Field>> = target.fields().to_vec();
    let mut columns = target.fields().iter().map(|f| Ok(cast(&get(&format!("{prefix}{}", f.name()))?, f.data_type())?)).collect::<Result<Vec<ArrayRef>>>()?;
    if ids {
        let old = if prefix == "__o_" { vec![sys::VERSION] } else { vec![] };
        for c in [ROW_ID, sys::CREATED].into_iter().chain(old) {
            let v = get(&format!("__o{c}"))?;
            fields.push(Arc::new(Field::new(c, v.data_type().clone(), true)));
            columns.push(v);
        }
    }
    Ok(RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)?)
}

async fn merge(lake: &Lake, table: &str, meta: &TableMeta, m: &Merge, upto: u64) -> Result<(Vec<RecordBatch>, Vec<RecordBatch>)> {
    let what = if m.upsert.is_some() { "INSERT … ON CONFLICT" } else { "MERGE" };
    let built;
    let m = match &m.upsert {
        Some(u) => {
            built = u.merge(meta, m)?;
            &built
        }
        None => m,
    };
    let (t, a, s, on) = (format!("{} AS {}", crate::write::sql_name(table), q(&m.alias)), q(&m.alias), &m.source, &m.on);
    // Each target row takes at most one source row (else which one's values?). A DELETE … USING
    // deletes a row once however many match it, as Postgres does.
    if !m.semi {
        let twice = format!("SELECT {a}.{} FROM {t} JOIN {s} ON {on} GROUP BY 1 HAVING count(*) > 1 LIMIT 1", q(ROW_ID));
        let ctx = session(lake, &twice, upto).await?;
        ensure!(ctx.sql(&twice).await?.count().await? == 0, "{what}: a row of {table} matches more than one source row");
    }
    let (mut old, mut new) = (vec![], vec![]);
    for (i, c) in m.clauses.iter().enumerate() {
        // A row takes the first clause of its kind whose condition holds.
        let earlier: Vec<String> = m.clauses[..i].iter().filter(|e| e.kind == c.kind).map(|e| format!(" AND NOT coalesce(({}), false)", e.when.as_deref().unwrap_or("true"))).collect();
        let when = format!("coalesce(({}), false){}", c.when.as_deref().unwrap_or("true"), earlier.concat());
        let rest = match c.kind {
            Kind::Matched if m.semi => format!("FROM {t} WHERE EXISTS (SELECT 1 FROM {s} WHERE {on}) AND {when}"),
            Kind::Matched => format!("FROM {t} JOIN {s} ON {on} WHERE {when}"),
            Kind::NotMatched => format!("FROM {s} WHERE NOT EXISTS (SELECT 1 FROM {t} WHERE {on}) AND {when}"),
            Kind::NotMatchedBySource => format!("FROM {t} WHERE NOT EXISTS (SELECT 1 FROM {s} WHERE {on}) AND {when}"),
        };
        let (o, n) = match (&c.action, c.kind) {
            (Action::Insert(..), Kind::Matched | Kind::NotMatchedBySource) => bail!("MERGE: INSERT goes with WHEN NOT MATCHED"),
            (Action::Update(_) | Action::Delete, Kind::NotMatched) => bail!("MERGE: WHEN NOT MATCHED takes an INSERT"),
            (Action::Update(set), _) => {
                ensure!(set.iter().all(|(c, _)| meta.columns.iter().any(|(n, _)| n == c) && !meta.key.contains(c) && !sys::NAMES.contains(&c.as_str())), "MERGE: UPDATE sets the table's own columns (not its key)");
                select_as(lake, meta, Some(set), None, &rest, upto, &a).await?
            }
            (Action::Delete, _) => select_as(lake, meta, None, None, &rest, upto, &a).await?,
            (Action::Insert(columns, values), _) => {
                let given = if columns.is_empty() { meta.columns.iter().map(|(c, _)| c.clone()).filter(|c| c != "_deleted").collect() } else { columns.clone() };
                ensure!(given.len() == values.len(), "MERGE: INSERT gives {} values for {} columns", values.len(), given.len());
                ensure!(given.iter().all(|c| meta.columns.iter().any(|(n, _)| n == c)), "MERGE: INSERT names the table's own columns");
                let values: Vec<String> = meta.columns.iter().map(|(c, _)| given.iter().position(|g| g == c).map_or("NULL".into(), |i| values[i].clone())).collect();
                select_as(lake, meta, None, Some(&values), &rest, upto, &a).await?
            }
        };
        old.extend(o);
        new.extend(n);
    }
    Ok((old, new))
}

/// The change as one commit: new versions and new rows into the table; the old versions into
/// `{t}$deleted` (an append table) or as delete markers (a keyed one). `meta` as stored; the rows
/// are under their SQL names (the log takes them so: `log::pack`).
async fn commit(lake: &Lake, seq: &Sequencer, table: &str, meta: &TableMeta, old: Vec<RecordBatch>, new: Vec<RecordBatch>, job: &str) -> Result<Value> {
    let (pending, (replaced, added, inserted)) = appends(lake, seq, table, meta, old, new, &format!("sql:{job}")).await?;
    if pending.is_empty() {
        return Ok(j!({"rows": 0}));
    }
    Ok(match submit(lake, seq, &pending).await? {
        Outcome::Acks(acks) if acks.iter().all(|a| !a.duplicate) => j!({"rows": replaced + inserted, "updated": added - inserted, "deleted": replaced + inserted - added, "inserted": inserted}),
        _ => j!({"duplicate": true}), // (this job's change is already in)
    })
}

/// Appends as one flush, packed again while the views change under it.
pub async fn submit(lake: &Lake, seq: &Sequencer, pending: &[Append]) -> Result<Outcome> {
    loop {
        match seq.submit(pack(lake, pending).await?).await? {
            Outcome::Retry(r) if r.is_empty() => tokio::time::sleep(std::time::Duration::from_millis(10)).await, // (views changed: pack again)
            o => return Ok(o),
        }
    }
}

/// A change of `table` as appends, stamped (producer `{producer}` and `{producer}:deleted`, seq 1):
/// and its counts, (rows replaced, rows added, of them new). None to append: nothing changes.
#[allow(clippy::type_complexity)]
pub async fn appends(lake: &Lake, seq: &Sequencer, table: &str, meta: &TableMeta, old: Vec<RecordBatch>, new: Vec<RecordBatch>, producer: &str) -> Result<(Vec<Append>, (usize, usize, usize))> {
    let rows = |b: &[RecordBatch]| b.iter().map(|b| b.num_rows()).sum::<usize>();
    let (replaced, added) = (rows(&old), rows(&new));
    if replaced + added == 0 {
        return Ok((vec![], (0, 0, 0)));
    }
    // (new versions carry their ids, new rows don't yet: one schema for both)
    let one = |b: Vec<RecordBatch>, s: SchemaRef| -> Result<Option<RecordBatch>> {
        match b.is_empty() {
            true => Ok(None),
            false => Ok(Some(concat_batches(&s, &b.iter().map(|b| crate::query::conform(b, &s)).collect::<Result<Vec<_>>>()?)?)),
        }
    };
    let ids = with_ids(&crate::query::schema(&meta.logical().columns)?);
    let old = match old.first().map(|f| f.schema()) {
        Some(s) => one(old, s)?,
        None => None,
    };
    let new = one(new, ids.clone())?;
    if let Some(n) = &new {
        crate::defaults::check(meta, table, n)?; // (an UPDATE may not empty a NOT NULL column)
    }
    let inserted = new.as_ref().and_then(|n| Some(n.column_by_name(ROW_ID)?.null_count())).unwrap_or(0); // (new rows: no id yet)
    let src = |p: &str| Src { producer: format!("{producer}{p}"), seq: 1, prev: None };
    let append = |table: &str, batch: RecordBatch, src: Src| Append { table: table.into(), src, batch, ack: tokio::sync::oneshot::channel().0 };
    let mut pending = vec![];
    if meta.key.is_empty() {
        if old.is_some() {
            companion(lake, table, meta).await?;
            for (view, vmeta) in crate::views::row_views(lake, table).await? {
                companion(lake, &view, &vmeta).await?; // (its rows of the old ones go there: `views::derive`)
            }
        }
        pending.push(append(table, new.unwrap_or_else(|| RecordBatch::new_empty(ids)), src(""))); // (empty: its producer's seq still advances)
        if let Some(old) = old {
            pending.push(append(&sys::deleted(table), as_deleted(&old)?, src(":deleted")));
        }
    } else {
        // Keyed: delete markers are the old rows no new version replaces, marked.
        let kept: std::collections::HashSet<i64> = new.iter().flat_map(|n| ids_of(n)).flatten().collect();
        let gone = |o: &RecordBatch| -> Result<RecordBatch> {
            let keep = BooleanArray::from(ids_of(o).iter().map(|id| Some(!id.is_some_and(|i| kept.contains(&i)))).collect::<Vec<_>>());
            Ok(datafusion::arrow::compute::filter_record_batch(o, &keep)?)
        };
        let marked = old.map(|o| gone(&o)).transpose()?.map(|o| mark_deleted(&o)).transpose()?.map(|o| without(&o, sys::VERSION)).transpose()?;
        let all: Vec<RecordBatch> = new.into_iter().chain(marked).collect();
        let s = all[0].schema();
        pending.push(append(table, concat_batches(&s, &all.iter().map(|b| crate::query::conform(b, &s)).collect::<Result<Vec<_>>>()?)?, src("")));
    }
    for a in pending.iter_mut().filter(|a| a.batch.num_rows() > 0) {
        let first = loop {
            if let Some(f) = lake.ids.take(a.batch.num_rows() as u64) {
                break f;
            }
            lake.ids.refill(seq.block().await?);
        };
        a.batch = sys::stamp(&a.batch, first)?;
    }
    Ok((pending, (replaced, added, inserted)))
}

/// A batch's `_row_id`s.
fn ids_of(b: &RecordBatch) -> Vec<Option<i64>> {
    use datafusion::arrow::array::AsArray;
    b.column_by_name(ROW_ID).map(|c| c.as_primitive::<datafusion::arrow::datatypes::Int64Type>().iter().collect()).unwrap_or_default()
}

/// A table's columns with the ids a changed row keeps (`_row_id`, `_created_at`).
fn with_ids(s: &SchemaRef) -> SchemaRef {
    let mut f = s.fields().to_vec();
    f.push(Arc::new(Field::new(ROW_ID, DataType::Int64, true)));
    f.push(Arc::new(Field::new(sys::CREATED, crate::query::dtype(&sys::columns()[2].1).expect("a type"), true)));
    Arc::new(Schema::new(f))
}

fn mark_deleted(b: &RecordBatch) -> Result<RecordBatch> {
    let i = b.schema().index_of("_deleted").context("DELETE on a keyed table needs its Boolean _deleted column")?;
    let mut cols = b.columns().to_vec();
    cols[i] = Arc::new(BooleanArray::from(vec![true; b.num_rows()]));
    Ok(RecordBatch::try_new(b.schema(), cols)?)
}

fn without(b: &RecordBatch, name: &str) -> Result<RecordBatch> {
    let keep: Vec<usize> = (0..b.num_columns()).filter(|&i| b.schema().field(i).name() != name).collect();
    Ok(b.project(&keep)?)
}

/// An append table's `{t}$deleted`, made the first time its rows change: its columns, and the
/// version each old row had (`_version`, which it keeps as `_old_version`: its own is the change's).
pub async fn companion(lake: &Lake, table: &str, meta: &TableMeta) -> Result<()> {
    if meta.changed {
        return Ok(());
    }
    let mut columns = meta.columns.clone();
    columns.push(("_old_version".into(), "Int64".into()));
    let deleted = TableMeta { columns, tiered: lake.visible(), ids: true, names: meta.names.clone(), dropped: meta.dropped.clone(), ..Default::default() };
    let changed = TableMeta { changed: true, ..meta.clone() };
    lake.cat.commit(vec![(table_key(&sys::deleted(table)), json(&deleted)), (table_key(table), json(&changed))], &[]).await
}

/// The old rows as `{t}$deleted` holds them: `_version` renamed `_old_version`.
pub fn as_deleted(b: &RecordBatch) -> Result<RecordBatch> {
    let fields = b.schema().fields().iter().map(|f| match f.name() == sys::VERSION {
        true => Arc::new(Field::new("_old_version", f.data_type().clone(), true)),
        false => f.clone(),
    }).collect::<Vec<_>>();
    Ok(RecordBatch::try_new(Arc::new(Schema::new(fields)), b.columns().to_vec())?)
}

/// A table's changes committed in (after, upto], as Delta's change data feed gives them: each row
/// with its system columns and `_change_type`: `insert`, `update_preimage` and `update_postimage`
/// (a row's old and new versions), `delete` — and, for a keyed table, `upsert` and `delete`.
/// Commit by commit, old versions first; `_version` and `_updated_at` are the change's commit.
pub async fn feed(lake: &Lake, table: &str, after: u64, upto: Option<u64>) -> Result<Vec<RecordBatch>> {
    use datafusion::arrow::array::{Array, AsArray, StringArray};
    use datafusion::arrow::datatypes::Int64Type;
    use std::collections::HashSet;
    let meta: TableMeta = lake.cat.get(&table_key(table)).await?.with_context(|| format!("no table {table}"))?;
    let rows = crate::query::tail_of(lake, table, after, upto, false, true, true).await?;
    let mut gone = match meta.changed {
        true => crate::query::tail_of(lake, &sys::deleted(table), after, upto, false, true, false).await?,
        false => vec![],
    };
    gone.extend(taken_out(lake, table, after, upto).await?);
    let key = |b: &RecordBatch, i: usize| {
        let col = |c: &str| b.column_by_name(c).expect("a system column").as_primitive::<Int64Type>().value(i);
        (col(ROW_ID), col(sys::VERSION))
    };
    let keys = |bs: &[RecordBatch]| bs.iter().flat_map(|b| (0..b.num_rows()).map(move |i| key(b, i))).collect::<HashSet<_>>();
    let (new, old) = (keys(&rows), keys(&gone));
    let out = crate::query::schema(&sys::with_sys(&meta).columns)?;
    let mut fields = out.fields().to_vec();
    fields.push(Arc::new(Field::new("_change_type", DataType::Utf8, false)));
    let s = Arc::new(Schema::new(fields));
    let label = |b: &RecordBatch, kind: &dyn Fn(usize) -> &'static str| -> Result<(i64, RecordBatch)> {
        let mut cols = crate::query::conform(b, &out)?.columns().to_vec();
        cols.push(Arc::new(StringArray::from_iter_values((0..b.num_rows()).map(kind))));
        Ok((key(b, 0).1, RecordBatch::try_new(s.clone(), cols)?))
    };
    let deleted = |b: &RecordBatch, i: usize| b.column_by_name("_deleted").is_some_and(|c| c.as_boolean_opt().is_some_and(|c| c.is_valid(i) && c.value(i)));
    let mut all = vec![];
    for b in gone.iter().filter(|b| b.num_rows() > 0) {
        all.push((label(b, &|i| if new.contains(&key(b, i)) { "update_preimage" } else { "delete" })?, 0));
    }
    for b in rows.iter().filter(|b| b.num_rows() > 0) {
        let kind = |i| match () {
            _ if !meta.key.is_empty() && deleted(b, i) => "delete",
            _ if !meta.key.is_empty() => "upsert",
            _ if old.contains(&key(b, i)) => "update_postimage",
            _ => "insert",
        };
        all.push((label(b, &kind)?, 1));
    }
    all.sort_by_key(|((version, _), side)| (*version, *side)); // (stable: each side's own order kept)
    all.into_iter().map(|((_, b), _)| meta.to_logical(&b)).collect() // (under the names SQL knows)
}

/// The rows file commits took out of `table` in (after, upto] — the rows of the files they took
/// out (another engine's copy-on-write `DELETE`, `UPDATE`, `MERGE`, overwrite: ADR-029 §7) and the
/// rows they deleted by position (its merge-on-read ones: §4) — as those commits' old versions:
/// `_version` and `_updated_at` the commit's.
pub async fn taken_out(lake: &Lake, table: &str, after: u64, upto: Option<u64>) -> Result<Vec<RecordBatch>> {
    let end = upto.map_or("s0".to_string(), |u| seg_key(u + 1));
    let mut out = vec![];
    for (key, seg) in lake.cat.scan::<Segment>(&seg_key(after + 1), &end).await? {
        if seg.files.get(table).is_some_and(|f| !f.removed.is_empty() || !f.deleted.is_empty()) {
            let n: u64 = key[2..].parse()?;
            let mut rows = lake.filed_rows(&seg, table, true, 0, u64::MAX).await?;
            rows.extend(lake.deleted_rows(&seg, table).await?);
            for b in rows {
                out.push(sys::at_commit(&b, n, seg.ts_ms)?);
            }
        }
    }
    Ok(out)
}

// ---------------------------------------------------------------- a change for another leader (ADR-028)

/// Tables a change reads that its leader can't: their names, and their rows (Arrow IPC, base64).
pub type Sent = Vec<(String, String)>;

/// `sql`, a change of `target`'s table written `written` (`local` in that lake), for that lake's
/// leader: the target as that lake names it, and each other relation it reads that the leader
/// can't — another lake's table or view, a file or table function here, this session's temporary
/// tables — read here and sent with it, as `__sent_1`, … (under the name the statement gave it).
pub async fn for_leader(query: &Lake, target: &Lake, written: &str, local: &str, sql: &str, files: bool) -> Result<(String, Sent)> {
    use datafusion::sql::sqlparser::{ast::*, dialect::GenericDialect, parser::Parser};
    use std::collections::{HashMap, HashSet};
    use std::ops::ControlFlow;
    let mut stmts = Parser::parse_sql(&GenericDialect {}, sql)?;
    // A relation as written, without its alias: the key, and what `SELECT * FROM …` reads here.
    let key = |t: &TableFactor| {
        let mut t = t.clone();
        if let TableFactor::Table { alias, .. } = &mut t {
            *alias = None;
        }
        t.to_string()
    };
    struct Seen<F> {
        ctes: HashSet<String>,
        tables: Vec<(String, ObjectName, bool)>, // (key, name, a file or a function's rows)
        key: F,
    }
    impl<F: Fn(&TableFactor) -> String> Visitor for Seen<F> {
        type Break = ();
        fn pre_visit_query(&mut self, q: &Query) -> ControlFlow<()> {
            self.ctes.extend(q.with.iter().flat_map(|w| &w.cte_tables).map(|c| c.alias.name.value.to_lowercase()));
            ControlFlow::Continue(())
        }
        fn pre_visit_table_factor(&mut self, t: &TableFactor) -> ControlFlow<()> {
            if let TableFactor::Table { name, args, .. } = t {
                let quoted = name.0.len() == 1 && name.0[0].as_ident().is_some_and(|i| i.quote_style == Some('\''));
                self.tables.push(((self.key)(t), name.clone(), args.is_some() || quoted));
            }
            ControlFlow::Continue(())
        }
    }
    let mut seen = Seen { ctes: HashSet::new(), tables: vec![], key };
    let _ = Visit::visit(&stmts, &mut seen);
    let same = std::ptr::eq(query, target);
    let (mut renamed, mut sent, mut kept) = (HashMap::new(), vec![], HashMap::new());
    for (k, name, rows_of_call) in seen.tables {
        let n = crate::write::object(&name);
        let rows_of_call = rows_of_call || crate::ext::is(&n); // (files here, as the statement names them by now: `ext.rs`)
        if kept.contains_key(&k) || renamed.contains_key(&k) || (!rows_of_call && seen.ctes.contains(&n)) {
            continue;
        }
        let here = match rows_of_call {
            true => true, // (a file here, or a function's rows)
            false if n == written => {
                renamed.insert(k, local.to_string());
                continue;
            }
            false if crate::temp::mentioned(&n) => true, // (the session's own)
            false => match crate::ddl::resolve(query, &n).await {
                Ok((Some(o), t)) if std::ptr::eq(&*o, target) => {
                    renamed.insert(k, t);
                    continue;
                }
                Ok((Some(_), _)) => true, // (a third lake's)
                Ok((None, t)) if !same => query.cat.get_raw(&crate::store::table_key(&t)).await?.is_some() || query.cat.get_raw(&crate::ddl::query_key(&t)).await?.is_some(),
                _ => false, // (the leader's own, or none: it says so)
            },
        };
        if here {
            let read = format!("SELECT * FROM {k}");
            let ctx = crate::query::session(query, &read, "").await?;
            let ctx = if files { ctx.enable_url_table() } else { ctx };
            let batches = ctx.sql(&crate::asof::rewrite(&read)?).await?.collect().await.with_context(|| format!("reading {k} for the change"))?;
            let alias = if rows_of_call { None } else { name.0.last().and_then(|p| p.as_ident()).map(|i| i.value.clone()) }; // (a table's name stays its rows' name)
            let as_sent = format!("__sent_{}", sent.len() + 1);
            kept.insert(k, (as_sent.clone(), alias));
            sent.push((as_sent, B64.encode(crate::query::ipc(&batches)?)));
        }
    }
    struct Rewrite<'a> {
        renamed: &'a HashMap<String, String>,
        kept: &'a HashMap<String, (String, Option<String>)>,
    }
    impl VisitorMut for Rewrite<'_> {
        type Break = ();
        fn pre_visit_table_factor(&mut self, t: &mut TableFactor) -> ControlFlow<()> {
            let k = {
                let mut c = t.clone();
                if let TableFactor::Table { alias, .. } = &mut c {
                    *alias = None;
                }
                c.to_string()
            };
            if let TableFactor::Table { name, alias, args, .. } = t {
                let named = |n: &str| ObjectName::from(n.split('.').map(|p| Ident::with_quote('"', p)).collect::<Vec<_>>());
                if let Some(local) = self.renamed.get(&k) {
                    *name = named(local);
                } else if let Some((as_sent, was)) = self.kept.get(&k) {
                    (*name, *args) = (ObjectName::from(vec![Ident::new(as_sent)]), None);
                    if let (None, Some(was)) = (&alias, was) {
                        *alias = Some(TableAlias { explicit: true, name: Ident::with_quote('"', was), columns: vec![], at: None }); // (so `src.id` still means it)
                    }
                }
            }
            ControlFlow::Continue(())
        }
    }
    let _ = VisitMut::visit(&mut stmts, &mut Rewrite { renamed: &renamed, kept: &kept });
    Ok((stmts.iter().map(|s| s.to_string()).collect::<Vec<_>>().join("; "), sent))
}

/// The rows a change was sent with, as tables its queries read by name (`query::SENT`).
pub fn unpack(sent: &Sent) -> Result<Vec<(String, Vec<RecordBatch>)>> {
    sent.iter().map(|(n, b)| Ok((n.clone(), crate::query::read_ipc(&B64.decode(b)?)?))).collect()
}

use base64::Engine;
const B64: base64::engine::GeneralPurpose = base64::engine::general_purpose::STANDARD;
