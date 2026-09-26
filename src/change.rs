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

/// `MERGE INTO t [AS a] USING s ON … WHEN …`, as SQL pieces.
pub struct Merge {
    pub sql: String,    // the statement as written
    pub target: String, // as SQL names it (`write::object`)
    alias: String,
    source: String, // the table or subquery, with its alias, as written
    on: String,
    clauses: Vec<Clause>,
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

/// A MERGE statement's pieces (None: not one Pondra takes).
pub fn merge_of(m: &ast::Merge) -> Option<Merge> {
    let ast::TableFactor::Table { name, alias, .. } = &m.table else { return None };
    let target = crate::write::object(name);
    let alias = alias.as_ref().map(|a| a.name.value.clone()).unwrap_or_else(|| target.rsplit('.').next().unwrap_or(&target).to_string());
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
    Some(Merge { sql: m.to_string(), target, alias, source: m.source.to_string(), on: m.on.to_string(), clauses })
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
    ensure!(other.is_none(), "{table} is another lake's: change it on a node of that lake");
    let meta: TableMeta = lake.cat.get(&table_key(&table)).await?.with_context(|| format!("no table {table}"))?;
    let view = lake.cat.get::<crate::views::View>(&crate::views::view_key(&table)).await?;
    ensure!(view.is_none() && meta.merge.is_empty(), "{table} is a view's: change the table it follows");
    let append = meta.key.is_empty();
    ensure!(meta.ids || !append, "{table} holds rows from before row ids (made before Pondra 0.19): copy it once (CREATE TABLE t2 AS SELECT * FROM {table}) and change the copy");
    crate::views::can_follow(lake, &table, append).await?;
    let upto = lake.visible(); // (one snapshot for every query below: the lock keeps its files)
    let (old, new) = match &stmt {
        crate::write::Stmt::Update(_, set, cond) => update(lake, &table, &meta, set, cond, upto).await?,
        crate::write::Stmt::Delete(_, cond) => (select(lake, &table, &meta, &[], &format!("FROM {} {}", from(&table), filter(cond)), upto).await?.0, vec![]),
        crate::write::Stmt::Merge(m) => merge(lake, &table, &meta, m, upto).await?,
        _ => bail!("not an UPDATE, DELETE or MERGE"),
    };
    commit(lake, seq, &table, &meta, old, new, job).await
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
    let (t, a, s, on) = (format!("{} AS {}", crate::write::sql_name(table), q(&m.alias)), q(&m.alias), &m.source, &m.on);
    // Each target row takes at most one source row (else which one's values?).
    let twice = format!("SELECT {a}.{} FROM {t} JOIN {s} ON {on} GROUP BY 1 HAVING count(*) > 1 LIMIT 1", q(ROW_ID));
    let ctx = session(lake, &twice, upto).await?;
    ensure!(ctx.sql(&twice).await?.count().await? == 0, "MERGE: a row of {table} matches more than one source row");
    let (mut old, mut new) = (vec![], vec![]);
    for (i, c) in m.clauses.iter().enumerate() {
        // A row takes the first clause of its kind whose condition holds.
        let earlier: Vec<String> = m.clauses[..i].iter().filter(|e| e.kind == c.kind).map(|e| format!(" AND NOT coalesce(({}), false)", e.when.as_deref().unwrap_or("true"))).collect();
        let when = format!("coalesce(({}), false){}", c.when.as_deref().unwrap_or("true"), earlier.concat());
        let rest = match c.kind {
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
/// `{t}$deleted` (an append table) or as delete markers (a keyed one).
async fn commit(lake: &Lake, seq: &Sequencer, table: &str, meta: &TableMeta, old: Vec<RecordBatch>, new: Vec<RecordBatch>, job: &str) -> Result<Value> {
    let rows = |b: &[RecordBatch]| b.iter().map(|b| b.num_rows()).sum::<usize>();
    let (replaced, added) = (rows(&old), rows(&new));
    if replaced + added == 0 {
        return Ok(j!({"rows": 0}));
    }
    // (new versions carry their ids, new rows don't yet: one schema for both)
    let one = |b: Vec<RecordBatch>, s: SchemaRef| -> Result<Option<RecordBatch>> {
        match b.is_empty() {
            true => Ok(None),
            false => Ok(Some(concat_batches(&s, &b.iter().map(|b| crate::query::conform(b, &s)).collect::<Result<Vec<_>>>()?)?)),
        }
    };
    let ids = with_ids(&crate::query::schema(&meta.columns)?);
    let old = match old.first().map(|f| f.schema()) {
        Some(s) => one(old, s)?,
        None => None,
    };
    let new = one(new, ids.clone())?;
    let inserted = new.as_ref().and_then(|n| Some(n.column_by_name(ROW_ID)?.null_count())).unwrap_or(0); // (new rows: no id yet)
    let src = |p: &str| Src { producer: format!("sql:{job}{p}"), seq: 1, prev: None };
    let append = |table: &str, batch: RecordBatch, src: Src| Append { table: table.into(), src, batch, ack: tokio::sync::oneshot::channel().0 };
    let mut pending = vec![];
    if meta.key.is_empty() {
        companion(lake, table, meta).await?;
        for (view, vmeta) in crate::views::row_views(lake, table).await? {
            companion(lake, &view, &vmeta).await?; // (its rows of the old ones go there: `views::derive`)
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
            lake.ids.refill(seq.reserve().await?.0);
        };
        a.batch = sys::stamp(&a.batch, first)?;
    }
    let outcome = seq.submit(pack(lake, &pending).await?).await?;
    Ok(match outcome {
        Outcome::Acks(acks) if acks.iter().all(|a| !a.duplicate) => j!({"rows": replaced + inserted, "updated": added - inserted, "deleted": replaced + inserted - added, "inserted": inserted}),
        _ => j!({"duplicate": true}), // (this job's change is already in)
    })
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
async fn companion(lake: &Lake, table: &str, meta: &TableMeta) -> Result<()> {
    if meta.changed {
        return Ok(());
    }
    let mut columns = meta.columns.clone();
    columns.push(("_old_version".into(), "Int64".into()));
    let deleted = TableMeta { columns, tiered: lake.visible(), ids: true, ..Default::default() };
    let changed = TableMeta { changed: true, ..meta.clone() };
    lake.cat.commit(vec![(table_key(&sys::deleted(table)), json(&deleted)), (table_key(table), json(&changed))], &[]).await
}

/// The old rows as `{t}$deleted` holds them: `_version` renamed `_old_version`.
fn as_deleted(b: &RecordBatch) -> Result<RecordBatch> {
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
    let rows = crate::query::tail_of(lake, table, after, upto, false, true).await?;
    let gone = match meta.changed {
        true => crate::query::tail_of(lake, &sys::deleted(table), after, upto, false, true).await?,
        false => vec![],
    };
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
    Ok(all.into_iter().map(|((_, b), _)| b).collect())
}
