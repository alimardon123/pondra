//! UNIQUE, PRIMARY KEY and FOREIGN KEY as a table declares them (ADR-057). A UNIQUE constraint is
//! kept: every write to its table is checked on the leader, under the lake's lock, against the
//! rows the table holds then, so two writers can never both add one value. Said NOT ENFORCED, a
//! UNIQUE, a PRIMARY KEY (which then makes no key: the table takes rows as they come) and a FOREIGN
//! KEY (which must say so: nothing checks a reference yet) are facts about the data, kept for the
//! tools that read them (`pg_constraint`, `information_schema`, `SHOW CREATE`), never checked.
//!
//! Every loading door (appends, Flight, `COPY`, Kafka) takes such a table's rows the same way: `queue`
//! sends them to the leader, which checks them under the lake's lock and commits them as one change
//! (`leader`), once per (producer, seq), as a retry of a committed batch is answered without a check.
//!
//! A table's key (an enforced PRIMARY KEY) is checked for a plain SQL INSERT and for a transaction's
//! changes at COMMIT: a key the table holds is refused, as Postgres does. A plain INSERT is checked on
//! the node that takes it, under the row locks of its keys (`txn::insert_as`), so two nodes inserting
//! one new key at once wait for each other and the second is refused. A change keeps the keys it
//! replaces or deletes, so an UPDATE or an upsert of a key still replaces its row, and the loading
//! doors keep doing so, which is what a keyed stream is.
use crate::log::{Ack, Append, Outcome, Sequencer, Src, To};
use crate::store::{Lake, TableMeta};
use anyhow::{bail, ensure, Result};
use datafusion::arrow::array::{Array, RecordBatch};
use datafusion::sql::sqlparser::ast;
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::sync::{Arc, Weak};

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Constraint {
    pub name: String,
    pub kind: Kind,
    pub columns: Vec<String>, // stored names (SQL's in a table's spec and in `logical()`)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub references: Option<(String, Vec<String>)>, // a foreign key's table and its columns, as written
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub enforced: bool,
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    Unique,
    PrimaryKey,
    ForeignKey,
}

impl Constraint {
    /// The same constraint with its columns named another way (stored names to SQL's, or back).
    pub fn named(&self, n: &dyn Fn(&String) -> String) -> Constraint { Constraint { columns: self.columns.iter().map(n).collect(), ..self.clone() } }

    /// As `CREATE TABLE` and `ALTER TABLE … ADD` write it.
    pub fn sql(&self, ident: &dyn Fn(&str) -> String) -> String { format!("CONSTRAINT {} {}", ident(&self.name), self.def(ident)) }

    /// Without its name, as Postgres's `pg_get_constraintdef` writes it.
    pub fn def(&self, ident: &dyn Fn(&str) -> String) -> String {
        let list = |c: &[String]| c.iter().map(|c| ident(c)).collect::<Vec<_>>().join(", ");
        let what = match self.kind {
            Kind::Unique => format!("UNIQUE ({})", list(&self.columns)),
            Kind::PrimaryKey => format!("PRIMARY KEY ({})", list(&self.columns)),
            Kind::ForeignKey => {
                let (t, c) = self.references.clone().unwrap_or_default();
                format!("FOREIGN KEY ({}) REFERENCES {t}{}", list(&self.columns), if c.is_empty() { String::new() } else { format!("({})", list(&c)) })
            }
        };
        format!("{what}{}", if self.enforced { "" } else { " NOT ENFORCED" })
    }
}

/// Does a write to this table have to be checked on the leader (an enforced UNIQUE)?
pub fn checked(meta: &TableMeta) -> bool { meta.constraints.iter().any(|c| c.enforced) }

/// Does a plain INSERT into this table have its key checked on the leader? A keyed table's key is
/// its PRIMARY KEY: a plain INSERT of a key the table holds is refused. A GROUP BY view's table
/// (`merge`) combines a key's rows by design, so it has no such refusal.
pub fn key_checked(meta: &TableMeta) -> bool { !meta.key.is_empty() && meta.merge.is_empty() }

/// Does `check` have to see this write? Every write to a table with an enforced UNIQUE does; a plain
/// INSERT does too when its table's key is checked (`key_checked`). Loaders and views pass `plain` false.
pub fn needs_check(meta: &TableMeta, plain: bool) -> bool { checked(meta) || (plain && key_checked(meta)) }

/// Refused at a door that writes rows without the leader's check: another engine's file commit into
/// a table with an enforced UNIQUE (`adopt.rs`). The loading doors take such rows through `queue`,
/// checked on the leader, and SQL through the change path.
pub fn door(meta: &TableMeta, table: &str) -> Result<()> {
    match meta.constraints.iter().find(|c| c.enforced) {
        Some(c) => bail!("{table} has UNIQUE ({}): it takes rows through SQL's INSERT, UPDATE and MERGE, which check them on the leader", c.columns.iter().map(|s| meta.name_of(s)).collect::<Vec<_>>().join(", ")),
        None => Ok(()),
    }
}

/// Is a PRIMARY KEY (a column's or the table's) said NOT ENFORCED: a fact, not the table's key?
pub fn informational(c: &Option<ast::ConstraintCharacteristics>) -> bool { matches!(c, Some(c) if c.enforced == Some(false)) }

/// The UNIQUE, NOT ENFORCED PRIMARY KEY and FOREIGN KEY constraints a `CREATE TABLE` declares
/// (columns by SQL's names), named as Postgres names them when they aren't: `{table}_{columns}_key`,
/// `{table}_pkey`, `{table}_{columns}_fkey`.
pub fn declared(c: &ast::CreateTable, table: &str) -> Result<Vec<Constraint>> {
    let mut out: Vec<Constraint> = vec![];
    for col in &c.columns {
        let one = vec![crate::text::ident(&col.name)];
        for o in &col.options {
            let named = |n: &Option<ast::Ident>| n.as_ref().or(o.name.as_ref()).map(crate::text::ident);
            match &o.option {
                ast::ColumnOption::Unique(u) => out.push(unique(named(&u.name), one.clone(), u, table)?),
                ast::ColumnOption::PrimaryKey(p) if informational(&p.characteristics) => out.push(fact(named(&p.name), Kind::PrimaryKey, one.clone(), None, table)),
                ast::ColumnOption::ForeignKey(f) => out.push(foreign(named(&f.name), one.clone(), f, table)?),
                _ => {}
            }
        }
    }
    for k in &c.constraints {
        if let Some(k) = of(k, table)? {
            out.push(k);
        }
    }
    let keys = c.columns.iter().flat_map(|c| &c.options).filter(|o| matches!(o.option, ast::ColumnOption::PrimaryKey(_))).count() + c.constraints.iter().filter(|k| matches!(k, ast::TableConstraint::PrimaryKey(_))).count();
    ensure!(keys <= 1, "multiple primary keys for table \"{table}\" are not allowed"); // (two columns' PRIMARY KEYs made one key of both)
    let mut names = std::collections::HashSet::new();
    for k in out.iter_mut() {
        let base = k.name.clone();
        k.name = (1..).map(|i| if i == 1 { base.clone() } else { format!("{base}{}", i - 1) }).find(|n| names.insert(n.clone())).expect("a free name");
    }
    Ok(out)
}

/// A table constraint (`CONSTRAINT … UNIQUE (a, b)`, `… FOREIGN KEY …`, a NOT ENFORCED PRIMARY
/// KEY) as kept; None for the others (an enforced PRIMARY KEY is the table's key, a CHECK is a
/// check).
pub fn of(k: &ast::TableConstraint, table: &str) -> Result<Option<Constraint>> {
    let cols = |c: &[ast::IndexColumn]| c.iter().map(|i| match &i.column.expr {
        ast::Expr::Identifier(i) => Ok(crate::text::ident(i)),
        e => bail!("{e}: a constraint is on columns"),
    }).collect::<Result<Vec<_>>>();
    let name = |n: &Option<ast::Ident>| n.as_ref().map(crate::text::ident);
    Ok(match k {
        ast::TableConstraint::Unique(u) => Some(unique(name(&u.name), cols(&u.columns)?, u, table)?),
        ast::TableConstraint::PrimaryKey(p) if informational(&p.characteristics) => Some(fact(name(&p.name), Kind::PrimaryKey, cols(&p.columns)?, None, table)),
        ast::TableConstraint::ForeignKey(f) => Some(foreign(name(&f.name), f.columns.iter().map(crate::text::ident).collect(), f, table)?),
        _ => None,
    })
}

fn unique(name: Option<String>, columns: Vec<String>, u: &ast::UniqueConstraint, table: &str) -> Result<Constraint> {
    if matches!(u.nulls_distinct, ast::NullsDistinctOption::NotDistinct) {
        return Err(crate::codes::coded("0A000", "UNIQUE NULLS NOT DISTINCT isn't taken yet: a UNIQUE column holds any number of NULLs, as by default"));
    }
    deferred(&u.characteristics)?;
    Ok(Constraint { name: name.unwrap_or_else(|| format!("{table}_{}_key", columns.join("_"))), kind: Kind::Unique, columns, references: None, enforced: !informational(&u.characteristics) })
}

fn foreign(name: Option<String>, columns: Vec<String>, f: &ast::ForeignKeyConstraint, table: &str) -> Result<Constraint> {
    let refs = format!("REFERENCES {}", f.foreign_table);
    if !informational(&f.characteristics) {
        // (a feature not taken yet, not a fault: SQLSTATE 0A000, as the other "not taken" refusals)
        return Err(crate::codes::coded("0A000", format!("{} {refs}: a reference isn't checked yet; say NOT ENFORCED to keep it as a fact about the data (BI tools and the planner read it)", columns.join(", "))));
    }
    let to = (crate::text::object(&f.foreign_table), f.referred_columns.iter().map(crate::text::ident).collect());
    Ok(fact(name, Kind::ForeignKey, columns, Some(to), table))
}

fn fact(name: Option<String>, kind: Kind, columns: Vec<String>, references: Option<(String, Vec<String>)>, table: &str) -> Constraint {
    let name = name.unwrap_or_else(|| match kind {
        Kind::PrimaryKey => format!("{table}_pkey"),
        _ => format!("{table}_{}_fkey", columns.join("_")),
    });
    Constraint { name, kind, columns, references, enforced: false }
}

fn deferred(c: &Option<ast::ConstraintCharacteristics>) -> Result<()> {
    match c {
        Some(c) if c.deferrable == Some(true) => Err(crate::codes::coded("0A000", "DEFERRABLE: a constraint is checked at each statement")),
        _ => Ok(()),
    }
}

// ---------------------------------------------------------------- the check

/// Leader, under the lake's lock: refuse a change of `table` whose `new` rows (SQL's names; an
/// updated row keeps its `_row_id`) would give an enforced UNIQUE's columns a value twice, among
/// themselves or with a row the table keeps (every row but the `old` ones it replaces, and, in a
/// keyed table, the rows of the keys it writes). NULLs are distinct, as in Postgres.
///
/// `plain`: the rows are a plain INSERT's. Then a keyed table's key must be new: two rows of one key
/// are refused, and so is a key a live row of the table holds (`key_checked`). Nothing is replaced,
/// so the rows of the keys they give stay.
pub async fn check(lake: &Lake, table: &str, meta: &TableMeta, old: Option<&RecordBatch>, new: Option<&RecordBatch>, plain: bool) -> Result<()> {
    let Some(new) = new.filter(|n| n.num_rows() > 0) else { return Ok(()) };
    let named = meta.logical();
    if plain && key_checked(meta) {
        key_check(lake, table, meta, old, new).await?;
    }
    for c in named.constraints.iter().filter(|c| c.enforced && c.kind == Kind::Unique) {
        if let Some(key) = twice(new, &c.columns, &named.key)? {
            return Err(violated(&c.name, &c.columns, key));
        }
        if let Some(key) = kept(lake, table, &named, &c.columns, old, new, plain).await? {
            return Err(violated(&c.name, &c.columns, key));
        }
    }
    Ok(())
}

/// The 23505 a duplicate gives, with Postgres's words.
fn violated(name: &str, columns: &[String], key: String) -> anyhow::Error {
    crate::codes::coded("23505", format!("duplicate key value violates unique constraint \"{name}\": Key ({})=({key}) already exists", columns.join(", ")))
}

/// The PRIMARY KEY of a plain INSERT, or of a transaction's changes at COMMIT (`check`, `txn.rs`):
/// two new rows of one key are refused, and so is a new key a live row of the table holds (`held`).
/// A new row whose key one of the `old` rows had is no new key (an UPDATE keeps its key, a swap moves
/// two keys between rows, a key the change deletes is free), so it is not looked up. Every new row
/// still counts for the first test. `meta` is the table as it is stored; `new` and `old` are in SQL's
/// column names. The table is read as this node has it now.
pub async fn key_check(lake: &Lake, table: &str, meta: &TableMeta, old: Option<&RecordBatch>, new: &RecordBatch) -> Result<()> {
    let kept: Vec<Option<bool>> = match old.filter(|o| o.num_rows() > 0) {
        None => vec![None; new.num_rows()], // (a plain INSERT's: nothing taken away, and no key's text to make)
        Some(o) => {
            let key: Vec<String> = meta.key.iter().map(|c| meta.name_of(c).to_string()).collect();
            let gone: std::collections::HashSet<String> = crate::txn::key_texts(o, &key)?.into_iter().collect();
            crate::txn::key_texts(new, &key)?.iter().map(|k| gone.contains(k).then_some(false)).collect()
        }
    };
    key_check_over(lake, table, meta, new, &kept).await
}

/// The key's name and its columns in SQL's names, after checking that no two of `new`'s rows share a key: refused as a plain
/// INSERT's are (23505 with `{table}_pkey` and Postgres's words).
fn key_once(table: &str, meta: &TableMeta, new: &RecordBatch) -> Result<(String, Vec<String>)> {
    let name = format!("{}_pkey", table.rsplit('.').next().unwrap_or(table)); // (Postgres's name for a primary key that isn't named)
    let key: Vec<String> = meta.key.iter().map(|c| meta.name_of(c).to_string()).collect(); // (SQL's names, as `logical` gives them)
    if let Some(found) = twice(new, &key, &[])? {
        return Err(violated(&name, &key, found));
    }
    Ok((name, key))
}

/// A change's new rows of one key are refused as a plain INSERT's are (`key_once`). `change::run` calls it for a MERGE whose
/// inserts match on the key alone: those are not looked up, so this is their only check.
pub fn distinct_keys(meta: &TableMeta, table: &str, new: &[RecordBatch]) -> Result<()> {
    let rows: Vec<&RecordBatch> = new.iter().filter(|b| b.num_rows() > 0).collect();
    let Some(first) = rows.first() else { return Ok(()) };
    let schema = first.schema(); // (an update's rows carry their ids and an insert's don't: conformed to one schema)
    let all = datafusion::arrow::compute::concat_batches(&schema, &rows.iter().map(|b| crate::query::conform(b, &schema)).collect::<Result<Vec<_>>>()?)?;
    key_once(table, meta, &all).map(|_| ())
}

/// `key_check`, told what the writer kept of the table for each new row's key (`kept`, as `txn::ByKey::kept` answers): Some(true) a
/// row it keeps holds the key (refused), Some(false) it took the table's row of it away (free), None neither (looked up in the table
/// as this node has it now, `held`). A plain INSERT in a transaction is checked this way at its statement, as Postgres checks it,
/// so a SAVEPOINT can roll the refusal back. Under READ COMMITTED its keys are its locks by then, so a key another transaction
/// committed while it waited is held; COMMIT checks every key again either way.
pub async fn key_check_over(lake: &Lake, table: &str, meta: &TableMeta, new: &RecordBatch, kept: &[Option<bool>]) -> Result<()> {
    let (name, key) = key_once(table, meta, new)?;
    let columns = key.iter().map(|c| new.column_by_name(c).cloned().ok_or_else(|| anyhow::anyhow!("no column {c}"))).collect::<Result<Vec<_>>>()?;
    if let Some(i) = kept.iter().position(|k| *k == Some(true)) {
        return Err(violated(&name, &key, shown(&columns, i)));
    }
    // (the key columns, in the table's key order, of the rows whose key is neither: one row per key by now, `twice`)
    let all = RecordBatch::try_from_iter(key.iter().map(String::as_str).zip(columns))?;
    let fresh = datafusion::arrow::compute::filter_record_batch(&all, &datafusion::arrow::array::BooleanArray::from(kept.iter().map(|k| Some(k.is_none())).collect::<Vec<_>>()))?;
    if fresh.num_rows() > 0 {
        if let Some(found) = held(lake, table, meta, &fresh).await? {
            return Err(violated(&name, &key, found));
        }
    }
    Ok(())
}

/// For each row of `of`: is its key (the columns `key`) absent from `among`'s rows? `among`'s key
/// columns are cast to `of`'s types first. A change uses it to tell which old rows a new version
/// replaces: by key, so an UPDATE that moves a key frees the old one, and a DELETE and INSERT of one
/// key in one change keep the key's new row.
pub fn absent(of: &RecordBatch, among: &RecordBatch, key: &[String]) -> Result<datafusion::arrow::array::BooleanArray> {
    use datafusion::arrow::row::{RowConverter, SortField};
    let columns = |b: &RecordBatch| key.iter().map(|c| b.column_by_name(c).cloned().ok_or_else(|| anyhow::anyhow!("no column {c}"))).collect::<Result<Vec<_>>>();
    let mine = columns(of)?;
    let converter = RowConverter::new(mine.iter().map(|a| SortField::new(a.data_type().clone())).collect())?;
    let theirs = columns(among)?.iter().zip(&mine).map(|(a, m)| datafusion::arrow::compute::cast(a, m.data_type())).collect::<std::result::Result<Vec<_>, _>>()?;
    let held: std::collections::HashSet<Vec<u8>> = converter.convert_columns(&theirs)?.iter().map(|r| r.as_ref().to_vec()).collect();
    Ok(datafusion::arrow::array::BooleanArray::from(converter.convert_columns(&mine)?.iter().map(|r| Some(!held.contains(r.as_ref()))).collect::<Vec<_>>()))
}

/// Is one of these keys (`keys`: the key columns in the table's key order, named as SQL names them, one
/// row per key) held by a live row of `table`, as this node has it now? A deleted key is free. A table
/// without `order` and up to 1,000 keys of types the point lookup reads (integers, text) are looked up one
/// by one (`serve::lookup_versioned`, on the stored `meta`: the log tail, then the files by their key
/// ranges); more keys, or an `order` table (its lookups go through SQL: invariant 79), are one query, the
/// keys as a table of their own, so the files' key ranges prune. Returns the first key held, as Postgres's
/// DETAIL writes it.
pub async fn held(lake: &Lake, table: &str, meta: &TableMeta, keys: &RecordBatch) -> Result<Option<String>> {
    let point = meta.ids && meta.order.is_none() && keys.num_rows() <= 1000 && meta.key.iter().all(|k| meta.columns.iter().any(|(c, t)| c == k && is_point_type(t)));
    if point {
        for i in 0..keys.num_rows() {
            let values = keys.columns().iter().map(|c| datafusion::arrow::util::display::array_value_to_string(c, i)).collect::<std::result::Result<Vec<_>, _>>()?;
            if crate::serve::lookup_versioned(lake, table, meta, &values).await?.is_some() {
                return Ok(Some(shown(keys.columns(), i)));
            }
        }
        return Ok(None);
    }
    let q = |c: &str| format!("\"{}\"", c.replace('"', "\"\""));
    let names: Vec<String> = keys.schema().fields().iter().map(|f| f.name().clone()).collect();
    let cols = names.iter().map(|c| format!("t.{}", q(c))).collect::<Vec<_>>().join(", ");
    let on = names.iter().map(|c| format!("t.{0} = n.{0}", q(c))).collect::<Vec<_>>().join(" AND ");
    let sql = format!("SELECT {cols} FROM {} AS t JOIN __pondra_keys AS n ON {on} LIMIT 1", crate::write::sql_name(table));
    let ctx = crate::query::session_at(lake, &sql, "", Some(lake.visible())).await?;
    ctx.register_batch("__pondra_keys", keys.clone())?;
    let found = crate::engine::sql(&ctx, &sql).await?.collect().await?;
    Ok(found.iter().find(|b| b.num_rows() > 0).map(|b| shown(b.columns(), 0)))
}

/// The types `serve::lookup_versioned` turns a key's text back into.
fn is_point_type(stored: &str) -> bool {
    matches!(stored, "Int8" | "Int16" | "Int32" | "Int64" | "UInt8" | "UInt16" | "UInt32" | "UInt64" | "Utf8")
}

/// A value of `columns` two new rows give (of two keys, in a keyed table: one key's rows are its
/// versions), as Postgres's DETAIL writes it; None if there's none. The rows are hashed a column at a
/// time, as a hash join hashes its keys, and only rows of one hash are compared (`hashed`); a type
/// that can't be hashed or compared so, or two values sharing a 64-bit hash, goes the exact way.
fn twice(new: &RecordBatch, columns: &[String], key: &[String]) -> Result<Option<String>> {
    let pick = |names: &[String]| names.iter().map(|c| new.column_by_name(c).cloned().ok_or_else(|| anyhow::anyhow!("no column {c}"))).collect::<Result<Vec<_>>>();
    let (values, keys) = (pick(columns)?, pick(key)?);
    hashed(&values, &keys).or_else(|_| exactly(&values, &keys))
}

/// `twice` by each row's hash: a million new keys take tens of milliseconds, where hashing each
/// row's bytes in turn (`exactly`) took a fifth of a second. Errs when it can't decide.
fn hashed(values: &[datafusion::arrow::array::ArrayRef], keys: &[datafusion::arrow::array::ArrayRef]) -> Result<Option<String>> {
    use datafusion::arrow::array::{make_comparator, DynComparator};
    use datafusion::common::hash_utils::{create_hashes, RandomState};
    let n = values.first().map_or(0, |v| v.len());
    let mut hashes = vec![0u64; n];
    create_hashes(values, &RandomState::with_seed(0), &mut hashes)?;
    let comparators = |columns: &[datafusion::arrow::array::ArrayRef]| {
        columns.iter().map(|c| make_comparator(c.as_ref(), c.as_ref(), Default::default())).collect::<std::result::Result<Vec<DynComparator>, _>>()
    };
    let (value, key) = (comparators(values)?, comparators(keys)?);
    let equal = |columns: &[DynComparator], i: usize, j: usize| columns.iter().all(|c| c(i, j).is_eq());
    let mut first = std::collections::HashMap::with_capacity_and_hasher(n, RandomState::with_seed(0)); // (each hash's first row)
    for i in 0..n {
        if values.iter().any(|v| v.is_null(i)) {
            continue;
        }
        let j = *first.entry(hashes[i]).or_insert(i);
        if j == i {
            continue;
        }
        ensure!(equal(&value, i, j), "two values share a hash"); // (rare enough to go over every row again)
        if keys.is_empty() || !equal(&key, i, j) {
            return Ok(Some(shown(values, i)));
        }
    }
    Ok(None)
}

/// `twice` by each row's bytes (`RowConverter`), for what `hashed` can't decide.
fn exactly(values: &[datafusion::arrow::array::ArrayRef], keys: &[datafusion::arrow::array::ArrayRef]) -> Result<Option<String>> {
    use datafusion::arrow::row::{RowConverter, SortField};
    let converter = |a: &[datafusion::arrow::array::ArrayRef]| RowConverter::new(a.iter().map(|a| SortField::new(a.data_type().clone())).collect());
    let rows = converter(values)?.convert_columns(values)?;
    let key_rows = match keys.is_empty() {
        true => None,
        false => Some(converter(keys)?.convert_columns(keys)?),
    };
    // (rows held as references into the converted buffers: a Vec made for each row took a million-row INSERT's check 0.5–0.9 s)
    let mut seen = std::collections::HashMap::with_capacity(rows.num_rows());
    for i in 0..rows.num_rows() {
        if values.iter().any(|v| v.is_null(i)) {
            continue;
        }
        let k = key_rows.as_ref().map(|k| k.row(i));
        if let Some(before) = seen.insert(rows.row(i), k) {
            if key_rows.is_none() || before != k {
                return Ok(Some(shown(values, i)));
            }
        }
    }
    Ok(None)
}

/// A value of `columns` a new row gives that a row the table keeps has already. `plain`: the new rows
/// are a plain INSERT's, which replace no row of their keys, so those rows count too.
async fn kept(lake: &Lake, table: &str, meta: &TableMeta, columns: &[String], old: Option<&RecordBatch>, new: &RecordBatch, plain: bool) -> Result<Option<String>> {
    let q = |c: &str| format!("\"{}\"", c.replace('"', "\"\""));
    let on = columns.iter().map(|c| format!("t.{0} = n.{0}", q(c))).collect::<Vec<_>>().join(" AND ");
    let given = columns.iter().map(|c| format!("n.{} IS NOT NULL", q(c))).collect::<Vec<_>>().join(" AND ");
    let mut sql = format!("SELECT {} FROM {} AS t JOIN __pondra_new AS n ON {on} WHERE {given}", columns.iter().map(|c| format!("t.{}", q(c))).collect::<Vec<_>>().join(", "), crate::write::sql_name(table));
    if old.is_some_and(|o| o.num_rows() > 0) {
        sql += &format!(" AND t.{0} NOT IN (SELECT {0} FROM __pondra_old)", crate::text::ROW_ID); // (the rows the change replaces)
    }
    if !meta.key.is_empty() && !plain {
        let same = meta.key.iter().map(|k| format!("w.{0} = t.{0}", q(k))).collect::<Vec<_>>().join(" AND ");
        sql += &format!(" AND NOT EXISTS (SELECT 1 FROM __pondra_new AS w WHERE {same})"); // (a key's new version replaces its row)
    }
    sql += " LIMIT 1";
    let ctx = crate::query::session_at(lake, &sql, "", Some(lake.visible())).await?;
    ctx.register_batch("__pondra_new", new.clone())?;
    if let Some(o) = old.filter(|o| o.num_rows() > 0) {
        let ids = o.column_by_name(crate::text::ROW_ID).cloned().ok_or_else(|| anyhow::anyhow!("the replaced rows carry no {}", crate::text::ROW_ID))?;
        ctx.register_batch("__pondra_old", RecordBatch::try_from_iter([(crate::text::ROW_ID, ids)])?)?;
    }
    let found = crate::engine::sql(&ctx, &sql).await?.collect().await?;
    Ok(found.iter().find(|b| b.num_rows() > 0).map(|b| shown(b.columns(), 0)))
}

/// Row `i` of these columns, as Postgres's DETAIL writes a key: `a@b.c, 2`.
fn shown(columns: &[datafusion::arrow::array::ArrayRef], i: usize) -> String {
    columns.iter().map(|c| datafusion::arrow::util::display::array_value_to_string(c, i).unwrap_or_default()).collect::<Vec<_>>().join(", ")
}

// ---------------------------------------------------------------- ALTER TABLE … ADD / DROP CONSTRAINT

/// What `ALTER TABLE` does to a table's constraints.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum Change {
    Add(Constraint, bool),      // SQL's column names; true: nothing if one of its name is there (`CREATE UNIQUE INDEX IF NOT EXISTS`)
    AddCheck(String, String),   // name, condition
    Drop { name: String, if_exists: bool },
}

/// `ALTER TABLE t ADD [CONSTRAINT n] UNIQUE (…) | CHECK (…) | … NOT ENFORCED`, `ALTER TABLE t DROP
/// CONSTRAINT [IF EXISTS] n` as a change; None for any other ALTER TABLE.
pub fn change(op: &ast::AlterTableOperation, table: &str) -> Option<Result<Change>> {
    let table = table.rsplit('.').next().unwrap_or(table);
    Some(match op {
        ast::AlterTableOperation::AddConstraint { constraint: ast::TableConstraint::Check(k), .. } => {
            Ok(Change::AddCheck(k.name.as_ref().map(crate::text::ident).unwrap_or_else(|| format!("{table}_check")), k.expr.to_string()))
        }
        ast::AlterTableOperation::AddConstraint { constraint: ast::TableConstraint::PrimaryKey(p), .. } if !informational(&p.characteristics) => {
            Err(anyhow::anyhow!("ADD PRIMARY KEY: a table's key is made with it (CREATE TABLE … PRIMARY KEY), since its rows are kept by key; PRIMARY KEY (…) NOT ENFORCED adds it as a fact"))
        }
        ast::AlterTableOperation::AddConstraint { constraint, .. } => match of(constraint, table) {
            Ok(Some(c)) => Ok(Change::Add(c, false)),
            Ok(None) => Err(anyhow::anyhow!("ALTER TABLE … ADD {constraint}: tables take UNIQUE, CHECK, and PRIMARY KEY or FOREIGN KEY said NOT ENFORCED")),
            Err(e) => Err(e),
        },
        ast::AlterTableOperation::DropConstraint { if_exists, name, .. } => Ok(Change::Drop { name: crate::text::ident(name), if_exists: *if_exists }),
        _ => return None,
    })
}

/// Leader, under the lake's lock: carry out a change of `table`'s constraints. A UNIQUE or a CHECK
/// added must hold for every row already there.
pub async fn apply(lake: &Lake, table: &str, change: Change) -> Result<serde_json::Value> {
    let key = crate::store::table_key(table);
    let mut m: TableMeta = lake.cat.get(&key).await?.ok_or_else(|| anyhow::anyhow!("relation \"{table}\" does not exist"))?;
    ensure!(!crate::sys::hidden(table) && lake.cat.get::<crate::views::View>(&crate::views::view_key(table)).await?.is_none(), "{table} is a view's: a constraint is on a table");
    let taken = |m: &TableMeta, n: &str| m.constraints.iter().any(|c| c.name == n) || m.checks.iter().any(|(c, _)| c == n);
    let out = match change {
        Change::Add(c, if_not_exists) => {
            if if_not_exists && taken(&m, &c.name) {
                return Ok(serde_json::json!({"table": table, "constraint": c.name, "exists": true, "notice": format!("relation \"{}\" already exists, skipping", c.name)}));
            }
            ensure!(!taken(&m, &c.name), "constraint \"{}\" for relation \"{table}\" already exists", c.name);
            ensure!(c.kind != Kind::PrimaryKey || (m.key.is_empty() && !m.constraints.iter().any(|k| k.kind == Kind::PrimaryKey)), "multiple primary keys for table \"{table}\" are not allowed");
            let stored = c.columns.iter().map(|n| m.stored(n).map(str::to_string).ok_or_else(|| anyhow::anyhow!("column \"{n}\" of relation \"{table}\" does not exist"))).collect::<Result<Vec<_>>>()?;
            let (name, before) = (c.name.clone(), m.clone());
            m.constraints.push(Constraint { columns: stored, ..c.clone() });
            if c.enforced {
                // Kept first, so every write from here on is checked (or refused, from a node
                // that doesn't know of it yet: `log::commit`); then, once the flushes sequenced
                // before it are in, the rows already there are checked, and it goes if two share a value.
                lake.cat.commit(vec![(key.clone(), crate::store::json(&m))], &[]).await?;
                if let Some(to) = lake.to.get() {
                    to.block().await?; // (a flush through the sequencer: those before it are in)
                }
                let q = |c: &str| format!("\"{}\"", c.replace('"', "\"\""));
                let cols = c.columns.iter().map(|c| q(c)).collect::<Vec<_>>().join(", ");
                let given = c.columns.iter().map(|c| format!("{} IS NOT NULL", q(c))).collect::<Vec<_>>().join(" AND ");
                let sql = format!("SELECT {cols} FROM {} WHERE {given} GROUP BY {cols} HAVING count(*) > 1 LIMIT 1", crate::write::sql_name(table));
                let found = async { crate::engine::sql(&crate::query::session_at(lake, &sql, "", Some(lake.visible())).await?, &sql).await?.collect().await.map_err(anyhow::Error::from) }.await;
                let twice = match &found {
                    Ok(found) => found.iter().find(|b| b.num_rows() > 0).map(|b| shown(b.columns(), 0)),
                    Err(_) => None,
                };
                if found.is_err() || twice.is_some() {
                    lake.cat.commit(vec![(key, crate::store::json(&before))], &[]).await?; // (under the lock: nothing else rewrote the entry)
                    found?;
                    return Err(crate::codes::coded("23505", format!("could not create unique constraint \"{name}\": Key ({})=({}) is duplicated", c.columns.join(", "), twice.unwrap_or_default())));
                }
                return Ok(serde_json::json!({"table": table, "constraint": name}));
            }
            serde_json::json!({"table": table, "constraint": name})
        }
        Change::AddCheck(name, cond) => {
            ensure!(!taken(&m, &name), "constraint \"{name}\" for relation \"{table}\" already exists");
            let sql = format!("SELECT count(*) AS n FROM {} WHERE NOT ({cond})", crate::write::sql_name(table));
            let found = crate::engine::sql(&crate::query::session_at(lake, &sql, "", Some(lake.visible())).await?, &sql).await?.collect().await?;
            let n = found.first().and_then(|b| b.column(0).as_any().downcast_ref::<datafusion::arrow::array::Int64Array>().map(|a| a.value(0))).unwrap_or(0);
            if n > 0 {
                return Err(anyhow::Error::new(crate::codes::Violation(format!("check constraint \"{name}\" of relation \"{table}\" is violated by {n} row{}", if n == 1 { "" } else { "s" }), "23514")));
            }
            m.checks.push((name.clone(), cond));
            serde_json::json!({"table": table, "constraint": name})
        }
        Change::Drop { name, if_exists } => {
            let before = m.constraints.len() + m.checks.len();
            m.constraints.retain(|c| c.name != name);
            m.checks.retain(|(c, _)| *c != name);
            if m.constraints.len() + m.checks.len() == before {
                ensure!(if_exists, "constraint \"{name}\" of relation \"{table}\" does not exist");
                return Ok(serde_json::json!({"table": table, "constraint": name, "exists": false}));
            }
            serde_json::json!({"table": table, "constraint": name, "dropped": true})
        }
    };
    lake.cat.commit(vec![(key, crate::store::json(&m))], &[]).await?;
    Ok(out)
}

/// `CREATE UNIQUE INDEX [IF NOT EXISTS] [name] ON t (column, …)`, as ORMs write a UNIQUE (Prisma's
/// `@unique`, Rails' `unique: true`): the same constraint, named as Postgres names an index.
pub fn unique_index(c: &ast::CreateIndex) -> Result<crate::ddl::Ddl> {
    let what = "a unique index is a UNIQUE constraint: on columns, with no expression, WHERE, INCLUDE or method but btree";
    let using = c.using.as_ref().map(|u| u.to_string().to_lowercase());
    ensure!(c.predicate.is_none() && c.include.is_empty() && using.as_deref().is_none_or(|u| u == "btree") && c.with.is_empty() && !c.nulls_distinct.is_some_and(|d| !d), "CREATE UNIQUE INDEX: {what}");
    let columns = c.columns.iter().map(|k| match &k.column.expr {
        ast::Expr::Identifier(i) if k.operator_class.is_none() => Ok(crate::text::ident(i)),
        e => bail!("CREATE UNIQUE INDEX … ({e}): {what}"),
    }).collect::<Result<Vec<_>>>()?;
    let table = crate::text::object(&c.table_name);
    let short = table.rsplit('.').next().unwrap_or(&table).to_string();
    let name = c.name.as_ref().map(|n| crate::text::object(n).rsplit('.').next().unwrap_or_default().to_string()).unwrap_or_else(|| format!("{short}_{}_idx", columns.join("_")));
    let constraint = Constraint { name, kind: Kind::Unique, columns, references: None, enforced: true };
    Ok(crate::ddl::Ddl::Constraint { table, change: Change::Add(constraint, c.if_not_exists) })
}

/// `DROP INDEX name` of a unique index made as a constraint: the table in `schema` holding a UNIQUE of
/// that name, if one does.
pub async fn index_of(lake: &Lake, schema: &str, name: &str) -> Result<Option<String>> {
    for (key, m) in lake.cat.scan::<TableMeta>("t/", "t0").await? {
        let table = &key[2..];
        if crate::text::split(table).0 == schema && m.constraints.iter().any(|c| c.name == name && c.kind == Kind::Unique) {
            return Ok(Some(table.to_string()));
        }
    }
    Ok(None)
}

/// A dropped column's constraints go with it, as in Postgres (`m` as stored).
pub fn without_column(m: &mut TableMeta, stored: &str) { m.constraints.retain(|c| !c.columns.iter().any(|s| s == stored)) }

// ---------------------------------------------------------------- the loading doors

crate::per_node!(
    /// Each lake's lock, as its node's `App` holds it: `change::run` takes that lock for every change of
    /// the lake, so a checked append takes it too. `server::router` lends it (`share_lock`), keyed by the
    /// lake itself, so a checked append finds its own lake's lock and never another's. (A process serves
    /// one lake today; keyed anyway, so two lakes could not share one lock.) Weak, so a lake that is gone
    /// drops out of the list.
    static LOCKS: std::sync::Mutex<Vec<(Weak<Lake>, Arc<tokio::sync::Mutex<()>>)>> = std::sync::Mutex::new(Vec::new())
);

/// This node's checked appends, one at a time: each is answered before the next is sent, so a
/// producer's pipelined batches reach the leader in the order they were queued (invariant 29).
static TURN: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Lend `lake` its node's lock (`server::router` does, with its app's lock of that same lake).
pub fn share_lock(lake: &Arc<Lake>, lock: Arc<tokio::sync::Mutex<()>>) {
    let mut all = LOCKS.lock().unwrap_or_else(|e| e.into_inner());
    all.retain(|(l, _)| l.strong_count() > 0);
    all.push((Arc::downgrade(lake), lock));
}

/// The lock lent for this lake, if its node's router has been made.
fn lock_of(lake: &Arc<Lake>) -> Option<Arc<tokio::sync::Mutex<()>>> {
    let all = LOCKS.lock().unwrap_or_else(|e| e.into_inner());
    let found = all.iter().find(|(l, _)| Weak::ptr_eq(l, &Arc::downgrade(lake))).map(|(_, m)| m.clone());
    found
}

/// A checked append a loading door queues, for a table with an enforced UNIQUE (`Log::queue` calls
/// this in place of its own path, which such a table's rows never take). The future answers once the
/// leader has committed the rows (`leader`), or refused them. The guard of `TURN` is taken when the
/// future is first polled: a producer that awaits its batches in order (Flight, Kafka) sends them in
/// order, one answered before the next goes.
pub fn queue(lake: Arc<Lake>, meta: &TableMeta, table: String, src: Src, batch: RecordBatch) -> Result<impl Future<Output = Result<Ack>> + use<>> {
    crate::defaults::check(meta, &table, &batch)?; // (NOT NULL, refused here as the plain path refuses it, not after a round trip)
    crate::metrics::add(&crate::metrics::ROWS_IN, batch.num_rows() as u64);
    let to = lake.to.get().cloned().ok_or_else(|| anyhow::anyhow!("no leader for {table} yet"))?;
    Ok(async move {
        let _turn = TURN.lock().await; // (held until the answer)
        match to {
            To::Local(seq) => leader(lake, seq, table, src, batch).await,
            To::Leader(addr) => send(&addr, &table, &src, &batch).await,
        }
    })
}

/// Leader: a checked append, under the lake's lock. The work runs in a task of its own, so a request
/// that goes away mid-way still holds the lock until its commit is in: the next check then sees the
/// rows (a dropped future would release the lock with the commit still on its way).
pub async fn leader(lake: Arc<Lake>, seq: Arc<Sequencer>, table: String, src: Src, batch: RecordBatch) -> Result<Ack> {
    // (no lock yet: this process's router isn't made, so the node is still starting; a retry reaches it)
    let lock = lock_of(&lake).ok_or_else(|| anyhow::anyhow!("{table}: the node is still starting, catching up with its leader: try again"))?;
    let task = crate::panics::task(crate::panics::door(async move {
        let _one = lock.lock().await;
        append(&lake, &seq, &table, src, batch).await
    }));
    match task.await {
        Ok(Ok(ack)) => ack,
        Ok(Err(panic)) => Err(anyhow::anyhow!(panic)),
        Err(e) => Err(anyhow::anyhow!(e)),
    }
}

/// The leader's part of a checked append, under the lock. A batch its producer already committed is a
/// duplicate (the sequencer's own test: `log::commit`), answered before any check; a `prev` that
/// doesn't match is a conflict, as the sequencer answers it. Otherwise the rows are checked against
/// the table (`change::appends` runs `defaults::check` and `check`) and committed as one flush whose
/// table append carries the producer's seq, so the sequencer's progress moves with the rows.
async fn append(lake: &Lake, seq: &Sequencer, table: &str, src: Src, batch: RecordBatch) -> Result<Ack> {
    let meta: TableMeta = lake.cat.get(&crate::store::table_key(table)).await?.ok_or_else(|| anyhow::anyhow!("no table {table}"))?;
    if !src.producer.is_empty() {
        let last = lake.cat.get::<u64>(&crate::store::producer_key(&src.producer)).await?.unwrap_or(0);
        if src.seq <= last {
            return Ok(Ack { duplicate: true, ..Default::default() });
        }
        if src.prev.is_some_and(|p| p != last) {
            return Ok(Ack { conflict: true, ..Default::default() });
        }
    }
    // (an empty batch still moves its producer's seq: `change::appends` has nothing to append for it)
    let mut pending = match batch.num_rows() {
        0 => vec![Append { table: table.into(), src: src.clone(), batch: RecordBatch::new_empty(crate::answer::schema(&meta.logical_columns())?), ack: tokio::sync::oneshot::channel().0 }],
        _ => crate::change::appends(lake, seq, table, &meta, vec![], vec![batch], "", false).await?.0, // (a loader's rows: upserts)
    };
    let table_append = pending.first_mut().ok_or_else(|| anyhow::anyhow!("no rows for {table}"))?;
    table_append.src = src;
    let answer = match crate::change::submit(lake, seq, &pending).await? {
        Outcome::Acks(acks) => acks.first().copied(),
        Outcome::Retry(retried) => retried.iter().find(|(i, _)| *i == 0).map(|(_, a)| *a),
        Outcome::Refused(why) => bail!(why),
    };
    let ack = answer.ok_or_else(|| anyhow::anyhow!("no answer from the sequencer for {table}"))?;
    ensure!(!ack.gone, "no table {table}"); // (renamed or dropped as it was sent)
    Ok(ack)
}

/// The SQLSTATEs a checked append's leader answers with: the errors `leader` raises (a duplicate, a
/// CHECK, NOT NULL, an enum label, a table gone, a conflict). Any other code is read back from the
/// leader's words, as `codes::of` reads them there.
const ANSWERS: [&str; 6] = ["23505", "23514", "23502", "22P02", "42P01", "40001"];

/// A checked append to the leader over HTTP (`/cluster/checked`). A refusal is the leader's own words
/// with the SQLSTATE its `x-pondra-sqlstate` header carries (`server::E` writes it), so a client on
/// this node sees exactly what a client on the leader sees.
async fn send(addr: &str, table: &str, src: &Src, batch: &RecordBatch) -> Result<Ack> {
    let res = crate::cluster::http().post(crate::tls::url(&format!("{addr}/cluster/checked"))).body(encode(table, src, batch)?).send().await?;
    if res.status().is_success() {
        return Ok(res.json().await?);
    }
    let code = res.headers().get("x-pondra-sqlstate").and_then(|v| v.to_str().ok()).and_then(|c| ANSWERS.into_iter().find(|a| *a == c));
    let text = res.text().await?;
    Err(match code {
        Some(code) => crate::codes::coded(code, text),
        None => anyhow::anyhow!(text),
    })
}

/// A checked append as the leader is sent it: u32 header length | JSON (table, src) | Arrow IPC.
fn encode(table: &str, src: &Src, batch: &RecordBatch) -> Result<Vec<u8>> {
    let head = serde_json::to_vec(&(table, src))?;
    let rows = crate::log::encode_ipc(std::slice::from_ref(batch))?;
    Ok([&(head.len() as u32).to_le_bytes()[..], &head, &rows].concat())
}

/// `encode`'s body, read back: a body cut short is an error (`tools/fuzz_doors.py` sends such).
pub fn decode(body: bytes::Bytes) -> Result<(String, Src, RecordBatch)> {
    let n = u32::from_le_bytes(body.get(..4).ok_or_else(|| anyhow::anyhow!("an empty append"))?.try_into()?) as usize;
    body.get(4..4usize.saturating_add(n)).ok_or_else(|| anyhow::anyhow!("an append cut short"))?;
    let (table, src): (String, Src) = serde_json::from_slice(&body[4..4 + n])?;
    let rows = crate::log::decode(body.get(4 + n..).ok_or_else(|| anyhow::anyhow!("an append cut short"))?)?;
    let batch = rows.into_iter().next().ok_or_else(|| anyhow::anyhow!("an append with no rows"))?;
    Ok((table, src, batch))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{ArrayRef, Int64Array, StringArray};

    fn ints(v: &[Option<i64>]) -> ArrayRef {
        Arc::new(Int64Array::from(v.to_vec()))
    }

    #[test]
    fn two_rows_of_one_value_are_found_by_their_hash_as_by_their_bytes() {
        let names: ArrayRef = Arc::new(StringArray::from(vec![Some("a"), Some("b"), None, Some("a")]));
        let cases: Vec<(Vec<ArrayRef>, Vec<ArrayRef>, Option<&str>)> = vec![
            (vec![ints(&[Some(1), Some(2), Some(3)])], vec![], None),
            (vec![ints(&[Some(1), Some(2), Some(1)])], vec![], Some("1")),
            (vec![ints(&[None, Some(2), None])], vec![], None), // (NULLs never meet)
            (vec![names.clone()], vec![], Some("a")),
            (vec![ints(&[Some(1), Some(1), Some(2), Some(2)]), names.clone()], vec![], None), // (two columns: each pair differs)
            (vec![ints(&[Some(7), Some(7)])], vec![ints(&[Some(1), Some(1)])], None), // (UNIQUE: one key's versions)
            (vec![ints(&[Some(7), Some(7)])], vec![ints(&[Some(1), Some(2)])], Some("7")), // (UNIQUE: two keys)
        ];
        for (values, keys, want) in cases {
            let by_hash = hashed(&values, &keys).expect("decided");
            assert_eq!(by_hash, exactly(&values, &keys).expect("decided"), "{values:?} {keys:?}");
            assert_eq!(by_hash.as_deref(), want, "{values:?} {keys:?}");
        }
    }
}
