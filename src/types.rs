//! Enum types (round 34): `CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')`, as Postgres and
//! DuckDB make them, and MySQL's `ENUM('a', 'b')` written as a column's type. A column of one holds
//! text in its files, so Delta and Iceberg readers and every client read it as a string, and every
//! door that writes rows refuses a value its labels don't list (`check`, from `rules::checks`:
//! Postgres's 22P02, the writer's alone). Each table keeps the labels of its enum columns
//! (`TableMeta::enums`), so a write's check reads nothing else; `ALTER TYPE … ADD VALUE` adds the
//! label to every table using the type, in the type's own commit. A label is never renamed or
//! taken out: rows keep the text they were written with, and files are never rewritten. Values
//! compare and sort as text, not in their labels' order (`array_position(enum_range(NULL::mood),
//! m)` is that order).
//!
//! A type is in a schema, under `ty/{schema.name}`; it is listed in `pondra.objects`, shown by
//! `SHOW CREATE TYPE`, described by `COMMENT ON TYPE`, and isn't dropped while a column uses it.
//! Postgres's catalog lists it as Postgres does (`pg_type`, `pg_enum`, the column's type and
//! `information_schema`: `pg_catalog.rs`), while the wire sends the values as text. Where SQL comes
//! in, a cast to it is a cast to text (a literal checked against its labels) and `enum_range`,
//! `enum_first` and `enum_last` are its labels (`pg_casts::rewrite`).
//!
//! `UUID`, `JSON`, `JSONB` and `VARCHAR(n)` are held as text too, as Delta, Spark, Snowflake and BigQuery hold one, and
//! how each keeps and refuses its values is `rules::typed`'s.
use crate::store::{json, table_key, Lake, TableMeta};
use crate::write::Stmt;
use anyhow::{ensure, Context, Result};
use datafusion::sql::sqlparser::ast::{self, Statement};
use datafusion::sql::sqlparser::{dialect::GenericDialect, parser::Parser};
use serde::{Deserialize, Serialize};
use serde_json::{json as j, Value};
use std::collections::BTreeMap;
use std::sync::LazyLock;
use crate::pg_casts::quoted;
use crate::rules::create::{enum_labels, labelled, sql_type};
use crate::rules::typed::length;

pub fn key(name: &str) -> String { format!("ty/{name}") }

/// A type as the catalog keeps it: its labels, in order.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Type {
    pub labels: Vec<String>,
}

/// An enum column, as its table keeps it: the labels it takes, and the type it is of (none for
/// `ENUM('a', 'b')` written in the column).
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Enum {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub of: Option<String>,
    pub labels: Vec<String>,
}

impl Enum {
    /// The column's type as SQL writes it: its type's name, or `ENUM('a', 'b')`.
    pub fn sql(&self) -> String {
        match &self.of {
            Some(t) => crate::objects::name_sql(t),
            None => format!("ENUM({})", quoted(&self.labels)),
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(rename_all = "snake_case")]
pub enum Change {
    Create { name: String, labels: Vec<String>, if_not_exists: bool },
    Add { name: String, label: String, if_not_exists: bool, before: Option<String>, after: Option<String> },
    Rename { name: String, to: String },
    Drop { names: Vec<String>, if_exists: bool },
}

/// `CREATE TYPE … AS ENUM`, `ALTER TYPE`, `DROP TYPE`, or None for anything else.
pub fn statement(sql: &str) -> Option<Stmt> {
    static WORD: LazyLock<regex::Regex> = LazyLock::new(|| regex::Regex::new(r"(?is)^\s*(CREATE(\s+OR\s+REPLACE)?|ALTER|DROP)\s+TYPE\b").expect("a regex"));
    static UNLESS: LazyLock<regex::Regex> = LazyLock::new(|| regex::Regex::new(r"(?is)^(\s*CREATE\s+TYPE\s+)IF\s+NOT\s+EXISTS\s+").expect("a regex"));
    static DROP: LazyLock<regex::Regex> = LazyLock::new(|| regex::Regex::new(r#"(?is)^\s*DROP\s+TYPE\s+(IF\s+EXISTS\s+)?(.+?)(\s+(CASCADE|RESTRICT))?\s*;?\s*$"#).expect("a regex"));
    let first = crate::text::first_word(sql);
    let c = WORD.captures(first)?;
    let ddl = |c: Change| Some(Stmt::Ddl(vec![crate::ddl::Ddl::Type(c)]));
    let invalid = |e: String| Some(Stmt::Invalid(e));
    let word = c[1].to_uppercase();
    if c.get(2).is_some() {
        return invalid("CREATE OR REPLACE TYPE: a type's columns keep their labels (ALTER TYPE … ADD VALUE adds one)".into());
    }
    if word == "DROP" {
        // (CASCADE and RESTRICT are the same here: a drop never takes a column with it, so a type a column
        // uses is refused by name whichever is written, and one no column uses is dropped either way)
        let d = DROP.captures(first)?;
        let names = d[2].split(',').map(|n| crate::seq::object_of(n.trim())).collect();
        return ddl(Change::Drop { names, if_exists: d.get(1).is_some() });
    }
    let unless = UNLESS.is_match(first);
    let text = UNLESS.replace(first, "$1");
    let parsed = Parser::parse_sql(&GenericDialect {}, &text).map(|mut s| s.pop());
    Some(match parsed {
        Ok(Some(Statement::CreateType { name, representation: Some(ast::UserDefinedTypeRepresentation::Enum { labels }) })) => {
            return ddl(Change::Create { name: crate::text::object(&name), labels: labels.iter().map(|l| l.value.clone()).collect(), if_not_exists: unless })
        }
        Ok(Some(Statement::CreateType { .. })) | Err(_) if word.starts_with("CREATE") => Stmt::Invalid("CREATE TYPE [IF NOT EXISTS] name AS ENUM ('label', …): enums are the types kept (a composite, a range or another type's alias isn't)".into()),
        Ok(Some(Statement::AlterType(a))) => {
            let name = crate::text::object(&a.name);
            match a.operation {
                ast::AlterTypeOperation::Rename(r) => return ddl(Change::Rename { name, to: crate::text::ident(&r.new_name) }),
                ast::AlterTypeOperation::AddValue(v) => {
                    let (before, after) = match v.position {
                        Some(ast::AlterTypeAddValuePosition::Before(n)) => (Some(n.value), None),
                        Some(ast::AlterTypeAddValuePosition::After(n)) => (None, Some(n.value)),
                        None => (None, None),
                    };
                    return ddl(Change::Add { name, label: v.value.value, if_not_exists: v.if_not_exists, before, after });
                }
                ast::AlterTypeOperation::RenameValue(_) => Stmt::Invalid("ALTER TYPE … RENAME VALUE: rows keep the text they were written with (files are never rewritten); add the new label, UPDATE the rows to it".into()),
            }
        }
        _ => Stmt::Invalid("ALTER TYPE name ADD VALUE [IF NOT EXISTS] 'label' [BEFORE | AFTER 'label'] | RENAME TO new_name".into()),
    })
}

/// Leader: carry one out.
pub async fn apply(lake: &Lake, c: Change) -> Result<Value> {
    match c {
        Change::Create { name, labels, if_not_exists } => {
            let name = crate::ddl::new_name(lake, &name).await?;
            if lake.cat.get::<Type>(&key(&name)).await?.is_some() {
                ensure!(if_not_exists, "type \"{name}\" already exists");
                return Ok(j!({"type": name, "exists": true, "notice": format!("type \"{name}\" already exists, skipping")}));
            }
            labelled(&labels)?;
            lake.cat.commit(vec![(key(&name), json(&Type { labels }))], &[]).await?;
            Ok(j!({"type": name}))
        }
        Change::Add { name, label, if_not_exists, before, after } => {
            let (name, mut t) = find(lake, &name).await?.with_context(|| format!("type \"{name}\" does not exist"))?;
            if t.labels.contains(&label) {
                ensure!(if_not_exists, "enum label \"{label}\" already exists");
                return Ok(j!({"type": name, "unchanged": true, "notice": format!("enum label \"{label}\" already exists, skipping")}));
            }
            let at = match (&before, &after) {
                (Some(n), _) => t.labels.iter().position(|l| l == n).with_context(|| format!("\"{n}\" is not an existing enum label"))?,
                (_, Some(n)) => t.labels.iter().position(|l| l == n).with_context(|| format!("\"{n}\" is not an existing enum label"))? + 1,
                _ => t.labels.len(),
            };
            t.labels.insert(at, label.clone());
            labelled(&t.labels)?;
            // (every table using it takes the label in the same commit: a write checks its own copy)
            let mut puts = vec![(key(&name), json(&t))];
            for (k, mut m) in users(lake, &name).await? {
                m.enums.values_mut().filter(|e| e.of.as_deref() == Some(name.as_str())).for_each(|e| e.labels = t.labels.clone());
                puts.push((k, json(&m)));
            }
            lake.cat.commit(puts, &[]).await?;
            Ok(j!({"type": name, "added": label}))
        }
        Change::Rename { name, to } => {
            let (name, t) = find(lake, &name).await?.with_context(|| format!("type \"{name}\" does not exist"))?;
            let to = crate::ddl::new_name(lake, &crate::text::join(crate::text::split(&name).0, &to)).await?;
            ensure!(lake.cat.get::<Type>(&key(&to)).await?.is_none(), "type \"{to}\" already exists");
            let mut puts = vec![(key(&to), json(&t))];
            for (k, mut m) in users(lake, &name).await? {
                m.enums.values_mut().filter(|e| e.of.as_deref() == Some(name.as_str())).for_each(|e| e.of = Some(to.clone()));
                puts.push((k, json(&m)));
            }
            lake.cat.commit(puts, &[key(&name)]).await?;
            Ok(j!({"type": name, "renamed": to}))
        }
        Change::Drop { names, if_exists } => {
            let mut gone = vec![];
            for n in names {
                let Some((n, _)) = find(lake, &n).await? else {
                    ensure!(if_exists, "type \"{n}\" does not exist");
                    continue;
                };
                // Every column that uses the type, named as table.column (SQL's names), for the same message
                // whichever way the drop was written.
                let mut using = vec![];
                for (k, m) in users(lake, &n).await? {
                    for (c, e) in &m.enums {
                        if e.of.as_deref() == Some(n.as_str()) && !m.dropped.contains(c) {
                            using.push(format!("{}.{}", &k[2..], m.name_of(c)));
                        }
                    }
                }
                let (what, verb) = if using.len() == 1 { ("column", "uses") } else { ("columns", "use") };
                ensure!(using.is_empty(), "cannot drop type {n} because {what} {verb} it: {}; drop or change them first", using.join(", "));
                gone.push(n);
            }
            lake.cat.commit(vec![], &gone.iter().map(|n| key(n)).collect::<Vec<_>>()).await?;
            Ok(j!({"type": gone.first(), "dropped": gone}))
        }
    }
}

/// The type a name resolves to in this lake.
async fn find(lake: &Lake, name: &str) -> Result<Option<(String, Type)>> {
    let (other, local) = crate::ddl::resolve(lake, name).await?;
    ensure!(other.is_none(), "{name} is an attached lake's type: change it from a node of that lake");
    Ok(lake.cat.get::<Type>(&key(&local)).await?.map(|t| (local, t)))
}

/// The labels of the enum type `name` names in this lake, in order (`Host::enum_labels`); None when there is no such type.
pub(crate) async fn labels(lake: &Lake, name: &str) -> Option<Vec<String>> { find(lake, name).await.ok().flatten().map(|(_, t)| t.labels) }

/// The tables (their catalog keys and entries) with a column of this type, dropped columns aside.
async fn users(lake: &Lake, name: &str) -> Result<Vec<(String, TableMeta)>> {
    let all = lake.cat.scan::<TableMeta>("t/", "t0").await?;
    Ok(all.into_iter().filter(|(_, m)| m.enums.iter().any(|(c, e)| e.of.as_deref() == Some(name) && !m.dropped.contains(c))).collect())
}

/// A column's declared type, if it is an enum: `ENUM('a', 'b')`, or a type of this lake's.
pub async fn of(lake: &Lake, t: &ast::DataType) -> Result<Option<Enum>> {
    Ok(match t {
        ast::DataType::Enum(..) => enum_labels(t)?.map(|labels| Enum { of: None, labels }),
        ast::DataType::Custom(name, args) if args.is_empty() => {
            let name = crate::text::object(name);
            find(lake, &name).await?.map(|(of, t)| Enum { of: Some(of), labels: t.labels })
        }
        _ => None,
    })
}

/// A table's enum columns and its other SQL types' columns by SQL's names (`CREATE TABLE t LIKE
/// s`): none when it has none, or isn't this lake's table.
pub async fn of_table(lake: &Lake, name: &str) -> (BTreeMap<String, Enum>, BTreeMap<String, String>) {
    let Ok((None, local)) = crate::ddl::resolve(lake, name).await else { return Default::default() };
    match lake.cat.get::<TableMeta>(&table_key(&local)).await {
        Ok(Some(m)) => {
            let l = m.logical();
            (l.enums, l.sql_types)
        }
        _ => Default::default(),
    }
}

/// A table's declared columns with every enum's, uuid's and VARCHAR(n)'s type said as text (what its files
/// hold), the enums by column, and the other SQL types by column (`sql_types`).
pub async fn columns(lake: &Lake, cols: &[ast::ColumnDef]) -> Result<(Vec<ast::ColumnDef>, BTreeMap<String, Enum>, BTreeMap<String, String>)> {
    let (mut out, mut enums, mut sql_types) = (cols.to_vec(), BTreeMap::new(), BTreeMap::new());
    for c in out.iter_mut() {
        if let Some(e) = of(lake, &c.data_type).await? {
            enums.insert(crate::text::ident(&c.name), e);
            c.data_type = ast::DataType::Varchar(None);
        } else if let Some(t) = sql_type(&c.data_type)? {
            sql_types.insert(crate::text::ident(&c.name), t);
            c.data_type = ast::DataType::Varchar(None);
        }
    }
    Ok((out, enums, sql_types))
}

/// A column's type as SQL writes it (`SHOW CREATE`, `pondra.parts`): an enum's, `UUID`, or what its
/// Arrow type is (`objects::sql_type`). `m` and `column` alike by stored or alike by SQL's names.
pub fn shown(m: &TableMeta, column: &str, arrow: &str) -> String {
    match (m.enums.get(column), m.sql_types.get(column)) {
        (Some(e), _) => e.sql(),
        (None, Some(t)) => t.to_uppercase(),
        (None, None) => crate::objects::sql_type(arrow),
    }
}

/// A type as `ALTER COLUMN … TYPE` writes it, as `sql_type` keeps it (None for one it doesn't, or
/// one it can't read).
pub fn declared(written: &str) -> Result<Option<String>> {
    match Parser::new(&GenericDialect {}).try_with_sql(written).and_then(|mut p| p.parse_data_type()) {
        Ok(t) => sql_type(&t),
        Err(_) => Ok(None),
    }
}

/// A column held as `had` (None: as plain text) may be declared `wanted` from now on: its rows were
/// checked against that, or against a shorter length.
pub fn grows(had: Option<&str>, wanted: &str) -> bool {
    had == Some(wanted) || matches!((had.and_then(length), length(wanted)), (Some(k), Some(n)) if n >= k)
}

/// `CREATE TYPE name AS ENUM (…)`, as `SHOW CREATE TYPE` writes it.
pub fn create_sql(name: &str, t: &Type) -> String { format!("CREATE TYPE {name} AS ENUM ({})", quoted(&t.labels)) }
