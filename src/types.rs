//! Enum types (round 34): `CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')`, as Postgres and
//! DuckDB make them, and MySQL's `ENUM('a', 'b')` written as a column's type. A column of one holds
//! text in its files, so Delta and Iceberg readers and every client read it as a string, and every
//! door that writes rows refuses a value its labels don't list (`check`, from `defaults::check`:
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
//! `enum_first` and `enum_last` are its labels (`rewrite`).
//!
//! `UUID`, `JSON` and `JSONB` are held as text too, as Delta, Spark, Snowflake and BigQuery hold
//! one: every reader and client reads a string, and a uuid compares, sorts and joins as one. Each
//! table keeps which columns are uuids, json or jsonb (`TableMeta::sql_types`), and every door refuses
//! a value that isn't a uuid written Postgres's one way, lower case with hyphens (`check`), so two
//! spellings of one uuid never meet. A json value is refused (22P02) unless it parses as JSON; its
//! text is kept as written, so a `jsonb` isn't reordered or respaced as Postgres prints it.
//! SQL reads the other spellings Postgres takes as it does, and writes them that way: a cast to
//! `uuid` (`'A0EE…'::uuid`, `pondra_uuid`), a cast to `json` or `jsonb` (`pondra_json`, checked where
//! SQL comes in), a string compared with a uuid or written into one (`pg_types::rewrite`), and the
//! Postgres port's `COPY` (`spelled`); the loading doors of Pondra's own (JSON, Arrow, Kafka) take the
//! one way. The Postgres port sends a uuid, json or jsonb column as Postgres's own type
//! (`pg_types.rs`). `gen_random_uuid()` and
//! `uuidv4()` are DataFusion's `uuid()`; `uuidv7()` (Postgres 18's) starts with the time, so a key
//! of them comes in order and files hold narrow ranges of it.
//!
//! `VARCHAR(n)` (`CHARACTER VARYING(n)`, `NVARCHAR(n)`) is text whose values are at most `n`
//! characters, as Postgres, Snowflake and Spark hold one: kept as `varchar(n)` in `sql_types`, and
//! every door refuses a longer value (`check`: 22001), but for trailing spaces, which Postgres
//! takes and cuts (kept here). Its length only grows (`ALTER COLUMN … TYPE VARCHAR(m)`, m ≥ n, or
//! to plain text); a column holding text gets none, since its rows were never checked against one.
use crate::store::{json, table_key, Lake, TableMeta};
use crate::write::Stmt;
use anyhow::{ensure, Context, Result};
use datafusion::arrow::array::{Array, AsArray, RecordBatch};
use datafusion::arrow::datatypes::DataType;
use datafusion::sql::sqlparser::ast::{self, Statement, VisitMut, VisitorMut};
use datafusion::sql::sqlparser::{dialect::GenericDialect, parser::Parser};
use serde::{Deserialize, Serialize};
use serde_json::{json as j, Value};
use std::collections::{BTreeMap, HashMap};
use std::ops::ControlFlow;
use std::sync::LazyLock;

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

fn quoted(labels: &[String]) -> String { labels.iter().map(|l| format!("'{}'", l.replace('\'', "''"))).collect::<Vec<_>>().join(", ") }

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
    let first = crate::write::first_word(sql);
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
            return ddl(Change::Create { name: crate::write::object(&name), labels: labels.iter().map(|l| l.value.clone()).collect(), if_not_exists: unless })
        }
        Ok(Some(Statement::CreateType { .. })) | Err(_) if word.starts_with("CREATE") => Stmt::Invalid("CREATE TYPE [IF NOT EXISTS] name AS ENUM ('label', …): enums are the types kept (a composite, a range or another type's alias isn't)".into()),
        Ok(Some(Statement::AlterType(a))) => {
            let name = crate::write::object(&a.name);
            match a.operation {
                ast::AlterTypeOperation::Rename(r) => return ddl(Change::Rename { name, to: crate::write::ident(&r.new_name) }),
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
            let to = crate::ddl::new_name(lake, &crate::ddl::join(crate::ddl::split(&name).0, &to)).await?;
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

/// Labels an enum may have: some, each once, none empty or too long (Postgres's 63 bytes).
fn labelled(labels: &[String]) -> Result<()> {
    ensure!(!labels.is_empty(), "an enum has at least one label: ENUM ('a', 'b')");
    for (i, l) in labels.iter().enumerate() {
        ensure!(!l.is_empty() && l.len() <= 63, "invalid enum label \"{l}\": labels are 1 to 63 bytes long");
        ensure!(!labels[..i].contains(l), "enum label \"{l}\" used more than once");
    }
    Ok(())
}

/// The type a name resolves to in this lake.
async fn find(lake: &Lake, name: &str) -> Result<Option<(String, Type)>> {
    let (other, local) = crate::ddl::resolve(lake, name).await?;
    ensure!(other.is_none(), "{name} is an attached lake's type: change it from a node of that lake");
    Ok(lake.cat.get::<Type>(&key(&local)).await?.map(|t| (local, t)))
}

/// The tables (their catalog keys and entries) with a column of this type, dropped columns aside.
async fn users(lake: &Lake, name: &str) -> Result<Vec<(String, TableMeta)>> {
    let all = lake.cat.scan::<TableMeta>("t/", "t0").await?;
    Ok(all.into_iter().filter(|(_, m)| m.enums.iter().any(|(c, e)| e.of.as_deref() == Some(name) && !m.dropped.contains(c))).collect())
}

/// A column's declared type, if it is an enum: `ENUM('a', 'b')`, or a type of this lake's.
pub async fn of(lake: &Lake, t: &ast::DataType) -> Result<Option<Enum>> {
    Ok(match t {
        ast::DataType::Enum(members, _) => {
            let labels: Vec<String> = members.iter().map(|m| match m {
                ast::EnumMember::Name(n) | ast::EnumMember::NamedValue(n, _) => n.clone(),
            }).collect();
            labelled(&labels)?;
            Some(Enum { of: None, labels })
        }
        ast::DataType::Custom(name, args) if args.is_empty() => {
            let name = crate::write::object(name);
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
            enums.insert(crate::write::ident(&c.name), e);
            c.data_type = ast::DataType::Varchar(None);
        } else if let Some(t) = sql_type(&c.data_type)? {
            sql_types.insert(crate::write::ident(&c.name), t);
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

/// A column's declared type, if it is one held as text that isn't an enum: `uuid`, `json`, `jsonb`, or
/// `varchar(n)` for a length (`VARCHAR(n)`, `CHARACTER VARYING(n)`, `NVARCHAR(n)`). An array of uuids is
/// refused by name (a list's items aren't checked yet).
pub fn sql_type(t: &ast::DataType) -> Result<Option<String>> {
    use ast::{ArrayElemTypeDef as A, CharacterLength as L, DataType as D};
    match t {
        D::Uuid => Ok(Some("uuid".into())),
        D::JSON => Ok(Some("json".into())),
        D::JSONB => Ok(Some("jsonb".into())),
        D::Varchar(Some(L::IntegerLength { length, .. }))
        | D::CharacterVarying(Some(L::IntegerLength { length, .. }))
        | D::CharVarying(Some(L::IntegerLength { length, .. }))
        | D::Nvarchar(Some(L::IntegerLength { length, .. })) => match *length {
            0 => Err(crate::codes::coded("22023", format!("{t}: a length is at least 1"))),
            n => Ok(Some(format!("varchar({n})"))),
        },
        D::Array(A::AngleBracket(e) | A::SquareBracket(e, _) | A::Parenthesis(e)) if matches!(**e, D::Uuid) => {
            Err(crate::codes::coded("0A000", "UUID[]: an array of uuids isn't kept yet (TEXT[] holds them as text, or a table of its own holds one a row)"))
        }
        _ => Ok(None),
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

/// The most characters a `varchar(n)` column's values have.
pub fn length(t: &str) -> Option<usize> {
    t.strip_prefix("varchar(")?.strip_suffix(')')?.parse().ok()
}

/// A type a column keeps as text and checks (`TableMeta::sql_types`): `uuid`, `json`, `jsonb` or `varchar(n)`.
pub fn held(t: &str) -> bool { matches!(t, "uuid" | "json" | "jsonb") || length(t).is_some() }

/// `v` is longer than `n` characters, but for trailing spaces (Postgres takes those, and cuts them).
fn too_long(v: &str, n: usize) -> bool {
    v.len() > n && v.trim_end_matches(' ').chars().count() > n
}

/// `s` is a uuid as Postgres writes one, the only way a uuid column holds it: 36 characters, lower
/// case, hyphens after the 8th, 12th, 16th and 20th digits.
pub fn is_uuid(s: &str) -> bool {
    s.len() == 36 && s.bytes().enumerate().all(|(i, b)| if matches!(i, 8 | 13 | 18 | 23) { b == b'-' } else { b.is_ascii_digit() || (b'a'..=b'f').contains(&b) })
}

/// Any spelling of a uuid Postgres takes (upper case, braces, no hyphens), written the one way;
/// refused as Postgres refuses it (22P02).
pub fn uuid_of(s: &str) -> Result<String> {
    match uuid::Uuid::try_parse(s) {
        Ok(u) => Ok(u.hyphenated().to_string()),
        Err(_) => Err(crate::codes::coded("22P02", format!("invalid input syntax for type uuid: \"{s}\""))),
    }
}

/// `b`'s uuid columns (`sql_types`, by `b`'s names) written the one way, from any spelling Postgres
/// takes, as Postgres's `COPY` reads them; what isn't a uuid is refused (22P02). A column already
/// written so is left as it is (nearly always: a pass over its values).
pub fn spelled(b: RecordBatch, sql_types: &BTreeMap<String, String>) -> Result<RecordBatch> {
    let mut cols = b.columns().to_vec();
    for (i, f) in b.schema().fields().iter().enumerate() {
        let Some(a) = cols[i].as_string_opt::<i32>().filter(|_| sql_types.get(f.name()).is_some_and(|t| t == "uuid")) else { continue };
        if a.iter().flatten().all(is_uuid) {
            continue;
        }
        let one_way: datafusion::arrow::array::StringArray = a.iter().map(|v| v.map(uuid_of).transpose()).collect::<Result<_>>()?;
        cols[i] = std::sync::Arc::new(one_way);
    }
    Ok(RecordBatch::try_new(b.schema(), cols)?)
}

/// Why a uuid column refuses a value: not a uuid, or one written another way than the column holds.
fn refused_uuid(column: &str, s: &str) -> String {
    match uuid_of(s) {
        Ok(u) => format!("invalid input syntax for type uuid: \"{s}\" (column {column} holds a uuid one way, lower case with hyphens: write '{u}', or cast it, '{s}'::uuid)"),
        Err(_) => format!("invalid input syntax for type uuid: \"{s}\" (column {column} holds uuids)"),
    }
}

/// `s` parses as JSON (`json` and `jsonb` alike): its syntax is checked, nothing is built.
pub fn is_json(s: &str) -> bool { serde_json::from_str::<serde::de::IgnoredAny>(s).is_ok() }

/// Postgres's words for a value that isn't JSON, which it quotes as far as 60 characters (jsonb's
/// refusals say `type json` too).
pub(crate) fn json_refusal(s: &str) -> String {
    let head: String = s.chars().take(60).collect();
    format!("invalid input syntax for type json: \"{head}\"")
}

/// `s` as JSON text the lake keeps: itself when it parses as JSON, else Postgres's refusal (22P02).
pub fn json_of(s: &str) -> Result<String> {
    match is_json(s) {
        true => Ok(s.to_string()),
        false => Err(crate::codes::coded("22P02", json_refusal(s))),
    }
}

/// Why a value of a column held as `t` is refused, with its message and SQLSTATE; None when it is taken.
/// The column's name is the one the message gives.
fn refusal(t: &str, column: &str, v: &str) -> Option<(String, &'static str)> {
    match t {
        "uuid" if !is_uuid(v) => Some((refused_uuid(column, v), "22P02")),
        "json" | "jsonb" if !is_json(v) => Some((format!("{} (column {column} holds {t})", json_refusal(v)), "22P02")),
        _ => match length(t) {
            Some(n) if too_long(v, n) => Some((format!("value too long for type character varying({n}) (column {column}: a value of {} characters)", v.chars().count()), "22001")),
            _ => None,
        },
    }
}

/// Every door's rows (`defaults::check`): a value its column's enum doesn't list, or that isn't a
/// uuid written the one way, or JSON, is refused (Postgres's 22P02), and one longer than its VARCHAR(n)
/// (22001), the writer's alone. `marker`: a keyed table's delete markers carry no values.
pub fn check(meta: &TableMeta, rows: &RecordBatch, sql_names: bool, marker: &dyn Fn(usize) -> bool) -> Result<()> {
    for (stored, e) in &meta.enums {
        let Some(c) = rows.column_by_name(if sql_names { meta.name_of(stored) } else { stored }) else { continue };
        if c.null_count() == c.len() {
            continue;
        }
        let text = datafusion::arrow::compute::cast(c, &DataType::Utf8)?;
        let text = text.as_string::<i32>();
        let labels: std::collections::HashSet<&str> = e.labels.iter().map(String::as_str).collect();
        if let Some(i) = (0..text.len()).find(|&i| text.is_valid(i) && !labels.contains(text.value(i)) && !marker(i)) {
            let what = e.of.clone().unwrap_or_else(|| format!("column {}", meta.name_of(stored)));
            let message = format!("invalid input value for enum {what}: \"{}\" (its labels: {})", text.value(i), quoted(&e.labels));
            return Err(anyhow::Error::new(crate::views::Violation(message, "22P02")));
        }
    }
    for (stored, t) in &meta.sql_types {
        let Some(c) = rows.column_by_name(if sql_names { meta.name_of(stored) } else { stored }) else { continue };
        if !held(t) || c.null_count() == c.len() {
            continue; // (a type a newer release keeps: its own nodes check it)
        }
        let text = datafusion::arrow::compute::cast(c, &DataType::Utf8)?;
        let text = text.as_string::<i32>();
        let column = meta.name_of(stored);
        let first = (0..text.len()).filter(|&i| text.is_valid(i) && !marker(i)).find_map(|i| refusal(t, column, text.value(i)));
        if let Some((message, code)) = first {
            return Err(anyhow::Error::new(crate::views::Violation(message, code)));
        }
    }
    Ok(())
}

/// Could `sql` name a type of one's own, in a cast or one of the enum functions? (Only then is
/// it parsed for one: the built-in types' names are left alone.)
pub fn wanted(sql: &str) -> bool {
    static SET_FUNCTION: LazyLock<regex::Regex> = LazyLock::new(|| regex::Regex::new(r"(?i)\b(generate_series|range|unnest)\s*\(").expect("a regex"));
    if SET_FUNCTION.is_match(sql) {
        return true; // (a set function in a FROM that reads a column is written by `lateral::functions`)
    }
    static NAMED: LazyLock<regex::Regex> = LazyLock::new(|| regex::Regex::new(r#"(?i)(?:::|\bas)\s*"?([a-z_][a-z0-9_]*)"?\s*(?:[.]\s*"?([a-z_][a-z0-9_]*))?|\b(?:enum_(?:range|first|last)|pg_typeof)\s*\(|'(\{?[0-9a-fA-F]{8}-?(?:[0-9a-fA-F]{4}-?){3}[0-9a-fA-F]{12}\}?)'"#).expect("a regex"));
    const BUILT_IN: &[&str] = &["int", "integer", "bigint", "smallint", "tinyint", "int2", "int4", "int8", "hugeint", "ubigint", "uinteger", "usmallint", "utinyint", "float", "float4",
        "float8", "real", "double", "decimal", "numeric", "dec", "text", "varchar", "char", "character", "string", "bpchar", "bool", "boolean", "date", "time", "timestamp", "timestamptz",
        "datetime", "interval", "json", "jsonb", "uuid", "bytea", "blob", "binary", "varbinary", "bytes", "regclass", "regtype", "regproc", "regnamespace", "oid", "name", "variant", "bit"];
    NAMED.captures_iter(sql).any(|c| match (c.get(1), c.get(2)) {
        _ if c.get(3).is_some() => c.get(3).is_some_and(|s| !is_uuid(s.as_str())), // (`'A0EE…'`: read as a uuid where one is compared with it or written into one)
        (None, _) => true, // (enum_range(…), pg_typeof(…))
        (Some(_), Some(_)) => true,
        (Some(n), None) => ["uuid", "json", "jsonb"].iter().any(|t| n.as_str().eq_ignore_ascii_case(t)) || !BUILT_IN.contains(&n.as_str().to_lowercase().as_str()), // (a cast to uuid, json or jsonb: written as text)
    })
}

/// Where SQL comes in: a cast to an enum type is a cast to text (a literal one of its labels, or
/// refused as Postgres refuses it), and `enum_range(NULL::t)`, `enum_first`, `enum_last` its
/// labels. A cast to `uuid` is the uuid written the one way a column holds it: a literal at once
/// (`'A0EE…'::uuid` is `'a0ee…'`), anything else by `pondra_uuid(x)` (`TRY_CAST`: `pondra_uuid(x,
/// true)`, NULL for what isn't one). A string compared with a uuid or written into one is read as one, and
/// `pg_typeof(x)` of one is 'uuid' (`pg_types::rewrite`), before the casts it may read are written out.
/// Whether anything changed.
pub async fn rewrite(lake: &Lake, stmts: &mut [Statement]) -> Result<bool> {
    let mut read = false;
    for s in stmts.iter_mut() {
        read |= crate::pg_types::rewrite(lake, s).await; // (strings read as uuids, pg_typeof)
    }
    for s in stmts.iter_mut() {
        read |= crate::lateral::functions(s)?; // (`generate_series(1, t.n)` in a FROM: a LATERAL subquery it can plan)
    }
    let mut f = Casts { types: HashMap::new(), named: vec![], labelled: vec![], typed: false, collecting: true, failed: None };
    for s in stmts.iter_mut() {
        let _ = s.visit(&mut f); // (the first pass only collects the names)
    }
    if f.named.is_empty() && !f.typed {
        return Ok(read);
    }
    for n in std::mem::take(&mut f.named) {
        if let Ok(Some((_, t))) = find(lake, &n).await {
            f.types.insert(n, t);
        }
    }
    if let Some(n) = f.labelled.iter().find(|n| !f.types.contains_key(*n)) {
        return Err(crate::codes::coded("42704", format!("type \"{n}\" does not exist"))); // (not DataFusion's "Invalid function 'enum_range'")
    }
    if f.types.is_empty() && !f.typed {
        return Ok(read);
    }
    f.collecting = false;
    for s in stmts.iter_mut() {
        let _ = s.visit(&mut f);
    }
    match f.failed {
        Some(e) => Err(e),
        None => Ok(true),
    }
}

struct Casts {
    types: HashMap<String, Type>, // the names found, by their name as written (`crate::write::object`)
    named: Vec<String>,
    labelled: Vec<String>, // the types `enum_range` and its kin name, which must be there
    typed: bool,           // a cast to uuid, json or jsonb was found
    collecting: bool,      // the first pass, which only finds what is named
    failed: Option<anyhow::Error>,
}

impl Casts {
    fn custom(t: &ast::DataType) -> Option<String> {
        match t {
            ast::DataType::Custom(name, args) if args.is_empty() => Some(crate::write::object(name)),
            _ => None,
        }
    }

    /// `NULL::t` or `CAST(NULL AS t)`: the type it names.
    fn null_of(e: &ast::Expr) -> Option<String> {
        match e {
            ast::Expr::Cast { data_type, .. } => Self::custom(data_type),
            _ => None,
        }
    }
}

impl VisitorMut for Casts {
    type Break = ();

    fn pre_visit_expr(&mut self, e: &mut ast::Expr) -> ControlFlow<()> {
        if self.failed.is_some() {
            return ControlFlow::Break(());
        }
        let collecting = self.collecting;
        if let ast::Expr::Cast { data_type, expr, kind, .. } = e {
            if let Some(to) = held_cast(data_type) {
                if collecting {
                    self.typed = true;
                    return ControlFlow::Continue(());
                }
                let trying = matches!(kind, ast::CastKind::TryCast | ast::CastKind::SafeCast);
                let literal = match expr.as_ref() {
                    ast::Expr::Value(ast::ValueWithSpan { value: ast::Value::SingleQuotedString(text), .. }) => Some(text.clone()),
                    _ => None,
                };
                let written = match literal.map(|text| one_way(to, &text)) {
                    // (a json literal stays a call of its type's function: a projection names `'{}'::json` and
                    // `'{}'::jsonb` apart, as Postgres does, where two equal literals would clash)
                    Some(Ok(v)) if to == "uuid" => ast::Expr::value(ast::Value::SingleQuotedString(v)),
                    Some(Ok(v)) => function(&format!("pondra_{to}"), vec![ast::Expr::value(ast::Value::SingleQuotedString(v))]),
                    Some(Err(_)) if trying => ast::Expr::value(ast::Value::Null),
                    Some(Err(err)) => {
                        self.failed = Some(err);
                        return ControlFlow::Break(());
                    }
                    None => {
                        let mut args = vec![std::mem::replace(expr.as_mut(), ast::Expr::value(ast::Value::Null))];
                        if trying {
                            args.push(ast::Expr::value(ast::Value::Boolean(true)));
                        }
                        function(&format!("pondra_{to}"), args)
                    }
                };
                *e = written;
                return ControlFlow::Continue(());
            }
        }
        if let ast::Expr::Function(f) = e {
            let word = f.name.to_string().to_lowercase();
            if matches!(word.as_str(), "enum_range" | "enum_first" | "enum_last") {
                let ast::FunctionArguments::List(args) = &f.args else { return ControlFlow::Continue(()) };
                let [ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(arg))] = &args.args[..] else { return ControlFlow::Continue(()) };
                let Some(name) = Self::null_of(arg) else { return ControlFlow::Continue(()) };
                if collecting {
                    self.labelled.push(name.clone());
                    self.named.push(name);
                    return ControlFlow::Continue(());
                }
                let Some(t) = self.types.get(&name) else { return ControlFlow::Continue(()) };
                let text = match word.as_str() {
                    "enum_range" => format!("make_array({})", quoted(&t.labels)),
                    "enum_first" => quoted(&t.labels[..1]),
                    _ => quoted(&t.labels[t.labels.len() - 1..]),
                };
                *e = Parser::new(&GenericDialect {}).try_with_sql(&text).and_then(|mut p| p.parse_expr()).expect("labels as SQL");
                return ControlFlow::Continue(());
            }
        }
        if let ast::Expr::Cast { data_type, expr, .. } = e {
            let Some(name) = Self::custom(data_type) else { return ControlFlow::Continue(()) };
            if collecting {
                self.named.push(name);
                return ControlFlow::Continue(());
            }
            let Some(t) = self.types.get(&name) else { return ControlFlow::Continue(()) };
            if let ast::Expr::Value(v) = expr.as_ref() {
                if let ast::Value::SingleQuotedString(s) = &v.value {
                    if !t.labels.contains(s) {
                        self.failed = Some(crate::codes::coded("22P02", format!("invalid input value for enum {name}: \"{s}\" (its labels: {})", quoted(&t.labels))));
                        return ControlFlow::Break(());
                    }
                }
            }
            *data_type = ast::DataType::Varchar(None);
        }
        ControlFlow::Continue(())
    }
}

/// The type a cast writes its value as, when the lake holds the value as text that is checked: "uuid",
/// "json" or "jsonb" (the function that writes it is `pondra_{type}`).
fn held_cast(t: &ast::DataType) -> Option<&'static str> {
    match t {
        ast::DataType::Uuid => Some("uuid"),
        ast::DataType::JSON => Some("json"),
        ast::DataType::JSONB => Some("jsonb"),
        _ => None,
    }
}

/// `text` written as a value of `to` ("uuid", "json" or "jsonb"): the one way the lake holds it, or Postgres's refusal.
fn one_way(to: &str, text: &str) -> Result<String> {
    match to {
        "uuid" => uuid_of(text),
        _ => json_of(text),
    }
}

/// `name(args…)` as SQL's tree has a call.
pub fn function(name: &str, args: Vec<ast::Expr>) -> ast::Expr {
    ast::Expr::Function(ast::Function {
        name: ast::ObjectName::from(vec![ast::Ident::new(name)]),
        uses_odbc_syntax: false,
        parameters: ast::FunctionArguments::None,
        args: ast::FunctionArguments::List(ast::FunctionArgumentList {
            duplicate_treatment: None,
            args: args.into_iter().map(|a| ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(a))).collect(),
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
    })
}

/// The functions every session has for these types: `gen_random_uuid()` and `uuidv4()` (DataFusion's
/// `uuid()` by Postgres's names), `uuidv7()`, `pondra_uuid(text [, try])`, `pondra_json(text [, try])` and
/// `pondra_jsonb(text [, try])` (a cast to uuid, json or jsonb: `rewrite`) and `pg_typeof(x)`.
pub fn register(ctx: &datafusion::prelude::SessionContext) {
    use datafusion::logical_expr::{Signature, TypeSignature, Volatility};
    let uuid = datafusion::functions::string::uuid();
    ctx.register_udf(uuid.as_ref().clone().with_aliases(["gen_random_uuid", "uuidv4"]));
    ctx.register_udf(made("uuidv7", Signature::nullary(Volatility::Volatile), |_, n| {
        Ok(std::sync::Arc::new(datafusion::arrow::array::StringArray::from_iter_values((0..n).map(|_| uuid::Uuid::now_v7().hyphenated().to_string()))))
    }));
    let text = || TypeSignature::String(1);
    let trying = || TypeSignature::Exact(vec![DataType::Utf8, DataType::Boolean]);
    ctx.register_udf(made("pondra_uuid", Signature::one_of(vec![text(), trying()], Volatility::Immutable), |a, _| {
        let text = datafusion::arrow::compute::cast(&a[0], &DataType::Utf8)?;
        let out = text.as_string::<i32>().iter().map(|v| match v.map(uuid_of) {
            Some(Ok(u)) => Ok(Some(u)),
            Some(Err(_)) if is_trying(a) => Ok(None),
            Some(Err(e)) => Err(datafusion::error::DataFusionError::Execution(e.to_string())),
            None => Ok(None),
        }).collect::<datafusion::error::Result<datafusion::arrow::array::StringArray>>()?;
        Ok(std::sync::Arc::new(out))
    }));
    // (`pondra_jsonb` is its own function, so a projection names a cast to each apart; its text is jsonb as Postgres prints it)
    let cast_sig = || Signature::one_of(vec![text(), trying()], Volatility::Immutable);
    ctx.register_udf(made_marked("pondra_json", cast_sig(), "json", |a, _| cast_each(a, json_text)));
    ctx.register_udf(made_marked("pondra_jsonb", cast_sig(), "jsonb", |a, _| cast_each(a, crate::pg_json_make::jsonb_text)));
    ctx.register_udf(made("pg_typeof", Signature::any(1, Volatility::Immutable), |a, n| {
        let name = crate::pg_catalog::type_of(a[0].data_type());
        Ok(std::sync::Arc::new(datafusion::arrow::array::StringArray::from_iter_values((0..n).map(|_| name.clone()))))
    }));
}

type Body = fn(&[datafusion::arrow::array::ArrayRef], usize) -> datafusion::error::Result<datafusion::arrow::array::ArrayRef>;

/// A function of `register`'s: text out, its rows' count known (a function of no arguments needs it). `mark` is the
/// JSON type its answers are ("json" or "jsonb"), if they are JSON: the column they make says so.
#[derive(Debug)]
struct Made {
    name: &'static str,
    signature: datafusion::logical_expr::Signature,
    mark: Option<&'static str>,
    body: Body,
}

// (one of each name: its name says which, never its code's address)
impl PartialEq for Made {
    fn eq(&self, other: &Self) -> bool { self.name == other.name }
}

impl Eq for Made {}

impl std::hash::Hash for Made {
    fn hash<H: std::hash::Hasher>(&self, h: &mut H) { self.name.hash(h) }
}

impl datafusion::logical_expr::ScalarUDFImpl for Made {
    fn name(&self) -> &str { self.name }
    fn signature(&self) -> &datafusion::logical_expr::Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> datafusion::error::Result<DataType> { Ok(DataType::Utf8) }
    // (the mark rides on the field, so a projection of the answer keeps it and the port sends it as that type)
    fn return_field_from_args(&self, _: datafusion::logical_expr::ReturnFieldArgs) -> datafusion::error::Result<datafusion::arrow::datatypes::FieldRef> {
        let field = datafusion::arrow::datatypes::Field::new(self.name, DataType::Utf8, true);
        Ok(std::sync::Arc::new(match self.mark {
            Some(mark) => field.with_metadata([(crate::pg_types::MARK.to_string(), mark.to_string())].into()),
            None => field,
        }))
    }
    fn invoke_with_args(&self, args: datafusion::logical_expr::ScalarFunctionArgs) -> datafusion::error::Result<datafusion::logical_expr::ColumnarValue> {
        let n = args.number_rows;
        let arrays = args.args.iter().map(|a| a.to_array(n)).collect::<datafusion::error::Result<Vec<_>>>()?;
        Ok(datafusion::logical_expr::ColumnarValue::Array((self.body)(&arrays, n)?))
    }
}

fn made(name: &'static str, signature: datafusion::logical_expr::Signature, body: Body) -> datafusion::logical_expr::ScalarUDF {
    datafusion::logical_expr::ScalarUDF::new_from_impl(Made { name, signature, mark: None, body })
}

/// `made`, whose answers are JSON text of the type `mark` names ("json" or "jsonb").
fn made_marked(name: &'static str, signature: datafusion::logical_expr::Signature, mark: &'static str, body: Body) -> datafusion::logical_expr::ScalarUDF {
    datafusion::logical_expr::ScalarUDF::new_from_impl(Made { name, signature, mark: Some(mark), body })
}

/// The second argument of `pondra_uuid(x, true)` and `pondra_json(x, true)`: a TRY_CAST, which answers
/// NULL for a value it refuses instead of failing.
fn is_trying(a: &[datafusion::arrow::array::ArrayRef]) -> bool { a.get(1).is_some_and(|t| t.as_boolean_opt().is_some_and(|t| t.is_valid(0) && t.value(0))) }

/// `s` as the json text the lake keeps (`json_of`), its refusal a query error with its SQLSTATE (22P02).
fn json_text(s: &str) -> datafusion::error::Result<String> {
    json_of(s).map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))
}

/// A cast of each text to json or jsonb: `of` answers each value, or refuses it with its own error. A `TRY_CAST`
/// answers NULL for a refusal instead.
fn cast_each(a: &[datafusion::arrow::array::ArrayRef], of: fn(&str) -> datafusion::error::Result<String>) -> datafusion::error::Result<datafusion::arrow::array::ArrayRef> {
    let text = datafusion::arrow::compute::cast(&a[0], &DataType::Utf8)?;
    let out = text.as_string::<i32>().iter().map(|v| match v.map(of) {
        Some(Ok(j)) => Ok(Some(j)),
        Some(Err(_)) if is_trying(a) => Ok(None),
        Some(Err(e)) => Err(e),
        None => Ok(None),
    }).collect::<datafusion::error::Result<datafusion::arrow::array::StringArray>>()?;
    Ok(std::sync::Arc::new(out))
}

/// `CREATE TYPE name AS ENUM (…)`, as `SHOW CREATE TYPE` writes it.
pub fn create_sql(name: &str, t: &Type) -> String { format!("CREATE TYPE {name} AS ENUM ({})", quoted(&t.labels)) }

#[cfg(test)]
mod tests {
    use super::*;

    /// The one text `sql` answers, through the functions `register` makes.
    async fn one(sql: &str) -> String {
        let ctx = datafusion::prelude::SessionContext::new();
        register(&ctx);
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        batches[0].column(0).as_string::<i32>().value(0).to_string()
    }

    #[tokio::test]
    async fn jsonb_is_written_as_postgres_prints_it_and_json_as_it_was_written() {
        assert_eq!(one(r#"SELECT pondra_jsonb('{"b":1,"a":2}')"#).await, r#"{"a": 2, "b": 1}"#);
        assert_eq!(one(r#"SELECT pondra_json('{"b":1,"a":2}')"#).await, r#"{"b":1,"a":2}"#);
    }
}
