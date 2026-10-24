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
//! text is kept as written. A `jsonb` value is written as jsonb writes it (`as_held`: keys sorted and
//! once, Postgres's spacing), so `=`, `GROUP BY` and `DISTINCT` compare values as Postgres does.
//! SQL reads the other spellings Postgres takes as it does, and writes them that way: a cast to
//! `uuid` (`'A0EE…'::uuid`, `pondra_uuid`), a cast to `json` or `jsonb` (`pondra_json`, checked where
//! SQL comes in), a string compared with a uuid or written into one (`pg_trace::rewrite`), and the
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
use crate::pg_json::json_refusal;
use crate::store::{json, table_key, Lake, TableMeta};
use crate::write::Stmt;
use anyhow::{ensure, Context, Result};
use datafusion::arrow::array::{Array, AsArray, RecordBatch};
use datafusion::arrow::datatypes::DataType;
use datafusion::sql::sqlparser::ast::{self, Statement, VisitMut};
use datafusion::sql::sqlparser::{dialect::GenericDialect, parser::Parser};
use serde::{Deserialize, Serialize};
use serde_json::{json as j, Value};
use std::collections::{BTreeMap, HashMap};
use std::sync::LazyLock;
use crate::text::is_uuid;
use crate::pg_casts::{quoted, Casts};
use crate::front::Host;

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
        ast::DataType::Enum(members, _) => {
            let labels: Vec<String> = members.iter().map(|m| match m {
                ast::EnumMember::Name(n) | ast::EnumMember::NamedValue(n, _) => n.clone(),
            }).collect();
            labelled(&labels)?;
            Some(Enum { of: None, labels })
        }
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

pub use crate::pg_casts::uuid_of;

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

/// `rows` with each jsonb column's values (`TableMeta::sql_types`) written as jsonb writes them (`jsonb_text`: keys
/// sorted and once, Postgres's spacing), so `=`, `GROUP BY` and `DISTINCT` compare values as Postgres's do; a json column
/// keeps its text as written. What isn't JSON is refused as `check` refuses it (22P02). A column already written
/// so is left as it is. `sql_names`: whether `rows` are under SQL's names or stored ones, as for `check`.
pub fn as_held(meta: &TableMeta, rows: RecordBatch, sql_names: bool) -> Result<RecordBatch> {
    if !meta.sql_types.values().any(|t| t == "jsonb") {
        return Ok(rows);
    }
    let mut cols = rows.columns().to_vec();
    let mut changed = false;
    for (stored, _) in meta.sql_types.iter().filter(|(_, t)| t.as_str() == "jsonb") {
        let name = if sql_names { meta.name_of(stored) } else { stored.as_str() };
        let Some(i) = rows.schema().index_of(name).ok() else { continue };
        let c = cols[i].clone();
        if c.null_count() == c.len() {
            continue;
        }
        let text = datafusion::arrow::compute::cast(&c, &DataType::Utf8)?;
        let text = text.as_string::<i32>();
        let (mut same, mut refused) = (true, None);
        let written = crate::pg_json_make::per_row(text.len(), |r, out| {
            if text.is_null(r) {
                return Ok(false);
            }
            let v = text.value(r);
            if let Err(e) = crate::pg_json_make::jsonb_into(out, v) {
                refused = Some(jsonb_refused(name, v, e));
                return Err(datafusion::error::DataFusionError::Internal("a refused jsonb value".into())); // (stops the batch: `refused` says why)
            }
            same &= out.as_str() == v;
            Ok(true)
        });
        if let Some(e) = refused {
            return Err(e);
        }
        if same {
            continue;
        }
        cols[i] = datafusion::arrow::compute::cast(&written?, c.data_type())?;
        changed = true;
    }
    match changed {
        true => Ok(RecordBatch::try_new(rows.schema(), cols)?),
        false => Ok(rows),
    }
}

/// The refusal of a jsonb value `jsonb_into` couldn't write: `refusal`'s message and SQLSTATE, as `check` words them. Where
/// `refusal` takes no value (it is JSON, but a number past what `numeric` holds is only jsonb's), the words are JSON's and
/// the error's own SQLSTATE stays (a lone surrogate's escape is JSON, but not jsonb's).
fn jsonb_refused(column: &str, v: &str, e: datafusion::error::DataFusionError) -> anyhow::Error {
    let code = crate::codes::of(&anyhow::Error::from(e));
    let (message, code) = refusal("jsonb", column, v).unwrap_or_else(|| (json_refusal(v), code));
    anyhow::Error::new(crate::codes::Violation(message, code))
}

/// Why a uuid column refuses a value: not a uuid, or one written another way than the column holds.
fn refused_uuid(column: &str, s: &str) -> String {
    match uuid_of(s) {
        Ok(u) => format!("invalid input syntax for type uuid: \"{s}\" (column {column} holds a uuid one way, lower case with hyphens: write '{u}', or cast it, '{s}'::uuid)"),
        Err(_) => format!("invalid input syntax for type uuid: \"{s}\" (column {column} holds uuids)"),
    }
}

/// Why a value of a column held as `t` is refused, with its message and SQLSTATE; None when it is taken.
/// The column's name is the one the message gives.
fn refusal(t: &str, column: &str, v: &str) -> Option<(String, &'static str)> {
    match t {
        "uuid" if !is_uuid(v) => Some((refused_uuid(column, v), "22P02")),
        // (the reader the functions use decides: 22P02 for text that isn't JSON, 54001 past its levels)
        "json" | "jsonb" => crate::pg_json_make::refused(v).map(|code| match code {
            "54001" => (format!("stack depth limit exceeded (column {column} holds {t}: a document nested past {} levels)", crate::pg_json_make::MAX_DEPTH), code),
            _ => (format!("{} (column {column} holds {t})", json_refusal(v)), code),
        }),
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
            return Err(anyhow::Error::new(crate::codes::Violation(message, "22P02")));
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
            return Err(anyhow::Error::new(crate::codes::Violation(message, code)));
        }
    }
    Ok(())
}

/// Where SQL comes in: a cast to an enum type is a cast to text (a literal one of its labels, or
/// refused as Postgres refuses it), and `enum_range(NULL::t)`, `enum_first`, `enum_last` its
/// labels. A cast to `uuid` is the uuid written the one way a column holds it: a literal at once
/// (`'A0EE…'::uuid` is `'a0ee…'`), anything else by `pondra_uuid(x)` (`TRY_CAST`: `pondra_uuid(x,
/// true)`, NULL for what isn't one). A string compared with a uuid or written into one is read as one, and
/// `pg_typeof(x)` of one is 'uuid' (`pg_trace::rewrite`), before the casts it may read are written out.
/// Whether anything changed.
pub async fn rewrite(lake: &Lake, stmts: &mut [Statement]) -> Result<bool> {
    let mut read = false;
    for s in stmts.iter_mut() {
        // (a bare name of a JSON call's rows is its value unless a relation has a column of that name: the catalog is asked
        // only for a statement with such a name)
        let columns = match crate::pg_json_sets::bare_aliases(s) {
            true => crate::pg_trace::columns_named(lake, s).await,
            false => None,
        };
        read |= crate::pg_json_sets::sets(s, columns.as_ref())?; // (set-returning JSON functions: rows of a document, before the casts they may read)
        read |= crate::pg_trace::rewrite(lake, s).await?; // (strings read as uuids, pg_typeof, JSON makers' arguments)
    }
    for s in stmts.iter_mut() {
        read |= crate::lateral::functions(s)?; // (`generate_series(1, t.n)` in a FROM: a LATERAL subquery it can plan)
    }
    for s in stmts.iter_mut() {
        read |= crate::pg_text::array_literals(s)?; // (a string cast to an array type reads as Postgres reads an array literal)
    }
    let mut f = Casts { types: HashMap::new(), named: vec![], labelled: vec![], typed: false, collecting: true, failed: None };
    for s in stmts.iter_mut() {
        let _ = s.visit(&mut f); // (the first pass only collects the names)
    }
    if f.named.is_empty() && !f.typed {
        return Ok(read);
    }
    for n in std::mem::take(&mut f.named) {
        if let Some(labels) = lake.enum_labels(&n).await {
            f.types.insert(n, labels);
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

/// `CREATE TYPE name AS ENUM (…)`, as `SHOW CREATE TYPE` writes it.
pub fn create_sql(name: &str, t: &Type) -> String { format!("CREATE TYPE {name} AS ENUM ({})", quoted(&t.labels)) }

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{ArrayRef, StringArray, StringViewArray};
    use datafusion::arrow::datatypes::{Field, Schema};
    use std::sync::Arc;

    /// A table with a jsonb column `j` and a json column `k`, as `TableMeta::sql_types` keeps them.
    fn held_meta() -> TableMeta {
        TableMeta {
            columns: vec![("j".into(), "Utf8".into()), ("k".into(), "Utf8".into())],
            sql_types: BTreeMap::from([("j".to_string(), "jsonb".to_string()), ("k".to_string(), "json".to_string())]),
            ..Default::default()
        }
    }

    /// Two text columns, `j` and `k` (None: NULL).
    fn held_batch(j: Vec<Option<&str>>, k: Vec<Option<&str>>) -> RecordBatch {
        let schema = Schema::new(vec![Field::new("j", DataType::Utf8, true), Field::new("k", DataType::Utf8, true)]);
        let cols: Vec<ArrayRef> = vec![Arc::new(StringArray::from(j)) as ArrayRef, Arc::new(StringArray::from(k)) as ArrayRef];
        RecordBatch::try_new(Arc::new(schema), cols).unwrap()
    }

    #[test]
    fn as_held_writes_jsonb_as_postgres_does_and_leaves_json_as_written() {
        let rows = held_batch(vec![Some(r#"{"b":2,"a":1}"#), Some("[1,  2]"), None, Some(r#"{"a": 1}"#)], vec![Some(r#"{"b":2}"#); 4]);
        let k_before = rows.column_by_name("k").unwrap().clone();
        let out = as_held(&held_meta(), rows, true).unwrap();
        let j = out.column_by_name("j").unwrap().as_string::<i32>();
        assert_eq!(j.iter().collect::<Vec<_>>(), vec![Some(r#"{"a": 1, "b": 2}"#), Some("[1, 2]"), None, Some(r#"{"a": 1}"#)]);
        assert!(Arc::ptr_eq(&k_before, out.column_by_name("k").unwrap()), "a json column is not touched");
    }

    #[test]
    fn as_held_returns_a_batch_already_in_jsonb_form_with_the_same_array() {
        let rows = held_batch(vec![Some(r#"{"a": 1, "b": 2}"#), Some("[1, 2]"), None], vec![Some(r#"{"b":2}"#); 3]);
        let before = rows.column_by_name("j").unwrap().clone();
        let out = as_held(&held_meta(), rows, true).unwrap();
        assert!(Arc::ptr_eq(&before, out.column_by_name("j").unwrap()));
    }

    #[test]
    fn as_held_refuses_what_is_not_json_with_checks_words() {
        let (meta, rows) = (held_meta(), held_batch(vec![Some(r#"{"a":"#)], vec![Some(r#"{"b":2}"#)]));
        let e = as_held(&meta, rows.clone(), true).unwrap_err();
        assert_eq!(crate::codes::of(&e), "22P02");
        assert!(e.to_string().contains("(column j holds jsonb)"), "{e}");
        let checked = check(&meta, &rows, true, &|_: usize| false).unwrap_err();
        assert_eq!(e.to_string(), checked.to_string());
    }

    #[test]
    fn as_held_keeps_a_utf8view_column_a_view() {
        let schema = Schema::new(vec![Field::new("j", DataType::Utf8View, true)]);
        let col = StringViewArray::from(vec![Some(r#"{"b":2,"a":1}"#)]);
        let rows = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(col) as ArrayRef]).unwrap(); // (no `k`: a column the batch lacks is skipped)
        let out = as_held(&held_meta(), rows, true).unwrap();
        let j = out.column_by_name("j").unwrap();
        assert_eq!(j.data_type(), &DataType::Utf8View);
        assert_eq!(j.as_string_view().value(0), r#"{"a": 1, "b": 2}"#);
    }

    #[test]
    fn a_json_column_takes_the_levels_the_functions_read() {
        let nested = |n: usize| format!("{}{}", "[".repeat(n), "]".repeat(n));
        assert_eq!(refusal("jsonb", "c", &nested(200)), None);
        assert_eq!(refusal("jsonb", "c", "[1,").map(|(_, code)| code), Some("22P02"));
        let (message, code) = refusal("json", "c", &nested(crate::pg_json_make::MAX_DEPTH + 1)).unwrap();
        assert_eq!(code, "54001");
        assert!(message.contains(&crate::pg_json_make::MAX_DEPTH.to_string()), "{message}");
    }
}
