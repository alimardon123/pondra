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
//! Postgres's catalog shows its columns as text (`pg_type` and `pg_enum` don't list it yet). Where SQL comes in, a cast to it is
//! a cast to text (a literal checked against its labels) and `enum_range`, `enum_first` and
//! `enum_last` are its labels (`rewrite`).
use crate::store::{json, table_key, Lake, TableMeta};
use crate::write::Stmt;
use anyhow::{bail, ensure, Context, Result};
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
        let d = DROP.captures(first)?;
        if d.get(4).is_some_and(|w| w.as_str().eq_ignore_ascii_case("cascade")) {
            return invalid("DROP TYPE … CASCADE would drop the columns of that type: drop or change them first".into());
        }
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
                if let Some((k, m)) = users(lake, &n).await?.into_iter().next() {
                    let column = m.enums.iter().find(|(_, e)| e.of.as_deref() == Some(n.as_str())).map_or("", |(c, _)| m.name_of(c));
                    bail!("cannot drop type {n} because column {}.{column} uses it", &k[2..]);
                }
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

/// A table's enum columns by SQL's names (`CREATE TABLE t LIKE s`): none when it has none, or
/// isn't this lake's table.
pub async fn of_table(lake: &Lake, name: &str) -> BTreeMap<String, Enum> {
    let Ok((None, local)) = crate::ddl::resolve(lake, name).await else { return BTreeMap::new() };
    match lake.cat.get::<TableMeta>(&table_key(&local)).await {
        Ok(Some(m)) => m.logical().enums,
        _ => BTreeMap::new(),
    }
}

/// A table's declared columns with every enum's type said as text (what its files hold), and the
/// enums by column.
pub async fn columns(lake: &Lake, cols: &[ast::ColumnDef]) -> Result<(Vec<ast::ColumnDef>, BTreeMap<String, Enum>)> {
    let (mut out, mut enums) = (cols.to_vec(), BTreeMap::new());
    for c in out.iter_mut() {
        if let Some(e) = of(lake, &c.data_type).await? {
            enums.insert(crate::write::ident(&c.name), e);
            c.data_type = ast::DataType::Varchar(None);
        }
    }
    Ok((out, enums))
}

/// Every door's rows (`defaults::check`): a value its column's enum doesn't list is refused, the
/// writer's alone (Postgres's 22P02). `marker`: a keyed table's delete markers carry no values.
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
    Ok(())
}

/// Could `sql` name a type of one's own, in a cast or one of the enum functions? (Only then is
/// it parsed for one: the built-in types' names are left alone.)
pub fn wanted(sql: &str) -> bool {
    static NAMED: LazyLock<regex::Regex> = LazyLock::new(|| regex::Regex::new(r#"(?i)(?:::|\bas)\s*"?([a-z_][a-z0-9_]*)"?\s*(?:[.]\s*"?([a-z_][a-z0-9_]*))?|\benum_(?:range|first|last)\s*\("#).expect("a regex"));
    const BUILT_IN: &[&str] = &["int", "integer", "bigint", "smallint", "tinyint", "int2", "int4", "int8", "hugeint", "ubigint", "uinteger", "usmallint", "utinyint", "float", "float4",
        "float8", "real", "double", "decimal", "numeric", "dec", "text", "varchar", "char", "character", "string", "bpchar", "bool", "boolean", "date", "time", "timestamp", "timestamptz",
        "datetime", "interval", "json", "jsonb", "uuid", "bytea", "blob", "binary", "varbinary", "bytes", "regclass", "regtype", "regproc", "regnamespace", "oid", "name", "variant", "bit"];
    NAMED.captures_iter(sql).any(|c| match (c.get(1), c.get(2)) {
        (None, _) => true, // (enum_range(…))
        (Some(_), Some(_)) => true,
        (Some(n), None) => !BUILT_IN.contains(&n.as_str().to_lowercase().as_str()),
    })
}

/// Where SQL comes in: a cast to an enum type is a cast to text (a literal one of its labels, or
/// refused as Postgres refuses it), and `enum_range(NULL::t)`, `enum_first`, `enum_last` its
/// labels. Whether anything changed.
pub async fn rewrite(lake: &Lake, stmts: &mut [Statement]) -> Result<bool> {
    let mut f = Casts { types: HashMap::new(), named: vec![], labelled: vec![], failed: None };
    for s in stmts.iter_mut() {
        let _ = s.visit(&mut f); // (the first pass only collects the names)
    }
    if f.named.is_empty() {
        return Ok(false);
    }
    for n in std::mem::take(&mut f.named) {
        if let Ok(Some((_, t))) = find(lake, &n).await {
            f.types.insert(n, t);
        }
    }
    if let Some(n) = f.labelled.iter().find(|n| !f.types.contains_key(*n)) {
        return Err(crate::codes::coded("42704", format!("type \"{n}\" does not exist"))); // (not DataFusion's "Invalid function 'enum_range'")
    }
    if f.types.is_empty() {
        return Ok(false);
    }
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
        let collecting = self.types.is_empty();
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

/// `CREATE TYPE name AS ENUM (…)`, as `SHOW CREATE TYPE` writes it.
pub fn create_sql(name: &str, t: &Type) -> String { format!("CREATE TYPE {name} AS ENUM ({})", quoted(&t.labels)) }
