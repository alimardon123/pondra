//! Sequences and identity columns (round 34). `CREATE SEQUENCE s` keeps a counter in the catalog
//! (`sq/{name}`), and only the leader's sequencer moves it: `nextval('s')` takes a block of values
//! in the sequencer's next commit, durable before any is handed out, and hands them out on its own
//! node. So a busy column costs the leader a commit now and then, not a request a row: a node's
//! blocks double while they run out within a second (a key many rows take is then written about
//! once a second), and halve again when they last a minute. Values from one node come in order;
//! the nodes' interleave; a node that stops loses the rest of its block, a gap, as Postgres's
//! `CACHE` leaves one.
//!
//! An identity column (`GENERATED { ALWAYS | BY DEFAULT } AS IDENTITY`, `serial`, MySQL's
//! `AUTO_INCREMENT`, Snowflake's `IDENTITY(1, 1)` and `AUTOINCREMENT`) is a sequence its column
//! owns, as in Postgres: `DEFAULT nextval('{table}_{column}_seq')` and NOT NULL, made with the
//! table and gone with it (`DROP TABLE … PURGE`; a table kept to be undropped keeps it), and not
//! an object of its own. `ALWAYS` refuses a value given for it, and an UPDATE of it.
use crate::log::{Ack, Outcome};
use crate::store::{json, Lake, TableMeta};
use crate::write::Stmt;
use anyhow::{bail, ensure, Context};
use datafusion::arrow::array::{Array, AsArray, Int64Array};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{exec_err, Result as DfResult};
use datafusion::logical_expr::{async_udf::{AsyncScalarUDF, AsyncScalarUDFImpl}, ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use datafusion::prelude::{create_udf, SessionContext};
use datafusion::sql::sqlparser::ast;
use serde::{Deserialize, Serialize};
use serde_json::{json as j, Value};
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};

type Result<T> = anyhow::Result<T>;

pub fn key(name: &str) -> String { format!("sq/{name}") }

/// One option as written (`INCREMENT BY 2`, `NO MAXVALUE`, `AS integer`); a later one of a kind wins.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum Opt {
    As(String),
    Increment(i64),
    Min(Option<i64>),
    Max(Option<i64>),
    Start(i64),
    Cache(i64),
    Cycle(bool),
}

impl Opt {
    pub fn rank(&self) -> usize { [matches!(self, Opt::As(_)), matches!(self, Opt::Increment(_)), matches!(self, Opt::Min(_)), matches!(self, Opt::Max(_)), matches!(self, Opt::Start(_)), matches!(self, Opt::Cache(_)), matches!(self, Opt::Cycle(_))].iter().position(|m| *m).unwrap_or(0) }
    fn sql(&self) -> String {
        match self {
            Opt::As(t) => format!("AS {t}"),
            Opt::Increment(n) => format!("INCREMENT BY {n}"),
            Opt::Min(Some(n)) => format!("MINVALUE {n}"),
            Opt::Min(None) => "NO MINVALUE".into(),
            Opt::Max(Some(n)) => format!("MAXVALUE {n}"),
            Opt::Max(None) => "NO MAXVALUE".into(),
            Opt::Start(n) => format!("START WITH {n}"),
            Opt::Cache(n) => format!("CACHE {n}"),
            Opt::Cycle(true) => "CYCLE".into(),
            Opt::Cycle(false) => "NO CYCLE".into(),
        }
    }
}

/// The options as SQL, each kind once (the last written), in Postgres's order.
/// The options a `CREATE SEQUENCE` statement declares, or None when it isn't one (an apply compares them).
pub fn declared(sql: &str) -> Option<Vec<Opt>> {
    match statement(sql) {
        Some(Stmt::Ddl(mut d)) => match d.pop() {
            Some(crate::ddl::Ddl::Sequence(Change::Create { declared, .. })) => Some(declared),
            _ => None,
        },
        _ => None,
    }
}

pub fn options_sql(declared: &[Opt]) -> String {
    let last: Vec<&Opt> = (0..7).filter_map(|r| declared.iter().rev().find(|o| o.rank() == r)).collect();
    last.iter().map(|o| o.sql()).collect::<Vec<_>>().join(" ")
}

/// A sequence as the catalog keeps it.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Sequence {
    pub declared: Vec<Opt>,
    pub last: i64,    // Postgres's last_value…
    pub called: bool, // …and is_called: the next value is `last` (false) or the one after it
    pub version: u64, // the commit that made or last changed it: a node's block from before goes
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owned: Option<String>, // `table.column`: an identity column's
}

/// What a sequence's options come to.
pub struct Shape {
    pub start: i64,
    pub increment: i64,
    pub min: i64,
    pub max: i64,
    pub cache: i64,
    pub cycle: bool,
}

/// The options worked out, Postgres's defaults where none is given, and checked.
pub fn shape(declared: &[Opt]) -> Result<Shape> {
    let last = |f: fn(&Opt) -> Option<Option<i64>>| declared.iter().rev().find_map(f);
    let as_type = match declared.iter().rev().find_map(|o| if let Opt::As(t) = o { Some(t.to_lowercase()) } else { None }).as_deref() {
        None | Some("bigint" | "int8" | "long") => "BIGINT",
        Some("integer" | "int" | "int4") => "INTEGER",
        Some("smallint" | "int2" | "short") => "SMALLINT",
        Some(t) => bail!("a sequence counts in smallint, integer or bigint, not {t}"),
    };
    let (lo, hi) = match as_type {
        "SMALLINT" => (i16::MIN as i64, i16::MAX as i64),
        "INTEGER" => (i32::MIN as i64, i32::MAX as i64),
        _ => (i64::MIN, i64::MAX),
    };
    let increment = last(|o| if let Opt::Increment(n) = o { Some(Some(*n)) } else { None }).flatten().unwrap_or(1);
    ensure!(increment != 0, "INCREMENT must not be zero");
    let min = last(|o| if let Opt::Min(n) = o { Some(*n) } else { None }).flatten().unwrap_or(if increment > 0 { 1 } else { lo });
    let max = last(|o| if let Opt::Max(n) = o { Some(*n) } else { None }).flatten().unwrap_or(if increment > 0 { hi } else { -1 });
    ensure!(lo <= min && max <= hi, "MINVALUE ({min}) and MAXVALUE ({max}) must be within {as_type}'s range");
    ensure!(min < max, "MINVALUE ({min}) must be less than MAXVALUE ({max})");
    let start = last(|o| if let Opt::Start(n) = o { Some(Some(*n)) } else { None }).flatten().unwrap_or(if increment > 0 { min } else { max });
    ensure!((min..=max).contains(&start), "START value ({start}) must be between MINVALUE ({min}) and MAXVALUE ({max})");
    let cache = last(|o| if let Opt::Cache(n) = o { Some(Some(*n)) } else { None }).flatten().unwrap_or(1);
    ensure!(cache >= 1, "CACHE ({cache}) must be greater than zero");
    let cycle = declared.iter().rev().find_map(|o| if let Opt::Cycle(c) = o { Some(*c) } else { None }).unwrap_or(false);
    Ok(Shape { start, increment, min, max, cache, cycle })
}

/// `CREATE SEQUENCE` again, as declared.
pub fn create_sql(name: &str, s: &Sequence) -> String {
    let options = options_sql(&s.declared);
    format!("CREATE SEQUENCE {name}{}{options}", if options.is_empty() { "" } else { " " })
}

// ---------------------------------------------------------------- statements

#[derive(Serialize, Deserialize, Clone)]
#[serde(rename_all = "snake_case")]
pub enum Change {
    Create { name: String, declared: Vec<Opt>, replace: bool, if_not_exists: bool },
    Alter { name: String, declared: Vec<Opt>, restart: Option<Option<i64>>, rename: Option<String>, if_exists: bool },
    Drop { names: Vec<String>, if_exists: bool },
}

/// `CREATE [OR REPLACE] SEQUENCE`, `ALTER SEQUENCE`, `DROP SEQUENCE`, or None for anything else.
pub fn statement(sql: &str) -> Option<Stmt> {
    use std::sync::LazyLock;
    static CREATE: LazyLock<regex::Regex> = LazyLock::new(|| regex::Regex::new(r#"(?is)^\s*CREATE\s+(OR\s+REPLACE\s+)?((?:TEMP|TEMPORARY)\s+)?SEQUENCE\s+(IF\s+NOT\s+EXISTS\s+)?([\w."$-]+)(.*?)\s*;?\s*$"#).expect("a regex"));
    static ALTER: LazyLock<regex::Regex> = LazyLock::new(|| regex::Regex::new(r#"(?is)^\s*ALTER\s+SEQUENCE\s+(IF\s+EXISTS\s+)?([\w."$-]+)(.*?)\s*;?\s*$"#).expect("a regex"));
    static DROP: LazyLock<regex::Regex> = LazyLock::new(|| regex::Regex::new(r"(?is)^\s*DROP\s+SEQUENCE\b").expect("a regex"));
    static RENAME: LazyLock<regex::Regex> = LazyLock::new(|| regex::Regex::new(r#"(?is)^\s*RENAME\s+TO\s+([\w."$-]+)\s*$"#).expect("a regex"));
    let first = crate::write::first_word(sql);
    let invalid = |e: anyhow::Error| Stmt::Invalid(format!("{e:#}"));
    if let Some(c) = CREATE.captures(first) {
        if c.get(2).is_some() {
            return Some(Stmt::Invalid("CREATE TEMPORARY SEQUENCE isn't taken yet: a sequence is the lake's (CREATE SEQUENCE)".into()));
        }
        let (replace, if_not_exists) = (c.get(1).is_some(), c.get(3).is_some());
        return Some(match parse_options(&c[5], false) {
            _ if replace && if_not_exists => Stmt::Invalid("CREATE OR REPLACE SEQUENCE … IF NOT EXISTS: one or the other".into()),
            Ok((declared, _)) => Stmt::Ddl(vec![crate::ddl::Ddl::Sequence(Change::Create { name: object_of(&c[4]), declared, replace, if_not_exists })]),
            Err(e) => invalid(e),
        });
    }
    if let Some(c) = ALTER.captures(first) {
        let (if_exists, name, rest) = (c.get(1).is_some(), object_of(&c[2]), c[3].to_string());
        if let Some(r) = RENAME.captures(&rest) {
            return Some(Stmt::Ddl(vec![crate::ddl::Ddl::Sequence(Change::Alter { name, declared: vec![], restart: None, rename: Some(object_of(&r[1])), if_exists })]));
        }
        return Some(match parse_options(&rest, true) {
            Ok((declared, None)) if declared.is_empty() => {
                Stmt::Invalid(format!("ALTER SEQUENCE {name}: what to change (RESTART [WITH n], INCREMENT BY n, MINVALUE n, MAXVALUE n, START WITH n, CACHE n, [NO] CYCLE, AS type, RENAME TO name)"))
            }
            Ok((declared, restart)) => Stmt::Ddl(vec![crate::ddl::Ddl::Sequence(Change::Alter { name, declared, restart, rename: None, if_exists })]),
            Err(e) => invalid(e),
        });
    }
    if DROP.is_match(first) {
        let parsed = datafusion::sql::sqlparser::parser::Parser::parse_sql(&datafusion::sql::sqlparser::dialect::GenericDialect {}, sql).map(|mut s| s.pop());
        return Some(match parsed {
            Ok(Some(ast::Statement::Drop { object_type: ast::ObjectType::Sequence, names, if_exists, .. })) => {
                Stmt::Ddl(vec![crate::ddl::Ddl::Sequence(Change::Drop { names: names.iter().map(crate::write::object).collect(), if_exists })])
            }
            _ => Stmt::Invalid("DROP SEQUENCE [IF EXISTS] name [, …]".into()),
        });
    }
    None
}

/// A name as SQL resolves it (`S` is `s`, `"S"` is `S`), as `nextval('…')` and `ALTER` give it.
pub fn object_of(name: &str) -> String {
    use datafusion::sql::sqlparser::{dialect::GenericDialect, parser::Parser};
    let parsed = Parser::new(&GenericDialect {}).try_with_sql(name).and_then(|mut p| p.parse_object_name(false));
    parsed.map(|n| crate::write::object(&n)).unwrap_or_else(|_| name.to_lowercase())
}

/// A sequence's options as Postgres takes them, in any order (`START 10 INCREMENT 5`, `INCREMENT BY
/// 5 START WITH 10`), checked; with `alter`, `RESTART [[WITH] n]` too.
fn parse_options(text: &str, alter: bool) -> Result<(Vec<Opt>, Option<Option<i64>>)> {
    const USAGE: &str = "[AS smallint | integer | bigint] [INCREMENT [BY] n] [MINVALUE n | NO MINVALUE] [MAXVALUE n | NO MAXVALUE] [START [WITH] n] [CACHE n] [[NO] CYCLE]";
    let words: Vec<String> = text.split_whitespace().map(str::to_uppercase).collect();
    let (mut out, mut restart, mut i) = (vec![], None, 0);
    let number = |i: usize| -> Result<i64> {
        let w = words.get(i).with_context(|| format!("a number after {}", words[i - 1]))?;
        w.parse().with_context(|| format!("{w}: a sequence's options are whole numbers"))
    };
    let after = |i: usize, w: &str| words.get(i).is_some_and(|x| x == w);
    while i < words.len() {
        let skip = |by: &str| if after(i + 1, by) { 2 } else { 1 };
        match words[i].as_str() {
            "AS" => {
                out.push(Opt::As(words.get(i + 1).context("AS smallint | integer | bigint")?.to_lowercase()));
                i += 2;
            }
            "INCREMENT" => {
                let k = skip("BY");
                out.push(Opt::Increment(number(i + k)?));
                i += k + 1;
            }
            "START" => {
                let k = skip("WITH");
                out.push(Opt::Start(number(i + k)?));
                i += k + 1;
            }
            "MINVALUE" | "MAXVALUE" | "CACHE" => {
                let n = number(i + 1)?;
                out.push(match words[i].as_str() { "MINVALUE" => Opt::Min(Some(n)), "MAXVALUE" => Opt::Max(Some(n)), _ => Opt::Cache(n) });
                i += 2;
            }
            "CYCLE" => {
                out.push(Opt::Cycle(true));
                i += 1;
            }
            "NO" => {
                out.push(match words.get(i + 1).map(String::as_str) {
                    Some("MINVALUE") => Opt::Min(None),
                    Some("MAXVALUE") => Opt::Max(None),
                    Some("CYCLE") => Opt::Cycle(false),
                    _ => bail!("NO MINVALUE, NO MAXVALUE or NO CYCLE"),
                });
                i += 2;
            }
            "RESTART" if alter => {
                let k = skip("WITH");
                restart = Some(if words.get(i + k).is_some_and(|w| w.parse::<i64>().is_ok()) { Some(number(i + k)?) } else { None });
                i += if restart == Some(None) { 1 } else { k + 1 };
            }
            "OWNED" => bail!("OWNED BY: an identity column owns its sequence (GENERATED BY DEFAULT AS IDENTITY); a sequence of its own is no column's"),
            w => bail!("{w}: a sequence takes {USAGE}{}", if alter { ", RESTART [WITH n] or RENAME TO name" } else { "" }),
        }
    }
    if !alter {
        shape(&out)?; // (refused as written: a MINVALUE over the MAXVALUE, a START outside them, …)
    }
    Ok((out, restart))
}

/// `… AS IDENTITY (START WITH 100 INCREMENT BY 10)` with its options in the order the parser
/// takes them (Postgres takes any), or None when there's nothing to put in order.
pub fn in_order(sql: &str) -> Option<String> {
    static OPTIONS: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| regex::Regex::new(r"(?is)\bAS\s+IDENTITY\s*\(([^()]*)\)").expect("a regex"));
    if !OPTIONS.is_match(sql) {
        return None;
    }
    let out = OPTIONS.replace_all(sql, |c: &regex::Captures| match parse_options(&c[1], false) {
        Ok((o, _)) => format!("AS IDENTITY ({})", options_sql(&o)),
        Err(_) => c[0].to_string(), // (the parser says what's wrong)
    });
    (out != sql).then(|| out.into_owned())
}

/// The parser's options (an identity column's), checked.
pub fn options(given: &[ast::SequenceOptions]) -> Result<Vec<Opt>> {
    use ast::SequenceOptions as S;
    let n = |e: &ast::Expr| e.to_string().replace(' ', "").parse::<i64>().with_context(|| format!("{e}: a sequence's options are whole numbers"));
    let mut out = vec![];
    for o in given {
        out.push(match o {
            S::IncrementBy(e, _) => Opt::Increment(n(e)?),
            S::MinValue(e) => Opt::Min(e.as_ref().map(n).transpose()?),
            S::MaxValue(e) => Opt::Max(e.as_ref().map(n).transpose()?),
            S::StartWith(e, _) => Opt::Start(n(e)?),
            S::Cache(e) => Opt::Cache(n(e)?),
            S::Cycle(no) => Opt::Cycle(!no),
        });
    }
    shape(&out)?;
    Ok(out)
}

/// Leader: carry one out (under the lake's lock); the counters move in the sequencer (`Held`).
pub async fn apply(lake: &Lake, c: Change) -> Result<Value> {
    match c {
        Change::Create { name, declared, replace, if_not_exists } => {
            let name = crate::ddl::new_name(lake, &name).await?;
            if let Some(s) = lake.cat.get::<Sequence>(&key(&name)).await? {
                if if_not_exists {
                    return Ok(j!({"sequence": name, "exists": true}));
                }
                ensure!(replace, "relation \"{name}\" already exists (CREATE OR REPLACE SEQUENCE, or IF NOT EXISTS)");
                ensure!(s.owned.is_none(), "sequence {name} numbers {}: it goes with its table", s.owned.unwrap_or_default());
            }
            ensure!(!relation(lake, &name).await?, "relation \"{name}\" already exists: a table or view has the name");
            submit(lake, Op::Make { name: name.clone(), declared, owned: None }).await?;
            Ok(j!({"sequence": name}))
        }
        Change::Alter { name, declared, restart, rename, if_exists } => {
            let Some((name, s)) = find(lake, &name).await? else {
                ensure!(if_exists, "relation \"{name}\" does not exist");
                return Ok(j!({"sequence": name, "exists": false}));
            };
            if let Some(to) = rename {
                ensure!(s.owned.is_none(), "sequence {name} numbers {}: it is named with its table", s.owned.unwrap_or_default());
                if let Some(c) = used_by(lake, &name).await? {
                    bail!("cannot rename sequence {name}: {c} takes its DEFAULT from it by name");
                }
                let to = crate::ddl::new_name(lake, &to).await?;
                ensure!(!relation(lake, &to).await? && lake.cat.get::<Sequence>(&key(&to)).await?.is_none(), "relation \"{to}\" already exists");
                submit(lake, Op::Rename { name: name.clone(), to: to.clone() }).await?;
                return Ok(j!({"sequence": name, "renamed": to}));
            }
            let all = [s.declared.clone(), declared.clone()].concat();
            let shape = shape(&all)?;
            if let Some(Some(n)) = restart {
                ensure!((shape.min..=shape.max).contains(&n), "RESTART value ({n}) must be between MINVALUE ({}) and MAXVALUE ({})", shape.min, shape.max);
            }
            submit(lake, Op::Alter { name: name.clone(), declared, restart }).await?;
            Ok(j!({"sequence": name, "altered": true}))
        }
        Change::Drop { names, if_exists } => {
            let mut dropped = vec![];
            for n in names {
                match find(lake, &n).await? {
                    Some((name, s)) => {
                        ensure!(s.owned.is_none(), "sequence {name} numbers {}: it goes with its table (DROP TABLE … PURGE)", s.owned.unwrap_or_default());
                        if let Some(c) = used_by(lake, &name).await? {
                            bail!("cannot drop sequence {name}: {c} takes its DEFAULT from it");
                        }
                        submit(lake, Op::Drop { name: name.clone() }).await?;
                        dropped.push(name);
                    }
                    None => ensure!(if_exists, "relation \"{n}\" does not exist"),
                }
            }
            Ok(j!({"sequence": dropped.join(", "), "dropped": !dropped.is_empty()}))
        }
    }
}

/// A table, view or index of that name?
async fn relation(lake: &Lake, name: &str) -> Result<bool> {
    Ok(lake.cat.get::<Value>(&crate::store::table_key(name)).await?.is_some() || lake.cat.get::<Value>(&crate::ddl::query_key(name)).await?.is_some() || lake.cat.get::<Value>(&crate::index::key(name)).await?.is_some())
}

/// The sequence a name resolves to in this lake.
async fn find(lake: &Lake, name: &str) -> Result<Option<(String, Sequence)>> {
    let (other, local) = crate::ddl::resolve(lake, name).await?;
    ensure!(other.is_none(), "{name} is an attached lake's sequence: use it from a node of that lake");
    Ok(lake.cat.get::<Sequence>(&key(&local)).await?.map(|s| (local, s)))
}

/// An identity column's sequence, made with its table (leader): `{table}_{column}_seq`, or the
/// first `…_seq1`, `…_seq2` no relation or sequence has. Its name, for the column's default.
pub async fn owned(lake: &Lake, table: &str, column: &str, declared: Vec<Opt>) -> Result<String> {
    let (schema, t) = crate::ddl::split(table);
    let base = crate::ddl::join(schema, &format!("{t}_{column}_seq"));
    for i in 0.. {
        let name = if i == 0 { base.clone() } else { format!("{base}{i}") };
        if !relation(lake, &name).await? && lake.cat.get::<Sequence>(&key(&name)).await?.is_none() {
            submit(lake, Op::Make { name: name.clone(), declared, owned: Some(format!("{table}.{column}")) }).await?;
            return Ok(name);
        }
    }
    unreachable!("a free name")
}

/// The sequences a table's identity columns own, dropped with it (leader).
pub async fn drop_owned(lake: &Lake, meta: &TableMeta) -> Result<()> {
    for i in meta.identity.values() {
        if lake.cat.get::<Sequence>(&key(&i.sequence)).await?.is_some_and(|s| s.owned.is_some()) {
            submit(lake, Op::Drop { name: i.sequence.clone() }).await?;
        }
    }
    Ok(())
}

/// An identity column, as its table keeps it (`TableMeta::identity`, by stored name).
#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct Identity {
    pub always: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub declared: Vec<Opt>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub sequence: String,
}

impl Identity {
    /// How `CREATE TABLE` says it.
    pub fn sql(&self) -> String {
        let options = options_sql(&self.declared);
        format!("GENERATED {} AS IDENTITY{}", if self.always { "ALWAYS" } else { "BY DEFAULT" }, if options.is_empty() { String::new() } else { format!(" ({options})") })
    }
}

/// A column of `CREATE TABLE` that is an identity: ALWAYS or not, and its options. A computed
/// column (`GENERATED ALWAYS AS (expr)`) isn't taken yet, and says so.
pub fn identity_of(c: &ast::ColumnDef) -> Result<Option<Identity>> {
    let mut out = None;
    for o in &c.options {
        out = match &o.option {
            ast::ColumnOption::Generated { generation_expr: Some(e), .. } => bail!("{} … GENERATED ALWAYS AS ({e}): computed columns aren't taken yet (a view can compute it)", c.name),
            ast::ColumnOption::Generated { generated_as, sequence_options, .. } => {
                let always = matches!(generated_as, ast::GeneratedAs::Always);
                Some(Identity { always, declared: options(sequence_options.as_deref().unwrap_or_default())?, ..Default::default() })
            }
            ast::ColumnOption::Identity(ast::IdentityPropertyKind::Identity(p) | ast::IdentityPropertyKind::Autoincrement(p)) => {
                let declared = match &p.parameters {
                    Some(ast::IdentityPropertyFormatKind::FunctionCall(x) | ast::IdentityPropertyFormatKind::StartAndIncrement(x)) => options(&[ast::SequenceOptions::StartWith(x.seed.clone(), true), ast::SequenceOptions::IncrementBy(x.increment.clone(), true)])?,
                    None => vec![],
                };
                Some(Identity { declared, ..Default::default() })
            }
            ast::ColumnOption::DialectSpecific(t) if matches!(&t[..], [w] if ["AUTO_INCREMENT", "AUTOINCREMENT"].contains(&w.to_string().to_uppercase().as_str())) => Some(Identity::default()),
            _ => out,
        };
    }
    if let ast::DataType::Custom(n, a) = &c.data_type {
        if a.is_empty() && ["serial", "bigserial", "smallserial", "serial4", "serial8", "serial2"].contains(&n.to_string().to_lowercase().as_str()) {
            out = out.or(Some(Identity::default()));
        }
    }
    Ok(out)
}

/// The types `serial` stands for.
pub fn serial_type(t: &ast::DataType) -> Option<&'static str> {
    let ast::DataType::Custom(n, a) = t else { return None };
    match (a.is_empty(), n.to_string().to_lowercase().as_str()) {
        (true, "serial" | "serial4") => Some("INTEGER"),
        (true, "bigserial" | "serial8") => Some("BIGINT"),
        (true, "smallserial" | "serial2") => Some("SMALLINT"),
        _ => None,
    }
}

/// The columns (as SQL names them) of a table's ALWAYS identities. `meta` as stored or `logical()`.
fn always(meta: &TableMeta) -> impl Iterator<Item = &str> {
    meta.live().filter(|(s, _, _)| meta.identity.get(*s).is_some_and(|i| i.always)).map(|(_, n, _)| n)
}

/// `INSERT` giving `columns` (as SQL names them): refused if one is an ALWAYS identity (428C9).
pub fn check_given(meta: &TableMeta, table: &str, columns: &[String]) -> Result<()> {
    match always(meta).find(|n| columns.iter().any(|c| c == n)) {
        Some(name) => bail!("cannot insert a non-DEFAULT value into column \"{name}\" of {table}: it is an identity column defined as GENERATED ALWAYS (leave it out, or say DEFAULT)"),
        None => Ok(()),
    }
}

/// `UPDATE` setting `columns`: refused if one is an ALWAYS identity (428C9).
pub fn check_set(meta: &TableMeta, columns: &[String]) -> Result<()> {
    match always(meta).find(|n| columns.iter().any(|c| c == n)) {
        Some(name) => bail!("column \"{name}\" can only be updated to DEFAULT: it is an identity column defined as GENERATED ALWAYS"),
        None => Ok(()),
    }
}

/// Does `sql` call `nextval` or `setval`? (A materialized view can't: its fill and every write
/// would number its rows again.)
pub fn calls(sql: &str) -> bool {
    static WORDS: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| regex::Regex::new(r"(?i)\b(nextval|setval)\s*\(").expect("a regex"));
    WORDS.is_match(sql)
}

/// A positional `INSERT INTO t query` gives every column: refused when one is an ALWAYS identity.
pub async fn check_every(lake: &Lake, table: &str) -> Result<()> {
    // (a name not in a lake here, say an outside catalog's table: the INSERT's own path takes it)
    let Ok((other, name)) = crate::ddl::resolve(lake, table).await else { return Ok(()) };
    let Some(meta) = other.as_deref().unwrap_or(lake).cat.get::<TableMeta>(&crate::store::table_key(&name)).await? else { return Ok(()) };
    let first = always(&meta).next().map(str::to_string);
    match first {
        Some(name) => bail!("cannot insert a non-DEFAULT value into column \"{name}\" of {table}: it is an identity column defined as GENERATED ALWAYS (name the columns you give, or say DEFAULT)"),
        None => Ok(()),
    }
}

static NAMED: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| regex::Regex::new(r"(?i)\bnextval\s*\(\s*'((?:[^']|'')*)'\s*\)").expect("a regex"));

/// `nextval('s')` in a default: the names it takes values from.
fn named(expr: &str) -> Vec<String> { NAMED.captures_iter(expr).map(|c| object_of(&c[1].replace("''", "'"))).collect() }

/// `INSERT … VALUES` whose defaults take sequences' values: `defaults` are the defaults its rows
/// take, in order. DataFusion plans a `VALUES` with a call in it as one-row projections run at
/// once, so the values are taken here, in the rows' order, and written in as numbers (`put`).
pub async fn taken(lake: &Lake, defaults: &[String]) -> Result<HashMap<String, std::collections::VecDeque<i64>>> {
    let mut wanted: Vec<(String, usize)> = vec![];
    for name in defaults.iter().flat_map(|d| named(d)) {
        match wanted.iter_mut().find(|(n, _)| *n == name) {
            Some((_, k)) => *k += 1,
            None => wanted.push((name, 1)),
        }
    }
    let mut out = HashMap::new();
    for (name, n) in wanted {
        out.insert(name.clone(), next(lake, &name, n).await?.into());
    }
    Ok(out)
}

/// A default with each `nextval('s')` replaced by the next value taken for it.
pub fn put(expr: &str, taken: &mut HashMap<String, std::collections::VecDeque<i64>>) -> String {
    NAMED.replace_all(expr, |c: &regex::Captures| match taken.get_mut(&object_of(&c[1].replace("''", "'"))).and_then(|q| q.pop_front()) {
        Some(v) => v.to_string(),
        None => c[0].to_string(),
    }).into_owned()
}

/// The sequences a default names are there: a table isn't made over a missing one.
pub async fn named_exist(lake: &Lake, expr: &str) -> Result<()> {
    ensure!(!regex::Regex::new(r"(?i)\bsetval\s*\(").expect("a regex").is_match(expr), "DEFAULT {expr}: a default may take a sequence's next value, not set it");
    for name in named(expr) {
        ensure!(find(lake, &name).await?.is_some(), "relation \"{name}\" does not exist (CREATE SEQUENCE {name} first)");
    }
    Ok(())
}

/// A table's column whose default takes values from the sequence (`table.column`), if one does.
async fn used_by(lake: &Lake, name: &str) -> Result<Option<String>> {
    for (k, m) in lake.cat.scan::<TableMeta>("t/", "t0").await? {
        if let Some((c, _)) = m.defaults.iter().find(|(c, e)| !m.identity.contains_key(*c) && named(e).iter().any(|n| n == name)) {
            return Ok(Some(format!("{}.{}", &k[2..], m.name_of(c))));
        }
    }
    Ok(None)
}

// ---------------------------------------------------------------- the sequencer's side

/// What the sequencer is asked to do to the counters.
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "snake_case")]
pub enum Op {
    Take { name: String, count: u64 },                                      // a block of values
    Make { name: String, declared: Vec<Opt>, owned: Option<String> },        // made anew (one there replaced)
    Alter { name: String, declared: Vec<Opt>, restart: Option<Option<i64>> }, // options added; RESTART [WITH n]
    Set { name: String, value: i64, called: bool },                         // setval
    Rename { name: String, to: String },
    Drop { name: String },
}

/// The sequencer's copy of the counters it has touched (it alone writes `sq/`), and which this
/// commit changed.
#[derive(Default)]
pub struct Held {
    known: HashMap<String, Option<Sequence>>,
    touched: BTreeSet<String>,
}

impl Held {
    async fn get(&mut self, lake: &Lake, name: &str) -> Result<&mut Option<Sequence>> {
        if !self.known.contains_key(name) {
            let s = lake.cat.get::<Sequence>(&key(name)).await?;
            self.known.insert(name.to_string(), s);
        }
        Ok(self.known.get_mut(name).expect("loaded"))
    }

    /// Carry out one op as commit `version`: its answer (a block's first value, how many and its
    /// step: `block`), or why not.
    pub async fn apply(&mut self, lake: &Lake, op: Op, version: u64) -> Result<Outcome> {
        let refused = |e: String| Ok(Outcome::Refused(e));
        let name = match &op {
            Op::Take { name, .. } | Op::Make { name, .. } | Op::Alter { name, .. } | Op::Set { name, .. } | Op::Rename { name, .. } | Op::Drop { name } => name.clone(),
        };
        let slot = self.get(lake, &name).await?;
        let (first, n, step) = match (op, slot.as_mut()) {
            (Op::Make { declared, owned, .. }, _) => {
                let start = match shape(&declared) {
                    Ok(s) => s.start,
                    Err(e) => return refused(format!("{e:#}")),
                };
                *slot = Some(Sequence { declared, last: start, called: false, version, owned });
                (0, 0, 0)
            }
            (_, None) => return refused(format!("relation \"{name}\" does not exist")),
            (Op::Take { count, .. }, Some(s)) => match take(s, &name, count) {
                Ok(b) => b,
                Err(e) => return refused(e),
            },
            (Op::Alter { declared, restart, .. }, Some(s)) => {
                let all = [s.declared.clone(), declared].concat();
                match shape(&all) {
                    Ok(shape) => {
                        if let Some(at) = restart {
                            (s.last, s.called) = (at.unwrap_or(shape.start), false);
                        }
                        (s.declared, s.version) = (all, version);
                    }
                    Err(e) => return refused(format!("{e:#}")),
                }
                (0, 0, 0)
            }
            (Op::Set { value, called, .. }, Some(s)) => {
                let Ok(shape) = shape(&s.declared) else { return refused(format!("sequence {name}'s options don't hold")) };
                if !(shape.min..=shape.max).contains(&value) {
                    return refused(format!("setval: value {value} is out of bounds for sequence \"{name}\" ({}..{})", shape.min, shape.max));
                }
                (s.last, s.called, s.version) = (value, called, version);
                (0, 0, 0)
            }
            (Op::Rename { to, .. }, Some(_)) => {
                let mut s = slot.take().expect("there");
                s.version = version;
                self.touched.insert(name);
                *self.get(lake, &to).await? = Some(s);
                self.touched.insert(to);
                return Ok(Outcome::Acks(vec![Ack::default()]));
            }
            (Op::Drop { .. }, Some(_)) => {
                *slot = None;
                (0, 0, 0)
            }
        };
        self.touched.insert(name);
        Ok(Outcome::Acks(vec![Ack { block: first as u64, row: n, ms: step as u64, seg: version, ..Default::default() }]))
    }

    /// What this commit writes: the sequences it changed, and those it dropped.
    pub fn writes(&mut self) -> (Vec<(String, Vec<u8>)>, Vec<String>) {
        let (mut puts, mut deletes) = (vec![], vec![]);
        for name in std::mem::take(&mut self.touched) {
            match self.known.get(&name).cloned().flatten() {
                Some(s) => puts.push((key(&name), json(&s))),
                None => deletes.push(key(&name)),
            }
        }
        (puts, deletes)
    }
}

/// Up to `count` values of `s`, from the next on: (the first, how many, the step). Past its
/// bounds it starts again at the other end if it cycles; otherwise it is used up.
fn take(s: &mut Sequence, name: &str, count: u64) -> std::result::Result<(i64, u64, i64), String> {
    let shape = shape(&s.declared).map_err(|e| format!("{e:#}"))?;
    let (inc, min, max) = (shape.increment, shape.min, shape.max);
    let next = if s.called { s.last.checked_add(inc) } else { Some(s.last) };
    let first = match next.filter(|v| (min..=max).contains(v)) {
        Some(v) => v,
        None if shape.cycle => if inc > 0 { min } else { max },
        None if inc > 0 => return Err(format!("nextval: reached maximum value of sequence \"{name}\" ({max})")),
        None => return Err(format!("nextval: reached minimum value of sequence \"{name}\" ({min})")),
    };
    let room = if inc > 0 { (max as i128 - first as i128) / inc as i128 } else { (first as i128 - min as i128) / -(inc as i128) } + 1;
    let n = (count.max(1) as i128).min(room);
    s.last = (first as i128 + (n - 1) * inc as i128) as i64; // (within the bounds: no overflow)
    s.called = true;
    Ok((first, n as u64, inc))
}

/// Ask the sequencer (this lake's leader's): its ack.
async fn submit(lake: &Lake, op: Op) -> Result<Ack> {
    let to = match lake.to.get() {
        Some(to) => to.clone(),
        None => {
            // (`pondra sql` without a node: the leader that is there, if any)
            let t = crate::cluster::latest(&lake.store).await?.filter(|t| !t.addr.is_empty()).context("a sequence's values come from the lake's leader, and none is running: start a node (pondra serve)")?;
            crate::log::To::Leader(t.addr)
        }
    };
    to.sequence(op).await
}

// ---------------------------------------------------------------- each node's blocks

/// A block of values this node hands out: `left` more from `next` on, `step` apart.
#[derive(Default)]
pub struct Block {
    version: u64,
    next: i64,
    left: u64,
    step: i64,
    size: u64,
    at: Option<Instant>,
}

const FIRST: u64 = 32; // a node's first block (CACHE asks for more)
const MOST: u64 = 1 << 20;

/// `n` values of the sequence `name` for this node's rows, in order.
pub async fn next(lake: &Lake, name: &str, n: usize) -> Result<Vec<i64>> {
    let (name, s) = find(lake, name).await?.with_context(|| format!("relation \"{name}\" does not exist (CREATE SEQUENCE {name})"))?;
    may_write(&name)?;
    let base = (shape(&s.declared)?.cache as u64).max(if lake.to.get().is_some() { FIRST } else { 1 });
    let mut blocks = lake.sequences.lock().await; // (one refill at a time: the others wait for it)
    let b = blocks.entry(name.clone()).or_default();
    if b.version < s.version {
        *b = Block::default(); // (made again, changed or set since)
    }
    let mut out = Vec::with_capacity(n);
    while out.len() < n {
        if b.left == 0 {
            let size = match b.at.map(|t| t.elapsed()) {
                None => base,
                Some(t) if t < Duration::from_secs(1) => (b.size * 2).min(MOST),
                Some(t) if t > Duration::from_secs(60) => (b.size / 2).max(base),
                Some(_) => b.size,
            };
            let a = submit(lake, Op::Take { name: name.clone(), count: size.max((n - out.len()) as u64) }).await?;
            *b = Block { version: a.seg, next: a.block as i64, left: a.row, step: a.ms as i64, size, at: Some(Instant::now()) };
        }
        let k = ((n - out.len()) as u64).min(b.left);
        out.extend((0..k as i64).map(|i| b.next + i * b.step));
        b.left -= k;
        b.next = b.next.wrapping_add(k as i64 * b.step); // (past the block's end only when it's used up)
    }
    if let (Some(session), Some(last)) = (crate::temp::current(), out.last()) {
        let _ = crate::temp::with(&session, false, |x| Ok(x.currval.insert(name, *last)));
    }
    Ok(out)
}

/// `setval('s', value [, is_called])`: the sequence's next value is the one after `value` (or
/// `value` itself when not called). Every node's block from before goes.
async fn set(lake: &Lake, name: &str, value: i64, called: bool) -> Result<i64> {
    let (name, _) = find(lake, name).await?.with_context(|| format!("relation \"{name}\" does not exist"))?;
    may_write(&name)?;
    submit(lake, Op::Set { name: name.clone(), value, called }).await?;
    lake.sequences.lock().await.remove(&name);
    Ok(value)
}

/// A sequence moves for a writer only: a read token's queries never take its values.
fn may_write(name: &str) -> Result<()> {
    match crate::auth::current() {
        Some(p) if p.access.is_none() && p.role < crate::auth::Role::Write => bail!("permission denied for sequence {name}: nextval and setval are a writer's"),
        _ => Ok(()),
    }
}

/// `nextval`, `currval` and `setval` in a query's session. `currval` is the session's: the last
/// value its `nextval` gave.
pub fn register(ctx: &SessionContext, lake: Arc<Lake>) {
    let session = crate::temp::current();
    ctx.register_udf(AsyncScalarUDF::new(Arc::new(Counter { lake: lake.clone(), setval: false, session: session.clone(), signature: Signature::string(1, Volatility::Volatile) })).into_scalar_udf());
    ctx.register_udf(AsyncScalarUDF::new(Arc::new(Counter { lake, setval: true, session: session.clone(), signature: Signature::variadic_any(Volatility::Volatile) })).into_scalar_udf());
    let current = move |args: &[ColumnarValue]| -> DfResult<ColumnarValue> {
        let Some(session) = crate::temp::current().or(session.clone()) else { return exec_err!("currval is a session's: a Postgres connection's, or a client's (x-pondra-session)") };
        let names = args[0].to_array(1)?;
        let names = datafusion::arrow::compute::cast(&names, &DataType::Utf8)?;
        let names = names.as_string::<i32>();
        let mut out = vec![];
        for i in 0..names.len() {
            let name = names.value(i).to_lowercase();
            let got = crate::temp::with(&session, false, |x| Ok(x.currval.get(&name).copied())).ok().flatten();
            match got {
                Some(v) => out.push(v),
                None => return exec_err!("currval of sequence \"{name}\" is not yet defined in this session"),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(Int64Array::from(out))))
    };
    ctx.register_udf(create_udf("currval", vec![DataType::Utf8], DataType::Int64, Volatility::Volatile, Arc::new(current)));
}

/// `nextval(name)`, or `setval(name, value [, is_called])`.
#[derive(Debug)]
struct Counter {
    lake: Arc<Lake>,
    setval: bool,
    session: Option<String>,
    signature: Signature,
}

impl PartialEq for Counter {
    fn eq(&self, other: &Self) -> bool { Arc::ptr_eq(&self.lake, &other.lake) && self.setval == other.setval }
}
impl Eq for Counter {}
impl std::hash::Hash for Counter {
    fn hash<H: std::hash::Hasher>(&self, h: &mut H) { self.setval.hash(h) }
}

impl ScalarUDFImpl for Counter {
    fn name(&self) -> &str { if self.setval { "setval" } else { "nextval" } }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DfResult<DataType> { Ok(DataType::Int64) }
    fn invoke_with_args(&self, _: ScalarFunctionArgs) -> DfResult<ColumnarValue> { exec_err!("{} is asynchronous", self.name()) }
}

#[async_trait::async_trait]
impl AsyncScalarUDFImpl for Counter {
    async fn invoke_async_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        let rows = args.number_rows;
        let arrays: Vec<_> = args.args.iter().map(|a| a.to_array(rows)).collect::<DfResult<_>>()?;
        let text = |i: usize| datafusion::arrow::compute::cast(&arrays[i], &DataType::Utf8);
        let err = |e: anyhow::Error| datafusion::error::DataFusionError::External(e.into());
        let names = text(0)?;
        let names = names.as_string::<i32>();
        let session = crate::temp::current().or(self.session.clone()); // (currval is the session's)
        if !self.setval {
            // (rows of one name, the usual, take their values in one go)
            let mut out = vec![None; rows]; // (a NULL name, a NULL value)
            let mut by_name: HashMap<&str, Vec<usize>> = HashMap::new();
            for i in (0..rows).filter(|i| names.is_valid(*i)) {
                by_name.entry(names.value(i)).or_default().push(i);
            }
            for (name, at) in by_name {
                let values = crate::temp::SESSION.scope(session.clone(), next(&self.lake, &object_of(name), at.len())).await.map_err(err)?;
                at.iter().zip(values).for_each(|(i, v)| out[*i] = Some(v));
            }
            return Ok(ColumnarValue::Array(Arc::new(Int64Array::from(out))));
        }
        if !(2..=3).contains(&arrays.len()) {
            return exec_err!("setval(sequence, value [, is_called])");
        }
        let values = datafusion::arrow::compute::cast(&arrays[1], &DataType::Int64)?;
        let called = arrays.get(2).map(|c| datafusion::arrow::compute::cast(c, &DataType::Boolean)).transpose()?;
        let mut out = vec![];
        for i in 0..rows {
            let called = called.as_ref().is_none_or(|c| c.as_boolean().value(i));
            out.push(crate::temp::SESSION.scope(session.clone(), set(&self.lake, &object_of(names.value(i)), values.as_primitive::<datafusion::arrow::datatypes::Int64Type>().value(i), called)).await.map_err(err)?);
        }
        Ok(ColumnarValue::Array(Arc::new(Int64Array::from(out))))
    }
}
