//! A `CREATE TABLE`'s rules, read in the core, so a page makes a table's rules with the node's code. The parts of a statement
//! that read only its AST move here from `constraints`, `seq`, `types` and `write`: the constraints, the sequence options an
//! identity column takes, an inline ENUM's labels, the types a column's text holds, and `declared`, what the statement declares
//! (the key, identity, NOT NULL, DEFAULTs, CHECKs, WITH options, MERGE and constraints). `Table::of_create` makes the table
//! those declare over the columns a form planned. Nothing here reads a catalog, a lake or a session, so the same rules run in
//! a browser as on a node.
use crate::text::{ident, object};
use anyhow::{bail, ensure, Context, Result};
use datafusion::sql::sqlparser::ast;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use super::{Column, Table};

// ---------------------------------------------------------------- constraints (`constraints.rs`)

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

/// Is a PRIMARY KEY (a column's or the table's) said NOT ENFORCED: a fact, not the table's key?
pub fn informational(c: &Option<ast::ConstraintCharacteristics>) -> bool { matches!(c, Some(c) if c.enforced == Some(false)) }

/// The UNIQUE, NOT ENFORCED PRIMARY KEY and FOREIGN KEY constraints a `CREATE TABLE` declares
/// (columns by SQL's names), named as Postgres names them when they aren't: `{table}_{columns}_key`,
/// `{table}_pkey`, `{table}_{columns}_fkey`.
pub fn constraints_of(c: &ast::CreateTable, table: &str) -> Result<Vec<Constraint>> {
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

// ---------------------------------------------------------------- identity columns (`seq.rs`)

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
pub fn options_sql(declared: &[Opt]) -> String {
    let last: Vec<&Opt> = (0..7).filter_map(|r| declared.iter().rev().find(|o| o.rank() == r)).collect();
    last.iter().map(|o| o.sql()).collect::<Vec<_>>().join(" ")
}

/// What a sequence's options come to.
pub struct Shape {
    pub start: i64,
    pub increment: i64,
    pub min: i64,
    pub max: i64,
    pub cache: i64,
    pub cycle: bool,
    pub as_type: &'static str, // its type: BIGINT, INTEGER or SMALLINT (the catalog's `seqtypid`)
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
    Ok(Shape { start, increment, min, max, cache, cycle, as_type })
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
            ast::ColumnOption::Generated { generation_expr: Some(e), .. } => return Err(crate::codes::coded("0A000", format!("{} … GENERATED ALWAYS AS ({e}): computed columns aren't taken yet (a view can compute it)", c.name))),
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

/// The base name of an identity column's sequence, `{table}_{column}_seq` in the table's schema: `seq::owned` makes the first
/// of it, or of `…_seq1`, `…_seq2`, that no relation has, and the column's DEFAULT names it.
pub fn identity_sequence(table: &str, column: &str) -> String {
    let (schema, t) = crate::text::split(table);
    crate::text::join(schema, &format!("{t}_{column}_seq"))
}

/// The DEFAULT of an identity column that counts in `sequence`: `nextval('…')`, the name's quotes doubled.
pub fn identity_default(sequence: &str) -> String { format!("nextval('{}')", sequence.replace('\'', "''")) }

// ---------------------------------------------------------------- types (`types.rs`)

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

/// The labels of an inline ENUM type (`ENUM('a', 'b')` written as a column's type), checked; None for any other type.
pub fn enum_labels(t: &ast::DataType) -> Result<Option<Vec<String>>> {
    match t {
        ast::DataType::Enum(members, _) => {
            let labels: Vec<String> = members.iter().map(|m| match m {
                ast::EnumMember::Name(n) | ast::EnumMember::NamedValue(n, _) => n.clone(),
            }).collect();
            labelled(&labels)?;
            Ok(Some(labels))
        }
        _ => Ok(None),
    }
}

/// Labels an enum may have: some, each once, none empty or too long (Postgres's 63 bytes).
pub(crate) fn labelled(labels: &[String]) -> Result<()> {
    ensure!(!labels.is_empty(), "an enum has at least one label: ENUM ('a', 'b')");
    for (i, l) in labels.iter().enumerate() {
        ensure!(!l.is_empty() && l.len() <= 63, "invalid enum label \"{l}\": labels are 1 to 63 bytes long");
        ensure!(!labels[..i].contains(l), "enum label \"{l}\" used more than once");
    }
    Ok(())
}

// ---------------------------------------------------------------- statements (`write.rs`)

/// A table's definition refused: invalid_table_definition (42P16) unless the error says its own
/// code, never an internal error (XX000, which HTTP answers 500).
pub(crate) fn definition(e: anyhow::Error) -> anyhow::Error {
    match crate::codes::of(&e) {
        "XX000" => crate::codes::coded("42P16", format!("{e:#}")),
        _ => e,
    }
}

/// An option's value as text: a quoted string as it says (`'op = ''D'''` is `op = 'D'`), anything
/// else as written.
pub(crate) fn option_text(v: &ast::Expr) -> String {
    match v {
        ast::Expr::Value(ast::ValueWithSpan { value: ast::Value::SingleQuotedString(s) | ast::Value::DoubleQuotedString(s), .. }) => s.clone(),
        // (a list: `publish = (delta, iceberg)`, `cluster_by = (a, b)`, as a string's commas)
        ast::Expr::Tuple(t) => t.iter().map(option_text).collect::<Vec<_>>().join(", "),
        ast::Expr::Nested(e) => option_text(e),
        other => other.to_string().trim_matches('\'').to_string(),
    }
}

/// The WITH options a table takes.
const OPTIONS: [&str; 7] = ["publish", "cluster_by", "partition_by", "ttl", "order_by", "merge", "retention"];

/// What a `CREATE TABLE` declares, read from its AST alone. `write::create_spec` keeps it in a table's spec. It is computed
/// in the order a node refuses a statement's faults: the key, the identity columns, the NOT NULLs, the DEFAULTs, the CHECKs,
/// the WITH options (an unknown one, or MySQL's `ENGINE=`, refused), MERGE, and the constraints.
#[derive(Clone, Debug, PartialEq)]
pub struct Declared {
    /// The enforced PRIMARY KEY's columns, the table's then the columns' (as written).
    pub key: Vec<String>,
    /// Each identity column (as written), with its options and whether it is ALWAYS.
    pub identity: BTreeMap<String, Identity>,
    /// The columns said NOT NULL (as written).
    pub not_null: Vec<String>,
    /// Each column's DEFAULT, as written.
    pub defaults: BTreeMap<String, String>,
    /// Each CHECK: its name (Postgres's when none is written) and its condition, as written.
    pub checks: Vec<(String, String)>,
    /// The WITH options, the names lowercased and the values as text.
    pub options: BTreeMap<String, String>,
    /// The MERGE functions by column (`merge = 'total:sum'`).
    pub merge: BTreeMap<String, String>,
    /// The UNIQUE, and the PRIMARY KEY and FOREIGN KEY said NOT ENFORCED, constraints (`constraints_of`).
    pub constraints: Vec<Constraint>,
}

/// What `c` declares (see `Declared`). Its refusals are the node's, with the same codes and words.
pub fn declared(c: &ast::CreateTable) -> Result<Declared> {
    let name = |e: &ast::Expr| e.to_string().trim_matches('"').to_string();
    // (a PRIMARY KEY said NOT ENFORCED is a fact, not the key: `informational`)
    let mut key: Vec<String> = c.constraints.iter().flat_map(|k| match k {
        ast::TableConstraint::PrimaryKey(pk) if !informational(&pk.characteristics) => pk.columns.iter().map(|i| name(&i.column.expr)).collect(),
        _ => vec![],
    }).collect();
    key.extend(c.columns.iter().filter(|c| c.options.iter().any(|o| matches!(&o.option, ast::ColumnOption::PrimaryKey(pk) if !informational(&pk.characteristics)))).map(|c| c.name.value.clone()));
    let identity: BTreeMap<String, Identity> = c.columns.iter().filter_map(|c| identity_of(c).transpose().map(|i| i.map(|i| (c.name.value.clone(), i)))).collect::<Result<_>>().map_err(definition)?;
    let not_null: Vec<String> = c.columns.iter().filter(|c| c.options.iter().any(|o| matches!(o.option, ast::ColumnOption::NotNull))).map(|c| c.name.value.clone()).collect();
    let defaults: BTreeMap<String, String> = c.columns.iter().filter_map(|c| c.options.iter().find_map(|o| match &o.option {
        ast::ColumnOption::Default(e) => Some((c.name.value.clone(), e.to_string())),
        _ => None,
    })).collect();
    // CHECKs, named as Postgres names them when they aren't: `{table}_{column}_check`, `{table}_check`.
    let table = object(&c.name).rsplit('.').next().unwrap_or_default().to_string();
    let mut checks: Vec<(String, String)> = vec![];
    let mut check = |name: Option<&ast::Ident>, base: String, expr: &ast::Expr| {
        let name = name.map(ident).unwrap_or_else(|| (1..).map(|i| if i == 1 { base.clone() } else { format!("{base}{}", i - 1) }).find(|n| !checks.iter().any(|(c, _)| c == n)).unwrap_or(base));
        checks.push((name, expr.to_string()));
    };
    for col in &c.columns {
        for o in &col.options {
            if let ast::ColumnOption::Check(k) = &o.option {
                check(k.name.as_ref().or(o.name.as_ref()), format!("{table}_{}_check", col.name.value), &k.expr);
            }
        }
    }
    for k in &c.constraints {
        if let ast::TableConstraint::Check(k) = k {
            check(k.name.as_ref(), format!("{table}_check"), &k.expr);
        }
    }
    let mut opts: BTreeMap<String, String> = BTreeMap::new();
    if let ast::CreateTableOptions::With(o) | ast::CreateTableOptions::Options(o) = &c.table_options {
        for o in o {
            if let ast::SqlOption::KeyValue { key, value } = o {
                opts.insert(key.value.to_lowercase(), option_text(value));
            }
        }
    }
    // (`PARTITION BY`, `CLUSTER BY`, `SEQUENCE BY`, `TTL` and `MERGE sum` came in as these options: `layout.rs`)
    if let o @ (ast::CreateTableOptions::Plain(_) | ast::CreateTableOptions::TableProperties(_)) = &c.table_options {
        // (MySQL's `ENGINE=…` and the like: a feature of another engine, refused by name with its SQLSTATE)
        return Err(crate::codes::coded("0A000", format!("CREATE TABLE {} … {o}: a table's options go in WITH (name = value, …)", c.name)));
    }
    if let Some(k) = opts.keys().find(|k| !OPTIONS.contains(&k.as_str())) {
        return Err(crate::codes::coded("22023", format!("CREATE TABLE … WITH ({k} = …): tables take {}", OPTIONS.join(", ")))); // (invalid_parameter_value, as Postgres refuses an option it doesn't know)
    }
    let mut d = Declared { key, identity, not_null, defaults, checks, options: opts, merge: BTreeMap::new(), constraints: vec![] };
    d.merge = d.list("merge").unwrap_or_default().iter().filter_map(|m| m.split_once(':')).map(|(c, f)| (c.into(), f.into())).collect();
    d.constraints = constraints_of(c, &table).map_err(definition)?;
    Ok(d)
}

impl Declared {
    /// A WITH option as a list: its comma-separated items, trimmed, the empty ones left out.
    pub fn list(&self, k: &str) -> Option<Vec<String>> { self.options.get(k).map(|v| v.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect()) }

    /// Is the table keyed: its key is checked by a plain INSERT, and no MERGE combines its rows (`constraints::key_checked`).
    pub fn keyed(&self) -> bool { !self.key.is_empty() && self.merge.is_empty() }
}

/// Must a write give `column` a value: it is NOT NULL, or a key's, or an identity column. `create_table` and `of_create` both
/// read it so, by the column's name as the table's columns have it.
pub fn required(column: &str, not_null: &[String], key: &[String], identity: &BTreeMap<String, Identity>) -> bool {
    not_null.iter().any(|c| c == column) || key.iter().any(|c| c == column) || identity.contains_key(column)
}

/// The refusal a constraint meets when it names a column the table doesn't have (42703). `columns` are the planned columns
/// (name, type), the delete marker among them, as `create_table` has them.
pub fn constraint_columns(columns: &[(String, String)], constraints: &[Constraint]) -> Result<()> {
    match constraints.iter().flat_map(|k| &k.columns).find(|c| !columns.iter().any(|(n, _)| n == *c)) {
        Some(c) => Err(crate::codes::coded("42703", format!("column \"{c}\" named in a constraint does not exist"))),
        None => Ok(()),
    }
}

/// The refusal an identity column meets when it isn't smallint, integer or bigint. `columns` are the planned columns (name,
/// type), as `create_table` has them.
pub fn identity_types(columns: &[(String, String)], identity: &BTreeMap<String, Identity>) -> Result<()> {
    if let Some((c, t)) = columns.iter().find(|(c, t)| identity.contains_key(c) && !["Int16", "Int32", "Int64"].contains(&t.as_str())) {
        bail!("{c} is an identity column: it counts in smallint, integer or bigint, not {t}");
    }
    Ok(())
}

/// Is `t` a named type (a CREATE TYPE's, that a node's catalog would hold): a name with no arguments that is no serial and not
/// VARIANT, which a node plans as text (`write::sql_type`).
fn named(t: &ast::DataType) -> bool {
    matches!(t, ast::DataType::Custom(_, args) if args.is_empty()) && serial_type(t).is_none() && !t.to_string().eq_ignore_ascii_case("variant")
}

impl Table {
    /// The rules a CREATE TABLE declares, over `schema`: its columns as the form planned them (a uuid, json, varchar(n)
    /// or enum column as text), without `_deleted`. A node's table has the same rules (`TableMeta::to_rules`).
    #[cfg_attr(feature = "server", allow(dead_code))] // (the node's tests make it; a page calls it)
    pub fn of_create(c: &ast::CreateTable, schema: &datafusion::arrow::datatypes::SchemaRef) -> Result<Table> {
        // The statement's column types first, as the node reads them (`types::columns`, before the spec): an inline ENUM's
        // labels, a held type, and a named type, which this form does not keep.
        let mut written: BTreeMap<String, (Option<String>, Option<Vec<String>>)> = BTreeMap::new();
        for w in &c.columns {
            let labels = enum_labels(&w.data_type)?;
            let held = sql_type(&w.data_type)?;
            if labels.is_none() && held.is_none() && named(&w.data_type) {
                return Err(crate::codes::coded("0A000", format!("{} {}: a named type (CREATE TYPE) isn't kept on this form yet", ident(&w.name), w.data_type)));
            }
            written.insert(ident(&w.name), (held, labels));
        }
        let d = declared(c)?;
        // The columns as the node's table holds them: each type as `create_spec` and `create_table` write it, the delete marker last.
        let mut fields: Vec<(String, String)> = schema
            .fields()
            .iter()
            .map(|f| -> Result<(String, String)> { Ok((f.name().clone(), crate::answer::type_name(&crate::answer::dtype(&crate::answer::type_name(f.data_type()))?))) })
            .collect::<Result<_>>()?;
        if d.keyed() && !fields.iter().any(|(n, _)| n == "_deleted") {
            fields.push(("_deleted".into(), "Boolean".into()));
        }
        let table = object(&c.name); // (the table's name as the node has it: its identity columns' sequences are named from it)
        let columns: Vec<Column> = fields
            .iter()
            .map(|(name, _)| {
                let (held, labels) = written.get(name).cloned().unwrap_or((None, None));
                let identity = d.identity.get(name);
                Column {
                    name: name.clone(),
                    stored: name.clone(),
                    not_null: required(name, &d.not_null, &d.key, &d.identity),
                    default: match identity {
                        Some(_) => Some(identity_default(&identity_sequence(&table, name))),
                        None => d.defaults.get(name).cloned(),
                    },
                    always: identity.is_some_and(|i| i.always),
                    held,
                    labels: labels.map(|l| (None, l)),
                }
            })
            .collect();
        constraint_columns(&fields, &d.constraints).map_err(definition)?; // (create_table's refusals, as its block maps them)
        identity_types(&fields, &d.identity).map_err(definition)?;
        let schema = crate::answer::schema(&fields)?;
        Ok(Table {
            schema,
            columns,
            key: d.key.clone(),
            key_checked: d.keyed(),
            unique: d.constraints.iter().filter(|c| c.enforced && c.kind == Kind::Unique).map(|c| (c.name.clone(), c.columns.clone())).collect(),
            checks: d.checks.clone(),
            mapped: false,
        })
    }
}

// (the node's parser, store and sequencer make the tables these compare with: server only)
#[cfg(all(test, feature = "server"))]
mod tests {
    use super::*;
    use crate::store::{table_key, Lake, TableMeta};
    use crate::write::Stmt;
    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use std::sync::Arc;

    /// The statement's CREATE TABLE, as a node parses it.
    fn create(sql: &str) -> ast::CreateTable {
        match crate::write::parse(sql) {
            Some(Stmt::Create(c)) => *c,
            _ => panic!("not a CREATE TABLE: {sql}"),
        }
    }

    /// `c`'s columns, each of a text type: all a page's refusals need of its form's columns.
    fn text_schema(c: &ast::CreateTable) -> SchemaRef {
        Arc::new(Schema::new(c.columns.iter().map(|w| Field::new(ident(&w.name), DataType::Utf8, true)).collect::<Vec<_>>()))
    }

    /// `c`'s columns as the node plans them (`create_spec`'s): the planned fields a page's form gives `of_create`.
    async fn planned(lake: &Lake, c: &ast::CreateTable) -> Result<SchemaRef> {
        let (columns, _, _) = crate::types::columns(lake, &c.columns).await?;
        let fields = crate::write::declared(&crate::write::columns_sql(&columns)).await?;
        Ok(Arc::new(Schema::new(fields)))
    }

    /// The node's refusal for `c`: its spec's (`create_spec`), else its table's (`create_table`), in the words it has.
    async fn node_refusal(lake: &Lake, c: &ast::CreateTable) -> Option<anyhow::Error> {
        let spec = match crate::write::create_spec(c, lake, false).await {
            Ok(spec) => spec,
            Err(e) => return Some(e),
        };
        crate::write::create_table(lake, &object(&c.name), &spec).await.err()
    }

    /// Statements the node refuses, each with the SQLSTATE and the words its refusal has.
    const REFUSED: [(&str, &str, &str); 12] = [
        ("CREATE TABLE t (a INT, FOREIGN KEY (a) REFERENCES s (x))", "0A000", "a reference isn't checked yet"),
        ("CREATE TABLE t (a INT GENERATED ALWAYS AS (a * 2) STORED)", "0A000", "computed columns aren't taken yet"),
        ("CREATE TABLE t (a INT, UNIQUE (a) DEFERRABLE)", "0A000", "DEFERRABLE: a constraint is checked at each statement"),
        ("CREATE TABLE t (a INT, UNIQUE NULLS NOT DISTINCT (a))", "0A000", "UNIQUE NULLS NOT DISTINCT isn't taken yet"),
        ("CREATE TABLE t (a INT PRIMARY KEY, b INT PRIMARY KEY)", "42P16", "multiple primary keys for table"),
        ("CREATE TABLE t (a INT) WITH (colour = 'red')", "22023", "tables take"),
        ("CREATE TABLE t (a INT) ENGINE=InnoDB", "0A000", "a table's options go in WITH"),
        ("CREATE TABLE t (a VARCHAR(0))", "22023", "a length is at least 1"),
        ("CREATE TABLE t (a UUID[])", "0A000", "an array of uuids isn't kept yet"),
        ("CREATE TABLE t (a INT, UNIQUE (zz))", "42703", "column \"zz\" named in a constraint does not exist"),
        ("CREATE TABLE t (a INT PRIMARY KEY, b INT, FOREIGN KEY (zz) REFERENCES s (x) NOT ENFORCED)", "42703", "named in a constraint does not exist"),
        ("CREATE TABLE t (id TEXT GENERATED ALWAYS AS IDENTITY)", "42P16", "is an identity column: it counts in smallint, integer or bigint, not Utf8"),
    ];

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_statement_the_node_refuses_is_refused_the_same_by_a_page() -> Result<()> {
        let dir = std::env::temp_dir().join(format!("pondra-create-refused-{}", uuid::Uuid::new_v4()));
        let lake = Lake::open(dir.to_str().unwrap(), true, false).await?;
        for (sql, code, words) in REFUSED {
            let c = create(sql);
            let node = node_refusal(&lake, &c).await.unwrap_or_else(|| panic!("the node takes {sql}"));
            // (a statement whose columns the node can't plan is refused by its column types first: a text form stands in)
            let schema = match planned(&lake, &c).await {
                Ok(schema) => schema,
                Err(_) => text_schema(&c),
            };
            let page = Table::of_create(&c, &schema).err().unwrap_or_else(|| panic!("a page takes {sql}"));
            assert_eq!(crate::codes::of(&page), crate::codes::of(&node), "{sql}");
            assert_eq!(format!("{page:#}"), format!("{node:#}"), "{sql}");
            assert_eq!(crate::codes::of(&page), code, "{sql}");
            assert!(crate::codes::words(&page).contains(words), "{sql}: {}", crate::codes::words(&page));
        }
        lake.cat.close().await?;
        let _ = std::fs::remove_dir_all(&dir);
        Ok(())
    }

    #[test]
    fn a_named_type_is_refused_on_a_page_and_serial_and_variant_are_not() -> Result<()> {
        let c = create("CREATE TABLE t (a mood)");
        let e = Table::of_create(&c, &text_schema(&c)).err().expect("a named type");
        assert_eq!(crate::codes::of(&e), "0A000");
        assert_eq!(crate::codes::words(&e), "a mood: a named type (CREATE TYPE) isn't kept on this form yet");
        // (a serial is an integer column the form plans, a VARIANT a text one)
        let c = create("CREATE TABLE t (id SERIAL PRIMARY KEY, a VARIANT)");
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false), Field::new("a", DataType::Utf8, true)]));
        Table::of_create(&c, &schema)?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_page_makes_a_tables_rules_as_the_node_does() -> Result<()> {
        let dir = std::env::temp_dir().join(format!("pondra-create-rules-{}", uuid::Uuid::new_v4()));
        let lake = Lake::open(dir.to_str().unwrap(), true, false).await?;
        let _sequencer = crate::log::Sequencer::start(lake.clone(), None).await?;
        for sql in [
            "CREATE TABLE t (id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, a TEXT NOT NULL DEFAULT 'x', u UUID, v VARCHAR(5), j JSONB, m ENUM('a','b'), n INT CHECK (n > 0), o INT UNIQUE, p INT, CONSTRAINT pq UNIQUE (p, o), CHECK (n < 100), CHECK (p <> 0), r INT REFERENCES s(x) NOT ENFORCED)",
            "CREATE TABLE t2 (a INT, b TEXT)",
            "CREATE TABLE t3 (k INT PRIMARY KEY, v BIGINT) WITH (merge = 'v:sum')",
            "CREATE TABLE t4 (id SERIAL PRIMARY KEY, a INT)",
        ] {
            let c = create(sql);
            let name = object(&c.name);
            let page = Table::of_create(&c, &planned(&lake, &c).await?)?;
            let spec = crate::write::create_spec(&c, &lake, false).await?;
            crate::write::create_table(&lake, &name, &spec).await?;
            let node = lake.cat.get::<TableMeta>(&table_key(&name)).await?.expect("the table is made").to_rules()?;
            assert_eq!(page, node, "{sql}");
        }
        lake.cat.close().await?;
        let _ = std::fs::remove_dir_all(&dir);
        Ok(())
    }
}
