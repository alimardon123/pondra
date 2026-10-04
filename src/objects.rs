//! Every kind of object a lake holds, in one registry (ADR-049), with the same verbs for each.
//! `pondra.objects` lists them all, with their comments and the statements that make them;
//! `SHOW CREATE <kind> <name>` gives those statements; `COMMENT ON <kind> <name> IS '…'` describes
//! one; `CREATE OR ALTER TABLE` makes a table, or brings the one there to its definition (what a
//! project's files run again and again). `GET /kinds` and `pondra.kinds` say what kinds there are.
//!
//! A new kind is an entry in `KINDS`, and its family's lister in `FAMILIES`: nothing else needs
//! editing for it to be listed, described and shown. Comments are kept apart (`cm/{family}/{name}`,
//! `cm/column/{table}/{stored column}`), and follow a rename or go with a drop (`follow`, from
//! `ddl::apply`).
use crate::ddl::{join, split, Ddl, PUBLIC};
use crate::store::{json, table_key, Lake, TableMeta};
use anyhow::{bail, ensure, Context, Result};
use datafusion::sql::sqlparser::{dialect::GenericDialect, keywords::Keyword, parser::Parser, tokenizer::Token};
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use serde_json::{json as j, Value};
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, LazyLock};

/// A kind of object, as SQL names it; `family` is the kinds whose names are one namespace (a table
/// and a view can't share a name), and what it is listed and described by.
pub struct Kind {
    pub name: &'static str,
    pub family: &'static str,
    pub verbs: &'static [&'static str],
}

const RELATION: &[&str] = &["CREATE", "CREATE OR ALTER", "CREATE OR REPLACE", "ALTER", "DROP", "COMMENT ON", "SHOW CREATE"];
pub static KINDS: &[Kind] = &[
    Kind { name: "schema", family: "schema", verbs: &["CREATE", "DROP", "COMMENT ON", "SHOW CREATE"] },
    Kind { name: "table", family: "relation", verbs: &["CREATE", "CREATE OR ALTER", "CREATE OR REPLACE", "ALTER", "DROP", "UNDROP", "COMMENT ON", "SHOW CREATE"] },
    Kind { name: "view", family: "relation", verbs: RELATION },
    Kind { name: "materialized view", family: "relation", verbs: &["CREATE", "CREATE OR REPLACE", "ALTER", "DROP", "COMMENT ON", "SHOW CREATE"] },
    Kind { name: "external table", family: "relation", verbs: &["CREATE", "CREATE OR REPLACE", "DROP", "COMMENT ON"] },
    Kind { name: "function", family: "routine", verbs: &["CREATE", "CREATE OR REPLACE", "DROP", "COMMENT ON", "SHOW CREATE"] },
    Kind { name: "macro", family: "routine", verbs: &["CREATE", "CREATE OR REPLACE", "DROP", "COMMENT ON", "SHOW CREATE"] },
    Kind { name: "table function", family: "routine", verbs: &["CREATE", "CREATE OR REPLACE", "DROP", "COMMENT ON", "SHOW CREATE"] },
    Kind { name: "procedure", family: "routine", verbs: &["CREATE", "CREATE OR REPLACE", "CALL", "DROP", "COMMENT ON", "SHOW CREATE"] },
    Kind { name: "task", family: "task", verbs: &["CREATE", "CREATE OR REPLACE", "ALTER", "EXECUTE", "DROP", "COMMENT ON", "SHOW CREATE"] },
    Kind { name: "secret", family: "secret", verbs: &["CREATE", "CREATE OR REPLACE", "DROP", "COMMENT ON"] },
    Kind { name: "user", family: "user", verbs: &["CREATE", "ALTER", "DROP", "GRANT", "REVOKE", "COMMENT ON"] },
    Kind { name: "role", family: "user", verbs: &["CREATE", "DROP", "GRANT", "REVOKE", "COMMENT ON", "SHOW CREATE"] },
    Kind { name: "database", family: "database", verbs: &["CREATE", "ATTACH", "DETACH", "DROP", "COMMENT ON", "SHOW CREATE"] },
];

fn kind(name: &str) -> Option<&'static Kind> { KINDS.iter().find(|k| k.name == name) }

/// One object: of a lake (this one or an attached one), in a schema if its kind has them.
pub struct Object {
    pub kind: &'static str,
    pub lake: String,
    pub schema: Option<String>,
    pub name: String,
    pub comment: Option<String>,
    pub definition: Option<String>,
}

impl Object {
    fn new(kind: &'static str, lake: &str, name: &str, definition: Option<String>) -> Object {
        let (schema, name) = match self::kind(kind).map(|k| k.family) {
            Some("relation" | "routine" | "task") => split(name),
            _ => ("", name),
        };
        Object { kind, lake: lake.into(), schema: Some(schema.to_string()).filter(|s| !s.is_empty()), name: name.into(), comment: None, definition }
    }
    /// Its name in its lake's catalog: `schema.name`, or `name` in public.
    fn local(&self) -> String { self.schema.as_deref().map_or_else(|| self.name.clone(), |s| join(s, &self.name)) }
    fn family(&self) -> &'static str { kind(self.kind).map_or("", |k| k.family) }
}

type Lister = for<'a> fn(&'a Lake) -> BoxFuture<'a, Result<Vec<Object>>>;

/// Each family's objects, read from the catalog alone (no query run).
static FAMILIES: &[(&str, Lister)] = &[("schema", schemas), ("relation", relations), ("routine", routines), ("task", tasks), ("secret", secrets), ("user", users), ("database", databases)];

/// Every object of this lake and the lakes attached to it, with its comment, as the caller may see
/// them (a user limited by grants: the tables it may read, and no secrets, users or roles).
pub async fn list(lake: &Lake) -> Result<Vec<Object>> {
    let mut all = vec![];
    for (_, lister) in FAMILIES {
        all.extend(lister(lake).await?);
    }
    let mut notes = HashMap::new();
    for (name, l) in lakes(lake) {
        let cm: BTreeMap<String, String> = l.cat.scan::<String>("cm/", "cm0").await?.into_iter().map(|(k, v)| (k[3..].to_string(), v)).collect();
        notes.insert(name, cm);
    }
    for o in &mut all {
        o.comment = notes.get(&o.lake).and_then(|n| n.get(&format!("{}/{}", o.family(), o.local()))).cloned();
    }
    Ok(all)
}

/// This lake and the lakes attached to it, by name.
fn lakes(lake: &Lake) -> Vec<(String, Arc<Lake>)> {
    let attached: Vec<(String, Arc<Lake>)> = lake.attached.read().unwrap().clone();
    std::iter::once((crate::ddl::lake_name(lake), lake.arc())).chain(attached).collect()
}

fn schemas(lake: &Lake) -> BoxFuture<'_, Result<Vec<Object>>> {
    Box::pin(async move {
        let here = crate::ddl::lake_name(lake);
        Ok(crate::ddl::schemas(lake).await?.into_iter().map(|s| {
            let def = (s != PUBLIC).then(|| format!("CREATE SCHEMA {}", ident(&s)));
            Object::new("schema", &here, &s, def)
        }).collect())
    })
}

fn relations(lake: &Lake) -> BoxFuture<'_, Result<Vec<Object>>> {
    Box::pin(async move {
        let mut views = HashMap::new();
        for (name, l) in lakes(lake) {
            for (k, v) in l.cat.scan::<crate::views::View>("v/", "v0").await? {
                views.insert((name.clone(), k[2..].to_string()), v);
            }
        }
        let here = crate::ddl::lake_name(lake);
        let mut all = crate::ddl::listed(lake).await?;
        if let Some(a) = crate::auth::limited() {
            all.retain(|o| a.may("select", &if o.lake == here { join(&o.schema, &o.name) } else { format!("{}.{}", o.lake, join(&o.schema, &o.name)) }));
        }
        Ok(all.into_iter().map(|o| {
            let local = join(&o.schema, &o.name);
            let named = if o.lake == here { name_sql(&local) } else { format!("{}.{}.{}", ident(&o.lake), ident(&o.schema), ident(&o.name)) };
            let def = match o.kind {
                "table" => o.meta.as_ref().map(|m| table_sql(&named, m)),
                "view" => o.sql.as_ref().map(|s| format!("CREATE VIEW {named} AS\n{s}")),
                "materialized view" => views.get(&(o.lake.clone(), local.clone())).or_else(|| views.get(&(o.lake.clone(), crate::once::open(&local)))).map(|v| materialized_sql(&named, v, o.meta.as_ref())),
                _ => None, // (an external table's statement isn't kept as written; a window view's `_final` table is made with it)
            };
            let kind = KINDS.iter().find(|k| k.name == o.kind).map_or("table", |k| k.name);
            Object::new(kind, &o.lake, &local, def)
        }).collect())
    })
}

fn routines(lake: &Lake) -> BoxFuture<'_, Result<Vec<Object>>> {
    Box::pin(async move {
        use crate::routines::Kind as R;
        let here = crate::ddl::lake_name(lake);
        let all = crate::routines::listed(lake).await?;
        let mut names: Vec<&String> = all.keys().collect();
        names.sort();
        Ok(names.into_iter().map(|n| {
            let r = &all[n];
            let kind = match (r.kind, r.what()) {
                (R::Procedure, _) => "procedure",
                (_, "macro") => "macro",
                (R::Table, _) => "table function",
                _ => "function",
            };
            Object::new(kind, &here, n, Some(routine_sql(&name_sql(n), r)))
        }).collect())
    })
}

fn tasks(lake: &Lake) -> BoxFuture<'_, Result<Vec<Object>>> {
    Box::pin(async move {
        let here = crate::ddl::lake_name(lake);
        Ok(lake.cat.scan::<crate::runs::Task>("j/", "j0").await?.into_iter().map(|(k, t)| Object::new("task", &here, &k[2..], Some(task_sql(&name_sql(&k[2..]), &t)))).collect())
    })
}

fn secrets(lake: &Lake) -> BoxFuture<'_, Result<Vec<Object>>> {
    Box::pin(async move {
        if crate::auth::limited().is_some() {
            return Ok(vec![]); // (their names and scopes are an admin's to see)
        }
        let here = crate::ddl::lake_name(lake);
        Ok(crate::ext::list(lake).await?.into_iter().map(|(n, _)| Object::new("secret", &here, &n, None)).collect()) // (values are never shown)
    })
}

fn users(lake: &Lake) -> BoxFuture<'_, Result<Vec<Object>>> {
    Box::pin(async move {
        let here = crate::ddl::lake_name(lake);
        if crate::auth::limited().is_some() {
            return Ok(vec![]); // (an admin's to list: a user sees itself in pondra.users)
        }
        let all = lake.cat.scan::<crate::users::User>("u/", "u0").await?;
        Ok(all.into_iter().filter(|(k, _)| &k[2..] != "public").map(|(k, u)| match u.login {
            true => Object::new("user", &here, &k[2..], None), // (a password or token can't be shown)
            false => Object::new("role", &here, &k[2..], Some(format!("CREATE ROLE {}", ident(&k[2..])))),
        }).collect())
    })
}

fn databases(lake: &Lake) -> BoxFuture<'_, Result<Vec<Object>>> {
    Box::pin(async move {
        let here = crate::ddl::lake_name(lake);
        let mut all: Vec<Object> = lake.cat.scan::<crate::ddl::Attachment>("a/", "a0").await?.into_iter()
            .map(|(k, a)| Object::new("database", &here, &k[2..], Some(format!("ATTACH {} AS {}", literal(&a.dir), ident(&k[2..]))))).collect();
        for (n, a) in crate::ext::attached(lake).await? {
            let options: String = a.options.iter().map(|(k, v)| format!(", {} {}", k.to_uppercase(), literal(v))).collect();
            all.push(Object::new("database", &here, &n, Some(format!("ATTACH {} AS {} (TYPE {}{options})", literal(&a.url), ident(&n), a.kind))));
        }
        Ok(all)
    })
}

// ---------------------------------------------------------------- definitions

/// A name in SQL: each part bare if it can be, quoted if not.
fn name_sql(local: &str) -> String { local.split('.').map(ident).collect::<Vec<_>>().join(".") }

/// One part of a name as SQL reads it back: bare when lower case and not a word SQL reserves.
pub fn ident(n: &str) -> String {
    // (Postgres's reserved words, and the words a table's layout clauses start with)
    const RESERVED: &[&str] = &["all", "analyse", "analyze", "and", "any", "array", "as", "asc", "asymmetric", "both", "case", "cast", "check", "cluster", "collate", "column",
        "constraint", "create", "current_date", "current_role", "current_time", "current_timestamp", "current_user", "default", "deferrable", "desc", "distinct", "do", "else",
        "end", "except", "false", "fetch", "for", "foreign", "from", "grant", "group", "having", "in", "initially", "intersect", "interval", "into", "is", "lateral", "leading", "limit",
        "localtime", "localtimestamp", "merge", "not", "null", "offset", "on", "only", "or", "order", "partition", "partitioned", "placing", "primary", "references", "returning",
        "select", "sequence", "session_user", "some", "symmetric", "table", "then", "to", "trailing", "true", "ttl", "union", "unique", "user", "using", "variadic", "when", "where",
        "window", "with"];
    let bare = n.starts_with(|c: char| c.is_ascii_lowercase() || c == '_') && n.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_') && !RESERVED.contains(&n);
    if bare { n.to_string() } else { format!("\"{}\"", n.replace('"', "\"\"")) }
}

fn list_sql(names: &[String]) -> String { names.iter().map(|n| ident(n)).collect::<Vec<_>>().join(", ") }

/// A string literal.
fn literal(s: &str) -> String { format!("'{}'", s.replace('\'', "''")) }

/// A body between dollar quotes that it doesn't hold itself.
fn dollar(body: &str) -> String {
    let tag = (0..).map(|i| if i == 0 { String::new() } else { format!("body{i}") }).find(|t| !body.contains(&format!("${t}$"))).expect("a free tag");
    format!("${tag}$\n{}\n${tag}$", body.trim_matches('\n'))
}

/// Seconds as SQL's interval text: `2 hours`, `7 days`, `90 seconds`.
pub(crate) fn span(s: u64) -> String {
    let (n, unit) = [(86400, "day"), (3600, "hour"), (60, "minute"), (1, "second")].into_iter().find(|(u, _)| s >= *u && s % u == 0).map_or((s, "second"), |(u, w)| (s / u, w));
    format!("{n} {unit}{}", if n == 1 { "" } else { "s" })
}

/// An Arrow type, as the lake records it, in SQL.
fn sql_type(t: &str) -> String {
    use datafusion::arrow::datatypes::{DataType as D, TimeUnit as U};
    fn of(d: &D) -> String {
        match d {
            D::Boolean => "BOOLEAN".into(),
            D::Int8 => "TINYINT".into(),
            D::Int16 => "SMALLINT".into(),
            D::Int32 => "INT".into(),
            D::Int64 => "BIGINT".into(),
            D::UInt8 => "TINYINT UNSIGNED".into(),
            D::UInt16 => "SMALLINT UNSIGNED".into(),
            D::UInt32 => "INT UNSIGNED".into(),
            D::UInt64 => "BIGINT UNSIGNED".into(),
            D::Float16 | D::Float32 => "REAL".into(),
            D::Float64 => "DOUBLE".into(),
            D::Utf8 | D::LargeUtf8 | D::Utf8View => "VARCHAR".into(),
            D::Binary | D::LargeBinary | D::BinaryView | D::FixedSizeBinary(_) => "BYTEA".into(),
            D::Date32 | D::Date64 => "DATE".into(),
            D::Time32(_) | D::Time64(_) => "TIME".into(),
            D::Timestamp(u, tz) => {
                let p = match u { U::Second => "(0)", U::Millisecond => "(3)", U::Microsecond => "", U::Nanosecond => "(9)" };
                format!("TIMESTAMP{p}{}", if tz.is_some() { " WITH TIME ZONE" } else { "" })
            }
            D::Decimal128(p, s) | D::Decimal256(p, s) => format!("DECIMAL({p}, {s})"),
            D::Interval(_) => "INTERVAL".into(),
            D::List(f) | D::LargeList(f) | D::FixedSizeList(f, _) => format!("{}[]", of(f.data_type())),
            d => d.to_string(),
        }
    }
    crate::query::dtype(t).map_or_else(|_| t.to_string(), |d| of(&d))
}

/// `CREATE TABLE`, its layout as clauses (`layout.rs`): what makes the table again, without rows.
fn table_sql(name: &str, m: &TableMeta) -> String {
    let mut parts: Vec<String> = m.columns.iter().filter(|(c, _)| !m.marker(c)).map(|(c, t)| { // (a keyed table's `_deleted` is its own)
        let merge = m.merge.get(c).map(|f| format!(" MERGE {f}")).unwrap_or_default();
        let null = if m.not_null.contains(c) && !m.key.contains(c) { " NOT NULL" } else { "" };
        let default = m.defaults.get(c).map(|d| format!(" DEFAULT {d}")).unwrap_or_default();
        format!("{} {}{merge}{null}{default}", ident(c), sql_type(t))
    }).collect();
    if !m.key.is_empty() {
        parts.push(format!("PRIMARY KEY ({})", list_sql(&m.key)));
    }
    parts.extend(m.checks.iter().map(|(n, c)| format!("CONSTRAINT {} CHECK ({c})", ident(n))));
    let mut s = format!("CREATE TABLE {name} (\n  {}\n)", parts.join(",\n  "));
    if let Some(p) = &m.partition {
        s += &format!("\nPARTITION BY {p}");
    }
    if !m.cluster.is_empty() {
        s += &format!("\nCLUSTER BY ({})", list_sql(&m.cluster));
    }
    if let Some(o) = &m.order {
        s += &format!("\nSEQUENCE BY {}", ident(o));
    }
    if let Some((c, secs)) = &m.ttl {
        s += &format!("\nTTL {} + INTERVAL '{}'", ident(c), span(*secs));
    }
    let mut with = vec![];
    if !m.publish.is_empty() {
        with.push(format!("publish = ({})", m.publish.join(", ")));
    }
    if let Some(r) = m.retention_secs {
        with.push(format!("retention = '{}'", span(r)));
    }
    if !with.is_empty() {
        s += &format!("\nWITH ({})", with.join(", "));
    }
    s
}

/// `CREATE MATERIALIZED VIEW`: its expectations, its options and its query.
fn materialized_sql(name: &str, v: &crate::views::View, meta: Option<&TableMeta>) -> String {
    use crate::views::OnViolation;
    let expect: Vec<String> = v.expect.iter().map(|e| match e.on {
        OnViolation::Fail => format!("CONSTRAINT {} CHECK ({})", ident(&e.name), e.check),
        OnViolation::Keep => format!("CONSTRAINT {} EXPECT ({})", ident(&e.name), e.check),
        OnViolation::Drop => format!("CONSTRAINT {} EXPECT ({}) ON VIOLATION DROP ROW", ident(&e.name), e.check),
    }).collect();
    if let Some((sql, with)) = crate::once::written(v) {
        // (EMIT FINAL's, a session view's too: the window in the GROUP BY)
        let with = if with.is_empty() { String::new() } else { format!(" WITH ({})", with.join(", ")) };
        return format!("CREATE MATERIALIZED VIEW {name}{with} AS\n{sql}");
    }
    let mut with: Vec<String> = vec![];
    if let Some(e) = &v.emit {
        with.push(format!("window = {}, size_secs = {}", literal(&e.window), e.size_secs));
        with.extend(e.slide_secs.map(|s| format!("slide_secs = {s}")));
        with.extend((e.lateness_secs > 0).then(|| format!("lateness_secs = {}", e.lateness_secs)));
    }
    if let Some(jn) = &v.join {
        with.push("join = 'streams'".into());
        with.extend((!jn.time.is_empty()).then(|| format!("time = {}", literal(&jn.time.join(", ")))));
        with.extend(jn.within_secs.map(|w| format!("within_secs = {w}")));
    }
    if let Some(h) = meta.and_then(|m| m.history.as_ref()) {
        with.push(format!("history = {}, sequence_by = {}", literal(&h.key.join(", ")), literal(&h.sequence_by)));
    }
    let expect = if expect.is_empty() { String::new() } else { format!(" (\n  {}\n)", expect.join(",\n  ")) };
    let with = if with.is_empty() { String::new() } else { format!(" WITH ({})", with.join(", ")) };
    format!("CREATE MATERIALIZED VIEW {name}{expect}{with} AS\n{}", v.sql)
}

/// `CREATE FUNCTION`, `CREATE MACRO` or `CREATE PROCEDURE`, in the form it was made in.
fn routine_sql(name: &str, r: &crate::routines::Routine) -> String {
    use crate::routines::Kind as R;
    let macro_ = r.what() == "macro";
    let params = r.params.iter().map(|p| {
        let named = Some(p.name.as_str()).filter(|n| !n.chars().all(|c| c.is_ascii_digit())).map(ident);
        let default = p.default.as_ref().map(|d| if macro_ { format!(":= {d}") } else { format!("DEFAULT {d}") });
        [named, p.ty.clone(), default].into_iter().flatten().collect::<Vec<_>>().join(" ")
    }).collect::<Vec<_>>().join(", ");
    if macro_ {
        return format!("CREATE MACRO {name}({params}) AS {}{}", if r.kind == R::Table { "TABLE " } else { "" }, r.body);
    }
    let what = if r.kind == R::Procedure { "PROCEDURE" } else { "FUNCTION" };
    let mut s = format!("CREATE {what} {name}({params})");
    if let Some(t) = &r.returns {
        s += &format!(" RETURNS {t}");
    }
    let o = &r.with;
    if let Some(v) = &o.volatility {
        s += &format!(" {}", v.to_uppercase());
    }
    if o.strict {
        s += " STRICT";
    }
    let mut with = vec![];
    if o.vectorized {
        with.push("vectorized = true".to_string());
    }
    if !o.packages.is_empty() {
        with.push(format!("packages = {}", literal(&o.packages)));
    }
    if !o.entry.is_empty() {
        with.push(format!("entry = {}", literal(&o.entry)));
    }
    with.extend(o.timeout.map(|t| format!("timeout = {t}")));
    with.extend(o.cache.map(|c| format!("cache = '{}'", span(c))));
    if !with.is_empty() {
        s += &format!(" WITH ({})", with.join(", "));
    }
    // (a SQL function of an expression: RETURN it; anything else is a body in dollar quotes)
    let expression = r.language == "sql" && r.kind == R::Macro && Parser::new(&GenericDialect {}).try_with_sql(&r.body).and_then(|mut p| p.parse_expr().map(|_| p.peek_token().token == Token::EOF)).unwrap_or(false);
    match expression {
        true => format!("{s} RETURN {}", r.body),
        false => format!("{s} LANGUAGE {} AS {}", r.language, dollar(&r.body)),
    }
}

/// `CREATE TASK`: when, after what, on what condition and with what options it runs.
fn task_sql(name: &str, t: &crate::runs::Task) -> String {
    let mut s = format!("CREATE TASK {name}");
    if t.after.is_empty() {
        s += &format!(" SCHEDULE {}", literal(&t.schedule));
    } else {
        s += &format!(" AFTER {}", t.after.iter().map(|a| name_sql(a)).collect::<Vec<_>>().join(", "));
    }
    if let Some(w) = &t.when {
        s += &format!(" WHEN {w}");
    }
    let o = &t.with;
    let mut with = vec![];
    if o.retries > 0 {
        with.push(format!("retries = {}", o.retries));
    }
    with.extend(o.retry_delay.map(|d| format!("retry_delay = '{}'", span(d))));
    with.extend(o.timeout.map(|d| format!("timeout = '{}'", span(d))));
    with.extend(o.on_failure.as_ref().map(|p| format!("on_failure = {}", name_sql(p))));
    if !with.is_empty() {
        s += &format!(" WITH ({})", with.join(", "));
    }
    format!("{s} AS\n{}", t.sql)
}

// ---------------------------------------------------------------- statements

/// What the registry's statements ask the leader to do (`Ddl::Object`).
#[derive(Serialize, Deserialize, Clone)]
#[serde(tag = "do", rename_all = "snake_case")]
pub enum Op {
    /// `COMMENT ON <kind> name IS '…' | NULL` (`kind` as written: `table`, `column`, …).
    Comment { kind: String, name: String, text: Option<String>, if_exists: bool },
    /// `CREATE OR ALTER TABLE`: `sql` is the statement as `CREATE TABLE`.
    OrAlterTable { name: String, sql: String },
}

/// The kind words a statement names (`materialized view`, `column`, …), longest first.
static KIND_WORDS: LazyLock<String> = LazyLock::new(|| {
    let mut words: Vec<&str> = KINDS.iter().map(|k| k.name).chain(["column"]).collect();
    words.sort_by_key(|w| std::cmp::Reverse(w.len()));
    words.iter().map(|w| w.replace(' ', r"\s+")).collect::<Vec<_>>().join("|")
});

/// `COMMENT ON …`, `CREATE OR ALTER TABLE | VIEW …`, or None for anything else.
pub fn statement(sql: &str) -> Option<crate::write::Stmt> {
    use crate::write::Stmt;
    static COMMENT: LazyLock<regex::Regex> = LazyLock::new(|| regex::Regex::new(&format!(r"(?is)^\s*comment\s+(if\s+exists\s+)?on\s+({})\s+(.*)$", *KIND_WORDS)).expect("a regex"));
    static OR_ALTER: LazyLock<regex::Regex> = LazyLock::new(|| regex::Regex::new(r"(?is)^(\s*create\s+)or\s+alter\s+(\w+(?:\s+view)?)\b").expect("a regex"));
    let first = crate::write::first_word(sql);
    if let Some(c) = COMMENT.captures(first) {
        let kind = c[2].split_whitespace().collect::<Vec<_>>().join(" ").to_lowercase();
        return Some(match comment_of(&c[3]) {
            Ok((name, text)) => Stmt::Ddl(vec![Ddl::Object(Op::Comment { kind, name, text, if_exists: c.get(1).is_some() })]),
            Err(e) => Stmt::Invalid(format!("COMMENT ON {} name IS 'text' | NULL: {e:#}", kind.to_uppercase())),
        });
    }
    let c = OR_ALTER.captures(first)?;
    let what = c[2].split_whitespace().collect::<Vec<_>>().join(" ").to_uppercase();
    let plain = format!("{}{}", &c[1], &first[c.get(0).expect("a match").end() - c[2].len()..]);
    match what.as_str() {
        "VIEW" => crate::write::parse(&plain.replacen(&c[1], &format!("{}OR REPLACE ", &c[1]), 1)), // (a view holds nothing to keep)
        "TABLE" => Some(match crate::write::parse(&plain) {
            Some(Stmt::Create(t)) if t.query.is_none() && t.like.is_none() && !t.or_replace && !t.if_not_exists && !t.temporary => {
                Stmt::Ddl(vec![Ddl::Object(Op::OrAlterTable { name: crate::write::object(&t.name), sql: plain })])
            }
            Some(Stmt::Invalid(e)) => Stmt::Invalid(e),
            _ => Stmt::Invalid("CREATE OR ALTER TABLE takes the table's columns (… AS SELECT makes it anew: CREATE OR REPLACE TABLE)".into()),
        }),
        w => Some(Stmt::Invalid(format!("CREATE OR ALTER {w}: tables and views (CREATE OR REPLACE {w} makes one anew)"))),
    }
}

/// `name IS 'text' | NULL`: the name as SQL resolves it, and the text (None: no comment).
fn comment_of(rest: &str) -> Result<(String, Option<String>)> {
    let mut p = Parser::new(&GenericDialect {}).try_with_sql(rest)?;
    let name = crate::write::object(&p.parse_object_name(false)?);
    p.expect_keyword_is(Keyword::IS)?;
    let text = match p.next_token().token {
        Token::Word(w) if w.keyword == Keyword::NULL => None,
        Token::SingleQuotedString(s) => Some(s),
        Token::DollarQuotedString(s) => Some(s.value),
        t => bail!("a string or NULL, not {t}"),
    };
    let _ = p.consume_token(&Token::SemiColon);
    ensure!(p.peek_token().token == Token::EOF, "one name, then IS 'text' or IS NULL");
    Ok((name, text.filter(|t| !t.is_empty())))
}

/// Leader: carry one out (under the lake's lock).
pub async fn apply(lake: &Lake, op: Op) -> Result<Value> {
    match op {
        Op::Comment { kind, name, text, if_exists } => comment(lake, &kind, &name, text, if_exists).await,
        Op::OrAlterTable { name, sql } => or_alter(lake, &name, &sql).await,
    }
}

/// A name of this lake, as the catalog keys it; another lake's objects are described from there.
async fn local(lake: &Lake, name: &str, family: &str) -> Result<String> {
    if !matches!(family, "relation" | "routine" | "task") {
        return Ok(name.to_string());
    }
    let (other, local) = crate::ddl::resolve(lake, name).await?;
    ensure!(other.is_none(), "{name} is an attached lake's: describe it from a node of that lake");
    Ok(local)
}

/// Is the object a comment key names (`family/name`, `column/table/stored`) there?
async fn there(lake: &Lake, key: &str) -> Result<bool> {
    let (family, name) = key.split_once('/').unwrap_or((key, ""));
    let has = |k: String| async move { lake.cat.get::<Value>(&k).await.map(|v| v.is_some()) };
    Ok(match family {
        "relation" => !crate::sys::hidden(name) && (has(table_key(name)).await? || has(crate::ddl::query_key(name)).await?),
        "column" => match name.rsplit_once('/') {
            Some((t, c)) => lake.cat.get::<TableMeta>(&table_key(t)).await?.is_some_and(|m| m.live().any(|(s, _, _)| s == c)),
            None => false,
        },
        "routine" => has(crate::routines::key(name)).await?,
        "task" => has(crate::runs::task_key(name)).await?,
        "schema" => name == PUBLIC || has(crate::ddl::schema_key(name)).await?,
        "secret" => has(format!("e/{name}")).await?,
        "user" => has(format!("u/{name}")).await?,
        "database" => has(crate::ddl::attachment_key(name)).await? || has(format!("o/{name}")).await?,
        _ => false,
    })
}

async fn comment(lake: &Lake, word: &str, name: &str, text: Option<String>, if_exists: bool) -> Result<Value> {
    let key = match word {
        "column" => {
            let (table, column) = name.rsplit_once('.').context("COMMENT ON COLUMN table.column")?;
            let table = local(lake, table, "relation").await?;
            let meta = lake.cat.get::<TableMeta>(&table_key(&table)).await?.filter(|_| !crate::sys::hidden(&table));
            let stored = meta.as_ref().and_then(|m| m.stored(column).map(str::to_string));
            match stored {
                Some(s) => format!("column/{table}/{s}"),
                None if if_exists => return Ok(j!({"column": name, "exists": false})),
                None => bail!("no column {column} in {table}"),
            }
        }
        w => {
            let family = kind(w).context("a kind of object")?.family;
            let key = format!("{family}/{}", local(lake, name, family).await?);
            if !there(lake, &key).await? {
                ensure!(if_exists, "no {w} {name}");
                return Ok(j!({word: name, "exists": false}));
            }
            key
        }
    };
    match &text {
        Some(t) => lake.cat.commit(vec![(format!("cm/{key}"), json(t))], &[]).await?,
        None => lake.cat.commit(vec![], &[format!("cm/{key}")]).await?,
    };
    Ok(j!({word: name, "comment": text}))
}

/// Does carrying out `d` drop or rename something a comment may be on?
pub fn moves(d: &Ddl) -> bool {
    matches!(d, Ddl::DropTable { .. } | Ddl::DropView { .. } | Ddl::DropSchema { .. } | Ddl::DropRoutine { .. } | Ddl::DropTask { .. } | Ddl::DropSecret { .. }
        | Ddl::Detach { .. } | Ddl::DropDatabase { .. } | Ddl::RenameTable { .. } | Ddl::AlterColumn { .. } | Ddl::Users(_))
}

/// After a drop or a rename (`out`: what it answered): comments follow a renamed table or view, and
/// go with what is gone. (One commit after the statement's own; until then a comment on what
/// went is only left over, never shown: `list` reads comments by what is there.)
pub async fn follow(lake: &Lake, out: &Value) -> Result<()> {
    let notes = lake.cat.scan::<String>("cm/", "cm0").await?;
    if notes.is_empty() {
        return Ok(());
    }
    let (mut put, mut gone) = (vec![], vec![]);
    let renamed = match (out["table"].as_str().or(out["view"].as_str()), out["renamed"].as_str()) {
        (Some(from), Some(to)) => Some((from.to_string(), to.to_string())),
        _ => None,
    };
    for (k, text) in notes {
        let moved = renamed.as_ref().and_then(|(from, to)| {
            let rest = k.strip_prefix("cm/")?;
            if rest == format!("relation/{from}") {
                Some(format!("cm/relation/{to}"))
            } else {
                rest.strip_prefix(&format!("column/{from}/")).map(|c| format!("cm/column/{to}/{c}"))
            }
        });
        match moved {
            Some(to) => {
                put.push((to, json(&text)));
                gone.push(k);
            }
            None if !there(lake, &k[3..]).await? => gone.push(k),
            None => {}
        }
    }
    if !put.is_empty() || !gone.is_empty() {
        lake.cat.commit(put, &gone).await?;
    }
    Ok(())
}

/// `CREATE OR ALTER TABLE`: the table made, or the one there brought to this definition — columns
/// added at the end, types widened, its layout and options as written (one left out is taken
/// away). What would lose rows or change what they mean is refused, with the statement that does
/// it on purpose.
async fn or_alter(lake: &Lake, name: &str, sql: &str) -> Result<Value> {
    use crate::write::{parse, Stmt};
    let Some(Stmt::Create(create)) = parse(sql) else { bail!("CREATE OR ALTER TABLE: a table's columns") };
    let table = local(lake, name, "relation").await?;
    let spec = crate::write::create_spec(&create, lake, false).await?;
    let Some(old) = lake.cat.get::<TableMeta>(&table_key(&table)).await? else {
        let out = crate::write::create_table(lake, &table, &spec).await?;
        return Ok(j!({"table": out["table"], "created": true}));
    };
    let old = old.logical();
    let mut s: Value = serde_json::from_str(&spec)?;
    let pairs = |v: &Value| -> Vec<(String, String)> { serde_json::from_value(v.clone()).unwrap_or_default() };
    let strings = |v: &Value| -> Vec<String> { serde_json::from_value(v.clone()).unwrap_or_default() };
    // (a keyed table's `_deleted` is its own, wherever it is: never in a definition)
    let columns: Vec<(String, String)> = pairs(&s["columns"]).into_iter().filter(|(c, _)| !old.marker(c)).collect();
    let was: Vec<&(String, String)> = old.columns.iter().filter(|(c, _)| !old.marker(c)).collect();
    // What can't be brought along: a column taken away, moved or renamed; another key or merge.
    for (i, (c, _)) in was.iter().enumerate() {
        match columns.iter().position(|(n, _)| n == c) {
            None => bail!("{table}.{c} isn't in the definition: CREATE OR ALTER adds columns, it doesn't take them away (ALTER TABLE {table} DROP COLUMN {c}, or RENAME COLUMN {c} TO …)"),
            Some(j) => ensure!(i == j, "{table}.{c} moved: columns are added at the end, after {}", was.last().map_or("", |l| l.0.as_str())),
        }
    }
    let key = strings(&s["key"]);
    ensure!(key == old.key, "{table}'s PRIMARY KEY can't change ({} now): CREATE OR REPLACE TABLE makes it anew", if old.key.is_empty() { "none".to_string() } else { old.key.join(", ") });
    let merge: BTreeMap<String, String> = serde_json::from_value(s["merge"].clone()).unwrap_or_default();
    ensure!(was.iter().all(|(c, _)| merge.get(c) == old.merge.get(c)), "{table}'s MERGE functions can't change: CREATE OR REPLACE TABLE makes it anew");
    ensure!(s["partition_by"].as_str() == old.partition.as_deref(), "{table}'s PARTITION BY can't change: each of its files holds one partition");
    ensure!(old.order.is_none() || s["order_by"].as_str().is_some(), "{table}'s SEQUENCE BY can't be taken away: CREATE OR REPLACE TABLE makes it anew");
    let not_null: Vec<String> = strings(&s["not_null"]).into_iter().chain(key.iter().cloned()).collect();
    let defaults: BTreeMap<String, String> = serde_json::from_value(s["defaults"].clone()).unwrap_or_default();
    for (c, _) in &was {
        ensure!(not_null.contains(c) == old.not_null.contains(c) && defaults.get(c) == old.defaults.get(c), "{table}.{c}: NOT NULL and DEFAULT of a column there can't change yet");
    }
    let checks: Vec<(String, String)> = serde_json::from_value(s["checks"].clone()).unwrap_or_default();
    ensure!(checks == old.checks, "{table}'s CHECK constraints can't change yet");
    // Types widened, as ALTER COLUMN … TYPE does (and refuses, by name, one that would narrow).
    let mut changed = vec![];
    for ((c, t), (_, new)) in was.iter().zip(&columns) {
        if crate::query::dtype(t).ok() != crate::query::dtype(new).ok() {
            let written = create.columns.iter().find(|d| d.name.value == *c || d.name.value.to_lowercase() == *c).map_or_else(|| sql_type(new), |d| d.data_type.to_string());
            Box::pin(crate::ddl::apply(lake, Ddl::AlterColumn { table: table.clone(), column: c.clone(), change: crate::ddl::Change::Type(written) })).await?;
            changed.push(format!("{c} {}", sql_type(new)));
        }
    }
    // The table's columns as they are now, then the new ones (as `ALTER TABLE … ADD COLUMN` adds them).
    let now = lake.cat.get::<TableMeta>(&table_key(&table)).await?.context("the table")?.logical();
    s["columns"] = j!(now.columns.iter().cloned().chain(columns[was.len()..].iter().cloned()).collect::<Vec<_>>());
    // Layout and options as written: one left out is taken away.
    s["cluster_by"] = s["cluster_by"].take().as_array().map_or(j!([]), |a| j!(a));
    s["publish"] = if s["publish"].is_null() { j!(crate::store::default_publish()) } else { s["publish"].take() };
    crate::write::create_table(lake, &table, &s.to_string()).await?;
    let mut meta = lake.cat.get::<TableMeta>(&table_key(&table)).await?.context("the table")?;
    let (ttl, retention) = (s["ttl"].is_null() && meta.ttl.is_some(), s["retention"].is_null() && meta.retention_secs.is_some());
    if ttl || retention {
        meta.ttl = meta.ttl.filter(|_| !ttl);
        meta.retention_secs = meta.retention_secs.filter(|_| !retention);
        lake.cat.commit(vec![(table_key(&table), json(&meta))], &[]).await?;
    }
    let new = meta.logical();
    changed.extend(new.columns[old.columns.len()..].iter().map(|(c, _)| format!("+ {c}")));
    for (what, was, now) in [
        ("CLUSTER BY", old.cluster.join(", "), new.cluster.join(", ")),
        ("SEQUENCE BY", old.order.clone().unwrap_or_default(), new.order.clone().unwrap_or_default()),
        ("TTL", old.ttl.as_ref().map(|t| span(t.1)).unwrap_or_default(), new.ttl.as_ref().map(|t| span(t.1)).unwrap_or_default()),
        ("publish", old.publish.join(", "), new.publish.join(", ")),
        ("retention", old.retention_secs.map(span).unwrap_or_default(), new.retention_secs.map(span).unwrap_or_default()),
    ] {
        if was != now {
            changed.push(format!("{what}: {} → {}", if was.is_empty() { "none" } else { &was }, if now.is_empty() { "none" } else { &now }));
        }
    }
    Ok(match changed.is_empty() {
        true => j!({"table": table, "unchanged": true}),
        false => j!({"table": table, "altered": changed}),
    })
}

// ---------------------------------------------------------------- reading them

/// `SHOW CREATE <kind> name`: the statements that make it again (its comments too), as one row.
/// None for any other statement.
pub async fn show_create(lake: &Lake, sql: &str) -> Result<Option<String>> {
    static SHOW: LazyLock<regex::Regex> = LazyLock::new(|| regex::Regex::new(&format!(r"(?is)^\s*show\s+create\s+({})\s+(.+?)\s*;?\s*$", *KIND_WORDS)).expect("a regex"));
    if !sql.trim_start().get(..4).is_some_and(|w| w.eq_ignore_ascii_case("show")) {
        return Ok(None);
    }
    let Some(c) = SHOW.captures(sql) else { return Ok(None) };
    let word = c[1].split_whitespace().collect::<Vec<_>>().join(" ").to_lowercase();
    let name = {
        let mut p = Parser::new(&GenericDialect {}).try_with_sql(&c[2])?;
        let n = crate::write::object(&p.parse_object_name(false)?);
        ensure!(p.peek_token().token == Token::EOF, "SHOW CREATE {} name", word.to_uppercase());
        n
    };
    let wanted = kind(&word).context("SHOW CREATE TABLE | VIEW | MATERIALIZED VIEW | FUNCTION | PROCEDURE | TASK | SCHEMA | ROLE | DATABASE name")?;
    let here = crate::ddl::lake_name(lake);
    let parts: Vec<&str> = name.split('.').collect();
    let all = list(lake).await?;
    let schema = |o: &Object, s: &str| o.schema.as_deref().unwrap_or(PUBLIC) == s;
    let found = all.iter().filter(|o| {
        o.family() == wanted.family && (o.kind == wanted.name || matches!(wanted.name, "table" | "function")) && match parts[..] {
            [n] => o.name == n && o.lake == here && schema(o, PUBLIC),
            [s, n] => o.name == n && ((o.lake == here && schema(o, s)) || (o.lake == s && schema(o, PUBLIC))),
            [l, s, n] => o.name == n && o.lake == l && schema(o, s),
            _ => false,
        }
    }).min_by_key(|o| o.lake != here); // (this lake's schema before an attached lake's name)
    let o = found.with_context(|| format!("no {word} {name}"))?;
    let def = o.definition.as_ref().with_context(|| format!("{} {name}: {}", o.kind, match o.kind {
        "secret" => "a secret's values are never shown",
        "user" => "a user's password and tokens are never shown (CREATE USER … PASSWORD '…')",
        "external table" => "its statement isn't kept as written yet (its query is in pondra.tables)",
        _ => "made with its materialized view",
    }))?;
    let mut script = vec![def.clone()];
    let word = match o.family() {
        "routine" if o.kind == "procedure" => "PROCEDURE".to_string(),
        "routine" => "FUNCTION".into(),
        "relation" if o.kind == "external table" => "TABLE".into(),
        _ => o.kind.to_uppercase(),
    };
    let named = if o.lake == here { name_sql(&o.local()) } else { format!("{}.{}", ident(&o.lake), name_sql(&join(o.schema.as_deref().unwrap_or(PUBLIC), &o.name))) };
    if let Some(c) = &o.comment {
        script.push(format!("COMMENT ON {word} {named} IS {}", literal(c)));
    }
    if o.family() == "relation" && o.lake == here {
        if let Some(m) = lake.cat.get::<TableMeta>(&table_key(&o.local())).await? {
            for (k, text) in lake.cat.scan::<String>(&format!("cm/column/{}/", o.local()), &format!("cm/column/{}0", o.local())).await? {
                let stored = k.rsplit('/').next().unwrap_or_default();
                script.push(format!("COMMENT ON COLUMN {named}.{} IS {}", ident(m.name_of(stored)), literal(&text)));
            }
        }
    }
    Ok(Some(format!("SELECT {} AS definition", literal(&format!("{};", script.join(";\n"))))))
}

/// Does `sql` read `pondra.objects` or `pondra.kinds`?
pub fn mentioned(sql: &str) -> bool {
    let s = sql.to_lowercase();
    s.contains("pondra.objects") || s.contains("pondra.kinds")
}

/// `pondra.objects` and `pondra.kinds`, as they are now (when `text` reads them).
pub async fn tables(lake: &Lake, text: &str) -> Result<Vec<(&'static str, Arc<dyn datafusion::catalog::TableProvider>)>> {
    use datafusion::arrow::array::{ArrayRef, RecordBatch, StringArray};
    use datafusion::datasource::MemTable;
    if !mentioned(text) {
        return Ok(vec![]);
    }
    let mem = |b: RecordBatch| -> Result<Arc<dyn datafusion::catalog::TableProvider>> { Ok(Arc::new(MemTable::try_new(b.schema(), vec![vec![b]])?)) };
    let all = list(lake).await?;
    let o = |f: &dyn Fn(&Object) -> Option<String>| Arc::new(all.iter().map(f).collect::<StringArray>()) as ArrayRef;
    let objects = RecordBatch::try_from_iter(vec![
        ("kind", o(&|o| Some(o.kind.to_string()))),
        ("lake", o(&|o| Some(o.lake.clone()))),
        ("schema", o(&|o| o.schema.clone())),
        ("name", o(&|o| Some(o.name.clone()))),
        ("comment", o(&|o| o.comment.clone())),
        ("definition", o(&|o| o.definition.clone())),
    ])?;
    let k = |f: &dyn Fn(&Kind) -> String| Arc::new(KINDS.iter().map(|k| Some(f(k))).collect::<StringArray>()) as ArrayRef;
    let kinds = RecordBatch::try_from_iter(vec![("kind", k(&|k| k.name.into())), ("family", k(&|k| k.family.into())), ("statements", k(&|k| k.verbs.join(", ")))])?;
    Ok(vec![("objects", mem(objects)?), ("kinds", mem(kinds)?)])
}

/// `GET /kinds`: every kind of object, its family and the statements it takes.
pub fn kinds() -> Value { Value::Array(KINDS.iter().map(|k| j!({"kind": k.name, "family": k.family, "statements": k.verbs})).collect()) }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statements_and_names() {
        let ddl = |s: &str| match statement(s) {
            Some(crate::write::Stmt::Ddl(d)) => serde_json::to_value(&d[0]).unwrap(),
            Some(crate::write::Stmt::Invalid(e)) => j!({"error": e}),
            _ => j!(null),
        };
        assert_eq!(ddl("COMMENT ON TABLE sales.Orders IS 'one row per order'")["text"], "one row per order");
        assert_eq!(ddl("comment on materialized view v is null")["kind"], "materialized view");
        assert_eq!(ddl("COMMENT IF EXISTS ON COLUMN t.\"Amount\" IS $$in euros$$")["name"], "t.Amount");
        assert_eq!(ddl("COMMENT ON TASK nightly IS ''")["text"], j!(null));
        assert!(ddl("COMMENT ON TABLE t IS 42")["error"].is_string());
        assert_eq!(ddl("CREATE OR ALTER TABLE t (a BIGINT) CLUSTER BY (a)")["do"], "or_alter_table");
        assert!(ddl("CREATE OR ALTER TABLE t AS SELECT 1")["error"].is_string());
        assert!(ddl("CREATE OR ALTER MATERIALIZED VIEW v AS SELECT 1")["error"].as_str().unwrap().contains("CREATE OR REPLACE MATERIALIZED VIEW"));
        assert_eq!(ddl("SELECT 1"), j!(null));
        assert_eq!((ident("orders"), ident("Orders"), ident("order"), ident("2nd")), ("orders".into(), "\"Orders\"".into(), "\"order\"".into(), "\"2nd\"".into()));
        assert_eq!((span(7200), span(86400 * 7), span(90), sql_type("Timestamp(Microsecond, None)"), sql_type("Float32[]")), ("2 hours".into(), "7 days".into(), "90 seconds".into(), "TIMESTAMP".into(), "REAL[]".into()));
    }
}
