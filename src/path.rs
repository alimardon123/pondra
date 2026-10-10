//! A session's search path (round 34): `USE` and Postgres's `SET search_path`. A session says which
//! database and schemas its one-part names mean, and that is worked out where SQL comes in: the
//! session's path is written into the statement's text (`door`), so everything after it (the result
//! cache, spread queries, the leader's writes, a task's stored body, history) sees full names and
//! needs no session. A stored view is written and read apart: one made under `USE crm` keeps
//! `crm.t`, and one made without a path reads `t` as public, whoever reads it (`routines::expand_stored`).
//! Names already full, and this lake's own `public`, come out as they went in.
use crate::ddl::{self, join, lake_name, PUBLIC};
use crate::codes::coded;
use crate::store::{table_key, Lake};
use anyhow::{Context, Result};
use datafusion::sql::sqlparser::ast::{self, CommentObject, CopySource, CreateTableLikeKind, DataType, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, GrantObjects, ObjectName, ObjectNamePart, ObjectType, Query, Statement, TableFactor, TableObject, Value, VisitMut, VisitorMut};
use datafusion::sql::sqlparser::{dialect::GenericDialect, keywords::Keyword, parser::Parser};
use regex::Regex;
use std::collections::HashSet;
use std::ops::ControlFlow;
use std::sync::{Arc, LazyLock};

/// The schemas a session's one-part names are looked for in, in order: (database, schema). A
/// database is this lake (`here`) or an attached one.
#[derive(Clone, Debug, PartialEq)]
pub struct Path {
    pub here: String,
    pub entries: Vec<(String, String)>,
}

impl Path {
    /// This lake's public, as a session with no path has it.
    fn plain(here: &str) -> Path { Path { here: here.into(), entries: vec![(here.into(), PUBLIC.into())] } }
    pub fn database(&self) -> &str { &self.entries[0].0 }
    pub fn schema(&self) -> &str { &self.entries[0].1 }
    /// The parts a name of `e` gets in front: none for this lake's public (the name is as it was),
    /// its schema for another schema here, the database and schema for another lake's.
    fn prefix(&self, e: &(String, String)) -> Vec<String> {
        match (e.0 == self.here, e.1 == PUBLIC) {
            (true, true) => vec![],
            (true, false) => vec![e.1.clone()],
            (false, _) => vec![e.0.clone(), e.1.clone()],
        }
    }
}

/// What a name names. Tables, views, sequences and indexes share one namespace, as in Postgres.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
enum Fam {
    Relation,
    Routine,
    Type,
    Task,
}

/// What exists, in each place the path has it: (family, database, name in the lake), and each
/// database's schemas (for `s.t` in another database). Built for the names one statement uses.
#[derive(Default)]
struct World {
    names: HashSet<(Fam, String, String)>,
    schemas: HashSet<(String, String)>,
}

impl World {
    fn holds(&self, fam: Fam, db: &str, name: &str) -> bool { self.names.contains(&(fam, db.to_string(), name.to_string())) }
}

/// The path a `search_path` text says: entries split at commas outside quotes, each a schema or a
/// `database.schema`, quotes taken off and unquoted parts lower-cased. `$user` is left out, and no
/// entry means this lake's public.
pub fn parse(here: &str, text: &str) -> Path {
    static ENTRY: LazyLock<Regex> = LazyLock::new(|| Regex::new(r#"(?:"[^"]*"|[^,"])+"#).expect("a regex"));
    let mut entries: Vec<(String, String)> = ENTRY.find_iter(text).filter_map(|e| match name_parts(e.as_str()).as_slice() {
        [s] if s.as_str() != "$user" => Some((here.to_string(), s.clone())),
        [d, s] => Some((d.clone(), s.clone())),
        _ => None,
    }).collect();
    if entries.is_empty() {
        entries.push((here.to_string(), PUBLIC.to_string()));
    }
    Path { here: here.to_string(), entries }
}

/// The path the current session set, if it set one other than this lake's public (else None).
pub fn current(lake: &Lake) -> Option<Path> {
    let here = lake_name(lake);
    let path = parse(&here, &crate::settings::shown("search_path")?);
    (path != Path::plain(&here)).then_some(path)
}

/// This lake's schemas on the current session's path (`public` when none is set): what Postgres's
/// catalog calls visible (`pg_catalog.rs`).
pub fn schemas_here(lake: &Lake) -> Vec<String> {
    match current(lake) {
        Some(p) => p.entries.iter().filter(|(d, _)| *d == p.here).map(|(_, s)| s.clone()).collect(),
        None => vec![PUBLIC.into()],
    }
}

/// Is `sql` a `USE`? The session's to set (`settings.rs` checks for one first).
pub fn is_use(sql: &str) -> bool {
    static USE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(?i)^use\s").expect("a regex"));
    USE.is_match(crate::write::first_word(sql))
}

/// `USE crm`, `USE dev`, `USE dev.sales`, `USE DATABASE dev`, `USE CATALOG dev`, `USE SCHEMA sales`
/// and `USE SCHEMA dev.sales`: the `search_path` text it sets. A name that is neither a database nor
/// a schema here is refused by name, and so is one that is both.
pub async fn use_of(lake: &Lake, sql: &str) -> Result<String> {
    static USE: LazyLock<Regex> = LazyLock::new(|| Regex::new(&format!(r"(?is)^\s*use\s+(?:(database|catalog|schema)\s+)?({NAME})(?:\s*\.\s*({NAME}))?\s*;?\s*$")).expect("a regex"));
    let m = USE.captures(sql).context("USE takes a database, a schema or database.schema (USE crm; USE DATABASE dev)")?;
    let kw = m.get(1).map(|k| k.as_str().to_lowercase());
    let first = name_parts(&m[2]).into_iter().next().unwrap_or_default();
    let second = m.get(3).map(|s| name_parts(s.as_str()).into_iter().next().unwrap_or_default());
    let here = lake_name(lake);
    let now = current(lake).unwrap_or_else(|| Path::plain(&here));
    let entry = match (kw.as_deref(), second) {
        (_, Some(schema)) => {
            schema_in(lake, &first, &schema).await?;
            (first, schema)
        }
        (Some("database" | "catalog"), None) => {
            db_lake(lake, &first).await?;
            (first, PUBLIC.into())
        }
        (Some(_), None) => {
            schema_in(lake, now.database(), &first).await?;
            (now.database().into(), first)
        }
        (None, None) => {
            let is_db = ddl::database(lake, &first).await?.is_some();
            let db = db_lake(lake, now.database()).await?;
            let is_schema = ddl::has_schema(&db, &first).await?;
            match (is_db, is_schema) {
                (true, true) => return Err(coded("42P09", format!("{first} is both a database and a schema: USE DATABASE {first} or USE SCHEMA {first}"))),
                (true, false) => (first, PUBLIC.into()),
                (false, true) => (now.database().into(), first),
                (false, false) => return Err(coded("3F000", format!("no database or schema {first}"))),
            }
        }
    };
    Ok(text_of(&here, &[entry]))
}

/// `SHOW search_path` as a one-row answer: the session's path as it was set, `public` when none
/// was. Every door reads it so (the Postgres port answers it the same way: `pg.rs`).
pub fn show(sql: &str) -> Option<String> {
    static SHOW: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(?i)^\s*show\s+search_path\s*;?\s*$").expect("a regex"));
    SHOW.is_match(sql).then(|| {
        let path = crate::settings::shown("search_path").unwrap_or_else(|| PUBLIC.to_string());
        format!("SELECT '{}' AS search_path", path.replace('\'', "''"))
    })
}

/// `SET SCHEMA 'x'` (Postgres's alias of `SET search_path TO x`, which sqlparser doesn't read): x.
pub fn set_schema(sql: &str) -> Option<String> {
    static SET: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(?is)^\s*set\s+schema\s+(?:'((?:[^']|'')*)'|(\S+?))\s*;?\s*$").expect("a regex"));
    let m = SET.captures(sql)?;
    Some(match m.get(1) {
        Some(q) => q.as_str().replace("''", "'"),
        None => m[2].to_string(),
    })
}

/// `sql` as the current session's path reads it: one-part names (and two-part ones in another
/// database) written in full, and `current_database()`, `current_catalog()` and `current_schema()`
/// as their values. `skip` names what is not a table (a client's frames). Text with nothing to
/// change comes back as it came.
pub async fn door(lake: &Lake, sql: &str, skip: &HashSet<String>) -> Result<String> {
    if crate::temp::current().is_none() && (is_use(sql) || set_schema(sql).is_some()) {
        return Err(coded("0A000", crate::settings::NO_SESSION)); // (Flight SQL and MCP hold no session: refused by name, at once)
    }
    let path = match current(lake) {
        Some(p) => p,
        None if !mentions_current(sql) => return Ok(sql.to_string()),
        None => Path::plain(&lake_name(lake)),
    };
    if let Some(out) = head(lake, &path, sql, &HEADS).await? {
        return Ok(out);
    }
    if let Some(out) = create_task(lake, &path, skip, sql).await? {
        return Ok(out);
    }
    let Some(mut stmts) = parse_text(sql) else {
        return Ok(head(lake, &path, sql, &LATE).await?.unwrap_or_else(|| sql.to_string())); // (text sqlparser can't read)
    };
    let wanted = collect(&path, skip, &mut stmts);
    let w = world(lake, &path, &wanted).await?;
    if !apply(&path, skip, &w, &mut stmts) {
        return Ok(sql.to_string());
    }
    Ok(stmts.iter().map(crate::routines::sql).collect::<Vec<_>>().join(";\n"))
}

/// A name's parts as SQL means them: a quoted part as it is written, an unquoted one in lower case
/// (`"My.s".t` has two parts).
fn name_parts(name: &str) -> Vec<String> {
    static PART: LazyLock<Regex> = LazyLock::new(|| Regex::new(r#""[^"]*"|[^."\s]+"#).expect("a regex"));
    PART.find_iter(name).map(|m| match m.as_str().strip_prefix('"').and_then(|p| p.strip_suffix('"')) {
        Some(quoted) => quoted.to_string(),
        None => m.as_str().to_lowercase(),
    }).collect()
}

/// A name in SQL: its part as it is when it is a plain lower-case word, quoted otherwise.
fn plain(p: &str) -> bool { !p.is_empty() && p.chars().enumerate().all(|(i, c)| c == '_' || c.is_ascii_lowercase() || (i > 0 && c.is_ascii_digit())) }

fn quoted(p: &str) -> String { if plain(p) { p.into() } else { format!("\"{p}\"") } }

fn qualified(parts: &[String]) -> String { parts.iter().map(|p| quoted(p)).collect::<Vec<_>>().join(".") }

/// A part of a name the path adds, as a SQL identifier.
fn part(p: &str) -> ObjectNamePart {
    ObjectNamePart::Identifier(if plain(p) { ast::Ident::new(p) } else { ast::Ident::with_quote('"', p) })
}

/// The `search_path` text of entries: this lake's schemas bare, another database's as `db.schema`.
fn text_of(here: &str, entries: &[(String, String)]) -> String {
    entries.iter().map(|(d, s)| if d.as_str() == here { quoted(s) } else { format!("{}.{}", quoted(d), quoted(s)) }).collect::<Vec<_>>().join(", ")
}

/// Does `hay` hold `needle`, whatever its case?
fn contains_ci(hay: &str, needle: &str) -> bool { hay.as_bytes().windows(needle.len()).any(|w| w.eq_ignore_ascii_case(needle.as_bytes())) }

fn mentions_current(sql: &str) -> bool {
    ["current_schema", "current_database", "current_catalog"].iter().any(|w| contains_ci(sql, w))
}

/// The database a name is: this lake, or an attached one (or the error that names it).
async fn db_lake(lake: &Lake, name: &str) -> Result<Arc<Lake>> {
    ddl::database(lake, name).await?.ok_or_else(|| coded("3D000", format!("no database {name} (CREATE DATABASE {name})")))
}

/// Fails by name unless `s` is a schema of database `db`.
async fn schema_in(lake: &Lake, db: &str, s: &str) -> Result<()> {
    let found = db_lake(lake, db).await?;
    if ddl::has_schema(&found, s).await? {
        return Ok(());
    }
    Err(coded("3F000", format!("no schema {s} in {db} (CREATE SCHEMA {s})")))
}

/// `sql` read as statements, with the past's and DuckDB's forms first (as `expand_with` reads them).
fn parse_text(sql: &str) -> Option<Vec<Statement>> {
    let sql = crate::past::syntax(sql);
    let sql = crate::friendly::text(&sql).ok()?;
    Parser::parse_sql(&GenericDialect {}, &sql).ok()
}

/// A name's parts, one place for one name: the family's own, when the name is one part.
fn one_name(fam: Fam, parts: &[String]) -> Vec<(Fam, String)> {
    match parts {
        [n] => vec![(fam, n.clone())],
        _ => vec![],
    }
}

/// `name` (dotted, one place or more) with the parts the path puts in front of it, when it goes
/// somewhere else; None when it stays.
fn placed(path: &Path, w: &World, fam: Fam, name: &str, creating: bool) -> Option<String> {
    let parts: Vec<String> = name.split('.').map(str::to_string).collect();
    let prefix = resolve(path, w, fam, &parts, creating).filter(|p| !p.is_empty())?;
    Some(format!("{}.{name}", qualified(&prefix)))
}

/// The parts to put in front of a name of `fam` that is written `parts`. Empty: it stays. None: it
/// is not a name the path places (a routine or a type nothing on the path has stays as written, so
/// `run` and the others are never taken for a function of the session's).
fn resolve(path: &Path, w: &World, fam: Fam, parts: &[String], creating: bool) -> Option<Vec<String>> {
    match parts {
        [n] => pick(path, w, fam, n, creating).map(|e| path.prefix(e)),
        [s, _] if path.database() != path.here && w.schemas.contains(&(path.database().to_string(), s.clone())) => Some(vec![path.database().to_string()]),
        _ => None,
    }
}

/// The first place of the path a one-part name of `fam` is in: the first place for a new name, and
/// the only one when there is one place (a table is there whether or not it is made yet).
fn pick<'a>(path: &'a Path, w: &World, fam: Fam, n: &str, creating: bool) -> Option<&'a (String, String)> {
    let first = &path.entries[0];
    if creating || (path.entries.len() == 1 && matches!(fam, Fam::Relation | Fam::Task)) {
        return Some(first);
    }
    match path.entries.iter().find(|e| w.holds(fam, &e.0, &join(&e.1, n))) {
        Some(e) => Some(e),
        None if matches!(fam, Fam::Routine | Fam::Type) => None,
        None => Some(first),
    }
}

/// Statements sqlparser doesn't read, or that `expand_with` answers before it parses: the head,
/// the family of the name after it, and whether that name is made here.
type Head = (Regex, Fam, bool);

const NAME: &str = r#"(?:"[^"]*"|[A-Za-z_][A-Za-z0-9_-]*)"#;

fn heads(list: &[(&str, Fam, bool)]) -> Vec<Head> {
    list.iter()
        .map(|(h, f, c)| (Regex::new(&format!(r"(?is)^\s*{h}(?:\s+if\s+(?:not\s+)?exists)?\s+({NAME}(?:\s*\.\s*{NAME})*)")).expect("a regex"), *f, *c))
        .collect()
}

static HEADS: LazyLock<Vec<Head>> = LazyLock::new(|| heads(&[
    (r"show\s+create\s+(?:table|view|materialized\s+view|external\s+table|sequence|index)", Fam::Relation, false),
    (r"show\s+create\s+type", Fam::Type, false),
    (r"show\s+create\s+(?:function|procedure|macro)", Fam::Routine, false),
    (r"show\s+create\s+task", Fam::Task, false),
    (r"summarize", Fam::Relation, false),
    (r"restore\s+table", Fam::Relation, false),
    (r"undrop\s+(?:table|view|materialized\s+view)", Fam::Relation, false),
    (r"alter\s+materialized\s+view", Fam::Relation, false),
    (r"alter\s+sequence", Fam::Relation, false),
    (r"refresh\s+materialized\s+view(?:\s+concurrently)?", Fam::Relation, false),
    (r"create(?:\s+or\s+replace)?\s+procedure", Fam::Routine, true),
    (r"drop\s+macro(?:\s+table)?", Fam::Routine, false),
    (r"(?:alter|drop)\s+task", Fam::Task, false),
    (r"execute\s+task", Fam::Task, false),
]));

/// The heads that count only when sqlparser can't read the statement (its names are then left alone otherwise).
static LATE: LazyLock<Vec<Head>> = LazyLock::new(|| heads(&[(r"alter\s+view", Fam::Relation, false), (r"insert\s+into", Fam::Relation, false), (r"comment\s+on\s+task", Fam::Task, false)]));

/// The name after a head, placed as the path says, and the text with only that name changed.
async fn head(lake: &Lake, path: &Path, sql: &str, heads: &[Head]) -> Result<Option<String>> {
    for (re, fam, creating) in heads {
        let Some(m) = re.captures(sql) else { continue };
        let g = m.get(1).context("a name after the statement's head")?;
        let parts = name_parts(g.as_str());
        if matches!(parts.as_slice(), [v] if matches!(v.as_str(), "select" | "from" | "with")) {
            return Ok(Some(sql.to_string())); // (SUMMARIZE SELECT …: a query, not a name)
        }
        let w = world(lake, path, &one_name(*fam, &parts)).await?;
        return Ok(Some(match resolve(path, &w, *fam, &parts, *creating).filter(|p| !p.is_empty()) {
            Some(prefix) => format!("{}{}.{}{}", &sql[..g.start()], qualified(&prefix), g.as_str(), &sql[g.end()..]),
            None => sql.to_string(),
        }));
    }
    Ok(None)
}

/// `CREATE [OR REPLACE] [IF NOT EXISTS] TASK`: its name, its AFTER and ON FAILURE names, its WHEN
/// and its body placed as the path says (the body is kept and run later, with no session), and the
/// statement printed again. None: not a task.
async fn create_task(lake: &Lake, path: &Path, skip: &HashSet<String>, sql: &str) -> Result<Option<String>> {
    static TASK: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(?is)^\s*create\s+(?:or\s+replace\s+)?task\b").expect("a regex"));
    if !TASK.is_match(sql) {
        return Ok(None);
    }
    // (the parser is dropped before the first await: it holds a `dyn Dialect`, which is not Send)
    let (replace, quiet, name, mut task) = {
        let mut p = Parser::new(&GenericDialect {}).try_with_sql(sql)?;
        let _ = p.parse_keyword(Keyword::CREATE);
        let replace = p.parse_keywords(&[Keyword::OR, Keyword::REPLACE]);
        let _ = p.parse_keyword(Keyword::TASK);
        let quiet = p.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let (name, task) = crate::runs::task(&mut p, sql)?;
        (replace, quiet, name, task)
    };
    let mut wanted = one_name(Fam::Task, &name_parts_dotted(&name));
    for a in &task.after {
        wanted.extend(one_name(Fam::Task, &name_parts_dotted(a)));
    }
    if let Some(f) = &task.with.on_failure {
        wanted.extend(one_name(Fam::Routine, &name_parts_dotted(f)));
    }
    let w = world(lake, path, &wanted).await?;
    let place = |fam, n: &str, creating| placed(path, &w, fam, n, creating).unwrap_or_else(|| n.to_string());
    let full = place(Fam::Task, &name, true);
    task.after = task.after.iter().map(|a| place(Fam::Task, a, false)).collect();
    task.with.on_failure = task.with.on_failure.as_deref().map(|f| place(Fam::Routine, f, false));
    if let Some(when) = task.when.take() {
        task.when = Some(expr(lake, skip, &when).await?);
    }
    task.sql = Box::pin(door(lake, &task.sql, skip)).await?;
    let text = crate::objects::task_sql(&crate::objects::name_sql(&full), &task);
    let rest = text.strip_prefix("CREATE TASK ").context("a task's text")?;
    Ok(Some(format!("CREATE {}TASK {}{rest}", if replace { "OR REPLACE " } else { "" }, if quiet { "IF NOT EXISTS " } else { "" })))
}

fn name_parts_dotted(name: &str) -> Vec<String> { name.split('.').map(str::to_string).collect() }

/// A WHEN condition, placed as a query's expression is (`SELECT <condition>` goes through `door`).
async fn expr(lake: &Lake, skip: &HashSet<String>, e: &str) -> Result<String> {
    let q = Box::pin(door(lake, &format!("SELECT {e}"), skip)).await?;
    Ok(q.strip_prefix("SELECT ").map_or_else(|| e.to_string(), str::to_string))
}

/// The names a statement uses that the path may mean, each once: what `world` must look up.
fn collect(path: &Path, skip: &HashSet<String>, stmts: &mut [Statement]) -> Vec<(Fam, String)> {
    let mut walk = Walk::new(path, skip, None);
    for s in stmts.iter_mut() {
        let _ = s.visit(&mut walk);
    }
    let mut wanted = walk.wanted;
    wanted.sort();
    wanted.dedup();
    wanted
}

/// Writes the path into `stmts`. Whether anything changed.
fn apply(path: &Path, skip: &HashSet<String>, w: &World, stmts: &mut [Statement]) -> bool {
    let mut walk = Walk::new(path, skip, Some(w));
    for s in stmts.iter_mut() {
        let _ = s.visit(&mut walk);
    }
    walk.changed
}

/// What exists of `wanted`, in each place the path has it. A name in one place is looked up only
/// when there is more than one (a table is that place whether it exists or not).
async fn world(lake: &Lake, path: &Path, wanted: &[(Fam, String)]) -> Result<World> {
    let mut w = World::default();
    if path.database() != path.here {
        if let Some(db) = ddl::database(lake, path.database()).await? {
            w.schemas = ddl::schemas(&db).await?.into_iter().map(|s| (path.database().to_string(), s)).collect();
        }
    }
    let single = path.entries.len() == 1;
    for (db, schema) in &path.entries {
        let Some(l) = ddl::database(lake, db).await? else { continue };
        for (fam, n) in wanted {
            if single && matches!(fam, Fam::Relation | Fam::Task) {
                continue;
            }
            let canon = join(schema, n);
            if exists(&l, *fam, &canon).await? {
                w.names.insert((*fam, db.clone(), canon));
            }
        }
    }
    Ok(w)
}

/// Is a name of `fam` in lake `l` (in its own names: `s.t` for a table of schema `s`)?
async fn exists(l: &Lake, fam: Fam, name: &str) -> Result<bool> {
    let keys = match fam {
        Fam::Relation => vec![table_key(name), ddl::query_key(name), crate::seq::key(name), crate::index::key(name)],
        Fam::Type => vec![crate::types::key(name)],
        Fam::Task => vec![crate::runs::task_key(name)],
        Fam::Routine => return Ok(crate::routines::listed(l).await?.contains_key(name)),
    };
    for k in keys {
        if l.cat.get_raw(&k).await?.is_some() {
            return Ok(true);
        }
    }
    Ok(false)
}

/// The visitor over a statement's names. Collecting, it notes the names the path may mean; applying
/// (with the World), it writes the path into them.
struct Walk<'a> {
    path: &'a Path,
    skip: &'a HashSet<String>,
    world: Option<&'a World>, // (None: collecting)
    ctes: Vec<Vec<String>>,   // the CTE names of the queries around: `WITH t AS …` makes no table `t`
    temp: Vec<String>,        // the session's temporary tables and views, and the tables sent with the request
    wanted: Vec<(Fam, String)>,
    changed: bool,
}

impl<'a> Walk<'a> {
    fn new(path: &'a Path, skip: &'a HashSet<String>, world: Option<&'a World>) -> Walk<'a> {
        let (tables, views) = crate::temp::listed();
        let sent = crate::query::SENT.try_with(|t| t.iter().map(|(n, _)| n.clone()).collect::<Vec<_>>()).unwrap_or_default();
        let temp = tables.into_iter().map(|(n, _)| n).chain(views.into_iter().map(|(n, _)| n)).chain(sent).collect();
        Walk { path, skip, world, ctes: vec![], temp, wanted: vec![], changed: false }
    }

    /// Names a name the path never changes: a CTE, a temporary table, a table sent with the request
    /// or one a view names (for a relation), Postgres's catalog, and a file or a table as it was
    /// (`ext:`, `at:`).
    fn free(&self, parts: &[String], fam: Fam) -> bool {
        let [n] = parts else { return false };
        let shadowed = fam == Fam::Relation && (self.skip.contains(n) || self.temp.contains(n) || self.ctes.iter().any(|c| c.contains(n)));
        shadowed || n.starts_with("pg_") || n.contains(['.', '/', ':']) // (a file's or a past's name: `"ext:…"`, `"at:…"`, `"x.parquet"`)
    }

    /// A name in the statement, of `fam` (`creating`: a new one). Its path places are written in
    /// front of it when applying. Whether it was.
    fn name(&mut self, name: &mut ObjectName, fam: Fam, creating: bool) -> bool {
        if name.0.iter().any(|p| p.as_ident().is_some_and(|i| i.quote_style == Some('\''))) {
            return false; // ('data/x.csv': a file, as DuckDB reads it: `ext.rs`)
        }
        let Some(parts) = name.0.iter().map(|p| p.as_ident().map(crate::write::ident)).collect::<Option<Vec<String>>>() else { return false };
        if self.free(&parts, fam) {
            return false;
        }
        let Some(w) = self.world else {
            self.wanted.extend(one_name(fam, &parts));
            return false;
        };
        let Some(prefix) = resolve(self.path, w, fam, &parts, creating).filter(|p| !p.is_empty()) else { return false };
        name.0.splice(0..0, prefix.iter().map(|p| part(p)));
        self.changed = true;
        true
    }

    /// `t.c` (a column's comment, its sequence's owner): the table's part is placed, the column's not.
    fn column_table(&mut self, name: &mut ObjectName) {
        let n = name.0.len();
        if n < 2 {
            return;
        }
        let mut table = ObjectName(name.0[..n - 1].to_vec());
        self.name(&mut table, Fam::Relation, false);
        name.0.splice(..n - 1, table.0);
    }

    fn ty(&mut self, t: &mut DataType) {
        if let DataType::Custom(name, args) = t {
            if args.is_empty() {
                self.name(name, Fam::Type, false);
            }
        }
    }

    /// `GRANT … ON TABLE t` and the sequences, which are relations too.
    fn grants(&mut self, objects: Option<&mut GrantObjects>) {
        if let Some(GrantObjects::Tables(names) | GrantObjects::Sequences(names)) = objects {
            for n in names.iter_mut() {
                self.name(n, Fam::Relation, false);
            }
        }
    }

    /// `pondra_at(t, version => …)` (`past::syntax`'s form of `t AT (…)`): `t` is the first argument.
    fn at(&mut self, a: &mut ast::TableFunctionArgs) {
        let Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(e))) = a.args.first_mut() else { return };
        let mut name = match e {
            Expr::Identifier(i) => ObjectName::from(i.clone()),
            Expr::CompoundIdentifier(p) => ObjectName::from(p.clone()),
            _ => return,
        };
        if self.name(&mut name, Fam::Relation, false) {
            *e = Expr::CompoundIdentifier(name.0.iter().filter_map(|p| p.as_ident().cloned()).collect());
        }
    }

    /// `current_database()`, `current_catalog()` and `current_schema()` (or the bare words): the
    /// session's names, as text.
    fn current(&mut self, e: &mut Expr, word: &str) {
        if self.world.is_none() {
            return;
        }
        let value = if word == "current_schema" { self.path.schema() } else { self.path.database() };
        *e = Expr::Value(Value::SingleQuotedString(value.to_string()).into());
        self.changed = true;
    }

    /// `nextval('s')`, `currval('s')` and `setval('s', …)`: the sequence's name, a string.
    fn sequence_arg(&mut self, f: &mut ast::Function) {
        let FunctionArguments::List(l) = &mut f.args else { return };
        let Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(v)))) = l.args.first_mut() else { return };
        let Value::SingleQuotedString(s) = &mut v.value else { return };
        let parts: Vec<String> = s.split('.').map(str::to_string).collect();
        if self.free(&parts, Fam::Relation) {
            return;
        }
        match self.world {
            None => self.wanted.extend(one_name(Fam::Relation, &parts)),
            Some(w) => {
                if let Some(prefix) = resolve(self.path, w, Fam::Relation, &parts, false).filter(|p| !p.is_empty()) {
                    let full = format!("{}.{s}", qualified(&prefix));
                    *s = full;
                    self.changed = true;
                }
            }
        }
    }
}

impl VisitorMut for Walk<'_> {
    type Break = ();

    fn pre_visit_query(&mut self, q: &mut Query) -> ControlFlow<()> {
        let names = q.with.as_ref().map(|w| w.cte_tables.iter().map(|c| crate::write::ident(&c.alias.name)).collect()).unwrap_or_default();
        self.ctes.push(names);
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _: &mut Query) -> ControlFlow<()> {
        self.ctes.pop();
        ControlFlow::Continue(())
    }

    /// A table in a FROM (or an UPDATE, DELETE, MERGE's target: the same node).
    fn pre_visit_table_factor(&mut self, t: &mut TableFactor) -> ControlFlow<()> {
        if let TableFactor::Table { name, args, .. } = t {
            match args {
                None => {
                    self.name(name, Fam::Relation, false);
                }
                Some(a) if name.0.len() == 1 && crate::write::object(name) == "pondra_at" => self.at(a),
                Some(_) => {}
            }
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, e: &mut Expr) -> ControlFlow<()> {
        let word = match e {
            Expr::Function(f) => one_part(&f.name),
            Expr::Identifier(i) if i.quote_style.is_none() => Some(i.value.to_lowercase()),
            _ => None,
        };
        match (word.as_deref(), e) {
            (Some(w @ ("current_database" | "current_catalog" | "current_schema")), e) => self.current(e, w),
            (Some("nextval" | "currval" | "setval"), Expr::Function(f)) => self.sequence_arg(f),
            (_, Expr::Function(f)) => {
                self.name(&mut f.name, Fam::Routine, false);
            }
            (_, Expr::Cast { data_type, .. }) => self.ty(data_type),
            _ => {}
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_statement(&mut self, s: &mut Statement) -> ControlFlow<()> {
        match s {
            Statement::Insert(i) => {
                if let TableObject::TableName(n) = &mut i.table {
                    self.name(n, Fam::Relation, false);
                }
            }
            Statement::CreateTable(c) => {
                if !c.temporary {
                    self.name(&mut c.name, Fam::Relation, true);
                }
                for col in c.columns.iter_mut() {
                    self.ty(&mut col.data_type);
                }
                if let Some(CreateTableLikeKind::Parenthesized(l) | CreateTableLikeKind::Plain(l)) = &mut c.like {
                    self.name(&mut l.name, Fam::Relation, false);
                }
                if let Some(src) = &mut c.clone {
                    self.name(src, Fam::Relation, false);
                }
            }
            Statement::CreateView(v) => {
                if !v.temporary {
                    self.name(&mut v.name, Fam::Relation, true);
                }
            }
            Statement::CreateIndex(i) => {
                self.name(&mut i.table_name, Fam::Relation, false);
                if let Some(n) = &mut i.name {
                    self.name(n, Fam::Relation, true);
                }
            }
            Statement::Drop { object_type, names, .. } => {
                let fam = match object_type {
                    ObjectType::Type => Fam::Type,
                    ObjectType::Table | ObjectType::View | ObjectType::MaterializedView | ObjectType::Index | ObjectType::Sequence => Fam::Relation,
                    _ => return ControlFlow::Continue(()),
                };
                for n in names.iter_mut() {
                    self.name(n, fam, false);
                }
            }
            Statement::DropFunction(d) => {
                for f in d.func_desc.iter_mut() {
                    self.name(&mut f.name, Fam::Routine, false);
                }
            }
            Statement::DropProcedure { proc_desc, .. } => {
                for f in proc_desc.iter_mut() {
                    self.name(&mut f.name, Fam::Routine, false);
                }
            }
            Statement::AlterTable(a) => {
                self.name(&mut a.name, Fam::Relation, false);
            }
            Statement::AlterView { name, .. } | Statement::AlterIndex { name, .. } => {
                self.name(name, Fam::Relation, false);
            }
            Statement::AlterType(a) => {
                self.name(&mut a.name, Fam::Type, false);
            }
            Statement::Truncate(t) => {
                for x in t.table_names.iter_mut() {
                    self.name(&mut x.name, Fam::Relation, false);
                }
            }
            Statement::Comment { object_type, object_name, .. } => match object_type {
                CommentObject::Column => self.column_table(object_name),
                CommentObject::Table | CommentObject::View | CommentObject::MaterializedView | CommentObject::Sequence | CommentObject::Index => {
                    self.name(object_name, Fam::Relation, false);
                }
                CommentObject::Type => {
                    self.name(object_name, Fam::Type, false);
                }
                CommentObject::Function | CommentObject::Procedure => {
                    self.name(object_name, Fam::Routine, false);
                }
                _ => {}
            },
            Statement::CreateSequence { name, owned_by, .. } => {
                self.name(name, Fam::Relation, true);
                if let Some(o) = owned_by.as_mut().filter(|o| o.0.len() > 1) {
                    self.column_table(o);
                }
            }
            Statement::CreateType { name, .. } => {
                self.name(name, Fam::Type, true);
            }
            Statement::CreateFunction(f) => {
                self.name(&mut f.name, Fam::Routine, true);
            }
            Statement::CreateMacro { name, .. } => {
                self.name(name, Fam::Routine, true);
            }
            Statement::Call(f) => {
                self.name(&mut f.name, Fam::Routine, false);
            }
            Statement::Grant(g) => self.grants(g.objects.as_mut()),
            Statement::Revoke(r) => self.grants(r.objects.as_mut()),
            Statement::ExplainTable { table_name, .. } => {
                self.name(table_name, Fam::Relation, false);
            }
            Statement::Copy { source: CopySource::Table { table_name, .. }, .. } => {
                self.name(table_name, Fam::Relation, false);
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }
}

/// A function's name, when it has one part (the lower-case word).
fn one_part(name: &ObjectName) -> Option<String> {
    match name.0.as_slice() {
        [p] => p.as_ident().map(crate::write::ident),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn world(names: &[(Fam, &str, &str)], schemas: &[(&str, &str)]) -> World {
        World {
            names: names.iter().map(|(f, d, n)| (*f, d.to_string(), n.to_string())).collect(),
            schemas: schemas.iter().map(|(d, s)| (d.to_string(), s.to_string())).collect(),
        }
    }

    fn rewrite(path: &Path, w: &World, sql: &str) -> String {
        let mut stmts = parse_text(sql).expect("the statement parses");
        apply(path, &HashSet::new(), w, &mut stmts);
        stmts.iter().map(crate::routines::sql).collect::<Vec<_>>().join(";\n")
    }

    #[test]
    fn parses_search_paths() {
        assert_eq!(parse("lake", r#""$user", public"#), Path::plain("lake"));
        assert_eq!(parse("lake", "").entries, Path::plain("lake").entries);
        assert_eq!(parse("lake", "crm, public").entries, vec![("lake".to_string(), "crm".to_string()), ("lake".to_string(), "public".to_string())]);
        assert_eq!(parse("lake", "dev.sales").entries, vec![("dev".to_string(), "sales".to_string())]);
        assert_eq!(parse("lake", r#""My Crm""#).entries, vec![("lake".to_string(), "My Crm".to_string())]);
        assert_eq!(parse("lake", "CRM").entries, vec![("lake".to_string(), "crm".to_string())]);
    }

    #[test]
    fn prefixes() {
        let p = parse("lake", "crm, public");
        assert_eq!(p.prefix(&p.entries[0]), vec!["crm".to_string()]);
        assert!(p.prefix(&p.entries[1]).is_empty());
        assert_eq!(parse("lake", "dev.public").prefix(&("dev".into(), "public".into())), vec!["dev".to_string(), "public".to_string()]);
    }

    #[test]
    fn resolves_names() {
        let single = parse("lake", "crm");
        let none = world(&[], &[]);
        assert_eq!(resolve(&single, &none, Fam::Relation, &["t".to_string()], false), Some(vec!["crm".to_string()]));
        assert_eq!(resolve(&single, &none, Fam::Routine, &["f".to_string()], false), None); // (a routine nothing has stays as written)
        let with_f = world(&[(Fam::Routine, "lake", "crm.f")], &[]);
        assert_eq!(resolve(&single, &with_f, Fam::Routine, &["f".to_string()], false), Some(vec!["crm".to_string()]));

        let two = parse("lake", "crm, public");
        let in_second = world(&[(Fam::Relation, "lake", "t")], &[]);
        assert_eq!(resolve(&two, &in_second, Fam::Relation, &["t".to_string()], false), Some(vec![]), "public's t is the one meant");
        let in_first = world(&[(Fam::Relation, "lake", "t"), (Fam::Relation, "lake", "crm.t")], &[]);
        assert_eq!(resolve(&two, &in_first, Fam::Relation, &["t".to_string()], false), Some(vec!["crm".to_string()]));
        assert_eq!(resolve(&two, &none, Fam::Relation, &["new".to_string()], true), Some(vec!["crm".to_string()]), "a new name goes first");
        assert_eq!(resolve(&two, &none, Fam::Relation, &["gone".to_string()], false), Some(vec!["crm".to_string()]), "a missing table is named in the first place");

        let other = parse("lake", "dev.public");
        let dev = world(&[], &[("dev", "sales")]);
        assert_eq!(resolve(&other, &dev, Fam::Relation, &["sales".to_string(), "u".to_string()], false), Some(vec!["dev".to_string()]));
        assert_eq!(resolve(&other, &dev, Fam::Relation, &["s".to_string(), "u".to_string()], false), None, "not a schema of dev: it may be lake.table");
        assert_eq!(resolve(&single, &dev, Fam::Relation, &["sales".to_string(), "u".to_string()], false), None, "this lake's own: left as written");
    }

    #[test]
    fn writes_names_into_statements() {
        let crm = parse("lake", "crm");
        let none = world(&[], &[]);
        let joined = rewrite(&crm, &none, "SELECT * FROM t JOIN s.u ON t.a = u.a");
        assert!(joined.contains("FROM crm.t JOIN s.u") && joined.contains("ON t.a = u.a"), "{joined}"); // (s.u is this lake's schema s, not the path's: left as written)
        assert!(rewrite(&crm, &none, "INSERT INTO t SELECT * FROM u").starts_with("INSERT INTO crm.t SELECT * FROM crm.u"));
        assert!(rewrite(&crm, &none, "CREATE TEMP TABLE x AS SELECT * FROM t").contains("CREATE TEMPORARY TABLE x AS SELECT * FROM crm.t"));
        assert!(rewrite(&crm, &none, "WITH t AS (SELECT 1 AS a) SELECT * FROM t").contains("FROM t"), "a CTE is no table");
        let s = rewrite(&crm, &none, "SELECT nextval('s'), current_schema(), current_database()");
        assert!(s.contains("'crm.s'") && s.contains("'crm'") && s.contains("'lake'"), "{s}");
        assert!(rewrite(&crm, &none, "SELECT * FROM pondra_at(t, version => 3)").contains("pondra_at(crm.t"));
        assert!(rewrite(&crm, &none, "SELECT * FROM pg_class").contains("FROM pg_class"));
        assert!(rewrite(&crm, &none, "SELECT * FROM 'data/x.csv' JOIN t USING (a)").contains("FROM 'data/x.csv' JOIN crm.t"), "a file is no table");
        let f = world(&[(Fam::Routine, "lake", "crm.f"), (Fam::Type, "lake", "crm.mood")], &[]);
        let s = rewrite(&crm, &f, "SELECT f(1), upper('x'), 'sad'::mood");
        assert!(s.contains("crm.f(1)") && s.contains("upper('x')") && s.contains("crm.mood"), "{s}");
        let two = parse("lake", "crm, public");
        let in_public = world(&[(Fam::Relation, "lake", "t")], &[]);
        assert_eq!(rewrite(&two, &in_public, "SELECT * FROM t"), "SELECT * FROM t");
    }

    #[test]
    fn names_a_statement_puts_by_their_use() {
        let crm = parse("lake", "crm");
        let none = world(&[], &[]);
        assert!(rewrite(&crm, &none, "CALL run('etl/orders.sql')").contains("run("), "run is no routine of the session's");
        assert!(rewrite(&crm, &none, "DROP TABLE t").contains("DROP TABLE crm.t"));
        assert!(rewrite(&crm, &none, "COMMENT ON COLUMN t.a IS 'x'").contains("crm.t.a"));
        assert!(rewrite(&crm, &none, "TRUNCATE TABLE t").contains("TRUNCATE TABLE crm.t"));
    }

    #[test]
    fn shows_the_path() {
        assert_eq!(show("show search_path;").as_deref(), Some("SELECT 'public' AS search_path")); // (no session here: public)
        assert_eq!(show("SHOW datafusion.execution.batch_size"), None);
    }

    #[test]
    fn sets_schemas_with_set_schema() {
        assert_eq!(set_schema("SET SCHEMA 'crm';"), Some("crm".to_string()));
        assert_eq!(set_schema("set schema dev.sales"), Some("dev.sales".to_string()));
        assert_eq!(set_schema("SET search_path TO crm"), None);
        assert!(is_use("  use crm"));
        assert!(!is_use("SELECT 1"));
        assert_eq!(text_of("lake", &[("lake".to_string(), "crm".to_string())]), "crm");
        assert_eq!(text_of("lake", &[("dev".to_string(), "public".to_string())]), "dev.public");
    }
}
