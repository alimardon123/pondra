//! Plan and deploy (ADR-047 §4): a project, files that say what every object is, made true in a
//! database; and a database written out as such files (`export`).
//!
//! - **A project** is files: `pondra.toml` (its name, and each environment's values and attached
//!   catalogs), `objects/**/*.sql` (definitions in any layout, several to a file), `migrations/*.sql`
//!   (one-off steps, each run once in each database, in name order, after what is added and before
//!   what is replaced: a backfill finds its new column, a rename makes a table match again) and
//!   `tests/*.sql` (queries: any row back is a failure). The command line sends the folder;
//!   `CALL plan('files/sales')` reads one kept in the workspace.
//! - **The plan** compares each object the project declares with the database: its fingerprint (its
//!   statement's tokens, comments, spacing and case aside, with the values bound into it) against
//!   what the last deploy recorded, and against the object as the database has it now (`export`'s
//!   statement for it). A table is compared column by column: columns added at the end become
//!   `ADD COLUMN`, widened ones `ALTER COLUMN … TYPE`, changed options `ALTER TABLE … SET`; a column
//!   gone, renamed, moved or narrowed, or another key, is refused with the migration to write.
//!   Anything else that changed is made again (`CREATE OR REPLACE`), a materialized view with what
//!   follows it. An object no longer declared is kept, unless the deploy prunes.
//! - **A deploy** applies the plan it was shown (refused if the database changed since), one at a
//!   time per database (`dl`, kept fresh while it runs), each statement as the caller under its own
//!   part of the deploy's job, then the tests. Each is an entry `dp/<n>` (`pondra.deploys`), each
//!   migration once `dm/<project>/<file>`, and its files are kept under `files/.deploys/<n>/`. Run
//!   again, a deploy finds nothing to do: that is also how one that stopped half way is finished.
//! - **One owner per object**: a deploy refuses to change an object another project's deploy made.
//!   In a branch, the tasks a deploy makes start suspended, as the branch's own did (ADR-047 §3).

use crate::objects::{ident, materialized_sql, name_sql as quoted, routine_sql, sql_type, table_sql, task_sql}; // (`SHOW CREATE`'s statements: an export reads as it does)
use crate::routines::{Outcome, Who};
use crate::server::App;
use crate::store::{json, Lake, TableMeta};
use anyhow::{bail, ensure, Context, Result};
use datafusion::sql::sqlparser::{dialect::GenericDialect, tokenizer::{Token, Tokenizer}};
use serde::{Deserialize, Serialize};
use serde_json::{json as j, Value};
use std::collections::{BTreeMap, HashMap};

const LOCK: &str = "dl"; // (the deploy under way: its number and when it last said it was alive)
const STALE_MS: u64 = 120_000;
fn record_key(n: u64) -> String { format!("dp/{n:010}") }
fn migration_key(project: &str, file: &str) -> String { format!("dm/{project}/{file}") }

/// What is asked of a project: `POST /plan`, `/deploy` and `/test` (`CALL plan`, `CALL deploy`).
#[derive(Clone, Copy, PartialEq)]
pub enum Verb {
    Plan,
    Deploy,
    Test,
}

/// A deploy, plan or test asked for: the project's files, as the command line sends them.
#[derive(Deserialize, Default)]
pub struct Ask {
    pub files: BTreeMap<String, String>, // its path in the project → its text
    #[serde(default)]
    pub env: Option<String>, // whose `[env.…]` values and attached catalogs
    #[serde(default)]
    pub commit: Option<String>, // git's, when the project is in git
    #[serde(default)]
    pub secrets: BTreeMap<String, String>, // `$name` values from the deploying machine: bound, never kept
    #[serde(default)]
    pub test: bool, // a deploy's: its tests after it
    #[serde(default)]
    pub prune: bool, // drop what the project no longer declares
    #[serde(default)]
    pub plan: Option<String>, // a deploy's: the plan it showed, refused if that is no longer the plan
}

#[derive(Deserialize, Default)]
struct Toml {
    #[serde(default)]
    project: Section,
    #[serde(default)]
    env: BTreeMap<String, Env>,
}

#[derive(Deserialize, Default)]
struct Section {
    #[serde(default)]
    name: String,
}

#[derive(Deserialize, Default)]
struct Env {
    #[serde(default)]
    values: BTreeMap<String, toml::Value>,
    #[serde(default)]
    attach: BTreeMap<String, Attach>,
}

#[derive(Deserialize, Clone)]
struct Attach {
    url: String,
    #[serde(default, rename = "type")]
    kind: Option<String>,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
enum Kind {
    Schema,
    Role,
    Secret,
    Attach,
    Table,
    Function,
    Macro,
    View,
    Materialized,
    Procedure,
    Task,
    Grant,
}

impl Kind {
    fn word(self) -> &'static str {
        match self {
            Kind::Schema => "schema",
            Kind::Role => "role",
            Kind::Secret => "secret",
            Kind::Attach => "attach",
            Kind::Table => "table",
            Kind::Function => "function",
            Kind::Macro => "macro",
            Kind::View => "view",
            Kind::Materialized => "materialized view",
            Kind::Procedure => "procedure",
            Kind::Task => "task",
            Kind::Grant => "grant",
        }
    }

    fn of(word: &str) -> Option<Kind> {
        [Kind::Schema, Kind::Role, Kind::Secret, Kind::Attach, Kind::Table, Kind::Function, Kind::Macro, Kind::View, Kind::Materialized, Kind::Procedure, Kind::Task, Kind::Grant].into_iter().find(|k| k.word() == word)
    }

    /// The order things are made in: what others name first.
    fn rank(self) -> u8 {
        match self {
            Kind::Schema => 0,
            Kind::Role => 1,
            Kind::Secret | Kind::Attach => 2,
            Kind::Table => 3,
            Kind::Function | Kind::Macro => 4,
            Kind::View | Kind::Materialized => 5,
            Kind::Procedure => 6,
            Kind::Task => 7,
            Kind::Grant => 8,
        }
    }

    /// The statement that takes one away (`--prune`).
    fn drop(self, name: &str) -> String {
        match self {
            Kind::Grant => revoke(&format!("GRANT {name}")),
            Kind::Attach => format!("DETACH {}", quoted(name)),
            k => format!("DROP {} {}", k.word().to_uppercase(), quoted(name)),
        }
    }
}

/// An object a project declares: its statement as written, and its fingerprint.
#[derive(Clone)]
struct Declared {
    kind: Kind,
    name: String,
    sql: String,
    file: String,
    print: String,
}

impl Declared {
    fn key(&self) -> String { key(self.kind, &self.name) }
}

fn key(kind: Kind, name: &str) -> String { format!("{} {name}", kind.word()) }

/// What a project's files say, for one environment.
struct Project {
    name: String,
    objects: Vec<Declared>,
    migrations: Vec<(String, String)>, // (file, text), in name order
    tests: Vec<(String, String)>,
    values: HashMap<String, Value>,
}

fn project(ask: &Ask) -> Result<Project> {
    let toml: Toml = match ask.files.get("pondra.toml") {
        Some(t) => toml::from_str(t).context("pondra.toml")?,
        None => Toml::default(),
    };
    ensure!(!toml.project.name.is_empty(), "pondra.toml says the project's name: [project] name = \"sales\" (pondra init writes one)");
    ensure!(toml.project.name.chars().all(|c| c.is_alphanumeric() || "_-".contains(c)), "a project's name is letters, digits, _ and -");
    let env = match &ask.env {
        Some(e) => toml.env.get(e),
        None => None,
    };
    let values: HashMap<String, Value> = env.map(|e| e.values.iter().map(|(k, v)| Ok((k.clone(), serde_json::to_value(v)?))).collect::<Result<_>>()).transpose()?.unwrap_or_default();
    let mut objects = vec![];
    for (name, a) in env.map(|e| e.attach.clone()).unwrap_or_default() {
        let sql = match &a.kind {
            Some(t) => format!("ATTACH '{}' AS {} (TYPE {t})", a.url.replace('\'', "''"), quoted(&name)),
            None => format!("ATTACH '{}' AS {}", a.url.replace('\'', "''"), quoted(&name)),
        };
        objects.push(Declared { kind: Kind::Attach, name: name.clone(), print: fingerprint(&sql, &values, &ask.secrets), sql, file: "pondra.toml".into() });
    }
    let mut seen: HashMap<String, String> = HashMap::new();
    for (path, text) in &ask.files {
        if !path.starts_with("objects/") || !path.ends_with(".sql") {
            continue;
        }
        for sql in crate::routines::split(text) {
            let Some((kind, name)) = head(&sql).with_context(|| format!("{path}: {}", first_line(&sql)))? else { continue };
            let d = Declared { kind, name, print: fingerprint(&sql, &values, &ask.secrets), sql: sql.trim().trim_end_matches(';').trim_end().to_string(), file: path.clone() };
            if let Some(other) = seen.insert(d.key(), path.clone()) {
                bail!("{} is declared twice: in {other} and in {path}", d.key());
            }
            objects.push(d);
        }
    }
    let of = |dir: &str| ask.files.iter().filter(|(p, _)| p.starts_with(dir) && p.ends_with(".sql") && !p[dir.len()..].contains('/')).map(|(p, t)| (p[dir.len()..].to_string(), t.clone())).collect::<Vec<_>>();
    Ok(Project { name: toml.project.name, objects: ordered(objects), migrations: of("migrations/"), tests: of("tests/"), values })
}

fn first_line(sql: &str) -> String { sql.trim().lines().next().unwrap_or_default().chars().take(80).collect() }

/// What a statement in `objects/` makes, and its name inside the database. None: nothing (a
/// comment alone).
const SHARES: &str = "shares and recipients are each environment's: prod's never reach its branches, so a partner's token never reaches dev; make and grant them in the database (CREATE SHARE …)";

fn head(sql: &str) -> Result<Option<(Kind, String)>> {
    let Ok(tokens) = Tokenizer::new(&GenericDialect {}, sql).tokenize() else { bail!("not SQL the plan can read") };
    let solid: Vec<&Token> = tokens.iter().filter(|t| !matches!(t, Token::Whitespace(_))).collect();
    if solid.is_empty() {
        return Ok(None);
    }
    let word = |i: usize| match solid.get(i) {
        Some(Token::Word(w)) if w.quote_style.is_none() => w.value.to_lowercase(),
        _ => String::new(),
    };
    let mut i = 0;
    match word(0).as_str() {
        "grant" => {
            ensure!(!(0..solid.len()).any(|i| matches!((word(i).as_str(), word(i + 1).as_str()), ("on", "share") | ("to", "recipient"))), "{SHARES}");
            return Ok(Some((Kind::Grant, sql.trim().trim_end_matches(';')[5..].split_whitespace().collect::<Vec<_>>().join(" ")))); // (named by what it grants: `SELECT ON TABLE t TO analyst`)
        }
        "create" => i += 1,
        "attach" => bail!("an attached lake or catalog is each environment's: [env.prod] attach.events = {{ type = \"kafka\", url = \"…\" }} in pondra.toml"),
        _ => bail!("objects/ holds what objects are (CREATE …, GRANT …); rows and one-off changes go in migrations/"),
    }
    if word(i) == "or" && word(i + 1) == "replace" {
        i += 2;
    }
    ensure!(!matches!(word(i).as_str(), "temp" | "temporary"), "a temporary object is a session's, not a project's");
    let kind = match (word(i).as_str(), word(i + 1).as_str()) {
        ("materialized", "view") => {
            i += 1;
            Kind::Materialized
        }
        ("external", "table") => {
            i += 1;
            Kind::View // (a view of files: `CREATE EXTERNAL TABLE`)
        }
        ("schema", _) => Kind::Schema,
        ("role", _) => Kind::Role,
        ("secret", _) => Kind::Secret,
        ("table", _) => Kind::Table,
        ("view", _) => Kind::View,
        ("function", _) => Kind::Function,
        ("macro", _) => Kind::Macro,
        ("procedure", _) => Kind::Procedure,
        ("task", _) => Kind::Task,
        ("share" | "recipient", _) => bail!("{SHARES}"),
        ("user", _) => bail!("users are each environment's: a project makes roles (CREATE ROLE analyst), and an environment's admin grants them (GRANT analyst TO ann)"),
        (w, _) => bail!("CREATE {}: a project declares schemas, tables, views, materialized views, functions, macros, procedures, tasks, secrets, roles and grants", w.to_uppercase()),
    };
    i += 1;
    if word(i) == "if" && word(i + 1) == "not" && word(i + 2) == "exists" {
        i += 3;
    }
    let mut parts = vec![];
    loop {
        match solid.get(i) {
            Some(Token::Word(w)) => parts.push(if w.quote_style.is_some() { w.value.clone() } else { w.value.to_lowercase() }),
            _ => bail!("its name is missing"),
        }
        match solid.get(i + 1) {
            Some(Token::Period) => i += 2,
            _ => break,
        }
    }
    let name = match &parts[..] {
        [t] => t.clone(),
        [s, t] if kind != Kind::Schema => crate::ddl::join(s, t),
        _ => bail!("{}: a project names objects without their database (sales.orders), as each environment is one", parts.join(".")),
    };
    Ok(Some((kind, name)))
}

/// A statement's fingerprint: its tokens, comments, spacing, case, `OR REPLACE`, `IF NOT EXISTS` and
/// a last `;` aside, with each `$name` the value it will be given (a secret's as its hash, so the
/// fingerprint never holds it).
fn fingerprint(sql: &str, values: &HashMap<String, Value>, secrets: &BTreeMap<String, String>) -> String {
    let Ok(tokens) = Tokenizer::new(&GenericDialect {}, sql).tokenize() else {
        return crate::users::sha256(&sql.split_whitespace().collect::<Vec<_>>().join(" ").to_lowercase());
    };
    let mut out: Vec<String> = vec![];
    for t in tokens.iter().filter(|t| !matches!(t, Token::Whitespace(_))) {
        out.push(match t {
            Token::Word(w) if w.quote_style.is_none() => w.value.to_lowercase(),
            Token::Placeholder(p) => {
                let name = p.trim_start_matches('$');
                match (secrets.get(name), values.get(name)) {
                    (Some(s), _) => format!("'secret:{}'", crate::users::sha256(s)),
                    (_, Some(v)) => v.to_string(),
                    _ => p.clone(),
                }
            }
            t => t.to_string(),
        });
    }
    while out.last().is_some_and(|t| t == ";") {
        out.pop();
    }
    let mut text = out.join(" ");
    for (from, to) in [("create or replace ", "create "), (" if not exists ", " ")] {
        text = text.replacen(from, to, 1);
    }
    crate::users::sha256(&text)
}

/// Each kind in its turn; views and materialized views after those they read (as written when
/// they don't name each other).
fn ordered(mut all: Vec<Declared>) -> Vec<Declared> {
    all.sort_by_key(|d| d.kind.rank()); // (stable: as written within a kind)
    let (mut out, mut views): (Vec<Declared>, Vec<Declared>) = all.into_iter().partition(|d| d.kind.rank() != Kind::View.rank());
    let at = out.iter().position(|d| d.kind.rank() > Kind::View.rank()).unwrap_or(out.len());
    let mut done: Vec<Declared> = vec![];
    while !views.is_empty() {
        let names: Vec<String> = views.iter().map(|v| v.name.clone()).collect();
        let ready = views.iter().position(|v| !names.iter().any(|n| *n != v.name && mentions(&v.sql, n))).unwrap_or(0); // (a loop: as written)
        done.push(views.remove(ready));
    }
    out.splice(at..at, done);
    out
}

/// Does `sql` name `name` (as a whole word, its schema too)?
fn mentions(sql: &str, name: &str) -> bool {
    let (s, t) = crate::ddl::split(name);
    let lower = sql.to_lowercase();
    let whole = |n: &str| lower.match_indices(n).any(|(i, _)| {
        let before = lower[..i].chars().next_back();
        let after = lower[i + n.len()..].chars().next();
        !before.is_some_and(|c| c.is_alphanumeric() || c == '_' || c == '.') && !after.is_some_and(|c| c.is_alphanumeric() || c == '_')
    });
    if s == crate::ddl::PUBLIC { whole(t) } else { whole(&format!("{s}.{t}")) }
}

/// `GRANT … TO r` → `REVOKE … FROM r`.
fn revoke(sql: &str) -> String {
    let at = sql.to_lowercase().rfind(" to ").unwrap_or(sql.len());
    let body = sql.trim()[5..at.min(sql.trim().len())].trim();
    format!("REVOKE {body} FROM {}", sql[at..].trim()[2..].trim())
}

// ---------------------------------------------------------------- the database as files

/// An object as the database has it: its statement as `export` writes it.
struct Current {
    sql: String,
    meta: Option<TableMeta>,
}

/// Every object of the lake a project could declare, by its key, as the statement that makes it.
async fn current(lake: &Lake) -> Result<BTreeMap<String, Current>> {
    let mut out = BTreeMap::new();
    let mut put = |kind: Kind, name: &str, sql: String, meta: Option<TableMeta>| {
        out.insert(key(kind, name), Current { sql, meta });
    };
    for s in crate::ddl::schemas(lake).await?.into_iter().filter(|s| s != crate::ddl::PUBLIC) {
        put(Kind::Schema, &s, format!("CREATE SCHEMA {}", ident(&s)), None);
    }
    let materialized: BTreeMap<String, crate::views::View> = lake.cat.scan::<crate::views::View>("v/", "v0").await?.into_iter().map(|(k, v)| (k[2..].to_string(), v)).collect();
    for (k, meta) in lake.cat.scan::<TableMeta>("t/", "t0").await? {
        let name = &k[2..];
        let made = |n: &str| materialized.contains_key(n) || n.strip_suffix("_final").is_some_and(|v| materialized.contains_key(v));
        if crate::sys::hidden(name) || made(name) || meta.ext.is_some() || meta.outside.is_some() {
            continue;
        }
        let m = meta.logical();
        put(Kind::Table, name, table_sql(&quoted(name), &m), Some(m));
    }
    for (name, v) in &materialized {
        let meta = lake.cat.get::<TableMeta>(&crate::store::table_key(name)).await?;
        put(Kind::Materialized, name, materialized_sql(&quoted(name), v, meta.as_ref()), None);
    }
    for (k, v) in lake.cat.scan::<crate::ddl::StoredView>("q/", "q0").await? {
        let name = &k[2..];
        put(Kind::View, name, format!("CREATE VIEW {} AS\n{}", quoted(name), v.sql.trim()), None);
    }
    for (name, r) in crate::routines::listed(lake).await?.iter() {
        let kind = match r.what() {
            "procedure" => Kind::Procedure,
            "macro" => Kind::Macro,
            _ => Kind::Function,
        };
        put(kind, name, routine_sql(&quoted(name), r), None);
    }
    for (name, t) in crate::runs::tasks(lake).await?.iter() {
        put(Kind::Task, name, task_sql(&quoted(name), t), None);
    }
    for (k, u) in lake.cat.scan::<crate::users::User>("u/", "u0").await? {
        let name = &k[2..];
        if u.login {
            continue; // (a user is the environment's, not the project's)
        }
        put(Kind::Role, name, format!("CREATE ROLE {}", ident(name)), None);
    }
    Ok(out)
}

/// The grants each role has, as statements (never compared: a grant is the project's once a
/// deploy made it, and making it again changes nothing).
async fn grants(lake: &Lake) -> Result<Vec<String>> {
    use crate::users::On;
    let mut out = vec![];
    for (k, u) in lake.cat.scan::<crate::users::User>("u/", "u0").await? {
        if u.login {
            continue;
        }
        for g in &u.grants {
            let columns = if g.columns.is_empty() { String::new() } else { format!(" ({})", g.columns.iter().map(|c| ident(c)).collect::<Vec<_>>().join(", ")) };
            let on = match &g.on {
                On::Table(t) => format!("TABLE {}", quoted(t)),
                On::Schema(s) => format!("ALL TABLES IN SCHEMA {}", ident(s)),
                On::Lake => "ALL TABLES".into(),
                On::Secret(s) => format!("SECRET {}", ident(s)),
            };
            out.push(format!("GRANT {}{columns} ON {on} TO {}", g.privilege.to_uppercase(), ident(&k[2..])));
        }
    }
    Ok(out)
}

fn text(s: &str) -> String { format!("'{}'", s.replace('\'', "''")) }

/// The lake's objects as a project's files (`pondra export`): `pondra.toml`, a file per schema's
/// object under `objects/`, roles and their grants in `objects/access.sql`. Secrets stay out (their
/// values are the environment's), and so do users.
pub async fn export(lake: &Lake) -> Result<BTreeMap<String, String>> {
    let now = current(lake).await?;
    let mut files: BTreeMap<String, String> = BTreeMap::new();
    let mut add = |path: String, sql: &str| {
        let f = files.entry(path).or_default();
        if !f.is_empty() {
            f.push('\n');
        }
        f.push_str(sql.trim_end());
        f.push_str(";\n");
    };
    for (k, c) in &now {
        let (word, name) = k.split_once(' ').map(|(w, n)| if w == "materialized" { ("materialized view", &n[5..]) } else { (w, n) }).unwrap_or(("", k));
        let path = match Kind::of(word) {
            Some(Kind::Schema) => "objects/schemas.sql".to_string(),
            Some(Kind::Role) => "objects/access.sql".to_string(),
            _ => {
                let (s, t) = crate::ddl::split(name);
                if s == crate::ddl::PUBLIC { format!("objects/{t}.sql") } else { format!("objects/{s}/{t}.sql") }
            }
        };
        add(path, &c.sql);
    }
    for g in grants(lake).await? {
        add("objects/access.sql".into(), &g);
    }
    let name = crate::ddl::lake_name(lake);
    let mut toml = format!("[project]\nname = \"{name}\"\n# server = \"https://pondra.example.com\"   # where the databases are (pondra serve --lakes)\n\n[env.{name}]\n# values = {{ min_order = 10 }}             # $name values its statements are given\n");
    for (n, a) in lake.cat.scan::<crate::ddl::Attachment>("a/", "a0").await? {
        toml.push_str(&format!("attach.{} = {{ url = {} }}\n", &n[2..], serde_json::to_string(&a.dir)?));
    }
    files.insert("pondra.toml".into(), toml);
    Ok(files)
}

// ---------------------------------------------------------------- the plan

/// One line of a plan: what happens to an object, and the statements that do it.
#[derive(Serialize, Clone, Debug)]
pub struct Step {
    pub mark: &'static str, // + made, ~ changed, ↻ made again, ▶ a migration, - dropped, ! kept or refused
    pub change: &'static str, // the same as a word to filter by: create, alter, replace, rebuild, migrate, drop, keep, refuse
    pub kind: String,
    pub name: String,
    pub what: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub sql: Vec<String>,
}

/// A step's mark as a word (`!` here: refused until the migrations have run).
fn change_of(mark: &str, kind: Kind) -> &'static str {
    match mark {
        "+" => "create",
        "~" if kind == Kind::Table => "alter",
        "~" => "replace",
        "↻" => "rebuild",
        "▶" => "migrate",
        "-" => "drop",
        _ => "refuse",
    }
}

#[derive(Serialize)]
pub struct Plan {
    pub id: String,
    pub after: u64, // the deploy it follows (0: none)
    pub steps: Vec<Step>,
    pub refused: Vec<String>,
    pub tests: usize,
    #[serde(skip)]
    objects: BTreeMap<String, String>, // key → fingerprint, for the record
}

/// A deploy as it is kept (`dp/<n>`): who, which commit, the plan, each step, the tests.
#[derive(Serialize, Deserialize, Clone, Default)]
pub struct Record {
    pub n: u64,
    pub project: String,
    #[serde(default)]
    pub env: Option<String>,
    #[serde(default)]
    pub commit: Option<String>,
    pub who: String,
    pub started_ms: u64,
    #[serde(default)]
    pub ended_ms: Option<u64>,
    pub status: String, // running, ok, failed, tests_failed (as pondra.runs': one word each)
    #[serde(default)]
    pub steps: Vec<Value>,
    #[serde(default)]
    pub tests: Vec<Value>,
    #[serde(default)]
    pub error: Option<String>,
    /// Each object it left declared: its fingerprint as written, and as the database has it after.
    #[serde(default)]
    pub objects: BTreeMap<String, (String, String)>,
}

/// The deploy entries, newest last.
async fn records(lake: &Lake) -> Result<Vec<Record>> { Ok(lake.cat.scan::<Record>("dp/", "dp0").await?.into_iter().map(|(_, r)| r).collect()) }

/// The plan for `p` here, as the database is now.
async fn plan(lake: &Lake, p: &Project, prune: bool, branch: bool) -> Result<Plan> {
    let all = records(lake).await?;
    let after = all.last().map_or(0, |r| r.n);
    // What each project's last finished deploy left declared, and who owns what.
    let mut last: BTreeMap<String, &Record> = BTreeMap::new();
    for r in all.iter().filter(|r| r.status == "ok" || r.status == "tests_failed") { // (what a finished deploy left declared)
        last.insert(r.project.clone(), r);
    }
    let mine: BTreeMap<String, (String, String)> = last.get(&p.name).map(|r| r.objects.clone()).unwrap_or_default();
    let owner: HashMap<&String, &String> = last.iter().filter(|(n, _)| **n != p.name).flat_map(|(n, r)| r.objects.keys().map(move |k| (k, n))).collect();
    let now = current(lake).await?;
    let done: Vec<String> = lake.cat.scan::<u64>(&format!("dm/{}/", p.name), &format!("dm/{}0", p.name)).await?.into_iter().map(|(k, _)| k[4 + p.name.len()..].to_string()).collect();
    let (mut steps, mut refused) = (vec![], vec![]);
    let pending: Vec<&(String, String)> = p.migrations.iter().filter(|(f, _)| !done.contains(f)).collect();
    let print = |sql: &str| fingerprint(sql, &HashMap::new(), &BTreeMap::new());
    let mut rebuilt: Vec<String> = vec![];
    for d in &p.objects {
        let k = d.key();
        if let Some(o) = owner.get(&k) {
            refused.push(format!("{k} is project {o}'s: a deploy changes only its own project's objects"));
            continue;
        }
        let step = |mark, what: &str, sql: Vec<String>| Step { mark, change: change_of(mark, d.kind), kind: d.kind.word().into(), name: d.name.clone(), what: what.into(), sql };
        let Some(cur) = now.get(&k) else {
            // (grants, secrets and attachments aren't read back: compared with what the last deploy made)
            match (d.kind, mine.get(&k)) {
                (Kind::Grant, Some(_)) => continue,
                (Kind::Attach | Kind::Secret, Some((was, _))) if *was == d.print => continue,
                (Kind::Attach, Some(_)) => {
                    steps.push(step("~", "replaced", vec![d.kind.drop(&d.name), d.sql.clone()]));
                    continue;
                }
                _ => {}
            }
            let mut sql = vec![if d.kind == Kind::Secret { or_replace(&d.sql) } else { d.sql.clone() }];
            if d.kind == Kind::Task && branch {
                sql.push(format!("ALTER TASK {} SUSPEND", quoted(&d.name))); // (a branch's tasks start suspended)
            }
            steps.push(step("+", "made", sql));
            continue;
        };
        if d.kind == Kind::Table {
            match table_change(lake, d, cur.meta.as_ref().expect("a table's meta")).await {
                Ok(sql) if sql.is_empty() => {}
                Ok(sql) => steps.push(step("~", &sql.iter().map(|s| s.splitn(4, ' ').nth(3).unwrap_or_default()).collect::<Vec<_>>().join("; "), sql)), // (ALTER TABLE t ADD COLUMN c: ADD COLUMN c)
                Err(e) if !pending.is_empty() => steps.push(step("!", &format!("{e:#} (checked again after the migrations)"), vec![])),
                Err(e) => refused.push(format!("table {} ({}): {e:#}", d.name, d.file)),
            }
            continue;
        }
        let (was, then) = mine.get(&k).cloned().unwrap_or_default();
        let same = match mine.contains_key(&k) {
            true => was == d.print && then == print(&cur.sql),
            false => print(&cur.sql) == d.print,
        };
        if same || matches!(d.kind, Kind::Schema | Kind::Role) {
            continue;
        }
        let drift = mine.contains_key(&k) && was == d.print;
        let what = if drift { "changed outside a deploy: made as the project says" } else { "replaced" };
        match d.kind {
            Kind::Materialized => {
                // Made again from the rows already there, with every view that follows it.
                let follow: Vec<&Declared> = followers(lake, &p.objects, &d.name).await?;
                if let Some(missing) = follow.iter().find(|f| f.kind != Kind::Materialized) {
                    refused.push(format!("{} follows {} and isn't in the project as a materialized view", missing.name, d.name));
                    continue;
                }
                let fresh: Vec<&Declared> = follow.iter().copied().filter(|f| !rebuilt.contains(&f.name)).collect();
                let mut sql: Vec<String> = fresh.iter().rev().map(|f| format!("DROP MATERIALIZED VIEW IF EXISTS {}", quoted(&f.name))).collect();
                sql.push(format!("DROP MATERIALIZED VIEW IF EXISTS {}", quoted(&d.name)));
                sql.push(d.sql.clone());
                steps.push(step("↻", &format!("{what}, from the rows already there"), vec![]));
                for f in &fresh {
                    sql.push(f.sql.clone());
                    rebuilt.push(f.name.clone());
                    steps.push(Step { mark: "↻", change: "rebuild", kind: f.kind.word().into(), name: f.name.clone(), what: format!("made again with {}", d.name), sql: vec![] });
                }
                rebuilt.push(d.name.clone());
                let at = steps.len() - fresh.len() - 1;
                steps[at].sql = sql; // (the view's step runs them all, in order)
            }
            _ if rebuilt.contains(&d.name) => {}
            Kind::Attach => steps.push(step("~", what, vec![d.kind.drop(&d.name), d.sql.clone()])),
            _ => {
                let mut sql = vec![or_replace(&d.sql)];
                if d.kind == Kind::Task && branch {
                    sql.push(format!("ALTER TASK {} SUSPEND", quoted(&d.name)));
                }
                steps.push(step("~", what, sql));
            }
        }
    }
    let declared: Vec<String> = p.objects.iter().map(Declared::key).collect();
    let mut gone: Vec<Step> = vec![];
    for k in mine.keys().filter(|k| !declared.contains(k)) {
        let (word, name) = split_key(k);
        let Some(kind) = Kind::of(word) else { continue };
        gone.push(match prune {
            true => Step { mark: "-", change: "drop", kind: word.into(), name: name.into(), what: "dropped: no longer in the project".into(), sql: vec![kind.drop(name)] },
            false => Step { mark: "!", change: "keep", kind: word.into(), name: name.into(), what: "no longer in the project: kept (prune drops it)".into(), sql: vec![] },
        });
    }
    gone.reverse(); // (what was made last goes first)
    steps.extend(gone);
    // In the order they run: what is added (objects made, tables' columns), then the migrations
    // (a backfill finds its column, a first deploy's rows their table), then what is replaced or
    // dropped (planned again after them: a rename makes a table match its declaration).
    let (mut first, rest): (Vec<Step>, Vec<Step>) = steps.into_iter().partition(|s| s.mark == "+" || s.mark == "~" && s.kind == "table");
    first.extend(pending.iter().map(|(f, text)| Step { mark: "▶", change: "migrate", kind: "migration".into(), name: f.clone(), what: "runs once".into(), sql: vec![text.clone()] }));
    first.extend(rest);
    let steps = first;
    let objects: BTreeMap<String, String> = p.objects.iter().map(|d| (d.key(), d.print.clone())).collect();
    let shown = serde_json::to_string(&(&steps, &refused, after))?;
    Ok(Plan { id: crate::users::sha256(&shown)[..16].to_string(), after, steps, refused, tests: p.tests.len(), objects })
}

fn split_key(k: &str) -> (&str, &str) {
    match k.strip_prefix("materialized view ") {
        Some(n) => ("materialized view", n),
        None => k.split_once(' ').unwrap_or((k, "")),
    }
}

/// `CREATE OR REPLACE …` for a statement that may not say it.
fn or_replace(sql: &str) -> String {
    let t = sql.trim_start();
    if t.len() > 6 && t[..6].eq_ignore_ascii_case("create") && !t[6..].trim_start().to_lowercase().starts_with("or replace") {
        format!("CREATE OR REPLACE{}", &t[6..])
    } else {
        sql.to_string()
    }
}

/// The project's materialized views that follow `name` here, in the order they're made.
async fn followers<'a>(lake: &Lake, objects: &'a [Declared], name: &str) -> Result<Vec<&'a Declared>> {
    let views: BTreeMap<String, crate::views::View> = lake.cat.scan::<crate::views::View>("v/", "v0").await?.into_iter().map(|(k, v)| (k[2..].to_string(), v)).collect();
    let mut set = vec![name.to_string()];
    loop {
        let more: Vec<String> = views.iter().filter(|(n, v)| !set.contains(n) && set.iter().any(|s| v.follows(s))).map(|(n, _)| n.clone()).collect();
        if more.is_empty() {
            break;
        }
        set.extend(more);
    }
    let mut out = vec![];
    for n in &set[1..] {
        match objects.iter().find(|d| d.name == *n && matches!(d.kind, Kind::Materialized | Kind::View)) {
            Some(d) => out.push(d),
            None => bail!("{n} follows {name} and isn't in the project: declare it, or drop it first"),
        }
    }
    out.sort_by_key(|d| objects.iter().position(|o| o.name == d.name));
    Ok(out)
}

/// What makes the table as declared from the table as it is: columns added at the end, widened
/// types, options; or why not.
async fn table_change(lake: &Lake, d: &Declared, m: &TableMeta) -> Result<Vec<String>> {
    let Some(crate::write::Stmt::Create(c)) = crate::write::parse(&d.sql) else { bail!("not a CREATE TABLE Pondra reads") };
    if c.query.is_some() {
        return Ok(vec![]); // (CREATE TABLE … AS: made once, from its query)
    }
    let spec: Value = serde_json::from_str(&crate::write::create_spec(&c, lake, false).await?)?;
    let pairs = |v: &Value| -> Vec<(String, String)> { serde_json::from_value(v.clone()).unwrap_or_default() };
    let (columns, opts) = match spec.is_array() {
        true => (pairs(&spec), Value::Null),
        false => (pairs(&spec["columns"]), spec.clone()),
    };
    let declared: Vec<(String, String, String)> = columns.iter().filter(|(n, _)| n != "_deleted").map(|(n, t)| Ok((n.clone(), crate::query::type_name(&crate::query::dtype(t)?), sql_type(t)))).collect::<Result<_>>()?;
    let have: Vec<&(String, String)> = m.columns.iter().filter(|(c, _)| !crate::sys::NAMES.contains(&c.as_str()) && c != "_deleted").collect();
    let names = |v: &mut dyn Iterator<Item = &String>| v.cloned().collect::<Vec<_>>().join(", ");
    // (ALTER TABLE … RENAME, DROP and ALTER COLUMN are refused while views read the table: a
    // migration drops them first, and the deploy makes them again after it)
    let readers = crate::ddl::readers(lake, &d.name).await?;
    let first = if readers.is_empty() { String::new() } else { format!(", after dropping what reads it ({}): the deploy makes them again", readers.join(", ")) };
    ensure!(declared.len() >= have.len() && have.iter().zip(&declared).all(|((a, _), (b, ..))| a == b),
        "its columns are ({}) here and ({}) in the project: a column renamed, dropped or moved is a migration's (ALTER TABLE {} RENAME COLUMN … TO …, DROP COLUMN …{first})",
        names(&mut have.iter().map(|(c, _)| c)), names(&mut declared.iter().map(|(c, ..)| c)), quoted(&d.name));
    let mut sql = vec![];
    for ((c, old), (_, new, written)) in have.iter().zip(&declared) {
        if old != new {
            let widens = crate::ddl::widens(&crate::query::dtype(old)?, &crate::query::dtype(new)?);
            ensure!(widens, "{c} is {} here and {written} in the project: a type only widens in place; a migration can copy it into a new column", sql_type(old));
            ensure!(readers.is_empty(), "{c} widens to {written}: a migration's (ALTER TABLE {} ALTER COLUMN {} TYPE {written}{first})", quoted(&d.name), ident(c));
            sql.push(format!("ALTER TABLE {} ALTER COLUMN {} TYPE {written}", quoted(&d.name), ident(c)));
        }
    }
    for (c, _, written) in &declared[have.len()..] {
        sql.push(format!("ALTER TABLE {} ADD COLUMN {} {written}", quoted(&d.name), ident(c)));
    }
    let list = |v: &Value| -> Vec<String> { serde_json::from_value(v.clone()).unwrap_or_default() };
    let key: Vec<String> = list(&opts["key"]);
    ensure!(key == m.key, "its key is ({}) here and ({}) in the project: another key is another table (make it, and move the rows in a migration)", m.key.join(", "), key.join(", "));
    let partition = opts["partition_by"].as_str().map(String::from);
    ensure!(partition == m.partition, "its partition_by can't change: each file holds one partition (make a new table in a migration)");
    let merge: BTreeMap<String, String> = serde_json::from_value(opts["merge"].clone()).unwrap_or_default();
    ensure!(merge == m.merge, "its merge functions can't change in place");
    let not_null: Vec<String> = list(&opts["not_null"]).into_iter().chain(key.iter().cloned()).collect();
    let mut nn_have: Vec<&String> = m.not_null.iter().collect();
    let mut nn_want: Vec<&String> = m.columns.iter().map(|(c, _)| c).filter(|c| not_null.contains(c)).collect();
    nn_have.sort();
    nn_want.sort();
    nn_want.dedup();
    let added: Vec<&String> = declared[have.len()..].iter().map(|(c, ..)| c).collect();
    ensure!(nn_have == nn_want || nn_want.iter().all(|c| nn_have.contains(c) || added.contains(c)) && nn_have.iter().all(|c| nn_want.contains(c)), "its NOT NULL columns differ: a migration's (a new table, the rows moved)");
    let checks: Vec<(String, String)> = serde_json::from_value(opts["checks"].clone()).unwrap_or_default();
    ensure!(checks == m.checks, "its CHECKs differ: a migration's (a new table, the rows moved)");
    let defaults: BTreeMap<String, String> = serde_json::from_value(opts["defaults"].clone()).unwrap_or_default();
    ensure!(defaults.iter().filter(|(c, _)| !added.contains(c)).all(|(c, v)| m.defaults.get(c) == Some(v)) && m.defaults.keys().all(|c| defaults.contains_key(c)), "its DEFAULTs differ: a migration's");
    let mut set = vec![];
    let publish = opts.get("publish").filter(|v| !v.is_null()).map(list).unwrap_or_else(crate::store::default_publish);
    if publish != m.publish {
        set.push(format!("publish = {}", text(&publish.join(","))));
    }
    let cluster = list(&opts["cluster_by"]);
    if cluster != m.cluster {
        set.push(format!("cluster_by = {}", text(&cluster.join(", "))));
    }
    if let Some(t) = opts["ttl"].as_str().filter(|t| Some(t.to_string()) != m.ttl.as_ref().map(|(c, s)| format!("{c}:{s}"))) {
        set.push(format!("ttl = {}", text(t)));
    }
    if let Some(o) = opts["order_by"].as_str().filter(|o| Some(o.to_string()) != m.order) {
        set.push(format!("order_by = {}", text(o)));
    }
    if let Some(r) = opts["retention"].as_str().filter(|r| crate::ddl::retention(r).ok() != m.retention_secs) {
        set.push(format!("retention = {}", text(r)));
    }
    if !set.is_empty() {
        sql.push(format!("ALTER TABLE {} SET ({})", quoted(&d.name), set.join(", ")));
    }
    Ok(sql)
}

// ---------------------------------------------------------------- the deploy

/// `POST /deploy`: the plan (`apply` false), the deploy, or the tests alone, of the project sent.
pub async fn ask(app: &App, verb: Verb, ask: Ask, who: Who) -> Result<Value> {
    ensure!(verb != Verb::Deploy || who.role >= crate::auth::Role::Admin, "a deploy changes what the database is: it needs an admin token (a plan or a test runs as you are)");
    let p = project(&ask)?;
    if verb == Verb::Test {
        let tests = tests(app, &p, who).await?;
        let ok = tests.iter().all(|t| t["ok"] == true);
        return Ok(j!({"tests": tests, "ok": ok}));
    }
    let branch = app.lake.cat.get_raw(crate::branch::BASES).await?.is_some();
    let plan = plan(&app.lake, &p, ask.prune, branch).await?;
    if verb == Verb::Plan {
        return Ok(j!({"plan": plan, "project": p.name, "ok": plan.refused.is_empty()}));
    }
    if let Some(shown) = &ask.plan {
        ensure!(*shown == plan.id, "the database changed since that plan (someone deployed, or changed what the project makes): plan again");
    }
    ensure!(plan.refused.is_empty(), "the plan refuses: {}", plan.refused.join("; "));
    let who_name = crate::auth::current().map(|p| p.name).filter(|n| !n.is_empty()).unwrap_or_else(|| format!("{:?}", who.role).to_lowercase());
    let mut record = Record { project: p.name.clone(), env: ask.env.clone(), commit: ask.commit.clone(), who: who_name, started_ms: crate::log::now_ms(), status: "running".into(), ..Default::default() };
    let claimed = crate::write::on_node_as(app, crate::write::Stmt::Ddl(vec![crate::ddl::Ddl::Deploy { claim: true, after: plan.after, record: json(&record) }]), None, false).await?;
    record.n = claimed["deploy"].as_u64().context("a deploy number")?;
    keep_files(&app.lake, record.n, &ask.files).await?;
    let alive = tokio::spawn(beat(app.clone(), record.n));
    let out = apply(app, &p, &plan, &mut record, who, ask.test, &ask.secrets).await;
    alive.abort();
    if let Err(e) = &out {
        record.status = "failed".into();
        record.error = Some(crate::ext::said(e));
    }
    record.ended_ms = Some(crate::log::now_ms());
    crate::write::on_node_as(app, crate::write::Stmt::Ddl(vec![crate::ddl::Ddl::Deploy { claim: false, after: record.n, record: json(&record) }]), None, false).await?;
    out?;
    Ok(j!({"deploy": record.n, "status": record.status, "steps": record.steps, "tests": record.tests, "ok": record.status == "ok"}))
}

/// The deploy's statements, then its record of what it left declared, then the tests.
async fn apply(app: &App, p: &Project, plan: &Plan, record: &mut Record, who: Who, test: bool, secrets: &BTreeMap<String, String>) -> Result<()> {
    let mut values: HashMap<String, Value> = p.values.clone();
    values.extend(secrets.iter().map(|(k, v)| (k.clone(), Value::String(v.clone()))));
    let job = format!("deploy:{}:{}", p.name, record.n);
    let none = HashMap::new();
    // What is added, then the migrations, then the rest: planned again against what they left.
    let ran = plan.steps.iter().position(|s| s.mark == "▶");
    let added: Vec<&Step> = plan.steps.iter().take(ran.unwrap_or(0)).collect();
    for (i, s) in added.iter().enumerate() {
        for (j, sql) in s.sql.iter().enumerate() {
            crate::routines::script(app, sql, &values, &none, who, Some(format!("{job}:a{i}.{j}"))).await.with_context(|| format!("{} {}: {}", s.kind, s.name, first_line(sql)))?;
        }
        record.steps.push(j!({"mark": s.mark, "kind": s.kind, "name": s.name, "what": s.what}));
    }
    let mut steps = plan.steps.clone();
    let migrations: Vec<Step> = steps.iter().filter(|s| s.mark == "▶").cloned().collect();
    for m in &migrations {
        crate::routines::script(app, &m.sql[0], &values, &none, who, Some(format!("migration:{}:{}", p.name, m.name))).await.with_context(|| format!("migration {}", m.name))?;
        crate::write::on_node_as(app, crate::write::Stmt::Ddl(vec![crate::ddl::Ddl::Deploy { claim: false, after: record.n, record: json(&j!({"migration": migration_key(&p.name, &m.name)})) }]), None, false).await?;
        record.steps.push(j!({"mark": m.mark, "kind": m.kind, "name": m.name, "what": "ran"}));
    }
    if !migrations.is_empty() {
        let branch = app.lake.cat.get_raw(crate::branch::BASES).await?.is_some();
        let again = plan_again(&app.lake, p, plan, branch).await?;
        ensure!(again.refused.is_empty(), "after the migrations, the plan refuses: {}", again.refused.join("; "));
        steps = again.steps;
    }
    for (i, s) in steps.iter().filter(|s| s.mark != "▶" && s.mark != "!").enumerate() {
        for (j, sql) in s.sql.iter().enumerate() {
            crate::routines::script(app, sql, &values, &none, who, Some(format!("{job}:b{i}.{j}"))).await.with_context(|| format!("{} {}: {}", s.kind, s.name, first_line(sql)))?;
        }
        record.steps.push(j!({"mark": s.mark, "kind": s.kind, "name": s.name, "what": s.what}));
    }
    let now = current(&app.lake).await?;
    let print = |sql: &str| fingerprint(sql, &HashMap::new(), &BTreeMap::new());
    record.objects = plan.objects.iter().map(|(k, f)| (k.clone(), (f.clone(), now.get(k).map(|c| print(&c.sql)).unwrap_or_default()))).collect();
    record.status = "ok".into();
    if test {
        record.tests = tests(app, p, who).await?;
        if record.tests.iter().any(|t| t["ok"] != true) {
            record.status = "tests_failed".into();
        }
    }
    Ok(())
}

/// The plan after the migrations ran: what remains of the one shown.
async fn plan_again(lake: &Lake, p: &Project, shown: &Plan, branch: bool) -> Result<Plan> {
    let mut again = plan(lake, p, shown.steps.iter().any(|s| s.mark == "-"), branch).await?;
    again.steps.retain(|s| s.mark != "▶");
    Ok(again)
}

/// Every test: its query's rows (none is a pass), as the caller.
async fn tests(app: &App, p: &Project, who: Who) -> Result<Vec<Value>> {
    let (mut out, none) = (vec![], HashMap::new());
    for (file, sql) in &p.tests {
        let got = crate::routines::script(app, sql, &p.values, &none, who, None).await;
        out.push(match got {
            Ok(Outcome::Rows(rows)) => {
                let n: usize = rows.iter().map(|b| b.num_rows()).sum();
                let first = rows_json(&rows, 3).unwrap_or_default();
                match n {
                    0 => j!({"test": file, "ok": true}),
                    n => j!({"test": file, "ok": false, "rows": n, "first": first}),
                }
            }
            Ok(Outcome::Done(_)) => j!({"test": file, "ok": false, "error": "a test is a query: its rows are the failures"}),
            Err(e) => j!({"test": file, "ok": false, "error": crate::ext::said(&e)}),
        });
    }
    Ok(out)
}

/// The project as deployed, under `files/.deploys/<n>/`, so the database says which code it runs.
async fn keep_files(lake: &Lake, n: u64, files: &BTreeMap<String, String>) -> Result<()> {
    for (p, t) in files {
        match lake.put(&format!("{}{n}/{p}", crate::files::DEPLOYS), t.as_bytes().to_vec()).await {
            Err(e) if format!("{e:#}").contains("already exists") => {} // (a retried deploy's)
            done => done?,
        }
    }
    Ok(())
}

/// While a deploy runs, it says so every 30 s: a deploy whose word is older than two minutes
/// stopped (its node went), and the next may start.
async fn beat(app: App, n: u64) {
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(30)).await;
        let alive = Record { n, status: "alive".into(), started_ms: crate::log::now_ms(), ..Default::default() };
        let _ = crate::write::on_node_as(&app, crate::write::Stmt::Ddl(vec![crate::ddl::Ddl::Deploy { claim: false, after: n, record: json(&alive) }]), None, false).await;
    }
}

#[derive(Serialize, Deserialize)]
struct Held {
    n: u64,
    alive_ms: u64,
}

/// Leader, under the lake's lock: a deploy's entry. A claim (`claim`) takes the next number when
/// the newest deploy is still `after` and none is under way; else it writes the record, a
/// migration done, or that the deploy under way is alive.
pub async fn keep(lake: &Lake, claim: bool, after: u64, record: &[u8]) -> Result<Value> {
    let now = crate::log::now_ms();
    let v: Value = serde_json::from_slice(record)?;
    if let Some(k) = v["migration"].as_str() {
        ensure!(k.starts_with("dm/"), "not a migration's entry");
        lake.cat.commit(vec![(k.to_string(), json(&now))], &[]).await?;
        return Ok(j!({"migration": k}));
    }
    let mut r: Record = serde_json::from_value(v)?;
    if r.status == "alive" {
        lake.cat.commit(vec![(LOCK.into(), json(&Held { n: r.n, alive_ms: now }))], &[]).await?;
        return Ok(j!({"deploy": r.n}));
    }
    if !claim {
        let done = r.status != "running";
        let held: Option<Held> = lake.cat.get(LOCK).await?;
        let free: Vec<String> = if done && held.is_some_and(|h| h.n == r.n) { vec![LOCK.into()] } else { vec![] };
        lake.cat.commit(vec![(record_key(r.n), json(&r))], &free).await?;
        return Ok(j!({"deploy": r.n}));
    }
    let all = records(lake).await?;
    let newest = all.last().map_or(0, |r| r.n);
    if let Some(h) = lake.cat.get::<Held>(LOCK).await? {
        ensure!(now.saturating_sub(h.alive_ms) > STALE_MS, "deploy {} is under way: wait for it to end", h.n);
    }
    ensure!(newest == after, "deploy {newest} ran since that plan: plan again");
    let mut puts = vec![];
    for mut old in all.into_iter().filter(|r| r.status == "running") {
        old.status = "stopped".into(); // (its node went before it ended: what it did is done)
        puts.push((record_key(old.n), json(&old)));
    }
    r.n = newest + 1;
    puts.push((record_key(r.n), json(&r)));
    puts.push((LOCK.into(), json(&Held { n: r.n, alive_ms: now })));
    lake.cat.commit(puts, &[]).await?;
    Ok(j!({"deploy": r.n}))
}

/// `pondra.deploys`: every deploy, newest last.
pub async fn table(lake: &Lake) -> Result<datafusion::arrow::array::RecordBatch> {
    use datafusion::arrow::array::{ArrayRef, Int64Array, StringArray, TimestampMicrosecondArray};
    use std::sync::Arc;
    let all = records(lake).await?;
    let s = |f: &dyn Fn(&Record) -> Option<String>| Arc::new(all.iter().map(f).collect::<StringArray>()) as ArrayRef;
    let at = |f: &dyn Fn(&Record) -> Option<u64>| Arc::new(all.iter().map(|r| f(r).map(|ms| ms as i64 * 1000)).collect::<TimestampMicrosecondArray>().with_timezone("UTC")) as ArrayRef;
    Ok(datafusion::arrow::array::RecordBatch::try_from_iter(vec![
        ("id", Arc::new(all.iter().map(|r| Some(r.n as i64)).collect::<Int64Array>()) as ArrayRef),
        ("project", s(&|r| Some(r.project.clone()))),
        ("env", s(&|r| r.env.clone())),
        ("commit", s(&|r| r.commit.clone())),
        ("caller", s(&|r| Some(r.who.clone()))),
        ("started", at(&|r| Some(r.started_ms))),
        ("ended", at(&|r| r.ended_ms)),
        ("status", s(&|r| Some(r.status.clone()))),
        ("steps", s(&|r| Some(serde_json::to_string(&r.steps).unwrap_or_default()))),
        ("tests", s(&|r| Some(serde_json::to_string(&r.tests).unwrap_or_default()))),
        ("error", s(&|r| r.error.clone())),
    ])?)
}

// ---------------------------------------------------------------- CALL plan(…), CALL deploy(…)

/// The first `n` rows as JSON objects.
fn rows_json(rows: &[datafusion::arrow::array::RecordBatch], n: usize) -> Result<Vec<Value>> {
    let mut w = datafusion::arrow::json::ArrayWriter::new(Vec::new());
    w.write_batches(&rows.iter().collect::<Vec<_>>())?;
    w.finish()?;
    let all: Vec<Value> = serde_json::from_slice(&w.into_inner()).unwrap_or_default();
    Ok(all.into_iter().take(n).collect())
}

/// Pondra's own procedures for a project kept in the workspace.
pub fn is_own(name: &str) -> bool { matches!(name.to_ascii_lowercase().as_str(), "plan" | "deploy" | "pondra.plan" | "pondra.deploy") }

/// `CALL plan('files/sales', env => 'prod')`, `CALL deploy('files/sales', env => 'prod', test => true,
/// prune => false)`: the plan's lines, or the deploy's steps, as rows.
pub async fn call(app: &App, name: &str, args: &[datafusion::sql::sqlparser::ast::FunctionArg], who: Who) -> Result<Outcome> {
    use datafusion::sql::sqlparser::ast::{FunctionArg, FunctionArgExpr};
    let apply = name.to_ascii_lowercase().ends_with("deploy");
    let verb = if apply { Verb::Deploy } else { Verb::Plan };
    let mut select = vec![];
    for (i, a) in args.iter().enumerate() {
        select.push(match a {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) if i == 0 => format!("CAST(({e}) AS VARCHAR) AS folder"),
            FunctionArg::Named { name, arg: FunctionArgExpr::Expr(e), .. } if ["env", "test", "prune", "commit"].contains(&crate::write::ident(name).as_str()) => format!("({e}) AS {}", crate::write::ident(name)),
            _ => bail!("{name}('files/sales', env => 'prod'{}): the project's folder in the workspace, then env (and test, prune) by name", if apply { ", test => true" } else { "" }),
        });
    }
    ensure!(!select.is_empty(), "{name}: the project's folder in the workspace ('files/sales')");
    let rows = app.query(&format!("SELECT {}", select.join(", ")), None).await?;
    let v = rows_json(&rows, 1)?.into_iter().next().unwrap_or_default();
    let folder = crate::files::under_files(v["folder"].as_str().unwrap_or_default());
    let folder = format!("{}/", folder.trim_end_matches('/'));
    let mut files = BTreeMap::new();
    use futures::TryStreamExt;
    let listed: Vec<object_store::ObjectMeta> = app.lake.store.list(Some(&object_store::path::Path::from(folder.as_str()))).try_collect().await?;
    for path in listed.into_iter().map(|o| o.location.to_string()) {
        let rel = path[folder.len()..].to_string();
        if rel.split('/').any(|p| p.starts_with('.')) || !(rel == "pondra.toml" || rel.ends_with(".sql")) {
            continue;
        }
        files.insert(rel, String::from_utf8(app.lake.object(&path).await?.to_vec()).with_context(|| format!("{path} isn't text"))?);
    }
    ensure!(files.contains_key("pondra.toml"), "{folder} holds no pondra.toml: a project's folder (pondra export, or pondra init)");
    let a = Ask { files, env: v["env"].as_str().map(String::from), commit: v["commit"].as_str().map(String::from), test: v["test"] == true, prune: v["prune"] == true, ..Default::default() };
    let out = ask(app, verb, a, who).await?;
    let steps: Vec<Value> = match apply {
        true => out["steps"].as_array().cloned().unwrap_or_default(),
        false => {
            let mut s = out["plan"]["steps"].as_array().cloned().unwrap_or_default();
            s.extend(out["plan"]["refused"].as_array().into_iter().flatten().map(|r| j!({"mark": "!", "change": "refuse", "kind": "refused", "name": "", "what": r})));
            s
        }
    };
    use datafusion::arrow::array::{ArrayRef, StringArray};
    use std::sync::Arc;
    let col = |k: &str| Arc::new(steps.iter().map(|s| s[k].as_str().map(String::from)).collect::<StringArray>()) as ArrayRef;
    let batch = datafusion::arrow::array::RecordBatch::try_from_iter(vec![("mark", col("mark")), ("change", col("change")), ("kind", col("kind")), ("name", col("name")), ("what", col("what"))])?;
    Ok(Outcome::Rows(vec![batch]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn heads() {
        let h = |s: &str| head(s).unwrap().unwrap();
        assert_eq!(h("CREATE TABLE Sales.Orders (id BIGINT)"), (Kind::Table, "sales.orders".into()));
        assert_eq!(h("create or replace materialized view public.big as select 1"), (Kind::Materialized, "big".into()));
        assert_eq!(h("-- the rate\nCREATE MACRO IF NOT EXISTS \"Rate\"(x) AS x * 2"), (Kind::Macro, "Rate".into()));
        assert_eq!(h("CREATE EXTERNAL TABLE logs STORED AS PARQUET LOCATION 'x/'"), (Kind::View, "logs".into()));
        assert_eq!(h("GRANT SELECT ON TABLE t TO analyst").0, Kind::Grant);
        assert!(head("INSERT INTO t VALUES (1)").unwrap_err().to_string().contains("migrations/"));
        assert!(head("CREATE USER ann").unwrap_err().to_string().contains("environment's"));
        assert!(head("CREATE SHARE acme").unwrap_err().to_string().contains("shares and recipients"));
        assert!(head("CREATE RECIPIENT acme_corp").unwrap_err().to_string().contains("shares and recipients"));
        assert!(head("GRANT SELECT ON SHARE acme TO RECIPIENT acme_corp").unwrap_err().to_string().contains("shares and recipients"));
        assert!(head("CREATE TABLE prod.sales.orders (id INT)").unwrap_err().to_string().contains("without their database"));
        assert!(head("-- only a comment").unwrap().is_none());
    }

    #[test]
    fn fingerprints() {
        let none = (HashMap::new(), BTreeMap::new());
        let f = |s: &str| fingerprint(s, &none.0, &none.1);
        assert_eq!(f("CREATE TABLE t (a INT)"), f("create table T(\n  a int -- the a\n);"));
        assert_eq!(f("CREATE OR REPLACE VIEW v AS SELECT 1"), f("CREATE VIEW v AS SELECT 1"));
        assert_ne!(f("CREATE VIEW v AS SELECT 1"), f("CREATE VIEW v AS SELECT 2"));
        assert_ne!(f("CREATE VIEW v AS SELECT 'a'"), f("CREATE VIEW v AS SELECT 'A'"));
        let values = HashMap::from([("min".to_string(), j!(10))]);
        assert_ne!(fingerprint("CREATE VIEW v AS SELECT * FROM t WHERE a > $min", &values, &none.1), fingerprint("CREATE VIEW v AS SELECT * FROM t WHERE a > $min", &HashMap::from([("min".to_string(), j!(20))]), &none.1));
    }

    #[test]
    fn order() {
        let d = |kind, name: &str, sql: &str| Declared { kind, name: name.into(), sql: sql.into(), file: String::new(), print: String::new() };
        let all = ordered(vec![d(Kind::Task, "t", ""), d(Kind::Materialized, "b", "SELECT * FROM a"), d(Kind::Materialized, "a", "SELECT * FROM orders"), d(Kind::Table, "orders", ""), d(Kind::Schema, "s", "")]);
        assert_eq!(all.iter().map(|d| d.name.as_str()).collect::<Vec<_>>(), ["s", "orders", "a", "b", "t"]);
        assert_eq!(revoke("GRANT SELECT ON TABLE t TO analyst"), "REVOKE SELECT ON TABLE t FROM analyst");
        assert_eq!(or_replace("CREATE VIEW v AS SELECT 1"), "CREATE OR REPLACE VIEW v AS SELECT 1");
        assert_eq!(quoted("sales.order"), "sales.\"order\"");
    }
}
