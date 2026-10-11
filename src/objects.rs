//! Every kind of object a lake holds, in one registry (ADR-049), with the same verbs for each.
//! `pondra.objects` lists them all, with their comments and the statements that make them;
//! `SHOW CREATE <kind> <name>` gives those statements; `COMMENT ON <kind> <name> IS '…'` describes
//! one; `CREATE OR ALTER TABLE` makes a table, or brings the one there to its definition (what a
//! project's files run again and again). `GET /kinds` and `pondra.kinds` say what kinds there are:
//! parts (`PARTS`) and patterns (`PATTERNS`) are listed beside the kinds, and `pondra.kinds` lists all three.
//!
//! A new kind is an entry in `KINDS`, and its family's lister in `FAMILIES`: nothing else needs
//! editing for it to be listed, described and shown. Comments are kept apart (`cm/{family}/{name}`,
//! `cm/column/{table}/{stored column}`, a view's column by its name), and follow a rename or go with a drop (`follow`, from
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
/// and a view can't share a name), and what it is listed and described by. The rest is what code
/// used to keep in lists of its own: its catalog prefix, what GRANT gives on it, what CREATE DATABASE
/// … CLONE does with it, whether a project may declare it, and its line for the glossary.
pub struct Kind {
    pub name: &'static str,
    pub family: &'static str,
    pub verbs: &'static [&'static str],
    /// Its catalog prefix, which its entries are keyed by (`t/` for a table).
    pub prefix: &'static str,
    /// What GRANT gives on it: none is an admin's alone.
    pub privileges: &'static [&'static str],
    pub on_clone: OnClone,
    /// None: a project's `objects/` may declare it. Some: refused, saying why.
    pub project: Option<&'static str>,
    /// Has parts never shown (a secret's values, a user's password).
    pub secret: bool,
    /// One line: what it is.
    pub about: &'static str,
    /// Other products' names for it.
    pub also: &'static [&'static str],
}

/// What CREATE DATABASE … CLONE does with an object: its entries are copied; its files are pinned
/// where they are (the base's files are listed, not copied); or it is left out, since it is the base's
/// own and a branch never takes it.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum OnClone {
    Copy,
    Pin,
    Leave,
}

impl OnClone {
    pub fn word(self) -> &'static str {
        match self {
            OnClone::Copy => "copy",
            OnClone::Pin => "pin",
            OnClone::Leave => "leave",
        }
    }
}

/// Something stored inside one object, living and dying with it (AGENTS.md: every new concept is an
/// object, a part or a pattern).
pub struct Part {
    pub name: &'static str,
    pub inside: &'static [&'static str],
    pub about: &'static str,
    pub also: &'static [&'static str],
}

/// A way of using objects and parts together: nothing stored, read from what exists; `lists` names
/// what lists it.
pub struct Pattern {
    pub name: &'static str,
    pub lists: &'static str,
    pub about: &'static str,
    pub also: &'static [&'static str],
}

const RELATION: &[&str] = &["CREATE", "CREATE OR ALTER", "CREATE OR REPLACE", "ALTER", "DROP", "COMMENT ON", "SHOW CREATE"];
const ROUTINE: &[&str] = &["CREATE", "CREATE OR REPLACE", "DROP", "COMMENT ON", "SHOW CREATE"];
const READ: &[&str] = &["SELECT"];
const ROWS: &[&str] = &["SELECT", "INSERT", "UPDATE", "DELETE"];
/// A project's refusal for shares and recipients (apply.rs says it): each environment's, so a
/// partner's token never reaches dev.
const SHARES_PROJECT: &str = "shares and recipients are each environment's: prod's never reach its branches, so a partner's token never reaches dev; make and grant them in the database (CREATE SHARE …)";
pub static KINDS: &[Kind] = &[
    Kind { name: "schema", family: "schema", verbs: &["CREATE", "DROP", "COMMENT ON", "SHOW CREATE"], prefix: "ns/", privileges: &["SELECT", "INSERT", "UPDATE", "DELETE", "CLONE"], on_clone: OnClone::Copy, project: None, secret: false, about: "a namespace for a database's objects, named `schema.name`", also: &["namespace", "dataset"] },
    Kind { name: "table", family: "relation", verbs: &["CREATE", "CREATE OR ALTER", "CREATE OR REPLACE", "ALTER", "DROP", "UNDROP", "CLONE", "COMMENT ON", "SHOW CREATE"], prefix: "t/", privileges: ROWS, on_clone: OnClone::Pin, project: None, secret: false, about: "rows in the lake's files and log, queried and changed with SQL", also: &["relation"] },
    Kind { name: "view", family: "relation", verbs: RELATION, prefix: "q/", privileges: READ, on_clone: OnClone::Copy, project: None, secret: false, about: "a stored query with a name, run where it is used", also: &["virtual table"] },
    Kind { name: "materialized view", family: "relation", verbs: &["CREATE", "CREATE OR REPLACE", "ALTER", "DROP", "COMMENT ON", "SHOW CREATE"], prefix: "v/", privileges: READ, on_clone: OnClone::Copy, project: None, secret: false, about: "a query's result kept current as its tables change, read like a table", also: &["dynamic table", "live table", "continuous query"] },
    Kind { name: "external table", family: "relation", verbs: &["CREATE", "CREATE OR REPLACE", "DROP", "COMMENT ON"], prefix: "q/", privileges: READ, on_clone: OnClone::Copy, project: None, secret: false, about: "a view of files outside the lake, read in place where they are", also: &["foreign table"] },
    Kind { name: "sequence", family: "relation", verbs: &["CREATE", "CREATE OR REPLACE", "ALTER", "DROP", "COMMENT ON", "SHOW CREATE"], prefix: "sq/", privileges: &[], on_clone: OnClone::Copy, project: Some("sequences aren't declared in a project yet: make them in a migration (migrations/…)"), secret: false, about: "numbers handed out one at a time, never twice (`nextval`)", also: &["serial", "auto increment"] },
    Kind { name: "index", family: "relation", verbs: &["CREATE", "ALTER", "DROP", "COMMENT ON", "SHOW CREATE"], prefix: "ix/", privileges: &[], on_clone: OnClone::Copy, project: Some("indexes aren't declared in a project yet: make them in a migration (migrations/…)"), secret: false, about: "a named set of a table's columns, kept as a definition: nothing is built", also: &[] },
    Kind { name: "type", family: "type", verbs: &["CREATE", "ALTER", "DROP", "COMMENT ON", "SHOW CREATE"], prefix: "ty/", privileges: &[], on_clone: OnClone::Copy, project: Some("types aren't declared in a project yet: make them in a migration (migrations/…)"), secret: false, about: "a named set of labels a column may hold", also: &["enum"] },
    Kind { name: "function", family: "routine", verbs: ROUTINE, prefix: "r/", privileges: &[], on_clone: OnClone::Copy, project: None, secret: false, about: "a named computation, in SQL or Python, run per row, per batch or in a query", also: &["UDF", "user-defined function"] },
    Kind { name: "macro", family: "routine", verbs: ROUTINE, prefix: "r/", privileges: &[], on_clone: OnClone::Copy, project: None, secret: false, about: "a named SQL expression or query, expanded where it is written", also: &["SQL macro"] },
    Kind { name: "table function", family: "routine", verbs: ROUTINE, prefix: "r/", privileges: &[], on_clone: OnClone::Copy, project: None, secret: false, about: "a function that returns the rows of a table, used where a table is named", also: &["UDTF"] },
    Kind { name: "procedure", family: "routine", verbs: &["CREATE", "CREATE OR REPLACE", "CALL", "DROP", "COMMENT ON", "SHOW CREATE"], prefix: "r/", privileges: &[], on_clone: OnClone::Copy, project: None, secret: false, about: "a named piece of work, run with CALL as its caller", also: &["stored procedure"] },
    Kind { name: "task", family: "task", verbs: &["CREATE", "CREATE OR REPLACE", "ALTER", "EXECUTE", "DROP", "COMMENT ON", "SHOW CREATE"], prefix: "j/", privileges: &[], on_clone: OnClone::Copy, project: None, secret: false, about: "a statement run on a schedule, or after other tasks (AFTER)", also: &["job", "schedule", "cron"] },
    Kind { name: "secret", family: "secret", verbs: &["CREATE", "CREATE OR REPLACE", "DROP", "COMMENT ON"], prefix: "e/", privileges: &["USAGE"], on_clone: OnClone::Leave, project: None, secret: true, about: "credentials sealed in the lake, handed to a procedure and never shown", also: &["credential", "connection"] },
    Kind { name: "user", family: "user", verbs: &["CREATE", "ALTER", "DROP", "GRANT", "REVOKE", "COMMENT ON"], prefix: "u/", privileges: &[], on_clone: OnClone::Copy, project: Some("users are each environment's: a project makes roles (CREATE ROLE analyst), and an environment's admin grants them (GRANT analyst TO ann)"), secret: true, about: "a person who signs in, with a password or token, and grants", also: &["login"] },
    Kind { name: "role", family: "user", verbs: &["CREATE", "DROP", "GRANT", "REVOKE", "COMMENT ON", "SHOW CREATE"], prefix: "u/", privileges: &[], on_clone: OnClone::Copy, project: None, secret: false, about: "a group of grants, given to users", also: &["group"] },
    Kind { name: "database", family: "database", verbs: &["CREATE", "CLONE", "ATTACH", "DETACH", "DROP", "COMMENT ON", "SHOW CREATE"], prefix: "a/", privileges: &["CLONE"], on_clone: OnClone::Copy, project: Some("a database is each environment's: an attached lake or catalog goes in pondra.toml ([env.prod] attach.events = { type = \"kafka\", url = \"…\" })"), secret: false, about: "another lake this one reads, attached by name, or a branch cloned from one", also: &["catalog", "lake", "workspace"] },
    Kind { name: "share", family: "share", verbs: &["CREATE", "ALTER", "DROP", "GRANT", "REVOKE", "COMMENT ON", "SHOW CREATE"], prefix: "sh/", privileges: READ, on_clone: OnClone::Leave, project: Some(SHARES_PROJECT), secret: false, about: "tables handed to other companies at published versions, through links that end", also: &["Delta Share", "data share"] },
    Kind { name: "recipient", family: "recipient", verbs: &["CREATE", "ALTER", "DROP", "COMMENT ON"], prefix: "sr/", privileges: &[], on_clone: OnClone::Leave, project: Some(SHARES_PROJECT), secret: false, about: "a company that a share is granted to, with the token it reads with", also: &["data consumer"] },
];

/// The parts: stored inside one object, and listed beside the kinds.
pub static PARTS: &[Part] = &[
    Part { name: "column", inside: &["table", "view", "materialized view", "external table"], about: "a named, typed field of every row", also: &["field", "attribute"] },
    Part { name: "key", inside: &["table"], about: "PRIMARY KEY or UNIQUE: the columns that name a row", also: &["primary key", "identifier"] },
    Part { name: "link", inside: &["table"], about: "FOREIGN KEY … NOT ENFORCED: a fact that one table's rows name another's", also: &["foreign key", "relationship", "reference"] },
    Part { name: "check", inside: &["table"], about: "CHECK: a condition every row written must meet", also: &["constraint"] },
    Part { name: "parameter", inside: &["function", "macro", "table function", "procedure"], about: "an argument a routine takes, with its type and default", also: &["argument"] },
    Part { name: "label", inside: &["type"], about: "one value an enum type allows", also: &["enum value"] },
    Part { name: "expectation", inside: &["materialized view"], about: "a condition a view's new rows are counted against, and kept, dropped or failed by", also: &["data quality check", "assertion"] },
];

/// The patterns: ways of using objects and parts together, nothing stored, listed beside the kinds.
pub static PATTERNS: &[Pattern] = &[
    Pattern { name: "flow", lists: "pondra.flows", about: "materialized views of views, moved in one commit", also: &["pipeline", "DAG"] },
    Pattern { name: "branch", lists: "pondra.databases", about: "a database cloned from another with no data copied, and refreshed from it", also: &["zero-copy clone", "fork", "dev environment"] },
    Pattern { name: "task graph", lists: "pondra.tasks", about: "tasks that run after others (AFTER), once per tick of the first", also: &["workflow", "job"] },
    Pattern { name: "history view", lists: "pondra.tables", about: "a materialized view keeping every version of each row", also: &["SCD type 2", "slowly changing dimension"] },
];

pub fn kind(name: &str) -> Option<&'static Kind> { KINDS.iter().find(|k| k.name == name) }

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
            Some("relation" | "routine" | "task" | "type") => split(name),
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
static FAMILIES: &[(&str, Lister)] = &[("schema", schemas), ("relation", relations), ("relation", sequences), ("relation", indexes), ("type", types), ("routine", routines), ("task", tasks), ("secret", secrets), ("user", users), ("database", databases),
    ("share", shares), ("recipient", recipients)];

/// Every object of this lake and the lakes attached to it, with its comment, as the caller may see
/// them (a user limited by grants: the tables it may read, and no secrets, users or roles).
pub async fn list(lake: &Lake) -> Result<Vec<Object>> {
    let mut all = vec![];
    for (_, lister) in FAMILIES {
        all.extend(lister(lake).await?);
    }
    let mut notes = HashMap::new();
    for (name, l) in lakes(lake).await {
        let cm: BTreeMap<String, String> = l.cat.scan::<String>("cm/", "cm0").await?.into_iter().map(|(k, v)| (k[3..].to_string(), v)).collect();
        notes.insert(name, cm);
    }
    for o in &mut all {
        o.comment = notes.get(&o.lake).and_then(|n| n.get(&format!("{}/{}", o.family(), o.local()))).cloned();
    }
    Ok(all)
}

/// This lake and the lakes attached to it that the caller may use, by name: one that signs in on
/// its own is listed only for whoever runs the nodes (invariant 240), as its tables are read.
async fn lakes(lake: &Lake) -> Vec<(String, Arc<Lake>)> {
    let mut all = vec![(crate::ddl::lake_name(lake), lake.arc())];
    let attached: Vec<(String, Arc<Lake>)> = lake.attached.read().unwrap().clone();
    for (name, l) in attached {
        if crate::users::across(&l, &name).await.is_ok() {
            all.push((name, l));
        }
    }
    all
}

/// Schemas of this lake and those attached. (An attached lake's schemas, routines and tasks are made
/// by a node of that lake: their statements are shown there.)
fn schemas(lake: &Lake) -> BoxFuture<'_, Result<Vec<Object>>> {
    Box::pin(async move {
        let here = crate::ddl::lake_name(lake);
        let mut all = vec![];
        for (name, l) in lakes(lake).await {
            for s in crate::ddl::schemas(&l).await? {
                let def = (s != PUBLIC && name == here).then(|| format!("CREATE SCHEMA {}", ident(&s)));
                all.push(Object::new("schema", &name, &s, def));
            }
        }
        Ok(all)
    })
}

fn relations(lake: &Lake) -> BoxFuture<'_, Result<Vec<Object>>> {
    Box::pin(async move {
        let mut views = HashMap::new();
        for (name, l) in lakes(lake).await {
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

/// Sequences of this lake and those attached; an identity column's is its table's, not listed.
fn sequences(lake: &Lake) -> BoxFuture<'_, Result<Vec<Object>>> {
    Box::pin(async move {
        let here = crate::ddl::lake_name(lake);
        let mut all = vec![];
        for (name, l) in lakes(lake).await {
            for (k, s) in l.cat.scan::<crate::seq::Sequence>("sq/", "sq0").await?.into_iter().filter(|(_, s)| s.owned.is_none()) {
                let named = if name == here { name_sql(&k[3..]) } else { format!("{}.{}", ident(&name), name_sql(&k[3..])) };
                all.push(Object::new("sequence", &name, &k[3..], Some(crate::seq::create_sql(&named, &s))));
            }
        }
        Ok(all)
    })
}

/// Indexes of this lake and those attached, each in its table's schema.
fn indexes(lake: &Lake) -> BoxFuture<'_, Result<Vec<Object>>> {
    Box::pin(async move {
        let here = crate::ddl::lake_name(lake);
        let mut all = vec![];
        for (name, l) in lakes(lake).await {
            for (k, i) in l.cat.scan::<crate::index::Index>("ix/", "ix0").await? {
                if crate::auth::limited().is_some_and(|a| !a.may("select", &if name == here { i.table.clone() } else { format!("{name}.{}", i.table) })) {
                    continue;
                }
                let meta = l.cat.get::<TableMeta>(&table_key(&i.table)).await?;
                let lake_part = if name == here { String::new() } else { format!("{}.", ident(&name)) };
                let def = crate::index::create_sql(&ident(crate::ddl::split(&k[3..]).1), &format!("{lake_part}{}", name_sql(&i.table)), &i, meta.as_ref());
                all.push(Object::new("index", &name, &k[3..], Some(def)));
            }
        }
        Ok(all)
    })
}

/// Types of this lake and those attached (`types.rs`: enums), each in its schema.
fn types(lake: &Lake) -> BoxFuture<'_, Result<Vec<Object>>> {
    Box::pin(async move {
        let here = crate::ddl::lake_name(lake);
        let mut all = vec![];
        for (name, l) in lakes(lake).await {
            for (k, t) in l.cat.scan::<crate::types::Type>("ty/", "ty0").await? {
                let named = if name == here { name_sql(&k[3..]) } else { format!("{}.{}", ident(&name), name_sql(&k[3..])) };
                all.push(Object::new("type", &name, &k[3..], Some(crate::types::create_sql(&named, &t))));
            }
        }
        Ok(all)
    })
}

fn routines(lake: &Lake) -> BoxFuture<'_, Result<Vec<Object>>> {
    Box::pin(async move {
        use crate::routines::Kind as R;
        let here = crate::ddl::lake_name(lake);
        let mut out = vec![];
        for (name, l) in lakes(lake).await {
            let all = crate::routines::listed(&l).await?;
            let mut names: Vec<&String> = all.keys().collect();
            names.sort();
            out.extend(names.into_iter().map(|n| {
                let r = &all[n];
                let kind = match (r.kind, r.what()) {
                    (R::Procedure, _) => "procedure",
                    (_, "macro") => "macro",
                    (R::Table, _) => "table function",
                    _ => "function",
                };
                Object::new(kind, &name, n, (name == here).then(|| routine_sql(&name_sql(n), r)))
            }));
        }
        Ok(out)
    })
}

fn tasks(lake: &Lake) -> BoxFuture<'_, Result<Vec<Object>>> {
    Box::pin(async move {
        let here = crate::ddl::lake_name(lake);
        let mut out = vec![];
        for (name, l) in lakes(lake).await {
            for (k, t) in l.cat.scan::<crate::runs::Task>("j/", "j0").await? {
                out.push(Object::new("task", &name, &k[2..], (name == here).then(|| task_sql(&name_sql(&k[2..]), &t))));
            }
        }
        Ok(out)
    })
}

fn secrets(lake: &Lake) -> BoxFuture<'_, Result<Vec<Object>>> {
    Box::pin(async move {
        if crate::auth::limited().is_some() {
            return Ok(vec![]); // (their names and scopes are an admin's to see)
        }
        let mut out = vec![];
        for (name, l) in lakes(lake).await {
            out.extend(crate::ext::list(&l).await?.into_iter().map(|(n, _)| Object::new("secret", &name, &n, None))); // (values are never shown)
        }
        Ok(out)
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
        let mut all = vec![];
        for (k, a) in lake.cat.scan::<crate::ddl::Attachment>("a/", "a0").await? {
            let made = branched(lake, &k[2..], &a.dir).await.unwrap_or_else(|| format!("ATTACH {} AS {}", literal(&a.dir), ident(&k[2..])));
            let made = match attached_protected(lake, &k[2..]).await {
                true => format!("{made};\nALTER DATABASE {} SET (protected = true)", ident(&k[2..])), // (its protection is its own: `protect.rs`)
                false => made,
            };
            all.push(Object::new("database", &here, &k[2..], Some(made)));
        }
        for (n, a) in crate::ext::attached(lake).await? {
            let options: String = a.options.iter().map(|(k, v)| format!(", {} {}", k.to_uppercase(), literal(v))).collect();
            all.push(Object::new("database", &here, &n, Some(format!("ATTACH {} AS {} (TYPE {}{options})", literal(&a.url), ident(&n), a.kind))));
        }
        Ok(all)
    })
}

/// Whether an attached database is protected, read from its own catalog (`protect.rs`).
async fn attached_protected(lake: &Lake, name: &str) -> bool {
    let db = lake.attached.read().unwrap().iter().find(|(n, _)| n == name).map(|(_, l)| l.clone());
    match db {
        Some(db) => crate::protect::protected(&db).await.unwrap_or(false),
        None => false,
    }
}

/// A branch is made by the clone that made it (ADR-047), never by its `ATTACH`: `DROP DATABASE`
/// deletes a branch, so there would be nothing left to attach.
async fn branched(lake: &Lake, name: &str, dir: &str) -> Option<String> {
    let lakes = lake.attached.read().unwrap().clone();
    let branch = lakes.iter().find(|(n, _)| n == name)?.1.cat.get::<crate::branch::Bases>(crate::branch::BASES).await.ok()??;
    let base = match branch.base == lake.url {
        true => crate::ddl::lake_name(lake),
        false => lakes.iter().find(|(_, l)| l.url == branch.base)?.0.clone(), // (a base not attached here can't be named)
    };
    let beside = crate::ddl::full(&crate::ddl::beside(&lake.url, name)).ok();
    let location = if beside.as_deref() == Some(dir) { String::new() } else { format!(" LOCATION {}", literal(dir)) };
    let schemas = match branch.schemas.is_empty() {
        true => String::new(),
        false => format!(" WITH (schemas = ({}))", branch.schemas.iter().map(|s| ident(s)).collect::<Vec<_>>().join(", ")),
    };
    let data = if branch.data { "" } else { " WITH NO DATA" };
    Some(format!("CREATE DATABASE {}{location} CLONE {}{schemas}{data}", ident(name), ident(&base)))
}

fn shares(lake: &Lake) -> BoxFuture<'_, Result<Vec<Object>>> {
    Box::pin(async move {
        if crate::auth::limited().is_some() {
            return Ok(vec![]); // (an admin's, as pondra.shares is)
        }
        let here = crate::ddl::lake_name(lake);
        let mut all = vec![];
        for (n, s) in crate::shares::shares(lake).await? {
            all.push(Object::new("share", &here, &n, Some(share_sql(lake, &n, &s).await?)));
        }
        Ok(all)
    })
}

fn recipients(lake: &Lake) -> BoxFuture<'_, Result<Vec<Object>>> {
    Box::pin(async move {
        if crate::auth::limited().is_some() {
            return Ok(vec![]);
        }
        let here = crate::ddl::lake_name(lake);
        Ok(crate::shares::recipients(lake).await?.into_iter().map(|(n, _)| Object::new("recipient", &here, &n, None)).collect()) // (its token was shown once)
    })
}

// ---------------------------------------------------------------- definitions

/// A name in SQL: each part bare if it can be, quoted if not.
pub fn name_sql(local: &str) -> String { local.split('.').map(ident).collect::<Vec<_>>().join(".") }

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
pub(crate) fn sql_type(t: &str) -> String {
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
pub(crate) fn table_sql(name: &str, m: &TableMeta) -> String {
    let mut parts: Vec<String> = m.columns.iter().filter(|(c, _)| !m.marker(c)).map(|(c, t)| { // (a keyed table's `_deleted` is its own)
        let merge = m.merge.get(c).map(|f| format!(" MERGE {f}")).unwrap_or_default();
        let null = if m.not_null.contains(c) && !m.key.contains(c) && !m.identity.contains_key(c) { " NOT NULL" } else { "" };
        let default = match m.identity.get(c) {
            Some(i) => format!(" {}", i.sql()), // (its sequence is the table's: made with it)
            None => m.defaults.get(c).map(|d| format!(" DEFAULT {d}")).unwrap_or_default(),
        };
        let ty = m.enums.get(c).map_or_else(|| sql_type(t), |e| e.sql()); // (an enum column: its type, not the text it holds)
        format!("{} {ty}{merge}{null}{default}", ident(c))
    }).collect();
    if !m.key.is_empty() {
        parts.push(format!("PRIMARY KEY ({})", list_sql(&m.key)));
    }
    parts.extend(m.checks.iter().map(|(n, c)| format!("CONSTRAINT {} CHECK ({c})", ident(n))));
    parts.extend(m.constraints.iter().map(|c| c.sql(&ident)));
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
pub(crate) fn materialized_sql(name: &str, v: &crate::views::View, meta: Option<&TableMeta>) -> String {
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
    if let Some(b) = v.rerun.as_ref().filter(|b| b.asked) {
        // (chosen, it is chosen again from the query; asked for, it is asked for again)
        with.push(format!("refresh = '{}'", if b.full.is_some() { "full" } else { "by key" }));
        with.extend(b.lag_secs.map(|s| format!("lag = '{s} seconds'")));
    }
    let expect = if expect.is_empty() { String::new() } else { format!(" (\n  {}\n)", expect.join(",\n  ")) };
    let with = if with.is_empty() { String::new() } else { format!(" WITH ({})", with.join(", ")) };
    format!("CREATE MATERIALIZED VIEW {name}{expect}{with} AS\n{}", v.query())
}

/// `CREATE FUNCTION`, `CREATE MACRO` or `CREATE PROCEDURE`, in the form it was made in.
pub(crate) fn routine_sql(name: &str, r: &crate::routines::Routine) -> String {
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

/// `CREATE SHARE`, the tables it hands out (their partitions, the names they are shared as, their
/// history) and the recipients it is granted to.
async fn share_sql(lake: &Lake, name: &str, s: &crate::shares::Share) -> Result<String> {
    let mut script = vec![format!("CREATE SHARE {}", ident(name))];
    for t in &s.tables {
        let mut add = format!("ALTER SHARE {} ADD TABLE {}", ident(name), name_sql(&t.table));
        if !t.partitions.is_empty() {
            let column = lake.cat.get::<TableMeta>(&table_key(&t.table)).await?.and_then(|m| m.logical().partition).unwrap_or_default();
            add += &format!(" PARTITION {}", t.partitions.iter().map(|v| format!("({} = {})", ident(&column), literal(v))).collect::<Vec<_>>().join(", "));
        }
        if (t.schema.as_str(), t.name.as_str()) != split(&t.table) {
            add += &format!(" AS {}.{}", ident(&t.schema), ident(&t.name));
        }
        if t.history {
            add += " WITH HISTORY";
        }
        script.push(add);
    }
    if !s.recipients.is_empty() {
        script.push(format!("GRANT SELECT ON SHARE {} TO RECIPIENT {}", ident(name), list_sql(&s.recipients)));
    }
    Ok(script.join(";\n"))
}

/// `CREATE TASK`: when, after what, on what condition and with what options it runs.
pub(crate) fn task_sql(name: &str, t: &crate::runs::Task) -> String {
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
        "relation" => !crate::sys::hidden(name) && (has(table_key(name)).await? || has(crate::ddl::query_key(name)).await? || has(crate::seq::key(name)).await? || has(crate::index::key(name)).await?),
        "column" => match name.rsplit_once('/') {
            Some((t, c)) => match lake.cat.get::<TableMeta>(&table_key(t)).await? {
                Some(m) => m.live().any(|(s, _, _)| s == c),
                None => has(crate::ddl::query_key(t)).await?, // (a view's: SHOW CREATE shows those of the columns it has)
            },
            None => false,
        },
        "routine" => has(crate::routines::key(name)).await?,
        "type" => has(crate::types::key(name)).await?,
        "task" => has(crate::runs::task_key(name)).await?,
        "schema" => name == PUBLIC || has(crate::ddl::schema_key(name)).await?,
        "secret" => has(format!("e/{name}")).await?,
        "user" => has(format!("u/{name}")).await?,
        "database" => has(crate::ddl::attachment_key(name)).await? || has(format!("o/{name}")).await?,
        "share" => has(crate::shares::share_key(name)).await?,
        "recipient" => has(crate::shares::recipient_key(name)).await?,
        _ => false,
    })
}

async fn comment(lake: &Lake, word: &str, name: &str, text: Option<String>, if_exists: bool) -> Result<Value> {
    let key = match word {
        "column" => {
            let (table, column) = name.rsplit_once('.').context("COMMENT ON COLUMN table.column")?;
            let table = local(lake, table, "relation").await?;
            let meta = lake.cat.get::<TableMeta>(&table_key(&table)).await?.filter(|_| !crate::sys::hidden(&table));
            let stored = match &meta {
                Some(m) => m.stored(column).map(str::to_string),
                None => view_columns(lake, &table).await?.and_then(|c| c.into_iter().find(|c| c == column)),
            };
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

/// A (stored) view's columns as its query names them now, or None for no such view. (Boxed: planning
/// it expands SQL, which may show a SHOW CREATE, which comes back here.)
fn view_columns<'a>(lake: &'a Lake, view: &'a str) -> BoxFuture<'a, Result<Option<Vec<String>>>> {
    Box::pin(async move {
        if lake.cat.get::<crate::ddl::StoredView>(&crate::ddl::query_key(view)).await?.is_none() {
            return Ok(None);
        }
        let sql = format!("SELECT * FROM {} LIMIT 0", name_sql(view));
        let df = crate::query::sql(&crate::query::session(lake, &sql, "").await?, &sql).await?;
        Ok(Some(df.schema().fields().iter().map(|f| f.name().clone()).collect()))
    })
}

/// Does carrying out `d` drop or rename something a comment may be on?
pub fn moves(d: &Ddl) -> bool {
    matches!(d, Ddl::Sequence(_) | Ddl::Index(_) | Ddl::Type(_) | Ddl::DropTable { .. } | Ddl::DropView { .. } | Ddl::DropSchema { .. } | Ddl::DropRoutine { .. } | Ddl::DropTask { .. } | Ddl::DropSecret { .. }
        | Ddl::Detach { .. } | Ddl::DropDatabase { .. } | Ddl::RenameTable { .. } | Ddl::AlterColumn { .. } | Ddl::Users(_)
        | Ddl::Shares(crate::shares::Change::DropShare { .. } | crate::shares::Change::DropRecipient { .. }))
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
    let relation = out["table"].as_str().or(out["view"].as_str()).or(out["sequence"].as_str()).or(out["index"].as_str()).map(|n| ("relation", n));
    let renamed = match (relation.or(out["type"].as_str().map(|n| ("type", n))), out["renamed"].as_str()) {
        (Some((family, from)), Some(to)) => Some((family, from.to_string(), to.to_string())),
        _ => None,
    };
    for (k, text) in notes {
        let moved = renamed.as_ref().and_then(|(family, from, to)| {
            let rest = k.strip_prefix("cm/")?;
            if rest == format!("{family}/{from}") {
                Some(format!("cm/{family}/{to}"))
            } else {
                rest.strip_prefix(&format!("column/{from}/")).filter(|_| *family == "relation").map(|c| format!("cm/column/{to}/{c}"))
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
    let constraints: Vec<crate::constraints::Constraint> = serde_json::from_value(s["constraints"].clone()).unwrap_or_default();
    ensure!(constraints == old.constraints, "{table}'s UNIQUE, PRIMARY KEY and FOREIGN KEY constraints change with ALTER TABLE {table} ADD | DROP CONSTRAINT");
    let enums: BTreeMap<String, crate::types::Enum> = serde_json::from_value(s["enums"].clone()).unwrap_or_default();
    if let Some((c, _)) = was.iter().find(|(c, _)| enums.get(c) != old.enums.get(c)) {
        bail!("{table}.{c}: a column's enum can't change here (a type takes a new label with ALTER TYPE … ADD VALUE)");
    }
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
    let wanted = kind(&word).context("SHOW CREATE TABLE | VIEW | MATERIALIZED VIEW | FUNCTION | PROCEDURE | TASK | SCHEMA | ROLE | DATABASE | SHARE name")?;
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
        _ if o.lake != here && o.family() != "relation" => "it is made by a node of its own lake: SHOW CREATE it there",
        "secret" => "a secret's values are never shown",
        "user" => "a user's password and tokens are never shown (CREATE USER … PASSWORD '…')",
        "recipient" => "a recipient's token is shown once, when it is made (ALTER RECIPIENT … ROTATE TOKEN gives it another)",
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
        let meta = lake.cat.get::<TableMeta>(&table_key(&o.local())).await?;
        let view = match meta {
            None => view_columns(lake, &o.local()).await?,
            Some(_) => None,
        };
        for (k, text) in lake.cat.scan::<String>(&format!("cm/column/{}/", o.local()), &format!("cm/column/{}0", o.local())).await? {
            let stored = k.rsplit('/').next().unwrap_or_default();
            let column = match (&meta, &view) {
                (Some(m), _) => m.name_of(stored),
                (None, Some(columns)) if columns.iter().any(|c| c == stored) => stored, // (a view's columns go by their names)
                _ => continue,
            };
            script.push(format!("COMMENT ON COLUMN {named}.{} IS {}", ident(column), literal(&text)));
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
    use datafusion::arrow::array::{ArrayRef, BooleanArray, RecordBatch, StringArray};
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
    let registry = rows();
    let cell = |c: &str| Arc::new(registry.iter().map(|r| words(&r[c])).collect::<StringArray>()) as ArrayRef;
    let flag = |c: &str| Arc::new(registry.iter().map(|r| r[c].as_bool()).collect::<BooleanArray>()) as ArrayRef;
    let kinds = RecordBatch::try_from_iter(vec![
        ("kind", cell("kind")), ("is", cell("is")), ("family", cell("family")), ("inside", cell("inside")), ("statements", cell("statements")),
        ("privileges", cell("privileges")), ("on_clone", cell("on_clone")), ("in_project", flag("in_project")), ("undrop", flag("undrop")), ("secret", flag("secret")),
        ("lists", cell("lists")), ("about", cell("about")), ("also", cell("also")),
    ])?;
    Ok(vec![("objects", mem(objects)?), ("kinds", mem(kinds)?)])
}

/// One row per kind, then per part, then per pattern: what `pondra.kinds` and `GET /kinds` list. A
/// column that doesn't apply to a row is null (a part has no statements; a pattern no family).
fn rows() -> Vec<Value> {
    let objects = KINDS.iter().map(|k| j!({"kind": k.name, "is": "object", "family": k.family, "inside": null, "statements": k.verbs, "privileges": k.privileges,
        "on_clone": k.on_clone.word(), "in_project": k.project.is_none(), "undrop": k.verbs.contains(&"UNDROP"), "secret": k.secret, "lists": null, "about": k.about, "also": k.also}));
    let parts = PARTS.iter().map(|p| j!({"kind": p.name, "is": "part", "family": null, "inside": p.inside, "statements": null, "privileges": null, "on_clone": null,
        "in_project": null, "undrop": null, "secret": null, "lists": null, "about": p.about, "also": p.also}));
    let patterns = PATTERNS.iter().map(|p| j!({"kind": p.name, "is": "pattern", "family": null, "inside": null, "statements": null, "privileges": null, "on_clone": null,
        "in_project": null, "undrop": null, "secret": null, "lists": p.lists, "about": p.about, "also": p.also}));
    objects.chain(parts).chain(patterns).collect()
}

/// A row's cell as text: a list joined with ", " (an empty list is ""), a string as it is, null as NULL.
fn words(v: &Value) -> Option<String> {
    match v {
        Value::Array(a) => Some(a.iter().filter_map(Value::as_str).collect::<Vec<_>>().join(", ")),
        Value::String(s) => Some(s.clone()),
        _ => None,
    }
}

/// `GET /kinds`: every kind, part and pattern, as `pondra.kinds` lists them.
pub fn kinds() -> Value { Value::Array(rows()) }

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
        assert_eq!((ddl("COMMENT ON SHARE acme IS 'for Acme'")["kind"].clone(), ddl("comment on recipient acme_corp is null")["kind"].clone()), (j!("share"), j!("recipient")));
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

    #[test]
    fn one_word_one_meaning() {
        let names: Vec<String> = KINDS.iter().map(|k| k.name).chain(PARTS.iter().map(|p| p.name)).chain(PATTERNS.iter().map(|p| p.name)).map(|n| n.to_lowercase()).collect();
        let mut unique = names.clone();
        unique.sort();
        unique.dedup();
        assert_eq!(unique.len(), names.len(), "a name is used twice: {names:?}");
        assert!(PARTS.iter().all(|p| p.inside.iter().all(|i| kind(i).is_some())), "a part sits inside a name that isn't a kind");
        assert!(KINDS.iter().all(|k| !k.prefix.is_empty() && k.prefix.ends_with('/') && !k.about.is_empty()), "a kind lacks its prefix or its line");
        let mut leave: Vec<&str> = KINDS.iter().filter(|k| k.on_clone == OnClone::Leave).map(|k| k.name).collect();
        leave.sort();
        assert_eq!(leave, ["recipient", "secret", "share"]);
        assert_eq!(kinds().as_array().map(Vec::len), Some(KINDS.len() + PARTS.len() + PATTERNS.len()));
    }
}
