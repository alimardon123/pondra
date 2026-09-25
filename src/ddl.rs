//! Schemas, names, and the statements that shape a lake: `CREATE`/`DROP SCHEMA`, `DROP TABLE`,
//! `CREATE VIEW` (a stored query), `CREATE MATERIALIZED VIEW` (a live view, `views.rs`) and
//! `DROP VIEW`.
//!
//! A lake is a database, and its tables live in schemas: `public` unless one is named. Inside the
//! lake a table of schema `s` is called `s.t`, and one of `public` just `t`, as before schemas
//! existed. So everything keyed by a table's name works unchanged: the catalog, the log, the
//! files, Kafka topics. In SQL:
//! - `t` is `public.t`;
//! - `s.t` is schema `s` here, or, if no schema here has that name, an attached lake's `public.t`
//!   (what two-part names meant before);
//! - `l.s.t` is lake `l`: this one (by its folder's name) or an attached one.
//!
//! Other lakes are attached with `--attach name=dir` when a node starts, or in SQL with
//! `ATTACH 'dir' AS name` (kept in the catalog, so every node attaches it) and `DETACH name`.
use crate::store::{json, table_key, Lake, TableMeta};
use anyhow::{bail, ensure, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json as j, Value};
use std::sync::Arc;

pub const PUBLIC: &str = "public";

pub fn schema_key(s: &str) -> String { format!("ns/{s}") }
/// A stored view (`CREATE VIEW`): a query with a name, run where it is used.
pub fn query_key(v: &str) -> String { format!("q/{v}") }

#[derive(Serialize, Deserialize, Default)]
pub struct Schema {
    pub created_ms: u64,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct StoredView {
    pub sql: String,
}

/// Another lake, attached in SQL (`ATTACH 'dir' AS name`): every node attaches it.
pub fn attachment_key(n: &str) -> String { format!("a/{n}") }

#[derive(Serialize, Deserialize, Clone)]
pub struct Attachment {
    pub dir: String,
}

/// A table's schema, and its name within it.
pub fn split(name: &str) -> (&str, &str) { name.split_once('.').unwrap_or((PUBLIC, name)) }

/// The name a table of `schema` has inside the lake.
pub fn join(schema: &str, table: &str) -> String {
    if schema == PUBLIC { table.to_string() } else { format!("{schema}.{table}") }
}

/// What a schema, table or view may be called: letters, digits, `_` and `-`. A dot separates the
/// parts of a name; quotes, slashes and spaces would get in the way of paths and SQL.
pub fn check(part: &str) -> Result<()> {
    ensure!(!part.is_empty() && part.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '-'),
            "{part:?}: names are letters, digits, _ and - (a dot separates schema and table)");
    Ok(())
}

/// This lake's own name in three-part names: its folder's (or prefix's) last part.
pub fn lake_name(lake: &Lake) -> String {
    lake.url.trim_end_matches('/').rsplit('/').next().unwrap_or("lake").to_lowercase()
}

pub async fn schemas(lake: &Lake) -> Result<Vec<String>> {
    let mut out = vec![PUBLIC.to_string()];
    out.extend(lake.cat.scan::<Schema>("ns/", "ns0").await?.into_iter().map(|(k, _)| k[3..].to_string()));
    Ok(out)
}

async fn has_schema(lake: &Lake, s: &str) -> Result<bool> {
    Ok(s == PUBLIC || lake.cat.get::<Schema>(&schema_key(s)).await?.is_some())
}

/// Where a name points: this lake (None) or an attached one, and the name inside it. Names are as
/// SQL resolves them (`write::object`): unquoted parts already lower-case.
pub async fn resolve(lake: &Lake, name: &str) -> Result<(Option<Arc<Lake>>, String)> {
    let attached = |l: &str| lake.attached.read().unwrap().iter().find(|(n, _)| n == l).map(|(_, o)| o.clone());
    let parts: Vec<&str> = name.split('.').collect();
    Ok(match parts[..] {
        [t] => (None, t.to_string()),
        [s, t] if has_schema(lake, s).await? => (None, join(s, t)),
        [l, t] => match attached(l) {
            Some(other) => (Some(other), t.to_string()),
            None => bail!("no schema {l} (CREATE SCHEMA {l})"),
        },
        [l, s, t] if l == lake_name(lake) => {
            ensure!(has_schema(lake, s).await?, "no schema {s} (CREATE SCHEMA {s})");
            (None, join(s, t))
        }
        [l, s, t] => (Some(attached(l).with_context(|| format!("no lake {l}: this one is {}, and none is attached as {l}", lake_name(lake)))?), join(s, t)),
        _ => bail!("{name}: a name is table, schema.table or lake.schema.table"),
    })
}

/// A table of this lake by its name inside it, from a name as SQL wrote it (`write::object`):
/// None for another lake's. (A two-part name may yet be an attached lake's: no table has it here.)
pub fn local(lake: &Lake, name: &str) -> Option<String> {
    match name.split('.').collect::<Vec<_>>()[..] {
        [t] => Some(t.to_string()),
        [s, t] => Some(join(s, t)),
        [l, s, t] if l == lake_name(lake) => Some(join(s, t)),
        _ => None,
    }
}

/// A name for something new in this lake: its schema exists and its parts are fine. `public.t`
/// is `t`.
pub async fn new_name(lake: &Lake, name: &str) -> Result<String> {
    let (s, t) = split(name);
    check(s)?;
    check(t)?;
    ensure!(!t.contains('.'), "{name}: a name is table, schema.table or lake.schema.table");
    ensure!(has_schema(lake, s).await?, "no schema {s} (CREATE SCHEMA {s})");
    Ok(join(s, t))
}

// ---------------------------------------------------------------- statements

/// A statement that changes what the lake holds, not its rows: the leader carries it out.
#[derive(Serialize, Deserialize, Clone)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Ddl {
    CreateSchema { name: String, if_not_exists: bool },
    DropSchema { name: String, if_exists: bool, cascade: bool },
    DropTable { name: String, if_exists: bool },
    CreateView { name: String, sql: String, replace: bool },
    CreateMaterialized { name: String, sql: String, options: std::collections::BTreeMap<String, String> }, // (`views::options`)
    DropView { name: String, if_exists: bool },
    Attach { name: String, dir: String },
    Detach { name: String, if_exists: bool },
}

/// Leader: carry one out (under the lake's lock).
pub async fn apply(lake: &Lake, d: Ddl) -> Result<Value> {
    match d {
        Ddl::CreateSchema { name, if_not_exists } => {
            check(&name)?;
            if has_schema(lake, &name).await? {
                ensure!(if_not_exists, "schema {name} already exists");
                return Ok(j!({"schema": name, "unchanged": true}));
            }
            lake.cat.commit(vec![(schema_key(&name), json(&Schema { created_ms: crate::log::now_ms() }))], &[]).await?;
            Ok(j!({"schema": name}))
        }
        Ddl::DropSchema { name, if_exists, cascade } => {
            ensure!(name != PUBLIC, "the public schema stays");
            if !has_schema(lake, &name).await? {
                ensure!(if_exists, "no schema {name}");
                return Ok(j!({"schema": name, "dropped": false}));
            }
            let inside = |k: &str, p: usize| split(&k[p..]).0 == name && k[p..].contains('.');
            let views: Vec<String> = lake.cat.scan::<Value>("v/", "v0").await?.into_iter().chain(lake.cat.scan::<Value>("q/", "q0").await?)
                .filter(|(k, _)| inside(k, 2)).map(|(k, _)| k[2..].to_string()).collect();
            let tables: Vec<String> = lake.cat.scan::<TableMeta>("t/", "t0").await?.into_iter().filter(|(k, _)| inside(k, 2)).map(|(k, _)| k[2..].to_string()).collect();
            ensure!(cascade || (views.is_empty() && tables.is_empty()), "schema {name} isn't empty ({}): drop them first, or DROP SCHEMA {name} CASCADE", [&views[..], &tables[..]].concat().join(", "));
            for v in &views {
                drop_view(lake, v, true).await?;
            }
            for t in tables.iter().filter(|t| !views.iter().any(|v| *t == v || **t == format!("{v}_final"))) {
                drop_table(lake, t, true).await?;
            }
            lake.cat.commit(vec![], &[schema_key(&name)]).await?;
            Ok(j!({"schema": name, "dropped": true}))
        }
        Ddl::DropTable { name, if_exists } => drop_table(lake, &name, if_exists).await,
        Ddl::CreateView { name, sql, replace } => {
            let name = new_name(lake, &name).await?;
            ensure!(lake.cat.get::<TableMeta>(&table_key(&name)).await?.is_none(), "{name} is a table");
            ensure!(lake.cat.get::<Value>(&crate::views::view_key(&name)).await?.is_none(), "{name} is a materialized view");
            ensure!(replace || lake.cat.get::<StoredView>(&query_key(&name)).await?.is_none(), "view {name} already exists (CREATE OR REPLACE VIEW)");
            let planned = crate::asof::rewrite(&sql)?;
            crate::query::session(lake, &planned, "").await?.sql(&planned).await.context("the view's query")?; // (it plans)
            lake.cat.commit(vec![(query_key(&name), json(&StoredView { sql }))], &[]).await?;
            Ok(j!({"view": name}))
        }
        Ddl::CreateMaterialized { name, sql, options } => {
            let (emit, sessions) = crate::views::options(&options)?;
            let name = new_name(lake, &name).await?;
            ensure!(lake.cat.get::<StoredView>(&query_key(&name)).await?.is_none(), "{name} is a (stored) view");
            crate::views::create(lake, &name, &sql, emit, sessions).await?;
            Ok(j!({"view": name, "materialized": true, "follows": "rows written from now on"})) // (no backfill yet: ADR-019)
        }
        Ddl::DropView { name, if_exists } => drop_view(lake, &name, if_exists).await,
        Ddl::Attach { name, dir } => {
            check(&name)?;
            ensure!(name != lake_name(lake), "this lake is called {name}: attach the other under another name");
            ensure!(!has_schema(lake, &name).await?, "a schema here is called {name}: attach the other under another name");
            let dir = lake_dir(&dir).await?;
            ensure!(dir != lake.url, "{dir} is this lake");
            if let Some(a) = lake.cat.get::<Attachment>(&attachment_key(&name)).await? {
                ensure!(a.dir == dir, "{name} is attached already, to {}", a.dir);
                return Ok(j!({"attached": name, "dir": dir, "unchanged": true}));
            }
            ensure!(!lake.attached.read().unwrap().iter().any(|(n, _)| *n == name), "{name} is attached already (--attach)");
            lake.cat.commit(vec![(attachment_key(&name), json(&Attachment { dir: dir.clone() }))], &[]).await?;
            Ok(j!({"attached": name, "dir": dir}))
        }
        Ddl::Detach { name, if_exists } => {
            if lake.cat.get::<Attachment>(&attachment_key(&name)).await?.is_none() {
                let flag = lake.attached.read().unwrap().iter().any(|(n, _)| *n == name);
                ensure!(if_exists && !flag, "{name} {}", if flag { "was attached with --attach, when the node started: it stays" } else { "isn't attached" });
                return Ok(j!({"detached": name, "unchanged": true}));
            }
            lake.cat.commit(vec![], &[attachment_key(&name)]).await?;
            Ok(j!({"detached": name}))
        }
    }
}

/// Where a lake to attach is, as its nodes open it: a bucket's URL, or a folder's full path (a
/// relative one would mean something else on every machine). A lake is there: it has a catalog.
async fn lake_dir(dir: &str) -> Result<String> {
    let dir = match dir.contains("://") {
        true => dir.trim_end_matches('/').to_string(),
        false => std::fs::canonicalize(dir).with_context(|| format!("no lake at {dir}"))?.to_string_lossy().trim_start_matches(r"\\?\").to_string(), // (as `store::open_store` has it)
    };
    let store = crate::store::open_store(&dir)?.1;
    let catalog = futures::StreamExt::next(&mut store.list(Some(&object_store::path::Path::from("catalog")))).await.transpose()?;
    ensure!(catalog.is_some(), "no lake at {dir}: it has no catalog");
    Ok(dir)
}

/// Attach lake `dir` as `name` here. A node (`follow`) keeps it fresh from its catalog, until it
/// is detached; `stream` also follows its leader's commit stream (`--attach`: for good).
pub async fn attach(home: &Lake, name: &str, dir: &str, me: &str, follow: bool, stream: bool) -> Result<()> {
    let leader = crate::cluster::latest(&crate::store::open_store(dir)?.1).await?.map(|t| t.addr).filter(|a| !a.is_empty());
    let live = match (&leader, stream) {
        (Some(a), true) => crate::cluster::http().get(format!("http://{a}/cluster/leader")).timeout(std::time::Duration::from_secs(2)).send().await.is_ok(),
        _ => false,
    };
    let other = Lake::open(dir, false, live).await?;
    if let (true, Some(a)) = (live, leader) {
        crate::cluster::mirror(other.clone(), a, me.to_string(), None);
    }
    if follow {
        let (home, weak) = (Arc::downgrade(&home.arc()), Arc::downgrade(&other));
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_millis(250)).await;
                let (Some(home), Some(other)) = (home.upgrade(), weak.upgrade()) else { return }; // (detached)
                if !home.attached.read().unwrap().iter().any(|(_, o)| Arc::ptr_eq(o, &other)) {
                    return;
                }
                if let Err(e) = other.refresh().await {
                    eprintln!("background job failed: {e:#}");
                }
            }
        });
    }
    home.attach(name, other)
}

/// The lakes this process attached because the catalog said so, per home lake.
static FROM_SQL: std::sync::Mutex<Vec<(String, String)>> = std::sync::Mutex::new(Vec::new());

/// Attach here the lakes the catalog lists (`ATTACH`), and let go of those it no longer does
/// (`DETACH`). Lakes attached with `--attach` stay. One that can't be opened is skipped (and
/// tried again next time).
pub async fn sync(lake: &Lake, me: &str, follow: bool) -> Result<()> {
    let listed = lake.cat.scan::<Attachment>("a/", "a0").await?;
    for (key, a) in &listed {
        let name = &key[2..];
        if lake.attached.read().unwrap().iter().any(|(n, _)| n == name) {
            continue;
        }
        match attach(lake, name, &a.dir, me, follow, false).await {
            Ok(()) => FROM_SQL.lock().unwrap().push((lake.url.clone(), name.to_string())),
            Err(e) => eprintln!("attaching {name} ({}): {e:#}", a.dir),
        }
    }
    let gone: Vec<String> = FROM_SQL.lock().unwrap().iter().filter(|(u, n)| *u == lake.url && !listed.iter().any(|(k, _)| &k[2..] == n)).map(|(_, n)| n.clone()).collect();
    if !gone.is_empty() {
        lake.attached.write().unwrap().retain(|(n, _)| !gone.contains(n));
        FROM_SQL.lock().unwrap().retain(|(u, n)| !(*u == lake.url && gone.contains(n)));
    }
    Ok(())
}

/// After this node sent an `ATTACH` or `DETACH`: once its catalog shows it, as it does here.
pub async fn settle(lake: &Lake, d: &Ddl, me: &str) -> Result<()> {
    let (Ddl::Attach { name, .. } | Ddl::Detach { name, .. }) = d else { return Ok(()) };
    let want = matches!(d, Ddl::Attach { .. });
    for _ in 0..100 {
        if lake.cat.get::<Attachment>(&attachment_key(name)).await?.is_some() == want {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    sync(lake, me, true).await
}

/// `DROP TABLE`: the table leaves the catalog at once, and its files once they are a day old
/// (`tier::collect_orphans`). Refused while a view or task reads it or it is a view's own.
async fn drop_table(lake: &Lake, name: &str, if_exists: bool) -> Result<Value> {
    let Some(meta) = lake.cat.get::<TableMeta>(&table_key(name)).await? else {
        ensure!(if_exists, "no table {name}");
        return Ok(j!({"table": name, "dropped": false}));
    };
    let owner = name.strip_suffix("_final").unwrap_or(name);
    ensure!(lake.cat.get::<Value>(&crate::views::view_key(owner)).await?.is_none(), "{name} is materialized view {owner}'s: DROP MATERIALIZED VIEW {owner}");
    let mut readers: Vec<String> = vec![];
    for (k, v) in lake.cat.scan::<crate::views::View>("v/", "v0").await? {
        if v.source == name || reads(lake, &v.sql, name) {
            readers.push(format!("materialized view {}", &k[2..]));
        }
    }
    for (k, v) in lake.cat.scan::<StoredView>("q/", "q0").await? {
        if reads(lake, &v.sql, name) {
            readers.push(format!("view {}", &k[2..]));
        }
    }
    for (k, t) in lake.cat.scan::<crate::tasks::Task>("k/", "k0").await? {
        if t.source == name || t.target == name {
            readers.push(format!("task {}", &k[2..]));
        }
    }
    ensure!(readers.is_empty(), "{name} is used by {}: drop them first", readers.join(", "));
    for format in &meta.publish {
        crate::delta::unpublish(lake, name, format).await?; // (no copy left for other engines)
    }
    lake.cat.commit(vec![], &[table_key(name)]).await?;
    Ok(j!({"table": name, "dropped": true}))
}

/// `DROP VIEW` or `DROP MATERIALIZED VIEW`: a stored view, or a live one with its tables and state.
async fn drop_view(lake: &Lake, name: &str, if_exists: bool) -> Result<Value> {
    if lake.cat.get::<StoredView>(&query_key(name)).await?.is_some() {
        lake.cat.commit(vec![], &[query_key(name)]).await?;
        return Ok(j!({"view": name, "dropped": true}));
    }
    let Some(_) = lake.cat.get::<crate::views::View>(&crate::views::view_key(name)).await? else {
        ensure!(if_exists, "no view {name}");
        return Ok(j!({"view": name, "dropped": false}));
    };
    let mut gone = vec![crate::views::view_key(name), format!("w/{name}"), crate::store::producer_key(&format!("emit:{name}"))];
    for table in [name.to_string(), format!("{name}_final")] {
        if let Some(meta) = lake.cat.get::<TableMeta>(&table_key(&table)).await? {
            for format in &meta.publish {
                crate::delta::unpublish(lake, &table, format).await?;
            }
            gone.push(table_key(&table));
        }
    }
    lake.cat.commit(vec![], &gone).await?;
    Ok(j!({"view": name, "dropped": true}))
}

/// Does query `sql` read this lake's table `t`? (If it can't be parsed, a word match: cautious, so
/// a drop is refused rather than a view broken.)
fn reads(lake: &Lake, sql: &str, t: &str) -> bool {
    match crate::spmd::tables(sql) {
        Some(names) => names.iter().any(|n| local(lake, n).as_deref() == Some(t)),
        None => mentions(sql, t),
    }
}

/// Might `sql` name table `t`? A word match on its last part (a session registers the tables that
/// might be read: one too many costs nothing).
pub fn mentions(sql: &str, t: &str) -> bool {
    let last = split(t).1.to_lowercase();
    sql.to_lowercase().split(|c: char| !(c.is_alphanumeric() || c == '_' || c == '-')).any(|w| w == last)
}
