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

/// This lake's own name in three-part names: its folder's (or prefix's) last part (`mylake` of
/// `D:\data\mylake` too).
pub fn lake_name(lake: &Lake) -> String {
    lake.url.trim_end_matches(['/', '\\']).rsplit(['/', '\\']).next().unwrap_or("lake").to_lowercase()
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
    CreateDatabase { name: String, if_not_exists: bool, dir: Option<String> }, // a new lake (beside this one unless `dir`), attached
    AlterColumn { table: String, column: String, change: Change }, // ALTER TABLE … RENAME/DROP/ALTER COLUMN (ADR-022)
    RenameTable { name: String, to: String }, // (not yet: refused with the way round it)
    CreateRoutine { name: String, routine: crate::routines::Routine, replace: bool }, // CREATE MACRO, CREATE PROCEDURE (ADR-023)
    DropRoutine { name: String, if_exists: bool },
}

/// What `ALTER TABLE` does to a column: rename it, drop it, or widen its type (a SQL type).
#[derive(Serialize, Deserialize, Clone)]
#[serde(rename_all = "snake_case")]
pub enum Change {
    Rename(String),
    Drop { if_exists: bool },
    Type(String),
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
            let routines: Vec<String> = lake.cat.scan::<Value>("r/", "r0").await?.into_iter().filter(|(k, _)| inside(k, 2)).map(|(k, _)| k[2..].to_string()).collect();
            let tables: Vec<String> = lake.cat.scan::<TableMeta>("t/", "t0").await?.into_iter().filter(|(k, _)| inside(k, 2) && !crate::sys::hidden(k)).map(|(k, _)| k[2..].to_string()).collect();
            ensure!(cascade || (views.is_empty() && tables.is_empty() && routines.is_empty()), "schema {name} isn't empty ({}): drop them first, or DROP SCHEMA {name} CASCADE", [&views[..], &tables[..], &routines[..]].concat().join(", "));
            for r in &routines {
                crate::routines::drop(lake, r, true).await?;
            }
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
            let expanded = crate::routines::expand(lake, &sql).await?; // (kept as written: macros are read when it is used)
            let planned = crate::asof::rewrite(&expanded)?;
            crate::query::session(lake, &planned, "").await?.sql(&planned).await.context("the view's query")?; // (it plans)
            lake.cat.commit(vec![(query_key(&name), json(&StoredView { sql }))], &[]).await?;
            Ok(j!({"view": name}))
        }
        Ddl::CreateMaterialized { name, sql, options } => {
            let (emit, sessions, join) = crate::views::options(&options)?;
            let name = new_name(lake, &name).await?;
            ensure!(lake.cat.get::<StoredView>(&query_key(&name)).await?.is_none(), "{name} is a (stored) view");
            crate::views::create(lake, &name, &sql, emit, sessions, join).await?;
            crate::views::forget(lake); // (the sequencer holds flushes to it from its next commit)
            Ok(j!({"view": name, "materialized": true}))
        }
        Ddl::DropView { name, if_exists } => drop_view(lake, &name, if_exists).await,
        Ddl::Attach { name, dir } => {
            check(&name)?;
            ensure!(name != lake_name(lake), "this lake is called {name}: attach the other under another name");
            ensure!(!has_schema(lake, &name).await?, "a schema here is called {name}: attach the other under another name");
            if let Some(a) = lake.cat.get::<Attachment>(&attachment_key(&name)).await? {
                ensure!(a.dir == full(&dir)?, "{name} is attached already, to {}", a.dir);
                return Ok(j!({"attached": name, "dir": a.dir, "unchanged": true}));
            }
            ensure!(!lake.attached.read().unwrap().iter().any(|(n, _)| *n == name), "{name} is attached already (--attach, or found beside this lake by the shell)");
            ensure!(full(&dir)? != lake.url, "{dir} is this lake");
            let (dir, created) = lake_dir(&dir).await?;
            lake.cat.commit(vec![(attachment_key(&name), json(&Attachment { dir: dir.clone() }))], &[]).await?;
            Ok(match created {
                true => j!({"attached": name, "dir": dir, "created": true}),
                false => j!({"attached": name, "dir": dir}),
            })
        }
        Ddl::CreateDatabase { name, if_not_exists, dir } => {
            check(&name)?;
            let dir = dir.unwrap_or_else(|| beside(&lake.url, &name));
            if has_catalog(&full(&dir)?).await? {
                ensure!(if_not_exists, "a lake is at {dir} already: ATTACH '{dir}' AS {name} (or CREATE DATABASE IF NOT EXISTS {name})");
            }
            Box::pin(apply(lake, Ddl::Attach { name, dir })).await
        }
        Ddl::AlterColumn { table, column, change } => alter_column(lake, &table, &column, change).await,
        // A table's name is where its files, log rows, Delta and Iceberg copies and Kafka topic are:
        // renaming one is a new table (ADR-022).
        Ddl::RenameTable { name, to } => bail!("ALTER TABLE … RENAME TO isn't supported yet: a table's name is where its files, log and Delta and Iceberg copies live. CREATE TABLE {to} AS SELECT * FROM {name}; then DROP TABLE {name}; does it"),
        Ddl::CreateRoutine { name, routine, replace } => crate::routines::create(lake, &name, routine, replace).await,
        Ddl::DropRoutine { name, if_exists } => crate::routines::drop(lake, &name, if_exists).await,
        Ddl::Detach { name, if_exists } => {
            if lake.cat.get::<Attachment>(&attachment_key(&name)).await?.is_none() {
                let flag = lake.attached.read().unwrap().iter().any(|(n, _)| *n == name);
                ensure!(if_exists && !flag, "{name} {}", if flag { "was attached when the node started (--attach, or found beside this lake by the shell): it stays" } else { "isn't attached" });
                return Ok(j!({"detached": name, "unchanged": true}));
            }
            lake.cat.commit(vec![], &[attachment_key(&name)]).await?;
            Ok(j!({"detached": name}))
        }
    }
}

/// Where a lake to attach is, as its nodes open it: a bucket's URL, or a folder's full path (a
/// relative one would mean something else on every machine); and whether it was made just now.
/// Nothing there yet (no catalog): a new, empty lake is made there, as DuckDB's ATTACH makes a
/// new database file.
async fn lake_dir(dir: &str) -> Result<(String, bool)> {
    if !dir.contains("://") {
        std::fs::create_dir_all(dir).with_context(|| format!("no folder {dir}"))?;
    }
    let dir = full(dir)?;
    if has_catalog(&dir).await? {
        return Ok((dir, false));
    }
    crate::inbox::lead_once(&dir).await?; // (whoever leads a lake first makes it)
    ensure!(has_catalog(&dir).await?, "couldn't make a lake at {dir}");
    Ok((dir, true))
}

/// A lake's place as its nodes name it: a bucket's URL as it is, a folder's full path.
fn full(dir: &str) -> Result<String> {
    Ok(match dir.contains("://") {
        true => dir.trim_end_matches('/').to_string(),
        false => match std::fs::canonicalize(dir) {
            Ok(p) => p.to_string_lossy().trim_start_matches(r"\\?\").to_string(), // (as `store::open_store` has it)
            Err(_) => std::path::absolute(dir)?.to_string_lossy().to_string(), // (not there yet)
        },
    })
}

async fn has_catalog(dir: &str) -> Result<bool> {
    if !dir.contains("://") && !std::path::Path::new(dir).exists() {
        return Ok(false);
    }
    let store = crate::store::open_store(dir)?.1;
    Ok(futures::StreamExt::next(&mut store.list(Some(&object_store::path::Path::from("catalog")))).await.transpose()?.is_some())
}

/// Lake `name` in the folder (or prefix) this lake's is in.
fn beside(url: &str, name: &str) -> String {
    match url.contains("://") {
        true => format!("{}/{name}", url.trim_end_matches('/').rsplit_once('/').map_or(url, |(p, _)| p)),
        false => std::path::Path::new(url).parent().unwrap_or(std::path::Path::new(".")).join(name).to_string_lossy().to_string(),
    }
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

/// The lakes in `folder` (its subfolders that hold one), attached under their folder names for as
/// long as this node runs: the shell's own folder, so the lakes side by side are its databases, as
/// a database server's are (ADR-024). Nothing is written to the catalog. Left out: this lake, a
/// name the catalog attaches (that ATTACH says where), a schema's name here, and a lake that won't
/// open (said on the log). The names attached.
pub async fn attach_found(home: &Lake, folder: &str, me: &str) -> Vec<String> {
    let mut dirs: Vec<_> = std::fs::read_dir(folder).into_iter().flatten().flatten().map(|e| e.path()).filter(|p| p.is_dir()).collect();
    dirs.sort();
    let mut found = vec![];
    for path in dirs {
        let (Some(name), Ok(dir)) = (path.file_name().and_then(|n| n.to_str()).map(str::to_lowercase), full(&path.to_string_lossy())) else { continue };
        let taken = dir == home.url || check(&name).is_err() || name == lake_name(home)
            || home.attached.read().unwrap().iter().any(|(n, _)| *n == name)
            || !matches!(home.cat.get::<Attachment>(&attachment_key(&name)).await, Ok(None))
            || has_schema(home, &name).await.unwrap_or(true);
        if taken || !has_catalog(&dir).await.unwrap_or(false) {
            continue;
        }
        match attach(home, &name, &dir, me, true, false).await {
            Ok(()) => found.push(name),
            Err(e) => eprintln!("attaching {name} ({dir}), found in {folder}: {e:#}"),
        }
    }
    found
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

/// After this node sent an `ATTACH` or `DETACH`: once its catalog shows it, as it does here. After
/// `CREATE MATERIALIZED VIEW`: once it is filled from the rows already there (`views::fill_all`).
pub async fn settle(lake: &Lake, d: &Ddl, me: &str) -> Result<()> {
    if let Ddl::CreateMaterialized { name, .. } = d {
        let name = local(lake, name).unwrap_or_default();
        let filled = crate::store::producer_key(&format!("fill:{name}"));
        for _ in 0..12_000 {
            let view = lake.cat.get::<crate::views::View>(&crate::views::view_key(&name)).await?;
            if view.is_none_or(|v| v.fill.is_none()) || lake.cat.get::<u64>(&filled).await?.is_some() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        return Ok(());
    }
    if let Ddl::AlterColumn { table, column, change } = d {
        // (a write to this node right after uses the new names: it must know them)
        let key = table_key(&local(lake, table).unwrap_or_default());
        for _ in 0..200 {
            let Some(m) = lake.cat.get::<TableMeta>(&key).await? else { break };
            let done = match change {
                Change::Rename(to) => m.stored(to).is_some(),
                Change::Drop { .. } => m.stored(column).is_none(),
                Change::Type(_) => true, // (the same names either way)
            };
            if done {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        return Ok(());
    }
    if let Ddl::CreateRoutine { name, .. } | Ddl::DropRoutine { name, .. } = d {
        // (a statement sent here right after may use it, or expect it gone)
        let (key, want) = (crate::routines::key(&local(lake, name).unwrap_or_default()), match d {
            Ddl::CreateRoutine { routine, .. } => Some(routine.clone()),
            _ => None,
        });
        for _ in 0..200 {
            if lake.cat.get::<crate::routines::Routine>(&key).await? == want {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        return Ok(());
    }
    let (Ddl::Attach { name, .. } | Ddl::Detach { name, .. } | Ddl::CreateDatabase { name, .. }) = d else { return Ok(()) };
    let want = !matches!(d, Ddl::Detach { .. });
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
    let readers = readers(lake, name).await?;
    ensure!(readers.is_empty(), "{name} is used by {}: drop them first", readers.join(", "));
    for format in &meta.publish {
        crate::delta::unpublish(lake, name, format).await?; // (no copy left for other engines)
    }
    lake.cat.commit(vec![], &[table_key(name), table_key(&crate::sys::deleted(name))]).await?; // (and its replaced rows)
    Ok(j!({"table": name, "dropped": true}))
}

/// What reads or writes table `name` by its columns: views, stored views, tasks.
async fn readers(lake: &Lake, name: &str) -> Result<Vec<String>> {
    let mut out: Vec<String> = vec![];
    for (k, v) in lake.cat.scan::<crate::views::View>("v/", "v0").await? {
        if v.source == name || reads(lake, &v.sql, name) {
            out.push(format!("materialized view {}", &k[2..]));
        }
    }
    for (k, v) in lake.cat.scan::<StoredView>("q/", "q0").await? {
        if reads(lake, &v.sql, name) {
            out.push(format!("view {}", &k[2..]));
        }
    }
    for (k, t) in lake.cat.scan::<crate::tasks::Task>("k/", "k0").await? {
        if t.source == name || t.target == name {
            out.push(format!("task {}", &k[2..]));
        }
    }
    Ok(out)
}

/// `ALTER TABLE t RENAME COLUMN a TO b`, `DROP COLUMN a`, `ALTER COLUMN a TYPE BIGINT`: the
/// catalog changes, the files never do (ADR-022). A column keeps the name it was written under
/// (`TableMeta::columns`); `names` says what SQL calls it, `dropped` that nothing reads it again.
/// A type only widens (what every file holds still reads as it). Refused while a view or task
/// names the table's columns: it would break.
async fn alter_column(lake: &Lake, table: &str, column: &str, change: Change) -> Result<Value> {
    let name = local(lake, table).ok_or_else(|| anyhow::anyhow!("{table} is an attached lake's: ALTER it from a node of that lake"))?;
    let Some(mut m) = lake.cat.get::<TableMeta>(&table_key(&name)).await? else { anyhow::bail!("no table {name}") };
    let owner = name.strip_suffix("_final").unwrap_or(&name);
    ensure!(lake.cat.get::<Value>(&crate::views::view_key(owner)).await?.is_none(), "{name} is materialized view {owner}'s: its columns are its query's");
    let readers = readers(lake, &name).await?;
    ensure!(readers.is_empty(), "{name} is used by {}, which name its columns: drop them first", readers.join(", "));
    let system = |c: &str| crate::sys::NAMES.contains(&c) || c == "_old_version" || c == "_deleted" || c.contains('~');
    ensure!(!system(column), "{column} is a system column: it stays as it is");
    let Some(stored) = m.stored(column).map(str::to_string) else {
        ensure!(matches!(change, Change::Drop { if_exists: true }), "{name} has no column {column}");
        return Ok(j!({"table": name, "unchanged": true}));
    };
    let partition = m.partition.as_deref().map(|p| p.split_once('(').map_or(p, |(_, c)| c.trim_end_matches(')')));
    let what = match change {
        Change::Rename(to) => {
            ensure!(!to.is_empty(), "a column needs a name");
            ensure!(!system(&to), "{to} is a system column's name");
            ensure!(m.stored(&to).is_none(), "{name} already has a column {to}");
            match to == stored {
                true => m.names.remove(&stored),
                false => m.names.insert(stored.clone(), to.clone()),
            };
            j!({"renamed": column, "to": to})
        }
        Change::Drop { .. } => {
            let role = match () {
                _ if m.key.contains(&stored) => "key",
                _ if partition == Some(stored.as_str()) => "partition_by",
                _ if m.ttl.as_ref().is_some_and(|(c, _)| *c == stored) => "ttl",
                _ if m.order.as_ref() == Some(&stored) => "order_by",
                _ => "",
            };
            ensure!(role.is_empty(), "{column} is {name}'s {role} column: it stays");
            ensure!(m.live().count() > 1 + usize::from(m.stored("_deleted").is_some()), "{column} is {name}'s last column (DROP TABLE {name})");
            m.cluster.retain(|c| *c != stored);
            m.merge.remove(&stored);
            m.names.remove(&stored);
            m.dropped.push(stored.clone());
            j!({"dropped": column})
        }
        Change::Type(sql_type) => {
            let ctx = datafusion::prelude::SessionContext::new();
            ctx.sql(&format!("CREATE TABLE t (c {sql_type})")).await?;
            let new = crate::write::stored(ctx.table("t").await?.schema().field(0).data_type());
            let at = m.columns.iter().position(|(c, _)| *c == stored).expect("a live column");
            let old = crate::query::dtype(&m.columns[at].1)?;
            if old == new {
                return Ok(j!({"table": name, "unchanged": true}));
            }
            ensure!(widens(&old, &new), "{column} is {old}: a column's type can only widen (a smaller integer to a bigger one, FLOAT to DOUBLE, a DECIMAL to more digits), which every file already written still reads as");
            ensure!(partition != Some(stored.as_str()), "{column} is {name}'s partition_by column: its type stays");
            m.columns[at].1 = crate::query::type_name(&new);
            j!({"column": column, "type": m.columns[at].1})
        }
    };
    let mut puts = vec![(table_key(&name), json(&m))];
    let del = crate::sys::deleted(&name);
    if let Some(mut d) = lake.cat.get::<TableMeta>(&table_key(&del)).await? {
        // (its replaced rows' table follows: the same columns, before its `_old_version`)
        d.columns = m.columns.iter().cloned().chain([("_old_version".to_string(), "Int64".to_string())]).collect();
        (d.names, d.dropped) = (m.names.clone(), m.dropped.clone());
        puts.push((table_key(&del), json(&d)));
    }
    lake.cat.commit(puts, &[]).await?;
    Ok(j!({"table": name, "altered": what}))
}

/// Does every value of type `old` read as `new`, exactly? The widenings Iceberg and Delta allow.
fn widens(old: &datafusion::arrow::datatypes::DataType, new: &datafusion::arrow::datatypes::DataType) -> bool {
    use datafusion::arrow::datatypes::DataType::*;
    let int = |t: &datafusion::arrow::datatypes::DataType| match t {
        Int8 => Some(8),
        Int16 => Some(16),
        Int32 => Some(32),
        Int64 => Some(64),
        _ => None,
    };
    match (old, new) {
        (Float16 | Float32, Float64) | (Float16, Float32) => true,
        (Decimal128(p, s), Decimal128(q, t)) => q > p && s == t,
        _ => matches!((int(old), int(new)), (Some(a), Some(b)) if b > a),
    }
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
    let producers = ["emit", "join", "fill"].map(|p| format!("{p}:{name}"));
    let mut gone: Vec<String> = [crate::views::view_key(name), format!("w/{name}")].into_iter().chain(producers.iter().map(|p| crate::store::producer_key(p))).collect();
    for table in [name.to_string(), format!("{name}_final")] {
        if let Some(meta) = lake.cat.get::<TableMeta>(&table_key(&table)).await? {
            for format in &meta.publish {
                crate::delta::unpublish(lake, &table, format).await?;
            }
            gone.push(table_key(&table));
        }
    }
    lake.cat.commit(vec![], &gone).await?;
    crate::log::forget_producers(producers); // (a view made again under this name starts over)
    crate::views::forget(lake);
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
