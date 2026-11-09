//! A dropped definition, kept to be undropped (reviews/undrop-every-kind-2026-11-08.md, approved 11-08): a view, an
//! external table, a function, macro or procedure, a task, a type, a secret or a connection. A drop sets the object's
//! records aside as they were, with the comments on it, in one record under `dk/{family}/{name}/{at}`, in the drop's
//! own commit, so a drop that leaves no record can't be. `UNDROP <kind> name` puts the newest back, under the rules a
//! table's has (ADR-043): refused while anything of its family has the name (rename or drop that, then undrop again,
//! to reach an older copy), and needing the right its DROP needed. A definition holds no rows, so it is kept for the
//! default `ddl::KEEP_MS` (7 days); then `tier::expire` lets it go. A table's own record is `ddl::Dropped` (`dt/`),
//! which keeps its files too. An older release reads no `dk/` record: it leaves them, and they expire once this one
//! leads again. A secret comes back still sealed, and nothing shows a record's values: `dk/` is no page's
//! (`objects::readable`), and a branch never takes one.
use crate::store::{json, Lake};
use anyhow::{Context, Result};
use object_store::ObjectStoreExt as _;
use serde::{Deserialize, Serialize};
use serde_json::{json as j, Value};

pub const PREFIX: &str = "dk/";

/// This module's records.
pub const PREFIXES: &[crate::records::Prefix] =
    &[crate::records::Prefix { prefix: PREFIX, owner: "undrop", holds: "a dropped definition (view, routine, task, type, secret, connection), kept while it can be undropped (dk/family/name/at)" }];

/// One dropped definition: its records as they were, by key.
#[derive(Serialize, Deserialize)]
pub struct Kept {
    pub at_ms: u64,
    pub keep_ms: u64,
    /// Its kind, as SQL names it (`view`).
    pub kind: String,
    /// Who dropped it (`auth::name`).
    pub by: String,
    /// Its records as the drop found them: its own, and the comments on it and on its columns.
    pub records: Vec<(String, Value)>,
    /// What its drop took with it, as (kind, name), each kept in its own record (a table's `dt/`): a schema's tables, views
    /// and routines, dropped with CASCADE. UNDROP brings them back with it.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub with: Vec<(String, String)>,
    /// The statement that makes it again, for a kind whose rows are worked out from others' (a materialized view): run
    /// when it comes back, so its rows are its sources' as they are then.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub made: String,
}

impl Kept {
    /// When it is let go (ms since the epoch).
    pub fn until(&self) -> u64 { self.at_ms.saturating_add(self.keep_ms) }
}

/// A record's times alone, read without its records.
#[derive(Deserialize)]
struct Times {
    at_ms: u64,
    keep_ms: u64,
}

fn key(family: &str, name: &str, at_ms: u64) -> String { format!("{PREFIX}{family}/{name}/{at_ms:020}") }

fn family(kind: &str) -> Result<&'static str> {
    crate::objects::kind(kind).map(|k| k.family).ok_or_else(|| anyhow::anyhow!("no kind {kind}"))
}

/// The puts that keep `name`, of `kind`, as `keys` hold it now, with the comments on it: they go in the drop's own commit.
/// A key that holds nothing is left out.
pub async fn aside(lake: &Lake, kind: &str, name: &str, keys: &[String]) -> Result<Vec<(String, Vec<u8>)>> {
    aside_with(lake, kind, name, keys, vec![], crate::ddl::KEEP_MS).await
}

/// The same, for a drop that took `with` along (kept for `keep_ms`: as long as the longest kept of them, and never less
/// than the default).
pub async fn aside_with(lake: &Lake, kind: &str, name: &str, keys: &[String], with: Vec<(String, String)>, keep_ms: u64) -> Result<Vec<(String, Vec<u8>)>> {
    record(lake, kind, name, keys, with, keep_ms, String::new()).await
}

/// The puts that keep `name`, of `kind`, as the statement `made` that makes it again, with the comments on it.
pub async fn aside_made(lake: &Lake, kind: &str, name: &str, made: String) -> Result<Vec<(String, Vec<u8>)>> {
    record(lake, kind, name, &[], vec![], crate::ddl::KEEP_MS, made).await
}

async fn record(lake: &Lake, kind: &str, name: &str, keys: &[String], with: Vec<(String, String)>, keep_ms: u64, made: String) -> Result<Vec<(String, Vec<u8>)>> {
    let family = family(kind)?;
    let mut records = vec![];
    for k in keys.iter().cloned().chain([format!("cm/{family}/{name}")]) {
        if let Some(v) = lake.cat.get::<Value>(&k).await? {
            records.push((k, v));
        }
    }
    if family == "relation" {
        records.extend(lake.cat.scan::<Value>(&format!("cm/column/{name}/"), &format!("cm/column/{name}0")).await?); // (its columns')
    }
    // (later than the name's last kept copy, so two drops in one millisecond keep two)
    let last = lake.cat.scan::<Times>(&format!("{PREFIX}{family}/{name}/"), &format!("{PREFIX}{family}/{name}0")).await?.last().map(|(_, t)| t.at_ms);
    let at_ms = crate::log::now_ms().max(last.map_or(0, |l| l + 1));
    let kept = Kept { at_ms, keep_ms: keep_ms.max(crate::ddl::KEEP_MS), kind: kind.to_string(), by: crate::auth::name(), records, with, made };
    Ok(vec![(key(family, name, at_ms), json(&kept))])
}

/// Does UNDROP of `asked` bring back what was kept as `kept`? The same kind, or any routine for any routine: DROP FUNCTION
/// drops a procedure or a macro under the name too.
fn same(asked: &str, kept: &str) -> bool { asked == kept || [asked, kept].iter().all(|k| family(k).is_ok_and(|f| f == "routine")) }

/// The kept copies of `name` that UNDROP `kind` would bring back, oldest first, by key.
async fn copies(lake: &Lake, kind: &str, name: &str) -> Result<Vec<(String, Kept)>> {
    let family = family(kind)?;
    let mut kept = lake.cat.scan::<Kept>(&format!("{PREFIX}{family}/{name}/"), &format!("{PREFIX}{family}/{name}0")).await?;
    kept.retain(|(_, k)| same(kind, &k.kind));
    Ok(kept)
}

/// Is a copy of `name`, of `kind`, kept?
pub async fn has(lake: &Lake, kind: &str, name: &str) -> Result<bool> { Ok(!copies(lake, kind, name).await?.is_empty()) }

/// The copy of `name`, of `kind`, that UNDROP would bring back: the newest.
pub async fn newest(lake: &Lake, kind: &str, name: &str) -> Result<Option<Kept>> { Ok(copies(lake, kind, name).await?.pop().map(|(_, k)| k)) }

/// `UNDROP <kind> name`: the copy of `name` dropped last, back as it was, the comments on it too. One kept as the
/// statement that makes it is made again, planned for `scope` (the asker's, as its CREATE would be).
pub async fn undrop(lake: &Lake, kind: &str, name: &str, scope: &crate::rights::Scope) -> Result<Value> {
    let family = family(kind)?;
    let word = kind.to_uppercase();
    let relation = family == "relation";
    let Some((at, kept)) = copies(lake, kind, name).await?.pop() else {
        let code = match family {
            "relation" => "42P01",
            "routine" => "42883",
            _ => "42704",
        };
        return Err(crate::codes::coded(code, format!("no dropped {kind} {name} is kept (pondra.dropped lists those that are)")));
    };
    // (anything of its family under the name, or any record it would put back: a table, a view and a sequence share names)
    let mut held = vec![];
    for k in crate::objects::all_kinds().filter(|k| k.family == family) {
        held.push(format!("{}{name}", k.prefix));
    }
    held.extend(kept.records.iter().map(|(k, _)| k.clone()).filter(|k| !k.starts_with("cm/")));
    for k in held {
        if lake.cat.get::<Value>(&k).await?.is_some() {
            let code = if relation { "42P07" } else { "42710" };
            return Err(crate::codes::coded(code, format!("{name} exists: drop or rename it, then UNDROP {word} {name}")));
        }
    }
    let shown = kept.kind.clone();
    if !kept.made.is_empty() {
        let Some(crate::write::Stmt::Ddl(ds)) = crate::write::parse(&kept.made) else {
            anyhow::bail!("{name}'s definition doesn't read back as a statement: {}", kept.made);
        };
        for d in ds {
            Box::pin(crate::ddl::apply(lake, d, scope)).await.with_context(|| format!("making {kind} {name} again"))?;
        }
    }
    let puts = kept.records.into_iter().map(|(k, v)| (k, json(&v))).collect();
    lake.cat.commit(puts, &[at]).await?;
    let mut out = j!({shown: name, "undropped": true});
    if !kept.with.is_empty() {
        let (back, left) = members(lake, &kept.with, scope).await;
        out["with"] = j!(back);
        if !left.is_empty() {
            out["left"] = j!(left);
        }
    }
    Ok(out)
}

/// What a drop took along, brought back after it: tables first, then routines, then what reads them (a schema's views,
/// a materialized view made again from its query). Each that is still kept comes back, and the answer says why each other
/// one isn't (its time was up, or its name is taken).
async fn members(lake: &Lake, with: &[(String, String)], scope: &crate::rights::Scope) -> (Vec<String>, Vec<Value>) {
    let (mut back, mut left) = (vec![], vec![]);
    let mut with: Vec<&(String, String)> = with.iter().collect();
    with.sort_by_key(|(kind, _)| match kind.as_str() {
        "table" => 0,
        k if family(k).is_ok_and(|f| f == "routine") => 1,
        _ => 2,
    });
    for (kind, name) in with {
        let out = match kind.as_str() {
            "table" => Box::pin(crate::ddl::undrop(lake, name, scope)).await, // (boxed: UNDROP TABLE may bring back a kept external table)
            _ => Box::pin(undrop(lake, kind, name, scope)).await,
        };
        match out {
            Ok(_) => back.push(format!("{kind} {name}")),
            Err(e) => left.push(j!({"kind": kind, "name": name, "why": format!("{e:#}")})),
        }
    }
    (back, left)
}

/// The name UNDROP `kind` reads `name` as: placed in its schema, which must be there, for a kind that has schemas (a
/// relation, a routine, a task, a type); as written for one that hasn't (a secret, a connection).
pub async fn named(lake: &Lake, kind: &str, name: &str) -> Result<String> {
    match family(kind)? {
        "relation" | "routine" | "task" | "type" => crate::ddl::new_name(lake, name).await,
        _ => Ok(name.to_string()),
    }
}

/// Every kept definition: (its name, what was kept), oldest first for each name.
pub async fn all(lake: &Lake) -> Result<Vec<(String, Kept)>> {
    let name = |k: &str| {
        let rest = k[PREFIX.len()..].split_once('/').map_or("", |(_, r)| r);
        rest.rsplit_once('/').map_or(rest, |(n, _)| n).to_string()
    };
    Ok(lake.cat.scan::<Kept>(PREFIX, "dk0").await?.into_iter().map(|(k, d)| (name(&k), d)).collect())
}

/// The keys of the kept definitions whose time is up at `now`, for `tier::expire` to delete.
pub async fn expired(lake: &Lake, now: u64) -> Result<Vec<String>> {
    Ok(lake.cat.scan::<Times>(PREFIX, "dk0").await?.into_iter().filter(|(_, t)| now >= t.at_ms.saturating_add(t.keep_ms)).map(|(k, _)| k).collect())
}

/// The statement that brings it back, as `pondra.dropped` says it.
pub fn statement(kind: &str, name: &str) -> String { format!("UNDROP {} {}", kind.to_uppercase(), crate::objects::name_sql(name)) }

// A dropped database: its folder stays as it was, under a mark at its root (`LAKE_MARK`), out of every listing of
// databases (`ddl::lakes_in`) and refused to every open by name, until UNDROP DATABASE takes the mark off, or a new
// database takes its name (it is let go then, and that CREATE says so), or its time is up: `pondra serve`'s sweep
// (`sweep_lakes`) deletes it then, and for a branch its base's hourly one too (`branch::sweep`), whose pin keeps the
// base's files for it until then. The mark is the truth, so every node of a server sees it. An older release reads no
// mark: it serves such a database again as if it weren't dropped, and nothing of it is lost.

/// The mark a dropped database's folder holds at its root.
pub const LAKE_MARK: &str = "dropped.json";

/// What a dropped database's mark says.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct KeptLake {
    pub at_ms: u64,
    pub keep_ms: u64,
    /// Who dropped it (`auth::name`).
    pub by: String,
}

impl KeptLake {
    /// When it is let go (ms since the epoch).
    pub fn until(&self) -> u64 { self.at_ms.saturating_add(self.keep_ms) }
}

fn mark() -> object_store::path::Path { object_store::path::Path::from(LAKE_MARK) }

/// A time (ms since the epoch) as a message says it.
pub fn when(ms: u64) -> String {
    chrono::DateTime::from_timestamp_millis(ms as i64).map(|d| d.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)).unwrap_or_default()
}

/// The mark on the lake at `dir`, if it was dropped (None for a folder that isn't there).
pub async fn lake_kept(dir: &str) -> Result<Option<KeptLake>> {
    if (!dir.contains("://") && !std::path::Path::new(dir).is_dir()) || (crate::memory_lake::in_memory(dir) && !crate::memory_lake::memory_held(dir)) {
        return Ok(None); // (as `ddl::has_catalog`: asking a lake in memory that nothing here opened would make one)
    }
    match crate::store::open_store(dir)?.1.get(&mark()).await {
        Ok(got) => Ok(Some(serde_json::from_slice(&got.bytes().await?).with_context(|| format!("reading {dir}/{LAKE_MARK}"))?)),
        Err(object_store::Error::NotFound { .. }) => Ok(None),
        Err(e) => Err(anyhow::Error::new(e).context(format!("reading {dir}/{LAKE_MARK}"))),
    }
}

/// `DROP DATABASE`: the lake at `dir` marked dropped by `by`, kept for the default `ddl::KEEP_MS`. Refused while a
/// process leads it, as its delete is (`ddl::unled`): its node is stopped first.
pub async fn keep_lake(dir: &str, by: &str) -> Result<KeptLake> {
    crate::ddl::unled(dir).await?;
    let kept = KeptLake { at_ms: crate::log::now_ms(), keep_ms: crate::ddl::KEEP_MS, by: by.to_string() };
    crate::store::open_store(dir)?.1.put(&mark(), serde_json::to_vec(&kept)?.into()).await.with_context(|| format!("marking {dir} dropped"))?;
    Ok(kept)
}

/// `UNDROP DATABASE`: the mark taken off the lake at `dir`, a database again as it was.
pub async fn unkeep_lake(dir: &str) -> Result<()> {
    crate::store::open_store(dir)?.1.delete(&mark()).await.with_context(|| format!("taking {dir}'s drop back"))?;
    Ok(())
}

/// The dropped databases in `folder`: (name, where, its mark), by name.
pub async fn lakes_kept(folder: &str) -> Result<Vec<(String, String, KeptLake)>> {
    let mut out = vec![];
    for (name, dir) in crate::ddl::lakes_found(folder).await? {
        if let Some(k) = lake_kept(&dir).await? {
            out.push((name, dir, k));
        }
    }
    Ok(out)
}

/// `pondra serve`'s sweep: the dropped databases in `folder` whose time is up at `now`, deleted. The names let go.
pub async fn sweep_lakes(folder: &str, now: u64) -> Vec<String> {
    let mut gone = vec![];
    let kept = lakes_kept(folder).await.unwrap_or_else(|e| {
        eprintln!("listing the dropped databases in {folder}: {e:#}");
        vec![]
    });
    for (name, dir, _) in kept.into_iter().filter(|(_, _, k)| now >= k.until()) {
        match crate::ddl::delete_lake(&dir).await {
            Ok(()) => gone.push(name),
            Err(e) => eprintln!("letting dropped database {name} go ({dir}): {e:#}"),
        }
    }
    gone
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn lake() -> std::sync::Arc<Lake> {
        Lake::open(&format!("memory://undrop-{}", uuid::Uuid::new_v4().simple()), true, false, crate::node_state::NodeState::unhosted()).await.unwrap()
    }

    /// Statements through the core's one path: `write::parse`, then the leader's `ddl::apply`, as the operator's.
    async fn run(lake: &Lake, sql: &str) -> Result<Value> {
        let node = crate::auth::Principal::node().scope;
        let mut out = Value::Null;
        match crate::write::parse(sql) {
            Some(crate::write::Stmt::Ddl(ds)) => {
                for d in ds {
                    out = crate::ddl::apply(lake, d, &node).await?;
                }
            }
            _ => anyhow::bail!("{sql}: not a statement of the catalog's"),
        }
        Ok(out)
    }

    #[tokio::test]
    async fn each_definition_comes_back_as_it_was() {
        crate::auth::as_node(each_definition()).await
    }

    async fn each_definition() {
        let lake = lake().await;
        crate::write::create_table(&lake, "t", r#"{"columns": [["a", "Int64"]]}"#).await.unwrap();
        for (make, drop, undrop, key) in [
            ("CREATE VIEW v AS SELECT a FROM t", "DROP VIEW v", "UNDROP VIEW v", "q/v"),
            ("CREATE FUNCTION twice(x INT) RETURNS INT AS 'x * 2'", "DROP FUNCTION twice", "UNDROP FUNCTION twice", "r/twice"),
            ("CREATE TYPE mood AS ENUM ('sad', 'ok')", "DROP TYPE mood", "UNDROP TYPE mood", "ty/mood"),
            ("CREATE TASK nightly SCHEDULE '1 day' AS SELECT 1", "DROP TASK nightly", "UNDROP TASK nightly", "j/nightly"),
            ("CREATE MACRO plus_one(x) AS x + 1", "DROP MACRO plus_one", "UNDROP MACRO plus_one", "r/plus_one"),
            ("CREATE SECRET k (TYPE s3, KEY_ID 'a', SECRET 'b')", "DROP SECRET k", "UNDROP SECRET k", "e/k"),
            ("CREATE CONNECTION landing TYPE s3 WITH (url = 's3://bucket/incoming')", "DROP CONNECTION landing", "UNDROP CONNECTION landing", "cn/landing"),
        ] {
            run(&lake, make).await.unwrap_or_else(|e| panic!("{make}: {e:#}"));
            let was: Value = lake.cat.get(key).await.unwrap().unwrap();
            let name = &key[key.find('/').unwrap() + 1..];
            let listed = crate::objects::list(&lake, &crate::auth::Principal::node().scope).await.unwrap();
            let kind = listed.iter().find(|o| o.name == name).map(|o| o.kind).unwrap_or_else(|| panic!("{make}: listed"));
            run(&lake, drop).await.unwrap_or_else(|e| panic!("{drop}: {e:#}"));
            assert!(lake.cat.get::<Value>(key).await.unwrap().is_none(), "{drop} drops it");
            let kept = all(&lake).await.unwrap();
            assert_eq!(kept.iter().map(|(n, k)| (n.as_str(), k.kind.as_str())).collect::<Vec<_>>(), vec![(name, kind)], "{drop} keeps one record, of the kind it is listed as");
            let out = run(&lake, undrop).await.unwrap_or_else(|e| panic!("{undrop}: {e:#}"));
            assert_eq!(out["undropped"], true, "{undrop}: {out}");
            assert_eq!(lake.cat.get::<Value>(key).await.unwrap(), Some(was), "{undrop} brings it back as it was");
            assert!(all(&lake).await.unwrap().is_empty(), "{undrop} takes its record");
            let again = run(&lake, undrop).await.unwrap_err();
            assert!(format!("{again:#}").contains("pondra.dropped"), "{undrop} again: {again:#}");
        }
    }

    #[tokio::test]
    async fn the_newest_comes_back_and_a_taken_name_is_refused() {
        let lake = lake().await;
        run(&lake, "CREATE VIEW v AS SELECT 1 AS one").await.unwrap();
        run(&lake, "COMMENT ON VIEW v IS 'the first'").await.unwrap();
        run(&lake, "DROP VIEW v").await.unwrap();
        run(&lake, "CREATE VIEW v AS SELECT 2 AS two").await.unwrap();
        let refused = format!("{:#}", run(&lake, "UNDROP VIEW v").await.unwrap_err());
        assert!(refused.contains("v exists"), "{refused}");
        run(&lake, "DROP VIEW v").await.unwrap();
        run(&lake, "UNDROP VIEW v").await.unwrap();
        let v: crate::ddl::StoredView = lake.cat.get("q/v").await.unwrap().unwrap();
        assert!(v.sql.contains('2'), "the newest comes back: {}", v.sql);
        run(&lake, "ALTER VIEW v RENAME TO w").await.unwrap(); // (renamed away, the name is free for the older copy)
        run(&lake, "UNDROP VIEW v").await.unwrap();
        let v: crate::ddl::StoredView = lake.cat.get("q/v").await.unwrap().unwrap();
        assert!(v.sql.contains('1'), "then the older: {}", v.sql);
        assert_eq!(lake.cat.get::<String>("cm/relation/v").await.unwrap().as_deref(), Some("the first"), "with its comment");
        // (a table under the name holds it too)
        run(&lake, "DROP VIEW v").await.unwrap();
        crate::write::create_table(&lake, "v", r#"{"columns": [["a", "Int64"]]}"#).await.unwrap();
        assert!(format!("{:#}", run(&lake, "UNDROP VIEW v").await.unwrap_err()).contains("v exists"));
    }

    /// A schema dropped with CASCADE is kept with what it held, and UNDROP SCHEMA brings all of it back. Until then UNDROP
    /// of a name in it is refused: the schema is not there. (Its tables, which a leader sends to files first, are
    /// `tools/history_check.py`'s.)
    #[tokio::test]
    async fn a_schema_dropped_with_cascade_comes_back_with_what_it_held() {
        let lake = lake().await;
        run(&lake, "CREATE SCHEMA s").await.unwrap();
        run(&lake, "CREATE VIEW s.v AS SELECT 1 AS a").await.unwrap();
        run(&lake, "CREATE MACRO s.m(x) AS x + 1").await.unwrap();
        run(&lake, "COMMENT ON SCHEMA s IS 'sales'").await.unwrap();
        let view_was: Value = lake.cat.get("q/s.v").await.unwrap().unwrap();
        let macro_was: Value = lake.cat.get("r/s.m").await.unwrap().unwrap();
        run(&lake, "DROP SCHEMA s CASCADE").await.unwrap();
        assert!(lake.cat.get::<Value>("ns/s").await.unwrap().is_none(), "the schema is dropped");
        assert!(lake.cat.get::<Value>("q/s.v").await.unwrap().is_none(), "its view is dropped");
        assert!(lake.cat.get::<Value>("r/s.m").await.unwrap().is_none(), "its macro is dropped");
        let early = format!("{:#}", run(&lake, "UNDROP VIEW s.v").await.unwrap_err());
        assert!(early.contains("no schema s"), "{early}");
        let out = run(&lake, "UNDROP SCHEMA s").await.unwrap();
        assert_eq!(out["undropped"], true, "{out}");
        let with: Vec<&str> = out["with"].as_array().unwrap().iter().map(|w| w.as_str().unwrap()).collect();
        assert_eq!(with, ["macro s.m", "view s.v"], "routines before what reads them");
        assert!(out.get("left").is_none(), "{out}");
        assert!(lake.cat.get::<Value>("ns/s").await.unwrap().is_some(), "the schema is back");
        assert_eq!(lake.cat.get::<Value>("q/s.v").await.unwrap(), Some(view_was), "its view is back as it was");
        assert_eq!(lake.cat.get::<Value>("r/s.m").await.unwrap(), Some(macro_was), "its macro is back as it was");
        assert_eq!(lake.cat.get::<String>("cm/schema/s").await.unwrap().as_deref(), Some("sales"), "with its comment");
        assert!(all(&lake).await.unwrap().is_empty(), "no kept record is left");
    }

    /// A materialized view is kept as the statement that makes it again: UNDROP runs it, so its rows are worked out from
    /// its query as it is then, and its comment comes back with it.
    #[tokio::test]
    async fn a_materialized_view_is_made_again_from_its_query() {
        let lake = lake().await;
        crate::write::create_table(&lake, "t", r#"{"columns": [["a", "Int64"]]}"#).await.unwrap();
        run(&lake, "CREATE MATERIALIZED VIEW mv AS SELECT a, count(*) AS n FROM t GROUP BY a").await.unwrap();
        run(&lake, "COMMENT ON MATERIALIZED VIEW mv IS 'counts'").await.unwrap();
        let was: crate::views::View = lake.cat.get(&crate::views::view_key("mv")).await.unwrap().unwrap();
        run(&lake, "DROP MATERIALIZED VIEW mv").await.unwrap();
        assert!(lake.cat.get::<Value>(&crate::views::view_key("mv")).await.unwrap().is_none(), "DROP takes the view");
        let kept = all(&lake).await.unwrap();
        assert_eq!(kept.iter().map(|(n, k)| (n.as_str(), k.kind.as_str())).collect::<Vec<_>>(), vec![("mv", "materialized view")], "one record");
        assert!(kept[0].1.made.starts_with("CREATE MATERIALIZED VIEW mv"), "{}", kept[0].1.made);
        let out = run(&lake, "UNDROP MATERIALIZED VIEW mv").await.unwrap();
        assert_eq!(out["undropped"], true, "{out}");
        let back: crate::views::View = lake.cat.get(&crate::views::view_key("mv")).await.unwrap().unwrap();
        assert_eq!(back.sql, was.sql, "made again from its query");
        assert_eq!(lake.cat.get::<String>("cm/relation/mv").await.unwrap().as_deref(), Some("counts"), "with its comment");
        assert!(all(&lake).await.unwrap().is_empty(), "UNDROP takes its record");
        let again = format!("{:#}", run(&lake, "UNDROP MATERIALIZED VIEW mv").await.unwrap_err());
        assert!(again.contains("pondra.dropped"), "{again}");
        run(&lake, "CREATE OR REPLACE MATERIALIZED VIEW mv AS SELECT a FROM t").await.unwrap();
        assert!(all(&lake).await.unwrap().is_empty(), "a view replaced isn't kept: only a DROP keeps one");
    }

    /// A dropped database stays as it was in its folder, under its mark: out of the listing of databases and refused an
    /// attach, until UNDROP DATABASE takes the mark off and attaches it again; a new database of its name lets it go, and so
    /// does the sweep once its time is up.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_dropped_database_stays_in_its_folder_until_undropped_or_let_go() {
        let folder = std::env::temp_dir().join(format!("pondra-kept-lakes-{}", uuid::Uuid::new_v4().simple()));
        let at = |n: &str| folder.join(n).to_string_lossy().to_string();
        let home = Lake::open(&at("home"), true, false, crate::node_state::NodeState::unhosted()).await.unwrap();
        for n in ["b", "c"] {
            let other = Lake::open(&at(n), true, false, crate::node_state::NodeState::unhosted()).await.unwrap();
            crate::write::create_table(&other, "t", r#"{"columns": [["a", "Int64"]]}"#).await.unwrap();
            other.cat.close().await.unwrap();
        }
        let names = |all: Vec<(String, String)>| all.into_iter().map(|(n, _)| n).collect::<Vec<_>>();
        assert_eq!(names(crate::ddl::lakes_in(folder.to_str().unwrap()).await.unwrap()), ["b", "c", "home"]);
        let kept = keep_lake(&at("b"), "alice").await.unwrap();
        assert_eq!(lake_kept(&at("b")).await.unwrap(), Some(kept.clone()));
        assert_eq!(names(crate::ddl::lakes_in(folder.to_str().unwrap()).await.unwrap()), ["c", "home"], "a dropped one is no database");
        let listed = lakes_kept(folder.to_str().unwrap()).await.unwrap();
        assert_eq!(listed.iter().map(|(n, _, k)| (n.as_str(), k.by.as_str())).collect::<Vec<_>>(), [("b", "alice")]);
        let refused = format!("{:#}", crate::ddl::attach(&home, "b", &at("b"), false, false).await.err().expect("refused"));
        assert!(refused.contains("UNDROP DATABASE b"), "{refused}");
        assert!(sweep_lakes(folder.to_str().unwrap(), kept.until() - 1).await.is_empty(), "kept until its time is up");
        // (UNDROP DATABASE on a node serving one lake: the one beside it, attached again with its tables)
        let out = run(&home, "UNDROP DATABASE b").await.unwrap();
        assert_eq!(out["undropped"], true, "{out}");
        assert_eq!(lake_kept(&at("b")).await.unwrap(), None);
        let back = home.attached.read().unwrap().iter().find(|(n, _)| n == "b").map(|(_, l)| l.clone()).expect("attached again");
        assert!(back.cat.get::<Value>(&crate::store::table_key("t")).await.unwrap().is_some(), "with its tables");
        let again = format!("{:#}", run(&home, "UNDROP DATABASE b").await.unwrap_err());
        assert!(again.contains("pondra.dropped"), "{again}");
        // (a new database under a dropped one's name lets it go, and says so)
        keep_lake(&at("c"), "bob").await.unwrap();
        let notice = crate::ddl::let_go(&at("c"), "c").await.unwrap().expect("let go");
        assert!(notice.contains("is let go"), "{notice}");
        assert!(!folder.join("c").exists(), "its folder deleted");
        // (and the sweep lets one go once its time is up)
        let d = Lake::open(&at("d"), true, false, crate::node_state::NodeState::unhosted()).await.unwrap();
        d.cat.close().await.unwrap();
        let kept = keep_lake(&at("d"), "carol").await.unwrap();
        assert_eq!(sweep_lakes(folder.to_str().unwrap(), kept.until()).await, ["d"]);
        assert!(!folder.join("d").exists());
        home.cat.close().await.unwrap();
        let _ = std::fs::remove_dir_all(&folder);
    }

    /// UNDROP reads each kind whose verbs list it, the longest words first, and names those kinds for any other.
    #[test]
    fn undrop_reads_each_kind_that_keeps_its_drops() {
        use crate::ddl::Ddl;
        use crate::write::{parse, Stmt};
        for (sql, kind) in [("UNDROP TABLE FUNCTION f", "table function"), ("undrop external  table x", "external table"), ("UNDROP VIEW s.v", "view"), ("UNDROP CONNECTION landing", "connection")] {
            assert!(matches!(parse(sql), Some(Stmt::Ddl(ds)) if matches!(&ds[..], [Ddl::UndropKept { kind: k, .. }] if k == kind)), "{sql}");
        }
        assert!(matches!(parse("UNDROP TABLE t"), Some(Stmt::Ddl(ds)) if matches!(&ds[..], [Ddl::Undrop { name }] if name == "t")));
        for (sql, kind) in [("UNDROP SCHEMA s", "schema"), ("UNDROP DATABASE d", "database")] {
            assert!(matches!(parse(sql), Some(Stmt::Ddl(ds)) if matches!(&ds[..], [Ddl::UndropKept { kind: k, .. }] if k == kind)), "{sql}");
        }
        match parse("UNDROP SEQUENCE s") {
            Some(Stmt::Invalid(e)) => assert!(e.contains("EXTERNAL TABLE, FUNCTION") && e.contains("DATABASE") && e.contains("SCHEMA") && e.contains("pondra.dropped"), "{e}"),
            _ => panic!("UNDROP SEQUENCE: refused, naming the kinds"),
        }
    }

    #[tokio::test]
    async fn a_record_goes_when_its_time_is_up() {
        let lake = lake().await;
        run(&lake, "CREATE VIEW v AS SELECT 1 AS one").await.unwrap();
        run(&lake, "DROP VIEW v").await.unwrap();
        let kept = all(&lake).await.unwrap();
        assert_eq!(kept.len(), 1);
        assert_eq!((kept[0].0.as_str(), kept[0].1.kind.as_str()), ("v", "view"));
        assert!(expired(&lake, kept[0].1.until() - 1).await.unwrap().is_empty());
        assert_eq!(expired(&lake, kept[0].1.until()).await.unwrap().len(), 1);
        // (two drops of one name in one millisecond keep two copies: the second is a millisecond on)
        run(&lake, "CREATE VIEW v AS SELECT 2 AS two").await.unwrap();
        let first = kept[0].1.at_ms;
        let puts = aside(&lake, "view", "v", &["q/v".to_string()]).await.unwrap();
        let again: Kept = serde_json::from_slice(&puts[0].1).unwrap();
        assert!(again.at_ms > first, "{} after {first}", again.at_ms);
    }
}
