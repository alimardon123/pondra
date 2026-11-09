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
use anyhow::Result;
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
    let kept = Kept { at_ms, keep_ms: crate::ddl::KEEP_MS, kind: kind.to_string(), by: crate::auth::name(), records };
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

/// `UNDROP <kind> name`: the copy of `name` dropped last, back as it was, the comments on it too.
pub async fn undrop(lake: &Lake, kind: &str, name: &str) -> Result<Value> {
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
    let puts = kept.records.into_iter().map(|(k, v)| (k, json(&v))).collect();
    lake.cat.commit(puts, &[at]).await?;
    Ok(j!({shown: name, "undropped": true}))
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

    /// UNDROP reads each kind whose verbs list it, the longest words first, and names those kinds for any other.
    #[test]
    fn undrop_reads_each_kind_that_keeps_its_drops() {
        use crate::ddl::Ddl;
        use crate::write::{parse, Stmt};
        for (sql, kind) in [("UNDROP TABLE FUNCTION f", "table function"), ("undrop external  table x", "external table"), ("UNDROP VIEW s.v", "view"), ("UNDROP CONNECTION landing", "connection")] {
            assert!(matches!(parse(sql), Some(Stmt::Ddl(ds)) if matches!(&ds[..], [Ddl::UndropKept { kind: k, .. }] if k == kind)), "{sql}");
        }
        assert!(matches!(parse("UNDROP TABLE t"), Some(Stmt::Ddl(ds)) if matches!(&ds[..], [Ddl::Undrop { name }] if name == "t")));
        match parse("UNDROP SCHEMA s") {
            Some(Stmt::Invalid(e)) => assert!(e.contains("EXTERNAL TABLE, FUNCTION") && e.contains("pondra.dropped"), "{e}"),
            _ => panic!("UNDROP SCHEMA: refused, naming the kinds"),
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
