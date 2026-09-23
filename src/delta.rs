//! Open formats, for the tables that ask for them (`TableMeta::publish`): Pondra itself reads
//! the catalog, which is fresher; these are for engines that don't know Pondra — Spark,
//! Databricks, DuckDB, Polars, Trino, Athena, Snowflake. Iceberg is in `iceberg.rs`; Delta here.
//! A table's Parquet files already live in `data/{table}/`; this keeps that folder's
//! `_delta_log/` in step with the catalog: one JSON commit per change to the table's file list,
//! plus a Parquet checkpoint every 10 commits so readers never replay a long log; the last 1,000
//! versions are kept.
//!
//! The catalog stays the source of truth: a commit is derived only from what the catalog has
//! already committed, so a crash can at worst leave the Delta log one step behind.
//! Append tables are published every tiering round. Keyed tables (upsert, merge) hold several
//! versions of a key between compactions, so they are published whenever a single file holds
//! exactly one row per key: after each compaction (at most 8 tiering rounds apart).
use crate::store::*;
use anyhow::Result;
use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema};
use datafusion::arrow::json as arrow_json;
use object_store::{path::Path, ObjectStoreExt};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::Arc;

const CHECKPOINT_EVERY: u64 = 10; // as Delta itself does: a reader replays at most 9 JSON commits
const HISTORY: u64 = 1000; // versions of the log kept (the files older ones name are gone after --retain-secs)

/// What the Delta log says right now (kept in the catalog under `x/{table}`).
#[derive(Serialize, Deserialize, Default)]
struct Published {
    version: Option<u64>,
    id: String,
    schema: String,
    /// The sealed manifests whose files are in the log (path -> how many), and the inline files
    /// (path in the table folder -> (bytes, published at ms)). Both are small: a manifest holds
    /// up to 4,096 files, and only `INLINE` files are listed inline.
    #[serde(default)]
    manifests: BTreeMap<String, u64>,
    #[serde(default, alias = "files")]
    inline: BTreeMap<String, (u64, u64)>,
}

/// Every table in every format it's published in at once (each is an object-store write or
/// two), then one catalog write that records what was published. It isn't awaited: until it is
/// durable, the next round sees the older state and takes over what was written (see `publish`).
pub async fn publish_all(lake: &Lake) -> Result<()> {
    lake.cat.wait_durable(lake.cat.committed()).await; // (replicated acks: publish only what's in the bucket)
    // A table whose metadata is as last published is skipped without loading its manifests.
    static SEEN: std::sync::LazyLock<std::sync::Mutex<std::collections::HashMap<String, u64>>> = std::sync::LazyLock::new(Default::default);
    let print = |meta: &TableMeta| std::hash::BuildHasher::hash_one(&std::hash::BuildHasherDefault::<std::collections::hash_map::DefaultHasher>::default(), json(meta));
    let tables = lake.cat.scan::<TableMeta>("t/", "t0").await?;
    let changed: Vec<_> = tables.iter().filter(|(key, meta)| SEEN.lock().unwrap().get(key) != Some(&print(meta))).collect();
    let jobs = changed.iter().flat_map(|(key, meta)| meta.publish.iter().map(move |f| (&key[2..], meta, f.as_str())));
    let states = futures::future::try_join_all(jobs.map(|(table, meta, format)| async move {
        match format {
            "delta" => publish(lake, table, meta).await,
            _ => crate::iceberg::publish(lake, table, meta).await,
        }
    }))
    .await?;
    let puts: Vec<(String, Vec<u8>)> = states.into_iter().flatten().collect();
    if !puts.is_empty() {
        drop(lake.cat.write(puts, &[]).await?);
    }
    SEEN.lock().unwrap().extend(changed.iter().map(|(key, meta)| (key.clone(), print(meta))));
    Ok(())
}

/// What another engine should read as the table, if it reads right without Pondra: an append
/// table's sealed manifests (immutable, so what they hold is published once) and its inline
/// files; for a keyed table only a single file with one row per key and no delete markers (a
/// compaction's output, or a first fold of a table without deletes) — between compactions the
/// last published version stays.
///
/// Published in these two parts, a commit costs what changed, not what the table holds: a table
/// of a million files publishes a new manifest's worth, not a million paths (ADR-012).
pub struct Parts {
    pub manifests: Vec<crate::manifest::Manifest>,
    pub inline: Vec<DataFile>,
}

pub async fn publishable(lake: &Lake, meta: &TableMeta) -> Result<Option<Parts>> {
    let deletes = meta.columns.iter().any(|(c, _)| c == "_deleted");
    Ok(match meta.key.is_empty() {
        true => Some(Parts { manifests: crate::manifest::list(lake, meta).await?, inline: meta.files.clone() }),
        false if meta.files.len() == 1 && (meta.files[0].whole || !deletes) => Some(Parts { manifests: vec![], inline: meta.files.clone() }),
        false => None,
    })
}

/// Stop publishing a table in `format`: its metadata goes, so no engine reads a stale copy.
pub async fn unpublish(lake: &Lake, table: &str, format: &str) -> Result<()> {
    let (dir, state) = match format {
        "delta" => (format!("data/{table}/_delta_log"), format!("x/{table}")),
        _ => (format!("data/{table}/metadata"), format!("i/{table}")),
    };
    lake.cat.commit(vec![], &[state]).await?;
    let objects: Vec<_> = futures::TryStreamExt::try_collect(lake.store.list(Some(&Path::from(dir)))).await?;
    futures::future::join_all(objects.iter().map(|o| lake.delete(o.location.as_ref()))).await;
    Ok(())
}

/// Delta logs and Iceberg metadata: open-format files Pondra writes but never reads.
pub fn open_format(path: &str) -> bool { path.contains("/_delta_log/") || path.contains("/metadata/") }

/// A Decimal128(p, s) column's precision and scale.
pub fn decimal(t: &str) -> Option<(u8, i8)> {
    let (p, s) = t.strip_prefix("Decimal128(")?.strip_suffix(')')?.split_once(',')?;
    Some((p.trim().parse().ok()?, s.trim().parse().ok()?))
}

/// The table's next Delta commit, if its files changed; returns the new publish state to record.
async fn publish(lake: &Lake, table: &str, meta: &TableMeta) -> Result<Option<(String, Vec<u8>)>> {
    let Some(schema) = schema_string(&meta.columns) else { return Ok(None) }; // a type Delta can't carry
    let Some(parts) = publishable(lake, meta).await? else { return Ok(None) };
    let (dir, key) = (format!("data/{table}/"), format!("x/{table}"));
    let mut state: Published = lake.cat.get(&key).await?.unwrap_or_default();
    // Only the manifests that came or went are read: their files are what the log gains or loses.
    let now: BTreeMap<String, u64> = parts.manifests.iter().map(|m| (m.path.clone(), m.files)).collect();
    let came: Vec<&crate::manifest::Manifest> = parts.manifests.iter().filter(|m| !state.manifests.contains_key(&m.path)).collect();
    let went: Vec<crate::manifest::Manifest> = state.manifests.keys().filter(|p| !now.contains_key(*p)).map(|p| crate::manifest::Manifest { path: p.clone(), ..Default::default() }).collect();
    let gained = load(lake, came.into_iter().cloned().collect()).await?;
    let lost = load(lake, went).await?;
    let mut want: BTreeMap<String, u64> = gained.iter().map(|f| (name(&dir, f), f.bytes)).collect();
    want.extend(parts.inline.iter().map(|f| (name(&dir, f), f.bytes)));
    let mut gone: BTreeMap<String, u64> = lost.iter().map(|f| (name(&dir, f), f.bytes)).collect();
    gone.extend(state.inline.iter().filter(|(p, _)| !want.contains_key(*p)).map(|(p, (b, _))| (p.clone(), *b)));
    let adds: Vec<(String, u64)> = want.iter().filter(|(p, _)| !state.inline.contains_key(*p)).map(|(p, b)| (p.clone(), *b)).collect();
    let removes: Vec<String> = gone.keys().filter(|p| !want.contains_key(*p)).cloned().collect();
    if state.version.is_some() && adds.is_empty() && removes.is_empty() && state.schema == schema {
        return Ok(None);
    }
    let (mut version, ms) = (state.version.map_or(0, |v| v + 1), crate::log::now_ms());
    if state.id.is_empty() {
        state.id = uuid::Uuid::new_v4().to_string();
    }
    let (mut adds, mut removes) = (adds, removes);
    loop {
        let mut actions = vec![];
        if version == 0 || protocol(&state.schema) != protocol(&schema) {
            actions.push(protocol(&schema));
        }
        if state.schema != schema {
            actions.push(metadata(&state.id, table, &schema, ms));
        }
        actions.extend(removes.iter().map(|p| json!({"remove": {"path": p, "deletionTimestamp": ms, "dataChange": true}})));
        actions.extend(adds.iter().map(|(p, b)| add(p, *b, ms)));
        let body = actions.iter().map(Value::to_string).collect::<Vec<_>>().join("\n");
        // Written once, never overwritten. If it's already there, an earlier attempt wrote it and
        // crashed before recording it: it was derived from committed state too, so whatever it
        // published counts, and this commit carries only what is left over (usually nothing).
        if lake.put(&format!("{dir}_delta_log/{version:020}.json"), body.into_bytes()).await.is_ok() {
            break;
        }
        let earlier = lake.store.get(&Path::from(format!("{dir}_delta_log/{version:020}.json"))).await?.bytes().await?;
        let (added, removed) = applied(&earlier)?;
        adds.retain(|(p, _)| !added.contains(p));
        removes.retain(|p| !removed.contains(p));
        state.schema = schema.clone();
        version += 1;
        if adds.is_empty() && removes.is_empty() {
            version -= 1; // (nothing left to write: that commit is where the log stands)
            break;
        }
    }
    state.version = Some(version);
    state.schema = schema;
    state.manifests = now;
    state.inline = parts.inline.iter().map(|f| (name(&dir, f), (f.bytes, ms))).collect();
    if version > 0 && version % CHECKPOINT_EVERY == 0 {
        checkpoint(lake, &dir, version, &state, table, meta).await?;
    }
    Ok(Some((key, json(&state))))
}

/// A file's path inside the table's folder, as Delta lists it.
fn name(dir: &str, f: &DataFile) -> String { f.path.strip_prefix(dir).unwrap_or(&f.path).to_string() }

/// The files of these manifests (a manifest is immutable, so this is read once per manifest).
async fn load(lake: &Lake, manifests: Vec<crate::manifest::Manifest>) -> Result<Vec<DataFile>> {
    let mut out = vec![];
    for m in &manifests {
        out.extend(crate::manifest::files(lake, m).await?);
    }
    Ok(out)
}

/// What a commit added and removed (paths), to take over one an earlier attempt wrote.
fn applied(commit: &[u8]) -> Result<(std::collections::HashSet<String>, std::collections::HashSet<String>)> {
    let (mut added, mut removed) = (std::collections::HashSet::new(), std::collections::HashSet::new());
    for line in commit.split(|b| *b == b'\n').filter(|l| !l.is_empty()) {
        let a: Value = serde_json::from_slice(line)?;
        if let Some(p) = a["add"]["path"].as_str() {
            added.insert(p.to_string());
        }
        if let Some(p) = a["remove"]["path"].as_str() {
            removed.insert(p.to_string());
        }
    }
    Ok((added, removed))
}

/// A Parquet snapshot of the table (protocol, metadata, live files), so a reader starts here
/// instead of replaying every JSON commit. Arrow's JSON reader builds the nested columns from
/// the same action objects the commits use.
async fn checkpoint(lake: &Lake, dir: &str, version: u64, state: &Published, table: &str, meta: &TableMeta) -> Result<()> {
    // The actions are built here, a manifest at a time, and written on a blocking thread: a table
    // of a million files never has all their paths in memory at once.
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<Value>>(2);
    let writing = tokio::task::spawn_blocking(move || -> Result<(Vec<u8>, usize)> {
        let (mut buf, mut rows) = (vec![], 0);
        {
            let mut w = datafusion::parquet::arrow::ArrowWriter::try_new(&mut buf, checkpoint_schema(), None)?;
            while let Some(actions) = rx.blocking_recv() {
                rows += actions.len();
                let body = actions.iter().map(Value::to_string).collect::<Vec<_>>().join("\n");
                for b in arrow_json::ReaderBuilder::new(checkpoint_schema()).build(body.as_bytes())? {
                    w.write(&b?)?;
                }
            }
            w.close()?;
        }
        Ok((buf, rows))
    });
    tx.send(vec![protocol(&state.schema), metadata(&state.id, table, &state.schema, 0)]).await?;
    for m in crate::manifest::list(lake, meta).await? {
        let files = crate::manifest::files(lake, &m).await?;
        tx.send(files.iter().map(|f| add(f.path.strip_prefix(dir).unwrap_or(&f.path), f.bytes, 0)).collect()).await?;
    }
    tx.send(state.inline.iter().map(|(p, (b, t))| add(p, *b, *t)).collect()).await?;
    drop(tx);
    let (buf, rows) = writing.await??;
    lake.put(&format!("{dir}_delta_log/{version:020}.checkpoint.parquet"), buf).await.ok(); // (a retry may find it written)
    // The one pointer Delta keeps; readers also find checkpoints by listing, so it is only a hint.
    let last = json!({"version": version, "size": rows}).to_string();
    lake.store.put(&Path::from(format!("{dir}_delta_log/_last_checkpoint")), last.into_bytes().into()).await?;
    // Older history goes, as Delta's own log cleanup would: the checkpoint and commits from
    // before the oldest version kept.
    if let Some(gone) = version.checked_sub(HISTORY + CHECKPOINT_EVERY) {
        lake.delete(&format!("{dir}_delta_log/{gone:020}.checkpoint.parquet")).await;
        for v in gone..gone + CHECKPOINT_EVERY {
            lake.delete(&format!("{dir}_delta_log/{v:020}.json")).await;
        }
    }
    Ok(())
}

/// The protocol a schema needs: `timestamp_ntz` columns take the `timestampNtz` table feature.
fn protocol(schema: &str) -> Value {
    match schema.contains("\"timestamp_ntz\"") {
        true => json!({"protocol": {"minReaderVersion": 3, "minWriterVersion": 7, "readerFeatures": ["timestampNtz"], "writerFeatures": ["timestampNtz"]}}),
        false => json!({"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}}),
    }
}

fn metadata(id: &str, table: &str, schema: &str, now: u64) -> Value {
    json!({"metaData": {"id": id, "name": table, "format": {"provider": "parquet", "options": {}}, "schemaString": schema,
        "partitionColumns": [], "configuration": {}, "createdTime": now}})
}

fn add(path: &str, bytes: u64, at: u64) -> Value {
    json!({"add": {"path": path, "partitionValues": {}, "size": bytes, "modificationTime": at, "dataChange": true}})
}

/// One Delta type name, for a list's elements.
fn element(t: &str) -> Option<Value> {
    let one = schema_string(&[("x".to_string(), t.to_string())])?;
    let parsed: Value = serde_json::from_str(&one).ok()?;
    Some(parsed["fields"][0]["type"].clone())
}

/// The table's columns as a Delta schema, if every type has a Delta equivalent.
fn schema_string(columns: &[(String, String)]) -> Option<String> {
    let fields = columns.iter().map(|(name, t)| {
        if let Some(item) = t.strip_suffix("[]") {
            // A list column (an embedding, say): Delta's array type.
            let element = element(item)?;
            return Some(json!({"name": name, "type": {"type": "array", "elementType": element, "containsNull": true}, "nullable": true, "metadata": {}}));
        }
        let t = match t.as_str() {
            "Int64" => "long",
            "Int32" => "integer",
            "Int16" => "short",
            "Int8" => "byte",
            "Float64" => "double",
            "Float32" => "float",
            "Utf8" | "LargeUtf8" | "Utf8View" => "string",
            "Boolean" => "boolean",
            "Date32" => "date",
            "Binary" | "LargeBinary" => "binary",
            // (Delta's `timestamp` is an instant; `timestamp_ntz` a wall-clock time)
            t => match t.parse() {
                Ok(DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Microsecond, tz)) => if tz.is_some() { "timestamp" } else { "timestamp_ntz" },
                _ => return decimal(t).map(|(p, s)| json!({"name": name, "type": format!("decimal({p},{s})"), "nullable": true, "metadata": {}})),
            },
        };
        Some(json!({"name": name, "type": t, "nullable": true, "metadata": {}}))
    });
    Some(json!({"type": "struct", "fields": fields.collect::<Option<Vec<_>>>()?}).to_string())
}

/// Delta's checkpoint columns (the subset of the spec a reader needs: protocol, metadata, files).
fn checkpoint_schema() -> Arc<Schema> {
    let s = |n: &str| Field::new(n, DataType::Utf8, true);
    let i = |n: &str, t: DataType| Field::new(n, t, true);
    let map = |n: &str| Field::new_map(n, "key_value", Field::new("key", DataType::Utf8, false), s("value"), false, true);
    let st = |n: &str, f: Vec<Field>| Field::new(n, DataType::Struct(Fields::from(f)), true);
    let list = |n: &str| Field::new_list(n, Field::new("element", DataType::Utf8, true), true);
    Arc::new(Schema::new(vec![
        st("protocol", vec![i("minReaderVersion", DataType::Int32), i("minWriterVersion", DataType::Int32), list("readerFeatures"), list("writerFeatures")]),
        st("metaData", vec![s("id"), s("name"), s("description"), st("format", vec![s("provider"), map("options")]), s("schemaString"),
            list("partitionColumns"), map("configuration"), i("createdTime", DataType::Int64)]),
        st("add", vec![s("path"), map("partitionValues"), i("size", DataType::Int64), i("modificationTime", DataType::Int64), i("dataChange", DataType::Boolean), s("stats")]),
        st("remove", vec![s("path"), i("deletionTimestamp", DataType::Int64), i("dataChange", DataType::Boolean)]),
    ]))
}
