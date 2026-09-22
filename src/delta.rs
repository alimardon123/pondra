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
    files: BTreeMap<String, (u64, u64)>, // path in the table folder -> (bytes, published at ms)
}

/// Every table in every format it's published in at once (each is an object-store write or
/// two), then one catalog write that records what was published. It isn't awaited: until it is
/// durable, the next round sees the older state and takes over what was written (see `publish`).
pub async fn publish_all(lake: &Lake) -> Result<()> {
    lake.cat.wait_durable(lake.cat.committed()).await; // (replicated acks: publish only what's in the bucket)
    let tables = lake.cat.scan::<TableMeta>("t/", "t0").await?;
    let jobs = tables.iter().flat_map(|(key, meta)| meta.publish.iter().map(move |f| (&key[2..], meta, f.as_str())));
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
    Ok(())
}

/// The files another engine should read as the table, if they read right without Pondra: all of
/// an append table's; for a keyed table only a single file with one row per key and no delete
/// markers (a compaction's output, or a first fold of a table without deletes). Between
/// compactions the last published version stays.
pub fn publishable(meta: &TableMeta) -> Option<Vec<&DataFile>> {
    let deletes = meta.columns.iter().any(|(c, _)| c == "_deleted");
    match meta.key.is_empty() {
        true => Some(meta.files.iter().collect()),
        false if meta.files.len() == 1 && (meta.files[0].whole || !deletes) => Some(meta.files.iter().collect()),
        false => None,
    }
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
    let Some(files) = publishable(meta) else { return Ok(None) };
    let (dir, key) = (format!("data/{table}/"), format!("x/{table}"));
    let want: BTreeMap<&str, u64> = files.iter().filter_map(|f| Some((f.path.strip_prefix(&dir)?, f.bytes))).collect();
    let mut state: Published = lake.cat.get(&key).await?.unwrap_or_default();
    let adds: Vec<(&str, u64)> = want.iter().filter(|(p, _)| !state.files.contains_key(**p)).map(|(p, b)| (*p, *b)).collect();
    let removes: Vec<String> = state.files.keys().filter(|p| !want.contains_key(p.as_str())).cloned().collect();
    if state.version.is_some() && adds.is_empty() && removes.is_empty() && state.schema == schema {
        return Ok(None);
    }
    let (version, now) = (state.version.map_or(0, |v| v + 1), crate::log::now_ms());
    if state.id.is_empty() {
        state.id = uuid::Uuid::new_v4().to_string();
    }
    let mut actions = vec![];
    if version == 0 {
        actions.push(json!({"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}}));
    }
    if state.schema != schema {
        actions.push(metadata(&state.id, table, &schema, now));
    }
    actions.extend(removes.iter().map(|p| json!({"remove": {"path": p, "deletionTimestamp": now, "dataChange": true}})));
    actions.extend(adds.iter().map(|(p, b)| add(p, *b, now)));
    let body = actions.iter().map(Value::to_string).collect::<Vec<_>>().join("\n");
    // Written once, never overwritten. If it's already there, an earlier attempt wrote it and
    // crashed before recording it: it was derived from committed state too, so take it over.
    if lake.put(&format!("{dir}_delta_log/{version:020}.json"), body.into_bytes()).await.is_err() {
        let earlier = lake.store.get(&Path::from(format!("{dir}_delta_log/{version:020}.json"))).await?.bytes().await?;
        return Ok(Some((key, json(&adopt(state, version, &earlier)?))));
    }
    state.version = Some(version);
    state.schema = schema;
    for p in &removes {
        state.files.remove(p);
    }
    state.files.extend(adds.iter().map(|(p, b)| (p.to_string(), (*b, now))));
    if version > 0 && version % CHECKPOINT_EVERY == 0 {
        checkpoint(lake, &dir, version, &state, table).await?;
    }
    Ok(Some((key, json(&state))))
}

/// Take over a Delta commit an earlier attempt wrote (see `publish`); the next round catches up.
fn adopt(mut state: Published, version: u64, commit: &[u8]) -> Result<Published> {
    for line in commit.split(|b| *b == b'\n').filter(|l| !l.is_empty()) {
        let a: Value = serde_json::from_slice(line)?;
        if let Some(p) = a["remove"]["path"].as_str() {
            state.files.remove(p);
        }
        if let Some(p) = a["add"]["path"].as_str() {
            state.files.insert(p.to_string(), (a["add"]["size"].as_u64().unwrap_or(0), a["add"]["modificationTime"].as_u64().unwrap_or(0)));
        }
        if let Some(s) = a["metaData"]["schemaString"].as_str() {
            state.schema = s.to_string();
        }
    }
    state.version = Some(version);
    Ok(state)
}

/// A Parquet snapshot of the table (protocol, metadata, live files), so a reader starts here
/// instead of replaying every JSON commit. Arrow's JSON reader builds the nested columns from
/// the same action objects the commits use.
async fn checkpoint(lake: &Lake, dir: &str, version: u64, state: &Published, table: &str) -> Result<()> {
    let mut rows = vec![json!({"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}}), metadata(&state.id, table, &state.schema, 0)];
    rows.extend(state.files.iter().map(|(p, (b, t))| add(p, *b, *t)));
    let body = rows.iter().map(Value::to_string).collect::<Vec<_>>().join("\n");
    let batches = arrow_json::ReaderBuilder::new(checkpoint_schema()).build(body.as_bytes())?.collect::<Result<Vec<_>, _>>()?;
    let mut buf = vec![];
    let mut w = datafusion::parquet::arrow::ArrowWriter::try_new(&mut buf, checkpoint_schema(), None)?;
    for b in &batches {
        w.write(b)?;
    }
    w.close()?;
    lake.put(&format!("{dir}_delta_log/{version:020}.checkpoint.parquet"), buf).await.ok(); // (a retry may find it written)
    // The one pointer Delta keeps; readers also find checkpoints by listing, so it is only a hint.
    let last = json!({"version": version, "size": rows.len()}).to_string();
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

fn metadata(id: &str, table: &str, schema: &str, now: u64) -> Value {
    json!({"metaData": {"id": id, "name": table, "format": {"provider": "parquet", "options": {}}, "schemaString": schema,
        "partitionColumns": [], "configuration": {}, "createdTime": now}})
}

fn add(path: &str, bytes: u64, at: u64) -> Value {
    json!({"add": {"path": path, "partitionValues": {}, "size": bytes, "modificationTime": at, "dataChange": true}})
}

/// The table's columns as a Delta schema, if every type has a Delta equivalent.
fn schema_string(columns: &[(String, String)]) -> Option<String> {
    let fields = columns.iter().map(|(name, t)| {
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
            // (Delta's `timestamp` is an instant: time-zone-aware columns only)
            t if matches!(t.parse(), Ok(DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Microsecond, Some(_)))) => "timestamp",
            t => return decimal(t).map(|(p, s)| json!({"name": name, "type": format!("decimal({p},{s})"), "nullable": true, "metadata": {}})),
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
    Arc::new(Schema::new(vec![
        st("protocol", vec![i("minReaderVersion", DataType::Int32), i("minWriterVersion", DataType::Int32)]),
        st("metaData", vec![s("id"), s("name"), s("description"), st("format", vec![s("provider"), map("options")]), s("schemaString"),
            Field::new_list("partitionColumns", Field::new("element", DataType::Utf8, true), true), map("configuration"), i("createdTime", DataType::Int64)]),
        st("add", vec![s("path"), map("partitionValues"), i("size", DataType::Int64), i("modificationTime", DataType::Int64), i("dataChange", DataType::Boolean), s("stats")]),
        st("remove", vec![s("path"), i("deletionTimestamp", DataType::Int64), i("dataChange", DataType::Boolean)]),
    ]))
}
