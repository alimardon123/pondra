//! Iceberg publishing (format v2), for tables with "iceberg" in `publish`: the same Parquet files
//! as the table, described under `data/{table}/metadata/` — a `v{N}.metadata.json` per change,
//! the manifest list and manifest it names (Avro, written here by hand: a few dozen bytes per
//! file), and `version-hint.text` naming the newest. Readers that take a table folder (DuckDB, a
//! Hadoop catalog) or a metadata file (PyIceberg, Polars, Trino's `register_table`) find it.
//!
//! As with Delta, it is derived from committed catalog state only. Version N is snapshot N and
//! sequence number N; a version an earlier attempt wrote but never recorded is skipped, never
//! rewritten. Pondra's Parquet files carry no Iceberg field ids, so columns are mapped by name
//! (`schema.name-mapping.default`).
use crate::store::*;
use anyhow::Result;
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use object_store::{path::Path, ObjectStoreExt};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeMap;

const HISTORY: usize = 100; // snapshots (and metadata files) kept

/// What the Iceberg metadata says right now (kept in the catalog under `i/{table}`).
///
/// One Iceberg manifest per Pondra manifest: ours are immutable, so a sealed manifest's files are
/// written to Avro once and every later snapshot names that same object. Only the inline files'
/// manifest is rewritten, and it holds at most `INLINE` of them. So a snapshot of a table with a
/// million files costs one manifest, not a million entries (ADR-012).
#[derive(Serialize, Deserialize, Default)]
struct Published {
    uuid: String,
    version: u64,
    manifests: BTreeMap<String, Avro>, // our manifest's path -> the Iceberg manifest written for it
    inline: Option<Avro>,              // the inline files' manifest
    inlined: BTreeMap<String, u64>,    // those files (path -> rows), to see when they change
    snapshots: Vec<(Value, String)>,   // kept, oldest first: (snapshot entry, its manifest list)
    dropped: Vec<(u64, String)>,       // manifests no longer named, deleted once no kept snapshot names them
}

/// An Iceberg manifest this lake wrote.
#[derive(Serialize, Deserialize, Default, Clone)]
struct Avro {
    path: String,
    bytes: u64,
    files: u64,
    rows: u64,
    seq: u64, // the snapshot that added it; its entries carry this sequence number
}

/// The table's next Iceberg version, if its files changed; returns the new state to record.
pub async fn publish(lake: &Lake, table: &str, meta: &TableMeta) -> Result<Option<(String, Vec<u8>)>> {
    let Some(fields) = fields(&meta.columns) else { return Ok(None) }; // a type Iceberg can't carry
    let Some(parts) = crate::delta::publishable(lake, meta).await? else { return Ok(None) };
    let key = format!("i/{table}");
    let mut st: Published = lake.cat.get(&key).await?.unwrap_or_default();
    let inlined: BTreeMap<String, u64> = parts.inline.iter().map(|f| (f.path.clone(), f.rows)).collect();
    let fresh: Vec<&crate::manifest::Manifest> = parts.manifests.iter().filter(|m| !st.manifests.contains_key(&m.path)).collect();
    let went: Vec<String> = st.manifests.keys().filter(|p| !parts.manifests.iter().any(|m| m.path == **p)).cloned().collect();
    let inline_changed = st.inline.is_none() != parts.inline.is_empty() || st.inlined != inlined;
    if st.version > 0 && fresh.is_empty() && went.is_empty() && !inline_changed {
        return Ok(None);
    }
    if st.uuid.is_empty() {
        st.uuid = uuid::Uuid::new_v4().to_string();
    }
    let (dir, now, v) = (format!("data/{table}/metadata"), crate::log::now_ms(), st.version + 1);
    let schema = json!({"type": "struct", "schema-id": 0, "fields": fields});
    // The manifests this snapshot adds: one per new manifest of ours, and one for the inline files.
    let mut written = vec![];
    for m in &fresh {
        let files = crate::manifest::files(lake, m).await?;
        written.push((Some(m.path.clone()), write_manifest(lake, &dir, &schema, v, &files).await?));
    }
    if inline_changed && !parts.inline.is_empty() {
        written.push((None, write_manifest(lake, &dir, &schema, v, &parts.inline).await?));
    }
    // The snapshot's manifest list: the ones written now, plus the ones it keeps from before.
    let kept: Vec<Avro> = parts.manifests.iter().filter_map(|m| st.manifests.get(&m.path).cloned()).collect();
    let inline = match inline_changed {
        true => written.iter().find(|(of, _)| of.is_none()).map(|(_, a)| a.clone()),
        false => st.inline.clone(),
    };
    let all: Vec<Avro> = written.iter().map(|(_, a)| a.clone()).chain(kept).chain(inline.clone().filter(|_| !inline_changed)).collect();
    let entries: Vec<Vec<u8>> = all.iter().map(|a| {
        let new = a.seq == v;
        manifest_file(&lake.full(&a.path), a.bytes as usize, v, a.seq, [if new { a.files as usize } else { 0 }, if new { 0 } else { a.files as usize }],
                      [if new { a.rows as i64 } else { 0 }, if new { 0 } else { a.rows as i64 }])
    }).collect();
    let list = format!("{dir}/snap-{v}-{}.avro", uuid::Uuid::new_v4());
    let parent = st.snapshots.last().map(|(s, _)| s["snapshot-id"].clone());
    let list_meta = [("snapshot-id", v.to_string()), ("parent-snapshot-id", parent.as_ref().map_or("null".into(), Value::to_string)), ("sequence-number", v.to_string()), ("format-version", "2".into())];
    lake.put(&list, ocf(&list_schema(), &list_meta, &entries)).await?;
    let (files, rows) = (all.iter().map(|a| a.files).sum::<u64>(), all.iter().map(|a| a.rows).sum::<u64>());
    let added: u64 = written.iter().map(|(_, a)| a.files).sum();
    let op = if went.is_empty() && !inline_changed { "append" } else { "overwrite" };
    let mut snapshot = json!({"snapshot-id": v, "sequence-number": v, "timestamp-ms": now, "manifest-list": lake.full(&list), "schema-id": 0,
        "summary": {"operation": op, "added-data-files": added.to_string(), "total-data-files": files.to_string(), "total-records": rows.to_string()}});
    if let Some(p) = parent {
        snapshot["parent-snapshot-id"] = p;
    }
    st.snapshots.push((snapshot, list));
    let gone: Vec<(Value, String)> = st.snapshots.drain(..st.snapshots.len().saturating_sub(HISTORY)).collect();
    let body = metadata(lake, table, &st.uuid, v, now, &schema, &meta.columns, &st.snapshots);
    // Written once, never overwritten; if it's there, an attempt that crashed wrote it, and this
    // one's objects are garbage the next round's version replaces.
    lake.put(&format!("{dir}/v{v}.metadata.json"), body.to_string().into_bytes()).await?;
    lake.store.put(&Path::from(format!("{dir}/version-hint.text")), v.to_string().into_bytes().into()).await?; // (only a hint)
    // Manifests no longer named go once no snapshot that named them is kept; so do dropped snapshots.
    for p in &went {
        if let Some(a) = st.manifests.remove(p) {
            st.dropped.push((v, a.path));
        }
    }
    if inline_changed {
        st.dropped.extend(st.inline.take().map(|a| (v, a.path)));
    }
    for (s, list) in &gone {
        lake.delete(&format!("{dir}/v{}.metadata.json", s["snapshot-id"])).await;
        lake.delete(list).await;
    }
    let oldest = st.snapshots.first().map_or(v, |(s, _)| s["snapshot-id"].as_u64().unwrap_or(v));
    for (_, path) in st.dropped.iter().filter(|(at, _)| *at < oldest) {
        lake.delete(path).await;
    }
    st.dropped.retain(|(at, _)| *at >= oldest);
    for (of, a) in written {
        match of {
            Some(m) => drop(st.manifests.insert(m, a)),
            None => st.inline = Some(a),
        }
    }
    if !inline_changed {
        st.inline = inline;
    }
    if parts.inline.is_empty() {
        st.inline = None;
    }
    (st.version, st.inlined) = (v, inlined);
    Ok(Some((key, json(&st))))
}

/// One Iceberg manifest holding `files`, added by snapshot `v`.
async fn write_manifest(lake: &Lake, dir: &str, schema: &Value, v: u64, files: &[DataFile]) -> Result<Avro> {
    let entries: Vec<Vec<u8>> = files.iter().map(|f| entry(true, v, &lake.full(&f.path), f.rows, f.bytes)).collect();
    let spec = [("schema", schema.to_string()), ("schema-id", "0".into()), ("partition-spec", "[]".into()), ("partition-spec-id", "0".into()), ("format-version", "2".into()), ("content", "data".into())];
    let body = ocf(&entry_schema(), &spec, &entries);
    let path = format!("{dir}/{}-m0.avro", uuid::Uuid::new_v4());
    let a = Avro { bytes: body.len() as u64, files: files.len() as u64, rows: files.iter().map(|f| f.rows).sum(), seq: v, path: path.clone() };
    lake.put(&path, body).await?;
    Ok(a)
}

/// The table metadata file (format v2): one unpartitioned spec, no sort order, the snapshots kept.
#[allow(clippy::too_many_arguments)]
fn metadata(lake: &Lake, table: &str, uuid: &str, v: u64, now: u64, schema: &Value, columns: &[(String, String)], snapshots: &[(Value, String)]) -> Value {
    // A list's elements need a mapping of their own: arrow-rs writes them as `item` (parquet-mr as `element`).
    let names: Vec<Value> = columns.iter().enumerate().map(|(i, (c, t))| match t.ends_with("[]") {
        true => json!({"field-id": i + 1, "names": [c], "fields": [{"field-id": columns.len() + i + 1, "names": ["item", "element"]}]}),
        false => json!({"field-id": i + 1, "names": [c]}),
    }).collect();
    let older = &snapshots[..snapshots.len() - 1];
    json!({
        "format-version": 2, "table-uuid": uuid, "location": lake.full(&format!("data/{table}")),
        "last-sequence-number": v, "last-updated-ms": now, "last-column-id": 2 * columns.len(), // (list elements take ids after the columns')
        "current-schema-id": 0, "schemas": [schema],
        "default-spec-id": 0, "partition-specs": [{"spec-id": 0, "fields": []}], "last-partition-id": 999,
        "default-sort-order-id": 0, "sort-orders": [{"order-id": 0, "fields": []}],
        "properties": {"schema.name-mapping.default": Value::Array(names).to_string(), "written-by": "pondra"},
        "current-snapshot-id": v, "refs": {"main": {"snapshot-id": v, "type": "branch"}},
        "snapshots": snapshots.iter().map(|(s, _)| s).collect::<Vec<_>>(),
        "snapshot-log": snapshots.iter().map(|(s, _)| json!({"snapshot-id": s["snapshot-id"], "timestamp-ms": s["timestamp-ms"]})).collect::<Vec<_>>(),
        "metadata-log": older.iter().map(|(s, _)| json!({"metadata-file": lake.full(&format!("data/{table}/metadata/v{}.metadata.json", s["snapshot-id"])), "timestamp-ms": s["timestamp-ms"]})).collect::<Vec<_>>(),
    })
}

/// The table's columns as Iceberg fields (ids 1..n, all optional), if every type has an equivalent.
fn fields(columns: &[(String, String)]) -> Option<Vec<Value>> {
    let iceberg = |t: &str| -> Option<String> {
        Some(match t {
            "Int64" => "long".into(),
            "Int32" => "int".into(),
            "Float64" => "double".into(),
            "Float32" => "float".into(),
            "Utf8" | "LargeUtf8" | "Utf8View" => "string".into(),
            "Boolean" => "boolean".into(),
            "Date32" => "date".into(),
            "Binary" | "LargeBinary" => "binary".into(),
            t => match t.parse() {
                Ok(DataType::Timestamp(TimeUnit::Microsecond, tz)) => if tz.is_some() { "timestamptz" } else { "timestamp" }.into(),
                _ => crate::delta::decimal(t).map(|(p, s)| format!("decimal({p}, {s})"))?,
            },
        })
    };
    let n = columns.len();
    // A list column (an embedding, say) needs an id for its elements too: they follow the columns'.
    let kind = |i: usize, t: &String| match t.strip_suffix("[]") {
        Some(item) => Some(json!({"type": "list", "element-id": n + i + 1, "element": iceberg(item)?, "element-required": false})),
        None => Some(Value::String(iceberg(t)?)),
    };
    columns.iter().enumerate().map(|(i, (name, t))| Some(json!({"id": i + 1, "name": name, "required": false, "type": kind(i, t)?}))).collect()
}

// ---------------------------------------------------------------- Avro, just what manifests need

/// Avro's zig-zag varint (both `int` and `long`).
fn long(b: &mut Vec<u8>, v: i64) {
    let mut z = ((v << 1) ^ (v >> 63)) as u64;
    while z >= 0x80 {
        b.push(z as u8 | 0x80);
        z >>= 7;
    }
    b.push(z as u8);
}

fn bytes(b: &mut Vec<u8>, v: &[u8]) {
    long(b, v.len() as i64);
    b.extend(v);
}

/// An Avro object container file: header (schema, codec, Iceberg's keys), one block of records.
fn ocf(schema: &str, meta: &[(&str, String)], records: &[Vec<u8>]) -> Vec<u8> {
    let (mut b, sync) = (b"Obj\x01".to_vec(), *uuid::Uuid::new_v4().as_bytes());
    let header = [("avro.schema", schema.to_string()), ("avro.codec", "null".into())];
    long(&mut b, (header.len() + meta.len()) as i64);
    for (k, v) in header.iter().chain(meta) {
        bytes(&mut b, k.as_bytes());
        bytes(&mut b, v.as_bytes());
    }
    long(&mut b, 0);
    b.extend(sync);
    if !records.is_empty() {
        let body = records.concat();
        long(&mut b, records.len() as i64);
        bytes(&mut b, &body);
        b.extend(sync);
    }
    b
}

/// A manifest entry for a data file (status 1 = added by this snapshot, 0 = existing).
fn entry(added: bool, seq: u64, path: &str, rows: u64, size: u64) -> Vec<u8> {
    let mut b = vec![];
    long(&mut b, added as i64);
    for v in [seq, seq, seq] {
        long(&mut b, 1); // (the union's "long" branch) snapshot id = sequence number = file sequence number
        long(&mut b, v as i64);
    }
    long(&mut b, 0); // content: data
    bytes(&mut b, path.as_bytes());
    bytes(&mut b, b"PARQUET");
    long(&mut b, rows as i64); // (the partition is an empty record: no bytes)
    long(&mut b, size as i64);
    b.extend([0; 10]); // the ten optional fields (column stats, split offsets…): null
    b
}

/// A manifest list entry for the snapshot's one manifest.
fn manifest_file(path: &str, len: usize, seq: u64, min_seq: u64, files: [usize; 2], rows: [i64; 2]) -> Vec<u8> {
    let mut b = vec![];
    bytes(&mut b, path.as_bytes());
    for v in [len as i64, 0, 0, seq as i64, min_seq as i64, seq as i64, files[0] as i64, files[1] as i64, 0, rows[0], rows[1], 0] {
        long(&mut b, v); // length, spec id, content (data), sequence numbers, snapshot, file and row counts
    }
    b.extend([0, 0]); // partitions, key metadata: null
    b
}

fn req(name: &str, id: u32, t: Value) -> Value { json!({"name": name, "field-id": id, "type": t}) }
fn opt(name: &str, id: u32, t: Value) -> Value { json!({"name": name, "field-id": id, "type": ["null", t], "default": null}) }

/// The Avro schema of a manifest entry (Iceberg v2).
fn entry_schema() -> String {
    let map = |k: u32, v: u32, t: &str| json!({"type": "array", "logicalType": "map", "items": {"type": "record", "name": format!("k{k}_v{v}"), "fields": [{"name": "key", "type": "int", "field-id": k}, {"name": "value", "type": t, "field-id": v}]}});
    let list = |id: u32, t: &str| json!({"type": "array", "element-id": id, "items": t});
    let data_file = json!({"type": "record", "name": "r2", "fields": [
        req("content", 134, json!("int")), req("file_path", 100, json!("string")), req("file_format", 101, json!("string")),
        req("partition", 102, json!({"type": "record", "name": "r102", "fields": []})),
        req("record_count", 103, json!("long")), req("file_size_in_bytes", 104, json!("long")),
        opt("column_sizes", 108, map(117, 118, "long")), opt("value_counts", 109, map(119, 120, "long")),
        opt("null_value_counts", 110, map(121, 122, "long")), opt("nan_value_counts", 137, map(138, 139, "long")),
        opt("lower_bounds", 125, map(126, 127, "bytes")), opt("upper_bounds", 128, map(129, 130, "bytes")),
        opt("key_metadata", 131, json!("bytes")), opt("split_offsets", 132, list(133, "long")),
        opt("equality_ids", 135, list(136, "int")), opt("sort_order_id", 140, json!("int")),
    ]});
    let fields = [req("status", 0, json!("int")), opt("snapshot_id", 1, json!("long")), opt("sequence_number", 3, json!("long")),
        opt("file_sequence_number", 4, json!("long")), req("data_file", 2, data_file)];
    json!({"type": "record", "name": "manifest_entry", "fields": fields}).to_string()
}

/// The Avro schema of a manifest list entry (Iceberg v2).
fn list_schema() -> String {
    let summary = json!({"type": "record", "name": "r508", "fields": [req("contains_null", 509, json!("boolean")),
        opt("contains_nan", 518, json!("boolean")), opt("lower_bound", 510, json!("bytes")), opt("upper_bound", 511, json!("bytes"))]});
    let ints = [("manifest_length", 501, "long"), ("partition_spec_id", 502, "int"), ("content", 517, "int"), ("sequence_number", 515, "long"),
        ("min_sequence_number", 516, "long"), ("added_snapshot_id", 503, "long"), ("added_files_count", 504, "int"), ("existing_files_count", 505, "int"),
        ("deleted_files_count", 506, "int"), ("added_rows_count", 512, "long"), ("existing_rows_count", 513, "long"), ("deleted_rows_count", 514, "long")];
    let mut fields = vec![req("manifest_path", 500, json!("string"))];
    fields.extend(ints.iter().map(|(n, id, t)| req(n, *id, json!(t))));
    fields.extend([opt("partitions", 507, json!({"type": "array", "element-id": 508, "items": summary})), opt("key_metadata", 519, json!("bytes"))]);
    json!({"type": "record", "name": "manifest_file", "fields": fields}).to_string()
}

// ---------------------------------------------------------------- the REST catalog

/// The Iceberg REST catalog API over what Pondra publishes (read-only): engines attach a node
/// by URL — PyIceberg, DuckDB, Spark, Trino, Snowflake — instead of pointing at metadata files.
/// Namespace `default` is this lake's `public` schema, and each other schema is a namespace of
/// its own; an attached lake `l` is namespace `l` (its `public`), and `l.s` (`["l", "s"]`) for
/// its others. Tables appear once they publish Iceberg. Tokens work as elsewhere
/// (`Authorization: Bearer …`, any role).
pub fn rest() -> axum::Router<crate::server::App> {
    use axum::routing::get;
    axum::Router::new()
        .route("/v1/config", get(|| async { axum::Json(json!({"defaults": {}, "overrides": {}})) }))
        .route("/v1/namespaces", get(namespaces))
        .route("/v1/namespaces/{ns}", get(namespace).head(namespace))
        .route("/v1/namespaces/{ns}/tables", get(tables))
        .route("/v1/namespaces/{ns}/tables/{table}", get(load).head(load))
}

type Reply = Result<axum::Json<Value>, (axum::http::StatusCode, axum::Json<Value>)>;

fn missing(what: &str, kind: &str) -> (axum::http::StatusCode, axum::Json<Value>) {
    (axum::http::StatusCode::NOT_FOUND, axum::Json(json!({"error": {"message": format!("no {what}"), "type": kind, "code": 404}})))
}

/// Every namespace: its parts, its lake, and the schema in it.
async fn spaces(app: &crate::server::App) -> Vec<(Vec<String>, std::sync::Arc<Lake>, String)> {
    use crate::ddl::{schemas, PUBLIC};
    let attached = app.lake.attached.read().unwrap().clone();
    let mut out = vec![];
    for (name, lake) in [(None, app.lake.clone())].into_iter().chain(attached.into_iter().map(|(n, l)| (Some(n), l))) {
        for s in schemas(&lake).await.unwrap_or_default() {
            let parts = match (&name, s == PUBLIC) {
                (None, true) => vec!["default".to_string()],
                (None, false) => vec![s.clone()],
                (Some(n), true) => vec![n.clone()],
                (Some(n), false) => vec![n.clone(), s.clone()],
            };
            out.push((parts, lake.clone(), s));
        }
    }
    out
}

/// A namespace from the URL (its parts joined by 0x1F, as the REST spec has it).
async fn space(app: &crate::server::App, ns: &str) -> Result<(Vec<String>, std::sync::Arc<Lake>, String), (axum::http::StatusCode, axum::Json<Value>)> {
    let parts: Vec<&str> = ns.split('\u{1f}').collect();
    spaces(app).await.into_iter().find(|(p, _, _)| *p == parts).ok_or_else(|| missing(&format!("namespace {}", parts.join(".")), "NoSuchNamespaceException"))
}

async fn namespaces(axum::extract::State(app): axum::extract::State<crate::server::App>) -> axum::Json<Value> {
    axum::Json(json!({"namespaces": spaces(&app).await.into_iter().map(|(p, _, _)| p).collect::<Vec<_>>()}))
}

async fn namespace(axum::extract::State(app): axum::extract::State<crate::server::App>, axum::extract::Path(ns): axum::extract::Path<String>) -> Reply {
    let (parts, _, _) = space(&app, &ns).await?;
    Ok(axum::Json(json!({"namespace": parts, "properties": {}})))
}

/// The tables with Iceberg metadata published.
async fn tables(axum::extract::State(app): axum::extract::State<crate::server::App>, axum::extract::Path(ns): axum::extract::Path<String>) -> Reply {
    let (parts, lake, schema) = space(&app, &ns).await?;
    let published = lake.cat.scan::<Published>("i/", "i0").await.unwrap_or_default();
    let ids: Vec<Value> = published.iter().filter(|(k, p)| p.version > 0 && crate::ddl::split(&k[2..]).0 == schema)
        .map(|(k, _)| json!({"namespace": parts, "name": crate::ddl::split(&k[2..]).1})).collect();
    Ok(axum::Json(json!({"identifiers": ids})))
}

/// A table: its current metadata file, and what it says.
async fn load(axum::extract::State(app): axum::extract::State<crate::server::App>, axum::extract::Path((ns, table)): axum::extract::Path<(String, String)>) -> Reply {
    let (_, lake, schema) = space(&app, &ns).await?;
    let no_table = || missing(&format!("table {}.{table}", ns.replace('\u{1f}', ".")), "NoSuchTableException");
    let table = crate::ddl::join(&schema, &table);
    let st: Published = lake.cat.get(&format!("i/{table}")).await.ok().flatten().filter(|p: &Published| p.version > 0).ok_or_else(no_table)?;
    let path = format!("data/{table}/metadata/v{}.metadata.json", st.version);
    let metadata: Value = serde_json::from_slice(&lake.object(&path).await.map_err(|_| no_table())?).map_err(|_| no_table())?;
    Ok(axum::Json(json!({"metadata-location": lake.full(&path), "metadata": metadata, "config": {}})))
}
