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
use anyhow::{Context, Result};
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

/// The table's next Iceberg version, if its files changed (or `named`: another engine's commit
/// is in them, and its version carries that engine's snapshot id); returns the new state to record.
pub async fn publish(lake: &Lake, table: &str, meta: &TableMeta, named: Option<&Named<'_>>) -> Result<Option<(String, Vec<u8>)>> {
    let Some(fields) = fields(meta) else { return Ok(None) }; // a type Iceberg can't carry
    let Some(parts) = crate::delta::publishable(lake, meta).await? else { return Ok(None) };
    let key = format!("i/{table}");
    let mut st: Published = lake.cat.get(&key).await?.unwrap_or_default();
    let inlined: BTreeMap<String, u64> = parts.inline.iter().map(|f| (f.path.clone(), f.rows)).collect();
    let fresh: Vec<&crate::manifest::Manifest> = parts.manifests.iter().filter(|m| !st.manifests.contains_key(&m.path)).collect();
    let went: Vec<String> = st.manifests.keys().filter(|p| !parts.manifests.iter().any(|m| m.path == **p)).cloned().collect();
    let inline_changed = st.inline.is_none() != parts.inline.is_empty() || st.inlined != inlined;
    if st.version > 0 && fresh.is_empty() && went.is_empty() && !inline_changed && named.is_none() {
        return Ok(None);
    }
    if st.uuid.is_empty() {
        st.uuid = uuid::Uuid::new_v4().to_string();
    }
    let (dir, now, mut v) = (format!("data/{}/metadata", meta.folder(table)), crate::log::now_ms(), st.version + 1);
    // (a version already there was written by an attempt that crashed before recording it: it's
    // never overwritten, so this one takes the next number)
    while lake.store.head(&Path::from(format!("{dir}/v{v}.metadata.json"))).await.is_ok() {
        v += 1;
    }
    // Version N is sequence number N; its snapshot id is N too, unless another engine's commit
    // named it (`record`), so the engine finds its snapshot.
    let id = named.map_or(v as i64, |n| n.id);
    let schema = json!({"type": "struct", "schema-id": 0, "fields": fields});
    // The manifests this snapshot adds: one per new manifest of ours, and one for the inline files.
    let mut written = vec![];
    for m in &fresh {
        let files = crate::manifest::files(lake, m).await?;
        written.push((Some(m.path.clone()), write_manifest(lake, &dir, &schema, v, id, &files).await?));
    }
    if inline_changed && !parts.inline.is_empty() {
        written.push((None, write_manifest(lake, &dir, &schema, v, id, &parts.inline).await?));
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
        manifest_file(&lake.full(&a.path), a.bytes as usize, v, a.seq, id, [if new { a.files as usize } else { 0 }, if new { 0 } else { a.files as usize }],
                      [if new { a.rows as i64 } else { 0 }, if new { 0 } else { a.rows as i64 }])
    }).collect();
    let list = format!("{dir}/snap-{v}-{}.avro", uuid::Uuid::new_v4());
    let parent = st.snapshots.last().map(|(s, _)| s["snapshot-id"].clone());
    let list_meta = [("snapshot-id", id.to_string()), ("parent-snapshot-id", parent.as_ref().map_or("null".into(), Value::to_string)), ("sequence-number", v.to_string()), ("format-version", "2".into())];
    lake.put(&list, ocf(&list_schema(), &list_meta, &entries)).await?;
    let (files, rows) = (all.iter().map(|a| a.files).sum::<u64>(), all.iter().map(|a| a.rows).sum::<u64>());
    let added: u64 = written.iter().map(|(_, a)| a.files).sum();
    let op = if went.is_empty() && !inline_changed { "append" } else { "overwrite" };
    let mut summary = named.map(|n| n.summary.clone()).unwrap_or_default(); // (the writer's own keys, `pondra.job` say)
    for (k, val) in [("operation", op.to_string()), ("added-data-files", added.to_string()), ("total-data-files", files.to_string()), ("total-records", rows.to_string())] {
        summary.insert(k.into(), val.into());
    }
    let mut snapshot = json!({"snapshot-id": id, "sequence-number": v, "timestamp-ms": now, "manifest-list": lake.full(&list), "schema-id": 0, "summary": summary});
    if let Some(p) = parent {
        snapshot["parent-snapshot-id"] = p;
    }
    st.snapshots.push((snapshot, list));
    let gone: Vec<(Value, String)> = st.snapshots.drain(..st.snapshots.len().saturating_sub(HISTORY)).collect();
    let body = metadata(lake, meta.folder(table), &st.uuid, v, now, &schema, &meta.columns, &st.snapshots);
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
        lake.delete(&format!("{dir}/v{}.metadata.json", s["sequence-number"])).await;
        lake.delete(list).await;
    }
    let oldest = st.snapshots.first().map_or(v, |(s, _)| s["sequence-number"].as_u64().unwrap_or(v));
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

/// One Iceberg manifest holding `files`, added by version `v` (snapshot `id`).
async fn write_manifest(lake: &Lake, dir: &str, schema: &Value, v: u64, id: i64, files: &[DataFile]) -> Result<Avro> {
    let entries: Vec<Vec<u8>> = files.iter().map(|f| entry(true, v, id, &lake.full(&f.path), f.rows, f.bytes)).collect();
    let spec = [("schema", schema.to_string()), ("schema-id", "0".into()), ("partition-spec", "[]".into()), ("partition-spec-id", "0".into()), ("format-version", "2".into()), ("content", "data".into())];
    let body = ocf(&entry_schema(), &spec, &entries);
    let path = format!("{dir}/{}-m0.avro", uuid::Uuid::new_v4());
    let a = Avro { bytes: body.len() as u64, files: files.len() as u64, rows: files.iter().map(|f| f.rows).sum(), seq: v, path: path.clone() };
    lake.put(&path, body).await?;
    Ok(a)
}

/// The table metadata file (format v2): one unpartitioned spec, no sort order, the snapshots kept.
#[allow(clippy::too_many_arguments)]
fn metadata(lake: &Lake, folder: &str, uuid: &str, v: u64, now: u64, schema: &Value, columns: &[(String, String)], snapshots: &[(Value, String)]) -> Value {
    // A list's elements need a mapping of their own: arrow-rs writes them as `item` (parquet-mr as `element`).
    let mut names: Vec<Value> = columns.iter().enumerate().map(|(i, (c, t))| match t.ends_with("[]") {
        true => json!({"field-id": i + 1, "names": [c], "fields": [{"field-id": columns.len() + i + 1, "names": ["item", "element"]}]}),
        false => json!({"field-id": i + 1, "names": [c]}),
    }).collect();
    // The files also hold the rows' system columns (`sys.rs`), which the schema leaves out: named
    // too (ids of their own, never the schema's), so a reader that maps a file's every column by
    // name — PyIceberg — takes them, and leaves them out as the schema does.
    names.extend(crate::sys::NAMES.iter().enumerate().map(|(i, c)| json!({"field-id": 1_000_001 + i, "names": [c]})));
    let older = &snapshots[..snapshots.len() - 1];
    let current = &snapshots[snapshots.len() - 1].0["snapshot-id"];
    json!({
        "format-version": 2, "table-uuid": uuid, "location": lake.full(&format!("data/{folder}")),
        "last-sequence-number": v, "last-updated-ms": now, "last-column-id": 2 * columns.len(), // (list elements take ids after the columns')
        "current-schema-id": 0, "schemas": [schema],
        "default-spec-id": 0, "partition-specs": [{"spec-id": 0, "fields": []}], "last-partition-id": 999,
        "default-sort-order-id": 0, "sort-orders": [{"order-id": 0, "fields": []}],
        "properties": {"schema.name-mapping.default": Value::Array(names).to_string(), "written-by": "pondra"},
        "current-snapshot-id": current, "refs": {"main": {"snapshot-id": current, "type": "branch"}},
        "snapshots": snapshots.iter().map(|(s, _)| s).collect::<Vec<_>>(),
        "snapshot-log": snapshots.iter().map(|(s, _)| json!({"snapshot-id": s["snapshot-id"], "timestamp-ms": s["timestamp-ms"]})).collect::<Vec<_>>(),
        "metadata-log": older.iter().map(|(s, _)| json!({"metadata-file": lake.full(&format!("data/{folder}/metadata/v{}.metadata.json", s["sequence-number"])), "timestamp-ms": s["timestamp-ms"]})).collect::<Vec<_>>(),
    })
}

/// A new table's first metadata, with no snapshot (`COPY … TO … (FORMAT iceberg)`: ADR-028).
pub fn empty(location: &str, columns: &[(String, String)], now: u64) -> Option<Value> {
    let fields = fields(&TableMeta { columns: columns.to_vec(), ..Default::default() })?;
    Some(json!({
        "format-version": 2, "table-uuid": uuid::Uuid::new_v4().to_string(), "location": location,
        "last-sequence-number": 0, "last-updated-ms": now, "last-column-id": 2 * columns.len(),
        "current-schema-id": 0, "schemas": [{"type": "struct", "schema-id": 0, "fields": fields}],
        "default-spec-id": 0, "partition-specs": [{"spec-id": 0, "fields": []}], "last-partition-id": 999,
        "default-sort-order-id": 0, "sort-orders": [{"order-id": 0, "fields": []}],
        "properties": {"written-by": "pondra"}, "current-snapshot-id": -1, "refs": {}, "snapshots": [], "snapshot-log": [], "metadata-log": [],
    }))
}

/// The table's columns as Iceberg fields, if every type has an equivalent: all optional, each
/// with its place among the stored columns as its id and the name SQL knows it by (a renamed
/// column keeps its id; a dropped one leaves the schema, its id unused again: ADR-022).
fn fields(meta: &TableMeta) -> Option<Vec<Value>> {
    let columns = &meta.columns;
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
    let live = columns.iter().enumerate().filter(|(_, (c, _))| !meta.dropped.contains(c));
    live.map(|(i, (c, t))| Some(json!({"id": i + 1, "name": meta.name_of(c), "required": false, "type": kind(i, t)?}))).collect()
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
fn entry(added: bool, seq: u64, id: i64, path: &str, rows: u64, size: u64) -> Vec<u8> {
    let mut b = vec![];
    long(&mut b, added as i64);
    for v in [id, seq as i64, seq as i64] {
        long(&mut b, 1); // (the union's "long" branch) snapshot id, sequence number, file sequence number
        long(&mut b, v);
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
fn manifest_file(path: &str, len: usize, seq: u64, min_seq: u64, id: i64, files: [usize; 2], rows: [i64; 2]) -> Vec<u8> {
    let mut b = vec![];
    bytes(&mut b, path.as_bytes());
    for v in [len as i64, 0, 0, seq as i64, min_seq as i64, id, files[0] as i64, files[1] as i64, 0, rows[0], rows[1], 0] {
        long(&mut b, v); // length, spec id, content (data), sequence numbers, snapshot, file and row counts
    }
    b.extend([0, 0]); // partitions, key metadata: null
    b
}

pub fn req(name: &str, id: u32, t: Value) -> Value { json!({"name": name, "field-id": id, "type": t}) }
pub fn opt(name: &str, id: u32, t: Value) -> Value { json!({"name": name, "field-id": id, "type": ["null", t], "default": null}) }

/// The Avro schema of a manifest entry (Iceberg v2).
fn entry_schema() -> String { entry_schema_with(vec![]).to_string() }

/// The Avro schema of a manifest entry (Iceberg v2), its partition's fields as given.
pub fn entry_schema_with(partition: Vec<Value>) -> Value {
    let map = |k: u32, v: u32, t: &str| json!({"type": "array", "logicalType": "map", "items": {"type": "record", "name": format!("k{k}_v{v}"), "fields": [{"name": "key", "type": "int", "field-id": k}, {"name": "value", "type": t, "field-id": v}]}});
    let list = |id: u32, t: &str| json!({"type": "array", "element-id": id, "items": t});
    let data_file = json!({"type": "record", "name": "r2", "fields": [
        req("content", 134, json!("int")), req("file_path", 100, json!("string")), req("file_format", 101, json!("string")),
        req("partition", 102, json!({"type": "record", "name": "r102", "fields": partition})),
        req("record_count", 103, json!("long")), req("file_size_in_bytes", 104, json!("long")),
        opt("column_sizes", 108, map(117, 118, "long")), opt("value_counts", 109, map(119, 120, "long")),
        opt("null_value_counts", 110, map(121, 122, "long")), opt("nan_value_counts", 137, map(138, 139, "long")),
        opt("lower_bounds", 125, map(126, 127, "bytes")), opt("upper_bounds", 128, map(129, 130, "bytes")),
        opt("key_metadata", 131, json!("bytes")), opt("split_offsets", 132, list(133, "long")),
        opt("equality_ids", 135, list(136, "int")), opt("sort_order_id", 140, json!("int")),
    ]});
    let fields = [req("status", 0, json!("int")), opt("snapshot_id", 1, json!("long")), opt("sequence_number", 3, json!("long")),
        opt("file_sequence_number", 4, json!("long")), req("data_file", 2, data_file)];
    json!({"type": "record", "name": "manifest_entry", "fields": fields})
}

/// The Avro schema of a manifest list entry (Iceberg v2).
pub fn list_schema() -> String {
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

/// The Iceberg REST catalog API over what Pondra publishes: engines attach a node by URL —
/// PyIceberg, DuckDB, Spark, Trino, Snowflake — instead of pointing at metadata files, and append
/// to its tables (`update`, ADR-028). Namespace `default` is this lake's `public` schema, and each
/// other schema is a namespace of its own; an attached lake `l` is namespace `l` (its `public`),
/// and `l.s` (`["l", "s"]`) for its others. Tables appear once they publish Iceberg. Tokens work
/// as elsewhere (`Authorization: Bearer …`; a commit needs a writer's).
pub fn rest() -> axum::Router<crate::server::App> {
    use axum::routing::{get, post};
    let endpoints = ["GET /v1/{prefix}/namespaces", "GET /v1/{prefix}/namespaces/{namespace}", "HEAD /v1/{prefix}/namespaces/{namespace}",
        "GET /v1/{prefix}/namespaces/{namespace}/tables", "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}",
        "HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}", "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}"];
    let refuse = |what: &'static str| move || async move { Err::<axum::Json<Value>, _>(bad(format!("{what} in Pondra's SQL; other engines read its tables and append to them"))) };
    axum::Router::new()
        .route("/v1/config", get(move || async move { axum::Json(json!({"defaults": {}, "overrides": {}, "endpoints": endpoints})) }))
        .route("/v1/namespaces", get(namespaces).post(refuse("CREATE SCHEMA")))
        .route("/v1/namespaces/{ns}", get(namespace).head(namespace).delete(refuse("DROP SCHEMA")))
        .route("/v1/namespaces/{ns}/tables", get(tables).post(refuse("CREATE TABLE … WITH (publish = 'iceberg')")))
        .route("/v1/namespaces/{ns}/tables/{table}", get(load).head(load).post(update).delete(refuse("DROP TABLE")))
        .route("/v1/tables/rename", post(refuse("ALTER TABLE … RENAME")))
        .route("/v1/transactions/commit", post(refuse("A change to several tables at once goes")))
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
    Ok(axum::Json(loaded(&lake, &crate::ddl::join(&schema, &table)).await.map_err(|_| no_table())?))
}

/// A table's current version, as the catalog answers it (`metadata-location`, `metadata`).
async fn loaded(lake: &Lake, table: &str) -> Result<Value> {
    let st: Published = lake.cat.get(&format!("i/{table}")).await?.filter(|p: &Published| p.version > 0).context("not published")?;
    let path = format!("data/{}/metadata/v{}.metadata.json", crate::store::folder_of(lake, table).await?, st.version);
    let metadata: Value = serde_json::from_slice(&lake.object(&path).await?)?;
    Ok(json!({"metadata-location": lake.full(&path), "metadata": metadata, "config": {}}))
}

// ---------------------------------------------------------------- other engines' appends (ADR-028)

/// Another engine's commit whose version is being published: its snapshot id, and its summary's
/// own keys (`pondra.job`, say), so the engine finds its snapshot.
pub struct Named<'a> {
    pub table: &'a str,
    pub id: i64,
    pub summary: &'a serde_json::Map<String, Value>,
}

/// Another engine's append, as the leader records it (`record`).
#[derive(Serialize, Deserialize)]
pub struct Commit {
    pub table: String,
    pub snapshot: i64, // the writer's snapshot id: the version published for it carries it
    summary: serde_json::Map<String, Value>,
    uuid: Option<String>,        // assert-table-uuid
    parent: Option<Option<i64>>, // assert-ref-snapshot-id on main (Some(None): no snapshot yet)
    incoming: Vec<String>,       // the writer's data files (paths in the lake)
    cleanup: Vec<String>,        // its manifest list and the manifests it added
    /// Those rows as the table's own files, written by the node that took the commit (None: they
    /// go through the log, for the views and tasks that follow the table).
    written: Option<crate::write::Files>,
}

impl Commit {
    fn job(&self) -> String { format!("iceberg:{}:{}", self.table, self.snapshot) }
}

/// The leader's answer when the table changed since the writer read it: 409, so it retries.
pub const CONFLICT: &str = "the table changed since the writer read it";
/// …and when it refused the commit before recording anything: 400.
const REFUSED: &str = "Pondra refused the commit";

type Refusal = (axum::http::StatusCode, axum::Json<Value>);

fn refused(code: u16, kind: &str, message: String) -> Refusal {
    (axum::http::StatusCode::from_u16(code).expect("a status"), axum::Json(json!({"error": {"message": message, "type": kind, "code": code}})))
}

fn bad(message: String) -> Refusal { refused(400, "BadRequestException", message) }

fn conflict(message: String) -> Refusal { refused(409, "CommitFailedException", message) }

/// `POST /v1/namespaces/{ns}/tables/{table}`: another engine's commit. An append's files become
/// the table's rows as a bulk INSERT's would: rewritten here with their row ids, sized and
/// partitioned as the table says (or through the log, when views or tasks follow the table). The
/// leader checks what the commit asserts and records it, and the table's next version is
/// published under the writer's snapshot id. Anything else is refused by name.
async fn update(axum::extract::State(app): axum::extract::State<crate::server::App>, axum::extract::Path((ns, table)): axum::extract::Path<(String, String)>, body: bytes::Bytes) -> Reply {
    let (_, lake, schema) = space(&app, &ns).await?;
    let name = crate::ddl::join(&schema, &table);
    let current = loaded(&lake, &name).await.map_err(|_| missing(&format!("table {}.{table}", ns.replace('\u{1f}', ".")), "NoSuchTableException"))?;
    let asked: Value = serde_json::from_slice(&body).map_err(|e| bad(format!("the commit: {e}")))?;
    let meta = lake.cat.get::<TableMeta>(&crate::store::table_key(&name)).await.ok().flatten().ok_or_else(|| bad(format!("no table {name}")))?;
    if !meta.key.is_empty() {
        return Err(bad(format!("{name} is a keyed table: other engines append to append tables (keyed ones: not yet; INSERT through Pondra)")));
    }
    let mut c = parse(&lake, &name, &current["metadata"], &asked).await?;
    if done(&lake, &c.job()).await.map_err(|e| bad(format!("{e:#}")))? {
        return Ok(axum::Json(current)); // (this commit is in already: a retry, answered as it was)
    }
    check(&lake, &c).await.map_err(|e| conflict(format!("{e:#}")))?;
    let here = std::sync::Arc::ptr_eq(&lake, &app.lake);
    if !c.incoming.is_empty() && !crate::write::follows(&lake, &name).await.map_err(|e| bad(format!("{e:#}")))? {
        let written = async {
            let stamp = match here { true => Some(app.to().reserve().await?), false => crate::write::reserve(&lake).await };
            let (ctx, query) = incoming(&lake, &meta, &c.incoming).await?;
            crate::write::write_files(&lake, &ctx, &name, &query, &c.job(), stamp).await
        };
        c.written = written.await.map_err(|e| bad(format!("{REFUSED}: {e:#}")))?;
    }
    let files: Vec<String> = c.written.iter().flat_map(|f| f.paths()).collect();
    let out = match here {
        true => app.record_iceberg(c).await,
        false => crate::write::send(&lake.url, crate::write::Request::Iceberg(Box::new(c))).await,
    };
    out.map(axum::Json).map_err(|e| {
        let e = format!("{e:#}");
        match (e.contains(CONFLICT), e.contains(REFUSED)) {
            (true, _) => {
                let lake = lake.clone();
                tokio::spawn(async move { futures::future::join_all(files.iter().map(|f| lake.delete(f))).await }); // (never recorded)
                conflict(e)
            }
            (_, true) => bad(e),
            _ => refused(500, "CommitStateUnknownException", e),
        }
    })
}

/// What a commit asks, if Pondra takes it: one append snapshot, on main, and the Parquet files it
/// adds in the table's `data/` folder.
async fn parse(lake: &Lake, table: &str, meta: &Value, asked: &Value) -> Result<Commit, Refusal> {
    let (mut uuid, mut parent, mut snapshot, mut main) = (None, None, None, None);
    for r in asked["requirements"].as_array().into_iter().flatten() {
        let kind = r["type"].as_str().unwrap_or_default();
        let same = |mine: &str| r[kind.trim_start_matches("assert-")] == meta[mine];
        match kind {
            "assert-table-uuid" => uuid = r["uuid"].as_str().map(String::from),
            "assert-ref-snapshot-id" if r["ref"] == "main" => parent = Some(r["snapshot-id"].as_i64()),
            "assert-ref-snapshot-id" => return Err(bad(format!("branch or tag {}: Pondra's tables have main only", r["ref"]))),
            "assert-current-schema-id" | "assert-default-spec-id" | "assert-default-sort-order-id" => {
                if !same(kind.trim_start_matches("assert-")) {
                    return Err(conflict(format!("{kind}: {CONFLICT}")));
                }
            }
            "assert-last-assigned-field-id" | "assert-last-assigned-partition-id" => {
                if !same(if kind.ends_with("field-id") { "last-column-id" } else { "last-partition-id" }) {
                    return Err(conflict(format!("{kind}: {CONFLICT}")));
                }
            }
            "assert-create" => return Err(conflict(format!("{table} exists"))),
            k => return Err(bad(format!("requirement {k}: not one Pondra checks"))),
        }
    }
    for u in asked["updates"].as_array().into_iter().flatten() {
        match u["action"].as_str().unwrap_or_default() {
            "add-snapshot" if snapshot.is_none() => snapshot = Some(u["snapshot"].clone()),
            "set-snapshot-ref" if u["ref-name"] == "main" && u["type"] == "branch" => main = u["snapshot-id"].as_i64(),
            "set-snapshot-ref" => return Err(bad(format!("branch or tag {}: Pondra's tables have main only", u["ref-name"]))),
            a => return Err(bad(format!("{a}: a Pondra table changes through its SQL (ALTER TABLE; DELETE, UPDATE, MERGE), and other engines append to it"))),
        }
    }
    let s = snapshot.ok_or_else(|| bad("a commit with no snapshot: other engines append to Pondra's tables".into()))?;
    let id = s["snapshot-id"].as_i64().ok_or_else(|| bad("a snapshot without its snapshot-id".into()))?;
    if main != Some(id) {
        return Err(bad("a snapshot not made main (staged): not taken".into()));
    }
    let op = s["summary"]["operation"].as_str().unwrap_or("append");
    if op != "append" {
        return Err(bad(format!("a snapshot that does {op}: other engines append; deletes and overwrites go through Pondra's SQL (DELETE, UPDATE, MERGE)")));
    }
    if !s["schema-id"].is_null() && s["schema-id"] != meta["current-schema-id"] {
        return Err(conflict(format!("the snapshot's schema: {CONFLICT}")));
    }
    let home = crate::store::folder_of(lake, table).await.map_err(|e| bad(format!("{e:#}")))?; // (its files' folder: its name, unless it was renamed)
    let under = |uri: &Value, folder: &str| -> Result<String, Refusal> {
        let uri = uri.as_str().unwrap_or_default();
        inside(lake, uri).filter(|p| p.starts_with(&format!("data/{home}/{folder}/"))).ok_or_else(|| bad(format!("{uri}: not in {table}'s {folder} folder")))
    };
    let get = |path: String| async move {
        let bytes = lake.store.get(&Path::from(path.as_str())).await.map_err(|e| bad(format!("{path}: {e}")))?.bytes().await.map_err(|e| bad(format!("{path}: {e}")))?;
        crate::avro::records(&bytes).map_err(|e| bad(format!("{path}: {e:#}")))
    };
    let list = under(&s["manifest-list"], "metadata")?;
    let (mut incoming, mut cleanup) = (vec![], vec![list.clone()]);
    for m in get(list).await? {
        if m["added_snapshot_id"].as_i64() != Some(id) {
            continue; // (the table's manifests, as they were)
        }
        if m["content"].as_i64().unwrap_or(0) != 0 {
            return Err(bad("delete files: other engines append; deletes go through Pondra's SQL".into()));
        }
        let path = under(&m["manifest_path"], "metadata")?;
        cleanup.push(path.clone());
        for e in get(path).await? {
            match e["status"].as_i64() {
                Some(1) => {}
                Some(0) => continue, // (a file already in the table, in a manifest the writer merged)
                _ => return Err(bad("an append that deletes files: not taken".into())),
            }
            let d = &e["data_file"];
            if d["content"].as_i64().unwrap_or(0) != 0 || !d["file_format"].as_str().unwrap_or_default().eq_ignore_ascii_case("parquet") {
                return Err(bad(format!("{}: Pondra takes Parquet data files", d["file_path"])));
            }
            incoming.push(under(&d["file_path"], "data")?);
        }
    }
    let summary = s["summary"].as_object().cloned().unwrap_or_default();
    Ok(Commit { table: table.into(), snapshot: id, summary, uuid, parent, incoming, cleanup, written: None })
}

/// A writer's URI as a path in the lake, if it is in it (`file:` or none, for a lake on disk).
fn inside(lake: &Lake, uri: &str) -> Option<String> {
    let plain = |u: &str| u.strip_prefix("file://").or_else(|| u.strip_prefix("file:")).unwrap_or(u).replacen("s3a://", "s3://", 1);
    let rest = plain(uri).strip_prefix(&format!("{}/", plain(&lake.url).trim_end_matches('/')))?.to_string();
    (!rest.split('/').any(|p| p == ".." || p == "." || p.is_empty())).then_some(rest)
}

/// Does the table still stand as the writer read it (its uuid, and main's snapshot)?
async fn check(lake: &Lake, c: &Commit) -> Result<()> {
    let st: Published = lake.cat.get(&format!("i/{}", c.table)).await?.unwrap_or_default();
    anyhow::ensure!(c.uuid.as_ref().is_none_or(|u| *u == st.uuid), "{CONFLICT}: it is another table now (a new uuid)");
    let main = st.snapshots.last().and_then(|(s, _)| s["snapshot-id"].as_i64());
    anyhow::ensure!(c.parent.is_none_or(|p| p == main), "{CONFLICT}: main is snapshot {main:?} now, not {:?}", c.parent.flatten());
    anyhow::ensure!(!st.snapshots.iter().any(|(s, _)| s["snapshot-id"].as_i64() == Some(c.snapshot)), "{CONFLICT}: snapshot id {} is taken", c.snapshot);
    Ok(())
}

/// Has this commit been recorded?
async fn done(lake: &Lake, job: &str) -> Result<bool> { Ok(lake.cat.get::<u64>(&crate::store::producer_key(&format!("job:{job}"))).await?.is_some()) }

/// The writer's files as a table `__incoming` of the table's columns (by name), and the query
/// that reads them in the table's order.
async fn incoming(lake: &Lake, meta: &TableMeta, files: &[String]) -> Result<(datafusion::prelude::SessionContext, String)> {
    let meta = meta.logical();
    let schema = crate::query::schema(&meta.columns)?;
    let ctx = lake.session();
    let urls: Vec<String> = files.iter().map(|f| lake.full(f)).collect();
    let df = ctx.read_parquet(urls, datafusion::prelude::ParquetReadOptions::default().schema(&schema)).await?;
    ctx.register_table("__incoming", df.into_view())?;
    let columns: Vec<String> = meta.columns.iter().map(|(c, _)| format!("\"{}\"", c.replace('"', "\"\""))).collect();
    Ok((ctx, format!("SELECT {} FROM __incoming", columns.join(", "))))
}

/// Leader, under the lake's lock: record another engine's append, once, if the table still
/// stands as the writer read it; then publish its next version under the writer's snapshot id,
/// and delete the writer's own files. Answers the table as the catalog does.
pub async fn record(lake: &Lake, seq: &crate::log::Sequencer, c: Commit, nodes: &[String], me: &str) -> Result<Value> {
    if !done(lake, &c.job()).await? {
        check(lake, &c).await?;
        let files: Vec<String> = c.written.iter().flat_map(|f| f.paths()).collect();
        let into_files = match c.written {
            Some(ref f) => match crate::write::record(lake, f.clone(), Some(seq)).await {
                Err(e) if format!("{e:#}").contains(crate::write::AGAIN) => {
                    futures::future::join_all(files.iter().map(|f| lake.delete(f))).await; // (a view came: through the log)
                    false
                }
                r => r.map(|_| true)?,
            },
            None => false,
        };
        if !into_files {
            through_log(lake, seq, &c, nodes, me).await?;
        }
        crate::delta::publish_named(lake, Some(&Named { table: &c.table, id: c.snapshot, summary: &c.summary })).await?;
        futures::future::join_all(c.incoming.iter().map(|f| lake.delete(f))).await; // (its rows are the table's now)
        // Its manifests go with the table's replaced files, after the retention period: the writer
        // may read them once more as it cleans up after its commit (Iceberg 1.10 does).
        let key = crate::store::table_key(&c.table);
        if let Some(mut meta) = lake.cat.get::<TableMeta>(&key).await? {
            let now = crate::log::now_ms();
            meta.garbage.extend(c.cleanup.iter().map(|p| (p.clone(), now)));
            lake.cat.commit(vec![(key, json(&meta))], &[]).await?;
        }
    }
    loaded(lake, &c.table).await
}

/// The writer's rows into the log, for the views and tasks that follow the table (one append:
/// all or nothing), then into files at once, so the version published holds them.
async fn through_log(lake: &Lake, seq: &crate::log::Sequencer, c: &Commit, nodes: &[String], me: &str) -> Result<()> {
    use crate::log::{pack, Append, Outcome, Src};
    if !c.incoming.is_empty() {
        let meta = lake.cat.get::<TableMeta>(&crate::store::table_key(&c.table)).await?.context("no table")?;
        let rows = async {
            let (ctx, query) = incoming(lake, &meta, &c.incoming).await?;
            crate::write::rows(&ctx, &meta.logical(), &query).await
        };
        let batch = rows.await.context(REFUSED)?;
        let first = loop {
            if let Some(f) = lake.ids.take(batch.num_rows() as u64) {
                break f;
            }
            lake.ids.refill(seq.reserve().await?.0);
        };
        let batch = crate::sys::stamp(&batch, first)?;
        let append = [Append { table: c.table.clone(), src: Src { producer: c.job(), seq: 1, prev: None }, batch, ack: tokio::sync::oneshot::channel().0 }];
        while let Outcome::Retry(r) = seq.submit(pack(lake, &append).await?).await? {
            anyhow::ensure!(r.is_empty(), "the log refused the commit's rows");
            tokio::time::sleep(std::time::Duration::from_millis(10)).await; // (views changed: pack again)
        }
        let upto = lake.visible();
        while !crate::tier::tier_table(lake, &c.table, upto, nodes, me).await?.1 {}
    }
    lake.cat.commit(vec![(crate::store::producer_key(&format!("job:{}", c.job())), json(&1u64))], &[]).await
}
