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
use object_store::{path::Path, ObjectStoreExt};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeMap;

const HISTORY: usize = 100; // snapshots (and metadata files) kept

/// What the Iceberg metadata says right now (kept in the catalog under `i/{table}`).
#[derive(Serialize, Deserialize, Default)]
struct Published {
    uuid: String,
    version: u64,
    files: BTreeMap<String, (u64, u64, u64)>, // path in the lake -> (bytes, rows, version that added it)
    snapshots: Vec<(Value, [String; 2])>,     // kept, oldest first: (snapshot entry, [manifest, manifest list])
}

/// The table's next Iceberg version, if its files changed; returns the new state to record.
pub async fn publish(lake: &Lake, table: &str, meta: &TableMeta) -> Result<Option<(String, Vec<u8>)>> {
    let Some(fields) = fields(&meta.columns) else { return Ok(None) }; // a type Iceberg can't carry
    let Some(files) = crate::delta::publishable(meta) else { return Ok(None) };
    let key = format!("i/{table}");
    let mut st: Published = lake.cat.get(&key).await?.unwrap_or_default();
    if st.version > 0 && files.len() == st.files.len() && files.iter().all(|f| st.files.contains_key(&f.path)) {
        return Ok(None);
    }
    if st.uuid.is_empty() {
        st.uuid = uuid::Uuid::new_v4().to_string();
    }
    let (dir, now) = (format!("data/{table}/metadata"), crate::log::now_ms());
    for v in st.version + 1.. {
        // Every live file, in one manifest: new ones as added by this snapshot, the rest as they were.
        let live: BTreeMap<&str, (u64, u64, u64)> = files.iter().map(|f| (f.path.as_str(), st.files.get(&f.path).copied().unwrap_or((f.bytes, f.rows, v)))).collect();
        let entries: Vec<Vec<u8>> = live.iter().map(|(p, &(bytes, rows, added))| entry(added == v, added, &lake.full(p), rows, bytes)).collect();
        let schema = json!({"type": "struct", "schema-id": 0, "fields": fields});
        let spec = [("schema", schema.to_string()), ("schema-id", "0".into()), ("partition-spec", "[]".into()), ("partition-spec-id", "0".into()), ("format-version", "2".into()), ("content", "data".into())];
        let manifest_bytes = ocf(&entry_schema(), &spec, &entries);
        let id = uuid::Uuid::new_v4();
        let (manifest, list) = (format!("{dir}/{id}-m0.avro"), format!("{dir}/snap-{v}-{id}.avro"));
        let (added, existing): (Vec<_>, Vec<_>) = live.values().partition(|f| f.2 == v);
        let rows = |fs: &[&(u64, u64, u64)]| fs.iter().map(|f| f.1 as i64).sum::<i64>();
        let min_seq = live.values().map(|f| f.2).min().unwrap_or(v);
        let list_entry = manifest_file(&lake.full(&manifest), manifest_bytes.len(), v, min_seq, [added.len(), existing.len()], [rows(&added), rows(&existing)]);
        let parent = st.snapshots.last().map(|(s, _)| s["snapshot-id"].clone());
        let list_meta = [("snapshot-id", v.to_string()), ("parent-snapshot-id", parent.as_ref().map_or("null".into(), Value::to_string)), ("sequence-number", v.to_string()), ("format-version", "2".into())];
        futures::try_join!(lake.put(&manifest, manifest_bytes), lake.put(&list, ocf(&list_schema(), &list_meta, &[list_entry])))?;
        let op = if existing.len() == st.files.len() { "append" } else { "overwrite" }; // (nothing removed, or something)
        let mut snapshot = json!({"snapshot-id": v, "sequence-number": v, "timestamp-ms": now, "manifest-list": lake.full(&list), "schema-id": 0,
            "summary": {"operation": op, "added-data-files": added.len().to_string(), "total-data-files": live.len().to_string(), "total-records": rows(&live.values().collect::<Vec<_>>()).to_string()}});
        if let Some(p) = parent {
            snapshot["parent-snapshot-id"] = p;
        }
        let mut snapshots = st.snapshots.clone();
        snapshots.push((snapshot, [manifest, list]));
        let gone: Vec<(Value, [String; 2])> = snapshots.drain(..snapshots.len().saturating_sub(HISTORY)).collect();
        let body = metadata(lake, table, &st.uuid, v, now, &schema, &meta.columns, &snapshots);
        // Written once, never overwritten; if it's there, an attempt that crashed wrote it: skip it.
        match lake.put(&format!("{dir}/v{v}.metadata.json"), body.to_string().into_bytes()).await {
            Err(e) if e.downcast_ref::<object_store::Error>().is_some_and(|e| matches!(e, object_store::Error::AlreadyExists { .. })) => continue,
            r => r?,
        }
        lake.store.put(&Path::from(format!("{dir}/version-hint.text")), v.to_string().into_bytes().into()).await?; // (only a hint)
        for (s, [manifest, list]) in &gone {
            lake.delete(&format!("{dir}/v{}.metadata.json", s["snapshot-id"])).await;
            lake.delete(manifest).await;
            lake.delete(list).await;
        }
        (st.version, st.snapshots) = (v, snapshots);
        st.files = live.into_iter().map(|(p, f)| (p.to_string(), f)).collect();
        return Ok(Some((key, json(&st))));
    }
    unreachable!("versions never run out")
}

/// The table metadata file (format v2): one unpartitioned spec, no sort order, the snapshots kept.
#[allow(clippy::too_many_arguments)]
fn metadata(lake: &Lake, table: &str, uuid: &str, v: u64, now: u64, schema: &Value, columns: &[(String, String)], snapshots: &[(Value, [String; 2])]) -> Value {
    let names: Vec<Value> = columns.iter().enumerate().map(|(i, (c, _))| json!({"field-id": i + 1, "names": [c]})).collect();
    let older = &snapshots[..snapshots.len() - 1];
    json!({
        "format-version": 2, "table-uuid": uuid, "location": lake.full(&format!("data/{table}")),
        "last-sequence-number": v, "last-updated-ms": now, "last-column-id": columns.len(),
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
            t => crate::delta::decimal(t).map(|(p, s)| format!("decimal({p}, {s})"))?,
        })
    };
    columns.iter().enumerate().map(|(i, (name, t))| Some(json!({"id": i + 1, "name": name, "required": false, "type": iceberg(t)?}))).collect()
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
