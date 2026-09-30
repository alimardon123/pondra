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
    #[serde(default)]
    shape: u64, // its schema's and layout's (a hash): a version is published when they change too
}

/// An Iceberg manifest this lake wrote.
#[derive(Serialize, Deserialize, Default, Clone)]
struct Avro {
    path: String,
    bytes: u64,
    files: u64,
    rows: u64,
    seq: u64, // the snapshot that added it; its entries carry this sequence number
    #[serde(default)]
    spec: i32, // its partition spec: 0 (none), or 1 (`Layout`)
}

/// A table's layout as Iceberg says it, for writers to follow (ADR-029 §6): its partition spec
/// (`partition_by`: identity, or year, month, day or hour of a time; spec 1, since spec 0, no
/// partitions, is what manifests written before said), its sort order (`cluster_by` on one
/// column; two or more follow a Hilbert curve, which Iceberg can't say), and its key as the
/// schema's identifier fields (required, as Iceberg asks; a key is NOT NULL).
pub struct Layout {
    partition: Option<(usize, String, DataType)>, // the stored column's place, the transform, its type
    name: String,                                 // the partition field's name
    sort: Option<usize>,
    keys: Vec<usize>,
}

impl Layout {
    pub fn of(meta: &TableMeta) -> Layout {
        let at = |c: &str| meta.columns.iter().position(|(n, _)| n == c);
        let partition = meta.partition.as_deref().and_then(|p| {
            let (transform, col) = match p.split_once('(') {
                Some((f, c)) => (f.trim().to_string(), c.trim_end_matches(')').trim()),
                None => ("identity".to_string(), p.trim()),
            };
            let i = at(col)?;
            let t = crate::query::dtype(&meta.columns[i].1).ok()?;
            let time = matches!(t, DataType::Date32 | DataType::Timestamp(TimeUnit::Microsecond, _));
            let fits = match transform.as_str() {
                "identity" => time || matches!(t, DataType::Int32 | DataType::Int64 | DataType::Utf8 | DataType::Boolean),
                _ => time,
            };
            fits.then_some((i, transform, t))
        });
        let name = partition.as_ref().map_or(String::new(), |(i, t, _)| match t.as_str() {
            "identity" => meta.name_of(&meta.columns[*i].0).to_string(),
            t => format!("{}_{t}", meta.name_of(&meta.columns[*i].0)),
        });
        let sort = (meta.cluster.len() == 1 && meta.key.is_empty()).then(|| at(&meta.cluster[0])).flatten();
        Layout { partition, name, sort, keys: meta.key.iter().filter_map(|k| at(k)).collect() }
    }

    /// Does the table publish its partitions (or have none)? A writer lays out its files so only then.
    pub fn followable(meta: &TableMeta) -> bool { meta.partition.is_none() || Layout::of(meta).partition.is_some() }

    fn spec(&self) -> Value {
        match &self.partition {
            Some((i, t, _)) => json!([{"source-id": i + 1, "field-id": 1000, "name": self.name, "transform": t}]),
            None => json!([]),
        }
    }

    /// The partition record's Avro fields, as manifests carry them.
    fn avro(&self) -> Vec<Value> {
        let Some((_, t, dt)) = &self.partition else { return vec![] };
        let kind = match (t.as_str(), dt) {
            ("identity", DataType::Int32) => json!("int"),
            ("identity", DataType::Int64) => json!("long"),
            ("identity", DataType::Utf8) => json!("string"),
            ("identity", DataType::Boolean) => json!("boolean"),
            ("identity", DataType::Date32) => json!({"type": "int", "logicalType": "date"}),
            ("identity", _) => json!({"type": "long", "logicalType": "timestamp-micros", "adjust-to-utc": matches!(dt, DataType::Timestamp(_, Some(_)))}),
            _ => json!("int"),
        };
        vec![opt(&self.name, 1000, kind)]
    }

    /// A file's partition record, Avro-encoded, from its partition value (`DataFile::part`: the
    /// text `tier::split` gives it).
    fn record(&self, part: &str) -> Result<Vec<u8>> {
        use datafusion::arrow::array::{AsArray, StringArray};
        use datafusion::arrow::datatypes::{Date32Type, Int32Type, Int64Type, TimestampMicrosecondType};
        let Some((_, t, dt)) = &self.partition else { return Ok(vec![]) };
        if part == "null" {
            return Ok(vec![0]); // (the union's null)
        }
        let mut b = vec![];
        long(&mut b, 1);
        let text: datafusion::arrow::array::ArrayRef = std::sync::Arc::new(StringArray::from(vec![part]));
        let micros = |a: datafusion::arrow::array::ArrayRef| a.as_primitive::<TimestampMicrosecondType>().value(0);
        let as_time = || -> Result<i64> { Ok(micros(datafusion::arrow::compute::cast(&text, &DataType::Timestamp(TimeUnit::Microsecond, None))?)) };
        match (t.as_str(), dt) {
            ("identity", DataType::Utf8) => bytes(&mut b, part.as_bytes()),
            ("identity", DataType::Boolean) => b.push((part == "true") as u8),
            ("identity", DataType::Int32) => long(&mut b, datafusion::arrow::compute::cast(&text, dt)?.as_primitive::<Int32Type>().value(0) as i64),
            ("identity", DataType::Int64) => long(&mut b, datafusion::arrow::compute::cast(&text, dt)?.as_primitive::<Int64Type>().value(0)),
            ("identity", DataType::Date32) => long(&mut b, datafusion::arrow::compute::cast(&text, dt)?.as_primitive::<Date32Type>().value(0) as i64),
            ("identity", _) => long(&mut b, micros(datafusion::arrow::compute::cast(&text, dt)?)),
            (unit, _) => {
                use chrono::Datelike;
                let us = as_time()?;
                let d = chrono::DateTime::from_timestamp_micros(us).context("a time out of range")?.naive_utc();
                long(&mut b, match unit {
                    "year" => d.year() as i64 - 1970,
                    "month" => (d.year() as i64 - 1970) * 12 + d.month0() as i64,
                    "day" => us.div_euclid(86_400_000_000),
                    _ => us.div_euclid(3_600_000_000),
                });
            }
        }
        Ok(b)
    }
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
    let layout = Layout::of(meta);
    let shape = std::hash::BuildHasher::hash_one(&std::hash::BuildHasherDefault::<std::collections::hash_map::DefaultHasher>::default(),
        json!([fields, layout.spec(), layout.sort, layout.keys, meta.columns.iter().map(|(c, _)| meta.name_of(c)).collect::<Vec<_>>()]).to_string()); // (a rename, say)
    if st.version > 0 && fresh.is_empty() && went.is_empty() && !inline_changed && named.is_none() && st.shape == shape {
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
    let schema = json!({"type": "struct", "schema-id": 0, "fields": fields, "identifier-field-ids": layout.keys.iter().map(|i| i + 1).collect::<Vec<_>>()});
    // The manifests this snapshot adds: one per new manifest of ours, and one for the inline files.
    let mut written = vec![];
    for m in &fresh {
        let files = crate::manifest::files(lake, m).await?;
        written.push((Some(m.path.clone()), write_manifest(lake, &dir, &schema, &layout, v, id, &files).await?));
    }
    if inline_changed && !parts.inline.is_empty() {
        written.push((None, write_manifest(lake, &dir, &schema, &layout, v, id, &parts.inline).await?));
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
        manifest_file(&lake.full(&a.path), a.bytes as usize, a.spec, v, a.seq, id, [if new { a.files as usize } else { 0 }, if new { 0 } else { a.files as usize }],
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
    let body = metadata(lake, meta.folder(table), &st.uuid, v, now, &schema, &layout, meta, &st.snapshots);
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
    (st.version, st.inlined, st.shape) = (v, inlined, shape);
    Ok(Some((key, json(&st))))
}

/// One Iceberg manifest holding `files`, added by version `v` (snapshot `id`), each file with its
/// partition (under the layout's spec).
#[allow(clippy::too_many_arguments)]
async fn write_manifest(lake: &Lake, dir: &str, schema: &Value, layout: &Layout, v: u64, id: i64, files: &[DataFile]) -> Result<Avro> {
    let entries: Vec<Vec<u8>> = files.iter().map(|f| Ok(entry(true, v, id, &lake.full(&f.path), &layout.record(&f.part)?, f.rows, f.bytes))).collect::<Result<_>>()?;
    let spec_id = if layout.partition.is_some() { 1 } else { 0 };
    let spec = [("schema", schema.to_string()), ("schema-id", "0".into()), ("partition-spec", layout.spec().to_string()), ("partition-spec-id", spec_id.to_string()), ("format-version", "2".into()), ("content", "data".into())];
    let body = ocf(&entry_schema_with(layout.avro()).to_string(), &spec, &entries);
    let path = format!("{dir}/{}-m0.avro", uuid::Uuid::new_v4());
    let a = Avro { bytes: body.len() as u64, files: files.len() as u64, rows: files.iter().map(|f| f.rows).sum(), seq: v, path: path.clone(), spec: spec_id };
    lake.put(&path, body).await?;
    Ok(a)
}

/// The table metadata file (format v2): its layout (`Layout`), the snapshots kept.
#[allow(clippy::too_many_arguments)]
fn metadata(lake: &Lake, folder: &str, uuid: &str, v: u64, now: u64, schema: &Value, layout: &Layout, meta: &TableMeta, snapshots: &[(Value, String)]) -> Value {
    let columns = &meta.columns;
    // A list's elements need a mapping of their own: arrow-rs writes them as `item` (parquet-mr as `element`).
    // (a renamed column by both names: Pondra's files keep the stored one, other writers write SQL's)
    let known = |c: &String| if meta.name_of(c) == c { json!([c]) } else { json!([meta.name_of(c), c]) };
    let mut names: Vec<Value> = columns.iter().enumerate().map(|(i, (c, t))| match t.ends_with("[]") {
        true => json!({"field-id": i + 1, "names": known(c), "fields": [{"field-id": columns.len() + i + 1, "names": ["item", "element"]}]}),
        false => json!({"field-id": i + 1, "names": known(c)}),
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
        "default-spec-id": layout.partition.is_some() as i32, "last-partition-id": if layout.partition.is_some() { 1000 } else { 999 },
        "partition-specs": std::iter::once(json!({"spec-id": 0, "fields": []})).chain(layout.partition.as_ref().map(|_| json!({"spec-id": 1, "fields": layout.spec()}))).collect::<Vec<_>>(),
        "default-sort-order-id": layout.sort.is_some() as i32,
        "sort-orders": std::iter::once(json!({"order-id": 0, "fields": []})).chain(layout.sort.map(|i| json!({"order-id": 1, "fields": [{"transform": "identity", "source-id": i + 1, "direction": "asc", "null-order": "nulls-last"}]}))).collect::<Vec<_>>(),
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

/// The table's columns as Iceberg fields, if every type has an equivalent: optional but a key's, each
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
    live.map(|(i, (c, t))| Some(json!({"id": i + 1, "name": meta.name_of(c), "required": meta.key.contains(c), "type": kind(i, t)?}))).collect() // (a key: identifier fields are required)
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

/// A manifest entry for a data file (status 1 = added by this snapshot, 0 = existing), its
/// partition record as encoded (`Layout::record`).
fn entry(added: bool, seq: u64, id: i64, path: &str, partition: &[u8], rows: u64, size: u64) -> Vec<u8> {
    let mut b = vec![];
    long(&mut b, added as i64);
    for v in [id, seq as i64, seq as i64] {
        long(&mut b, 1); // (the union's "long" branch) snapshot id, sequence number, file sequence number
        long(&mut b, v);
    }
    long(&mut b, 0); // content: data
    bytes(&mut b, path.as_bytes());
    bytes(&mut b, b"PARQUET");
    b.extend(partition);
    long(&mut b, rows as i64);
    long(&mut b, size as i64);
    b.extend([0; 10]); // the ten optional fields (column stats, split offsets…): null
    b
}

/// A manifest list entry for one of the snapshot's manifests.
#[allow(clippy::too_many_arguments)]
fn manifest_file(path: &str, len: usize, spec: i32, seq: u64, min_seq: u64, id: i64, files: [usize; 2], rows: [i64; 2]) -> Vec<u8> {
    let mut b = vec![];
    bytes(&mut b, path.as_bytes());
    for v in [len as i64, spec as i64, 0, seq as i64, min_seq as i64, id, files[0] as i64, files[1] as i64, 0, rows[0], rows[1], 0] {
        long(&mut b, v); // length, spec id, content (data), sequence numbers, snapshot, file and row counts
    }
    b.extend([0, 0]); // partitions, key metadata: null
    b
}

pub fn req(name: &str, id: u32, t: Value) -> Value { json!({"name": name, "field-id": id, "type": t}) }
pub fn opt(name: &str, id: u32, t: Value) -> Value { json!({"name": name, "field-id": id, "type": ["null", t], "default": null}) }

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
        "GET /v1/{prefix}/namespaces/{namespace}/tables", "POST /v1/{prefix}/namespaces/{namespace}/tables", "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}",
        "HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}", "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}",
        "DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}", "POST /v1/{prefix}/tables/rename"];
    let refuse = |what: &'static str| move || async move { Err::<axum::Json<Value>, _>(bad(format!("{what} in Pondra's SQL; other engines read its tables and append to them"))) };
    axum::Router::new()
        .route("/v1/config", get(move || async move { axum::Json(json!({"defaults": {}, "overrides": {}, "endpoints": endpoints})) }))
        .route("/v1/namespaces", get(namespaces).post(refuse("CREATE SCHEMA")))
        .route("/v1/namespaces/{ns}", get(namespace).head(namespace).delete(refuse("DROP SCHEMA")))
        .route("/v1/namespaces/{ns}/tables", get(tables).post(create))
        .route("/v1/namespaces/{ns}/tables/{table}", get(load).head(load).post(update).delete(drop_table))
        .route("/v1/tables/rename", post(rename))
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

/// A table's name as SQL says it, from its namespace (`spaces`).
fn sql_name(app: &crate::server::App, parts: &[String], lake: &std::sync::Arc<Lake>, schema: &str, table: &str) -> String {
    let q = |n: &str| format!("\"{}\"", n.replace('"', "\"\""));
    match (std::sync::Arc::ptr_eq(lake, &app.lake), schema == crate::ddl::PUBLIC) {
        (true, true) => q(table),
        (true, false) => format!("{}.{}", q(schema), q(table)),
        (false, _) => format!("{}.{}.{}", q(&parts[0]), q(schema), q(table)),
    }
}

/// A statement for the catalog's table changes, run as the caller (DDL: an admin's).
async fn as_caller(app: &crate::server::App, role: crate::auth::Role, sql: &str) -> Result<(), Refusal> {
    let who = crate::routines::Who { role, files: false, depth: 0 };
    Box::pin(crate::routines::one(app, sql, who, None)).await.map(|_| ()).map_err(|e| match format!("{e:#}") {
        e if e.contains("may not") => refused(403, "ForbiddenException", e),
        e if e.contains("already exists") => refused(409, "AlreadyExistsException", e),
        e => bad(e),
    })
}

/// `POST /v1/namespaces/{ns}/tables`: a table made through the catalog (ADR-029 §8), as `CREATE
/// TABLE` makes it: its schema's types as Pondra's (a required field NOT NULL, the identifier
/// fields its key), its partition spec as `partition_by` (one field: identity, or year, month,
/// day or hour), its write order as `cluster_by` (columns sorted ascending), published as Iceberg
/// (and anything else its `publish` property names). The metadata answered is its first
/// version's. What Pondra's tables can't be is refused by name.
async fn create(axum::extract::State(app): axum::extract::State<crate::server::App>, axum::Extension(role): axum::Extension<crate::auth::Role>, axum::extract::Path(ns): axum::extract::Path<String>, body: bytes::Bytes) -> Reply {
    let (parts, lake, schema) = space(&app, &ns).await?;
    let asked: Value = serde_json::from_slice(&body).map_err(|e| bad(format!("the table: {e}")))?;
    let table = asked["name"].as_str().filter(|n| !n.is_empty()).ok_or_else(|| bad("a table without its name".into()))?;
    let sql = create_sql(&sql_name(&app, &parts, &lake, &schema, table), &asked).map_err(|e| bad(format!("{e:#}")))?;
    as_caller(&app, role, &sql).await?;
    let name = crate::ddl::join(&schema, table);
    for _ in 0..600 {
        if let Ok(v) = loaded(&lake, &name).await {
            return Ok(axum::Json(v)); // (its first version: published by the leader's next round)
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    Err(refused(500, "ServiceUnavailableException", format!("{name} was made, and isn't published yet: load it again")))
}

/// An Iceberg field's type as Pondra's SQL says it.
fn sql_type(t: &Value) -> Result<String> {
    use anyhow::bail;
    let prim = |t: &str| -> Result<String> {
        Ok(match t {
            "boolean" => "BOOLEAN".into(),
            "int" => "INT".into(),
            "long" => "BIGINT".into(),
            "float" => "REAL".into(),
            "double" => "DOUBLE".into(),
            "date" => "DATE".into(),
            "timestamp" => "TIMESTAMP".into(),
            "timestamptz" => "TIMESTAMPTZ".into(),
            "string" => "VARCHAR".into(),
            "binary" => "BYTEA".into(),
            t if t.starts_with("decimal(") => t.to_uppercase(),
            t => bail!("an Iceberg {t} column: not one Pondra has (boolean, int, long, float, double, date, timestamp, timestamptz, string, binary, decimal, and lists of them)"),
        })
    };
    match t {
        Value::String(t) => prim(t),
        t if t["type"] == "list" && t["element"].is_string() => Ok(format!("{}[]", prim(t["element"].as_str().unwrap_or_default())?)),
        t => bail!("an Iceberg {} column: not one Pondra has (lists of plain values are)", t["type"]),
    }
}

/// A catalog's table request as `CREATE TABLE` (`create`).
fn create_sql(name: &str, asked: &Value) -> Result<String> {
    use anyhow::bail;
    anyhow::ensure!(!asked["stage-create"].as_bool().unwrap_or(false), "a staged create (a table made with its first commit): not yet; make it, then append");
    let fields = asked["schema"]["fields"].as_array().context("a table without its schema's fields")?;
    let q = |n: &str| format!("\"{}\"", n.replace('"', "\"\""));
    let by_id = |id: &Value| fields.iter().find(|f| f["id"] == *id).and_then(|f| f["name"].as_str()).with_context(|| format!("no field {id} in the schema"));
    let mut columns = vec![];
    for f in fields {
        let n = f["name"].as_str().context("a field without its name")?;
        columns.push(format!("{} {}{}", q(n), sql_type(&f["type"])?, if f["required"].as_bool().unwrap_or(false) { " NOT NULL" } else { "" }));
    }
    let keys: Vec<String> = asked["schema"]["identifier-field-ids"].as_array().into_iter().flatten().map(|id| by_id(id).map(q)).collect::<Result<_>>()?;
    if !keys.is_empty() {
        columns.push(format!("PRIMARY KEY ({})", keys.join(", ")));
    }
    let mut options = vec![];
    let publish = asked["properties"]["publish"].as_str().unwrap_or("iceberg");
    anyhow::ensure!(publish.split(',').any(|f| f.trim() == "iceberg"), "publish = '{publish}': a table made through the Iceberg catalog publishes Iceberg");
    options.push(format!("publish = '{}'", publish.replace('\'', "")));
    let spec = asked["partition-spec"]["fields"].as_array().cloned().unwrap_or_default();
    match spec.as_slice() {
        [] => {}
        [f] => {
            let col = by_id(&f["source-id"])?;
            let by = match f["transform"].as_str().unwrap_or("identity") {
                "identity" => col.to_string(),
                t @ ("year" | "month" | "day" | "hour") => format!("{t}({col})"),
                t => bail!("a partition by {t}: Pondra's tables partition by a column, or the year, month, day or hour of one"),
            };
            options.push(format!("partition_by = '{}'", by.replace('\'', "")));
        }
        _ => bail!("a partition spec of {} fields: Pondra's tables partition by one", spec.len()),
    }
    let order = asked["write-order"]["fields"].as_array().cloned().unwrap_or_default();
    if !order.is_empty() {
        let mut cols = vec![];
        for f in &order {
            anyhow::ensure!(f["transform"].as_str().unwrap_or("identity") == "identity" && f["direction"].as_str().unwrap_or("asc") == "asc", "a write order by {} {}: Pondra's tables sort by columns, ascending (cluster_by)", f["transform"], f["direction"]);
            cols.push(by_id(&f["source-id"])?.replace('\'', ""));
        }
        options.push(format!("cluster_by = '{}'", cols.join(",")));
    }
    Ok(format!("CREATE TABLE {name} ({}) WITH ({})", columns.join(", "), options.join(", ")))
}

/// A commit that changes the schema (`add-schema` and `set-current-schema`, no snapshot: PyIceberg's
/// `update_schema`, Spark's `ALTER TABLE`) as the `ALTER TABLE` statements it comes to (ADR-029
/// §8), by field id against the current schema: a field gone is dropped, one renamed renamed, one
/// of a wider type widened, a new one (optional, at the end) added. What Pondra's tables can't
/// take (a new required column, a key changed, columns reordered, a type narrowed) is refused by
/// name before anything is changed.
fn schema_sql(name: &str, meta: &Value, asked: &Value) -> Result<Vec<String>> {
    use anyhow::ensure;
    let updates = asked["updates"].as_array().context("a commit without its updates")?;
    let mut new = None;
    for u in updates {
        match u["action"].as_str().unwrap_or_default() {
            "add-schema" if new.is_none() => new = Some(&u["schema"]),
            "set-current-schema" => {}
            // (the name mapping, as the writer made it for the new schema: Pondra publishes its own)
            "set-properties" if u["updates"].as_object().is_some_and(|p| p.keys().all(|k| k == "schema.name-mapping.default")) => {}
            a => anyhow::bail!("{a} with a schema change: a commit changes the schema alone (a table's properties are Pondra's: ALTER TABLE … SET)"),
        }
    }
    let new = new.context("a schema change without its schema")?;
    let id = meta["current-schema-id"].clone();
    let old = meta["schemas"].as_array().into_iter().flatten().find(|s| s["schema-id"] == id).context("the table's current schema")?;
    let by_id = |s: &Value| s["fields"].as_array().into_iter().flatten().map(|f| (f["id"].as_i64().unwrap_or(-1), f.clone())).collect::<Vec<_>>();
    let (was, now) = (by_id(old), by_id(new));
    let keys = |s: &Value| s["identifier-field-ids"].as_array().cloned().unwrap_or_default();
    ensure!(keys(old) == keys(new), "a key changed: a Pondra table keeps its key");
    let kept: Vec<i64> = now.iter().map(|(i, _)| *i).filter(|i| was.iter().any(|(w, _)| w == i)).collect();
    let before: Vec<i64> = was.iter().map(|(i, _)| *i).filter(|i| kept.contains(i)).collect();
    let last_old = now.iter().rposition(|(i, _)| kept.contains(i)).map_or(0, |p| p + 1);
    ensure!(kept == before && now[last_old..].iter().all(|(i, _)| !kept.contains(i)), "columns reordered: a Pondra table's columns keep their order, and a new one goes last");
    let q = |n: &Value| format!("\"{}\"", n.as_str().unwrap_or_default().replace('"', "\"\""));
    let mut out = vec![];
    for (i, f) in &was {
        if !now.iter().any(|(n, _)| n == i) {
            out.push(format!("ALTER TABLE {name} DROP COLUMN {}", q(&f["name"])));
        }
    }
    for (i, f) in &now {
        match was.iter().find(|(w, _)| w == i).map(|(_, w)| w) {
            Some(w) => {
                ensure!(w["required"] == f["required"], "{}: required or optional, a column stays as it was", f["name"]);
                if w["name"] != f["name"] {
                    out.push(format!("ALTER TABLE {name} RENAME COLUMN {} TO {}", q(&w["name"]), q(&f["name"])));
                }
                if w["type"] != f["type"] {
                    out.push(format!("ALTER TABLE {name} ALTER COLUMN {} TYPE {}", q(&f["name"]), sql_type(&f["type"])?));
                }
            }
            None => {
                ensure!(!f["required"].as_bool().unwrap_or(false), "{}: a column added can't be required (NOT NULL): the rows already there have no value for it", f["name"]);
                out.push(format!("ALTER TABLE {name} ADD COLUMN {} {}", q(&f["name"]), sql_type(&f["type"])?));
            }
        }
    }
    Ok(out)
}

/// `DELETE /v1/namespaces/{ns}/tables/{table}`: `DROP TABLE`, its files with it (the catalog's
/// `purgeRequested` or not: a Pondra table's files are its own).
async fn drop_table(axum::extract::State(app): axum::extract::State<crate::server::App>, axum::Extension(role): axum::Extension<crate::auth::Role>, axum::extract::Path((ns, table)): axum::extract::Path<(String, String)>) -> Result<axum::http::StatusCode, Refusal> {
    let (parts, lake, schema) = space(&app, &ns).await?;
    loaded(&lake, &crate::ddl::join(&schema, &table)).await.map_err(|_| missing(&format!("table {}.{table}", parts.join(".")), "NoSuchTableException"))?;
    as_caller(&app, role, &format!("DROP TABLE {}", sql_name(&app, &parts, &lake, &schema, &table))).await?;
    Ok(axum::http::StatusCode::NO_CONTENT)
}

/// `POST /v1/tables/rename`: `ALTER TABLE … RENAME TO`, within its lake and schema.
async fn rename(axum::extract::State(app): axum::extract::State<crate::server::App>, axum::Extension(role): axum::Extension<crate::auth::Role>, body: bytes::Bytes) -> Result<axum::http::StatusCode, Refusal> {
    let asked: Value = serde_json::from_slice(&body).map_err(|e| bad(format!("the rename: {e}")))?;
    let ns = |v: &Value| v["namespace"].as_array().into_iter().flatten().filter_map(Value::as_str).collect::<Vec<_>>().join("\u{1f}");
    let (from, to) = (&asked["source"], &asked["destination"]);
    let (parts, lake, schema) = space(&app, &ns(from)).await?;
    let (name, new) = (from["name"].as_str().unwrap_or_default(), to["name"].as_str().unwrap_or_default());
    if ns(from) != ns(to) {
        return Err(bad(format!("{name} to another namespace: a Pondra table is renamed within its schema")));
    }
    loaded(&lake, &crate::ddl::join(&schema, name)).await.map_err(|_| missing(&format!("table {}.{name}", parts.join(".")), "NoSuchTableException"))?;
    let sql = format!("ALTER TABLE {} RENAME TO \"{}\"", sql_name(&app, &parts, &lake, &schema, name), new.replace('"', "\"\""));
    as_caller(&app, role, &sql).await?;
    Ok(axum::http::StatusCode::NO_CONTENT)
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
    incoming: Vec<DataFile>,     // the writer's data files (paths in the lake, rows, bytes)
    cleanup: Vec<String>,        // its manifest list and the manifests it added
    /// The files become the table's as written (`adopt.rs`), their footers read and checked.
    #[serde(default)]
    adopt: bool,
    /// The table's files it takes out (a copy-on-write change: ADR-029 §2), paths in the lake.
    #[serde(default)]
    removed: Vec<String>,
    /// Those rows as the table's own files, written by the node that took the commit (None: they
    /// go through the log, for the views and tasks that follow the table).
    written: Option<crate::write::Files>,
}

impl Commit {
    fn job(&self) -> String { format!("iceberg:{}:{}", self.table, self.snapshot) }
}

/// The leader's answer when the table changed since the writer read it: 409, so it retries.
pub const CONFLICT: &str = "the table changed since the writer read it";
/// …and when a commit's deletes are files of positions or values (merge-on-read), not yet taken.
const MERGE_ON_READ: &str = "delete files (merge-on-read): not yet; write the change copy-on-write (Spark: the table's write.delete.mode, write.update.mode and write.merge.mode = copy-on-write, Iceberg's default), or use Pondra's SQL (DELETE, UPDATE, MERGE)";
/// …and when it refused the commit before recording anything: 400.
const REFUSED: &str = "Pondra refused the commit";

type Refusal = (axum::http::StatusCode, axum::Json<Value>);

fn refused(code: u16, kind: &str, message: String) -> Refusal {
    (axum::http::StatusCode::from_u16(code).expect("a status"), axum::Json(json!({"error": {"message": message, "type": kind, "code": code}})))
}

fn bad(message: String) -> Refusal { refused(400, "BadRequestException", message) }

fn conflict(message: String) -> Refusal { refused(409, "CommitFailedException", message) }

/// `POST /v1/namespaces/{ns}/tables/{table}`: another engine's commit. An append's files become
/// the table's where the writer put them (ADR-029), their footers read here and checked; the
/// leader records them with their lineage. A table that views or tasks follow takes the rows
/// through the log, and one whose files the writer can't lay out as its own (renamed columns, a
/// partition spec: `adopt::fits_as_written`) has them rewritten here, as a bulk INSERT's (round
/// 25's copy). A copy-on-write change (`DELETE`, `UPDATE`, `MERGE`, an overwrite) takes files out
/// as it adds others, on a table neither followed nor renamed, and only when the writer read the
/// table as Pondra has it (`adopt::stale`: else 409, once its rows are in its files). The leader
/// checks what the commit asserts and records it, and the table's next version is published under
/// the writer's snapshot id. Anything else is refused by name.
async fn update(axum::extract::State(app): axum::extract::State<crate::server::App>, axum::Extension(role): axum::Extension<crate::auth::Role>, axum::extract::Path((ns, table)): axum::extract::Path<(String, String)>, body: bytes::Bytes) -> Reply {
    let (parts, lake, schema) = space(&app, &ns).await?;
    let name = crate::ddl::join(&schema, &table);
    let current = loaded(&lake, &name).await.map_err(|_| missing(&format!("table {}.{table}", ns.replace('\u{1f}', ".")), "NoSuchTableException"))?;
    let asked: Value = serde_json::from_slice(&body).map_err(|e| bad(format!("the commit: {e}")))?;
    let updates = asked["updates"].as_array().cloned().unwrap_or_default();
    if updates.iter().any(|u| u["action"] == "add-schema") && !updates.iter().any(|u| u["action"] == "add-snapshot") {
        requirements(&current["metadata"], &asked)?;
        let sql = schema_sql(&sql_name(&app, &parts, &lake, &schema, &table), &current["metadata"], &asked).map_err(|e| bad(format!("{e:#}")))?;
        for s in &sql {
            as_caller(&app, role, s).await?;
        }
        let version = current["metadata"]["last-sequence-number"].as_u64().unwrap_or(0);
        for _ in 0..200 {
            match loaded(&lake, &name).await {
                Ok(v) if sql.is_empty() || v["metadata"]["last-sequence-number"].as_u64().unwrap_or(0) > version => return Ok(axum::Json(v)),
                _ => tokio::time::sleep(std::time::Duration::from_millis(50)).await, // (published on the leader: this node sees it a moment later)
            }
        }
        return loaded(&lake, &name).await.map(axum::Json).map_err(|e| bad(format!("{e:#}")));
    }
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
    let followed = crate::write::follows(&lake, &name).await.map_err(|e| bad(format!("{e:#}")))?;
    if !c.removed.is_empty() && (followed || !crate::adopt::fits_as_written(&meta)) {
        return Err(bad(format!("{name}: {}, so other engines append to it; change its rows with Pondra's SQL (DELETE, UPDATE, MERGE)",
            if followed { "views or tasks follow it" } else { "it has a renamed or dropped column" })));
    }
    if (!c.incoming.is_empty() || !c.removed.is_empty()) && !followed && crate::adopt::fits_as_written(&meta) {
        crate::adopt::footers(&lake, &name, &meta, &mut c.incoming).await.map_err(|e| bad(format!("{REFUSED}: {e:#}")))?;
        c.adopt = true;
    } else if !c.incoming.is_empty() && !followed {
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

/// What a commit asks, if Pondra takes it: its snapshots on main (an append, a delete, an overwrite:
/// one after the other, the last made main), the Parquet files they add in the table's `data/`
/// folder, and the table's files they take out.
async fn parse(lake: &Lake, table: &str, meta: &Value, asked: &Value) -> Result<Commit, Refusal> {
    let (mut uuid, mut parent, mut main) = (None, None, None);
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
    let mut snapshots = vec![];
    for u in asked["updates"].as_array().into_iter().flatten() {
        match u["action"].as_str().unwrap_or_default() {
            "add-snapshot" => snapshots.push(u["snapshot"].clone()), // (an overwrite: a delete's and an append's, one after the other)
            "set-snapshot-ref" if u["ref-name"] == "main" && u["type"] == "branch" => main = u["snapshot-id"].as_i64(),
            "set-snapshot-ref" => return Err(bad(format!("branch or tag {}: Pondra's tables have main only", u["ref-name"]))),
            a => return Err(bad(format!("{a}: a Pondra table changes through its SQL (ALTER TABLE), and other engines append to it and change its rows"))),
        }
    }
    let s = snapshots.last().cloned().ok_or_else(|| bad("a commit with no snapshot: other engines append to Pondra's tables and change their rows".into()))?;
    let id = s["snapshot-id"].as_i64().ok_or_else(|| bad("a snapshot without its snapshot-id".into()))?;
    if main != Some(id) {
        return Err(bad("a snapshot not made main (staged): not taken".into()));
    }
    let home = crate::store::folder_of(lake, table).await.map_err(|e| bad(format!("{e:#}")))?; // (its files' folder: its name, unless it was renamed)
    let under = |uri: &Value, folder: &str| -> Result<String, Refusal> {
        let uri = uri.as_str().unwrap_or_default();
        let at = format!("data/{home}/{folder}{}", if folder.is_empty() { "" } else { "/" });
        inside(lake, uri).filter(|p| p.starts_with(&at)).ok_or_else(|| bad(format!("{uri}: not in {table}'s {} folder", if folder.is_empty() { "own" } else { folder })))
    };
    let get = |path: String| async move {
        let bytes = lake.store.get(&Path::from(path.as_str())).await.map_err(|e| bad(format!("{path}: {e}")))?.bytes().await.map_err(|e| bad(format!("{path}: {e}")))?;
        crate::avro::records(&bytes).map_err(|e| bad(format!("{path}: {e:#}")))
    };
    let (mut incoming, mut cleanup, mut removed) = (Vec::<DataFile>::new(), vec![], vec![]);
    for (i, s) in snapshots.iter().enumerate() {
        let id = s["snapshot-id"].as_i64().ok_or_else(|| bad("a snapshot without its snapshot-id".into()))?;
        if i > 0 && s["parent-snapshot-id"] != snapshots[i - 1]["snapshot-id"] {
            return Err(bad("snapshots that don't follow one another: not taken".into()));
        }
        match s["summary"]["operation"].as_str().unwrap_or("append") {
            "append" | "overwrite" | "delete" => {}
            "replace" => return Err(bad("another engine's compaction (a snapshot that does replace): Pondra merges its tables' files itself".into())),
            op => return Err(bad(format!("a snapshot that does {op}: not one Pondra takes (append, overwrite, delete)"))),
        }
        if !s["schema-id"].is_null() && s["schema-id"] != meta["current-schema-id"] {
            return Err(conflict(format!("the snapshot's schema: {CONFLICT}")));
        }
        let list = under(&s["manifest-list"], "metadata")?;
        cleanup.push(list.clone());
        for m in get(list).await? {
            if m["added_snapshot_id"].as_i64() != Some(id) {
                continue; // (the table's manifests, as they were)
            }
            if m["content"].as_i64().unwrap_or(0) != 0 {
                return Err(bad(MERGE_ON_READ.into()));
            }
            let path = under(&m["manifest_path"], "metadata")?;
            cleanup.push(path.clone());
            for e in get(path).await? {
                let d = &e["data_file"];
                if d["content"].as_i64().unwrap_or(0) != 0 {
                    return Err(bad(MERGE_ON_READ.into()));
                }
                match e["status"].as_i64() {
                    Some(1) => {}
                    Some(0) => continue, // (a file already in the table, in a manifest the writer merged)
                    Some(2) => {
                        let path = under(&d["file_path"], "")?; // (the table's file, Pondra's or another engine's, taken out)
                        match incoming.iter().position(|f| f.path == path) {
                            Some(at) => drop(incoming.remove(at)), // (added by an earlier snapshot of this commit)
                            None => removed.push(path),
                        }
                        continue;
                    }
                    s => return Err(bad(format!("a manifest entry of status {s:?}"))),
                }
                if !d["file_format"].as_str().unwrap_or_default().eq_ignore_ascii_case("parquet") {
                    return Err(bad(format!("{}: Pondra takes Parquet data files", d["file_path"])));
                }
                let (rows, bytes) = (d["record_count"].as_u64().unwrap_or(0), d["file_size_in_bytes"].as_u64().unwrap_or(0));
                incoming.push(DataFile { path: under(&d["file_path"], "data")?, rows, bytes, ..Default::default() });
            }
        }
    }
    let summary = s["summary"].as_object().cloned().unwrap_or_default();
    Ok(Commit { table: table.into(), snapshot: id, summary, uuid, parent, incoming, cleanup, written: None, adopt: false, removed })
}

/// A writer's URI as a path in the lake, if it is in it (`file:` or none, for a lake on disk).
fn inside(lake: &Lake, uri: &str) -> Option<String> {
    let plain = |u: &str| u.strip_prefix("file://").or_else(|| u.strip_prefix("file:")).unwrap_or(u).replacen("s3a://", "s3://", 1);
    let rest = plain(uri).strip_prefix(&format!("{}/", plain(&lake.url).trim_end_matches('/')))?.to_string();
    (!rest.split('/').any(|p| p == ".." || p == "." || p.is_empty())).then_some(rest)
}

/// A schema change's requirements, checked against the table as published: its uuid, its schema
/// and its main snapshot (409 when it changed since the writer read it).
fn requirements(meta: &Value, asked: &Value) -> Result<(), Refusal> {
    for r in asked["requirements"].as_array().into_iter().flatten() {
        let ok = match r["type"].as_str().unwrap_or_default() {
            "assert-table-uuid" => r["uuid"] == meta["table-uuid"],
            "assert-current-schema-id" => r["current-schema-id"] == meta["current-schema-id"],
            "assert-last-assigned-field-id" => r["last-assigned-field-id"] == meta["last-column-id"],
            "assert-ref-snapshot-id" => r["snapshot-id"] == meta["current-snapshot-id"],
            k => return Err(bad(format!("requirement {k}: not one Pondra checks with a schema change"))),
        };
        if !ok {
            return Err(conflict(format!("{}: {CONFLICT}", r["type"])));
        }
    }
    Ok(())
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
async fn incoming(lake: &Lake, meta: &TableMeta, files: &[DataFile]) -> Result<(datafusion::prelude::SessionContext, String)> {
    let meta = meta.logical();
    let schema = crate::query::schema(&meta.columns)?;
    let ctx = lake.session();
    let urls: Vec<String> = files.iter().map(|f| lake.full(&f.path)).collect();
    let df = ctx.read_parquet(urls, datafusion::prelude::ParquetReadOptions::default().schema(&schema)).await?;
    ctx.register_table("__incoming", df.into_view())?;
    let columns: Vec<String> = meta.columns.iter().map(|(c, _)| format!("\"{}\"", c.replace('"', "\"\""))).collect();
    Ok((ctx, format!("SELECT {} FROM __incoming", columns.join(", "))))
}

/// Leader, under the lake's lock: record another engine's append, once, if the table still
/// stands as the writer read it; then publish its next version under the writer's snapshot id,
/// and delete the writer's own files. Answers the table as the catalog does.
pub async fn record(lake: &Lake, seq: &crate::log::Sequencer, c: Commit, nodes: &[String], me: &str, retain_ms: u64) -> Result<Value> {
    if !done(lake, &c.job()).await? {
        check(lake, &c).await?;
        if !c.removed.is_empty() && crate::adopt::stale(lake, &c.table).await? {
            up_to_date(lake, &c.table, nodes, me, retain_ms).await?;
            anyhow::bail!("{CONFLICT}: it had rows the writer couldn't read yet (in the log, or changed), now in its files: read it again");
        }
        let files: Vec<String> = c.written.iter().flat_map(|f| f.paths()).collect();
        let adopted = c.adopt && crate::adopt::record(lake, seq, &c.table, &c.job(), &c.incoming, &c.removed).await?;
        let into_files = adopted || match c.written {
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
        if !adopted {
            futures::future::join_all(c.incoming.iter().map(|f| lake.delete(&f.path))).await; // (its rows are the table's own files, or the log's, now)
        }
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

/// The table's log rows into its files, its changed rows out of them, and its next version
/// published: what another engine reads is then the table as Pondra has it (ADR-029 §3).
async fn up_to_date(lake: &Lake, table: &str, nodes: &[String], me: &str, retain_ms: u64) -> Result<()> {
    let upto = lake.visible();
    for t in [table.to_string(), crate::sys::deleted(table)] {
        while !crate::tier::tier_table(lake, &t, upto, nodes, me).await?.1 {}
    }
    crate::tier::purge(lake, table, nodes, me, retain_ms, true).await?;
    crate::delta::publish_all(lake).await
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
            lake.ids.refill(seq.block().await?);
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
