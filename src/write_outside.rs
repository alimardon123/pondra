//! INSERT into other engines' tables (ADR-026): into a table attached `(TYPE delta | iceberg)`,
//! the rows are written from the node that ran the statement as Parquet files in the table's
//! folder — its columns as the table names, types and numbers them, a file per partition — then
//! the table's next version is committed the way its format says: a Delta log entry, an Iceberg
//! snapshot (a new metadata file, or its REST catalog's commit). Both are put-if-absent: a writer
//! that loses the race reads what won and tries the next version. A statement retried with its
//! job id is applied once (Delta's `txn`, an Iceberg snapshot's `pondra.job`). What a table needs
//! of its writers that isn't done here is refused by name.
use crate::read_delta;
use crate::store::Lake;
use anyhow::{bail, ensure, Context, Result};
use datafusion::arrow::array::{Array, ArrayRef, AsArray, RecordBatch, UInt32Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::common::ScalarValue;
use datafusion::parquet::arrow::ArrowWriter;
use futures::StreamExt;
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

const FILE_BYTES: usize = 128 << 20; // (a file this big is closed, and another started)

/// INSERT INTO `table` (a table of files: `ext::attached_table`) the rows of `query`.
pub async fn insert(lake: &Lake, table: &str, query: &str, job: &str) -> Result<Value> {
    let spec = crate::ext::spec(table).context("not another engine's table")?;
    crate::ext::check(lake, &spec).await?; // (written by whoever may read it: a secret covering it, or the node's owner)
    let query = crate::routines::expand(lake, query).await?;
    let df = crate::query::session(lake, &query, "").await?.sql_with_options(&query, crate::query::read_only()).await?;
    match spec.format.as_str() {
        "delta" => delta(lake, spec.urls[0].trim_end_matches('/'), df, job).await,
        "iceberg" => crate::write_outside::iceberg(lake, &spec, df, job).await,
        f => bail!("INSERT into {f} files: into a table attached as delta or iceberg"),
    }
}

// ---------------------------------------------------------------- Delta

/// Writer features handled here (Delta's protocol, writer version 7).
const DELTA_WRITES: &[&str] = &["appendOnly", "invariants", "checkConstraints", "changeDataFeed", "columnMapping", "deletionVectors", "timestampNtz", "typeWidening",
    "typeWidening-preview", "v2Checkpoint", "vacuumProtocolCheck", "domainMetadata", "inCommitTimestamp", "clustering", "generatedColumns", "identityColumns"];

async fn delta(lake: &Lake, root: &str, df: datafusion::prelude::DataFrame, job: &str) -> Result<Value> {
    let app = format!("pondra:{job}");
    let log = read_delta::replay(lake, root, None).await?;
    if log.txns.contains(&app) {
        return Ok(json!({"duplicate": true}));
    }
    delta_writable(root, &log)?;
    let (columns, partitions, mapped) = read_delta::schema(root, &log.metadata)?;
    let fields = read_delta_fields(&log.metadata)?;
    let target: Vec<Field> = columns.iter().map(|(p, l, t)| {
        let f = Field::new(p, t.clone(), true);
        match fields.get(l).and_then(|m| m["delta.columnMapping.id"].as_i64()).filter(|_| mapped) {
            Some(id) => f.with_metadata([("PARQUET:field_id".to_string(), id.to_string())].into()),
            None => f,
        }
    }).collect();
    let df = conform(df, &target, root)?;
    let partition_at: Vec<usize> = partitions.iter().map(|p| target.iter().position(|f| f.name() == p).expect("a partition column")).collect();
    let file_fields: Vec<Field> = target.iter().enumerate().filter(|(i, _)| !partition_at.contains(i)).map(|(_, f)| f.clone()).collect();
    let parts = |b: &RecordBatch| -> Result<Vec<Part>> {
        let text: Vec<ArrayRef> = partition_at.iter().map(|&i| partition_text(b.column(i))).collect::<Result<_>>()?;
        let mut groups: BTreeMap<Vec<Option<String>>, Vec<u32>> = BTreeMap::new();
        for r in 0..b.num_rows() {
            let key = text.iter().map(|a| (!a.is_null(r)).then(|| a.as_string::<i32>().value(r).to_string())).collect();
            groups.entry(key).or_default().push(r as u32);
        }
        Ok(groups.into_iter().map(|(values, rows)| {
            let folder = partitions.iter().zip(&values).map(|(p, v)| format!("{p}={}/", v.as_deref().map_or(crate::ext::NULL_FOLDER.to_string(), hive_escape))).collect::<String>();
            Part { folder, rows: UInt32Array::from(rows), values: json!(partitions.iter().cloned().zip(values).collect::<BTreeMap<_, _>>()) }
        }).collect())
    };
    let keep: Vec<usize> = (0..target.len()).filter(|i| !partition_at.contains(i)).collect();
    let written = write_files(lake, root, "", df, Arc::new(Schema::new(file_fields)), &keep, &parts).await?;
    let rows: u64 = written.iter().map(|w| w.rows).sum();
    let now = crate::log::now_ms();
    let adds: Vec<Value> = written.iter().map(|w| json!({"add": {
        "path": uri_escape(&w.path), "partitionValues": w.values, "size": w.bytes, "modificationTime": now, "dataChange": true, "stats": delta_stats(w).to_string(),
    }})).collect();
    let mut version = log.version + 1;
    let mut last = log;
    loop {
        let mut info = json!({"timestamp": now, "operation": "WRITE", "operationParameters": {"mode": "Append", "partitionBy": serde_json::to_string(&partitions)?},
                              "isBlindAppend": true, "engineInfo": format!("Pondra/{}", env!("CARGO_PKG_VERSION")), "txnId": uuid::Uuid::new_v4().to_string()});
        if last.metadata["configuration"]["delta.enableInCommitTimestamps"].as_str() == Some("true") {
            info["inCommitTimestamp"] = json!(now.max(previous_ict(lake, root, version - 1).await? + 1)); // (its commits' times only grow)
        }
        let actions = std::iter::once(json!({"commitInfo": info})).chain([json!({"txn": {"appId": app, "version": 0, "lastUpdated": now}})]).chain(adds.iter().cloned());
        let body = actions.map(|a| a.to_string()).collect::<Vec<_>>().join("\n") + "\n";
        match put_new(lake, &format!("{root}/_delta_log/{version:020}.json"), body.into_bytes()).await? {
            true => return Ok(json!({"rows": rows, "version": version})),
            false => {
                // Another writer took this version: after it, unless it changed what we wrote for.
                let newer = read_delta::replay(lake, root, None).await?;
                if newer.txns.contains(&app) {
                    return Ok(json!({"duplicate": true}));
                }
                ensure!(newer.metadata["schemaString"] == last.metadata["schemaString"] && newer.metadata["partitionColumns"] == last.metadata["partitionColumns"] && newer.protocol == last.protocol,
                        "{root}: the table's schema or protocol changed while this INSERT wrote: run it again");
                version = newer.version + 1;
                last = newer;
            }
        }
    }
}

/// Refuse a table whose writers must do what isn't done here.
fn delta_writable(root: &str, log: &read_delta::Log) -> Result<()> {
    let p = &log.protocol;
    let version = p["minWriterVersion"].as_i64().unwrap_or(2);
    ensure!(version <= 7, "{root}: a Delta table of writer version {version}, which Pondra doesn't write yet (up to 7)");
    for f in p["writerFeatures"].as_array().into_iter().flatten().filter_map(Value::as_str) {
        ensure!(DELTA_WRITES.contains(&f), "{root}: a Delta table with the writer feature {f}, which Pondra doesn't write yet (it writes {})", DELTA_WRITES.join(", "));
    }
    let fields = read_delta_fields(&log.metadata)?;
    for (name, meta) in &fields {
        for (k, what) in [("delta.invariants", "invariants"), ("delta.generationExpression", "generated columns"), ("delta.identity.start", "identity columns")] {
            ensure!(meta.get(k).is_none(), "{root}: column {name} has {what}, which Pondra doesn't compute or check yet: INSERT with Spark or delta-rs");
        }
    }
    let config = log.metadata["configuration"].as_object().cloned().unwrap_or_default();
    if let Some(k) = config.keys().find(|k| k.starts_with("delta.constraints.")) {
        bail!("{root}: the table has a CHECK constraint ({}), which Pondra doesn't check yet", &k["delta.constraints.".len()..]);
    }
    ensure!(config.get("delta.appendOnly").is_none_or(|v| v == "true" || v == "false"), "{root}: delta.appendOnly");
    Ok(())
}

/// A Delta schema's top-level fields' metadata, by logical name.
fn read_delta_fields(metadata: &Value) -> Result<HashMap<String, Value>> {
    let s: Value = serde_json::from_str(metadata["schemaString"].as_str().context("no schema")?)?;
    Ok(s["fields"].as_array().into_iter().flatten().filter_map(|f| Some((f["name"].as_str()?.to_string(), f["metadata"].clone()))).collect())
}

/// The last commit's in-commit timestamp (Delta's `inCommitTimestamp`), or 0.
async fn previous_ict(lake: &Lake, root: &str, version: i64) -> Result<u64> {
    if version < 0 {
        return Ok(0);
    }
    let body = crate::ext::get(lake, &format!("{root}/_delta_log/{version:020}.json")).await?;
    let first: Value = serde_json::from_slice(body.split(|b| *b == b'\n').next().unwrap_or_default()).unwrap_or_default();
    Ok(first["commitInfo"]["inCommitTimestamp"].as_u64().unwrap_or(0))
}

/// A file's statistics as Delta's readers skip files by them.
fn delta_stats(w: &Written) -> Value {
    let (mut lo, mut hi, mut nulls) = (serde_json::Map::new(), serde_json::Map::new(), serde_json::Map::new());
    for (c, (min, max)) in &w.ranges {
        if let (Some(a), Some(b)) = (stat_json(min, false), stat_json(max, true)) {
            lo.insert(c.clone(), a);
            hi.insert(c.clone(), b);
        }
    }
    for (c, n) in &w.nulls {
        nulls.insert(c.clone(), json!(n));
    }
    json!({"numRecords": w.rows, "minValues": lo, "maxValues": hi, "nullCount": nulls})
}

/// A bound as Delta's statistics hold it: numbers as numbers, dates as text, timestamps to the
/// millisecond (a max rounded up), text as is. None: not kept (floats: NaN).
fn stat_json(v: &ScalarValue, max: bool) -> Option<Value> {
    Some(match v {
        ScalarValue::Int8(Some(x)) => json!(x),
        ScalarValue::Int16(Some(x)) => json!(x),
        ScalarValue::Int32(Some(x)) => json!(x),
        ScalarValue::Int64(Some(x)) => json!(x),
        ScalarValue::Utf8(Some(s)) | ScalarValue::Utf8View(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => json!(s),
        ScalarValue::Date32(Some(_)) | ScalarValue::Decimal128(Some(_), ..) => serde_json::from_str::<Value>(&crate::manifest::text(v)?).unwrap_or_else(|_| json!(crate::manifest::text(v))),
        ScalarValue::TimestampMicrosecond(Some(us), tz) => {
            let ms = if max { us.div_euclid(1000) + (us.rem_euclid(1000) > 0) as i64 } else { us.div_euclid(1000) };
            let t = chrono::DateTime::from_timestamp_millis(ms)?;
            json!(if tz.is_some() { t.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string() } else { t.format("%Y-%m-%dT%H:%M:%S%.3f").to_string() })
        }
        _ => return None,
    })
}

/// A partition column's values as text, as Delta's log and Hive's folders spell them.
fn partition_text(a: &ArrayRef) -> Result<ArrayRef> {
    let text = datafusion::arrow::compute::cast(a, &DataType::Utf8)?;
    Ok(match a.data_type() {
        DataType::Timestamp(..) => Arc::new(text.as_string::<i32>().iter().map(|v| v.map(|s| s.replacen('T', " ", 1))).collect::<datafusion::arrow::array::StringArray>()),
        _ => text,
    })
}

/// A value as a Hive folder names it (Spark's escaping, spaces too).
fn hive_escape(v: &str) -> String {
    v.chars().map(|c| match c {
        '"' | '#' | '%' | '\'' | '*' | '/' | ':' | '=' | '?' | '\\' | '\x7f' | '{' | '[' | ']' | '^' | ' ' => format!("%{:02X}", c as u32),
        c if (c as u32) < 0x20 => format!("%{:02X}", c as u32),
        c => c.to_string(),
    }).collect()
}

/// A relative path as Delta's log holds it (a URI's path: `%` and the rest escaped).
fn uri_escape(p: &str) -> String {
    p.bytes().map(|b| match b {
        b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' | b'/' | b'=' => (b as char).to_string(),
        b => format!("%{b:02X}"),
    }).collect()
}

// ---------------------------------------------------------------- the files

/// A partition's rows of a batch: its folder, the rows, its values (as the format records them).
pub struct Part {
    pub folder: String,
    pub rows: UInt32Array,
    pub values: Value,
}

/// A file written.
pub struct Written {
    pub path: String, // relative to the table's folder
    pub bytes: u64,
    pub rows: u64,
    pub values: Value,
    pub ranges: Vec<(String, (ScalarValue, ScalarValue))>, // columns' min and max (by the names in the file)
    pub nulls: Vec<(String, u64)>,
}

/// The rows as Parquet files under `root` (in `dir`, then each partition's folder): the columns
/// `keep` of each batch, as `schema`; files closed at `FILE_BYTES`.
pub async fn write_files(lake: &Lake, root: &str, dir: &str, df: datafusion::prelude::DataFrame, schema: SchemaRef, keep: &[usize], parts: &(dyn Fn(&RecordBatch) -> Result<Vec<Part>> + Send + Sync)) -> Result<Vec<Written>> {
    use datafusion::parquet::basic::Compression;
    use datafusion::parquet::file::properties::WriterProperties;
    let props = || WriterProperties::builder().set_compression(Compression::SNAPPY).build(); // (read by every engine)
    let mut open: HashMap<String, (ArrowWriter<Vec<u8>>, u64, Value)> = HashMap::new();
    let mut out = vec![];
    let mut stream = df.execute_stream().await?;
    while let Some(b) = stream.next().await.transpose()? {
        for p in parts(&b)? {
            let cols = keep.iter().map(|&i| Ok(datafusion::arrow::compute::take(b.column(i), &p.rows, None)?)).collect::<Result<Vec<_>>>()?;
            let piece = RecordBatch::try_new(schema.clone(), cols)?;
            let w = match open.entry(p.folder.clone()) {
                std::collections::hash_map::Entry::Occupied(e) => e.into_mut(),
                std::collections::hash_map::Entry::Vacant(e) => e.insert((ArrowWriter::try_new(vec![], schema.clone(), Some(props()))?, 0, p.values)),
            };
            w.0.write(&piece)?;
            w.1 += piece.num_rows() as u64;
            if w.0.bytes_written() + w.0.in_progress_size() > FILE_BYTES {
                let (writer, rows, values) = open.remove(&p.folder).expect("open");
                out.push(finish(lake, root, &format!("{dir}{}", p.folder), writer, rows, values, &schema).await?);
            }
        }
    }
    for (folder, (writer, rows, values)) in open {
        out.push(finish(lake, root, &format!("{dir}{folder}"), writer, rows, values, &schema).await?);
    }
    Ok(out)
}

async fn finish(lake: &Lake, root: &str, folder: &str, w: ArrowWriter<Vec<u8>>, rows: u64, values: Value, schema: &SchemaRef) -> Result<Written> {
    use datafusion::parquet::arrow::arrow_reader::statistics::StatisticsConverter;
    let mut w = w;
    let footer = w.finish()?;
    let buf = std::mem::take(w.inner_mut()); // (finished: its buffer is the whole file)
    let md = datafusion::parquet::file::metadata::ParquetMetaData::new(footer.file_metadata().clone(), footer.row_groups().to_vec());
    let mut ranges = vec![];
    let mut nulls = vec![];
    for f in schema.fields() {
        if let Some(r) = crate::manifest::from_footer(&md, schema, f.name()).filter(|_| !f.data_type().is_floating()) {
            ranges.push((f.name().clone(), r));
        }
        if let Ok(c) = StatisticsConverter::try_new(f.name(), schema, md.file_metadata().schema_descr()) {
            if let Ok(n) = c.row_group_null_counts(md.row_groups().iter()) {
                nulls.push((f.name().clone(), n.iter().flatten().sum::<u64>()));
            }
        }
    }
    let path = format!("{folder}pondra-{}.parquet", uuid::Uuid::new_v4());
    let bytes = buf.len() as u64;
    ensure!(put_new(lake, &format!("{root}/{path}"), buf).await?, "{root}/{path} is there already");
    Ok(Written { path, bytes, rows, values, ranges, nulls })
}

/// The query's rows as the table's columns (by position, cast to its types, under its names).
fn conform(df: datafusion::prelude::DataFrame, target: &[Field], table: &str) -> Result<datafusion::prelude::DataFrame> {
    use datafusion::prelude::{cast, col};
    let have = df.schema().fields().len();
    ensure!(have == target.len(), "INSERT into {table}: the query gives {have} columns, the table has {} ({})", target.len(), target.iter().map(|f| f.name().as_str()).collect::<Vec<_>>().join(", "));
    let exprs = df.schema().columns().into_iter().zip(target).map(|(c, f)| cast(col(c), f.data_type().clone()).alias(f.name())).collect::<Vec<_>>();
    Ok(df.select(exprs)?)
}

/// Write an object only if nothing is there: false if something is (another writer won).
pub async fn put_new(lake: &Lake, url: &str, body: Vec<u8>) -> Result<bool> {
    let (store, path) = crate::ext::store(lake, url).await?;
    let opts = object_store_df::PutOptions { mode: object_store_df::PutMode::Create, ..Default::default() };
    match store.put_opts(&path, body.into(), opts).await {
        Ok(_) => Ok(true),
        Err(object_store_df::Error::AlreadyExists { .. } | object_store_df::Error::Precondition { .. }) => Ok(false),
        Err(e) => Err(e).with_context(|| format!("writing {url}")),
    }
}

// ---------------------------------------------------------------- Iceberg

/// INSERT into an Iceberg table (format v2): data files, a manifest of them, a manifest list of
/// the snapshot's manifests and it, and the next metadata — a new file beside the last (put if
/// absent), or the REST catalog's commit (the current snapshot asserted).
pub async fn iceberg(lake: &Lake, spec: &crate::ext::Spec, df: datafusion::prelude::DataFrame, job: &str) -> Result<Value> {
    let url = spec.urls[0].trim_end_matches('/');
    let rest = spec.options.contains_key("namespace");
    let (mut at, mut meta) = crate::read_iceberg::current(lake, url, &spec.options).await?;
    if done(&meta, job) {
        return Ok(json!({"duplicate": true}));
    }
    let format = meta["format-version"].as_i64().unwrap_or(1);
    ensure!(format == 2, "{url}: Iceberg format v{format} tables aren't written yet (v2 are)");
    let location = meta["location"].as_str().context("a table without its location")?.trim_end_matches('/').to_string();
    let (schema_json, columns) = crate::read_iceberg::schema_of(&meta)?;
    let target: Vec<Field> = columns.iter().map(|(n, id, t)| Field::new(n, t.clone(), true).with_metadata([("PARQUET:field_id".to_string(), id.to_string())].into())).collect();
    let df = conform(df, &target, url)?;
    let spec_id = meta["default-spec-id"].as_i64().unwrap_or(0);
    let pspec = meta["partition-specs"].as_array().into_iter().flatten().find(|s| s["spec-id"].as_i64() == Some(spec_id)).cloned().unwrap_or(json!({"fields": []}));
    let fields: Vec<(String, usize, String, DataType)> = pspec["fields"].as_array().into_iter().flatten().map(|f| {
        let src = f["source-id"].as_i64().context("a partition field without its source")?;
        let at = columns.iter().position(|(_, id, _)| *id == src).with_context(|| format!("{url}: a partition on field {src}, not in the schema"))?;
        Ok((f["name"].as_str().unwrap_or_default().to_string(), at, f["transform"].as_str().unwrap_or("identity").to_string(), columns[at].2.clone()))
    }).collect::<Result<_>>()?;
    let partition_avro: Vec<Value> = pspec["fields"].as_array().into_iter().flatten().zip(&fields)
        .map(|(f, (name, _, t, dt))| Ok(crate::iceberg::opt(name, f["field-id"].as_u64().unwrap_or(1000) as u32, avro_type(t, dt)?))).collect::<Result<_>>()?;
    let parts = |b: &RecordBatch| -> Result<Vec<Part>> {
        let values: Vec<Vec<Value>> = fields.iter().map(|(_, i, t, _)| transform(b.column(*i), t)).collect::<Result<_>>()?;
        let mut groups: BTreeMap<String, (Vec<u32>, Value)> = BTreeMap::new();
        for r in 0..b.num_rows() {
            let tuple: Vec<&Value> = values.iter().map(|v| &v[r]).collect();
            let folder = fields.iter().zip(&tuple).map(|((n, ..), v)| format!("{n}={}/", match v { Value::Null => "null".into(), Value::String(s) => hive_escape(s), v => hive_escape(&v.to_string()) })).collect::<String>();
            let record = json!(fields.iter().zip(&tuple).map(|((n, ..), v)| (n.clone(), (*v).clone())).collect::<serde_json::Map<_, _>>());
            groups.entry(folder).or_insert_with(|| (vec![], record)).0.push(r as u32);
        }
        Ok(groups.into_iter().map(|(folder, (rows, values))| Part { folder, rows: UInt32Array::from(rows), values }).collect())
    };
    let keep: Vec<usize> = (0..target.len()).collect();
    let written = write_files(lake, &location, "data/", df, Arc::new(Schema::new(target.clone())), &keep, &parts).await?;
    let rows: u64 = written.iter().map(|w| w.rows).sum();
    let ids: HashMap<&str, i64> = columns.iter().map(|(n, id, _)| (n.as_str(), *id)).collect();
    loop {
        let snapshot_id = (uuid::Uuid::new_v4().as_u64_pair().0 >> 1) as i64;
        let seq = meta["last-sequence-number"].as_i64().unwrap_or(0) + 1;
        let now = crate::log::now_ms() as i64;
        // The manifest of the new files.
        let entries: Vec<Value> = written.iter().map(|w| {
            let bounds = |max: bool| -> Value {
                let pairs: Vec<Value> = w.ranges.iter().filter_map(|(c, (lo, hi))| Some(json!({"key": ids.get(c.as_str())?, "value": bound_bytes(if max { hi } else { lo })?}))).collect();
                if pairs.is_empty() { Value::Null } else { json!(pairs) }
            };
            json!({"status": 1, "snapshot_id": snapshot_id, "sequence_number": null, "file_sequence_number": null, "data_file": {
                "content": 0, "file_path": format!("{location}/{}", w.path), "file_format": "PARQUET", "partition": w.values,
                "record_count": w.rows, "file_size_in_bytes": w.bytes,
                "null_value_counts": w.nulls.iter().filter_map(|(c, n)| Some(json!({"key": ids.get(c.as_str())?, "value": n}))).collect::<Vec<_>>(),
                "lower_bounds": bounds(false), "upper_bounds": bounds(true),
            }})
        }).collect();
        let avro_meta = [("schema", schema_json.to_string()), ("schema-id", meta["current-schema-id"].to_string()), ("partition-spec", pspec["fields"].to_string()),
                         ("partition-spec-id", spec_id.to_string()), ("format-version", "2".into()), ("content", "data".into())];
        let manifest = crate::avro::write(&crate::iceberg::entry_schema_with(partition_avro.clone()), &avro_meta, &entries)?;
        let manifest_path = format!("{location}/metadata/{}-m0.avro", uuid::Uuid::new_v4());
        let manifest_len = manifest.len();
        ensure!(put_new(lake, &manifest_path, manifest).await?, "{manifest_path} is there already");
        // The manifest list: the current snapshot's manifests, and the new one.
        let current = meta["current-snapshot-id"].as_i64().filter(|id| *id >= 0);
        let snapshot = |id: i64| meta["snapshots"].as_array().into_iter().flatten().find(|s| s["snapshot-id"].as_i64() == Some(id)).cloned();
        let mut list = match current.and_then(snapshot) {
            Some(s) => crate::avro::records(&crate::ext::get(lake, s["manifest-list"].as_str().context("a snapshot without its manifest list")?).await?)?,
            None => vec![],
        };
        list.push(json!({"manifest_path": manifest_path, "manifest_length": manifest_len, "partition_spec_id": spec_id, "content": 0, "sequence_number": seq, "min_sequence_number": seq,
                         "added_snapshot_id": snapshot_id, "added_files_count": written.len(), "existing_files_count": 0, "deleted_files_count": 0,
                         "added_rows_count": rows, "existing_rows_count": 0, "deleted_rows_count": 0, "partitions": null, "key_metadata": null}));
        let list_schema: Value = serde_json::from_str(&crate::iceberg::list_schema())?;
        let list_meta = [("snapshot-id", snapshot_id.to_string()), ("parent-snapshot-id", current.map_or("null".into(), |c| c.to_string())), ("sequence-number", seq.to_string()), ("format-version", "2".into())];
        let list_body = crate::avro::write(&list_schema, &list_meta, &list)?;
        let list_path = format!("{location}/metadata/snap-{snapshot_id}-1-{}.avro", uuid::Uuid::new_v4());
        ensure!(put_new(lake, &list_path, list_body).await?, "{list_path} is there already");
        let total = |k: &str, add: u64| -> String { (current.and_then(snapshot).and_then(|s| s["summary"][k].as_str().and_then(|v| v.parse::<u64>().ok())).unwrap_or(0) + add).to_string() };
        let snap = json!({"snapshot-id": snapshot_id, "parent-snapshot-id": current, "sequence-number": seq, "timestamp-ms": now, "manifest-list": list_path,
                          "schema-id": meta["current-schema-id"], "summary": {"operation": "append", "added-data-files": written.len().to_string(), "added-records": rows.to_string(),
                          "added-files-size": written.iter().map(|w| w.bytes).sum::<u64>().to_string(), "total-records": total("total-records", rows),
                          "total-data-files": total("total-data-files", written.len() as u64), "pondra.job": job}});
        let committed = match rest {
            true => crate::read_iceberg::rest_commit(lake, url, &spec.options, current, &snap).await?,
            false => {
                let mut next = meta.clone();
                next["last-sequence-number"] = json!(seq);
                next["last-updated-ms"] = json!(now);
                next["current-snapshot-id"] = json!(snapshot_id);
                push(&mut next, "snapshots", snap.clone());
                push(&mut next, "snapshot-log", json!({"snapshot-id": snapshot_id, "timestamp-ms": now}));
                push(&mut next, "metadata-log", json!({"metadata-file": at, "timestamp-ms": meta["last-updated-ms"]}));
                next["refs"]["main"] = json!({"snapshot-id": snapshot_id, "type": "branch"});
                crate::read_iceberg::commit_file(lake, url, &at, &next).await?
            }
        };
        if committed {
            return Ok(json!({"rows": rows, "snapshot": snapshot_id}));
        }
        // Another writer committed first: on top of theirs (our files stay; the lists are new).
        (at, meta) = crate::read_iceberg::current(lake, url, &spec.options).await?;
        if done(&meta, job) {
            return Ok(json!({"duplicate": true}));
        }
    }
}

/// Has this job's snapshot been committed?
fn done(meta: &Value, job: &str) -> bool { meta["snapshots"].as_array().into_iter().flatten().any(|s| s["summary"]["pondra.job"].as_str() == Some(job)) }

fn push(v: &mut Value, key: &str, item: Value) {
    match v[key].as_array_mut() {
        Some(a) => a.push(item),
        None => v[key] = json!([item]),
    }
}

/// A partition field's Avro type: what its transform gives.
fn avro_type(transform: &str, source: &DataType) -> Result<Value> {
    Ok(match transform {
        "year" | "month" | "day" | "hour" => json!("int"),
        t if t.starts_with("bucket") => json!("int"),
        "identity" | "void" => primitive(source)?,
        t if t.starts_with("truncate") => primitive(source)?,
        t => bail!("partitions by {t} aren't written yet"),
    })
}

fn primitive(t: &DataType) -> Result<Value> {
    Ok(match t {
        DataType::Int32 | DataType::Date32 => json!("int"),
        DataType::Int64 => json!("long"),
        DataType::Timestamp(TimeUnit::Microsecond, tz) => json!({"type": "long", "logicalType": if tz.is_some() { "timestamp-micros" } else { "local-timestamp-micros" }}),
        DataType::Utf8 => json!("string"),
        DataType::Boolean => json!("boolean"),
        t => bail!("partitions of {t} columns aren't written yet"),
    })
}

/// A partition transform of a column, row by row, as JSON (Iceberg's partition values).
fn transform(a: &ArrayRef, t: &str) -> Result<Vec<Value>> {
    use datafusion::arrow::datatypes::{Date32Type, Int32Type, Int64Type, TimestampMicrosecondType};
    let n = a.len();
    let micros = |i: usize| -> Option<i64> {
        match a.data_type() {
            DataType::Date32 => Some(a.as_primitive::<Date32Type>().value(i) as i64 * 86_400_000_000),
            DataType::Timestamp(TimeUnit::Microsecond, _) => Some(a.as_primitive::<TimestampMicrosecondType>().value(i)),
            _ => None,
        }
    };
    let each = |f: &dyn Fn(usize) -> Result<Value>| (0..n).map(|i| if a.is_null(i) { Ok(Value::Null) } else { f(i) }).collect::<Result<Vec<_>>>();
    let date = |us: i64| chrono::DateTime::from_timestamp_micros(us).map(|d| d.naive_utc());
    match t {
        "identity" => each(&|i| Ok(match a.data_type() {
            DataType::Int32 => json!(a.as_primitive::<Int32Type>().value(i)),
            DataType::Int64 => json!(a.as_primitive::<Int64Type>().value(i)),
            DataType::Date32 => json!(a.as_primitive::<Date32Type>().value(i)),
            DataType::Timestamp(TimeUnit::Microsecond, _) => json!(a.as_primitive::<TimestampMicrosecondType>().value(i)),
            DataType::Utf8 => json!(a.as_string::<i32>().value(i)),
            DataType::Boolean => json!(a.as_boolean().value(i)),
            other => bail!("identity partitions of {other} columns aren't written yet"),
        })),
        "void" => Ok(vec![Value::Null; n]),
        "year" | "month" | "day" | "hour" => each(&|i| {
            let us = micros(i).with_context(|| format!("a {t} partition of a {} column", a.data_type()))?;
            let d = date(us).context("a time out of range")?;
            use chrono::Datelike;
            Ok(json!(match t {
                "year" => d.year() as i64 - 1970,
                "month" => (d.year() as i64 - 1970) * 12 + d.month0() as i64,
                "day" => us.div_euclid(86_400_000_000),
                _ => us.div_euclid(3_600_000_000),
            }))
        }),
        t if t.starts_with("bucket[") => {
            let buckets: i64 = t[7..t.len() - 1].parse()?;
            each(&|i| {
                let bytes = match a.data_type() {
                    DataType::Int32 => (a.as_primitive::<Int32Type>().value(i) as i64).to_le_bytes().to_vec(),
                    DataType::Int64 => a.as_primitive::<Int64Type>().value(i).to_le_bytes().to_vec(),
                    DataType::Date32 => (a.as_primitive::<Date32Type>().value(i) as i64).to_le_bytes().to_vec(),
                    DataType::Timestamp(TimeUnit::Microsecond, _) => a.as_primitive::<TimestampMicrosecondType>().value(i).to_le_bytes().to_vec(),
                    DataType::Utf8 => a.as_string::<i32>().value(i).as_bytes().to_vec(),
                    other => bail!("bucket partitions of {other} columns aren't written yet"),
                };
                Ok(json!(((murmur3(&bytes) & i32::MAX as u32) as i64) % buckets))
            })
        }
        t if t.starts_with("truncate[") => {
            let w: i64 = t[9..t.len() - 1].parse()?;
            each(&|i| Ok(match a.data_type() {
                DataType::Int32 => { let v = a.as_primitive::<Int32Type>().value(i) as i64; json!(v - v.rem_euclid(w)) }
                DataType::Int64 => { let v = a.as_primitive::<Int64Type>().value(i); json!(v - v.rem_euclid(w)) }
                DataType::Utf8 => json!(a.as_string::<i32>().value(i).chars().take(w as usize).collect::<String>()),
                other => bail!("truncate partitions of {other} columns aren't written yet"),
            }))
        }
        t => bail!("partitions by {t} aren't written yet"),
    }
}

/// MurmurHash3 (x86, 32-bit, seed 0): Iceberg's bucket hash.
fn murmur3(data: &[u8]) -> u32 {
    let (c1, c2) = (0xcc9e2d51u32, 0x1b873593u32);
    let mut h = 0u32;
    let chunks = data.chunks_exact(4);
    let tail = chunks.remainder();
    for c in chunks {
        let k = u32::from_le_bytes(c.try_into().expect("four bytes")).wrapping_mul(c1).rotate_left(15).wrapping_mul(c2);
        h = (h ^ k).rotate_left(13).wrapping_mul(5).wrapping_add(0xe6546b64);
    }
    if !tail.is_empty() {
        let mut k = 0u32;
        for (i, b) in tail.iter().enumerate() {
            k |= (*b as u32) << (8 * i);
        }
        h ^= k.wrapping_mul(c1).rotate_left(15).wrapping_mul(c2);
    }
    h ^= data.len() as u32;
    h ^= h >> 16;
    h = h.wrapping_mul(0x85ebca6b);
    h ^= h >> 13;
    h = h.wrapping_mul(0xc2b2ae35);
    h ^ (h >> 16)
}

/// A bound in Iceberg's single-value form (hex, as `avro.rs` writes bytes), or None.
fn bound_bytes(v: &ScalarValue) -> Option<String> {
    let b = match v {
        ScalarValue::Int32(Some(x)) | ScalarValue::Date32(Some(x)) => x.to_le_bytes().to_vec(),
        ScalarValue::Int64(Some(x)) | ScalarValue::TimestampMicrosecond(Some(x), _) => x.to_le_bytes().to_vec(),
        ScalarValue::Utf8(Some(s)) | ScalarValue::Utf8View(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => s.as_bytes().to_vec(),
        ScalarValue::Boolean(Some(x)) => vec![*x as u8],
        _ => return None,
    };
    Some(b.iter().map(|x| format!("{x:02x}")).collect())
}
