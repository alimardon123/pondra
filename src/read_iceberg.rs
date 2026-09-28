//! Iceberg tables, read natively (ADR-026): `iceberg_scan('s3://…/table')` (or its metadata
//! file), and tables of a catalog attached `(TYPE iceberg)`. Metadata (v1 to v3), the snapshot's
//! manifest list and manifests (Avro: `avro.rs`) give the files, each with its data sequence
//! number, row count, column bounds and NULL counts by field id; deletes follow Iceberg's rules:
//! position deletes and deletion vectors by position (`scan.rs`), equality deletes against files
//! older than them. Columns are matched by field id (renamed columns read right), or by the
//! table's name mapping. A feature not handled here is refused by name.
use crate::scan::{Delete, Outside};
use crate::store::{DataFile, Lake, TableMeta};
use anyhow::{bail, ensure, Context, Result};
use datafusion::arrow::datatypes::{DataType, Field, TimeUnit};
use datafusion::common::ScalarValue;
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, LazyLock, Mutex};

/// Manifests already read, by path: a manifest, once written, never changes.
static MANIFESTS: LazyLock<Mutex<lru::LruCache<String, Arc<Vec<Value>>>>> = LazyLock::new(|| Mutex::new(lru::LruCache::new(std::num::NonZeroUsize::new(256).unwrap())));

/// The table at `url` (a table's folder or a metadata file), at its current snapshot or the one
/// asked for: `version` (of its metadata), `snapshot_from_id`, `snapshot_from_timestamp`.
pub async fn resolve(lake: &Lake, url: &str, options: &BTreeMap<String, String>) -> Result<TableMeta> {
    let url = url.trim_end_matches('/');
    if options.contains_key("namespace") {
        return rest(lake, url, options).await;
    }
    let url = crate::ddl::full(url)?; // (a folder or file named relatively: from where the node runs)
    let url = url.as_str();
    let (at, meta) = metadata(lake, url, options.get("version").map(String::as_str)).await?;
    resolve_metadata(lake, url, &at, &meta, options).await
}

/// A table's current metadata (and where it is): its newest metadata file, or its REST
/// catalog's (for writing it: `write_outside`).
pub async fn current(lake: &Lake, url: &str, options: &BTreeMap<String, String>) -> Result<(String, Value)> {
    match options.contains_key("namespace") {
        true => {
            let body = rest_table(lake, url, options).await?;
            Ok((body["metadata-location"].as_str().unwrap_or_default().to_string(), body["metadata"].clone()))
        }
        false => metadata(lake, url, None).await,
    }
}

/// The current schema, and its columns (name, field id, type).
pub fn schema_of(meta: &Value) -> Result<(Value, Vec<(String, i64, DataType)>)> {
    let schema = current_schema(meta).context("no current schema")?;
    let columns = fields(&schema)?;
    Ok((schema, columns))
}

/// Commit a folder's table's next metadata: the file after `at` (put if absent), then its
/// version hint. False: another writer's is there.
pub async fn commit_file(lake: &Lake, url: &str, at: &str, next: &Value) -> Result<bool> {
    let name = at.rsplit('/').next().unwrap_or_default();
    let (hinted, number) = match name.strip_prefix('v') {
        Some(rest) => (true, rest.split('.').next().and_then(|n| n.parse::<i64>().ok())),
        None => (false, name.split('-').next().and_then(|n| n.parse::<i64>().ok())),
    };
    let n = number.with_context(|| format!("{at}: which version is this metadata?"))? + 1;
    let file = if hinted { format!("v{n}.metadata.json") } else { format!("{n:05}-{}.metadata.json", uuid::Uuid::new_v4()) };
    let dir = &at[..at.len() - name.len()];
    if !crate::write_outside::put_new(lake, &format!("{dir}{file}"), serde_json::to_vec_pretty(next)?).await? {
        return Ok(false);
    }
    if hinted {
        let (store, path) = crate::ext::store(lake, &format!("{dir}version-hint.text")).await?;
        object_store_df::ObjectStoreExt::put(&store, &path, n.to_string().into_bytes().into()).await.with_context(|| format!("writing {url}'s version hint"))?;
    }
    Ok(true)
}

/// Commit a snapshot through a REST catalog, asserting the one it follows. False: another
/// writer's came first.
pub async fn rest_commit(lake: &Lake, url: &str, options: &BTreeMap<String, String>, after: Option<i64>, snapshot: &Value) -> Result<bool> {
    let (at, token) = rest_path(lake, url, options).await?;
    let body = serde_json::json!({
        "requirements": [{"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": after}],
        "updates": [{"action": "add-snapshot", "snapshot": snapshot}, {"action": "set-snapshot-ref", "ref-name": "main", "type": "branch", "snapshot-id": snapshot["snapshot-id"]}],
    });
    let mut r = crate::ext::web().post(&at).json(&body);
    if let Some(t) = &token {
        r = r.bearer_auth(t);
    }
    let r = r.send().await.with_context(|| format!("reaching {at}"))?;
    let status = r.status();
    if status.as_u16() == 409 {
        return Ok(false);
    }
    let answer: Value = r.json().await.unwrap_or_default();
    ensure!(status.is_success(), "{url}: the commit was refused ({status}: {})", answer["error"]["message"].as_str().unwrap_or(""));
    Ok(true)
}

/// A table's path in its REST catalog (after the catalog's own prefix), and the token to use.
async fn rest_path(lake: &Lake, url: &str, options: &BTreeMap<String, String>) -> Result<(String, Option<String>)> {
    let token = crate::ext::rest_token(lake, url).await?;
    let enc = |s: &str| url::form_urlencoded::byte_serialize(s.as_bytes()).collect::<String>().replace('+', "%20");
    let mut config = format!("{url}/v1/config");
    if let Some(w) = options.get("warehouse") {
        config += &format!("?warehouse={}", enc(w));
    }
    let mut r = crate::ext::web().get(&config);
    if let Some(t) = &token {
        r = r.bearer_auth(t);
    }
    let r = r.send().await.with_context(|| format!("reaching {config}"))?;
    let conf: Value = if r.status().is_success() { r.json().await.unwrap_or_default() } else { Value::Null }; // (a catalog with no config: no prefix)
    let prefix = conf["overrides"]["prefix"].as_str().or(conf["defaults"]["prefix"].as_str()).map(|p| format!("{p}/")).unwrap_or_default();
    let (ns, table) = (&options["namespace"], &options["table"]);
    Ok((format!("{url}/v1/{prefix}namespaces/{}/tables/{}", enc(&ns.replace('.', "\u{1f}")), enc(table)), token))
}

/// A table as its REST catalog gives it (`metadata-location`, `metadata`, `config`).
async fn rest_table(lake: &Lake, url: &str, options: &BTreeMap<String, String>) -> Result<Value> {
    let (at, token) = rest_path(lake, url, options).await?;
    let mut r = crate::ext::web().get(&at);
    if let Some(t) = &token {
        r = r.bearer_auth(t);
    }
    let r = r.send().await.with_context(|| format!("reaching {at}"))?;
    let status = r.status();
    let body: Value = r.json().await.unwrap_or_default();
    ensure!(status.is_success(), "{url}: no table {}.{} ({status}: {})", options["namespace"], options["table"], body["error"]["message"].as_str().unwrap_or(""));
    Ok(body)
}

/// A table of an Iceberg REST catalog at `url`: its metadata as the catalog gives it (with the
/// token the secret covering the catalog gets: `ext::rest_token`).
async fn rest(lake: &Lake, url: &str, options: &BTreeMap<String, String>) -> Result<TableMeta> {
    let body = rest_table(lake, url, options).await?;
    let location = body["metadata-location"].as_str().unwrap_or(url).to_string();
    resolve_metadata(lake, body["metadata"]["location"].as_str().unwrap_or(url), &location, &body["metadata"], options).await
}


/// A table from its metadata (read from a file, or given by a REST catalog).
pub async fn resolve_metadata(lake: &Lake, url: &str, at: &str, meta: &Value, options: &BTreeMap<String, String>) -> Result<TableMeta> {
    let format = meta["format-version"].as_i64().unwrap_or(1);
    ensure!(format <= 3, "{at}: Iceberg format version {format}, which Pondra doesn't read yet (1 to 3)");
    let schema = current_schema(meta).with_context(|| format!("{at}: no current schema"))?;
    let columns = fields(&schema).with_context(|| at.to_string())?;
    let ids: HashMap<i64, (String, DataType)> = columns.iter().map(|(n, id, t)| (*id, (n.clone(), t.clone()))).collect();
    // A table moved since it was written names its files where they were (DuckDB's option).
    let location = meta["location"].as_str().unwrap_or_default().trim_end_matches('/').to_string();
    let moved = options.get("allow_moved_paths").is_some_and(|v| v == "true");
    let place = |p: &str| if moved && !location.is_empty() && p.starts_with(&location) { format!("{url}{}", &p[location.len()..]) } else { p.to_string() };
    let mut out = TableMeta { columns: columns.iter().map(|(n, _, t)| (n.clone(), crate::query::type_name(t))).collect(), ..Default::default() };
    out.outside = Some(crate::scan::Table { field_ids: columns.iter().map(|(n, id, _)| (n.clone(), *id)).collect(), name_mapping: meta["properties"]["schema.name-mapping.default"].as_str().map(str::to_string) });
    let Some(snapshot) = snapshot(meta, options)? else { return Ok(out) }; // (no snapshot yet: no rows)
    let manifests: Vec<Value> = match snapshot["manifest-list"].as_str() {
        Some(list) => crate::avro::records(&crate::ext::get(lake, &place(list)).await?)?,
        None => snapshot["manifests"].as_array().into_iter().flatten().map(|m| serde_json::json!({"manifest_path": m, "content": 0, "sequence_number": 0})).collect(), // (v1)
    };
    let mut reads = vec![];
    for m in &manifests {
        reads.push(manifest(lake, place(m["manifest_path"].as_str().context("a manifest without its path")?)));
    }
    let entries: Vec<Arc<Vec<Value>>> = futures::StreamExt::collect::<Vec<_>>(futures::StreamExt::buffered(futures::stream::iter(reads), 16)).await.into_iter().collect::<Result<_>>()?;
    let read: Vec<(&Value, Arc<Vec<Value>>)> = manifests.iter().zip(entries).collect();
    let (mut data, mut positions, mut equality) = (vec![], vec![], vec![]);
    for (m, entries) in &read {
        for e in entries.iter().filter(|e| e["status"].as_i64() != Some(2)) {
            let f = &e["data_file"];
            // Its data sequence number: its own, or (added in this manifest) the manifest's.
            let seq = e["sequence_number"].as_i64().or(m["sequence_number"].as_i64()).unwrap_or(0);
            match f["content"].as_i64().unwrap_or(0) {
                0 => data.push((seq, f.clone())),
                1 => positions.push((seq, f.clone())),
                2 => equality.push((seq, f.clone())),
                c => bail!("{at}: manifest content {c}"),
            }
        }
    }
    for (_, f) in &data {
        let format = f["file_format"].as_str().unwrap_or("PARQUET");
        ensure!(format.eq_ignore_ascii_case("parquet"), "{at}: {} is {format} (Pondra reads Iceberg's Parquet files)", f["file_path"]);
    }
    let mut files = vec![];
    for (seq, f) in &data {
        let written = f["file_path"].as_str().context("a data file without its path")?; // (as its deletes name it)
        let path = place(written);
        let mut d = DataFile { path: path.clone(), bytes: f["file_size_in_bytes"].as_u64().unwrap_or(0), rows: f["record_count"].as_u64().unwrap_or(0), ..Default::default() };
        let (lower, upper) = (by_id(&f["lower_bounds"]), by_id(&f["upper_bounds"]));
        for (id, (name, t)) in &ids {
            if let (Some(lo), Some(hi)) = (lower.get(id).and_then(|b| bound(b, t)), upper.get(id).and_then(|b| bound(b, t))) {
                d.stats.insert(name.clone(), (lo, hi));
            }
        }
        let nulls = by_count(&f["null_value_counts"]);
        d.nulls = ids.keys().all(|id| nulls.contains_key(id)).then(|| ids.iter().filter(|(id, _)| nulls[*id] > 0).map(|(_, (n, _))| n.clone()).collect());
        let mut deletes = vec![];
        for (dseq, p) in &positions {
            let applies = match p["referenced_data_file"].as_str() {
                Some(t) => t == written,
                None => *dseq >= *seq && in_bounds(p, written), // (a position delete: files no newer than it)
            };
            if !applies {
                continue;
            }
            let at_file = place(p["file_path"].as_str().context("a delete file without its path")?);
            deletes.push(match p["file_format"].as_str().unwrap_or("PARQUET").to_uppercase().as_str() {
                "PUFFIN" => Delete::Blob { path: at_file, offset: p["content_offset"].as_u64().context("a deletion vector without its offset")?, size: p["content_size_in_bytes"].as_u64().context("a deletion vector without its size")? },
                "PARQUET" => Delete::Positions { path: at_file, file: written.to_string() },
                other => bail!("{at}: position deletes in {other}, which aren't read"),
            });
        }
        for (dseq, e) in &equality {
            if *dseq > *seq {
                let names = e["equality_ids"].as_array().into_iter().flatten().filter_map(Value::as_i64).map(|id| ids.get(&id).map(|(n, _)| n.clone()).with_context(|| format!("{at}: an equality delete on field {id}, not in the schema"))).collect::<Result<Vec<_>>>()?;
                deletes.push(Delete::Equality { path: place(e["file_path"].as_str().context("a delete file without its path")?), columns: names, seq: *dseq });
            }
        }
        // Rows deleted by position: its count is no longer exact, just an upper bound.
        // (with equality deletes about, every file's sequence number: a newer row isn't deleted)
        d.outside = (!deletes.is_empty() || !equality.is_empty()).then(|| Box::new(Outside { values: vec![], deletes, rows: Some(d.rows), seq: Some(*seq) }));
        files.push(d);
    }
    files.sort_by(|a, b| a.path.cmp(&b.path));
    out.files = files;
    Ok(out)
}

/// A manifest's entries (read once: a manifest never changes).
async fn manifest(lake: &Lake, path: String) -> Result<Arc<Vec<Value>>> {
    if let Some(e) = MANIFESTS.lock().unwrap().get(&path).cloned() {
        return Ok(e);
    }
    let e = Arc::new(crate::avro::records(&crate::ext::get(lake, &path).await?)?);
    MANIFESTS.lock().unwrap().put(path, e.clone());
    Ok(e)
}

/// The metadata file: the one named, the one `version` names, the one `version-hint.text` names,
/// or the newest in `metadata/`.
pub async fn metadata(lake: &Lake, url: &str, version: Option<&str>) -> Result<(String, Value)> {
    let read = |at: String| async move {
        let b = crate::ext::get(lake, &at).await?;
        let b = if at.ends_with(".gz") { let mut v = vec![]; std::io::Read::read_to_end(&mut flate2::read::GzDecoder::new(&b[..]), &mut v)?; v.into() } else { b };
        anyhow::Ok((at, serde_json::from_slice::<Value>(&b)?))
    };
    if url.ends_with(".metadata.json") || url.ends_with(".metadata.json.gz") {
        return read(url.to_string()).await;
    }
    let (store, dir) = crate::ext::store(lake, &format!("{url}/metadata/")).await?;
    let listed: Vec<String> = futures::TryStreamExt::try_collect::<Vec<_>>(futures::TryStreamExt::map_ok(store.list(Some(&dir)), |o| o.location.filename().unwrap_or_default().to_string())).await?;
    let number = |n: &str| -> Option<i64> { n.strip_prefix('v').unwrap_or(n).split(['.', '-']).next()?.parse().ok() };
    let files: Vec<&String> = listed.iter().filter(|n| n.ends_with(".metadata.json") || n.ends_with(".metadata.json.gz")).collect();
    let wanted = match version {
        Some(v) => Some(v.parse::<i64>().with_context(|| format!("version is a number, not {v}"))?),
        None if listed.iter().any(|n| n == "version-hint.text") => {
            let hint = String::from_utf8(crate::ext::get(lake, &format!("{url}/metadata/version-hint.text")).await?.to_vec())?;
            match hint.trim().parse::<i64>() {
                Ok(v) => Some(v),
                Err(_) => return read(format!("{url}/metadata/{}", hint.trim())).await,
            }
        }
        None => None,
    };
    let chosen = match wanted {
        Some(v) => files.iter().find(|n| number(n) == Some(v)).with_context(|| format!("{url}: no metadata version {v}"))?,
        None => files.iter().max_by_key(|n| number(n).unwrap_or(-1)).with_context(|| format!("{url}: no Iceberg table there (no metadata/*.metadata.json)"))?,
    };
    read(format!("{url}/metadata/{chosen}")).await
}

fn current_schema(meta: &Value) -> Option<Value> {
    match meta["current-schema-id"].as_i64() {
        Some(id) => meta["schemas"].as_array()?.iter().find(|s| s["schema-id"].as_i64() == Some(id)).cloned(),
        None => meta.get("schema").cloned(), // (v1)
    }
}

/// The snapshot to read: current, or the one asked for by id or time.
fn snapshot(meta: &Value, options: &BTreeMap<String, String>) -> Result<Option<Value>> {
    let snaps = meta["snapshots"].as_array().cloned().unwrap_or_default();
    if let Some(id) = options.get("snapshot_from_id") {
        let id: i64 = id.parse().with_context(|| format!("snapshot_from_id is a number, not {id}"))?;
        return Ok(Some(snaps.into_iter().find(|s| s["snapshot-id"].as_i64() == Some(id)).with_context(|| format!("no snapshot {id}"))?));
    }
    if let Some(t) = options.get("snapshot_from_timestamp") {
        let ms = match ScalarValue::Utf8(Some(t.clone())).cast_to(&DataType::Timestamp(TimeUnit::Millisecond, None)) {
            Ok(ScalarValue::TimestampMillisecond(Some(ms), _)) => ms,
            _ => bail!("snapshot_from_timestamp: a time, as '2026-09-28 12:00:00', not {t}"),
        };
        return Ok(Some(snaps.into_iter().filter(|s| s["timestamp-ms"].as_i64().is_some_and(|at| at <= ms)).max_by_key(|s| s["timestamp-ms"].as_i64()).with_context(|| format!("no snapshot as early as {t}"))?));
    }
    let Some(id) = meta["current-snapshot-id"].as_i64().filter(|id| *id >= 0) else { return Ok(None) };
    Ok(snaps.into_iter().find(|s| s["snapshot-id"].as_i64() == Some(id)))
}

/// A schema's top-level columns: (name, field id, type).
fn fields(schema: &Value) -> Result<Vec<(String, i64, DataType)>> {
    schema["fields"].as_array().into_iter().flatten().map(|f| {
        let name = f["name"].as_str().context("a field without a name")?.to_string();
        ensure!(f.get("initial-default").is_none_or(Value::is_null), "column {name} has a default for older rows (a v3 feature not read yet)");
        Ok((name.clone(), f["id"].as_i64().context("a field without its id")?, arrow_type(&f["type"]).with_context(|| format!("column {name}"))?))
    }).collect()
}

/// An Iceberg type as Arrow's (nested fields carry their ids as Parquet does).
fn arrow_type(t: &Value) -> Result<DataType> {
    let field = |name: &str, id: &Value, t: &Value, required: bool| -> Result<Arc<Field>> {
        let f = Field::new(name, arrow_type(t)?, !required);
        Ok(Arc::new(match id.as_i64() {
            Some(id) => f.with_metadata([("PARQUET:field_id".to_string(), id.to_string())].into()),
            None => f,
        }))
    };
    Ok(match t {
        Value::String(s) => match s.as_str() {
            "boolean" => DataType::Boolean,
            "int" => DataType::Int32,
            "long" => DataType::Int64,
            "float" => DataType::Float32,
            "double" => DataType::Float64,
            "date" => DataType::Date32,
            "time" => DataType::Time64(TimeUnit::Microsecond),
            "timestamp" => DataType::Timestamp(TimeUnit::Microsecond, None),
            "timestamptz" => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            "timestamp_ns" => DataType::Timestamp(TimeUnit::Nanosecond, None),
            "timestamptz_ns" => DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            "string" => DataType::Utf8,
            "uuid" => DataType::FixedSizeBinary(16),
            "binary" => DataType::Binary,
            d if d.starts_with("decimal(") => {
                let (p, s) = d[8..d.len() - 1].split_once(',').context("decimal(p, s)")?;
                DataType::Decimal128(p.trim().parse()?, s.trim().parse()?)
            }
            d if d.starts_with("fixed[") => DataType::FixedSizeBinary(d[6..d.len() - 1].parse()?),
            other => bail!("the type {other} isn't read yet"),
        },
        Value::Object(o) => match o.get("type").and_then(Value::as_str) {
            Some("list") => DataType::List(field("element", &o["element-id"], &o["element"], o["element-required"].as_bool().unwrap_or(false))?),
            Some("struct") => DataType::Struct(o["fields"].as_array().into_iter().flatten().map(|f| field(f["name"].as_str().unwrap_or_default(), &f["id"], &f["type"], f["required"].as_bool().unwrap_or(false))).collect::<Result<Vec<_>>>()?.into()),
            Some("map") => {
                let entries = Field::new("key_value", DataType::Struct(vec![field("key", &o["key-id"], &o["key"], true)?, field("value", &o["value-id"], &o["value"], o["value-required"].as_bool().unwrap_or(false))?].into()), false);
                DataType::Map(Arc::new(entries), false)
            }
            Some(other) => bail!("the type {other} isn't read yet"),
            None => bail!("a type without a name"),
        },
        other => bail!("the type {other} isn't read"),
    })
}

/// A manifest's map from field id (Avro: a list of key/value records, or a map) to its value.
fn by_id(v: &Value) -> HashMap<i64, Vec<u8>> {
    let pairs: Vec<(i64, &Value)> = match v {
        Value::Array(a) => a.iter().filter_map(|kv| Some((kv["key"].as_i64()?, &kv["value"]))).collect(),
        Value::Object(o) => o.iter().filter_map(|(k, v)| Some((k.parse().ok()?, v))).collect(),
        _ => vec![],
    };
    pairs.into_iter().filter_map(|(k, v)| Some((k, crate::avro::unhex(v.as_str()?).ok()?))).collect()
}

fn by_count(v: &Value) -> HashMap<i64, i64> {
    match v {
        Value::Array(a) => a.iter().filter_map(|kv| Some((kv["key"].as_i64()?, kv["value"].as_i64()?))).collect(),
        _ => HashMap::new(),
    }
}

/// A bound in Iceberg's single-value form as a file's range bound (text), or None where it can't
/// bound: floats (NaN), and types not ordered here.
fn bound(b: &[u8], t: &DataType) -> Option<String> {
    let le8 = || Some(i64::from_le_bytes(b.get(..8)?.try_into().ok()?));
    let le4 = || Some(i32::from_le_bytes(b.get(..4)?.try_into().ok()?));
    let v = match t {
        DataType::Boolean => ScalarValue::Boolean(Some(*b.first()? != 0)),
        DataType::Int32 => ScalarValue::Int32(Some(le4()?)),
        DataType::Int64 => ScalarValue::Int64(Some(le8()?)),
        DataType::Date32 => ScalarValue::Date32(Some(le4()?)),
        DataType::Timestamp(TimeUnit::Microsecond, tz) => ScalarValue::TimestampMicrosecond(Some(le8()?), tz.clone()),
        DataType::Timestamp(TimeUnit::Nanosecond, tz) => ScalarValue::TimestampNanosecond(Some(le8()?), tz.clone()),
        DataType::Utf8 => ScalarValue::Utf8(Some(String::from_utf8(b.to_vec()).ok()?)),
        DataType::Decimal128(p, s) => {
            let mut full = [if b.first()? & 0x80 != 0 { 0xff } else { 0 }; 16];
            full[16 - b.len().min(16)..].copy_from_slice(&b[b.len().saturating_sub(16)..]);
            ScalarValue::Decimal128(Some(i128::from_be_bytes(full)), *p, *s)
        }
        _ => return None,
    };
    crate::manifest::text(&v)
}

/// Might a position-delete file hold rows of `path`? (Its bounds on `file_path`, if it has them.)
fn in_bounds(delete: &Value, path: &str) -> bool {
    const FILE_PATH: i64 = 2147483546; // (the field id of a position delete's file_path)
    let text = |v: &Value| by_id(v).get(&FILE_PATH).and_then(|b| String::from_utf8(b.clone()).ok());
    match (text(&delete["lower_bounds"]), text(&delete["upper_bounds"])) {
        (Some(lo), Some(hi)) => lo.as_str() <= path && path <= hi.as_str(),
        _ => true,
    }
}
