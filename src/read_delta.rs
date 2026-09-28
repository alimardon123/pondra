//! Delta Lake tables, read natively (ADR-026): `delta_scan('s3://…/table')` and the tables of a
//! folder attached `(TYPE delta)`. The log — JSON commits and Parquet checkpoints (classic, in
//! parts, and v2 with sidecars) — replayed to the table's files, schema and features: column
//! mapping (by name and by id), partition values, deletion vectors, statistics that skip files,
//! and an older version (`version => n`). A reader feature not handled here is refused by name,
//! never read wrongly.
use crate::scan::{Delete, Outside};
use crate::store::{DataFile, Lake, TableMeta};
use anyhow::{bail, ensure, Context, Result};
use datafusion::arrow::datatypes::{DataType, Field, TimeUnit};
use datafusion::common::ScalarValue;
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, LazyLock, Mutex};

/// The reader features read here (Delta's protocol, reader version 3).
const FEATURES: &[&str] = &["columnMapping", "deletionVectors", "timestampNtz", "typeWidening", "typeWidening-preview", "v2Checkpoint", "vacuumProtocolCheck"];

/// A table as its log says at a version.
#[derive(Default, Clone)]
pub struct Log {
    pub version: i64,
    pub protocol: Value,
    pub metadata: Value,
    files: HashMap<String, Value>, // add actions, by logical file (its path and deletion vector)
    pub txns: std::collections::HashSet<String>, // the writers' transactions applied (their app ids)
}

impl Log {
    /// The table's files, as their add actions.
    pub fn live(&self) -> impl Iterator<Item = &Value> { self.files.values() }

    /// One action. A checkpoint's removes are tombstones for cleanup: its adds are the table.
    fn apply(&mut self, a: &Value, checkpoint: bool) {
        if let Some(p) = a.get("protocol") {
            self.protocol = p.clone();
        }
        if let Some(m) = a.get("metaData") {
            self.metadata = m.clone();
        }
        if let Some(add) = a.get("add") {
            self.files.insert(logical(add), add.clone());
        }
        if let (Some(r), false) = (a.get("remove"), checkpoint) {
            self.files.remove(&logical(r));
        }
        if let Some(id) = a["txn"]["appId"].as_str() {
            self.txns.insert(id.to_string());
        }
    }
}

/// A file's identity: its path and deletion vector (a new vector makes it a new logical file).
fn logical(a: &Value) -> String {
    let dv = &a["deletionVector"];
    format!("{}|{}{}@{}", a["path"].as_str().unwrap_or_default(), dv["storageType"].as_str().unwrap_or_default(), dv["pathOrInlineDv"].as_str().unwrap_or_default(), dv["offset"])
}

/// Logs already replayed, by table and version: a commit, once written, never changes, so the
/// next statement reads only the commits after the newest it knows.
static SEEN: LazyLock<Mutex<lru::LruCache<String, Arc<Log>>>> = LazyLock::new(|| Mutex::new(lru::LruCache::new(std::num::NonZeroUsize::new(16).unwrap())));

/// The table at `root` (its latest version, or `version`), as a table of files.
pub async fn resolve(lake: &Lake, root: &str, version: Option<i64>) -> Result<TableMeta> {
    let root = crate::ddl::full(root)?; // (a folder named relatively: from where the node runs)
    let root = root.as_str();
    let log = replay(lake, root, version).await?;
    features(root, &log.protocol)?;
    let (columns, partitions, mapped) = schema(root, &log.metadata)?;
    let types: HashMap<&str, DataType> = columns.iter().map(|(p, _, t)| (p.as_str(), t.clone())).collect();
    let mut files: Vec<DataFile> = log.files.values().map(|add| file(root, add, &partitions, &types)).collect::<Result<_>>()?;
    files.sort_by(|a, b| a.path.cmp(&b.path)); // (every node lists them in one order)
    Ok(TableMeta {
        columns: columns.iter().map(|(p, _, t)| (p.clone(), crate::query::type_name(t))).collect(),
        names: columns.iter().filter(|(p, l, _)| mapped && p != l).map(|(p, l, _)| (p.clone(), l.clone())).collect(),
        files,
        ..Default::default()
    })
}

/// The log replayed to `version` (the newest there is if none): from the newest version already
/// replayed or the newest checkpoint before it, then the commits after.
pub async fn replay(lake: &Lake, root: &str, version: Option<i64>) -> Result<Arc<Log>> {
    let (store, dir) = crate::ext::store(lake, &format!("{root}/_delta_log/")).await?;
    let known = SEEN.lock().unwrap().get(root).cloned();
    // Only what's newer than we know (or than a checkpoint): the log is listed from there.
    let from = known.as_ref().map(|l| l.version).filter(|v| version.is_none_or(|w| *v <= w)).unwrap_or(0);
    let offset = object_store_df::path::Path::parse(format!("{dir}/{from:020}")).ok();
    let listed = match &offset {
        Some(o) if from > 0 => store.list_with_offset(Some(&dir), o),
        _ => store.list(Some(&dir)),
    };
    let names: Vec<String> = futures::TryStreamExt::try_collect::<Vec<_>>(futures::TryStreamExt::map_ok(listed, |o| o.location.filename().unwrap_or_default().to_string())).await?;
    let mut commits: BTreeMap<i64, String> = BTreeMap::new();
    let mut checkpoints: BTreeMap<i64, Vec<String>> = BTreeMap::new();
    let mut parts: BTreeMap<(i64, usize), Vec<String>> = BTreeMap::new();
    for n in &names {
        let Some((v, rest)) = n.split_once('.').and_then(|(v, r)| Some((v.parse::<i64>().ok().filter(|_| v.len() == 20)?, r))) else { continue };
        match rest.split('.').collect::<Vec<_>>()[..] {
            ["json"] => drop(commits.insert(v, n.clone())),
            ["checkpoint", "parquet"] => checkpoints.entry(v).or_default().push(n.clone()),
            ["checkpoint", id, "json" | "parquet"] if id.len() == 36 => drop(checkpoints.insert(v, vec![n.clone()])), // (v2: one file, sidecars named in it)
            ["checkpoint", part, of, "parquet"] => {
                let of: usize = of.parse().unwrap_or(0);
                let list = parts.entry((v, of)).or_default();
                list.push(n.clone());
                if list.len() == of && part.len() == 10 {
                    checkpoints.insert(v, list.clone()); // (in parts: every part there)
                }
            }
            _ => {}
        }
    }
    let newest = commits.keys().chain(checkpoints.keys()).max().copied().or(known.as_ref().map(|l| l.version));
    let upto = match version {
        Some(v) => v,
        None => newest.with_context(|| format!("{root}: no Delta table there (no _delta_log/)"))?,
    };
    ensure!(newest.is_some_and(|n| n >= upto), "{root}: the table has no version {upto} yet");
    if let Some(l) = known.as_ref().filter(|l| l.version == upto) {
        return Ok(l.clone());
    }
    let mut log = match known.as_ref().filter(|l| l.version < upto && (l.version + 1..=upto).all(|v| commits.contains_key(&v))) {
        Some(l) => (**l).clone(),
        None => match checkpoints.range(..=upto).next_back() {
            Some((v, files)) => {
                let mut log = Log { version: *v, ..Default::default() };
                for a in checkpoint(lake, root, files).await? {
                    log.apply(&a, true);
                }
                log
            }
            None if from > 0 => return Box::pin(replay_from_start(lake, root, version)).await, // (older than what was listed)
            None => Log { version: -1, ..Default::default() },
        },
    };
    for v in log.version + 1..=upto {
        let name = commits.get(&v).with_context(|| format!("{root}: the log has no commit {v} (its retention removed it?)"))?;
        for a in lines(&crate::ext::get(lake, &format!("{root}/_delta_log/{name}")).await?)? {
            log.apply(&a, false);
        }
        log.version = v;
    }
    let log = Arc::new(log);
    if version.is_none() {
        SEEN.lock().unwrap().put(root.to_string(), log.clone());
    }
    Ok(log)
}

/// When what was known is older than every commit left, from the whole listing.
async fn replay_from_start(lake: &Lake, root: &str, version: Option<i64>) -> Result<Arc<Log>> {
    SEEN.lock().unwrap().pop(root);
    replay(lake, root, version).await
}

/// A checkpoint's actions: its Parquet parts, or a v2 checkpoint's own actions and its sidecars'.
async fn checkpoint(lake: &Lake, root: &str, files: &[String]) -> Result<Vec<Value>> {
    let url = |n: &str| format!("{root}/_delta_log/{n}");
    let mut out = match files {
        [one] if one.ends_with(".json") => lines(&crate::ext::get(lake, &url(one)).await?)?,
        _ => parquet_actions(lake, files.iter().map(|n| url(n)).collect()).await?,
    };
    let sidecars: Vec<String> = out.iter().filter_map(|a| a["sidecar"]["path"].as_str()).map(|p| if p.contains("://") { p.to_string() } else { url(&format!("_sidecars/{}", unescape(p))) }).collect();
    if !sidecars.is_empty() {
        out.extend(parquet_actions(lake, sidecars).await?);
    }
    Ok(out)
}

/// Parquet files of actions (checkpoints, sidecars) as JSON actions, as a commit holds them.
async fn parquet_actions(lake: &Lake, urls: Vec<String>) -> Result<Vec<Value>> {
    use datafusion::arrow::json as arrow_json;
    let batches = lake.session().read_parquet(urls, Default::default()).await?.collect().await?;
    let mut w = arrow_json::LineDelimitedWriter::new(vec![]);
    for b in &batches {
        w.write(b)?;
    }
    w.finish()?;
    lines(&w.into_inner())
}

fn lines(body: &[u8]) -> Result<Vec<Value>> {
    body.split(|b| *b == b'\n').filter(|l| !l.iter().all(u8::is_ascii_whitespace)).map(|l| Ok(serde_json::from_slice(l)?)).collect()
}

/// Refuse a table whose reading needs what isn't done here.
fn features(root: &str, protocol: &Value) -> Result<()> {
    let version = protocol["minReaderVersion"].as_i64().unwrap_or(1);
    ensure!(version <= 3, "{root}: a Delta table of reader version {version}, which Pondra doesn't read yet (1 to 3)");
    for f in protocol["readerFeatures"].as_array().into_iter().flatten().filter_map(Value::as_str) {
        ensure!(FEATURES.contains(&f), "{root}: a Delta table with the reader feature {f}, which Pondra doesn't read yet (it reads {})", FEATURES.join(", "));
    }
    Ok(())
}

/// The table's columns — (physical name, logical name, type), partition columns in their place —
/// its partition columns (physical names), and whether names are mapped (column mapping).
pub fn schema(root: &str, metadata: &Value) -> Result<(Vec<(String, String, DataType)>, Vec<String>, bool)> {
    ensure!(metadata["format"]["provider"].as_str().is_none_or(|p| p == "parquet"), "{root}: a Delta table of {} files (Parquet only)", metadata["format"]["provider"]);
    let mode = metadata["configuration"]["delta.columnMapping.mode"].as_str().unwrap_or("none");
    let s: Value = serde_json::from_str(metadata["schemaString"].as_str().with_context(|| format!("{root}: no schema in the log"))?)?;
    let mut columns = vec![];
    for f in s["fields"].as_array().into_iter().flatten() {
        let logical = f["name"].as_str().context("a field without a name")?.to_string();
        let physical = match mode {
            "none" => logical.clone(),
            _ => f["metadata"]["delta.columnMapping.physicalName"].as_str().unwrap_or(&logical).to_string(),
        };
        columns.push((physical, logical.clone(), arrow_type(&f["type"]).with_context(|| format!("{root}: column {logical}"))?));
    }
    let by_logical: HashMap<&str, &str> = columns.iter().map(|(p, l, _)| (l.as_str(), p.as_str())).collect();
    let partitions = metadata["partitionColumns"].as_array().into_iter().flatten().filter_map(Value::as_str)
        .map(|l| by_logical.get(l).map(|p| p.to_string()).with_context(|| format!("{root}: partition column {l} isn't in the schema")))
        .collect::<Result<Vec<_>>>()?;
    Ok((columns, partitions, mode != "none"))
}

/// A Delta type as Arrow's.
fn arrow_type(t: &Value) -> Result<DataType> {
    let item = |v: &Value, n: &str, nullable: bool| -> Result<Arc<Field>> { Ok(Arc::new(Field::new(n, arrow_type(v)?, nullable))) };
    Ok(match t {
        Value::String(s) => match s.as_str() {
            "string" => DataType::Utf8,
            "long" => DataType::Int64,
            "integer" => DataType::Int32,
            "short" => DataType::Int16,
            "byte" => DataType::Int8,
            "float" => DataType::Float32,
            "double" => DataType::Float64,
            "boolean" => DataType::Boolean,
            "binary" => DataType::Binary,
            "date" => DataType::Date32,
            "timestamp" => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            "timestamp_ntz" => DataType::Timestamp(TimeUnit::Microsecond, None),
            d if d.starts_with("decimal(") => {
                let (p, s) = d[8..d.len() - 1].split_once(',').context("decimal(p, s)")?;
                DataType::Decimal128(p.trim().parse()?, s.trim().parse()?)
            }
            other => bail!("the type {other} isn't read yet"),
        },
        Value::Object(o) => match o.get("type").and_then(Value::as_str) {
            Some("array") => DataType::List(item(&o["elementType"], "element", o["containsNull"].as_bool().unwrap_or(true))?),
            Some("struct") => DataType::Struct(o["fields"].as_array().into_iter().flatten().map(|f| item(&f["type"], f["name"].as_str().unwrap_or_default(), true)).collect::<Result<Vec<_>>>()?.into()),
            Some(other) => bail!("the type {other} isn't read yet"),
            None => bail!("a type without a name"),
        },
        other => bail!("the type {other} isn't read"),
    })
}

/// One of the table's files: where, how big, its rows (not counting deleted ones), its columns'
/// ranges and NULLs from its statistics, its partition values, its deletion vector.
fn file(root: &str, add: &Value, partitions: &[String], types: &HashMap<&str, DataType>) -> Result<DataFile> {
    let rel = add["path"].as_str().context("an add without a path")?;
    let path = if rel.contains("://") { unescape(rel) } else { format!("{root}/{}", unescape(rel)) }; // (the object's own name)
    let stats: Value = add["stats"].as_str().and_then(|s| serde_json::from_str(s).ok()).unwrap_or_default();
    let physical = stats["numRecords"].as_u64();
    let dv = &add["deletionVector"];
    let gone = dv["cardinality"].as_u64().unwrap_or(0);
    let mut f = DataFile { path, bytes: add["size"].as_u64().unwrap_or_default(), rows: physical.map_or(add["size"].as_u64().unwrap_or_default() / 100, |n| n.saturating_sub(gone)), ..Default::default() };
    let mut nulls = Some(vec![]);
    for (c, t) in types.iter().filter(|(c, _)| !partitions.iter().any(|p| p == *c)) {
        let bound = |v: &Value, max: bool| bound(v, t, max);
        if let (Some(lo), Some(hi)) = (bound(&stats["minValues"][c], false), bound(&stats["maxValues"][c], true)) {
            f.stats.insert(c.to_string(), (lo, hi));
        }
        match (stats["nullCount"][c].as_u64(), nulls.as_mut()) {
            (Some(0), _) => {}
            (Some(_), Some(n)) => n.push(c.to_string()),
            _ => nulls = None,
        }
    }
    let mut values = vec![];
    for p in partitions {
        let v = add["partitionValues"][p].as_str().filter(|v| !v.is_empty()).map(str::to_string);
        match v.as_deref().and_then(|v| bound(&Value::String(v.into()), &types[p.as_str()], false)) {
            Some(text) => drop(f.stats.insert(p.clone(), (text.clone(), text))),
            None => nulls.iter_mut().for_each(|n| n.push(p.clone())), // (NULL's partition: `IS NULL` finds it)
        }
        values.push((p.clone(), v));
    }
    f.nulls = nulls;
    let deletes = match dv["storageType"].as_str() {
        None => vec![],
        Some(kind) => {
            let (at, size) = (dv["offset"].as_u64().unwrap_or(1), dv["sizeInBytes"].as_u64().context("a deletion vector without its size")?);
            let where_ = dv["pathOrInlineDv"].as_str().context("a deletion vector without a place")?;
            vec![match kind {
                "i" => Delete::Inline { z85: where_.to_string(), size },
                "u" => {
                    let (prefix, id) = where_.split_at(where_.len().checked_sub(20).context("a deletion vector's name")?);
                    let u = uuid::Uuid::from_slice(&crate::scan::z85(id)?)?;
                    let dir = if prefix.is_empty() { String::new() } else { format!("{prefix}/") };
                    Delete::Vector { path: format!("{root}/{dir}deletion_vector_{u}.bin"), offset: at, size }
                }
                "p" => Delete::Vector { path: unescape(where_), offset: at, size },
                k => bail!("{rel}: a deletion vector stored as {k:?}, which isn't read yet"),
            }]
        }
    };
    if !values.is_empty() || !deletes.is_empty() {
        f.outside = Some(Box::new(Outside { values, deletes, rows: physical, seq: None }));
    }
    Ok(f)
}

/// A statistic (or partition value) as a file's range bound in the column's type, or None if it
/// can't bound: floats (NaN), text cut short (Delta keeps 32 characters: a max may be a prefix).
/// Timestamps are kept to the millisecond: a max is widened to the end of its millisecond.
fn bound(v: &Value, t: &DataType, max: bool) -> Option<String> {
    let text = match v {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        _ => return None,
    };
    if t.is_floating() || (max && matches!(t, DataType::Utf8) && text.chars().count() >= 32) {
        return None;
    }
    let mut s = ScalarValue::Utf8(Some(text)).cast_to(t).ok()?;
    if let (true, ScalarValue::TimestampMicrosecond(Some(us), _)) = (max, &mut s) {
        *us += 999;
    }
    crate::manifest::text(&s)
}

/// A path in the log, as the object it names (the log escapes it as a URI).
pub fn unescape(p: &str) -> String { url::form_urlencoded::parse(format!("x={}", p.replace('+', "%2B")).as_bytes()).next().map(|(_, v)| v.into_owned()).unwrap_or_default() }
