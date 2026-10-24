//! Delta Lake tables, read natively (ADR-026): `delta_scan('s3://…/table')` and the tables of a
//! folder attached `(TYPE delta)`. The log — JSON commits and Parquet checkpoints (classic, in
//! parts, and v2 with sidecars) — replayed to the table's files, schema and features: column
//! mapping (by name and by id), partition values, deletion vectors, statistics that skip files,
//! and an older version (`version => n`). A reader feature not handled here is refused by name,
//! never read wrongly.
//! In the core over `reach::Reach`, so a page reads a lake's published Delta tables too; a node's `TableMeta` is made
//! from what it finds (`ext::table_meta`).
use crate::data_file::DataFile;
use crate::reach::Reach;
use crate::scan::{Delete, Outside};
use anyhow::{bail, ensure, Context, Result};
use datafusion::arrow::datatypes::{DataType, Field, TimeUnit};
use datafusion::common::ScalarValue;
use datafusion::object_store::{path::Path, ObjectStoreExt};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

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

crate::per_node!(
    /// Logs already replayed, by table and version: a commit, once written, never changes, so the
    /// next statement reads only the commits after the newest it knows.
    static SEEN: Mutex<lru::LruCache<String, Arc<Log>>> = Mutex::new(lru::LruCache::new(std::num::NonZeroUsize::new(16).unwrap())); clear = |m| m.lock().unwrap_or_else(|e| e.into_inner()).clear());

/// A table as its log says it at a version: its columns (name and type), the names of its renamed columns, and its
/// files. A node makes its own table from it (`ext::table_meta`).
pub struct Found {
    pub columns: Vec<(String, String)>,
    pub names: BTreeMap<String, String>,
    pub files: Vec<DataFile>,
}

/// The table at `root` (its latest version, or `version`), as a table of files.
pub async fn resolve(reach: &impl Reach, root: &str, version: Option<i64>) -> Result<Found> {
    let log = replay(reach, root, version).await?;
    table(root, &log.protocol, &log.metadata, log.files.values())
}

/// A table of files from a version's protocol, metadata and add actions (a log's, or what a Delta
/// Sharing server answered: `sharing::read`).
pub fn table<'a>(root: &str, protocol: &Value, metadata: &Value, adds: impl Iterator<Item = &'a Value>) -> Result<Found> {
    features(root, protocol)?;
    let (columns, partitions, mapped) = schema(root, metadata)?;
    let types: HashMap<&str, DataType> = columns.iter().map(|(p, _, t)| (p.as_str(), t.clone())).collect();
    let mut files: Vec<DataFile> = adds.map(|add| file(root, add, &partitions, &types)).collect::<Result<_>>()?;
    files.sort_by(|a, b| a.path.cmp(&b.path)); // (every node lists them in one order)
    Ok(Found {
        columns: columns.iter().map(|(p, _, t)| (p.clone(), crate::answer::type_name(t))).collect(),
        names: columns.iter().filter(|(p, l, _)| mapped && p != l).map(|(p, l, _)| (p.clone(), l.clone())).collect(),
        files,
    })
}

/// The log replayed to `version` (the newest there is if none): from the newest version already
/// replayed or the newest checkpoint before it, then the commits after.
pub async fn replay(reach: &impl Reach, root: &str, version: Option<i64>) -> Result<Arc<Log>> {
    if matches!(crate::ext_names::scheme(root), Some("http" | "https")) {
        return replay_unlisted(reach, root, version).await; // (a web server lists nothing: the log is read without a listing)
    }
    let (store, dir) = reach.store(&format!("{root}/_delta_log/")).await?;
    let known = SEEN.lock().unwrap().get(root).cloned();
    // Only what's newer than we know (or than a checkpoint): the log is listed from there.
    let from = known.as_ref().map(|l| l.version).filter(|v| version.is_none_or(|w| *v <= w)).unwrap_or(0);
    let offset = Path::parse(format!("{dir}/{from:020}")).ok();
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
            Some((v, files)) => from_checkpoint(reach, root, *v, files).await?,
            None if from > 0 => return Box::pin(replay_from_start(reach, root, version)).await, // (older than what was listed)
            None => Log { version: -1, ..Default::default() },
        },
    };
    for v in log.version + 1..=upto {
        let name = commits.get(&v).with_context(|| format!("{root}: the log has no commit {v} (its retention removed it?)"))?;
        for a in lines(&crate::reach::get(reach, &format!("{root}/_delta_log/{name}")).await?)? {
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
async fn replay_from_start(reach: &impl Reach, root: &str, version: Option<i64>) -> Result<Arc<Log>> {
    SEEN.lock().unwrap().pop(root);
    replay(reach, root, version).await
}

/// A log at `version` from a checkpoint's files: its actions applied, with the removes left out (a checkpoint's
/// removes are tombstones for cleanup, and its adds are the table).
async fn from_checkpoint(reach: &impl Reach, root: &str, version: i64, files: &[String]) -> Result<Log> {
    let mut log = Log { version, ..Default::default() };
    for a in checkpoint(reach, root, files).await? {
        log.apply(&a, true);
    }
    Ok(log)
}

/// The log of a web root, which can't list `_delta_log/`: it starts from the newest log known (`SEEN`) or the
/// checkpoint `_last_checkpoint` names, and reads the commits after its start by name until one isn't there. Only the
/// latest is kept, as a listed read keeps it.
async fn replay_unlisted(reach: &impl Reach, root: &str, version: Option<i64>) -> Result<Arc<Log>> {
    let known = SEEN.lock().unwrap().get(root).cloned();
    if let Some(l) = known.as_ref().filter(|l| Some(l.version) == version) {
        return Ok(l.clone());
    }
    let last = last_checkpoint(reach, root).await?;
    // (the newest the table is known to have reached: a commit a named version needs and that isn't there is past it)
    let newest = known.as_ref().map_or(-1, |l| l.version).max(last.as_ref().map_or(-1, |(c, _)| *c));
    let mut log = start(reach, root, known.as_ref(), last.as_ref(), version).await?;
    apply_commits(reach, root, &mut log, version, newest).await?;
    let log = Arc::new(log);
    if version.is_none() {
        SEEN.lock().unwrap().put(root.to_string(), log.clone());
    }
    Ok(log)
}

/// Where an unlisted read starts: the newest of what's known and a checkpoint, each at or below `version` (any, for the
/// latest). Neither: version -1, and the commits are read from 0.
async fn start(reach: &impl Reach, root: &str, known: Option<&Arc<Log>>, last: Option<&(i64, Vec<String>)>, version: Option<i64>) -> Result<Log> {
    let known = known.filter(|l| version.is_none_or(|v| l.version <= v));
    let at = match (last, version) {
        (Some((c, _)), Some(v)) if *c > v => spaced(reach, root, v).await?, // (the newest is past `version`: the checkpoint at its ten, if there)
        (Some(last), _) => Some(last.clone()),
        (None, _) => None,
    };
    Ok(match (known, at) {
        (Some(l), Some((c, _))) if l.version >= c => (**l).clone(),
        (_, Some((c, files))) => from_checkpoint(reach, root, c, &files).await?,
        (Some(l), None) => (**l).clone(),
        (None, None) => Log { version: -1, ..Default::default() },
    })
}

/// The newest checkpoint a web root names in `_last_checkpoint` (a hint Delta keeps for readers that can't list): its
/// version, and the files its actions are in, as `checkpoint` reads them.
async fn last_checkpoint(reach: &impl Reach, root: &str) -> Result<Option<(i64, Vec<String>)>> {
    let Some(body) = crate::reach::found(reach, &format!("{root}/_delta_log/_last_checkpoint")).await? else { return Ok(None) };
    let last: Value = serde_json::from_slice(&body).with_context(|| format!("{root}: _last_checkpoint isn't JSON"))?;
    let c = last["version"].as_i64().with_context(|| format!("{root}: _last_checkpoint without its version"))?;
    let names = match (last["v2Checkpoint"]["path"].as_str(), last["parts"].as_u64()) {
        (Some(path), _) => vec![path.to_string()], // (v2: one file, its sidecars named in it)
        (None, Some(parts)) => (1..=parts).map(|i| format!("{c:020}.checkpoint.{i:010}.{parts:010}.parquet")).collect(),
        (None, None) => vec![format!("{c:020}.checkpoint.parquet")],
    };
    Ok(Some((c, names)))
}

/// The checkpoint at `version`'s ten (Delta and Pondra both checkpoint every ten versions), if it is above 0 and there.
/// A HEAD tells whether it is: a GET would read a whole checkpoint, and the start is read after.
async fn spaced(reach: &impl Reach, root: &str, version: i64) -> Result<Option<(i64, Vec<String>)>> {
    let at = version - version % 10;
    if at <= 0 {
        return Ok(None);
    }
    let name = format!("{at:020}.checkpoint.parquet");
    Ok(exists(reach, &format!("{root}/_delta_log/{name}")).await?.then_some((at, vec![name])))
}

/// Whether an object is there, by a HEAD (its bytes aren't read).
async fn exists(reach: &impl Reach, url: &str) -> Result<bool> {
    let (store, path) = reach.store(url).await?;
    match store.head(&path).await {
        Ok(_) => Ok(true),
        Err(datafusion::object_store::Error::NotFound { .. }) => Ok(false),
        Err(e) => Err(e).with_context(|| format!("reading {url}")),
    }
}

/// Applies the commits after `log`'s version, a GET each: up to `version`, or on a latest read until one isn't there.
/// A commit a named version needs that isn't there is an error that says why: nothing at all is no table; a version past
/// `newest` is not written yet; any other is a log that lost the commit.
async fn apply_commits(reach: &impl Reach, root: &str, log: &mut Log, version: Option<i64>, newest: i64) -> Result<()> {
    let mut k = log.version + 1;
    while version.is_none_or(|v| k <= v) {
        let Some(body) = crate::reach::found(reach, &format!("{root}/_delta_log/{k:020}.json")).await? else {
            return match version {
                _ if k == 0 && newest < 0 => bail!("{root}: no Delta table there (no _delta_log/)"),
                None => Ok(()),
                Some(v) if k > newest => bail!("{root}: the table has no version {v} yet"),
                Some(_) => bail!("{root}: the log has no commit {k} (its retention removed it?)"),
            };
        };
        for a in lines(&body)? {
            log.apply(&a, false);
        }
        log.version = k;
        k += 1;
    }
    Ok(())
}

/// A checkpoint's actions: its Parquet parts, or a v2 checkpoint's own actions and its sidecars'.
async fn checkpoint(reach: &impl Reach, root: &str, files: &[String]) -> Result<Vec<Value>> {
    let url = |n: &str| format!("{root}/_delta_log/{n}");
    let mut out = match files {
        [one] if one.ends_with(".json") => lines(&crate::reach::get(reach, &url(one)).await?)?,
        _ => parquet_actions(reach, files.iter().map(|n| url(n)).collect()).await?,
    };
    let sidecars: Vec<String> = out.iter().filter_map(|a| a["sidecar"]["path"].as_str()).map(|p| if p.contains("://") { p.to_string() } else { url(&format!("_sidecars/{}", unescape(p))) }).collect();
    if !sidecars.is_empty() {
        out.extend(parquet_actions(reach, sidecars).await?);
    }
    Ok(out)
}

/// Parquet files of actions (checkpoints, sidecars) as JSON actions, as a commit holds them.
async fn parquet_actions(reach: &impl Reach, urls: Vec<String>) -> Result<Vec<Value>> {
    use datafusion::arrow::json as arrow_json;
    let batches = reach.session().read_parquet(urls, Default::default()).await?.collect().await?;
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

/// A Delta type Pondra doesn't read, and those it does.
fn unread(t: &str) -> String {
    format!("a Delta {t}, which Pondra doesn't read: it reads Delta's numbers, strings, binary, booleans, dates, timestamps, decimals, arrays and structs")
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
            other => bail!("{}", unread(other)),
        },
        Value::Object(o) => match o.get("type").and_then(Value::as_str) {
            Some("array") => DataType::List(item(&o["elementType"], "element", o["containsNull"].as_bool().unwrap_or(true))?),
            Some("struct") => DataType::Struct(o["fields"].as_array().into_iter().flatten().map(|f| item(&f["type"], f["name"].as_str().unwrap_or_default(), true)).collect::<Result<Vec<_>>>()?.into()),
            Some(other) => bail!("{}", unread(other)),
            None => bail!("a type without a name"),
        },
        other => bail!("a type written as {other}, which Delta's protocol doesn't have"),
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
                    let u = uuid_text(&crate::scan::z85(id)?)?;
                    let dir = if prefix.is_empty() { String::new() } else { format!("{prefix}/") };
                    Delete::Vector { path: format!("{root}/{dir}deletion_vector_{u}.bin"), offset: at, size }
                }
                "p" => Delete::Vector { path: unescape(where_), offset: at, size },
                k => bail!("{rel}: a deletion vector of storage type '{k}', which Delta's protocol doesn't have (it has 'u', 'i' and 'p')"),
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
    crate::data_file::text(&s)
}

/// A path in the log, as the object it names (the log escapes it as a URI).
pub fn unescape(p: &str) -> String { url::form_urlencoded::parse(format!("x={}", p.replace('+', "%2B")).as_bytes()).next().map(|(_, v)| v.into_owned()).unwrap_or_default() }

/// A deletion vector's id (16 bytes) as its file's name writes it: lower-case hex in 8-4-4-4-12 groups, as a UUID is
/// written. Another length is refused by name, since it isn't a UUID at all.
fn uuid_text(bytes: &[u8]) -> Result<String> {
    ensure!(bytes.len() == 16, "a deletion vector's id of {} bytes, not the 16 of a UUID", bytes.len());
    let hex: String = crate::text::hex(bytes);
    Ok(format!("{}-{}-{}-{}-{}", &hex[..8], &hex[8..12], &hex[12..16], &hex[16..20], &hex[20..]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::error::ArrowError;
    use datafusion::arrow::json::{reader::infer_json_schema_from_iterator, ReaderBuilder};
    use datafusion::datasource::listing::ListingTableUrl;
    use datafusion::object_store::{memory::InMemory, ObjectStore};
    use datafusion::parquet::arrow::ArrowWriter;
    use datafusion::prelude::SessionContext;
    use std::io::Cursor;

    const ROOT: &str = "https://lake.test";
    const PROTOCOL: &str = r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#;
    const METADATA: &str = r#"{"metaData":{"id":"t1","schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":true},{\"name\":\"s\",\"type\":\"string\",\"nullable\":true}]}"}}"#;

    /// A web server over memory: a listing (a URL ending in `/`) is refused, so a read passes only through the
    /// list-less path. Its session reads the same store.
    struct Web {
        ctx: SessionContext,
        store: Arc<InMemory>,
    }

    impl Reach for Web {
        fn store(&self, url: &str) -> impl std::future::Future<Output = Result<(Arc<dyn ObjectStore>, Path)>> + Send {
            std::future::ready(self.locate(url))
        }
        fn session(&self) -> SessionContext { self.ctx.clone() }
    }

    impl Web {
        fn locate(&self, url: &str) -> Result<(Arc<dyn ObjectStore>, Path)> {
            ensure!(!url.ends_with('/'), "{url}: a web server lists nothing");
            let store: Arc<dyn ObjectStore> = self.store.clone();
            Ok((store, ListingTableUrl::parse(url)?.prefix().clone()))
        }
    }

    fn web() -> Web {
        let ctx = SessionContext::new();
        let store = Arc::new(InMemory::new());
        ctx.register_object_store(&url::Url::parse(ROOT).unwrap(), store.clone());
        Web { ctx, store }
    }

    fn add(path: &str) -> String { format!(r#"{{"add":{{"path":"{path}","size":10,"modificationTime":0,"dataChange":true}}}}"#) }

    fn remove(path: &str) -> String { format!(r#"{{"remove":{{"path":"{path}","deletionTimestamp":0,"dataChange":true}}}}"#) }

    /// The actions of the table's three commits: its protocol and schema with `a.parquet`; then `b.parquet`; then
    /// `a.parquet` removed and `c.parquet` added.
    fn commits() -> Vec<Vec<String>> {
        vec![
            vec![PROTOCOL.into(), METADATA.into(), add("a.parquet")],
            vec![add("b.parquet")],
            vec![remove("a.parquet"), add("c.parquet")],
        ]
    }

    /// A commit's file: one action a line.
    fn commit(actions: &[String]) -> Vec<u8> { actions.iter().map(|a| format!("{a}\n")).collect::<String>().into_bytes() }

    /// Puts `body` at `url` in the test's store.
    async fn put(web: &Web, url: &str, body: Vec<u8>) {
        let (store, path) = web.locate(url).unwrap();
        store.put(&path, body.into()).await.unwrap();
    }

    /// Puts the table's three commits under `root`, except the versions in `skip`.
    async fn put_commits(web: &Web, root: &str, skip: &[i64]) {
        for (v, actions) in commits().iter().enumerate().filter(|(v, _)| !skip.contains(&(*v as i64))) {
            put(web, &format!("{root}/_delta_log/{v:020}.json"), commit(actions)).await;
        }
    }

    /// A checkpoint's Parquet file of these actions, written as Delta writes one (a reader gets its rows as JSON).
    fn checkpoint_parquet(actions: &[String]) -> Vec<u8> {
        let values: Vec<Value> = actions.iter().map(|a| serde_json::from_str(a).unwrap()).collect();
        let schema = Arc::new(infer_json_schema_from_iterator(values.iter().map(Ok::<_, ArrowError>)).unwrap());
        let reader = ReaderBuilder::new(schema.clone()).build(Cursor::new(actions.join("\n").into_bytes())).unwrap();
        let mut body = vec![];
        let mut writer = ArrowWriter::try_new(&mut body, schema, None).unwrap();
        for batch in reader {
            writer.write(&batch.unwrap()).unwrap();
        }
        writer.close().unwrap();
        body
    }

    fn paths(found: &Found) -> Vec<String> { found.files.iter().map(|f| f.path.clone()).collect() }

    #[tokio::test]
    async fn the_latest_version_is_read_from_its_commits_without_a_listing() {
        let web = web();
        let root = format!("{ROOT}/t");
        put_commits(&web, &root, &[]).await;
        assert_eq!(replay(&web, &root, None).await.unwrap().version, 2);
        assert_eq!(paths(&resolve(&web, &root, None).await.unwrap()), vec![format!("{root}/b.parquet"), format!("{root}/c.parquet")]);
    }

    #[tokio::test]
    async fn a_named_version_is_read_to_it_and_no_further() {
        let web = web();
        let root = format!("{ROOT}/t0");
        put_commits(&web, &root, &[]).await;
        assert_eq!(paths(&resolve(&web, &root, Some(0)).await.unwrap()), vec![format!("{root}/a.parquet")]);
        let err = replay(&web, &root, Some(5)).await.err().expect("a version the table doesn't have");
        assert!(err.to_string().contains("the table has no version 5 yet"), "{err}");
    }

    #[tokio::test]
    async fn a_checkpoint_the_log_names_stands_for_the_commits_it_holds() {
        let web = web();
        let root = format!("{ROOT}/c");
        // Commits 0 and 1 are only in the checkpoint: the latest is read from it, then commit 2.
        put_commits(&web, &root, &[0, 1]).await;
        let all = commits();
        let through_one: Vec<String> = all[0].iter().chain(&all[1]).cloned().collect();
        put(&web, &format!("{root}/_delta_log/00000000000000000001.checkpoint.parquet"), checkpoint_parquet(&through_one)).await;
        put(&web, &format!("{root}/_delta_log/_last_checkpoint"), br#"{"version":1}"#.to_vec()).await;
        assert_eq!(replay(&web, &root, None).await.unwrap().version, 2);
        assert_eq!(paths(&resolve(&web, &root, None).await.unwrap()), vec![format!("{root}/b.parquet"), format!("{root}/c.parquet")]);
    }

    #[tokio::test]
    async fn a_root_with_no_log_says_there_is_no_table() {
        let web = web();
        let err = replay(&web, &format!("{ROOT}/none"), None).await.err().expect("no table there");
        assert!(err.to_string().contains("no Delta table there"), "{err}");
    }
}
