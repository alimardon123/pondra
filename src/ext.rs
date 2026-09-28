//! Outside the lake (ADR-026): files anywhere — S3 (R2, MinIO), Google Cloud Storage, Azure,
//! HTTPS, and this machine's for the program that started the node — read as tables wherever SQL
//! takes one, with DuckDB's names (`FROM 's3://b/x/*.parquet'`, `read_csv('gs://…', header =>
//! true)`), and spread over the nodes as a table's files are; and the secrets they are read with
//! (`CREATE SECRET`), kept encrypted in the catalog.
//!
//! Where SQL comes in (`routines::expand`), each reference becomes a table named for what it
//! reads (`"ext:<its spec>"`), so every node and every later step sees the same name; `meta`
//! makes that name a table of files — listed, their schema inferred, Parquet footers giving rows
//! and each column's range — which the query engine, the pruning and the spreading treat as any
//! append table's files (with no log).
use crate::store::{json, DataFile, Lake, TableMeta};
use anyhow::{anyhow, bail, ensure, Context, Result};
use aws_lc_rs::aead::{Aad, LessSafeKey, Nonce, UnboundKey, AES_256_GCM};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD as B64, Engine};
use datafusion::datasource::file_format::{csv::CsvFormat, json::JsonFormat, parquet::ParquetFormat, FileFormat};
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::sql::sqlparser::ast::{Expr, FunctionArg, FunctionArgExpr, TableFactor, Value};
use futures::{StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

tokio::task_local! {
    /// The caller is the program that started this node (the shell, `local()`: `server::owner`),
    /// or another node running its share of a query its coordinator checked: this machine's files,
    /// and URLs no secret covers, read with the node's own credentials.
    static OWNER: bool;
    /// The files each table of files in this statement reads, listed once for it (planning asks
    /// several times; each node of a spread query has its coordinator's list). The next statement
    /// lists again: a file changed under its name is read as it is now, never by an old size.
    static LISTED: std::cell::RefCell<HashMap<String, TableMeta>>;
}

/// Run a statement (or a node's share of one) as its caller: `owner` or not, listing each table
/// of files once.
pub async fn scope<F: std::future::Future>(owner: bool, f: F) -> F::Output {
    OWNER.scope(owner, listing(f)).await
}

/// Run `f` listing each table of files once (within a statement already doing so: as it is).
pub async fn listing<F: std::future::Future>(f: F) -> F::Output {
    match LISTED.try_with(|_| ()) {
        Ok(()) => f.await,
        Err(_) => LISTED.scope(Default::default(), f).await,
    }
}

pub(crate) fn owner() -> bool { OWNER.try_with(|o| *o).unwrap_or(false) }

// ---------------------------------------------------------------- what a query reads

/// Files a query reads as one table: where, in which format, with which options.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Spec {
    pub urls: Vec<String>,
    pub format: String, // parquet, csv, json (one object a line)
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub options: BTreeMap<String, String>,
}

const PREFIX: &str = "ext:";

pub fn is(name: &str) -> bool { name.starts_with(PREFIX) }

fn name(spec: &Spec) -> String { format!("{PREFIX}{}", B64.encode(serde_json::to_vec(spec).expect("a spec"))) }

pub fn spec(name: &str) -> Option<Spec> { serde_json::from_slice(&B64.decode(name.strip_prefix(PREFIX)?).ok()?).ok() }

/// Might `sql` name files? (A cheap test, before anything is parsed.)
pub fn mentions(sql: &str) -> bool {
    static FILES: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
        regex::Regex::new(r"(?i)'(s3|r2|gs|gcs|az|azure|abfss?|https?)://|\b(from|join|using)\s+'|\b(read_parquet|parquet_scan|read_csv|read_csv_auto|read_json|read_json_auto|read_ndjson|delta_scan|iceberg_scan)\s*\(").expect("a regex")
    });
    FILES.is_match(sql)
}

/// The table a reference to files is: `'s3://…/x.parquet'` or `'sales/*.csv'` (a URL or a path,
/// its format by extension), or `read_parquet(…)`, `read_csv(…)`, `read_json(…)` with URLs or
/// paths and DuckDB's options by name. None: not files.
pub fn table(t: &TableFactor) -> Result<Option<String>> {
    let TableFactor::Table { name: n, args, .. } = t else { return Ok(None) };
    let Some(args) = args else {
        let [part] = &n.0[..] else { return Ok(None) };
        let Some(id) = part.as_ident().filter(|i| i.quote_style == Some('\'')) else { return Ok(None) }; // (a name in single quotes: files, as DuckDB reads it)
        let (format, options) = format_of(&id.value)?;
        return Ok(Some(name(&Spec { urls: vec![id.value.clone()], format, options })));
    };
    let f = n.to_string().to_lowercase();
    let format = match f.as_str() {
        "read_parquet" | "parquet_scan" => "parquet",
        "read_csv" | "read_csv_auto" => "csv",
        "read_json" | "read_json_auto" | "read_ndjson" => "json",
        "delta_scan" => "delta",
        "iceberg_scan" => "iceberg",
        _ => return Ok(None),
    };
    let (mut urls, mut options) = (vec![], BTreeMap::new());
    for a in &args.args {
        match a {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => urls.extend(strings(e).with_context(|| format!("{f}: files are a string or a list of them"))?),
            FunctionArg::Named { name, arg: FunctionArgExpr::Expr(e), .. } | FunctionArg::ExprNamed { name: Expr::Identifier(name), arg: FunctionArgExpr::Expr(e), .. } => {
                options.insert(name.value.to_lowercase(), literal(e).with_context(|| format!("{f}: {name} is a value"))?);
            }
            _ => bail!("{f}: files first, then options by name ({f}('s3://bucket/path/*.{format}', …))"),
        }
    }
    ensure!(!urls.is_empty(), "{f}: which files? {f}('s3://bucket/path/*.{format}')");
    let known: &[&str] = match format {
        "csv" => &["header", "delim", "sep", "delimiter", "quote", "escape", "hive_partitioning", "union_by_name"],
        "delta" => &["version"],
        "iceberg" => &["version", "snapshot_from_id", "snapshot_from_timestamp", "allow_moved_paths"],
        _ => &["hive_partitioning", "union_by_name"],
    };
    ensure!(!["delta", "iceberg"].contains(&format) || urls.len() == 1, "{f}: one table at a time");
    if let Some(k) = options.keys().find(|k| !known.contains(&k.as_str())) {
        bail!("{f} has no option {k}: {}", known.join(", "));
    }
    Ok(Some(name(&Spec { urls, format: format.into(), options })))
}

fn strings(e: &Expr) -> Option<Vec<String>> {
    match e {
        Expr::Array(a) => a.elem.iter().map(|e| literal(e).ok()).collect(),
        e => Some(vec![literal(e).ok()?]),
    }
}

fn literal(e: &Expr) -> Result<String> {
    match e {
        Expr::Value(v) => match &v.value {
            Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => Ok(s.clone()),
            Value::Number(n, _) => Ok(n.to_string()),
            Value::Boolean(b) => Ok(b.to_string()),
            v => bail!("{v}"),
        },
        Expr::Identifier(i) => Ok(i.value.clone()), // (true, false written bare)
        e => bail!("{e}"),
    }
}

/// A file's format by its extension (a glob's too: `*.parquet`).
pub(crate) fn format_of(url: &str) -> Result<(String, BTreeMap<String, String>)> {
    if url.starts_with("kafka://") {
        return Ok(("kafka".into(), BTreeMap::new())); // (a topic)
    }
    let path = url.split(['?', '#']).next().unwrap_or(url).to_lowercase();
    let tsv = BTreeMap::from([("delim".to_string(), "\t".to_string())]);
    Ok(match path.rsplit('.').next().unwrap_or_default() {
        "parquet" | "pq" => ("parquet".into(), BTreeMap::new()),
        "csv" => ("csv".into(), BTreeMap::new()),
        "tsv" => ("csv".into(), tsv),
        "json" | "ndjson" | "jsonl" => ("json".into(), BTreeMap::new()),
        "gz" | "zst" | "bz2" | "xz" => bail!("{url}: compressed files aren't read yet: Parquet, CSV and JSON as they are"),
        _ => bail!("{url}: which format? name files by their extension (.parquet, .csv, .json), or read them with read_parquet(…), read_csv(…) or read_json(…)"),
    })
}

/// A URL's scheme, if it names one Pondra reads (not a path on this machine).
pub(crate) fn scheme(url: &str) -> Option<&str> {
    let s = url.split_once("://")?.0;
    ["s3", "r2", "gs", "gcs", "az", "azure", "abfs", "abfss", "http", "https", "kafka", "file"].contains(&s).then_some(s)
}

// ---------------------------------------------------------------- the table they make

/// The tables of files named in `sql` (after `routines::expand`).
pub fn names(sql: &str) -> Vec<String> {
    static NAMES: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| regex::Regex::new(r#""(ext:[A-Za-z0-9_-]+)""#).expect("a regex"));
    let mut out: Vec<String> = NAMES.captures_iter(sql).map(|c| c[1].to_string()).collect();
    out.sort();
    out.dedup();
    out
}

/// Text (an error, a plan) with each table of files named as SQL named it: `'s3://…'`, or
/// `read_csv('…', delim => ';')`, not its internal name.
pub fn readable(text: &str) -> String {
    static NAMES: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| regex::Regex::new(r#""?(ext:[A-Za-z0-9_-]+)"?"#).expect("a regex"));
    NAMES.replace_all(text, |c: &regex::Captures| {
        let Some(s) = spec(&c[1]) else { return c[0].to_string() };
        let urls = s.urls.iter().map(|u| format!("'{u}'")).collect::<Vec<_>>();
        let urls = if urls.len() == 1 { urls[0].clone() } else { format!("[{}]", urls.join(", ")) };
        match s.options.is_empty() {
            true if s.urls.len() == 1 => urls,
            _ => format!("read_{}({urls}{})", s.format, s.options.iter().map(|(k, v)| format!(", {k} => '{v}'")).collect::<String>()),
        }
    }).into_owned()
}

/// An EXPLAIN's rows, its tables of files named as SQL named them (`readable`).
pub fn readable_rows(batches: Vec<datafusion::arrow::record_batch::RecordBatch>) -> Result<Vec<datafusion::arrow::record_batch::RecordBatch>> {
    use datafusion::arrow::array::{ArrayRef, AsArray, StringArray};
    batches.into_iter().map(|b| {
        let columns = b.columns().iter().map(|c| match c.data_type() {
            datafusion::arrow::datatypes::DataType::Utf8 => Arc::new(c.as_string::<i32>().iter().map(|v| v.map(readable)).collect::<StringArray>()) as ArrayRef,
            _ => c.clone(),
        });
        Ok(datafusion::arrow::record_batch::RecordBatch::try_new(b.schema(), columns.collect())?)
    }).collect()
}

/// Does this table read files on the node's machine? (Such a query stays on it.)
pub fn local(table: &str) -> bool { spec(table).is_some_and(|s| s.urls.iter().any(|u| scheme(u).is_none_or(|s| s == "file"))) }

/// A table by name: this lake's, or the files an `ext:` name reads (checked: see `check`).
pub async fn meta(lake: &Lake, table: &str) -> Result<Option<TableMeta>> {
    if !is(table) {
        return lake.cat.get::<TableMeta>(&crate::store::table_key(table)).await;
    }
    let spec = spec(table).context("not a table of files")?;
    check(lake, &spec).await?;
    if let Ok(Some(m)) = LISTED.try_with(|l| l.borrow().get(table).cloned()) {
        return Ok(Some(m));
    }
    let m = resolve(lake, &spec).await?;
    keep(table, &m);
    Ok(Some(m))
}

fn keep(table: &str, m: &TableMeta) { _ = LISTED.try_with(|l| l.borrow_mut().insert(table.to_string(), m.clone())); }

/// The coordinator's list of the tables of files among `tables`, for its slices.
pub async fn of(lake: &Lake, tables: &[String]) -> Result<Vec<(String, TableMeta)>> {
    let (mut out, files) = (vec![], tables.iter().filter(|t| is(t)).cloned().collect::<Vec<_>>());
    for t in files {
        let m = meta(lake, &t).await?.expect("files");
        out.push((t, m));
    }
    Ok(out)
}

/// A node's share of a spread query: its coordinator's list, and the stores to read them through.
pub async fn prime(lake: &Lake, tables: &[(String, TableMeta)]) -> Result<()> {
    for (t, m) in tables {
        for u in m.ext.as_ref().map(|s| s.urls.clone()).unwrap_or_default() {
            OWNER.scope(true, register(lake, &u)).await?;
        }
        keep(t, m);
    }
    Ok(())
}

/// May this caller read these files? A path on this machine: only the program that started the
/// node. A URL: with the secret whose scope covers it (an admin made it: the grant), or, if none
/// does, only that program again, with the node's own credentials.
pub async fn check(lake: &Lake, spec: &Spec) -> Result<()> {
    if owner() {
        return Ok(());
    }
    let secrets = list(lake).await?;
    spec.urls.iter().try_for_each(|u| allowed(&secrets, u))
}

fn allowed(secrets: &[(String, Secret)], u: &str) -> Result<()> {
    match scheme(u).filter(|s| *s != "file") {
        None => bail!("{u} is a file on the node's machine: only the program that started the node (the shell, local()) reads those"),
        Some(_) => ensure!(covering(secrets, u).is_some(), "{}", uncovered(u)),
    }
    Ok(())
}

/// Another engine's table names its files wherever it likes (Iceberg's by their whole URL):
/// each must be one this caller may read, as the table is.
async fn check_files(lake: &Lake, m: &TableMeta) -> Result<()> {
    if owner() {
        return Ok(());
    }
    let secrets = list(lake).await?;
    let mut folders: Vec<&str> = m.files.iter().flat_map(|f| {
        let deletes = f.outside.iter().flat_map(|o| o.deletes.iter()).filter_map(|d| match d {
            crate::scan::Delete::Vector { path, .. } | crate::scan::Delete::Blob { path, .. } | crate::scan::Delete::Positions { path, .. } | crate::scan::Delete::Equality { path, .. } => Some(path.as_str()),
            crate::scan::Delete::Inline { .. } => None,
        });
        std::iter::once(f.path.as_str()).chain(deletes)
    }).map(|p| &p[..p.rfind('/').unwrap_or(p.len())]).collect();
    folders.sort_unstable();
    folders.dedup();
    folders.into_iter().try_for_each(|d| allowed(&secrets, &format!("{d}/")))
}

/// What to do about a URL no secret covers: the statement that would, for its store.
pub(crate) fn uncovered(u: &str) -> String {
    let (kind, keys) = match scheme(u) {
        Some("gs" | "gcs") => ("gcs", "SERVICE_ACCOUNT_KEY '…'"),
        Some("az" | "azure" | "abfs" | "abfss") => ("azure", "CONNECTION_STRING '…'"),
        Some("http" | "https") => ("http", "BEARER_TOKEN '…'"),
        Some("kafka") => ("kafka", "SECURITY_PROTOCOL 'SASL_SSL', USERNAME '…', PASSWORD '…'"),
        _ => ("s3", "KEY_ID '…', SECRET '…'"),
    };
    format!("no secret covers {u}: an admin makes one (CREATE SECRET name (TYPE {kind}, {keys}, SCOPE '{}'))", root(u).unwrap_or_default())
}

/// The files, their schema, and (Parquet) each file's rows and column ranges from its footer.
async fn resolve(lake: &Lake, spec: &Spec) -> Result<TableMeta> {
    let version = |k: &str| spec.options.get(k).map(|v| v.parse::<i64>().with_context(|| format!("{k} is a number, not {v}"))).transpose();
    match spec.format.as_str() {
        "kafka" => return Ok(TableMeta { ext: Some(spec.clone()), ..crate::kafka_client::resolve(lake, &spec.urls[0]).await? }),
        "delta" | "iceberg" => {
            let m = match spec.format.as_str() {
                "delta" => crate::read_delta::resolve(lake, &spec.urls[0], version("version")?).await?,
                _ => crate::read_iceberg::resolve(lake, &spec.urls[0], &spec.options).await?,
            };
            check_files(lake, &m).await?;
            return Ok(TableMeta { ext: Some(spec.clone()), ..m });
        }
        _ => {}
    }
    let ctx = lake.session();
    let format = file_format(spec)?;
    let (mut objects, mut urls) = (vec![], vec![]);
    let deep = spec.urls.iter().any(|u| u.contains("**") || glob_at(u).is_some_and(|i| u[i..].contains('/')));
    ctx.state_ref().write().config_mut().options_mut().execution.listing_table_ignore_subdirectory = !deep; // (`**`, `*/x.parquet`: into folders)
    let state = ctx.state();
    for u in &spec.urls {
        let url = listing_url(u)?;
        register(lake, u).await?;
        let store = lake.rt.object_store(&url)?;
        let wanted = if u.ends_with('/') { format!(".{}", spec.format) } else { String::new() }; // (a folder: its files of that format)
        let found: Vec<_> = url.list_all_files(&state, store.as_ref(), &wanted).await?.try_collect().await?;
        ensure!(!found.is_empty(), "no files at {u}");
        objects.extend(found.into_iter().map(|o| (store.clone(), url.clone(), o)));
        urls.push(url);
    }
    objects.sort_by(|a, b| a.2.location.cmp(&b.2.location)); // (every node lists them in one order)
    let (store, first) = (objects[0].0.clone(), objects.iter().take(if spec.format == "parquet" { usize::MAX } else { 16 }).map(|o| o.2.clone()).collect::<Vec<_>>());
    let schema = format.infer_schema(&state, &store, &first).await.with_context(|| format!("reading {}", spec.urls.join(", ")))?;
    let mut columns: Vec<(String, String)> = schema.fields().iter().map(|f| (f.name().clone(), crate::query::type_name(f.data_type()))).collect();
    // Hive-style folders (`day=2026-09-28/`): each a column, the same in every file's path, typed
    // by what all its values are (as Spark, DuckDB and Polars do).
    let hive = |url: &ListingTableUrl, o: &object_store_df::ObjectMeta| -> Vec<(String, String)> {
        let rest = o.location.as_ref().strip_prefix(url.prefix().as_ref()).unwrap_or_default();
        rest.split('/').filter_map(|s| s.split_once('=')).map(|(k, v)| (k.to_string(), v.to_string())).collect()
    };
    // Found unless turned off (DuckDB's and Spark's default); when asked for, every file has them.
    let asked = spec.options.get("hive_partitioning").map(|v| v != "false" && v != "0");
    let found: Vec<Vec<String>> = if asked == Some(false) { vec![] } else { objects.iter().map(|(_, u, o)| hive(u, o).into_iter().map(|(k, _)| k).collect()).collect() };
    let same = found.windows(2).all(|w| w[0] == w[1]);
    ensure!(same || asked != Some(true), "{}: files in other folders than {} (hive_partitioning)", spec.urls.join(", "), found[0].join("/"));
    let keys = if same { found.first().cloned().unwrap_or_default() } else { vec![] };
    let hived = !keys.is_empty();
    for (i, k) in keys.iter().enumerate() {
        ensure!(!columns.iter().any(|(c, _)| c == k), "{k} is a folder's key and a column in the files");
        let values: Vec<String> = objects.iter().filter_map(|(_, url, o)| hive_value(hive(url, o).get(i)?.1.as_str())).collect();
        columns.push((k.clone(), hive_type(&values).into()));
    }
    let parquet = spec.format == "parquet";
    let stats: Vec<_> = objects.into_iter().map(|(store, url, o)| {
        let (format, schema, state) = (format.clone(), schema.clone(), state.clone());
        let part = if hived { hive(&url, &o) } else { vec![] };
        async move {
            let root = url.object_store();
            let full = format!("{}{}", root.as_str().strip_suffix('/').unwrap_or(root.as_str()), url_path(&o)); // (`file:///x`, `s3://b/x`)
            let s = if parquet { Some(format.infer_stats(&state, &store, schema.clone(), &o).await?) } else { None };
            let mut f = file(full, o.size, s.as_ref(), &schema);
            f.part = part.iter().map(|(k, v)| format!("{k}={v}")).collect::<Vec<_>>().join("/");
            if parquet && hived {
                let values = part.iter().map(|(k, v)| (k.clone(), hive_value(v))).collect();
                f.outside = Some(Box::new(crate::scan::Outside { values, ..Default::default() })); // (its folders' values, as `scan::read` takes them)
            }
            for (k, v) in part {
                match hive_value(&v) {
                    Some(v) => _ = f.stats.insert(k, (v.clone(), v)),
                    None => f.nulls.iter_mut().for_each(|n| n.push(k.clone())), // (a NULL's folder: `IS NULL` finds it)
                }
            }
            anyhow::Ok(f)
        }
    }).collect();
    let files = futures::stream::iter(stats).buffered(32).try_collect().await?;
    Ok(TableMeta { columns, files, ext: Some(spec.clone()), ..Default::default() })
}

/// A NULL's folder, as Hive, Spark, pyarrow and Polars name it.
pub const NULL_FOLDER: &str = "__HIVE_DEFAULT_PARTITION__";

/// A folder's value as written (percent-escaped where a path can't hold a character), or None
/// for a NULL's folder.
fn hive_value(v: &str) -> Option<String> {
    if v == NULL_FOLDER {
        return None;
    }
    let (b, mut out, mut i) = (v.as_bytes(), Vec::with_capacity(v.len()), 0);
    while i < b.len() {
        match (b[i], v.get(i + 1..i + 3).and_then(|h| u8::from_str_radix(h, 16).ok())) {
            (b'%', Some(c)) => (out.push(c), i += 3),
            (c, _) => (out.push(c), i += 1),
        };
    }
    Some(String::from_utf8_lossy(&out).into_owned())
}

/// A folder key's type: whole numbers, numbers, dates, or text.
fn hive_type(values: &[String]) -> &'static str {
    let all = |ok: fn(&str) -> bool| !values.is_empty() && values.iter().all(|v| ok(v));
    if all(|v| v.parse::<i64>().is_ok()) {
        "Int64"
    } else if all(|v| v.parse::<f64>().is_ok_and(f64::is_finite)) {
        "Float64"
    } else if all(|v| v.len() == 10 && chrono::NaiveDate::parse_from_str(v, "%Y-%m-%d").is_ok()) {
        "Date32"
    } else {
        "Utf8"
    }
}

/// Where a URL's glob starts (`*`, `[`; `?` too, but not in a web address: its query).
fn glob_at(u: &str) -> Option<usize> {
    let web = u.starts_with("http");
    u.char_indices().find(|(_, c)| *c == '*' || *c == '[' || (*c == '?' && !web)).map(|(i, _)| i)
}

/// A URL as DataFusion lists it, its glob split off (DataFusion splits only paths' globs).
fn listing_url(u: &str) -> Result<ListingTableUrl> {
    let Some(at) = glob_at(u).filter(|_| scheme(u).is_some_and(|s| s != "file")) else { return Ok(ListingTableUrl::parse(u)?) };
    let cut = u[..at].rfind('/').map_or(0, |j| j + 1);
    let (prefix, glob) = u.split_at(cut);
    Ok(ListingTableUrl::try_new(url::Url::parse(prefix)?, Some(glob::Pattern::new(glob)?))?)
}

fn url_path(o: &object_store_df::ObjectMeta) -> String { format!("/{}", o.location) }

/// A file as a table's `DataFile`: its rows and exact column ranges when its footer says.
fn file(path: String, bytes: u64, s: Option<&datafusion::common::Statistics>, schema: &datafusion::arrow::datatypes::Schema) -> DataFile {
    use datafusion::common::stats::Precision::Exact;
    let Some(s) = s else { return DataFile { path, bytes, rows: bytes / 100, ..Default::default() } }; // (CSV, JSON: rows unknown; about 100 bytes each)
    let (mut stats, mut nulls) = (BTreeMap::new(), Some(vec![]));
    for (f, c) in schema.fields().iter().zip(&s.column_statistics) {
        if let (Exact(lo), Exact(hi), false) = (&c.min_value, &c.max_value, f.data_type().is_floating()) {
            if let (Some(lo), Some(hi)) = (crate::manifest::text(lo), crate::manifest::text(hi)) {
                stats.insert(f.name().clone(), (lo, hi));
            }
        }
        match (&c.null_count, nulls.as_mut()) {
            (Exact(0), _) => {}
            (Exact(_), Some(n)) => n.push(f.name().clone()),
            _ => nulls = None,
        }
    }
    let rows = match s.num_rows { Exact(n) => n as u64, _ => bytes / 100 };
    DataFile { path, bytes, rows, stats, nulls, ..Default::default() }
}

fn file_format(spec: &Spec) -> Result<Arc<dyn FileFormat>> {
    let o = |k: &str| spec.options.get(k).map(String::as_str);
    let byte = |k: &str, v: &str| -> Result<u8> { v.bytes().next().filter(|_| v.len() == 1).with_context(|| format!("{k} is one character, not {v:?}")) };
    Ok(match spec.format.as_str() {
        "parquet" => Arc::new(ParquetFormat::default()),
        "json" => Arc::new(JsonFormat::default()),
        "csv" => {
            let mut f = CsvFormat::default().with_has_header(o("header").is_none_or(|h| h != "false"));
            if let Some(d) = o("delim").or(o("sep")).or(o("delimiter")) {
                f = f.with_delimiter(byte("delim", d)?);
            }
            if let Some(q) = o("quote") {
                f = f.with_quote(byte("quote", q)?);
            }
            if let Some(e) = o("escape") {
                f = f.with_escape(Some(byte("escape", e)?));
            }
            Arc::new(f)
        }
        f => bail!("files as {f}: parquet, csv or json"),
    })
}

/// The rows of `files` of an `ext:` table, as `schema`: DataFusion's own readers, without the hot
/// columns (outside the lake, a file may change under the same name).
pub async fn read(lake: &Lake, ctx: &datafusion::prelude::SessionContext, files: &[&DataFile], schema: &datafusion::arrow::datatypes::SchemaRef, spec: &Spec, table: Option<&crate::scan::Table>) -> Result<datafusion::prelude::DataFrame> {
    if ["delta", "iceberg"].contains(&spec.format.as_str()) {
        return crate::scan::read(ctx, files, schema, table).await; // (another engine's table: its partition values, its deletes)
    }
    if spec.format == "kafka" {
        return crate::kafka_client::read(lake, ctx, files, schema).await; // (a topic)
    }
    if spec.format == "parquet" {
        return crate::scan::read(ctx, files, schema, None).await; // (as listed: no second look at each file before it is read)
    }
    use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig};
    use datafusion::prelude::{cast, ident, lit};
    let folders = |f: &DataFile| f.part.split('/').filter_map(|s| s.split_once('=')).map(|(k, v)| (k.to_string(), v.to_string())).collect::<Vec<_>>();
    let keys: Vec<String> = files.first().map(|f| folders(f).into_iter().map(|(k, _)| k).collect()).unwrap_or_default();
    let fields: Vec<_> = schema.fields().iter().filter(|f| !keys.contains(f.name())).cloned().collect();
    let stored = crate::query::schema(&fields.iter().map(|f| (f.name().clone(), crate::query::type_name(f.data_type()))).collect::<Vec<_>>())?; // (as the files hold it)
    let in_files = if spec.format == "parquet" { Arc::new(datafusion::arrow::datatypes::Schema::new(fields)) } else { stored };
    let mut by_folder: BTreeMap<&str, Vec<&DataFile>> = BTreeMap::new();
    files.iter().for_each(|f| by_folder.entry(f.part.as_str()).or_default().push(f));
    let mut out: Option<datafusion::prelude::DataFrame> = None;
    for group in by_folder.into_values() {
        let urls = group.iter().map(|f| ListingTableUrl::parse(&f.path)).collect::<datafusion::error::Result<Vec<_>>>()?;
        let options = ListingOptions::new(file_format(spec)?).with_file_extension("");
        let table = ListingTable::try_new(ListingTableConfig::new_with_multi_paths(urls).with_listing_options(options).with_schema(in_files.clone()))?;
        let mut df = ctx.read_table(Arc::new(table))?;
        for (k, v) in folders(group[0]) {
            df = df.with_column(&k, lit(datafusion::common::ScalarValue::Utf8(hive_value(&v))))?; // (a folder's value, for each of its rows)
        }
        let typed = schema.fields().iter().map(|f| cast(ident(f.name()), f.data_type().clone()).alias(f.name())).collect::<Vec<_>>();
        let df = df.select(typed)?;
        out = Some(match out {
            Some(o) => o.union(df)?,
            None => df,
        });
    }
    out.context("no files")
}

// ---------------------------------------------------------------- where they are

/// A URL's store root: scheme and bucket (or container, or host).
fn root(url: &str) -> Option<String> {
    let u = url::Url::parse(url).ok()?;
    Some(format!("{}://{}", u.scheme(), &u[url::Position::BeforeUsername..url::Position::AfterPort]))
}

/// DataFusion reads `url` through a store for its bucket, made with the secret covering it (or,
/// for the node's owner, the node's own credentials). The lake's own bucket keeps its store.
pub(crate) async fn register(lake: &Lake, url: &str) -> Result<()> {
    let Some(root) = root(url).filter(|_| scheme(url).is_some_and(|s| s != "file" && s != "kafka")) else { return Ok(()) }; // (this machine's: DataFusion's own; a topic: a Kafka client's)
    let lake_root = self::root(&lake.url);
    let attached: Vec<Option<String>> = lake.attached.read().unwrap().iter().map(|(_, o)| self::root(&o.url)).collect();
    if lake_root.as_deref() == Some(root.as_str()) || attached.iter().any(|a| a.as_deref() == Some(root.as_str())) {
        return Ok(()); // (a lake's bucket: its store, the SSD tier's)
    }
    let secrets = list(lake).await?;
    let secret = covering(&secrets, url);
    let made_with = secret.as_ref().map_or("the node's own".to_string(), |(n, s)| format!("{n}:{}", s.sealed));
    static MADE: Mutex<Option<HashMap<String, String>>> = Mutex::new(None);
    if MADE.lock().unwrap().get_or_insert_default().get(&root) == Some(&made_with) {
        return Ok(());
    }
    let params = secret.as_ref().map(|(n, s)| open(n, s)).transpose()?;
    let store = build(&url::Url::parse(&root)?, params.as_ref())?;
    lake.rt.register_object_store(&url::Url::parse(&root)?, Arc::new(crate::cache::CachedStore::files(store))); // (a file's ranges kept by its version)
    MADE.lock().unwrap().get_or_insert_default().insert(root, made_with);
    Ok(())
}

/// The store `url` is read through (made as `register` makes it), and its path there.
pub async fn store(lake: &Lake, url: &str) -> Result<(Arc<dyn object_store_df::ObjectStore>, object_store_df::path::Path)> {
    register(lake, url).await?;
    let u = ListingTableUrl::parse(url)?;
    Ok((lake.rt.object_store(&u)?, u.prefix().clone()))
}

/// An object's bytes (a log's commit, a manifest), through its store.
pub async fn get(lake: &Lake, url: &str) -> Result<bytes::Bytes> {
    let (store, path) = store(lake, url).await?;
    Ok(object_store_df::ObjectStoreExt::get(&store, &path).await.with_context(|| format!("reading {url}"))?.bytes().await?)
}

/// A store for a bucket, container or host, with a secret's settings or the environment's.
fn build(url: &url::Url, p: Option<&BTreeMap<String, String>>) -> Result<Arc<dyn object_store::ObjectStore>> {
    use object_store::{aws::AmazonS3Builder, azure::MicrosoftAzureBuilder, gcp::GoogleCloudStorageBuilder, http::HttpBuilder};
    let get = |k: &str| p.and_then(|p| p.get(k)).cloned();
    let chain = p.is_none() || get("provider").as_deref() == Some("credential_chain");
    let bucket = url.host_str().context("no bucket in the URL")?.to_string();
    let s3 = |endpoint: Option<String>| -> Result<Arc<dyn object_store::ObjectStore>> {
        let mut b = if chain { AmazonS3Builder::from_env() } else { AmazonS3Builder::new().with_region(get("region").unwrap_or_else(|| "us-east-1".into())) };
        b = b.with_bucket_name(&bucket);
        if let Some(e) = endpoint.or_else(|| get("endpoint")) {
            let e = if e.contains("://") { e } else if get("use_ssl").as_deref() == Some("false") { format!("http://{e}") } else { format!("https://{e}") };
            b = b.with_allow_http(e.starts_with("http://")).with_endpoint(e);
        }
        if let Some(r) = get("region") {
            b = b.with_region(r);
        }
        match (get("key_id"), get("secret")) {
            (Some(k), Some(s)) => b = b.with_access_key_id(k).with_secret_access_key(s),
            _ if !chain => b = b.with_skip_signature(true), // (a secret without keys: a public bucket)
            _ => {}
        }
        if let Some(t) = get("session_token") {
            b = b.with_token(t);
        }
        Ok(Arc::new(b.with_virtual_hosted_style_request(get("url_style").as_deref() == Some("vhost")).build()?))
    };
    Ok(match url.scheme() {
        "s3" => s3(None)?,
        "r2" => s3(get("account_id").map(|a| format!("https://{a}.r2.cloudflarestorage.com")))?,
        "gs" | "gcs" if get("key_id").is_some() => s3(Some("https://storage.googleapis.com".into()))?, // (HMAC keys: GCS's S3 interface)
        "gs" | "gcs" => {
            use object_store::gcp::GoogleConfigKey as K;
            let mut b = if chain { GoogleCloudStorageBuilder::from_env() } else { GoogleCloudStorageBuilder::new() }.with_bucket_name(&bucket);
            match get("service_account_key") {
                Some(k) => b = b.with_service_account_key(k),
                None if !chain => b = b.with_skip_signature(true), // (a secret without a key: a public bucket)
                None => {}
            }
            if let Some(e) = get("endpoint") {
                b = b.with_config(K::Client(object_store::ClientConfigKey::AllowHttp), e.starts_with("http://").to_string()).with_base_url(&e);
            }
            Arc::new(b.build()?)
        }
        "az" | "azure" | "abfs" | "abfss" => {
            use object_store::azure::AzureConfigKey as K;
            let mut b = if chain { MicrosoftAzureBuilder::from_env() } else { MicrosoftAzureBuilder::new() }.with_url(url.as_str());
            let conn: BTreeMap<String, String> = get("connection_string").unwrap_or_default().split(';').filter_map(|kv| kv.split_once('=')).map(|(k, v)| (k.to_lowercase(), v.to_string())).collect();
            let endpoint = get("endpoint").or(conn.get("blobendpoint").cloned());
            let http = endpoint.as_ref().map(|e| e.starts_with("http://").to_string());
            for (key, found) in [(K::AccountName, get("account_name").or(conn.get("accountname").cloned())), (K::AccessKey, get("account_key").or(conn.get("accountkey").cloned())),
                                 (K::SasKey, get("sas_token")), (K::AuthorityId, get("tenant_id")), (K::ClientId, get("client_id")), (K::ClientSecret, get("client_secret")),
                                 (K::Endpoint, endpoint), (K::Client(object_store::ClientConfigKey::AllowHttp), http), (K::UseEmulator, get("use_emulator"))] {
                if let Some(v) = found {
                    b = b.with_config(key, v);
                }
            }
            Arc::new(b.build()?)
        }
        "http" | "https" => {
            let mut options = object_store::ClientOptions::new().with_allow_http(url.scheme() == "http");
            if let Some(t) = get("bearer_token") {
                let mut h = object_store::HeaderMap::new();
                h.insert("authorization", format!("Bearer {t}").parse()?);
                options = options.with_default_headers(h);
            }
            Arc::new(HttpBuilder::new().with_url(url.as_str()).with_client_options(options).build()?)
        }
        s => bail!("{s}:// isn't read (s3, r2, gs, az, abfss, https)"),
    })
}

// ---------------------------------------------------------------- other engines' catalogs

/// Another engine's tables, attached (`ATTACH 'url' AS name (TYPE delta | iceberg, …)`): one
/// table read as `name`, a folder of them as `name.table` and `name.folder.table`, or an Iceberg
/// REST catalog's as `name.namespace.table`. Kept in the catalog (`o/`): every node reads them.
#[derive(Serialize, Deserialize, Clone)]
pub struct Attached {
    pub kind: String,
    pub url: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub options: BTreeMap<String, String>,
}

fn attached_key(n: &str) -> String { format!("o/{n}") }

pub async fn attached(lake: &Lake) -> Result<Vec<(String, Attached)>> {
    Ok(lake.cat.scan::<Attached>("o/", "o0").await?.into_iter().map(|(k, a)| (k[2..].to_string(), a)).collect())
}

/// Is this attached as another engine's tables?
pub async fn is_attached(lake: &Lake, name: &str) -> Result<bool> { Ok(lake.cat.get::<Attached>(&attached_key(name)).await?.is_some()) }

/// The table of files a name of an attached catalog is (None: not one of theirs).
pub fn attached_table(all: &[(String, Attached)], parts: &[String]) -> Result<Option<String>> {
    let Some((_, a)) = all.iter().find(|(n, _)| *n == parts[0]) else { return Ok(None) };
    let rest = &parts[1..];
    if a.kind == "kafka" {
        let [topic] = rest else { bail!("{}: a Kafka cluster's topics are {0}.topic", parts[0]) };
        return Ok(Some(name(&Spec { urls: vec![format!("{}/{topic}", a.url.trim_end_matches('/'))], format: "kafka".into(), options: BTreeMap::new() })));
    }
    let endpoint = a.options.get("endpoint").cloned().or_else(|| a.url.starts_with("http").then(|| a.url.clone()));
    let spec = match endpoint {
        Some(endpoint) => {
            let (ns, t) = match rest {
                [t] => ("default", t),
                [ns, t] => (ns.as_str(), t),
                _ => bail!("{}: a REST catalog's tables are {0}.namespace.table", parts[0]),
            };
            let mut options = BTreeMap::from([("namespace".to_string(), ns.to_string()), ("table".to_string(), t.clone())]);
            if a.options.contains_key("endpoint") {
                options.insert("warehouse".into(), a.url.clone());
            }
            Spec { urls: vec![endpoint.trim_end_matches('/').to_string()], format: "iceberg".into(), options }
        }
        None => Spec { urls: vec![std::iter::once(a.url.trim_end_matches('/')).chain(rest.iter().map(String::as_str)).collect::<Vec<_>>().join("/")], format: a.kind.clone(), options: BTreeMap::new() },
    };
    Ok(Some(name(&spec)))
}

/// The table of files a statement's target is, if it names an attached catalog's table.
pub async fn outside_target(lake: &Lake, name: &str) -> Result<Option<String>> {
    let all = attached(lake).await?;
    if all.is_empty() {
        return Ok(None);
    }
    attached_table(&all, &name.split('.').map(str::to_string).collect::<Vec<_>>())
}

/// Leader: keep an attachment of another engine's tables.
pub async fn attach(lake: &Lake, name: &str, url: &str, kind: &str, options: BTreeMap<String, String>) -> Result<serde_json::Value> {
    crate::ddl::check(name)?;
    ensure!(["delta", "iceberg", "kafka"].contains(&kind), "ATTACH … (TYPE {kind}): delta, iceberg or kafka (or a Pondra lake, with no TYPE)");
    ensure!((kind == "kafka") == url.starts_with("kafka://"), "ATTACH … (TYPE kafka) takes a kafka://brokers URL, and only it does");
    let known = ["endpoint", "secret", "read_only"];
    if let Some(k) = options.keys().find(|k| !known.contains(&k.as_str())) {
        bail!("ATTACH … (TYPE {kind}) has no option {k}: {}", known.join(", "));
    }
    ensure!(kind == "iceberg" || !options.contains_key("endpoint"), "ENDPOINT is an Iceberg REST catalog's");
    ensure!(!lake.attached.read().unwrap().iter().any(|(n, _)| n == name) && name != crate::ddl::lake_name(lake), "{name} is a lake's name here: attach under another name");
    // A REST catalog is reached with the secret whose scope covers it (never one named anywhere else).
    if let Some(secret) = options.get("secret") {
        let at = options.get("endpoint").unwrap_or(&url.to_string()).clone();
        let found = list(lake).await?.into_iter().find(|(n, _)| n == secret).with_context(|| format!("no secret {secret}"))?;
        ensure!(covering(&[found.clone()], &at).is_some(), "secret {secret}'s SCOPE doesn't cover {at} (CREATE OR REPLACE SECRET {secret} (…, SCOPE '{at}'))");
    }
    let a = Attached { kind: kind.into(), url: url.trim_end_matches('/').into(), options };
    if let Some(had) = lake.cat.get::<Attached>(&attached_key(name)).await? {
        ensure!(had.url == a.url && had.kind == a.kind, "{name} is attached already, to {}", had.url);
        return Ok(serde_json::json!({"attached": name, "unchanged": true}));
    }
    lake.cat.commit(vec![(attached_key(name), json(&a))], &[]).await?;
    Ok(serde_json::json!({"attached": name, "type": kind, "url": a.url}))
}

/// Leader: DETACH one (false: not attached this way).
pub async fn detach(lake: &Lake, name: &str) -> Result<bool> {
    if lake.cat.get::<Attached>(&attached_key(name)).await?.is_none() {
        return Ok(false);
    }
    lake.cat.commit(vec![], &[attached_key(name)]).await?;
    Ok(true)
}

/// The settings of the secret covering `url`, for a client of the service it names (Kafka).
pub async fn secret_for(lake: &Lake, url: &str) -> Result<Option<BTreeMap<String, String>>> {
    covering(&list(lake).await?, url).map(|(n, s)| open(&n, &s)).transpose()
}

/// A client for other engines' services (REST catalogs): Pondra's own tokens never go there.
pub fn web() -> &'static reqwest::Client {
    static CLIENT: std::sync::OnceLock<reqwest::Client> = std::sync::OnceLock::new();
    CLIENT.get_or_init(|| reqwest::Client::builder().timeout(std::time::Duration::from_secs(60)).build().expect("an HTTP client"))
}

/// The bearer token for a REST catalog, from the secret covering it: its TOKEN, or one its
/// CLIENT_ID and CLIENT_SECRET get (OAuth's client credentials), kept until it expires.
pub async fn rest_token(lake: &Lake, url: &str) -> Result<Option<String>> {
    static TOKENS: Mutex<Option<HashMap<String, (String, std::time::Instant)>>> = Mutex::new(None);
    let Some((name, secret)) = covering(&list(lake).await?, url).filter(|(_, s)| s.kind == "iceberg") else { return Ok(None) };
    let p = open(&name, &secret)?;
    if let Some(t) = p.get("token") {
        return Ok(Some(t.clone()));
    }
    let (Some(id), Some(pass)) = (p.get("client_id"), p.get("client_secret")) else { return Ok(None) };
    let key = format!("{name}:{}", secret.sealed);
    if let Some((t, until)) = TOKENS.lock().unwrap().get_or_insert_default().get(&key) {
        if std::time::Instant::now() < *until {
            return Ok(Some(t.clone()));
        }
    }
    let at = p.get("oauth2_server_uri").cloned().unwrap_or_else(|| format!("{url}/v1/oauth/tokens"));
    let form = [("grant_type", "client_credentials"), ("client_id", id), ("client_secret", pass), ("scope", p.get("oauth2_scope").map_or("catalog", String::as_str))];
    let body = url::form_urlencoded::Serializer::new(String::new()).extend_pairs(form).finish();
    let r = web().post(&at).header("content-type", "application/x-www-form-urlencoded").body(body).send().await.with_context(|| format!("asking {at} for a token"))?;
    let status = r.status();
    let body: serde_json::Value = r.json().await.unwrap_or_default();
    ensure!(status.is_success(), "{at} gave no token ({status}): {}", body["error_description"].as_str().or(body["error"].as_str()).unwrap_or(""));
    let t = body["access_token"].as_str().context("a token answer without access_token")?.to_string();
    let life = body["expires_in"].as_u64().unwrap_or(3600).saturating_sub(60);
    TOKENS.lock().unwrap().get_or_insert_default().insert(key, (t.clone(), std::time::Instant::now() + std::time::Duration::from_secs(life)));
    Ok(Some(t))
}

// ---------------------------------------------------------------- secrets

/// A secret as the catalog keeps it: its type and scope in the clear, its values sealed.
#[derive(Serialize, Deserialize, Clone)]
pub struct Secret {
    pub kind: String,
    pub scope: Option<String>,
    pub sealed: String, // base64 (nonce, AES-256-GCM ciphertext) of its values, as JSON
}

fn secret_key(name: &str) -> String { format!("e/{name}") }

/// Each type's settings (DuckDB's names), and the URL schemes it serves.
fn kind(t: &str) -> Result<(&'static [&'static str], &'static [&'static str])> {
    Ok(match t {
        "s3" | "r2" => (&["key_id", "secret", "region", "session_token", "endpoint", "url_style", "use_ssl", "account_id", "provider", "scope"], &["s3", "r2"]),
        "gcs" => (&["key_id", "secret", "service_account_key", "endpoint", "provider", "scope"], &["gs", "gcs"]),
        "azure" => (&["connection_string", "account_name", "account_key", "sas_token", "tenant_id", "client_id", "client_secret", "endpoint", "use_emulator", "provider", "scope"], &["az", "azure", "abfs", "abfss"]),
        "http" => (&["bearer_token", "scope"], &["http", "https"]),
        "iceberg" => (&["token", "client_id", "client_secret", "oauth2_server_uri", "oauth2_scope", "scope"], &["http", "https"]), // (a REST catalog)
        "kafka" => (&["security_protocol", "sasl_mechanism", "username", "password", "scope"], &["kafka"]),
        "generic" => (&[], &[]), // (any settings: for procedures, `pondra.secret(name)`)
        t => bail!("secrets of TYPE {t}: s3, r2, gcs, azure, http, iceberg, kafka or generic"),
    })
}

/// `CREATE [OR REPLACE] [PERSISTENT] SECRET [IF NOT EXISTS] name (TYPE t, KEY value, …)` and
/// `DROP SECRET [IF EXISTS] name`, parsed here: a value is a string, a word or a number.
pub fn statement(sql: &str) -> Option<crate::write::Stmt> {
    use crate::ddl::Ddl;
    use datafusion::sql::sqlparser::{dialect::GenericDialect, keywords::Keyword, parser::Parser, tokenizer::Token};
    let mut p = Parser::new(&GenericDialect {}).try_with_sql(sql).ok()?;
    if let Some(copy) = copy_statement(&mut p) {
        return Some(copy);
    }
    let mut p = Parser::new(&GenericDialect {}).try_with_sql(sql).ok()?;
    let invalid = |e: String| Some(crate::write::Stmt::Invalid(e));
    if p.parse_keyword(Keyword::ATTACH) {
        return attach_statement(&mut p);
    }
    if p.parse_keyword(Keyword::DROP) {
        let _ = p.parse_one_of_keywords(&[Keyword::PERSISTENT, Keyword::TEMPORARY]);
        if !p.parse_keyword(Keyword::SECRET) {
            return None;
        }
        let if_exists = p.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
        return match p.parse_identifier() {
            Ok(n) => Some(crate::write::Stmt::Ddl(vec![Ddl::DropSecret { name: n.value.to_lowercase(), if_exists }])),
            Err(e) => invalid(format!("DROP SECRET name: {e}")),
        };
    }
    if !p.parse_keyword(Keyword::CREATE) {
        return None;
    }
    let replace = p.parse_keywords(&[Keyword::OR, Keyword::REPLACE]);
    if p.parse_keyword(Keyword::TEMPORARY) {
        return p.parse_keyword(Keyword::SECRET).then(|| crate::write::Stmt::Invalid("CREATE TEMPORARY SECRET: secrets are kept in the lake (CREATE SECRET)".into()));
    }
    let _ = p.parse_keyword(Keyword::PERSISTENT);
    if !p.parse_keyword(Keyword::SECRET) {
        return None;
    }
    let usage = "CREATE SECRET name (TYPE s3, KEY_ID '…', SECRET '…', SCOPE 's3://bucket')";
    let if_not_exists = p.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
    let Ok(name) = p.parse_identifier() else { return invalid(format!("which name? {usage}")) };
    if !p.consume_token(&Token::LParen) {
        return invalid(usage.into());
    }
    let mut params = BTreeMap::new();
    loop {
        let (Ok(k), v) = (p.parse_identifier(), p.next_token().token) else { return invalid(usage.into()) };
        let v = match v {
            Token::SingleQuotedString(s) | Token::DoubleQuotedString(s) | Token::Number(s, _) => s,
            Token::Word(w) => w.value,
            t => return invalid(format!("{}: a value, not {t} ({usage})", k.value)),
        };
        params.insert(k.value.to_lowercase(), v);
        if p.consume_token(&Token::RParen) {
            break;
        }
        if !p.consume_token(&Token::Comma) {
            return invalid(usage.into());
        }
    }
    Some(crate::write::Stmt::Ddl(vec![Ddl::CreateSecret { name: name.value.to_lowercase(), params, replace, if_not_exists }]))
}

/// `ATTACH [DATABASE] [IF NOT EXISTS] 'url' [AS] name (TYPE delta | iceberg, ENDPOINT '…', …)`:
/// another engine's tables. None: another lake (no TYPE, or TYPE pondra), as before.
fn attach_statement(p: &mut datafusion::sql::sqlparser::parser::Parser) -> Option<crate::write::Stmt> {
    use datafusion::sql::sqlparser::{keywords::Keyword, tokenizer::Token};
    let usage = "ATTACH 's3://bucket/tables' AS name (TYPE delta), or ATTACH 'https://catalog' AS name (TYPE iceberg)";
    let _ = p.parse_keyword(Keyword::DATABASE);
    let _ = p.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
    let url = p.parse_literal_string().ok()?;
    let _ = p.parse_keyword(Keyword::AS);
    let name = p.parse_identifier().ok()?.value.to_lowercase();
    if !p.consume_token(&Token::LParen) {
        return None;
    }
    let mut options = BTreeMap::new();
    loop {
        let Ok(k) = p.parse_identifier() else { return Some(crate::write::Stmt::Invalid(usage.into())) };
        let v = match p.peek_token().token {
            Token::Comma | Token::RParen => "true".to_string(),
            _ => match p.next_token().token {
                Token::SingleQuotedString(s) | Token::Number(s, _) => s,
                Token::Word(w) => w.value,
                t => return Some(crate::write::Stmt::Invalid(format!("{}: a value, not {t} ({usage})", k.value))),
            },
        };
        options.insert(k.value.to_lowercase(), v);
        if p.consume_token(&Token::RParen) {
            break;
        }
        if !p.consume_token(&Token::Comma) {
            return Some(crate::write::Stmt::Invalid(usage.into()));
        }
    }
    let kind = options.remove("type").unwrap_or_default().to_lowercase();
    if kind.is_empty() || kind == "pondra" {
        return None; // (a lake: `ddl::Attach`)
    }
    Some(crate::write::Stmt::Ddl(vec![crate::ddl::Ddl::AttachOutside { name, url, kind, options }]))
}

/// `COPY (query) TO 'url' (FORMAT parquet, PARTITION_BY (a, b), HEADER true, DELIMITER ';')` or
/// `COPY table TO …`, DuckDB's: a value is a word, a string, a number or a list; an option alone
/// is true. None: not a COPY to a file (`COPY … TO STDOUT` is the Postgres port's).
fn copy_statement(p: &mut datafusion::sql::sqlparser::parser::Parser) -> Option<crate::write::Stmt> {
    use datafusion::sql::sqlparser::{keywords::Keyword, tokenizer::Token};
    if !p.parse_keyword(Keyword::COPY) {
        return None;
    }
    let invalid = |e: String| Some(crate::write::Stmt::Invalid(format!("COPY … TO 'url' (FORMAT parquet, …): {e}")));
    let query = match p.consume_token(&Token::LParen) {
        true => match p.parse_query().and_then(|q| p.expect_token(&Token::RParen).map(|_| q)) {
            Ok(q) => q.to_string(),
            Err(e) => return invalid(e.to_string()),
        },
        false => match p.parse_object_name(false) {
            Ok(t) => format!("SELECT * FROM {t}"),
            Err(e) => return invalid(e.to_string()),
        },
    };
    if !p.parse_keyword(Keyword::TO) {
        return None; // (COPY … FROM: the Postgres port's)
    }
    let Ok(to) = p.parse_literal_string() else { return None }; // (TO STDOUT)
    let mut options = BTreeMap::new();
    if p.consume_token(&Token::LParen) && !p.consume_token(&Token::RParen) {
        loop {
            let Ok(k) = p.parse_identifier() else { return invalid("an option's name".into()) };
            let v = match p.peek_token().token {
                Token::Comma | Token::RParen => "true".to_string(),
                Token::LParen => match p.expect_token(&Token::LParen).and_then(|_| p.parse_comma_separated(|p| p.parse_identifier())).and_then(|l| p.expect_token(&Token::RParen).map(|_| l)) {
                    Ok(l) => l.iter().map(|i| if i.quote_style.is_some() { i.value.clone() } else { i.value.to_lowercase() }).collect::<Vec<_>>().join(","), // (as SQL reads names)
                    Err(e) => return invalid(e.to_string()),
                },
                _ => match p.next_token().token {
                    Token::SingleQuotedString(s) | Token::DoubleQuotedString(s) | Token::Number(s, _) => s,
                    Token::Word(w) => w.value,
                    t => return invalid(format!("{}: a value, not {t}", k.value)),
                },
            };
            options.insert(k.value.to_lowercase(), v);
            if p.consume_token(&Token::RParen) {
                break;
            }
            if !p.consume_token(&Token::Comma) {
                return invalid("options are separated by commas".into());
            }
        }
    }
    Some(crate::write::Stmt::CopyTo(query, to, options))
}

/// Leader: keep a secret, sealed. Its scope's bucket may have no other secret's scope in it:
/// a bucket is read with one secret.
pub async fn create(lake: &Lake, name: &str, mut params: BTreeMap<String, String>, replace: bool, if_not_exists: bool) -> Result<serde_json::Value> {
    crate::ddl::check(name)?;
    let t = params.remove("type").context("which TYPE? (s3, r2, gcs, azure, http or generic)")?.to_lowercase();
    let (known, schemes) = kind(&t)?;
    if let Some(k) = params.keys().find(|k| !known.is_empty() && !known.contains(&k.as_str())) {
        bail!("a {t} secret has no {k}: {}", known.join(", "));
    }
    let scope = params.remove("scope");
    if let Some(s) = &scope {
        ensure!(scheme(s).is_some_and(|x| schemes.contains(&x)), "a {t} secret's SCOPE is a URL of {}", schemes.iter().map(|s| format!("{s}://")).collect::<Vec<_>>().join(" or "));
    }
    if lake.cat.get::<Secret>(&secret_key(name)).await?.is_some() {
        ensure!(replace || if_not_exists, "secret {name} already exists (CREATE OR REPLACE SECRET)");
        if !replace {
            return Ok(serde_json::json!({"secret": name, "unchanged": true}));
        }
    }
    let bucket = |s: &str| root(s);
    if let Some(b) = scope.as_deref().and_then(bucket) {
        for (other, s) in list(lake).await? {
            let same = s.scope.as_deref().is_some_and(|o| bucket(o).as_deref() == Some(b.as_str()));
            ensure!(other == name || !same, "{b} already has secret {other} for {}: a bucket is read with one secret (one SCOPE in it)", s.scope.unwrap_or_default());
        }
    }
    let secret = Secret { kind: t, scope, sealed: seal(&serde_json::to_vec(&params)?)? };
    lake.cat.commit(vec![(secret_key(name), json(&secret))], &[]).await?;
    Ok(serde_json::json!({"secret": name}))
}

pub async fn drop(lake: &Lake, name: &str, if_exists: bool) -> Result<serde_json::Value> {
    if lake.cat.get::<Secret>(&secret_key(name)).await?.is_none() {
        ensure!(if_exists, "no secret {name}");
        return Ok(serde_json::json!({"secret": name, "dropped": false}));
    }
    lake.cat.commit(vec![], &[secret_key(name)]).await?;
    Ok(serde_json::json!({"secret": name, "dropped": true}))
}

pub async fn list(lake: &Lake) -> Result<Vec<(String, Secret)>> {
    Ok(lake.cat.scan::<Secret>("e/", "e0").await?.into_iter().map(|(k, s)| (k[2..].to_string(), s)).collect())
}

/// The secret for a URL: the longest scope that is a prefix of it (no scope: every URL of its
/// type's schemes).
pub(crate) fn covering(secrets: &[(String, Secret)], url: &str) -> Option<(String, Secret)> {
    let s = scheme(url)?;
    secrets.iter().filter(|(_, x)| kind(&x.kind).is_ok_and(|(_, schemes)| schemes.contains(&s)))
        .filter(|(_, x)| x.scope.as_deref().is_none_or(|p| url.starts_with(p)))
        .max_by_key(|(_, x)| x.scope.as_ref().map_or(0, |p| p.len() + 1)).cloned()
}

/// A secret's values, opened with the nodes' key.
fn open(name: &str, s: &Secret) -> Result<BTreeMap<String, String>> {
    let sealed = B64.decode(&s.sealed)?;
    ensure!(sealed.len() > 12, "secret {name} is damaged");
    let (nonce, data) = sealed.split_at(12);
    let mut data = data.to_vec();
    let plain = cipher()?.open_in_place(Nonce::try_assume_unique_for_key(nonce).map_err(|_| anyhow!("nonce"))?, Aad::empty(), &mut data)
        .map_err(|_| anyhow!("secret {name} was sealed with another key: every node needs the same PONDRA_SECRET_KEY"))?;
    Ok(serde_json::from_slice(plain)?)
}

fn seal(plain: &[u8]) -> Result<String> {
    let mut nonce = [0u8; 12];
    aws_lc_rs::rand::fill(&mut nonce).map_err(|_| anyhow!("no randomness"))?;
    let mut data = plain.to_vec();
    cipher()?.seal_in_place_append_tag(Nonce::assume_unique_for_key(nonce), Aad::empty(), &mut data).map_err(|_| anyhow!("sealing failed"))?;
    Ok(B64.encode([nonce.as_slice(), &data].concat()))
}

/// The key secrets are sealed with: `PONDRA_SECRET_KEY` (the same on every node of a cluster), or
/// one made for this machine (`~/.pondra/secret.key`), enough for nodes that share it.
fn cipher() -> Result<LessSafeKey> {
    let key = match std::env::var("PONDRA_SECRET_KEY") {
        Ok(k) if !k.is_empty() => k,
        _ => machine_key()?,
    };
    let digest = aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, key.as_bytes());
    Ok(LessSafeKey::new(UnboundKey::new(&AES_256_GCM, digest.as_ref()).map_err(|_| anyhow!("key"))?))
}

fn machine_key() -> Result<String> {
    let home = std::env::var_os("HOME").or_else(|| std::env::var_os("USERPROFILE")).context("secrets need a key: set PONDRA_SECRET_KEY")?;
    let path = std::path::PathBuf::from(home).join(".pondra").join("secret.key");
    if let Ok(k) = std::fs::read_to_string(&path) {
        return Ok(k.trim().to_string());
    }
    std::fs::create_dir_all(path.parent().expect("a folder"))?;
    let mut bytes = [0u8; 32];
    aws_lc_rs::rand::fill(&mut bytes).map_err(|_| anyhow!("no randomness"))?;
    let key: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
    let mut f = std::fs::OpenOptions::new();
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut f, 0o600);
    match f.write(true).create_new(true).open(&path) {
        Ok(mut file) => std::io::Write::write_all(&mut file, key.as_bytes())?,
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => return Ok(std::fs::read_to_string(&path)?.trim().to_string()), // (another node, at once)
        Err(e) => return Err(e.into()),
    }
    Ok(key)
}

/// `secrets()`: each secret's name, type and scope — never its values.
pub fn register_secrets(ctx: &datafusion::prelude::SessionContext, lake: Arc<Lake>) {
    ctx.register_udtf("secrets", Arc::new(Listing(lake)));
}

#[derive(Debug)]
struct Listing(Arc<Lake>);

impl datafusion::catalog::TableFunctionImpl for Listing {
    fn call(&self, _: &[datafusion::prelude::Expr]) -> datafusion::error::Result<Arc<dyn datafusion::catalog::TableProvider>> {
        Ok(Arc::new(Secrets(self.0.clone())))
    }
}

struct Secrets(Arc<Lake>);

impl std::fmt::Debug for Secrets {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result { write!(f, "secrets()") }
}

#[async_trait::async_trait]
impl datafusion::catalog::TableProvider for Secrets {
    fn schema(&self) -> datafusion::arrow::datatypes::SchemaRef {
        use datafusion::arrow::datatypes::{DataType::Utf8, Field, Schema};
        Arc::new(Schema::new(vec![Field::new("name", Utf8, false), Field::new("type", Utf8, false), Field::new("scope", Utf8, true)]))
    }
    fn table_type(&self) -> datafusion::datasource::TableType { datafusion::datasource::TableType::View }

    async fn scan(&self, state: &dyn datafusion::catalog::Session, projection: Option<&Vec<usize>>, _: &[datafusion::prelude::Expr], _: Option<usize>) -> datafusion::error::Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        use datafusion::arrow::array::StringArray;
        let e = |e: anyhow::Error| datafusion::error::DataFusionError::External(e.into());
        let all = list(&self.0).await.map_err(e)?;
        let columns: Vec<Arc<dyn datafusion::arrow::array::Array>> = vec![
            Arc::new(all.iter().map(|(n, _)| Some(n.as_str())).collect::<StringArray>()),
            Arc::new(all.iter().map(|(_, s)| Some(s.kind.as_str())).collect::<StringArray>()),
            Arc::new(all.iter().map(|(_, s)| s.scope.as_deref()).collect::<StringArray>()),
        ];
        let batch = datafusion::arrow::record_batch::RecordBatch::try_new(self.schema(), columns)?;
        datafusion::catalog::MemTable::try_new(self.schema(), vec![vec![batch]])?.scan(state, projection, &[], None).await
    }
}
