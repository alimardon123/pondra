//! `ext:` names: files and other engines' tables as SQL names them (ADR-026), in every build. A node and a page read
//! them with the same listing, footers and reader (`data_files`, `read`); `ext.rs` adds the other engines' tables and the secrets.
use crate::data_file::DataFile;
use anyhow::{bail, ensure, Context, Result};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD as B64, Engine};
use datafusion::datasource::file_format::{csv::CsvFormat, json::JsonFormat, parquet::ParquetFormat, FileFormat};
use datafusion::arrow::datatypes::{DataType, FieldRef, Schema, SchemaRef};
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::execution::SessionState;
use datafusion::object_store::{ObjectMeta, ObjectStore};
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::sql::sqlparser::ast::{Expr, TableFactor, Value};
use futures::{StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;

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

pub(crate) fn name(spec: &Spec) -> String { format!("{PREFIX}{}", B64.encode(serde_json::to_vec(spec).expect("a spec"))) }

pub fn spec(name: &str) -> Option<Spec> { serde_json::from_slice(&B64.decode(name.strip_prefix(PREFIX)?).ok()?).ok() }

/// The `version` option as a number: the Delta version a read asks for, or None for its latest. Refused by name when it
/// is not a number, so a node and a page say the same words.
pub fn version(spec: &Spec) -> Result<Option<i64>> {
    spec.options.get("version").map(|v| v.parse::<i64>().with_context(|| format!("version is a number, not {v}"))).transpose()
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

/// Might `sql` name files? (A cheap test, before anything is parsed.)
pub fn mentions(sql: &str) -> bool {
    static FILES: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
        regex::Regex::new(r"(?i)'(s3|r2|gs|gcs|az|azure|abfss?|https?)://|\b(from|join|using)\s+'|\b(read_parquet|parquet_scan|read_csv|read_csv_auto|read_json|read_json_auto|read_ndjson|read_arrow|read_delta|read_iceberg|delta_scan|iceberg_scan)\s*\(").expect("a regex")
    });
    FILES.is_match(sql)
}

/// The options `file_format` reads for CSV (DuckDB's names). `all_varchar` reads every column as text, with no type
/// inference (`file_format`).
pub(crate) const CSV_OPTIONS: &[&str] = &["header", "delim", "sep", "delimiter", "quote", "escape", "comment", "new_line", "all_varchar"];
/// The options every listing of files takes, on a node.
const LISTING_OPTIONS: &[&str] = &["hive_partitioning", "hive_types", "union_by_name", "columns"];

/// The table a reference to files is: `'s3://…/x.parquet'` or `'sales/*.csv'` (a URL or a path,
/// its format by extension), or `read_parquet(…)`, `read_csv(…)`, `read_json(…)`, `read_delta(…)`,
/// `read_iceberg(…)` with URLs or paths and DuckDB's options by name. Pondra's names come first;
/// DuckDB's (`parquet_scan`, `read_csv_auto`, `read_json_auto`, `read_ndjson`, `delta_scan`,
/// `iceberg_scan`) are the same (ADR-028). None: not files.
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
        "read_arrow" => "arrow",
        "read_delta" | "delta_scan" => "delta",
        "read_iceberg" | "iceberg_scan" => "iceberg",
        _ => return Ok(None),
    };
    let (mut urls, mut options) = (vec![], BTreeMap::new());
    let a = crate::text::args(&f, &args.args)?;
    for e in a.given {
        urls.extend(strings(e).with_context(|| format!("{f}: files are a string or a list of them"))?);
    }
    for (name, e) in a.named {
        let value = match (name.to_lowercase().as_str(), e) {
            ("columns" | "hive_types" | "types", Expr::Dictionary(d)) => d.iter().map(|c| Ok(format!("{} {}", quoted(&c.key.value), literal(&c.value)?))).collect::<Result<Vec<_>>>().map(|c| c.join(", ")),
            ("types", _) => bail!("{f}: types names each column's type in a struct, types => {{'zip': 'VARCHAR'}} (DuckDB's list form isn't read)"),
            _ => literal(e),
        };
        options.insert(name.to_lowercase(), value.with_context(|| format!("{f}: {name} is a value"))?);
    }
    ensure!(!urls.is_empty(), "{f}: which files? {f}('s3://bucket/path/*.{format}')");
    let known: Vec<&str> = match format {
        "csv" => [CSV_OPTIONS, LISTING_OPTIONS, &["types"][..]].concat(),
        "delta" => vec!["version"],
        "iceberg" => vec!["version", "snapshot_from_id", "snapshot_from_timestamp", "allow_moved_paths"],
        _ => LISTING_OPTIONS.to_vec(),
    };
    ensure!(!["delta", "iceberg"].contains(&format) || urls.len() == 1, "{f}: one table at a time");
    if let Some(k) = options.keys().find(|k| !known.contains(&k.as_str())) {
        bail!("{f} has no option {k}: {}", known.join(", "));
    }
    if options.contains_key("types") {
        ensure!(!options.contains_key("columns"), "{f}: types and columns: columns names every column's type, so types would do nothing");
        ensure!(!options.get("all_varchar").is_some_and(|v| v != "false"), "{f}: types and all_varchar => true: one is enough (all_varchar reads every column as text)");
    }
    Ok(Some(name(&Spec { urls, format: format.into(), options })))
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
        "arrow" | "feather" | "ipc" => ("arrow".into(), BTreeMap::new()), // (Arrow's file format: IPC with a footer)
        "gz" | "zst" | "bz2" | "xz" => bail!("{url}: compressed files aren't read yet: Parquet, CSV, JSON and Arrow as they are"),
        _ => bail!("{url}: which format? name files by their extension (.parquet, .csv, .json, .arrow), or read them with read_parquet(…), read_csv(…), read_json(…) or read_arrow(…)"),
    })
}

/// A URL's scheme, if it names one Pondra reads (not a path on this machine).
#[cfg_attr(not(feature = "server"), allow(dead_code))] // (the node's readers call it; a page's build doesn't yet)
pub(crate) fn scheme(url: &str) -> Option<&str> {
    let s = url.split_once("://")?.0;
    ["s3", "r2", "gs", "gcs", "az", "azure", "abfs", "abfss", "http", "https", "kafka", "file"].contains(&s).then_some(s)
}

/// Where a URL's glob starts (`*`, `[`; `?` too, but not in a web address: its query).
pub(crate) fn glob_at(u: &str) -> Option<usize> {
    let web = u.starts_with("http");
    u.char_indices().find(|(_, c)| *c == '*' || *c == '[' || (*c == '?' && !web)).map(|(i, _)| i)
}

/// A URL as DataFusion lists it, its glob split off (DataFusion splits only paths' globs).
pub(crate) fn listing_url(u: &str) -> Result<ListingTableUrl> {
    let Some(at) = glob_at(u).filter(|_| u.split_once("://").is_some_and(|(s, _)| s != "file")) else { return Ok(ListingTableUrl::parse(u)?) };
    let cut = u[..at].rfind('/').map_or(0, |j| j + 1);
    let (prefix, glob) = u.split_at(cut);
    Ok(ListingTableUrl::try_new(url::Url::parse(prefix)?, Some(glob::Pattern::new(glob)?))?)
}

/// The tables of files named in `sql` (after `routines::expand`).
pub fn names(sql: &str) -> Vec<String> {
    static NAMES: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| regex::Regex::new(r#""(ext:[A-Za-z0-9_-]+)""#).expect("a regex"));
    let mut out: Vec<String> = NAMES.captures_iter(sql).map(|c| c[1].to_string()).collect();
    out.sort();
    out.dedup();
    out
}

/// A name as SQL writes it, in double quotes (`"a"`; a quote inside doubled).
pub(crate) fn quoted(name: &str) -> String { format!("\"{}\"", name.replace('"', "\"\"")) }

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

/// The table of files a name of an attached catalog is (None: not one of theirs).
pub fn attached_table(all: &[(String, Attached)], parts: &[String]) -> Result<Option<String>> {
    let Some((_, a)) = all.iter().find(|(n, _)| *n == parts[0]) else { return Ok(None) };
    let rest = &parts[1..];
    if a.kind == "kafka" {
        let [topic] = rest else { bail!("{}: a Kafka cluster's topics are {0}.topic", parts[0]) };
        return Ok(Some(name(&Spec { urls: vec![format!("{}/{topic}", a.url.trim_end_matches('/'))], format: "kafka".into(), options: BTreeMap::new() })));
    }
    if a.kind == "share" {
        let (schema, t) = match rest {
            [t] => ("public", t),
            [s, t] => (s.as_str(), t),
            _ => bail!("{}: a share's tables are {0}.schema.table", parts[0]),
        };
        let options = BTreeMap::from([("share".to_string(), a.options.get("share").cloned().unwrap_or_default()), ("schema".to_string(), schema.to_string()), ("table".to_string(), t.clone())]);
        return Ok(Some(name(&Spec { urls: vec![a.url.clone()], format: "share".into(), options })));
    }
    let endpoint = endpoint(a);
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
            options.insert("catalog".into(), parts[0].clone()); // (the attachment's name: its settings, `catalogs::open`)
            Spec { urls: vec![endpoint.trim_end_matches('/').to_string()], format: "iceberg".into(), options }
        }
        None => Spec { urls: vec![std::iter::once(a.url.trim_end_matches('/')).chain(rest.iter().map(String::as_str)).collect::<Vec<_>>().join("/")], format: a.kind.clone(), options: BTreeMap::new() },
    };
    Ok(Some(name(&spec)))
}

/// An attached Iceberg catalog's REST endpoint, if it has one.
pub fn endpoint(a: &Attached) -> Option<String> {
    a.options.get("endpoint").cloned().or_else(|| a.url.starts_with("http").then(|| a.url.clone())).map(|e| e.trim_end_matches('/').to_string())
}

/// How a Spec's format and options read its files (CSV's options are CSV_OPTIONS, DuckDB's names), on a node and in a page.
/// Parquet is read with the session's Parquet options (`PONDRA_SQL_OPTIONS`, a page's plain strings), so they reach every
/// file read this way: `ParquetFormat::default()` ignored them. `default_table_options()`, not `table_options()`: the
/// latter is fixed when the session is built, and a later `SET` never reaches it.
pub(crate) fn file_format(spec: &Spec, state: &dyn datafusion::catalog::Session) -> Result<Arc<dyn FileFormat>> {
    let o = |k: &str| spec.options.get(k).map(String::as_str);
    let byte = |k: &str, v: &str| -> Result<u8> { v.bytes().next().filter(|_| v.len() == 1).with_context(|| format!("{k} is one character, not {v:?}")) };
    Ok(match spec.format.as_str() {
        "parquet" => Arc::new(ParquetFormat::default().with_options(state.default_table_options().parquet)),
        "json" => Arc::new(JsonFormat::default()),
        "arrow" => Arc::new(datafusion::datasource::file_format::arrow::ArrowFormat),
        "csv" => {
            let mut f = CsvFormat::default().with_has_header(o("header").is_none_or(|h| h != "false"));
            if o("all_varchar").is_some_and(|v| v != "false") {
                f = f.with_schema_infer_max_rec(0); // (no inference: every column is text, as DuckDB's all_varchar reads it)
            }
            if let Some(d) = o("delim").or(o("sep")).or(o("delimiter")) {
                f = f.with_delimiter(byte("delim", d)?);
            }
            if let Some(q) = o("quote") {
                f = f.with_quote(byte("quote", q)?);
            }
            if let Some(e) = o("escape") {
                f = f.with_escape(Some(byte("escape", e)?));
            }
            if let Some(c) = o("comment") {
                f = f.with_comment(Some(byte("comment", c)?)); // (lines starting with it are skipped)
            }
            match o("new_line") {
                None | Some("\n" | "\r\n" | "\\n" | "\\r\\n") => {} // (either, as read)
                Some("\r" | "\\r") => f = f.with_terminator(Some(b'\r')),
                Some(t) => bail!("new_line is \\n, \\r\\n or \\r, not {t:?}"),
            }
            Arc::new(f)
        }
        f => bail!("files as {f}: parquet, csv, json or arrow"),
    })
}

// ---------------------------------------------------------------- one listing, one reader

/// A file a spec names, as every form lists it: the session's store for it, the URL it was listed under, and its
/// listing (its name, size, time and e-tag).
pub struct Found {
    pub store: Arc<dyn ObjectStore>,
    pub url: ListingTableUrl,
    pub object: ObjectMeta,
}

impl Found {
    /// The file's whole URL, as it is written (`file:///x`, `s3://b/x`, `mem:///files/x`).
    pub fn full(&self) -> String {
        let root = self.url.object_store();
        format!("{}/{}", root.as_str().strip_suffix('/').unwrap_or(root.as_str()), self.object.location)
    }

    /// The file's folders as text (`part_of`).
    pub fn part(&self) -> String { part_of(&folders(&self.url, &self.object)) }
}

/// Folders as text, `k=v/…`, as `DataFile::part` holds them and `read_files` reads them.
pub fn part_of(folders: &[(String, String)]) -> String { folders.iter().map(|(k, v)| format!("{k}={v}")).collect::<Vec<_>>().join("/") }

/// The files a spec names, listed as every form lists them: a glob, `**` or a glob over folders reaches into
/// folders, and a folder (`…/`) gives its files of the spec's format. Sorted by name, so every node lists them in
/// one order. A URL's store must be in the session already (a node registers it with its secret: `ext::register`;
/// a page has `mem://`). A URL with no files is `none(url)`'s error, or allowed when it gives None.
pub async fn listed(state: &SessionState, spec: &Spec, none: impl Fn(&str) -> Option<anyhow::Error>) -> Result<Vec<Found>> {
    let deep = spec.urls.iter().any(|u| u.contains("**") || glob_at(u).is_some_and(|i| u[i..].contains('/')));
    let mut state = state.clone(); // (a copy: the caller's session keeps its own listing)
    state.config_mut().options_mut().execution.listing_table_ignore_subdirectory = !deep; // (`**`, `*/x.parquet`: into folders)
    let mut found = vec![];
    for u in &spec.urls {
        let url = listing_url(u)?;
        let store = state.runtime_env().object_store(&url)?;
        let wanted = if u.ends_with('/') { format!(".{}", spec.format) } else { String::new() }; // (a folder: its files of that format)
        let objects: Vec<ObjectMeta> = url.list_all_files(&state, store.as_ref(), &wanted).await?.try_collect().await?;
        if objects.is_empty() {
            if let Some(e) = none(u) {
                return Err(e);
            }
        }
        found.extend(objects.into_iter().map(|object| Found { store: store.clone(), url: url.clone(), object }));
    }
    found.sort_by(|a, b| a.object.location.cmp(&b.object.location)); // (every node lists them in one order)
    Ok(found)
}

/// The schema of a listing's files: every Parquet file's (else the first 16), with the files whose notes differ
/// merged without them (`inferred`).
pub async fn schema_of(state: &SessionState, spec: &Spec, found: &[Found]) -> Result<SchemaRef> {
    let format = file_format(spec, state)?;
    let store = found.first().context("no files")?.store.clone();
    let first: Vec<ObjectMeta> = found.iter().take(if spec.format == "parquet" { usize::MAX } else { 16 }).map(|f| f.object.clone()).collect();
    inferred(format.as_ref(), state, &store, &first).await.with_context(|| format!("reading {}", spec.urls.join(", ")))
}

/// The files' columns; files whose notes differ (pandas' metadata, each file's own) merged without them.
pub async fn inferred(format: &dyn FileFormat, state: &dyn datafusion::catalog::Session, store: &Arc<dyn ObjectStore>, files: &[ObjectMeta]) -> Result<SchemaRef> {
    let mut each = vec![];
    if format.get_ext() == "arrow" {
        for f in files {
            each.push(match arrow_file_schema(store, f).await? {
                Some(schema) => schema,
                None => format.infer_schema(state, store, std::slice::from_ref(f)).await?.as_ref().clone(), // (a stream)
            });
        }
        return merged(each);
    }
    match format.infer_schema(state, store, files).await {
        Err(e) if e.to_string().contains("conflicting metadata") => {
            for f in files {
                each.push(format.infer_schema(state, store, std::slice::from_ref(f)).await?.as_ref().clone());
            }
            merged(each)
        }
        r => Ok(r?),
    }
}

/// Schemas merged; when their notes differ, merged without them.
fn merged(each: Vec<Schema>) -> Result<SchemaRef> {
    match Schema::try_merge(each.clone()) {
        Err(e) if e.to_string().contains("conflicting metadata") => {
            Ok(Arc::new(Schema::try_merge(each.into_iter().map(|s| s.with_metadata(Default::default())))?))
        }
        r => Ok(Arc::new(r?)),
    }
}

/// The schema with the types `types` names replaced (`read_csv`'s `types`): the other columns stay as inferred. A name
/// the files don't have is refused, naming it and the columns they have.
pub fn retyped(schema: &SchemaRef, types: &[(String, DataType)]) -> Result<SchemaRef> {
    let mut fields: Vec<FieldRef> = schema.fields().iter().cloned().collect();
    for (name, t) in types {
        let Some(field) = fields.iter_mut().find(|f| f.name() == name) else {
            let columns: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
            bail!("types names {name}, which the files don't have: their columns are {}", columns.join(", "))
        };
        *field = Arc::new(field.as_ref().clone().with_data_type(t.clone()));
    }
    Ok(Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone())))
}

/// An Arrow IPC file's schema, from its footer (two small reads), or None for a stream. DataFusion reads a file
/// fetched as a stream as if it were one, taking the 64-byte padding arrow-rs writes after the magic for the
/// schema's length ("Unable to read IPC message schema"), on a page and on a bucket alike.
async fn arrow_file_schema(store: &Arc<dyn ObjectStore>, f: &ObjectMeta) -> Result<Option<Schema>> {
    use datafusion::arrow::ipc;
    use datafusion::object_store::ObjectStoreExt;
    const MAGIC: &[u8] = b"ARROW1";
    if f.size < 18 {
        return Ok(None); // (the magic and its padding, the footer's length, the magic again)
    }
    let tail = store.get_range(&f.location, f.size - 10..f.size).await?;
    if &tail[4..] != MAGIC {
        return Ok(None);
    }
    let length = ipc::reader::read_footer_length(tail[..].try_into()?)? as u64;
    ensure!(length + 10 <= f.size, "{} is not an Arrow file: its footer is longer than it", f.location);
    let footer = store.get_range(&f.location, f.size - 10 - length..f.size - 10).await?;
    let footer = ipc::root_as_footer(&footer).map_err(|e| anyhow::anyhow!("{} is not an Arrow file: {e}", f.location))?;
    let schema = footer.schema().with_context(|| format!("{} is an Arrow file with no schema", f.location))?;
    Ok(Some(ipc::convert::fb_to_schema(schema)))
}

/// A NULL's folder, as Hive, Spark, pyarrow and Polars name it.
pub const NULL_FOLDER: &str = "__HIVE_DEFAULT_PARTITION__";

/// A folder's value as written (percent-escaped where a path can't hold a character), or None
/// for a NULL's folder.
pub(crate) fn hive_value(v: &str) -> Option<String> {
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

/// The Hive-style folders (`day=2026-09-28/`) a file sits in below its listed folder, as (key, value) pairs.
pub fn folders(url: &ListingTableUrl, object: &ObjectMeta) -> Vec<(String, String)> {
    let rest = object.location.as_ref().strip_prefix(url.prefix().as_ref()).unwrap_or_default();
    rest.split('/').filter_map(|s| s.split_once('=')).map(|(k, v)| (k.to_string(), v.to_string())).collect()
}

/// The keys of Hive-style folders (`day=2026-09-28/`) the files sit in, each with its type: as declared in
/// `hive_types`, else by what all its values are. Found unless the spec turns them off (`hive_partitioning`), and
/// when asked for, every file must be in such folders. A key that is one of `columns`, or a key before it
/// (`a=1/a=2/f.csv`), is refused.
pub fn keys(found: &[Found], spec: &Spec, hive_types: &[FieldRef], columns: &[(String, String)]) -> Result<Vec<(String, String)>> {
    // Hive-style folders (`day=2026-09-28/`): each a column, the same in every file's path, typed
    // by what all its values are (as Spark, DuckDB and Polars do).
    // Found unless turned off (DuckDB's and Spark's default); when asked for, every file has them.
    let asked = spec.options.get("hive_partitioning").map(|v| v != "false" && v != "0").or((!hive_types.is_empty()).then_some(true));
    let names: Vec<Vec<String>> = if asked == Some(false) { vec![] } else { found.iter().map(|f| folders(&f.url, &f.object).into_iter().map(|(k, _)| k).collect()).collect() };
    let same = names.windows(2).all(|w| w[0] == w[1]);
    ensure!(same || asked != Some(true), "{}: files in other folders than {} (hive_partitioning)", spec.urls.join(", "), names[0].join("/"));
    let folder_keys = if same { names.first().cloned().unwrap_or_default() } else { vec![] };
    let mut out: Vec<(String, String)> = vec![];
    for (i, k) in folder_keys.iter().enumerate() {
        ensure!(!columns.iter().chain(&out).any(|(c, _)| c == k), "{k} is a folder's key and a column in the files");
        let values: Vec<String> = found.iter().filter_map(|f| hive_value(folders(&f.url, &f.object).get(i)?.1.as_str())).collect();
        let typed = hive_types.iter().find(|f| f.name() == k).map(|f| crate::answer::type_name(f.data_type()));
        out.push((k.clone(), typed.unwrap_or_else(|| hive_type(&values).into())));
    }
    Ok(out)
}

/// The rows of these files (each its full URL and its folders, `k=v/…`, as `DataFile::part` writes them) as
/// `schema`, read by DataFusion's listing tables a folder at a time, each folder's keys as its value, cast to the
/// schema's types.
pub async fn read_files(ctx: &SessionContext, files: &[(&str, &str)], schema: &SchemaRef, spec: &Spec) -> Result<DataFrame> {
    use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig};
    use datafusion::prelude::{cast, ident, lit};
    let part_keys = |part: &str| -> Vec<(String, String)> { part.split('/').filter_map(|s| s.split_once('=')).map(|(k, v)| (k.to_string(), v.to_string())).collect() };
    let folder_keys: Vec<String> = files.first().map(|&(_, part)| part_keys(part).into_iter().map(|(k, _)| k).collect()).unwrap_or_default();
    let fields: Vec<FieldRef> = schema.fields().iter().filter(|f| !folder_keys.contains(f.name())).cloned().collect();
    let stored = crate::answer::schema(&fields.iter().map(|f| (f.name().clone(), crate::answer::type_name(f.data_type()))).collect::<Vec<_>>())?; // (as the files hold it)
    let in_files = if spec.format == "parquet" { Arc::new(datafusion::arrow::datatypes::Schema::new(fields)) } else { stored };
    let mut by_folder: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    files.iter().for_each(|&(path, part)| by_folder.entry(part).or_default().push(path));
    let state = ctx.state();
    let format = file_format(spec, &state)?;
    let mut out: Option<DataFrame> = None;
    for (part, paths) in by_folder {
        let urls = paths.iter().map(|p| ListingTableUrl::parse(p)).collect::<datafusion::error::Result<Vec<_>>>()?;
        let held = match spec.format.as_str() {
            "arrow" => {
                // (as each file holds its columns: Arrow's reader doesn't cast them, the select below
                // does; the files' notes left out, as the table's columns have none)
                use datafusion::object_store::ObjectStoreExt;
                let store = state.runtime_env().object_store(&urls[0])?;
                let mut metas = Vec::with_capacity(urls.len());
                for u in &urls {
                    metas.push(store.head(u.prefix()).await?);
                }
                Arc::new(inferred(format.as_ref(), &state, &store, &metas).await?.as_ref().clone().with_metadata(Default::default()))
            }
            _ => in_files.clone(),
        };
        let options = ListingOptions::new(format.clone()).with_file_extension("");
        let config = ListingTableConfig::new_with_multi_paths(urls).with_listing_options(options).with_schema(held);
        let table = ListingTable::try_new(config)?;
        let mut df = ctx.read_table(Arc::new(table))?;
        for (k, v) in part_keys(part) {
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

// ---------------------------------------------------------------- one footer, one reader for a node and a page

/// A file as a table's `DataFile`: its rows and exact column ranges when its footer says.
pub fn data_file(path: String, bytes: u64, s: Option<&datafusion::common::Statistics>, schema: &Schema) -> DataFile {
    use datafusion::common::stats::Precision::Exact;
    let Some(s) = s else { return DataFile { path, bytes, rows: bytes / 100, ..Default::default() } }; // (CSV, JSON: rows unknown; about 100 bytes each)
    let (mut stats, mut nulls) = (BTreeMap::new(), Some(vec![]));
    for (f, c) in schema.fields().iter().zip(&s.column_statistics) {
        if let (Exact(lo), Exact(hi), false) = (&c.min_value, &c.max_value, f.data_type().is_floating()) {
            if let (Some(lo), Some(hi)) = (crate::data_file::text(lo), crate::data_file::text(hi)) {
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

/// Each file a listing found, as a table's `DataFile`. Parquet's footer gives its rows and column ranges (other
/// formats give rows as an estimate from their bytes, and no ranges); `hived` makes its folders' values columns,
/// which a NULL's folder gives no statistics for (DataFusion would take the column for one without NULLs).
/// `as_stored` are the columns whose ranges are kept (a column read as another type keeps none), and `missing`
/// says a declared column the files don't hold is all NULL.
pub async fn data_files(state: &SessionState, format: &Arc<dyn FileFormat>, schema: &SchemaRef, found: Vec<Found>, hived: bool, as_stored: &[String], missing: bool) -> Result<Vec<DataFile>> {
    let parquet = format.get_ext() == "parquet"; // (only a Parquet footer says rows and ranges)
    let stats: Vec<_> = found.into_iter().map(|hit| {
        let (format, schema, state, as_stored) = (format.clone(), schema.clone(), state.clone(), as_stored.to_vec());
        let part = if hived { folders(&hit.url, &hit.object) } else { vec![] };
        async move {
            let full = hit.full(); // (`file:///x`, `s3://b/x`)
            let (store, o) = (hit.store, hit.object);
            let s = if parquet { Some(format.infer_stats(&state, &store, schema.clone(), &o).await?) } else { None };
            let mut f = data_file(full, o.size, s.as_ref(), &schema);
            f.stats.retain(|c, _| as_stored.contains(c));
            if missing {
                f.nulls = None; // (a column declared that the files don't hold is all NULL)
            }
            f.part = part_of(&part);
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
    futures::stream::iter(stats).buffered(32).try_collect().await
}

/// Columns as SQL declares them (`"id" BIGINT, "name" VARCHAR`): their Arrow fields, as DataFusion types them. The
/// session is the caller's, and its dialect is the one the columns are read in: a node's (`write::declared`), or a
/// page's own.
pub async fn declared(ctx: SessionContext, cols: &str) -> Result<Vec<FieldRef>> {
    ctx.sql(&format!("CREATE TABLE t ({cols})")).await.with_context(|| format!("the columns ({cols})"))?;
    Ok(ctx.table("t").await?.schema().fields().iter().cloned().collect::<Vec<_>>())
}

/// A table's columns as `(name, type)`, the types as `answer::type_name` writes them.
pub(crate) fn named(fields: &[FieldRef]) -> Vec<(String, String)> { fields.iter().map(|f| (f.name().clone(), crate::answer::type_name(f.data_type()))).collect() }

/// A table of files, from the spec's listing to its files' footers: its columns (the Hive folders' keys too) and its
/// data files. A node's `resolve` and a page's `register` both call it, so the same files are read the same way on
/// either. Declared columns are the table's: CSV and JSON read their files by them (by position, and by name), Parquet
/// by name. With no file yet, declared columns make an empty table; without them, `none` says what no file is (and
/// gives None when columns are declared, as the caller's words for it).
pub async fn files_table(state: &SessionState, spec: &Spec, declared: Option<Vec<FieldRef>>, hive_types: &[FieldRef], types: Option<&[(String, DataType)]>, none: impl Fn(&str) -> Option<anyhow::Error>) -> Result<(Vec<(String, String)>, Vec<DataFile>)> {
    let found = listed(state, spec, &none).await?; // (declared: none yet is an empty table)
    if found.is_empty() {
        return Ok(([named(declared.as_deref().unwrap_or_default()), named(hive_types)].concat(), vec![]));
    }
    // CSV's declared columns are its columns by position, JSON's by name: the readers take them as
    // they are. Parquet's files say their own (for their statistics), read by name as declared.
    let schema = match (&declared, spec.format.as_str()) {
        (Some(d), "csv" | "json") => Arc::new(Schema::new(d.clone())),
        _ => schema_of(state, spec, &found).await?,
    };
    // `types => {'zip': 'VARCHAR'}` (`read_csv`, DuckDB's): the named columns are read as those types, the rest as inferred.
    let schema = match types {
        Some(types) => retyped(&schema, types)?,
        None => schema,
    };
    let mut columns = named(declared.as_deref().unwrap_or(&schema.fields()[..]));
    // (a column read as another type than the file's: its ranges are the file's type's, so none is kept)
    let as_stored: Vec<String> = match &declared {
        Some(d) => d.iter().filter(|f| schema.field_with_name(f.name()).is_ok_and(|g| g.data_type() == f.data_type())).map(|f| f.name().clone()).collect(),
        None => schema.fields().iter().map(|f| f.name().clone()).collect(),
    };
    let missing = declared.is_some() && spec.format == "parquet" && columns.iter().any(|(c, _)| schema.field_with_name(c).is_err());
    // Hive-style folders (`day=2026-09-28/`): each a column, the keys and types worked out by `keys`.
    let keys = keys(&found, spec, hive_types, &columns)?;
    columns.extend(keys.iter().cloned());
    let hived = !keys.is_empty();
    let format = file_format(spec, state)?;
    let files = data_files(state, &format, &schema, found, hived, &as_stored, missing).await?; // (the listing's files, as a page reads them too)
    Ok((columns, files))
}

/// The rows of `files` of a spec, as `schema`: Parquet by `scan::read` (the footers' rows and ranges, the folders'
/// values), every other format by DataFusion's listing tables (`read_files`).
pub async fn read(ctx: &SessionContext, files: &[&DataFile], schema: &SchemaRef, spec: &Spec) -> Result<DataFrame> {
    if spec.format == "parquet" {
        return crate::scan::read(ctx, files, schema, None).await; // (as listed: no second look at each file before it is read)
    }
    let files: Vec<(&str, &str)> = files.iter().map(|f| (f.path.as_str(), f.part.as_str())).collect();
    read_files(ctx, &files, schema, spec).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Array, Int64Array};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::object_store::{memory::InMemory, path::Path, ObjectStoreExt, PutPayload};

    /// `d/day=1/a.csv` and `d/day=2/b.csv` in a memory store, registered as `mem://` on a plain session.
    async fn daily() -> (SessionContext, Spec) {
        let ctx = SessionContext::new();
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        for (key, text) in [("d/day=1/a.csv", "id,name\n1,ann\n2,bo\n"), ("d/day=2/b.csv", "id,name\n3,cy\n")] {
            store.put(&Path::from(key), PutPayload::from(bytes::Bytes::from(text))).await.unwrap();
        }
        ctx.register_object_store(&url::Url::parse("mem://").unwrap(), store);
        (ctx, Spec { urls: vec!["mem:///d/**/*.csv".to_string()], format: "csv".to_string(), options: BTreeMap::new() })
    }

    #[tokio::test]
    async fn a_folder_lists_keys_and_reads_as_one_table() {
        let (ctx, spec) = daily().await;
        let state = ctx.state();
        let found = listed(&state, &spec, |_| None).await.unwrap();
        let full: Vec<String> = found.iter().map(|f| f.full()).collect();
        assert_eq!(full, ["mem:///d/day=1/a.csv", "mem:///d/day=2/b.csv"]);
        let columns = [("id".to_string(), "Int64".to_string()), ("name".to_string(), "Utf8".to_string())];
        assert_eq!(keys(&found, &spec, &[], &columns).unwrap(), [("day".to_string(), "Int64".to_string())]);

        let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new("day", DataType::Int64, true), Field::new("id", DataType::Int64, true), Field::new("name", DataType::Utf8, true)]));
        let files = [("mem:///d/day=1/a.csv", "day=1"), ("mem:///d/day=2/b.csv", "day=2")];
        let batches = read_files(&ctx, &files, &schema, &spec).await.unwrap().collect().await.unwrap();
        let mut rows: Vec<(i64, i64)> = vec![];
        for b in &batches {
            let day = b.column_by_name("day").unwrap().as_any().downcast_ref::<Int64Array>().unwrap();
            let id = b.column_by_name("id").unwrap().as_any().downcast_ref::<Int64Array>().unwrap();
            rows.extend((0..b.num_rows()).map(|i| (day.value(i), id.value(i))));
        }
        rows.sort();
        assert_eq!(rows, [(1, 1), (1, 2), (2, 3)]);

        let nope = Spec { urls: vec!["mem:///nope/*.csv".to_string()], ..spec.clone() };
        let err = listed(&state, &nope, |u| Some(anyhow::anyhow!("no files at {u}"))).await.err();
        assert_eq!(err.map(|e| e.to_string()), Some("no files at mem:///nope/*.csv".to_string()));
    }

    /// A Parquet file's strings are read as the session's Parquet options say, including a `SET` made after the session
    /// is built: views by default, plain once the session says so (`file_format` takes the state's default options).
    #[tokio::test]
    async fn parquet_strings_follow_the_sessions_options() {
        use datafusion::arrow::array::StringArray;
        use datafusion::arrow::record_batch::RecordBatch;
        use datafusion::parquet::arrow::ArrowWriter;
        let ctx = SessionContext::new();
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, false)]));
        let mut body = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut body, schema.clone(), None).unwrap();
        writer.write(&RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["a", "b"]))]).unwrap()).unwrap();
        writer.close().unwrap();
        store.put(&Path::from("p/s.parquet"), PutPayload::from(bytes::Bytes::from(body))).await.unwrap();
        ctx.register_object_store(&url::Url::parse("mem://").unwrap(), store);
        let spec = Spec { urls: vec!["mem:///p/*.parquet".to_string()], format: "parquet".to_string(), options: BTreeMap::new() };
        let found = listed(&ctx.state(), &spec, |_| None).await.unwrap();
        let first = schema_of(&ctx.state(), &spec, &found).await.unwrap();
        assert_eq!(first.field(0).data_type(), &DataType::Utf8View);
        ctx.sql("SET datafusion.execution.parquet.schema_force_view_types = false").await.unwrap().collect().await.unwrap();
        let plain = schema_of(&ctx.state(), &spec, &found).await.unwrap();
        assert_eq!(plain.field(0).data_type(), &DataType::Utf8);
    }

    /// An Arrow IPC file as arrow-rs writes it (64-byte padding after its magic) and a stream, from a store that
    /// answers with a stream as a bucket does, are read as one table: the file's schema from its footer.
    #[tokio::test]
    async fn arrow_files_and_streams_from_a_store_are_one_table() {
        use datafusion::arrow::ipc::writer::{FileWriter, StreamWriter};
        use datafusion::arrow::record_batch::RecordBatch;
        let ctx = SessionContext::new();
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        for (i, file) in [(0i64, true), (1, false)] {
            let note = [("a".to_string(), i.to_string())];
            let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]).with_metadata(note.into()));
            let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![i]))]).unwrap();
            let mut body = Vec::new();
            if file {
                let mut w = FileWriter::try_new(&mut body, &schema).unwrap();
                w.write(&batch).unwrap();
                w.finish().unwrap();
            } else {
                let mut w = StreamWriter::try_new(&mut body, &schema).unwrap();
                w.write(&batch).unwrap();
                w.finish().unwrap();
            }
            store.put(&Path::from(format!("p/{i}.arrow")), PutPayload::from(bytes::Bytes::from(body))).await.unwrap();
        }
        ctx.register_object_store(&url::Url::parse("mem://").unwrap(), store);
        let spec = Spec { urls: vec!["mem:///p/*.arrow".to_string()], format: "arrow".to_string(), options: BTreeMap::new() };
        let found = listed(&ctx.state(), &spec, |_| None).await.unwrap();
        let schema = schema_of(&ctx.state(), &spec, &found).await.unwrap();
        assert_eq!((schema.fields().len(), schema.field(0).data_type()), (1, &DataType::Int64));
        assert!(schema.metadata().is_empty());
    }

    /// The table factor of `FROM <sql>`, as the expander takes it.
    fn table_of(sql: &str) -> TableFactor {
        use datafusion::sql::sqlparser::{ast::{SetExpr, Statement}, dialect::GenericDialect, parser::Parser};
        let Ok(mut stmts) = Parser::parse_sql(&GenericDialect {}, &format!("SELECT * FROM {sql}")) else { panic!("{sql}") };
        let Statement::Query(q) = stmts.remove(0) else { panic!("a query") };
        let SetExpr::Select(s) = *q.body else { panic!("a select") };
        s.from.into_iter().next().expect("a table").relation
    }

    /// `types` is a struct of the columns and their types, kept in the spec as `"name" TYPE, …`. The list form, a string,
    /// `types` with `columns` or with `all_varchar => true`, and `types` on a format but CSV are refused.
    #[test]
    fn types_is_a_struct_of_the_columns_and_their_types() {
        let name = table(&table_of("read_csv('a.csv', types => {'zip': 'VARCHAR', 'n': 'INT'})")).unwrap().unwrap();
        assert_eq!(spec(&name).unwrap().options["types"], "\"zip\" VARCHAR, \"n\" INT");
        assert!(table(&table_of("read_csv('a.csv', types => {'zip': 'VARCHAR'}, all_varchar => false)")).is_ok());
        for sql in ["read_csv('a.csv', types => ['VARCHAR', 'INT'])", "read_csv('a.csv', types => 'zip VARCHAR')"] {
            assert!(table(&table_of(sql)).unwrap_err().to_string().contains("struct"), "{sql}");
        }
        let with_columns = table(&table_of("read_csv('a.csv', types => {'zip': 'VARCHAR'}, columns => {'zip': 'VARCHAR'})")).unwrap_err();
        assert!(with_columns.to_string().contains("columns"), "{with_columns}");
        let all_text = table(&table_of("read_csv('a.csv', types => {'zip': 'VARCHAR'}, all_varchar => true)")).unwrap_err();
        assert!(all_text.to_string().contains("all_varchar"), "{all_text}");
        assert!(table(&table_of("read_parquet('a.parquet', types => {'zip': 'VARCHAR'})")).is_err());
    }

    /// `retyped` replaces the named columns' types and keeps the others; a name the files don't have is refused, naming
    /// it and the columns they have.
    #[test]
    fn types_replace_the_named_columns_and_refuse_a_name_the_files_lack() {
        let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new("zip", DataType::Int64, true), Field::new("n", DataType::Int64, true)]));
        let typed = retyped(&schema, &[("zip".to_string(), DataType::Utf8)]).unwrap();
        let kinds: Vec<(&str, DataType)> = typed.fields().iter().map(|f| (f.name().as_str(), f.data_type().clone())).collect();
        assert_eq!(kinds, [("zip", DataType::Utf8), ("n", DataType::Int64)]);
        assert_eq!(schema.field(0).data_type(), &DataType::Int64, "the schema it was given is as it was");
        let err = retyped(&schema, &[("nope".to_string(), DataType::Utf8)]).unwrap_err();
        assert_eq!(err.to_string(), "types names nope, which the files don't have: their columns are zip, n");
    }

    /// A CSV read with its text column as text and its integer column as an integer keeps `007`, and a value the integer
    /// won't take is the reader's error, which `casts::parse_refusal` reads as Postgres's refusal.
    #[tokio::test]
    async fn csv_columns_are_read_as_the_types_given() {
        use datafusion::arrow::array::StringArray;
        let ctx = SessionContext::new();
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        store.put(&Path::from("c/a.csv"), PutPayload::from(bytes::Bytes::from("zip,n\n007,1\n"))).await.unwrap();
        store.put(&Path::from("c/b.csv"), PutPayload::from(bytes::Bytes::from("zip,n\n02134,2.5\n"))).await.unwrap();
        ctx.register_object_store(&url::Url::parse("mem://").unwrap(), store);
        let spec = Spec { urls: vec!["mem:///c/a.csv".to_string()], format: "csv".to_string(), options: BTreeMap::new() };
        let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new("zip", DataType::Utf8, true), Field::new("n", DataType::Int32, true)]));
        let batches = read_files(&ctx, &[("mem:///c/a.csv", "")], &schema, &spec).await.unwrap().collect().await.unwrap();
        let zip = batches[0].column_by_name("zip").unwrap().as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(zip.value(0), "007");
        let err = read_files(&ctx, &[("mem:///c/b.csv", "")], &schema, &spec).await.unwrap().collect().await.unwrap_err();
        let refusal = crate::casts::parse_refusal(&err.to_string()).expect("the reader's refusal, in its words");
        assert_eq!(crate::codes::words(&anyhow::Error::from(refusal)), r#"invalid input syntax for type integer: "2.5""#);
    }
}
