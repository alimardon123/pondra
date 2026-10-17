//! `ext:` names: files and other engines' tables as SQL names them (ADR-026), in every build. Reading them is `ext.rs`'s.
use anyhow::{bail, ensure, Context, Result};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD as B64, Engine};
use datafusion::datasource::file_format::{csv::CsvFormat, json::JsonFormat, parquet::ParquetFormat, FileFormat};
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::sql::sqlparser::ast::{Expr, TableFactor, Value};
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

fn name(spec: &Spec) -> String { format!("{PREFIX}{}", B64.encode(serde_json::to_vec(spec).expect("a spec"))) }

pub fn spec(name: &str) -> Option<Spec> { serde_json::from_slice(&B64.decode(name.strip_prefix(PREFIX)?).ok()?).ok() }

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

/// The options `file_format` reads for CSV (DuckDB's names).
pub(crate) const CSV_OPTIONS: &[&str] = &["header", "delim", "sep", "delimiter", "quote", "escape", "comment", "new_line"];
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
            ("columns" | "hive_types", Expr::Dictionary(d)) => d.iter().map(|c| Ok(format!("{} {}", quoted(&c.key.value), literal(&c.value)?))).collect::<Result<Vec<_>>>().map(|c| c.join(", ")),
            _ => literal(e),
        };
        options.insert(name.to_lowercase(), value.with_context(|| format!("{f}: {name} is a value"))?);
    }
    ensure!(!urls.is_empty(), "{f}: which files? {f}('s3://bucket/path/*.{format}')");
    let known: Vec<&str> = match format {
        "csv" => [CSV_OPTIONS, LISTING_OPTIONS].concat(),
        "delta" => vec!["version"],
        "iceberg" => vec!["version", "snapshot_from_id", "snapshot_from_timestamp", "allow_moved_paths"],
        _ => LISTING_OPTIONS.to_vec(),
    };
    ensure!(!["delta", "iceberg"].contains(&format) || urls.len() == 1, "{f}: one table at a time");
    if let Some(k) = options.keys().find(|k| !known.contains(&k.as_str())) {
        bail!("{f} has no option {k}: {}", known.join(", "));
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
pub(crate) fn file_format(spec: &Spec) -> Result<Arc<dyn FileFormat>> {
    let o = |k: &str| spec.options.get(k).map(String::as_str);
    let byte = |k: &str, v: &str| -> Result<u8> { v.bytes().next().filter(|_| v.len() == 1).with_context(|| format!("{k} is one character, not {v:?}")) };
    Ok(match spec.format.as_str() {
        "parquet" => Arc::new(ParquetFormat::default()),
        "json" => Arc::new(JsonFormat::default()),
        "arrow" => Arc::new(datafusion::datasource::file_format::arrow::ArrowFormat),
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
