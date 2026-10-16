//! `COPY … TO` (ADR-026): a query's rows written outside the lake — one file, a folder of files,
//! Hive-style folders by `PARTITION_BY`, or a Kafka topic. A folder written from a big table is
//! written by every node at once, each its own share's files (`spmd::copy`): nothing crosses
//! between the nodes but how many rows each wrote.
use crate::ext::{covering, format_of, list, owner, register, scheme, uncovered, NULL_FOLDER};
use crate::log::{forget_producers, now_ms};
use crate::server::App;
use crate::store::{json, producer_key, table_key, Lake, TableMeta, QUIET};
use crate::write::Stmt;
use std::sync::Arc;
use anyhow::{bail, ensure, Context, Result};
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::prelude::DataFrame;
use futures::{StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};

const KNOWN: &[&str] = &["format", "partition_by", "header", "delimiter", "delim", "sep", "compression", "row_group_size", "overwrite", "append", "overwrite_or_ignore", "key"];

/// Where a COPY writes, and how, checked once: every node writing a share of it writes alike.
#[derive(Serialize, Deserialize, Clone)]
pub struct Target {
    pub to: String,
    pub format: String,
    pub partition: Vec<String>,
    pub options: BTreeMap<String, String>,
}

impl Target {
    /// A folder of files (a name ending in `/`, or folders by value), not one file.
    pub fn folder(&self) -> bool { self.to.ends_with('/') || !self.partition.is_empty() }
}

/// `COPY … TO` a file or folder outside the lake: Parquet, CSV or JSON lines, by `FORMAT` or the
/// name's extension. A name ending in `/` is a folder of files; otherwise one file, replaced if it
/// is there. A folder that holds files already takes `OVERWRITE` (replace them) or `APPEND`
/// (DuckDB's). With the secret covering it (an admin's statement anyway: `auth::allows`), or, on
/// the node's machine, from its owner. `nodes`: the cluster's live members, `me` among them.
pub async fn copy_to(lake: &Lake, query: &str, to: &str, options: &BTreeMap<String, String>, nodes: &[String], me: &str) -> Result<serde_json::Value> {
    if let Some(k) = options.iter().find(|(k, v)| !KNOWN.contains(&k.as_str()) && writer(k, v).is_none()) {
        bail!("COPY … TO has no option {}: {} (or DataFusion's writers' own: 'format.data_page_row_count_limit', …)", k.0, KNOWN.join(", "));
    }
    let on = |k: &str| options.get(k).is_some_and(|v| !v.eq_ignore_ascii_case("false"));
    let lakes: Vec<String> = std::iter::once(lake.url.clone()).chain(lake.attached.read().unwrap().iter().map(|(_, o)| o.url.clone())).collect();
    let full = crate::ddl::full(to).unwrap_or_else(|_| to.to_string());
    if let Some(l) = lakes.iter().find(|l| full.trim_end_matches('/') == l.as_str() || full.starts_with(&format!("{l}/"))) {
        bail!("COPY … TO {to}: that's inside the lake at {l}, whose files are its own (write beside it, or INSERT into a table)");
    }
    let remote = scheme(to).is_some_and(|s| s != "file");
    match remote {
        false => ensure!(owner(), "COPY … TO {to}: a path on the server's own disk, which a client writes only through a node it started itself (the `pondra` shell, `pondra sql`, `connect()` of a lake): write it there, or to a bucket (s3://, gs://, az://)"),
        true => {
            ensure!(owner() || covering(&list(lake).await?, to).is_some(), "{}", uncovered(to));
            register(lake, to).await?;
        }
    }
    let query = crate::routines::expand(lake, query).await?; // (the files, macros and FROM-first queries it reads)
    if to.starts_with("kafka://") {
        let df = crate::query::sql(&crate::query::session(lake, &query, "").await?, &query).await?;
        return crate::kafka_client::copy_to(lake, df, to, options).await; // (rows as records: a topic's)
    }
    ensure!(!options.contains_key("key"), "KEY is a Kafka topic's (COPY … TO 'kafka://brokers/topic')");
    let format = match options.get("format") {
        Some(f) => f.to_lowercase(),
        None => format_of(to.trim_end_matches('/')).map(|f| f.0).with_context(|| format!("COPY … TO {to}: which FORMAT? (parquet, csv, json, delta, iceberg)"))?,
    };
    if format == "delta" || format == "iceberg" {
        ensure!(!options.contains_key("partition_by"), "COPY … TO as {format}: PARTITION_BY (a partitioned {format} table): not yet");
        let df = crate::query::sql(&crate::query::session(lake, &query, "").await?, &query).await?;
        return crate::write_outside::copy_table(lake, df, to, &format, on("append"), on("overwrite")).await; // (a table, not files: ADR-028)
    }
    ensure!(["parquet", "csv", "json", "arrow"].contains(&format.as_str()), "COPY … TO as {format}: parquet, csv, json, arrow, delta or iceberg");
    ensure!(format == "parquet" || !options.contains_key("compression") && !options.contains_key("row_group_size"), "COPY … TO as {format}: COMPRESSION and ROW_GROUP_SIZE are Parquet's (files are read as they are: not compressed)");
    if let Some((k, _)) = options.iter().find(|(k, v)| !KNOWN.contains(&k.as_str()) && writer(k, v) != Some(format.as_str())) {
        bail!("COPY … TO as {format}: {k} isn't {format}'s option");
    }
    let partition: Vec<String> = options.get("partition_by").map(|p| p.split(',').map(|c| c.trim().to_string()).collect()).unwrap_or_default();
    let target = Target { to: to.into(), format, partition, options: options.clone() };
    let mut kept = HashSet::new(); // (what the folder held before, and holds on)
    if target.folder() {
        // What is there: kept and added to, replaced, or a mistake (Spark's and DuckDB's default).
        let there = listed(lake, to).await?;
        if !there.is_empty() && !on("append") && !on("overwrite_or_ignore") {
            ensure!(on("overwrite"), "COPY … TO {to}: the folder holds files already: OVERWRITE replaces them, APPEND adds to them");
            delete(lake, to, there).await?;
        } else {
            kept = there.into_iter().collect();
        }
        // A folder from a big table, on remote storage: every node writes its own share's files.
        if remote {
            match crate::spmd::copy(lake, nodes, me, &query, &target).await {
                Ok(Some(rows)) => return Ok(serde_json::json!({"copied": rows, "to": to})),
                Ok(None) => {}
                Err(e) => {
                    // What the nodes wrote goes, and this node writes it all.
                    eprintln!("COPY … TO {to} across the nodes failed, writing it from here: {e:#}");
                    let written = listed(lake, to).await?.into_iter().filter(|p| !kept.contains(p)).collect();
                    delete(lake, to, written).await?;
                }
            }
        }
    }
    let rows = crate::query::frugally(|frugal| {
        let (query, target, kept) = (&query, &target, &kept);
        async move {
            if frugal && target.folder() {
                // (what the first try wrote goes, as after a spread COPY that failed)
                let written = listed(lake, &target.to).await?.into_iter().filter(|p| !kept.contains(p)).collect();
                delete(lake, &target.to, written).await?;
            }
            let ctx = crate::query::session(lake, query, "").await?;
            if frugal {
                crate::query::frugal(&ctx);
            }
            write(crate::query::sql(&ctx, query).await?, target).await
        }
    }).await?;
    Ok(serde_json::json!({"copied": rows, "to": to}))
}

/// The files under a folder (none if it isn't there).
async fn listed(lake: &Lake, to: &str) -> Result<Vec<object_store_df::path::Path>> {
    let url = ListingTableUrl::parse(to)?;
    let store = lake.rt.object_store(url.object_store())?;
    match store.list(Some(url.prefix())).map_ok(|o| o.location).try_collect().await {
        Err(object_store_df::Error::NotFound { .. }) => Ok(vec![]),
        r => Ok(r?),
    }
}

async fn delete(lake: &Lake, to: &str, files: Vec<object_store_df::path::Path>) -> Result<()> {
    let store = lake.rt.object_store(ListingTableUrl::parse(to)?.object_store())?;
    store.delete_stream(futures::stream::iter(files.into_iter().map(Ok)).boxed()).try_collect::<Vec<_>>().await?;
    Ok(())
}

/// Which of DataFusion's writers takes this option (`data_page_row_count_limit`, `quote_style`,
/// `content_defined_chunking.enabled`, …): "parquet", "csv" or "json".
fn writer(k: &str, v: &str) -> Option<&'static str> {
    use datafusion::config::{ConfigField, CsvOptions, JsonOptions, TableParquetOptions};
    fn takes<T: ConfigField + Default>(k: &str, v: &str) -> bool { T::default().set(k, v).is_ok() }
    [("parquet", takes::<TableParquetOptions>(k, v)), ("csv", takes::<CsvOptions>(k, v)), ("json", takes::<JsonOptions>(k, v))].into_iter().find(|w| w.1).map(|w| w.0)
}

/// DataFusion's own options for the format's writer, as given.
fn own<T: datafusion::config::ConfigField>(mut o: T, format: &str, options: &BTreeMap<String, String>) -> Result<T> {
    for (k, v) in options.iter().filter(|(k, _)| !KNOWN.contains(&k.as_str())) {
        ensure!(writer(k, v) == Some(format), "COPY … TO as {format}: {k} isn't {format}'s option");
        o.set(k, v)?;
    }
    Ok(o)
}

/// A query's rows written to the target, from this node: how many.
pub async fn write(mut df: DataFrame, t: &Target) -> Result<u64> {
    let options = &t.options;
    if !t.partition.is_empty() {
        // A folder a value, as text; NULL's folder as Hive and Spark name it (DataFusion's is empty).
        use datafusion::prelude::{cast, coalesce, ident, lit};
        let names: Vec<String> = df.schema().fields().iter().map(|f| f.name().clone()).collect();
        if let Some(c) = t.partition.iter().find(|c| !names.contains(c)) {
            bail!("PARTITION_BY {c}: no such column ({})", names.join(", "));
        }
        let text = |c: &String| coalesce(vec![cast(ident(c), datafusion::arrow::datatypes::DataType::Utf8), lit(NULL_FOLDER)]).alias(c);
        df = df.select(names.iter().map(|c| if t.partition.contains(c) { text(c) } else { ident(c) }).collect::<Vec<_>>())?;
    }
    let write = DataFrameWriteOptions::new().with_single_file_output(!t.folder()).with_partition_by(t.partition.clone());
    let out = match t.format.as_str() {
        "parquet" => {
            let mut parquet = own(datafusion::common::config::TableParquetOptions::default(), "parquet", options)?;
            if let Some(c) = options.get("compression") {
                let level = regex::Regex::new(r"^(zstd|gzip|brotli)\(\d+\)$").expect("a regex");
                parquet.global.compression = Some(match c.to_lowercase().as_str() {
                    "zstd" => "zstd(3)".into(),
                    "gzip" => "gzip(6)".into(),
                    "brotli" => "brotli(4)".into(),
                    c @ ("snappy" | "lz4" | "lz4_raw" | "uncompressed") => c.into(),
                    c if level.is_match(c) => c.into(), // (DataFusion's: a level)
                    c => bail!("COMPRESSION {c}: snappy, zstd, gzip, brotli, lz4 or uncompressed (zstd(1…22), gzip(0…9), brotli(0…11) at a level)"),
                });
            }
            if let Some(n) = options.get("row_group_size") {
                parquet.global.max_row_group_size = n.parse().context("ROW_GROUP_SIZE is a number of rows")?;
            }
            df.write_parquet(&t.to, write, Some(parquet)).await?
        }
        "json" => {
            let json = own(datafusion::common::config::JsonOptions::default(), "json", options)?;
            df.write_json(&t.to, write, Some(json)).await?
        }
        "csv" => {
            let mut csv = own(datafusion::common::config::CsvOptions::default(), "csv", options)?.with_has_header(options.get("header").is_none_or(|h| !h.eq_ignore_ascii_case("false")));
            if let Some(d) = options.get("delimiter").or(options.get("delim")).or(options.get("sep")) {
                csv = csv.with_delimiter(d.bytes().next().filter(|_| d.len() == 1).context("DELIMITER is one character")?);
            }
            df.write_csv(&t.to, write, Some(csv)).await?
        }
        "arrow" => {
            // (Arrow's file format, as DataFrame's write_json writes JSON)
            use datafusion::datasource::file_format::{arrow::ArrowFormatFactory, format_as_file_type};
            let single = std::collections::HashMap::from([("single_file_output".to_string(), (!t.folder()).to_string())]);
            let (state, plan) = df.into_parts();
            let plan = datafusion::logical_expr::LogicalPlanBuilder::copy_to(plan, t.to.clone(), format_as_file_type(std::sync::Arc::new(ArrowFormatFactory::new())), single, t.partition.clone())?.build()?;
            DataFrame::new(state, plan).collect().await?
        }
        f => bail!("COPY … TO as {f}: parquet, csv, json or arrow"),
    };
    use datafusion::arrow::array::AsArray;
    Ok(out.iter().map(|b| b.column(0).as_primitive::<datafusion::arrow::datatypes::UInt64Type>().iter().flatten().sum::<u64>()).sum())
}

// ---------------------------------------------------------------- COPY INTO: each file loaded once

/// Where `COPY INTO` reads from: a path (a file, a glob, a folder of the format's files), or a query
/// with one file reference in it, run once for each file that reference lists.
pub enum Source {
    Path(String),
    Query(String),
}

/// The options `COPY INTO` takes: the reader's own (`read_*`'s names) and FORCE.
const LOAD_KNOWN: &[&str] = &["format", "header", "delim", "sep", "delimiter", "quote", "escape", "comment", "new_line", "force"];

/// How long a file's load record is kept, and so how old a file may be and still be loaded again
/// without FORCE: a file last changed before this window is skipped, as its record may be gone
/// (Snowflake's 64 days of load metadata). `PONDRA_LOAD_RETENTION_SECS` shortens it, for tests; it is
/// read once.
fn retention_ms() -> u64 {
    static MS: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *MS.get_or_init(|| crate::env::parse::<u64>("PONDRA_LOAD_RETENTION_SECS").map_or(64 * 24 * 3600 * 1000, |s| s * 1000))
}

/// A file loaded into a table, as its job names it (`load:` and this JSON). The folder, not the
/// table's name, so that renames, swaps and UNDROP keep the loads with the rows they made; `run` is
/// set by FORCE, so that the job's mark doesn't dedupe the load.
#[derive(Serialize, Deserialize)]
pub struct Load {
    pub folder: String,
    pub file: String,
    pub version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run: Option<String>,
}

/// The record of a file loaded: `ld/{folder}/{sha256 of the file}`, written in the commit that
/// records its rows (`adopt::file`). `job` is the job mark it was loaded by (`p/job:{job}`).
#[derive(Serialize, Deserialize, Clone)]
pub struct Entry {
    pub file: String,
    pub version: String,
    pub rows: u64,
    pub at: u64,
    pub job: String,
}

/// `COPY INTO t FROM 's3://b/x/*.csv' (FORMAT csv, HEADER true)`: every file under the path, glob or
/// folder (or the one file reference of `FROM (SELECT … FROM '…')`) that this table hasn't loaded in
/// that version. Each file is one bulk INSERT whose job names the table's folder, the file and its
/// version, so a retry, a second statement at once or a failover loads it once (`write::on_node_as`).
/// Its record is written in the commit that records its rows (`adopt::file`, `entries`).
///
/// A file loaded is skipped when its record has the same version, or when it was last changed
/// before the retention window (its record may be gone); FORCE true loads every file again. A file
/// column the table lacks is refused by name; a table column the file lacks is NULL or its DEFAULT,
/// as INSERT does it. Refused: a view, a keyed table, a table of another lake.
///
/// Invariant 337: a file is loaded into a table once a version (`copy::copy_into`, `adopt::file`):
/// its load is a bulk INSERT whose job names the table's folder, the file and its version, so a
/// retry, a second statement at once or a failover records it once; its record (`ld/{folder}/`)
/// is written after the commit that records its rows, follows the rows' folder through renames,
/// swaps and drops, and goes with its job mark after the retention window, when a file older than
/// that isn't loaded again unless FORCE. `harness.py loads`.
pub async fn copy_into(app: &App, table: &str, from: &Source, options: &BTreeMap<String, String>) -> Result<serde_json::Value> {
    let lake = &app.lake;
    if let Some(k) = options.keys().find(|k| !LOAD_KNOWN.contains(&k.as_str())) {
        bail!("COPY INTO has no option {k}: {} (the reader's options, as read_csv takes them)", LOAD_KNOWN.join(", "));
    }
    let force = options.get("force").is_some_and(|v| !v.eq_ignore_ascii_case("false"));
    let (other, name) = crate::ddl::resolve(lake, table).await?;
    ensure!(other.is_none(), "COPY INTO {table}: a table of an attached lake is loaded by that lake's own node");
    let Some(meta) = lake.cat.get::<TableMeta>(&table_key(&name)).await? else {
        let view = lake.cat.get::<serde_json::Value>(&crate::views::view_key(&name)).await?.is_some() // (a materialized view)
            || lake.cat.get::<serde_json::Value>(&crate::ddl::query_key(&name)).await?.is_some(); // (a view: its stored query)
        bail!("no table {table}{}", if view { ": a view isn't loaded into (COPY INTO loads a table)" } else { "" });
    };
    ensure!(meta.key.is_empty(), "{table} is a keyed table: INSERT INTO {table} SELECT … FROM '…' upserts each run");
    let folder = meta.folder(&name).to_string();
    let (shown, positional, pieces) = plan(lake, from, options).await?;
    let known: BTreeMap<String, Entry> = lake.cat.scan::<Entry>(&format!("ld/{folder}/"), &format!("ld/{folder}0")).await?.into_iter().collect();
    if app.seq.is_some() {
        expire(app, &known).await?; // (the leader keeps the records as far as the retention: the next statement finds them gone)
    }
    let run = force.then(|| uuid::Uuid::new_v4().to_string());
    let cols: Vec<String> = meta.logical_columns().into_iter().map(|(c, _)| c).collect();
    let (now, mut loaded) = (now_ms(), vec![]);
    for p in pieces {
        let done = known.get(&ld_key(&folder, &p.url)).is_some_and(|e| e.version == p.version);
        let old = p.modified.saturating_add(retention_ms()) < now;
        if !force && (done || old) {
            continue;
        }
        let names = columns_of(lake, &p.query, positional, &cols, table, &p.url).await?;
        let load = Load { folder: folder.clone(), file: p.url.clone(), version: p.version.clone(), run: run.clone() };
        let out = crate::write::on_node_as(app, Stmt::InsertInto(table.to_string(), names, p.query), Some(job_of(&load)), owner()).await?;
        if let Some(rows) = out["rows"].as_u64() {
            loaded.push(serde_json::json!({"file": p.url, "version": p.version, "rows": rows}));
        }
    }
    let rows: u64 = loaded.iter().map(|l| l["rows"].as_u64().unwrap_or(0)).sum();
    let mut out = serde_json::json!({"rows": rows, "loaded": loaded});
    if out["loaded"].as_array().is_some_and(|l| l.is_empty()) {
        out["notice"] = format!("every file under '{shown}' was loaded before; FORCE true loads them again").into();
    }
    Ok(out)
}

/// One file a load reads: its URL, its version (its e-tag, else size and time), when it last
/// changed (ms), and the query that reads it, expanded.
struct Piece {
    url: String,
    version: String,
    modified: u64,
    query: String,
}

/// The source's files, as `(what to show, headerless CSV?, the files)`. A path's reader is its
/// FORMAT, else its extension's, with the options given; a query's is its file reference's own.
async fn plan(lake: &Lake, from: &Source, options: &BTreeMap<String, String>) -> Result<(String, bool, Vec<Piece>)> {
    let (shown, spec, query_of) = match from {
        Source::Path(p) => {
            let p = if scheme(p).is_none() { crate::ddl::full(p)? } else { p.clone() };
            let (format, mut reader) = match options.get("format") {
                Some(f) => (f.to_lowercase(), BTreeMap::new()),
                None => format_of(p.trim_end_matches('/')).context("COPY INTO: which FORMAT? (csv, parquet, json or arrow)")?,
            };
            reader.extend(options.iter().filter(|(k, _)| !matches!(k.as_str(), "format" | "force")).map(|(k, v)| (k.clone(), v.clone())));
            let query_of = (format.clone(), reader.clone());
            (p.clone(), crate::ext::Spec { urls: vec![p], format, options: reader }, Some(query_of))
        }
        Source::Query(q) => {
            ensure!(options.keys().all(|k| k == "force"), "COPY INTO (SELECT …): the reader's options go in the query: read_csv('…', header => true)");
            let expanded = crate::routines::expand(lake, q).await?;
            let names = crate::ext::names(&expanded);
            ensure!(names.len() == 1, "COPY INTO (SELECT …) names {} files: one outside source (FROM 'path'), loaded file by file", names.len());
            let spec = crate::ext::spec(&names[0]).context("COPY INTO (SELECT …): its FROM is a file")?;
            (spec.urls[0].clone(), spec, None)
        }
    };
    let positional = query_of.as_ref().is_some_and(|(f, r)| f == "csv" && r.get("header").is_some_and(|h| h == "false"));
    let listed = crate::ext::objects(lake, &spec).await?;
    ensure!(!listed.is_empty(), "no files at {shown}");
    let mut pieces = vec![];
    for (url, o) in listed {
        let query = match (&query_of, from) {
            (Some((format, reader)), _) => reader_query(format, &url, reader),
            (None, Source::Query(q)) => {
                let lit = format!("'{}'", spec.urls[0].replace('\'', "''"));
                ensure!(q.matches(&lit).count() == 1, "COPY INTO (SELECT …): the file reference {lit} must appear once, as written");
                q.replacen(&lit, &format!("'{}'", url.replace('\'', "''")), 1)
            }
            (None, Source::Path(_)) => unreachable!("a path has its reader"),
        };
        let query = crate::routines::expand(lake, &query).await?;
        let version = o.e_tag.clone().unwrap_or_else(|| format!("{}-{}", o.size, o.last_modified.timestamp_millis()));
        pieces.push(Piece { url, version, modified: o.last_modified.timestamp_millis().max(0) as u64, query });
    }
    Ok((shown, positional, pieces))
}

/// `SELECT * FROM read_csv('file', header => true)`: one file, read as a table's reader reads it.
fn reader_query(format: &str, file: &str, reader: &BTreeMap<String, String>) -> String {
    let args: String = reader.iter().map(|(k, v)| {
        let v = if v == "true" || v == "false" || v.parse::<f64>().is_ok() { v.clone() } else { format!("'{}'", v.replace('\'', "''")) };
        format!(", {k} => {v}")
    }).collect();
    format!("SELECT * FROM read_{format}('{}'{args})", file.replace('\'', "''"))
}

/// The columns a file's query gives, as the table's names they load into: by name, or by position
/// for a headerless CSV. Refused by name: a column the table lacks, with both ways out.
async fn columns_of(lake: &Lake, query: &str, positional: bool, cols: &[String], table: &str, url: &str) -> Result<Vec<String>> {
    let df = crate::query::sql(&crate::query::session(lake, query, "").await?, query).await?;
    let given: Vec<String> = df.schema().fields().iter().map(|f| f.name().clone()).collect();
    if positional {
        ensure!(given.len() <= cols.len(), "{url} has {} columns, {table} has {}", given.len(), cols.len());
        return Ok(cols[..given.len()].to_vec());
    }
    if let Some(c) = given.iter().find(|c| !cols.contains(c)) {
        bail!("{table} has no column {c} (in {url}): COPY INTO {table} FROM (SELECT … FROM '…') to choose the columns, or ALTER TABLE {table} ADD COLUMN {c} …");
    }
    Ok(given)
}

fn ld_key(folder: &str, file: &str) -> String {
    let hash = aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, file.as_bytes());
    format!("ld/{folder}/{}", hash.as_ref().iter().map(|b| format!("{b:02x}")).collect::<String>())
}

fn job_of(load: &Load) -> String { format!("load:{}", serde_json::to_string(load).expect("a load")) }

/// The load a job names, if it is one (`adopt::file` records it).
pub fn load_of(job: &str) -> Option<Load> { serde_json::from_str(job.strip_prefix("load:")?).ok() }

/// Leader, for the commit that records a file's rows (`adopt::file` puts these into its `Filing`):
/// the file's record, and the job mark of the version it replaces, which goes with it. A retry of
/// the same job is answered "loaded already" before the commit, so nothing is written twice.
pub async fn entries(lake: &Lake, load: &Load, job: &str, rows: u64) -> Result<(Vec<(String, Vec<u8>)>, Vec<String>)> {
    let key = ld_key(&load.folder, &load.file);
    let old = lake.cat.get::<Entry>(&key).await?.filter(|e| e.job != job);
    let entry = Entry { file: load.file.clone(), version: load.version.clone(), rows, at: now_ms(), job: job.to_string() };
    let marks: Vec<String> = old.iter().map(|e| producer_key(&format!("job:{}", e.job))).collect();
    Ok((vec![(key, json(&entry))], marks))
}

/// Leader: the records a folder keeps past the retention, and their job marks, go in one commit.
async fn expire(app: &App, known: &BTreeMap<String, Entry>) -> Result<()> {
    let cutoff = now_ms().saturating_sub(retention_ms());
    let old: Vec<(&String, &Entry)> = known.iter().filter(|(_, e)| e.at < cutoff).collect();
    if old.is_empty() {
        return Ok(());
    }
    let _guard = app.lock.lock().await;
    let deletes: Vec<String> = old.iter().flat_map(|(k, e)| [(*k).clone(), producer_key(&format!("job:{}", e.job))]).collect();
    app.lake.cat.commit(vec![(QUIET.into(), json(&true))], &deletes).await?;
    forget_producers(old.iter().map(|(_, e)| format!("job:{}", e.job)));
    Ok(())
}

/// Leader, inside a commit that lets folders go (`tier::expire` drops a dropped table kept past its
/// time): the records of folders no table uses any more, and their job marks, as keys to delete.
pub async fn gone(lake: &Lake, folders: &[String]) -> Result<Vec<String>> {
    let (mut keys, mut jobs) = (vec![], vec![]);
    for f in folders {
        for (k, e) in lake.cat.scan::<Entry>(&format!("ld/{f}/"), &format!("ld/{f}0")).await? {
            keys.push(k);
            keys.push(producer_key(&format!("job:{}", e.job)));
            jobs.push(format!("job:{}", e.job));
        }
    }
    forget_producers(jobs);
    Ok(keys)
}

/// `pondra.loads`, a system table (`query.rs`, as `pondra.tables` is): each file loaded into a
/// table the caller may read, with its version and rows and when it was loaded. Named only where
/// the text names it.
pub async fn tables(lake: &Lake, text: &str) -> Result<Vec<(&'static str, Arc<dyn datafusion::catalog::TableProvider>)>> {
    use datafusion::arrow::array::{ArrayRef, StringArray, TimestampMillisecondArray, UInt64Array};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    if !text.to_lowercase().contains("pondra.loads") {
        return Ok(vec![]);
    }
    let mut by_folder: BTreeMap<String, Vec<(String, String)>> = BTreeMap::new();
    for (key, m) in lake.cat.scan::<TableMeta>("t/", "t0").await? {
        let name = key[2..].to_string();
        let folder = m.folder(&name).to_string();
        let (schema, table) = crate::ddl::split(&name);
        by_folder.entry(folder).or_default().push((schema.to_string(), table.to_string()));
    }
    let (mut schema, mut table, mut file, mut version, mut rows, mut at) = (vec![], vec![], vec![], vec![], vec![], vec![]);
    for (key, e) in lake.cat.scan::<Entry>("ld/", "ld0").await? {
        let folder = key[3..].rsplit_once('/').map_or("", |(f, _)| f);
        for (s, t) in by_folder.get(folder).into_iter().flatten() {
            if !crate::auth::current().is_none_or(|p| p.may("select", &crate::ddl::join(s, t))) {
                continue;
            }
            schema.push(s.clone());
            table.push(t.clone());
            file.push(e.file.clone());
            version.push(e.version.clone());
            rows.push(e.rows);
            at.push(e.at as i64);
        }
    }
    let text_col = |v: Vec<String>| Arc::new(StringArray::from(v)) as ArrayRef;
    let batch = RecordBatch::try_from_iter(vec![
        ("schema", text_col(schema)),
        ("name", text_col(table)),
        ("file", text_col(file)),
        ("version", text_col(version)),
        ("rows", Arc::new(UInt64Array::from(rows)) as ArrayRef),
        ("loaded_at", Arc::new(TimestampMillisecondArray::from(at)) as ArrayRef),
    ])?;
    let table: Arc<dyn datafusion::catalog::TableProvider> = Arc::new(MemTable::try_new(batch.schema(), vec![vec![batch]])?);
    Ok(vec![("loads", table)])
}
