//! `COPY … TO` (ADR-026): a query's rows written outside the lake — one file, a folder of files,
//! Hive-style folders by `PARTITION_BY`, or a Kafka topic. A folder written from a big table is
//! written by every node at once, each its own share's files (`spmd::copy`): nothing crosses
//! between the nodes but how many rows each wrote.
use crate::ext::{covering, list, owner, register, uncovered};
use crate::ext_names::{compression_in, compression_named, compression_of, format_of, name_options, scheme, NULL_FOLDER};
use crate::log::{forget_producers, now_ms};
use crate::app::App;
use crate::store::{json, producer_key, table_key, Lake, TableMeta, QUIET};
use crate::write::Stmt;
use std::sync::Arc;
use anyhow::{bail, ensure, Context, Result};
use datafusion::arrow::datatypes::DataType;
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
    if !owner() && crate::lake_files::relative("", to).is_some() {
        bail!(crate::codes::coded("42501", format!("COPY … TO {to}: a client's relative name is a file of the lake's own (PUT /files/…), which COPY doesn't write: write to a bucket (s3://, gs://, az://), or PUT the file")));
    }
    let remote = !crate::ext::on_disk(to);
    match remote {
        false => ensure!(owner(), crate::codes::coded("42501", format!("COPY … TO {to}: a path on the server's own disk, which a client writes only through a node it started itself (the `pondra` shell, `pondra sql`, `connect()` of a lake): write it there, or to a bucket (s3://, gs://, az://)"))),
        true => {
            ensure!(owner() || covering(&list(lake).await?, to).is_some(), crate::codes::coded("42501", uncovered(to)));
            register(lake, to).await?;
        }
    }
    let query = crate::routines::expand(lake, query).await?; // (the files, macros and FROM-first queries it reads)
    if to.starts_with("kafka://") {
        let df = crate::engine::sql(&crate::query::session(lake, &query, "").await?, &query).await?;
        return crate::kafka_client::copy_to(lake, df, to, options).await; // (rows as records: a topic's; not retried, as records sent are sent)
    }
    ensure!(!options.contains_key("key"), "KEY is a Kafka topic's (COPY … TO 'kafka://brokers/topic')");
    let format = match options.get("format") {
        Some(f) => f.to_lowercase(),
        None => format_of(to.trim_end_matches('/')).map(|f| f.0).with_context(|| format!("COPY … TO {to}: which FORMAT? (parquet, csv, json, delta, iceberg)"))?,
    };
    if format == "delta" || format == "iceberg" {
        ensure!(!options.contains_key("partition_by"), "COPY … TO as {format}: PARTITION_BY (a partitioned {format} table): not yet");
        let ctx = crate::query::session(lake, &query, "").await?;
        let (append, overwrite) = (on("append"), on("overwrite"));
        return crate::query::frugally(&ctx, |ctx| {
            let (query, format) = (&query, &format);
            async move {
                let df = crate::engine::sql(&ctx, query).await?;
                // (a new table's first version is put before its rows, so a try that ran out of memory left an empty table: its retry adds to it)
                crate::write_outside::copy_table(lake, &ctx, df, to, format, append || crate::query::retrying(&ctx), overwrite).await
            }
        }).await; // (a table, not files: ADR-028)
    }
    ensure!(["parquet", "csv", "json", "arrow"].contains(&format.as_str()), "COPY … TO as {format}: parquet, csv, json, arrow, delta or iceberg");
    let text = matches!(format.as_str(), "csv" | "json"); // (the formats a compression is written in as well as Parquet)
    ensure!(format == "parquet" || !options.contains_key("row_group_size") && (text || !options.contains_key("compression")), "COPY … TO as {format}: COMPRESSION and ROW_GROUP_SIZE are Parquet's (CSV and JSON take COMPRESSION gzip, zstd, bzip2 or xz)");
    if let Some((k, _)) = options.iter().find(|(k, v)| !KNOWN.contains(&k.as_str()) && writer(k, v) != Some(format.as_str())) {
        bail!("COPY … TO as {format}: {k} isn't {format}'s option");
    }
    // A CSV or JSON file is compressed as COMPRESSION says, or as the name's ending says (`out.csv.gz`). Named here, before a
    // folder that holds files is emptied, so a name the writer can't take is refused with the folder as it was.
    let mut options = options.clone();
    if text {
        if let Some(c) = options.get("compression").cloned().or_else(|| compression_of(to).map(String::from)) {
            compression_named(&c)?;
            options.insert("compression".into(), c);
        }
    }
    let partition: Vec<String> = options.get("partition_by").map(|p| p.split(',').map(|c| c.trim().to_string()).collect()).unwrap_or_default();
    let target = Target { to: to.into(), format, partition, options };
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
                    undo(lake, to, &kept).await?;
                }
            }
        }
    }
    let ctx = crate::query::session(lake, &query, "").await?; // (made once: a try that runs out of memory retries on a copy of it)
    let rows = crate::query::frugally(&ctx, |ctx| {
        let (query, target, kept) = (&query, &target, &kept);
        async move {
            if crate::query::retrying(&ctx) && target.folder() {
                // (what the first try wrote goes, as after a spread COPY that failed)
                undo(lake, &target.to, kept).await?;
            }
            write(crate::engine::sql(&ctx, query).await?, target).await
        }
    }).await?;
    Ok(serde_json::json!({"copied": rows, "to": to}))
}

/// Deletes what is listed under `to` now except `kept` (what the folder held before the statement, and keeps): what
/// an attempt that failed wrote.
async fn undo(lake: &Lake, to: &str, kept: &HashSet<object_store_df::path::Path>) -> Result<()> {
    let written = listed(lake, to).await?.into_iter().filter(|p| !kept.contains(p)).collect();
    delete(lake, to, written).await
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
            let mut json = own(datafusion::common::config::JsonOptions::default(), "json", options)?;
            json.compression = compression_in(options)?;
            df.write_json(&t.to, write, Some(json)).await?
        }
        "csv" => {
            let mut csv = own(datafusion::common::config::CsvOptions::default(), "csv", options)?.with_has_header(options.get("header").is_none_or(|h| !h.eq_ignore_ascii_case("false")));
            csv.compression = compression_in(options)?;
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
const LOAD_KNOWN: &[&str] = &["format", "header", "delim", "sep", "delimiter", "quote", "escape", "comment", "new_line", "all_varchar", "compression", "force"];

/// How long a file's load record is kept, and so how old a file may be and still be loaded again
/// without FORCE: a file last changed before this window is skipped, as its record may be gone
/// (Snowflake's 64 days of load metadata). `PONDRA_LOAD_RETENTION_SECS` shortens it, for tests; it is
/// read once.
pub(crate) fn retention_ms() -> u64 {
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
/// A CSV file is read as `read_csv` reads it: a line `\.` is data, as DuckDB and Postgres 18 read a file (the end
/// marker is COPY FROM STDIN's); a blank line is no row (Postgres and DuckDB read one as a NULL in a one-column file);
/// a file with no rows loads nothing. Such a file is listed in the answer with 0 rows, and nothing is recorded for it:
/// each later run lists it again with 0 rows, FORCE changes nothing, and `pondra.loads` doesn't show it. A CSV's text
/// and integer columns are read as the table's types (`read_as`), as Postgres's COPY reads each value: `007` stays
/// `007`, and `2.5` into an integer is refused, where the file's own inferred type would round it. A value that won't
/// go into its column is refused with Postgres's words and SQLSTATE (`casts::into`, `casts::parse_refusal`).
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
    let logical = meta.logical_columns();
    let cols: Vec<String> = logical.iter().map(|(c, _)| c.clone()).collect();
    let table_types: BTreeMap<String, String> = logical.into_iter().collect();
    let (now, mut loaded) = (now_ms(), vec![]);
    for p in pieces {
        let done = known.get(&ld_key(&folder, &p.url)).is_some_and(|e| e.version == p.version);
        let old = p.modified.saturating_add(retention_ms()) < now;
        if !force && (done || old) {
            continue;
        }
        let (given, names) = columns_of(lake, &p.query, positional, &cols, table, &p.url).await?;
        if names.is_empty() {
            // (a file with no rows: an empty one, or of blank lines, reads no columns; nothing is written or recorded for it)
            loaded.push(serde_json::json!({"file": p.url, "version": p.version, "rows": 0}));
            continue;
        }
        // A CSV's text and integer columns are read as the table's types, so the file is read again with them (none
        // when all_varchar reads every column as text already).
        let query = match &p.reader {
            Some((format, reader)) if format == "csv" && !reader.get("all_varchar").is_some_and(|v| v != "false") => {
                let types = typed_columns(&given, &names, &table_types);
                if types.is_empty() {
                    p.query
                } else {
                    crate::routines::expand(lake, &reader_query(format, &p.url, reader, &types)).await?
                }
            }
            _ => p.query,
        };
        let load = Load { folder: folder.clone(), file: p.url.clone(), version: p.version.clone(), run: run.clone() };
        let out = match crate::write::on_node_as(app, Stmt::InsertInto(table.to_string(), names, query.clone()), Some(job_of(&load)), owner()).await {
            Ok(out) => out,
            Err(e) => return Err(refused_at(lake, &query, &p.url, p.dialect, e).await),
        };
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
/// changed (ms), the query that reads it, expanded, and for a path its reader (format and options), to read it again.
struct Piece {
    url: String,
    version: String,
    modified: u64,
    query: String,
    reader: Option<(String, BTreeMap<String, String>)>,
    dialect: Option<Dialect>, // (how the file's CSV reader splits it, when the cell scan can follow it)
}

/// The source's files, as `(what to show, headerless CSV?, the files)`. A path's reader is its
/// FORMAT, else its extension's, with the options given; a query's is its file reference's own.
async fn plan(lake: &Lake, from: &Source, options: &BTreeMap<String, String>) -> Result<(String, bool, Vec<Piece>)> {
    let (shown, spec, query_of, dialect) = match from {
        Source::Path(p) => {
            // (a relative path is the owner's folder's, made whole here; a client's is read as `objects` maps it: the lake's files)
            let p = if scheme(p).is_none() && owner() { crate::ddl::full(p)? } else { p.clone() };
            let (format, mut reader) = match options.get("format") {
                Some(f) => (f.to_lowercase(), name_options(&f.to_lowercase(), &p)?),
                None => format_of(p.trim_end_matches('/')).context("COPY INTO: which FORMAT? (csv, parquet, json or arrow)")?,
            };
            reader.extend(options.iter().filter(|(k, _)| !matches!(k.as_str(), "format" | "force")).map(|(k, v)| (k.clone(), v.clone())));
            let dialect = (format == "csv").then(|| dialect_of(&reader)).flatten();
            let query_of = (format.clone(), reader.clone());
            (p.clone(), crate::ext_names::Spec { urls: vec![p], format, options: reader }, Some(query_of), dialect)
        }
        Source::Query(q) => {
            ensure!(options.keys().all(|k| k == "force"), "COPY INTO (SELECT …): the reader's options go in the query: read_csv('…', header => true)");
            let expanded = crate::routines::expand(lake, q).await?;
            let names = crate::ext_names::names(&expanded);
            ensure!(names.len() == 1, "COPY INTO (SELECT …) names {} files: one outside source (FROM 'path'), loaded file by file", names.len());
            let spec = crate::ext_names::spec(&names[0]).context("COPY INTO (SELECT …): its FROM is a file")?;
            // (only `SELECT * FROM 'file'` reads the file's columns as they are: any other query's column numbers are its own)
            let lit = format!("'{}'", spec.urls[0].replace('\'', "''"));
            let plain = q.trim().trim_end_matches(';').trim_end().eq_ignore_ascii_case(&format!("SELECT * FROM {lit}"));
            let dialect = (spec.format == "csv" && plain).then(|| dialect_of(&spec.options)).flatten();
            (spec.urls[0].clone(), spec, None, dialect)
        }
    };
    let positional = query_of.as_ref().is_some_and(|(f, r)| f == "csv" && r.get("header").is_some_and(|h| h == "false"));
    let listed = crate::ext::objects(lake, &spec).await?;
    ensure!(!listed.is_empty(), crate::codes::coded("42P01", format!("no file matches {shown}")));
    let mut pieces = vec![];
    for (url, o) in listed {
        let query = match (&query_of, from) {
            (Some((format, reader)), _) => reader_query(format, &url, reader, &[]),
            (None, Source::Query(q)) => {
                let lit = format!("'{}'", spec.urls[0].replace('\'', "''"));
                ensure!(q.matches(&lit).count() == 1, "COPY INTO (SELECT …): the file reference {lit} must appear once, as written");
                q.replacen(&lit, &format!("'{}'", url.replace('\'', "''")), 1)
            }
            (None, Source::Path(_)) => unreachable!("a path has its reader"),
        };
        let query = crate::routines::expand(lake, &query).await?;
        let version = o.e_tag.clone().unwrap_or_else(|| format!("{}-{}", o.size, o.last_modified.timestamp_millis()));
        pieces.push(Piece { url, version, modified: o.last_modified.timestamp_millis().max(0) as u64, query, reader: query_of.clone(), dialect });
    }
    Ok((shown, positional, pieces))
}

/// `SELECT * FROM read_csv('file', header => true)`: one file, read as a table's reader reads it, with the columns `types`
/// names (their file names and types) read as those types.
fn reader_query(format: &str, file: &str, reader: &BTreeMap<String, String>, types: &[(String, String)]) -> String {
    let args: String = reader.iter().map(|(k, v)| {
        let v = if v == "true" || v == "false" || v.parse::<f64>().is_ok() { v.clone() } else { format!("'{}'", v.replace('\'', "''")) };
        format!(", {k} => {v}")
    }).collect();
    let types = match types.is_empty() {
        true => String::new(),
        false => format!(", types => {{{}}}", types.iter().map(|(c, t)| format!("'{}': '{t}'", c.replace('\'', "''"))).collect::<Vec<_>>().join(", ")),
    };
    format!("SELECT * FROM read_{format}('{}'{args}{types})", file.replace('\'', "''"))
}

/// The type a CSV column is read as, when its table column is text or an integer: the table's type as SQL spells it, so
/// each value is read as Postgres's COPY reads it (`007` stays `007`; `2.5` into an integer is refused). None when the
/// file's own inferred type is that one already, or the table's column is another type, which is read and cast as before.
fn read_as(table_type: &str, inferred: &DataType) -> Option<String> {
    let (want, spelled) = match crate::answer::dtype(table_type).ok()? {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => (DataType::Utf8, "VARCHAR".to_string()), // (Arrow's CSV reader can't parse LargeUtf8)
        t if t.is_integer() => (t, crate::objects::sql_type(table_type)),
        _ => return None,
    };
    (*inferred != want).then_some(spelled)
}

/// The columns a CSV file is read again with (`reader_query`'s `types`), by the file's own names: each whose table column
/// `read_as` says to read as another type.
fn typed_columns(given: &[(String, DataType)], names: &[String], table_types: &BTreeMap<String, String>) -> Vec<(String, String)> {
    given.iter().zip(names).filter_map(|((file, inferred), table)| Some((file.clone(), read_as(table_types.get(table)?, inferred)?))).collect()
}

/// A CSV value the reader refused (`22P02`, `22003`) with the file and the line its cell starts on (`casts::parse_refusal_in`).
/// The INSERT's error has Postgres's words only (its stream lost the reader's text), so the file is read again by `query`, as
/// the INSERT reads it: each of its partitions (byte ranges, in file order) on its own task, a batch at a time (no batch is
/// kept). The reader's own error names the column and the value. The first refusal in partition order is the file's first
/// refusal; the later partitions are stopped there. The line is then the file's first cell of that column with that text
/// (`line_of`), when the file's reader is one `dialect` describes. Without one, or when the bytes can't be read, the file
/// alone: never a line that may be wrong. Any other error is kept as it is.
async fn refused_at(lake: &Lake, query: &str, file: &str, dialect: Option<Dialect>, e: anyhow::Error) -> anyhow::Error {
    if !matches!(crate::codes::of(&e), "22P02" | "22003") {
        return e;
    }
    let read = async {
        let ctx = crate::query::session(lake, query, "").await?;
        {
            // (each partition reads only its own byte range, as planned: with the sharing of files between partitions on, a
            // partition takes whichever range is next, so a partition's order is not the file's)
            let state = ctx.state_ref();
            state.write().config_mut().options_mut().execution.enable_file_stream_work_stealing = false;
        }
        let df = crate::engine::sql(&ctx, query).await?;
        let mut parts: Vec<_> = df
            .execute_stream_partitioned()
            .await?
            .into_iter()
            .map(|mut part| {
                crate::panics::task(async move {
                    while let Some(batch) = part.next().await {
                        batch?; // (dropped as it is read: one batch at a time)
                    }
                    anyhow::Ok(())
                })
            })
            .collect();
        let mut first = anyhow::Ok(());
        for part in parts.iter_mut() {
            match part.await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    first = Err(e);
                    break;
                }
                Err(joined) => {
                    first = Err(joined.into());
                    break;
                }
            }
        }
        for part in &parts {
            part.abort(); // (the partitions after the first refusal are not needed)
        }
        first
    };
    let Err(r) = read.await else { return e };
    let text = format!("{r:#}");
    let Some((column, value)) = crate::casts::refused_cell(&text) else { return e };
    let line = match dialect {
        Some(dialect) => line_of(lake, file, dialect, column, &value).await,
        None => None,
    };
    let place = match line {
        Some(line) => format!("{file}, line {line}"),
        None => file.to_string(),
    };
    match crate::casts::parse_refusal_in(&text, &place) {
        Some(refusal) => anyhow::Error::from(refusal),
        None => e,
    }
}

/// The line the file's first cell of column `column` (from 0) with the text `value` starts on, found in the file's bytes as
/// they stream from its store; the read stops there. The reader refuses the first such cell in file order (the same text
/// is refused wherever it is), so this is the cell it refused. None for a file that starts with a compression's magic
/// number (the reader doesn't read those), for bytes that can't be read, and for no such cell.
async fn line_of(lake: &Lake, file: &str, dialect: Dialect, column: usize, value: &str) -> Option<u64> {
    if value.is_empty() {
        return None; // (an empty value is read as null, never refused)
    }
    let (store, path) = crate::ext::store(lake, file).await.ok()?;
    let mut chunks = object_store_df::ObjectStoreExt::get(&store, &path).await.ok()?.into_stream();
    let mut cells = Cells::new(dialect, column, value);
    let (mut head, mut checked) = (Vec::new(), false);
    while let Some(chunk) = chunks.next().await {
        let chunk = chunk.ok()?;
        if checked {
            if let Some(line) = cells.push(&chunk) {
                return Some(line);
            }
            continue;
        }
        head.extend_from_slice(&chunk); // (the first bytes, until a compression's magic number can be told)
        if head.len() < 6 {
            continue;
        }
        checked = true;
        if compressed(&head) {
            return None;
        }
        if let Some(line) = cells.push(&head) {
            return Some(line);
        }
    }
    if !checked {
        if compressed(&head) {
            return None;
        }
        if let Some(line) = cells.push(&head) {
            return Some(line);
        }
    }
    cells.finish()
}

/// The magic numbers of the compressed formats DataFusion reads (gzip, bzip2, xz, zstd): a file that starts with one is not
/// text. (bzip2's is `BZh` and a block size, a digit.)
fn compressed(head: &[u8]) -> bool {
    let magics: [&[u8]; 3] = [&[0x1f, 0x8b], &[0xfd, b'7', b'z', b'X', b'Z', 0x00], &[0x28, 0xb5, 0x2f, 0xfd]];
    magics.iter().any(|m| head.starts_with(m)) || (head.len() >= 4 && head.starts_with(b"BZh") && head[3].is_ascii_digit())
}

/// How a CSV reader splits a file into records and fields: the characters csv-core reads them with, as Arrow's reader gives
/// them (its delimiter, quote, escape, comment and terminator), and whether the first record is a header.
#[derive(Clone, Copy, Debug, PartialEq)]
struct Dialect {
    delim: u8,
    quote: u8,
    escape: Option<u8>,
    comment: Option<u8>,
    terminator: Option<u8>, // (None: CR, LF or CRLF, as the reader's default)
    header: bool,
}

/// A comma-separated file with a header and the reader's default characters.
#[cfg(test)]
const COMMA: Dialect = Dialect { delim: b',', quote: b'"', escape: None, comment: None, terminator: None, header: true };

/// The dialect of a CSV reader's options, as `ext_names::file_format` reads them: a comma unless `delim`, `sep` or `delimiter`
/// gives one character, a header unless `header` is false, and the quote, escape, comment and `new_line` the reader is given.
/// None for an option that isn't one character (or a `new_line` other than the reader's), and for a delimiter that is the quote.
fn dialect_of(reader: &BTreeMap<String, String>) -> Option<Dialect> {
    let o = |k: &str| reader.get(k).map(String::as_str);
    // (a line break is never a delimiter, quote, escape or comment: the line count takes them as breaks)
    let byte = |v: &str| (v.len() == 1 && !matches!(v, "\r" | "\n")).then(|| v.as_bytes()[0]);
    let terminator = match o("new_line") {
        None | Some("\n" | "\r\n" | "\\n" | "\\r\\n") => None,
        Some("\r" | "\\r") => Some(b'\r'),
        Some(_) => return None,
    };
    let delim = match o("delim").or(o("sep")).or(o("delimiter")) {
        None => b',',
        Some(d) => byte(d)?,
    };
    let quote = match o("quote") {
        None => b'"',
        Some(q) => byte(q)?,
    };
    let escape = match o("escape") {
        None => None,
        Some(e) => Some(byte(e)?),
    };
    let comment = match o("comment") {
        None => None,
        Some(c) => Some(byte(c)?),
    };
    (delim != quote).then_some(Dialect { delim, quote, escape, comment, terminator, header: o("header") != Some("false") })
}

/// Finds a CSV cell as the bytes arrive, in any split of them: the line its text starts on, for the first cell of column
/// `column` (from 0) whose text is `value`, outside the header. csv-core reads the fields, as Arrow's reader does, and keeps
/// its state from one piece to the next. What is left here is the line count: every CR, LF and CRLF is a line break, inside
/// quotes too, as an editor counts them. Blank lines and comment lines are skipped before a record, as csv-core skips them,
/// so a field's line is the line its first byte is on.
struct Cells {
    dialect: Dialect,
    column: usize,
    value: Vec<u8>,
    reader: csv_core::Reader,
    out: Vec<u8>,       // the field's unquoted text, as csv-core writes it, a piece at a time
    line: u64,          // the line the next byte is on
    cr: bool,           // the last byte counted was a CR, so a LF after it is the same break
    record_start: bool, // no byte of the record yet but its blank and comment lines
    comment: bool,      // in a comment line, to its LF
    field: usize,       // the record's field the next field is
    records: u64,       // the records ended so far (the header is the first)
    fresh: bool,        // no byte of the field read yet
    cell_line: u64,     // the line the field's first byte is on
    fits: bool,         // the field's text so far is the start of `value`
    len: usize,         // the field's text's length so far, one past `value`'s at most
}

impl Cells {
    fn new(dialect: Dialect, column: usize, value: &str) -> Self {
        let terminator = match dialect.terminator {
            None => csv_core::Terminator::CRLF,
            Some(t) => csv_core::Terminator::Any(t),
        };
        let reader = csv_core::ReaderBuilder::new()
            .delimiter(dialect.delim)
            .quote(dialect.quote)
            .escape(dialect.escape)
            .comment(dialect.comment)
            .terminator(terminator)
            .build();
        Cells { dialect, column, value: value.as_bytes().to_vec(), reader, out: vec![0; 1024], line: 1, cr: false, record_start: true, comment: false, field: 0, records: 0, fresh: true, cell_line: 1, fits: true, len: 0 }
    }

    /// The cell's line, once these bytes show it.
    fn push(&mut self, bytes: &[u8]) -> Option<u64> {
        self.feed(bytes, false)
    }

    /// The cell's line at the end of the bytes, when the last field is the cell.
    fn finish(&mut self) -> Option<u64> {
        self.feed(&[], true)
    }

    /// Reads fields from `input` until the cell is found or the input is used up. An empty input at the end (`end`) is the
    /// end of the file, which csv-core ends the last field at; any other empty input is no more bytes yet.
    fn feed(&mut self, mut input: &[u8], end: bool) -> Option<u64> {
        loop {
            if self.record_start {
                let n = self.skip(input);
                input = &input[n..];
                if self.record_start {
                    return None; // (nothing of the record yet: the rest comes with more input)
                }
            }
            if input.is_empty() && !end {
                return None;
            }
            if self.fresh {
                self.cell_line = self.line;
            }
            let mut out = std::mem::take(&mut self.out);
            let (res, nin, nout) = self.reader.read_field(input, &mut out);
            self.count(&input[..nin]);
            if nin > 0 {
                self.fresh = false;
            }
            input = &input[nin..];
            if self.field == self.column && self.fits {
                for &b in &out[..nout] {
                    self.text(b); // (only the column's cell's text is compared: no other field's)
                }
            }
            self.out = out;
            match res {
                csv_core::ReadFieldResult::OutputFull => {}
                csv_core::ReadFieldResult::Field { record_end } => {
                    let hit = self.end_field();
                    if record_end {
                        self.records += 1;
                        self.field = 0;
                        self.record_start = true;
                    } else {
                        self.field += 1;
                    }
                    self.fresh = true;
                    if hit.is_some() {
                        return hit;
                    }
                }
                csv_core::ReadFieldResult::InputEmpty | csv_core::ReadFieldResult::End => return None,
            }
        }
    }

    /// Skips the blank lines and comment lines csv-core skips at a record's start, in `input`, and clears `record_start` at
    /// the first byte of content. Returns how many bytes it used.
    fn skip(&mut self, input: &[u8]) -> usize {
        let mut i = 0;
        while i < input.len() {
            let b = input[i];
            i += 1;
            if self.comment {
                self.count_byte(b);
                self.comment = b != b'\n'; // (a comment runs to its LF, as csv-core has it)
            } else if Some(b) == self.dialect.comment {
                self.comment = true;
            } else if self.is_term(b) {
                self.count_byte(b); // (a blank line)
            } else {
                self.record_start = false;
                return i - 1;
            }
        }
        i
    }

    fn is_term(&self, b: u8) -> bool {
        match self.dialect.terminator {
            None => b == b'\r' || b == b'\n',
            Some(t) => b == t,
        }
    }

    fn count(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.count_byte(b);
        }
    }

    fn count_byte(&mut self, b: u8) {
        match b {
            b'\r' => {
                self.line += 1;
                self.cr = true;
            }
            b'\n' => {
                if !std::mem::take(&mut self.cr) {
                    self.line += 1; // (a CRLF is one break: its CR counted it)
                }
            }
            _ => self.cr = false,
        }
    }

    /// The field's text ends: its line when it is the cell. The next field starts empty.
    fn end_field(&mut self) -> Option<u64> {
        let header = self.dialect.header && self.records == 0;
        let hit = (self.field == self.column && !header && self.fits && self.len == self.value.len()).then_some(self.cell_line);
        self.fits = true;
        self.len = 0;
        hit
    }

    fn text(&mut self, b: u8) {
        self.fits &= self.value.get(self.len) == Some(&b);
        self.len = (self.len + 1).min(self.value.len() + 1);
    }
}

/// The columns a file's query gives, each with its inferred type, and the table's names they load into: by name, or by
/// position for a headerless CSV. Refused by name: a column the table lacks, with both ways out.
async fn columns_of(lake: &Lake, query: &str, positional: bool, cols: &[String], table: &str, url: &str) -> Result<(Vec<(String, DataType)>, Vec<String>)> {
    let df = crate::engine::sql(&crate::query::session(lake, query, "").await?, query).await?;
    let given: Vec<(String, DataType)> = df.schema().fields().iter().map(|f| (f.name().clone(), f.data_type().clone())).collect();
    if positional {
        ensure!(given.len() <= cols.len(), "{url} has {} columns, {table} has {}", given.len(), cols.len());
        let names = cols[..given.len()].to_vec();
        return Ok((given, names));
    }
    if let Some((c, _)) = given.iter().find(|(c, _)| !cols.contains(c)) {
        bail!("{table} has no column {c} (in {url}): COPY INTO {table} FROM (SELECT … FROM '…') to choose the columns, or ALTER TABLE {table} ADD COLUMN {c} …");
    }
    let names: Vec<String> = given.iter().map(|(c, _)| c.clone()).collect();
    Ok((given, names))
}

fn ld_key(folder: &str, file: &str) -> String {
    let hash = crate::crypto::sha256(file.as_bytes());
    format!("ld/{folder}/{}", crate::text::hex(&hash))
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
        let (schema, table) = crate::text::split(&name);
        by_folder.entry(folder).or_default().push((schema.to_string(), table.to_string()));
    }
    let (mut schema, mut table, mut file, mut version, mut rows, mut at) = (vec![], vec![], vec![], vec![], vec![], vec![]);
    for (key, e) in lake.cat.scan::<Entry>("ld/", "ld0").await? {
        let folder = key[3..].rsplit_once('/').map_or("", |(f, _)| f);
        for (s, t) in by_folder.get(folder).into_iter().flatten() {
            if !crate::auth::current().is_none_or(|p| p.may("select", &crate::text::join(s, t))) {
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

#[cfg(test)]
mod tests {
    use super::*;

    /// A CSV column is read as the table's type when the table's is text or an integer and the file's inferred type
    /// differs: VARCHAR for text, the integer's SQL type for an integer. Other table types are read as before.
    #[test]
    fn a_csv_column_is_read_as_its_text_or_integer_table_type() {
        assert_eq!(read_as("Utf8", &DataType::Int64), Some("VARCHAR".to_string()));
        assert_eq!(read_as("Utf8", &DataType::Utf8), None);
        assert_eq!(read_as("Int32", &DataType::Int64), Some("INT".to_string()));
        assert_eq!(read_as("Int64", &DataType::Int64), None, "the same type: the same query");
        assert_eq!(read_as("UInt8", &DataType::Float64), Some("TINYINT UNSIGNED".to_string()));
        assert_eq!(read_as("Decimal128(15, 2)", &DataType::Float64), None);
        assert_eq!(read_as("Date32", &DataType::Utf8), None);
    }

    /// The reader's options as they were, and `read_csv`'s `types` as a struct after them when there are some.
    #[test]
    fn the_types_go_to_the_reader_as_a_struct() {
        let reader = BTreeMap::from([("header".to_string(), "true".to_string())]);
        assert_eq!(reader_query("csv", "/x/a.csv", &reader, &[]), "SELECT * FROM read_csv('/x/a.csv', header => true)");
        assert_eq!(reader_query("csv", "/x/a.csv", &reader, &[("zip".to_string(), "VARCHAR".to_string())]), "SELECT * FROM read_csv('/x/a.csv', header => true, types => {'zip': 'VARCHAR'})");
    }

    /// The line a cell starts on, as the bytes arrive in pieces of `size` bytes.
    fn cell_at(text: &[u8], size: usize, dialect: Dialect, column: usize, value: &str) -> Option<u64> {
        let mut cells = Cells::new(dialect, column, value);
        text.chunks(size).find_map(|c| cells.push(c)).or_else(|| cells.finish())
    }

    /// A cell is found on the line its text starts on, in any split of the bytes: blank lines, CRLF, a lone CR, no final line
    /// break, a quoted line break, doubled quotes, text after a closing quote, a missing field, the header, the first match.
    #[test]
    fn a_cell_is_found_on_its_line_however_the_bytes_arrive() {
        let comma = COMMA;
        let cases: [(&[u8], usize, &str, Option<u64>); 12] = [
            (b"zip,n\n007,1\n\n008,2.5\n", 1, "2.5", Some(4)),
            (b"zip,n\r\n007,1\r\n\r\n008,2.5\r\n", 1, "2.5", Some(4)),
            (b"zip,n\r007,2.5\r", 1, "2.5", Some(2)),
            (b"zip,n\n007,2.5", 1, "2.5", Some(2)),
            (b"zip,n\n\"a\nb\",1\n008,2.5\n", 1, "2.5", Some(4)),
            (b"zip,n\n\"a\nb\",1\n008,2.5\n", 0, "a\nb", Some(2)),
            (b"zip,n\n\"say \"\"008\"\"\",1\n", 0, "say \"008\"", Some(2)),
            (b"zip,n\n\"x\"y,2.5\n", 0, "xy", Some(2)),
            (b"zip,n\n008\n007,2.5\n", 1, "2.5", Some(3)),
            (b"zip,n\n007,2.5\n008,2.5\n", 1, "2.5", Some(2)),
            (b"2.5,n\n007,2.5\n", 0, "2.5", None),
            (b"zip,n\n008,2.5\n", 1, "2.50", None),
        ];
        for (text, column, value, want) in cases {
            for size in 1..=text.len() {
                assert_eq!(cell_at(text, size, comma, column, value), want, "{:?} in pieces of {size}", String::from_utf8_lossy(text));
            }
        }
    }

    /// The dialect decides what a field is: a `;` file has one field a line under `,`; with no header the first record is a cell.
    #[test]
    fn the_dialect_decides_what_a_cell_is() {
        let semi = Dialect { delim: b';', ..COMMA };
        let comma = COMMA;
        let headerless = Dialect { header: false, ..COMMA };
        assert_eq!(cell_at(b"zip;n\n007;2.5\n", 3, semi, 1, "2.5"), Some(2));
        assert_eq!(cell_at(b"zip;n\n007;2.5\n", 3, comma, 1, "2.5"), None, "one field a line under ','");
        assert_eq!(cell_at(b"008,2.5\n", 2, headerless, 1, "2.5"), Some(1), "no header: the first record is a cell");
        assert_eq!(cell_at(b"008,2.5\n", 2, comma, 1, "2.5"), None, "a header is no cell");
    }

    /// The line a cell starts on, with the options csv-core reads (a quote, an escape, a comment, a CR terminator) as Arrow reads them.
    #[test]
    fn the_options_csv_core_reads_give_the_same_cells() {
        let cases: [(&[u8], Dialect, usize, &str, Option<u64>); 4] = [
            (b"zip,n\n'a,b',2.5\n", Dialect { quote: b'\'', ..COMMA }, 0, "a,b", Some(2)),
            (b"zip,n\n\"a\\\"b\",2.5\n", Dialect { escape: Some(b'\\'), ..COMMA }, 0, "a\"b", Some(2)),
            (b"zip,n\n# 007, 2.5\n007,2.5\n", Dialect { comment: Some(b'#'), ..COMMA }, 1, "2.5", Some(3)),
            (b"zip,n\r007,2.5\r", Dialect { terminator: Some(b'\r'), ..COMMA }, 1, "2.5", Some(2)),
        ];
        for (text, dialect, column, value, want) in cases {
            for size in 1..=text.len() {
                assert_eq!(cell_at(text, size, dialect, column, value), want, "{:?} in pieces of {size}", String::from_utf8_lossy(text));
            }
        }
    }

    /// A reader's options give its dialect, or none for an option that isn't a character or a delimiter that is the quote.
    #[test]
    fn a_readers_options_give_its_dialect_or_none() {
        let opts = |kv: &[(&str, &str)]| kv.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect::<BTreeMap<_, _>>();
        assert_eq!(dialect_of(&opts(&[])), Some(COMMA));
        assert_eq!(dialect_of(&opts(&[("header", "false")])), Some(Dialect { header: false, ..COMMA }));
        assert_eq!(dialect_of(&opts(&[("header", "true"), ("sep", ";")])), Some(Dialect { delim: b';', ..COMMA }));
        assert_eq!(dialect_of(&opts(&[("delimiter", "\t")])), Some(Dialect { delim: b'\t', ..COMMA }));
        assert_eq!(dialect_of(&opts(&[("delim", "ab")])), None, "one character");
        assert_eq!(dialect_of(&opts(&[("delim", "\"")])), None, "a delimiter that is the quote");
        assert_eq!(dialect_of(&opts(&[("quote", "\"")])), Some(COMMA));
        assert_eq!(dialect_of(&opts(&[("quote", "'")])), Some(Dialect { quote: b'\'', ..COMMA }));
        assert_eq!(dialect_of(&opts(&[("escape", "\\")])), Some(Dialect { escape: Some(b'\\'), ..COMMA }));
        assert_eq!(dialect_of(&opts(&[("comment", "#")])), Some(Dialect { comment: Some(b'#'), ..COMMA }));
        assert_eq!(dialect_of(&opts(&[("new_line", "\n")])), Some(COMMA));
        assert_eq!(dialect_of(&opts(&[("new_line", "\r")])), Some(Dialect { terminator: Some(b'\r'), ..COMMA }));
        assert_eq!(dialect_of(&opts(&[("new_line", "x")])), None, "a new_line the reader doesn't take");
    }

    /// The magic numbers the compressed formats start with, and what a text file starts with.
    #[test]
    fn a_compressed_file_is_told_by_its_magic_number() {
        assert!(compressed(&[0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00]), "gzip");
        assert!(compressed(&[0x28, 0xb5, 0x2f, 0xfd, 0x00, 0x00]), "zstd");
        assert!(compressed(&[0xfd, b'7', b'z', b'X', b'Z', 0x00]), "xz");
        assert!(compressed(b"BZh91AY&SY"), "bzip2");
        assert!(!compressed(b"zip,n\n"), "a CSV");
        assert!(!compressed(b"BZhost,n\n"), "a header that starts BZh");
    }
}
