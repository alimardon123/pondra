//! `COPY … TO` (ADR-026): a query's rows written outside the lake — one file, a folder of files,
//! Hive-style folders by `PARTITION_BY`, or a Kafka topic. A folder written from a big table is
//! written by every node at once, each its own share's files (`spmd::copy`): nothing crosses
//! between the nodes but how many rows each wrote.
use crate::ext::{covering, format_of, list, owner, register, scheme, uncovered, NULL_FOLDER};
use crate::store::Lake;
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
    if let Some(k) = options.keys().find(|k| !KNOWN.contains(&k.as_str())) {
        bail!("COPY … TO has no option {k}: {}", KNOWN.join(", "));
    }
    let on = |k: &str| options.get(k).is_some_and(|v| !v.eq_ignore_ascii_case("false"));
    let lakes: Vec<String> = std::iter::once(lake.url.clone()).chain(lake.attached.read().unwrap().iter().map(|(_, o)| o.url.clone())).collect();
    let full = crate::ddl::full(to).unwrap_or_else(|_| to.to_string());
    if let Some(l) = lakes.iter().find(|l| full.trim_end_matches('/') == l.as_str() || full.starts_with(&format!("{l}/"))) {
        bail!("COPY … TO {to}: that's inside the lake at {l}, whose files are its own (write beside it, or INSERT into a table)");
    }
    let remote = scheme(to).is_some_and(|s| s != "file");
    match remote {
        false => ensure!(owner(), "COPY … TO {to}: a file on the node's machine; only the program that started the node (the shell) writes those"),
        true => {
            ensure!(owner() || covering(&list(lake).await?, to).is_some(), "{}", uncovered(to));
            register(lake, to).await?;
        }
    }
    let query = crate::routines::expand(lake, query).await?; // (the files, macros and FROM-first queries it reads)
    if to.starts_with("kafka://") {
        let df = crate::query::session(lake, &query, "").await?.sql_with_options(&query, crate::query::read_only()).await?;
        return crate::kafka_client::copy_to(lake, df, to, options).await; // (rows as records: a topic's)
    }
    ensure!(!options.contains_key("key"), "KEY is a Kafka topic's (COPY … TO 'kafka://brokers/topic')");
    let format = match options.get("format") {
        Some(f) => f.to_lowercase(),
        None => format_of(to.trim_end_matches('/')).map(|f| f.0).with_context(|| format!("COPY … TO {to}: which FORMAT? (parquet, csv, json, delta, iceberg)"))?,
    };
    if format == "delta" || format == "iceberg" {
        ensure!(!options.contains_key("partition_by"), "COPY … TO as {format}: PARTITION_BY (a partitioned {format} table): not yet");
        let df = crate::query::session(lake, &query, "").await?.sql_with_options(&query, crate::query::read_only()).await?;
        return crate::write_outside::copy_table(lake, df, to, &format, on("append"), on("overwrite")).await; // (a table, not files: ADR-028)
    }
    ensure!(["parquet", "csv", "json"].contains(&format.as_str()), "COPY … TO as {format}: parquet, csv, json, delta or iceberg");
    ensure!(format == "parquet" || !options.contains_key("compression") && !options.contains_key("row_group_size"), "COPY … TO as {format}: COMPRESSION and ROW_GROUP_SIZE are Parquet's (files are read as they are: not compressed)");
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
    let df = crate::query::session(lake, &query, "").await?.sql_with_options(&query, crate::query::read_only()).await?;
    let rows = write(df, &target).await?;
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
            let mut parquet = datafusion::common::config::TableParquetOptions::default();
            if let Some(c) = options.get("compression") {
                parquet.global.compression = Some(match c.to_lowercase().as_str() {
                    "zstd" => "zstd(3)".into(),
                    "gzip" => "gzip(6)".into(),
                    "brotli" => "brotli(4)".into(),
                    c @ ("snappy" | "lz4" | "lz4_raw" | "uncompressed") => c.into(),
                    c => bail!("COMPRESSION {c}: snappy, zstd, gzip, brotli, lz4 or uncompressed"),
                });
            }
            if let Some(n) = options.get("row_group_size") {
                parquet.global.max_row_group_size = n.parse().context("ROW_GROUP_SIZE is a number of rows")?;
            }
            df.write_parquet(&t.to, write, Some(parquet)).await?
        }
        "json" => df.write_json(&t.to, write, None).await?,
        "csv" => {
            let mut csv = datafusion::common::config::CsvOptions::default().with_has_header(options.get("header").is_none_or(|h| !h.eq_ignore_ascii_case("false")));
            if let Some(d) = options.get("delimiter").or(options.get("delim")).or(options.get("sep")) {
                csv = csv.with_delimiter(d.bytes().next().filter(|_| d.len() == 1).context("DELIMITER is one character")?);
            }
            df.write_csv(&t.to, write, Some(csv)).await?
        }
        f => bail!("COPY … TO as {f}: parquet, csv or json"),
    };
    use datafusion::arrow::array::AsArray;
    Ok(out.iter().map(|b| b.column(0).as_primitive::<datafusion::arrow::datatypes::UInt64Type>().iter().flatten().sum::<u64>()).sum())
}
