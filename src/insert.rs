//! Bulk `INSERT INTO t SELECT …`, from any machine. Whoever runs the statement does the work —
//! runs the query and writes the Parquet into the bucket — and the lake only has to record the new
//! files: one catalog commit, the leader's job (`record`). `pondra sql` on a laptop hands them to
//! the running leader or, when nobody leads, records them itself (`from_cli`).
use crate::cluster::{alive, claim, http, latest, mark_alive, release};
use crate::store::*;
use anyhow::{bail, ensure, Result};
use datafusion::prelude::SessionContext;
use serde::{Deserialize, Serialize};
use serde_json::{json as j, Value};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// The files one INSERT wrote (`job`: a retried job is recorded once).
#[derive(Serialize, Deserialize)]
pub struct Files {
    table: String,
    job: String,
    columns: Vec<(String, String)>,
    files: Vec<DataFile>,
}

/// `INSERT INTO t <query>` → (t, query); anything else → None.
pub fn parse(sql: &str) -> Option<(String, String)> {
    use datafusion::sql::sqlparser::{ast::*, dialect::GenericDialect, parser::Parser};
    match Parser::parse_sql(&GenericDialect {}, sql).ok()?.as_slice() {
        [Statement::Insert(Insert { table: TableObject::TableName(name), source: Some(q), columns, .. })] if columns.is_empty() => Some((name.to_string(), q.to_string())),
        _ => None,
    }
}

/// Run the query here and write its rows as Parquet files into the table's folder (None: this
/// job was already recorded).
pub async fn write(lake: &Lake, ctx: SessionContext, table: &str, query: &str, job: &str) -> Result<Option<Files>> {
    if lake.cat.get::<u64>(&producer_key(&format!("job:{job}"))).await?.is_some() {
        return Ok(None);
    }
    let meta = lake.cat.get::<TableMeta>(&table_key(table)).await?;
    ensure!(meta.is_none_or(|m| m.key.is_empty()), "insert into keyed tables goes through /append");
    let df = ctx.sql(query).await?;
    let columns = df.schema().fields().iter().map(|f| (f.name().clone(), f.data_type().to_string())).collect();
    let files = crate::tier::write_stream(lake, table, df.execute_stream().await?, 1_000_000, &[]).await?;
    Ok(Some(Files { table: table.into(), job: job.into(), columns, files }))
}

/// Leader: record an INSERT's files in one commit, creating the table if it's new.
pub async fn record(lake: &Lake, f: Files) -> Result<Value> {
    let producer = producer_key(&format!("job:{}", f.job));
    if lake.cat.get::<u64>(&producer).await?.is_some() {
        return Ok(j!({"duplicate": true})); // the same job finished concurrently
    }
    let new = || TableMeta { columns: f.columns.clone(), publish: default_publish(), ..Default::default() };
    let mut meta = lake.cat.get::<TableMeta>(&table_key(&f.table)).await?.unwrap_or_else(new);
    let types = |c: &[(String, String)]| c.iter().map(|(_, t)| t.clone()).collect::<Vec<_>>();
    ensure!(meta.key.is_empty(), "insert into keyed tables goes through /append");
    ensure!(types(&meta.columns) == types(&f.columns), "query columns {:?} don't match table {}", f.columns, f.table);
    let rows: u64 = f.files.iter().map(|f| f.rows).sum();
    meta.files.extend(f.files);
    lake.cat.commit(vec![(table_key(&f.table), json(&meta)), (producer, json(&1u64))], &[]).await?;
    Ok(j!({"rows": rows}))
}

/// `pondra sql --dir … "INSERT INTO t …"` on any machine: this process runs the query (local
/// files too: `SELECT * FROM 'jan.parquet'`) and writes the Parquet; then the running leader
/// records the files. When nobody leads, this process leads for the moment it takes to record
/// them: it claims a term like a node would, so a node starting meanwhile waits for it, and never
/// takes over from a live leader (one it can't reach is an error, not a takeover).
pub async fn from_cli(dir: &str, table: &str, query: &str) -> Result<Value> {
    let job = std::env::var("PONDRA_JOB").unwrap_or_else(|_| uuid::Uuid::new_v4().to_string());
    std::env::set_var("PONDRA_JOB", &job); // (if fenced, the process restarts: the same job again)
    let job = job.as_str();
    let here = |lake: Arc<Lake>| async move {
        let ctx = crate::query::session(&lake, query, "").await?.enable_url_table();
        write(&lake, ctx, table, query, job).await
    };
    // (A lake with no catalog yet can't be read: then the files are written once this process leads.)
    let mut files = match Lake::open(dir, false, false).await {
        Ok(lake) => Some(here(lake).await?.ok_or(Duplicate)?),
        Err(_) => None,
    };
    let (store, deadline) = (open_store(dir)?.1, Instant::now() + Duration::from_secs(60));
    loop {
        match latest(&store).await? {
            Some(t) if alive(&store, &t).await => {
                if !t.addr.is_empty() {
                    let f = match &files {
                        Some(f) => f,
                        None => files.insert(here(Lake::open(dir, false, false).await?).await?.ok_or(Duplicate)?),
                    };
                    match http().post(format!("http://{}/cluster/files", t.addr)).json(f).send().await {
                        Ok(r) if r.status().is_success() => return Ok(r.json().await?),
                        Ok(r) => bail!("the leader at {}: {}", t.addr, r.text().await?),
                        Err(e) if Instant::now() > deadline => bail!("a leader runs at {} but this machine can't reach it ({e}): run the INSERT where it can, or join the cluster", t.addr),
                        Err(_) => {} // (it may have just stopped: its mark in the bucket goes stale soon)
                    }
                }
                tokio::time::sleep(Duration::from_secs(1)).await; // (or another `pondra sql` is recording: wait)
            }
            t => {
                let Some(term) = claim(&store, t.map_or(1, |t| t.n + 1), "").await? else { continue };
                let s = store.clone();
                let marks = tokio::spawn(async move {
                    loop {
                        tokio::time::sleep(Duration::from_secs(10)).await;
                        let _ = mark_alive(&s, term.n).await;
                    }
                });
                let lake = Lake::open(dir, true, false).await?; // (the catalog's writer: fences any older one)
                crate::replica::recover(&lake, "", term.n, None).await?;
                let f = match files.take() {
                    Some(f) => f,
                    None => here(lake.clone()).await?.ok_or(Duplicate)?,
                };
                let out = record(&lake, f).await?;
                lake.cat.checkpoint().await?; // (so every node's view has it without replaying the WAL)
                marks.abort();
                release(&store, term.n).await; // the next writer or node doesn't have to wait
                return Ok(out);
            }
        }
    }
}

/// This job was already recorded (a retry).
#[derive(Debug)]
pub struct Duplicate;

impl std::fmt::Display for Duplicate {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result { write!(f, "duplicate job") }
}

impl std::error::Error for Duplicate {}
