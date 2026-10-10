//! Feeds (ADR-026): a materialized view over another Kafka cluster's topic
//! (`CREATE MATERIALIZED VIEW orders_in AS SELECT … FROM k.orders`) is kept up to date as records
//! arrive. Each partition of the topic is a shard, run on one live node (`Cluster::runs_shard`):
//! it fetches the records after the offset its view has, runs the view's query over them (it
//! may join the lake's tables), and appends the rows with the offset it read up to as its
//! producer's seq (`feed:{view}:{partition}`), the offset it started from as `prev`. The rows and
//! the offset commit together, so every record lands once, whichever node runs the shard or
//! restarts; a run that lost the race is refused and starts again from what was committed.
//! No feed, no work: the loop looks at the catalog only when it changes.
use crate::server::App;
use crate::store::{json, producer_key, table_key, Lake, TableMeta};
use anyhow::{ensure, Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

#[derive(Serialize, Deserialize, Clone, PartialEq)]
pub struct Feed {
    pub topic: String, // the topic's table (an `ext:` name)
    pub sql: String,   // the view's query, over that table
    #[serde(default)]
    pub latest: bool, // start from the records written after the view was made (else the earliest kept)
}

pub fn feed_key(view: &str) -> String { format!("fd/{view}") } // (`f/` is the functions')

/// Leader: make the view's table and its feed (`CREATE MATERIALIZED VIEW` over a topic).
pub async fn create(lake: &Lake, name: &str, sql: &str, topic: &str, options: &std::collections::BTreeMap<String, String>) -> Result<()> {
    ensure!(lake.cat.get::<TableMeta>(&table_key(name)).await?.is_none(), "table {name} already exists");
    let start = options.get("start").map(|s| s.to_lowercase()).unwrap_or_else(|| "earliest".into());
    ensure!(["earliest", "latest"].contains(&start.as_str()), "start = '{start}': earliest or latest");
    ensure!(options.keys().all(|k| k == "start"), "a view over a topic takes one option: start = 'earliest' | 'latest'");
    let feed = Feed { topic: topic.into(), sql: sql.into(), latest: start == "latest" };
    if let Some(had) = lake.cat.get::<Feed>(&feed_key(name)).await? {
        ensure!(had == feed, "view {name} already exists, with other SQL or options");
        return Ok(());
    }
    // Its columns: the query planned over the topic's (no records needed).
    let schema = crate::query::schema(&crate::kafka_client::columns())?;
    let plan = over(lake, &feed, datafusion::arrow::record_batch::RecordBatch::new_empty(schema)).await?;
    let columns: Vec<(String, String)> = plan.schema().fields().iter().map(|f| (f.name().clone(), crate::query::type_name(f.data_type()))).collect();
    let meta = TableMeta { columns, publish: crate::store::default_publish(), ids: true, tiered: lake.visible(), ..Default::default() };
    lake.cat.commit(vec![(table_key(name), json(&meta)), (feed_key(name), json(&feed))], &[]).await
}

/// Every node: run this node's shards of every feed, starting and stopping them as feeds come
/// and go and as nodes join and leave.
pub fn start(app: App) {
    crate::panics::spawn(async move {
        let mut running: HashMap<(String, i32), tokio::task::JoinHandle<()>> = HashMap::new();
        let mut hwm = app.lake.hwm.subscribe();
        loop {
            let _ = tokio::time::timeout(Duration::from_secs(5), hwm.changed()).await; // (catalog changes, and now and then for nodes coming and going)
            let feeds = match app.lake.cat.scan::<Feed>("fd/", "fd0").await {
                Ok(f) => f,
                Err(e) => {
                    eprintln!("feeds: {e:#}");
                    continue;
                }
            };
            if feeds.is_empty() && running.is_empty() {
                continue;
            }
            let mut wanted = std::collections::HashSet::new();
            for (key, feed) in &feeds {
                let view = key[3..].to_string();
                let url = match crate::ext::spec(&feed.topic) {
                    Some(s) => s.urls[0].clone(),
                    None => continue,
                };
                let parts = match partitions(&app.lake, &url).await {
                    Ok(p) => p,
                    Err(e) => {
                        eprintln!("feed {view}: {e:#}");
                        continue;
                    }
                };
                for p in parts {
                    if app.cluster.runs_shard(p as u32) {
                        wanted.insert((view.clone(), p));
                    }
                }
            }
            running.retain(|k, h| {
                let keep = wanted.contains(k) && !h.is_finished();
                if !keep {
                    h.abort();
                }
                keep
            });
            for (view, p) in wanted {
                if running.contains_key(&(view.clone(), p)) {
                    continue;
                }
                let (a, v) = (app.clone(), view.clone());
                running.insert((view, p), crate::panics::spawn(async move {
                    if let Err(e) = shard(a, v.clone(), p).await {
                        eprintln!("feed {v}[{p}]: {e:#}");
                        tokio::time::sleep(Duration::from_secs(2)).await; // (then the loop starts it again)
                    }
                }));
            }
        }
    });
}

async fn partitions(lake: &Lake, url: &str) -> Result<Vec<i32>> {
    let (_, topic) = crate::kafka_client::parse(url)?;
    Ok(crate::kafka_client::client(lake, url).await?.partitions(&topic).await?.into_iter().map(|(p, _)| p).collect())
}

/// One partition of a feed, for as long as this node runs it.
async fn shard(app: App, view: String, p: i32) -> Result<()> {
    let lake = app.lake.clone();
    let feed: Feed = lake.cat.get(&feed_key(&view)).await?.context("the feed is gone")?;
    let url = crate::ext::spec(&feed.topic).context("a feed's topic")?.urls[0].clone();
    let (_, topic) = crate::kafka_client::parse(&url)?;
    let client = crate::kafka_client::client(&lake, &url).await?;
    let producer = format!("feed:{view}:{p}");
    let schema = crate::query::schema(&crate::kafka_client::columns())?;
    let mut leader = String::new();
    let mut at: Option<i64> = None; // (where this node reads next: past records that gave no rows)
    loop {
        if leader.is_empty() {
            leader = client.partitions(&topic).await?.into_iter().find(|(i, _)| *i == p).map(|(_, l)| l).context("the partition is gone")?;
        }
        let done: u64 = lake.cat.get(&producer_key(&producer)).await?.unwrap_or(0);
        let from = match (at, done) {
            (Some(a), d) if a as u64 >= d => a,
            (_, 0) => client.offset(&leader, &topic, p, if feed.latest { -1 } else { -2 }).await?,
            (_, d) => d as i64,
        };
        let (recs, next, _) = match client.fetch(&leader, &topic, p, from, 1000).await {
            Ok(r) => r,
            Err(e) => {
                leader.clear(); // (a new leader, maybe: ask again)
                return Err(e);
            }
        };
        // (made again, detached or dropped meanwhile: this run ends, and the loop starts the feed as
        // it is now; an append already made against the old offsets is refused by its `prev`)
        if lake.cat.get::<Feed>(&feed_key(&view)).await?.as_ref() != Some(&feed) {
            return Ok(());
        }
        at = Some(next.max(from));
        if recs.is_empty() {
            continue;
        }
        let batch = crate::kafka_client::batch(&schema, p, &recs)?;
        let df = over(&lake, &feed, batch).await?;
        let out_schema = Arc::new(df.schema().as_arrow().clone());
        let rows = datafusion::arrow::compute::concat_batches(&out_schema, &df.collect().await?)?;
        let target = crate::query::schema(&lake.cat.get::<TableMeta>(&table_key(&view)).await?.context("the view's table is gone")?.columns)?;
        let rows = crate::query::cast_as(&rows, &target)?;
        let src = crate::log::Src { producer: producer.clone(), seq: next as u64, prev: Some(done) }; // (the rows and the offset, if the offset is still `done`)
        let ack = app.log()?.append(view.clone(), src, rows).await?;
        if ack.conflict || ack.duplicate {
            at = None; // (another run got there first: on from what it committed)
        }
    }
}

/// The view's query over these records (in its topic's place).
async fn over(lake: &Lake, feed: &Feed, batch: datafusion::arrow::record_batch::RecordBatch) -> Result<datafusion::prelude::DataFrame> {
    const HERE: &str = "__pondra_records";
    let sql = feed.sql.replace(&format!("\"{}\"", feed.topic), &format!("\"{HERE}\"")); // (the session never lists the topic)
    let ctx = crate::query::session(lake, &sql, "").await?;
    ctx.register_batch(HERE, batch)?;
    crate::query::sql(&ctx, &sql).await
}
