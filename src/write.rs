//! Writes in SQL, from anywhere: `CREATE TABLE`, `INSERT`, `UPDATE`, `DELETE`, sent to any node
//! (`POST /sql`) or run with the binary on any machine (`pondra sql`). Whoever runs the statement
//! does the work — runs the query, writes the Parquet or the log segment — and the lake only has
//! to record the result: one small commit, the leader's job (`Request`, `handle`). A machine that
//! isn't a node reaches the leader over HTTP, through the bucket when it can't (`inbox.rs`), or
//! leads for a moment itself when nobody does.
use crate::cluster::{alive, claim, http, latest, mark_alive, release};
use crate::log::{decode_flush, encode_flush, pack, Append, Outcome, Sequencer, Src};
use crate::query::{schema, session};
use crate::store::*;
use anyhow::{bail, ensure, Result};
use bytes::Bytes;
use datafusion::arrow::compute::{cast, concat_batches};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{self, Statement};
use serde::{Deserialize, Serialize};
use serde_json::{json as j, Value};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

// ---------------------------------------------------------------- tables

/// A table's definition: `[["user","Utf8"],…]`, or `{"columns": […], "key": ["id"]}` for an
/// upsert table (a Boolean `_deleted` column marks deletes), with `"merge": {"total": "sum"}` for a
/// merge table (sum, min or max per key), `"publish": ["delta", "iceberg"]` for other engines and
/// `"cluster_by": ["user"]` (append tables) to sort every file by those columns.
#[derive(Deserialize)]
#[serde(untagged)]
enum TableSpec {
    Columns(Vec<(String, String)>),
    Full {
        columns: Vec<(String, String)>,
        #[serde(default)]
        key: Vec<String>,
        #[serde(default)]
        merge: BTreeMap<String, String>,
        publish: Option<Vec<String>>,
        #[serde(default)]
        cluster_by: Vec<String>,
        ttl: Option<String>, // keyed tables: "column:seconds"
    },
}

/// Leader: create a table (sent again for an existing one, `publish` changes). The caller holds
/// the lock that serialises table rewrites.
pub async fn create_table(lake: &Lake, name: &str, spec: &str) -> Result<Value> {
    let (columns, key, merge, publish, cluster, ttl) = match serde_json::from_str(spec)? {
        TableSpec::Columns(c) => (c, vec![], BTreeMap::new(), None, vec![], None),
        TableSpec::Full { columns, key, merge, publish, cluster_by, ttl } => (columns, key, merge, publish, cluster_by, ttl),
    };
    let ttl = ttl.map(|t| -> Result<(String, u64)> {
        let (c, s) = t.split_once(':').ok_or_else(|| anyhow::anyhow!("ttl: \"column:seconds\""))?;
        ensure!(!key.is_empty() && columns.iter().any(|(n, ty)| n == c && (ty.starts_with("Timestamp") || ty.starts_with("Date"))), "ttl: a timestamp or date column of a keyed table");
        Ok((c.to_string(), s.trim().parse()?))
    }).transpose()?;
    schema(&columns)?; // validate types
    ensure!(merge.values().all(|f| ["sum", "min", "max"].contains(&f.as_str())), "merge functions: sum, min, max");
    ensure!(merge.is_empty() || !key.is_empty(), "a merge table needs a key");
    ensure!(publish.iter().flatten().all(|f| ["delta", "iceberg"].contains(&f.as_str())), "publish formats: delta, iceberg");
    ensure!(cluster.iter().all(|c| columns.iter().any(|(n, _)| n == c)) && (cluster.is_empty() || key.is_empty()), "cluster_by: columns of an append table (keyed tables are sorted by key)");
    let meta = match lake.cat.get::<TableMeta>(&table_key(name)).await? {
        None => TableMeta { columns, key, merge, publish: publish.unwrap_or_else(default_publish), cluster, ttl, ..Default::default() },
        Some(m) if publish.is_none() || publish.as_ref() == Some(&m.publish) => return Ok(j!({"table": name, "publish": m.publish})),
        Some(mut m) => {
            let dropped: Vec<String> = m.publish.iter().filter(|f| !publish.iter().flatten().any(|p| p == *f)).cloned().collect();
            m.publish = publish.unwrap_or_default();
            for format in dropped {
                crate::delta::unpublish(lake, name, &format).await?; // (no stale copy left for other engines)
            }
            m
        }
    };
    lake.cat.commit(vec![(table_key(name), json(&meta))], &[]).await?;
    Ok(j!({"table": name, "publish": meta.publish}))
}

// ---------------------------------------------------------------- statements

/// A write statement.
pub enum Stmt {
    Create(Box<ast::CreateTable>),
    Insert(String, String),                              // table, the query giving the rows
    Update(String, Vec<(String, String)>, Option<String>), // table, column = expression, WHERE
    Delete(String, Option<String>),                      // table, WHERE
}

impl Stmt {
    /// The table it writes.
    fn table(&self) -> String {
        match self {
            Stmt::Create(c) => c.name.to_string(),
            Stmt::Insert(t, _) | Stmt::Update(t, ..) | Stmt::Delete(t, _) => t.clone(),
        }
    }
}

/// A write statement, or None for a query.
pub fn parse(sql: &str) -> Option<Stmt> {
    use datafusion::sql::sqlparser::{dialect::GenericDialect, parser::Parser};
    let where_ = |e: &Option<ast::Expr>| e.as_ref().map(|e| e.to_string());
    Some(match Parser::parse_sql(&GenericDialect {}, sql).ok()?.pop()? {
        Statement::CreateTable(c) => Stmt::Create(Box::new(c)),
        Statement::Insert(ast::Insert { table: ast::TableObject::TableName(t), source: Some(q), columns, .. }) if columns.is_empty() => Stmt::Insert(t.to_string(), q.to_string()),
        Statement::Update(u) => {
            let set = u.assignments.iter().filter_map(|a| match &a.target {
                ast::AssignmentTarget::ColumnName(c) => Some((c.to_string(), a.value.to_string())),
                _ => None,
            });
            Stmt::Update(u.table.relation.to_string(), set.collect(), where_(&u.selection))
        }
        Statement::Delete(d) => {
            let (ast::FromTable::WithFromKeyword(t) | ast::FromTable::WithoutKeyword(t)) = &d.from;
            Stmt::Delete(t.first()?.relation.to_string(), where_(&d.selection))
        }
        _ => return None,
    })
}

/// `CREATE TABLE t (a BIGINT, b VARCHAR, PRIMARY KEY (a)) [WITH (publish = 'delta,iceberg',
/// cluster_by = 'b', merge = 'total:sum')]` → the table name and its spec. SQL types become Arrow
/// types the way DataFusion maps them.
async fn create_spec(c: &ast::CreateTable) -> Result<(String, String)> {
    let cols = c.columns.iter().map(|c| format!("{} {}", c.name, c.data_type)).collect::<Vec<_>>().join(", ");
    let ctx = SessionContext::new();
    ctx.sql(&format!("CREATE TABLE t ({cols})")).await?;
    let columns: Vec<(String, String)> = ctx.table("t").await?.schema().fields().iter().map(|f| (f.name().clone(), f.data_type().to_string().replace("Utf8View", "Utf8"))).collect();
    let name = |e: &ast::Expr| e.to_string().trim_matches('"').to_string();
    let mut key: Vec<String> = c.constraints.iter().flat_map(|k| match k {
        ast::TableConstraint::PrimaryKey(pk) => pk.columns.iter().map(|i| name(&i.column.expr)).collect(),
        _ => vec![],
    }).collect();
    key.extend(c.columns.iter().filter(|c| c.options.iter().any(|o| matches!(o.option, ast::ColumnOption::PrimaryKey(_)))).map(|c| c.name.value.clone()));
    let mut opts: BTreeMap<String, String> = BTreeMap::new();
    if let ast::CreateTableOptions::With(o) | ast::CreateTableOptions::Options(o) = &c.table_options {
        for o in o {
            if let ast::SqlOption::KeyValue { key, value } = o {
                opts.insert(key.value.to_lowercase(), value.to_string().trim_matches('\'').to_string());
            }
        }
    }
    let list = |k: &str| opts.get(k).map(|v| v.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect::<Vec<_>>());
    let merge: BTreeMap<String, String> = list("merge").unwrap_or_default().iter().filter_map(|m| m.split_once(':')).map(|(c, f)| (c.into(), f.into())).collect();
    let mut columns = columns;
    if !key.is_empty() && merge.is_empty() && !columns.iter().any(|(c, _)| c == "_deleted") {
        columns.push(("_deleted".into(), "Boolean".into())); // (so DELETE works; writes leave it out)
    }
    let spec = j!({"columns": columns, "key": key, "merge": merge, "publish": list("publish"), "cluster_by": list("cluster_by").unwrap_or_default(), "ttl": opts.get("ttl")});
    Ok((c.name.to_string(), spec.to_string()))
}

/// The rows a write to a keyed table upserts: the INSERT's query, the updated rows, or the rows
/// to delete (marked `_deleted`). Keys never change; merge tables only take INSERTs.
fn rows_sql(meta: &TableMeta, stmt: &Stmt) -> Result<String> {
    let q = |c: &str| format!("\"{c}\"");
    let select = |pick: &dyn Fn(&str) -> String, table: &str, cond: &Option<String>| {
        let cols = meta.columns.iter().map(|(c, _)| format!("{} AS {}", pick(c), q(c))).collect::<Vec<_>>().join(", ");
        format!("SELECT {cols} FROM {table} {}", cond.as_ref().map(|w| format!("WHERE {w}")).unwrap_or_default())
    };
    let deletes = meta.columns.iter().any(|(c, _)| c == "_deleted");
    ensure!(matches!(stmt, Stmt::Insert(..)) || !meta.key.is_empty(), "UPDATE and DELETE need a keyed table (append tables only take INSERTs)");
    match stmt {
        Stmt::Insert(_, query) => Ok(query.clone()),
        Stmt::Update(t, set, cond) => {
            ensure!(meta.merge.is_empty(), "merge tables combine rows per key: INSERT into them instead");
            ensure!(set.iter().all(|(c, _)| !meta.key.contains(c) && meta.columns.iter().any(|(n, _)| n == c)), "UPDATE sets existing non-key columns");
            let pick = |c: &str| match set.iter().find(|(s, _)| s == c) {
                Some((_, e)) => format!("({e})"),
                None if c == "_deleted" => "false".into(),
                None => q(c),
            };
            Ok(select(&pick, t, cond))
        }
        Stmt::Delete(t, cond) => {
            ensure!(meta.merge.is_empty() && deletes, "DELETE needs an upsert table with a Boolean _deleted column");
            Ok(select(&|c: &str| if c == "_deleted" { "true".into() } else { q(c) }, t, cond))
        }
        Stmt::Create(_) => unreachable!("not a row write"),
    }
}

/// Does a write to this table go through the log? Keyed tables' do (new versions), and so do
/// those of append tables that views or streaming tasks follow (they only see the log); other
/// INSERTs go straight to Parquet.
async fn through_log(lake: &Lake, table: &str, meta: &TableMeta) -> Result<bool> {
    if !meta.key.is_empty() {
        return Ok(true);
    }
    let views = lake.cat.scan::<crate::views::View>("v/", "v0").await?.into_iter().any(|(_, v)| v.source == table);
    Ok(views || lake.cat.scan::<crate::tasks::Task>("k/", "k0").await?.into_iter().any(|(_, t)| t.source == table))
}

/// Run a row query here: its rows in the table's column order and types.
async fn rows(ctx: &SessionContext, meta: &TableMeta, sql: &str) -> Result<RecordBatch> {
    let target = schema(&meta.columns)?;
    let batches = ctx.sql(sql).await?.collect().await?;
    let Some(first) = batches.first() else { return Ok(RecordBatch::new_empty(target)) };
    let all = concat_batches(&first.schema(), &batches)?;
    let mut given = all.columns().to_vec();
    if given.len() + 1 == target.fields().len() && target.fields().last().is_some_and(|f| f.name() == "_deleted") {
        given.push(datafusion::arrow::array::new_null_array(&datafusion::arrow::datatypes::DataType::Boolean, all.num_rows())); // (INSERTs may leave `_deleted` out)
    }
    ensure!(given.len() == target.fields().len(), "{} columns given, the table has {}", given.len(), target.fields().len() - usize::from(target.fields().last().is_some_and(|f| f.name() == "_deleted")));
    let columns = given.iter().zip(target.fields()).map(|(c, f)| cast(c, f.data_type())).collect::<Result<Vec<_>, _>>()?;
    Ok(RecordBatch::try_new(target, columns)?)
}

// ---------------------------------------------------------------- bulk INSERT into append tables

/// The files one INSERT wrote (`job`: a retried job is recorded once).
#[derive(Serialize, Deserialize)]
pub struct Files {
    table: String,
    job: String,
    columns: Vec<(String, String)>,
    files: Vec<DataFile>,
}

/// Run the query here and write its rows as Parquet into the table's folder (None: this job was
/// already recorded).
pub async fn write_files(lake: &Lake, ctx: &SessionContext, table: &str, query: &str, job: &str) -> Result<Option<Files>> {
    use datafusion::arrow::datatypes::DataType;
    use datafusion::prelude::{cast as cast_to, Expr};
    if lake.cat.get::<u64>(&producer_key(&format!("job:{job}"))).await?.is_some() {
        return Ok(None);
    }
    // The query's columns, by position, as the table's (or, for a new table, with plain Utf8 strings).
    let df = ctx.sql(query).await?;
    let target: Vec<(String, DataType)> = match lake.cat.get::<TableMeta>(&table_key(table)).await? {
        Some(m) => schema(&m.columns)?.fields().iter().map(|f| (f.name().clone(), f.data_type().clone())).collect(),
        None => df.schema().fields().iter().map(|f| (f.name().clone(), if *f.data_type() == DataType::Utf8View { DataType::Utf8 } else { f.data_type().clone() })).collect(),
    };
    ensure!(target.len() == df.schema().fields().len(), "{} columns given, table {table} has {}", df.schema().fields().len(), target.len());
    let exprs = df.schema().columns().into_iter().zip(&target).map(|(c, (name, t))| cast_to(Expr::Column(c), t.clone()).alias(name)).collect::<Vec<_>>();
    let df = df.select(exprs)?;
    let columns = target.iter().map(|(n, t)| (n.clone(), t.to_string())).collect();
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
    ensure!(meta.key.is_empty(), "INSERT into a keyed table goes through the log");
    ensure!(types(&meta.columns) == types(&f.columns), "query columns {:?} don't match table {}", f.columns, f.table);
    let rows: u64 = f.files.iter().map(|f| f.rows).sum();
    meta.files.extend(f.files);
    lake.cat.commit(vec![(table_key(&f.table), json(&meta)), (producer, json(&1u64))], &[]).await?;
    Ok(j!({"rows": rows}))
}

// ---------------------------------------------------------------- on a node

/// `POST /sql` with a write statement: this node does the work; the leader records it.
pub async fn on_node(app: &crate::server::App, stmt: Stmt, job: Option<String>) -> Result<Value> {
    ensure!(!app.cluster.reader, "read-only node");
    let (lake, job) = (&app.lake, job.unwrap_or_else(|| uuid::Uuid::new_v4().to_string()));
    // A table of an attached lake (`name.table`): the work runs here, that lake's leader records it.
    let full = stmt.table();
    let other = full.split_once('.').and_then(|(ns, t)| Some((lake.attached.read().unwrap().iter().find(|(n, _)| n == ns)?.1.clone(), t.to_string())));
    if let Some((other, table)) = other {
        let req = prepare(lake, &other, &table, &stmt, &job, false).await?;
        return deliver(&other.url, Some(req), &stmt, &job).await;
    }
    let table = match &stmt {
        Stmt::Create(c) => {
            let (name, spec) = create_spec(c).await?;
            if app.seq.is_none() {
                return Ok(http().post(format!("http://{}/tables/{name}", app.cluster.leader.addr)).body(spec).send().await?.error_for_status()?.json().await?);
            }
            let _guard = app.lock.lock().await;
            return create_table(lake, &name, &spec).await;
        }
        Stmt::Insert(t, _) | Stmt::Update(t, ..) | Stmt::Delete(t, _) => t.clone(),
    };
    let meta = lake.cat.get::<TableMeta>(&table_key(&table)).await?;
    let sql = match (&meta, &stmt) {
        (Some(m), _) if through_log(lake, &table, m).await? => rows_sql(m, &stmt)?,
        (_, Stmt::Insert(_, query)) => {
            let ctx = session(lake, query, "").await?;
            let Some(f) = write_files(lake, &ctx, &table, query, &job).await? else { return Ok(j!({"duplicate": true})) };
            return app.record_files(f).await;
        }
        (None, _) => bail!("no table {table}"),
        _ => bail!("UPDATE and DELETE need a keyed table (append tables only take INSERTs)"),
    };
    let meta = meta.expect("a table");
    let batch = rows(&session(lake, &sql, "").await?, &meta, &sql).await?;
    let n = batch.num_rows();
    let ack = app.log()?.append(table, Src { producer: format!("sql:{job}"), seq: 1, prev: None }, batch).await?;
    Ok(if ack.duplicate { j!({"duplicate": true}) } else { j!({"rows": n}) })
}

// ---------------------------------------------------------------- what the leader records

/// What a writer asks the leader to record: new files, a log flush, or a table.
pub enum Request {
    Files(Files),
    Flush(Bytes), // the body of POST /cluster/commit
    Table(String, String),
}

impl Request {
    /// Where it goes over HTTP, and its body; its name and body in the bucket inbox.
    pub fn http(&self) -> Result<(String, Vec<u8>)> {
        Ok(match self {
            Request::Files(f) => ("/cluster/files".into(), serde_json::to_vec(f)?),
            Request::Flush(b) => ("/cluster/commit".into(), b.to_vec()),
            Request::Table(name, spec) => (format!("/tables/{name}"), spec.clone().into_bytes()),
        })
    }

    pub fn inbox(&self) -> Result<(String, Vec<u8>)> {
        Ok(match self {
            Request::Table(name, spec) => ("table".into(), serde_json::to_vec(&(name, spec))?),
            Request::Files(_) => ("files".into(), self.http()?.1),
            Request::Flush(_) => ("flush".into(), self.http()?.1),
        })
    }

    pub fn from_inbox(kind: &str, body: Bytes) -> Result<Request> {
        Ok(match kind {
            "files" => Request::Files(serde_json::from_slice(&body)?),
            "flush" => Request::Flush(body),
            "table" => {
                let (name, spec): (String, String) = serde_json::from_slice(&body)?;
                Request::Table(name, spec)
            }
            k => bail!("unknown inbox request {k}"),
        })
    }
}

/// Leader: record one request (from the inbox, or a `pondra sql` leading for a moment).
pub async fn handle(lake: &Lake, seq: &Sequencer, lock: &Mutex<()>, req: Request) -> Result<Value> {
    match req {
        Request::Files(f) => {
            let _guard = lock.lock().await;
            record(lake, f).await
        }
        Request::Flush(body) => Ok(serde_json::to_value(seq.submit(decode_flush(body)?).await?)?),
        Request::Table(name, spec) => {
            let _guard = lock.lock().await;
            create_table(lake, &name, &spec).await
        }
    }
}

// ---------------------------------------------------------------- from any machine

/// `pondra sql --dir … "<write>"` on any machine. This process does the work (local files too:
/// `SELECT * FROM 'jan.parquet'`), then the leader records it: over HTTP; through the bucket
/// inbox if this machine can't reach it; or, when nobody leads, this process leads for the moment
/// it takes, under its own term, so a node starting meanwhile waits for it. It never takes over
/// from a live leader.
pub async fn from_cli(dir: &str, stmt: Stmt) -> Result<Value> {
    let job = std::env::var("PONDRA_JOB").unwrap_or_else(|_| uuid::Uuid::new_v4().to_string());
    std::env::set_var("PONDRA_JOB", &job); // (if fenced, the process restarts: the same job again)
    // (A lake with no catalog yet can't be read: then the work is done once this process leads.)
    let req = match Lake::open(dir, false, false).await {
        Ok(lake) => Some(prepare(&lake, &lake, &stmt.table(), &stmt, &job, true).await?),
        Err(_) => None,
    };
    deliver(dir, req, &stmt, &job).await
}

/// Have the leader of the lake at `dir` record a write: over HTTP, through the bucket inbox if it
/// can't be reached, or, when nobody leads, by leading for a moment here.
async fn deliver(dir: &str, mut req: Option<Option<Request>>, stmt: &Stmt, job: &str) -> Result<Value> {
    let store = open_store(dir)?.1;
    loop {
        match latest(&store).await? {
            Some(t) if alive(&store, &t).await => {
                if t.addr.is_empty() {
                    tokio::time::sleep(Duration::from_secs(1)).await; // another `pondra sql` is recording: wait
                    continue;
                }
                if req.is_none() {
                    let lake = Lake::open(dir, false, false).await?;
                    req = Some(prepare(&lake, &lake, &stmt.table(), stmt, job, true).await?);
                }
                let Some(Some(r)) = &req else { return Ok(j!({"duplicate": true})) };
                let direct = std::env::var("PONDRA_NO_DIRECT").is_err(); // (test hook: act as if the leader were out of reach)
                match if direct { Some(post(&t.addr, r).await) } else { None } {
                    Some(Ok(v)) => return summary(matches!(r, Request::Flush(_)), v),
                    Some(Err(e)) if !unreachable(&e) => return Err(e),
                    _ => {} // this machine can't reach the leader: through the bucket
                }
                if let Some(v) = crate::inbox::send(&store, r).await? {
                    return summary(matches!(r, Request::Flush(_)), v);
                }
            }
            t => {
                let Some(term) = claim(&store, t.map_or(1, |t| t.n + 1), "").await? else { continue };
                return lead(dir, &store, term.n, req, stmt, job).await;
            }
        }
    }
}

/// Do the work of a write here: the query runs over `query`'s tables (and attached lakes'), the
/// result goes into `target`'s `table`. What its leader has to record (None: a retried job).
/// `files`: the query may read local files (`FROM 'jan.parquet'`) — on the author's own machine.
async fn prepare(query: &Lake, target: &Arc<Lake>, table: &str, stmt: &Stmt, job: &str, files: bool) -> Result<Option<Request>> {
    let open = |ctx: SessionContext| if files { ctx.enable_url_table() } else { ctx };
    if let Stmt::Create(c) = stmt {
        let (_, spec) = create_spec(c).await?;
        return Ok(Some(Request::Table(table.into(), spec)));
    }
    let meta = target.cat.get::<TableMeta>(&table_key(table)).await?;
    let log = match &meta {
        Some(m) => through_log(target, table, m).await?,
        None => false,
    };
    match (meta, stmt) {
        (Some(m), _) if log => {
            let sql = rows_sql(&m, stmt)?;
            let batch = rows(&open(session(query, &sql, "").await?), &m, &sql).await?;
            let (ack, _) = tokio::sync::oneshot::channel();
            let append = Append { table: table.into(), src: Src { producer: format!("sql:{job}"), seq: 1, prev: None }, batch, ack };
            Ok(Some(Request::Flush(encode_flush(&pack(target, &[append]).await?)?.into())))
        }
        (_, Stmt::Insert(_, sql)) => {
            let ctx = open(session(query, sql, "").await?);
            Ok(write_files(target, &ctx, table, sql, job).await?.map(Request::Files))
        }
        (None, _) => bail!("no table {table}"),
        _ => bail!("UPDATE and DELETE need a keyed table (append tables only take INSERTs)"),
    }
}

/// Send a request to the leader over HTTP.
async fn post(addr: &str, r: &Request) -> Result<Value> {
    let (path, body) = r.http()?;
    let res = http().post(format!("http://{addr}{path}")).header("content-type", "application/json").body(body).send().await?;
    ensure!(res.status().is_success(), "the leader at {addr}: {}", res.text().await?);
    Ok(res.json().await?)
}

/// A failure to reach the leader at all (not an answer from it).
fn unreachable(e: &anyhow::Error) -> bool {
    e.downcast_ref::<reqwest::Error>().is_some_and(|e| e.is_connect() || e.is_timeout())
}

/// What the user sees: the leader's answer, or for a log flush, whether it went in.
fn summary(flush: bool, v: Value) -> Result<Value> {
    if !flush {
        return Ok(v);
    }
    let acks = match serde_json::from_value::<Outcome>(v)? {
        Outcome::Acks(a) => a,
        Outcome::Retry(r) => r.into_iter().map(|(_, a)| a).collect(),
    };
    Ok(if acks.iter().all(|a| a.duplicate) { j!({"duplicate": true}) } else { j!({"committed": true}) })
}

/// Nobody leads: record the write under our own term, and whatever waits in the inbox, then let go.
async fn lead(dir: &str, store: &Store, term: u64, req: Option<Option<Request>>, stmt: &Stmt, job: &str) -> Result<Value> {
    let s = store.clone();
    let marks = tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(10)).await;
            let _ = mark_alive(&s, term).await;
        }
    });
    let lake = Lake::open(dir, true, false).await?; // (the catalog's writer: fences any older one)
    crate::replica::recover(&lake, "", term, None).await?;
    let (seq, lock) = (Sequencer::start(lake.clone(), None).await?, Mutex::new(()));
    let req = match req {
        Some(r) => r,
        None => prepare(&lake, &lake, &stmt.table(), stmt, job, true).await?,
    };
    let out = match req {
        Some(r) => {
            let flush = matches!(r, Request::Flush(_));
            summary(flush, handle(&lake, &seq, &lock, r).await?)?
        }
        None => j!({"duplicate": true}),
    };
    crate::inbox::drain(&lake, &seq, &lock).await?; // others who couldn't reach a leader
    lake.cat.checkpoint().await?; // (so every node's view has it without replaying the WAL)
    marks.abort();
    release(store, term).await; // the next writer or node doesn't have to wait
    Ok(out)
}
