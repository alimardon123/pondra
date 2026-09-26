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
use datafusion::arrow::datatypes::{DataType, TimeUnit};
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
/// `"cluster_by": ["user"]` (append tables) to sort every file by those columns, and
/// `"partition_by": "day(ts)"` (append tables; a column, or year/month/day/hour of a timestamp) so
/// no file mixes partitions.
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
        cluster_by: Option<Vec<String>>, // (None: as it is; sent again, the table keeps its own)
        ttl: Option<String>, // keyed tables: "column:seconds"
        partition_by: Option<String>,
    },
}

/// Leader: create a table (sent again for an existing one, `publish` changes). The caller holds
/// the lock that serialises table rewrites.
pub async fn create_table(lake: &Lake, name: &str, spec: &str) -> Result<Value> {
    let name = &crate::ddl::new_name(lake, name).await?; // (its schema exists; `public.t` is `t`)
    ensure!(lake.cat.get::<crate::ddl::StoredView>(&crate::ddl::query_key(name)).await?.is_none(), "{name} is a view");
    let (columns, key, merge, publish, cluster, ttl, partition) = match serde_json::from_str(spec)? {
        TableSpec::Columns(c) => (c, vec![], BTreeMap::new(), None, None, None, None),
        TableSpec::Full { columns, key, merge, publish, cluster_by, ttl, partition_by } => (columns, key, merge, publish, cluster_by, ttl, partition_by),
    };
    // Types as the lake records them: `VARIANT` is JSON text, `Float32[]` a list (see `query::dtype`).
    let columns = columns.iter().map(|(n, t)| Ok((n.clone(), crate::query::type_name(&crate::query::dtype(t)?)))).collect::<Result<Vec<_>>>()?;
    let ttl = ttl.map(|t| -> Result<(String, u64)> {
        let (c, s) = t.split_once(':').ok_or_else(|| anyhow::anyhow!("ttl: \"column:seconds\""))?;
        ensure!(!key.is_empty() && columns.iter().any(|(n, ty)| n == c && (ty.starts_with("Timestamp") || ty.starts_with("Date"))), "ttl: a timestamp or date column of a keyed table");
        Ok((c.to_string(), s.trim().parse()?))
    }).transpose()?;
    schema(&columns)?; // validate types
    ensure!(columns.iter().all(|(c, _)| !crate::sys::NAMES.contains(&c.as_str()) && c != "_old_version"), "{} are system columns: every table has them (`SELECT _row_id, * FROM t`)", crate::sys::NAMES.join(", "));
    ensure!(merge.values().all(|f| ["sum", "count", "min", "max"].contains(&f.as_str())), "merge functions: sum, count, min, max");
    ensure!(merge.is_empty() || !key.is_empty(), "a merge table needs a key");
    ensure!(publish.iter().flatten().all(|f| ["delta", "iceberg"].contains(&f.as_str())), "publish formats: delta, iceberg");
    let keyed = "a table with a PRIMARY KEY keeps its files sorted by the key (so a key's newest row is found fast): drop cluster_by and partition_by, or drop the PRIMARY KEY for an append table";
    ensure!(cluster.iter().flatten().next().is_none() || key.is_empty(), "cluster_by: {keyed}");
    ensure!(cluster.iter().flatten().all(|c| columns.iter().any(|(n, _)| n == c)), "cluster_by: columns of the table");
    if let Some(p) = &partition {
        ensure!(key.is_empty(), "partition_by: {keyed}");
        crate::tier::check_partition(p, &columns)?;
    }
    let meta = match lake.cat.get::<TableMeta>(&table_key(name)).await? {
        // (A new table reads the log from now on: a table of this name dropped earlier left rows
        // in segments that aren't expired yet.)
        None => TableMeta { columns, key, merge, publish: publish.unwrap_or_else(default_publish), cluster: cluster.unwrap_or_default(), ttl, partition, tiered: lake.visible(), ids: true, ..Default::default() },
        Some(mut m) => {
            ensure!(partition.is_none() || partition == m.partition, "{name}'s partition_by can't change");
            // Sent again: columns may only grow at the end (ALTER TABLE … ADD COLUMN; a re-sent
            // original definition is fine), and `publish` may change.
            let (old, new) = (m.columns.len(), columns.len());
            ensure!(columns[..new.min(old)] == m.columns[..new.min(old)], "{name} exists with other columns (only new ones can be added, at the end)");
            let republish = publish.as_ref().is_some_and(|p| *p != m.publish);
            let (recluster, rettl) = (cluster.as_ref().is_some_and(|c| *c != m.cluster), ttl.is_some() && ttl != m.ttl);
            if new <= old && !republish && !recluster && !rettl {
                return Ok(j!({"table": name, "publish": m.publish}));
            }
            m.cluster = cluster.unwrap_or(m.cluster);
            m.ttl = ttl.or(m.ttl);
            if new > old {
                m.columns = columns;
                if m.changed {
                    // (its replaced rows' table takes the column too, before its `_old_version`)
                    let del = crate::sys::deleted(name);
                    if let Some(mut d) = lake.cat.get::<TableMeta>(&table_key(&del)).await? {
                        d.columns = m.columns.iter().cloned().chain([("_old_version".to_string(), "Int64".to_string())]).collect();
                        lake.cat.commit(vec![(table_key(&del), json(&d))], &[]).await?;
                    }
                }
            }
            if let Some(publish) = publish.filter(|_| republish) {
                let dropped: Vec<String> = m.publish.iter().filter(|f| !publish.contains(f)).cloned().collect();
                m.publish = publish;
                for format in dropped {
                    crate::delta::unpublish(lake, name, &format).await?; // (no stale copy left for other engines)
                }
            }
            m
        }
    };
    lake.cat.commit(vec![(table_key(name), json(&meta))], &[]).await?;
    Ok(j!({"table": name, "publish": meta.publish}))
}

// ---------------------------------------------------------------- statements

/// A write statement. Tables are named as SQL resolves them (`object`): `t`, `schema.t` or
/// `lake.schema.t`, unquoted parts in lower case.
pub enum Stmt {
    Create(Box<ast::CreateTable>),                       // (with a query: CREATE TABLE … AS SELECT)
    Define(String, String),                              // table, its definition (CREATE TABLE … AS SELECT's first step)
    Insert(String, String),                              // table, the query giving the rows
    Update(String, Vec<(String, String)>, Option<String>), // table, column = expression, WHERE
    Delete(String, Option<String>),                      // table, WHERE
    AddColumn(String, String, String, bool),             // table, column, SQL type, IF NOT EXISTS
    SetOptions(String, Vec<(String, String)>),           // table, ALTER TABLE … SET (publish = 'delta', cluster_by = 'user', ttl = 'ts:3600')
    Ddl(Vec<crate::ddl::Ddl>),                            // schemas, views, drops: the leader's (`ddl.rs`)
    Merge(Box<crate::change::Merge>),                     // MERGE INTO … (`change.rs`)
}

impl Stmt {
    /// The table it writes.
    pub fn table(&self) -> String {
        match self {
            Stmt::Create(c) => object(&c.name),
            Stmt::Define(t, _) | Stmt::Insert(t, _) | Stmt::Update(t, ..) | Stmt::Delete(t, _) | Stmt::AddColumn(t, ..) | Stmt::SetOptions(t, _) => t.clone(),
            Stmt::Ddl(_) => String::new(),
            Stmt::Merge(m) => m.target.clone(),
        }
    }

    /// An UPDATE, DELETE or MERGE as SQL again, for the leader to carry out (`change.rs`).
    fn change_sql(&self) -> Option<String> {
        let w = |cond: &Option<String>| cond.as_ref().map(|c| format!(" WHERE {c}")).unwrap_or_default();
        match self {
            Stmt::Update(t, set, cond) => Some(format!("UPDATE {} SET {}{}", sql_name(t), set.iter().map(|(c, e)| format!("\"{c}\" = ({e})")).collect::<Vec<_>>().join(", "), w(cond))),
            Stmt::Delete(t, cond) => Some(format!("DELETE FROM {}{}", sql_name(t), w(cond))),
            Stmt::Merge(m) => Some(m.sql.clone()),
            _ => None,
        }
    }

    /// The same statement writing `table` (a name resolved to one inside its lake).
    fn on(self, table: String) -> Stmt {
        match self {
            Stmt::Insert(_, q) => Stmt::Insert(table, q),
            Stmt::Update(_, set, w) => Stmt::Update(table, set, w),
            Stmt::Delete(_, w) => Stmt::Delete(table, w),
            Stmt::AddColumn(_, c, ty, i) => Stmt::AddColumn(table, c, ty, i),
            Stmt::SetOptions(_, o) => Stmt::SetOptions(table, o),
            Stmt::Define(_, spec) => Stmt::Define(table, spec),
            Stmt::Merge(mut m) => {
                m.target = table;
                Stmt::Merge(m)
            }
            s => s,
        }
    }
}

/// A name as SQL resolves it: its parts, unquoted ones in lower case, joined by dots.
pub fn object(n: &ast::ObjectName) -> String {
    let part = |p: &ast::ObjectNamePart| p.as_ident().map(ident).unwrap_or_else(|| p.to_string());
    n.0.iter().map(part).collect::<Vec<_>>().join(".")
}

/// One part of a name: as written if quoted, else in lower case.
fn ident(i: &ast::Ident) -> String { if i.quote_style.is_some() { i.value.clone() } else { i.value.to_lowercase() } }

/// A name in SQL, each part quoted as it is: `"t"`, `"schema"."t"`, `"lake"."schema"."t"`.
pub fn sql_name(name: &str) -> String {
    name.split('.').map(|p| format!("\"{p}\"")).collect::<Vec<_>>().join(".")
}

/// A write statement, or None for a query.
pub fn parse(sql: &str) -> Option<Stmt> {
    use datafusion::sql::sqlparser::{dialect::GenericDialect, parser::Parser};
    use crate::ddl::Ddl;
    let where_ = |e: &Option<ast::Expr>| e.as_ref().map(|e| e.to_string());
    let relation = |r: &ast::TableFactor| match r {
        ast::TableFactor::Table { name, .. } => Some(object(name)),
        _ => None,
    };
    Some(match Parser::parse_sql(&GenericDialect {}, sql).ok()?.pop()? {
        Statement::CreateTable(c) => Stmt::Create(Box::new(c)),
        Statement::Insert(ast::Insert { table: ast::TableObject::TableName(t), source: Some(q), columns, .. }) if columns.is_empty() => Stmt::Insert(object(&t), q.to_string()),
        Statement::Update(u) => {
            let set = u.assignments.iter().filter_map(|a| match &a.target {
                ast::AssignmentTarget::ColumnName(c) => Some((c.0.last()?.as_ident().map(ident)?, a.value.to_string())),
                _ => None,
            });
            Stmt::Update(relation(&u.table.relation)?, set.collect(), where_(&u.selection))
        }
        Statement::AlterTable(a) => match &a.operations[..] {
            [ast::AlterTableOperation::AddColumn { if_not_exists, column_def: c, .. }] => Stmt::AddColumn(object(&a.name), c.name.value.clone(), c.data_type.to_string(), *if_not_exists),
            [ast::AlterTableOperation::SetOptionsParens { options } | ast::AlterTableOperation::SetTblProperties { table_properties: options }] => Stmt::SetOptions(object(&a.name), options.iter().map(|o| match o {
                ast::SqlOption::KeyValue { key, value } => Some((key.value.to_lowercase(), value.to_string().trim_matches('\'').to_string())),
                _ => None,
            }).collect::<Option<_>>()?),
            _ => return None,
        },
        Statement::Delete(d) => {
            let (ast::FromTable::WithFromKeyword(t) | ast::FromTable::WithoutKeyword(t)) = &d.from;
            Stmt::Delete(relation(&t.first()?.relation)?, where_(&d.selection))
        }
        Statement::CreateSchema { schema_name: ast::SchemaName::Simple(n) | ast::SchemaName::NamedAuthorization(n, _), if_not_exists, .. } => {
            Stmt::Ddl(vec![Ddl::CreateSchema { name: object(&n), if_not_exists }])
        }
        Statement::Drop { object_type, if_exists, names, cascade, .. } => Stmt::Ddl(names.iter().map(object).map(|name| match object_type {
            ast::ObjectType::Schema => Some(Ddl::DropSchema { name, if_exists, cascade }),
            ast::ObjectType::Table => Some(Ddl::DropTable { name, if_exists }),
            ast::ObjectType::View | ast::ObjectType::MaterializedView => Some(Ddl::DropView { name, if_exists }),
            _ => None,
        }).collect::<Option<Vec<_>>>()?),
        Statement::CreateView(v) if v.materialized => {
            let options = match &v.options {
                ast::CreateTableOptions::With(o) | ast::CreateTableOptions::Options(o) => o.iter().filter_map(|o| match o {
                    ast::SqlOption::KeyValue { key, value } => Some((key.value.to_lowercase(), value.to_string().trim_matches('\'').to_string())),
                    _ => None,
                }).collect(),
                _ => Default::default(),
            };
            Stmt::Ddl(vec![Ddl::CreateMaterialized { name: object(&v.name), sql: v.query.to_string(), options }])
        }
        Statement::CreateView(v) => Stmt::Ddl(vec![Ddl::CreateView { name: object(&v.name), sql: v.query.to_string(), replace: v.or_replace }]),
        Statement::AttachDatabase { schema_name, database_file_name: ast::Expr::Value(v), .. } => match &v.value {
            ast::Value::SingleQuotedString(dir) | ast::Value::DoubleQuotedString(dir) => Stmt::Ddl(vec![Ddl::Attach { name: ident(&schema_name), dir: dir.clone() }]),
            _ => return None,
        },
        Statement::CreateDatabase { db_name, if_not_exists, location, .. } => Stmt::Ddl(vec![Ddl::CreateDatabase { name: object(&db_name).to_lowercase(), if_not_exists, dir: location }]),
        Statement::DetachDuckDBDatabase { if_exists, database_alias, .. } => Stmt::Ddl(vec![Ddl::Detach { name: ident(&database_alias), if_exists }]),
        Statement::Merge(m) => Stmt::Merge(Box::new(crate::change::merge_of(&m)?)),
        _ => return None,
    })
}

/// `CREATE TABLE t (a BIGINT, b VARCHAR, PRIMARY KEY (a)) [WITH (publish = 'delta,iceberg',
/// cluster_by = 'b', merge = 'total:sum', partition_by = 'day(ts)')]` → the table name and its spec. SQL types become Arrow
/// types the way DataFusion maps them.
/// With `AS SELECT`, the columns are the query's (run over `from`'s tables; `files`: local files
/// too, for `pondra sql` on its own machine).
async fn create_spec(c: &ast::CreateTable, from: &Lake, files: bool) -> Result<String> {
    let fields = match &c.query {
        Some(q) => {
            let sql = q.to_string();
            let ctx = session(from, &sql, "").await?;
            let ctx = if files { ctx.enable_url_table() } else { ctx };
            ctx.sql(&sql).await?.schema().fields().iter().cloned().collect::<Vec<_>>()
        }
        None => {
            let cols = c.columns.iter().map(|c| format!("{} {}", c.name, c.data_type)).collect::<Vec<_>>().join(", ");
            let ctx = SessionContext::new();
            ctx.sql(&format!("CREATE TABLE t ({cols})")).await?;
            ctx.table("t").await?.schema().fields().iter().cloned().collect()
        }
    };
    let columns: Vec<(String, String)> = fields.iter().map(|f| (f.name().clone(), crate::query::type_name(f.data_type()))).collect();
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
    let spec = j!({"columns": columns, "key": key, "merge": merge, "publish": list("publish"), "cluster_by": list("cluster_by"), "ttl": opts.get("ttl"), "partition_by": opts.get("partition_by")});
    Ok(spec.to_string())
}

/// Create a table (or change it: a spec sent again) from any node: the leader does it.
pub async fn define(app: &crate::server::App, name: &str, spec: &str) -> Result<Value> {
    if app.seq.is_none() {
        return Ok(http().post(format!("http://{}/tables/{name}", app.cluster.leader.addr)).body(spec.to_string()).send().await?.error_for_status()?.json().await?);
    }
    let _guard = app.lock.lock().await;
    create_table(&app.lake, name, spec).await
}

/// How a SQL type is kept: strings as Utf8; timestamps in microseconds, as Iceberg, Delta, Spark
/// and Postgres keep them.
pub fn stored(t: &DataType) -> DataType {
    match t {
        DataType::Utf8View | DataType::LargeUtf8 => DataType::Utf8,
        DataType::Timestamp(_, tz) => DataType::Timestamp(TimeUnit::Microsecond, tz.clone()),
        t => t.clone(),
    }
}

/// `ALTER TABLE t ADD COLUMN c TYPE`: the table's definition with the new column at the end, sent
/// like a CREATE TABLE (the leader adds it; old rows read it as null). None: IF NOT EXISTS, and it does.
async fn alter_spec(lake: &Lake, stmt: &Stmt) -> Result<Option<String>> {
    let table = stmt.table();
    let m: TableMeta = lake.cat.get(&table_key(&table)).await?.ok_or_else(|| anyhow::anyhow!("no table {table}"))?;
    let ttl = m.ttl.as_ref().map(|(c, s)| format!("{c}:{s}"));
    let mut spec = j!({"columns": m.columns, "key": m.key, "merge": m.merge, "publish": m.publish, "cluster_by": m.cluster, "ttl": ttl, "partition_by": m.partition});
    match stmt {
        Stmt::AddColumn(_, column, sql_type, if_not_exists) => {
            if m.columns.iter().any(|(c, _)| c == column) {
                ensure!(*if_not_exists, "{table} already has a column {column}");
                return Ok(None);
            }
            let ctx = SessionContext::new();
            ctx.sql(&format!("CREATE TABLE t ({column} {sql_type})")).await?;
            let mut columns = m.columns.clone();
            columns.push((column.to_string(), crate::query::type_name(ctx.table("t").await?.schema().field(0).data_type())));
            spec["columns"] = j!(columns);
        }
        // (files written from now on follow them; the ones before keep their order until merged)
        Stmt::SetOptions(_, options) => {
            let list = |v: &str| v.split(',').map(str::trim).filter(|s| !s.is_empty()).map(String::from).collect::<Vec<_>>();
            for (k, v) in options {
                match k.as_str() {
                    "publish" | "cluster_by" => spec[k] = j!(list(v)),
                    "ttl" => spec["ttl"] = j!(v),
                    "partition_by" => bail!("partition_by can't change: each of {table}'s files holds one partition"),
                    other => bail!("{other}: ALTER TABLE … SET takes publish, cluster_by and ttl"),
                }
            }
        }
        _ => bail!("not an ALTER TABLE"),
    }
    Ok(Some(spec.to_string()))
}

/// The rows a write to a keyed table upserts: the INSERT's query, the updated rows, or the rows
/// to delete (marked `_deleted`). Keys never change; merge tables only take INSERTs.
fn rows_sql(meta: &TableMeta, stmt: &Stmt) -> Result<String> {
    let q = |c: &str| format!("\"{c}\"");
    // (a changed row keeps its `_row_id` and `_created_at`: `sys.rs`)
    let select = |pick: &dyn Fn(&str) -> String, table: &str, cond: &Option<String>| {
        let cols = meta.columns.iter().map(|(c, _)| format!("{} AS {}", pick(c), q(c))).collect::<Vec<_>>().join(", ");
        format!("SELECT {cols}, \"_row_id\", \"_created_at\" FROM {} {}", sql_name(table), cond.as_ref().map(|w| format!("WHERE {w}")).unwrap_or_default())
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
        Stmt::Create(_) | Stmt::Define(..) | Stmt::AddColumn(..) | Stmt::SetOptions(..) | Stmt::Ddl(_) | Stmt::Merge(_) => unreachable!("not a row write here"),
    }
}

/// `CHECKPOINT` (DuckDB's word for it): tier every table's log into Parquet now, and write the
/// catalog down, so what other engines read is up to date.
pub fn checkpoint(sql: &str) -> bool {
    let code: String = sql.lines().map(|l| l.split("--").next().unwrap_or("")).collect::<Vec<_>>().join(" "); // (comments aside)
    code.trim().trim_end_matches(';').trim().eq_ignore_ascii_case("checkpoint")
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
    let batches = ctx.sql(&crate::asof::rewrite(sql)?).await?.collect().await?;
    let Some(first) = batches.first() else { return Ok(RecordBatch::new_empty(target)) };
    let all = concat_batches(&first.schema(), &batches)?;
    // An UPDATE's rows end with the ids they keep (`sys.rs`): those go along, by name.
    let kept: Vec<usize> = (0..all.num_columns()).filter(|&i| [crate::sys::ROW_ID, crate::sys::CREATED].contains(&all.schema().field(i).name().as_str())).collect();
    let values: Vec<usize> = (0..all.num_columns()).filter(|i| !kept.contains(i)).collect();
    let (ids, all) = (all.project(&kept)?, all.project(&values)?);
    // The given columns fill the table's in order — all of them, or all but `_deleted`, or the
    // first few (later ones, such as added columns, are null).
    let skip_deleted = all.num_columns() < target.fields().len();
    let mut given = all.columns().iter();
    let columns = target.fields().iter().map(|f| match given.len() > 0 && !(skip_deleted && f.name() == "_deleted") {
        true => cast(given.next().expect("counted"), f.data_type()),
        false => Ok(datafusion::arrow::array::new_null_array(f.data_type(), all.num_rows())),
    }).collect::<Result<Vec<_>, _>>()?;
    ensure!(given.len() == 0, "{} columns given, the table has {}", all.num_columns(), target.fields().len());
    let (mut fields, mut columns) = (target.fields().to_vec(), columns);
    fields.extend(ids.schema().fields().iter().cloned());
    columns.extend(ids.columns().iter().cloned());
    Ok(RecordBatch::try_new(Arc::new(datafusion::arrow::datatypes::Schema::new(fields)), columns)?)
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
/// `stamp`: the commit number and time the rows' system columns get (`sys.rs`); None: the leader
/// stamps them when it records the files (`record`).
pub async fn write_files(lake: &Lake, ctx: &SessionContext, table: &str, query: &str, job: &str, stamp: Option<(u64, u64)>) -> Result<Option<Files>> {
    use datafusion::arrow::datatypes::DataType;
    use datafusion::prelude::{cast as cast_to, Expr};
    if lake.cat.get::<u64>(&producer_key(&format!("job:{job}"))).await?.is_some() {
        return Ok(None);
    }
    // Each partition of the query writes its own files, in the order its rows come: data that
    // arrives in order (by time, by key) lands in files that each hold a narrow range of it, which
    // is what lets a filter skip files and a distributed query split tables by key (`spmd`).
    // (Round-robin repartitioning would interleave them, so it is off here.)
    ctx.state_ref().write().config_mut().options_mut().optimizer.enable_round_robin_repartition = false;
    // The query's columns, by position, as the table's (or, for a new table, with plain Utf8 strings).
    let df = ctx.sql(&crate::asof::rewrite(query)?).await?;
    let meta = lake.cat.get::<TableMeta>(&table_key(table)).await?;
    let target: Vec<(String, DataType)> = match &meta {
        Some(m) => schema(&m.columns)?.fields().iter().map(|f| (f.name().clone(), f.data_type().clone())).collect(),
        None => df.schema().fields().iter().map(|f| (f.name().clone(), if *f.data_type() == DataType::Utf8View { DataType::Utf8 } else { f.data_type().clone() })).collect(),
    };
    ensure!(df.schema().fields().len() <= target.len(), "{} columns given, table {table} has {}", df.schema().fields().len(), target.len());
    let given = df.schema().columns(); // (the table's first columns; any after them are null)
    let null = || datafusion::prelude::lit(datafusion::scalar::ScalarValue::Null);
    let exprs = target.iter().enumerate().map(|(i, (name, t))| cast_to(given.get(i).map_or_else(null, |c| Expr::Column(c.clone())), t.clone()).alias(name)).collect::<Vec<_>>();
    let df = df.select(exprs)?;
    let columns = target.iter().map(|(n, t)| (n.clone(), t.to_string())).collect();
    let partition = meta.as_ref().and_then(|m| m.partition.clone());
    let next = Arc::new(std::sync::atomic::AtomicU64::new(0)); // (the streams share one block of ids)
    let streams = df.execute_stream_partitioned().await?.into_iter().map(|rows| match stamp {
        Some(reserved) => crate::sys::stamp_stream(rows, next.clone(), reserved),
        None => Ok(rows),
    });
    let writes = streams.collect::<Result<Vec<_>>>()?.into_iter().map(|rows| crate::tier::write_stream(lake, table, rows, 1_000_000, &[], true, partition.as_deref()));
    let files = futures::future::try_join_all(writes).await?.concat();
    Ok(Some(Files { table: table.into(), job: job.into(), columns, files }))
}

/// Leader: record an INSERT's files in one commit, creating the table if it's new. Files written
/// without their rows' system columns (their writer couldn't reserve a commit number: `pondra
/// sql`, the inbox) get them here first: rewritten, stamped (`seq`).
pub async fn record(lake: &Lake, mut f: Files, seq: Option<&Sequencer>) -> Result<Value> {
    let producer = producer_key(&format!("job:{}", f.job));
    if lake.cat.get::<u64>(&producer).await?.is_some() {
        return Ok(j!({"duplicate": true})); // the same job finished concurrently
    }
    let new = || TableMeta { columns: f.columns.clone(), publish: default_publish(), ids: true, ..Default::default() };
    let mut meta = lake.cat.get::<TableMeta>(&table_key(&f.table)).await?.unwrap_or_else(new);
    // Types as the lake stores them, so a writer may name them its own way (`Utf8View`, `VARIANT`).
    let types = |c: &[(String, String)]| c.iter().map(|(_, t)| crate::query::dtype(t).map(|t| crate::query::type_name(&t)).unwrap_or_else(|_| t.clone())).collect::<Vec<_>>();
    ensure!(meta.key.is_empty(), "INSERT into a keyed table goes through the log");
    ensure!(types(&meta.columns) == types(&f.columns), "query columns {:?} don't match table {}", f.columns, f.table);
    if let (Some(seq), true) = (seq, f.files.iter().any(|d| !d.sys)) {
        f.files = stamp_files(lake, &f.table, f.files, seq.reserve().await?).await?;
    }
    let rows: u64 = f.files.iter().map(|f| f.rows).sum();
    let ord = lake.visible(); // (append tables: files in the order they arrived)
    let mut files: Vec<DataFile> = f.files.into_iter().map(|f| DataFile { ord, ..f }).collect();
    crate::sketch::add(&mut meta, &mut files);
    meta.files.extend(files);
    if meta.files.len() > 4 * crate::manifest::INLINE {
        crate::manifest::seal(lake, &f.table, &mut meta).await?; // (a big INSERT's many files: sealed at once; small ones are merged first, by `tier::maintain`)
    }
    lake.cat.commit(vec![(table_key(&f.table), json(&meta)), (producer, json(&1u64))], &[]).await?;
    Ok(j!({"rows": rows}))
}

/// Files rewritten with their rows' system columns (`record`); the old ones are deleted.
async fn stamp_files(lake: &Lake, table: &str, files: Vec<DataFile>, reserved: (u64, u64)) -> Result<Vec<DataFile>> {
    let mut out = vec![];
    let mut first = (reserved.0 << 32) as i64;
    for d in files {
        if d.sys {
            out.push(d);
            continue;
        }
        let batches = lake.session().read_parquet(lake.full(&d.path), Default::default()).await?.collect().await?;
        let stamped = batches.iter().map(|b| {
            let s = crate::sys::stamp_new(b, first, reserved);
            first += b.num_rows() as i64;
            s
        }).collect::<Result<Vec<_>>>()?;
        if let Some(new) = crate::tier::write_file(lake, table, &stamped, &[], true).await? {
            out.push(DataFile { part: d.part.clone(), ..new });
        }
        lake.delete(&d.path).await;
    }
    Ok(out)
}

// ---------------------------------------------------------------- on a node

/// `POST /sql` with a write statement: this node does the work; the leader records it.
pub async fn on_node(app: &crate::server::App, stmt: Stmt, job: Option<String>) -> Result<Value> { on_node_as(app, stmt, job, false).await }

/// `on_node`; `files`: its SQL may read files on this machine (`FROM 'jan.csv'`: the shell's own
/// node, `server::owner`).
pub async fn on_node_as(app: &crate::server::App, stmt: Stmt, job: Option<String>, files: bool) -> Result<Value> {
    ensure!(!app.cluster.reader, "read-only node");
    let open = |ctx: SessionContext| if files { ctx.enable_url_table() } else { ctx };
    let (lake, job) = (&app.lake, job.unwrap_or_else(|| uuid::Uuid::new_v4().to_string()));
    if let Stmt::Ddl(ddls) = stmt {
        let mut out = j!({});
        for d in ddls {
            out = if app.seq.is_some() {
                let _guard = app.lock.lock().await;
                crate::ddl::apply(lake, d.clone()).await?
            } else {
                post(&app.cluster.leader.addr, &Request::Ddl(d.clone())).await?
            };
            crate::ddl::settle(lake, &d, &app.cluster.addr).await?; // (ATTACH, DETACH: here at once)
        }
        return Ok(out);
    }
    // CREATE TABLE … AS SELECT: the table, with the query's columns, then its rows.
    if let Stmt::Create(c) = &stmt {
        if let Some(q) = &c.query {
            let (name, query) = (object(&c.name), q.to_string());
            Box::pin(on_node_as(app, Stmt::Define(name.clone(), create_spec(c, lake, files).await?), Some(job.clone()), files)).await?;
            return Box::pin(on_node_as(app, Stmt::Insert(name, query), Some(job), files)).await;
        }
    }
    // UPDATE and DELETE of an append table, and MERGE: the leader's, from one snapshot (`change.rs`).
    if let Some(sql) = changes(lake, &stmt).await? {
        return match &app.seq {
            Some(seq) => {
                let _guard = app.lock.lock().await;
                crate::change::FILES.scope(files, crate::change::run(lake, seq, &sql, &job)).await
            }
            None => post(&app.cluster.leader.addr, &Request::Change(sql, job)).await, // (the leader can't read this machine's files)
        };
    }
    // A table of an attached lake: the work runs here, that lake's leader records it.
    let (other, table) = crate::ddl::resolve(lake, &stmt.table()).await?;
    if let Some(other) = other {
        let req = prepare(lake, &other, &table, &stmt, &job, files).await?;
        return deliver(&other.url, Some(req), &stmt, &job, false).await;
    }
    let stmt = stmt.on(table.clone());
    match &stmt {
        Stmt::Create(c) => return define(app, &table, &create_spec(c, lake, files).await?).await,
        Stmt::Define(_, spec) => return define(app, &table, spec).await,
        Stmt::AddColumn(t, ..) | Stmt::SetOptions(t, _) => {
            return match alter_spec(lake, &stmt).await? {
                Some(spec) => define(app, t, &spec).await,
                None => Ok(j!({"table": t, "unchanged": true})),
            };
        }
        _ => {}
    }
    let meta = lake.cat.get::<TableMeta>(&table_key(&table)).await?;
    let sql = match (&meta, &stmt) {
        (Some(m), _) if through_log(lake, &table, m).await? => rows_sql(m, &stmt)?,
        (_, Stmt::Insert(_, query)) => {
            let ctx = open(session(lake, query, "").await?);
            let Some(f) = write_files(lake, &ctx, &table, query, &job, Some(app.to().reserve().await?)).await? else { return Ok(j!({"duplicate": true})) };
            return app.record_files(f).await;
        }
        (None, _) => bail!("no table {table}"),
        _ => bail!("UPDATE and DELETE need a keyed table (append tables only take INSERTs)"),
    };
    let meta = meta.expect("a table");
    let batch = rows(&open(session(lake, &sql, "").await?), &meta, &sql).await?;
    let n = batch.num_rows();
    let ack = app.log()?.append(table, Src { producer: format!("sql:{job}"), seq: 1, prev: None }, batch).await?;
    Ok(if ack.duplicate { j!({"duplicate": true}) } else { j!({"rows": n}) })
}

/// The statement as the leader's to carry out (`change.rs`), if it is one: an UPDATE or DELETE of
/// an append table, or a MERGE.
async fn changes(lake: &Lake, stmt: &Stmt) -> Result<Option<String>> {
    let (Stmt::Update(..) | Stmt::Delete(..) | Stmt::Merge(_)) = stmt else { return Ok(None) };
    let (other, table) = crate::ddl::resolve(lake, &stmt.table()).await?;
    let lake = other.as_deref().unwrap_or(lake);
    let append = lake.cat.get::<TableMeta>(&table_key(&table)).await?.is_some_and(|m| m.key.is_empty());
    Ok(if append || matches!(stmt, Stmt::Merge(_)) { stmt.change_sql() } else { None })
}

// ---------------------------------------------------------------- what the leader records

/// What a writer asks the leader to record: new files, a log flush, or a table.
pub enum Request {
    Files(Files),
    Flush(Bytes), // the body of POST /cluster/commit
    Table(String, String),
    Ddl(crate::ddl::Ddl),
    Change(String, String), // an UPDATE, DELETE or MERGE the leader carries out (`change.rs`): SQL, job
}

impl Request {
    /// Where it goes over HTTP, and its body; its name and body in the bucket inbox.
    pub fn http(&self) -> Result<(String, Vec<u8>)> {
        Ok(match self {
            Request::Files(f) => ("/cluster/files".into(), serde_json::to_vec(f)?),
            Request::Flush(b) => ("/cluster/commit".into(), b.to_vec()),
            Request::Table(name, spec) => (format!("/tables/{name}"), spec.clone().into_bytes()),
            Request::Ddl(d) => ("/cluster/ddl".into(), serde_json::to_vec(d)?),
            Request::Change(sql, job) => ("/cluster/change".into(), serde_json::to_vec(&(sql, job))?),
        })
    }

    pub fn inbox(&self) -> Result<(String, Vec<u8>)> {
        Ok(match self {
            Request::Table(name, spec) => ("table".into(), serde_json::to_vec(&(name, spec))?),
            Request::Files(_) => ("files".into(), self.http()?.1),
            Request::Flush(_) => ("flush".into(), self.http()?.1),
            Request::Ddl(_) => ("ddl".into(), self.http()?.1),
            Request::Change(..) => ("change".into(), self.http()?.1),
        })
    }

    pub fn from_inbox(kind: &str, body: Bytes) -> Result<Request> {
        Ok(match kind {
            "files" => Request::Files(serde_json::from_slice(&body)?),
            "flush" => Request::Flush(body),
            "ddl" => Request::Ddl(serde_json::from_slice(&body)?),
            "change" => {
                let (sql, job): (String, String) = serde_json::from_slice(&body)?;
                Request::Change(sql, job)
            }
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
            record(lake, f, Some(seq)).await
        }
        Request::Flush(body) => Ok(serde_json::to_value(seq.submit(decode_flush(body)?).await?)?),
        Request::Table(name, spec) => {
            let _guard = lock.lock().await;
            create_table(lake, &name, &spec).await
        }
        Request::Ddl(d) => {
            let _guard = lock.lock().await;
            crate::ddl::apply(lake, d).await
        }
        Request::Change(sql, job) => {
            let _guard = lock.lock().await;
            crate::change::run(lake, seq, &sql, &job).await
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
    match stmt {
        // CREATE TABLE … AS SELECT: the table, with the query's columns, then its rows.
        Stmt::Create(c) if c.query.is_some() => {
            let lake = Lake::open(dir, false, false).await?;
            let (name, query) = (object(&c.name), c.query.as_ref().expect("a query").to_string());
            Box::pin(one_from_cli(dir, Stmt::Define(name.clone(), create_spec(&c, &lake, true).await?))).await?;
            Box::pin(one_from_cli(dir, Stmt::Insert(name, query))).await
        }
        Stmt::Ddl(ddls) => {
            let mut out = j!({});
            for d in ddls {
                out = Box::pin(one_from_cli(dir, Stmt::Ddl(vec![d]))).await?;
            }
            Ok(out)
        }
        stmt => one_from_cli(dir, stmt).await,
    }
}

async fn one_from_cli(dir: &str, stmt: Stmt) -> Result<Value> {
    let job = std::env::var("PONDRA_JOB").unwrap_or_else(|_| uuid::Uuid::new_v4().to_string());
    std::env::set_var("PONDRA_JOB", &job); // (if fenced, the process restarts: the same job again)
    // (A lake with no catalog yet can't be read: then the work is done once this process leads.)
    let (req, stmt, to) = match Lake::open(dir, false, false).await {
        Ok(lake) => {
            crate::ddl::sync(&lake, "", false).await?; // (lakes attached by ATTACH: read here too)
            let (other, table) = crate::ddl::resolve(&lake, &stmt.table()).await?;
            let (stmt, target) = (stmt.on(table.clone()), other.unwrap_or_else(|| lake.clone())); // (a write to an attached lake goes to its leader)
            (Some(prepare(&lake, &target, &table, &stmt, &job, true).await?), stmt, target.url.clone())
        }
        Err(_) => (None, stmt, dir.to_string()),
    };
    deliver(&to, req, &stmt, &job, true).await
}

/// Have the leader of the lake at `dir` record a write: over HTTP, through the bucket inbox if it
/// can't be reached, or, when nobody leads, by leading for a moment: here (`one_off`: a `pondra
/// sql` process, which ends right after), or, from a node, in a `pondra sql` of its own.
async fn deliver(dir: &str, mut req: Option<Option<Request>>, stmt: &Stmt, job: &str, one_off: bool) -> Result<Value> {
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
                if let Some(v) = crate::inbox::send(&store, r, None).await? {
                    return summary(matches!(r, Request::Flush(_)), v);
                }
            }
            _ if !one_off => {
                let Some(Some(r)) = &req else { return Ok(j!({"duplicate": true})) };
                if let Some(v) = crate::inbox::send(&store, r, Some(dir)).await? {
                    return summary(matches!(r, Request::Flush(_)), v);
                }
            }
            t => {
                let Some(term) = claim(&store, t.map_or(1, |t| t.n + 1), "").await? else { continue };
                return match lead(dir, &store, term.n, req, stmt, job).await {
                    // Another one-off writer took the catalog while this one was leading (both
                    // found the lake idle). Start over with the same job: whoever wins records it
                    // once, and the rest go through them.
                    Err(e) if fenced(&e) && tries() < 5 => crate::cluster::restart("fenced while writing"),
                    r => r,
                };
            }
        }
    }
}

/// Do the work of a write here: the query runs over `query`'s tables (and attached lakes'), the
/// result goes into `target`'s `table`. What its leader has to record (None: a retried job).
/// `files`: the query may read local files (`FROM 'jan.parquet'`) — on the author's own machine.
async fn prepare(query: &Lake, target: &Arc<Lake>, table: &str, stmt: &Stmt, job: &str, files: bool) -> Result<Option<Request>> {
    let open = |ctx: SessionContext| if files { ctx.enable_url_table() } else { ctx };
    match stmt {
        Stmt::Create(c) => return Ok(Some(Request::Table(table.into(), create_spec(c, query, files).await?))),
        Stmt::Define(_, spec) => return Ok(Some(Request::Table(table.into(), spec.clone()))),
        Stmt::Ddl(d) => return Ok(Some(Request::Ddl(d.first().cloned().ok_or_else(|| anyhow::anyhow!("nothing to do"))?))),
        _ => {}
    }
    if let Stmt::AddColumn(..) | Stmt::SetOptions(..) = stmt {
        return Ok(alter_spec(target, stmt).await?.map(|spec| Request::Table(table.into(), spec)));
    }
    if let Some(sql) = changes(target, stmt).await? {
        return Ok(Some(Request::Change(sql, job.into())));
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
            Ok(write_files(target, &ctx, table, sql, job, None).await?.map(Request::Files)) // (stamped as its leader records them)
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

/// Was this writer fenced out of the catalog by a newer one?
fn fenced(e: &anyhow::Error) -> bool {
    e.downcast_ref::<slatedb::Error>().is_some_and(|e| matches!(e.kind(), slatedb::ErrorKind::Closed(_))) || format!("{e:#}").contains("newer DB client")
}

/// How many times this command has already started over (it re-runs itself, keeping `PONDRA_JOB`).
fn tries() -> u32 {
    let n = std::env::var("PONDRA_TRIES").ok().and_then(|t| t.parse().ok()).unwrap_or(0);
    std::env::set_var("PONDRA_TRIES", (n + 1).to_string());
    n
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
