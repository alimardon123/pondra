//! Writes in SQL, from anywhere: `CREATE TABLE`, `INSERT`, `UPDATE`, `DELETE`, sent to any node
//! (`POST /sql`) or run with the binary on any machine (`pondra sql`). Whoever runs the statement
//! does the work — runs the query, writes the Parquet or the log segment — and the lake only has
//! to record the result: one small commit, the leader's job (`Request`, `handle`). A machine that
//! isn't a node reaches the leader over HTTP, through the bucket when it can't (`inbox.rs`), or
//! leads for a moment itself when nobody does.
use crate::cluster::http;
use crate::terms::{alive, claim, latest, release};
use crate::log::{decode_flush, encode_flush, pack, Append, Outcome, Sequencer, Src};
use crate::query::session;
use crate::answer::schema;
use crate::store::*;
use anyhow::{bail, ensure, Context, Result};
use bytes::Bytes;
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{self, Statement};
use serde::{Deserialize, Serialize};
use serde_json::{json as j, Value};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use crate::text::{first_word, head, ident, object};
pub(crate) use crate::rules::fit::{by_position, implicit};
pub(crate) use crate::rules::create::{definition, option_text};
#[cfg(test)] pub(crate) use crate::rules::fit::fit;

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
        #[serde(default)]
        order_by: Option<String>, // keyed tables: the column whose greatest value wins (event time)
        #[serde(default)]
        not_null: Vec<String>, // columns every row must give (`defaults.rs`)
        #[serde(default)]
        defaults: BTreeMap<String, String>, // column -> SQL expression for rows that leave it out
        properties: Option<BTreeMap<String, String>>, // other engines' (`write.delete.mode`, say): published for them
        #[serde(default)]
        checks: Vec<(String, String)>, // CHECK constraints: (name, condition), made with the table
        #[serde(default)]
        retention: Option<String>, // how long its past is kept: '7 days' (ADR-043)
        #[serde(default)]
        identity: BTreeMap<String, crate::seq::Identity>, // identity columns: numbered by sequences made with the table (`seq.rs`)
        #[serde(default)]
        constraints: Vec<crate::constraints::Constraint>, // UNIQUE, and keys and references said NOT ENFORCED (`constraints.rs`)
        #[serde(default)]
        enums: BTreeMap<String, crate::types::Enum>, // enum columns: the labels each takes (`types.rs`)
        #[serde(default)]
        sql_types: BTreeMap<String, String>, // columns held as text of another SQL type: "uuid", "json", "jsonb", "varchar(n)" (`types.rs`)
        #[serde(default)]
        made_by: Option<String>, // the job of the CREATE TABLE that sends it (`made_by_job`); none: sent again freely
    },
}

/// A CREATE TABLE of a table that's there, refused as Postgres refuses it (42P07).
fn already(name: &str) -> anyhow::Error {
    crate::codes::coded("42P07", format!("table {name} already exists (CREATE TABLE IF NOT EXISTS leaves it as it is; CREATE OR REPLACE TABLE makes it anew)"))
}

/// A table's definition, marked with the job of the CREATE TABLE that sends it: a `CREATE TABLE … AS SELECT` (or a
/// plain one) sends its definition under its own job only, so another statement's is refused while the table is there.
pub(crate) fn made_by_job(spec: String, job: &str) -> Result<String> {
    let mut spec: Value = serde_json::from_str(&spec)?; // (create_spec's: always an object)
    spec["made_by"] = job.into();
    Ok(spec.to_string())
}

/// Leader: create a table (sent again for an existing one, `publish` changes). The caller holds
/// the lock that serialises table rewrites.
pub async fn create_table(lake: &Lake, name: &str, spec: &str) -> Result<Value> {
    let name = &new_table(lake, name).await?; // (its schema exists; `public.t` is `t`; a staged name is checked by its table)
    let owner = crate::sys::staged_for(name.as_str()).unwrap_or(name.as_str()); // (a staged table takes its folder and sequences' names from the table it replaces)
    ensure!(lake.cat.get::<crate::ddl::StoredView>(&crate::ddl::query_key(name)).await?.is_none(), "{name} is a view");
    let (columns, key, merge, publish, cluster, ttl, partition, order, not_null, mut defaults, properties, checks, retention, identity, constraints, enums, sql_types, made_by) = match serde_json::from_str(spec)? {
        TableSpec::Columns(c) => (c, vec![], BTreeMap::new(), None, None, None, None, None, vec![], BTreeMap::new(), None, vec![], None, BTreeMap::new(), vec![], BTreeMap::new(), BTreeMap::new(), None),
        TableSpec::Full { columns, key, merge, publish, cluster_by, ttl, partition_by, order_by, not_null, defaults, properties, checks, retention, identity, constraints, enums, sql_types, made_by } => (columns, key, merge, publish, cluster_by, ttl, partition_by, order_by, not_null, defaults, properties, checks, retention, identity, constraints, enums, sql_types, made_by),
    };
    // (what the definition gets wrong is the statement's: 42P16 when it says no code of its own)
    let (retention, columns, ttl) = async {
        let retention = retention.as_deref().map(crate::ddl::retention).transpose()?;
        // Types as the lake records them: `VARIANT` is JSON text, `Float32[]` a list (see `answer::dtype`).
        let columns = columns.iter().map(|(n, t)| Ok((n.clone(), crate::answer::type_name(&crate::answer::dtype(t)?)))).collect::<Result<Vec<_>>>()?;
        let ttl = ttl.map(|t| -> Result<(String, u64)> {
            let (c, s) = t.split_once(':').ok_or_else(|| anyhow::anyhow!("ttl: \"column:seconds\""))?;
            ensure!(!key.is_empty() && columns.iter().any(|(n, ty)| n == c && (ty.starts_with("Timestamp") || ty.starts_with("Date"))), "TTL: a timestamp or date column of a keyed table");
            Ok((c.to_string(), s.trim().parse()?))
        }).transpose()?;
        schema(&columns)?; // validate types
        ensure!(columns.iter().all(|(c, _)| !crate::text::NAMES.contains(&c.as_str()) && c != "_old_version"), "{} are system columns: every table has them (`SELECT _row_id, * FROM t`)", crate::text::NAMES.join(", "));
        ensure!(merge.values().all(|f| ["sum", "count", "min", "max", crate::finish::MOMENTS].contains(&f.as_str())), "MERGE: sum, count, min or max"); // (moments: a view's variance, `finish.rs`; an ALTER of its table sends them back)
        ensure!(merge.is_empty() || !key.is_empty(), "MERGE needs a key: a merge table combines the rows of each key");
        ensure!(merge.keys().all(|c| columns.iter().any(|(n, _)| n == c)), "MERGE: columns of the table");
        if let Some((c, _)) = columns.iter().find(|(c, _)| !merge.is_empty() && !key.contains(c) && !merge.contains_key(c)) {
            bail!("a merge table combines every column but its key: {c} needs a merge function too ({c} … MERGE sum|count|min|max)");
        }
        ensure!(publish.iter().flatten().all(|f| ["delta", "iceberg"].contains(&f.as_str())), "publish formats: delta, iceberg");
        // (A keyed table takes them too: each tiering round's newest rows go into a file per
        // partition, sorted by cluster_by, then the key; reads let a newer round's key shadow an older
        // round's wherever its partition, `query::upsert_view`.)
        ensure!(cluster.iter().flatten().all(|c| columns.iter().any(|(n, _)| n == c)), "CLUSTER BY: columns of the table");
        ensure!(order.is_none() || (!key.is_empty() && merge.is_empty()), "SEQUENCE BY: a keyed table's (PRIMARY KEY), whose rows replace each other");
        ensure!(order.iter().all(|o| columns.iter().any(|(n, _)| n == o) && !key.contains(o)), "SEQUENCE BY: a column of the table, not its key");
        if let Some(p) = &partition {
            crate::tier::check_partition(p, &columns)?;
        }
        ensure!(not_null.iter().chain(defaults.keys()).chain(identity.keys()).all(|c| columns.iter().any(|(n, _)| n == c)), "NOT NULL, DEFAULT and identity: columns of the table");
        crate::rules::create::constraint_columns(&columns, &constraints)?;
        if let Some(c) = enums.keys().find(|c| !columns.iter().any(|(n, t)| n == *c && t == "Utf8")) {
            bail!("{c}: an enum column holds text (a column of the table, of type VARCHAR)");
        }
        if let Some((c, t)) = sql_types.iter().find(|(c, t)| !crate::rules::typed::held(t) || !columns.iter().any(|(n, ty)| n == *c && ty == "Utf8")) {
            bail!("{c} {t}: a uuid, JSON or VARCHAR(n) column holds text (a column of the table, of type VARCHAR)");
        }
        for (c, expr) in &defaults {
            ensure!(!identity.contains_key(c), "{c} is an identity column: its values come from its sequence, not a DEFAULT");
            crate::seq::named_exist(lake, expr).await?; // (DEFAULT nextval('s'): s is there)
        }
        crate::rules::create::identity_types(&columns, &identity)?;
        crate::defaults::definition(&columns, &defaults, &checks).await?; // (a DEFAULT, a CHECK: as a temporary table's are)
        Ok((retention, columns, ttl))
    }
    .await
    .map_err(definition)?;
    // (a key's columns are NOT NULL too: a row with no key has nothing to be found by)
    let not_null: Vec<String> = columns.iter().map(|(c, _)| c).filter(|c| crate::rules::create::required(c.as_str(), &not_null, &key, &identity)).cloned().collect();
    let mut defaulted = publish.is_none(); // (a new table published as the node's --publish says)
    let mut meta = match lake.cat.get::<TableMeta>(&table_key(name)).await? {
        // (A new table reads the log from now on: a table of this name dropped earlier left rows
        // in segments that aren't expired yet.)
        None => {
            crate::ddl::unclaimed(lake, owner).await?;
            // (a renamed table may still have this name's folder; a staged table's folder is its table's name, unless taken)
            // (a staged table replacing a live one takes that table's folder: its new files go beside the old one's (uuid
            // names), so the published copies stay where other engines read them)
            let live = match owner != name.as_str() {
                true => lake.cat.get::<TableMeta>(&table_key(owner)).await?,
                false => None,
            };
            let folder = match live {
                Some(old) => Some(old.folder(owner).to_string()),
                None => crate::ddl::free_folder(lake, owner).await?.or_else(|| (owner != name.as_str()).then(|| owner.to_string())),
            };
            let mut identity = identity;
            for (c, i) in identity.iter_mut() {
                let t = &columns.iter().find(|(n, _)| n == c).expect("a column").1;
                let as_type = match t.as_str() { "Int16" => "smallint", "Int32" => "integer", _ => "bigint" };
                i.sequence = crate::seq::owned(lake, owner, c, [vec![crate::seq::Opt::As(as_type.into())], i.declared.clone()].concat()).await?;
                defaults.insert(c.clone(), crate::rules::create::identity_default(&i.sequence));
            }
            TableMeta { columns, key, merge, publish: publish.unwrap_or_else(default_publish), cluster: cluster.unwrap_or_default(), ttl, partition, order, not_null, defaults, checks, identity, constraints, enums, sql_types, folder, tiered: lake.visible(), made: Some((lake.visible(), crate::log::now_ms())), made_by, ids: true, properties: properties.unwrap_or_default(), retention_secs: retention, ..Default::default() }
        }
        Some(mut m) => {
            // (a CREATE TABLE … AS SELECT's definition is sent again only under the job that made the table: the same
            // statement's re-send falls through to the logic below, another's is refused while the table is there)
            if made_by.is_some() && m.made_by != made_by {
                return Err(already(name));
            }
            defaulted = false;
            // (the spec names columns as SQL does; the table keeps its stored names: ADR-022)
            let l = m.logical();
            ensure!(partition.is_none() || partition == l.partition, "{name}'s partition_by can't change");
            // Sent again: columns may only grow at the end (ALTER TABLE … ADD COLUMN; a re-sent
            // original definition is fine), and `publish` may change.
            let (old, new) = (l.columns.len(), columns.len());
            ensure!(columns[..new.min(old)] == l.columns[..new.min(old)], "{name} exists with other columns (only new ones can be added, at the end)");
            let republish = publish.as_ref().is_some_and(|p| *p != m.publish);
            // (Delta and Iceberg name a table's files under its folder; a clone lists others': ADR-043)
            ensure!(!republish || m.shares.is_empty() || publish.as_ref().is_some_and(|p| p.is_empty()), "{name} is a clone, whose files are partly another table's: it can't be published (CREATE TABLE … AS SELECT * FROM {name} copies it)");
            if publish.iter().flatten().any(|f| !m.publish.contains(f)) {
                crate::ddl::publishes_alone(lake, name, m.folder(name), &[name]).await?;
            }
            let (recluster, rettl, reorder) = (cluster.as_ref().is_some_and(|c| *c != l.cluster), ttl.is_some() && ttl != l.ttl, order.is_some() && order != l.order);
            let reprop = properties.as_ref().is_some_and(|p| *p != m.properties);
            let reretain = retention.is_some() && retention != m.retention_secs;
            if new <= old && !republish && !recluster && !rettl && !reorder && !reprop && !reretain {
                return Ok(j!({"table": name, "publish": m.publish}));
            }
            if new > old {
                if let Some(c) = columns[old..].iter().map(|(c, _)| c).find(|c| not_null.contains(c) && !key.contains(c) || defaults.contains_key(*c) || identity.contains_key(*c)) {
                    bail!("{name}.{c}: a column added to a table can't be NOT NULL, an identity or have a DEFAULT, since the rows already there have no value for it (add it, then UPDATE {name} SET {c} = …)");
                }
                // A new column is stored under its name, or, if an older column (renamed or
                // dropped) is stored under that, `name~2`, `name~3`…: files are never rewritten.
                for (c, t) in &columns[old..] {
                    let taken = |s: &str| m.columns.iter().any(|(n, _)| n == s);
                    let s = (1..).map(|i| if i == 1 { c.clone() } else { format!("{c}~{i}") }).find(|s| !taken(s)).expect("a free name");
                    if s != *c {
                        m.names.insert(s.clone(), c.clone());
                    }
                    if let Some(e) = enums.get(c) {
                        m.enums.insert(s.clone(), e.clone()); // (ALTER TABLE … ADD COLUMN c mood)
                    }
                    if let Some(t) = sql_types.get(c) {
                        m.sql_types.insert(s.clone(), t.clone()); // (ALTER TABLE … ADD COLUMN c UUID)
                    }
                    m.columns.push((s, t.clone()));
                }
                if m.changed {
                    // (its replaced rows' table takes the column too, before its `_old_version`)
                    let del = crate::sys::deleted(name);
                    if let Some(mut d) = lake.cat.get::<TableMeta>(&table_key(&del)).await? {
                        d.columns = m.columns.iter().cloned().chain([("_old_version".to_string(), "Int64".to_string())]).collect();
                        d.names = m.names.clone();
                        lake.cat.commit(vec![(table_key(&del), json(&d))], &[]).await?;
                    }
                }
            }
            let stored = |c: &String| m.stored(c).unwrap_or(c).to_string();
            let cluster = cluster.map(|c| c.iter().map(stored).collect());
            let ttl = ttl.map(|(c, s)| (stored(&c), s));
            let order = order.map(|o| stored(&o));
            m.cluster = cluster.unwrap_or(m.cluster);
            m.ttl = ttl.or(m.ttl);
            m.order = order.or(m.order);
            m.properties = properties.unwrap_or(m.properties);
            m.retention_secs = retention.or(m.retention_secs);
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
    // (a format the table can't be published in: refused when asked for, or when a column added
    // to a published table has no type there; left out of the node's --publish with a notice)
    let notice = if defaulted { crate::delta::left_out(name, &meta) } else { None };
    if let Some((f, why)) = meta.publish.iter().find_map(|f| crate::delta::refused(&meta, f).map(|why| (f, why))) {
        ensure!(defaulted, "{name} can't be published as {f}: {why}");
    }
    meta.publish = meta.publish.iter().filter(|f| crate::delta::refused(&meta, f).is_none()).cloned().collect();
    lake.cat.commit(vec![(table_key(name), json(&meta))], &[]).await?;
    if !meta.publish.is_empty() {
        crate::delta::publish_all(lake).await?; // (an empty table is there for other engines at once: they may append to it, ADR-028)
    }
    let mut out = j!({"table": name, "publish": meta.publish});
    if let Some(n) = notice {
        out["notice"] = n.into();
    }
    Ok(out)
}

// ---------------------------------------------------------------- statements

/// A write statement. Tables are named as SQL resolves them (`object`): `t`, `schema.t` or
/// `lake.schema.t`, unquoted parts in lower case.
pub enum Stmt {
    Create(Box<ast::CreateTable>),                       // (with a query: CREATE TABLE … AS SELECT)
    Define(String, String),                              // table, its definition (CREATE TABLE … AS SELECT's first step)
    Insert(String, String),                              // table, the query giving the rows
    InsertInto(String, Vec<String>, String),             // INSERT INTO t (b, a) …: the columns it names (`whole_rows`)
    Update(String, Vec<(String, String)>, Option<String>), // table, column = expression, WHERE
    Delete(String, Option<String>),                      // table, WHERE
    AddColumn(String, String, String, bool),             // table, column, SQL type, IF NOT EXISTS
    SetOptions(String, Vec<(String, String)>),           // table, ALTER TABLE … SET (publish = 'delta', cluster_by = 'user', ttl = 'ts:3600')
    Ddl(Vec<crate::ddl::Ddl>),                            // schemas, views, drops: the leader's (`ddl.rs`)
    Merge(Box<crate::change::Merge>),                     // MERGE INTO … (`change.rs`)
    Invalid(String),                                      // CREATE PROCEDURE or DROP MACRO, written wrong: why
    CopyTo(String, String, std::collections::BTreeMap<String, String>), // COPY (query) TO 'url' (options): files outside the lake (`ext.rs`)
    CopyInto(String, crate::copy::Source, std::collections::BTreeMap<String, String>), // COPY INTO t FROM 'path' (options): each file loaded once a version (`copy.rs`)
    TempView(String, String, bool, bool),                  // CREATE [OR REPLACE] TEMP VIEW [IF NOT EXISTS] name AS query: the session's (`temp.rs`)
    TempSecret(String, std::collections::BTreeMap<String, String>, bool, bool), // CREATE [OR REPLACE] TEMPORARY SECRET [IF NOT EXISTS] name (…): the session's, in memory (`temp.rs`)
}

impl Stmt {
    /// The table it writes.
    pub fn table(&self) -> String {
        match self {
            Stmt::Create(c) => object(&c.name),
            Stmt::Define(t, _) | Stmt::Insert(t, _) | Stmt::InsertInto(t, ..) | Stmt::Update(t, ..) | Stmt::Delete(t, _) | Stmt::AddColumn(t, ..) | Stmt::SetOptions(t, _) | Stmt::CopyInto(t, ..) => t.clone(),
            Stmt::Ddl(_) | Stmt::Invalid(_) | Stmt::CopyTo(..) => String::new(),
            Stmt::TempView(v, ..) | Stmt::TempSecret(v, ..) => v.clone(),
            Stmt::Merge(m) => m.target.clone(),
        }
    }

    /// An UPDATE, DELETE, MERGE or checked INSERT as SQL again, for the leader to carry out (`change.rs`).
    fn change_sql(&self) -> Option<String> {
        let w = |cond: &Option<String>| cond.as_ref().map(|c| format!(" WHERE {c}")).unwrap_or_default();
        match self {
            // (a DEFAULT is written bare: `(DEFAULT)` would be read as a column, `rows_sql` takes the word)
            Stmt::Update(t, set, cond) => Some(format!("UPDATE {} SET {}{}", sql_name(t), set.iter().map(|(c, e)| match crate::rules::rows::is_word(e) {
                true => format!("\"{c}\" = DEFAULT"),
                false => format!("\"{c}\" = ({e})"),
            }).collect::<Vec<_>>().join(", "), w(cond))),
            Stmt::Delete(t, cond) => Some(format!("DELETE FROM {}{}", sql_name(t), w(cond))),
            Stmt::Merge(m) => Some(m.sql.clone()),
            Stmt::Insert(t, query) => Some(format!("INSERT INTO {} {query}", sql_name(t))), // (its rows checked: `constraints.rs`)
            _ => None,
        }
    }

    /// The same statement writing `table` (a name resolved to one inside its lake).
    fn on(self, table: String) -> Stmt {
        match self {
            Stmt::Insert(_, q) => Stmt::Insert(table, q),
            Stmt::InsertInto(_, c, q) => Stmt::InsertInto(table, c, q),
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

/// Puts a materialized view's expectations (JSON) in its options, through OR REPLACE's and IF NOT
/// EXISTS's wrappers; false for a statement that isn't a materialized view's.
fn expect_in(d: &mut crate::ddl::Ddl, expect: &str) -> bool {
    use crate::ddl::Ddl;
    match d {
        Ddl::CreateMaterialized { options, .. } => {
            options.insert("expect".into(), expect.into());
            true
        }
        Ddl::Unless { then, .. } | Ddl::Replacing { then, .. } => expect_in(then, expect),
        _ => false,
    }
}

/// A view's query with its column list (`CREATE VIEW v (a, b) AS …`, TPC-H q15's form) given to
/// its outputs: the select list's items renamed, so a materialized view keeps the shape `views.rs`
/// reads; a query whose outputs can't be renamed one by one (`*`, `VALUES`) is put in a subquery
/// named so (DataFusion then says when the counts differ).
fn view_sql(v: &ast::CreateView) -> String {
    let names: Vec<ast::Ident> = v.columns.iter().map(|c| c.name.clone()).collect();
    if names.is_empty() {
        return v.query.to_string();
    }
    let mut q = (*v.query).clone();
    if renamed(&mut q.body, &names) {
        return q.to_string();
    }
    let names = names.iter().map(|n| n.to_string()).collect::<Vec<_>>().join(", ");
    format!("SELECT * FROM ({}) AS pondra_view ({names})", v.query)
}

/// A query's outputs renamed in place (a set operation's are its first branch's), if each is one.
fn renamed(body: &mut ast::SetExpr, names: &[ast::Ident]) -> bool {
    use ast::SelectItem::{ExprWithAlias, UnnamedExpr};
    match body {
        ast::SetExpr::Select(s) if s.projection.len() == names.len() && s.projection.iter().all(|i| matches!(i, UnnamedExpr(_) | ExprWithAlias { .. })) => {
            let items = std::mem::take(&mut s.projection).into_iter().zip(names).map(|(i, n)| match i {
                UnnamedExpr(expr) | ExprWithAlias { expr, .. } => ExprWithAlias { expr, alias: n.clone() },
                other => other,
            });
            s.projection = items.collect();
            true
        }
        ast::SetExpr::SetOperation { left, .. } => renamed(left, names),
        ast::SetExpr::Query(q) => renamed(&mut q.body, names),
        _ => false,
    }
}

/// A partition clause as `partition_by` writes it: `ts`, `day(ts)`. Spark's and Iceberg's
/// `days(ts)`, BigQuery's `DATE(ts)` and `TIMESTAMP_TRUNC(ts, DAY)`, and `date_trunc('day', ts)`
/// are the same partitions.
pub fn partition_text(e: &ast::Expr) -> String {
    let column = |e: &ast::Expr| match e {
        ast::Expr::Identifier(i) => ident(i),
        e => e.to_string(),
    };
    let ast::Expr::Function(f) = e else { return column(e) };
    let ast::FunctionArguments::List(l) = &f.args else { return e.to_string() };
    let args: Vec<&ast::Expr> = l.args.iter().filter_map(|a| match a {
        ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => Some(e),
        _ => None,
    }).collect();
    let name = object(&f.name);
    let (unit, col) = match (name.as_str(), &args[..]) {
        ("date_trunc" | "timestamp_trunc" | "datetime_trunc", [a @ ast::Expr::Value(_), b]) => (option_text(a), *b),
        ("date_trunc" | "timestamp_trunc" | "datetime_trunc", [a, b]) => (b.to_string(), *a),
        (_, [a]) => (name.clone(), *a),
        _ => return e.to_string(),
    };
    let unit = unit.to_lowercase();
    format!("{}({})", match unit.trim_end_matches('s') { "date" => "day", u => u }, column(col))
}

/// The columns a `CLUSTER BY` names: `a, b` or `(a, b)`.
pub fn cluster_names(e: &ast::Expr) -> Vec<String> {
    match e {
        ast::Expr::Tuple(t) => t.iter().flat_map(cluster_names).collect(),
        ast::Expr::Nested(e) => cluster_names(e),
        ast::Expr::Identifier(i) => vec![ident(i)],
        e => vec![e.to_string()],
    }
}

/// A name in SQL, each part quoted as it is: `"t"`, `"schema"."t"`, `"lake"."schema"."t"`.
pub fn sql_name(name: &str) -> String {
    name.split('.').map(|p| format!("\"{p}\"")).collect::<Vec<_>>().join(".")
}

/// `d`, or with IF NOT EXISTS nothing when a `kind` of that name is there (`Ddl::Unless`).
pub fn unless(if_not_exists: bool, name: &str, kind: &str, d: crate::ddl::Ddl) -> crate::ddl::Ddl {
    match if_not_exists {
        true => crate::ddl::Ddl::Unless { name: name.to_string(), kind: kind.to_string(), then: Box::new(d) },
        false => d,
    }
}

/// A write statement, or None for a query.
pub fn parse(sql: &str) -> Option<Stmt> {
    use datafusion::sql::sqlparser::{dialect::GenericDialect, parser::Parser};
    use crate::ddl::{Change, Ddl};
    let where_ = |e: &Option<ast::Expr>| e.as_ref().map(|e| e.to_string());
    let alter = |a: &ast::AlterTable, c: &ast::Ident, change: Change| Stmt::Ddl(vec![Ddl::AlterColumn { table: object(&a.name), column: ident(c), change }]);
    let relation = |r: &ast::TableFactor| match r {
        ast::TableFactor::Table { name, .. } => Some(object(name)),
        _ => None,
    };
    let head = head(sql);
    // OR REPLACE where replacing one would lose what it holds: refused, saying why.
    static NO_REPLACE: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| regex::Regex::new(r"(?is)^\s*CREATE\s+OR\s+REPLACE\s+(SCHEMA|DATABASE|USER|ROLE|SHARE|RECIPIENT)\b").expect("a regex"));
    if let Some(c) = (head == "create").then(|| NO_REPLACE.captures(first_word(sql))).flatten() {
        let kind = c[1].to_uppercase();
        let lost = match kind.as_str() {
            "USER" | "ROLE" => "the rights given to it",
            "SHARE" => "its tables and grants",
            "RECIPIENT" => "its token and the shares granted to it",
            _ => "everything in it",
        };
        return Some(Stmt::Invalid(format!("CREATE OR REPLACE {kind}: replacing one would drop {lost}; CREATE {kind} IF NOT EXISTS leaves one that is there as it is")));
    }
    // The kinds below each begin with one of these words, so a query or a row's change goes past
    // them without being read again by each (every statement is read here more than once).
    let kinds = matches!(head.as_str(), "create" | "alter" | "drop" | "grant" | "revoke" | "comment" | "copy" | "attach");
    if let Some(s) = kinds.then(|| crate::shares::statement(sql)).flatten() {
        return Some(s); // (CREATE SHARE and RECIPIENT, GRANT SELECT ON SHARE: `shares.rs`, before users' GRANT)
    }
    if let Some(s) = kinds.then(|| crate::users::statement(sql)).flatten() {
        return Some(s); // (CREATE USER and ROLE, GRANT, REVOKE, CREATE TOKEN: `users.rs`)
    }
    if let Some(s) = kinds.then(|| crate::sso::statement(sql)).flatten() {
        return Some(s); // (CREATE and DROP IDENTITY PROVIDER: `sso.rs`)
    }
    if let Some(s) = kinds.then(|| crate::ext::statement(sql)).flatten() {
        return Some(s); // (CREATE SECRET: values of any kind; DROP SECRET)
    }
    if let Some(s) = kinds.then(|| crate::routines::statement(sql)).flatten() {
        return Some(s); // (CREATE FUNCTION and PROCEDURE as Postgres writes them, CREATE TASK, DROP TASK, DROP MACRO)
    }
    if let Some(s) = kinds.then(|| crate::objects::statement(sql)).flatten() {
        return Some(s); // (COMMENT ON, CREATE OR ALTER TABLE | VIEW: the registry's)
    }
    if let Some(s) = kinds.then(|| crate::seq::statement(sql)).flatten() {
        return Some(s); // (CREATE, ALTER, DROP SEQUENCE)
    }
    if let Some(s) = kinds.then(|| crate::index::statement(sql)).flatten() {
        return Some(s); // (CREATE, ALTER, DROP INDEX)
    }
    if let Some(s) = kinds.then(|| crate::types::statement(sql)).flatten() {
        return Some(s); // (CREATE TYPE … AS ENUM, ALTER and DROP TYPE)
    }
    if let Some(sql) = kinds.then(|| crate::seq::in_order(sql)).flatten() {
        return parse(&sql); // (an identity's options as the parser takes them)
    }
    // DataFusion's CREATE EXTERNAL TABLE (a stored view of files: `ext::external`), in its own words.
    static EXTERNAL: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
        regex::Regex::new(r"(?is)^\s*CREATE\s+(\w+\s+){0,3}EXTERNAL\s+TABLE\b").expect("a regex")
    });
    if let Some(m) = (head == "create").then(|| EXTERNAL.find(first_word(sql))).flatten() {
        use datafusion::sql::parser::{DFParserBuilder, Statement as DF};
        if m.as_str().split_whitespace().any(|w| w.eq_ignore_ascii_case("temp") || w.eq_ignore_ascii_case("temporary")) {
            return Some(Stmt::Invalid("CREATE TEMPORARY EXTERNAL TABLE: make a temporary view of the files instead (CREATE TEMP VIEW t AS SELECT * FROM read_parquet('…'))".into()));
        }
        return Some(match DFParserBuilder::new(sql).build().and_then(|mut p| p.parse_statements()) {
            Ok(mut all) if all.len() == 1 => match all.pop_front() {
                Some(DF::CreateExternalTable(c)) => crate::ext::external(&c).map(|d| Stmt::Ddl(vec![d])).unwrap_or_else(|e| Stmt::Invalid(format!("{e:#}"))),
                _ => Stmt::Invalid("CREATE EXTERNAL TABLE t … STORED AS … LOCATION '…'".into()),
            },
            Ok(all) => Stmt::Invalid(format!("CREATE EXTERNAL TABLE and {} more: send one statement at a time here (or as a script, over HTTP or in the shell)", all.len().saturating_sub(1))),
            Err(e) => Stmt::Invalid(format!("{e}")),
        });
    }
    // CREATE MATERIALIZED VIEW v AS SELECT … GROUP BY … EMIT FINAL: each group once, when it's over.
    if let Some(rest) = crate::once::emit_final(sql) {
        let wrong = || Stmt::Invalid("EMIT FINAL ends a materialized view's query: CREATE MATERIALIZED VIEW v AS SELECT … GROUP BY … EMIT FINAL".into());
        return Some(match parse(&rest) {
            Some(Stmt::Ddl(mut d)) if d.len() == 1 => match crate::once::mark(&mut d[0]) {
                true => Stmt::Ddl(d),
                false => wrong(),
            },
            Some(Stmt::Invalid(e)) => Stmt::Invalid(e),
            _ => wrong(),
        });
    }
    // A view's key facts in its list, and ALTER VIEW … ADD | DROP CONSTRAINT (ADR-065 §2), which the parser doesn't take.
    if let Some(s) = crate::facts::parse(sql) {
        return Some(s);
    }
    // CREATE MATERIALIZED VIEW v (CONSTRAINT c CHECK (…) ON VIOLATION DROP ROW, …): its
    // expectations (ADR-036 §2), which the parser doesn't take.
    match crate::views::constraints(sql) {
        Ok(Some((rest, expect))) => {
            let expect = serde_json::to_string(&expect).unwrap_or_default();
            return Some(match parse(&rest) {
                Some(Stmt::Ddl(mut d)) if d.len() == 1 => match expect_in(&mut d[0], &expect) {
                    true => Stmt::Ddl(d),
                    false => Stmt::Invalid("CREATE MATERIALIZED VIEW v (CONSTRAINT c CHECK (…)) AS SELECT …".into()),
                },
                other => other.unwrap_or_else(|| Stmt::Invalid("CREATE MATERIALIZED VIEW v (CONSTRAINT c CHECK (…)) AS SELECT …".into())),
            });
        }
        Ok(None) => {}
        Err(e) => return Some(Stmt::Invalid(format!("{e:#}"))),
    }
    let name = |s: &str| s.split('.').map(|p| if p.starts_with('"') { p.trim_matches('"').to_string() } else { p.to_lowercase() }).collect::<Vec<_>>().join(".");
    // `ALTER VIEW v RENAME TO w` (dbt's): the parser takes only ALTER VIEW … AS (`rules::alter::view_rename`).
    if let Some((from, to)) = (head == "alter").then(|| crate::rules::alter::view_rename(first_word(sql))).flatten() {
        return Some(Stmt::Ddl(vec![Ddl::RenameTable { name: name(&from), to: name(&to) }]));
    }
    // `ALTER MATERIALIZED VIEW v DETACH`: its rows stop following, and stay as a table of that name.
    static DETACH: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| regex::Regex::new(r#"(?is)^\s*ALTER\s+MATERIALIZED\s+VIEW\s+([\w."-]+)\s+DETACH\s*;?\s*$"#).expect("a regex"));
    if let Some(c) = (head == "alter").then(|| DETACH.captures(first_word(sql))).flatten() {
        return Some(Stmt::Ddl(vec![Ddl::DetachView { name: name(&c[1]) }]));
    }
    // `CREATE TABLE c [SHALLOW] CLONE t` (Snowflake's, Databricks'): a table of t's files as they are now, copying none (ADR-043).
    static CLONE: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| regex::Regex::new(r#"(?is)^\s*CREATE\s+TABLE\s+(IF\s+NOT\s+EXISTS\s+)?([\w."-]+)\s+(SHALLOW\s+)?CLONE\s+([\w."-]+)\s*;?\s*$"#).expect("a regex"));
    if let Some(c) = (head == "create").then(|| CLONE.captures(first_word(sql))).flatten() {
        let d = Ddl::Clone { name: name(&c[2]), from: name(&c[4]) };
        return Some(Stmt::Ddl(vec![if c.get(1).is_some() { unless(true, &name(&c[2]), "relation", d) } else { d }]));
    }
    // `CREATE DATABASE b CLONE a [WITH (schemas = (x, y))] [WITH NO DATA]`: a branch of a, copying none
    // of its files (ADR-047). (The parser takes `CLONE a` alone.)
    static DB_CLONE: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| regex::Regex::new(r#"(?is)^\s*CREATE\s+DATABASE\s+(IF\s+NOT\s+EXISTS\s+)?([\w"-]+)\s+(?:LOCATION\s+'((?:[^']|'')*)'\s+)?CLONE\s+([\w"-]+)((?:\s+WITH\s*\(\s*schemas\s*=\s*\([^)]*\)\s*\)|\s+WITH\s+NO\s+DATA)*)\s*;?\s*$"#).expect("a regex"));
    if let Some(c) = (head == "create").then(|| DB_CLONE.captures(first_word(sql))).flatten() {
        static SCHEMAS: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| regex::Regex::new(r"(?is)schemas\s*=\s*\(([^)]*)\)").expect("a regex"));
        let tail = c.get(5).map_or("", |m| m.as_str());
        let schemas = SCHEMAS.captures(tail).map(|s| s[1].split(',').map(|n| name(n.trim())).filter(|n| !n.is_empty()).collect()).unwrap_or_default();
        let clone = crate::branch::CloneOf { from: name(&c[4]), schemas, data: !regex::Regex::new(r"(?i)WITH\s+NO\s+DATA").expect("a regex").is_match(tail), owner: None };
        return Some(Stmt::Ddl(vec![Ddl::CreateDatabase { name: name(&c[2]), if_not_exists: c.get(1).is_some(), dir: c.get(3).map(|d| d.as_str().replace("''", "'")), clone: Some(clone) }]));
    }
    // `ALTER DATABASE b REFRESH [t, …]`: a branch's tables (all it took from its base, or those
    // named: a view stands for the tables it reads), and what follows them, as its base has them now (ADR-047).
    static DB_REFRESH: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| regex::Regex::new(r#"(?is)^\s*ALTER\s+DATABASE\s+([\w"-]+)\s+REFRESH((?:\s+[\w".$-]+(?:\s*,\s*[\w".$-]+)*)?)\s*;?\s*$"#).expect("a regex"));
    if let Some(c) = (head == "alter").then(|| DB_REFRESH.captures(first_word(sql))).flatten() {
        let tables = c[2].split(',').map(|t| name(t.trim())).filter(|t| !t.is_empty()).collect();
        return Some(Stmt::Ddl(vec![Ddl::Refresh { database: Some(name(&c[1])), tables }]));
    }
    // `ALTER DATABASE b SET LOCATION 'dir'`: a lake held in memory moves there, and goes on there (`memory.rs`).
    static DB_LOCATION: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| regex::Regex::new(r#"(?is)^\s*ALTER\s+DATABASE\s+([\w"-]+)\s+SET\s+LOCATION\s+'((?:[^']|'')*)'\s*;?\s*$"#).expect("a regex"));
    if let Some(c) = (head == "alter").then(|| DB_LOCATION.captures(first_word(sql))).flatten() {
        return Some(Stmt::Ddl(vec![Ddl::SetLocation { database: name(&c[1]), to: c[2].replace("''", "'") }]));
    }
    // `ALTER DATABASE b SET (protected = true | false)`: a database's protection (ADR-058: `protect.rs`).
    static DB_PROTECT: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| regex::Regex::new(r#"(?is)^\s*ALTER\s+DATABASE\s+([\w"-]+)\s+SET\s*\(\s*protected\s*=\s*(true|false)\s*\)\s*;?\s*$"#).expect("a regex"));
    if let Some(c) = (head == "alter").then(|| DB_PROTECT.captures(first_word(sql))).flatten() {
        return Some(Stmt::Ddl(vec![Ddl::Protect { database: name(&c[1]), on: c[2].eq_ignore_ascii_case("true"), by: String::new() }]));
    }
    // `BACKUP DATABASE [b] TO 'url' [WITH (retention_days = n)]` and `RESTORE DATABASE b FROM 'url'
    // [LOCATION 'dir'] [AT (TIMESTAMP => '…')]` (ADR-070: a backup is a lake, `backup.rs`).
    static BACKUP: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| regex::Regex::new(r#"(?is)^\s*BACKUP\s+DATABASE\s+(?:([\w"-]+)\s+)?TO\s+'((?:[^']|'')*)'(?:\s+WITH\s*\(\s*retention_days\s*=\s*(\d+)\s*\))?\s*;?\s*$"#).expect("a regex"));
    static RESTORE: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| regex::Regex::new(r#"(?is)^\s*RESTORE\s+DATABASE\s+([\w"-]+)\s+FROM\s+'((?:[^']|'')*)'(?:\s+LOCATION\s+'((?:[^']|'')*)')?(?:\s+AT\s*\(\s*TIMESTAMP\s*=>\s*'((?:[^']|'')*)'\s*\))?\s*;?\s*$"#).expect("a regex"));
    static BACKUP_ANY: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| regex::Regex::new(r"(?is)^\s*(BACKUP|RESTORE)\s+DATABASE\b").expect("a regex"));
    let text = |m: Option<regex::Match>| m.map(|m| m.as_str().replace("''", "'"));
    let backups = matches!(head.as_str(), "backup" | "restore"); // (a query compiles none of these)
    if let Some(c) = backups.then(|| BACKUP.captures(first_word(sql))).flatten() {
        let retention_days = c.get(3).map(|d| d.as_str().parse::<u64>()).transpose();
        let Ok(retention_days) = retention_days else { return Some(Stmt::Invalid("retention_days is a number of days".into())) };
        return Some(Stmt::Ddl(vec![Ddl::BackupDatabase { database: c.get(1).map(|n| name(n.as_str())), to: c[2].replace("''", "'"), retention_days }]));
    }
    if let Some(c) = backups.then(|| RESTORE.captures(first_word(sql))).flatten() {
        let at = match text(c.get(4)).map(|t| crate::backup::instant(&t)).transpose() {
            Ok(at) => at,
            Err(e) => return Some(Stmt::Invalid(format!("{e:#}"))),
        };
        return Some(Stmt::Ddl(vec![Ddl::RestoreDatabase { name: name(&c[1]), from: c[2].replace("''", "'"), dir: text(c.get(3)), at }]));
    }
    if backups && BACKUP_ANY.is_match(first_word(sql)) {
        return Some(Stmt::Invalid("a backup is written as BACKUP DATABASE [name] TO 'place' [WITH (retention_days = 7)], and restored as RESTORE DATABASE name FROM 'place' [LOCATION 'place'] [AT (TIMESTAMP => '2026-10-12 03:00:00')]".into()));
    }
    // DuckDB's `EXPORT DATABASE 'dir'` and `IMPORT DATABASE 'dir'` (a folder of a database's files):
    // not Pondra's yet, so refused by name with the ways it has, not with the parser's words.
    static PORTABLE: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| regex::Regex::new(r"(?is)^\s*(EXPORT|IMPORT)\s+DATABASE\b").expect("a regex"));
    if matches!(head.as_str(), "export" | "import") && PORTABLE.is_match(first_word(sql)) {
        return Some(Stmt::Invalid("EXPORT DATABASE and IMPORT DATABASE are not supported yet: BACKUP DATABASE TO 'place' copies a database (RESTORE DATABASE name FROM 'place' brings it back), and COPY t TO 'dir/t.parquet' (FORMAT parquet) writes a table's rows".into()));
    }
    // `UNDROP TABLE t` (Snowflake's, Databricks'): the table dropped last under that name, back (ADR-043).
    static UNDROP: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| regex::Regex::new(r#"(?is)^\s*UNDROP\s+TABLE\s+([\w."-]+)\s*;?\s*$"#).expect("a regex"));
    if let Some(c) = (head == "undrop").then(|| UNDROP.captures(first_word(sql))).flatten() {
        return Some(Stmt::Ddl(vec![Ddl::Undrop { name: name(&c[1]) }]));
    }
    // DuckDB's CREATE MACRO … IF NOT EXISTS (the parser takes only OR REPLACE): read without it, kept unless one is there.
    static MACRO_QUIET: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| regex::Regex::new(r"(?is)^(\s*CREATE\s+(OR\s+REPLACE\s+)?(TEMP\s+|TEMPORARY\s+)?MACRO\s+)IF\s+NOT\s+EXISTS\s+").expect("a regex"));
    if let Some(c) = (head == "create").then(|| MACRO_QUIET.captures(first_word(sql))).flatten() {
        if c.get(2).is_some() {
            return Some(Stmt::Invalid("CREATE OR REPLACE MACRO … IF NOT EXISTS: one or the other".into()));
        }
        return Some(match parse(&MACRO_QUIET.replace(first_word(sql), "$1")) {
            Some(Stmt::Ddl(d)) => Stmt::Ddl(d.into_iter().map(|d| match &d {
                Ddl::CreateRoutine { name, .. } => {
                    let name = name.clone();
                    unless(true, &name, "routine", d)
                }
                _ => d,
            }).collect()),
            other => other?,
        });
    }
    // A table's layout as clauses (`PARTITION BY day(ts)`, `TTL ts + INTERVAL '1 hour'`, …): read as its options.
    let laid = if matches!(head.as_str(), "create" | "alter") { crate::layout::clauses(sql) } else { Ok(std::borrow::Cow::Borrowed(sql)) };
    let sql = &match laid {
        Ok(sql) => sql,
        Err(e) => return Some(Stmt::Invalid(format!("{e:#}"))),
    };
    let parsed = Parser::parse_sql(&GenericDialect {}, sql).or_else(|e| crate::settings::dialect().map_or(Err(e), |d| Parser::parse_sql(d.as_ref(), sql))); // (DuckDB's STRUCT(a INT), …: the session's dialect)
    let parsed = match parsed {
        Ok(mut s) => s.pop()?,
        Err(_) => return None,
    };
    // Postgres's UPDATE … FROM and DELETE … USING read one more relation: a table or a subquery.
    let one = |f: &[ast::TableWithJoins], what: &str| match f {
        [t] if t.joins.is_empty() => Ok(t.relation.clone()),
        _ => Err(Stmt::Invalid(format!("{what} takes one table or subquery: join several in a subquery, (SELECT … FROM a JOIN b ON …) AS s"))),
    };
    let text = parsed.to_string();
    Some(match parsed {
        // A lake's tables keep their files in the lake: LOCATION and Hive's STORED AS were read and
        // dropped, and the table made in the lake as if they weren't there.
        Statement::CreateTable(c) if c.location.is_some() || c.file_format.is_some() || c.hive_formats.as_ref().is_some_and(|h| h.location.is_some() || h.storage.is_some() || h.row_format.is_some() || h.serde_properties.is_some()) => {
            Stmt::Invalid("CREATE TABLE … LOCATION / STORED AS: a lake's tables keep their files in the lake; files elsewhere are CREATE EXTERNAL TABLE t … STORED AS PARQUET LOCATION '…' (a view of them), and a Delta or Iceberg table elsewhere is ATTACH 'url' AS name (TYPE delta | iceberg)".into())
        }
        Statement::CreateTable(c) => Stmt::Create(Box::new(c)),
        // What a write may say that Pondra doesn't do is refused by name, never read and dropped:
        // INSERT OVERWRITE appended, and RETURNING answered no rows.
        Statement::Insert(ast::Insert { overwrite: true, .. }) => Stmt::Invalid("INSERT OVERWRITE: write BEGIN; DELETE FROM t; INSERT INTO t …; COMMIT (one commit), or CREATE OR REPLACE TABLE t AS …".into()),
        // (the doors that answer rows take RETURNING before they get here: `returning.rs`)
        Statement::Insert(ast::Insert { returning: Some(_), .. }) | Statement::Update(ast::Update { returning: Some(_), .. }) | Statement::Delete(ast::Delete { returning: Some(_), .. }) => {
            Stmt::Invalid(format!("{} … RETURNING isn't supported here: send it to a node (Postgres, POST /sql, Flight SQL, MCP), or read the rows with a SELECT after it", first_word(&text).split_whitespace().next().unwrap_or("INSERT")))
        }
        // DuckDB's and SQLite's INSERT OR IGNORE and INSERT OR REPLACE, and MySQL's INSERT IGNORE:
        // ON CONFLICT DO NOTHING, and DO UPDATE of every column the rows give. (Read and then
        // dropped, OR IGNORE replaced a keyed table's row.)
        Statement::Insert(ast::Insert { table: ast::TableObject::TableName(t), source: Some(q), columns, or, ignore, on, .. }) if or.is_some() || ignore => {
            let update = match (or.unwrap_or(ast::SqliteOnConflict::Ignore), on) {
                (_, Some(_)) => return Some(Stmt::Invalid("INSERT OR … ON CONFLICT: one or the other".into())),
                (ast::SqliteOnConflict::Ignore, None) => None,
                (ast::SqliteOnConflict::Replace, None) => Some((vec![], None)), // (an empty SET: every column the rows give, `Upsert::merge`)
                (other, None) => return Some(Stmt::Invalid(format!("INSERT {other}: Pondra takes INSERT OR IGNORE and INSERT OR REPLACE, or ON CONFLICT (…) DO …"))),
            };
            let upsert = crate::change::Upsert { columns: columns.iter().map(object).collect(), query: q.to_string(), on: vec![], update };
            Stmt::Merge(Box::new(crate::change::upsert_of(object(&t), upsert, text)))
        }
        Statement::Insert(ast::Insert { table: ast::TableObject::TableName(t), source: Some(q), columns, on: Some(on), .. }) => match on {
            ast::OnInsert::OnConflict(oc) => {
                let on = match oc.conflict_target {
                    None => vec![],
                    Some(ast::ConflictTarget::Columns(c)) => c.iter().map(ident).collect(),
                    Some(_) => return Some(Stmt::Invalid("ON CONFLICT ON CONSTRAINT: name the columns instead, ON CONFLICT (id) …".into())),
                };
                let update = match oc.action {
                    ast::OnConflictAction::DoNothing => None,
                    ast::OnConflictAction::DoUpdate(u) => Some((u.assignments.iter().map(|a| match &a.target {
                        ast::AssignmentTarget::ColumnName(c) => Some((c.0.last()?.as_ident().map(ident)?, a.value.to_string())),
                        _ => None,
                    }).collect::<Option<Vec<_>>>()?, u.selection.map(|w| w.to_string()))),
                };
                let upsert = crate::change::Upsert { columns: columns.iter().map(object).collect(), query: q.to_string(), on, update };
                Stmt::Merge(Box::new(crate::change::upsert_of(object(&t), upsert, text)))
            }
            _ => Stmt::Invalid("ON DUPLICATE KEY UPDATE is MySQL's: write INSERT … ON CONFLICT (…) DO UPDATE SET …, or MERGE".into()),
        },
        Statement::Insert(ast::Insert { table: ast::TableObject::TableName(t), source: Some(mut q), columns, .. }) => match (distinct_names(&mut q), columns.is_empty()).1 {
            true if crate::rules::rows::says_default(&q) => Stmt::InsertInto(object(&t), vec![], q.to_string()),
            true => Stmt::Insert(object(&t), q.to_string()),
            false => Stmt::InsertInto(object(&t), columns.iter().map(object).collect(), q.to_string()),
        },
        Statement::Update(u) if u.from.is_some() => {
            let (Some(ast::UpdateTableFromKind::BeforeSet(f) | ast::UpdateTableFromKind::AfterSet(f)), true) = (&u.from, u.table.joins.is_empty()) else { return None };
            let set = u.assignments.iter().map(|a| a.to_string()).collect::<Vec<_>>().join(", ");
            match one(f, "UPDATE … FROM") {
                Ok(src) => Stmt::Merge(Box::new(crate::change::merge_from(&u.table.relation, &src, u.selection.as_ref(), &format!("UPDATE SET {set}"), false, text)?)),
                Err(e) => e,
            }
        }
        Statement::Update(u) => {
            let set = u.assignments.iter().filter_map(|a| match &a.target {
                ast::AssignmentTarget::ColumnName(c) => Some((c.0.last()?.as_ident().map(ident)?, a.value.to_string())),
                _ => None,
            });
            Stmt::Update(relation(&u.table.relation)?, set.collect(), where_(&u.selection))
        }
        Statement::AlterTable(a) => match &a.operations[..] {
            [ast::AlterTableOperation::AddColumn { column_def: c, .. }] if crate::rules::alter::fills_existing(c) => Stmt::Invalid(crate::rules::alter::owes_rows(&a.name.to_string(), &c.name.to_string())),
            [ast::AlterTableOperation::AddColumn { if_not_exists, column_def: c, .. }] => match crate::rules::alter::beside_type(c) {
                Some(what) => Stmt::Invalid(crate::rules::alter::not_alone(&a.name.to_string(), &c.name.to_string(), &what)), // (a CHECK or a key: not dropped unsaid)
                None => Stmt::AddColumn(object(&a.name), ident(&c.name), added_type(&c.data_type), *if_not_exists),
            },
            // (renames, drops and types: the catalog's names change, the files never do: ADR-022)
            [ast::AlterTableOperation::RenameColumn { old_column_name: from, new_column_name: to }] => alter(&a, from, Change::Rename(ident(to))),
            [ast::AlterTableOperation::DropColumn { column_names, if_exists, .. }] => Stmt::Ddl(column_names.iter().map(|c| Ddl::AlterColumn { table: object(&a.name), column: ident(c), change: Change::Drop { if_exists: *if_exists } }).collect()),
            [ast::AlterTableOperation::AlterColumn { column_name: c, op: ast::AlterColumnOperation::SetDataType { data_type, .. } }] => alter(&a, c, Change::Type(data_type.to_string())),
            // (defaults and NOT NULL: `defaults.rs`; `SET DEFAULT NULL` is no default, as `DEFAULT NULL` in ADD COLUMN)
            [ast::AlterTableOperation::AlterColumn { column_name: c, op: ast::AlterColumnOperation::SetDefault { value } }] if crate::defaults::is_null(value) => alter(&a, c, Change::Default(None)),
            [ast::AlterTableOperation::AlterColumn { column_name: c, op: ast::AlterColumnOperation::SetDefault { value } }] => alter(&a, c, Change::Default(Some(value.to_string()))),
            [ast::AlterTableOperation::AlterColumn { column_name: c, op: ast::AlterColumnOperation::DropDefault }] => alter(&a, c, Change::Default(None)),
            [ast::AlterTableOperation::AlterColumn { column_name: c, op: ast::AlterColumnOperation::SetNotNull }] => alter(&a, c, Change::NotNull(true)),
            [ast::AlterTableOperation::AlterColumn { column_name: c, op: ast::AlterColumnOperation::DropNotNull }] => alter(&a, c, Change::NotNull(false)),
            [ast::AlterTableOperation::RenameTable { table_name }] => {
                let to = table_name.to_string();
                let to = to.trim_start_matches("TO ").trim_start_matches("AS ");
                let to = to.split('.').map(|p| if p.starts_with('"') { p.trim_matches('"').to_string() } else { p.to_lowercase() }).collect::<Vec<_>>().join(".");
                Stmt::Ddl(vec![Ddl::RenameTable { name: object(&a.name), to }])
            }
            [ast::AlterTableOperation::SwapWith { table_name }] => Stmt::Ddl(vec![Ddl::Swap { name: object(&a.name), with: object(table_name) }]),
            // Snowflake's and Databricks' `ALTER TABLE t CLUSTER BY (a, b)`: `SET (cluster_by = 'a, b')`.
            [ast::AlterTableOperation::ClusterBy { exprs }] => Stmt::SetOptions(object(&a.name), vec![("cluster_by".into(), exprs.iter().flat_map(cluster_names).collect::<Vec<_>>().join(", "))]),
            [op @ (ast::AlterTableOperation::AddConstraint { .. } | ast::AlterTableOperation::DropConstraint { .. })] => match crate::constraints::change(op, &object(&a.name)) {
                Some(Ok(change)) => Stmt::Ddl(vec![Ddl::Constraint { table: object(&a.name), change }]),
                Some(Err(e)) => Stmt::Invalid(format!("{e:#}")),
                None => return None,
            },
            [ast::AlterTableOperation::SetOptionsParens { options } | ast::AlterTableOperation::SetTblProperties { table_properties: options }] => Stmt::SetOptions(object(&a.name), options.iter().map(|o| match o {
                ast::SqlOption::KeyValue { key, value } => Some((key.value.to_lowercase(), option_text(value))),
                _ => None,
            }).collect::<Option<_>>()?),
            _ => return None,
        },
        Statement::Delete(d) => {
            let (ast::FromTable::WithFromKeyword(t) | ast::FromTable::WithoutKeyword(t)) = &d.from;
            match &d.using {
                Some(using) => match one(using, "DELETE … USING") {
                    Ok(src) => Stmt::Merge(Box::new(crate::change::merge_from(&t.first()?.relation, &src, d.selection.as_ref(), "DELETE", true, text)?)),
                    Err(e) => e,
                },
                None => Stmt::Delete(relation(&t.first()?.relation)?, where_(&d.selection)),
            }
        }
        Statement::Truncate(t) => match &t.table_names[..] {
            [one] => Stmt::Delete(object(&one.name), None), // (every row, as a DELETE: views and the change feed follow)
            _ => Stmt::Invalid("TRUNCATE one table at a time".into()),
        },
        Statement::CreateSchema { schema_name: ast::SchemaName::Simple(n) | ast::SchemaName::NamedAuthorization(n, _), if_not_exists, .. } => {
            Stmt::Ddl(vec![Ddl::CreateSchema { name: object(&n), if_not_exists }])
        }
        Statement::Drop { object_type, if_exists, names, cascade, purge, .. } => Stmt::Ddl(names.iter().map(object).map(|name| match object_type {
            ast::ObjectType::Schema => Some(Ddl::DropSchema { name, if_exists, cascade }),
            ast::ObjectType::Table => Some(Ddl::DropTable { name, if_exists, purge, made_by: None }),
            ast::ObjectType::View | ast::ObjectType::MaterializedView => Some(Ddl::DropView { name, if_exists }),
            ast::ObjectType::Database => Some(Ddl::DropDatabase { name: name.to_lowercase(), if_exists }),
            _ => None,
        }).collect::<Option<Vec<_>>>()?),
        Statement::CreateView(v) if v.materialized => {
            let options = match &v.options {
                ast::CreateTableOptions::With(o) | ast::CreateTableOptions::Options(o) => o.iter().filter_map(|o| match o {
                    ast::SqlOption::KeyValue { key, value } => Some((key.value.to_lowercase(), option_text(value))),
                    _ => None,
                }).collect(),
                _ => Default::default(),
            };
            let made = Ddl::CreateMaterialized { name: object(&v.name), sql: view_sql(&v), options };
            Stmt::Ddl(vec![match (v.or_replace, v.if_not_exists) {
                (true, true) => return Some(Stmt::Invalid("CREATE OR REPLACE … IF NOT EXISTS: one or the other".into())),
                (true, false) => Ddl::Replacing { name: object(&v.name), then: Box::new(made) },
                (false, true) => Ddl::Unless { name: object(&v.name), kind: "relation".into(), then: Box::new(made) },
                (false, false) => made,
            }])
        }
        Statement::CreateView(v) if v.or_replace && v.if_not_exists => Stmt::Invalid("CREATE OR REPLACE VIEW … IF NOT EXISTS: one or the other".into()),
        Statement::CreateView(v) if v.temporary => Stmt::TempView(object(&v.name), view_sql(&v), v.or_replace, v.if_not_exists), // (the session's: `temp.rs`)
        Statement::CreateView(v) => Stmt::Ddl(vec![unless(v.if_not_exists, &object(&v.name), "relation", Ddl::CreateView { name: object(&v.name), sql: view_sql(&v), replace: v.or_replace, facts: None })]),
        Statement::AttachDatabase { schema_name, database_file_name: ast::Expr::Value(v), .. } => match &v.value {
            ast::Value::SingleQuotedString(dir) | ast::Value::DoubleQuotedString(dir) => Stmt::Ddl(vec![Ddl::Attach { name: ident(&schema_name), dir: dir.clone(), read_only: false, endpoint: None }]),
            _ => return None,
        },
        Statement::CreateDatabase { db_name, if_not_exists, location, clone, .. } => {
            let clone = clone.map(|c| crate::branch::CloneOf { from: object(&c).to_lowercase(), schemas: vec![], data: true, owner: None });
            Stmt::Ddl(vec![Ddl::CreateDatabase { name: object(&db_name).to_lowercase(), if_not_exists, dir: location, clone }])
        }
        Statement::DetachDuckDBDatabase { if_exists, database_alias, .. } => Stmt::Ddl(vec![Ddl::Detach { name: ident(&database_alias), if_exists }]),
        Statement::Merge(m) if crate::change::merge_refused(&m).is_some() => Stmt::Invalid(crate::change::merge_refused(&m).unwrap_or_default()),
        Statement::Merge(m) => Stmt::Merge(Box::new(crate::change::merge_of(&m)?)),
        Statement::Query(mut q) => {
            // `SELECT … INTO t FROM …`: CREATE TABLE t AS SELECT … FROM …
            let ast::SetExpr::Select(s) = q.body.as_mut() else { return None };
            let into = s.into.take()?;
            return parse(&format!("CREATE {}TABLE {} AS {q}", if into.temporary { "TEMP " } else { "" }, into.name));
        }
        Statement::CreateMacro { or_replace, name, args, definition, .. } => Stmt::Ddl(vec![Ddl::CreateRoutine { name: object(&name), routine: crate::routines::of_macro(&args, &definition), replace: or_replace }]),
        Statement::DropFunction(ast::DropFunction { if_exists, func_desc: names, .. }) | Statement::DropProcedure { if_exists, proc_desc: names, .. } => {
            Stmt::Ddl(names.iter().map(|f| Ddl::DropRoutine { name: object(&f.name), if_exists }).collect())
        }
        _ => return None,
    })
}

/// The type an ADD COLUMN passes on: a type the lake checks (uuid, JSON, VARCHAR(n)) keeps its own
/// spelling, so the AddColumn branch holds it as text and records it (`rules::create::sql_type`); any
/// other is as DataFusion plans it (`sql_type`).
fn added_type(t: &ast::DataType) -> String {
    match crate::rules::create::sql_type(t) {
        Ok(Some(_)) => t.to_string(),
        _ => sql_type(t),
    }
}

/// A column type as DataFusion plans it: bytes by any of their names (`BYTEA`), and JSON kept as
/// text (`VARIANT`, `JSON`, `JSONB`: the JSON functions read it).
pub fn sql_type(t: &ast::DataType) -> String {
    use ast::DataType as T;
    match t {
        T::Binary(_) | T::Varbinary(_) | T::Blob(_) | T::Bytes(_) => "BYTEA".into(),
        T::JSON | T::JSONB => "VARCHAR".into(),
        T::Custom(name, args) if args.is_empty() && name.to_string().eq_ignore_ascii_case("variant") => "VARCHAR".into(),
        t if crate::seq::serial_type(t).is_some() => crate::seq::serial_type(t).unwrap_or_default().into(), // (an identity column: `create_spec`)
        t => t.to_string(),
    }
}

/// A statement's declared columns (`CREATE TABLE t (…)`) as `declared` takes them.
pub fn columns_sql(columns: &[ast::ColumnDef]) -> String {
    columns.iter().map(|c| format!("{} {}", c.name, sql_type(&c.data_type))).collect::<Vec<_>>().join(", ")
}

async fn declared_of(columns: &[ast::ColumnDef]) -> Result<Vec<datafusion::arrow::datatypes::FieldRef>> { declared(&columns_sql(columns)).await }

/// Columns as a node declares them (`ext_names::declared`), in the node's own session: its config and its settings'
/// dialect, so DuckDB's `STRUCT(a INT)` reads here as it does in a query.
pub async fn declared(cols: &str) -> Result<Vec<datafusion::arrow::datatypes::FieldRef>> {
    let ctx = SessionContext::new_with_config(crate::optimize::config(datafusion::prelude::SessionConfig::new())); // (TIMESTAMPTZ in UTC)
    let ctx = crate::settings::apply(ctx).await?; // (the session's dialect: DuckDB's STRUCT(a INT), …)
    crate::ext_names::declared(ctx, cols).await
}

/// The `sql_types` of a table made from `sql`, whose columns are `names`: the query's output columns in its order, as
/// the table names them. Those the query takes from a uuid, json or jsonb (or a maker) keep their type, as `CREATE TABLE
/// … AS` keeps them and a view's table does too (`pg_types::columns`). A query that gives another number of columns
/// than `names` gives none: a type matched to the wrong name would be a guess. (A view kept as partial rows is no
/// query's columns: its caller passes none, `finish.rs`.)
pub async fn query_types(lake: &Lake, sql: &str, names: &[String]) -> BTreeMap<String, String> {
    let shape = crate::pg_types::columns(lake, sql).await;
    if shape.types.len() != names.len() {
        return BTreeMap::new();
    }
    names.iter().zip(&shape.types).filter_map(|(n, t)| (*t).map(|t| (n.clone(), t.name().to_string()))).collect()
}

/// `CREATE TABLE t (a BIGINT, b VARCHAR, PRIMARY KEY (a)) [WITH (publish = 'delta,iceberg',
/// cluster_by = 'b', merge = 'total:sum', partition_by = 'day(ts)')]` → the table name and its spec. SQL types become Arrow
/// types the way DataFusion maps them.
/// With `AS SELECT`, the columns are the query's (run over `from`'s tables; `files`: local files
/// too, for `pondra sql` on its own machine), or those it names (`CREATE TABLE t (a INT, b
/// VARCHAR) AS VALUES …`: the query's columns by position).
pub async fn create_spec(c: &ast::CreateTable, from: &Lake, files: bool) -> Result<String> {
    let (as_text, mut enums, mut sql_types) = crate::types::columns(from, &c.columns).await?; // (an enum or a uuid column holds text: `types.rs`)
    let declared = || declared_of(&as_text);
    let fields = match (&c.like, &c.query) {
        // `CREATE TABLE t LIKE s` takes s's columns and their types, as Postgres's does by default
        // (made with none, it was a table of no columns).
        (Some(ast::CreateTableLikeKind::Plain(l) | ast::CreateTableLikeKind::Parenthesized(l)), _) => {
            ensure!(c.columns.is_empty() && c.query.is_none(), "CREATE TABLE {} LIKE {}: columns or a query, or LIKE, not both", c.name, l.name);
            (enums, sql_types) = crate::types::of_table(from, &object(&l.name)).await; // (its enum and uuid columns are those here too)
            let sql = format!("SELECT * FROM {} LIMIT 0", l.name);
            crate::engine::sql(&session(from, &sql, "").await?, &sql).await?.schema().fields().iter().cloned().collect::<Vec<_>>()
        }
        (None, Some(q)) => {
            let sql = q.to_string();
            let ctx = session(from, &sql, "").await?;
            let ctx = if files { ctx.enable_url_table() } else { ctx };
            let df = crate::engine::sql(&ctx, &sql).await?;
            // (the types the query gives: the analyzer's rules make 1e15 a whole decimal and a float meeting a decimal a float, and the table must hold what the query gives)
            let fields = crate::query::run_schema(&ctx, df.logical_plan())?.fields().iter().cloned().collect::<Vec<_>>();
            match c.columns.is_empty() {
                true => {
                    let names: Vec<String> = fields.iter().map(|f| f.name().clone()).collect();
                    sql_types.extend(query_types(from, &sql, &names).await);
                    fields
                }
                false => {
                    ensure!(fields.len() == c.columns.len(), "CREATE TABLE {} names {} columns, and its query gives {}", c.name, c.columns.len(), fields.len());
                    declared().await?
                }
            }
        }
        (None, None) => declared().await?,
    };
    let mut columns: Vec<(String, String)> = fields.iter().map(|f| (f.name().clone(), crate::answer::type_name(f.data_type()))).collect();
    // (what the statement declares, read from its AST alone: `rules/create.rs`, refused in the order a node always has)
    let d = crate::rules::create::declared(c)?;
    if d.keyed() && !columns.iter().any(|(c, _)| c == "_deleted") {
        columns.push(("_deleted".into(), "Boolean".into())); // (so DELETE works; writes leave it out)
    }
    let spec = j!({"columns": columns, "key": d.key, "merge": d.merge, "publish": d.list("publish"), "cluster_by": d.list("cluster_by"), "ttl": d.options.get("ttl"), "partition_by": d.options.get("partition_by"), "order_by": d.options.get("order_by"), "not_null": d.not_null, "defaults": d.defaults, "checks": d.checks, "retention": d.options.get("retention"), "identity": d.identity, "constraints": d.constraints, "enums": enums, "sql_types": sql_types});
    Ok(spec.to_string())
}

/// Create a table (or change it: a spec sent again) from any node: the leader does it.
pub async fn define(app: &crate::app::App, name: &str, spec: &str) -> Result<Value> {
    let out: Value = match app.seq {
        None => post(&app.cluster.leader.addr, &Request::Table(name.into(), spec.into())).await?, // (the leader's refusal, by name)
        Some(_) => {
            let _guard = app.lock.lock().await;
            create_table(&app.lake, name, spec).await?
        }
    };
    if let Some(n) = out["notice"].as_str() {
        crate::routines::heard(n); // (told through the door the statement came in by)
    }
    Ok(out)
}

/// `ALTER TABLE t ADD COLUMN c TYPE`: the table's definition with the new column at the end, sent
/// like a CREATE TABLE (the leader adds it; old rows read it as null). None: IF NOT EXISTS, and it does.
async fn alter_spec(lake: &Lake, stmt: &Stmt) -> Result<Option<String>> {
    use datafusion::sql::sqlparser::{dialect::GenericDialect, parser::Parser};
    let table = stmt.table();
    let m: TableMeta = (*lake.table_meta(&table).await?).clone().logical(); // (as SQL names it)
    let ttl = m.ttl.as_ref().map(|(c, s)| format!("{c}:{s}"));
    let mut spec = j!({"columns": m.columns, "key": m.key, "merge": m.merge, "publish": m.publish, "cluster_by": m.cluster, "ttl": ttl, "partition_by": m.partition, "order_by": m.order, "not_null": m.not_null, "defaults": m.defaults, "properties": m.properties, "checks": m.checks, "retention": m.retention_secs.map(|s| format!("{s} seconds"))});
    match stmt {
        Stmt::AddColumn(_, column, sql_type, if_not_exists) => {
            if m.columns.iter().any(|(c, _)| c == column) {
                ensure!(*if_not_exists, crate::rules::alter::has_column(&table, column));
                return Ok(None);
            }
            let ctx = SessionContext::new_with_config(crate::optimize::config(datafusion::prelude::SessionConfig::new())); // (TIMESTAMPTZ in UTC)
            let declared = Parser::new(&GenericDialect {}).try_with_sql(sql_type).and_then(|mut p| p.parse_data_type());
            let enumerated = match &declared {
                Ok(t) => crate::types::of(lake, t).await?,
                Err(_) => None,
            };
            let held = match &declared {
                Ok(t) => crate::rules::create::sql_type(t)?,
                Err(_) => None,
            };
            let sql_type = if enumerated.is_some() || held.is_some() { "VARCHAR" } else { sql_type }; // (an enum, a uuid or a VARCHAR(n) column holds text)
            ctx.sql(&format!("CREATE TABLE t ({column} {sql_type})")).await?;
            let mut columns = m.columns.clone();
            columns.push((column.to_string(), crate::answer::type_name(ctx.table("t").await?.schema().field(0).data_type())));
            spec["columns"] = j!(columns);
            if let Some(e) = enumerated {
                spec["enums"] = j!({column: e});
            }
            if let Some(t) = held {
                spec["sql_types"] = j!({column: t});
            }
        }
        // (files written from now on follow them; the ones before keep their order until merged)
        Stmt::SetOptions(_, options) => {
            let list = |v: &str| v.split(',').map(str::trim).filter(|s| !s.is_empty()).map(String::from).collect::<Vec<_>>();
            for (k, v) in options {
                match k.as_str() {
                    "publish" | "cluster_by" => spec[k] = j!(list(v)),
                    "ttl" | "order_by" | "retention" => spec[k.as_str()] = j!(v),
                    "partition_by" => bail!("PARTITION BY can't change: each of {table}'s files holds one partition"),
                    // Other engines' own (`write.delete.mode`, `write.merge.mode`…): kept and published
                    // for them (Iceberg's table properties); '' takes one out.
                    p if p.contains('.') && v.is_empty() => drop(spec["properties"].as_object_mut().map(|o| o.remove(p))),
                    p if p.contains('.') => spec["properties"][p] = j!(v),
                    other => bail!("{other}: ALTER TABLE … SET takes publish, cluster_by, ttl, order_by and retention, and other engines' properties (write.delete.mode = 'merge-on-read')"),
                }
            }
        }
        _ => bail!("not an ALTER TABLE"),
    }
    Ok(Some(spec.to_string()))
}

/// The rows a write to a keyed table upserts: the INSERT's query, the updated rows, or the rows
/// to delete (marked `_deleted`). Keys never change; merge tables only take INSERTs.
pub(crate) fn rows_sql(meta: &TableMeta, stmt: &Stmt) -> Result<String> {
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
            if !meta.merge.is_empty() { return Err(crate::codes::coded("0A000", "merge tables combine rows per key: INSERT into them instead of UPDATE")); }
            ensure!(set.iter().all(|(c, _)| !meta.key.contains(c) && meta.columns.iter().any(|(n, _)| n == c)), "UPDATE sets existing non-key columns");
            let pick = |c: &str| match set.iter().find(|(s, _)| s == c) {
                Some((_, e)) => format!("({})", crate::rules::rows::value(meta, c, e)),
                None => q(c), // (`_deleted` too: an updated row keeps its own, as `change::select_as` does)
            };
            Ok(select(&pick, t, cond))
        }
        Stmt::Delete(t, cond) => {
            if !meta.merge.is_empty() { return Err(crate::codes::coded("0A000", "merge tables combine rows per key: INSERT into them instead of DELETE")); }
            ensure!(deletes, "DELETE needs an upsert table with a Boolean _deleted column");
            Ok(select(&|c: &str| if c == "_deleted" { "true".into() } else { q(c) }, t, cond))
        }
        Stmt::Create(_) | Stmt::Define(..) | Stmt::AddColumn(..) | Stmt::SetOptions(..) | Stmt::Ddl(_) | Stmt::Merge(_) | Stmt::Invalid(_) | Stmt::CopyTo(..) | Stmt::CopyInto(..) | Stmt::InsertInto(..) | Stmt::TempView(..) | Stmt::TempSecret(..) => unreachable!("not a row write here"),
    }
}

/// `INSERT INTO t (b, a) …` as an INSERT of whole rows: the columns it names from its query, by
/// position, the others NULL (`_deleted` left for `rows` to fill), in the table's order. `VALUES`
/// stay `VALUES`, so they still go through the log.
pub async fn whole_rows(lake: &Lake, table: &str, names: &[String], query: &str) -> Result<String> {
    let (other, name) = crate::ddl::resolve(lake, table).await?;
    let meta: TableMeta = other.as_deref().unwrap_or(lake).cat.get(&table_key(&name)).await?.with_context(|| format!("no table {table}"))?;
    whole_rows_of(lake, table, &meta, names, query).await
}

/// `whole_rows` for a table whose stored meta the caller holds (`meta`, as `lake` stores it, under the name `table` the
/// statement gives): a one-row upsert's proposed row is worked out here once, before its locks (`txn::upsert_point`).
pub(crate) async fn whole_rows_of(lake: &Lake, table: &str, meta: &TableMeta, names: &[String], query: &str) -> Result<String> {
    let columns = crate::rules::rows::row_columns(meta.logical_columns().into_iter().map(|(c, _)| c), names);
    let names = if names.is_empty() { implicit(&columns, crate::rules::rows::rows_width(query)) } else { names }; // (`INSERT INTO t VALUES (1, DEFAULT)`: the columns its rows give)
    let default = |c: &str| crate::rules::rows::default_sql(meta, c);
    if crate::rules::rows::plain_values(query).is_some() && meta.defaults.values().any(|d| crate::seq::calls(d)) {
        // (`nextval` in rows of VALUES: taken here, in the rows' order: `seq::taken`)
        let used = std::cell::RefCell::new(vec![]);
        crate::rules::rows::rows_for(table, &columns, names, query, &|c| default(c).inspect(|d| used.borrow_mut().push(d.clone())))?;
        let taken = std::cell::RefCell::new(crate::seq::taken(lake, &used.into_inner()).await?);
        return crate::rules::rows::rows_for(table, &columns, names, query, &|c| default(c).map(|d| crate::seq::put(&d, &mut taken.borrow_mut())));
    }
    crate::rules::rows::rows_for(table, &columns, names, query, &default)
}

/// An INSERT's query takes its columns by position, so the same column twice is fine
/// (`INSERT INTO t SELECT v, v FROM …`): the second one gets a name of its own for DataFusion.
fn distinct_names(q: &mut ast::Query) {
    let ast::SetExpr::Select(s) = q.body.as_mut() else { return };
    let mut seen = std::collections::HashSet::new();
    for (i, item) in s.projection.iter_mut().enumerate() {
        if let ast::SelectItem::UnnamedExpr(e) = item {
            if !seen.insert(e.to_string()) {
                *item = ast::SelectItem::ExprWithAlias { expr: e.clone(), alias: ast::Ident::new(format!("__pondra_{i}")) };
            }
        }
    }
}

/// `CHECKPOINT` (DuckDB's word for it): tier every table's log into Parquet now, and write the
/// catalog down, so what other engines read is up to date.
pub fn checkpoint(sql: &str) -> bool {
    let code: String = sql.lines().map(|l| l.split("--").next().unwrap_or("")).collect::<Vec<_>>().join(" "); // (comments aside)
    code.trim().trim_end_matches(';').trim().eq_ignore_ascii_case("checkpoint")
}

/// Does a write to this table go through the log? Keyed tables' do (new versions), and so does
/// every `INSERT … VALUES` (a few rows each: a Parquet file apiece would pile up — the log gathers
/// them into one per tiering round). Other INSERTs write Parquet, committed through the log as
/// files (`adopt::file`), which the views and tasks that follow the table take as they take the
/// log's rows.
fn through_log(meta: &TableMeta, stmt: &Stmt) -> bool {
    let values = |q: &str| first_word(q).get(..6).is_some_and(|w| w.eq_ignore_ascii_case("values"));
    !meta.key.is_empty() || matches!(stmt, Stmt::Insert(_, q) if values(q))
}

/// Do views or streaming tasks follow this table?
pub async fn follows(lake: &Lake, table: &str) -> Result<bool> {
    let views = lake.cat.scan::<crate::views::View>("v/", "v0").await?.into_iter().any(|(_, v)| v.follows(table));
    Ok(views || lake.cat.scan::<crate::tasks::Task>("k/", "k0").await?.into_iter().any(|(_, t)| t.source == table))
}

/// Run a row query here, its rows shaped once for `t`, the table `name` (`shaped`): in the table's column order and types,
/// with `how` saying which rules they meet. `lake` is the table's own: the one a DEFAULT's `nextval` takes its values from.
pub async fn rows(lake: &Lake, ctx: &SessionContext, t: &Arc<crate::rules::Table>, name: &str, sql: &str, how: crate::rules::How) -> Result<crate::rules::Shaped> {
    shaped(lake, t, name, planned(ctx, &t.schema, sql).await?, how).await
}

/// A query's rows as planned in `ctx`, before they fit the table (`rows`); None when it gives none.
pub(crate) async fn planned(ctx: &SessionContext, target: &SchemaRef, sql: &str) -> Result<Option<RecordBatch>> {
    let sql = &values_typed(sql, target);
    let one; // (`nextval` numbers `VALUES` in their order, as Postgres does: one partition)
    let ctx = match crate::seq::calls(sql) {
        true => {
            let mut state = ctx.state();
            state.config_mut().options_mut().execution.target_partitions = 1;
            one = SessionContext::new_with_state(state);
            &one
        }
        false => ctx,
    };
    let batches = crate::engine::sql(ctx, &crate::sys::planned(&crate::routines::rows_apart(sql))?).await?.collect().await?;
    let Some(first) = batches.first() else { return Ok(None) };
    Ok(Some(concat_batches(&first.schema(), &batches)?))
}

/// A query's rows (`all`; None: it gave none) shaped once for `t`, the table `name` (`rules::shape`): by position, the
/// columns left out at the end given their DEFAULT, held forms, the checks, and for `How::Insert` no key twice. `home` is the
/// table's own lake: the one a DEFAULT's `nextval` takes its values from.
async fn shaped(home: &Lake, t: &Arc<crate::rules::Table>, name: &str, all: Option<RecordBatch>, how: crate::rules::How) -> Result<crate::rules::Shaped> {
    let rows = match all {
        Some(all) => crate::rules::Rows::Positional(all),
        None => crate::rules::Rows::Whole(RecordBatch::new_empty(t.schema.clone())), // (no rows: in the table's columns already)
    };
    crate::rules::shape(t, name, rows, how, &crate::defaults::Node(home)).await
}

/// An INSERT's rows into `t`'s table `name`, from `sql` (its rows' query), shaped once for `t` (`shaped`). A `VALUES` of
/// literals is built here as DataFusion would plan it, with no session (`literals.rs`); anything else is planned in a session
/// of `lake` as of `upto` (`files`: this machine's files too). `home` is the table's own lake: the one a DEFAULT's `nextval`
/// takes its values from.
#[allow(clippy::too_many_arguments)]
pub async fn rows_of(lake: &Lake, home: &Lake, t: &Arc<crate::rules::Table>, name: &str, sql: &str, upto: Option<u64>, files: bool, how: crate::rules::How) -> Result<crate::rules::Shaped> {
    if let Some(all) = crate::literals::literal_rows(sql, &t.schema) {
        return shaped(home, t, name, Some(all), how).await;
    }
    let ctx = crate::query::session_at(lake, sql, "", upto).await?;
    let ctx = if files { ctx.enable_url_table() } else { ctx };
    shaped(home, t, name, planned(&ctx, &t.schema, sql).await?, how).await
}

/// An INSERT's `VALUES` with each value for a DOUBLE, REAL or DECIMAL column cast to that column's
/// type, as Postgres takes them. DataFusion first makes one type of a VALUES column's rows, and
/// `1e20` and `1e-20`, both decimals to it, made a DECIMAL(38, 20) that couldn't hold the first,
/// though the column was a DOUBLE. Values fill the table's columns as `rows` fills them: by position, or, in the
/// `SELECT * FROM (VALUES …) AS __pondra_v(…)` that `rules::rows::rows_for` makes for a list naming the delete marker, by name.
fn values_typed<'a>(sql: &'a str, target: &datafusion::arrow::datatypes::Schema) -> std::borrow::Cow<'a, str> {
    use datafusion::sql::sqlparser::{dialect::GenericDialect, parser::Parser};
    let typed = |t: &DataType| t.is_floating() || t.is_decimal();
    let values = first_word(sql).get(..6).is_some_and(|w| w.eq_ignore_ascii_case("values") || w.eq_ignore_ascii_case("select"));
    if !values || !target.fields().iter().any(|f| typed(f.data_type())) {
        return sql.into();
    }
    let parse = |s: &str| Parser::new(&GenericDialect {}).try_with_sql(s).and_then(|mut p| p.parse_query());
    let Ok(mut q) = parse(sql) else { return sql.into() };
    let Some((v, names)) = values_of(&mut q) else { return sql.into() };
    let fields: Vec<Option<&datafusion::arrow::datatypes::Field>> = match names {
        Some(names) => names.iter().map(|n| target.field_with_name(n).ok()).collect(),
        None => by_position(target).map(|f| Some(&**f)).collect(),
    };
    for row in &mut v.rows {
        for (e, f) in row.content.iter_mut().zip(fields.iter().copied()) {
            let Some(f) = f.filter(|f| typed(f.data_type())) else { continue };
            let Ok(cast) = Parser::new(&GenericDialect {}).try_with_sql(&format!("arrow_cast(NULL, '{}')", f.data_type())).and_then(|mut p| p.parse_expr()) else { continue };
            let ast::Expr::Function(mut cast) = cast else { continue };
            let ast::FunctionArguments::List(args) = &mut cast.args else { continue };
            args.args[0] = ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(std::mem::replace(e, ast::Expr::Value(ast::Value::Null.into()))));
            *e = ast::Expr::Function(cast);
        }
    }
    crate::text::sql(&Statement::Query(q)).into()
}

/// The `VALUES` a query is (with no names), or the one `rules::rows::rows_for` wraps for a list naming the delete marker
/// (`SELECT * FROM (VALUES …) AS __pondra_v(names)`), with the names its columns have.
fn values_of(q: &mut ast::Query) -> Option<(&mut ast::Values, Option<Vec<String>>)> {
    match q.body.as_mut() {
        ast::SetExpr::Values(v) => Some((v, None)),
        ast::SetExpr::Select(s) => {
            let [ast::TableWithJoins { relation: ast::TableFactor::Derived { subquery, alias: Some(alias), .. }, joins }] = s.from.as_mut_slice() else { return None };
            if !joins.is_empty() || alias.name.value != "__pondra_v" {
                return None;
            }
            let ast::SetExpr::Values(v) = subquery.body.as_mut() else { return None };
            Some((v, Some(alias.columns.iter().map(|c| c.name.value.clone()).collect())))
        }
        _ => None,
    }
}

// ---------------------------------------------------------------- bulk INSERT into append tables

/// The files one INSERT wrote (`job`: a retried job is recorded once).
#[derive(Serialize, Deserialize, Clone)]
pub struct Files {
    table: String,
    job: String,
    columns: Vec<(String, String)>,
    pub files: Vec<DataFile>,
}

impl Files {
    /// The table they go into.
    pub fn table(&self) -> &str { &self.table }

    /// Where its files are.
    pub fn paths(&self) -> Vec<String> { self.files.iter().map(|f| f.path.clone()).collect() }
}

/// Run the query here and write its rows as Parquet into the table's folder (None: this job was
/// already recorded).
/// `stamp`: the commit number and time the rows' system columns get (`sys.rs`); None: the leader
/// stamps them when it records the files (`record`).
pub async fn write_files(lake: &Lake, ctx: &SessionContext, table: &str, query: &str, job: &str, stamp: Option<crate::log::Reserved>) -> Result<Option<Files>> {
    if recorded(lake, job).await? {
        return Ok(None);
    }
    // (out of memory, once more frugally: the first try's files are never recorded, and the orphan sweep takes them)
    let (columns, files) = crate::query::frugally(ctx, |ctx| async move {
        in_order(&ctx);
        let df = crate::engine::sql(&ctx, &crate::sys::planned(query)?).await?;
        write_rows(lake, &ctx, table, df, stamp).await
    }).await?;
    Ok(Some(Files { table: table.into(), job: job.into(), columns, files }))
}

/// Whether the leader has recorded this INSERT's files already.
async fn recorded(lake: &Lake, job: &str) -> Result<bool> { Ok(lake.cat.get::<u64>(&producer_key(&format!("job:{job}"))).await?.is_some()) }

/// Has a job's plain INSERT (its log flush, producer `sql:{job}`) been recorded? A retry of it is a
/// duplicate, so the key check doesn't meet the job's own rows.
pub(crate) async fn sent(lake: &Lake, job: &str) -> Result<bool> { Ok(lake.cat.get::<u64>(&producer_key(&format!("sql:{job}"))).await?.unwrap_or(0) > 0) }

/// Each partition of a query planned in `ctx` from now on writes its own files, in the order its
/// rows come: data that arrives in order (by time, by key) lands in files that each hold a narrow
/// range of it, which is what lets a filter skip files and a distributed query split tables by
/// key (`spmd`). (Round-robin repartitioning would interleave them, so it is off.)
pub fn in_order(ctx: &SessionContext) { ctx.state_ref().write().config_mut().options_mut().optimizer.enable_round_robin_repartition = false; }

/// `df`'s rows written as Parquet into the table's folder, by position as the table's columns (or,
/// for a new table, the query's, with plain Utf8 strings): the columns, and the files. A value the query gives is cast
/// to its column's type as INSERT casts it, so a bulk INSERT, COPY INTO and a share refuse what INSERT refuses (`cast_given`).
pub async fn write_rows(lake: &Lake, ctx: &SessionContext, table: &str, df: datafusion::prelude::DataFrame, stamp: Option<crate::log::Reserved>) -> Result<(Vec<(String, String)>, Vec<DataFile>)> {
    use datafusion::arrow::datatypes::DataType;
    use datafusion::prelude::{cast as cast_to, Expr};
    let meta = lake.cat.meta(&table_key(table)).await?; // (as parsed once a version: `rules` is the memo)
    let rules = meta.as_ref().map(|m| m.rules()).transpose()?.filter(|t| t.ruled()); // (None: a new table, or one with no rule)
    // (a table's live columns, in order, written under their stored names: ADR-022)
    let target: Vec<(String, DataType)> = match &meta {
        Some(m) => m.live().map(|(s, _, t)| Ok((s.to_string(), crate::answer::dtype(t)?))).collect::<Result<_>>()?,
        // (a new table takes the types the query gives as it runs, as `create_spec` does)
        None => crate::query::run_schema(ctx, df.logical_plan())?.fields().iter().map(|f| (f.name().clone(), if *f.data_type() == DataType::Utf8View { DataType::Utf8 } else { f.data_type().clone() })).collect(),
    };
    if df.schema().fields().len() > target.len() {
        return Err(crate::codes::coded("42601", format!("INSERT has more expressions than target columns ({} given, {table} has {})", df.schema().fields().len(), target.len())));
    }
    let given = df.schema().columns(); // (the table's first columns; any after them are null)
    let null = || datafusion::prelude::lit(datafusion::scalar::ScalarValue::Null);
    // (a column the query leaves out: its DEFAULT, else NULL)
    let default = |stored: &str| -> Result<Expr> {
        match meta.as_ref().and_then(|m| m.defaults.get(stored)) {
            Some(e) => Ok(ctx.parse_sql_expr(e, df.schema())?),
            None => Ok(null()),
        }
    };
    // (the plan selects a given column as it comes, and cast_given casts it batch by batch: the same types, and the stream's
    // schema is the one the casts give, so the names and nullability stay as written)
    let picks = target.iter().enumerate().map(|(i, (name, t))| Ok(match given.get(i) {
        Some(c) => Expr::Column(c.clone()).alias(name),
        None => cast_to(default(name)?, t.clone()).alias(name),
    })).collect::<Result<Vec<_>>>()?;
    let casts = target.iter().enumerate().map(|(i, (name, t))| Ok(cast_to(match given.get(i) {
        Some(c) => Expr::Column(c.clone()),
        None => default(name)?,
    }, t.clone()).alias(name))).collect::<Result<Vec<_>>>()?;
    let schema: SchemaRef = Arc::new(df.clone().select(casts)?.schema().as_arrow().clone());
    let given_n = given.len();
    let df = df.select(picks)?;
    let columns = target.iter().map(|(n, t)| (n.clone(), t.to_string())).collect();
    let partition = meta.as_ref().and_then(|m| m.partition.clone());
    let next = Arc::new(std::sync::atomic::AtomicU64::new(0)); // (the streams share one block of ids)
    let streams = df.execute_stream_partitioned().await?.into_iter().map(|rows| cast_given(rows, schema.clone(), given_n)).map(|rows| checked_stream(rows, rules.clone(), table)).map(|rows| match stamp {
        Some(reserved) => crate::sys::stamp_stream(rows, next.clone(), reserved),
        None => Ok(rows),
    });
    // (files of up to 4M rows, as a merge writes them: `maintain` leaves them alone)
    let writes = streams.collect::<Result<Vec<_>>>()?.into_iter().map(|rows| crate::tier::write_stream(lake, table, rows, 4_000_000, &[], true, partition.as_deref()));
    Ok((columns, futures::future::try_join_all(writes).await?.concat()))
}

/// `rows` with its first `given` columns cast to the table's types, batch by batch, by `query::strict`, the cast INSERT
/// makes of a value: a value that won't go is the stream's error, with Postgres's words and SQLSTATE. The columns after
/// them are already of their types (a DEFAULT, or NULL, cast in the plan). `schema` is the stream's: every column's type.
/// A value a CSV reader refuses as its column's type (`read_csv`'s `types`) gets the same words (`casts::parse_refusal`).
fn cast_given(rows: datafusion::execution::SendableRecordBatchStream, schema: SchemaRef, given: usize) -> datafusion::execution::SendableRecordBatchStream {
    use futures::StreamExt;
    let out = schema.clone();
    let cast = rows.map(move |b| -> datafusion::error::Result<RecordBatch> {
        let mut columns = b.map_err(|e| crate::casts::parse_refusal(&e.to_string()).unwrap_or(e))?.columns().to_vec();
        for (column, field) in columns.iter_mut().zip(schema.fields()).take(given) {
            // (a refusal is the DataFusion error `casts::into` made, kept as it is: `codes::of` finds its code)
            *column = crate::query::strict(column, field.data_type()).map_err(|e| match e.downcast::<datafusion::error::DataFusionError>() {
                Ok(d) => d,
                Err(e) => datafusion::error::DataFusionError::External(e.into()),
            })?;
        }
        Ok(RecordBatch::try_new(schema.clone(), columns)?)
    });
    Box::pin(datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(out, cast))
}

/// A bulk INSERT's rows (under their stored names), checked as they stream into files: `rules::checked` on each batch
/// (held forms, then the checks), over `t` (None: nothing to check).
fn checked_stream(rows: datafusion::execution::SendableRecordBatchStream, t: Option<Arc<crate::rules::Table>>, table: &str) -> datafusion::execution::SendableRecordBatchStream {
    use futures::StreamExt;
    let Some(t) = t else { return rows };
    let (schema, table) = (rows.schema(), table.to_string());
    let checked = rows.map(move |b| crate::rules::checked(&t, &table, b?, false, crate::defaults::base()).map_err(|e| datafusion::error::DataFusionError::External(e.into())));
    Box::pin(datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(schema, checked))
}

/// Leader: record an INSERT's files in one commit through the log (`adopt::file`), creating the
/// table if it's new. Files written without their rows' system columns (a table something follows,
/// `pondra sql` where no leader answered, the inbox) take a lineage that gives them. Files written
/// with them, under a commit number reserved when the INSERT began, are refused once something
/// follows the table: the INSERT goes again without them, so that its rows' `_version` is the
/// commit that records them, which what follows a table goes by.
pub async fn record(lake: &Lake, f: Files, seq: &Sequencer) -> Result<Value> {
    if lake.cat.get::<u64>(&producer_key(&format!("job:{}", f.job))).await?.is_some() {
        return Ok(j!({"duplicate": true})); // the same job finished concurrently
    }
    let new = TableMeta { columns: f.columns.clone(), publish: default_publish(), ids: true, tiered: lake.visible(), made: Some((lake.visible(), crate::log::now_ms())), ..Default::default() }; // (a table `POST /insert/{t}` makes starts at the log's end, as CREATE TABLE's does; SQL's INSERT needs one already: `is_there`)
    let meta = lake.cat.get::<TableMeta>(&table_key(&f.table)).await?.unwrap_or_else(|| new.clone());
    // Types as the lake stores them, so a writer may name them its own way (`Utf8View`, `VARIANT`).
    let types = |c: &[(String, String)]| c.iter().map(|(_, t)| crate::answer::dtype(t).map(|t| crate::answer::type_name(&t)).unwrap_or_else(|_| t.clone())).collect::<Vec<_>>();
    ensure!(meta.key.is_empty(), "INSERT into a keyed table goes through the log");
    ensure!(!f.files.iter().any(|d| d.sys) || !follows(lake, &f.table).await?, "{AGAIN}: a materialized view or task of {} was made as it was written", f.table);
    let live: Vec<(String, String)> = meta.live().map(|(s, _, t)| (s.to_string(), t.to_string())).collect(); // (dropped columns aren't written)
    let listed = |c: &[(String, String)]| c.iter().map(|(n, t)| format!("{} {}", meta.name_of(n), crate::objects::sql_type(t))).collect::<Vec<_>>().join(", ");
    ensure!(types(&live) == types(&f.columns), "the rows' columns ({}) aren't {}'s ({}), in order and type; if the table changed while they were written, write them again", listed(&f.columns), f.table, listed(&live));
    let rows: u64 = f.files.iter().map(|f| f.rows).sum();
    let commit = crate::adopt::FileCommit { table: f.table.clone(), job: f.job.clone(), added: f.files, new: Some(new), ..Default::default() };
    Ok(match crate::adopt::file(lake, seq, &[commit]).await? {
        Some(_) => j!({"rows": rows}),
        None => j!({"duplicate": true}),
    })
}

/// What `record` says of files written with their rows' system columns once something follows
/// their table: the INSERT goes again, without them (`on_node_as` does it at once).
pub const AGAIN: &str = "INSERT again, its files without their system columns";

/// The commit number and time a bulk INSERT's files are stamped with, and its block of row ids
/// (None: its files are written without them, and take a lineage when recorded). A table views or
/// tasks follow takes none: what follows it goes by the commit that records the files (`record`).
pub async fn stamp(lake: &Lake, table: &str, to: Option<&crate::log::To>) -> Result<Option<crate::log::Reserved>> {
    if follows(lake, table).await? {
        return Ok(None);
    }
    Ok(match to {
        Some(to) => Some(to.reserve().await?),
        None => reserve(lake).await,
    })
}

// ---------------------------------------------------------------- on a node

/// `POST /sql` with a write statement: this node does the work; the leader records it.
pub async fn on_node(app: &crate::app::App, stmt: Stmt, job: Option<String>) -> Result<Value> { on_node_as(app, stmt, job, false).await }

/// `on_node`; `files`: its SQL may read files on this machine (`FROM 'jan.csv'`: the shell's own
/// node, `server::owner`).
#[inline(never)] // (its work on the heap, made here: `App::query_as`)
pub fn on_node_as(app: &crate::app::App, stmt: Stmt, job: Option<String>, files: bool) -> futures::future::BoxFuture<'_, Result<Value>> {
    Box::pin(async move {
        app.lake.caught_up().await?; // (a node that just started: not against an older catalog than its leader's)
        let out = crate::ext::listing(on_node_listed(app, stmt, job, files)).await?; // (files outside the lake: listed once a statement)
        seen_here(app).await;
        Ok(out)
    })
}

/// SQL's CREATE TABLE of a table that's there, as Postgres has it: refused (42P07), left as it is with IF NOT EXISTS. With
/// OR REPLACE it is replaced by `create_as`: the old table stays until the new one takes its place.
async fn existing(app: &crate::app::App, c: &ast::CreateTable) -> Result<Option<Value>> {
    let mut found = there(&app.lake, c).await;
    if !matches!(found, Ok(None)) {
        seen_here(app).await; // (a table just made or dropped through another node: this one holds it once it holds what its leader had)
        found = there(&app.lake, c).await;
    }
    Ok(match found? {
        Some(true) => Some(j!({"table": object(&c.name), "exists": true})),
        _ => None,
    })
}

/// A name a CREATE TABLE is made under: `ddl::new_name`'s, and a staged name's (`sys::staged`, which only a replace makes),
/// checked by the table it is for.
async fn new_table(lake: &Lake, name: &str) -> Result<String> {
    match crate::sys::staged_for(name) {
        Some(table) => {
            crate::ddl::new_name(lake, table).await?;
            Ok(name.to_string())
        }
        None => crate::ddl::new_name(lake, name).await,
    }
}

/// The steps of a CREATE TABLE with a query, or OR REPLACE: `run` sends each statement where its door sends it (a node's
/// writer, or `pondra sql`'s leader), so both doors take the same steps. `armed` hears the name of the table a stopped
/// statement must drop (a node's memory watch: `Undo`).
async fn create_as<F>(run: F, lake: &Lake, c: &ast::CreateTable, job: &str, files: bool, armed: &mut (dyn FnMut(&str) + Send)) -> Result<Value>
where
    F: Fn(Stmt) -> futures::future::BoxFuture<'static, Result<Value>>,
{
    let name = object(&c.name);
    if c.or_replace {
        crate::replace::replacing(&run, lake, c, &name, job, files, armed).await
    } else {
        ctas(&run, lake, c, &name, job, files, armed).await
    }
}

/// CREATE TABLE (with a query): the table, with the query's columns, then its rows. A step that fails or stops drops the table
/// it made (`undo_made`): nothing lands, so the table goes too.
async fn ctas<F>(run: &F, lake: &Lake, c: &ast::CreateTable, name: &str, job: &str, files: bool, armed: &mut (dyn FnMut(&str) + Send)) -> Result<Value>
where
    F: Fn(Stmt) -> futures::future::BoxFuture<'static, Result<Value>>,
{
    let spec = made_by_job(create_spec(c, lake, files).await?, job)?; // (the table is this statement's: its `made_by`)
    let defined = run(Stmt::Define(name.to_string(), spec)).await;
    if defined.is_err() {
        return raced(c.if_not_exists, name, defined); // (nothing was made here: no table to drop)
    }
    armed(name);
    let Some(q) = &c.query else { return defined };
    let out = run(Stmt::Insert(name.to_string(), q.to_string())).await;
    if out.is_err() {
        undo_made(run, name, job).await;
    }
    out
}

/// Drops the table a statement made under `name` after it failed or stopped: purged, as `made_drop` says (never another
/// statement's table). The statement's own error stays its answer.
pub(crate) async fn undo_made<F>(run: &F, name: &str, job: &str)
where
    F: Fn(Stmt) -> futures::future::BoxFuture<'static, Result<Value>>,
{
    if let Err(e) = run(made_drop(name, job)).await {
        eprintln!("left {name} as it is: the drop after its CREATE TABLE … AS failed didn't answer: {e:#}");
    }
}

/// `create_as` on a node: each of its statements runs here, as the caller (`on_node_as`). A request stopped before its steps
/// answer drops the table it made (`Undo`: the memory watch stops a request by dropping its future, `oom.rs`).
async fn create_on_node(app: &crate::app::App, c: &ast::CreateTable, job: &str, files: bool) -> Result<Value> {
    let (app_run, job_run) = (app.clone(), job.to_string());
    let run = move |s: Stmt| -> futures::future::BoxFuture<'static, Result<Value>> {
        let (app, job) = (app_run.clone(), job_run.clone());
        Box::pin(async move { on_node_as(&app, s, Some(job), files).await })
    };
    let mut undo = Undo(None);
    let out = create_as(run, &app.lake, c, job, files, &mut |table: &str| undo.0 = Some((app.clone(), table.to_string(), job.to_string()))).await;
    undo.0 = None; // (answered, or refused: whatever it made is dropped by now, or is the statement's answer)
    out
}

/// Is the table a CREATE TABLE names there? None: no; Some(true): leave it (IF NOT EXISTS);
/// Some(false): replace it (OR REPLACE); otherwise refused.
async fn there(lake: &Lake, c: &ast::CreateTable) -> Result<Option<bool>> {
    let name = object(&c.name);
    ensure!(!(c.or_replace && c.if_not_exists), "CREATE OR REPLACE TABLE … IF NOT EXISTS: one or the other");
    let (other, table) = crate::ddl::resolve(lake, &name).await?;
    let lake = other.unwrap_or_else(|| lake.arc());
    if lake.cat.get::<TableMeta>(&table_key(&table)).await?.is_none() {
        return Ok(None);
    }
    if !(c.if_not_exists || c.or_replace) {
        return Err(already(&name));
    }
    Ok(Some(c.if_not_exists))
}

/// A write done through a follower is in its reads before it answers: the next statement on this
/// node sees it (a script's INSERT, then its SELECT), as on the leader. The follower waits until
/// its view holds everything the leader had committed once the write was done (5 s at most: a
/// view that far behind catches up by itself).
pub async fn seen_here(app: &crate::app::App) {
    if app.seq.is_some() || app.cluster.reader {
        return;
    }
    let url = crate::tls::url(&format!("{}/cluster/visible", app.cluster.leader.addr));
    let asked = async { http().get(url).timeout(std::time::Duration::from_secs(2)).send().await?.json::<u64>().await };
    let Ok(upto) = asked.await else { return };
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while app.lake.visible() < upto && std::time::Instant::now() < deadline {
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
}

/// Is there a table for SQL's INSERT into `table` to write? A file, another engine's table
/// attached, and a name this lake can't resolve answer yes: their own paths take them (or refuse
/// them by name).
async fn is_there(lake: &Lake, table: &str) -> Result<bool> {
    if crate::ext_names::spec(table).is_some() || crate::ext::outside_target(lake, table).await?.is_some() {
        return Ok(true);
    }
    let Ok((other, name)) = crate::ddl::resolve(lake, table).await else { return Ok(true) };
    Ok(other.as_deref().unwrap_or(lake).cat.get::<TableMeta>(&table_key(&name)).await?.is_some())
}

/// SQL's INSERT into a name no table has: refused, as Postgres and DuckDB refuse it (42P01).
/// `CREATE TABLE … AS SELECT` makes a table of a query (`POST /insert/{t}`, the bulk door, still
/// makes one).
fn no_table(table: &str) -> anyhow::Error {
    crate::codes::coded("42P01", format!("no table {table} (CREATE TABLE {table} AS SELECT … makes one from a query)"))
}

/// A write to an attached lake is in this node's reads of it before it answers, as one here is
/// (`seen_here`): our view of that lake follows its catalog every 250 ms, so the next statement
/// could read it from before the write (`smoke.py`'s CREATE TABLE … AS SELECT into another
/// database counted 0 rows, once, on macOS). It waits until the view holds the write's own mark
/// (`mark`: its job's progress, or the table made), 5 s at most.
async fn seen_there(other: &Lake, mark: Option<String>) {
    let Some(key) = mark else { return };
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while !matches!(other.cat.get::<Value>(&key).await, Ok(Some(_))) && std::time::Instant::now() < deadline {
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    // (and how far this node reads it, which its refresh moves every 250 ms: a read before then
    // took an UPDATE's old row out, from the view, but not its new one, from the log up to there)
    let _ = other.refresh().await;
}

/// The drop after a `CREATE TABLE … AS SELECT`'s INSERT failed or stopped: the table `job` made under `name`, purged. The
/// leader decides which table that is (`ddl::drop_table`, under the lake's lock): this node may not see the table yet, and
/// another statement's table under the name (its `made_by` isn't `job`: a race, or a drop and a new table through another
/// statement) is that statement's rows, which it leaves.
fn made_drop(name: &str, job: &str) -> Stmt {
    Stmt::Ddl(vec![crate::ddl::Ddl::DropTable { name: name.into(), if_exists: true, purge: true, made_by: Some(job.into()) }])
}

/// A `CREATE TABLE … IF NOT EXISTS` that another statement made first, between its check and its definition (refused
/// 42P07 by the definition), is the table that's there: answered as `existing` answers it. Any other answer stands.
fn raced(if_not_exists: bool, name: &str, out: Result<Value>) -> Result<Value> {
    match out {
        Err(e) if if_not_exists && crate::codes::of(&e) == "42P07" => Ok(j!({"table": name, "exists": true})),
        out => out,
    }
}

/// The table a `CREATE TABLE … AS SELECT` made, dropped if its request ends before the INSERT answers: the
/// memory watch stops a request by dropping its future (`oom.rs`), so the code after the INSERT never runs.
struct Undo(Option<(crate::app::App, String, String)>); // (app, table, job)

impl Drop for Undo {
    fn drop(&mut self) {
        let Some((app, name, job)) = self.0.take() else { return };
        // (carried: the drop runs as the caller that was stopped, with its sign-in and session, which tokio still
        // holds while a future is dropped)
        crate::panics::task(crate::auth::carried(async move {
            if let Err(e) = Box::pin(on_node_as(&app, made_drop(&name, &job), None, false)).await {
                eprintln!("left {name} as it is: the drop after its CREATE TABLE … AS, which stopped before it answered, didn't answer: {e:#}");
            }
        }));
    }
}

async fn on_node_listed(app: &crate::app::App, stmt: Stmt, job: Option<String>, files: bool) -> Result<Value> {
    let stmt = crate::protect::door(&app.lake, stmt).await?; // (a project's objects in a protected database: an apply's only)
    if let Some(out) = crate::temp::statement(app, &stmt, files).await? {
        return Ok(out); // (the session's own tables and views: on this node, in memory; a temporary table has no identity column)
    }
    identity(&app.lake, &stmt).await?; // (every SQL write's identity, before the DDL and transaction splits: autocommit and transactions both meet it)
    // DDL in an open transaction runs at once, as a commit of its own (ADR-067 §7); the notice follows its success. A
    // CREATE TABLE … AS's steps run at once too (`at_once`), so they are not kept.
    if let Some(note) = Box::pin(crate::txn::ddl_at_once(app, &stmt)).await? {
        let out = Box::pin(crate::txn::at_once(run_listed(app, stmt, job, files))).await?;
        if out["exists"] != true && out["unchanged"] != true {
            crate::routines::heard(&note); // (IF NOT EXISTS left the table as it was: nothing ran)
        }
        return Ok(out);
    }
    if let Some(out) = Box::pin(crate::txn::statement(app, &stmt, files)).await? {
        return Ok(out); // (in a transaction: kept until COMMIT, ADR-036 §5)
    }
    run_listed(app, stmt, job, files).await
}

/// A statement once the session's own tables and its transaction have had their turn (`on_node_listed`): written here
/// or by the leader.
async fn run_listed(app: &crate::app::App, stmt: Stmt, job: Option<String>, files: bool) -> Result<Value> {
    if let Stmt::Invalid(why) = stmt {
        bail!(why);
    }
    // (a backup is read, and backed up itself; COPY … TO writes outside it: ADR-070)
    if crate::backup::frozen(&app.lake) && !crate::backup::reads_only(&stmt) {
        return Err(crate::backup::refused(Some(&app.lake.url)));
    }
    // (COPY … TO reads the lake and writes no lake: a reader may export, as Postgres's read-only transactions allow it)
    if let Stmt::CopyTo(query, to, options) = &stmt {
        return crate::copy::copy_to(&app.lake, query, to, options, &app.cluster.nodes(), &app.cluster.addr).await; // (files outside the lake: from here, or every node its share)
    }
    // (above a view of a folder's write: a reader writes no file either)
    ensure!(!app.cluster.reader, "read-only node");
    if let Some((query, to, options)) = crate::ext::view_write(&app.lake, &stmt).await? {
        let out = crate::copy::copy_to(&app.lake, &query, &to, &options, &app.cluster.nodes(), &app.cluster.addr).await?; // (a view of a folder: a new file in it)
        return Ok(j!({"rows": out["copied"]}));
    }
    let stmt = match stmt {
        Stmt::InsertInto(t, names, query) => Stmt::Insert(t.clone(), whole_rows(&app.lake, &t, &names, &query).await?),
        Stmt::Insert(t, query) => {
            if !is_there(&app.lake, &t).await? {
                seen_here(app).await; // (a table just made through another node: this one holds it once it holds what its leader had)
                if !is_there(&app.lake, &t).await? {
                    return Err(no_table(&t));
                }
            }
            Stmt::Insert(t, query)
        }
        s => s,
    };
    if let Stmt::CopyInto(table, from, options) = &stmt {
        return crate::copy::copy_into(app, table, from, options).await; // (files into a table, each once a version)
    }
    let open = |ctx: SessionContext| if files { ctx.enable_url_table() } else { ctx };
    let named = job.is_some(); // (a job a client sent: its retry may be a duplicate already committed)
    let (lake, job) = (&app.lake, job.unwrap_or_else(crate::log::auto_job));
    if let Stmt::Ddl(ddls) = stmt {
        let mut out = j!({});
        for mut d in ddls {
            crate::branch::door(lake, &mut d).await?; // (a branch: who may make or drop one, and its owner; another database's sign-in: users::across)
            out = match &app.seq {
                Some(seq) => ddl_here(lake, seq, &app.lock, d.clone()).await?,
                None => post(&app.cluster.leader.addr, &Request::Ddl(d.clone())).await?,
            };
            crate::ddl::settle(lake, &d, &app.cluster.addr).await?; // (ATTACH, DETACH: here at once)
            if let Some(n) = out["notice"].as_str() {
                crate::routines::heard(n); // (carried out on the leader, perhaps over HTTP: told here, through this door)
            }
        }
        return Ok(out);
    }
    // CREATE TABLE … AS SELECT, and OR REPLACE: the steps `create_as` takes, on this node.
    if let Stmt::Create(c) = &stmt {
        if let Some(out) = Box::pin(existing(app, c)).await? {
            return Ok(out);
        }
        if c.query.is_some() || c.or_replace {
            return Box::pin(create_on_node(app, c, &job, files)).await;
        }
    }
    // Another engine's table, attached (`ATTACH … (TYPE delta | iceberg)`): its next version,
    // written from here.
    if let Some(target) = crate::ext::outside_target(lake, &stmt.table()).await? {
        return match &stmt {
            Stmt::Insert(_, query) => crate::write_outside::insert(lake, &target, query, &job).await,
            _ => bail!("{}: another engine's table takes INSERTs from Pondra (UPDATE, DELETE and MERGE: not yet)", stmt.table()),
        };
    }
    // (an UPDATE's, DELETE's or MERGE's target reaches here named as its files already: it read
    // "no table" with a quoted path)
    if let Some(spec) = crate::ext_names::spec(&stmt.table()) {
        bail!("{}: files or another engine's table; Pondra INSERTs into a Delta or Iceberg table (UPDATE, DELETE and MERGE: not yet)", spec.urls.join(", "));
    }
    // UPDATE and DELETE of an append table, and MERGE: the leader's (of the table's lake), from
    // one snapshot (`change.rs`). Another leader is sent what it can't read (ADR-028).
    if let Some(sql) = changes(lake, &stmt).await? {
        let (other, local) = crate::ddl::resolve(lake, &stmt.table()).await?;
        if let (None, Some(seq)) = (&other, &app.seq) {
            let _guard = app.lock.lock().await;
            return crate::change::FILES.scope(files, crate::change::run(lake, seq, &sql, &job)).await;
        }
        if let Some(o) = &other {
            crate::users::across_write(o, &attached_as(&stmt), &local, &stmt).await?; // (that lake's own sign-in)
        }
        let target = other.clone().unwrap_or_else(|| lake.arc());
        let (sql, sent) = crate::change::for_leader(lake, &target, &stmt.table(), &local, &sql, files).await?;
        let Some(o) = other else {
            crate::cluster::signed(lake).await; // (the nodes' key, before a user's change goes to the leader)
            return post_as(&app.cluster.leader.addr, &Request::Change(sql, job, sent), crate::auth::as_user()).await;
        };
        let out = deliver(&o.url, Some(Request::Change(sql, job.clone(), sent)), &job, false).await?;
        seen_there(&o, Some(producer_key(&format!("sql:{job}")))).await;
        return Ok(out);
    }
    // A table of an attached lake: the work runs here, that lake's leader records it.
    let (other, table) = crate::ddl::resolve(lake, &stmt.table()).await?;
    if let Some(other) = other {
        crate::users::across_write(&other, &attached_as(&stmt), &table, &stmt).await?; // (that lake's own sign-in)
        let req = prepare(lake, &other, &table, &stmt, &job, files).await?;
        let mark = match &req {
            Some(Request::Files(f)) => Some(producer_key(&format!("job:{}", f.job))),
            Some(Request::Flush(_)) => Some(producer_key(&format!("sql:{job}"))),
            Some(Request::Table(name, _)) => Some(table_key(name)),
            _ => None,
        };
        let out = deliver(&other.url, req, &job, false).await;
        if out.is_ok() {
            seen_there(&other, mark).await;
        }
        return match &stmt {
            Stmt::Create(c) => raced(c.if_not_exists, &object(&c.name), out),
            _ => out,
        };
    }
    let stmt = stmt.on(table.clone());
    match &stmt {
        Stmt::Create(c) => {
            let spec = made_by_job(create_spec(c, lake, files).await?, &job)?; // (a plain CREATE TABLE is its statement's too)
            return raced(c.if_not_exists, &object(&c.name), define(app, &table, &spec).await);
        }
        Stmt::Define(_, spec) => return define(app, &table, spec).await,
        Stmt::AddColumn(t, ..) | Stmt::SetOptions(t, _) => {
            return match alter_spec(lake, &stmt).await? {
                Some(spec) => define(app, t, &spec).await,
                None => Ok(j!({"table": t, "unchanged": true})),
            };
        }
        _ => {}
    }
    let stored = lake.cat.meta(&table_key(&table)).await?; // (a plain INSERT's routing reads it as stored)
    let meta = stored.as_ref().map(|m| m.logical()); // (rows under SQL's names: the log keeps them stored, `log::pack`)
    // A keyed table's UPDATE or DELETE outside a transaction is a transaction of its own (ADR-067 §7): it locks the keys
    // it changes, so a change worked out from a snapshot can't overwrite a commit that landed meanwhile. Its producer is
    // the job, so a retry of it is a duplicate. A MERGE gets here only when `changes` took it as a one-row upsert of one
    // key (`point_upsert`, which asked the same question): the other MERGEs and the UNIQUE tables never do.
    // A plain INSERT into a table whose key is checked (`constraints::key_checked`) is one too: its key must be new, and its
    // locks make two nodes' INSERTs of one new key wait for each other (`txn::autocommit_as`).
    let keyed = meta.as_ref().is_some_and(|m| !m.key.is_empty());
    let key_insert = matches!(stmt, Stmt::Insert(..)) && stored.as_ref().is_some_and(|m| crate::constraints::key_checked(m));
    if (keyed && matches!(stmt, Stmt::Update(..) | Stmt::Delete(..) | Stmt::Merge(_))) || key_insert {
        return Box::pin(crate::change::FILES.scope(files, crate::txn::autocommit(app, &stmt, &job))).await;
    }
    let sql = match (&meta, &stmt) {
        (Some(m), _) if through_log(m, &stmt) || crate::returning::asked() => {
            // A resend whose job's mark is still kept is answered as the duplicate it is, before its statement runs again (the
            // leader's log would answer the same, once the rows were worked out: `Ack::duplicate`).
            if named && matches!(stmt, Stmt::Insert(..)) && sent(lake, &job).await? {
                return Ok(j!({"duplicate": true}));
            }
            rows_sql(m, &stmt)? // (a RETURNING's rows: worked out here)
        }
        (_, Stmt::Insert(_, query)) => {
            let to = app.to();
            let stamped = stamp(lake, &table, Some(&to)).await?;
            let f = match everywhere(app, &table, query, &job, stamped, &to, files || meta.is_none()).await? {
                Some(f) => f,
                None => write_files(lake, &open(session(lake, query, "").await?), &table, query, &job, stamped).await?,
            };
            let Some(f) = f else { return Ok(j!({"duplicate": true})) };
            return match app.record_files(f).await {
                Err(e) if format!("{e:#}").contains(AGAIN) => {
                    tokio::time::sleep(std::time::Duration::from_millis(200)).await; // (this node sees the view by then: without them)
                    Box::pin(on_node_as(app, Stmt::Insert(table, query.clone()), Some(format!("{job}-again")), files)).await
                }
                other => other,
            };
        }
        (None, _) => bail!("no table {table}"),
        _ => bail!("UPDATE and DELETE need a keyed table (append tables only take INSERTs)"),
    };
    // (shaped once, each jsonb in its own form, so `RETURNING` answers what is held; the log doesn't check the rows again
    // while the table is the one they were shaped for)
    let rules = stored.as_ref().expect("a table").rules()?;
    let how = if matches!(stmt, Stmt::Insert(..)) { crate::rules::How::Insert } else { crate::rules::How::Change };
    let rows = rows_of(lake, lake, &rules, &table, &sql, None, files, how).await?;
    crate::returning::wrote(&[], std::slice::from_ref(rows.rows()));
    let n = rows.rows().num_rows();
    let ack = app.log()?.append_shaped(table, Src { producer: format!("sql:{job}"), seq: 1, prev: None }, rows).await?;
    Ok(if ack.duplicate { j!({"duplicate": true}) } else { j!({"rows": n}) })
}

/// The name a write's table is attached under (`crm` of `crm.customers`, `crm.s.t`).
fn attached_as(stmt: &Stmt) -> String { stmt.table().split('.').next().unwrap_or_default().to_string() }

/// An INSERT's files written by every node at once (`spmd::insert`), when its query's rows split
/// over them as they are; None: from here (`here`: a query that reads this machine's files, or a
/// table not made yet). A spread that fails leaves what it wrote to the orphan sweep (no entry
/// names those files), and this node writes it all.
async fn everywhere(app: &crate::app::App, table: &str, query: &str, job: &str, stamp: Option<crate::log::Reserved>, to: &crate::log::To, here: bool) -> Result<Option<Option<Files>>> {
    let query = crate::sys::planned(query)?;
    if here || app.here_only(&query).await {
        return Ok(crate::spmd::refused("it reads what only this node has"));
    }
    if recorded(&app.lake, job).await? {
        return Ok(Some(None));
    }
    match crate::spmd::insert(&app.lake, &app.cluster.nodes(), &app.cluster.addr, &query, table, stamp.map(|r| (r, to))).await {
        Ok(Some((columns, files))) => Ok(Some(Some(Files { table: table.into(), job: job.into(), columns, files }))),
        Ok(None) => Ok(None),
        Err(e) => {
            eprintln!("INSERT into {table} across the nodes failed, writing it from here: {e:#}");
            Ok(None)
        }
    }
}

/// The identity check every SQL write meets once, however it came (`on_node_listed`, `through_bucket_statement`, and the leader's
/// `change::run`): an ALWAYS identity column is given no value by an INSERT (428C9), and set by no UPDATE or MERGE, unless
/// the value is DEFAULT. What each form gives is what Postgres takes (`change::Merge::written` for a MERGE and the INSERT …
/// ON CONFLICT and OR … forms, `rules::rows::defaulted` for the DEFAULTs in VALUES). A name no lake here holds is Ok: its own path answers.
pub(crate) async fn identity(lake: &Lake, stmt: &Stmt) -> Result<()> {
    use crate::rules::keys::{given, set};
    if !matches!(stmt, Stmt::Insert(..) | Stmt::InsertInto(..) | Stmt::Update(..) | Stmt::Merge(_)) {
        return Ok(());
    }
    let table = stmt.table();
    // (a name not in a lake here, say an outside catalog's table: its own path takes it)
    let Ok((other, name)) = crate::ddl::resolve(lake, &table).await else { return Ok(()) };
    let Some(meta) = other.as_deref().unwrap_or(lake).cat.meta(&table_key(&name)).await? else { return Ok(()) }; // (as parsed once a version: `rules` is the memo)
    if !meta.identity.values().any(|i| i.always) {
        return Ok(());
    }
    let t = meta.rules()?;
    match stmt {
        // (a positional `INSERT INTO t query` and `INSERT INTO t (names) query`, as the core reads each: a value that says DEFAULT
        // is not given, and the first columns the rows have are the ones a positional row gives)
        Stmt::Insert(_, query) => crate::rules::keys::inserted(&t, &table, &[], query),
        Stmt::InsertInto(_, names, query) => crate::rules::keys::inserted(&t, &table, names, query),
        Stmt::Update(_, assigned, _) => set(&t, &assigned.iter().filter(|(_, e)| !crate::rules::rows::is_word(e)).map(|(c, _)| c.clone()).collect::<Vec<_>>()),
        Stmt::Merge(m) => {
            let written = m.written(&t);
            for (columns, said) in written.given {
                given(&t, &table, &columns, said)?;
            }
            set(&t, &written.set)
        }
        _ => Ok(()),
    }
}

/// The statement as the leader's to carry out (`change.rs`), if it is one: an UPDATE or DELETE of
/// an append table, or a MERGE. None for a MERGE that is a one-row upsert of one key this database holds, which the node
/// works out from the key's row (`txn::autocommit`, as `point_upsert` says).
pub(crate) async fn changes(lake: &Lake, stmt: &Stmt) -> Result<Option<String>> {
    let (Stmt::Update(..) | Stmt::Delete(..) | Stmt::Merge(_) | Stmt::Insert(..)) = stmt else { return Ok(None) };
    let (other, table) = match crate::ddl::resolve(lake, &stmt.table()).await {
        Err(_) if matches!(stmt, Stmt::Insert(..)) => return Ok(None), // (an INSERT's own path names what it can't find, or finds an attached lake's)
        r => r?,
    };
    let lake = other.as_deref().unwrap_or(lake);
    let meta = lake.cat.meta(&table_key(&table)).await?;
    if let (Stmt::Merge(m), None, Some(stored)) = (stmt, &other, &meta) {
        if m.point_upsert(&table, stored).is_some() {
            return Ok(None);
        }
    }
    // (a table with an enforced UNIQUE: every write is checked on the leader, `constraints.rs`; a
    // keyed table's plain INSERT too, since its key must be new)
    let checked = meta.as_ref().is_some_and(|m| crate::constraints::checked(m));
    let append = meta.is_some_and(|m| m.key.is_empty());
    Ok(match stmt {
        // (a plain INSERT into a keyed table is no change here: `on_node_listed` sends it to `txn::autocommit`, under its locks)
        Stmt::Insert(..) if !checked => None,
        Stmt::Insert(..) => stmt.change_sql(),
        _ if append || checked || matches!(stmt, Stmt::Merge(_)) => stmt.change_sql(),
        _ => None,
    })
}

// ---------------------------------------------------------------- what the leader records

/// What a writer asks the leader to record: new files, a log flush, or a table.
pub enum Request {
    Files(Files),
    Flush(Bytes), // the body of POST /cluster/commit
    Table(String, String),
    Ddl(crate::ddl::Ddl),
    Change(String, String, crate::change::Sent), // an UPDATE, DELETE or MERGE the leader carries out (`change.rs`): SQL, job, the rows it reads that the leader can't
    Iceberg(Vec<crate::iceberg::Commit>), // another engine's commit (ADR-028), or its transaction over several tables
    Txn(crate::txn::Commit),              // a transaction's writes, one commit (ADR-036 §5)
}

impl Request {
    /// Where it goes over HTTP, and its body; its name and body in the bucket inbox.
    pub fn http(&self) -> Result<(String, Vec<u8>)> {
        Ok(match self {
            Request::Files(f) => ("/cluster/files".into(), serde_json::to_vec(f)?),
            Request::Flush(b) => ("/cluster/commit".into(), b.to_vec()),
            Request::Table(name, spec) => (format!("/tables/{name}"), spec.clone().into_bytes()),
            Request::Ddl(d) => ("/cluster/ddl".into(), serde_json::to_vec(d)?),
            Request::Change(sql, job, sent) => ("/cluster/change".into(), serde_json::to_vec(&(sql, job, sent))?),
            Request::Iceberg(c) => ("/cluster/iceberg".into(), serde_json::to_vec(c)?),
            Request::Txn(c) => ("/cluster/txn".into(), serde_json::to_vec(c)?),
        })
    }

    pub fn inbox(&self) -> Result<(String, Vec<u8>)> {
        Ok(match self {
            Request::Table(name, spec) => ("table".into(), serde_json::to_vec(&(name, spec))?),
            Request::Files(_) => ("files".into(), self.http()?.1),
            Request::Flush(_) => ("flush".into(), self.http()?.1),
            Request::Ddl(_) => ("ddl".into(), self.http()?.1),
            Request::Change(..) => ("change".into(), self.http()?.1),
            Request::Iceberg(_) => ("iceberg".into(), self.http()?.1),
            Request::Txn(_) => ("txn".into(), self.http()?.1),
        })
    }

    pub fn from_inbox(kind: &str, body: Bytes) -> Result<Request> {
        Ok(match kind {
            "files" => Request::Files(serde_json::from_slice(&body)?),
            "flush" => Request::Flush(body),
            "iceberg" => Request::Iceberg(serde_json::from_slice(&body)?),
            "ddl" => Request::Ddl(serde_json::from_slice(&body)?),
            "txn" => Request::Txn(serde_json::from_slice(&body)?),
            "change" => {
                let (sql, job, sent): (String, String, crate::change::Sent) = serde_json::from_slice(&body)?;
                Request::Change(sql, job, sent)
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
    if crate::backup::frozen(lake) && !crate::backup::allowed(&req) {
        return Err(crate::backup::refused(Some(&lake.url))); // (a backup takes its runs and its clones' pins: ADR-070)
    }
    match req {
        Request::Files(f) => {
            let _guard = lock.lock().await;
            record(lake, f, seq).await
        }
        Request::Flush(body) => Ok(serde_json::to_value(seq.submit(decode_flush(body)?).await?)?),
        Request::Table(name, spec) => {
            let _guard = lock.lock().await;
            create_table(lake, &name, &spec).await
        }
        Request::Ddl(d) => ddl_here(lake, seq, lock, d).await,
        Request::Change(sql, job, sent) => {
            let _guard = lock.lock().await;
            crate::query::SENT.scope(Arc::new(crate::change::unpack(&sent)?), crate::change::run(lake, seq, &sql, &job)).await
        }
        Request::Txn(c) => crate::txn::commit_here(lake, seq, lock, c).await, // (it takes the lock)
        Request::Iceberg(c) => {
            let _guard = lock.lock().await;
            crate::iceberg::record(lake, seq, c, &[String::new()], "", 60_000).await // (a leader answering through the bucket: --retain-secs' default)
        }
    }
}

// ---------------------------------------------------------------- from any machine

/// `pondra lead`: lead the lake at `dir` once, making it if it's new, for what waits in its inbox
/// (or nothing, when someone leads it already: they answer the bell). No reader of the lake is
/// opened and no statement run, so a node's CREATE DATABASE, a branch's first commit and a write
/// to a lake nobody leads pay for one lead, not two and four catalogs opened.
pub async fn lead_only(dir: &str) -> Result<()> {
    deliver(dir, None, "", true).await.map(drop)
}

/// `pondra sql`'s write to a lake whose leader is alive but doesn't answer from the guest's start (`cli.rs`'s `through_inbox`):
/// the statement is checked here as a node's door checks it and prepared as the leader takes it, then each request is sent as
/// a one-off writer sends it (`deliver`): over HTTP if this machine reaches the leader after all, else through the bucket's
/// inbox, or led here when nobody leads. Each request sets its lake's node key first (`through_bucket_one`).
pub(crate) async fn through_bucket_statement(lake: &Arc<Lake>, stmt: Stmt) -> Result<Value> {
    if let Stmt::Invalid(why) = stmt {
        bail!(why); // (said as a node says it)
    }
    let stmt = crate::protect::door(lake, stmt).await?; // (a node's door too: this path never reaches `on_node_listed`)
    identity(lake, &stmt).await?; // (as a node's: every SQL write's identity, here before `whole_rows` rewrites an INSERT INTO t (names))
    if let Some((query, to, options)) = crate::ext::view_write(lake, &stmt).await? {
        let out = crate::copy::copy_to(lake, &query, &to, &options, &[], "").await?; // (a view of a folder: a new file in it)
        return Ok(j!({"rows": out["copied"]}));
    }
    let stmt = match stmt {
        Stmt::InsertInto(t, names, query) => Stmt::Insert(t.clone(), whole_rows(lake, &t, &names, &query).await?),
        Stmt::Insert(t, query) => {
            if !is_there(lake, &t).await? {
                return Err(no_table(&t));
            }
            Stmt::Insert(t, query)
        }
        s => s,
    };
    if let Stmt::Create(c) = &stmt {
        if matches!(there(lake, c).await?, Some(true)) {
            return Ok(j!({"table": object(&c.name), "exists": true})); // (CREATE TABLE IF NOT EXISTS of one that's there)
        }
    }
    match stmt {
        Stmt::CopyTo(query, to, options) => {
            let lake = lake.clone();
            crate::ext::scope(true, async move { crate::copy::copy_to(&lake, &query, &to, &options, &[], "").await }).await // (its user's own machine)
        }
        // CREATE TABLE … AS SELECT, and OR REPLACE: the steps `create_as` takes, each one sent through the bucket.
        Stmt::Create(c) if c.query.is_some() || c.or_replace => {
            let owned = lake.clone();
            let run = move |s: Stmt| -> futures::future::BoxFuture<'static, Result<Value>> {
                let owned = owned.clone();
                Box::pin(async move { through_bucket_one(&owned, s).await })
            };
            create_as(run, lake, &c, job(), true, &mut |_: &str| {}).await
        }
        Stmt::Create(c) => {
            let (if_not_exists, name) = (c.if_not_exists, object(&c.name));
            raced(if_not_exists, &name, through_bucket_one(lake, Stmt::Create(c)).await) // (a plain CREATE TABLE: its definition carries the job)
        }
        Stmt::Ddl(ddls) => {
            let mut out = j!({});
            for d in ddls {
                out = through_bucket_one(lake, Stmt::Ddl(vec![d])).await?;
            }
            Ok(out)
        }
        stmt => through_bucket_one(lake, stmt).await,
    }
}

/// One request of a statement, sent as `through_bucket_statement` describes: to the lake it writes (an attached lake's leader,
/// if the name is one), under this process's job, so a retry of it is answered as a duplicate. The node key set is the target
/// lake's (each lake has its own), before its leader is called (`cluster::signed`: a key set already is kept).
async fn through_bucket_one(lake: &Arc<Lake>, stmt: Stmt) -> Result<Value> {
    let (other, table) = crate::ddl::resolve(lake, &stmt.table()).await?;
    let (stmt, target) = (stmt.on(table.clone()), other.unwrap_or_else(|| lake.clone())); // (a write to an attached lake goes to its leader)
    let job = job().to_string();
    crate::cluster::signed(&target).await;
    match prepare(lake, &target, &table, &stmt, &job, true).await? {
        Some(req) => deliver(&target.url, Some(req), &job, true).await,
        None => Ok(j!({"duplicate": true})), // (a retry: the write it asks is recorded already)
    }
}

tokio::task_local! {
    /// A key a request to another lake's leader goes with instead of this process's token: a
    /// branch's key in its base (`branch::keyed`).
    static BEARER: String;
    /// Set while a request goes through the bucket alone (`through_bucket`): it is not sent over HTTP.
    static BUCKET: ();
}

/// `send`, with `key` as its `Authorization` when given.
pub async fn send_as(dir: &str, req: Request, key: Option<&str>) -> Result<Value> {
    match key {
        Some(k) => BEARER.scope(k.to_string(), send(dir, req)).await,
        None => send(dir, req).await,
    }
}

/// Have the leader of the lake at `dir` record `req` (made on this node), however it's reached.
pub async fn send(dir: &str, req: Request) -> Result<Value> { deliver(dir, Some(req), "", false).await }

/// Have the leader of the lake at `dir` record `req`, through the bucket alone: its inbox, or this
/// process leading for a moment when nobody does. For a caller with no token (`pondra token`, which
/// the leader would refuse over HTTP): whoever can write the bucket owns the lake (invariant 18).
pub async fn through_bucket(dir: &str, req: Request) -> Result<Value> {
    BUCKET.scope((), deliver(dir, Some(req), job(), true)).await
}

/// Have the leader of the lake at `dir` record a write: over HTTP, through the bucket inbox if it
/// can't be reached, or, when nobody leads, by leading for a moment: here (`one_off`: a `pondra
/// sql` process, which ends right after), or, from a node, in a `pondra sql` of its own. `req` None is
/// nothing to record (`lead_only`): the lake is led for the moment, and its inbox answered.
async fn deliver(dir: &str, req: Option<Request>, job: &str, one_off: bool) -> Result<Value> {
    if let Some(r) = crate::store::reach_of(dir).filter(|r| r.read_only) {
        return across(dir, &r, req).await; // (a lake on another server: ADR-058)
    }
    let store = open_store(dir)?.1;
    loop {
        match latest(&store).await? {
            Some(t) if alive(&store, &t).await => {
                if t.addr.is_empty() {
                    // (another `pondra sql` is recording: wait. One elsewhere that was killed doesn't mark itself when rung)
                    if !crate::terms::deaf(&store, &t).await {
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                    continue;
                }
                let Some(r) = &req else {
                    // (the lake is led, so its inbox is answered: unless its leader elsewhere doesn't mark itself when rung)
                    if crate::terms::deaf(&store, &t).await {
                        continue;
                    }
                    return Ok(j!({"duplicate": true}));
                };
                // (test hook PONDRA_NO_DIRECT: act as if the leader were out of reach; `through_bucket` never goes over HTTP)
                let direct = !crate::env::is_set("PONDRA_NO_DIRECT") && BUCKET.try_with(|_| ()).is_err();
                match if direct { Some(post(&t.addr, r).await) } else { None } {
                    Some(Ok(v)) => return summary(matches!(r, Request::Flush(_)), v),
                    Some(Err(e)) if !unreachable(&e) => return Err(e),
                    _ => {} // this machine can't reach the leader: through the bucket
                }
                if let Some(v) = crate::inbox::send(&store, r, None, Some(&t)).await? {
                    return summary(matches!(r, Request::Flush(_)), v);
                }
            }
            _ if !one_off => {
                let Some(r) = &req else { return Ok(j!({"duplicate": true})) };
                if let Some(v) = crate::inbox::send(&store, r, Some(dir), None).await? {
                    return summary(matches!(r, Request::Flush(_)), v);
                }
            }
            t => {
                let Some(term) = claim(&store, t.map_or(1, |t| t.n + 1), "").await? else { continue };
                return match lead(dir, &store, term.n, req).await {
                    // Another one-off writer took the catalog while this one was leading (both
                    // found the lake idle). Start over with the same job: whoever wins records it
                    // once, and the rest go through them.
                    Err(e) if fenced(&e) && tries() < 5 => crate::cluster::restart_with("fenced while writing", &[("PONDRA_JOB", job.to_string()), ("PONDRA_TRIES", (tries() + 1).to_string())]),
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
    if crate::backup::frozen(target) && !crate::backup::reads_only(stmt) {
        return Err(crate::backup::refused(Some(&target.url))); // (before a file is written into it: ADR-070)
    }
    let open = |ctx: SessionContext| if files { ctx.enable_url_table() } else { ctx };
    match stmt {
        Stmt::Create(c) => return Ok(Some(Request::Table(table.into(), made_by_job(create_spec(c, query, files).await?, job)?))),
        Stmt::Define(_, spec) => return Ok(Some(Request::Table(table.into(), spec.clone()))),
        Stmt::Ddl(d) => return Ok(Some(Request::Ddl(d.first().cloned().ok_or_else(|| anyhow::anyhow!("nothing to do"))?))),
        _ => {}
    }
    if let Stmt::AddColumn(..) | Stmt::SetOptions(..) = stmt {
        return Ok(alter_spec(target, stmt).await?.map(|spec| Request::Table(table.into(), spec)));
    }
    if let Some(sql) = changes(target, stmt).await? {
        let t = stmt.table();
        let (sql, sent) = crate::change::for_leader(query, target, &t, &t, &sql, files).await?; // (files here: this machine's)
        return Ok(Some(Request::Change(sql, job.into(), sent)));
    }
    let stored = target.cat.meta(&table_key(table)).await?; // (as parsed once a version: `rules` is the memo)
    let meta = stored.as_ref().map(|m| m.logical());
    let log = meta.as_ref().is_some_and(|m| through_log(m, stmt));
    match (meta, stmt) {
        (Some(m), _) if log => {
            let sql = rows_sql(&m, stmt)?;
            let rules = stored.as_ref().expect("a table").rules()?;
            // (an INSERT's keys are looked at below, after `sent`, as before: `Load`)
            let how = if matches!(stmt, Stmt::Insert(..)) { crate::rules::How::Load } else { crate::rules::How::Change };
            let rows = rows_of(query, target, &rules, table, &sql, None, files, how).await?; // (a DEFAULT's sequence is the attached lake's)
            if let Some(s) = stored.as_ref().filter(|s| matches!(stmt, Stmt::Insert(..)) && crate::constraints::key_checked(s)) {
                if sent(target, job).await? {
                    return Ok(None); // (a retry: `deliver` answers it as a duplicate)
                }
                // (no row lock here, unlike a node's own plain INSERT (`txn::autocommit_as`): `pondra sql` and an attached
                // lake's INSERT write without one, so two writers of one new key can both pass this check. A documented race.)
                crate::constraints::key_check(target, table, s, None, rows.rows(), false).await?; // (the keys are new: checked here, where the rows are)
            }
            let (ack, _) = tokio::sync::oneshot::channel();
            let append = Append { table: table.into(), src: Src { producer: format!("sql:{job}"), seq: 1, prev: None }, batch: rows.into_rows(), ack };
            Ok(Some(Request::Flush(encode_flush(&pack(target, &[append]).await?)?.into())))
        }
        (_, Stmt::Insert(_, sql)) => {
            let ctx = open(session(query, sql, "").await?);
            Ok(write_files(target, &ctx, table, sql, job, stamp(target, table, None).await?).await?.map(Request::Files))
        }
        (None, _) => bail!("no table {table}"),
        _ => bail!("UPDATE and DELETE need a keyed table (append tables only take INSERTs)"),
    }
}

/// Row ids for a bulk INSERT from any machine: a block from the lake's leader, if it can be
/// reached, so the files are written with their system columns (`sys.rs`); None: the leader
/// records them with their lineage instead (the inbox, or no leader yet).
pub async fn reserve(lake: &Lake) -> Option<crate::log::Reserved> {
    let t = latest(&lake.store).await.ok()??;
    if t.addr.is_empty() || !alive(&lake.store, &t).await || crate::env::is_set("PONDRA_NO_DIRECT") {
        return None;
    }
    crate::log::To::Leader(t.addr).reserve().await.ok()
}

/// Send a request to the leader over HTTP.
/// A DDL statement on the leader, under the lake's lock; REFRESH takes it itself, once it has read
/// its base (`branch::refresh`).
pub async fn ddl_here(lake: &Lake, seq: &crate::log::Sequencer, lock: &Mutex<()>, d: crate::ddl::Ddl) -> Result<Value> {
    match d {
        crate::ddl::Ddl::Refresh { database, tables } => crate::branch::refresh(lake, seq, lock, database, tables).await,
        crate::ddl::Ddl::SetLocation { database, to } => crate::memory::set_location(lake, &database, &to).await, // (copies outside the lake's lock: `memory.rs`)
        d @ (crate::ddl::Ddl::BackupDatabase { .. } | crate::ddl::Ddl::RestoreDatabase { .. }) => crate::backup::statement(lake, lock, d).await,
        d => {
            let _guard = lock.lock().await;
            crate::ddl::apply(lake, d).await
        }
    }
}

/// The refusal for a write to a lake attached here read-only, with a key that only reads it (`across`, and a guest
/// that writes: `Node::start`).
pub(crate) fn read_only_refused(dir: &str) -> anyhow::Error {
    anyhow::anyhow!("{dir} is attached here read-only, with a key that only reads it: write it through its own server")
}

/// A lake on another server, read here with a read-only key (ADR-058): nothing here writes its
/// bucket (no inbox, no term claimed), and only a branch's pin goes to its leader, at its
/// ENDPOINT, with the pondra secret's token or the branch's own key.
async fn across(dir: &str, r: &crate::store::Reach, req: Option<Request>) -> Result<Value> {
    let Some(req @ Request::Ddl(crate::ddl::Ddl::Pin { .. } | crate::ddl::Ddl::Unpin { .. })) = req else {
        return Err(read_only_refused(dir));
    };
    // (never sent without one: this server's own key, or its user's token, must not go to another
    // server; only a branch's own `pb_` key is meant for its base's leader)
    let Some(token) = BEARER.try_with(|k| k.clone()).ok().filter(|k| k.starts_with("pb_")).or_else(|| r.token.clone()) else {
        bail!("{dir}'s leader is asked with a token: CREATE SECRET … (TYPE pondra, TOKEN '…', SCOPE '<its endpoint>')");
    };
    let at = match &r.endpoint {
        Some(e) => e.trim_end_matches('/').to_string(),
        None => {
            let store = open_store(dir)?.1;
            match latest(&store).await? {
                Some(t) if !t.addr.is_empty() && alive(&store, &t).await => crate::tls::url(&t.addr),
                _ => bail!("nobody leads {dir}"),
            }
        }
    };
    let (path, body) = req.http()?;
    let res = http().post(format!("{at}{path}")).header("content-type", "application/json").bearer_auth(token).body(body).send().await
        .with_context(|| format!("{dir}'s leader doesn't answer at {at}: a branch is pinned while its base's server is up"))?;
    if !res.status().is_success() {
        bail!("{dir}'s leader at {at}: {}", res.text().await?);
    }
    Ok(res.json().await?)
}

pub async fn post(addr: &str, r: &Request) -> Result<Value> { post_as(addr, r, None).await }

/// `post`, naming the user a change is for to this lake's own leader, which runs it as them
/// (`auth::AS_USER`). Never to another lake's: it has users of its own.
async fn post_as(addr: &str, r: &Request, user: Option<String>) -> Result<Value> {
    let (path, body) = r.http()?;
    let call = http().post(crate::tls::url(&format!("{addr}{path}"))).header("content-type", "application/json").body(body);
    let call = match BEARER.try_with(|k| k.clone()) {
        Ok(k) => call.bearer_auth(k), // (a branch's key, sent as this request's own Authorization)
        Err(_) => call,
    };
    let call = match user {
        Some(user) => call.header(crate::auth::AS_USER, user),
        None => call,
    };
    let res = call.send().await?;
    if res.status() == reqwest::StatusCode::UNAUTHORIZED {
        bail!("the leader at {addr} asks for a sign-in: set PONDRA_TOKEN to one of the lake's tokens ({})", res.text().await?);
    }
    if !res.status().is_success() {
        // (the leader's refusal keeps its SQLSTATE: a follower's client tells it apart as the leader's would)
        let sqlstate = res.headers().get("x-pondra-sqlstate").cloned();
        return Err(refusal(sqlstate.as_ref(), format!("the leader at {addr}: {}", res.text().await?)));
    }
    Ok(res.json().await?)
}

/// The error a node's answer that isn't a success is, as a client takes it: the answer's words, with the SQLSTATE its
/// `x-pondra-sqlstate` header names (none: the words alone). Shared by a follower's forwarded write (`post_as`) and a
/// guest's answer (cli.rs), so both give the same error.
pub(crate) fn refusal(sqlstate: Option<&http::HeaderValue>, said: String) -> anyhow::Error {
    match sqlstate.and_then(|v| v.to_str().ok()).and_then(crate::codes::received) {
        Some(code) => crate::codes::coded(code, said),
        None => anyhow::anyhow!("{said}"),
    }
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
        Outcome::Retry(r) if r.is_empty() => bail!("the table changed as this was sent (a materialized view of it made or dropped, or the table renamed, swapped or dropped): run it again (it goes in once)"),
        Outcome::Retry(r) => r.into_iter().map(|(_, a)| a).collect(),
        Outcome::Refused(why) => bail!(why),
    };
    ensure!(!acks.iter().any(|a| a.refused), "the table has a UNIQUE constraint: it takes rows through SQL's INSERT, UPDATE and MERGE, which check them on the leader");
    ensure!(!acks.iter().any(|a| a.gone), "the table isn't there any more (renamed or dropped as this was sent)");
    Ok(if acks.iter().all(|a| a.duplicate) { j!({"duplicate": true}) } else { j!({"committed": true}) })
}

/// Was this writer fenced out of the catalog by a newer one? (`pondra sql`'s guest asks it of an answer's words too.)
pub(crate) fn fenced(e: &anyhow::Error) -> bool {
    e.downcast_ref::<crate::kv::Error>().is_some_and(|e| e.closed()) || format!("{e:#}").contains("newer DB client")
}

/// How many times this command has already started over (it runs itself again, keeping its job).
pub(crate) fn tries() -> u32 {
    crate::env::or("PONDRA_TRIES", 0)
}

/// This `pondra sql`'s job, the same for every statement it records: a run started over takes the
/// job of the run before (`PONDRA_JOB`), so whoever wins the race records it once.
pub(crate) fn job() -> &'static str {
    static JOB: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    JOB.get_or_init(|| crate::env::get("PONDRA_JOB").unwrap_or_else(crate::log::auto_job))
}

/// Nobody leads: record the write under our own term, and whatever waits in the inbox, then let go. `req` None records
/// nothing (`lead_only`).
async fn lead(dir: &str, store: &Store, term: u64, req: Option<Request>) -> Result<Value> {
    let marks = crate::terms::mark_every(store.clone(), term);
    let lake = Lake::open(dir, true, false).await?; // (the catalog's writer: fences any older one)
    crate::replica::recover(&lake, "", term, None).await?;
    let (seq, lock) = (Sequencer::start(lake.clone(), None).await?, Mutex::new(()));
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

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Float64Array;
    use datafusion::arrow::datatypes::{Field, Schema};

    #[tokio::test]
    async fn a_list_naming_the_marker_casts_its_floats_as_plain_values_do() {
        let target: SchemaRef = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, true), Field::new("x", DataType::Float64, true), Field::new("_deleted", DataType::Boolean, true)]));
        let ctx = SessionContext::new();
        // (a plain VALUES gives its values to the columns by position, the marker left out: two values)
        let plain = planned(&ctx, &target, "VALUES (1, 1e20), (2, 1e-20)").await.expect("plain").expect("rows");
        let named = planned(&ctx, &target, "SELECT * FROM (VALUES (1, 1e20, false), (2, 1e-20, false)) AS __pondra_v(\"k\", \"x\", \"_deleted\")").await.expect("named").expect("rows");
        let floats = |b: &RecordBatch| b.column(1).as_any().downcast_ref::<Float64Array>().expect("floats").values().to_vec();
        assert_eq!(floats(&fit(&target, &plain).expect("fits")), [1e20, 1e-20]);
        assert_eq!(floats(&fit(&target, &named).expect("fits")), [1e20, 1e-20]);
    }

    #[tokio::test]
    async fn rows_of_shapes_its_rows_once() -> Result<()> {
        use crate::rules::How;
        use datafusion::arrow::array::StringArray;
        async fn of(lake: &Lake, t: &Arc<crate::rules::Table>, sql: &str, how: How) -> Result<crate::rules::Shaped> {
            rows_of(lake, lake, t, "t", sql, None, false, how).await
        }
        fn text(b: &RecordBatch, column: &str) -> String {
            b.column_by_name(column).and_then(|c| c.as_any().downcast_ref::<StringArray>().map(|s| s.value(0).to_string())).expect("a text value")
        }
        let dir = std::env::temp_dir().join(format!("pondra-shaped-{}", uuid::Uuid::new_v4()));
        let lake = Lake::open(dir.to_str().unwrap(), true, false).await?;
        let meta = TableMeta {
            columns: vec![("id".into(), "Int64".into()), ("n".into(), "Int32".into()), ("j".into(), "Utf8".into()), ("d".into(), "Utf8".into())],
            key: vec!["id".into()],
            not_null: vec!["id".into(), "n".into()],
            checks: vec![("t_n_check".into(), "n >= 0".into())],
            defaults: [("d".to_string(), "'x'".to_string())].into(),
            sql_types: [("j".to_string(), "jsonb".to_string())].into(),
            ids: true,
            ..Default::default()
        };
        let t = Arc::new(meta.to_rules()?);
        let one = of(&lake, &t, r#"VALUES (1, 5, '{"b":1,"a":2}')"#, How::Insert).await?;
        assert_eq!(one.rows().num_rows(), 1);
        assert!(Arc::ptr_eq(one.table(), &t));
        assert_eq!(text(one.rows(), "d"), "x");
        assert_eq!(text(one.rows(), "j"), r#"{"a": 2, "b": 1}"#);
        let err = of(&lake, &t, "VALUES (1, NULL, NULL)", How::Insert).await.err().expect("a NULL in NOT NULL");
        assert_eq!(crate::codes::of(&err), "23502");
        assert!(crate::codes::words(&err).contains("t.n is NOT NULL"), "{}", crate::codes::words(&err));
        let err = of(&lake, &t, "VALUES (1, -1, NULL)", How::Insert).await.err().expect("a CHECK");
        assert_eq!(crate::codes::of(&err), "23514");
        assert!(crate::codes::words(&err).contains(r#"violates check constraint "t_n_check""#), "{}", crate::codes::words(&err));
        let twice = "VALUES (1, 1, NULL), (1, 2, NULL)";
        let err = of(&lake, &t, twice, How::Insert).await.err().expect("a key twice");
        assert_eq!(crate::codes::of(&err), "23505");
        assert!(crate::codes::words(&err).contains(r#"duplicate key value violates unique constraint "t_pkey": Key (id)=(1) already exists"#), "{}", crate::codes::words(&err));
        assert_eq!(of(&lake, &t, twice, How::Load).await?.rows().num_rows(), 2);
        assert_eq!(of(&lake, &t, twice, How::Change).await?.rows().num_rows(), 2);
        let none = of(&lake, &t, "SELECT 1 AS a, 1 AS b, NULL AS c WHERE false", How::Insert).await?; // (aliased: DataFusion names two 1s alike)
        assert_eq!(none.rows().num_rows(), 0);
        assert_eq!(none.rows().schema(), t.schema);
        // (the same by a query's own session: `rows` shapes what `planned` gives, for the table as it is)
        let ctx = SessionContext::new();
        let shaped = rows(&lake, &ctx, &t, "t", "SELECT 1, 5, NULL", How::Insert).await?;
        assert_eq!(shaped.rows().num_rows(), 1);
        assert!(Arc::ptr_eq(shaped.table(), &t));
        assert_eq!(text(shaped.rows(), "d"), "x");
        assert_eq!(rows(&lake, &ctx, &t, "t", "SELECT 1, 5, NULL WHERE false", How::Insert).await?.rows().num_rows(), 0);
        lake.cat.close().await?;
        std::fs::remove_dir_all(&dir)?;
        Ok(())
    }
}
