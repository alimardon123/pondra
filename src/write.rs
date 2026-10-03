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
use anyhow::{bail, ensure, Context, Result};
use bytes::Bytes;
use datafusion::arrow::compute::concat_batches;
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
    },
}

/// Leader: create a table (sent again for an existing one, `publish` changes). The caller holds
/// the lock that serialises table rewrites.
pub async fn create_table(lake: &Lake, name: &str, spec: &str) -> Result<Value> {
    let name = &crate::ddl::new_name(lake, name).await?; // (its schema exists; `public.t` is `t`)
    ensure!(lake.cat.get::<crate::ddl::StoredView>(&crate::ddl::query_key(name)).await?.is_none(), "{name} is a view");
    let (columns, key, merge, publish, cluster, ttl, partition, order, not_null, defaults, properties, checks, retention) = match serde_json::from_str(spec)? {
        TableSpec::Columns(c) => (c, vec![], BTreeMap::new(), None, None, None, None, None, vec![], BTreeMap::new(), None, vec![], None),
        TableSpec::Full { columns, key, merge, publish, cluster_by, ttl, partition_by, order_by, not_null, defaults, properties, checks, retention } => (columns, key, merge, publish, cluster_by, ttl, partition_by, order_by, not_null, defaults, properties, checks, retention),
    };
    let retention = retention.as_deref().map(crate::ddl::retention).transpose()?;
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
    ensure!(merge.keys().all(|c| columns.iter().any(|(n, _)| n == c)), "merge: columns of the table");
    if let Some((c, _)) = columns.iter().find(|(c, _)| !merge.is_empty() && !key.contains(c) && !merge.contains_key(c)) {
        bail!("a merge table combines every column but its key: {c} needs a merge function too (merge = '…, {c}:sum|count|min|max')");
    }
    ensure!(publish.iter().flatten().all(|f| ["delta", "iceberg"].contains(&f.as_str())), "publish formats: delta, iceberg");
    // (A keyed table takes them too: each tiering round's newest rows go into a file per
    // partition, sorted by cluster_by, then the key; reads let a newer round's key shadow an older
    // round's wherever its partition, `query::upsert_view`.)
    ensure!(cluster.iter().flatten().all(|c| columns.iter().any(|(n, _)| n == c)), "cluster_by: columns of the table");
    ensure!(order.is_none() || (!key.is_empty() && merge.is_empty()), "order_by: a keyed table's (PRIMARY KEY), whose rows replace each other");
    ensure!(order.iter().all(|o| columns.iter().any(|(n, _)| n == o) && !key.contains(o)), "order_by: a column of the table, not its key");
    if let Some(p) = &partition {
        crate::tier::check_partition(p, &columns)?;
    }
    ensure!(not_null.iter().chain(defaults.keys()).all(|c| columns.iter().any(|(n, _)| n == c)), "NOT NULL and DEFAULT: columns of the table");
    for (c, expr) in &defaults {
        let t = &columns.iter().find(|(n, _)| n == c).expect("a column").1;
        crate::defaults::validate(c, t, expr).await?;
    }
    let empty = RecordBatch::new_empty(schema(&columns)?);
    for (n, c) in &checks {
        crate::defaults::breaking(&empty, c).with_context(|| format!("CONSTRAINT {n} CHECK ({c})"))?; // (a condition over its columns)
    }
    // (a key's columns are NOT NULL too: a row with no key has nothing to be found by)
    let not_null: Vec<String> = columns.iter().map(|(c, _)| c).filter(|c| not_null.contains(c) || key.contains(c)).cloned().collect();
    let meta = match lake.cat.get::<TableMeta>(&table_key(name)).await? {
        // (A new table reads the log from now on: a table of this name dropped earlier left rows
        // in segments that aren't expired yet.)
        None => {
            let folder = crate::ddl::free_folder(lake, name).await?; // (a renamed table may still have this name's folder)
            TableMeta { columns, key, merge, publish: publish.unwrap_or_else(default_publish), cluster: cluster.unwrap_or_default(), ttl, partition, order, not_null, defaults, checks, folder, tiered: lake.visible(), ids: true, properties: properties.unwrap_or_default(), retention_secs: retention, ..Default::default() }
        }
        Some(mut m) => {
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
            let (recluster, rettl, reorder) = (cluster.as_ref().is_some_and(|c| *c != l.cluster), ttl.is_some() && ttl != l.ttl, order.is_some() && order != l.order);
            let reprop = properties.as_ref().is_some_and(|p| *p != m.properties);
            let reretain = retention.is_some() && retention != m.retention_secs;
            if new <= old && !republish && !recluster && !rettl && !reorder && !reprop && !reretain {
                return Ok(j!({"table": name, "publish": m.publish}));
            }
            if new > old {
                if let Some(c) = columns[old..].iter().map(|(c, _)| c).find(|c| not_null.contains(c) && !key.contains(c) || defaults.contains_key(*c)) {
                    bail!("{name}.{c}: a column added to a table can't be NOT NULL or have a DEFAULT, since the rows already there have no value for it (add it, then UPDATE {name} SET {c} = …)");
                }
                // A new column is stored under its name, or, if an older column (renamed or
                // dropped) is stored under that, `name~2`, `name~3`…: files are never rewritten.
                for (c, t) in &columns[old..] {
                    let taken = |s: &str| m.columns.iter().any(|(n, _)| n == s);
                    let s = (1..).map(|i| if i == 1 { c.clone() } else { format!("{c}~{i}") }).find(|s| !taken(s)).expect("a free name");
                    if s != *c {
                        m.names.insert(s.clone(), c.clone());
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
    lake.cat.commit(vec![(table_key(name), json(&meta))], &[]).await?;
    if !meta.publish.is_empty() {
        crate::delta::publish_all(lake).await?; // (an empty table is there for other engines at once: they may append to it, ADR-028)
    }
    Ok(j!({"table": name, "publish": meta.publish}))
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
    TempView(String, String, bool, bool),                  // CREATE [OR REPLACE] TEMP VIEW [IF NOT EXISTS] name AS query: the session's (`temp.rs`)
    TempSecret(String, std::collections::BTreeMap<String, String>, bool, bool), // CREATE [OR REPLACE] TEMPORARY SECRET [IF NOT EXISTS] name (…): the session's, in memory (`temp.rs`)
}

impl Stmt {
    /// The table it writes.
    pub fn table(&self) -> String {
        match self {
            Stmt::Create(c) => object(&c.name),
            Stmt::Define(t, _) | Stmt::Insert(t, _) | Stmt::InsertInto(t, ..) | Stmt::Update(t, ..) | Stmt::Delete(t, _) | Stmt::AddColumn(t, ..) | Stmt::SetOptions(t, _) => t.clone(),
            Stmt::Ddl(_) | Stmt::Invalid(_) | Stmt::CopyTo(..) => String::new(),
            Stmt::TempView(v, ..) | Stmt::TempSecret(v, ..) => v.clone(),
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

/// An option's value as text: a quoted string as it says (`'op = ''D'''` is `op = 'D'`), anything
/// else as written.
fn option_text(v: &ast::Expr) -> String {
    match v {
        ast::Expr::Value(ast::ValueWithSpan { value: ast::Value::SingleQuotedString(s) | ast::Value::DoubleQuotedString(s), .. }) => s.clone(),
        other => other.to_string().trim_matches('\'').to_string(),
    }
}

/// A name as SQL resolves it: its parts, unquoted ones in lower case, joined by dots.
pub fn object(n: &ast::ObjectName) -> String {
    let part = |p: &ast::ObjectNamePart| p.as_ident().map(ident).unwrap_or_else(|| p.to_string());
    n.0.iter().map(part).collect::<Vec<_>>().join(".")
}

/// One part of a name: as written if quoted, else in lower case.
pub fn ident(i: &ast::Ident) -> String { if i.quote_style.is_some() { i.value.clone() } else { i.value.to_lowercase() } }

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
    // OR REPLACE where replacing one would lose what it holds: refused, saying why.
    static NO_REPLACE: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| regex::Regex::new(r"(?is)^\s*CREATE\s+OR\s+REPLACE\s+(SCHEMA|DATABASE|USER|ROLE)\b").expect("a regex"));
    if let Some(c) = NO_REPLACE.captures(first_word(sql)) {
        let kind = c[1].to_uppercase();
        let lost = if matches!(kind.as_str(), "USER" | "ROLE") { "the rights given to it" } else { "everything in it" };
        return Some(Stmt::Invalid(format!("CREATE OR REPLACE {kind}: replacing one would drop {lost}; CREATE {kind} IF NOT EXISTS leaves one that is there as it is")));
    }
    if let Some(s) = crate::shares::statement(sql) {
        return Some(s); // (CREATE SHARE and RECIPIENT, GRANT SELECT ON SHARE: `shares.rs`, before users' GRANT)
    }
    if let Some(s) = crate::users::statement(sql) {
        return Some(s); // (CREATE USER and ROLE, GRANT, REVOKE, CREATE TOKEN: `users.rs`)
    }
    if let Some(s) = crate::ext::statement(sql) {
        return Some(s); // (CREATE SECRET: values of any kind; DROP SECRET)
    }
    if let Some(s) = crate::routines::statement(sql) {
        return Some(s); // (CREATE FUNCTION and PROCEDURE as Postgres writes them, CREATE TASK, DROP TASK, DROP MACRO)
    }
    // `ALTER VIEW v RENAME TO w` (dbt's): the parser takes only ALTER VIEW … AS.
    static VIEW_RENAME: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
        regex::Regex::new(r#"(?is)^\s*ALTER\s+(MATERIALIZED\s+)?VIEW\s+(IF\s+EXISTS\s+)?([\w."-]+)\s+RENAME\s+TO\s+([\w."-]+)\s*;?\s*$"#).expect("a regex")
    });
    // DataFusion's CREATE EXTERNAL TABLE (a stored view of files: `ext::external`), in its own words.
    static EXTERNAL: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
        regex::Regex::new(r"(?is)^\s*CREATE\s+(\w+\s+){0,3}EXTERNAL\s+TABLE\b").expect("a regex")
    });
    if let Some(m) = EXTERNAL.find(first_word(sql)) {
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
    // CREATE MATERIALIZED VIEW v (CONSTRAINT c CHECK (…) ON VIOLATION DROP ROW, …): its
    // expectations (ADR-036 §2), which the parser doesn't take.
    match crate::views::constraints(sql) {
        Ok(Some((rest, expect))) => {
            return Some(match parse(&rest) {
                Some(Stmt::Ddl(mut d)) if matches!(d[..], [Ddl::CreateMaterialized { .. }]) => {
                    if let Some(Ddl::CreateMaterialized { options, .. }) = d.first_mut() {
                        options.insert("expect".into(), serde_json::to_string(&expect).unwrap_or_default());
                    }
                    Stmt::Ddl(d)
                }
                other => other.unwrap_or_else(|| Stmt::Invalid("CREATE MATERIALIZED VIEW v (CONSTRAINT c CHECK (…)) AS SELECT …".into())),
            });
        }
        Ok(None) => {}
        Err(e) => return Some(Stmt::Invalid(format!("{e:#}"))),
    }
    let name = |s: &str| s.split('.').map(|p| if p.starts_with('"') { p.trim_matches('"').to_string() } else { p.to_lowercase() }).collect::<Vec<_>>().join(".");
    if let Some(c) = VIEW_RENAME.captures(first_word(sql)) {
        return Some(Stmt::Ddl(vec![Ddl::RenameTable { name: name(&c[3]), to: name(&c[4]) }]));
    }
    // `ALTER MATERIALIZED VIEW v DETACH`: its rows stop following, and stay as a table of that name.
    static DETACH: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| regex::Regex::new(r#"(?is)^\s*ALTER\s+MATERIALIZED\s+VIEW\s+([\w."-]+)\s+DETACH\s*;?\s*$"#).expect("a regex"));
    if let Some(c) = DETACH.captures(first_word(sql)) {
        return Some(Stmt::Ddl(vec![Ddl::DetachView { name: name(&c[1]) }]));
    }
    // `CREATE TABLE c [SHALLOW] CLONE t` (Snowflake's, Databricks'): a table of t's files as they are now, copying none (ADR-043).
    static CLONE: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| regex::Regex::new(r#"(?is)^\s*CREATE\s+TABLE\s+(IF\s+NOT\s+EXISTS\s+)?([\w."-]+)\s+(SHALLOW\s+)?CLONE\s+([\w."-]+)\s*;?\s*$"#).expect("a regex"));
    if let Some(c) = CLONE.captures(first_word(sql)) {
        let d = Ddl::Clone { name: name(&c[2]), from: name(&c[4]) };
        return Some(Stmt::Ddl(vec![if c.get(1).is_some() { unless(true, &name(&c[2]), "relation", d) } else { d }]));
    }
    // `UNDROP TABLE t` (Snowflake's, Databricks'): the table dropped last under that name, back (ADR-043).
    static UNDROP: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| regex::Regex::new(r#"(?is)^\s*UNDROP\s+TABLE\s+([\w."-]+)\s*;?\s*$"#).expect("a regex"));
    if let Some(c) = UNDROP.captures(first_word(sql)) {
        return Some(Stmt::Ddl(vec![Ddl::Undrop { name: name(&c[1]) }]));
    }
    // DuckDB's CREATE MACRO … IF NOT EXISTS (the parser takes only OR REPLACE): read without it, kept unless one is there.
    static MACRO_QUIET: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| regex::Regex::new(r"(?is)^(\s*CREATE\s+(OR\s+REPLACE\s+)?(TEMP\s+|TEMPORARY\s+)?MACRO\s+)IF\s+NOT\s+EXISTS\s+").expect("a regex"));
    if let Some(c) = MACRO_QUIET.captures(first_word(sql)) {
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
        Statement::CreateTable(c) => Stmt::Create(Box::new(c)),
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
            true if says_default(&q) => Stmt::InsertInto(object(&t), vec![], q.to_string()),
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
            [ast::AlterTableOperation::AddColumn { column_def: c, .. }] if c.options.iter().any(|o| matches!(o.option, ast::ColumnOption::NotNull | ast::ColumnOption::Default(_))) => {
                Stmt::Invalid(format!("ALTER TABLE {} ADD COLUMN {} … NOT NULL or DEFAULT: the rows already there would have no value for it (add it, then UPDATE {} SET {} = …)", a.name, c.name, a.name, c.name))
            }
            [ast::AlterTableOperation::AddColumn { if_not_exists, column_def: c, .. }] => Stmt::AddColumn(object(&a.name), ident(&c.name), sql_type(&c.data_type), *if_not_exists),
            // (renames, drops and types: the catalog's names change, the files never do: ADR-022)
            [ast::AlterTableOperation::RenameColumn { old_column_name: from, new_column_name: to }] => alter(&a, from, Change::Rename(ident(to))),
            [ast::AlterTableOperation::DropColumn { column_names, if_exists, .. }] => Stmt::Ddl(column_names.iter().map(|c| Ddl::AlterColumn { table: object(&a.name), column: ident(c), change: Change::Drop { if_exists: *if_exists } }).collect()),
            [ast::AlterTableOperation::AlterColumn { column_name: c, op: ast::AlterColumnOperation::SetDataType { data_type, .. } }] => alter(&a, c, Change::Type(data_type.to_string())),
            [ast::AlterTableOperation::RenameTable { table_name }] => {
                let to = table_name.to_string();
                let to = to.trim_start_matches("TO ").trim_start_matches("AS ");
                let to = to.split('.').map(|p| if p.starts_with('"') { p.trim_matches('"').to_string() } else { p.to_lowercase() }).collect::<Vec<_>>().join(".");
                Stmt::Ddl(vec![Ddl::RenameTable { name: object(&a.name), to }])
            }
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
            ast::ObjectType::Table => Some(Ddl::DropTable { name, if_exists, purge }),
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
        Statement::CreateView(v) => Stmt::Ddl(vec![unless(v.if_not_exists, &object(&v.name), "relation", Ddl::CreateView { name: object(&v.name), sql: view_sql(&v), replace: v.or_replace })]),
        Statement::AttachDatabase { schema_name, database_file_name: ast::Expr::Value(v), .. } => match &v.value {
            ast::Value::SingleQuotedString(dir) | ast::Value::DoubleQuotedString(dir) => Stmt::Ddl(vec![Ddl::Attach { name: ident(&schema_name), dir: dir.clone() }]),
            _ => return None,
        },
        Statement::CreateDatabase { db_name, if_not_exists, location, .. } => Stmt::Ddl(vec![Ddl::CreateDatabase { name: object(&db_name).to_lowercase(), if_not_exists, dir: location }]),
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

/// `CREATE TABLE t (a BIGINT, b VARCHAR, PRIMARY KEY (a)) [WITH (publish = 'delta,iceberg',
/// cluster_by = 'b', merge = 'total:sum', partition_by = 'day(ts)')]` → the table name and its spec. SQL types become Arrow
/// types the way DataFusion maps them.
/// With `AS SELECT`, the columns are the query's (run over `from`'s tables; `files`: local files
/// too, for `pondra sql` on its own machine), or those it names (`CREATE TABLE t (a INT, b
/// VARCHAR) AS VALUES …`: the query's columns by position).
/// `sql` from its first word on: past leading whitespace and comments (`-- …`, `/* … */`).
pub fn first_word(sql: &str) -> &str {
    let mut s = sql.trim_start();
    loop {
        if let Some(rest) = s.strip_prefix("--") {
            s = rest.find('\n').map_or("", |i| &rest[i + 1..]).trim_start();
        } else if let Some(rest) = s.strip_prefix("/*") {
            s = rest.find("*/").map_or("", |i| &rest[i + 2..]).trim_start();
        } else {
            return s;
        }
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
        t => t.to_string(),
    }
}

/// A statement's declared columns (`CREATE TABLE t (…)`) as `declared` takes them.
pub fn columns_sql(columns: &[ast::ColumnDef]) -> String {
    columns.iter().map(|c| format!("{} {}", c.name, sql_type(&c.data_type))).collect::<Vec<_>>().join(", ")
}

async fn declared_of(columns: &[ast::ColumnDef]) -> Result<Vec<datafusion::arrow::datatypes::FieldRef>> { declared(&columns_sql(columns)).await }

/// Columns as SQL declares them (`"id" BIGINT, "name" VARCHAR`): their Arrow fields, as
/// DataFusion types them.
pub async fn declared(cols: &str) -> Result<Vec<datafusion::arrow::datatypes::FieldRef>> {
    let ctx = SessionContext::new_with_config(crate::optimize::config(datafusion::prelude::SessionConfig::new())); // (TIMESTAMPTZ in UTC)
    let ctx = crate::settings::apply(ctx).await?; // (the session's dialect: DuckDB's STRUCT(a INT), …)
    ctx.sql(&format!("CREATE TABLE t ({cols})")).await.with_context(|| format!("the columns ({cols})"))?;
    Ok(ctx.table("t").await?.schema().fields().iter().cloned().collect::<Vec<_>>())
}

pub async fn create_spec(c: &ast::CreateTable, from: &Lake, files: bool) -> Result<String> {
    let declared = || declared_of(&c.columns);
    let fields = match &c.query {
        Some(q) => {
            let sql = q.to_string();
            let ctx = session(from, &sql, "").await?;
            let ctx = if files { ctx.enable_url_table() } else { ctx };
            let fields = ctx.sql(&sql).await?.schema().fields().iter().cloned().collect::<Vec<_>>();
            match c.columns.is_empty() {
                true => fields,
                false => {
                    ensure!(fields.len() == c.columns.len(), "CREATE TABLE {} names {} columns, and its query gives {}", c.name, c.columns.len(), fields.len());
                    declared().await?
                }
            }
        }
        None => declared().await?,
    };
    let columns: Vec<(String, String)> = fields.iter().map(|f| (f.name().clone(), crate::query::type_name(f.data_type()))).collect();
    let name = |e: &ast::Expr| e.to_string().trim_matches('"').to_string();
    let mut key: Vec<String> = c.constraints.iter().flat_map(|k| match k {
        ast::TableConstraint::PrimaryKey(pk) => pk.columns.iter().map(|i| name(&i.column.expr)).collect(),
        _ => vec![],
    }).collect();
    key.extend(c.columns.iter().filter(|c| c.options.iter().any(|o| matches!(o.option, ast::ColumnOption::PrimaryKey(_)))).map(|c| c.name.value.clone()));
    let not_null: Vec<String> = c.columns.iter().filter(|c| c.options.iter().any(|o| matches!(o.option, ast::ColumnOption::NotNull))).map(|c| c.name.value.clone()).collect();
    let defaults: BTreeMap<String, String> = c.columns.iter().filter_map(|c| c.options.iter().find_map(|o| match &o.option {
        ast::ColumnOption::Default(e) => Some((c.name.value.clone(), e.to_string())),
        _ => None,
    })).collect();
    // CHECKs, named as Postgres names them when they aren't: `{table}_{column}_check`, `{table}_check`.
    let table = object(&c.name).rsplit('.').next().unwrap_or_default().to_string();
    let mut checks: Vec<(String, String)> = vec![];
    let mut check = |name: Option<&ast::Ident>, base: String, expr: &ast::Expr| {
        let name = name.map(ident).unwrap_or_else(|| (1..).map(|i| if i == 1 { base.clone() } else { format!("{base}{}", i - 1) }).find(|n| !checks.iter().any(|(c, _)| c == n)).unwrap_or(base));
        checks.push((name, expr.to_string()));
    };
    for col in &c.columns {
        for o in &col.options {
            if let ast::ColumnOption::Check(k) = &o.option {
                check(k.name.as_ref().or(o.name.as_ref()), format!("{table}_{}_check", col.name.value), &k.expr);
            }
        }
    }
    for k in &c.constraints {
        if let ast::TableConstraint::Check(k) = k {
            check(k.name.as_ref(), format!("{table}_check"), &k.expr);
        }
    }
    let mut opts: BTreeMap<String, String> = BTreeMap::new();
    if let ast::CreateTableOptions::With(o) | ast::CreateTableOptions::Options(o) = &c.table_options {
        for o in o {
            if let ast::SqlOption::KeyValue { key, value } = o {
                opts.insert(key.value.to_lowercase(), option_text(value));
            }
        }
    }
    const OPTIONS: [&str; 7] = ["publish", "cluster_by", "partition_by", "ttl", "order_by", "merge", "retention"];
    if let Some(k) = opts.keys().find(|k| !OPTIONS.contains(&k.as_str())) {
        bail!("CREATE TABLE … WITH ({k} = …): tables take {}", OPTIONS.join(", "));
    }
    let list = |k: &str| opts.get(k).map(|v| v.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect::<Vec<_>>());
    let merge: BTreeMap<String, String> = list("merge").unwrap_or_default().iter().filter_map(|m| m.split_once(':')).map(|(c, f)| (c.into(), f.into())).collect();
    let mut columns = columns;
    if !key.is_empty() && merge.is_empty() && !columns.iter().any(|(c, _)| c == "_deleted") {
        columns.push(("_deleted".into(), "Boolean".into())); // (so DELETE works; writes leave it out)
    }
    let spec = j!({"columns": columns, "key": key, "merge": merge, "publish": list("publish"), "cluster_by": list("cluster_by"), "ttl": opts.get("ttl"), "partition_by": opts.get("partition_by"), "order_by": opts.get("order_by"), "not_null": not_null, "defaults": defaults, "checks": checks, "retention": opts.get("retention")});
    Ok(spec.to_string())
}

/// Create a table (or change it: a spec sent again) from any node: the leader does it.
pub async fn define(app: &crate::server::App, name: &str, spec: &str) -> Result<Value> {
    if app.seq.is_none() {
        return Ok(http().post(crate::tls::url(&format!("{}/tables/{name}", app.cluster.leader.addr))).body(spec.to_string()).send().await?.error_for_status()?.json().await?);
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
    let m: TableMeta = lake.cat.get::<TableMeta>(&table_key(&table)).await?.ok_or_else(|| anyhow::anyhow!("no table {table}"))?.logical(); // (as SQL names it)
    let ttl = m.ttl.as_ref().map(|(c, s)| format!("{c}:{s}"));
    let mut spec = j!({"columns": m.columns, "key": m.key, "merge": m.merge, "publish": m.publish, "cluster_by": m.cluster, "ttl": ttl, "partition_by": m.partition, "order_by": m.order, "not_null": m.not_null, "defaults": m.defaults, "properties": m.properties, "checks": m.checks, "retention": m.retention_secs.map(|s| format!("{s} seconds"))});
    match stmt {
        Stmt::AddColumn(_, column, sql_type, if_not_exists) => {
            if m.columns.iter().any(|(c, _)| c == column) {
                ensure!(*if_not_exists, "{table} already has a column {column}");
                return Ok(None);
            }
            let ctx = SessionContext::new_with_config(crate::optimize::config(datafusion::prelude::SessionConfig::new())); // (TIMESTAMPTZ in UTC)
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
                    "ttl" | "order_by" | "retention" => spec[k.as_str()] = j!(v),
                    "partition_by" => bail!("partition_by can't change: each of {table}'s files holds one partition"),
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
        Stmt::Create(_) | Stmt::Define(..) | Stmt::AddColumn(..) | Stmt::SetOptions(..) | Stmt::Ddl(_) | Stmt::Merge(_) | Stmt::Invalid(_) | Stmt::CopyTo(..) | Stmt::InsertInto(..) | Stmt::TempView(..) | Stmt::TempSecret(..) => unreachable!("not a row write here"),
    }
}

/// `INSERT INTO t (b, a) …` as an INSERT of whole rows: the columns it names from its query, by
/// position, the others NULL (`_deleted` left for `rows` to fill), in the table's order. `VALUES`
/// stay `VALUES`, so they still go through the log.
pub async fn whole_rows(lake: &Lake, table: &str, names: &[String], query: &str) -> Result<String> {
    let (other, name) = crate::ddl::resolve(lake, table).await?;
    let meta: TableMeta = other.as_deref().unwrap_or(lake).cat.get(&table_key(&name)).await?.with_context(|| format!("no table {table}"))?;
    let columns: Vec<String> = meta.logical().columns.into_iter().map(|(c, _)| c).filter(|c| c != "_deleted" || names.contains(c)).collect();
    let names = if names.is_empty() { &columns[..] } else { names }; // (`INSERT INTO t VALUES (1, DEFAULT)`: every column)
    rows_for(table, &columns, names, query, &|c| crate::defaults::sql_of(&meta, c))
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

/// Is this `VALUES` item the word DEFAULT (the column's default, or NULL)?
fn is_default(e: &ast::Expr) -> bool { matches!(e, ast::Expr::Identifier(i) if i.quote_style.is_none() && i.value.eq_ignore_ascii_case("default")) }

/// `INSERT INTO t VALUES (…, DEFAULT, …)`: the query says DEFAULT somewhere.
fn says_default(q: &ast::Query) -> bool { matches!(q.body.as_ref(), ast::SetExpr::Values(v) if v.rows.iter().any(|r| r.iter().any(is_default))) }

/// `INSERT INTO table (names) query` as a query of all its `columns`, in order (`whole_rows`; a
/// temporary table's too, `temp.rs`).
pub fn rows_for(table: &str, columns: &[String], names: &[String], query: &str, default: &dyn Fn(&str) -> Option<String>) -> Result<String> {
    use datafusion::sql::sqlparser::{dialect::GenericDialect, parser::Parser};
    if let Some(n) = names.iter().find(|n| !columns.contains(n)) {
        bail!("INSERT INTO {table} ({n}): no such column ({})", columns.join(", "));
    }
    ensure!(names.iter().collect::<std::collections::HashSet<_>>().len() == names.len(), "INSERT INTO {table}: a column named twice");
    let at = |c: &String| names.iter().position(|n| n == c);
    let parsed = Parser::new(&GenericDialect {}).try_with_sql(query)?.parse_query()?;
    if let (ast::SetExpr::Values(v), None) = (parsed.body.as_ref(), &parsed.order_by) {
        let mut rows = vec![];
        for r in &v.rows {
            ensure!(r.len() == names.len(), "INSERT INTO {table} names {} columns, and a row has {}", names.len(), r.len());
            let value = |c: &String| match at(c) {
                Some(i) if !is_default(&r[i]) => r[i].to_string(),
                _ => default(c).unwrap_or_else(|| "NULL".into()),
            };
            rows.push(format!("({})", columns.iter().map(value).collect::<Vec<_>>().join(", ")));
        }
        return Ok(format!("VALUES {}", rows.join(", ")));
    }
    let q = |c: &str| format!("\"{}\"", c.replace('"', "\"\""));
    let list = columns.iter().map(|c| match at(c) {
        Some(i) => format!("__pondra_q.c{i} AS {}", q(c)),
        None => format!("{} AS {}", default(c).unwrap_or_else(|| "NULL".into()), q(c)),
    });
    let aliases = (0..names.len()).map(|i| format!("c{i}")).collect::<Vec<_>>().join(", ");
    Ok(format!("SELECT {} FROM ({query}) AS __pondra_q({aliases})", list.collect::<Vec<_>>().join(", ")))
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

/// Run a row query here: its rows in the table's column order and types.
pub async fn rows(ctx: &SessionContext, meta: &TableMeta, sql: &str) -> Result<RecordBatch> {
    let target = schema(&meta.columns)?;
    let batches = ctx.sql(&crate::asof::rewrite(&crate::routines::rows_apart(sql))?).await?.collect().await?;
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
        true => crate::query::strict(given.next().expect("counted"), f.data_type()),
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
#[derive(Serialize, Deserialize, Clone)]
pub struct Files {
    table: String,
    job: String,
    columns: Vec<(String, String)>,
    pub files: Vec<DataFile>,
}

impl Files {
    /// Where its files are.
    pub fn paths(&self) -> Vec<String> { self.files.iter().map(|f| f.path.clone()).collect() }
}

/// Run the query here and write its rows as Parquet into the table's folder (None: this job was
/// already recorded).
/// `stamp`: the commit number and time the rows' system columns get (`sys.rs`); None: the leader
/// stamps them when it records the files (`record`).
pub async fn write_files(lake: &Lake, ctx: &SessionContext, table: &str, query: &str, job: &str, stamp: Option<crate::log::Reserved>) -> Result<Option<Files>> {
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
    // (a table's live columns, in order, written under their stored names: ADR-022)
    let target: Vec<(String, DataType)> = match &meta {
        Some(m) => m.live().map(|(s, _, t)| Ok((s.to_string(), crate::query::dtype(t)?))).collect::<Result<_>>()?,
        None => df.schema().fields().iter().map(|f| (f.name().clone(), if *f.data_type() == DataType::Utf8View { DataType::Utf8 } else { f.data_type().clone() })).collect(),
    };
    ensure!(df.schema().fields().len() <= target.len(), "{} columns given, table {table} has {}", df.schema().fields().len(), target.len());
    let given = df.schema().columns(); // (the table's first columns; any after them are null)
    let null = || datafusion::prelude::lit(datafusion::scalar::ScalarValue::Null);
    // (a column the query leaves out: its DEFAULT, else NULL)
    let default = |stored: &str| -> Result<Expr> {
        match meta.as_ref().and_then(|m| m.defaults.get(stored)) {
            Some(e) => Ok(ctx.parse_sql_expr(e, df.schema())?),
            None => Ok(null()),
        }
    };
    let exprs = target.iter().enumerate().map(|(i, (name, t))| Ok(cast_to(match given.get(i) {
        Some(c) => Expr::Column(c.clone()),
        None => default(name)?,
    }, t.clone()).alias(name))).collect::<Result<Vec<_>>>()?;
    let df = df.select(exprs)?;
    let columns = target.iter().map(|(n, t)| (n.clone(), t.to_string())).collect();
    let partition = meta.as_ref().and_then(|m| m.partition.clone());
    let next = Arc::new(std::sync::atomic::AtomicU64::new(0)); // (the streams share one block of ids)
    let streams = df.execute_stream_partitioned().await?.into_iter().map(|rows| crate::defaults::checked(rows, &meta, table)).map(|rows| match stamp {
        Some(reserved) => crate::sys::stamp_stream(rows, next.clone(), reserved),
        None => Ok(rows),
    });
    let writes = streams.collect::<Result<Vec<_>>>()?.into_iter().map(|rows| crate::tier::write_stream(lake, table, rows, 1_000_000, &[], true, partition.as_deref()));
    let files = futures::future::try_join_all(writes).await?.concat();
    Ok(Some(Files { table: table.into(), job: job.into(), columns, files }))
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
    let new = TableMeta { columns: f.columns.clone(), publish: default_publish(), ids: true, ..Default::default() };
    let meta = lake.cat.get::<TableMeta>(&table_key(&f.table)).await?.unwrap_or_else(|| new.clone());
    // Types as the lake stores them, so a writer may name them its own way (`Utf8View`, `VARIANT`).
    let types = |c: &[(String, String)]| c.iter().map(|(_, t)| crate::query::dtype(t).map(|t| crate::query::type_name(&t)).unwrap_or_else(|_| t.clone())).collect::<Vec<_>>();
    ensure!(meta.key.is_empty(), "INSERT into a keyed table goes through the log");
    ensure!(!f.files.iter().any(|d| d.sys) || !follows(lake, &f.table).await?, "{AGAIN}: a materialized view or task of {} was made as it was written", f.table);
    let live: Vec<(String, String)> = meta.live().map(|(s, _, t)| (s.to_string(), t.to_string())).collect(); // (dropped columns aren't written)
    ensure!(types(&live) == types(&f.columns), "query columns {:?} don't match table {}", f.columns, f.table);
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
pub async fn on_node(app: &crate::server::App, stmt: Stmt, job: Option<String>) -> Result<Value> { on_node_as(app, stmt, job, false).await }

/// `on_node`; `files`: its SQL may read files on this machine (`FROM 'jan.csv'`: the shell's own
/// node, `server::owner`).
#[inline(never)] // (its work on the heap, made here: `App::query_as`)
pub fn on_node_as(app: &crate::server::App, stmt: Stmt, job: Option<String>, files: bool) -> futures::future::BoxFuture<'_, Result<Value>> {
    Box::pin(async move {
        app.lake.caught_up().await; // (a node that just started: not against an older catalog than its leader's)
        let out = crate::ext::listing(on_node_listed(app, stmt, job, files)).await?; // (files outside the lake: listed once a statement)
        seen_here(app).await;
        Ok(out)
    })
}

/// SQL's CREATE TABLE of a table that's there, as Postgres has it: refused (42P07), left as it is
/// with IF NOT EXISTS, dropped first with OR REPLACE. (`POST /tables/{name}`, which may only add
/// columns, is the call that may be sent again.)
async fn existing(app: &crate::server::App, c: &ast::CreateTable, job: &str, files: bool) -> Result<Option<Value>> {
    match there(&app.lake, c).await? {
        Some(true) => Ok(Some(j!({"table": object(&c.name), "exists": true}))),
        Some(false) => {
            let drop = Stmt::Ddl(vec![crate::ddl::Ddl::DropTable { name: object(&c.name), if_exists: true, purge: false }]);
            Box::pin(on_node_as(app, drop, Some(format!("{job}:replaced")), files)).await.map(|_| None)
        }
        None => Ok(None),
    }
}

/// Is the table a CREATE TABLE names there? None: no; Some(true): leave it (IF NOT EXISTS);
/// Some(false): drop it first (OR REPLACE); otherwise refused.
async fn there(lake: &Lake, c: &ast::CreateTable) -> Result<Option<bool>> {
    let name = object(&c.name);
    ensure!(!(c.or_replace && c.if_not_exists), "CREATE OR REPLACE TABLE … IF NOT EXISTS: one or the other");
    let (other, table) = crate::ddl::resolve(lake, &name).await?;
    let lake = other.unwrap_or_else(|| lake.arc());
    if lake.cat.get::<TableMeta>(&table_key(&table)).await?.is_none() {
        return Ok(None);
    }
    ensure!(c.if_not_exists || c.or_replace, "table {name} already exists (CREATE TABLE IF NOT EXISTS leaves it as it is; CREATE OR REPLACE TABLE makes it anew)");
    Ok(Some(c.if_not_exists))
}

/// A write done through a follower is in its reads before it answers: the next statement on this
/// node sees it (a script's INSERT, then its SELECT), as on the leader. The follower waits until
/// its view holds everything the leader had committed once the write was done (5 s at most: a
/// view that far behind catches up by itself).
pub async fn seen_here(app: &crate::server::App) {
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

async fn on_node_listed(app: &crate::server::App, stmt: Stmt, job: Option<String>, files: bool) -> Result<Value> {
    if let Some(out) = crate::temp::statement(app, &stmt, files).await? {
        return Ok(out); // (the session's own tables and views: on this node, in memory)
    }
    if let Some(out) = Box::pin(crate::txn::statement(app, &stmt, files)).await? {
        return Ok(out); // (in a transaction: kept until COMMIT, ADR-036 §5)
    }
    ensure!(!app.cluster.reader, "read-only node");
    if let Stmt::Invalid(why) = stmt {
        bail!(why);
    }
    if let Some((query, to, options)) = crate::ext::view_write(&app.lake, &stmt).await? {
        let out = crate::copy::copy_to(&app.lake, &query, &to, &options, &app.cluster.nodes(), &app.cluster.addr).await?; // (a view of a folder: a new file in it)
        return Ok(j!({"rows": out["copied"]}));
    }
    let stmt = match stmt {
        Stmt::InsertInto(t, names, query) => Stmt::Insert(t.clone(), whole_rows(&app.lake, &t, &names, &query).await?),
        s => s,
    };
    if let Stmt::CopyTo(query, to, options) = &stmt {
        return crate::copy::copy_to(&app.lake, query, to, options, &app.cluster.nodes(), &app.cluster.addr).await; // (files outside the lake: from here, or every node its share)
    }
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
        if let Some(out) = Box::pin(existing(app, c, &job, files)).await? {
            return Ok(out);
        }
        if let Some(q) = &c.query {
            let (name, query) = (object(&c.name), q.to_string());
            Box::pin(on_node_as(app, Stmt::Define(name.clone(), create_spec(c, lake, files).await?), Some(job.clone()), files)).await?;
            return Box::pin(on_node_as(app, Stmt::Insert(name, query), Some(job), files)).await;
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
    // UPDATE and DELETE of an append table, and MERGE: the leader's (of the table's lake), from
    // one snapshot (`change.rs`). Another leader is sent what it can't read (ADR-028).
    if let Some(sql) = changes(lake, &stmt).await? {
        let (other, local) = crate::ddl::resolve(lake, &stmt.table()).await?;
        if let (None, Some(seq)) = (&other, &app.seq) {
            let _guard = app.lock.lock().await;
            return crate::change::FILES.scope(files, crate::change::run(lake, seq, &sql, &job)).await;
        }
        let target = other.clone().unwrap_or_else(|| lake.arc());
        let (sql, sent) = crate::change::for_leader(lake, &target, &stmt.table(), &local, &sql, files).await?;
        return match other {
            None => post(&app.cluster.leader.addr, &Request::Change(sql, job, sent)).await,
            Some(o) => deliver(&o.url, Some(Some(Request::Change(sql, job.clone(), sent))), &stmt, &job, false).await,
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
    let meta = lake.cat.get::<TableMeta>(&table_key(&table)).await?.map(|m| m.logical()); // (rows under SQL's names: the log keeps them stored, `log::pack`)
    let sql = match (&meta, &stmt) {
        (Some(m), _) if through_log(m, &stmt) => rows_sql(m, &stmt)?,
        (_, Stmt::Insert(_, query)) => {
            let ctx = open(session(lake, query, "").await?);
            let Some(f) = write_files(lake, &ctx, &table, query, &job, stamp(lake, &table, Some(&app.to())).await?).await? else { return Ok(j!({"duplicate": true})) };
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
    let meta = meta.expect("a table");
    let point = match &stmt {
        // (one key's UPDATE: its row from the serving path, no planning, ADR-036 §6)
        Stmt::Update(_, set, Some(cond)) => match lake.cat.get::<TableMeta>(&table_key(&table)).await? {
            Some(stored) => Box::pin(crate::txn::point_change(lake, &table, &stored, None, set, cond)).await?.map(|(_, new)| new),
            None => None,
        },
        _ => None,
    };
    let batch = match point {
        Some(new) if !new.is_empty() => new.into_iter().next().expect("a row"),
        _ => rows(&open(session(lake, &sql, "").await?), &meta, &sql).await?,
    };
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
        Request::Ddl(d) => {
            let _guard = lock.lock().await;
            crate::ddl::apply(lake, d).await
        }
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

/// `pondra sql --dir … "<write>"` on any machine. This process does the work (local files too:
/// `SELECT * FROM 'jan.parquet'`), then the leader records it: over HTTP; through the bucket
/// inbox if this machine can't reach it; or, when nobody leads, this process leads for the moment
/// it takes, under its own term, so a node starting meanwhile waits for it. It never takes over
/// from a live leader.
/// The lake at `dir`, made first if there is none (`pondra sql --dir new "CREATE TABLE t AS SELECT
/// * FROM 'x.csv'"`): this process leads for a moment, writing nothing but an empty catalog.
pub async fn made(dir: &str) -> Result<Arc<Lake>> {
    if let Ok(lake) = Lake::open(dir, false, false).await {
        return Ok(lake);
    }
    deliver(dir, Some(None), &Stmt::Invalid(String::new()), "", true).await?;
    Lake::open(dir, false, false).await
}

pub async fn from_cli(dir: &str, stmt: Stmt) -> Result<Value> {
    if let Stmt::Invalid(why) = stmt {
        bail!(why); // (said as a node says it)
    }
    if let Ok(lake) = Lake::open(dir, false, false).await {
        if let Some((query, to, options)) = crate::ext::view_write(&lake, &stmt).await? {
            let out = crate::copy::copy_to(&lake, &query, &to, &options, &[], "").await?; // (a view of a folder: a new file in it)
            return Ok(j!({"rows": out["copied"]}));
        }
    }
    let stmt = match stmt {
        Stmt::InsertInto(t, names, query) => Stmt::Insert(t.clone(), whole_rows(&*Lake::open(dir, false, false).await?, &t, &names, &query).await?),
        s => s,
    };
    if let Stmt::Create(c) = &stmt {
        if Lake::open(dir, false, false).await.is_ok() && Box::pin(created(dir, c)).await?.is_some() {
            return Ok(j!({"table": object(&c.name), "exists": true})); // (CREATE TABLE IF NOT EXISTS of one that's there)
        }
    }
    match stmt {
        Stmt::CopyTo(query, to, options) => {
            let lake = Lake::open(dir, false, false).await?;
            crate::ext::scope(true, async move { crate::copy::copy_to(&lake, &query, &to, &options, &[], "").await }).await // (its user's own machine)
        }
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

/// The shell's CREATE TABLE of a table that's there: Some (left, IF NOT EXISTS), None (made
/// next: OR REPLACE dropped it), or refused.
async fn created(dir: &str, c: &ast::CreateTable) -> Result<Option<()>> {
    let lake = Lake::open(dir, false, false).await?;
    match there(&lake, c).await? {
        Some(true) => Ok(Some(())),
        Some(false) => Box::pin(one_from_cli(dir, Stmt::Ddl(vec![crate::ddl::Ddl::DropTable { name: object(&c.name), if_exists: true, purge: false }]))).await.map(|_| None),
        None => Ok(None),
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

/// Have the leader of the lake at `dir` record `req` (made on this node), however it's reached.
pub async fn send(dir: &str, req: Request) -> Result<Value> { deliver(dir, Some(Some(req)), &Stmt::Invalid(String::new()), "", false).await }

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
        let t = stmt.table();
        let (sql, sent) = crate::change::for_leader(query, target, &t, &t, &sql, files).await?; // (files here: this machine's)
        return Ok(Some(Request::Change(sql, job.into(), sent)));
    }
    let meta = target.cat.get::<TableMeta>(&table_key(table)).await?.map(|m| m.logical());
    let log = meta.as_ref().is_some_and(|m| through_log(m, stmt));
    match (meta, stmt) {
        (Some(m), _) if log => {
            let sql = rows_sql(&m, stmt)?;
            let batch = rows(&open(session(query, &sql, "").await?), &m, &sql).await?;
            crate::defaults::check(&m, table, &batch)?; // (NOT NULL: as a node's own log checks its rows)
            let (ack, _) = tokio::sync::oneshot::channel();
            let append = Append { table: table.into(), src: Src { producer: format!("sql:{job}"), seq: 1, prev: None }, batch, ack };
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
    if t.addr.is_empty() || !alive(&lake.store, &t).await || std::env::var_os("PONDRA_NO_DIRECT").is_some() {
        return None;
    }
    crate::log::To::Leader(t.addr).reserve().await.ok()
}

/// Send a request to the leader over HTTP.
pub async fn post(addr: &str, r: &Request) -> Result<Value> {
    let (path, body) = r.http()?;
    let res = http().post(crate::tls::url(&format!("{addr}{path}"))).header("content-type", "application/json").body(body).send().await?;
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
        Outcome::Retry(r) if r.is_empty() => bail!("a materialized view of the table was made or dropped as this was sent: run it again (it goes in once)"),
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
