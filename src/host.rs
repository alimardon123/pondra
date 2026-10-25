//! A node as the SQL front's host (`front.rs`): what the front asks, answered from the lake.

use crate::ext_names::Attached;
use crate::front::{Host, Own, Relation};
use crate::expander::Routine;
use crate::measures::Definition;
use crate::pg_trace::Ty;
use crate::store::{Lake, TableMeta};
use crate::ddl;
use anyhow::Result;
use std::collections::{BTreeSet, HashMap};
use std::future::Future;
use std::sync::{Arc, Mutex};

impl Host for Lake {
    fn name(&self) -> String { crate::ddl::lake_name(self) }

    // (A free function, not the method's body: its awaits reach the front again through queries, and
    // a method's future is checked against the trait's `+ Send` while its own type is still being made,
    // which the compiler reports as a cycle (E0391). `own` below is the same pass.)
    fn own(&self, sql: &str) -> impl Future<Output = Result<Own>> + Send { own(self, sql) }

    async fn routines(&self) -> Result<Arc<HashMap<String, Routine>>> { crate::routines::listed(self).await }

    async fn attached(&self) -> Result<Vec<(String, Attached)>> { crate::ext::attached(self).await }

    async fn columns(&self, tables: BTreeSet<String>) -> Result<HashMap<String, Vec<String>>> {
        let mut out = HashMap::new();
        for n in tables {
            if let Some(m) = self.cat.meta(&crate::store::table_key(&n)).await? {
                out.insert(n, m.logical_columns().into_iter().map(|(c, _)| c).chain(crate::text::NAMES.map(String::from)).collect());
            }
        }
        Ok(out)
    }

    async fn measure_views(&self, sql: &str) -> Result<Arc<HashMap<String, Arc<Definition>>>> { measure_views(self, sql).await }

    async fn relation(&self, name: &str) -> Option<Relation<Self>> { relation(self, name).await }

    async fn enum_labels(&self, name: &str) -> Option<Vec<String>> { crate::types::labels(self, name).await }

    // (The one below wraps a free function for the same reason as `own`: its pass asks queries.)
    fn ask(&self, sql: &str) -> impl Future<Output = Result<datafusion::prelude::DataFrame>> + Send { ask_of(self, sql) }

    // (a node's watch is its own, `oom.rs`: the frame is collected as it is)
    async fn collect(&self, frame: datafusion::prelude::DataFrame) -> Result<Vec<datafusion::arrow::record_batch::RecordBatch>> { Ok(frame.collect().await?) }
}

/// What the front's passes ask, planned as the node plans a query: expanded, ASOF JOIN's marker, the caller's session.
async fn ask_of(lake: &Lake, sql: &str) -> Result<datafusion::prelude::DataFrame> {
    let sql = crate::routines::expand(lake, sql).await?;
    let sql = crate::sys::planned(&sql)?.into_owned();
    let ctx = crate::query::session(lake, &sql, "").await?;
    crate::engine::sql(&ctx, &sql).await
}

/// The node's own statements, asked before anything else (`Host::own`): the six passes of the
/// front, in their order.
async fn own(lake: &Lake, sql: &str) -> Result<Own> {
    let sql = &crate::vars::parameters_in(lake, sql).await?; // (`pondra.parameters('etl/orders.sql')`: a file's parameters, as rows)
    let sql = &crate::find::functions_in(lake, sql).await?; // (`pondra.find('revenue')`: the catalog searched, as rows)
    let sql = &crate::git::status_in(lake, sql).await?; // (`pondra.git_status('sales')`: a Git folder's local changes, as rows)
    if let Some(q) = crate::routines::show(sql) {
        return Ok(Own::Answer(q));
    }
    if let Some(q) = crate::objects::show_create(lake, sql).await? {
        return Ok(Own::Answer(q)); // (`SHOW CREATE TABLE t`: the statements that make it again)
    }
    let sql = &crate::past::restore(lake, sql).await?.unwrap_or_else(|| sql.to_string()); // (`RESTORE TABLE t TO VERSION AS OF n`: a MERGE, ADR-043)
    Ok(Own::Go(sql.to_string()))
}

// The lake's views with measures: what `measure_views` answers.

/// The views with measures a lake holds, by local name (`sales`, or `crm.sales`). Read again only after a commit, as
/// `routines::listed` is: every query asks.
async fn own_measure_views(lake: &Lake) -> Result<Arc<HashMap<String, Arc<Definition>>>> {
    type Seen = Mutex<HashMap<String, (u64, Arc<HashMap<String, Arc<Definition>>>)>>;
    crate::per_node!(static SEEN: Seen = Default::default());
    let version = lake.cat.version();
    if let (Some(v), Some((at, all))) = (version, SEEN.lock().unwrap().get(&lake.url)) {
        if *at == v {
            return Ok(all.clone());
        }
    }
    let mut out = HashMap::new();
    for (key, view) in lake.cat.scan::<ddl::StoredView>("q/", "q0").await? {
        let Some(local) = key.strip_prefix("q/") else { continue };
        // A stored view that won't parse can't be read either way: it is skipped, not a failure of every query.
        if let Ok(Some(d)) = Definition::parse(&view.sql) {
            out.insert(local.to_string(), Arc::new(d));
        }
    }
    let all = Arc::new(out);
    if let Some(v) = version {
        SEEN.lock().unwrap().insert(lake.url.clone(), (v, all.clone()));
    }
    Ok(all)
}

/// The views with measures a lake can read: its own by local name, and those of the lakes attached to it (`lake.sales`).
/// A statement that names no attached lake gets its own map as it is: every statement asks, so it is not copied.
async fn measure_views(lake: &Lake, sql: &str) -> Result<Arc<HashMap<String, Arc<Definition>>>> {
    // (another lake's only when the statement names it: its catalog has no version to keep them by, so each look reads it)
    let attached: Vec<(String, Arc<Lake>)> = lake.attached.read().unwrap().iter().filter(|(name, _)| crate::text::mentions(sql, name)).cloned().collect();
    if attached.is_empty() {
        return own_measure_views(lake).await;
    }
    let mut out: HashMap<String, Arc<Definition>> = own_measure_views(lake).await?.iter().map(|(k, d)| (k.clone(), d.clone())).collect();
    for (name, other) in attached {
        for (local, d) in own_measure_views(&other).await?.iter() {
            if !local.contains('.') {
                out.insert(format!("{name}.public.{local}"), d.clone());
            }
            out.insert(format!("{name}.{local}"), d.clone());
        }
    }
    Ok(Arc::new(out))
}

// What a name is to a node (`Host::relation`): the columns of a table, or a stored view's query. The front's traces read
// them (`pg_trace`), so a node answers as it answered when the trace was the node's own.

/// What `name` is to this node: a session's temporary table, a session's temporary view, or a table or stored view of
/// this lake (or of the lake attached under its first part).
async fn relation(lake: &Lake, name: &str) -> Option<Relation<Lake>> {
    if let Some(cols) = crate::temp::columns_of(name) {
        // (a session's temporary table shadows a lake table of its name, so its own columns and types come first)
        return Some(Relation::Columns(cols.into_iter().map(|(c, t)| (c, t.as_deref().and_then(Ty::of))).collect()));
    }
    // (a session's temporary view: its query, traced as a stored view's is)
    if let Some((_, sql)) = crate::temp::views(name).into_iter().find(|(n, _)| n == name) {
        return Some(Relation::View { sql, lake: None });
    }
    let (other, local) = crate::ddl::resolve(lake, name).await.ok()?;
    let lake = other.as_deref().unwrap_or(lake);
    if let Ok(Some(m)) = lake.cat.meta(&crate::store::table_key(&local)).await {
        if m.finish.is_some() {
            // (a view that finishes its answers shows the columns it answers, SQL's names as `SELECT *` shows them, not its partial ones)
            let l = m.described();
            return Some(Relation::Columns(l.columns.iter().filter(|(c, _)| !l.marker(c)).map(|(c, _)| (c.clone(), l.sql_types.get(c).and_then(|t| Ty::of(t)))).collect()));
        }
        return Some(Relation::Columns(typed_columns(&m).map(|(c, t)| (c.to_string(), t)).collect()));
    }
    let view = lake.cat.get::<crate::ddl::StoredView>(&crate::ddl::query_key(&local)).await.ok()??;
    Some(Relation::View { sql: view.sql, lake: other })
}

/// A table's columns as SQL sees them, in order, without an upsert table's delete markers, each with the type the port names it
/// by. `sql_types` is keyed by the stored name.
fn typed_columns(m: &TableMeta) -> impl Iterator<Item = (&str, Option<Ty>)> + '_ {
    m.live().filter(|(_, n, _)| !m.marker(n)).map(|(s, n, _)| (n, m.sql_types.get(s).and_then(|t| Ty::of(t))))
}
