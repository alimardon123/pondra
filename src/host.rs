//! A node as the SQL front's host (`front.rs`): what the front asks, answered from the lake.

use crate::ext_names::Attached;
use crate::front::{Host, Own};
use crate::expander::Routine;
use crate::store::Lake;
use anyhow::Result;
use datafusion::sql::sqlparser::ast::Statement;
use std::collections::{BTreeSet, HashMap};
use std::future::Future;
use std::sync::Arc;

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
            if let Some(m) = self.cat.get::<crate::store::TableMeta>(&crate::store::table_key(&n)).await? {
                let m = m.logical();
                out.insert(n, m.columns.into_iter().map(|(c, _)| c).chain(crate::text::NAMES.map(String::from)).collect());
            }
        }
        Ok(out)
    }

    // (The four below wrap free functions for the same reason as `own`: their passes ask queries.)
    fn measured(&self, sql: &str) -> impl Future<Output = Result<bool>> + Send { measured_of(self, sql) }

    fn measures(&self, sql: &str, stmts: &mut [Statement]) -> impl Future<Output = Result<bool>> + Send { measures_of(self, sql, stmts) }

    fn types(&self, stmts: &mut [Statement]) -> impl Future<Output = Result<bool>> + Send { types_of(self, stmts) }

    fn friendly(&self, stmts: &mut [Statement]) -> impl Future<Output = Result<bool>> + Send { friendly_of(self, stmts) }

    fn ask(&self, sql: &str) -> impl Future<Output = Result<datafusion::prelude::DataFrame>> + Send { ask_of(self, sql) }
}

async fn measured_of(lake: &Lake, sql: &str) -> Result<bool> { crate::measures::wanted(lake, sql).await }

async fn measures_of(lake: &Lake, sql: &str, stmts: &mut [Statement]) -> Result<bool> { crate::measures::rewrite(lake, sql, stmts).await }

async fn types_of(lake: &Lake, stmts: &mut [Statement]) -> Result<bool> { crate::types::rewrite(lake, stmts).await }

async fn friendly_of(lake: &Lake, stmts: &mut [Statement]) -> Result<bool> {
    let defaulted = crate::friendly::defaults(lake, stmts).await?;
    Ok(crate::friendly::rewrite(lake, stmts).await? || defaulted)
}

/// What the front's passes ask, planned as the node plans a query: expanded, ASOF JOIN's marker, the caller's session.
async fn ask_of(lake: &Lake, sql: &str) -> Result<datafusion::prelude::DataFrame> {
    let sql = crate::routines::expand(lake, sql).await?;
    let sql = crate::asof::rewrite(&sql)?.into_owned();
    let ctx = crate::query::session(lake, &sql, "").await?;
    crate::engine::sql(&ctx, &sql).await
}

/// The node's own statements, asked before anything else (`Host::own`): the seven passes of the
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
    if let Some(q) = crate::friendly::summarize(lake, sql).await? {
        return Ok(Own::Answer(q));
    }
    let sql = &crate::past::restore(lake, sql).await?.unwrap_or_else(|| sql.to_string()); // (`RESTORE TABLE t TO VERSION AS OF n`: a MERGE, ADR-043)
    Ok(Own::Go(sql.to_string()))
}
