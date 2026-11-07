//! The write path (design v2.1 §"The write path"; step 9, `l3/write-path-prep.md`). A door that takes rows opens a
//! [`Writer`] on its table and gives it the rows a batch at a time. The steps and their order are kept here, not at the
//! doors: the caller's rights first, then the columns a batch leaves out, the casts and the checks (`rules::shape`),
//! then the log. A door reads its own wire (COPY's text, Kafka's records, Flight's and HTTP's batches) and nothing else.
//! COPY, Kafka, Flight's DoPut and HTTP's `/append` write through it, and each load leaves its line in the audit log
//! ([`Load`]). SQL's writes come with step 8.

use crate::app::App;
use crate::auth::{Principal, Role};
use crate::log::{Ack, Src};
use crate::rules::{How, Rows, Shaped};
use crate::store::TableMeta;
use anyhow::Result;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// The step that refused a write. Each door says each in its own words: Kafka a bad record or the topic's
/// authorization, Flight an invalid argument or permission denied.
pub enum Refused {
    /// Not the caller's to write ([`may`]).
    Rights(anyhow::Error),
    /// No such table, or its rules don't make.
    Table(anyhow::Error),
    /// Rows the table can't take: a value its column can't hold, NOT NULL, a CHECK.
    Rows(anyhow::Error),
    /// The log's: a read-only node, a frozen lake, a write the leader refused.
    Log(anyhow::Error),
}

impl From<Refused> for anyhow::Error {
    fn from(r: Refused) -> Self {
        match r {
            Refused::Rights(e) | Refused::Table(e) | Refused::Rows(e) | Refused::Log(e) => e,
        }
    }
}

/// May `who` write rows into `table` (as the catalog names it)? The one place a row door asks: before it reads a row
/// against the table's columns, and again for each batch, so a grant taken back stops a load at its next batch (step
/// 6's `may` takes this place). A token that writes, or a user with the table's INSERT grant, and never a system table
/// (`auth::check_rows`); with the table's entry, UPDATE too for a keyed table (its rows replace their keys'), and
/// DELETE when `deletes` (rows marked `_deleted`, or a COPY that names that column): S11. `who` None is the node's
/// own work, or a door that is in the caller's scope already.
pub fn may(who: Option<&Principal>, table: &str, meta: Option<&TableMeta>, deletes: bool) -> Result<()> {
    let ask = || -> Result<()> {
        if let Some(p) = crate::auth::current() {
            if p.access.is_none() && p.role < Role::Write {
                return Err(crate::codes::coded("42501", "this token may not write")); // (as `Auth::allows` says it)
            }
        }
        crate::auth::check_rows(table)?;
        match meta {
            Some(meta) => crate::auth::check_changes(table, !meta.key.is_empty(), deletes),
            None => Ok(()),
        }
    };
    match who {
        Some(w) => crate::auth::WHO.sync_scope(w.clone(), ask),
        None => ask(),
    }
}

/// One load at a row door (a COPY FROM STDIN, a DoPut's stream, a produce's partition), as the audit log keeps it: one
/// line when it ends, the INSERT it stands for (`audit::rows`, as `/append`'s), in its caller's name, with the first
/// error it met. A load dropped before it ended (its client went away) ends as failed.
pub struct Load {
    app: App,
    who: Option<Principal>,
    door: &'static str,
    table: String,
    start: datafusion::common::instant::Instant,
    end: Option<Option<String>>,
}

impl Load {
    /// A load into `table` through `door`, by `who` (None: the caller in scope).
    pub fn new(app: &App, who: Option<Principal>, door: &'static str, table: &str) -> Load {
        Load { app: app.clone(), who: who.or_else(crate::auth::current), door, table: table.to_string(), start: datafusion::common::instant::Instant::now(), end: None }
    }

    /// It ended: well, or with `error`. The first end counts.
    pub fn end(&mut self, error: Option<String>) {
        self.end.get_or_insert(error);
    }

    /// A load refused before it began: its line now.
    pub fn refused(app: &App, who: Option<Principal>, door: &'static str, table: &str, error: &anyhow::Error) {
        Load::new(app, who, door, table).end(Some(error.to_string()));
    }
}

impl Drop for Load {
    fn drop(&mut self) {
        let error = self.end.take().unwrap_or_else(|| Some("the client went away before the load ended".into()));
        let line = || crate::audit::rows(&self.app, self.door, &self.table, error, self.start.elapsed());
        match self.who.clone() {
            Some(w) => crate::auth::WHO.sync_scope(w, line),
            None => line(),
        }
    }
}

/// A batch on its way into the log: its ack, to wait for (COPY, `/append`) or to read in order (Kafka and Flight
/// pipeline theirs, and send a batch again when the one after it overtook it: `Log::sent_again`), with what it was.
pub struct Queued {
    pub ack: Pin<Box<dyn Future<Output = Result<Ack>> + Send>>,
    pub src: Src,
    pub rows: Shaped,
}

/// A write into one table, opened by one caller.
pub struct Writer {
    app: App,
    who: Option<Principal>,
    table: String,
    meta: Arc<TableMeta>,
    rules: Arc<crate::rules::Table>,
    how: How,
    deletes: bool,
}

impl Writer {
    /// `table` for `who`, written as `how` says. The caller's rights come first, so a caller without them learns
    /// nothing of the table, not even whether it is there. Then its entry (a follower catches up first: `table_meta`),
    /// then the rights the entry asks for (UPDATE, for a keyed table).
    pub async fn open(app: &App, who: Option<Principal>, table: &str, how: How, deletes: bool) -> Result<Writer, Refused> {
        may(who.as_ref(), &crate::auth::catalog_name(table), None, deletes).map_err(Refused::Rights)?;
        let meta = app.lake.table_meta(table).await.map_err(Refused::Table)?;
        let rules = meta.rules().map_err(Refused::Table)?;
        Self::with(app, who, table, meta, rules, how, deletes)
    }

    /// The same, for a door that has read the table's entry itself (Kafka reads it once for a topic's partitions).
    pub fn with(app: &App, who: Option<Principal>, table: &str, meta: Arc<TableMeta>, rules: Arc<crate::rules::Table>, how: How, deletes: bool) -> Result<Writer, Refused> {
        may(who.as_ref(), &crate::auth::catalog_name(table), Some(&meta), deletes).map_err(Refused::Rights)?;
        Ok(Writer { app: app.clone(), who, table: table.to_string(), meta, rules, how, deletes })
    }

    /// The table's entry, for a door that reads its wire against it (Kafka's and JSON's rows without a key).
    pub fn meta(&self) -> &Arc<TableMeta> { &self.meta }

    /// The table's rules: its columns under SQL's names (ADR-022), for a door's decoding.
    pub fn rules(&self) -> &Arc<crate::rules::Table> { &self.rules }

    /// One batch. First `rules::shape`: the columns it leaves out take their DEFAULT, the rest are cast to the
    /// table's types, and its checks run. Then the caller's rights again, now that it is known whether any row is
    /// marked `_deleted`. Then the log's queue.
    pub async fn put(&self, rows: Rows, src: Src) -> Result<Queued, Refused> {
        let shaped = crate::rules::shape(&self.rules, &self.table, rows, self.how, &crate::defaults::Node(&self.app.lake)).await.map_err(Refused::Rows)?;
        let marked = shaped.rows().column_by_name("_deleted").and_then(|c| c.as_any().downcast_ref::<datafusion::arrow::array::BooleanArray>()).is_some_and(|d| d.true_count() > 0);
        may(self.who.as_ref(), &crate::auth::catalog_name(&self.table), Some(&self.meta), self.deletes || marked).map_err(Refused::Rights)?;
        let log = self.app.log().map_err(Refused::Log)?;
        let ack = log.queue_shaped(self.table.clone(), src.clone(), shaped.clone()).await.map_err(Refused::Log)?;
        Ok(Queued { ack: Box::pin(ack), src, rows: shaped })
    }
}
