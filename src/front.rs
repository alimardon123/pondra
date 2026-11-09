//! The SQL front: what a statement goes through before anything plans it (macros, DuckDB's and Spark's spellings,
//! FROM first, files by name, …), generic over what it asks of where it runs (`Host`). A node is a host
//! (`host.rs`), and so is a browser page (`page.rs`). Each pass runs once a statement or once a name.

use crate::ext_names::Attached;
use crate::expander::{Kind, Routine};
use crate::measures::Definition;
use anyhow::Result;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::DataFrame;
use datafusion::sql::sqlparser::ast::Statement;
use futures::future::BoxFuture;
use std::collections::{BTreeSet, HashMap};
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// What the front asks of where it runs. Asked once a statement, or once a name, never once a batch. The front runs the
/// core passes itself (the host's own statements, the measures, DuckDB's and Snowflake's forms), so every host answers
/// them alike; a host answers only what it keeps (routines, attached catalogs, the views with measures it can read,
/// types of its own). Every method is required, so a host says what it keeps rather than inherit a silent "none"
/// (reviews/form-parity.md §2).
pub trait Host: Send + Sync {
    /// The lake's name, the first part of a three-part name (`text::local_in`).
    fn name(&self) -> String;
    /// The host's own statements, asked before anything else (a node's `SHOW CREATE`, `pondra.find`, `RESTORE`, …):
    /// `Own::Go` with the text when it has none.
    fn own(&self, sql: &str) -> impl Future<Output = Result<Own>> + Send;
    /// The macros, functions and procedures a statement may call, by name (`routines::listed`, and a node's session's own).
    fn routines(&self) -> impl Future<Output = Result<Routines>> + Send;
    /// Other engines' catalogs attached (`ext::attached`).
    fn attached(&self) -> impl Future<Output = Result<Vec<(String, Attached)>>> + Send;
    /// The columns of these tables of the lake (names inside it, as `text::local_in` gives them), system columns last;
    /// a name with no table is left out.
    fn columns(&self, tables: BTreeSet<String>) -> impl Future<Output = Result<HashMap<String, Vec<String>>>> + Send;
    /// The views with measures a statement may read, by every name it may give one (`sales`, `crm.sales`, an
    /// attached lake's `other.sales` and `other.public.sales`): a node's from its lake's views.
    fn measure_views(&self, sql: &str) -> impl Future<Output = Result<Arc<HashMap<String, Arc<Definition>>>>> + Send;
    /// What a name is to the host, for the front's traces (`pg_trace`): a relation's columns, or a stored view's query.
    /// None: not a relation here (a CTE, a file, a table function, an unknown name).
    fn relation(&self, name: &str) -> impl Future<Output = Option<Relation<Self>>> + Send;
    /// An enum type's labels, in order; None: no such type.
    fn enum_labels(&self, name: &str) -> impl Future<Output = Option<Vec<String>>> + Send;
    /// A query the front's passes ask before they can write theirs (`friendly`: a `PIVOT`'s values, the columns
    /// `COLUMNS(…)` and `SUMMARIZE` read), planned as the host plans a user's query, after its front. Its rows are
    /// read only if the pass needs them.
    fn ask(&self, sql: &str) -> impl Future<Output = Result<DataFrame>> + Send;
    /// The rows of a frame `ask` gave, collected as the host collects a query's rows (a page stops the query that grows it
    /// past its memory line, `page::watched`).
    fn collect(&self, frame: DataFrame) -> impl Future<Output = Result<Vec<RecordBatch>>> + Send;
}

/// What the host's own statements made of one: a query to run in its place, or the text to go on with.
pub enum Own {
    #[cfg_attr(not(feature = "lake"), allow(dead_code))] // (only a node's own statements answer: a lake-less build has none)
    Answer(String),
    Go(String),
}

/// What a name is to the host, for the front's traces (`pg_trace`): a relation's columns by SQL name, each with the
/// Postgres type it holds as text (none: its Arrow type says), or a stored view's query, with the lake it is in when
/// that is another, attached one (its query names that lake's tables).
pub enum Relation<H: ?Sized> {
    Columns(crate::pg_trace::Cols),
    #[cfg_attr(not(feature = "lake"), allow(dead_code))] // (only a node has views: a lake-less build makes none)
    View { sql: String, lake: Option<Arc<H>> },
}

/// What a statement may call: `own` in its own text (a node's session's temporary functions over the lake's, `temp.rs`),
/// and `lake` in the body of one of the lake's: a session's function never changes what the lake's own functions mean,
/// so a lake function, or a stored view, reads the same for every reader (Postgres never looks in `pg_temp` for a
/// function either). With no temporary functions the two are one map.
pub struct Routines {
    pub own: Arc<HashMap<String, Routine>>,
    pub lake: Arc<HashMap<String, Routine>>,
}

/// The host as the registry's tree passes take it (`Trees`): `Host`'s questions with boxed answers, so a pass is a plain
/// function the registry holds. Every host is one (the impl below), and the front hands its host on as one only at the
/// tree passes, so the quick path boxes nothing. `dyn Front` is a host again (`impl Host for dyn Front`): a pass calls the
/// same generic functions the front called before.
pub trait Front: Send + Sync {
    fn name(&self) -> String;
    fn own<'a>(&'a self, sql: &'a str) -> BoxFuture<'a, Result<Own>>;
    fn routines(&self) -> BoxFuture<'_, Result<Routines>>;
    fn attached(&self) -> BoxFuture<'_, Result<Vec<(String, Attached)>>>;
    fn columns(&self, tables: BTreeSet<String>) -> BoxFuture<'_, Result<HashMap<String, Vec<String>>>>;
    fn measure_views<'a>(&'a self, sql: &'a str) -> BoxFuture<'a, Result<Arc<HashMap<String, Arc<Definition>>>>>;
    fn relation<'a>(&'a self, name: &'a str) -> BoxFuture<'a, Option<Relation<dyn Front>>>;
    fn enum_labels<'a>(&'a self, name: &'a str) -> BoxFuture<'a, Option<Vec<String>>>;
    fn ask<'a>(&'a self, sql: &'a str) -> BoxFuture<'a, Result<DataFrame>>;
    fn collect(&self, frame: DataFrame) -> BoxFuture<'_, Result<Vec<RecordBatch>>>;
}

impl<H: Host + 'static> Front for H {
    fn name(&self) -> String { Host::name(self) }
    fn own<'a>(&'a self, sql: &'a str) -> BoxFuture<'a, Result<Own>> { Box::pin(Host::own(self, sql)) }
    fn routines(&self) -> BoxFuture<'_, Result<Routines>> { Box::pin(Host::routines(self)) }
    fn attached(&self) -> BoxFuture<'_, Result<Vec<(String, Attached)>>> { Box::pin(Host::attached(self)) }
    fn columns(&self, tables: BTreeSet<String>) -> BoxFuture<'_, Result<HashMap<String, Vec<String>>>> { Box::pin(Host::columns(self, tables)) }
    fn measure_views<'a>(&'a self, sql: &'a str) -> BoxFuture<'a, Result<Arc<HashMap<String, Arc<Definition>>>>> { Box::pin(Host::measure_views(self, sql)) }
    fn relation<'a>(&'a self, name: &'a str) -> BoxFuture<'a, Option<Relation<dyn Front>>> {
        Box::pin(async move {
            Host::relation(self, name).await.map(|r| match r {
                Relation::Columns(c) => Relation::Columns(c),
                Relation::View { sql, lake } => Relation::View { sql, lake: lake.map(|l| l as Arc<dyn Front>) },
            })
        })
    }
    fn enum_labels<'a>(&'a self, name: &'a str) -> BoxFuture<'a, Option<Vec<String>>> { Box::pin(Host::enum_labels(self, name)) }
    fn ask<'a>(&'a self, sql: &'a str) -> BoxFuture<'a, Result<DataFrame>> { Box::pin(Host::ask(self, sql)) }
    fn collect(&self, frame: DataFrame) -> BoxFuture<'_, Result<Vec<RecordBatch>>> { Box::pin(Host::collect(self, frame)) }
}

// (for any lifetime of the object, not only 'static: an async block's Send check reads its lifetime as any)
impl<'x> Host for dyn Front + 'x {
    fn name(&self) -> String { Front::name(self) }
    fn own(&self, sql: &str) -> impl Future<Output = Result<Own>> + Send { async move { Front::own(self, sql).await } }
    fn routines(&self) -> impl Future<Output = Result<Routines>> + Send { Front::routines(self) }
    fn attached(&self) -> impl Future<Output = Result<Vec<(String, Attached)>>> + Send { Front::attached(self) }
    fn columns(&self, tables: BTreeSet<String>) -> impl Future<Output = Result<HashMap<String, Vec<String>>>> + Send { Front::columns(self, tables) }
    fn measure_views(&self, sql: &str) -> impl Future<Output = Result<Arc<HashMap<String, Arc<Definition>>>>> + Send { async move { Front::measure_views(self, sql).await } }
    fn relation(&self, name: &str) -> impl Future<Output = Option<Relation<Self>>> + Send {
        async move {
            Front::relation(self, name).await.map(|r| match r {
                Relation::Columns(c) => Relation::Columns(c),
                Relation::View { sql, lake } => Relation::View { sql, lake: lake.map(|l| l as Arc<dyn Front + 'x>) },
            })
        }
    }
    fn enum_labels(&self, name: &str) -> impl Future<Output = Option<Vec<String>>> + Send { async move { Front::enum_labels(self, name).await } }
    fn ask(&self, sql: &str) -> impl Future<Output = Result<DataFrame>> + Send { async move { Front::ask(self, sql).await } }
    fn collect(&self, frame: DataFrame) -> impl Future<Output = Result<Vec<RecordBatch>>> + Send { Front::collect(self, frame) }
}

/// What the front knows of a statement when its tree passes run, so a pass reads it here rather than look at the text
/// again: the text the tree was read from (after the text passes), the request's own views, what it may call, the
/// catalogs attached, the views with measures it may read, and two flags a pass may set.
pub struct Facts<'f> {
    /// This lake's name (`Host::name`).
    pub name: String,
    pub sql: &'f str,
    pub views: &'f HashMap<String, String>,
    pub routines: Routines,
    pub outside: Vec<(String, Attached)>,
    pub measure_views: Arc<HashMap<String, Arc<Definition>>>,
    /// The text names a measure, or a view with measures (`measures::wanted`), or the fallback dialect read `AS MEASURE`.
    pub measured: bool,
    /// The tree was read only by the fallback dialect, or as FROM first: it is printed whatever the passes do.
    pub read_apart: bool,
    /// Something in it is expanded: a macro, function or view it calls or names, a FROM first, a name of an attached
    /// catalog, a `pondra.diff` (set by its pass), measures (set by theirs). The expander's pass runs when it is set.
    pub expands: AtomicBool,
}

impl Facts<'_> {
    pub fn expands(&self) -> bool { self.expands.load(Ordering::Relaxed) }
    /// Something a pass wrote is to be expanded too (`expands`).
    pub fn expand(&self) { self.expands.store(true, Ordering::Relaxed) }
}

/// A tree pass: it changes the statements as it needs (true: it changed them), asking the host what it must.
pub type TreeRun = for<'a, 'f> fn(&'a (dyn Front + 'static), &'a mut [Statement], &'a Facts<'f>) -> BoxFuture<'a, Result<bool>>;

/// A tree pass of the front, by its name (`module.look`) and its order among them.
pub struct TreePass {
    pub name: &'static str,
    pub order: u16,
    pub run: TreeRun,
}

/// The registry's tree passes (step 8, PR 9b): what the front runs over a statement's tree once it is parsed, in order.
/// The core's (`pondra.diff`, the expander, DuckDB's and Snowflake's forms) and the extensions' (measures, MATCH_RECOGNIZE,
/// Postgres's casts) are each added by the extension that has them, so the front names none.
#[derive(Default)]
pub struct Trees {
    all: Vec<TreePass>,
}

impl crate::registry::Section for Trees {
    const NAME: &'static str = "tree passes";
}

impl Trees {
    /// Adds a pass. Two can't share an order or a name.
    pub fn add(&mut self, pass: TreePass) {
        assert!(self.all.iter().all(|p| p.name != pass.name), "two tree passes are named {}", pass.name);
        if let Some(other) = self.all.iter().find(|p| p.order == pass.order) {
            panic!("tree passes {} and {} both take order {}", other.name, pass.name, pass.order);
        }
        self.all.push(pass);
        self.all.sort_by_key(|p| p.order);
    }

    /// The passes, in the order the front runs them.
    pub fn all(&self) -> &[TreePass] {
        &self.all
    }
}

/// The section of this build's registry.
pub fn trees() -> &'static Trees {
    crate::registry::get().section::<Trees>()
}

/// The front:
pub async fn expand<H: Host + 'static>(host: &H, sql: &str, views: &HashMap<String, String>) -> Result<String> {
    // (the registry's passes, each where its trigger is in the text, `passes.rs`: first the refusals, by name and with what
    // to write (1.1's statements, `kept.rs`; another database's, `dialect_stmts.rs`; MATCH_RECOGNIZE's SUBSET, `pattern.rs`))
    let passes = crate::passes::get();
    passes.refuse(sql)?;
    let sql = &match Host::own(host, sql).await? {
        Own::Answer(q) => return Ok(q),
        Own::Go(sql) => sql,
    };
    // (`SUMMARIZE t` is the front's for every host, so a page and a node answer it alike)
    if let Some(q) = crate::friendly::summarize(host, sql).await? {
        return Ok(q);
    }
    // (then the text's passes, in order: `spark_sql('…')` as Pondra's SQL; `t AT (VERSION => n)`, ADR-043; `PIVOT t ON g`, `[x FOR x IN l]`, DuckDB's ASOF … ON; FINAL and RUNNING before a
    // measure, a subquery before MATCH_RECOGNIZE: each as SQL that parses)
    let sql: &str = &passes.text(sql)?.into_owned();
    let routines = Host::routines(host).await?;
    let outside = Host::attached(host).await?;
    let named = |n: &String| crate::text::mentions(sql, n);
    let expands = routines.own.iter().any(|(n, r)| r.kind != Kind::Procedure && named(n)) || views.keys().any(named) || crate::expander::FROM_FIRST.is_match(sql) || crate::ext_names::mentions(sql)
        || outside.iter().any(|(n, _)| named(n)) || sql.contains("pondra_at(");
    let as_written = || Ok(crate::expander::named_apart(sql).unwrap_or_else(|| sql.to_string()));
    let measure_views = Host::measure_views(host, sql).await?;
    let measured = crate::measures::wanted(sql, &measure_views); // (a view with measures read, or a measure named)
    if !expands && !measured && !passes.wants(sql)? { // (a tree pass's look: friendly's forms, MATCH_RECOGNIZE, a type of one's own)
        return as_written();
    }
    // (read as Pondra's SQL, the core's language, `parsed::pondra`: parsed once for the front's looks, a text generic refuses
    // read with the fallback dialect's hooks, and DuckDB's `FROM t WHERE …` as `SELECT * FROM t WHERE …`)
    let mut stmts = match crate::parsed::pondra(sql) {
        Ok(stmts) => stmts,
        // (the fallback read further than generic did: its error, which DataFusion's parse of the text never gives)
        Err(e) if crate::parsed::generic(sql).err().is_some_and(|g| g != e) => return Err(datafusion::error::DataFusionError::SQL(Box::new(e), None).into()),
        Err(_) => return if expands { Ok(sql.to_string()) } else { as_written() },
    };
    // (a tree only the fallback dialect read, or the FROM-first reading: printed, as nothing after the front reads the text)
    let read_apart = crate::parsed::generic(sql).is_err();
    // (a view's `sum(x) AS MEASURE revenue`, which the fallback dialect reads as an alias, ADR-065 §3: `measures::passes`)
    let measured = measured || (read_apart && crate::measures::marked(&stmts));
    let facts = Facts { name: Host::name(host), sql, views, routines, outside, measure_views, measured, read_apart, expands: AtomicBool::new(expands) };
    // (the registry's tree passes, in order (`Trees`): `pondra.diff`, the measures at the query's own grain (ADR-065 §3),
    // the expander, MATCH_RECOGNIZE as SQL around one window, Postgres's casts and JSON set functions, DuckDB's and
    // Snowflake's forms; each asks the host as a `Front`)
    let mut changed = read_apart;
    for pass in trees().all() {
        changed |= (pass.run)(host as &dyn Front, &mut stmts, &facts).await?;
    }
    if !changed {
        return as_written();
    }
    Ok(crate::text::text_kept(stmts)) // (kept as the printed text's parse: the engine plans from the tree, `parsed::keep`)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn none<'a>(_: &'a (dyn Front + 'static), _: &'a mut [Statement], _: &'a Facts<'_>) -> BoxFuture<'a, Result<bool>> { Box::pin(async { Ok(false) }) }

    /// The build's tree passes, in the order the front runs them: each extension's own, added by it. Two passes of one
    /// order, or of one name, are refused as the registry is built.
    #[test]
    fn the_builds_tree_passes_in_order() {
        let names: Vec<&str> = trees().all().iter().map(|p| p.name).collect();
        assert_eq!(names, ["diff.calls", "measures.rewrite", "expander.expand", "pattern.rewrite", "pg_casts.rewrite", "friendly.rewrite"]);
        let mut t = Trees::default();
        t.add(TreePass { name: "b", order: 2, run: none });
        t.add(TreePass { name: "a", order: 1, run: none });
        assert_eq!(t.all().iter().map(|p| p.name).collect::<Vec<_>>(), ["a", "b"], "in their order, not as added");
        let order = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| t.add(TreePass { name: "c", order: 1, run: none }))).unwrap_err();
        assert_eq!(order.downcast_ref::<String>().map(String::as_str), Some("tree passes a and c both take order 1"));
        let name = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| t.add(TreePass { name: "a", order: 3, run: none }))).unwrap_err();
        assert_eq!(name.downcast_ref::<String>().map(String::as_str), Some("two tree passes are named a"));
    }
}
