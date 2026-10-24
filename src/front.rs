//! The SQL front: what a statement goes through before anything plans it (macros, DuckDB's and Spark's spellings,
//! FROM first, files by name, …), generic over what it asks of where it runs (`Host`). A node is a host
//! (`host.rs`), and so is a browser page (`page.rs`). Each pass runs once a statement or once a name.

use crate::ext_names::Attached;
use crate::expander::{Expander, Kind, Routine};
use crate::measures::Definition;
use anyhow::Result;
use datafusion::prelude::DataFrame;
use datafusion::sql::sqlparser::ast::Statement;
use datafusion::sql::sqlparser::{ast::{self, VisitMut}, dialect::GenericDialect, parser::Parser};
use std::collections::{BTreeSet, HashMap};
use std::future::Future;
use std::ops::ControlFlow;
use std::sync::Arc;

/// What the front asks of where it runs. Asked once a statement, or once a name, never once a batch. The front runs the
/// core passes itself (the host's own statements, the measures, DuckDB's and Snowflake's forms), so every host answers
/// them alike; a host answers only what it keeps (routines, attached catalogs, the views with measures it can read,
/// types of its own). Every method is required, so a host says what it keeps rather than inherit a silent "none"
/// (reviews/form-parity.md §2).
pub trait Host: Sync {
    /// The lake's name, the first part of a three-part name (`text::local_in`).
    fn name(&self) -> String;
    /// The host's own statements, asked before anything else (a node's `SHOW CREATE`, `pondra.find`, `RESTORE`, …):
    /// `Own::Go` with the text when it has none.
    fn own(&self, sql: &str) -> impl Future<Output = Result<Own>> + Send;
    /// The lake's macros, functions and procedures, by name (`routines::listed`).
    fn routines(&self) -> impl Future<Output = Result<Arc<HashMap<String, Routine>>>> + Send;
    /// Other engines' catalogs attached (`ext::attached`).
    fn attached(&self) -> impl Future<Output = Result<Vec<(String, Attached)>>> + Send;
    /// The columns of these tables of the lake (names inside it, as `text::local_in` gives them), system columns last;
    /// a name with no table is left out.
    fn columns(&self, tables: BTreeSet<String>) -> impl Future<Output = Result<HashMap<String, Vec<String>>>> + Send;
    /// The views with measures a statement may read, by every name it may give one (`sales`, `crm.sales`, an
    /// attached lake's `other.sales` and `other.public.sales`): a node's from its lake's views.
    fn measure_views(&self, sql: &str) -> impl Future<Output = Result<Arc<HashMap<String, Arc<Definition>>>>> + Send;
    /// An enum's casts as text (`types::rewrite`): whether it changed anything.
    fn types(&self, stmts: &mut [Statement]) -> impl Future<Output = Result<bool>> + Send;
    /// A query the front's passes ask before they can write theirs (`friendly`: a `PIVOT`'s values, the columns
    /// `COLUMNS(…)` and `SUMMARIZE` read), planned as the host plans a user's query, after its front. Its rows are
    /// read only if the pass needs them.
    fn ask(&self, sql: &str) -> impl Future<Output = Result<DataFrame>> + Send;
}

/// What the host's own statements made of one: a query to run in its place, or the text to go on with.
pub enum Own {
    #[cfg_attr(not(feature = "server"), allow(dead_code))] // (only a node's own statements answer: a lake-less build has none)
    Answer(String),
    Go(String),
}

/// The front:
pub async fn expand<H: Host>(host: &H, sql: &str, views: &HashMap<String, String>) -> Result<String> {
    let sql = &match host.own(sql).await? {
        Own::Answer(q) => return Ok(q),
        Own::Go(sql) => sql,
    };
    // (`SUMMARIZE t` is the front's for every host, so a page and a node answer it alike)
    if let Some(q) = crate::friendly::summarize(host, sql).await? {
        return Ok(q);
    }
    let sql = &crate::sparksql::inline(sql)?; // (`spark_sql('…')`: Spark SQL as Pondra's, then expanded as any)
    let sql = &crate::diff::diffs(sql)?; // (`pondra.diff('prod.t', 'dev.t')`: rows apart, ADR-047)
    let sql = &crate::past_names::syntax(sql).into_owned(); // (`t AT (VERSION => n)`: a table as it was, ADR-043)
    let sql = &crate::friendly::text(sql)?.into_owned(); // (`PIVOT t ON g`, `[x FOR x IN l]`, DuckDB's ASOF … ON: as SQL that parses)
    let marked = crate::measures::text(sql); // (`sum(x) AS MEASURE revenue`: an alias that parses, ADR-065 §3)
    let sql: &str = &marked;
    let all = host.routines().await?;
    let outside = host.attached().await?;
    let named = |n: &String| crate::text::mentions(sql, n);
    let expands = all.iter().any(|(n, r)| r.kind != Kind::Procedure && named(n)) || views.keys().any(named) || crate::expander::FROM_FIRST.is_match(sql) || crate::ext_names::mentions(sql)
        || outside.iter().any(|(n, _)| named(n)) || sql.contains("pondra_at(");
    let as_written = || Ok(crate::expander::named_apart(sql).unwrap_or_else(|| sql.to_string()));
    let measure_views = host.measure_views(sql).await?;
    let measured = crate::measures::wanted(sql, &measure_views); // (a view with measures read, or a measure named)
    if !expands && !measured && !crate::friendly::wanted(sql) && !crate::text::names_a_type(sql) {
        return as_written();
    }
    // DuckDB's `FROM t WHERE …` (FROM first, with clauses after it): `SELECT * FROM t WHERE …`.
    static LEADING_FROM: std::sync::LazyLock<regex::Regex> =
        std::sync::LazyLock::new(|| regex::Regex::new(r"(?is)^\s*(?:(?:--[^\n]*\n|/\*.*?\*/)\s*)*from\b").expect("a regex"));
    let parsed = Parser::parse_sql(&GenericDialect {}, sql).or_else(|e| match LEADING_FROM.is_match(sql) {
        true => Parser::parse_sql(&GenericDialect {}, &format!("SELECT * {sql}")),
        false => Err(e),
    });
    let Ok(mut stmts) = parsed else { return if expands { Ok(sql.to_string()) } else { as_written() } };
    let name = host.name();
    let me = format!("{name}.");
    // Measures first, at the query's own grain (ADR-065 §3): what they put in (a view's FROM, its macros) is expanded below.
    let expands = (measured && crate::measures::rewrite_with(&mut stmts, &|n| crate::measures::candidates(&crate::text::object(n), &me).iter().find_map(|c| measure_views.get(c).cloned()), &|n| all.contains_key(n))?) || expands;
    let columns = match expands {
        true => crate::expander::columns(host, &stmts, &all).await?,
        false => HashMap::new(),
    };
    for s in stmts.iter_mut().filter(|_| expands) {
        if !matches!(s, Statement::CreateMacro { .. } | Statement::CreateFunction(_) | Statement::CreateView(ast::CreateView { materialized: false, .. })) {
            if let ControlFlow::Break(e) = s.visit(&mut Expander { name: &name, all: &all, views, outside: &outside, depth: 0, own: 0, columns: &columns, scopes: vec![] }) {
                return Err(e);
            }
        }
    }
    let typed = host.types(&mut stmts).await?; // (`'sad'::mood`, `enum_range(NULL::mood)`: an enum's casts as text)
    // DuckDB's and Snowflake's forms (`friendly.rs`): a text sent on as it was when none is in it
    let friendly = crate::friendly::rewrite(host, &mut stmts).await?;
    if !expands && !friendly && !typed {
        return as_written();
    }
    Ok(crate::text::text(&stmts))
}
