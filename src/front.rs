//! The SQL front: what a statement goes through before anything plans it (macros, DuckDB's and Spark's spellings,
//! FROM first, files by name, …), generic over what it asks of where it runs (`Host`). A node is a host
//! (`host.rs`); a page will be one (designs/l2-b4-front-and-web.md). Each pass runs once a statement or once a name.

use crate::ext::Attached;
use crate::routines::{Expander, Kind, Routine};
use anyhow::Result;
use datafusion::sql::sqlparser::{ast::{self, Statement, VisitMut}, dialect::GenericDialect, parser::Parser};
use std::collections::{BTreeSet, HashMap};
use std::future::Future;
use std::ops::ControlFlow;
use std::sync::Arc;

/// What the front asks of where it runs. Asked once a statement, or once a name, never once a batch.
pub trait Host: Sync {
    /// The lake's name, the first part of a three-part name (`ddl::local_in`).
    fn name(&self) -> String;
    /// The node's own statements, asked before anything else (`SHOW CREATE`, `pondra.find`, `SUMMARIZE`, …).
    fn own(&self, sql: &str) -> impl Future<Output = Result<Own>> + Send;
    /// The lake's macros, functions and procedures, by name (`routines::listed`).
    fn routines(&self) -> impl Future<Output = Result<Arc<HashMap<String, Routine>>>> + Send;
    /// Other engines' catalogs attached (`ext::attached`).
    fn attached(&self) -> impl Future<Output = Result<Vec<(String, Attached)>>> + Send;
    /// The columns of these tables of the lake (names inside it, as `ddl::local_in` gives them), system columns last;
    /// a name with no table is left out.
    fn columns(&self, tables: BTreeSet<String>) -> impl Future<Output = Result<HashMap<String, Vec<String>>>> + Send;
    /// Whether `sql` reads a view with measures or names a measure (`measures::wanted`).
    fn measured(&self, sql: &str) -> impl Future<Output = Result<bool>> + Send;
    /// Measures worked out at the query's own grain (`measures::rewrite`): whether it changed anything.
    fn measures(&self, sql: &str, stmts: &mut [Statement]) -> impl Future<Output = Result<bool>> + Send;
    /// An enum's casts as text (`types::rewrite`): whether it changed anything.
    fn types(&self, stmts: &mut [Statement]) -> impl Future<Output = Result<bool>> + Send;
    /// DuckDB's and Snowflake's forms that ask the data (`friendly::rewrite`): whether it changed anything.
    fn friendly(&self, stmts: &mut [Statement]) -> impl Future<Output = Result<bool>> + Send;
}

/// What the host's own statements made of one: a query to run in its place, or the text to go on with.
pub enum Own {
    Answer(String),
    Go(String),
}

/// The front (`routines::expand_with` before):
pub async fn expand<H: Host>(host: &H, sql: &str, views: &HashMap<String, String>) -> Result<String> {
    let sql = &match host.own(sql).await? {
        Own::Answer(q) => return Ok(q),
        Own::Go(sql) => sql,
    };
    let sql = &crate::sparksql::inline(sql)?; // (`spark_sql('…')`: Spark SQL as Pondra's, then expanded as any)
    let sql = &crate::branch::diffs(sql)?; // (`pondra.diff('prod.t', 'dev.t')`: rows apart, ADR-047)
    let sql = &crate::past::syntax(sql).into_owned(); // (`t AT (VERSION => n)`: a table as it was, ADR-043)
    let sql = &crate::friendly::text(sql)?.into_owned(); // (`PIVOT t ON g`, `[x FOR x IN l]`, DuckDB's ASOF … ON: as SQL that parses)
    let marked = crate::measures::text(sql); // (`sum(x) AS MEASURE revenue`: an alias that parses, ADR-065 §3)
    let sql: &str = &marked;
    let all = host.routines().await?;
    let outside = host.attached().await?;
    let named = |n: &String| crate::ddl::mentions(sql, n);
    let expands = all.iter().any(|(n, r)| r.kind != Kind::Procedure && named(n)) || views.keys().any(named) || crate::routines::FROM_FIRST.is_match(sql) || crate::ext::mentions(sql)
        || outside.iter().any(|(n, _)| named(n)) || sql.contains("pondra_at(");
    let as_written = || Ok(crate::routines::named_apart(sql).unwrap_or_else(|| sql.to_string()));
    let measured = host.measured(sql).await?; // (a view with measures read, or a measure named)
    if !expands && !measured && !crate::friendly::wanted(sql) && !crate::types::wanted(sql) {
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
    // Measures first, at the query's own grain (ADR-065 §3): what they put in (a view's FROM, its macros) is expanded below.
    let expands = (measured && host.measures(sql, &mut stmts).await?) || expands;
    let columns = match expands {
        true => crate::routines::columns(host, &stmts, &all).await?,
        false => HashMap::new(),
    };
    let name = host.name();
    for s in stmts.iter_mut().filter(|_| expands) {
        if !matches!(s, Statement::CreateMacro { .. } | Statement::CreateFunction(_) | Statement::CreateView(ast::CreateView { materialized: false, .. })) {
            if let ControlFlow::Break(e) = s.visit(&mut Expander { name: &name, all: &all, views, outside: &outside, depth: 0, own: 0, columns: &columns, scopes: vec![] }) {
                return Err(e);
            }
        }
    }
    let typed = host.types(&mut stmts).await?; // (`'sad'::mood`, `enum_range(NULL::mood)`: an enum's casts as text)
    // DuckDB's and Snowflake's forms (`friendly.rs`): a text sent on as it was when none is in it
    let friendly = host.friendly(&mut stmts).await?;
    if !expands && !friendly && !typed {
        return as_written();
    }
    Ok(crate::routines::text(&stmts))
}
