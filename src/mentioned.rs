//! What a statement names (redesign step 8's seam with the session tiers, l6/step8-prep.md): read once from the tree the
//! front made of it, the one the engine plans from, so the per-query tier (`query::session_reading`) registers what the
//! statement names rather than match the text's words again for each table and column it might name. A name that is
//! only in a string or a comment is no name here. A text the parser doesn't take has none: the tier reads its words, as
//! it did before (`query::named_records`).
use datafusion::common::TableReference;
use datafusion::sql::parser::{CopyToSource, CopyToStatement, Statement as DFStatement};
use datafusion::sql::sqlparser::ast::{Expr, ObjectName, Statement, TableFactor, Visitor};
use datafusion::sql::sqlparser::tokenizer::Token;
use std::collections::BTreeSet;
use std::ops::ControlFlow;

/// What one statement names, and the views and temporary views it reads (`union`).
#[derive(Debug, Default, Clone, PartialEq)]
pub(crate) struct Mentioned {
    /// Each relation it reads, CTEs left out: as DataFusion's planner asks its schemas for it (an unquoted part lower-cased,
    /// when the session says so) and as written (`resolve_table_references`).
    pub tables: BTreeSet<TableReference>,
    /// The relations whose name is written quoted, as written: a staged table is read only where it is named so.
    pub quoted: BTreeSet<TableReference>,
    /// Each function it calls (scalar, aggregate, window or table), by the last part of its name, lower-cased.
    pub functions: BTreeSet<String>,
    /// The statement lists every table: it reads `information_schema`, or is SHOW TABLES or SHOW COLUMNS. Its own only: a
    /// view over `information_schema` reads it as a view does (`union`).
    pub every: bool,
    /// It names a system column (`_row_id`, `_version`, `_created_at`, `_updated_at`; `columns`).
    pub system_columns: bool,
    /// It names a keyed table's `_deleted` (`columns`).
    pub deleted: bool,
}

impl Mentioned {
    /// What `stmt`, the parse of `sql`, names; None where DataFusion can't resolve a relation's name (more than three parts).
    /// `normalize`: the session's `enable_ident_normalization`.
    pub(crate) fn of(sql: &str, stmt: &DFStatement, normalize: bool) -> Option<Mentioned> {
        use datafusion::sql::resolve::resolve_table_references;
        let tables = resolve_table_references(stmt, normalize).ok()?.0.into_iter().chain(resolve_table_references(stmt, false).ok()?.0).collect();
        let mut names = Names::default();
        let _ = inner(stmt, &mut names);
        let (system_columns, deleted) = columns(sql);
        Some(Mentioned { tables, quoted: names.quoted, functions: names.functions, every: names.every, system_columns, deleted })
    }

    /// What `sql` names, its statements' read as the engine reads them (`parsed::generic`, kept in the statement's context);
    /// None for a text the parser doesn't take.
    pub(crate) fn of_text(sql: &str, normalize: bool) -> Option<Mentioned> {
        let mut all: Option<Mentioned> = None;
        for s in crate::parsed::generic(sql).ok()? {
            let m = Mentioned::of(sql, &DFStatement::Statement(Box::new(s)), normalize)?;
            match &mut all {
                Some(a) => a.union(m),
                None => all = Some(m),
            }
        }
        all
    }

    /// `other`'s names added: a view's, or a temporary view's, the statement reads. `every` stays the statement's own.
    pub(crate) fn union(&mut self, other: Mentioned) {
        self.tables.extend(other.tables);
        self.quoted.extend(other.quoted);
        self.functions.extend(other.functions);
        self.system_columns |= other.system_columns;
        self.deleted |= other.deleted;
    }
}

/// Whether `sql` names a system column, and whether it names `_deleted`: as a word of the SQL (a name, quoted or not),
/// not of a string or a comment. The text's tokens are read only where its bytes hold one of those words.
pub(crate) fn columns(sql: &str) -> (bool, bool) {
    let has = |w: &str| crate::text::contains_ci(sql, w);
    let (system, deleted) = (crate::text::NAMES.iter().any(|n| has(n)), has("_deleted"));
    if !system && !deleted {
        return (false, false);
    }
    let Some(tokens) = crate::text::tokens(sql) else { return (system, deleted) }; // (a text the tokenizer refuses: its words)
    let word = |names: &[&str]| tokens.toks.iter().any(|t| matches!(t, Token::Word(w) if names.iter().any(|n| w.value.eq_ignore_ascii_case(n))));
    (system && word(&crate::text::NAMES), deleted && word(&["_deleted"]))
}

/// The parts of a statement the walk reads names in: a query's, a COPY's query, what an EXPLAIN explains.
fn inner(stmt: &DFStatement, names: &mut Names) -> ControlFlow<()> {
    match stmt {
        DFStatement::Statement(s) => crate::walk::visit(s.as_ref(), names),
        DFStatement::CopyTo(CopyToStatement { source: CopyToSource::Query(q), .. }) => crate::walk::visit(q.as_ref(), names),
        DFStatement::Explain(e) => inner(&e.statement, names),
        _ => ControlFlow::Continue(()), // (COPY t TO …, CREATE EXTERNAL TABLE, RESET: their relation is in `tables`)
    }
}

/// The walk's findings, but the relations, which DataFusion's own walk resolves (`Mentioned::of`).
#[derive(Default)]
struct Names {
    quoted: BTreeSet<TableReference>,
    functions: BTreeSet<String>,
    every: bool,
}

/// The last part of a name, lower-cased.
fn last(name: &ObjectName) -> Option<String> { name.0.last().and_then(|p| p.as_ident()).map(|i| i.value.to_lowercase()) }

impl Visitor for Names {
    type Break = ();

    fn pre_visit_statement(&mut self, s: &Statement) -> ControlFlow<()> {
        self.every |= matches!(s, Statement::ShowTables { .. } | Statement::ShowColumns { .. });
        if let Statement::Call(f) = s {
            self.functions.extend(last(&f.name)); // (`CALL p(…)`: its function is no expression)
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_relation(&mut self, name: &ObjectName) -> ControlFlow<()> {
        let parts: Vec<_> = name.0.iter().filter_map(|p| p.as_ident()).collect();
        self.every |= parts.iter().any(|i| i.value.eq_ignore_ascii_case("information_schema"));
        if parts.last().is_some_and(|i| i.quote_style.is_some()) {
            if let Ok(r) = datafusion::sql::planner::object_name_to_table_reference(name.clone(), false) {
                self.quoted.insert(r);
            }
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_table_factor(&mut self, t: &TableFactor) -> ControlFlow<()> {
        // (a table function: `read_parquet('…')`, `generate_series(1, 3)`, LATERAL `f(…)`)
        if let TableFactor::Table { name, args: Some(_), .. } | TableFactor::Function { name, .. } = t {
            self.functions.extend(last(name));
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, e: &Expr) -> ControlFlow<()> {
        if let Expr::Function(f) = e {
            self.functions.extend(last(&f.name));
        }
        ControlFlow::Continue(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn of(sql: &str) -> Mentioned {
        let mut stmts = crate::parsed::generic(sql).expect("it parses");
        assert_eq!(stmts.len(), 1, "{sql}");
        Mentioned::of(sql, &DFStatement::Statement(Box::new(stmts.remove(0))), true).expect("its names resolve")
    }

    fn set<T: Ord + Clone>(items: &[T]) -> BTreeSet<T> { items.iter().cloned().collect() }

    #[test]
    fn a_statement_names_its_tables_as_the_planner_asks_for_them_and_as_written() {
        let m = of("WITH c AS (SELECT * FROM a) SELECT * FROM c JOIN s.\"MiXed\" ON true WHERE x IN (SELECT x FROM B)");
        let r = |s: Option<&str>, t: &str| match s {
            Some(s) => TableReference::partial(s, t),
            None => TableReference::bare(t),
        };
        assert_eq!(m.tables, set(&[r(None, "a"), r(Some("s"), "MiXed"), r(None, "b"), r(None, "B")]), "the CTE left out; `B` both ways");
        assert_eq!(m.quoted, set(&[r(Some("s"), "MiXed")]));
        assert!(!m.every && !m.system_columns && !m.deleted);
        assert_eq!(of("SELECT * FROM \"a$new-0123abcd\"").quoted, set(&[r(None, "a$new-0123abcd")]), "a staged table, named quoted");
    }

    #[test]
    fn a_statement_names_the_functions_it_calls() {
        let m = of("SELECT upper(s), count(*), row_number() OVER () FROM read_parquet('f.parquet'), generate_series(1, 3) WHERE pondra.my_fn(x)");
        assert_eq!(m.functions, set(&["upper", "count", "row_number", "read_parquet", "generate_series", "my_fn"].map(String::from)));
        assert!(of("SELECT 'upper(s)' -- lower(t)").functions.is_empty(), "a string's and a comment's words call nothing");
        assert_eq!(of("CALL s.refresh(1)").functions, set(&["refresh".to_string()]), "a procedure called");
    }

    #[test]
    fn a_listing_names_every_table() {
        for sql in ["SELECT * FROM information_schema.tables", "SELECT * FROM INFORMATION_SCHEMA.columns", "SHOW TABLES", "SHOW COLUMNS FROM t"] {
            assert!(of(sql).every, "{sql}");
        }
        assert!(!of("SELECT 'information_schema' AS s").every, "a word in a string lists nothing");
    }

    #[test]
    fn a_system_column_or_deleted_is_named_as_a_name_not_in_a_string() {
        let flags = |sql: &str| (of(sql).system_columns, of(sql).deleted);
        assert_eq!(flags("SELECT _row_id, x FROM t"), (true, false));
        assert_eq!(flags("SELECT * FROM t WHERE \"_UPDATED_AT\" > now() AND t._deleted"), (true, true));
        assert_eq!(flags("SELECT * FROM t JOIN u USING (_version)"), (true, false), "a name outside any expression too");
        assert_eq!(flags("SELECT * FROM t WHERE note = '_row_id' OR note = '_deleted' -- _version"), (false, false));
        assert_eq!(flags("SELECT _row_ids FROM t"), (false, false), "a word, not a part of one");
        assert_eq!(columns("SELECT _row_id FROM t WHERE s = 'it''s"), (true, false), "a text the tokenizer refuses: its words");
    }

    #[test]
    fn a_view_adds_its_names_but_lists_nothing() {
        let mut m = of("SELECT * FROM v");
        m.union(of("SELECT _deleted, f(x) FROM information_schema.tables"));
        assert!(m.deleted && m.functions.contains("f") && !m.every, "{m:?}");
        assert!(m.tables.contains(&TableReference::partial("information_schema", "tables")));
    }
}
