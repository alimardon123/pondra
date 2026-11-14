//! A definition read in another database: a view's stored query, or a query, with the tables of its own database named
//! in `db`, and the views of its own database it reads inlined, each read the same way. `pondra.diff`'s `data => 'db'`
//! reads both sides so (Trellis T1c), and a session reads another database's view so by its name there
//! (`query::their_views`). Another database's names, a CTE's, a table function's, a session's temporary table and a name the
//! database doesn't know are as written. The names are looked up first (the host is asked, `redirect`), then the query
//! is rewritten by a pure function (`redirected`).
use crate::front::{Host, Relation};
use anyhow::Result;
use datafusion::sql::sqlparser::ast::{self, TableFactor, VisitorMut};
use std::collections::{BTreeSet, HashMap};
use std::future::Future;
use std::ops::ControlFlow;
use std::pin::Pin;

/// Whether `name` is the session's temporary table, which shadows a table of its name and is no database's.
#[cfg_attr(not(feature = "lake"), allow(unused_variables))] // (a lake-less build keeps no session's tables)
pub(crate) fn temporary(name: &str) -> bool {
    #[cfg(feature = "lake")]
    return crate::temp::columns_of(name).is_some();
    #[cfg(not(feature = "lake"))]
    false
}

/// A name's part as SQL reads it back: bare when it is a plain lower-case word, else in double quotes.
pub(crate) fn part(p: &str) -> ast::Ident {
    let plain = p.chars().next().is_some_and(|c| c.is_ascii_lowercase() || c == '_') && p.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
    if plain { ast::Ident::new(p) } else { ast::Ident::with_quote('"', p) }
}

/// How deep views may read views when a definition runs over another database's tables (`redirect`).
const DEEPEST: usize = 8;

/// What a name in a definition is, to the host that reads it (`redirect`).
#[derive(Clone, Debug, PartialEq)]
enum Found {
    /// A table of the host's own database.
    Table,
    /// A view of the host's own database, with its query already over the other database's tables.
    View(String),
}

/// A definition's query with its own database's tables read from `db`'s, and each view of its own
/// database it reads inlined, read the same way. Another database's tables and views, a CTE's name, a table function
/// and a name the host doesn't know are as written. The names are looked up first (the host is asked), then the query
/// is rewritten (`redirected`).
pub(crate) fn redirect<'a, H: Host>(host: &'a H, sql: String, db: String, depth: usize) -> Pin<Box<dyn Future<Output = Result<String>> + Send + 'a>> {
    Box::pin(async move {
        if depth > DEEPEST {
            return Err(crate::codes::coded("54000", format!("a definition read over another database's tables inlines the views it reads, and these read views more than {DEEPEST} deep")));
        }
        let mut q = crate::expander::parse_query(&sql)?;
        let lake = host.name();
        let mut found = HashMap::new();
        for name in read_names(&q) {
            if crate::text::local_in(&lake, &name).is_none() || temporary(&name) {
                continue; // (another database's, or the session's: read as written)
            }
            match host.relation(&name).await {
                Some(Relation::Columns(_)) => {
                    found.insert(name, Found::Table);
                }
                Some(Relation::View { sql, lake: None }) => {
                    let inner = redirect(host, sql, db.clone(), depth + 1).await?;
                    found.insert(name, Found::View(inner));
                }
                _ => {}
            }
        }
        redirected(&mut q, &lake, &db, &found)?;
        Ok(q.to_string())
    })
}

/// The names a query reads as tables (no table function's, no CTE's of the query), as `text::object` writes them.
pub(crate) fn read_names(q: &ast::Query) -> BTreeSet<String> {
    struct Names {
        ctes: BTreeSet<String>,
        read: BTreeSet<String>,
    }
    impl ast::Visitor for Names {
        type Break = ();
        fn pre_visit_query(&mut self, q: &ast::Query) -> ControlFlow<()> {
            if let Some(w) = &q.with {
                self.ctes.extend(w.cte_tables.iter().map(|c| crate::text::ident(&c.alias.name)));
            }
            ControlFlow::Continue(())
        }
        fn pre_visit_table_factor(&mut self, t: &TableFactor) -> ControlFlow<()> {
            if let TableFactor::Table { name, args: None, .. } = t {
                self.read.insert(crate::text::object(name));
            }
            ControlFlow::Continue(())
        }
    }
    let mut n = Names { ctes: BTreeSet::new(), read: BTreeSet::new() };
    let _ = crate::walk::visit(q, &mut n);
    n.read.into_iter().filter(|r| r.contains('.') || !n.ctes.contains(r)).collect()
}

/// `q` with each name `found` knows rewritten: a table of `lake` named in `db` (`s.t` and `lake.s.t` as `db.s.t`, `t` as
/// `db.public.t`), a view as its query in parentheses, under the name's last part unless it has an alias. Pure: the host
/// was asked already.
fn redirected(q: &mut ast::Query, lake: &str, db: &str, found: &HashMap<String, Found>) -> Result<()> {
    struct Put<'a> {
        lake: &'a str,
        db: &'a str,
        found: &'a HashMap<String, Found>,
    }
    impl VisitorMut for Put<'_> {
        type Break = anyhow::Error;
        fn post_visit_table_factor(&mut self, t: &mut TableFactor) -> ControlFlow<Self::Break> {
            let TableFactor::Table { name, args: None, alias, .. } = t else { return ControlFlow::Continue(()) };
            match self.found.get(&crate::text::object(name)) {
                Some(Found::Table) => {
                    let parts: Vec<ast::Ident> = name.0.iter().filter_map(|p| p.as_ident().cloned()).collect();
                    let db = || part(&crate::text::database(self.db));
                    let named = match &parts[..] {
                        [l, s, t] if crate::text::ident(l) == self.lake => vec![db(), s.clone(), t.clone()],
                        [s, t] => vec![db(), s.clone(), t.clone()],
                        [t] => vec![db(), ast::Ident::new(crate::text::PUBLIC), t.clone()],
                        _ => return ControlFlow::Continue(()),
                    };
                    *name = ast::ObjectName::from(named);
                }
                Some(Found::View(sql)) => {
                    let subquery = match crate::expander::parse_query(sql) {
                        Ok(q) => q,
                        Err(e) => return ControlFlow::Break(e),
                    };
                    let last = name.0.last().and_then(|p| p.as_ident()).cloned().unwrap_or_else(|| ast::Ident::new("v"));
                    let alias = alias.take().or(Some(ast::TableAlias { explicit: true, name: last, columns: vec![], at: None }));
                    *t = TableFactor::Derived { lateral: false, subquery: Box::new(subquery), alias, sample: None };
                }
                None => {}
            }
            ControlFlow::Continue(())
        }
    }
    match crate::walk::visit_mut(q, &mut Put { lake, db, found }) {
        ControlFlow::Break(e) => Err(e),
        ControlFlow::Continue(()) => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The look-ups: the names a definition reads as tables, not its CTEs' or a table function's.
    #[test]
    fn a_definition_reads_the_names_it_names() {
        let names = |sql: &str| read_names(&crate::expander::parse_query(sql).unwrap()).into_iter().collect::<Vec<_>>();
        assert_eq!(names("WITH t AS (SELECT * FROM sales.orders) SELECT * FROM t JOIN public.t ON true"), ["public.t", "sales.orders"]);
        assert_eq!(names("SELECT * FROM range(3) r, (SELECT * FROM s.x) y WHERE r.value IN (SELECT n FROM S.Y)"), ["s.x", "s.y"]);
    }

    /// The rewrite, once the host has said what each name is: its own database's tables read from `db`, its
    /// views inlined (their own queries rewritten already), and anything else as written.
    #[test]
    fn a_definition_reads_another_databases_tables() {
        let found: HashMap<String, Found> = [
            ("sales.orders".to_string(), Found::Table),
            ("prod.sales.items".to_string(), Found::Table),
            ("lines".to_string(), Found::Table),
            ("sales.v".to_string(), Found::View("SELECT id FROM dev.sales.orders".to_string())),
        ]
        .into_iter()
        .collect();
        let run = |sql: &str, db: &str| {
            let mut q = crate::expander::parse_query(sql).unwrap();
            redirected(&mut q, "prod", db, &found).unwrap();
            q.to_string()
        };
        assert_eq!(run("SELECT * FROM sales.orders", "dev"), "SELECT * FROM dev.sales.orders");
        assert_eq!(run("SELECT * FROM prod.sales.items AS i", "dev"), "SELECT * FROM dev.sales.items AS i");
        assert_eq!(run("SELECT * FROM lines", "dev"), "SELECT * FROM dev.public.lines");
        assert_eq!(run("SELECT v.id FROM sales.v", "dev"), "SELECT v.id FROM (SELECT id FROM dev.sales.orders) AS v");
        assert_eq!(run("SELECT w.id FROM sales.v AS w", "dev"), "SELECT w.id FROM (SELECT id FROM dev.sales.orders) AS w");
        assert_eq!(run("SELECT * FROM other.sales.orders", "dev"), "SELECT * FROM other.sales.orders", "another database's: as written");
        assert_eq!(run("SELECT * FROM sales.unknown", "dev"), "SELECT * FROM sales.unknown", "a name the host didn't know: as written");
        assert_eq!(run("SELECT * FROM sales.orders", r#""My-Dev""#), r#"SELECT * FROM "my-dev".sales.orders"#, "a database is named in lower case");
        assert_eq!(
            run("SELECT o.id FROM sales.orders AS o WHERE o.id IN (SELECT id FROM sales.v)", "dev"),
            "SELECT o.id FROM dev.sales.orders AS o WHERE o.id IN (SELECT id FROM (SELECT id FROM dev.sales.orders) AS v)"
        );
    }
}
