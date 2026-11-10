//! The languages a statement may be written in (step 8, PR 7; ADR-075 §4a): each turns its text into Pondra's tree, so
//! the planner, the rights and the tree passes see one language, and a language picks only the parser. Pondra's SQL is the
//! core's (`pondra`, `engine::ENGINE`); Spark SQL is the `spark` extension's (`sparksql.rs`). A session picks one with
//! `SET language = 'spark'` (`set.rs`); a query in another is that language's table function in a FROM (`spark_sql('…')`,
//! `inline`), found here by its name.
use anyhow::Result;
use datafusion::sql::sqlparser::ast::{self, Expr, FunctionArg, FunctionArgExpr, SetExpr, Statement, TableFactor, VisitorMut};
use std::ops::ControlFlow;

/// A language: what it is called, and its text as Pondra's statements.
pub struct Language {
    /// Its name, as `SHOW language` answers.
    pub name: &'static str,
    /// The other names `SET language` takes for it (`databricks`, Spark SQL's), in lower case.
    pub names: &'static [&'static str],
    /// Its text, read with its grammar and turned into Pondra's statements.
    pub parse: fn(&str) -> Result<Vec<Statement>>,
    /// The table function whose argument is a query in it, in lower case (`spark_sql`): None, it has none.
    pub inline: Option<&'static str>,
    /// What it is, in a few words, for the languages listed where one is asked for by a name none has.
    pub about: &'static str,
}

/// The registry's languages section.
#[derive(Default)]
pub struct Languages(Vec<&'static Language>);

impl crate::registry::Section for Languages {
    const NAME: &'static str = "languages";

    /// Two languages can't answer to one name, nor share a table function, and each table function has a text pass
    /// whose trigger is its name (`inline` runs only where one is written).
    fn check(&self, r: &crate::registry::Registry) -> Result<(), String> {
        let mut seen = std::collections::HashSet::new();
        for l in &self.0 {
            for n in std::iter::once(l.name).chain(l.names.iter().copied()).chain(l.inline) {
                if n != n.to_ascii_lowercase() || !seen.insert(n) {
                    return Err(format!("{n}: a language's name or table function is lower case and no other language's"));
                }
            }
            if let Some(f) = l.inline {
                if !r.find::<crate::passes::Passes>().is_some_and(|p| p.marked(f)) {
                    return Err(format!("{}'s table function {f} has no text pass that it triggers", l.name));
                }
            }
        }
        Ok(())
    }
}

impl Languages {
    pub fn add(&mut self, l: &'static Language) {
        self.0.push(l);
    }

    /// The language that answers to this name, in any case.
    pub fn named(&self, n: &str) -> Option<&'static Language> {
        let n = n.trim().to_ascii_lowercase();
        self.0.iter().copied().find(|l| l.name == n || l.names.contains(&n.as_str()))
    }

    /// The languages by name, each with what it is: `pondra (Pondra's SQL) or spark (Spark SQL)`.
    pub fn listed(&self) -> String {
        let mut all = self.0.iter().map(|l| format!("{} ({})", l.name, l.about)).collect::<Vec<_>>();
        all.sort();
        match all.split_last() {
            Some((last, [])) => last.clone(),
            Some((last, rest)) => format!("{} or {last}", rest.join(", ")),
            None => String::new(),
        }
    }

    fn of_function(&self, f: &str) -> Option<&'static Language> {
        self.0.iter().copied().find(|l| l.inline == Some(f))
    }
}

/// The section of this build's registry.
pub fn get() -> &'static Languages {
    crate::registry::get().section::<Languages>()
}

/// Every language's table function in a FROM (`spark_sql('…')`), replaced by its query as that language reads it: a
/// statement that names none is given back as it is. What comes after reads the query as Pondra's SQL: the door places its
/// names (`path::door`), a routine's call binds its parameters (`routines::bind`), and the front expands it (`front.rs`, a
/// text pass of the language's extension that runs this).
pub fn inline(sql: &str) -> Result<String> {
    let languages = get();
    if !languages.0.iter().filter_map(|l| l.inline).any(|f| crate::text::contains_ci(sql, f)) {
        return Ok(sql.to_string());
    }
    let Ok(mut stmts) = crate::parsed::generic(sql) else { return Ok(sql.to_string()) }; // (its own error later, as written)
    let mut found = Found { languages, any: false };
    if let ControlFlow::Break(e) = crate::walk::visit_mut(&mut stmts, &mut found) {
        return Err(e);
    }
    Ok(match found.any {
        true => stmts.iter().map(crate::text::sql).collect::<Vec<_>>().join(";\n"),
        false => sql.to_string(),
    })
}

struct Found {
    languages: &'static Languages,
    any: bool,
}

impl Found {
    /// The query of a language's table function in a FROM, as Pondra's (None: another table).
    fn query(&self, t: &TableFactor) -> Option<Result<ast::Query>> {
        let TableFactor::Table { name, args: Some(args), .. } = t else { return None };
        let f = crate::text::object(name);
        let l = self.languages.of_function(&f)?;
        let text = match &args.args[..] {
            [FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(v)))] => match &v.value {
                ast::Value::SingleQuotedString(s) | ast::Value::DollarQuotedString(ast::DollarQuotedString { value: s, .. }) => Some(s),
                _ => None,
            },
            _ => None,
        };
        let Some(text) = text else { return Some(Err(anyhow::anyhow!("{f} takes the query as a string: {f}('SELECT …')"))) };
        Some((l.parse)(text).and_then(|mut stmts| {
            anyhow::ensure!(stmts.len() == 1, "{f} takes one query, not {}", stmts.len());
            match stmts.remove(0) {
                Statement::Query(q) => Ok(*q),
                _ => anyhow::bail!("{f} takes a query (SELECT, WITH, VALUES): other statements are Pondra's SQL"),
            }
        }))
    }
}

impl VisitorMut for Found {
    type Break = anyhow::Error;

    /// `SELECT * FROM spark_sql('…')` alone is that query itself, so its ORDER BY holds (a subquery's sort is dropped, as
    /// SQL does, and Spark may sort by a column it leaves out).
    fn pre_visit_query(&mut self, q: &mut ast::Query) -> ControlFlow<Self::Break> {
        let SetExpr::Select(s) = q.body.as_ref() else { return ControlFlow::Continue(()) };
        let [from] = &s.from[..] else { return ControlFlow::Continue(()) };
        if !from.joins.is_empty() || q.to_string() != format!("SELECT * FROM {}", from.relation) {
            return ControlFlow::Continue(());
        }
        match self.query(&from.relation) {
            Some(Ok(to)) => {
                *q = to;
                self.any = true;
                ControlFlow::Continue(())
            }
            Some(Err(e)) => ControlFlow::Break(e),
            None => ControlFlow::Continue(()),
        }
    }

    fn post_visit_table_factor(&mut self, t: &mut TableFactor) -> ControlFlow<Self::Break> {
        match self.query(t) {
            Some(Ok(q)) => {
                let TableFactor::Table { name, alias, .. } = t else { unreachable!() };
                let alias = alias.clone().or_else(|| Some(ast::TableAlias { explicit: true, name: ast::Ident::new(crate::text::object(name)), columns: vec![], at: None }));
                *t = TableFactor::Derived { lateral: false, subquery: Box::new(q), alias, sample: None };
                self.any = true;
                ControlFlow::Continue(())
            }
            Some(Err(e)) => ControlFlow::Break(e),
            None => ControlFlow::Continue(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::{BuiltIn, Registry};

    fn none(_: &str) -> Result<Vec<Statement>> { Ok(vec![]) }

    #[test]
    fn a_name_is_one_languages() {
        static A: Language = Language { name: "a", names: &["b"], parse: none, inline: None, about: "A" };
        static B: Language = Language { name: "b", names: &[], parse: none, inline: None, about: "B" };
        static UPPER: Language = Language { name: "c", names: &["Dee"], parse: none, inline: None, about: "C" };
        static UNMARKED: Language = Language { name: "e", names: &[], parse: none, inline: Some("e_sql"), about: "E" };
        for (two, said) in [([&A, &B], "b: a language's name"), ([&A, &UPPER], "Dee: a language's name"), ([&A, &UNMARKED], "e's table function e_sql has no text pass")] {
            let built = std::panic::catch_unwind(|| {
                static TWO: std::sync::Mutex<Vec<&'static Language>> = std::sync::Mutex::new(vec![]);
                *TWO.lock().unwrap() = two.to_vec();
                Registry::build(&[&BuiltIn { name: "x", requires: &[], register: |r| TWO.lock().unwrap().iter().for_each(|l| r.section_mut::<Languages>().add(*l)) }])
            });
            let e = built.err().and_then(|e| e.downcast_ref::<String>().cloned()).unwrap_or_default();
            assert!(e.contains(said), "{said}: {e}");
        }
    }

    #[test]
    fn the_builds_languages() {
        let l = get();
        assert_eq!(l.named("pondra").map(|l| l.name), Some("pondra"));
        assert_eq!(l.named(" Spark ").map(|l| l.name), Some("spark"));
        assert_eq!(l.named("DATABRICKS").map(|l| l.name), Some("spark"));
        assert!(l.named("mysql").is_none() && l.named("generic").is_none());
        assert_eq!(l.listed(), "pondra (Pondra's SQL) or spark (Spark SQL)");
    }

    #[test]
    fn a_languages_query_in_a_from_is_pondras() {
        assert_eq!(inline("SELECT * FROM spark_sql('SELECT a FROM t ORDER BY b')").unwrap(), "SELECT a FROM t ORDER BY b"); // (its sort holds)
        assert_eq!(inline("SELECT a FROM spark_sql('SELECT a FROM t') s WHERE a > 1").unwrap(), "SELECT a FROM (SELECT a FROM t) s WHERE a > 1");
        assert_eq!(inline("SELECT a FROM SPARK_SQL('SELECT \"x\" AS a')").unwrap(), "SELECT a FROM (SELECT 'x' AS a) AS spark_sql");
        assert_eq!(inline("SELECT 'spark_sql' AS a").unwrap(), "SELECT 'spark_sql' AS a");
        for (sql, said) in [
            ("SELECT * FROM spark_sql('INSERT INTO t SELECT 1')", "spark_sql takes a query (SELECT, WITH, VALUES)"),
            ("SELECT * FROM spark_sql('SELECT 1; SELECT 2')", "spark_sql takes one query, not 2"),
            ("SELECT * FROM spark_sql(1)", "spark_sql takes the query as a string: spark_sql('SELECT …')"),
        ] {
            assert!(inline(sql).is_err_and(|e| e.to_string().contains(said)), "{sql}");
        }
    }
}
