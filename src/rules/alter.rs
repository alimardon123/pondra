//! What an ALTER TABLE asks of a table's columns, and the words its refusals say: a node changes its catalog entry
//! (`ddl::alter_column`, `write::alter_spec`), reading the statement and naming its faults here. Every form is a node's
//! member (a page too, ADR-072), so every form says the same thing.
use crate::text::ident;
use datafusion::sql::sqlparser::ast;
use std::ops::ControlFlow;

/// Is `e` NULL, through parentheses and casts? `DEFAULT NULL` is no default.
pub fn is_null(e: &ast::Expr) -> bool {
    match e {
        ast::Expr::Value(v) => matches!(v.value, ast::Value::Null),
        ast::Expr::Nested(e) | ast::Expr::Cast { expr: e, .. } => is_null(e),
        _ => false,
    }
}

/// Does an ADD COLUMN owe the rows already there a value: NOT NULL, a DEFAULT that isn't NULL, or an identity? A table
/// takes a new column only as NULL in the rows it has (`owes_rows`).
pub fn fills_existing(c: &ast::ColumnDef) -> bool {
    let owes = c.options.iter().any(|o| match &o.option {
        ast::ColumnOption::NotNull => true,
        ast::ColumnOption::Default(e) => !is_null(e),
        _ => false,
    });
    owes || !matches!(super::create::identity_of(c), Ok(None))
}

/// What an ADD COLUMN declares besides its type, NULL and a NULL default, which a column added to a table doesn't take:
/// a CHECK, a key, UNIQUE, a reference, a collation. None when it declares none.
pub fn beside_type(c: &ast::ColumnDef) -> Option<String> {
    c.options.iter().find(|o| !matches!(&o.option, ast::ColumnOption::Null | ast::ColumnOption::NotNull | ast::ColumnOption::Default(_))).map(|o| o.option.to_string())
}

/// The refusal of an ADD COLUMN that owes the rows already there a value (`fills_existing`).
pub fn owes_rows(table: &str, column: &str) -> String {
    format!("ALTER TABLE {table} ADD COLUMN {column} … NOT NULL, DEFAULT or an identity: the rows already there would have no value for it (add it, then UPDATE {table} SET {column} = …)")
}

/// The refusal of an ADD COLUMN that declares `what` besides its type (`beside_type`).
pub fn not_alone(table: &str, column: &str, what: &str) -> String {
    format!("ALTER TABLE {table} ADD COLUMN {column} … {what}: a column added to a table takes its type alone (add it, then ALTER TABLE {table} ADD CONSTRAINT …)")
}

/// A column added, or renamed, under a name the table has.
pub fn has_column(table: &str, column: &str) -> String { format!("{table} already has a column {column}") }

/// A column dropped or renamed that the table doesn't have.
pub fn no_column(table: &str, column: &str) -> String { format!("{table} has no column {column}") }

/// A drop of a column the table is laid out by: its key (`role` "key"), or a node's partition, TTL or order column.
pub fn stays(table: &str, column: &str, role: &str) -> String { format!("{column} is {table}'s {role} column: it stays") }

/// A drop of the table's last column.
pub fn last_column(table: &str, column: &str) -> String { format!("{column} is {table}'s last column (DROP TABLE {table})") }

/// A column renamed, dropped or retyped while `readers` (`view v`, …) name the table's columns, with Postgres's code for a
/// column a view uses: 2BP01 for a drop, 0A000 for a rename or a type.
pub fn used_by(table: &str, readers: &[String], drop: bool) -> anyhow::Error {
    let words = match readers {
        [one] => format!("{table} is used by {one}, which names its columns: drop it first"),
        _ => format!("{table} is used by {}, which name its columns: drop them first", readers.join(", ")),
    };
    crate::codes::coded(if drop { "2BP01" } else { "0A000" }, words)
}

/// The column `from` named `to` wherever `node` (a CHECK's condition, a whole CREATE TABLE) names it in an expression: a
/// CHECK's condition, and a key's or a UNIQUE's columns, which sqlparser holds as expressions. A column's own definition
/// names it apart (`ColumnDef::name`), so a caller renames that one itself.
pub fn rename_column_in<V: ast::VisitMut>(node: &mut V, from: &str, to: &str) {
    let _ = crate::walk::visit_expressions_mut(node, |x| {
        let named = match x {
            ast::Expr::Identifier(i) => Some(i),
            ast::Expr::CompoundIdentifier(parts) => parts.last_mut(),
            _ => None,
        };
        if let Some(i) = named.filter(|i| ident(i) == from) {
            *i = ast::Ident::with_quote('"', to); // (quoted: the name as it is, whatever its case or words)
        }
        ControlFlow::<()>::Continue(())
    });
}

/// Whether `node` (a CHECK's condition, a UNIQUE's columns) names the column `column` in an expression.
pub fn names_column<V: ast::Visit>(node: &V, column: &str) -> bool {
    crate::walk::visit_expressions(node, |x| match x {
        ast::Expr::Identifier(i) if ident(i) == column => ControlFlow::Break(()),
        ast::Expr::CompoundIdentifier(parts) if parts.last().is_some_and(|i| ident(i) == column) => ControlFlow::Break(()),
        _ => ControlFlow::Continue(()),
    })
    .is_break()
}

/// A CHECK's condition once its column `from` is named `to` (`RENAME COLUMN`), as Postgres's checks follow a rename: the
/// condition keeps reading the column it read. A condition that doesn't parse is left as it is.
pub fn check_renamed(condition: &str, from: &str, to: &str) -> String {
    let Some(mut e) = condition_of(condition) else { return condition.to_string() };
    rename_column_in(&mut e, from, to);
    e.to_string()
}

/// Whether a CHECK's condition reads the column `column`: a drop of the column takes the check with it, as in Postgres.
pub fn check_reads(condition: &str, column: &str) -> bool { condition_of(condition).is_some_and(|e| names_column(&e, column)) }

/// `ALTER [MATERIALIZED] VIEW [IF EXISTS] v RENAME TO w`, as the node's text takes it (the parser takes only ALTER VIEW … AS):
/// the two names as written, or None for any other statement. The node and a page rename a view by it.
pub fn view_rename(sql: &str) -> Option<(String, String)> {
    static VIEW_RENAME: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
        regex::Regex::new(r#"(?is)^\s*ALTER\s+(MATERIALIZED\s+)?VIEW\s+(IF\s+EXISTS\s+)?([\w."-]+)\s+RENAME\s+TO\s+([\w."-]+)\s*;?\s*$"#).expect("a regex")
    });
    let c = VIEW_RENAME.captures(sql)?;
    Some((c[3].to_string(), c[4].to_string()))
}

/// A CHECK's condition, read back from its text: the whole text, or None.
fn condition_of(text: &str) -> Option<ast::Expr> {
    use datafusion::sql::sqlparser::{dialect::GenericDialect, parser::Parser, tokenizer::Token};
    let mut parser = Parser::new(&GenericDialect {}).try_with_sql(text).ok()?;
    let condition = parser.parse_expr().ok()?;
    (parser.peek_token().token == Token::EOF).then_some(condition) // (an expression is read up to where it ends: the rest must be nothing)
}

/// Does this query read some table's columns by position: a `*` (`t.*`, `* EXCLUDE …`) or DuckDB's `COLUMNS(…)`, whose
/// columns come in the table's order? Read by its tokens, so any of Pondra's statements is read, and a `*` that multiplies
/// or counts (`count(*)`) is not one. A table's column moved (`ALTER COLUMN c AFTER b`) would move such a query's columns.
pub fn by_position(sql: &str) -> bool {
    use datafusion::sql::sqlparser::{dialect::GenericDialect, tokenizer::{Token, Tokenizer}};
    let Ok(tokens) = Tokenizer::new(&GenericDialect {}, sql).tokenize() else { return true }; // (unread: taken as one)
    let tokens: Vec<Token> = tokens.into_iter().filter(|t| !matches!(t, Token::Whitespace(_))).collect();
    let word = |t: Option<&Token>, w: &str| matches!(t, Some(Token::Word(x)) if x.quote_style.is_none() && x.value.eq_ignore_ascii_case(w));
    (0..tokens.len()).any(|i| match &tokens[i] {
        Token::Mul => i == 0 || matches!(tokens[i - 1], Token::Comma | Token::Period) || ["SELECT", "DISTINCT", "ALL"].iter().any(|w| word(tokens.get(i - 1), w)),
        Token::Word(_) => word(tokens.get(i), "COLUMNS") && tokens.get(i + 1) == Some(&Token::LParen),
        _ => false,
    })
}

/// Why a column can't move while `readers` read its table by position and keep what they read (`ddl::by_position`).
pub fn read_by_position(table: &str, column: &str, readers: &[String]) -> anyhow::Error {
    let (who, verb) = match readers {
        [one] => (one.clone(), "reads"),
        _ => (readers.join(", "), "read"),
    };
    crate::codes::coded("0A000", format!("{column} can't move while {who} {verb} {table}'s columns by position (a `*`, or a task's rows into it), keeping them in the order they were: name the columns in its query (CREATE OR REPLACE MATERIALIZED VIEW … AS SELECT a, b, …), then move it"))
}

/// Why `column` of view `name` (`kind`: VIEW or MATERIALIZED VIEW) can't move in its query, and the way that always
/// works: the view made again with its columns listed in the order wanted.
pub fn cant_move(name: &str, column: &str, why: &str, kind: &str) -> anyhow::Error {
    let again = if kind == "VIEW" { "" } else { " (its rows worked out again)" };
    crate::codes::coded("0A000", format!("{column} can't move in {name}'s query: {why}. CREATE OR REPLACE {kind} {name} AS … with its columns in the order you want{again}"))
}

/// A view's query with its outputs in a new order (`ALTER VIEW v ALTER COLUMN c AFTER b`): `order[i]` is the place, in
/// the outputs as they are, of the one that comes `i`-th. Each SELECT list is put in that order (every branch of a
/// UNION, INTERSECT or EXCEPT, and every row of a VALUES, so their columns still pair up), and what names an output by
/// its place follows it (`ORDER BY 2`, `GROUP BY 1`); a `SELECT *` of one subquery moves that subquery's outputs and its
/// alias's names. Nothing else in the query changes. Err: why this query can't be moved so (its outputs come from a
/// table's `*`, a branch matched BY NAME, an ORDER BY ALL, …), for the caller to say with the way round it. The caller
/// proves the result (`ddl::place_view_column`: it plans to the same columns, in the new order).
pub fn moved(sql: &str, order: &[usize]) -> Result<String, String> {
    use datafusion::sql::sqlparser::{dialect::GenericDialect, parser::Parser};
    let mut q = Parser::new(&GenericDialect {}).try_with_sql(sql).and_then(|mut p| p.parse_query()).map_err(|e| format!("its query doesn't read back ({e})"))?;
    query(&mut q, order)?;
    return Ok(q.to_string());

    fn query(q: &mut ast::Query, order: &[usize]) -> Result<(), String> {
        body(&mut q.body, order)?;
        // (`ORDER BY ALL` is read as a column `all` here, and made positions as the query runs: `friendly::order_by_all`)
        let all = |e: &ast::Expr| matches!(e, ast::Expr::Identifier(i) if i.quote_style.is_none() && i.value.eq_ignore_ascii_case("all"));
        match q.order_by.as_mut().map(|o| &mut o.kind) {
            Some(ast::OrderByKind::All(_)) => Err("its ORDER BY ALL sorts by the columns in their order: write them out (ORDER BY a, b, …)".into()),
            Some(ast::OrderByKind::Expressions(by)) if matches!(by.as_slice(), [b] if all(&b.expr)) => Err("its ORDER BY ALL sorts by the columns in their order: write them out (ORDER BY a, b, …)".into()),
            Some(ast::OrderByKind::Expressions(by)) => by.iter_mut().try_for_each(|b| place(&mut b.expr, order)),
            None => Ok(()),
        }
    }
    fn body(b: &mut ast::SetExpr, order: &[usize]) -> Result<(), String> {
        use ast::SetQuantifier as Q;
        match b {
            ast::SetExpr::Select(s) => select(s, order),
            ast::SetExpr::Query(q) => query(q, order),
            ast::SetExpr::SetOperation { set_quantifier: Q::ByName | Q::AllByName | Q::DistinctByName, .. } => Err("its branches are matched BY NAME, in the first one's order".into()),
            ast::SetExpr::SetOperation { left, right, .. } => body(left, order).and_then(|_| body(right, order)),
            ast::SetExpr::Values(v) => v.rows.iter_mut().try_for_each(|r| put(&mut r.content, order)),
            _ => Err("its query isn't a SELECT".into()),
        }
    }
    fn select(s: &mut ast::Select, order: &[usize]) -> Result<(), String> {
        let star = |i: &ast::SelectItem| matches!(i, ast::SelectItem::Wildcard(_) | ast::SelectItem::QualifiedWildcard(..));
        if s.exclude.is_some() || s.projection.iter().any(|i| matches!(i, ast::SelectItem::ExprWithAliases { .. })) {
            return Err("its SELECT list doesn't name one column an item".into());
        }
        if s.projection.iter().any(star) {
            // (`SELECT * FROM (subquery) [AS] x [(names)]`: the subquery's outputs are its outputs, as it lists them)
            let plain = |o: &ast::WildcardAdditionalOptions| o.opt_ilike.is_none() && o.opt_exclude.is_none() && o.opt_except.is_none() && o.opt_replace.is_none() && o.opt_rename.is_none();
            let one = matches!(s.projection.as_slice(), [ast::SelectItem::Wildcard(o)] if plain(o)) && s.from.len() == 1 && s.from[0].joins.is_empty();
            let Some(ast::TableFactor::Derived { subquery, alias, .. }) = s.from.first_mut().map(|f| &mut f.relation).filter(|_| one) else {
                return Err("its `*` lists the columns of what it reads, in their order".into());
            };
            query(subquery, order)?;
            if let Some(a) = alias.as_mut().filter(|a| !a.columns.is_empty()) {
                put(&mut a.columns, order)?;
            }
        } else {
            put(&mut s.projection, order)?;
        }
        match &mut s.group_by {
            ast::GroupByExpr::Expressions(by, _) => by.iter_mut().try_for_each(|e| place(e, order)),
            ast::GroupByExpr::All(_) => Ok(()), // (every column that isn't an aggregate: the same set in any order)
        }
    }
    /// `items` in `order`: as many as there are outputs, or it isn't one item a column.
    fn put<T: Clone>(items: &mut Vec<T>, order: &[usize]) -> Result<(), String> {
        if items.len() != order.len() {
            return Err("its SELECT list doesn't name one column an item".into());
        }
        *items = order.iter().map(|&i| items[i].clone()).collect();
        Ok(())
    }
    /// An output named by its place (`ORDER BY 2`): the place it has now.
    fn place(e: &mut ast::Expr, order: &[usize]) -> Result<(), String> {
        if let ast::Expr::Value(v) = e {
            if let ast::Value::Number(n, _) = &mut v.value {
                if let Some(was) = n.parse::<usize>().ok().and_then(|n| n.checked_sub(1)).filter(|&n| n < order.len()) {
                    *n = (order.iter().position(|&i| i == was).expect("a permutation") + 1).to_string();
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn a_query_reads_columns_by_position_with_a_star_or_columns() {
        for sql in ["SELECT * FROM t", "SELECT a, * FROM t", "SELECT t.* FROM t JOIN u ON true", "SELECT DISTINCT * FROM t", "SELECT * EXCLUDE (a) FROM t",
            "WITH x AS (SELECT * FROM t) SELECT a FROM x", "SELECT COLUMNS('a|b') FROM t", "SELECT a FROM t UNION ALL SELECT * FROM u"] {
            assert!(by_position(sql), "{sql}");
        }
        for sql in ["SELECT a, b FROM t", "SELECT count(*) FROM t", "SELECT a * b, 2 * a FROM t", "SELECT '*' AS s, a FROM t", "SELECT a FROM t -- *", "SELECT x.columns FROM t x"] {
            assert!(!by_position(sql), "{sql}");
        }
    }

    use super::*;

    #[test]
    fn a_check_follows_its_columns_rename_and_goes_with_its_drop() {
        assert_eq!(check_renamed("n > 0", "n", "m"), "\"m\" > 0");
        assert_eq!(check_renamed("t.n > 0 AND nn < 2", "n", "Big One"), "t.\"Big One\" > 0 AND nn < 2");
        assert_eq!(check_renamed("\"N\" > 0", "n", "m"), "\"N\" > 0"); // (a quoted N is another column)
        assert_eq!(check_renamed("not sql (", "n", "m"), "not sql (");
        assert!(check_reads("n > 0 AND m < 3", "m"));
        assert!(check_reads("abs(t.m) < 3", "m"));
        assert!(!check_reads("nm > 0", "m"));
        assert!(!check_reads("'m' <> name", "m"));
    }

    #[test]
    fn a_views_outputs_move_with_what_names_them_by_place() {
        use datafusion::sql::sqlparser::{dialect::GenericDialect, parser::Parser};
        let read = |sql: &str| Parser::new(&GenericDialect {}).try_with_sql(sql).and_then(|mut p| p.parse_query()).unwrap().to_string();
        for (sql, order, want) in [
            ("SELECT a, b AS x, c + 1 FROM t", vec![2, 0, 1], "SELECT c + 1, a, b AS x FROM t"),
            ("SELECT a, b, count(*) FROM t GROUP BY 1, 2 ORDER BY 3 DESC, 1 LIMIT 5", vec![2, 0, 1], "SELECT count(*), a, b FROM t GROUP BY 2, 3 ORDER BY 1 DESC, 2 LIMIT 5"),
            ("SELECT a, b FROM t UNION ALL SELECT c, d FROM u ORDER BY 2", vec![1, 0], "SELECT b, a FROM t UNION ALL SELECT d, c FROM u ORDER BY 1"),
            ("SELECT a, b FROM t EXCEPT (SELECT c, d FROM u)", vec![1, 0], "SELECT b, a FROM t EXCEPT (SELECT d, c FROM u)"),
            ("VALUES (1, 'x'), (2, 'y')", vec![1, 0], "VALUES ('x', 1), ('y', 2)"),
            ("SELECT * FROM (VALUES (1, 'x')) AS v (n, s)", vec![1, 0], "SELECT * FROM (VALUES ('x', 1)) AS v (s, n)"),
            ("SELECT * FROM (SELECT a, b FROM t ORDER BY 1) AS s", vec![1, 0], "SELECT * FROM (SELECT b, a FROM t ORDER BY 2) AS s"),
            ("WITH w AS (SELECT * FROM t) SELECT a, b FROM w GROUP BY ALL", vec![1, 0], "WITH w AS (SELECT * FROM t) SELECT b, a FROM w GROUP BY ALL"),
            ("SELECT a, b FROM t ORDER BY a + 1, 9", vec![1, 0], "SELECT b, a FROM t ORDER BY a + 1, 9"),
        ] {
            assert_eq!(moved(sql, &order).as_deref(), Ok(read(want).as_str()), "{sql}");
        }
        for sql in ["SELECT * FROM t", "SELECT a, * FROM t", "SELECT t.* FROM t", "SELECT a, b FROM t ORDER BY ALL", "SELECT a, b FROM t ORDER BY ALL DESC LIMIT 2", "SELECT * FROM (SELECT a, b FROM t ORDER BY all) s", "SELECT a, b FROM t UNION BY NAME SELECT b, a FROM u",
            "SELECT * EXCLUDE (c) FROM (SELECT a, b, c FROM t) s", "SELECT * FROM (SELECT a FROM t) x JOIN (SELECT b FROM u) y ON true", "SELECT a FROM t", "INSERT INTO t VALUES (1, 2)"] {
            assert!(moved(sql, &[1, 0]).is_err(), "{sql}");
        }
    }

    #[test]
    fn a_view_rename_is_read_as_the_node_reads_it() {
        assert_eq!(view_rename("ALTER VIEW v RENAME TO w"), Some(("v".into(), "w".into())));
        assert_eq!(view_rename("alter materialized view if exists \"BigV\" rename to public.w;"), Some(("\"BigV\"".into(), "public.w".into())));
        assert_eq!(view_rename("ALTER VIEW v AS SELECT 1"), None);
        assert_eq!(view_rename("ALTER TABLE t RENAME TO u"), None);
    }

    #[test]
    fn an_added_column_takes_its_type_alone_and_null_in_the_rows_there() {
        let column = |sql: &str| {
            use datafusion::sql::sqlparser::{dialect::GenericDialect, parser::Parser};
            let Ok(statements) = Parser::parse_sql(&GenericDialect {}, &format!("ALTER TABLE t ADD COLUMN {sql}")) else { panic!("{sql}") };
            let ast::Statement::AlterTable(a) = &statements[0] else { panic!("{sql}") };
            let ast::AlterTableOperation::AddColumn { column_def, .. } = &a.operations[0] else { panic!("{sql}") };
            column_def.clone()
        };
        for (sql, owes) in [("c INT", false), ("c INT NULL", false), ("c INT DEFAULT NULL", false), ("c INT NOT NULL", true), ("c INT DEFAULT 7", true), ("c INT GENERATED ALWAYS AS IDENTITY", true)] {
            assert_eq!(fills_existing(&column(sql)), owes, "{sql}");
        }
        assert_eq!(beside_type(&column("c INT DEFAULT NULL")), None);
        assert_eq!(beside_type(&column("c INT CHECK (c > 0)")).as_deref(), Some("CHECK (c > 0)"));
        assert!(beside_type(&column("c INT UNIQUE")).is_some());
    }
}
