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
    let _ = ast::visit_expressions_mut(node, |x| {
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
    ast::visit_expressions(node, |x| match x {
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

#[cfg(test)]
mod tests {
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
