//! What an enum type's statements mean, and the words their refusals say, in every form: `CREATE TYPE … AS ENUM`, `ALTER TYPE`
//! and `DROP TYPE` are read here, and a node keeps the types they make in `types.rs` while a page keeps its own, so the two
//! forms take the same statements and say the same thing.
use crate::rules::create::labelled;
use anyhow::{ensure, Context, Result};
use datafusion::sql::sqlparser::ast::{self, Statement};
use datafusion::sql::sqlparser::{dialect::GenericDialect, parser::Parser};
use serde::{Deserialize, Serialize};
use std::sync::LazyLock;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum Change {
    Create { name: String, labels: Vec<String>, if_not_exists: bool },
    Add { name: String, label: String, if_not_exists: bool, before: Option<String>, after: Option<String> },
    Rename { name: String, to: String },
    Drop { names: Vec<String>, if_exists: bool },
}

/// `CREATE TYPE … AS ENUM`, `ALTER TYPE` or `DROP TYPE` as the change it is; `Err` with the words a statement of one of them
/// is refused in when it is wrong; None for any other statement.
pub fn change(sql: &str) -> Option<Result<Change, String>> {
    static WORD: LazyLock<regex::Regex> = LazyLock::new(|| regex::Regex::new(r"(?is)^\s*(CREATE(\s+OR\s+REPLACE)?|ALTER|DROP)\s+TYPE\b").expect("a regex"));
    static UNLESS: LazyLock<regex::Regex> = LazyLock::new(|| regex::Regex::new(r"(?is)^(\s*CREATE\s+TYPE\s+)IF\s+NOT\s+EXISTS\s+").expect("a regex"));
    static DROP: LazyLock<regex::Regex> = LazyLock::new(|| regex::Regex::new(r#"(?is)^\s*DROP\s+TYPE\s+(IF\s+EXISTS\s+)?(.+?)(\s+(CASCADE|RESTRICT))?\s*;?\s*$"#).expect("a regex"));
    let first = crate::text::first_word(sql);
    let c = WORD.captures(first)?;
    let word = c[1].to_uppercase();
    if c.get(2).is_some() {
        return Some(Err("CREATE OR REPLACE TYPE: a type's columns keep their labels (ALTER TYPE … ADD VALUE adds one)".into()));
    }
    if word == "DROP" {
        // (CASCADE and RESTRICT are the same here: a drop never takes a column with it, so a type a column
        // uses is refused by name whichever is written, and one no column uses is dropped either way)
        let d = DROP.captures(first)?;
        let names = d[2].split(',').map(|n| crate::text::object_of(n.trim())).collect();
        return Some(Ok(Change::Drop { names, if_exists: d.get(1).is_some() }));
    }
    let unless = UNLESS.is_match(first);
    let text = UNLESS.replace(first, "$1");
    let parsed = Parser::parse_sql(&GenericDialect {}, &text).map(|mut s| s.pop());
    Some(match parsed {
        Ok(Some(Statement::CreateType { name, representation: Some(ast::UserDefinedTypeRepresentation::Enum { labels }) })) => {
            Ok(Change::Create { name: crate::text::object(&name), labels: labels.iter().map(|l| l.value.clone()).collect(), if_not_exists: unless })
        }
        Ok(Some(Statement::CreateType { .. })) | Err(_) if word.starts_with("CREATE") => Err("CREATE TYPE [IF NOT EXISTS] name AS ENUM ('label', …): enums are the types kept (a composite, a range or another type's alias isn't)".into()),
        Ok(Some(Statement::AlterType(a))) => {
            let name = crate::text::object(&a.name);
            match a.operation {
                ast::AlterTypeOperation::Rename(r) => Ok(Change::Rename { name, to: crate::text::ident(&r.new_name) }),
                ast::AlterTypeOperation::AddValue(v) => {
                    let (before, after) = match v.position {
                        Some(ast::AlterTypeAddValuePosition::Before(n)) => (Some(n.value), None),
                        Some(ast::AlterTypeAddValuePosition::After(n)) => (None, Some(n.value)),
                        None => (None, None),
                    };
                    Ok(Change::Add { name, label: v.value.value, if_not_exists: v.if_not_exists, before, after })
                }
                ast::AlterTypeOperation::RenameValue(_) => Err("ALTER TYPE … RENAME VALUE: rows keep the text they were written with (files are never rewritten); add the new label, UPDATE the rows to it".into()),
            }
        }
        _ => Err("ALTER TYPE name ADD VALUE [IF NOT EXISTS] 'label' [BEFORE | AFTER 'label'] | RENAME TO new_name".into()),
    })
}

/// The refusal a CREATE TYPE meets when its type is there already.
pub fn exists(name: &str) -> String { format!("type \"{name}\" already exists") }

/// The refusal a type's name meets when no such type is there.
pub fn missing(name: &str) -> String { format!("type \"{name}\" does not exist") }

/// The notice a CREATE TYPE IF NOT EXISTS gives when its type is there already.
pub fn skipping(name: &str) -> String { format!("type \"{name}\" already exists, skipping") }

/// The notice an ADD VALUE IF NOT EXISTS gives when its label is there already.
pub fn label_skipping(label: &str) -> String { format!("enum label \"{label}\" already exists, skipping") }

/// Adds `label` to an enum's `labels`, after or before the label `after` or `before` names (last when neither): false, and
/// nothing changed, when the label is there already and `if_not_exists`; an error when it is there and not, or when the
/// label named for the place isn't there.
pub fn add_label(labels: &mut Vec<String>, label: &str, before: Option<&str>, after: Option<&str>, if_not_exists: bool) -> Result<bool> {
    if labels.iter().any(|l| l == label) {
        ensure!(if_not_exists, "enum label \"{label}\" already exists");
        return Ok(false);
    }
    let place = |n: &str| labels.iter().position(|l| l == n).with_context(|| format!("\"{n}\" is not an existing enum label"));
    let at = match (before, after) {
        (Some(n), _) => place(n)?,
        (_, Some(n)) => place(n)? + 1,
        _ => labels.len(),
    };
    labels.insert(at, label.to_string());
    labelled(labels)?;
    Ok(true)
}

/// The refusal a DROP TYPE of `name` meets while columns use it: `using` names them as table.column. Ok when none does.
pub fn in_use(name: &str, using: &[String]) -> Result<()> {
    let (what, verb) = if using.len() == 1 { ("column", "uses") } else { ("columns", "use") };
    ensure!(using.is_empty(), "cannot drop type {name} because {what} {verb} it: {}; drop or change them first", using.join(", "));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_type_statement_is_read_as_its_change_and_a_wrong_one_refused() {
        assert_eq!(change("CREATE TYPE mood AS ENUM ('a','b')"), Some(Ok(Change::Create { name: "mood".into(), labels: vec!["a".into(), "b".into()], if_not_exists: false })));
        assert_eq!(change("CREATE TYPE IF NOT EXISTS mood AS ENUM ('a')"), Some(Ok(Change::Create { name: "mood".into(), labels: vec!["a".into()], if_not_exists: true })));
        assert!(matches!(change("CREATE OR REPLACE TYPE m AS ENUM ('a')"), Some(Err(_))));
        assert_eq!(change("DROP TYPE IF EXISTS a, b CASCADE"), Some(Ok(Change::Drop { names: vec!["a".into(), "b".into()], if_exists: true })));
        assert_eq!(change("ALTER TYPE mood ADD VALUE 'c' BEFORE 'a'"), Some(Ok(Change::Add { name: "mood".into(), label: "c".into(), if_not_exists: false, before: Some("a".into()), after: None })));
        assert_eq!(change("SELECT 1"), None);
    }

    #[test]
    fn a_label_goes_where_it_is_placed_and_a_repeat_or_a_missing_place_is_refused() -> Result<()> {
        let mut labels = vec!["a".to_string(), "b".into()];
        assert!(add_label(&mut labels, "c", None, Some("a"), false)?);
        assert_eq!(labels, ["a", "c", "b"]);
        assert!(!add_label(&mut labels, "a", None, None, true)?);
        assert_eq!(labels, ["a", "c", "b"]);
        assert!(add_label(&mut labels, "a", None, None, false).is_err_and(|e| e.to_string() == "enum label \"a\" already exists"));
        assert!(add_label(&mut labels, "d", Some("z"), None, false).is_err_and(|e| e.to_string() == "\"z\" is not an existing enum label"));
        assert_eq!(labels, ["a", "c", "b"]);
        Ok(())
    }

    #[test]
    fn a_type_a_column_uses_is_not_dropped() -> Result<()> {
        in_use("mood", &[])?;
        let Err(e) = in_use("mood", &["t.m".into()]) else { panic!("a column uses it") };
        assert!(format!("{e:#}").contains("because column uses it: t.m"), "{e:#}");
        Ok(())
    }
}
