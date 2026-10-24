//! The table as a write's rules see it, built in every form: a node makes one from its catalog entry
//! (`TableMeta::rules`), and a page will from its own definitions. Names are SQL's, as rows reach the rules;
//! only `Column::stored` keeps the name a file or the log holds a column under. Nothing here reads files,
//! storage or the catalog, so the rules over it (`shape`) run the same in a browser as on a node.
pub mod fit;
pub mod keys;
pub mod typed;

use datafusion::arrow::datatypes::SchemaRef;

/// A table as a write's rules see it, made once a version of it is parsed (`TableMeta::to_rules`).
#[derive(Clone, Debug, PartialEq)]
pub struct Table {
    /// The columns as SQL names them, `_deleted` among them: the schema a write's rows are fitted to.
    pub schema: SchemaRef,
    /// Each column SQL sees, in order: the dropped ones left out.
    pub columns: Vec<Column>,
    /// The primary key's columns, in SQL's names (empty when the table has no key).
    pub key: Vec<String>,
    /// Does a plain INSERT check its key (a key, and no merge: `constraints::key_checked`).
    pub key_checked: bool,
    /// The enforced UNIQUE constraints: (name, columns in SQL's names).
    pub unique: Vec<(String, Vec<String>)>,
    /// The CHECK constraints: (name, condition), as the table keeps them, over SQL's names.
    pub checks: Vec<(String, String)>,
    /// Is any column renamed or dropped, so that a stored name is not SQL's.
    pub mapped: bool,
}

/// One column as the rules see it.
#[derive(Clone, Debug, PartialEq)]
pub struct Column {
    /// The column's name in SQL.
    pub name: String,
    /// The name its files and the log keep it under: a bulk INSERT's stream checks its rows by this.
    pub stored: String,
    /// Must a write give it a value (NOT NULL; a key's columns are too).
    pub not_null: bool,
    /// The DEFAULT as SQL writes it (an identity's is its `nextval('…')`).
    pub default: Option<String>,
    /// Is it GENERATED ALWAYS AS IDENTITY.
    pub always: bool,
    /// The SQL type a text column is held as: `uuid`, `json`, `jsonb` or `varchar(n)`.
    pub held: Option<String>,
    /// An enum column's type (None for an `ENUM('a', 'b')` written in the column) and its labels.
    pub labels: Option<(Option<String>, Vec<String>)>,
}

impl Table {
    /// The columns but the delete marker, as `write::by_position` counts them: the values a row fills.
    pub fn width(&self) -> usize { self.columns.iter().filter(|c| c.name != "_deleted").count() }

    /// The column SQL calls `name`, if the table has one.
    pub fn column(&self, name: &str) -> Option<&Column> { self.columns.iter().find(|c| c.name == name) }

    /// Is `name` a keyed table's delete marker (`_deleted`, as `TableMeta::marker` says)?
    pub fn marker(&self, name: &str) -> bool { name == "_deleted" && !self.key.is_empty() }

    /// Does the table enforce a UNIQUE constraint (`constraints::checked`)?
    pub fn checked(&self) -> bool { !self.unique.is_empty() }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::Schema;
    use std::sync::Arc;

    /// A table of plain columns (the schema is not what these check), keyed by `key`.
    fn table(key: &[&str], names: &[&str], unique: Vec<(String, Vec<String>)>) -> Table {
        let column = |n: &&str| Column { name: n.to_string(), stored: n.to_string(), not_null: false, default: None, always: false, held: None, labels: None };
        Table { schema: Arc::new(Schema::empty()), columns: names.iter().map(column).collect(), key: key.iter().map(|k| k.to_string()).collect(), key_checked: !key.is_empty(), unique, checks: vec![], mapped: false }
    }

    #[test]
    fn the_marker_is_a_keyed_tables_delete_column() {
        let keyed = table(&["id"], &["id", "v", "_deleted"], vec![]);
        assert!(keyed.marker("_deleted") && !keyed.marker("v"));
        assert_eq!(keyed.width(), 2, "the delete marker is left out, as by_position leaves it");
        assert!(keyed.column("v").is_some() && keyed.column("gone").is_none());
        let plain = table(&[], &["id", "_deleted"], vec![]);
        assert!(!plain.marker("_deleted"), "a table with no key has no marker");
        assert_eq!(plain.width(), 1);
    }

    #[test]
    fn checked_is_an_enforced_unique() {
        assert!(!table(&["id"], &["id"], vec![]).checked());
        assert!(table(&[], &["a"], vec![("a_key".into(), vec!["a".into()])]).checked());
    }
}
