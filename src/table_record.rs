//! The parts of a table's catalog record (`store::TableMeta`) whose rules the floors above apply: what a write's rules
//! see of it (`Table`, made by `TableMeta::to_rules`), its constraints, the files an `ext:` table reads, and a history
//! view's key. The types are the catalog's, as the record that holds them is; what they mean is their owners'
//! (`rules`, `constraints`, `ext_names`, `views`), whose code reads and makes them and names them as their own.
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::physical_expr::PhysicalExpr;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;

/// A table as a write's rules see it, made once a version of it is parsed (`TableMeta::to_rules`).
#[derive(Clone, Debug, PartialEq)]
pub struct Table {
    /// The columns as SQL names them, `_deleted` among them: the schema a write's rows are fitted to.
    pub schema: SchemaRef,
    /// Each column SQL sees, in order: the dropped ones left out.
    pub columns: Vec<Column>,
    /// The primary key's columns, in SQL's names (empty when the table has no key).
    pub key: Vec<String>,
    /// Does a plain INSERT check its key (a key, and no merge: `TableMeta::key_checked`).
    pub key_checked: bool,
    /// The enforced UNIQUE constraints: (name, columns in SQL's names).
    pub unique: Vec<(String, Vec<String>)>,
    /// The CHECK constraints: (name, condition), as the table keeps them, over SQL's names.
    pub checks: Vec<(String, String)>,
    /// Is any column renamed or dropped, so that a stored name is not SQL's.
    pub mapped: bool,
    /// SQL's names in SQL's order (`TableMeta::sql_order`): the columns an INSERT without a column list fills, first to
    /// last. `columns` and `schema` keep the stored order.
    pub order: Vec<String>,
    /// `checks` planned for `schema` once, at the first write a CHECK looks at (`checks`), not parsed and planned again at
    /// every write: three CHECKs cost a one-row INSERT 0.3 ms that way, and 0.6 ms in a transaction, whose COMMIT checks again.
    pub planned: Planned,
}

/// `Table::planned`: the session the plans were made in (`SessionContext::session_id`), and each CHECK's plan in the order
/// `checks` meets them. A clone starts without them, since its checks may be changed; two tables are equal by their other
/// fields.
#[derive(Default)]
pub struct Planned(pub(crate) std::sync::OnceLock<(String, Vec<Option<Arc<dyn PhysicalExpr>>>)>);

impl Clone for Planned {
    fn clone(&self) -> Self { Planned::default() }
}

impl PartialEq for Planned {
    fn eq(&self, _: &Self) -> bool { true }
}

impl std::fmt::Debug for Planned {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { f.write_str("Planned") }
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

/// A table's UNIQUE, PRIMARY KEY or FOREIGN KEY (ADR-057): the leader checks an enforced UNIQUE at every write
/// (`constraints.rs`), and the rest only describe the table.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Constraint {
    pub name: String,
    pub kind: Kind,
    pub columns: Vec<String>, // stored names (SQL's in a table's spec and in `logical()`)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub references: Option<(String, Vec<String>)>, // a foreign key's table and its columns, as written
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub enforced: bool,
}

/// Which constraint a `Constraint` is.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    Unique,
    PrimaryKey,
    ForeignKey,
}

/// Files a query reads as one table (an `ext:` name, `ext_names.rs`): where, in which format, with which options.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Spec {
    pub urls: Vec<String>,
    pub format: String, // parquet, csv, json (one object a line)
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub options: BTreeMap<String, String>,
}

/// History kept per key (SCD type 2, ADR-036 §8): `WITH (history = 'id', sequence_by = 'ts'
/// [, delete_when = 'op = ''D'''])`. The view keeps every version of each key as it arrives (an
/// append view: writers on any node need no coordination); a read gives each version its
/// `__start_at` (its `sequence_by`) and `__end_at` (the next version's, NULL for the current one),
/// worked out then, so versions that arrive out of order still make the right history. A version
/// `delete_when` holds for ends its key's history and isn't shown (`__delete`).
#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
pub struct History {
    pub key: Vec<String>,
    pub sequence_by: String,
    #[serde(default)]
    pub deletes: bool,
}

