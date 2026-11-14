//! Column types (step 5, the every-data-kind review's E1; l2-wip/step5-types-section.md): each type a column may be that
//! Arrow's own types don't say is an entry here, so a type is one entry and one register line, and what a door does with
//! a column of it asks the entry. The first are Postgres's types the lake holds as text, `uuid`, `json` and `jsonb`
//! (`pg_column_types`, the postgres extension's). An entry says what its files hold and the values a write refuses;
//! the column record's full type and VECTOR, FILE and INET come with the next pieces, and step 15 opens the section to
//! installed packages.
//!
//! A column of one is marked on its Arrow field (`mark`, the only writer of the mark): the Postgres port sends it as that
//! type, and a function's answer of one carries it on (`pg_functions`).
use datafusion::arrow::datatypes::{DataType, Field};

/// The field metadata that marks a column with its type's entry (`uuid`, `json`, `jsonb`): the Postgres port reads it,
/// other doors pass it by.
pub const MARK: &str = "pg_type";

/// A column type: what a column of it is, beside its Arrow type.
pub struct ColumnType {
    /// Its name, as a column's record keeps it (`Col::held`) and the mark says it, lower case.
    pub name: &'static str,
    /// Its Arrow extension name: Arrow's canonical one where its storage is the canonical one's (`arrow.json`), else
    /// `pondra.<name>`. Written on what leaves Pondra once the column record keeps the full type (5-TY-b).
    pub extension: &'static str,
    /// What its files hold, of its parameters (none yet). (Read once the column record keeps the full type, 5-TY-b.)
    #[allow(dead_code)]
    pub storage: fn(&[u64]) -> DataType,
    /// Why a write refuses a value of a column of it, named `column`: the message and its SQLSTATE; None when it is
    /// taken (`rules::typed`, at every door that writes rows).
    pub refusal: Option<fn(column: &str, value: &str) -> Option<(String, &'static str)>>,
}

/// The registry's column types section.
#[derive(Default)]
pub struct ColumnTypes(Vec<&'static ColumnType>);

impl crate::registry::Section for ColumnTypes {
    const NAME: &'static str = "column types";

    /// A type's name is lower case and no other's, and so is its Arrow extension name, Arrow's or Pondra's.
    fn check(&self, _: &crate::registry::Registry) -> Result<(), String> {
        let (mut names, mut extensions) = (std::collections::HashSet::new(), std::collections::HashSet::new());
        for t in &self.0 {
            if t.name != t.name.to_ascii_lowercase() || !names.insert(t.name) {
                return Err(format!("{}: a column type's name is lower case and no other type's", t.name));
            }
            if !t.extension.starts_with("arrow.") && t.extension != format!("pondra.{}", t.name) {
                return Err(format!("{}: its Arrow extension name is Arrow's canonical one or pondra.{}, not {}", t.name, t.name, t.extension));
            }
            if !extensions.insert(t.extension) {
                return Err(format!("{}: its Arrow extension name, {}, is another column type's", t.name, t.extension));
            }
        }
        Ok(())
    }
}

impl ColumnTypes {
    pub fn add(&mut self, t: &'static ColumnType) { self.0.push(t) }
}

/// The column type `name` names (a column record's `held`, a mark), if one does.
pub fn named(name: &str) -> Option<&'static ColumnType> {
    crate::registry::get().find::<ColumnTypes>()?.0.iter().copied().find(|t| t.name == name)
}

/// The column type a field is marked with (`mark`), if any.
pub fn of(field: &Field) -> Option<&'static ColumnType> { field.metadata().get(MARK).and_then(|n| named(n)) }

/// `field` marked as a column of `t`, its other metadata kept.
pub fn mark(field: Field, t: &ColumnType) -> Field {
    let mut meta = field.metadata().clone();
    meta.insert(MARK.into(), t.name.into());
    field.with_metadata(meta)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::{BuiltIn, Registry};

    fn text(_: &[u64]) -> DataType { DataType::Utf8 }

    #[test]
    fn a_type_is_named_once_and_so_is_its_extension() {
        static A: ColumnType = ColumnType { name: "a", extension: "pondra.a", storage: text, refusal: None };
        static UPPER: ColumnType = ColumnType { name: "B", extension: "pondra.b", storage: text, refusal: None };
        static SAME: ColumnType = ColumnType { name: "c", extension: "pondra.a", storage: text, refusal: None };
        for (two, said) in [([&A, &A], "a: a column type's name"), ([&A, &UPPER], "B: a column type's name"), ([&A, &SAME], "c: its Arrow extension name")] {
            let built = std::panic::catch_unwind(|| {
                static TWO: std::sync::Mutex<Vec<&'static ColumnType>> = std::sync::Mutex::new(vec![]);
                *TWO.lock().unwrap() = two.to_vec();
                Registry::build(&[&BuiltIn { name: "x", requires: &[], register: |r| TWO.lock().unwrap().iter().for_each(|t| r.section_mut::<ColumnTypes>().add(*t)) }])
            });
            let e = built.err().and_then(|e| e.downcast_ref::<String>().cloned()).unwrap_or_default();
            assert!(e.contains(said), "{said}: {e}");
        }
    }

    /// The build's types: Postgres's held as text, each marked on a field and read back from it, its metadata kept.
    #[test]
    fn the_builds_types_mark_a_field_and_read_back() {
        for name in ["uuid", "json", "jsonb"] {
            let t = named(name).unwrap_or_else(|| panic!("{name}: an entry"));
            let field = Field::new("c", (t.storage)(&[]), true).with_metadata([("x".to_string(), "y".to_string())].into());
            let marked = mark(field, t);
            assert_eq!(of(&marked).map(|t| t.name), Some(name));
            assert_eq!(marked.metadata().get("x").map(String::as_str), Some("y"), "{name}: its other metadata kept");
        }
        assert_eq!(named("json").map(|t| t.extension), Some("arrow.json"));
        assert!(named("varchar(3)").is_none() && of(&Field::new("c", DataType::Utf8, true)).is_none());
    }
}
