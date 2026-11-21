//! Column types (step 5, the every-data-kind review's E1; l2-wip/step5-types-section.md): each type a column may be that
//! Arrow's own types don't say is an entry here, so a type is one entry and one register line, and what a door does with
//! a column of it asks the entry. The first are Postgres's types the lake holds as text, `uuid`, `json` and `jsonb`
//! (`pg_column_types`, the postgres extension's). An entry says what its files hold and the values a write refuses;
//! the column record's full type and VECTOR, FILE and INET come with the next pieces, and step 15 opens the section to
//! installed packages.
//!
//! A column of one is marked on its Arrow field (`mark`, the only writer of the mark): the Postgres port sends it as that
//! type, and a function's answer of one carries it on (`pg_functions`).
//!
//! A type whose storage isn't its text (INET's bytes, `inet`) says how its values are read from text and shown as text
//! (`parse`, `show`), and each door asks it: a write fits text to it (`parsed`), a text answer shows it (`show_of`), and SQL
//! casts to it and from it and compares a string with it (`sql`).
pub mod inet;
pub mod sql;

use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use std::sync::Arc;

pub use inet::INET;

/// The field metadata that marks a column with its type's entry (`uuid`, `json`, `jsonb`): the Postgres port reads it,
/// other doors pass it by.
pub const MARK: &str = "pg_type";

/// A column type: what a column of it is, beside its Arrow type.
pub struct ColumnType {
    /// Its name, as a column's record keeps it (`Col::held`) and the mark says it, lower case.
    pub name: &'static str,
    /// Its Arrow extension name: Arrow's canonical one where its storage is the canonical one's (`arrow.json`), else
    /// `pondra.<name>`. `mark` writes it beside the mark.
    pub extension: &'static str,
    /// What its files hold, of its parameters (none yet). (Read once the column record keeps the full type, 5-TY-b.)
    #[allow(dead_code)]
    pub storage: fn(&[u64]) -> DataType,
    /// Why a write refuses a value of a column of it, named `column`: the message and its SQLSTATE; None when it is
    /// taken (`rules::typed`, at every door that writes rows).
    pub refusal: Option<fn(column: &str, value: &str) -> Option<(String, &'static str)>>,
    /// Its values read from their text (Utf8), for a type whose storage isn't text: the batch refused in the type's own words
    /// when a value isn't one (22P02). None for a type held as its text.
    pub parse: Option<fn(&ArrayRef) -> anyhow::Result<ArrayRef>>,
    /// Its values' text (Utf8), for a type whose storage isn't text: what a JSON or CSV answer and `x::text` show. None for a
    /// type held as its text.
    pub show: Option<fn(&ArrayRef) -> anyhow::Result<ArrayRef>>,
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

/// What a column of the type `held` names keeps in its files, where that isn't its text (INET's FixedSizeBinary(17)): None
/// for a type held as text (uuid, json, varchar(n)) and for a name that is no type's.
pub fn stored(held: &str) -> Option<DataType> { named(held).filter(|t| t.parse.is_some()).map(|t| (t.storage)(&[])) }

/// Text given for a column of `field`, read as its type where the field is marked with one whose storage isn't text
/// (`parse`); None for any other column, or values that aren't text (a write's `fit::strict` takes those).
pub fn parsed(c: &ArrayRef, field: &Field) -> Option<anyhow::Result<ArrayRef>> {
    let parse = of(field)?.parse?;
    let text = matches!(c.data_type(), DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 | DataType::Null);
    text.then(|| parse(&datafusion::arrow::compute::cast(c, &DataType::Utf8)?))
}

/// Each of `fields` of a type whose storage isn't its text (INET), by name and that type's name: what a table made from
/// a query's columns holds them as (`CREATE TABLE … AS`, a materialized view's table), so it keeps their type.
pub fn held_in(fields: &datafusion::arrow::datatypes::Fields) -> impl Iterator<Item = (String, String)> + '_ {
    fields.iter().filter_map(|f| of(f).filter(|t| t.parse.is_some()).map(|t| (f.name().clone(), t.name.to_string())))
}

/// How a column of `field` shows as text, where it is marked with a type whose storage isn't text (`show`): what a text
/// answer asks (`answer::Shown`).
pub fn show_of(field: &Field) -> Option<fn(&ArrayRef) -> anyhow::Result<ArrayRef>> { of(field)?.show }

/// `schema` with each column whose type `held` names (by the column's name) and whose storage isn't text marked as one
/// (`mark`), so a write's rows are read into it from text (`parsed`, in `fit`); the same schema when it has none.
pub fn parsing<'a>(schema: SchemaRef, held: impl Fn(&str) -> Option<&'a str>) -> SchemaRef {
    let typed = |f: &Field| held(f.name()).and_then(named).filter(|t| t.parse.is_some());
    if !schema.fields().iter().any(|f| typed(f).is_some()) {
        return schema;
    }
    let fields: Vec<Field> = schema.fields().iter().map(|f| match typed(f) {
        Some(t) => mark(f.as_ref().clone(), t),
        None => f.as_ref().clone(),
    }).collect();
    Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()))
}

/// The field metadata Arrow names an extension type by (`arrow_schema::extension::EXTENSION_TYPE_NAME_KEY`): Parquet,
/// IPC and Flight keep it, and pyarrow and DuckDB read a canonical one (`arrow.json`) as its type.
pub const EXTENSION: &str = "ARROW:extension:name";

/// `field` marked as a column of `t`, by its name (`MARK`) and its Arrow extension name (`EXTENSION`), its other metadata
/// kept: so a file or an answer a marked column goes out in says its type to other readers too.
pub fn mark(field: Field, t: &ColumnType) -> Field {
    let mut meta = field.metadata().clone();
    meta.insert(MARK.into(), t.name.into());
    meta.insert(EXTENSION.into(), t.extension.into());
    field.with_metadata(meta)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::{BuiltIn, Registry};

    fn text(_: &[u64]) -> DataType { DataType::Utf8 }

    #[test]
    fn a_type_is_named_once_and_so_is_its_extension() {
        static A: ColumnType = ColumnType { name: "a", extension: "pondra.a", storage: text, refusal: None, parse: None, show: None };
        static UPPER: ColumnType = ColumnType { name: "B", extension: "pondra.b", storage: text, refusal: None, parse: None, show: None };
        static SAME: ColumnType = ColumnType { name: "c", extension: "pondra.a", storage: text, refusal: None, parse: None, show: None };
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
        for name in ["uuid", "json", "jsonb", "inet"] {
            let t = named(name).unwrap_or_else(|| panic!("{name}: an entry"));
            let field = Field::new("c", (t.storage)(&[]), true).with_metadata([("x".to_string(), "y".to_string())].into());
            let marked = mark(field, t);
            assert_eq!(of(&marked).map(|t| t.name), Some(name));
            assert_eq!(marked.metadata().get("x").map(String::as_str), Some("y"), "{name}: its other metadata kept");
            assert_eq!(marked.metadata().get(EXTENSION).map(String::as_str), Some(t.extension), "{name}: its Arrow extension name");
        }
        assert_eq!(named("json").map(|t| t.extension), Some("arrow.json"));
        assert!(named("varchar(3)").is_none() && of(&Field::new("c", DataType::Utf8, true)).is_none());
        assert_eq!((stored("inet"), stored("uuid"), stored("varchar(3)")), (Some(DataType::FixedSizeBinary(inet::WIDTH as i32)), None, None));
    }

    /// A write's schema marks the columns whose storage isn't text, and text given for one is read as its type; any other
    /// column, or a value that isn't text, is the write's own cast's.
    #[test]
    fn text_written_into_a_typed_column_is_read_as_its_type() {
        use datafusion::arrow::array::{Array, AsArray, Int32Array, StringArray};
        let schema = Arc::new(Schema::new(vec![Field::new("ip", DataType::FixedSizeBinary(inet::WIDTH as i32), true), Field::new("u", DataType::Utf8, true)]));
        let held = |n: &str| match n { "ip" => Some("inet"), "u" => Some("uuid"), _ => None };
        let marked = parsing(schema.clone(), held);
        assert_eq!((of(marked.field(0)).map(|t| t.name), of(marked.field(1)).map(|t| t.name)), (Some("inet"), None));
        assert!(Arc::ptr_eq(&parsing(schema.clone(), |_| None), &schema), "none marked: the same schema");
        let text: ArrayRef = Arc::new(StringArray::from(vec![Some("10.0.0.1"), None]));
        let read = parsed(&text, marked.field(0)).expect("text for an inet").expect("an address");
        assert_eq!((read.data_type(), read.is_null(1)), (&DataType::FixedSizeBinary(inet::WIDTH as i32), true));
        assert_eq!(show_of(marked.field(0)).expect("inet shows")(&read).unwrap().as_string::<i32>().value(0), "10.0.0.1");
        assert!(parsed(&text, marked.field(1)).is_none() && parsed(&(Arc::new(Int32Array::from(vec![1])) as ArrayRef), marked.field(0)).is_none());
        let refused = parsed(&(Arc::new(StringArray::from(vec!["10.0.0.256"])) as ArrayRef), marked.field(0)).unwrap().unwrap_err();
        assert_eq!(crate::codes::of(&refused), "22P02");
    }
}
