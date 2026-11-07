//! A section read as a table (designs/redesign/redesign.md §6): `pondra.settings`, `pondra.kinds` and every list after
//! them is one `List` in the registry's lists section, added by the extension that owns the section, and this module makes
//! each one a table the same way. A docs page is made from the same rows (`rows`), so a list and its page can't disagree,
//! and no list is written by hand.
use crate::registry::{Registry, Section};
use datafusion::arrow::array::{ArrayRef, BooleanArray, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use serde_json::Value;
use std::sync::Arc;

/// What a column holds.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Cell {
    /// Text: a string as it is, a list's strings joined with ", " (an empty list is ""), anything else NULL.
    Text,
    /// true or false, anything else NULL.
    Flag,
}

/// One list: `pondra.<name>`, its columns in order, and its rows, each a JSON object of those columns, read from the
/// registry the list is in.
pub struct List {
    pub name: &'static str,
    pub columns: &'static [(&'static str, Cell)],
    pub rows: fn(&Registry) -> Vec<Value>,
}

/// The registry's lists section: every `pondra.*` list a section gives, each name once.
#[derive(Default)]
pub struct Lists(Vec<List>);

impl Section for Lists {
    const NAME: &'static str = "lists";
}

impl Lists {
    pub fn add(&mut self, list: List) {
        assert!(self.0.iter().all(|l| l.name != list.name), "two lists are named pondra.{}", list.name);
        self.0.push(list);
    }

    /// Every list, in the order its extensions added them.
    pub fn all(&self) -> &[List] {
        &self.0
    }
}

/// Every list of this build.
pub fn all() -> &'static [List] {
    crate::registry::get().section::<Lists>().all()
}

/// The lists `sql` names (`pondra.settings`, …), which are all a query reads of them.
pub fn mentioned(sql: &str) -> Vec<&'static List> {
    let s = sql.to_lowercase();
    all().iter().filter(|l| s.contains(&format!("pondra.{}", l.name))).collect()
}

/// A list's rows now, as a docs page reads them.
pub fn rows(list: &List) -> Vec<Value> {
    (list.rows)(crate::registry::get())
}

/// These rows as a batch of the list's columns, typed as it says: a list with no rows keeps its columns.
pub fn batch(list: &List, rows: &[Value]) -> datafusion::error::Result<RecordBatch> {
    let fields: Vec<Field> = list.columns.iter().map(|&(name, cell)| Field::new(name, match cell { Cell::Text => DataType::Utf8, Cell::Flag => DataType::Boolean }, true)).collect();
    let columns: Vec<ArrayRef> = list
        .columns
        .iter()
        .map(|&(name, cell)| match cell {
            Cell::Text => Arc::new(rows.iter().map(|r| text(&r[name])).collect::<StringArray>()) as ArrayRef,
            Cell::Flag => Arc::new(rows.iter().map(|r| r[name].as_bool()).collect::<BooleanArray>()) as ArrayRef,
        })
        .collect();
    Ok(RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)?)
}

/// The lists `sql` names, as tables of the `pondra` schema.
pub fn tables(sql: &str) -> datafusion::error::Result<Vec<(&'static str, Arc<dyn datafusion::catalog::TableProvider>)>> {
    mentioned(sql)
        .into_iter()
        .map(|l| {
            let b = batch(l, &rows(l))?;
            Ok((l.name, Arc::new(datafusion::datasource::MemTable::try_new(b.schema(), vec![vec![b]])?) as Arc<dyn datafusion::catalog::TableProvider>))
        })
        .collect()
}

/// A cell as text.
fn text(v: &Value) -> Option<String> {
    match v {
        Value::Array(a) => Some(a.iter().filter_map(Value::as_str).collect::<Vec<_>>().join(", ")),
        Value::String(s) => Some(s.clone()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::BuiltIn;
    use datafusion::arrow::array::Array;
    use serde_json::json;

    const COLUMNS: &[(&str, Cell)] = &[("name", Cell::Text), ("also", Cell::Text), ("on", Cell::Flag)];

    fn words(_: &Registry) -> Vec<Value> {
        vec![json!({"name": "one", "also": ["a", "b"], "on": true}), json!({"name": "two", "also": [], "on": null}), json!({"name": "three", "also": null, "on": false})]
    }

    /// A list's rows become its columns, typed: lists joined, null as NULL.
    #[test]
    fn a_list_is_its_columns() {
        let r = Registry::build(&[&BuiltIn { name: "words", requires: &[], register: |r| r.section_mut::<Lists>().add(List { name: "words", columns: COLUMNS, rows: words }) }]);
        let list = &r.section::<Lists>().all()[0];
        let b = batch(list, &(list.rows)(&r)).unwrap();
        assert_eq!(b.schema().fields().iter().map(|f| (f.name().as_str(), f.data_type().clone())).collect::<Vec<_>>(), [("name", DataType::Utf8), ("also", DataType::Utf8), ("on", DataType::Boolean)]);
        let also = b.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!((also.value(0), also.value(1), also.is_null(2)), ("a, b", "", true));
        let on = b.column(2).as_any().downcast_ref::<BooleanArray>().unwrap();
        assert_eq!((on.value(0), on.is_null(1), on.value(2)), (true, true, false));
        assert_eq!(batch(list, &[]).unwrap().schema(), b.schema(), "an empty list keeps its columns");
    }

    #[test]
    #[should_panic(expected = "two lists are named pondra.words")]
    fn a_name_is_one_lists() {
        let add = |r: &mut Registry| r.section_mut::<Lists>().add(List { name: "words", columns: COLUMNS, rows: words });
        Registry::build(&[&BuiltIn { name: "a", requires: &[], register: add }, &BuiltIn { name: "b", requires: &[], register: add }]);
    }

    /// Each list of the build gives rows of exactly its columns, so a docs page and the table read one shape.
    #[test]
    fn each_lists_rows_are_its_columns() {
        assert!(all().iter().any(|l| l.name == "settings"));
        for l in all() {
            let names: Vec<&str> = l.columns.iter().map(|c| c.0).collect();
            for r in rows(l) {
                let keys: Vec<&str> = r.as_object().expect("a row is an object").keys().map(String::as_str).collect();
                let (mut k, mut n) = (keys.clone(), names.clone());
                k.sort();
                n.sort();
                assert_eq!(k, n, "pondra.{}'s row {r} isn't its columns", l.name);
            }
        }
    }

    #[test]
    fn a_query_names_its_lists() {
        assert_eq!(mentioned("SELECT * FROM Pondra.Settings").iter().map(|l| l.name).collect::<Vec<_>>(), ["settings"]);
        assert!(mentioned("SELECT 1").is_empty());
    }
}
