//! How a column of a held type keeps its values and refuses the ones it can't, at every door that writes rows: a uuid one
//! way, lower case with hyphens; a json value as written, once it parses; a jsonb value as jsonb writes it (keys sorted and
//! once), so `=`, `GROUP BY` and `DISTINCT` compare as Postgres does; a `varchar(n)` value at most `n` characters, trailing
//! spaces aside (Postgres cuts those, and this keeps them). An enum column takes only its labels, and each refusal carries
//! Postgres's SQLSTATE (22P02, 22001, 54001).
use crate::pg_casts::{quoted, uuid_of};
use crate::pg_json::json_refusal;
use crate::rules::{Column, Table};
use crate::text::is_uuid;
use anyhow::Result;
use datafusion::arrow::array::{Array, AsArray, RecordBatch};
use datafusion::arrow::datatypes::DataType;

/// The most characters a `varchar(n)` column's values have.
pub fn length(t: &str) -> Option<usize> {
    t.strip_prefix("varchar(")?.strip_suffix(')')?.parse().ok()
}

/// A type a column keeps as text and checks (`Column::held`): `uuid`, `json`, `jsonb` or `varchar(n)`.
pub fn held(t: &str) -> bool { matches!(t, "uuid" | "json" | "jsonb") || length(t).is_some() }

/// `v` is longer than `n` characters, but for trailing spaces (Postgres takes those, and cuts them).
fn too_long(v: &str, n: usize) -> bool {
    v.len() > n && v.trim_end_matches(' ').chars().count() > n
}

/// The name a column's values are under in `rows`: SQL's, or the stored one.
fn in_rows(c: &Column, sql_names: bool) -> &str {
    if sql_names { &c.name } else { &c.stored }
}

/// `rows`' uuid columns (`Column::held`), under SQL's names when `sql_names` (else the stored ones), written the one way
/// from any spelling Postgres takes, as Postgres's `COPY` reads them; what isn't a uuid is refused (22P02). A column
/// already written so is left as it is (nearly always: a pass over its values).
pub fn spelled(t: &Table, rows: RecordBatch, sql_names: bool) -> Result<RecordBatch> {
    let mut cols = rows.columns().to_vec();
    for column in t.columns.iter().filter(|c| c.held.as_deref() == Some("uuid")) {
        let Some(i) = rows.schema().index_of(in_rows(column, sql_names)).ok() else { continue };
        let Some(a) = cols[i].as_string_opt::<i32>() else { continue };
        if a.iter().flatten().all(is_uuid) {
            continue;
        }
        let one_way: datafusion::arrow::array::StringArray = a.iter().map(|v| v.map(uuid_of).transpose()).collect::<Result<_>>()?;
        cols[i] = std::sync::Arc::new(one_way);
    }
    Ok(RecordBatch::try_new(rows.schema(), cols)?)
}

/// `rows` with each jsonb column's values (`Column::held`) written as jsonb writes them (`jsonb_text`: keys
/// sorted and once, Postgres's spacing), so `=`, `GROUP BY` and `DISTINCT` compare values as Postgres's do; a json column
/// keeps its text as written. What isn't JSON is refused as `check` refuses it (22P02). A column already written
/// so is left as it is. `sql_names`: whether `rows` are under SQL's names or stored ones, as for `check`.
pub fn as_held(t: &Table, rows: RecordBatch, sql_names: bool) -> Result<RecordBatch> {
    if !t.columns.iter().any(|c| c.held.as_deref() == Some("jsonb")) {
        return Ok(rows);
    }
    let mut cols = rows.columns().to_vec();
    let mut changed = false;
    for column in t.columns.iter().filter(|c| c.held.as_deref() == Some("jsonb")) {
        let name = in_rows(column, sql_names);
        let Some(i) = rows.schema().index_of(name).ok() else { continue };
        let c = cols[i].clone();
        if c.null_count() == c.len() {
            continue;
        }
        let text = datafusion::arrow::compute::cast(&c, &DataType::Utf8)?;
        let text = text.as_string::<i32>();
        let (mut same, mut refused) = (true, None);
        let written = crate::pg_json_make::per_row(text.len(), |r, out| {
            if text.is_null(r) {
                return Ok(false);
            }
            let v = text.value(r);
            if let Err(e) = crate::pg_json_make::jsonb_into(out, v) {
                refused = Some(jsonb_refused(name, v, e));
                return Err(datafusion::error::DataFusionError::Internal("a refused jsonb value".into())); // (stops the batch: `refused` says why)
            }
            same &= out.as_str() == v;
            Ok(true)
        });
        if let Some(e) = refused {
            return Err(e);
        }
        if same {
            continue;
        }
        cols[i] = datafusion::arrow::compute::cast(&written?, c.data_type())?;
        changed = true;
    }
    match changed {
        true => Ok(RecordBatch::try_new(rows.schema(), cols)?),
        false => Ok(rows),
    }
}

/// The refusal of a jsonb value `jsonb_into` couldn't write: `refusal`'s message and SQLSTATE, as `check` words them. Where
/// `refusal` takes no value (it is JSON, but a number past what `numeric` holds is only jsonb's), the words are JSON's and
/// the error's own SQLSTATE stays (a lone surrogate's escape is JSON, but not jsonb's).
fn jsonb_refused(column: &str, v: &str, e: datafusion::error::DataFusionError) -> anyhow::Error {
    let code = crate::codes::of(&anyhow::Error::from(e));
    let (message, code) = refusal("jsonb", column, v).unwrap_or_else(|| (json_refusal(v), code));
    anyhow::Error::new(crate::codes::Violation(message, code))
}

/// Why a uuid column refuses a value: not a uuid, or one written another way than the column holds.
fn refused_uuid(column: &str, s: &str) -> String {
    match uuid_of(s) {
        Ok(u) => format!("invalid input syntax for type uuid: \"{s}\" (column {column} holds a uuid one way, lower case with hyphens: write '{u}', or cast it, '{s}'::uuid)"),
        Err(_) => format!("invalid input syntax for type uuid: \"{s}\" (column {column} holds uuids)"),
    }
}

/// Why a value of a column held as `t` is refused, with its message and SQLSTATE; None when it is taken.
/// The column's name is the one the message gives.
fn refusal(t: &str, column: &str, v: &str) -> Option<(String, &'static str)> {
    match t {
        "uuid" if !is_uuid(v) => Some((refused_uuid(column, v), "22P02")),
        // (the reader the functions use decides: 22P02 for text that isn't JSON, 54001 past its levels)
        "json" | "jsonb" => crate::pg_json_make::refused(v).map(|code| match code {
            "54001" => (format!("stack depth limit exceeded (column {column} holds {t}: a document nested past {} levels)", crate::pg_json_make::MAX_DEPTH), code),
            _ => (format!("{} (column {column} holds {t})", json_refusal(v)), code),
        }),
        _ => match length(t) {
            Some(n) if too_long(v, n) => Some((format!("value too long for type character varying({n}) (column {column}: a value of {} characters)", v.chars().count()), "22001")),
            _ => None,
        },
    }
}

/// Every door's rows (`rules::checks`): a value its column's enum doesn't list, or that isn't a
/// uuid written the one way, or JSON, is refused (Postgres's 22P02), and one longer than its VARCHAR(n)
/// (22001), the writer's alone. `marker`: a keyed table's delete markers carry no values.
pub fn check(t: &Table, rows: &RecordBatch, sql_names: bool, marker: &dyn Fn(usize) -> bool) -> Result<()> {
    for column in &t.columns {
        let Some((of, labels)) = &column.labels else { continue };
        let Some(c) = rows.column_by_name(in_rows(column, sql_names)) else { continue };
        if c.null_count() == c.len() {
            continue;
        }
        let text = datafusion::arrow::compute::cast(c, &DataType::Utf8)?;
        let text = text.as_string::<i32>();
        let set: std::collections::HashSet<&str> = labels.iter().map(String::as_str).collect();
        if let Some(i) = (0..text.len()).find(|&i| text.is_valid(i) && !set.contains(text.value(i)) && !marker(i)) {
            let what = of.clone().unwrap_or_else(|| format!("column {}", column.name));
            let message = format!("invalid input value for enum {what}: \"{}\" (its labels: {})", text.value(i), quoted(labels));
            return Err(anyhow::Error::new(crate::codes::Violation(message, "22P02")));
        }
    }
    for column in &t.columns {
        let Some(h) = column.held.as_deref() else { continue };
        let Some(c) = rows.column_by_name(in_rows(column, sql_names)) else { continue };
        if !held(h) || c.null_count() == c.len() {
            continue; // (a type a newer release keeps: its own nodes check it)
        }
        let text = datafusion::arrow::compute::cast(c, &DataType::Utf8)?;
        let text = text.as_string::<i32>();
        let first = (0..text.len()).filter(|&i| text.is_valid(i) && !marker(i)).find_map(|i| refusal(h, &column.name, text.value(i)));
        if let Some((message, code)) = first {
            return Err(anyhow::Error::new(crate::codes::Violation(message, code)));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{ArrayRef, StringArray, StringViewArray};
    use datafusion::arrow::datatypes::{Field, Schema};
    use std::sync::Arc;

    /// A column of `name`, held as `held` (None: as plain text), with no rules else.
    fn column(name: &str, held: Option<&str>) -> Column {
        Column { name: name.into(), stored: name.into(), not_null: false, default: None, always: false, held: held.map(String::from), labels: None }
    }

    /// A table of these columns, with no key and no constraint.
    fn table(columns: Vec<Column>) -> Table {
        Table { schema: Arc::new(Schema::empty()), columns, key: vec![], key_checked: false, unique: vec![], checks: vec![], mapped: false }
    }

    /// A table with a jsonb column `j` and a json column `k`.
    fn held_table() -> Table { table(vec![column("j", Some("jsonb")), column("k", Some("json"))]) }

    /// Two text columns, `j` and `k` (None: NULL).
    fn held_batch(j: Vec<Option<&str>>, k: Vec<Option<&str>>) -> RecordBatch {
        let schema = Schema::new(vec![Field::new("j", DataType::Utf8, true), Field::new("k", DataType::Utf8, true)]);
        let cols: Vec<ArrayRef> = vec![Arc::new(StringArray::from(j)) as ArrayRef, Arc::new(StringArray::from(k)) as ArrayRef];
        RecordBatch::try_new(Arc::new(schema), cols).unwrap()
    }

    #[test]
    fn as_held_writes_jsonb_as_postgres_does_and_leaves_json_as_written() {
        let rows = held_batch(vec![Some(r#"{"b":2,"a":1}"#), Some("[1,  2]"), None, Some(r#"{"a": 1}"#)], vec![Some(r#"{"b":2}"#); 4]);
        let k_before = rows.column_by_name("k").unwrap().clone();
        let out = as_held(&held_table(), rows, true).unwrap();
        let j = out.column_by_name("j").unwrap().as_string::<i32>();
        assert_eq!(j.iter().collect::<Vec<_>>(), vec![Some(r#"{"a": 1, "b": 2}"#), Some("[1, 2]"), None, Some(r#"{"a": 1}"#)]);
        assert!(Arc::ptr_eq(&k_before, out.column_by_name("k").unwrap()), "a json column is not touched");
    }

    #[test]
    fn as_held_returns_a_batch_already_in_jsonb_form_with_the_same_array() {
        let rows = held_batch(vec![Some(r#"{"a": 1, "b": 2}"#), Some("[1, 2]"), None], vec![Some(r#"{"b":2}"#); 3]);
        let before = rows.column_by_name("j").unwrap().clone();
        let out = as_held(&held_table(), rows, true).unwrap();
        assert!(Arc::ptr_eq(&before, out.column_by_name("j").unwrap()));
    }

    #[test]
    fn as_held_refuses_what_is_not_json_with_checks_words() {
        let (table, rows) = (held_table(), held_batch(vec![Some(r#"{"a":"#)], vec![Some(r#"{"b":2}"#)]));
        let e = as_held(&table, rows.clone(), true).unwrap_err();
        assert_eq!(crate::codes::of(&e), "22P02");
        assert!(e.to_string().contains("(column j holds jsonb)"), "{e}");
        let checked = check(&table, &rows, true, &|_: usize| false).unwrap_err();
        assert_eq!(e.to_string(), checked.to_string());
    }

    #[test]
    fn as_held_keeps_a_utf8view_column_a_view() {
        let schema = Schema::new(vec![Field::new("j", DataType::Utf8View, true)]);
        let col = StringViewArray::from(vec![Some(r#"{"b":2,"a":1}"#)]);
        let rows = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(col) as ArrayRef]).unwrap(); // (no `k`: a column the batch lacks is skipped)
        let out = as_held(&held_table(), rows, true).unwrap();
        let j = out.column_by_name("j").unwrap();
        assert_eq!(j.data_type(), &DataType::Utf8View);
        assert_eq!(j.as_string_view().value(0), r#"{"a": 1, "b": 2}"#);
    }

    #[test]
    fn a_json_column_takes_the_levels_the_functions_read() {
        let nested = |n: usize| format!("{}{}", "[".repeat(n), "]".repeat(n));
        assert_eq!(refusal("jsonb", "c", &nested(200)), None);
        assert_eq!(refusal("jsonb", "c", "[1,").map(|(_, code)| code), Some("22P02"));
        let (message, code) = refusal("json", "c", &nested(crate::pg_json_make::MAX_DEPTH + 1)).unwrap();
        assert_eq!(code, "54001");
        assert!(message.contains(&crate::pg_json_make::MAX_DEPTH.to_string()), "{message}");
    }

    #[test]
    fn check_reads_stored_names_and_names_the_column_by_its_sql_name() {
        let renamed = Column { name: "pretty".into(), stored: "c1".into(), ..column("pretty", Some("varchar(3)")) };
        let rows = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("c1", DataType::Utf8, true)])),
            vec![Arc::new(StringArray::from(vec![Some("abc"), Some("abcd")])) as ArrayRef],
        )
        .unwrap();
        let e = check(&table(vec![renamed]), &rows, false, &|_: usize| false).unwrap_err();
        assert_eq!(crate::codes::of(&e), "22001");
        assert!(e.to_string().contains("pretty"), "{e}");
    }

    #[test]
    fn spelled_writes_a_uuid_the_one_way_and_refuses_what_is_not_one() {
        let uuid_table = table(vec![column("u", Some("uuid"))]);
        let batch = |v: &str| {
            RecordBatch::try_new(Arc::new(Schema::new(vec![Field::new("u", DataType::Utf8, true)])), vec![Arc::new(StringArray::from(vec![Some(v)])) as ArrayRef]).unwrap()
        };
        let out = spelled(&uuid_table, batch("A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11"), true).unwrap();
        assert_eq!(out.column_by_name("u").unwrap().as_string::<i32>().value(0), "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
        let e = spelled(&uuid_table, batch("nope"), true).unwrap_err();
        assert_eq!(crate::codes::of(&e), "22P02");
    }
}
