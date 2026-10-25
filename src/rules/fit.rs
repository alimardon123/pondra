//! How a write's rows are fitted to a table's columns, by position or by name, and cast to its types, refusing what won't go.
//! How a CHECK condition is evaluated over rows, in the session it is planned in.
use crate::optimize::seconds;
use anyhow::{bail, Result};
use datafusion::arrow::array::{Array, ArrayRef, AsArray, BooleanArray};
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::SessionContext;
use std::sync::Arc;

/// `b` with `s`'s columns, by name: older rows (written before an ALTER TABLE … ADD COLUMN) get
/// nulls for the columns added since.
pub fn conform(b: &RecordBatch, s: &SchemaRef) -> Result<RecordBatch> {
    if b.schema().fields() == s.fields() {
        return Ok(b.clone());
    }
    let columns = s.fields().iter().map(|f| match b.column_by_name(f.name()) {
        Some(c) => Ok(strict(c, f.data_type())?),
        None => Ok(datafusion::arrow::array::new_null_array(f.data_type(), b.num_rows())),
    });
    Ok(RecordBatch::try_new(s.clone(), columns.collect::<Result<Vec<_>>>()?)?)
}

/// A column as type `t`, or an error saying which value wouldn't go (never a quiet NULL): `casts::into`, which
/// rounds a fraction going into an integer column as a cast does. Text meant as a timestamp gets its seconds first.
pub fn strict(c: &ArrayRef, t: &DataType) -> Result<ArrayRef> {
    Ok(match (c.data_type(), t) {
        (DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8, DataType::Timestamp(..)) => crate::casts::into(&with_seconds(c)?, t)?, // ('… 10:00')
        _ => crate::casts::into(c, t)?,
    })
}

/// Text meant as timestamps, the ones without seconds given them (`seconds`).
fn with_seconds(c: &ArrayRef) -> Result<ArrayRef> {
    use datafusion::arrow::array::{AsArray, StringArray};
    let text = datafusion::arrow::compute::cast(c, &DataType::Utf8)?;
    let text = text.as_string::<i32>();
    if !text.iter().flatten().any(|s| seconds(s).is_some()) {
        return Ok(c.clone());
    }
    Ok(Arc::new(text.iter().map(|s| s.map(|s| seconds(s).unwrap_or_else(|| s.to_string()))).collect::<StringArray>()) as ArrayRef)
}

/// The columns an INSERT without a column list fills, as Postgres takes them: the first `width` of the table's (all of
/// them when the width isn't known, or isn't smaller). The rest take their DEFAULTs (`fitted`).
pub fn implicit(columns: &[String], width: Option<usize>) -> &[String] {
    match width {
        Some(n) if n < columns.len() => &columns[..n],
        _ => columns,
    }
}

/// A query's rows as the table's columns, in its order and types (`rows` fits them with `fitted`; the tests of `literals.rs`
/// fit a literal's rows with this, where no table has defaults to fill).
#[cfg(test)]
pub fn fit(target: &SchemaRef, all: &RecordBatch) -> Result<RecordBatch> { fit_with(target, all).map(|(batch, _)| batch) }

/// The columns a row fills by position: the table's, but its delete marker (`_deleted`), which SQL has no column for. A
/// row gives a marker only by name (a change's rows name it, and so does a column list naming it): by position a row one
/// value too wide made a delete of an INSERT, which answered `INSERT 0 1` and wrote nothing.
pub fn by_position(target: &datafusion::arrow::datatypes::Schema) -> impl Iterator<Item = &datafusion::arrow::datatypes::FieldRef> {
    target.fields().iter().filter(|f| f.name() != "_deleted")
}

/// `fit`, and the names of the columns the rows left out at the end: NULL in the batch until `fitted` gives them their
/// DEFAULTs. A batch's `_deleted` column is the table's marker, by name; its other columns fill the table's others in
/// order (`by_position`).
pub fn fit_with(target: &SchemaRef, all: &RecordBatch) -> Result<(RecordBatch, Vec<String>)> {
    // An UPDATE's rows end with the ids they keep (`sys.rs`): those go along, by name.
    let kept: Vec<usize> = (0..all.num_columns()).filter(|&i| [crate::text::ROW_ID, crate::text::CREATED].contains(&all.schema().field(i).name().as_str())).collect();
    // (the marker is taken only where the table has one: a table without one takes a `_deleted` column by position, as before)
    let marker = target.fields().iter().any(|f| f.name() == "_deleted").then(|| (0..all.num_columns()).find(|&i| all.schema().field(i).name() == "_deleted")).flatten();
    let values: Vec<usize> = (0..all.num_columns()).filter(|i| !kept.contains(i) && Some(*i) != marker).collect();
    let (ids, given_cols) = (all.project(&kept)?, all.project(&values)?);
    // The given columns fill the table's others in order, the first few (later ones, such as added columns, are null).
    let mut given = given_cols.columns().iter();
    let mut padded = vec![];
    let columns = target.fields().iter().map(|f| {
        if f.name() == "_deleted" {
            // (a row without a marker gives NULL: a missing marker is never a default, so it isn't padded)
            return match marker {
                Some(i) => strict(all.column(i), f.data_type()),
                None => Ok(datafusion::arrow::array::new_null_array(f.data_type(), all.num_rows())),
            };
        }
        match given.len() > 0 {
            true => strict(given.next().expect("counted"), f.data_type()),
            false => {
                padded.push(f.name().clone()); // (left out by the row: its DEFAULT, if it has one, in `fitted`)
                Ok(datafusion::arrow::array::new_null_array(f.data_type(), all.num_rows()))
            }
        }
    }).collect::<Result<Vec<_>, _>>()?;
    if given.len() > 0 {
        // (one value too many for a table with a marker is most likely the marker, declared or not: say how it's written)
        let hint = if given.len() == 1 && marker.is_none() && target.fields().iter().any(|f| f.name() == "_deleted") { "; its delete marker _deleted is written only by name: INSERT INTO t (…, _deleted)" } else { "" };
        return Err(crate::codes::coded("42601", format!("INSERT has more expressions than target columns ({} given, the table has {}){hint}", given_cols.num_columns(), by_position(target).count())));
    }
    let (mut fields, mut columns) = (target.fields().to_vec(), columns);
    fields.extend(ids.schema().fields().iter().cloned());
    columns.extend(ids.columns().iter().cloned());
    Ok((RecordBatch::try_new(Arc::new(datafusion::arrow::datatypes::Schema::new(fields)), columns)?, padded))
}

/// `expr` over `rows` (their columns by name), a value for each row; None if it doesn't plan
/// over them alone (a qualified name, a subquery: SQL works it out instead).
pub fn evaluate(base: &SessionContext, rows: &RecordBatch, expr: &str) -> Result<Option<ArrayRef>> {
    use datafusion::common::DFSchema;
    let schema = DFSchema::try_from(rows.schema().as_ref().clone())?;
    let Ok(e) = base.parse_sql_expr(expr, &schema).and_then(crate::optimize::whole_literals) else { return Ok(None) };
    let Ok(phys) = base.create_physical_expr(e, &schema) else { return Ok(None) };
    Ok(Some(phys.evaluate(rows)?.into_array(rows.num_rows())?))
}

/// Which of `rows` break `check`, a condition over their columns by name: those it is false for
/// (NULL passes, as with Postgres's CHECK). Expectations (`views::Expect`) and tables' CHECKs.
pub fn breaking(base: &SessionContext, rows: &RecordBatch, check: &str) -> Result<BooleanArray> {
    use datafusion::common::DFSchema;
    let schema = DFSchema::try_from(rows.schema().as_ref().clone())?;
    let expr = base.parse_sql_expr(check, &schema).and_then(crate::optimize::whole_literals)?;
    let value = base.create_physical_expr(expr, &schema)?.evaluate(rows)?.into_array(rows.num_rows())?;
    let Some(b) = value.as_boolean_opt() else { bail!("CHECK ({check}) is not a condition: it gives {}, not true or false (a condition, such as CHECK (amount > 0))", value.data_type()) };
    Ok(b.iter().map(|v| Some(v == Some(false))).collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{BooleanArray, Int32Array, StringArray};
    use datafusion::arrow::datatypes::{Field, Schema};

    /// A table of `a`, `b`, `c` and a delete marker, as a keyed table has them.
    fn target() -> SchemaRef {
        let int = |n: &str| Field::new(n, DataType::Int32, true);
        Arc::new(Schema::new(vec![int("a"), int("b"), int("c"), Field::new("_deleted", DataType::Boolean, true)]))
    }

    /// One row of the named columns: `_deleted` a Boolean, the others Int32.
    fn row(names: &[&str]) -> RecordBatch {
        let fields: Vec<Field> = names.iter().map(|n| Field::new(*n, if *n == "_deleted" { DataType::Boolean } else { DataType::Int32 }, true)).collect();
        let columns: Vec<ArrayRef> = names.iter().map(|n| -> ArrayRef { if *n == "_deleted" { Arc::new(BooleanArray::from(vec![false])) } else { Arc::new(Int32Array::from(vec![1])) } }).collect();
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("a row")
    }

    #[test]
    fn a_short_row_pads_what_it_leaves_out_but_not_a_delete_marker() {
        let (out, padded) = fit_with(&target(), &row(&["a"])).expect("fits");
        assert_eq!(padded, ["b", "c"]);
        assert_eq!((out.column(0).null_count(), out.column(1).null_count(), out.column(3).null_count()), (0, 1, 1));
    }

    #[test]
    fn a_full_row_pads_nothing() {
        assert!(fit_with(&target(), &row(&["a", "b", "c"])).expect("fits").1.is_empty());
        assert!(fit_with(&target(), &row(&["a", "b", "c", "_deleted"])).expect("fits").1.is_empty());
    }

    #[test]
    fn a_row_wider_than_the_table_is_refused_as_42601() {
        let err = fit(&target(), &row(&["a", "b", "c", "_deleted", "x"])).expect_err("too wide");
        assert_eq!(crate::codes::of(&err), "42601");
    }

    #[test]
    fn a_row_gives_a_delete_marker_only_by_name() {
        // one value too many, none of them named `_deleted`: refused, not written as a delete
        let err = fit(&target(), &row(&["a", "b", "c", "x"])).expect_err("one value too many");
        assert_eq!(crate::codes::of(&err), "42601");
        // named, the marker fits
        assert!(fit_with(&target(), &row(&["a", "b", "c", "_deleted"])).expect("fits").1.is_empty());
        // a row of a value and a marker: b and c are padded, the marker is the row's own
        let (out, padded) = fit_with(&target(), &row(&["a", "_deleted"])).expect("fits");
        assert_eq!(padded, ["b", "c"]);
        assert_eq!((out.column(1).null_count(), out.column(2).null_count(), out.column(3).null_count()), (1, 1, 0));
    }

    #[test]
    fn strict_refuses_a_text_past_an_integer_as_22003() {
        let text: ArrayRef = Arc::new(StringArray::from(vec!["2147483648"]));
        let err = strict(&text, &DataType::Int32).expect_err("past the range");
        assert_eq!(crate::codes::of(&err), "22003");
        assert_eq!(crate::codes::words(&err), "value \"2147483648\" is out of range for type integer");
    }

    #[test]
    fn conform_gives_nulls_for_a_column_the_batch_lacks() {
        let target: SchemaRef = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true), Field::new("b", DataType::Int32, true)]));
        let batch = RecordBatch::try_new(Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)])), vec![Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef]).expect("a batch");
        let out = conform(&batch, &target).expect("conforms");
        assert_eq!(out.schema(), target);
        assert_eq!((out.column(0).null_count(), out.column(1).null_count()), (0, 2));
    }

    #[test]
    fn breaking_finds_the_rows_a_check_is_false_for_and_passes_a_null() {
        let n: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), Some(0), None, Some(-1)]));
        let rows = RecordBatch::try_new(Arc::new(Schema::new(vec![Field::new("n", DataType::Int32, true)])), vec![n]).expect("rows");
        let broke = breaking(&SessionContext::new(), &rows, "n > 0").expect("plans");
        assert_eq!(broke.iter().collect::<Vec<_>>(), [Some(false), Some(true), Some(false), Some(true)]);
    }
}
