//! A write's rows may not give one key twice, and may not give an ALWAYS identity a value. The checks against rows a table
//! already keeps stay with the host, which holds those rows.
use super::Table;
use anyhow::{bail, ensure, Result};
use datafusion::arrow::array::RecordBatch;

/// The name Postgres gives a primary key that isn't named: `{t}_pkey`, `t` the table's name without its schema.
pub fn pkey(table: &str) -> String { format!("{}_pkey", table.rsplit('.').next().unwrap_or(table)) }

/// The key's name, and its columns in SQL's names (`key`), after checking that no two of `new`'s rows share a key: refused as a
/// plain INSERT's are (23505 with `{table}_pkey` and Postgres's words).
pub fn once(table: &str, key: &[String], new: &RecordBatch) -> Result<(String, Vec<String>)> {
    let name = pkey(table); // (Postgres's name for a primary key that isn't named)
    if let Some(found) = twice(new, key, &[])? {
        return Err(violated(&name, key, found));
    }
    Ok((name, key.to_vec()))
}

/// The 23505 a duplicate gives, with Postgres's words.
pub fn violated(name: &str, columns: &[String], key: String) -> anyhow::Error {
    crate::codes::coded("23505", format!("duplicate key value violates unique constraint \"{name}\": Key ({})=({key}) already exists", columns.join(", ")))
}

/// A value of `columns` two new rows give (of two keys, in a keyed table: one key's rows are its
/// versions), as Postgres's DETAIL writes it; None if there's none. The rows are hashed a column at a
/// time, as a hash join hashes its keys, and only rows of one hash are compared (`hashed`); a type
/// that can't be hashed or compared so, or two values sharing a 64-bit hash, goes the exact way.
pub fn twice(new: &RecordBatch, columns: &[String], key: &[String]) -> Result<Option<String>> {
    let pick = |names: &[String]| names.iter().map(|c| new.column_by_name(c).cloned().ok_or_else(|| anyhow::anyhow!("no column {c}"))).collect::<Result<Vec<_>>>();
    let (values, keys) = (pick(columns)?, pick(key)?);
    hashed(&values, &keys).or_else(|_| exactly(&values, &keys))
}

/// `twice` by each row's hash: a million new keys take tens of milliseconds, where hashing each
/// row's bytes in turn (`exactly`) took a fifth of a second. Errs when it can't decide.
fn hashed(values: &[datafusion::arrow::array::ArrayRef], keys: &[datafusion::arrow::array::ArrayRef]) -> Result<Option<String>> {
    use datafusion::arrow::array::{make_comparator, DynComparator};
    use datafusion::common::hash_utils::{create_hashes, RandomState};
    let n = values.first().map_or(0, |v| v.len());
    let mut hashes = vec![0u64; n];
    create_hashes(values, &RandomState::with_seed(0), &mut hashes)?;
    let comparators = |columns: &[datafusion::arrow::array::ArrayRef]| {
        columns.iter().map(|c| make_comparator(c.as_ref(), c.as_ref(), Default::default())).collect::<std::result::Result<Vec<DynComparator>, _>>()
    };
    let (value, key) = (comparators(values)?, comparators(keys)?);
    let equal = |columns: &[DynComparator], i: usize, j: usize| columns.iter().all(|c| c(i, j).is_eq());
    let mut first = std::collections::HashMap::with_capacity_and_hasher(n, RandomState::with_seed(0)); // (each hash's first row)
    for i in 0..n {
        if values.iter().any(|v| v.is_null(i)) {
            continue;
        }
        let j = *first.entry(hashes[i]).or_insert(i);
        if j == i {
            continue;
        }
        ensure!(equal(&value, i, j), "two values share a hash"); // (rare enough to go over every row again)
        if keys.is_empty() || !equal(&key, i, j) {
            return Ok(Some(shown(values, i)));
        }
    }
    Ok(None)
}

/// `twice` by each row's bytes (`RowConverter`), for what `hashed` can't decide.
fn exactly(values: &[datafusion::arrow::array::ArrayRef], keys: &[datafusion::arrow::array::ArrayRef]) -> Result<Option<String>> {
    use datafusion::arrow::row::{RowConverter, SortField};
    let converter = |a: &[datafusion::arrow::array::ArrayRef]| RowConverter::new(a.iter().map(|a| SortField::new(a.data_type().clone())).collect());
    let rows = converter(values)?.convert_columns(values)?;
    let key_rows = match keys.is_empty() {
        true => None,
        false => Some(converter(keys)?.convert_columns(keys)?),
    };
    // (rows held as references into the converted buffers: a Vec made for each row took a million-row INSERT's check 0.5–0.9 s)
    let mut seen = std::collections::HashMap::with_capacity(rows.num_rows());
    for i in 0..rows.num_rows() {
        if values.iter().any(|v| v.is_null(i)) {
            continue;
        }
        let k = key_rows.as_ref().map(|k| k.row(i));
        if let Some(before) = seen.insert(rows.row(i), k) {
            if key_rows.is_none() || before != k {
                return Ok(Some(shown(values, i)));
            }
        }
    }
    Ok(None)
}

/// For each row of `of`: is its key (the columns `key`) absent from `among`'s rows? `among`'s key
/// columns are cast to `of`'s types first. A change uses it to tell which old rows a new version
/// replaces: by key, so an UPDATE that moves a key frees the old one, and a DELETE and INSERT of one
/// key in one change keep the key's new row.
pub fn absent(of: &RecordBatch, among: &RecordBatch, key: &[String]) -> Result<datafusion::arrow::array::BooleanArray> {
    use datafusion::arrow::row::{RowConverter, SortField};
    let columns = |b: &RecordBatch| key.iter().map(|c| b.column_by_name(c).cloned().ok_or_else(|| anyhow::anyhow!("no column {c}"))).collect::<Result<Vec<_>>>();
    let mine = columns(of)?;
    let converter = RowConverter::new(mine.iter().map(|a| SortField::new(a.data_type().clone())).collect())?;
    let theirs = columns(among)?.iter().zip(&mine).map(|(a, m)| datafusion::arrow::compute::cast(a, m.data_type())).collect::<std::result::Result<Vec<_>, _>>()?;
    let held: std::collections::HashSet<Vec<u8>> = converter.convert_columns(&theirs)?.iter().map(|r| r.as_ref().to_vec()).collect();
    Ok(datafusion::arrow::array::BooleanArray::from(converter.convert_columns(&mine)?.iter().map(|r| Some(!held.contains(r.as_ref()))).collect::<Vec<_>>()))
}

/// Row `i` of these columns, as Postgres's DETAIL writes a key: `a@b.c, 2`.
pub fn shown(columns: &[datafusion::arrow::array::ArrayRef], i: usize) -> String {
    columns.iter().map(|c| datafusion::arrow::util::display::array_value_to_string(c, i).unwrap_or_default()).collect::<Vec<_>>().join(", ")
}

/// Which way an INSERT gives its columns: by name, or by position (`INSERT INTO t query`).
pub enum Said {
    /// An INSERT that names its columns.
    Named,
    /// A positional `INSERT INTO t query`, whose rows give the columns they have.
    Positional,
}

/// `INSERT` giving `columns` (as SQL names them): refused if one is an ALWAYS identity (428C9).
pub fn given(t: &Table, table: &str, columns: &[String], said: Said) -> Result<()> {
    let ending = match said {
        Said::Named => "leave it out, or say DEFAULT",
        Said::Positional => "name the columns you give, or say DEFAULT",
    };
    match t.columns.iter().find(|c| c.always && columns.contains(&c.name)) {
        Some(c) => bail!("cannot insert a non-DEFAULT value into column \"{}\" of {table}: it is an identity column defined as GENERATED ALWAYS ({ending})", c.name),
        None => Ok(()),
    }
}

/// `UPDATE` setting `columns` (as SQL names them): refused if one is an ALWAYS identity (428C9).
pub fn set(t: &Table, columns: &[String]) -> Result<()> {
    match t.columns.iter().find(|c| c.always && columns.contains(&c.name)) {
        Some(c) => bail!("column \"{}\" can only be updated to DEFAULT: it is an identity column defined as GENERATED ALWAYS", c.name),
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rules::Column;
    use datafusion::arrow::array::{ArrayRef, Int64Array, StringArray};
    use datafusion::arrow::datatypes::Schema;
    use std::sync::Arc;

    /// A table of plain columns, the ones in `always` ALWAYS identities (the schema is not what these check).
    fn table(names: &[&str], always: &[&str]) -> Table {
        let column = |n: &&str| Column { name: n.to_string(), stored: n.to_string(), not_null: false, default: None, always: always.contains(n), held: None, labels: None };
        Table { schema: Arc::new(Schema::empty()), columns: names.iter().map(column).collect(), key: vec![], key_checked: false, unique: vec![], checks: vec![], mapped: false }
    }

    fn ints(v: &[Option<i64>]) -> ArrayRef {
        Arc::new(Int64Array::from(v.to_vec()))
    }

    #[test]
    fn two_rows_of_one_value_are_found_by_their_hash_as_by_their_bytes() {
        let names: ArrayRef = Arc::new(StringArray::from(vec![Some("a"), Some("b"), None, Some("a")]));
        let cases: Vec<(Vec<ArrayRef>, Vec<ArrayRef>, Option<&str>)> = vec![
            (vec![ints(&[Some(1), Some(2), Some(3)])], vec![], None),
            (vec![ints(&[Some(1), Some(2), Some(1)])], vec![], Some("1")),
            (vec![ints(&[None, Some(2), None])], vec![], None), // (NULLs never meet)
            (vec![names.clone()], vec![], Some("a")),
            (vec![ints(&[Some(1), Some(1), Some(2), Some(2)]), names.clone()], vec![], None), // (two columns: each pair differs)
            (vec![ints(&[Some(7), Some(7)])], vec![ints(&[Some(1), Some(1)])], None), // (UNIQUE: one key's versions)
            (vec![ints(&[Some(7), Some(7)])], vec![ints(&[Some(1), Some(2)])], Some("7")), // (UNIQUE: two keys)
        ];
        for (values, keys, want) in cases {
            let by_hash = hashed(&values, &keys).expect("decided");
            assert_eq!(by_hash, exactly(&values, &keys).expect("decided"), "{values:?} {keys:?}");
            assert_eq!(by_hash.as_deref(), want, "{values:?} {keys:?}");
        }
    }

    #[test]
    fn a_primary_key_is_named_as_postgres_names_it() {
        assert_eq!(pkey("s.t"), "t_pkey");
        assert_eq!(pkey("t"), "t_pkey");
        let key = vec!["id".to_string()];
        let one = RecordBatch::try_from_iter([("id", ints(&[Some(1)]))]).unwrap();
        assert_eq!(once("s.t", &key, &one).unwrap().0, "t_pkey");
    }

    #[test]
    fn once_refuses_two_rows_of_one_key_as_a_plain_insert_does() {
        let key = vec!["id".to_string()];
        let two = RecordBatch::try_from_iter([("id", ints(&[Some(1), Some(1)]))]).unwrap();
        let err = once("s.t", &key, &two).unwrap_err();
        assert_eq!(crate::codes::of(&err), "23505");
        assert_eq!(err.to_string(), "duplicate key value violates unique constraint \"t_pkey\": Key (id)=(1) already exists");
        let apart = RecordBatch::try_from_iter([("id", ints(&[Some(1), Some(2)]))]).unwrap();
        assert_eq!(once("s.t", &key, &apart).unwrap(), ("t_pkey".to_string(), key.clone()));
    }

    #[test]
    fn given_refuses_an_always_identity_with_the_ending_of_each_form() {
        let t = table(&["id", "v"], &["id"]);
        let named = given(&t, "s.t", &["id".to_string()], Said::Named).unwrap_err();
        assert_eq!(named.to_string(), "cannot insert a non-DEFAULT value into column \"id\" of s.t: it is an identity column defined as GENERATED ALWAYS (leave it out, or say DEFAULT)");
        assert_eq!(crate::codes::of(&named), "428C9");
        let positional = given(&t, "s.t", &["v".to_string(), "id".to_string()], Said::Positional).unwrap_err();
        assert_eq!(positional.to_string(), "cannot insert a non-DEFAULT value into column \"id\" of s.t: it is an identity column defined as GENERATED ALWAYS (name the columns you give, or say DEFAULT)");
        given(&t, "s.t", &["v".to_string()], Said::Named).unwrap();
        given(&t, "s.t", &["v".to_string()], Said::Positional).unwrap();
    }

    #[test]
    fn set_refuses_an_always_identity_with_check_sets_words() {
        let t = table(&["id", "v"], &["id"]);
        assert_eq!(set(&t, &["id".to_string()]).unwrap_err().to_string(), "column \"id\" can only be updated to DEFAULT: it is an identity column defined as GENERATED ALWAYS");
        set(&t, &["v".to_string()]).unwrap();
    }
}
