//! The table as a write's rules see it, built in every form: a node makes one from its catalog entry
//! (`TableMeta::rules`), and a page will from its own definitions. Names are SQL's, as rows reach the rules;
//! only `Column::stored` keeps the name a file or the log holds a column under. Nothing here reads files,
//! storage or the catalog, so the rules over it (`shape`) run the same in a browser as on a node.
pub mod alter;
pub mod create;
pub mod fit;
pub mod keys;
pub mod rows;
pub mod typed;

use anyhow::{bail, Result};
use datafusion::arrow::array::{Array, ArrayRef, AsArray, BooleanArray};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::SessionContext;
use std::collections::HashMap;
use std::future::Future;
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

    /// Does any column have a DEFAULT? Only then does a loading door work out which rows left a column out.
    pub fn fills(&self) -> bool { self.columns.iter().any(|c| c.default.is_some()) }

    /// The columns a loading door's rows left out, for `shape`: none when no column has a DEFAULT (`fills`), else each column
    /// with a DEFAULT that `given` doesn't name (by SQL's name).
    pub fn left_out(&self, given: &[String]) -> LeftOut {
        match self.fills() {
            true => LeftOut::Columns(self.columns.iter().filter(|c| c.default.is_some() && !given.contains(&c.name)).map(|c| c.name.clone()).collect()),
            false => LeftOut::None,
        }
    }

    /// Do rows `shape` made for `a` meet `b`'s rules as they are? The same `Arc` does, and so does an equal `Table` built
    /// apart. `Table` is `PartialEq`, not `Eq`, so the pointer test is explicit.
    pub fn same(a: &Arc<Table>, b: &Arc<Table>) -> bool { Arc::ptr_eq(a, b) || **a == **b }
}

/// A write's rows as they come: by position (`INSERT INTO t VALUES …`), by name with the columns or rows that left a column
/// out (`LeftOut`, a loading door's), or whole, in the table's columns already (an UPDATE's or MERGE's new versions).
pub enum Rows {
    #[cfg_attr(feature = "server", allow(dead_code))] // (a node's SQL paths build it from piece 8)
    Positional(RecordBatch),
    Named(RecordBatch, LeftOut),
    #[cfg_attr(feature = "server", allow(dead_code))] // (a node's SQL paths build it from piece 8)
    Whole(RecordBatch),
}

/// Which rows left a column out, so its DEFAULT fills them: none, every row of these columns, or the rows each mask says.
pub enum LeftOut {
    None,
    Columns(Vec<String>),
    PerRow(HashMap<String, BooleanArray>),
}

/// What the rows are for: a plain INSERT, a loading door's rows, or an UPDATE's or MERGE's new versions.
pub enum How {
    #[cfg_attr(feature = "server", allow(dead_code))] // (a node's SQL paths build it from piece 8)
    Insert,
    Load,
    #[cfg_attr(feature = "server", allow(dead_code))] // (a node's SQL paths build it from piece 8)
    Change,
}

/// Rows `shape` made, and the table they were made for: only `rules` makes one.
#[derive(Clone)]
pub struct Shaped {
    rows: RecordBatch,
    table: Arc<Table>,
}

impl Shaped {
    /// The rows, as the table holds them.
    pub fn rows(&self) -> &RecordBatch { &self.rows }

    /// The rows, taken out of the `Shaped`.
    pub fn into_rows(self) -> RecordBatch { self.rows }

    /// The table the rows were shaped for.
    pub fn table(&self) -> &Arc<Table> { &self.table }
}

/// What a form gives the rules that only it has: the session a CHECK is planned in, and a DEFAULT's values.
pub trait Host: Sync {
    /// The session a CHECK is planned in (a node's `defaults::BASE`).
    fn base(&self) -> &SessionContext;

    /// `n` values of a DEFAULT, `expr` as SQL writes it, each row its own (`uuid()`, `nextval('t_id_seq')`): the rules
    /// cast them to the column's type.
    fn values(&self, expr: &str, n: usize) -> impl Future<Output = Result<ArrayRef>> + Send;
}

/// `rows` with each column's DEFAULT in the rows a write left the column out of: `left_out(column)` says which (None: none).
/// A column with no DEFAULT is not asked about, since `left_out` may be costly. Each value is cast to the column's type.
pub async fn fill<H: Host>(t: &Table, rows: RecordBatch, left_out: impl Fn(&str) -> Option<BooleanArray>, host: &H) -> Result<RecordBatch> {
    if !t.fills() || rows.num_rows() == 0 {
        return Ok(rows);
    }
    let n = rows.num_rows();
    let mut columns = rows.columns().to_vec();
    for (i, f) in rows.schema().fields().iter().enumerate() {
        let Some(default) = t.column(f.name()).and_then(|c| c.default.as_deref()) else { continue };
        let Some(mask) = left_out(f.name()).filter(|m| m.true_count() > 0 && m.len() == n) else { continue };
        let ty = t.schema.field_with_name(f.name())?.data_type();
        let values = fit::strict(&host.values(default, n).await?, ty)?;
        columns[i] = datafusion::arrow::compute::kernels::zip::zip(&mask, &values, &columns[i])?;
    }
    Ok(RecordBatch::try_new(rows.schema(), columns)?)
}

/// The rules a write's rows meet once their shape is right, over the table as it is, in the order Postgres meets them: an
/// enum's labels and a held type's values (`typed::check`), then each NOT NULL column (23502), then each CHECK (`fit::breaking`,
/// planned in `base`, 23514) in the order of its name, as Postgres sorts a table's CHECKs (so a row breaking two names the
/// first by name). `name` is the table's, for the words. `rows` are under SQL's names when `sql_names`, else under the stored
/// ones. A row whose delete marker is set passes the CHECKs and the NOT NULLs.
pub fn checks(t: &Table, name: &str, rows: &RecordBatch, sql_names: bool, base: &SessionContext) -> Result<()> {
    let ruled = !t.checks.is_empty() || t.columns.iter().any(|c| c.not_null || c.held.is_some() || c.labels.is_some());
    if !ruled || rows.num_rows() == 0 {
        return Ok(());
    }
    let deleted = rows.column_by_name("_deleted").and_then(|d| d.as_boolean_opt().cloned());
    let marker = |i: usize| deleted.as_ref().is_some_and(|d| d.is_valid(i) && d.value(i));
    typed::check(t, rows, sql_names, &marker)?;
    for c in t.columns.iter().filter(|c| c.not_null) {
        let Some(col) = rows.column_by_name(if sql_names { &c.name } else { &c.stored }).filter(|col| col.null_count() > 0) else { continue };
        if (0..col.len()).any(|i| col.is_null(i) && !marker(i)) {
            bail!("{name}.{} is NOT NULL, and a row gives it no value", c.name);
        }
    }
    if !t.checks.is_empty() {
        let mut ordered: Vec<&(String, String)> = t.checks.iter().collect(); // (by name: Postgres's order, `strcmp` on the names)
        ordered.sort_by(|a, b| a.0.cmp(&b.0));
        // (under SQL's names, every column there: one a write leaves out is NULL, which passes)
        let named = match sql_names || !t.mapped {
            true => rows.clone(),
            false => {
                let fields: Vec<_> = rows
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| {
                        let sql = t.columns.iter().find(|c| c.stored == *f.name()).map_or(f.name().as_str(), |c| c.name.as_str());
                        Arc::new(f.as_ref().clone().with_name(sql))
                    })
                    .collect();
                RecordBatch::try_new(Arc::new(datafusion::arrow::datatypes::Schema::new(fields)), rows.columns().to_vec())?
            }
        };
        let all = fit::conform(&named, &t.schema)?;
        for (check, condition) in ordered {
            let bad = fit::breaking(base, &all, condition)?;
            if (0..bad.len()).any(|i| bad.value(i) && !marker(i)) {
                return Err(anyhow::Error::new(crate::codes::Violation(format!("new row for relation \"{name}\" violates check constraint \"{check}\": CHECK ({condition})"), "23514")));
            }
        }
    }
    Ok(())
}

/// A write's rows as the table holds them, once checked (`typed::as_held`, then `checks`): what the bulk INSERT's stream runs
/// on each batch, and what a log or a commit runs again on rows already shaped. Rows that break nothing come back as they
/// were, with the same arrays.
pub fn checked(t: &Table, name: &str, rows: RecordBatch, sql_names: bool, base: &SessionContext) -> Result<RecordBatch> {
    let rows = typed::as_held(t, rows, sql_names)?;
    checks(t, name, &rows, sql_names, base)?;
    Ok(rows)
}

/// A write's rows, shaped for the table they go into, in the order every door runs them (§3 of the design, steps 1 to 8):
/// fitted to the table's columns, DEFAULTs for the columns a row left out, held forms written and checked, and, for a plain
/// INSERT into a keyed table, no key given twice. `name` is the table's, for the refusals' words.
pub async fn shape<H: Host>(t: &Arc<Table>, name: &str, rows: Rows, how: How, host: &H) -> Result<Shaped> {
    let rows = match rows {
        Rows::Positional(batch) => {
            let (batch, padded) = fit::fit_with(&t.schema, &batch)?;
            match padded.is_empty() {
                true => batch,
                false => {
                    let n = batch.num_rows();
                    fill(t, batch, |c| padded.iter().any(|p| p == c).then(|| BooleanArray::from(vec![true; n])), host).await?
                }
            }
        }
        Rows::Named(batch, left_out) => {
            let batch = fit::conform(&batch, &t.schema)?;
            let n = batch.num_rows();
            match left_out {
                LeftOut::None => batch,
                LeftOut::Columns(columns) => fill(t, batch, |c| columns.iter().any(|x| x == c).then(|| BooleanArray::from(vec![true; n])), host).await?,
                LeftOut::PerRow(masks) => fill(t, batch, |c| masks.get(c).cloned(), host).await?,
            }
        }
        Rows::Whole(batch) => batch,
    };
    let rows = checked(t, name, rows, true, host.base())?;
    if matches!(how, How::Insert) && t.key_checked {
        keys::once(name, &t.key, &rows)?;
    }
    Ok(Shaped { rows, table: t.clone() })
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    /// A table of `columns` (each with its type), keyed by `key`, with the CHECKs `checks`. A keyed table has its delete
    /// marker last, as it has in its schema. The schema is the columns, nullable as `answer::schema` makes them.
    fn table(key: &[&str], columns: Vec<(Column, DataType)>, checks: &[(&str, &str)]) -> Table {
        let mut fields: Vec<Field> = columns.iter().map(|(c, t)| Field::new(c.name.as_str(), t.clone(), true)).collect();
        let mut cols: Vec<Column> = columns.into_iter().map(|(c, _)| c).collect();
        if !key.is_empty() {
            fields.push(Field::new("_deleted", DataType::Boolean, true));
            cols.push(plain("_deleted"));
        }
        Table {
            schema: Arc::new(Schema::new(fields)),
            columns: cols,
            key: key.iter().map(|k| k.to_string()).collect(),
            key_checked: !key.is_empty(),
            unique: vec![],
            checks: checks.iter().map(|(n, c)| (n.to_string(), c.to_string())).collect(),
            mapped: false,
        }
    }

    /// A column of `name`, with no rules.
    fn plain(name: &str) -> Column {
        Column { name: name.into(), stored: name.into(), not_null: false, default: None, always: false, held: None, labels: None }
    }

    /// The keyed table of the fill and key tests: `id` (its key), `v` (BIGINT, DEFAULT 7), and the delete marker.
    fn keyed() -> Table {
        let id = Column { not_null: true, ..plain("id") };
        let v = Column { default: Some("7".into()), ..plain("v") };
        table(&["id"], vec![(id, DataType::Int64), (v, DataType::Int64)], &[])
    }

    /// The keyed table of the CHECK and NOT NULL tests: `id` (its key), `n` (CHECK (n >= 0)), `m` (NOT NULL), and the marker.
    fn guarded() -> Table {
        let id = Column { not_null: true, ..plain("id") };
        let m = Column { not_null: true, ..plain("m") };
        table(&["id"], vec![(id, DataType::Int64), (plain("n"), DataType::Int64), (m, DataType::Int64)], &[("n_nonneg", "n >= 0")])
    }

    /// The keyed table of the held-type tests: `id` (its key), `e` (an enum of happy and sad), `u` (a uuid), `s` (a varchar(3)),
    /// and the marker.
    fn typed_table() -> Table {
        let id = Column { not_null: true, ..plain("id") };
        let e = Column { labels: Some((Some("mood".into()), vec!["happy".into(), "sad".into()])), ..plain("e") };
        let u = Column { held: Some("uuid".into()), ..plain("u") };
        let s = Column { held: Some("varchar(3)".into()), ..plain("s") };
        table(&["id"], vec![(id, DataType::Int64), (e, DataType::Utf8), (u, DataType::Utf8), (s, DataType::Utf8)], &[])
    }

    /// A host whose DEFAULTs are their text: `values(expr, n)` is `n` copies of `expr`, which `fill` casts to the column's type.
    struct Text(SessionContext);

    impl Host for Text {
        fn base(&self) -> &SessionContext { &self.0 }

        async fn values(&self, expr: &str, n: usize) -> Result<ArrayRef> { Ok(Arc::new(StringArray::from(vec![expr; n]))) }
    }

    fn host() -> Text { Text(SessionContext::new()) }

    fn ints(v: &[Option<i64>]) -> ArrayRef { Arc::new(Int64Array::from(v.to_vec())) }

    fn texts(v: &[Option<&str>]) -> ArrayRef { Arc::new(StringArray::from(v.to_vec())) }

    fn bools(v: &[bool]) -> ArrayRef { Arc::new(BooleanArray::from(v.to_vec())) }

    fn batch(columns: Vec<(&str, ArrayRef)>) -> RecordBatch { RecordBatch::try_from_iter(columns).unwrap() }

    /// The values of the Int64 column `name` of `b`.
    fn column_ints(b: &RecordBatch, name: &str) -> Vec<Option<i64>> {
        b.column_by_name(name).unwrap().as_any().downcast_ref::<Int64Array>().unwrap().iter().collect()
    }

    #[test]
    fn the_marker_is_a_keyed_tables_delete_column() {
        let keyed = table(&["id"], vec![(plain("id"), DataType::Int64), (plain("v"), DataType::Int64)], &[]);
        assert!(keyed.marker("_deleted") && !keyed.marker("v"));
        assert_eq!(keyed.width(), 2, "the delete marker is left out, as by_position leaves it");
        assert!(keyed.column("v").is_some() && keyed.column("gone").is_none());
        let plain_table = table(&[], vec![(plain("id"), DataType::Int64)], &[]);
        assert!(!plain_table.marker("_deleted"), "a table with no key has no marker");
        assert_eq!(plain_table.width(), 1);
    }

    #[test]
    fn checked_is_an_enforced_unique() {
        assert!(!table(&["id"], vec![(plain("id"), DataType::Int64)], &[]).checked());
        let unique = Table { unique: vec![("a_key".into(), vec!["a".into()])], ..table(&[], vec![(plain("a"), DataType::Int64)], &[]) };
        assert!(unique.checked());
    }

    #[test]
    fn a_table_fills_only_when_a_column_has_a_default() {
        assert!(keyed().fills());
        assert!(!guarded().fills());
    }

    #[test]
    fn left_out_names_the_defaults_a_write_leaves_out() {
        assert!(matches!(guarded().left_out(&["id".to_string()]), LeftOut::None), "no DEFAULT: nothing to fill");
        let t = keyed(); // (v's DEFAULT is 7)
        assert!(matches!(t.left_out(&["id".to_string()]), LeftOut::Columns(c) if c == vec!["v".to_string()]), "left out: listed");
        assert!(matches!(t.left_out(&["id".to_string(), "v".to_string()]), LeftOut::Columns(c) if c.is_empty()), "given: not listed");
    }

    #[test]
    fn rows_shaped_for_a_table_meet_the_same_table_or_an_equal_one() {
        let a = Arc::new(keyed());
        assert!(Table::same(&a, &a.clone()), "the same Arc");
        assert!(Table::same(&a, &Arc::new(keyed())), "an equal table, built apart");
        let mut null = keyed();
        null.columns.iter_mut().find(|c| c.name == "v").unwrap().not_null = true;
        let mut check = keyed();
        check.checks.push(("v_pos".into(), "v > 0".into()));
        for changed in [null, check] {
            assert!(!Table::same(&a, &Arc::new(changed)), "a NOT NULL or a CHECK added is a change");
        }
        let typed = Arc::new(typed_table());
        let mut label = typed_table();
        label.columns.iter_mut().find(|c| c.name == "e").unwrap().labels.as_mut().unwrap().1.push("ok".into());
        assert!(!Table::same(&typed, &Arc::new(label)), "a label added is a change");
    }

    #[tokio::test]
    async fn a_positional_row_one_value_too_wide_is_refused_as_42601() {
        let t = Arc::new(keyed());
        let rows = batch(vec![("a", ints(&[Some(1)])), ("b", ints(&[Some(2)])), ("c", ints(&[Some(3)]))]);
        let err = shape(&t, "s.t", Rows::Positional(rows), How::Insert, &host()).await.err().expect("too wide");
        assert_eq!(crate::codes::of(&err), "42601");
        assert!(crate::codes::words(&err).starts_with("INSERT has more expressions than target columns (3 given, the table has 2)"), "{}", crate::codes::words(&err));
    }

    #[tokio::test]
    async fn a_positional_row_that_leaves_its_last_column_out_takes_the_default() {
        let t = Arc::new(keyed());
        let rows = batch(vec![("id", ints(&[Some(1), Some(2)]))]);
        let out = shape(&t, "s.t", Rows::Positional(rows), How::Insert, &host()).await.unwrap();
        assert_eq!(column_ints(out.rows(), "v"), vec![Some(7), Some(7)]);
    }

    #[tokio::test]
    async fn a_named_row_takes_its_default_only_where_the_mask_says() {
        let t = Arc::new(keyed());
        let rows = batch(vec![("id", ints(&[Some(1), Some(2)])), ("v", ints(&[Some(5), None]))]);
        let masks = HashMap::from([("v".to_string(), BooleanArray::from(vec![false, true]))]);
        let out = shape(&t, "s.t", Rows::Named(rows.clone(), LeftOut::PerRow(masks)), How::Insert, &host()).await.unwrap();
        assert_eq!(column_ints(out.rows(), "v"), vec![Some(5), Some(7)]);
        let all = shape(&t, "s.t", Rows::Named(rows, LeftOut::Columns(vec!["v".into()])), How::Insert, &host()).await.unwrap();
        assert_eq!(column_ints(all.rows(), "v"), vec![Some(7), Some(7)]);
    }

    #[tokio::test]
    async fn a_label_the_enum_does_not_list_is_refused_as_22p02() {
        let t = Arc::new(typed_table());
        let rows = batch(vec![
            ("id", ints(&[Some(1)])),
            ("e", texts(&[Some("angry")])),
            ("u", texts(&[Some("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")])),
            ("s", texts(&[Some("abc")])),
            ("_deleted", bools(&[false])),
        ]);
        let err = shape(&t, "s.t", Rows::Whole(rows), How::Change, &host()).await.err().expect("a label not listed");
        assert_eq!(crate::codes::of(&err), "22P02");
        assert!(crate::codes::words(&err).starts_with("invalid input value for enum mood: \"angry\""), "{}", crate::codes::words(&err));
    }

    #[tokio::test]
    async fn a_uuid_and_a_varchar_are_refused_with_their_words() {
        let t = Arc::new(typed_table());
        let rows = |u: &str, s: &str| {
            batch(vec![("id", ints(&[Some(1)])), ("e", texts(&[Some("happy")])), ("u", texts(&[Some(u)])), ("s", texts(&[Some(s)])), ("_deleted", bools(&[false]))])
        };
        let err = shape(&t, "s.t", Rows::Whole(rows("nope", "abc")), How::Change, &host()).await.err().expect("not a uuid");
        assert_eq!(crate::codes::of(&err), "22P02");
        assert_eq!(crate::codes::words(&err), "invalid input syntax for type uuid: \"nope\" (column u holds uuids)");
        let long = rows("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11", "abcd");
        let err = shape(&t, "s.t", Rows::Whole(long), How::Change, &host()).await.err().expect("too long");
        assert_eq!(crate::codes::of(&err), "22001");
        assert_eq!(crate::codes::words(&err), "value too long for type character varying(3) (column s: a value of 4 characters)");
    }

    #[tokio::test]
    async fn a_check_and_a_not_null_are_refused_and_a_deleted_row_passes_them() {
        let t = Arc::new(guarded());
        let rows = |n: Option<i64>, m: Option<i64>, deleted: bool| {
            batch(vec![("id", ints(&[Some(1)])), ("n", ints(&[n])), ("m", ints(&[m])), ("_deleted", bools(&[deleted]))])
        };
        let err = shape(&t, "s.t", Rows::Whole(rows(Some(-1), Some(5), false)), How::Change, &host()).await.err().expect("a CHECK");
        assert_eq!(crate::codes::of(&err), "23514");
        assert_eq!(crate::codes::words(&err), "new row for relation \"s.t\" violates check constraint \"n_nonneg\": CHECK (n >= 0)");
        let err = shape(&t, "s.t", Rows::Whole(rows(Some(1), None, false)), How::Change, &host()).await.err().expect("NOT NULL");
        assert_eq!(crate::codes::of(&err), "23502");
        assert_eq!(crate::codes::words(&err), "s.t.m is NOT NULL, and a row gives it no value");
        shape(&t, "s.t", Rows::Whole(rows(Some(-1), None, true)), How::Change, &host()).await.unwrap();
    }

    #[tokio::test]
    async fn a_not_null_is_refused_before_a_check_a_row_breaks_both_as_postgres_refuses_it() {
        let t = Arc::new(guarded());
        let rows = batch(vec![("id", ints(&[Some(1)])), ("n", ints(&[Some(-1)])), ("m", ints(&[None])), ("_deleted", bools(&[false]))]);
        let err = shape(&t, "s.t", Rows::Whole(rows), How::Change, &host()).await.err().expect("a CHECK and a NOT NULL");
        assert_eq!(crate::codes::of(&err), "23502");
        assert_eq!(crate::codes::words(&err), "s.t.m is NOT NULL, and a row gives it no value");
    }

    #[test]
    fn two_failing_checks_name_the_first_by_name_not_the_first_defined() {
        let t = table(&[], vec![(plain("a"), DataType::Int64), (plain("b"), DataType::Int64)], &[("zz", "a > 0"), ("aa", "b > 0")]);
        let rows = batch(vec![("a", ints(&[Some(-1)])), ("b", ints(&[Some(-1)]))]);
        let err = checks(&t, "s.t", &rows, true, &SessionContext::new()).err().expect("two CHECKs");
        assert_eq!(crate::codes::of(&err), "23514");
        assert_eq!(crate::codes::words(&err), "new row for relation \"s.t\" violates check constraint \"aa\": CHECK (b > 0)");
    }

    #[tokio::test]
    async fn two_rows_of_one_key_are_refused_for_an_insert_and_taken_for_a_load() {
        let t = Arc::new(keyed());
        let rows = || batch(vec![("id", ints(&[Some(1), Some(1)])), ("v", ints(&[Some(1), Some(2)])), ("_deleted", bools(&[false, false]))]);
        let err = shape(&t, "s.t", Rows::Named(rows(), LeftOut::None), How::Insert, &host()).await.err().expect("a key twice");
        assert_eq!(crate::codes::of(&err), "23505");
        assert_eq!(crate::codes::words(&err), "duplicate key value violates unique constraint \"t_pkey\": Key (id)=(1) already exists");
        shape(&t, "s.t", Rows::Named(rows(), LeftOut::None), How::Load, &host()).await.unwrap();
    }

    #[tokio::test]
    async fn whole_rows_that_break_nothing_come_back_with_the_same_arrays() {
        let t = Arc::new(keyed());
        let rows = batch(vec![("id", ints(&[Some(1), Some(2)])), ("v", ints(&[Some(3), None])), ("_deleted", bools(&[false, false]))]);
        let before: Vec<ArrayRef> = rows.columns().to_vec();
        let out = shape(&t, "s.t", Rows::Whole(rows), How::Change, &host()).await.unwrap();
        for (i, a) in before.iter().enumerate() {
            assert!(Arc::ptr_eq(a, out.rows().column(i)), "column {i} was copied");
        }
    }

    #[test]
    fn checks_over_stored_names_name_a_renamed_column_by_its_sql_name() {
        let renamed = Column { stored: "c1".into(), not_null: true, ..plain("n") };
        let t = Table { mapped: true, ..table(&[], vec![(renamed, DataType::Int64)], &[("n_nonneg", "n >= 0")]) };
        let base = SessionContext::new();
        let stored = |v: Vec<Option<i64>>| batch(vec![("c1", ints(&v))]);
        let err = checks(&t, "s.t", &stored(vec![Some(-1)]), false, &base).err().expect("the CHECK");
        assert_eq!(crate::codes::words(&err), "new row for relation \"s.t\" violates check constraint \"n_nonneg\": CHECK (n >= 0)");
        let err = checks(&t, "s.t", &stored(vec![None]), false, &base).err().expect("NOT NULL");
        assert_eq!(crate::codes::of(&err), "23502");
        assert_eq!(crate::codes::words(&err), "s.t.n is NOT NULL, and a row gives it no value");
    }
}
