//! A table's columns as one list (the redesign's step 1b: designs/redesign/redesign.md §3 in the project's files). Each
//! column has a real id, its Parquet field id: its stored position + 1, which every file it was ever written to already
//! carries (`tier::field_ids`), never reused, so no file moves for a rename, a drop or a move. Its SQL order (what
//! `SELECT *` lists and an INSERT without a column list fills) is worked out once, here, dropped columns kept and marked, so
//! every reader takes the order from here and none works it out again (the 11-03 audit's H5, M8 to M10 and L6 came from
//! readers that did). A table's entry keeps its columns as a list (`store::Stored`, step 1b's piece 6), and
//! `TableMeta::column_list` makes this from it.

/// The field id of a list column's elements in its files and in Iceberg's schema: the column's own id past this, so it never
/// moves when a column is added (the elements once took the ids after every column's, and ADD COLUMN gave one of them to
/// the new column: audit 11-20 H4). Columns' ids stay below it, and the system columns' are past 1,000,000 (`tier::field_ids`).
pub const ELEMENTS: u32 = 500_000;

/// The field ids of a struct column's fields (a FILE's path, version and content type), in its files, Iceberg's schema and
/// Delta's column mapping: field `k` of column `c` is `FIELDS + c × WIDTH + k`. Like `ELEMENTS`, it comes from what the
/// column record keeps (its id, and its type's fields in order), so ADD COLUMN, a rename or a drop never moves it, and it
/// meets no column's, no list's elements' and no system column's (past 1,000,000). Iceberg's ids are ints: column 500,000's
/// last field is 502,000,999.
pub const FIELDS: u32 = 2_000_000;

/// The most fields a struct column's files give ids to (`FIELDS`).
pub const WIDTH: u32 = 1_000;

/// The field id of field `k` of column `column`, a struct (`FIELDS`).
pub fn field_id(column: u32, k: usize) -> u32 { FIELDS + column * WIDTH + k as u32 }

/// A struct type's fields, where each has an id of its own (`field_id`): one level, at most `WIDTH` of them, none a struct,
/// list or map itself (one of those waits for ids a level down). None for any other type.
pub fn struct_fields(t: &datafusion::arrow::datatypes::DataType) -> Option<&datafusion::arrow::datatypes::Fields> {
    match t {
        datafusion::arrow::datatypes::DataType::Struct(fs) if fs.len() <= WIDTH as usize && !fs.iter().any(|f| f.data_type().is_nested()) => Some(fs),
        _ => None,
    }
}

/// A struct column's fields by its stored type's text (`Column::dtype`): each one's name, type's text and whether it may be
/// NULL (`struct_fields`).
pub fn struct_fields_of(dtype: &str) -> Option<Vec<(String, String, bool)>> {
    let t = dtype.starts_with("Struct(").then(|| dtype.parse::<datafusion::arrow::datatypes::DataType>().ok()).flatten()?;
    Some(struct_fields(&t)?.iter().map(|f| (f.name().clone(), crate::answer::type_name(f.data_type()), f.is_nullable())).collect())
}

/// The highest field id a schema of columns with these ids and stored types gives (a list's elements', a struct's fields'
/// too): Iceberg's `last-column-id`.
pub fn last_id<'a>(columns: impl Iterator<Item = (u32, &'a str)>) -> u32 {
    let last = |id: u32, t: &str| match (crate::answer::list_item(t), struct_fields_of(t)) {
        (Some(_), _) => ELEMENTS + id,
        (None, Some(fs)) if !fs.is_empty() => field_id(id, fs.len() - 1),
        _ => id,
    };
    columns.map(|(id, t)| last(id, t)).max().unwrap_or(0)
}

/// One column of a table.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Column {
    /// Its Parquet field id: its stored position + 1.
    pub id: u32,
    /// The name SQL knows it by.
    pub name: String,
    /// The name its files and the log hold it under: the first name it had, kept through a rename. The table's other
    /// facts (its key, clustering, partition and the rest) name it by this.
    pub stored: String,
    /// Its Arrow type, as text ("Int64", "Utf8", "Decimal128(15, 2)").
    pub dtype: String,
    /// Dropped: older files still hold it, and nothing reads it again.
    pub dropped: bool,
}

/// A table's columns, held by id (`by_id[i]` is column i + 1, the dropped ones too) with SQL's order of the live ones as
/// places in it (`sql_places`), so a lookup by id is one index and no reader sorts.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Columns {
    by_id: Vec<Column>,
    sql: Vec<usize>,
}

impl Columns {
    /// The columns in stored order (`stored`: (stored name, type) in field-id order), with SQL's names (`name`, which
    /// gives a stored name's name in SQL), whether each is dropped, and SQL's order (`sql_places`).
    pub fn new<'a>(stored: &'a [(String, String)], name: impl Fn(&'a str) -> &'a str, dropped: impl Fn(&str) -> bool, first: &[String]) -> Columns {
        let sql = sql_places(stored, &dropped, first);
        let by_id = stored
            .iter()
            .enumerate()
            .map(|(i, (s, t))| Column { id: i as u32 + 1, name: name(s).to_string(), stored: s.clone(), dtype: t.clone(), dropped: dropped(s) })
            .collect();
        Columns { by_id, sql }
    }

    /// The list as it is kept (`store::Stored`): every column by id, and SQL's order as places in it (`places`).
    pub fn from_parts(by_id: Vec<Column>, sql: Vec<usize>) -> Columns { Columns { by_id, sql } }
    /// The live columns in SQL's order.
    pub fn sql(&self) -> impl Iterator<Item = &Column> { self.sql.iter().map(|i| &self.by_id[*i]) }

    /// The live columns in stored order (field-id order): what storage and the log read and write.
    pub fn stored_order(&self) -> impl Iterator<Item = &Column> { self.by_id.iter().filter(|c| !c.dropped) }

    /// Every column, the dropped ones too, in stored order: a file's whole schema.
    pub fn all(&self) -> &[Column] { &self.by_id }

    /// The column (live or dropped) whose id is `id`.
    pub fn by_id(&self, id: u32) -> Option<&Column> {
        match self.by_id.get((id as usize).checked_sub(1)?) {
            Some(c) if c.id == id => Some(c),
            _ => self.by_id.iter().find(|c| c.id == id), // (an id that isn't its place + 1: `store::Stored::place`)
        }
    }

    /// The live column SQL calls `name`.
    pub fn named(&self, name: &str) -> Option<&Column> { self.sql().find(|c| c.name == name) }

    /// The column (live or dropped) stored as `stored`.
    pub fn of_stored(&self, stored: &str) -> Option<&Column> { self.by_id.iter().find(|c| c.stored == stored) }

    /// How many live columns there are.
    pub fn len(&self) -> usize { self.sql.len() }

    /// No live column.
    pub fn is_empty(&self) -> bool { self.sql.is_empty() }
}

/// SQL's order of a table's live columns, as places in its stored list (`stored`, in field-id order): the stored names
/// `first` lists come first in that order, and the other live columns follow in stored order, so a column added later
/// comes last. A name in `first` that isn't a live column, or is there twice, is passed over. The one place this order is
/// worked out: `Columns::new` and the entry's own `in_order` both take it from here.
pub fn sql_places(stored: &[(String, String)], dropped: impl Fn(&str) -> bool, first: &[String]) -> Vec<usize> {
    let first: Vec<usize> = first.iter().filter_map(|s| stored.iter().position(|(c, _)| c == s)).collect();
    places(stored.len(), |i| dropped(&stored[i].0), &first)
}

/// `sql_places` from places: of `n` columns in stored order, those at `first` (live, once each) come first in that order,
/// then the other live ones as stored. A list that keeps SQL's order by ids (`store::Stored`) takes its order from here.
pub fn places(n: usize, dropped: impl Fn(usize) -> bool, first: &[usize]) -> Vec<usize> {
    let mut taken: Vec<bool> = (0..n).map(&dropped).collect();
    let mut out = Vec::with_capacity(n);
    for &i in first {
        if i < n && !taken[i] {
            taken[i] = true;
            out.push(i);
        }
    }
    out.extend((0..n).filter(|i| !taken[*i]));
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cols(n: &[&str]) -> Vec<(String, String)> { n.iter().map(|c| (c.to_string(), "Int64".to_string())).collect() }

    /// Ids are stored places + 1; SQL's order puts `first`'s columns first, then the rest as stored, and the dropped
    /// ones last; a rename keeps the stored name.
    #[test]
    fn a_tables_columns_are_one_list_in_sqls_order() {
        let stored = cols(&["a", "b", "c", "d"]);
        let list = Columns::new(&stored, |s| if s == "b" { "bee" } else { s }, |s| s == "c", &["d".to_string(), "c".to_string(), "x".to_string()]);
        let sql: Vec<(&str, u32)> = list.sql().map(|c| (c.name.as_str(), c.id)).collect();
        assert_eq!(sql, [("d", 4), ("a", 1), ("bee", 2)], "d first, then a and b as stored; c dropped; x passed over");
        assert_eq!(list.stored_order().map(|c| c.id).collect::<Vec<_>>(), [1, 2, 4]);
        assert_eq!(list.all().iter().map(|c| (c.id, c.dropped)).collect::<Vec<_>>(), [(1, false), (2, false), (3, true), (4, false)]);
        assert_eq!(list.named("bee").map(|c| c.stored.as_str()), Some("b"));
        assert!(list.named("b").is_none() && list.named("c").is_none(), "a stored name isn't SQL's, a dropped column has none");
        assert_eq!(list.of_stored("c").map(|c| c.dropped), Some(true));
        assert_eq!(list.by_id(2).map(|c| c.name.as_str()), Some("bee"));
        assert!(list.by_id(0).is_none() && list.by_id(5).is_none());
        assert_eq!(list.len(), 3);
    }
}
