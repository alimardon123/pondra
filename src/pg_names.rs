//! The names of an answer's columns as Postgres gives them (`FigureColname` in Postgres's parser: `count`
//! for `count(*)`, `int4` for `'1'::int`, `?column?` for `1`, the ELSE's name for a CASE). Tools show these
//! names in the RowDescription, and some read their columns by them. DataFusion names its columns its own
//! way (`count(*)`, `Int64(1)`), and a SELECT may name two columns alike on Postgres (`SELECT c.oid, c.* …`),
//! where DataFusion needs `oid_1`. So the Postgres port names each column as Postgres does, from the
//! statement as written, and sends that name. Only the names change: nothing is planned or answered
//! differently, and HTTP, Flight and MCP keep DataFusion's names. Where the names can't be read the same
//! way as the columns (a `*` inside a scalar subquery, a list that doesn't fit the schema), the answer
//! keeps DataFusion's names.

use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::sql::sqlparser::ast::{self, AccessExpr, Expr, ObjectName, Query, SelectItem, SetExpr, Statement, TrimWhereField};

use crate::write::ident;

/// The field metadata that holds a column's Postgres name (`shown` reads it; `pg_types::typed` sets it).
pub const KEY: &str = "pg_name";

/// One item of a SELECT list (or a RETURNING list): a column with a name of its own, or any number of
/// columns (a `*`, a `t.*` with or without options, DuckDB's `COLUMNS(…)`), which keep the schema's names.
#[derive(Debug, Clone, PartialEq)]
pub enum Item {
    Named(String),
    Spread,
}

/// The items of a query's SELECT list, or of a write's RETURNING. A query's are its leftmost SELECT's, as
/// Postgres names a UNION by its first query; `TABLE t` is one `Spread`. None for anything else, VALUES
/// included: DataFusion's `column1`… are Postgres's own names.
pub fn of(stmt: &Statement) -> Option<Vec<Item>> {
    match stmt {
        Statement::Query(q) => body(&q.body),
        Statement::Insert(i) => list(i.returning.as_deref()?),
        Statement::Update(u) => list(u.returning.as_deref()?),
        Statement::Delete(d) => list(d.returning.as_deref()?),
        _ => None,
    }
}

fn body(s: &SetExpr) -> Option<Vec<Item>> {
    match s {
        SetExpr::Select(s) => list(&s.projection),
        SetExpr::Query(q) => body(&q.body),
        SetExpr::SetOperation { left, .. } => body(left),
        SetExpr::Table(_) => Some(vec![Item::Spread]),
        _ => None,
    }
}

fn list(items: &[SelectItem]) -> Option<Vec<Item>> {
    items.iter().map(|i| match i {
        SelectItem::UnnamedExpr(e) if is_columns(e) => Some(Item::Spread),
        SelectItem::UnnamedExpr(e) => item(colname(e)),
        SelectItem::ExprWithAlias { alias, .. } => Some(Item::Named(ident(alias))),
        SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(..) => Some(Item::Spread),
        _ => None,
    }).collect()
}

/// A named item, unless its name is empty (a name only the table decides: `item` gives None).
fn item(name: String) -> Option<Item> { (!name.is_empty()).then_some(Item::Named(name)) }

/// Is `e` DuckDB's `COLUMNS(…)`: any number of columns.
fn is_columns(e: &Expr) -> bool { matches!(e, Expr::Function(f) if last(&f.name) == "columns") }

/// The name of a SELECT item without an alias: Postgres's, `?column?` when it has none.
fn colname(e: &Expr) -> String {
    match figure(e) {
        (name, strength) if strength > 0 => name,
        _ => "?column?".into(),
    }
}

/// An expression's name and how strong it is, as `FigureColnameInternal` has them: 2 a name of its own (a
/// column, a function, a keyword the grammar makes a call of), 1 the name of the type a cast gives it, 0
/// none. A cast keeps a strong name of its expression (`x::text::int` is `x`), and takes the type's only
/// over a weak one (`'1'::int` is `int4`). An empty name with strength 2 is one only the table can give
/// (`(SELECT * FROM t)`'s first column), so `of` gives up on the statement.
fn figure(e: &Expr) -> (String, u8) {
    match e {
        Expr::Identifier(i) => (ident(i), 2),
        Expr::CompoundIdentifier(p) => (p.last().map(ident).unwrap_or_default(), 2),
        // (`(t).f` and `t[1].f`: the last field's name; `x[1]` is x's)
        Expr::CompoundFieldAccess { root, access_chain } => match access_chain.iter().rev().find_map(field) {
            Some(name) => (name, 2),
            None => figure(root),
        },
        Expr::Function(f) => (last(&f.name), 2),
        Expr::Cast { expr, data_type, .. } => cast(expr, &type_name(data_type)),
        // (a typed literal is a cast of its string: DATE '2026-01-01' is date)
        Expr::TypedString(t) => (type_name(&t.data_type), 1),
        Expr::Interval(_) => ("interval".into(), 1),
        Expr::Nested(e) | Expr::Collate { expr: e, .. } => figure(e),
        Expr::AtTimeZone { .. } => ("timezone".into(), 2),
        Expr::Extract { .. } => ("extract".into(), 2),
        Expr::Ceil { .. } => ("ceil".into(), 2),
        Expr::Floor { .. } => ("floor".into(), 2),
        Expr::Position { .. } => ("position".into(), 2),
        Expr::Substring { .. } => ("substring".into(), 2),
        Expr::Overlay { .. } => ("overlay".into(), 2),
        Expr::Trim { trim_where, .. } => {
            let name = match trim_where {
                Some(TrimWhereField::Leading) => "ltrim",
                Some(TrimWhereField::Trailing) => "rtrim",
                _ => "btrim",
            };
            (name.into(), 2)
        }
        Expr::Array(_) => ("array".into(), 2),
        Expr::Tuple(_) => ("row".into(), 2),
        Expr::Exists { negated: false, .. } => ("exists".into(), 2),
        Expr::Subquery(q) => (sublink(q), 2),
        // (a CASE takes its ELSE's strong name, else "case")
        Expr::Case { else_result, .. } => match else_result.as_deref().map(figure) {
            Some((name, 2)) => (name, 2),
            _ => ("case".into(), 1),
        },
        _ => (String::new(), 0),
    }
}

/// A field a `.` names in an access chain (`(t).f`).
fn field(a: &AccessExpr) -> Option<String> {
    match a {
        AccessExpr::Dot(Expr::Identifier(i)) => Some(ident(i)),
        _ => None,
    }
}

/// A cast's name: its expression's strong name, else the type's.
fn cast(e: &Expr, to: &str) -> (String, u8) {
    match figure(e) {
        (name, 2) => (name, 2),
        _ => (to.into(), 1),
    }
}

/// The name Postgres's grammar gives a type, which a cast takes when its expression has no strong name:
/// `int` and `integer` are `int4`, `bigint` `int8`, `char` `bpchar`, `timestamp with time zone`
/// `timestamptz`, an array its element's. Any other type is its last name as written (lower case unless
/// quoted).
fn type_name(t: &ast::DataType) -> String {
    use ast::DataType as T;
    let zone = |z: &ast::TimezoneInfo| matches!(z, ast::TimezoneInfo::WithTimeZone | ast::TimezoneInfo::Tz);
    match t {
        T::Int(_) | T::Integer(_) | T::Int4(_) => "int4".into(),
        T::BigInt(_) | T::Int8(_) => "int8".into(),
        T::SmallInt(_) | T::Int2(_) => "int2".into(),
        T::Real | T::Float4 => "float4".into(),
        T::DoublePrecision | T::Double(_) | T::Float8 => "float8".into(),
        T::Float(ast::ExactNumberInfo::Precision(p)) if *p <= 24 => "float4".into(),
        T::Float(_) => "float8".into(),
        T::Bool | T::Boolean => "bool".into(),
        T::Numeric(_) | T::Decimal(_) | T::Dec(_) => "numeric".into(),
        T::Char(_) | T::Character(_) => "bpchar".into(),
        T::Varchar(_) | T::CharacterVarying(_) | T::CharVarying(_) => "varchar".into(),
        T::Timestamp(_, z) => (if zone(z) { "timestamptz" } else { "timestamp" }).into(),
        T::Time(_, z) => (if zone(z) { "timetz" } else { "time" }).into(),
        T::Date => "date".into(),
        T::Interval { .. } => "interval".into(),
        T::Text => "text".into(),
        T::Uuid => "uuid".into(),
        T::JSON => "json".into(),
        T::JSONB => "jsonb".into(),
        T::Bytea => "bytea".into(),
        T::Regclass => "regclass".into(),
        T::Array(elem) => match elem {
            ast::ArrayElemTypeDef::AngleBracket(t) | ast::ArrayElemTypeDef::SquareBracket(t, _) | ast::ArrayElemTypeDef::Parenthesis(t) => type_name(t),
            ast::ArrayElemTypeDef::None => "array".into(),
        },
        T::Custom(name, _) => last(name),
        _ => t.to_string().split(['(', ' ']).next().unwrap_or_default().to_lowercase(),
    }
}

/// A scalar subquery's name: its first column's (`(SELECT max(x) FROM t)` is `max`). Empty when that
/// column is a `*` (or the query isn't a SELECT): the table names it, not the query.
fn sublink(q: &Query) -> String {
    match first_item(&q.body) {
        Some(SelectItem::UnnamedExpr(e)) => colname(e),
        Some(SelectItem::ExprWithAlias { alias, .. }) => ident(alias),
        _ => String::new(),
    }
}

fn first_item(s: &SetExpr) -> Option<&SelectItem> {
    match s {
        SetExpr::Select(s) => s.projection.first(),
        SetExpr::Query(q) => first_item(&q.body),
        SetExpr::SetOperation { left, .. } => first_item(left),
        _ => None,
    }
}

/// A name's last part, as SQL reads it.
fn last(n: &ObjectName) -> String { n.0.last().and_then(|p| p.as_ident()).map(ident).unwrap_or_default() }

/// The names of `schema`'s columns by place, as Postgres names them. A `Named` item is one column with its
/// own name. The columns a `Spread` covers keep the schema's names, since a table's columns are never
/// renamed (DataFusion's `oid_1` is only ever on an item the query wrote). A `Spread` covers the columns
/// the `Named` items leave. None when the places can't be known: a count that doesn't fit, or two runs of
/// `Spread`s with a `Named` item between them.
pub fn named(schema: &Schema, items: &[Item]) -> Option<Vec<String>> {
    let fields = schema.fields();
    let fixed = items.iter().filter(|i| matches!(i, Item::Named(_))).count();
    // (a run of spreads starts at a spread that is first or follows a named item)
    let runs = items.windows(2).filter(|w| matches!(w, [Item::Named(_), Item::Spread])).count() + usize::from(matches!(items.first(), Some(Item::Spread)));
    if runs > 1 {
        return None;
    }
    let width = fields.len().checked_sub(fixed)?;
    if runs == 0 && width != 0 {
        return None;
    }
    let mut out: Vec<String> = vec![];
    let mut spread_done = false;
    for it in items {
        match it {
            Item::Named(name) => out.push(name.clone()),
            Item::Spread if !spread_done => {
                spread_done = true;
                let at = out.len(); // (the named items before the run are one column each)
                out.extend(fields[at..at + width].iter().map(|f| f.name().clone()));
            }
            Item::Spread => {}
        }
    }
    Some(out)
}

/// The name a column is sent under: its Postgres name when `pg_types::typed` gave it one, else its own.
pub fn shown(f: &Field) -> &str { f.metadata().get(KEY).map_or(f.name().as_str(), String::as_str) }

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::DataType;
    use datafusion::sql::sqlparser::{dialect::GenericDialect, parser::Parser};

    fn stmt(sql: &str) -> Statement {
        let mut parsed = Parser::parse_sql(&GenericDialect {}, sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
        assert_eq!(parsed.len(), 1, "{sql}");
        parsed.pop().unwrap()
    }

    /// A schema of the given names, as an answer's columns are named (text, the type is not what's tested).
    fn schema(names: &[&str]) -> Schema {
        Schema::new(names.iter().map(|n| Field::new(*n, DataType::Utf8, true)).collect::<Vec<_>>())
    }

    /// Postgres 16's names for each SELECT list (or RETURNING list), as its cursor printed them: the
    /// statements ran on tables t (a int, b int, c text, x int, ts timestamp, arr int[]), u (a int, b int),
    /// q ("Col" int, OtherCol int) and pg_catalog's pg_collation. A `*` takes the table's names.
    const POSTGRES16: &[(&str, &[&str])] = &[
        (r#"SELECT a FROM t"#, &["a"]),
        (r#"SELECT a, b FROM t"#, &["a", "b"]),
        (r#"SELECT t.a FROM t"#, &["a"]),
        (r#"SELECT * FROM t"#, &["a", "b", "c", "x", "ts", "arr"]),
        (r#"SELECT t.* FROM t"#, &["a", "b", "c", "x", "ts", "arr"]),
        (r#"SELECT a, * FROM t"#, &["a", "a", "b", "c", "x", "ts", "arr"]),
        (r#"SELECT *, a FROM t"#, &["a", "b", "c", "x", "ts", "arr", "a"]),
        (r#"SELECT c.oid, c.* FROM pg_catalog.pg_collation c"#, &["oid", "oid", "collname", "collnamespace", "collowner", "collprovider", "collisdeterministic", "collencoding", "collcollate", "collctype", "colliculocale", "collicurules", "collversion"]),
        (r#"SELECT c.* , c.oid FROM pg_catalog.pg_collation c"#, &["oid", "collname", "collnamespace", "collowner", "collprovider", "collisdeterministic", "collencoding", "collcollate", "collctype", "colliculocale", "collicurules", "collversion", "oid"]),
        (r#"SELECT count(*) FROM t"#, &["count"]),
        (r#"SELECT sum(a), max(a) FROM t"#, &["sum", "max"]),
        (r#"SELECT count(DISTINCT a) FROM t"#, &["count"]),
        (r#"SELECT sum(a) FILTER (WHERE b > 1) FROM t"#, &["sum"]),
        (r#"SELECT 1"#, &["?column?"]),
        (r#"SELECT a + b FROM t"#, &["?column?"]),
        (r#"SELECT x IS NULL FROM t"#, &["?column?"]),
        (r#"SELECT x IS NOT NULL FROM t"#, &["?column?"]),
        (r#"SELECT (a > 1) IS TRUE FROM t"#, &["?column?"]),
        (r#"SELECT NULL"#, &["?column?"]),
        (r#"SELECT a::text FROM t"#, &["a"]),
        (r#"SELECT '1'::int"#, &["int4"]),
        (r#"SELECT CAST(1 AS bigint)"#, &["int8"]),
        (r#"SELECT CASE WHEN a > 1 THEN 1 ELSE 2 END FROM t"#, &["case"]),
        (r#"SELECT CASE WHEN a > 1 THEN b ELSE x END FROM t"#, &["x"]),
        (r#"SELECT CASE WHEN a > 1 THEN 1 END FROM t"#, &["case"]),
        (r#"SELECT CASE WHEN a > 1 THEN b ELSE NULL END FROM t"#, &["case"]),
        (r#"SELECT (SELECT max(a) FROM t)"#, &["max"]),
        (r#"SELECT EXISTS (SELECT 1)"#, &["exists"]),
        (r#"SELECT NOT EXISTS (SELECT 1 FROM t)"#, &["?column?"]),
        (r#"SELECT ARRAY[1, 2]"#, &["array"]),
        (r#"SELECT ROW(1, 2)"#, &["row"]),
        (r#"SELECT (1, 2)"#, &["row"]),
        (r#"SELECT current_timestamp"#, &["current_timestamp"]),
        (r#"SELECT current_user"#, &["current_user"]),
        (r#"SELECT session_user"#, &["session_user"]),
        (r#"SELECT user"#, &["user"]),
        (r#"SELECT current_date"#, &["current_date"]),
        (r#"SELECT localtime"#, &["localtime"]),
        (r#"SELECT localtimestamp"#, &["localtimestamp"]),
        (r#"SELECT current_time"#, &["current_time"]),
        (r#"SELECT current_role"#, &["current_role"]),
        (r#"SELECT current_catalog"#, &["current_catalog"]),
        (r#"SELECT current_schema"#, &["current_schema"]),
        (r#"SELECT current_timestamp(3)"#, &["current_timestamp"]),
        (r#"SELECT EXTRACT(year FROM ts) FROM t"#, &["extract"]),
        (r#"SELECT extract(epoch from ts) FROM t"#, &["extract"]),
        (r#"SELECT trim(' a ')"#, &["btrim"]),
        (r#"SELECT trim(leading 'x' from c) FROM t"#, &["ltrim"]),
        (r#"SELECT trim(trailing 'x' from c) FROM t"#, &["rtrim"]),
        (r#"SELECT trim(both 'x' from c) FROM t"#, &["btrim"]),
        (r#"SELECT ltrim(c) FROM t"#, &["ltrim"]),
        (r#"SELECT substring('abc' FROM 1 FOR 2)"#, &["substring"]),
        (r#"SELECT substring(c, 1, 2) FROM t"#, &["substring"]),
        (r#"SELECT position('a' in c) FROM t"#, &["position"]),
        (r#"SELECT overlay(c placing 'x' from 1) FROM t"#, &["overlay"]),
        (r#"SELECT ceil(a::numeric) FROM t"#, &["ceil"]),
        (r#"SELECT CEIL(1.5)"#, &["ceil"]),
        (r#"SELECT floor(1.5)"#, &["floor"]),
        (r#"SELECT ts AT TIME ZONE 'UTC' FROM t"#, &["timezone"]),
        (r#"SELECT a AS "Foo" FROM t"#, &["Foo"]),
        (r#"SELECT a AS foo FROM t"#, &["foo"]),
        (r#"SELECT a FROM t UNION SELECT b FROM t"#, &["a"]),
        (r#"SELECT b FROM t UNION ALL SELECT a FROM t"#, &["b"]),
        (r#"SELECT a, b FROM t UNION SELECT x, a FROM t"#, &["a", "b"]),
        (r#"SELECT a FROM t INTERSECT SELECT b FROM t"#, &["a"]),
        (r#"SELECT b FROM t EXCEPT SELECT a FROM t"#, &["b"]),
        (r#"(SELECT a FROM t) UNION (SELECT x FROM t)"#, &["a"]),
        (r#"SELECT 1 UNION ALL SELECT 2"#, &["?column?"]),
        (r#"SELECT count(*) FROM t UNION ALL SELECT 1"#, &["count"]),
        (r#"SELECT x::text::int FROM t"#, &["x"]),
        (r#"SELECT 't'::regclass::oid"#, &["oid"]),
        (r#"SELECT ((1::int8))::text"#, &["text"]),
        (r#"SELECT '1'::text::int8"#, &["int8"]),
        (r#"SELECT (a::text)::int FROM t"#, &["a"]),
        (r#"SELECT 't'::regclass"#, &["regclass"]),
        (r#"SELECT CAST(a AS text) FROM t"#, &["a"]),
        (r#"SELECT arr::bigint[] FROM t"#, &["arr"]),
        (r#"SELECT '{1}'::int[]"#, &["int4"]),
        (r#"SELECT '1'::numeric"#, &["numeric"]),
        (r#"SELECT '1'::decimal(10,2)"#, &["numeric"]),
        (r#"SELECT '1'::float"#, &["float8"]),
        (r#"SELECT '1'::real"#, &["float4"]),
        (r#"SELECT '1'::double precision"#, &["float8"]),
        (r#"SELECT '1'::boolean"#, &["bool"]),
        (r#"SELECT 'x'::char(3)"#, &["bpchar"]),
        (r#"SELECT 'x'::character varying"#, &["varchar"]),
        (r#"SELECT 'x'::varchar(3)"#, &["varchar"]),
        (r#"SELECT '2020-01-01'::timestamp with time zone"#, &["timestamptz"]),
        (r#"SELECT '2020-01-01'::timestamptz"#, &["timestamptz"]),
        (r#"SELECT '12:00'::time with time zone"#, &["timetz"]),
        (r#"SELECT '12:00'::time"#, &["time"]),
        (r#"SELECT DATE '2026-01-01'"#, &["date"]),
        (r#"SELECT INTERVAL '1 day'"#, &["interval"]),
        (r#"SELECT TIMESTAMP '2026-01-01 00:00'"#, &["timestamp"]),
        (r#"SELECT TIME '12:00'"#, &["time"]),
        (r#"SELECT true"#, &["?column?"]),
        (r#"SELECT false AS "F""#, &["F"]),
        (r#"SELECT NOT true"#, &["?column?"]),
        (r#"SELECT a = 1 FROM t"#, &["?column?"]),
        (r#"SELECT NOT (a > 1) FROM t"#, &["?column?"]),
        (r#"SELECT c || 'x' FROM t"#, &["?column?"]),
        (r#"SELECT c LIKE 'a%' FROM t"#, &["?column?"]),
        (r#"SELECT a BETWEEN 1 AND 2 FROM t"#, &["?column?"]),
        (r#"SELECT a IS DISTINCT FROM b FROM t"#, &["?column?"]),
        (r#"SELECT -a FROM t"#, &["?column?"]),
        (r#"SELECT (a) FROM t"#, &["a"]),
        (r#"SELECT ((a)) FROM t"#, &["a"]),
        (r#"SELECT c COLLATE "C" FROM t"#, &["c"]),
        (r#"SELECT "Col", OtherCol FROM q"#, &["Col", "othercol"]),
        (r#"SELECT UPPER(c) FROM t"#, &["upper"]),
        (r#"SELECT Lower(c) FROM t"#, &["lower"]),
        (r#"SELECT pg_catalog.lower(c) FROM t"#, &["lower"]),
        (r#"SELECT COALESCE(a, b) FROM t"#, &["coalesce"]),
        (r#"SELECT coalesce(a, 1) FROM t"#, &["coalesce"]),
        (r#"SELECT NULLIF(a, b) FROM t"#, &["nullif"]),
        (r#"SELECT GREATEST(a, b) FROM t"#, &["greatest"]),
        (r#"SELECT LEAST(a, b) FROM t"#, &["least"]),
        (r#"SELECT row_number() OVER (ORDER BY a) FROM t"#, &["row_number"]),
        (r#"SELECT sum(a) OVER (PARTITION BY b) FROM t"#, &["sum"]),
        (r#"SELECT a, rank() OVER w FROM t WINDOW w AS (ORDER BY a)"#, &["a", "rank"]),
        (r#"SELECT (SELECT a FROM t LIMIT 1)"#, &["a"]),
        (r#"SELECT (SELECT 1)"#, &["?column?"]),
        (r#"SELECT (SELECT count(*) FROM t)"#, &["count"]),
        (r#"SELECT (SELECT max(a) AS m FROM t)"#, &["m"]),
        (r#"SELECT a IN (SELECT a FROM t) FROM t"#, &["?column?"]),
        (r#"SELECT ARRAY(SELECT a FROM t)"#, &["array"]),
        (r#"SELECT a FROM (SELECT a FROM t) s"#, &["a"]),
        (r#"SELECT s.* FROM (SELECT a, b FROM t) s"#, &["a", "b"]),
        (r#"SELECT y FROM (SELECT x FROM t) AS s(y)"#, &["y"]),
        (r#"SELECT (SELECT a FROM t) + 1"#, &["?column?"]),
        (r#"SELECT (SELECT ts FROM t LIMIT 1)::date"#, &["ts"]),
        (r#"SELECT arr[1] FROM t"#, &["arr"]),
        (r#"SELECT arr[1][2] FROM t"#, &["arr"]),
        (r#"SELECT t.arr[1] FROM t"#, &["arr"]),
        (r#"SELECT (t).a FROM t"#, &["a"]),
        (r#"SELECT (ROW(1, 2)).f1"#, &["f1"]),
        (r#"SELECT $1::int"#, &["int4"]),
        (r#"SELECT a, a FROM t"#, &["a", "a"]),
        (r#"SELECT t.a, u.a FROM t JOIN u ON t.b = u.b"#, &["a", "a"]),
        (r#"SELECT u.*, t.* FROM t JOIN u ON t.b = u.b"#, &["a", "b", "a", "b", "c", "x", "ts", "arr"]),
        (r#"SELECT count(*), count(*) FROM t"#, &["count", "count"]),
        (r#"SELECT 1, 1"#, &["?column?", "?column?"]),
        (r#"SELECT a AS x, b AS x FROM t"#, &["x", "x"]),
        (r#"SELECT ts::date, ts::time FROM t"#, &["ts", "ts"]),
        (r#"SELECT a::bigint AS a2 FROM t"#, &["a2"]),
        (r#"SELECT 1 AS "1""#, &["1"]),
        (r#"SELECT TRUE AS "T""#, &["T"]),
        (r#"SELECT array_agg(a ORDER BY b) FROM t"#, &["array_agg"]),
        (r#"SELECT string_agg(c, ',') FROM t"#, &["string_agg"]),
        (r#"SELECT t.a AS "t.a" FROM t"#, &["t.a"]),
        (r#"SELECT max(t.a) FILTER (WHERE t.b > 0) AS m FROM t"#, &["m"]),
        (r#"SELECT (x) FROM t"#, &["x"]),
        (r#"SELECT ts - interval '1 day' FROM t"#, &["?column?"]),
        (r#"SELECT now() - ts FROM t"#, &["?column?"]),
        (r#"SELECT 1::int8 + 1"#, &["?column?"]),
        (r#"SELECT pg_typeof(a) FROM t"#, &["pg_typeof"]),
        (r#"SELECT now()"#, &["now"]),
        (r#"SELECT '{1,2}'::int[][]"#, &["int4"]),
        (r#"SELECT '{x}'::text[]"#, &["text"]),
        (r#"SELECT c::interval FROM t"#, &["c"]),
        (r#"SELECT GROUPING(a) FROM t GROUP BY ROLLUP(a)"#, &["grouping"]),
        (r#"SELECT a FROM t ORDER BY b"#, &["a"]),
        (r#"SELECT DISTINCT ON (a) a, b FROM t"#, &["a", "b"]),
        (r#"SELECT a FROM t LIMIT 1"#, &["a"]),
        (r#"WITH c AS (SELECT a, b FROM t) SELECT * FROM c"#, &["a", "b"]),
        (r#"WITH c AS (SELECT a AS z FROM t) SELECT z, z FROM c"#, &["z", "z"]),
        (r#"WITH c(p, q) AS (SELECT a, b FROM t) SELECT p, q FROM c"#, &["p", "q"]),
        (r#"WITH c AS (SELECT a FROM t) SELECT count(*) FROM c"#, &["count"]),
        (r#"WITH c AS (SELECT a FROM t) TABLE c"#, &["a"]),
        // (a bare `TABLE t` doesn't parse with the generic dialect, so the Postgres port keeps the table's own names)
        (r#"INSERT INTO t (a, b, c, x, ts, arr) VALUES (1, 2, 'x', 3, now(), '{1}') RETURNING a, b + 1, c::text"#, &["a", "?column?", "c"]),
        (r#"INSERT INTO t (a, b, c, x, ts, arr) SELECT a, b, c, x, ts, arr FROM t RETURNING *"#, &["a", "b", "c", "x", "ts", "arr"]),
        (r#"INSERT INTO t (a) VALUES (1) RETURNING a AS foo, a AS foo"#, &["foo", "foo"]),
        (r#"UPDATE t SET a = a RETURNING a, a + 1"#, &["a", "?column?"]),
        (r#"UPDATE t SET a = 1 WHERE false RETURNING c, c"#, &["c", "c"]),
        (r#"UPDATE t SET a = 1 WHERE false RETURNING t.*, a"#, &["a", "b", "c", "x", "ts", "arr", "a"]),
        (r#"DELETE FROM t WHERE false RETURNING *"#, &["a", "b", "c", "x", "ts", "arr"]),
        (r#"DELETE FROM t USING u WHERE t.a = u.a RETURNING t.a, u.b"#, &["a", "b"]),
    ];

    #[test]
    fn names_as_postgres_16() {
        assert!(POSTGRES16.len() >= 120, "{} cases", POSTGRES16.len());
        for (sql, want) in POSTGRES16 {
            let items = of(&stmt(sql)).unwrap_or_else(|| panic!("{sql}: no items"));
            // (the schema has each column's name; a Named item keeps its own whatever the schema says)
            let got = named(&schema(want), &items);
            assert_eq!(got, Some(want.iter().map(|s| s.to_string()).collect::<Vec<_>>()), "{sql}: {items:?}");
        }
    }

    #[test]
    fn named_keeps_items_own_names() {
        // DataFusion's own names for the items (`count(*)`, `Int64(1)`, `oid_1`) are not what is sent.
        let items = of(&stmt("SELECT c.oid, c.*, count(*) FROM pg_catalog.pg_collation c")).unwrap();
        let mut names = vec!["oid_1".to_string(), "oid".into(), "collname".into()];
        names.extend((0..10).map(|i| format!("col{i}")));
        names.push("Count(Int64(1))".into());
        let got = named(&schema(&names.iter().map(|s| s.as_str()).collect::<Vec<_>>()), &items).unwrap();
        assert_eq!(got[0], "oid");
        assert_eq!(&got[1..13], &names[1..13]); // (the table's columns: the schema's own)
        assert_eq!(got[13], "count");
    }

    #[test]
    fn named_gives_up_when_it_cannot_tell() {
        let one = |sql: &str, names: &[&str]| named(&schema(names), &of(&stmt(sql)).unwrap());
        // a count that doesn't fit: too few columns for the items, or one more than a list without a `*`
        assert_eq!(one("SELECT a, b FROM t", &["a"]), None);
        assert_eq!(one("SELECT a FROM t", &["a", "b"]), None);
        assert_eq!(one("SELECT a, b, c, *", &["a", "b"]), None);
        // two runs of `*` with a named item between them: its place is unknown
        assert_eq!(one("SELECT *, a, * FROM t", &["a", "b", "c", "x", "ts", "arr", "a", "b", "c", "x", "ts", "arr", "a"]), None);
        // the same run twice is one run: the names are the schema's
        assert_eq!(one("SELECT *, t.* FROM u", &["a", "b", "a", "b"]), Some(vec!["a".into(), "b".into(), "a".into(), "b".into()]));
        // an empty list has no names
        assert_eq!(named(&schema(&[]), &[]), Some(vec![]));
        assert_eq!(one("SELECT 1", &[]), None);
    }

    #[test]
    fn of_gives_up_on_what_it_cannot_name() {
        // a scalar subquery whose first column is a `*`: the table names it
        assert_eq!(of(&stmt("SELECT (SELECT * FROM t)")), None);
        // VALUES, and statements with no columns, are DataFusion's names
        assert_eq!(of(&stmt("VALUES (1, 2)")), None);
        assert_eq!(of(&stmt("CREATE TABLE z (a int)")), None);
        assert_eq!(of(&stmt("SELECT a FROM t WHERE false UNION VALUES (1)")), Some(vec![Item::Named("a".into())]));
        // DuckDB's COLUMNS is a run of columns
        assert_eq!(of(&stmt("SELECT COLUMNS('a.*') FROM t")), Some(vec![Item::Spread]));
    }

    #[test]
    fn shown_is_the_postgres_name_when_one_is_set() {
        let mut meta = std::collections::HashMap::new();
        meta.insert(KEY.to_string(), "oid".to_string());
        let named = Field::new("oid_1", DataType::Utf8, true).with_metadata(meta);
        assert_eq!(shown(&named), "oid");
        assert_eq!(shown(&Field::new("count(*)", DataType::Int64, true)), "count(*)");
    }
}
