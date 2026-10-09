//! Postgres's catalog as clients read it (ADR-030): psql's `\d` commands, dbt, SQLAlchemy, JDBC's
//! and ODBC's metadata calls, BI tools. Built from the lake's own catalog for each query that
//! reads it — tables, views, materialized views, their columns with Postgres's types, keys,
//! defaults, functions — with the functions those queries call, and their SQL rewritten where
//! it says things only Postgres says (`OPERATOR(pg_catalog.~)`, `::regclass`, `COLLATE`).
//!
//! Every object keeps its oid from one query to the next (a hash of its name), so a client that
//! looks a table up and then asks about its columns by oid gets the same table.
use crate::store::{Lake, TableMeta};
use anyhow::Result;
use datafusion::arrow::array::{new_null_array, Array, ArrayRef, AsArray, RecordBatch, StringArray, UInt32Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::catalog::{MemTable, MemorySchemaProvider, SchemaProvider};
use datafusion::common::ScalarValue;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature, Volatility};
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{self, VisitMut, VisitorMut};
use std::collections::BTreeMap;
use std::ops::ControlFlow;
use std::sync::Arc;

/// Does this query read Postgres's catalog, or say something only Postgres says?
pub fn wanted(sql: &str) -> bool {
    let s = sql.to_lowercase();
    ["pg_", "information_schema", "::reg", "operator(", "format_type(", "current_user", "session_user", "current_schemas", "to_regtype", "to_regclass", "collate", "obj_description", "col_description", "current_setting", "has_table_privilege", "has_schema_privilege", "quote_ident"]
        .iter().any(|w| s.contains(w))
}

// ---------------------------------------------------------------- types

/// A Postgres type: oid, name, how `format_type` says it, length, category, its array's oid (or,
/// for an array, its element's).
struct PgType(u32, &'static str, &'static str, i16, char, u32);

const TYPES: &[PgType] = &[
    PgType(16, "bool", "boolean", 1, 'B', 1000), PgType(17, "bytea", "bytea", -1, 'U', 1001), PgType(18, "char", "\"char\"", 1, 'Z', 1002),
    PgType(19, "name", "name", 64, 'S', 1003), PgType(20, "int8", "bigint", 8, 'N', 1016), PgType(21, "int2", "smallint", 2, 'N', 1005),
    PgType(22, "int2vector", "int2vector", -1, 'A', 1006), PgType(23, "int4", "integer", 4, 'N', 1007), PgType(24, "regproc", "regproc", 4, 'N', 1008),
    PgType(25, "text", "text", -1, 'S', 1009), PgType(26, "oid", "oid", 4, 'N', 1028), PgType(30, "oidvector", "oidvector", -1, 'A', 1013),
    PgType(114, "json", "json", -1, 'U', 199), PgType(700, "float4", "real", 4, 'N', 1021), PgType(701, "float8", "double precision", 8, 'N', 1022),
    PgType(1042, "bpchar", "character", -1, 'S', 1014), PgType(1043, "varchar", "character varying", -1, 'S', 1015), PgType(1082, "date", "date", 4, 'D', 1182),
    PgType(1083, "time", "time without time zone", 8, 'D', 1183), PgType(1114, "timestamp", "timestamp without time zone", 8, 'D', 1115),
    PgType(1184, "timestamptz", "timestamp with time zone", 8, 'D', 1185), PgType(1186, "interval", "interval", 16, 'T', 1187),
    PgType(1700, "numeric", "numeric", -1, 'N', 1231), PgType(2205, "regclass", "regclass", 4, 'N', 2210), PgType(2206, "regtype", "regtype", 4, 'N', 2211),
    PgType(2950, "uuid", "uuid", 16, 'U', 2951), PgType(3802, "jsonb", "jsonb", -1, 'U', 3807), PgType(2278, "void", "void", 4, 'P', 0),
    PgType(2249, "record", "record", -1, 'P', 2287),
];

/// A type's input, output, receive or send function, by Postgres's own name (drivers such as ADBC's
/// pick a type's binary format by its `typreceive`; newer types' names have an underscore).
fn type_function(t: &PgType, f: &str) -> String {
    match t.1 {
        "json" | "jsonb" | "date" | "time" | "timestamp" | "timestamptz" | "interval" | "numeric" | "uuid" | "void" | "record" => format!("{}_{f}", t.1),
        _ => format!("{}{f}", t.1),
    }
}

/// Every type function `pg_type` names, as rows of `pg_proc` in `pg_catalog`: Npgsql joins
/// `pg_proc` on a type's `typreceive` to learn which types are arrays (`array_recv`).
fn type_functions() -> Vec<String> {
    let mut all: Vec<String> = TYPES.iter().flat_map(|t| ["in", "out", "recv", "send"].map(|f| type_function(t, f))).collect();
    all.extend(["array_in", "array_out", "array_recv", "array_send"].map(String::from));
    all.sort();
    all.dedup();
    all
}

fn arrays() -> impl Iterator<Item = (u32, String, String, u32)> {
    TYPES.iter().filter(|t| t.5 != 0).map(|t| (t.5, format!("_{}", t.1), format!("{}[]", t.2), t.0))
}

/// A column's type as Postgres names it: (type oid, typmod, element type of an array or 0).
pub fn pg_of(t: &DataType) -> (u32, i32) {
    match t {
        DataType::Boolean => (16, -1),
        DataType::Int8 | DataType::Int16 | DataType::UInt8 => (21, -1),
        DataType::Int32 | DataType::UInt16 => (23, -1),
        DataType::Int64 | DataType::UInt64 => (20, -1),
        DataType::UInt32 => (26, -1),
        DataType::Float16 | DataType::Float32 => (700, -1),
        DataType::Float64 => (701, -1),
        DataType::Decimal128(p, s) | DataType::Decimal256(p, s) => (1700, ((*p as i32) << 16 | (*s as i32 & 0xffff)) + 4),
        DataType::Date32 | DataType::Date64 => (1082, -1),
        DataType::Time32(_) | DataType::Time64(_) => (1083, -1),
        DataType::Timestamp(_, Some(_)) => (1184, -1),
        DataType::Timestamp(_, None) => (1114, -1),
        DataType::Interval(_) | DataType::Duration(_) => (1186, -1),
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView | DataType::FixedSizeBinary(_) => (17, -1),
        DataType::List(f) | DataType::LargeList(f) | DataType::FixedSizeList(f, _) => {
            let (item, _) = pg_of(f.data_type());
            (TYPES.iter().find(|t| t.0 == item).map_or(1015, |t| t.5), -1) // (as the wire sends them: `pg.rs`)
        }
        _ => (1043, -1), // (strings, and what has no Postgres equal: shown as text)
    }
}

/// `format_type(oid, typmod)`.
fn format_type(oid: u32, typmod: i32) -> Option<String> {
    let name = TYPES.iter().find(|t| t.0 == oid).map(|t| t.2.to_string()).or_else(|| arrays().find(|a| a.0 == oid).map(|a| a.2))?;
    Some(match (oid, typmod) {
        (1700, m) if m >= 4 => format!("numeric({},{})", (m - 4) >> 16, (m - 4) & 0xffff),
        _ => name,
    })
}

fn type_name(oid: u32) -> Option<String> { TYPES.iter().find(|t| t.0 == oid).map(|t| t.1.to_string()).or_else(|| arrays().find(|a| a.0 == oid).map(|a| a.1)) }

// ---------------------------------------------------------------- what the lake holds

/// A table, view or materialized view, as Postgres's catalog lists it.
struct Rel {
    oid: u32,
    schema: String,
    name: String,
    kind: char, // 'r' table, 'v' view, 'm' materialized view
    columns: Vec<(String, DataType, bool, Option<String>)>, // name, type, not null, default
    key: Vec<String>,
    sql: Option<String>, // a view's definition
    identity: BTreeMap<String, crate::seq::Identity>, // identity columns, by name (`attidentity`)
    constraints: Vec<crate::constraints::Constraint>, // UNIQUE, and keys said NOT ENFORCED (SQL's names)
    rows: u64,
    bytes: u64,
}

/// An index made with `CREATE INDEX` (`index.rs`): a key's own index is its table's `Rel`'s.
struct PgIndex {
    oid: u32,
    schema: String,
    name: String,
    table: u32,
    table_name: String,
    at: Vec<i16>, // its keys' columns, numbered from 1 (0: an expression)
    def: String,
    unique: bool, // an enforced UNIQUE constraint's
}

struct Lakes {
    indexes: Vec<PgIndex>,
    database: String,
    databases: Vec<String>,
    user: String,
    schemas: Vec<(u32, String)>,
    rels: Vec<Rel>,
    routines: Vec<(u32, String, String, crate::routines::Routine)>, // oid, schema, name
}

/// A stable oid for an object: a hash of its kind and name, above Postgres's own (16384).
fn oid(kind: &str, name: &str) -> u32 {
    let h = format!("{kind}:{name}").bytes().fold(0x811c_9dc5u32, |h, b| (h ^ b as u32).wrapping_mul(0x0100_0193));
    16384 + h % 2_000_000_000
}

const TEMP: &str = "pg_temp_1"; // (a session's temporary tables' schema)

fn schema_oid(s: &str) -> u32 {
    match s {
        "pg_catalog" => 11,
        "public" => 2200,
        "information_schema" => 13_000,
        s => oid("n", s),
    }
}

/// Postgres's catalog tables Pondra has (the oids clients look them up by: `'pg_class'::regclass`).
const CATALOG: &[(&str, u32)] = &[("pg_class", 1259), ("pg_namespace", 2615), ("pg_type", 1247), ("pg_attribute", 1249), ("pg_proc", 1255), ("pg_constraint", 2606),
    ("pg_index", 2610), ("pg_attrdef", 2604), ("pg_description", 2609), ("pg_database", 1262), ("pg_am", 2601), ("pg_depend", 2608), ("pg_rewrite", 2618),
    ("pg_inherits", 2611), ("pg_enum", 3501), ("pg_collation", 3456), ("pg_extension", 3079), ("pg_roles", 1260), ("pg_settings", 12000), ("pg_tables", 12001),
    ("pg_views", 12002), ("pg_matviews", 12003), ("pg_user", 12004), ("pg_range", 3541), ("pg_trigger", 2620), ("pg_tablespace", 1213), ("pg_sequence", 2224),
    ("pg_foreign_table", 3118), ("pg_partitioned_table", 3350), ("pg_stat_activity", 12005), ("pg_policy", 3256), ("pg_statistic_ext", 3381), ("pg_publication", 6104),
    ("pg_authid", 1260), ("pg_shdescription", 2396), ("pg_publication_namespace", 6237), ("pg_publication_rel", 6106), ("pg_language", 2612), ("pg_opclass", 2616), ("pg_event_trigger", 3466), ("pg_cast", 2605), ("pg_indexes", 12006)];

async fn lakes(lake: &Lake, user: &str, columns: bool) -> Result<Lakes> {
    let schemas = crate::ddl::schemas(lake).await?;
    let mvs: std::collections::HashSet<String> = lake.cat.scan::<serde_json::Value>("v/", "v0").await?.into_iter().map(|(k, _)| k[2..].to_string()).collect();
    let mut rels = vec![];
    for (k, m) in lake.cat.scan::<TableMeta>("t/", "t0").await? {
        let name = &k[2..];
        if crate::sys::hidden(name) {
            continue;
        }
        let (schema, table) = crate::ddl::split(name);
        let l = m.logical();
        let shown = |c: &String| c != "_deleted" || l.key.is_empty(); // (as `SELECT *` shows it)
        let cols = l.columns.iter().filter(|(c, _)| shown(c)).map(|(c, t)| {
            let t = crate::query::dtype(t).unwrap_or(DataType::Utf8);
            (c.clone(), t, l.not_null.contains(c), l.defaults.get(c).cloned())
        }).collect();
        let (rows, bytes) = m.files.iter().fold((0, 0), |(r, b), f| (r + f.rows, b + f.bytes));
        let sealed = m.sealed.clone().unwrap_or_default();
        let kind = if mvs.contains(name) { 'm' } else { 'r' };
        rels.push(Rel { oid: oid("r", name), schema: schema.into(), name: table.into(), kind, columns: cols, key: l.key.clone(), sql: None, identity: l.identity.clone(), constraints: l.constraints.clone(), rows: rows + sealed.rows, bytes: bytes + sealed.bytes });
    }
    let views = lake.cat.scan::<crate::ddl::StoredView>("q/", "q0").await?;
    // (a view's columns are its query's: planned only when a client asks for columns, in a
    // session that has every table)
    let every = match columns && !views.is_empty() {
        true => Some(crate::query::session(lake, "information_schema", "").await?),
        false => None,
    };
    for (k, v) in views {
        let name = &k[2..];
        let (schema, view) = crate::ddl::split(name);
        let cols = match &every {
            Some(ctx) => match ctx.sql(&format!("SELECT * FROM \"{schema}\".\"{view}\" LIMIT 0")).await {
                Ok(df) => df.schema().fields().iter().map(|f| (f.name().clone(), f.data_type().clone(), false, None)).collect(),
                Err(_) => vec![],
            },
            None => vec![],
        };
        rels.push(Rel { oid: oid("r", name), schema: schema.into(), name: view.into(), kind: 'v', columns: cols, key: vec![], sql: Some(v.sql), identity: Default::default(), constraints: vec![], rows: 0, bytes: 0 });
    }
    // The session's own temporary tables and views, in its temporary schema (as Postgres has them).
    let (temp_tables, temp_views) = crate::temp::listed();
    for (name, cols) in temp_tables {
        let cols = cols.iter().map(|(c, t)| (c.clone(), crate::query::dtype(t).unwrap_or(DataType::Utf8), false, None)).collect();
        rels.push(Rel { oid: oid("tmp", &name), schema: TEMP.into(), name, kind: 'r', columns: cols, key: vec![], sql: None, identity: Default::default(), constraints: vec![], rows: 0, bytes: 0 });
    }
    for (name, sql) in temp_views {
        rels.push(Rel { oid: oid("tmp", &name), schema: TEMP.into(), name, kind: 'v', columns: vec![], key: vec![], sql: Some(sql), identity: Default::default(), constraints: vec![], rows: 0, bytes: 0 });
    }
    let routines = lake.cat.scan::<crate::routines::Routine>("r/", "r0").await?.into_iter().map(|(k, r)| {
        let (s, n) = crate::ddl::split(&k[2..]);
        (oid("p", &k[2..]), s.to_string(), n.to_string(), r)
    }).collect();
    let mut indexes = vec![];
    for (k, i) in lake.cat.scan::<crate::index::Index>("ix/", "ix0").await? {
        let Some(r) = rels.iter().find(|r| r.oid == oid("r", &i.table)) else { continue };
        let meta = lake.cat.get::<TableMeta>(&crate::store::table_key(&i.table)).await?;
        let name = crate::ddl::split(&k[3..]).1.to_string();
        let at = i.keys.iter().map(|key| {
            let column = key.expr.strip_prefix('"').and_then(|e| e.strip_suffix('"')).filter(|c| !c.contains('"'));
            let named = column.map(|c| meta.as_ref().map_or(c, |m| m.name_of(c)));
            named.and_then(|c| r.columns.iter().position(|(n, ..)| n == c)).map_or(0, |p| p as i16 + 1)
        }).collect();
        let table = format!("{}.{}", crate::objects::ident(&r.schema), crate::objects::ident(&r.name));
        let def = crate::index::create_sql(&crate::objects::ident(&name), &table, &i, meta.as_ref());
        indexes.push(PgIndex { oid: oid("ix", &k[3..]), schema: r.schema.clone(), name, table: r.oid, table_name: r.name.clone(), at, def, unique: false });
    }
    // An enforced UNIQUE is an index too, as Postgres makes one for it (`\d` lists it).
    for r in &rels {
        for c in r.constraints.iter().filter(|c| c.enforced && c.kind == crate::constraints::Kind::Unique) {
            let table = format!("{}.{}", crate::objects::ident(&r.schema), crate::objects::ident(&r.name));
            let cols = c.columns.iter().map(|c| crate::objects::ident(c)).collect::<Vec<_>>().join(", ");
            let def = format!("CREATE UNIQUE INDEX {} ON {table} USING btree ({cols})", crate::objects::ident(&c.name));
            indexes.push(PgIndex { oid: oid("uq", &format!("{}.{}.{}", r.schema, r.name, c.name)), schema: r.schema.clone(), name: c.name.clone(), table: r.oid, table_name: r.name.clone(), at: at_of(r, &c.columns), def, unique: true });
        }
    }
    let database = crate::ddl::lake_name(lake);
    let mut databases = vec![database.clone()];
    databases.extend(lake.attached.read().unwrap().iter().map(|(n, _)| n.clone()));
    let mut all_schemas: Vec<(u32, String)> = vec![(11, "pg_catalog".into()), (13_000, "information_schema".into())];
    all_schemas.extend(schemas.iter().map(|s| (schema_oid(s), s.clone())));
    all_schemas.push((schema_oid(TEMP), TEMP.into()));
    Ok(Lakes { indexes, database, databases, user: user.into(), schemas: all_schemas, rels, routines })
}

// ---------------------------------------------------------------- tables

/// A table of `columns` (name, type) and `rows` (a NULL where a value is `ScalarValue::Null`).
fn table(columns: &[(&str, DataType)], rows: Vec<Vec<ScalarValue>>) -> Result<Arc<MemTable>> {
    let schema = Arc::new(Schema::new(columns.iter().map(|(n, t)| Field::new(*n, t.clone(), true)).collect::<Vec<_>>()));
    let arrays = columns.iter().enumerate().map(|(i, (_, t))| {
        let values = rows.iter().map(|r| match &r[i] {
            ScalarValue::Null => ScalarValue::try_from(t),
            v => v.cast_to(t),
        }).collect::<Result<Vec<_>, _>>()?;
        Ok(if values.is_empty() { new_null_array(t, 0) } else { ScalarValue::iter_to_array(values)? })
    }).collect::<Result<Vec<ArrayRef>>>()?;
    Ok(Arc::new(MemTable::try_new(schema.clone(), vec![vec![RecordBatch::try_new(schema, arrays)?]])?))
}

fn o(v: u32) -> ScalarValue { ScalarValue::UInt32(Some(v)) }
fn s(v: impl Into<String>) -> ScalarValue { ScalarValue::Utf8(Some(v.into())) }
fn b(v: bool) -> ScalarValue { ScalarValue::Boolean(Some(v)) }
fn i2(v: i16) -> ScalarValue { ScalarValue::Int16(Some(v)) }
fn i4(v: i32) -> ScalarValue { ScalarValue::Int32(Some(v)) }
fn f4(v: f32) -> ScalarValue { ScalarValue::Float32(Some(v)) }
fn n() -> ScalarValue { ScalarValue::Null }
/// Columns' places in a relation, numbered from 1 (0: not one of its columns).
fn at_of(r: &Rel, columns: &[String]) -> Vec<i16> { columns.iter().map(|c| r.columns.iter().position(|(n, ..)| n == c).map_or(0, |p| p as i16 + 1)).collect() }

/// The relation a foreign key names, as the lake names tables (`schema.table`, or `table`).
fn referred<'a>(rels: &'a [Rel], name: &str) -> Option<&'a Rel> {
    let (schema, table) = crate::ddl::split(name);
    rels.iter().find(|r| r.kind != 'v' && r.schema == schema && r.name == table)
}

/// A constraint's oid: its table's, then its name.
fn constraint_oid(r: &Rel, c: &crate::constraints::Constraint) -> u32 { oid("cn", &format!("{}.{}.{}", r.schema, r.name, c.name)) }

fn i2s(v: &[i16]) -> ScalarValue { ScalarValue::List(ScalarValue::new_list_nullable(&v.iter().map(|x| i2(*x)).collect::<Vec<_>>(), &DataType::Int16)) }
fn texts(v: &[String]) -> ScalarValue { ScalarValue::List(ScalarValue::new_list_nullable(&v.iter().map(|x| s(x.clone())).collect::<Vec<_>>(), &DataType::Utf8)) }

const OID: DataType = DataType::UInt32;
const TEXT: DataType = DataType::Utf8;
const BOOL: DataType = DataType::Boolean;
const INT2: DataType = DataType::Int16;
const INT4: DataType = DataType::Int32;
const INT8: DataType = DataType::Int64;
fn list(t: DataType) -> DataType { DataType::List(Arc::new(Field::new("item", t, true))) }

fn tables(l: &Lakes) -> Result<Vec<(&'static str, Arc<MemTable>)>> {
    let owner = 10u32;
    let ns = |s: &str| schema_oid(s);
    let mut out = vec![];
    out.push(("pg_namespace", table(&[("oid", OID), ("nspname", TEXT), ("nspowner", OID), ("nspacl", list(TEXT))],
        l.schemas.iter().map(|(oid, name)| vec![o(*oid), s(name.clone()), o(owner), n()]).collect())?));
    // Relations: tables, views, materialized views, and each key's index.
    let mut classes = vec![];
    for r in &l.rels {
        classes.push(vec![o(r.oid), s(r.name.clone()), o(ns(&r.schema)), o(0), o(0), o(owner), o(if r.kind == 'v' { 0 } else { 2 }), o(r.oid), o(0), i4((r.bytes / 8192) as i32),
            f4(if r.kind == 'v' { -1.0 } else { r.rows as f32 }), i4(0), o(0), b(!r.key.is_empty() || l.indexes.iter().any(|i| i.table == r.oid)), b(false), s(if r.schema == TEMP { "t" } else { "p" }), s(r.kind.to_string()), i2(r.columns.len() as i16), i2(0),
            b(r.kind == 'v'), b(false), b(false), b(false), b(false), b(true), s("d"), b(false), o(0), n(), n(), n()]);
        if !r.key.is_empty() {
            classes.push(vec![o(oid("i", &format!("{}.{}", r.schema, r.name))), s(format!("{}_pkey", r.name)), o(ns(&r.schema)), o(0), o(0), o(owner), o(403), o(0), o(0), i4(1), f4(r.rows as f32),
                i4(0), o(0), b(false), b(false), s("p"), s("i"), i2(r.key.len() as i16), i2(0), b(false), b(false), b(false), b(false), b(false), b(true), s("n"), b(false), o(0), n(), n(), n()]);
        }
    }
    for i in &l.indexes {
        classes.push(vec![o(i.oid), s(i.name.clone()), o(ns(&i.schema)), o(0), o(0), o(owner), o(403), o(0), o(0), i4(1), f4(0.0),
            i4(0), o(0), b(false), b(false), s("p"), s("i"), i2(i.at.len() as i16), i2(0), b(false), b(false), b(false), b(false), b(false), b(true), s("n"), b(false), o(0), n(), n(), n()]);
    }
    for (name, oid) in CATALOG {
        classes.push(vec![o(*oid), s(*name), o(11), o(0), o(0), o(owner), o(0), o(*oid), o(0), i4(0), f4(-1.0), i4(0), o(0), b(false), b(false), s("p"), s("v"), i2(0), i2(0),
            b(true), b(false), b(false), b(false), b(false), b(true), s("n"), b(false), o(0), n(), n(), n()]);
    }
    out.push(("pg_class", table(&[("oid", OID), ("relname", TEXT), ("relnamespace", OID), ("reltype", OID), ("reloftype", OID), ("relowner", OID), ("relam", OID), ("relfilenode", OID),
        ("reltablespace", OID), ("relpages", INT4), ("reltuples", DataType::Float32), ("relallvisible", INT4), ("reltoastrelid", OID), ("relhasindex", BOOL), ("relisshared", BOOL),
        ("relpersistence", TEXT), ("relkind", TEXT), ("relnatts", INT2), ("relchecks", INT2), ("relhasrules", BOOL), ("relhastriggers", BOOL), ("relhassubclass", BOOL),
        ("relrowsecurity", BOOL), ("relforcerowsecurity", BOOL), ("relispopulated", BOOL), ("relreplident", TEXT), ("relispartition", BOOL), ("relrewrite", OID),
        ("relacl", list(TEXT)), ("reloptions", list(TEXT)), ("relpartbound", TEXT)], classes)?));
    // Columns (and their defaults), numbered from 1.
    let (mut attrs, mut defs) = (vec![], vec![]);
    for r in &l.rels {
        for (i, (name, t, not_null, default)) in r.columns.iter().enumerate() {
            let (typ, typmod) = pg_of(t);
            let len = TYPES.iter().find(|x| x.0 == typ).map_or(-1, |x| x.3);
            let not_null = *not_null || r.key.contains(name);
            let identity = r.identity.get(name).map_or("", |i| if i.always { "a" } else { "d" }); // (an identity has no default of its own, as in Postgres)
            let default = default.as_ref().filter(|_| identity.is_empty());
            attrs.push(vec![o(r.oid), s(name.clone()), o(typ), i4(-1), i2(len), i2(i as i16 + 1), i4(0), i4(-1), i4(typmod), b(len > 0 && len <= 8), s("i"), s("x"), s(""), b(not_null),
                b(default.is_some()), b(false), s(identity), s(""), b(false), b(true), i4(0), o(if matches!(typ, 25 | 1043) { 100 } else { 0 }), n(), n()]);
            if let Some(d) = default {
                defs.push(vec![o(oid("d", &format!("{}.{}.{name}", r.schema, r.name))), o(r.oid), i2(i as i16 + 1), s(d.clone())]);
            }
        }
    }
    // A key's index has its columns too (numbered from 1 within it): ODBC's primary keys read them.
    for r in l.rels.iter().filter(|r| !r.key.is_empty()) {
        let index = oid("i", &format!("{}.{}", r.schema, r.name));
        for (i, k) in r.key.iter().enumerate() {
            let Some((_, t, ..)) = r.columns.iter().find(|(c, ..)| c == k) else { continue };
            let (typ, typmod) = pg_of(t);
            attrs.push(vec![o(index), s(k.clone()), o(typ), i4(-1), i2(8), i2(i as i16 + 1), i4(0), i4(-1), i4(typmod), b(true), s("i"), s("p"), s(""), b(true), b(false), b(false), s(""), s(""), b(false), b(true), i4(0), o(0), n(), n()]);
        }
    }
    out.push(("pg_attribute", table(&[("attrelid", OID), ("attname", TEXT), ("atttypid", OID), ("attstattarget", INT4), ("attlen", INT2), ("attnum", INT2), ("attndims", INT4),
        ("attcacheoff", INT4), ("atttypmod", INT4), ("attbyval", BOOL), ("attalign", TEXT), ("attstorage", TEXT), ("attcompression", TEXT), ("attnotnull", BOOL), ("atthasdef", BOOL),
        ("atthasmissing", BOOL), ("attidentity", TEXT), ("attgenerated", TEXT), ("attisdropped", BOOL), ("attislocal", BOOL), ("attinhcount", INT4), ("attcollation", OID),
        ("attacl", list(TEXT)), ("attoptions", list(TEXT))], attrs)?));
    out.push(("pg_attrdef", table(&[("oid", OID), ("adrelid", OID), ("adnum", INT2), ("adbin", TEXT)], defs)?));
    // Types, and an array type for each.
    // (The functions by Postgres's own names: drivers such as ADBC's pick a type's binary format by
    // its `typreceive`. Newer types' names have an underscore.)
    let fun = |t: &PgType, f: &str| s(type_function(t, f));
    let mut types: Vec<Vec<ScalarValue>> = TYPES.iter().map(|t| vec![o(t.0), s(t.1), o(11), o(owner), i2(t.3), b(t.3 > 0 && t.3 <= 8), s(if t.4 == 'P' { "p" } else { "b" }), s(t.4.to_string()),
        b(false), b(true), s(","), o(0), o(0), o(t.5), fun(t, "in"), fun(t, "out"), fun(t, "recv"), fun(t, "send"), o(0), i4(-1), b(false), i4(0),
        o(if matches!(t.0, 25 | 1043 | 1042 | 19) { 100 } else { 0 }), n(), s("i"), s("p")]).collect();
    types.extend(arrays().map(|(oid, name, _, elem)| vec![o(oid), s(name), o(11), o(owner), i2(-1), b(false), s("b"), s("A"), b(false), b(true), s(","), o(0), o(elem), o(0), s("array_in"),
        s("array_out"), s("array_recv"), s("array_send"), o(0), i4(-1), b(false), i4(0), o(0), n(), s("i"), s("x")]));
    out.push(("pg_type", table(&[("oid", OID), ("typname", TEXT), ("typnamespace", OID), ("typowner", OID), ("typlen", INT2), ("typbyval", BOOL), ("typtype", TEXT), ("typcategory", TEXT),
        ("typispreferred", BOOL), ("typisdefined", BOOL), ("typdelim", TEXT), ("typrelid", OID), ("typelem", OID), ("typarray", OID), ("typinput", TEXT), ("typoutput", TEXT),
        ("typreceive", TEXT), ("typsend", TEXT), ("typbasetype", OID), ("typtypmod", INT4), ("typnotnull", BOOL), ("typndims", INT4), ("typcollation", OID), ("typdefault", TEXT),
        ("typalign", TEXT), ("typstorage", TEXT)], types)?));
    // Keys: an index and a constraint for each keyed table.
    let (mut indexes, mut constraints) = (vec![], vec![]);
    for r in l.rels.iter().filter(|r| !r.key.is_empty()) {
        let at: Vec<i16> = r.key.iter().filter_map(|k| r.columns.iter().position(|(c, ..)| c == k).map(|i| i as i16 + 1)).collect();
        let full = format!("{}.{}", r.schema, r.name);
        indexes.push(vec![o(oid("i", &full)), o(r.oid), i2(at.len() as i16), i2(at.len() as i16), b(true), b(false), b(true), b(false), b(true), b(false), b(true), b(false), b(true), b(true), b(false), i2s(&at), ScalarValue::List(ScalarValue::new_list_nullable(&at.iter().map(|_| o(0)).collect::<Vec<_>>(), &OID)),
            ScalarValue::List(ScalarValue::new_list_nullable(&at.iter().map(|_| o(3124)).collect::<Vec<_>>(), &OID)), i2s(&at.iter().map(|_| 0).collect::<Vec<_>>()), n(), n()]);
        constraints.push(vec![o(oid("c", &full)), s(format!("{}_pkey", r.name)), o(ns(&r.schema)), s("p"), b(false), b(false), b(true), o(r.oid), o(0), o(oid("i", &full)), o(0), o(0), s(" "), s(" "), s(" "),
            b(true), i4(0), b(true), i2s(&at), n(), n(), b(true)]);
    }
    for i in &l.indexes {
        let each = |v: ScalarValue| ScalarValue::List(ScalarValue::new_list_nullable(&i.at.iter().map(|_| v.clone()).collect::<Vec<_>>(), &OID));
        indexes.push(vec![o(i.oid), o(i.table), i2(i.at.len() as i16), i2(i.at.len() as i16), b(i.unique), b(false), b(false), b(false), b(true), b(false), b(true), b(false), b(true), b(true), b(false), i2s(&i.at), each(o(0)),
            each(o(3124)), i2s(&i.at.iter().map(|_| 0).collect::<Vec<_>>()), n(), n()]);
    }
    for r in &l.rels {
        for c in &r.constraints {
            let (kind, index) = match c.kind {
                crate::constraints::Kind::Unique => ("u", if c.enforced { oid("uq", &format!("{}.{}.{}", r.schema, r.name, c.name)) } else { 0 }),
                crate::constraints::Kind::PrimaryKey => ("p", 0),
                crate::constraints::Kind::ForeignKey => ("f", 0),
            };
            let to = c.references.as_ref().and_then(|(t, _)| referred(&l.rels, t));
            let to_cols = match (to, &c.references) {
                (Some(t), Some((_, cols))) if !cols.is_empty() => i2s(&at_of(t, cols)),
                (Some(t), _) => i2s(&at_of(t, &t.key)),
                _ => n(),
            };
            let foreign = c.kind == crate::constraints::Kind::ForeignKey;
            constraints.push(vec![o(constraint_oid(r, c)), s(c.name.clone()), o(ns(&r.schema)), s(kind), b(false), b(false), b(c.enforced), o(r.oid), o(0), o(index), o(0), o(to.map_or(0, |t| t.oid)),
                s(if foreign { "a" } else { " " }), s(if foreign { "a" } else { " " }), s(if foreign { "s" } else { " " }), b(true), i4(0), b(true), i2s(&at_of(r, &c.columns)), if foreign { to_cols } else { n() }, n(), b(c.enforced)]);
        }
    }
    out.push(("pg_index", table(&[("indexrelid", OID), ("indrelid", OID), ("indnatts", INT2), ("indnkeyatts", INT2), ("indisunique", BOOL), ("indnullsnotdistinct", BOOL), ("indisprimary", BOOL), ("indisexclusion", BOOL),
        ("indimmediate", BOOL), ("indisclustered", BOOL), ("indisvalid", BOOL), ("indcheckxmin", BOOL), ("indisready", BOOL), ("indislive", BOOL), ("indisreplident", BOOL),
        ("indkey", list(INT2)), ("indcollation", list(OID)), ("indclass", list(OID)), ("indoption", list(INT2)), ("indexprs", TEXT), ("indpred", TEXT)], indexes)?));
    out.push(("pg_constraint", table(&[("oid", OID), ("conname", TEXT), ("connamespace", OID), ("contype", TEXT), ("condeferrable", BOOL), ("condeferred", BOOL), ("convalidated", BOOL),
        ("conrelid", OID), ("contypid", OID), ("conindid", OID), ("conparentid", OID), ("confrelid", OID), ("confupdtype", TEXT), ("confdeltype", TEXT), ("confmatchtype", TEXT),
        ("conislocal", BOOL), ("coninhcount", INT4), ("connoinherit", BOOL), ("conkey", list(INT2)), ("confkey", list(INT2)), ("conbin", TEXT), ("conenforced", BOOL)], constraints)?));
    // The views Postgres builds over those.
    let rel_rows = |kind: char| l.rels.iter().filter(move |r| r.kind == kind);
    out.push(("pg_tables", table(&[("schemaname", TEXT), ("tablename", TEXT), ("tableowner", TEXT), ("tablespace", TEXT), ("hasindexes", BOOL), ("hasrules", BOOL), ("hastriggers", BOOL), ("rowsecurity", BOOL)],
        rel_rows('r').map(|r| vec![s(r.schema.clone()), s(r.name.clone()), s(l.user.clone()), n(), b(!r.key.is_empty()), b(false), b(false), b(false)]).collect())?));
    let mut listed: Vec<Vec<ScalarValue>> = l.rels.iter().filter(|r| !r.key.is_empty()).map(|r| {
        let def = format!("CREATE UNIQUE INDEX {}_pkey ON {}.{} USING btree ({})", r.name, r.schema, r.name, r.key.join(", "));
        vec![s(r.schema.clone()), s(r.name.clone()), s(format!("{}_pkey", r.name)), n(), s(def)]
    }).collect();
    listed.extend(l.indexes.iter().map(|i| vec![s(i.schema.clone()), s(i.table_name.clone()), s(i.name.clone()), n(), s(i.def.clone())]));
    out.push(("pg_indexes", table(&[("schemaname", TEXT), ("tablename", TEXT), ("indexname", TEXT), ("tablespace", TEXT), ("indexdef", TEXT)], listed)?));
    out.push(("pg_views", table(&[("schemaname", TEXT), ("viewname", TEXT), ("viewowner", TEXT), ("definition", TEXT)],
        rel_rows('v').map(|r| vec![s(r.schema.clone()), s(r.name.clone()), s(l.user.clone()), s(r.sql.clone().unwrap_or_default())]).collect())?));
    out.push(("pg_matviews", table(&[("schemaname", TEXT), ("matviewname", TEXT), ("matviewowner", TEXT), ("tablespace", TEXT), ("hasindexes", BOOL), ("ispopulated", BOOL), ("definition", TEXT)],
        rel_rows('m').map(|r| vec![s(r.schema.clone()), s(r.name.clone()), s(l.user.clone()), n(), b(!r.key.is_empty()), b(true), s("")]).collect())?));
    out.push(("pg_proc", table(&[("oid", OID), ("proname", TEXT), ("pronamespace", OID), ("proowner", OID), ("prolang", OID), ("procost", DataType::Float32), ("prorows", DataType::Float32),
        ("provariadic", OID), ("prosupport", TEXT), ("prokind", TEXT), ("prosecdef", BOOL), ("proleakproof", BOOL), ("proisstrict", BOOL), ("proretset", BOOL), ("provolatile", TEXT),
        ("proparallel", TEXT), ("pronargs", INT2), ("pronargdefaults", INT2), ("prorettype", OID), ("proargtypes", list(OID)), ("proallargtypes", list(OID)), ("proargmodes", list(TEXT)),
        ("proargnames", list(TEXT)), ("prosrc", TEXT), ("probin", TEXT), ("proconfig", list(TEXT)), ("proacl", list(TEXT))],
        l.routines.iter().map(|(oid, schema, name, r)| {
            use crate::routines::Kind;
            let kind = match r.kind { Kind::Procedure => "p", _ => "f" };
            let lang = if r.language == "python" { 13_000 } else { 14 };
            let rettype = match r.kind { Kind::Procedure => 2278, Kind::Table => 2249, Kind::Macro => r.returns.as_deref().map_or(25, sql_oid) };
            let args: Vec<ScalarValue> = r.params.iter().map(|p| o(p.ty.as_deref().map_or(25, sql_oid))).collect();
            let names: Vec<String> = r.params.iter().map(|p| p.name.clone()).collect();
            vec![o(*oid), s(name.clone()), o(schema_oid(schema)), o(owner), o(lang), f4(100.0), f4(if r.kind == Kind::Table { 1000.0 } else { 0.0 }), o(0), s("-"), s(kind), b(false), b(false), b(false),
                b(r.kind == Kind::Table), s("v"), s("u"), i2(r.params.len() as i16), i2(r.params.iter().filter(|p| p.default.is_some()).count() as i16), o(rettype),
                ScalarValue::List(ScalarValue::new_list_nullable(&args, &OID)), n(), n(), texts(&names), s(r.body.clone()), n(), n(), n()]
        }).chain(type_functions().into_iter().map(|f| vec![o(oid("tf", &f)), s(f.clone()), o(11), o(10), o(12), f4(1.0), f4(0.0), o(0), s("-"), s("f"), b(false), b(false), b(true),
            b(false), s("i"), s("s"), i2(1), i2(0), o(0), ScalarValue::List(ScalarValue::new_list_nullable(&[o(0)], &OID)), n(), n(), n(), s(f), n(), n(), n()])).collect())?));
    out.push(("pg_database", table(&[("oid", OID), ("datname", TEXT), ("datdba", OID), ("encoding", INT4), ("datlocprovider", TEXT), ("datistemplate", BOOL), ("datallowconn", BOOL),
        ("datconnlimit", INT4), ("datfrozenxid", OID), ("datminmxid", OID), ("dattablespace", OID), ("datcollate", TEXT), ("datctype", TEXT), ("daticulocale", TEXT), ("daticurules", TEXT),
        ("datcollversion", TEXT), ("datacl", list(TEXT))],
        l.databases.iter().map(|d| vec![o(if *d == l.database { 16384 } else { oid("db", d) }), s(d.clone()), o(owner), i4(6), s("c"), b(false), b(true), i4(-1), o(0), o(0), o(1663), s("C.UTF-8"), s("C.UTF-8"), n(), n(), n(), n()]).collect())?));
    let roles = [(10u32, "pondra", true), (oid("u", &l.user), l.user.as_str(), l.user == "admin")];
    let roles: Vec<_> = roles.iter().filter(|(o, n, _)| *o == 10 || *n != "pondra").collect();
    out.push(("pg_roles", table(&[("oid", OID), ("rolname", TEXT), ("rolsuper", BOOL), ("rolinherit", BOOL), ("rolcreaterole", BOOL), ("rolcreatedb", BOOL), ("rolcanlogin", BOOL),
        ("rolreplication", BOOL), ("rolconnlimit", INT4), ("rolpassword", TEXT), ("rolvaliduntil", TEXT), ("rolbypassrls", BOOL), ("rolconfig", list(TEXT))],
        roles.iter().map(|(oid, name, sup)| vec![o(*oid), s(*name), b(*sup), b(true), b(*sup), b(*sup), b(true), b(false), i4(-1), s("********"), n(), b(false), n()]).collect())?));
    out.push(("pg_user", table(&[("usename", TEXT), ("usesysid", OID), ("usecreatedb", BOOL), ("usesuper", BOOL), ("userepl", BOOL), ("usebypassrls", BOOL), ("passwd", TEXT), ("valuntil", TEXT), ("useconfig", list(TEXT))],
        roles.iter().map(|(oid, name, sup)| vec![s(*name), o(*oid), b(*sup), b(*sup), b(false), b(false), s("********"), n(), n()]).collect())?));
    out.push(("pg_settings", table(&[("name", TEXT), ("setting", TEXT), ("unit", TEXT), ("category", TEXT), ("short_desc", TEXT), ("context", TEXT), ("vartype", TEXT), ("source", TEXT)],
        SETTINGS.iter().map(|(k, v)| vec![s(*k), s(*v), n(), s("Pondra"), s(""), s("user"), s("string"), s("default")]).collect())?));
    out.push(("pg_am", table(&[("oid", OID), ("amname", TEXT), ("amhandler", TEXT), ("amtype", TEXT)], vec![vec![o(2), s("heap"), s("heap_tableam_handler"), s("t")], vec![o(403), s("btree"), s("bthandler"), s("i")]])?));
    out.push(("pg_tablespace", table(&[("oid", OID), ("spcname", TEXT), ("spcowner", OID), ("spcacl", list(TEXT)), ("spcoptions", list(TEXT))], vec![vec![o(1663), s("pg_default"), o(owner), n(), n()]])?));
    out.push(("pg_language", table(&[("oid", OID), ("lanname", TEXT), ("lanowner", OID), ("lanispl", BOOL), ("lanpltrusted", BOOL)],
        vec![vec![o(12), s("internal"), o(owner), b(false), b(false)], vec![o(14), s("sql"), o(owner), b(false), b(true)], vec![o(13_000), s("python"), o(owner), b(true), b(false)]])?));
    // Present, and empty: nothing in Pondra is one of these.
    for (name, cols) in [
        ("pg_description", vec![("objoid", OID), ("classoid", OID), ("objsubid", INT4), ("description", TEXT)]),
        ("pg_shdescription", vec![("objoid", OID), ("classoid", OID), ("description", TEXT)]),
        ("pg_depend", vec![("classid", OID), ("objid", OID), ("objsubid", INT4), ("refclassid", OID), ("refobjid", OID), ("refobjsubid", INT4), ("deptype", TEXT)]),
        ("pg_rewrite", vec![("oid", OID), ("rulename", TEXT), ("ev_class", OID), ("ev_type", TEXT), ("ev_enabled", TEXT), ("is_instead", BOOL), ("ev_qual", TEXT), ("ev_action", TEXT)]),
        ("pg_inherits", vec![("inhrelid", OID), ("inhparent", OID), ("inhseqno", INT4), ("inhdetachpending", BOOL)]),
        ("pg_enum", vec![("oid", OID), ("enumtypid", OID), ("enumsortorder", DataType::Float32), ("enumlabel", TEXT)]),
        ("pg_collation", vec![("oid", OID), ("collname", TEXT), ("collnamespace", OID), ("collowner", OID), ("collprovider", TEXT), ("collisdeterministic", BOOL), ("collencoding", INT4), ("collcollate", TEXT), ("collctype", TEXT)]),
        ("pg_extension", vec![("oid", OID), ("extname", TEXT), ("extowner", OID), ("extnamespace", OID), ("extrelocatable", BOOL), ("extversion", TEXT), ("extconfig", list(OID)), ("extcondition", list(TEXT))]),
        ("pg_range", vec![("rngtypid", OID), ("rngsubtype", OID), ("rngmultitypid", OID), ("rngcollation", OID), ("rngsubopc", OID), ("rngcanonical", TEXT), ("rngsubdiff", TEXT)]),
        ("pg_trigger", vec![("oid", OID), ("tgrelid", OID), ("tgparentid", OID), ("tgname", TEXT), ("tgfoid", OID), ("tgtype", INT2), ("tgenabled", TEXT), ("tgisinternal", BOOL), ("tgconstraint", OID)]),
        ("pg_sequence", vec![("seqrelid", OID), ("seqtypid", OID), ("seqstart", INT8), ("seqincrement", INT8), ("seqmax", INT8), ("seqmin", INT8), ("seqcache", INT8), ("seqcycle", BOOL)]),
        ("pg_foreign_table", vec![("ftrelid", OID), ("ftserver", OID), ("ftoptions", list(TEXT))]),
        ("pg_partitioned_table", vec![("partrelid", OID), ("partstrat", TEXT), ("partnatts", INT2), ("partdefid", OID), ("partattrs", list(INT2))]),
        ("pg_policy", vec![("oid", OID), ("polname", TEXT), ("polrelid", OID), ("polcmd", TEXT), ("polpermissive", BOOL), ("polroles", list(OID)), ("polqual", TEXT), ("polwithcheck", TEXT)]),
        ("pg_statistic_ext", vec![("oid", OID), ("stxrelid", OID), ("stxname", TEXT), ("stxnamespace", OID), ("stxowner", OID), ("stxkeys", list(INT2)), ("stxkind", list(TEXT))]),
        ("pg_publication", vec![("oid", OID), ("pubname", TEXT), ("pubowner", OID), ("puballtables", BOOL)]),
        ("pg_event_trigger", vec![("oid", OID), ("evtname", TEXT), ("evtevent", TEXT), ("evtowner", OID), ("evtfoid", OID), ("evtenabled", TEXT)]),
        ("pg_cast", vec![("oid", OID), ("castsource", OID), ("casttarget", OID), ("castfunc", OID), ("castcontext", TEXT), ("castmethod", TEXT)]),
        ("pg_opclass", vec![("oid", OID), ("opcmethod", OID), ("opcname", TEXT), ("opcnamespace", OID), ("opcowner", OID), ("opcfamily", OID), ("opcintype", OID), ("opcdefault", BOOL), ("opckeytype", OID)]),
        ("pg_stat_activity", vec![("datid", OID), ("datname", TEXT), ("pid", INT4), ("usename", TEXT), ("application_name", TEXT), ("client_addr", TEXT), ("state", TEXT), ("query", TEXT)]),
    ] {
        let cols: Vec<(&str, DataType)> = cols;
        out.push((name, table(&cols, vec![])?));
    }
    Ok(out)
}

/// Settings clients read (`SHOW`, `current_setting`, `pg_settings`).
pub const SETTINGS: &[(&str, &str)] = &[("server_version", "16.0"), ("server_version_num", "160000"), ("server_encoding", "UTF8"), ("client_encoding", "UTF8"),
    ("standard_conforming_strings", "on"), ("integer_datetimes", "on"), ("TimeZone", "UTC"), ("DateStyle", "ISO, MDY"), ("IntervalStyle", "postgres"),
    ("max_identifier_length", "63"), ("default_transaction_isolation", "read committed"), ("transaction_isolation", "read committed"), ("search_path", "\"$user\", public"),
    ("is_superuser", "off"), ("application_name", ""), ("default_transaction_read_only", "off"), ("lc_collate", "C.UTF-8"), ("lc_ctype", "C.UTF-8")];

/// The SQL standard's `information_schema`, Postgres's way: its types by Postgres's names.
fn information_schema(l: &Lakes) -> Result<Vec<(&'static str, Arc<MemTable>)>> {
    let db = || s(l.database.clone());
    let yes = |v: bool| s(if v { "YES" } else { "NO" });
    let mut out = vec![];
    out.push(("schemata", table(&[("catalog_name", TEXT), ("schema_name", TEXT), ("schema_owner", TEXT), ("default_character_set_catalog", TEXT), ("default_character_set_schema", TEXT),
        ("default_character_set_name", TEXT), ("sql_path", TEXT)], l.schemas.iter().map(|(_, sc)| vec![db(), s(sc.clone()), s(l.user.clone()), n(), n(), n(), n()]).collect())?));
    out.push(("tables", table(&[("table_catalog", TEXT), ("table_schema", TEXT), ("table_name", TEXT), ("table_type", TEXT), ("self_referencing_column_name", TEXT), ("reference_generation", TEXT),
        ("user_defined_type_catalog", TEXT), ("user_defined_type_schema", TEXT), ("user_defined_type_name", TEXT), ("is_insertable_into", TEXT), ("is_typed", TEXT), ("commit_action", TEXT)],
        l.rels.iter().map(|r| vec![db(), s(r.schema.clone()), s(r.name.clone()), s(if r.kind == 'v' { "VIEW" } else { "BASE TABLE" }), n(), n(), n(), n(), n(), yes(r.kind == 'r'), s("NO"), n()]).collect())?));
    let mut cols = vec![];
    for r in &l.rels {
        for (i, (name, t, not_null, default)) in r.columns.iter().enumerate() {
            let (typ, typmod) = pg_of(t);
            let array = arrays().find(|a| a.0 == typ);
            let data_type = match array { Some(_) => "ARRAY".to_string(), None => format_type(typ, -1).unwrap_or_else(|| "text".into()) };
            let (precision, scale, radix) = match t {
                DataType::Int16 | DataType::Int8 | DataType::UInt8 => (i4(16), i4(0), i4(2)),
                DataType::Int32 | DataType::UInt16 => (i4(32), i4(0), i4(2)),
                DataType::Int64 | DataType::UInt64 | DataType::UInt32 => (i4(64), i4(0), i4(2)),
                DataType::Float32 => (i4(24), n(), i4(2)),
                DataType::Float64 => (i4(53), n(), i4(2)),
                DataType::Decimal128(p, s_) | DataType::Decimal256(p, s_) => (i4(*p as i32), i4(*s_ as i32), i4(10)),
                _ => (n(), n(), n()),
            };
            let datetime = if matches!(typ, 1082) { i4(0) } else if matches!(typ, 1083 | 1114 | 1184) { i4(6) } else { n() };
            let udt = type_name(typ).unwrap_or_else(|| "text".into());
            let _ = typmod;
            cols.push(vec![db(), s(r.schema.clone()), s(r.name.clone()), s(name.clone()), i4(i as i32 + 1), default.clone().map_or(n(), s), yes(!(*not_null || r.key.contains(name))), s(data_type),
                n(), if matches!(typ, 25 | 1043) { i4(1_073_741_824) } else { n() }, precision, radix, scale, datetime, n(), n(), n(), n(), n(), n(), n(), n(), n(), n(), n(), db(), s("pg_catalog"), s(udt),
                n(), n(), n(), n(), s((i + 1).to_string()), s("NO"), s("NO"), n(), n(), n(), n(), n(), s("NO"), s("NEVER"), n(), yes(r.kind == 'r')]);
        }
    }
    out.push(("columns", table(&[("table_catalog", TEXT), ("table_schema", TEXT), ("table_name", TEXT), ("column_name", TEXT), ("ordinal_position", INT4), ("column_default", TEXT),
        ("is_nullable", TEXT), ("data_type", TEXT), ("character_maximum_length", INT4), ("character_octet_length", INT4), ("numeric_precision", INT4), ("numeric_precision_radix", INT4),
        ("numeric_scale", INT4), ("datetime_precision", INT4), ("interval_type", TEXT), ("interval_precision", INT4), ("character_set_catalog", TEXT), ("character_set_schema", TEXT),
        ("character_set_name", TEXT), ("collation_catalog", TEXT), ("collation_schema", TEXT), ("collation_name", TEXT), ("domain_catalog", TEXT), ("domain_schema", TEXT), ("domain_name", TEXT),
        ("udt_catalog", TEXT), ("udt_schema", TEXT), ("udt_name", TEXT), ("scope_catalog", TEXT), ("scope_schema", TEXT), ("scope_name", TEXT), ("maximum_cardinality", INT4),
        ("dtd_identifier", TEXT), ("is_self_referencing", TEXT), ("is_identity", TEXT), ("identity_generation", TEXT), ("identity_start", TEXT), ("identity_increment", TEXT),
        ("identity_maximum", TEXT), ("identity_minimum", TEXT), ("identity_cycle", TEXT), ("is_generated", TEXT), ("generation_expression", TEXT), ("is_updatable", TEXT)], cols)?));
    out.push(("views", table(&[("table_catalog", TEXT), ("table_schema", TEXT), ("table_name", TEXT), ("view_definition", TEXT), ("check_option", TEXT), ("is_updatable", TEXT),
        ("is_insertable_into", TEXT), ("is_trigger_updatable", TEXT), ("is_trigger_deletable", TEXT), ("is_trigger_insertable_into", TEXT)],
        l.rels.iter().filter(|r| r.kind == 'v').map(|r| vec![db(), s(r.schema.clone()), s(r.name.clone()), s(r.sql.clone().unwrap_or_default()), s("NONE"), s("NO"), s("NO"), s("NO"), s("NO"), s("NO")]).collect())?));
    // Each table's key, then its declared constraints, as the standard lists them.
    let yes = |b: bool| s(if b { "YES" } else { "NO" });
    let keyed: Vec<&Rel> = l.rels.iter().filter(|r| !r.key.is_empty()).collect();
    let mut table_constraints: Vec<Vec<ScalarValue>> = keyed.iter().map(|r| vec![db(), s(r.schema.clone()), s(format!("{}_pkey", r.name)), db(), s(r.schema.clone()), s(r.name.clone()), s("PRIMARY KEY"), s("NO"), s("NO"), s("YES"), n()]).collect();
    let mut key_columns: Vec<Vec<ScalarValue>> = keyed.iter().flat_map(|r| r.key.iter().enumerate().map(move |(i, k)| (r, i, k))).map(|(r, i, k)| vec![db(), s(r.schema.clone()), s(format!("{}_pkey", r.name)), db(), s(r.schema.clone()), s(r.name.clone()), s(k.clone()), i4(i as i32 + 1), n()]).collect();
    let mut used: Vec<Vec<ScalarValue>> = keyed.iter().flat_map(|r| r.key.iter().map(move |k| (r, k))).map(|(r, k)| vec![db(), s(r.schema.clone()), s(r.name.clone()), s(k.clone()), db(), s(r.schema.clone()), s(format!("{}_pkey", r.name))]).collect();
    let mut referential = vec![];
    for r in &l.rels {
        for c in &r.constraints {
            use crate::constraints::Kind;
            let kind = match c.kind {
                Kind::Unique => "UNIQUE",
                Kind::PrimaryKey => "PRIMARY KEY",
                Kind::ForeignKey => "FOREIGN KEY",
            };
            table_constraints.push(vec![db(), s(r.schema.clone()), s(c.name.clone()), db(), s(r.schema.clone()), s(r.name.clone()), s(kind), s("NO"), s("NO"), yes(c.enforced), if c.kind == Kind::Unique { s("YES") } else { n() }]);
            // (a foreign key's columns, and the key of the table it names: by place)
            let to = c.references.as_ref().and_then(|(t, _)| referred(&l.rels, t));
            let to_cols = match (&c.references, to) {
                (Some((_, cols)), _) if !cols.is_empty() => cols.clone(),
                (_, Some(t)) => t.key.clone(),
                _ => vec![],
            };
            for (i, col) in c.columns.iter().enumerate() {
                let place = if c.kind == Kind::ForeignKey && i < to_cols.len() { i4(i as i32 + 1) } else { n() };
                key_columns.push(vec![db(), s(r.schema.clone()), s(c.name.clone()), db(), s(r.schema.clone()), s(r.name.clone()), s(col.clone()), i4(i as i32 + 1), place]);
            }
            match (c.kind, to) {
                (Kind::ForeignKey, to) => {
                    if let Some(t) = to {
                        used.extend(to_cols.iter().map(|col| vec![db(), s(t.schema.clone()), s(t.name.clone()), s(col.clone()), db(), s(r.schema.clone()), s(c.name.clone())]));
                    }
                    // (the referred table's key or UNIQUE of those columns, when it has one)
                    let unique = to.and_then(|t| match t.key == to_cols {
                        true if !t.key.is_empty() => Some((t.schema.clone(), format!("{}_pkey", t.name))),
                        _ => t.constraints.iter().find(|u| u.kind != Kind::ForeignKey && u.columns == to_cols).map(|u| (t.schema.clone(), u.name.clone())),
                    });
                    referential.push(vec![db(), s(r.schema.clone()), s(c.name.clone()), unique.as_ref().map_or(n(), |_| db()), unique.as_ref().map_or(n(), |u| s(u.0.clone())), unique.map_or(n(), |u| s(u.1)), s("NONE"), s("NO ACTION"), s("NO ACTION")]);
                }
                _ => used.extend(c.columns.iter().map(|col| vec![db(), s(r.schema.clone()), s(r.name.clone()), s(col.clone()), db(), s(r.schema.clone()), s(c.name.clone())])),
            }
        }
    }
    out.push(("table_constraints", table(&[("constraint_catalog", TEXT), ("constraint_schema", TEXT), ("constraint_name", TEXT), ("table_catalog", TEXT), ("table_schema", TEXT), ("table_name", TEXT),
        ("constraint_type", TEXT), ("is_deferrable", TEXT), ("initially_deferred", TEXT), ("enforced", TEXT), ("nulls_distinct", TEXT)], table_constraints)?));
    out.push(("key_column_usage", table(&[("constraint_catalog", TEXT), ("constraint_schema", TEXT), ("constraint_name", TEXT), ("table_catalog", TEXT), ("table_schema", TEXT), ("table_name", TEXT),
        ("column_name", TEXT), ("ordinal_position", INT4), ("position_in_unique_constraint", INT4)], key_columns)?));
    out.push(("routines", table(&[("specific_catalog", TEXT), ("specific_schema", TEXT), ("specific_name", TEXT), ("routine_catalog", TEXT), ("routine_schema", TEXT), ("routine_name", TEXT),
        ("routine_type", TEXT), ("data_type", TEXT), ("routine_body", TEXT), ("routine_definition", TEXT), ("external_language", TEXT), ("is_deterministic", TEXT)],
        l.routines.iter().map(|(oid, schema, name, r)| {
            let procedure = r.kind == crate::routines::Kind::Procedure;
            vec![db(), s(schema.clone()), s(format!("{name}_{oid}")), db(), s(schema.clone()), s(name.clone()), s(if procedure { "PROCEDURE" } else { "FUNCTION" }), r.returns.clone().map_or(n(), s),
                s(if r.language == "python" { "EXTERNAL" } else { "SQL" }), s(r.body.clone()), s(if r.language.is_empty() { "SQL".to_string() } else { r.language.to_uppercase() }), s("NO")]
        }).collect())?));
    out.push(("referential_constraints", table(&[("constraint_catalog", TEXT), ("constraint_schema", TEXT), ("constraint_name", TEXT), ("unique_constraint_catalog", TEXT),
        ("unique_constraint_schema", TEXT), ("unique_constraint_name", TEXT), ("match_option", TEXT), ("update_rule", TEXT), ("delete_rule", TEXT)], referential)?));
    out.push(("constraint_column_usage", table(&[("table_catalog", TEXT), ("table_schema", TEXT), ("table_name", TEXT), ("column_name", TEXT), ("constraint_catalog", TEXT),
        ("constraint_schema", TEXT), ("constraint_name", TEXT)], used)?));
    Ok(out)
}

/// A type as SQL names it (a routine's parameters and result), as a Postgres type's oid.
fn sql_oid(t: &str) -> u32 {
    let t = t.trim().to_lowercase();
    let is = |w: &[&str]| w.iter().any(|w| t == *w || t.starts_with(&format!("{w}(")));
    match () {
        _ if t.ends_with("[]") => 1009,
        _ if is(&["bigint", "int8", "long"]) => 20,
        _ if is(&["int", "integer", "int4"]) => 23,
        _ if is(&["smallint", "int2", "tinyint"]) => 21,
        _ if is(&["double", "double precision", "float8", "float"]) => 701,
        _ if is(&["real", "float4"]) => 700,
        _ if is(&["boolean", "bool"]) => 16,
        _ if is(&["date"]) => 1082,
        _ if t.starts_with("timestamptz") || t.contains("with time zone") => 1184,
        _ if t.starts_with("timestamp") => 1114,
        _ if is(&["decimal", "numeric"]) => 1700,
        _ if is(&["bytea", "binary", "varbinary", "blob", "bytes"]) => 17,
        _ if t.starts_with("table") => 2249,
        _ => 1043,
    }
}

// ---------------------------------------------------------------- functions

type Body = dyn Fn(&[ArrayRef], usize) -> datafusion::error::Result<ArrayRef> + Send + Sync;

/// A function Postgres's catalog queries call, any arguments; `returns` from the arguments' types.
struct PgFn {
    name: &'static str,
    signature: Signature,
    returns: fn(&[DataType]) -> DataType,
    body: Arc<Body>,
}

impl std::fmt::Debug for PgFn {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result { write!(f, "PgFn({})", self.name) }
}
impl PartialEq for PgFn {
    fn eq(&self, o: &Self) -> bool { self.name == o.name }
}
impl Eq for PgFn {}
impl std::hash::Hash for PgFn {
    fn hash<H: std::hash::Hasher>(&self, h: &mut H) { self.name.hash(h) }
}

impl ScalarUDFImpl for PgFn {
    fn name(&self) -> &str { self.name }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, args: &[DataType]) -> datafusion::error::Result<DataType> { Ok((self.returns)(args)) }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> datafusion::error::Result<ColumnarValue> {
        let n = args.number_rows;
        let arrays = args.args.iter().map(|a| a.to_array(n)).collect::<datafusion::error::Result<Vec<_>>>()?;
        Ok(ColumnarValue::Array((self.body)(&arrays, n)?))
    }
}

fn udf(name: &'static str, returns: fn(&[DataType]) -> DataType, body: impl Fn(&[ArrayRef], usize) -> datafusion::error::Result<ArrayRef> + Send + Sync + 'static) -> ScalarUDF {
    let signature = Signature::one_of(vec![TypeSignature::Nullary, TypeSignature::VariadicAny], Volatility::Stable);
    ScalarUDF::new_from_impl(PgFn { name, signature, returns, body: Arc::new(body) })
}

/// An argument as text (NULL where it is), whatever string or number type it came as.
fn text_arg(a: &ArrayRef) -> datafusion::error::Result<Vec<Option<String>>> {
    let c = datafusion::arrow::compute::cast(a, &DataType::Utf8)?;
    Ok(c.as_string::<i32>().iter().map(|v| v.map(str::to_string)).collect())
}

fn int_arg(a: &ArrayRef) -> datafusion::error::Result<Vec<Option<i64>>> {
    let c = datafusion::arrow::compute::cast(a, &DataType::Int64)?;
    Ok(c.as_primitive::<datafusion::arrow::datatypes::Int64Type>().iter().collect())
}

fn texts_out(v: impl IntoIterator<Item = Option<String>>) -> ArrayRef { Arc::new(StringArray::from_iter(v)) }

fn register_functions(ctx: &SessionContext, l: Arc<Lakes>) {
    let to_text = |_: &[DataType]| DataType::Utf8;
    let to_bool = |_: &[DataType]| DataType::Boolean;
    let yes = |n: usize| -> ArrayRef { Arc::new(datafusion::arrow::array::BooleanArray::from(vec![true; n])) };
    let nulls = |n: usize| -> ArrayRef { new_null_array(&DataType::Utf8, n) };
    ctx.register_udf(udf("format_type", to_text, |a, n| {
        let (oids, mods) = (int_arg(&a[0])?, a.get(1).map(int_arg).transpose()?);
        Ok(texts_out((0..n).map(|i| format_type(oids[i]? as u32, mods.as_ref().and_then(|m| m[i]).unwrap_or(-1) as i32))))
    }));
    ctx.register_udf(udf("pg_get_expr", to_text, |a, _| Ok(Arc::new(datafusion::arrow::compute::cast(&a[0], &DataType::Utf8)?))));
    // Visible: on the search path, which is `public` (and a session's temporary schema, and the
    // catalog's own), as in Postgres: `\dt` and SQLAlchemy's default schema list only those.
    let off_path = |schema: &str| !matches!(schema, "public" | TEMP | "pg_catalog");
    let hidden: Arc<std::collections::HashSet<u32>> = Arc::new(l.rels.iter().filter(|r| off_path(&r.schema)).flat_map(|r| [r.oid, oid("i", &format!("{}.{}", r.schema, r.name))])
        .chain(l.routines.iter().filter(|(_, schema, ..)| off_path(schema)).map(|(o, ..)| *o)).collect());
    for name in ["pg_table_is_visible", "pg_function_is_visible"] {
        let hidden = hidden.clone();
        ctx.register_udf(udf(name, to_bool, move |a, n| {
            let oids = int_arg(&a[0])?;
            Ok(Arc::new((0..n).map(|i| oids[i].map(|o| !hidden.contains(&(o as u32)))).collect::<datafusion::arrow::array::BooleanArray>()))
        }));
    }
    for name in ["pg_type_is_visible", "has_table_privilege", "has_schema_privilege", "has_database_privilege",
        "has_column_privilege", "has_any_column_privilege", "has_function_privilege"] {
        ctx.register_udf(udf(name, to_bool, move |_, n| Ok(yes(n))));
    }
    for name in ["pg_is_in_recovery", "pg_is_other_temp_schema"] {
        ctx.register_udf(udf(name, to_bool, move |_, n| Ok(Arc::new(datafusion::arrow::array::BooleanArray::from(vec![false; n.max(1)])))));
    }
    for name in ["obj_description", "col_description", "shobj_description", "pg_get_triggerdef", "pg_get_ruledef", "pg_get_partkeydef", "pg_get_statisticsobjdef"] {
        ctx.register_udf(udf(name, to_text, move |_, n| Ok(nulls(n))));
    }
    let who = l.user.clone();
    ctx.register_udf(udf("pg_get_userbyid", to_text, move |a, n| Ok(texts_out((0..n).map(|i| if a.first().is_some_and(|c| c.is_null(i)) { None } else { Some(who.clone()) })))));
    ctx.register_udf(udf("pg_encoding_to_char", to_text, |_, n| Ok(texts_out((0..n).map(|_| Some("UTF8".to_string()))))));
    ctx.register_udf(udf("pg_backend_pid", |_| DataType::Int32, |_, n| Ok(Arc::new(datafusion::arrow::array::Int32Array::from(vec![std::process::id() as i32; n.max(1)])))));
    ctx.register_udf(udf("current_setting", to_text, |a, n| {
        let names = text_arg(&a[0])?;
        Ok(texts_out((0..n).map(|i| SETTINGS.iter().find(|(k, _)| names[i].as_deref().is_some_and(|x| x.eq_ignore_ascii_case(k))).map(|(_, v)| v.to_string()))))
    }));
    ctx.register_udf(udf("current_schemas", |_| list(DataType::Utf8), |_, n| {
        let one = ScalarValue::List(ScalarValue::new_list_nullable(&[s("pg_catalog"), s("public")], &DataType::Utf8));
        Ok(ScalarValue::iter_to_array(std::iter::repeat_n(one, n.max(1)))?)
    }));
    ctx.register_udf(udf("quote_ident", to_text, |a, n| {
        let v = text_arg(&a[0])?;
        Ok(texts_out((0..n).map(|i| v[i].as_ref().map(|x| if x.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_') { x.clone() } else { format!("\"{}\"", x.replace('"', "\"\"")) }))))
    }));
    ctx.register_udf(udf("pg_size_pretty", to_text, |a, n| {
        let v = int_arg(&a[0])?;
        Ok(texts_out((0..n).map(|i| v[i].map(|b| match b {
            b if b < 10 * 1024 => format!("{b} bytes"),
            b if b < 10 * 1024 * 1024 => format!("{} kB", b / 1024),
            b if b < 10 * 1024 * 1024 * 1024 => format!("{} MB", b / (1024 * 1024)),
            b => format!("{} GB", b / (1024 * 1024 * 1024)),
        }))))
    }));
    // Objects by name and by oid.
    let rels: Arc<Vec<(u32, String, String)>> = Arc::new(l.rels.iter().map(|r| (r.oid, r.schema.clone(), r.name.clone())).chain(CATALOG.iter().map(|(n, o)| (*o, "pg_catalog".to_string(), n.to_string()))).collect());
    let sizes: Arc<BTreeMap<u32, u64>> = Arc::new(l.rels.iter().map(|r| (r.oid, r.bytes)).collect());
    let by_name = move |rels: &[(u32, String, String)], name: &str| -> Option<u32> {
        let name = name.trim_matches('"');
        let (schema, table) = name.rsplit_once('.').map_or((None, name), |(s, t)| (Some(s.trim_matches('"')), t.trim_matches('"')));
        rels.iter().find(|(_, s, n)| n == table && schema.is_none_or(|x| x == s)).map(|(o, ..)| *o)
    };
    for (name, strict) in [("pondra_regclass", true), ("to_regclass", false)] {
        let rels = rels.clone();
        ctx.register_udf(udf(name, |t| if t.first().is_some_and(|t| t.is_integer()) { DataType::Utf8 } else { DataType::UInt32 }, move |a, n| {
            if a[0].data_type().is_integer() {
                let v = int_arg(&a[0])?;
                return Ok(texts_out((0..n).map(|i| v[i].map(|o| rels.iter().find(|r| r.0 as i64 == o).map_or(o.to_string(), |r| if r.1 == "public" || r.1 == "pg_catalog" { r.2.clone() } else { format!("{}.{}", r.1, r.2) })))));
            }
            let v = text_arg(&a[0])?;
            let out = (0..n).map(|i| match v[i].as_deref() {
                None => Ok(None),
                Some(x) => match by_name(&rels, x) {
                    Some(o) => Ok(Some(o)),
                    None if strict => datafusion::common::plan_err!("relation \"{x}\" does not exist"),
                    None => Ok(None),
                },
            }).collect::<datafusion::error::Result<UInt32Array>>()?;
            Ok(Arc::new(out))
        }));
    }
    for (name, strict) in [("pondra_regtype", true), ("to_regtype", false)] {
        ctx.register_udf(udf(name, |t| if t.first().is_some_and(|t| t.is_integer()) { DataType::Utf8 } else { DataType::UInt32 }, move |a, n| {
            if a[0].data_type().is_integer() {
                let v = int_arg(&a[0])?;
                return Ok(texts_out((0..n).map(|i| v[i].and_then(|o| format_type(o as u32, -1)))));
            }
            let v = text_arg(&a[0])?;
            let find = |x: &str| {
                let x = x.trim().trim_start_matches("pg_catalog.").trim_matches('"').to_lowercase();
                TYPES.iter().find(|t| t.1 == x || t.2 == x).map(|t| t.0).or_else(|| arrays().find(|a| a.1 == x || a.2 == x).map(|a| a.0)).or(match x.as_str() {
                    "int" | "integer" => Some(23), "bigint" => Some(20), "smallint" => Some(21), "float" | "double" => Some(701), "string" | "varchar" => Some(1043), "boolean" => Some(16),
                    "timestamp" => Some(1114), "decimal" => Some(1700), "character varying" => Some(1043), _ => None,
                })
            };
            let out = (0..n).map(|i| match v[i].as_deref() {
                None => Ok(None),
                Some(x) => match find(x) {
                    Some(o) => Ok(Some(o)),
                    None if strict => datafusion::common::plan_err!("type \"{x}\" does not exist"),
                    None => Ok(None),
                },
            }).collect::<datafusion::error::Result<UInt32Array>>()?;
            Ok(Arc::new(out))
        }));
    }
    let schemas: Arc<Vec<(u32, String)>> = Arc::new(l.schemas.clone());
    ctx.register_udf(udf("pondra_regnamespace", |t| if t.first().is_some_and(|t| t.is_integer()) { DataType::Utf8 } else { DataType::UInt32 }, move |a, n| {
        if a[0].data_type().is_integer() {
            let v = int_arg(&a[0])?;
            return Ok(texts_out((0..n).map(|i| v[i].and_then(|o| schemas.iter().find(|s| s.0 as i64 == o).map(|s| s.1.clone())))));
        }
        let v = text_arg(&a[0])?;
        Ok(Arc::new((0..n).map(|i| v[i].as_deref().and_then(|x| schemas.iter().find(|s| s.1 == x).map(|s| s.0))).collect::<UInt32Array>()))
    }));
    let procs: Arc<Vec<(u32, String)>> = Arc::new(l.routines.iter().map(|(o, _, n, _)| (*o, n.clone())).collect());
    ctx.register_udf(udf("pondra_regproc", |t| if t.first().is_some_and(|t| t.is_integer()) { DataType::Utf8 } else { DataType::Utf8 }, move |a, n| {
        if a[0].data_type().is_integer() {
            let v = int_arg(&a[0])?;
            return Ok(texts_out((0..n).map(|i| v[i].map(|o| procs.iter().find(|p| p.0 as i64 == o).map_or(o.to_string(), |p| p.1.clone())))));
        }
        Ok(texts_out(text_arg(&a[0])?)) // (a function's name, as `typinput = 'array_in'::regproc` compares it)
    }));
    let views: Arc<BTreeMap<u32, String>> = Arc::new(l.rels.iter().filter_map(|r| Some((r.oid, r.sql.clone()?))).collect());
    let by_name_views = rels.clone();
    ctx.register_udf(udf("pg_get_viewdef", to_text, move |a, n| {
        let oids: Vec<Option<u32>> = match a[0].data_type().is_integer() {
            true => int_arg(&a[0])?.into_iter().map(|o| o.map(|o| o as u32)).collect(),
            false => text_arg(&a[0])?.iter().map(|x| x.as_deref().and_then(|x| by_name(&by_name_views, x))).collect(),
        };
        Ok(texts_out((0..n).map(|i| oids[i].and_then(|o| views.get(&o).cloned()))))
    }));
    let keys: Arc<BTreeMap<u32, (String, Vec<String>)>> = Arc::new(l.rels.iter().filter(|r| !r.key.is_empty()).flat_map(|r| {
        let full = format!("{}.{}", r.schema, r.name);
        [(oid("c", &full), (r.name.clone(), r.key.clone())), (oid("i", &full), (r.name.clone(), r.key.clone()))]
    }).collect());
    let k2 = keys.clone();
    let declared: BTreeMap<u32, String> = l.rels.iter().flat_map(|r| r.constraints.iter().map(move |c| (constraint_oid(r, c), c.def(&|n| crate::objects::ident(n))))).collect();
    ctx.register_udf(udf("pg_get_constraintdef", to_text, move |a, n| {
        let v = int_arg(&a[0])?;
        Ok(texts_out((0..n).map(|i| v[i].and_then(|o| declared.get(&(o as u32)).cloned().or_else(|| k2.get(&(o as u32)).map(|(_, k)| format!("PRIMARY KEY ({})", k.join(", "))))))))
    }));
    let made: BTreeMap<u32, String> = l.indexes.iter().map(|i| (i.oid, i.def.clone())).collect();
    ctx.register_udf(udf("pg_get_indexdef", to_text, move |a, n| {
        let v = int_arg(&a[0])?;
        Ok(texts_out((0..n).map(|i| v[i].and_then(|o| made.get(&(o as u32)).cloned().or_else(|| keys.get(&(o as u32)).map(|(t, k)| format!("CREATE UNIQUE INDEX {t}_pkey ON {t} USING btree ({})", k.join(", "))))))))
    }));
    // Functions and procedures: what they take and give, as `\df` shows them.
    let sigs: Arc<BTreeMap<u32, (String, String)>> = Arc::new(l.routines.iter().map(|(oid, _, _, r)| {
        let args = r.params.iter().map(|p| format!("{} {}", p.name, p.ty.clone().unwrap_or_else(|| "any".into()))).collect::<Vec<_>>().join(", ");
        let result = match r.kind {
            crate::routines::Kind::Procedure => String::new(),
            _ => r.returns.clone().unwrap_or_else(|| "any".into()),
        };
        (*oid, (result, args))
    }).collect());
    for (name, pick) in [("pg_get_function_result", 0), ("pg_get_function_arguments", 1), ("pg_get_function_identity_arguments", 1)] {
        let sigs = sigs.clone();
        ctx.register_udf(udf(name, to_text, move |a, n| {
            let v = int_arg(&a[0])?;
            Ok(texts_out((0..n).map(|i| v[i].and_then(|o| sigs.get(&(o as u32))).map(|(r, a)| if pick == 0 { r.clone() } else { a.clone() }))))
        }));
    }
    for name in ["pg_relation_size", "pg_total_relation_size", "pg_table_size"] {
        let (sizes, rels) = (sizes.clone(), rels.clone());
        ctx.register_udf(udf(name, |_| DataType::Int64, move |a, n| {
            let oids: Vec<Option<u32>> = match a[0].data_type().is_integer() {
                true => int_arg(&a[0])?.into_iter().map(|o| o.map(|o| o as u32)).collect(),
                false => text_arg(&a[0])?.iter().map(|x| x.as_deref().and_then(|x| by_name(&rels, x))).collect(),
            };
            Ok(Arc::new((0..n).map(|i| oids[i].map(|o| *sizes.get(&o).unwrap_or(&0) as i64)).collect::<datafusion::arrow::array::Int64Array>()))
        }));
    }
    ctx.register_udf(udf("pg_indexes_size", |_| DataType::Int64, |_, n| Ok(Arc::new(datafusion::arrow::array::Int64Array::from(vec![0; n])))));
    ctx.register_udf(udf("txid_current", |_| DataType::Int64, |_, n| Ok(Arc::new(datafusion::arrow::array::Int64Array::from(vec![1; n.max(1)])))));
}

/// Postgres's catalog, its functions and its `information_schema` in `ctx`, for a client logged in as `user`.
pub async fn register(ctx: &SessionContext, lake: &Lake, user: &str, sql: &str) -> Result<()> {
    let lower = sql.to_lowercase();
    let columns = ["pg_attribute", "information_schema", "columns"].iter().any(|w| lower.contains(w));
    let l = Arc::new(lakes(lake, user, columns).await?);
    let catalog = ctx.catalog(&crate::ddl::lake_name(lake)).expect("the lake's catalog");
    let pg = Arc::new(MemorySchemaProvider::new());
    for (name, t) in tables(&l)? {
        pg.register_table(name.to_string(), t)?;
    }
    catalog.register_schema("pg_catalog", pg)?;
    let info = Arc::new(MemorySchemaProvider::new());
    for (name, t) in information_schema(&l)? {
        info.register_table(name.to_string(), t)?;
    }
    catalog.register_schema(PG_INFO, info)?;
    register_functions(ctx, l);
    Ok(())
}

const PG_INFO: &str = "pg_information_schema"; // (DataFusion keeps `information_schema` to itself)
const INFO_TABLES: &[&str] = &["schemata", "tables", "columns", "views", "table_constraints", "key_column_usage", "routines", "referential_constraints", "constraint_column_usage"];

// ---------------------------------------------------------------- Postgres's SQL, said DataFusion's way

/// `sql` with what only Postgres says said so DataFusion takes it: catalog tables by their
/// schema, `OPERATOR(pg_catalog.~)` as `~`, `::regclass` and its kin as functions, `COLLATE`
/// dropped, `current_user` as the user. Unchanged if it doesn't parse as Postgres's.
pub fn rewrite(sql: &str, user: &str) -> String {
    use datafusion::sql::sqlparser::{dialect::PostgreSqlDialect, parser::Parser};
    let Ok(mut stmts) = Parser::parse_sql(&PostgreSqlDialect {}, sql) else { return sql.to_string() };
    let mut r = Rewriter { user: if user.is_empty() { "pondra" } else { user } };
    for s in stmts.iter_mut() {
        let _ = s.visit(&mut r);
    }
    stmts.iter().map(crate::routines::sql).collect::<Vec<_>>().join("; ")
}

struct Rewriter<'a> {
    user: &'a str,
}

fn is_pg_table(name: &str) -> bool { CATALOG.iter().any(|(n, _)| *n == name) }

/// The catalog's true/false columns (`indisprimary`, `attnotnull`…), which clients compare to
/// `'t'` and `'f'` as Postgres lets them.
/// A column that names a function (Postgres's `regproc`), kept here as its name.
fn regproc(e: &ast::Expr) -> bool {
    let name = match e {
        ast::Expr::Identifier(i) => i.value.to_lowercase(),
        ast::Expr::CompoundIdentifier(p) => p.last().map(|i| i.value.to_lowercase()).unwrap_or_default(),
        _ => return false,
    };
    matches!(name.as_str(), "typinput" | "typoutput" | "typreceive" | "typsend" | "typmodin" | "typmodout" | "typanalyze" | "aggfnoid" | "oprcode")
}

fn boolean(e: &ast::Expr) -> bool {
    static NAMES: std::sync::LazyLock<std::collections::HashSet<String>> = std::sync::LazyLock::new(|| {
        let empty = Lakes { database: String::new(), databases: vec![], user: String::new(), schemas: vec![], rels: vec![], routines: vec![], indexes: vec![] };
        tables(&empty).unwrap_or_default().iter().flat_map(|(_, t)| datafusion::datasource::TableProvider::schema(t.as_ref()).fields().iter().filter(|f| *f.data_type() == DataType::Boolean).map(|f| f.name().clone()).collect::<Vec<_>>()).collect()
    });
    let name = match e {
        ast::Expr::Identifier(i) => i.value.to_lowercase(),
        ast::Expr::CompoundIdentifier(p) => p.last().map(|i| i.value.to_lowercase()).unwrap_or_default(),
        _ => return false,
    };
    NAMES.contains(&name)
}

/// A column that is an int2vector or oidvector in Postgres (its subscripts count from 0).
fn vector(e: &ast::Expr) -> bool {
    let name = match e {
        ast::Expr::Identifier(i) => i.value.to_lowercase(),
        ast::Expr::CompoundIdentifier(p) => p.last().map(|i| i.value.to_lowercase()).unwrap_or_default(),
        _ => return false,
    };
    ["indkey", "indclass", "indcollation", "indoption", "proargtypes", "stxkeys"].contains(&name.as_str())
}

/// Catalog tables that are always empty here: a subquery that joins one gives no row.
const EMPTY: &[&str] = &["pg_publication_namespace", "pg_publication_rel", "pg_description", "pg_shdescription", "pg_depend", "pg_rewrite", "pg_inherits", "pg_enum", "pg_collation", "pg_extension", "pg_range", "pg_trigger",
    "pg_sequence", "pg_foreign_table", "pg_partitioned_table", "pg_policy", "pg_statistic_ext", "pg_publication", "pg_event_trigger", "pg_cast", "pg_opclass"];

/// A scalar subquery DataFusion can't decorrelate as Postgres writes it, said another way: NULL if
/// it joins an always-empty catalog table; its conditions on the outer row alone taken out of it
/// (`(SELECT … WHERE d.x = a.x AND a.flag)` → `CASE WHEN a.flag THEN (SELECT … WHERE d.x = a.x) END`).
fn settle(q: &mut ast::Query) -> Option<ast::Expr> {
    let ast::SetExpr::Select(sel) = q.body.as_mut() else { return None };
    let mut inner: Vec<String> = vec![];
    let mut empty = false;
    for t in &sel.from {
        for f in std::iter::once(&t.relation).chain(t.joins.iter().filter(|j| matches!(j.join_operator, ast::JoinOperator::Inner(_) | ast::JoinOperator::Join(_))).map(|j| &j.relation)) {
            if let ast::TableFactor::Table { name, alias, .. } = f {
                let last = name.0.last().and_then(|p| p.as_ident()).map(|i| i.value.to_lowercase()).unwrap_or_default();
                empty |= EMPTY.contains(&last.as_str());
                inner.push(alias.as_ref().map_or(last, |a| a.name.value.to_lowercase()));
            }
        }
        for j in &t.joins {
            if let ast::TableFactor::Table { name, alias, .. } = &j.relation {
                let last = name.0.last().and_then(|p| p.as_ident()).map(|i| i.value.to_lowercase()).unwrap_or_default();
                inner.push(alias.as_ref().map_or(last, |a| a.name.value.to_lowercase()));
            }
        }
    }
    if empty {
        return expr("NULL");
    }
    let selection = sel.selection.take()?;
    let mut conds = vec![];
    split_and(selection, &mut conds);
    let outer_only = |c: &ast::Expr| {
        let mut qualifiers = vec![];
        let _ = ast::visit_expressions(c, |x| {
            if let ast::Expr::CompoundIdentifier(parts) = x {
                qualifiers.push(parts[parts.len().saturating_sub(2)].value.to_lowercase()); // (its table: `a.x`, `pg_catalog.pg_attrdef.x`)
            }
            ControlFlow::<()>::Continue(())
        });
        !qualifiers.is_empty() && qualifiers.iter().all(|q| !inner.contains(q))
    };
    let (outer, rest): (Vec<_>, Vec<_>) = conds.into_iter().partition(outer_only);
    sel.selection = rest.into_iter().reduce(|a, b| ast::Expr::BinaryOp { left: Box::new(a), op: ast::BinaryOperator::And, right: Box::new(b) });
    if outer.is_empty() {
        return None;
    }
    let when = outer.iter().map(|c| format!("({c})")).collect::<Vec<_>>().join(" AND ");
    if let ast::SetExpr::Select(sel) = q.body.as_mut() {
        if let (1, Some(ast::SelectItem::UnnamedExpr(item) | ast::SelectItem::ExprWithAlias { expr: item, .. })) = (sel.projection.len(), sel.projection.first_mut()) {
            if !aggregated(item) {
                *item = expr(&format!("max({item})"))?;
            }
        }
    }
    expr(&format!("CASE WHEN {when} THEN ({q}) END"))
}

fn split_and(e: ast::Expr, out: &mut Vec<ast::Expr>) {
    match e {
        ast::Expr::BinaryOp { left, op: ast::BinaryOperator::And, right } => {
            split_and(*left, out);
            split_and(*right, out);
        }
        ast::Expr::Nested(inner) if matches!(*inner, ast::Expr::BinaryOp { op: ast::BinaryOperator::And, .. }) => split_and(*inner, out),
        e => out.push(e),
    }
}

/// A query over nothing but always-empty catalog tables (psql's policies, publications…),
/// whose SQL DataFusion may not plan: its columns' names, for an empty answer.
pub fn empty_answer(sql: &str) -> Option<Vec<String>> {
    use datafusion::sql::sqlparser::{dialect::PostgreSqlDialect, parser::Parser};
    let stmts = Parser::parse_sql(&PostgreSqlDialect {}, sql).ok()?;
    let [ast::Statement::Query(q)] = &stmts[..] else { return None };
    fn over_empty(body: &ast::SetExpr) -> Option<&ast::Select> {
        let empty = |f: &ast::TableFactor| matches!(f, ast::TableFactor::Table { name, .. } if name.0.last().and_then(|p| p.as_ident()).is_some_and(|i| EMPTY.contains(&i.value.to_lowercase().as_str())));
        match body {
            ast::SetExpr::Select(sel) => {
                let first = sel.from.first()?;
                (empty(&first.relation) && first.joins.iter().all(|j| matches!(j.join_operator, ast::JoinOperator::Inner(_) | ast::JoinOperator::Join(_)) || empty(&j.relation))).then_some(sel.as_ref())
            }
            ast::SetExpr::SetOperation { left, right, .. } => over_empty(right).and(over_empty(left)),
            ast::SetExpr::Query(q) => over_empty(&q.body),
            _ => None,
        }
    }
    let sel = over_empty(&q.body)?;
    Some(sel.projection.iter().map(|p| match p {
        ast::SelectItem::ExprWithAlias { alias, .. } => alias.value.clone(),
        ast::SelectItem::UnnamedExpr(ast::Expr::Identifier(i)) => i.value.clone(),
        ast::SelectItem::UnnamedExpr(ast::Expr::CompoundIdentifier(p)) => p.last().map_or("?column?".into(), |i| i.value.clone()),
        _ => "?column?".into(),
    }).collect())
}

/// Does the expression aggregate (`count(*)`, `max(x)`, …)?
fn aggregated(e: &ast::Expr) -> bool {
    const AGGREGATES: &[&str] = &["count", "max", "min", "sum", "avg", "array_agg", "string_agg", "bool_or", "bool_and", "every", "first_value", "last_value"];
    let mut found = false;
    let _ = ast::visit_expressions(e, |x| {
        if let ast::Expr::Function(f) = x {
            found |= AGGREGATES.contains(&f.name.to_string().to_lowercase().trim_start_matches("pg_catalog.")) && f.over.is_none();
        }
        ControlFlow::<()>::Continue(())
    });
    found
}

fn expr(sql: &str) -> Option<ast::Expr> {
    use datafusion::sql::sqlparser::{dialect::PostgreSqlDialect, parser::Parser};
    Parser::new(&PostgreSqlDialect {}).try_with_sql(sql).ok()?.parse_expr().ok()
}

impl VisitorMut for Rewriter<'_> {
    type Break = ();

    fn pre_visit_relation(&mut self, name: &mut ast::ObjectName) -> ControlFlow<()> {
        let parts: Vec<String> = name.0.iter().filter_map(|p| p.as_ident().map(|i| i.value.to_lowercase())).collect();
        let fixed = match parts.iter().map(String::as_str).collect::<Vec<_>>()[..] {
            [t] if is_pg_table(t) => Some(vec!["pg_catalog".to_string(), t.to_string()]),
            [_, "pg_catalog", t] if is_pg_table(t) => Some(vec!["pg_catalog".to_string(), t.to_string()]),
            ["information_schema", t] | [_, "information_schema", t] if INFO_TABLES.contains(&t) => Some(vec![PG_INFO.to_string(), t.to_string()]),
            _ => None,
        };
        if let Some(parts) = fixed {
            *name = ast::ObjectName::from(parts.into_iter().map(ast::Ident::new).collect::<Vec<_>>());
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, e: &mut ast::Expr) -> ControlFlow<()> {
        let ast::Expr::CompoundFieldAccess { root, access_chain } = e else { return ControlFlow::Continue(()) };
        // pgjdbc's primary keys: `(information_schema._pg_expandarray(i.indkey)).n` and `.x` — each
        // element's place and value — as zipped `unnest`s; `(result.keys).x` as the element itself.
        // An int2vector (`i.indkey[0]`) counts from 0: DataFusion's lists from 1.
        for k in 0..access_chain.len() {
            let named = match k {
                0 => vector(root),
                _ => matches!(&access_chain[k - 1], ast::AccessExpr::Dot(x) if vector(x)),
            };
            if let (true, ast::AccessExpr::Subscript(ast::Subscript::Index { index })) = (named, &mut access_chain[k]) {
                *index = ast::Expr::Nested(Box::new(ast::Expr::BinaryOp { left: Box::new(index.clone()), op: ast::BinaryOperator::Plus, right: Box::new(expr("1").expect("a number")) }));
                return ControlFlow::Continue(());
            }
        }
        let field = match access_chain.as_slice() {
            [ast::AccessExpr::Dot(ast::Expr::Identifier(f))] => f.value.to_lowercase(),
            _ => return ControlFlow::Continue(()),
        };
        let inner = match root.as_ref() {
            ast::Expr::Nested(x) => x.as_ref(),
            x => x,
        };
        let replaced = match inner {
            ast::Expr::Function(f) if f.name.to_string().to_lowercase().ends_with("_pg_expandarray") => {
                let arg = match &f.args {
                    ast::FunctionArguments::List(l) => l.args.first().map(|a| a.to_string()),
                    _ => None,
                };
                match (field.as_str(), arg) {
                    ("n", Some(a)) => expr(&format!("unnest(generate_series(1, cardinality({a})))")),
                    ("x", Some(a)) => expr(&format!("unnest({a})")),
                    _ => None,
                }
            }
            x @ (ast::Expr::CompoundIdentifier(_) | ast::Expr::Identifier(_)) if field == "x" => Some(x.clone()),
            _ => None,
        };
        if let Some(r) = replaced {
            *e = r;
        }
        ControlFlow::Continue(())
    }

    fn post_visit_expr(&mut self, e: &mut ast::Expr) -> ControlFlow<()> {
        let replaced = match e {
            ast::Expr::Collate { expr, .. } => Some((**expr).clone()),
            // `typreceive != 0` (ADBC's): a function (`regproc`) column against 0, Postgres's "none",
            // which as text is `-`.
            // `pg_proc.oid = typ.typreceive` (Npgsql's): joined by the function's name, which is
            // what the column holds.
            ast::Expr::BinaryOp { left, op: ast::BinaryOperator::Eq | ast::BinaryOperator::NotEq, right } if regproc(left) || regproc(right) => {
                for side in [left, right] {
                    match side.as_mut() {
                        ast::Expr::Value(v) if matches!(&v.value, ast::Value::Number(n, _) if n == "0") => v.value = ast::Value::SingleQuotedString("-".into()),
                        ast::Expr::CompoundIdentifier(p) if p.last().is_some_and(|i| i.value.eq_ignore_ascii_case("oid")) => *p.last_mut().expect("a last part") = ast::Ident::new("proname"),
                        _ => {}
                    }
                }
                None
            }
            // `i.indisprimary = 't'`: the text as the true or false it means.
            ast::Expr::BinaryOp { left, op: ast::BinaryOperator::Eq | ast::BinaryOperator::NotEq, right } if boolean(left) || boolean(right) => {
                for side in [left, right] {
                    if let ast::Expr::Value(v) = side.as_mut() {
                        if let ast::Value::SingleQuotedString(t) = &v.value {
                            let b = matches!(t.to_lowercase().as_str(), "t" | "true" | "y" | "yes" | "on" | "1");
                            v.value = ast::Value::Boolean(b);
                        }
                    }
                }
                None
            }
            // `LIKE … ESCAPE '/'` (SQLAlchemy's): the pattern's escapes as backslashes, DataFusion's.
            ast::Expr::Like { pattern, escape_char, .. } | ast::Expr::ILike { pattern, escape_char, .. } => {
                if let Some(c) = escape_char.as_ref().and_then(|v| match &v.value {
                    ast::Value::SingleQuotedString(c) if c.chars().count() == 1 && c != "\\" => c.chars().next(),
                    _ => None,
                }) {
                    let _ = ast::visit_expressions_mut(pattern.as_mut(), |x| {
                        if let ast::Expr::Value(v) = x {
                            if let ast::Value::SingleQuotedString(t) = &mut v.value {
                                let mut out = String::new();
                                let mut chars = t.chars();
                                while let Some(ch) = chars.next() {
                                    match ch {
                                        _ if ch == c => out.extend(['\\'].into_iter().chain(chars.next())),
                                        '\\' => out.push_str("\\\\"),
                                        ch => out.push(ch),
                                    }
                                }
                                *t = out;
                            }
                        }
                        ControlFlow::<()>::Continue(())
                    });
                    *escape_char = None;
                }
                None
            }
            ast::Expr::BinaryOp { op: ast::BinaryOperator::PGCustomBinaryOperator(parts), .. } => {
                let op = match parts.last().map(String::as_str) {
                    Some("~") => ast::BinaryOperator::PGRegexMatch,
                    Some("~*") => ast::BinaryOperator::PGRegexIMatch,
                    Some("!~") => ast::BinaryOperator::PGRegexNotMatch,
                    Some("!~*") => ast::BinaryOperator::PGRegexNotIMatch,
                    Some("=") => ast::BinaryOperator::Eq,
                    Some("<>") => ast::BinaryOperator::NotEq,
                    Some("~~") => ast::BinaryOperator::PGLikeMatch,
                    _ => return ControlFlow::Continue(()),
                };
                if let ast::Expr::BinaryOp { op: o, .. } = e {
                    *o = op;
                }
                None
            }
            ast::Expr::Cast { expr: inner, data_type, .. } => {
                let t = data_type.to_string().to_lowercase();
                let t = t.trim_start_matches("pg_catalog.").trim_matches('"');
                match t {
                    "regclass" | "regtype" | "regproc" | "regprocedure" | "regnamespace" => expr(&format!("pondra_{}({inner})", if t == "regprocedure" { "regproc" } else { t })),
                    "oid" | "regrole" | "xid" | "cid" => expr(&format!("CAST({inner} AS INT UNSIGNED)")),
                    "name" | "char" | "bpchar" | "int2vector" | "oidvector" | "aclitem[]" | "pg_node_tree" | "text" | "varchar" => expr(&format!("CAST({inner} AS TEXT)")),
                    "int2" => expr(&format!("CAST({inner} AS SMALLINT)")),
                    "int4" | "int" => expr(&format!("CAST({inner} AS INT)")),
                    "int8" => expr(&format!("CAST({inner} AS BIGINT)")),
                    "bool" => expr(&format!("CAST({inner} AS BOOLEAN)")),
                    "float4" => expr(&format!("CAST({inner} AS REAL)")),
                    "float8" => expr(&format!("CAST({inner} AS DOUBLE)")),
                    "text[]" | "name[]" | "_text" => expr(&format!("CAST({inner} AS VARCHAR[])")),
                    _ => None,
                }
            }
            // `ARRAY(SELECT …)`: its rows as a list. Over `unnest` of a column (psql reads options
            // that way), none: Pondra's objects have no options.
            ast::Expr::Function(f) if f.name.to_string().eq_ignore_ascii_case("array") && matches!(f.args, ast::FunctionArguments::Subquery(_)) => {
                let ast::FunctionArguments::Subquery(q) = &f.args else { unreachable!() };
                let text = q.to_string();
                match (text.to_lowercase().contains("unnest("), q.body.as_ref()) {
                    (true, _) => expr("CAST(NULL AS VARCHAR[])"),
                    (false, ast::SetExpr::Select(sel)) if sel.projection.len() == 1 => {
                        let mut q = q.clone();
                        if let ast::SetExpr::Select(sel) = q.body.as_mut() {
                            if let ast::SelectItem::UnnamedExpr(e) | ast::SelectItem::ExprWithAlias { expr: e, .. } = &sel.projection[0] {
                                sel.projection[0] = ast::SelectItem::ExprWithAlias { expr: e.clone(), alias: ast::Ident::new("__v") };
                            }
                        }
                        expr(&format!("(SELECT array_agg(__a.__v) FROM ({q}) AS __a)"))
                    }
                    _ => None,
                }
            }
            ast::Expr::Function(f) if f.name.to_string().to_lowercase().ends_with("_pg_expandarray") => match &f.args {
                ast::FunctionArguments::List(l) => l.args.first().and_then(|a| expr(&format!("unnest({a})"))),
                _ => None,
            },
            ast::Expr::Function(f) => {
                let parts: Vec<String> = f.name.0.iter().filter_map(|p| p.as_ident().map(|i| i.value.to_lowercase())).collect();
                if parts.len() == 2 && parts[0] == "pg_catalog" {
                    f.name = ast::ObjectName::from(vec![ast::Ident::new(parts[1].clone())]);
                }
                let bare = matches!(f.args, ast::FunctionArguments::None);
                let args = match &f.args {
                    ast::FunctionArguments::List(l) => l.args.iter().map(|a| a.to_string()).collect::<Vec<_>>(),
                    _ => vec![],
                };
                match parts.last().map(String::as_str) {
                    Some("current_user" | "session_user" | "current_role") if bare => Some(ast::Expr::Value(ast::Value::SingleQuotedString(self.user.to_string()).into())),
                    // (beside `unnest(a)` in a select list, each element's place: zipped, as Postgres does)
                    Some("generate_subscripts") if !args.is_empty() => expr(&format!("unnest(generate_series(1, cardinality({})))", args[0])),
                    Some("current_schema") => Some(ast::Expr::Value(ast::Value::SingleQuotedString("public".into()).into())),
                    Some("current_database" | "current_catalog") => None,
                    _ => None,
                }
            }
            // A scalar subquery Postgres takes as one row (psql's, over the catalog): DataFusion takes
            // it once it's aggregated, and here it holds one row or none.
            ast::Expr::Subquery(q) => {
                if let Some(replacement) = settle(q) {
                    *e = replacement;
                    return ControlFlow::Continue(());
                }
                if let ast::SetExpr::Select(sel) = q.body.as_mut() {
                    let grouped = !matches!(&sel.group_by, ast::GroupByExpr::Expressions(g, _) if g.is_empty());
                    if sel.projection.len() == 1 && !grouped && q.limit_clause.is_none() {
                        if let ast::SelectItem::UnnamedExpr(item) | ast::SelectItem::ExprWithAlias { expr: item, .. } = &mut sel.projection[0] {
                            if !aggregated(item) {
                                if let Some(m) = expr(&format!("max({item})")) {
                                    *item = m;
                                }
                            }
                        }
                    }
                }
                None
            }
            ast::Expr::Value(v) => match &v.value {
                ast::Value::EscapedStringLiteral(s) => Some(ast::Expr::Value(ast::Value::SingleQuotedString(s.clone()).into())),
                _ => None,
            },
            _ => None,
        };
        if let Some(r) = replaced {
            *e = r;
        }
        ControlFlow::Continue(())
    }
}
