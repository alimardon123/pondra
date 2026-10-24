//! Postgres's catalog as clients read it (ADR-030): psql's `\d` commands, dbt, SQLAlchemy, JDBC's
//! and ODBC's metadata calls, BI tools. Built from the lake's own catalog for each query that
//! reads it — tables, views, materialized views, their columns with Postgres's types, keys,
//! defaults, functions — with the functions those queries call, and their SQL rewritten where
//! it says things only Postgres says (`OPERATOR(pg_catalog.~)`, `::regclass`, `COLLATE`).
//!
//! Every object keeps its oid from one query to the next (a hash of its name), so a client that
//! looks a table up and then asks about its columns by oid gets the same table.
use crate::pg_functions::{int_arg, text_arg, texts_out};
use crate::store::{Lake, TableMeta};
use crate::users::On;
use anyhow::Result;
use datafusion::arrow::array::{new_null_array, Array, ArrayRef, RecordBatch, UInt32Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::catalog::{MemTable, MemorySchemaProvider, SchemaProvider};
use datafusion::common::ScalarValue;
use datafusion::logical_expr::{ScalarUDF, Signature, TypeSignature, Volatility};
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{self, VisitMut, VisitorMut};
use std::collections::{BTreeMap, BTreeSet};
use std::ops::ControlFlow;
use std::sync::Arc;

tokio::task_local! {
    /// A request that speaks Postgres over HTTP (`POST /sql?catalog=postgres`: the DB-API and SQLAlchemy's
    /// dialect, in process too): its queries that read Postgres's catalog are answered as the port answers them.
    static POSTGRES: ();
}

/// `f`, speaking Postgres's catalog when `yes`.
pub async fn speaking<F: std::future::Future>(yes: bool, f: F) -> F::Output {
    match yes {
        true => POSTGRES.scope((), f).await,
        false => f.await,
    }
}

/// Does this request speak Postgres, and this query read its catalog? (Any other query, and every query of
/// a request that doesn't, reads as before: DataFusion's own `information_schema` over HTTP.)
pub fn asked(sql: &str) -> bool { POSTGRES.try_with(|_| ()).is_ok() && wanted(sql) }

/// A query over Postgres's catalog, as the Postgres port answers it: the catalog's tables and functions in
/// its session, `user` as `current_user`, and over tables that are always empty, no rows with the columns
/// asked for (whatever DataFusion made of it). `sql` is as `rewrite` left it.
pub async fn answer(lake: &Lake, sql: &str, user: &str) -> Result<Vec<RecordBatch>> {
    let ctx = crate::query::session(lake, sql, "").await?;
    register(&ctx, lake, user, sql).await?;
    match async { crate::engine::collect(crate::engine::sql(&ctx, sql).await?).await }.await { // (no rows: its columns as they run)
        Ok(batches) => Ok(batches),
        Err(e) => match empty_answer(sql) {
            Some(names) => Ok(vec![RecordBatch::new_empty(Arc::new(Schema::new(names.into_iter().map(|n| Field::new(n, DataType::Utf8, true)).collect::<Vec<_>>())))]),
            None => Err(e),
        },
    }
}

/// `answer` for a request over HTTP that speaks Postgres (`asked`): its SQL rewritten as the port rewrites
/// it, its principal as `current_user`, the session's schema as `current_schema()`.
pub async fn over_http(lake: &Lake, sql: &str) -> Result<Vec<RecordBatch>> {
    let user = crate::auth::current().map(|p| p.name).filter(|n| !n.is_empty()).unwrap_or_else(|| "pondra".into());
    let schema = crate::path::current(lake).map_or_else(|| "public".to_string(), |p| p.schema().to_string());
    answer(lake, &rewrite(sql, &user, &schema), &user).await
}

/// Does this query read Postgres's catalog, or say something only Postgres says?
pub fn wanted(sql: &str) -> bool {
    let s = sql.to_lowercase();
    ["pg_", "information_schema", "::reg", "operator(", "format_type(", "current_user", "session_user", "current_schemas", "to_regtype", "to_regclass", "collate", "obj_description", "col_description", "current_setting", "has_table_privilege", "has_schema_privilege"]
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
    all.extend(["enum_in", "enum_out", "enum_recv", "enum_send"].map(String::from)); // (an enum type's, `types.rs`)
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
        // (Pondra's one string type has no length, which is Postgres's text; pg_type keeps varchar for casts)
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => (25, -1),
        _ => (1043, -1), // (what has no Postgres equal: shown as a varchar)
    }
}

/// uuid's oid: a uuid column's type (`types.rs` holds it as text).
pub const UUID: u32 = 2950;

/// json's and jsonb's oids: a column of either type (`types.rs` holds it as text).
pub const JSON: u32 = 114;
pub const JSONB: u32 = 3802;

/// `pg_typeof(x)`: the name of the Postgres type a value of this Arrow type is sent as.
pub fn type_of(t: &DataType) -> String {
    match t {
        DataType::Null => "unknown".into(), // (an untyped NULL, as Postgres says of one)
        t => format_type(pg_of(t).0, -1).unwrap_or_else(|| "text".into()),
    }
}

/// `format_type(oid, typmod)`.
fn format_type(oid: u32, typmod: i32) -> Option<String> {
    let name = TYPES.iter().find(|t| t.0 == oid).map(|t| t.2.to_string()).or_else(|| arrays().find(|a| a.0 == oid).map(|a| a.2))?;
    Some(match (oid, typmod) {
        (1700, m) if m >= 4 => format!("numeric({},{})", (m - 4) >> 16, (m - 4) & 0xffff),
        (1043, m) if m >= 4 => format!("character varying({})", m - 4), // (a VARCHAR(n) column: n + 4, as Postgres keeps it)
        (1042, m) if m >= 4 => format!("character({})", m - 4),
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
    checks: Vec<(String, String)>, // CHECK constraints: (name, condition)
    typed: BTreeMap<String, (u32, i32)>, // columns of a type their Arrow type doesn't say (SQL's names), with its typmod: an enum's type (an inline ENUM('a') is text), uuid, VARCHAR(n)
    comment: Option<String>, // COMMENT ON the relation (`cm/relation/…`, `objects.rs`)
    comments: BTreeMap<String, String>, // COMMENT ON its columns, by SQL name (`cm/column/…` by the stored name)
    rows: u64,
    bytes: u64,
}

impl Rel {
    /// A column's type as Postgres names it: (oid, typmod, length, -1 when it varies). An enum
    /// column's own type, a uuid column's, a VARCHAR(n)'s with its length; the type `pg_of` gives every other.
    fn column_type(&self, name: &str, t: &DataType) -> (u32, i32, i16) {
        match self.typed.get(name) {
            Some(&(oid, typmod)) => (oid, typmod, TYPES.iter().find(|x| x.0 == oid).map_or(ENUM_LENGTH, |x| x.3)),
            None => {
                let (typ, typmod) = pg_of(t);
                (typ, typmod, TYPES.iter().find(|x| x.0 == typ).map_or(-1, |x| x.3))
            }
        }
    }
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

/// A sequence of the lake (`sq/`): its oid, its lake name (what an identity column names), its
/// schema and name, and its options as Postgres counts them.
struct SeqRel {
    oid: u32,
    lake_name: String,
    schema: String,
    name: String,
    shape: crate::seq::Shape,
}

struct Lakes {
    indexes: Vec<PgIndex>,
    database: String,
    databases: Vec<String>,
    user: String,
    schemas: Vec<(u32, String)>,
    rels: Vec<Rel>,
    sequences: Vec<SeqRel>,
    types: Vec<EnumType>,
    routines: Vec<(u32, String, String, crate::expander::Routine)>, // oid, schema, name
    visible: Vec<String>, // (this lake's schemas on the session's search path: `path.rs`; public when none is set)
    members: Vec<Member>, // the users and roles the caller may see (`pg_roles`, `pg_auth_members`, the grants)
}

/// A user or role of the lake, as far as the catalog reads it.
struct Member {
    name: String,
    login: bool,
    superuser: bool,
    roles: Vec<String>, // what it is a member of
    grants: Vec<crate::users::Grant>,
}

/// The name of the bootstrap role (oid 10, the first row of `pg_roles`); a lake user of this name is that row.
const BOOTSTRAP: &str = "pondra";

/// A role's oid: a hash of its name, or the bootstrap role's 10.
fn role_oid(name: &str) -> u32 { if name == BOOTSTRAP { 10 } else { oid("u", name) } }

/// The users and roles of the lake the caller may see. A limited caller sees what `users::principal`
/// lets it name: itself, the roles it is a member of (and theirs), and public.
fn members_of(all: &[(String, crate::users::User)], user: &str) -> Vec<Member> {
    let visible = crate::auth::limited().map(|_| {
        let mut seen = BTreeSet::new();
        let mut todo = vec![user.to_string(), "public".to_string()];
        while let Some(n) = todo.pop() {
            if seen.insert(n.clone()) {
                todo.extend(all.iter().find(|(k, _)| &k[2..] == n).into_iter().flat_map(|(_, u)| u.roles.clone()));
            }
        }
        seen
    });
    all.iter().filter(|(k, _)| visible.as_ref().is_none_or(|v| v.contains(&k[2..]))).map(|(k, u)| Member {
        name: k[2..].to_string(), login: u.login, superuser: u.superuser, roles: u.roles.clone(), grants: u.grants.clone(),
    }).collect()
}

impl Lakes {
    /// Is a schema off the search path? Its objects are shown qualified and hidden from the
    /// `pg_*_is_visible` functions. The search path is the session's (`visible`), and the session's
    /// temporary schema and `pg_catalog` are always on it.
    fn off_path(&self, schema: &str) -> bool { !self.visible.iter().any(|v| v == schema) && schema != TEMP && schema != "pg_catalog" }

    /// A name as `format_type` says it: qualified when its schema is off the search path.
    fn shown_name(&self, schema: &str, name: &str) -> String {
        match self.off_path(schema) {
            true => format!("{schema}.{name}"),
            false => name.to_string(),
        }
    }

    fn enum_type(&self, oid: u32) -> Option<&EnumType> { self.types.iter().find(|t| t.oid == oid) }

    /// `format_type(oid, typmod)`: an enum type's name, or its array's (`mood[]`), else what
    /// `format_type` says of the built-in types.
    fn format_type(&self, oid: u32, typmod: i32) -> Option<String> {
        if let Some(t) = self.enum_type(oid) {
            return Some(self.shown_name(&t.schema, &t.name));
        }
        if let Some(t) = self.types.iter().find(|t| t.array == oid) {
            return Some(format!("{}[]", self.shown_name(&t.schema, &t.name)));
        }
        format_type(oid, typmod)
    }
}

/// A stable oid for an object: a hash of its kind and name, above Postgres's own (16384).
fn oid(kind: &str, name: &str) -> u32 {
    let h = format!("{kind}:{name}").bytes().fold(0x811c_9dc5u32, |h, b| (h ^ b as u32).wrapping_mul(0x0100_0193));
    16384 + h % 2_000_000_000
}

const TEMP: &str = "pg_temp_1"; // (a session's temporary tables' schema)

/// An enum column's type, as Postgres's catalog has it (`types.rs`): 4 bytes, passed by value.
const ENUM_LENGTH: i16 = 4;

/// An enum type of the lake: its oid, its array's, its schema and name, and its labels in order.
struct EnumType {
    oid: u32,
    array: u32,
    schema: String,
    name: String,
    labels: Vec<String>,
}

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
    ("pg_authid", 1260), ("pg_shdescription", 2396), ("pg_publication_namespace", 6237), ("pg_publication_rel", 6106), ("pg_language", 2612), ("pg_opclass", 2616), ("pg_event_trigger", 3466), ("pg_cast", 2605), ("pg_indexes", 12006),
    ("pg_stat_user_tables", 12007), ("pg_stat_all_tables", 12008), ("pg_sequences", 12009), ("pg_policies", 12010), ("pg_available_extensions", 12011), ("pg_default_acl", 826),
    ("pg_db_role_setting", 2964), ("pg_aggregate", 2600), ("pg_conversion", 2607), ("pg_foreign_data_wrapper", 2328), ("pg_foreign_server", 1417), ("pg_auth_members", 1261)];

/// The rows each table holds in the log and not yet in its files. Tiering takes them within seconds,
/// but until then a table filled a moment ago would read as empty to a tool that sizes tables by
/// `n_live_tup` or `reltuples` (Metabase's sync leaves such a table's estimate out). An estimate:
/// past `FAR` segments of log (tiering stalled) it counts the files alone.
async fn log_rows(lake: &Lake, metas: &[(String, TableMeta)]) -> Result<BTreeMap<String, u64>> {
    const FAR: u64 = 100_000;
    let mut out = BTreeMap::new();
    let shown = metas.iter().filter(|(k, _)| !crate::sys::hidden(&k[2..]));
    let (Some(from), to) = (shown.clone().map(|(_, m)| m.tiered).min(), lake.visible()) else { return Ok(out) };
    if from >= to || to - from > FAR {
        return Ok(out);
    }
    let segs = lake.cat.scan::<crate::store::Segment>(&crate::store::seg_key(from + 1), &crate::store::seg_key(to + 1)).await?;
    let numbered: Vec<(u64, &crate::store::Segment)> = segs.iter().filter_map(|(k, s)| Some((k[2..].parse().ok()?, s))).collect();
    for (k, m) in shown {
        let n: u64 = numbered.iter().filter(|(at, _)| *at > m.tiered).map(|(_, s)| s.rows_of(&k[2..])).sum();
        if n > 0 {
            out.insert(k[2..].to_string(), n);
        }
    }
    Ok(out)
}

/// A table's columns as Postgres's catalog shows them: name, type, NOT NULL and the default it shows. A keyed table's
/// `_deleted` is left out, as `SELECT *` leaves it out. A lake table's and a temporary table's alike.
fn table_columns(l: &TableMeta) -> Vec<(String, DataType, bool, Option<String>)> {
    let shown = |c: &String| c != "_deleted" || l.key.is_empty(); // (as `SELECT *` shows it)
    l.columns.iter().filter(|(c, _)| shown(c)).map(|(c, t)| {
        let t = crate::answer::dtype(t).unwrap_or(DataType::Utf8);
        // (an identity column's default is its sequence: Postgres keeps no default for it in its catalog)
        let default = l.defaults.get(c).filter(|_| !l.identity.contains_key(c)).map(|d| shown_default(d, &t));
        (c.clone(), t, l.not_null.contains(c), default)
    }).collect()
}

/// The columns whose type their Arrow type doesn't say: a uuid, JSON, JSONB or VARCHAR(n) column (`types.rs`), as
/// Postgres types them. Enum columns are the lake's own (`lakes`).
fn held_types(l: &TableMeta) -> impl Iterator<Item = (String, (u32, i32))> + '_ {
    l.sql_types.iter().filter_map(|(c, t)| match t.as_str() {
        "uuid" => Some((c.clone(), (UUID, -1))),
        "json" => Some((c.clone(), (JSON, -1))),
        "jsonb" => Some((c.clone(), (JSONB, -1))),
        t => crate::types::length(t).map(|n| (c.clone(), (1043, n as i32 + 4))),
    })
}

async fn lakes(lake: &Lake, user: &str, columns: bool, counts: bool) -> Result<Lakes> {
    let schemas = crate::ddl::schemas(lake).await?;
    // (a materialized view's query as it was written: `pg_matviews.definition` and `pg_get_viewdef`, as `View::query` gives it)
    let mvs: BTreeMap<String, String> = lake.cat.scan::<serde_json::Value>("v/", "v0").await?.into_iter().map(|(k, v)| {
        let sql = ["written", "sql"].iter().find_map(|f| v.get(f)?.as_str()).unwrap_or_default();
        (k[2..].to_string(), sql.to_string())
    }).collect();
    let types = enum_types(lake).await?;
    let members = members_of(&lake.cat.scan::<crate::users::User>("u/", "u0").await?, user);
    // COMMENT ON, as `objects.rs` keeps it (`cm/relation/{name}`, `cm/column/{name}/{stored column}`).
    let notes: BTreeMap<String, String> = lake.cat.scan::<String>("cm/", "cm0").await?.into_iter().map(|(k, v)| (k[3..].to_string(), v)).collect();
    let mut rels = vec![];
    let metas = lake.cat.scan::<TableMeta>("t/", "t0").await?;
    let logged = if counts { log_rows(lake, &metas).await? } else { BTreeMap::new() };
    for (k, m) in metas {
        let name = &k[2..];
        if crate::sys::hidden(name) {
            continue;
        }
        let (schema, table) = crate::text::split(name);
        let l = m.described(); // (a view that finishes its answers: those columns)
        let cols = table_columns(&l);
        let (rows, bytes) = m.files.iter().fold((0, 0), |(r, b), f| (r + f.rows, b + f.bytes));
        let sealed = m.sealed.clone().unwrap_or_default();
        let kind = if mvs.contains_key(name) { 'm' } else { 'r' };
        // (an enum column is the type it was declared with, when the lake still has that type; a uuid column is
        // uuid; a VARCHAR(n) column character varying(n), its typmod n + 4 as Postgres keeps it)
        let enums = l.enums.iter().filter_map(|(c, e)| {
            let t = oid("ty", e.of.as_deref()?);
            types.iter().any(|x| x.oid == t).then(|| (c.clone(), (t, -1)))
        });
        let typed = enums.chain(held_types(&l)).collect();
        let comment = notes.get(&format!("relation/{name}")).cloned();
        let comments = l.columns.iter().filter_map(|(c, _)| Some((c.clone(), notes.get(&format!("column/{name}/{}", m.stored(c)?))?.clone()))).collect();
        rels.push(Rel { oid: oid("r", name), schema: schema.into(), name: table.into(), kind, columns: cols, key: l.key.clone(), sql: mvs.get(name).cloned(), identity: l.identity.clone(), constraints: l.constraints.clone(), checks: l.checks.clone(), typed, comment, comments, rows: rows + sealed.rows + logged.get(name).copied().unwrap_or(0), bytes: bytes + sealed.bytes });
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
        let (schema, view) = crate::text::split(name);
        let cols: Vec<(String, DataType, bool, Option<String>)> = match &every {
            Some(ctx) => match ctx.sql(&format!("SELECT * FROM \"{schema}\".\"{view}\" LIMIT 0")).await {
                Ok(df) => match crate::query::run_schema(ctx, df.logical_plan()) {
                    Ok(s) => s.fields().iter().map(|f| (f.name().clone(), f.data_type().clone(), false, None)).collect(),
                    Err(_) => vec![],
                },
                Err(_) => vec![],
            },
            None => vec![],
        };
        let comment = notes.get(&format!("relation/{name}")).cloned();
        let comments = cols.iter().filter_map(|(c, ..)| Some((c.clone(), notes.get(&format!("column/{name}/{c}"))?.clone()))).collect();
        rels.push(Rel { oid: oid("r", name), schema: schema.into(), name: view.into(), kind: 'v', columns: cols, key: vec![], sql: Some(crate::measures::written(&v.sql).into_owned()), identity: Default::default(), constraints: v.facts.clone(), checks: vec![], typed: BTreeMap::new(), comment, comments, rows: 0, bytes: 0 });
    }
    // A user granted some tables sees those, and of one granted by columns those columns, as at every other
    // listing (`ddl::listed`, invariant 382). Postgres's pg_class names every table; Pondra names none its
    // reader may not read, so a BI tool's table list is the user's.
    if let Some(a) = crate::auth::limited() {
        rels.retain_mut(|r| match a.columns("select", &crate::text::join(&r.schema, &r.name)) {
            None => false,
            Some(None) => true,
            Some(Some(these)) => {
                r.columns.retain(|(c, ..)| these.contains(c));
                true
            }
        });
    }
    // The session's own temporary tables and views, in its temporary schema (as Postgres has them).
    let (temp_tables, temp_views) = crate::temp::listed();
    for (name, meta) in temp_tables {
        // (a temporary table is shown as a lake table is: its key, NOT NULL, defaults and checks)
        rels.push(Rel { oid: oid("tmp", &name), schema: TEMP.into(), name, kind: 'r', columns: table_columns(&meta), key: meta.key.clone(), sql: None, identity: Default::default(), constraints: vec![], checks: meta.checks.clone(), typed: held_types(&meta).collect(), comment: None, comments: BTreeMap::new(), rows: 0, bytes: 0 });
    }
    for (name, sql) in temp_views {
        rels.push(Rel { oid: oid("tmp", &name), schema: TEMP.into(), name, kind: 'v', columns: vec![], key: vec![], sql: Some(sql), identity: Default::default(), constraints: vec![], checks: vec![], typed: BTreeMap::new(), comment: None, comments: BTreeMap::new(), rows: 0, bytes: 0 });
    }
    // Sequences (`sq/`): relations of their own, which an identity column counts by. One whose options
    // don't make sense (none can be made) is left out.
    let mut sequences = vec![];
    for (k, seq) in lake.cat.scan::<crate::seq::Sequence>("sq/", "sq0").await? {
        let Ok(shape) = crate::seq::shape(&seq.declared) else { continue };
        let name = &k[3..];
        let (schema, short) = crate::text::split(name);
        sequences.push(SeqRel { oid: oid("sq", name), lake_name: name.into(), schema: schema.into(), name: short.into(), shape });
    }
    let routines = lake.cat.scan::<crate::expander::Routine>("r/", "r0").await?.into_iter().map(|(k, r)| {
        let (s, n) = crate::text::split(&k[2..]);
        (oid("p", &k[2..]), s.to_string(), n.to_string(), r)
    }).collect();
    let mut indexes = vec![];
    for (k, i) in lake.cat.scan::<crate::index::Index>("ix/", "ix0").await? {
        let Some(r) = rels.iter().find(|r| r.oid == oid("r", &i.table)) else { continue };
        let meta = lake.cat.get::<TableMeta>(&crate::store::table_key(&i.table)).await?;
        let name = crate::text::split(&k[3..]).1.to_string();
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
    Ok(Lakes { indexes, database, databases, user: user.into(), schemas: all_schemas, rels, sequences, types, routines, visible: crate::path::schemas_here(lake), members })
}

/// A column's default as Postgres shows it: a text column's string literal gets the `::text` that
/// Postgres adds (`'a'` is `'a'::text`). Anything else, a concatenation among them, is shown as written.
fn shown_default(d: &str, t: &DataType) -> String {
    let inside = d.strip_prefix('\'').and_then(|x| x.strip_suffix('\''));
    let one_literal = inside.is_some_and(|x| !x.replace("''", "").contains('\''));
    match (t, one_literal) {
        (DataType::Utf8, true) => format!("{d}::text"),
        _ => d.to_string(),
    }
}

/// A CHECK constraint's oid: its table's, then its name (`ck` apart from the other constraints).
fn check_oid(r: &Rel, name: &str) -> u32 { oid("ck", &format!("{}.{}.{name}", r.schema, r.name)) }

/// The lake's enum types (`types.rs`), read with the rest of the catalog: only for SQL that names it.
async fn enum_types(lake: &Lake) -> Result<Vec<EnumType>> {
    let all = lake.cat.scan::<crate::types::Type>("ty/", "ty0").await?;
    Ok(all.into_iter().map(|(k, t)| {
        let name = &k[3..]; // (the type's name as the lake has it: `schema.name`, or `name` in public)
        let (schema, short) = crate::text::split(name);
        EnumType { oid: oid("ty", name), array: oid("ta", name), schema: schema.into(), name: short.into(), labels: t.labels }
    }).collect())
}

// ---------------------------------------------------------------- tables

/// A table of `columns` (name, type) and `rows` (a NULL where a value is `ScalarValue::Null`).
fn table(columns: &[(&str, DataType)], rows: Vec<Vec<ScalarValue>>) -> Result<Arc<MemTable>> {
    // (a row of the wrong width would drop or shift values without a word: refused instead, naming the
    // table by its columns)
    if let Some(bad) = rows.iter().find(|r| r.len() != columns.len()) {
        let names: Vec<&str> = columns.iter().map(|(n, _)| *n).collect();
        anyhow::bail!("a catalog table's rows don't match its columns: {} columns ({}), a row of {}", columns.len(), names.join(", "), bad.len());
    }
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
fn i8(v: i64) -> ScalarValue { ScalarValue::Int64(Some(v)) }
fn n() -> ScalarValue { ScalarValue::Null }
/// A timestamp with time zone, as `timestamptz` columns are typed.
fn timestamptz() -> DataType { DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())) }
/// Columns' places in a relation, numbered from 1 (0: not one of its columns).
fn at_of(r: &Rel, columns: &[String]) -> Vec<i16> { columns.iter().map(|c| r.columns.iter().position(|(n, ..)| n == c).map_or(0, |p| p as i16 + 1)).collect() }

/// The relation a foreign key names, as the lake names tables (`schema.table`, or `table`).
fn referred<'a>(rels: &'a [Rel], name: &str) -> Option<&'a Rel> {
    let (schema, table) = crate::text::split(name);
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

/// The columns of `pg_stat_all_tables` (and `pg_stat_user_tables`), in Postgres's order.
fn stat_columns() -> Vec<(&'static str, DataType)> {
    vec![("relid", OID), ("schemaname", TEXT), ("relname", TEXT), ("seq_scan", INT8), ("last_seq_scan", timestamptz()), ("seq_tup_read", INT8), ("idx_scan", INT8),
        ("last_idx_scan", timestamptz()), ("idx_tup_fetch", INT8), ("n_tup_ins", INT8), ("n_tup_upd", INT8), ("n_tup_del", INT8), ("n_tup_hot_upd", INT8),
        ("n_tup_newpage_upd", INT8), ("n_live_tup", INT8), ("n_dead_tup", INT8), ("n_mod_since_analyze", INT8), ("n_ins_since_vacuum", INT8), ("last_vacuum", timestamptz()),
        ("last_autovacuum", timestamptz()), ("last_analyze", timestamptz()), ("last_autoanalyze", timestamptz()), ("vacuum_count", INT8), ("autovacuum_count", INT8),
        ("analyze_count", INT8), ("autoanalyze_count", INT8)]
}

/// The roles `pg_roles` lists, as (oid, name, superuser, login): the bootstrap role first, then the
/// lake's users and roles the caller may see, then the session user when it is none of them (a token's
/// user, such as `admin`).
fn role_rows(l: &Lakes) -> Vec<(u32, String, bool, bool)> {
    let mut rows = vec![(10, BOOTSTRAP.to_string(), true, true)];
    rows.extend(l.members.iter().filter(|m| m.name != BOOTSTRAP).map(|m| (role_oid(&m.name), m.name.clone(), m.superuser, m.login)));
    if l.user != BOOTSTRAP && !l.members.iter().any(|m| m.name == l.user) {
        rows.push((oid("u", &l.user), l.user.clone(), l.user == "admin", true));
    }
    rows
}

fn tables(l: &Lakes, want: &dyn Fn(&str) -> bool) -> Result<Vec<(&'static str, Arc<MemTable>)>> {
    let owner = 10u32;
    let ns = |s: &str| schema_oid(s);
    let mut out = vec![];
    // (only the tables the statement names are built: a catalog query pays for what it reads)
    macro_rules! add {
        ($name:expr, $columns:expr, $rows:expr) => {
            if want($name) {
                out.push(($name, table($columns, $rows)?));
            }
        };
    }
    add!("pg_namespace", &[("oid", OID), ("nspname", TEXT), ("nspowner", OID), ("nspacl", list(TEXT))],
        l.schemas.iter().map(|(oid, name)| vec![o(*oid), s(name.clone()), o(owner), n()]).collect());
    // Relations: tables, views, materialized views, and each key's index.
    let mut classes = vec![];
    for r in &l.rels {
        classes.push(vec![o(r.oid), s(r.name.clone()), o(ns(&r.schema)), o(0), o(0), o(owner), o(if r.kind == 'v' { 0 } else { 2 }), o(r.oid), o(0), i4((r.bytes / 8192) as i32),
            f4(if r.kind == 'v' { -1.0 } else { r.rows as f32 }), i4(0), o(0), b(!r.key.is_empty() || l.indexes.iter().any(|i| i.table == r.oid)), b(false), s(if r.schema == TEMP { "t" } else { "p" }), s(r.kind.to_string()), i2(r.columns.len() as i16), i2(r.checks.len() as i16),
            b(r.kind == 'v'), b(false), b(false), b(false), b(false), b(true), s("d"), b(false), o(0), o(0), o(0), n(), n(), n()]);
        if !r.key.is_empty() {
            classes.push(vec![o(oid("i", &format!("{}.{}", r.schema, r.name))), s(format!("{}_pkey", r.name)), o(ns(&r.schema)), o(0), o(0), o(owner), o(403), o(0), o(0), i4(1), f4(r.rows as f32),
                i4(0), o(0), b(false), b(false), s("p"), s("i"), i2(r.key.len() as i16), i2(0), b(false), b(false), b(false), b(false), b(false), b(true), s("n"), b(false), o(0), o(0), o(0), n(), n(), n()]);
        }
    }
    for i in &l.indexes {
        classes.push(vec![o(i.oid), s(i.name.clone()), o(ns(&i.schema)), o(0), o(0), o(owner), o(403), o(0), o(0), i4(1), f4(0.0),
            i4(0), o(0), b(false), b(false), s("p"), s("i"), i2(i.at.len() as i16), i2(0), b(false), b(false), b(false), b(false), b(false), b(true), s("n"), b(false), o(0), o(0), o(0), n(), n(), n()]);
    }
    // A sequence is a relation of kind 'S', with its three columns (last_value, log_cnt, is_called).
    for q in &l.sequences {
        classes.push(vec![o(q.oid), s(q.name.clone()), o(ns(&q.schema)), o(0), o(0), o(owner), o(0), o(q.oid), o(0), i4(0), f4(-1.0), i4(0), o(0), b(false), b(false), s("p"), s("S"), i2(3), i2(0),
            b(false), b(false), b(false), b(false), b(false), b(true), s("n"), b(false), o(0), o(0), o(0), n(), n(), n()]);
    }
    for (name, oid) in CATALOG {
        classes.push(vec![o(*oid), s(*name), o(11), o(0), o(0), o(owner), o(0), o(*oid), o(0), i4(0), f4(-1.0), i4(0), o(0), b(false), b(false), s("p"), s("v"), i2(0), i2(0),
            b(true), b(false), b(false), b(false), b(false), b(true), s("n"), b(false), o(0), o(0), o(0), n(), n(), n()]);
    }
    add!("pg_class", &[("oid", OID), ("relname", TEXT), ("relnamespace", OID), ("reltype", OID), ("reloftype", OID), ("relowner", OID), ("relam", OID), ("relfilenode", OID),
        ("reltablespace", OID), ("relpages", INT4), ("reltuples", DataType::Float32), ("relallvisible", INT4), ("reltoastrelid", OID), ("relhasindex", BOOL), ("relisshared", BOOL),
        ("relpersistence", TEXT), ("relkind", TEXT), ("relnatts", INT2), ("relchecks", INT2), ("relhasrules", BOOL), ("relhastriggers", BOOL), ("relhassubclass", BOOL),
        ("relrowsecurity", BOOL), ("relforcerowsecurity", BOOL), ("relispopulated", BOOL), ("relreplident", TEXT), ("relispartition", BOOL), ("relrewrite", OID),
        ("relfrozenxid", OID), ("relminmxid", OID), ("relacl", list(TEXT)), ("reloptions", list(TEXT)), ("relpartbound", TEXT)], classes);
    // Columns (and their defaults), numbered from 1.
    let (mut attrs, mut defs) = (vec![], vec![]);
    for r in &l.rels {
        for (i, (name, t, not_null, default)) in r.columns.iter().enumerate() {
            let (typ, typmod, len) = r.column_type(name, t);
            let not_null = *not_null || r.key.contains(name);
            let identity = r.identity.get(name).map_or("", |i| if i.always { "a" } else { "d" }); // (an identity has no default of its own, as in Postgres)
            let default = default.as_ref().filter(|_| identity.is_empty());
            attrs.push(vec![o(r.oid), s(name.clone()), o(typ), i4(-1), i2(len), i2(i as i16 + 1), i4(0), i4(-1), i4(typmod), b(len > 0 && len <= 8), s("i"), s("x"), s(""), b(not_null),
                b(default.is_some()), b(false), s(identity), s(""), b(false), b(true), i4(0), o(if matches!(typ, 25 | 1043) { 100 } else { 0 }), n(), n(), n(), n()]);
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
            let (typ, typmod, _) = r.column_type(k, t);
            attrs.push(vec![o(index), s(k.clone()), o(typ), i4(-1), i2(8), i2(i as i16 + 1), i4(0), i4(-1), i4(typmod), b(true), s("i"), s("p"), s(""), b(true), b(false), b(false), s(""), s(""), b(false), b(true), i4(0), o(0), n(), n(), n(), n()]);
        }
    }
    add!("pg_attribute", &[("attrelid", OID), ("attname", TEXT), ("atttypid", OID), ("attstattarget", INT4), ("attlen", INT2), ("attnum", INT2), ("attndims", INT4),
        ("attcacheoff", INT4), ("atttypmod", INT4), ("attbyval", BOOL), ("attalign", TEXT), ("attstorage", TEXT), ("attcompression", TEXT), ("attnotnull", BOOL), ("atthasdef", BOOL),
        ("atthasmissing", BOOL), ("attidentity", TEXT), ("attgenerated", TEXT), ("attisdropped", BOOL), ("attislocal", BOOL), ("attinhcount", INT4), ("attcollation", OID),
        ("attacl", list(TEXT)), ("attoptions", list(TEXT)), ("attfdwoptions", list(TEXT)), ("attmissingval", TEXT)], attrs);
    add!("pg_attrdef", &[("oid", OID), ("adrelid", OID), ("adnum", INT2), ("adbin", TEXT)], defs);
    // COMMENT ON: a relation's (objsubid 0) and a column's (its number), all of pg_class (oid 1259).
    let mut described = vec![];
    for r in &l.rels {
        described.extend(r.comment.iter().map(|c| vec![o(r.oid), o(1259), i4(0), s(c.clone())]));
        described.extend(r.columns.iter().enumerate().filter_map(|(i, (c, ..))| Some(vec![o(r.oid), o(1259), i4(i as i32 + 1), s(r.comments.get(c)?.clone())])));
    }
    add!("pg_description", &[("objoid", OID), ("classoid", OID), ("objsubid", INT4), ("description", TEXT)], described);
    // Types, and an array type for each.
    // (The functions by Postgres's own names: drivers such as ADBC's pick a type's binary format by
    // its `typreceive`. Newer types' names have an underscore.)
    let fun = |t: &PgType, f: &str| s(type_function(t, f));
    // (the subscript, modifier and analyze functions are `-`: none, as Postgres says of a type without one)
    let mut types: Vec<Vec<ScalarValue>> = TYPES.iter().map(|t| vec![o(t.0), s(t.1), o(11), o(owner), i2(t.3), b(t.3 > 0 && t.3 <= 8), s(if t.4 == 'P' { "p" } else { "b" }), s(t.4.to_string()),
        b(false), b(true), s(","), o(0), s("-"), o(0), o(t.5), fun(t, "in"), fun(t, "out"), fun(t, "recv"), fun(t, "send"), s("-"), s("-"), s("-"), o(0), i4(-1), b(false), i4(0),
        o(if matches!(t.0, 25 | 1043 | 1042 | 19) { 100 } else { 0 }), n(), n(), s("i"), s("p"), n()]).collect();
    types.extend(arrays().map(|(oid, name, _, elem)| vec![o(oid), s(name), o(11), o(owner), i2(-1), b(false), s("b"), s("A"), b(false), b(true), s(","), o(0), s("-"), o(elem), o(0), s("array_in"),
        s("array_out"), s("array_recv"), s("array_send"), s("-"), s("-"), s("-"), o(0), i4(-1), b(false), i4(0), o(0), n(), n(), s("i"), s("x"), n()]));
    // Enum types: a row of type 'e' and its array's, and a row in pg_enum for each label, in order.
    let mut labels = vec![];
    for t in &l.types {
        types.push(vec![o(t.oid), s(t.name.clone()), o(schema_oid(&t.schema)), o(owner), i2(ENUM_LENGTH), b(true), s("e"), s("E"), b(false), b(true), s(","), o(0), s("-"), o(0), o(t.array),
            s("enum_in"), s("enum_out"), s("enum_recv"), s("enum_send"), s("-"), s("-"), s("-"), o(0), i4(-1), b(false), i4(0), o(0), n(), n(), s("i"), s("p"), n()]);
        types.push(vec![o(t.array), s(format!("_{}", t.name)), o(schema_oid(&t.schema)), o(owner), i2(-1), b(false), s("b"), s("A"), b(false), b(true), s(","), o(0), s("-"), o(t.oid), o(0),
            s("array_in"), s("array_out"), s("array_recv"), s("array_send"), s("-"), s("-"), s("-"), o(0), i4(-1), b(false), i4(0), o(0), n(), n(), s("i"), s("x"), n()]);
        labels.extend(t.labels.iter().enumerate().map(|(i, label)| vec![o(oid("ev", &format!("{}/{label}", t.oid))), o(t.oid), f4(i as f32 + 1.0), s(label.clone())]));
    }
    add!("pg_enum", &[("oid", OID), ("enumtypid", OID), ("enumsortorder", DataType::Float32), ("enumlabel", TEXT)], labels);
    add!("pg_type", &[("oid", OID), ("typname", TEXT), ("typnamespace", OID), ("typowner", OID), ("typlen", INT2), ("typbyval", BOOL), ("typtype", TEXT), ("typcategory", TEXT),
        ("typispreferred", BOOL), ("typisdefined", BOOL), ("typdelim", TEXT), ("typrelid", OID), ("typsubscript", TEXT), ("typelem", OID), ("typarray", OID), ("typinput", TEXT), ("typoutput", TEXT),
        ("typreceive", TEXT), ("typsend", TEXT), ("typmodin", TEXT), ("typmodout", TEXT), ("typanalyze", TEXT), ("typbasetype", OID), ("typtypmod", INT4), ("typnotnull", BOOL), ("typndims", INT4),
        ("typcollation", OID), ("typdefaultbin", TEXT), ("typdefault", TEXT), ("typalign", TEXT), ("typstorage", TEXT), ("typacl", list(TEXT))], types);
    // Keys: an index and a constraint for each keyed table.
    let (mut indexes, mut constraints) = (vec![], vec![]);
    for r in l.rels.iter().filter(|r| !r.key.is_empty()) {
        let at: Vec<i16> = r.key.iter().filter_map(|k| r.columns.iter().position(|(c, ..)| c == k).map(|i| i as i16 + 1)).collect();
        let full = format!("{}.{}", r.schema, r.name);
        indexes.push(vec![o(oid("i", &full)), o(r.oid), i2(at.len() as i16), i2(at.len() as i16), b(true), b(false), b(true), b(false), b(true), b(false), b(true), b(false), b(true), b(true), b(false), i2s(&at), ScalarValue::List(ScalarValue::new_list_nullable(&at.iter().map(|_| o(0)).collect::<Vec<_>>(), &OID)),
            ScalarValue::List(ScalarValue::new_list_nullable(&at.iter().map(|_| o(3124)).collect::<Vec<_>>(), &OID)), i2s(&at.iter().map(|_| 0).collect::<Vec<_>>()), n(), n()]);
        constraints.push(vec![o(oid("c", &full)), s(format!("{}_pkey", r.name)), o(ns(&r.schema)), s("p"), b(false), b(false), b(true), o(r.oid), o(0), o(oid("i", &full)), o(0), o(0), s(" "), s(" "), s(" "),
            b(true), i4(0), b(true), i2s(&at), n(), n(), n(), n(), n(), n(), n(), b(true)]);
    }
    for i in &l.indexes {
        let each = |v: ScalarValue| ScalarValue::List(ScalarValue::new_list_nullable(&i.at.iter().map(|_| v.clone()).collect::<Vec<_>>(), &OID));
        indexes.push(vec![o(i.oid), o(i.table), i2(i.at.len() as i16), i2(i.at.len() as i16), b(i.unique), b(false), b(false), b(false), b(true), b(false), b(true), b(false), b(true), b(true), b(false), i2s(&i.at), each(o(0)),
            each(o(3124)), i2s(&i.at.iter().map(|_| 0).collect::<Vec<_>>()), n(), n()]);
    }
    for r in &l.rels {
        for (name, cond) in &r.checks {
            // (the columns its condition names, by their words: Postgres keeps them as `conkey`)
            let reads: Vec<String> = r.columns.iter().map(|(c, ..)| c.clone()).filter(|c| cond.split(|ch: char| !(ch.is_alphanumeric() || ch == '_')).any(|w| w == c)).collect();
            constraints.push(vec![o(check_oid(r, name)), s(name.clone()), o(ns(&r.schema)), s("c"), b(false), b(false), b(true), o(r.oid), o(0), o(0), o(0), o(0), s(" "), s(" "), s(" "),
                b(true), i4(0), b(true), i2s(&at_of(r, &reads)), n(), n(), n(), n(), n(), n(), s(cond.clone()), b(true)]);
        }
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
                s(if foreign { "a" } else { " " }), s(if foreign { "a" } else { " " }), s(if foreign { "s" } else { " " }), b(true), i4(0), b(true), i2s(&at_of(r, &c.columns)), if foreign { to_cols } else { n() }, n(), n(), n(), n(), n(), n(), b(c.enforced)]);
        }
    }
    add!("pg_index", &[("indexrelid", OID), ("indrelid", OID), ("indnatts", INT2), ("indnkeyatts", INT2), ("indisunique", BOOL), ("indnullsnotdistinct", BOOL), ("indisprimary", BOOL), ("indisexclusion", BOOL),
        ("indimmediate", BOOL), ("indisclustered", BOOL), ("indisvalid", BOOL), ("indcheckxmin", BOOL), ("indisready", BOOL), ("indislive", BOOL), ("indisreplident", BOOL),
        ("indkey", list(INT2)), ("indcollation", list(OID)), ("indclass", list(OID)), ("indoption", list(INT2)), ("indexprs", TEXT), ("indpred", TEXT)], indexes);
    add!("pg_constraint", &[("oid", OID), ("conname", TEXT), ("connamespace", OID), ("contype", TEXT), ("condeferrable", BOOL), ("condeferred", BOOL), ("convalidated", BOOL),
        ("conrelid", OID), ("contypid", OID), ("conindid", OID), ("conparentid", OID), ("confrelid", OID), ("confupdtype", TEXT), ("confdeltype", TEXT), ("confmatchtype", TEXT),
        ("conislocal", BOOL), ("coninhcount", INT4), ("connoinherit", BOOL), ("conkey", list(INT2)), ("confkey", list(INT2)), ("conpfeqop", list(OID)), ("conppeqop", list(OID)),
        ("conffeqop", list(OID)), ("confdelsetcols", list(INT2)), ("conexclop", list(OID)), ("conbin", TEXT), ("conenforced", BOOL)], constraints);
    // Sequences: their type and options, as `seq::shape` works them out (Postgres's oids for the types).
    let sequence_rows = l.sequences.iter().map(|q| {
        let typ = match q.shape.as_type { "SMALLINT" => 21, "INTEGER" => 23, _ => 20 };
        vec![o(q.oid), o(typ), ScalarValue::Int64(Some(q.shape.start)), ScalarValue::Int64(Some(q.shape.increment)), ScalarValue::Int64(Some(q.shape.max)),
            ScalarValue::Int64(Some(q.shape.min)), ScalarValue::Int64(Some(q.shape.cache)), b(q.shape.cycle)]
    }).collect();
    add!("pg_sequence", &[("seqrelid", OID), ("seqtypid", OID), ("seqstart", INT8), ("seqincrement", INT8), ("seqmax", INT8), ("seqmin", INT8), ("seqcache", INT8), ("seqcycle", BOOL)], sequence_rows);
    // Statistics of each table and materialized view. Pondra keeps no counts but its rows, and BI tools
    // size a table by `n_live_tup`; both views list the same rows. (A temporary table is no one's.)
    let stats: Vec<Vec<ScalarValue>> = l.rels.iter().filter(|r| matches!(r.kind, 'r' | 'm') && r.schema != TEMP).map(|r| vec![o(r.oid), s(r.schema.clone()), s(r.name.clone()),
        i8(0), n(), i8(0), i8(0), n(), i8(0), i8(0), i8(0), i8(0), i8(0), i8(0), i8(r.rows as i64), i8(0), i8(0), i8(0), n(), n(), n(), n(), i8(0), i8(0), i8(0), i8(0)]).collect();
    add!("pg_stat_user_tables", &stat_columns(), stats.clone());
    add!("pg_stat_all_tables", &stat_columns(), stats);
    add!("pg_sequences", &[("schemaname", TEXT), ("sequencename", TEXT), ("sequenceowner", TEXT), ("data_type", TEXT), ("start_value", INT8), ("min_value", INT8),
        ("max_value", INT8), ("increment_by", INT8), ("cycle", BOOL), ("cache_size", INT8), ("last_value", INT8)],
        l.sequences.iter().map(|q| vec![s(q.schema.clone()), s(q.name.clone()), s(l.user.clone()), s(match q.shape.as_type { "SMALLINT" => "smallint", "INTEGER" => "integer", _ => "bigint" }),
            i8(q.shape.start), i8(q.shape.min), i8(q.shape.max), i8(q.shape.increment), b(q.shape.cycle), i8(q.shape.cache), n()]).collect());
    // The views Postgres builds over those.
    let rel_rows = |kind: char| l.rels.iter().filter(move |r| r.kind == kind);
    add!("pg_tables", &[("schemaname", TEXT), ("tablename", TEXT), ("tableowner", TEXT), ("tablespace", TEXT), ("hasindexes", BOOL), ("hasrules", BOOL), ("hastriggers", BOOL), ("rowsecurity", BOOL)],
        rel_rows('r').map(|r| vec![s(r.schema.clone()), s(r.name.clone()), s(l.user.clone()), n(), b(!r.key.is_empty()), b(false), b(false), b(false)]).collect());
    let mut listed: Vec<Vec<ScalarValue>> = l.rels.iter().filter(|r| !r.key.is_empty()).map(|r| {
        let def = format!("CREATE UNIQUE INDEX {}_pkey ON {}.{} USING btree ({})", r.name, r.schema, r.name, r.key.join(", "));
        vec![s(r.schema.clone()), s(r.name.clone()), s(format!("{}_pkey", r.name)), n(), s(def)]
    }).collect();
    listed.extend(l.indexes.iter().map(|i| vec![s(i.schema.clone()), s(i.table_name.clone()), s(i.name.clone()), n(), s(i.def.clone())]));
    add!("pg_indexes", &[("schemaname", TEXT), ("tablename", TEXT), ("indexname", TEXT), ("tablespace", TEXT), ("indexdef", TEXT)], listed);
    add!("pg_views", &[("schemaname", TEXT), ("viewname", TEXT), ("viewowner", TEXT), ("definition", TEXT)],
        rel_rows('v').map(|r| vec![s(r.schema.clone()), s(r.name.clone()), s(l.user.clone()), s(r.sql.clone().unwrap_or_default())]).collect());
    add!("pg_matviews", &[("schemaname", TEXT), ("matviewname", TEXT), ("matviewowner", TEXT), ("tablespace", TEXT), ("hasindexes", BOOL), ("ispopulated", BOOL), ("definition", TEXT)],
        rel_rows('m').map(|r| vec![s(r.schema.clone()), s(r.name.clone()), s(l.user.clone()), n(), b(!r.key.is_empty() || l.indexes.iter().any(|i| i.table == r.oid)), b(true), s(r.sql.clone().unwrap_or_default())]).collect());
    add!("pg_proc", &[("oid", OID), ("proname", TEXT), ("pronamespace", OID), ("proowner", OID), ("prolang", OID), ("procost", DataType::Float32), ("prorows", DataType::Float32),
        ("provariadic", OID), ("prosupport", TEXT), ("prokind", TEXT), ("prosecdef", BOOL), ("proleakproof", BOOL), ("proisstrict", BOOL), ("proretset", BOOL), ("provolatile", TEXT),
        ("proparallel", TEXT), ("pronargs", INT2), ("pronargdefaults", INT2), ("prorettype", OID), ("proargtypes", list(OID)), ("proallargtypes", list(OID)), ("proargmodes", list(TEXT)),
        ("proargnames", list(TEXT)), ("proargdefaults", TEXT), ("protrftypes", list(OID)), ("prosrc", TEXT), ("probin", TEXT), ("prosqlbody", TEXT), ("proconfig", list(TEXT)), ("proacl", list(TEXT))],
        l.routines.iter().map(|(oid, schema, name, r)| {
            use crate::expander::Kind;
            let kind = match r.kind { Kind::Procedure => "p", _ => "f" };
            let lang = if r.language == "python" { 13_000 } else { 14 };
            let rettype = match r.kind { Kind::Procedure => 2278, Kind::Table => 2249, Kind::Macro => r.returns.as_deref().map_or(25, sql_oid) };
            let args: Vec<ScalarValue> = r.params.iter().map(|p| o(p.ty.as_deref().map_or(25, sql_oid))).collect();
            let names: Vec<String> = r.params.iter().map(|p| p.name.clone()).collect();
            vec![o(*oid), s(name.clone()), o(schema_oid(schema)), o(owner), o(lang), f4(100.0), f4(if r.kind == Kind::Table { 1000.0 } else { 0.0 }), o(0), s("-"), s(kind), b(false), b(false), b(false),
                b(r.kind == Kind::Table), s("v"), s("u"), i2(r.params.len() as i16), i2(r.params.iter().filter(|p| p.default.is_some()).count() as i16), o(rettype),
                ScalarValue::List(ScalarValue::new_list_nullable(&args, &OID)), n(), n(), texts(&names), n(), n(), if crate::auth::shows_code() { s(r.body.clone()) } else { n() }, n(), n(), n(), n()]
        }).chain(type_functions().into_iter().map(|f| vec![o(oid("tf", &f)), s(f.clone()), o(11), o(10), o(12), f4(1.0), f4(0.0), o(0), s("-"), s("f"), b(false), b(false), b(true),
            b(false), s("i"), s("s"), i2(1), i2(0), o(0), ScalarValue::List(ScalarValue::new_list_nullable(&[o(0)], &OID)), n(), n(), n(), n(), n(), s(f), n(), n(), n(), n()])).collect());
    add!("pg_database", &[("oid", OID), ("datname", TEXT), ("datdba", OID), ("encoding", INT4), ("datlocprovider", TEXT), ("datistemplate", BOOL), ("datallowconn", BOOL),
        ("datconnlimit", INT4), ("datfrozenxid", OID), ("datminmxid", OID), ("dattablespace", OID), ("datcollate", TEXT), ("datctype", TEXT), ("daticulocale", TEXT), ("daticurules", TEXT),
        ("datcollversion", TEXT), ("datacl", list(TEXT))],
        l.databases.iter().map(|d| vec![o(if *d == l.database { 16384 } else { oid("db", d) }), s(d.clone()), o(owner), i4(6), s("c"), b(false), b(true), i4(-1), o(0), o(0), o(1663), s("C.UTF-8"), s("C.UTF-8"), n(), n(), n(), n()]).collect());
    let roles = role_rows(l);
    add!("pg_roles", &[("oid", OID), ("rolname", TEXT), ("rolsuper", BOOL), ("rolinherit", BOOL), ("rolcreaterole", BOOL), ("rolcreatedb", BOOL), ("rolcanlogin", BOOL),
        ("rolreplication", BOOL), ("rolconnlimit", INT4), ("rolpassword", TEXT), ("rolvaliduntil", TEXT), ("rolbypassrls", BOOL), ("rolconfig", list(TEXT))],
        roles.iter().map(|(oid, name, sup, login)| vec![o(*oid), s(name.clone()), b(*sup), b(true), b(*sup), b(*sup), b(*login), b(false), i4(-1), s("********"), n(), b(false), n()]).collect());
    add!("pg_user", &[("usename", TEXT), ("usesysid", OID), ("usecreatedb", BOOL), ("usesuper", BOOL), ("userepl", BOOL), ("usebypassrls", BOOL), ("passwd", TEXT), ("valuntil", TEXT), ("useconfig", list(TEXT))],
        roles.iter().filter(|r| r.3).map(|(oid, name, sup, _)| vec![s(name.clone()), o(*oid), b(*sup), b(*sup), b(false), b(false), s("********"), n(), n()]).collect());
    // Memberships: one row per role of each member, the role's oid even when the role isn't listed.
    let memberships: Vec<Vec<ScalarValue>> = l.members.iter().flat_map(|m| m.roles.iter().map(move |r| {
        vec![o(oid("am", &format!("{}/{r}", m.name))), o(role_oid(r)), o(role_oid(&m.name)), o(owner), b(false), b(true), b(true)]
    })).collect();
    add!("pg_auth_members", &[("oid", OID), ("roleid", OID), ("member", OID), ("grantor", OID), ("admin_option", BOOL), ("inherit_option", BOOL), ("set_option", BOOL)], memberships);
    // (boot_val and reset_val are the setting: nothing here changes it from its default)
    add!("pg_settings", &[("name", TEXT), ("setting", TEXT), ("unit", TEXT), ("category", TEXT), ("short_desc", TEXT), ("extra_desc", TEXT), ("context", TEXT), ("vartype", TEXT),
        ("source", TEXT), ("min_val", TEXT), ("max_val", TEXT), ("enumvals", list(TEXT)), ("boot_val", TEXT), ("reset_val", TEXT), ("sourcefile", TEXT), ("sourceline", INT4), ("pending_restart", BOOL)],
        SETTINGS.iter().map(|(k, v)| vec![s(*k), s(*v), n(), s("Pondra"), s(""), n(), s("user"), s("string"), s("default"), n(), n(), n(), s(*v), s(*v), n(), n(), b(false)]).collect());
    add!("pg_am", &[("oid", OID), ("amname", TEXT), ("amhandler", TEXT), ("amtype", TEXT)], vec![vec![o(2), s("heap"), s("heap_tableam_handler"), s("t")], vec![o(403), s("btree"), s("bthandler"), s("i")]]);
    add!("pg_tablespace", &[("oid", OID), ("spcname", TEXT), ("spcowner", OID), ("spcacl", list(TEXT)), ("spcoptions", list(TEXT))], vec![vec![o(1663), s("pg_default"), o(owner), n(), n()]]);
    // (no validator or inline handler here, and no privileges set: their OIDs are 0 and the list NULL)
    add!("pg_language", &[("oid", OID), ("lanname", TEXT), ("lanowner", OID), ("lanispl", BOOL), ("lanpltrusted", BOOL), ("lanplcallfoid", OID), ("laninline", OID),
        ("lanvalidator", OID), ("lanacl", list(TEXT))],
        vec![vec![o(12), s("internal"), o(owner), b(false), b(false), o(0), o(0), o(0), n()], vec![o(14), s("sql"), o(owner), b(false), b(true), o(0), o(0), o(0), n()],
            vec![o(13_000), s("python"), o(owner), b(true), b(false), o(0), o(0), o(0), n()]]);
    // Present, and empty: nothing in Pondra is one of these.
    for (name, cols) in [
        ("pg_shdescription", vec![("objoid", OID), ("classoid", OID), ("description", TEXT)]),
        ("pg_depend", vec![("classid", OID), ("objid", OID), ("objsubid", INT4), ("refclassid", OID), ("refobjid", OID), ("refobjsubid", INT4), ("deptype", TEXT)]),
        ("pg_rewrite", vec![("oid", OID), ("rulename", TEXT), ("ev_class", OID), ("ev_type", TEXT), ("ev_enabled", TEXT), ("is_instead", BOOL), ("ev_qual", TEXT), ("ev_action", TEXT)]),
        ("pg_inherits", vec![("inhrelid", OID), ("inhparent", OID), ("inhseqno", INT4), ("inhdetachpending", BOOL)]),
        ("pg_collation", vec![("oid", OID), ("collname", TEXT), ("collnamespace", OID), ("collowner", OID), ("collprovider", TEXT), ("collisdeterministic", BOOL), ("collencoding", INT4), ("collcollate", TEXT), ("collctype", TEXT),
            ("colliculocale", TEXT), ("collicurules", TEXT), ("collversion", TEXT)]),
        ("pg_extension", vec![("oid", OID), ("extname", TEXT), ("extowner", OID), ("extnamespace", OID), ("extrelocatable", BOOL), ("extversion", TEXT), ("extconfig", list(OID)), ("extcondition", list(TEXT))]),
        ("pg_range", vec![("rngtypid", OID), ("rngsubtype", OID), ("rngmultitypid", OID), ("rngcollation", OID), ("rngsubopc", OID), ("rngcanonical", TEXT), ("rngsubdiff", TEXT)]),
        ("pg_trigger", vec![("oid", OID), ("tgrelid", OID), ("tgparentid", OID), ("tgname", TEXT), ("tgfoid", OID), ("tgtype", INT2), ("tgenabled", TEXT), ("tgisinternal", BOOL),
            ("tgconstrrelid", OID), ("tgconstrindid", OID), ("tgconstraint", OID), ("tgdeferrable", BOOL), ("tginitdeferred", BOOL), ("tgnargs", INT2), ("tgattr", list(INT2)),
            ("tgargs", TEXT), ("tgqual", TEXT), ("tgoldtable", TEXT), ("tgnewtable", TEXT)]),
        ("pg_foreign_table", vec![("ftrelid", OID), ("ftserver", OID), ("ftoptions", list(TEXT))]),
        ("pg_partitioned_table", vec![("partrelid", OID), ("partstrat", TEXT), ("partnatts", INT2), ("partdefid", OID), ("partattrs", list(INT2))]),
        ("pg_policy", vec![("oid", OID), ("polname", TEXT), ("polrelid", OID), ("polcmd", TEXT), ("polpermissive", BOOL), ("polroles", list(OID)), ("polqual", TEXT), ("polwithcheck", TEXT)]),
        ("pg_statistic_ext", vec![("oid", OID), ("stxrelid", OID), ("stxname", TEXT), ("stxnamespace", OID), ("stxowner", OID), ("stxkeys", list(INT2)), ("stxkind", list(TEXT))]),
        ("pg_publication", vec![("oid", OID), ("pubname", TEXT), ("pubowner", OID), ("puballtables", BOOL)]),
        ("pg_event_trigger", vec![("oid", OID), ("evtname", TEXT), ("evtevent", TEXT), ("evtowner", OID), ("evtfoid", OID), ("evtenabled", TEXT), ("evttags", list(TEXT))]),
        ("pg_cast", vec![("oid", OID), ("castsource", OID), ("casttarget", OID), ("castfunc", OID), ("castcontext", TEXT), ("castmethod", TEXT)]),
        ("pg_opclass", vec![("oid", OID), ("opcmethod", OID), ("opcname", TEXT), ("opcnamespace", OID), ("opcowner", OID), ("opcfamily", OID), ("opcintype", OID), ("opcdefault", BOOL), ("opckeytype", OID)]),
        ("pg_stat_activity", vec![("datid", OID), ("datname", TEXT), ("pid", INT4), ("leader_pid", INT4), ("usesysid", OID), ("usename", TEXT), ("application_name", TEXT),
            ("client_addr", TEXT), ("client_hostname", TEXT), ("client_port", INT4), ("backend_start", timestamptz()), ("xact_start", timestamptz()), ("query_start", timestamptz()),
            ("state_change", timestamptz()), ("wait_event_type", TEXT), ("wait_event", TEXT), ("state", TEXT), ("backend_xid", OID), ("backend_xmin", OID), ("query_id", INT8),
            ("query", TEXT), ("backend_type", TEXT)]),
        ("pg_policies", vec![("schemaname", TEXT), ("tablename", TEXT), ("policyname", TEXT), ("permissive", TEXT), ("roles", list(TEXT)), ("cmd", TEXT), ("qual", TEXT), ("with_check", TEXT)]),
        ("pg_default_acl", vec![("oid", OID), ("defaclrole", OID), ("defaclnamespace", OID), ("defaclobjtype", TEXT), ("defaclacl", list(TEXT))]),
        ("pg_db_role_setting", vec![("setdatabase", OID), ("setrole", OID), ("setconfig", list(TEXT))]),
        ("pg_aggregate", vec![("aggfnoid", TEXT), ("aggkind", TEXT), ("aggnumdirectargs", INT2), ("aggtransfn", TEXT), ("aggfinalfn", TEXT), ("aggcombinefn", TEXT), ("aggserialfn", TEXT),
            ("aggdeserialfn", TEXT), ("aggmtransfn", TEXT), ("aggminvtransfn", TEXT), ("aggmfinalfn", TEXT), ("aggfinalextra", BOOL), ("aggmfinalextra", BOOL), ("aggfinalmodify", TEXT),
            ("aggmfinalmodify", TEXT), ("aggsortop", OID), ("aggtranstype", OID), ("aggtransspace", INT4), ("aggmtranstype", OID), ("aggmtransspace", INT4), ("agginitval", TEXT), ("aggminitval", TEXT)]),
        ("pg_conversion", vec![("oid", OID), ("conname", TEXT), ("connamespace", OID), ("conowner", OID), ("conforencoding", INT4), ("contoencoding", INT4), ("conproc", TEXT), ("condefault", BOOL)]),
        ("pg_foreign_data_wrapper", vec![("oid", OID), ("fdwname", TEXT), ("fdwowner", OID), ("fdwhandler", OID), ("fdwvalidator", OID), ("fdwacl", list(TEXT)), ("fdwoptions", list(TEXT))]),
        ("pg_foreign_server", vec![("oid", OID), ("srvname", TEXT), ("srvowner", OID), ("srvfdw", OID), ("srvtype", TEXT), ("srvversion", TEXT), ("srvacl", list(TEXT)), ("srvoptions", list(TEXT))]),
        ("pg_available_extensions", vec![("name", TEXT), ("default_version", TEXT), ("installed_version", TEXT), ("comment", TEXT)]),
    ] {
        let cols: Vec<(&str, DataType)> = cols;
        add!(name, &cols, vec![]);
    }
    Ok(out)
}

/// Settings clients read (`SHOW`, `current_setting`, `pg_settings`).
pub const SETTINGS: &[(&str, &str)] = &[("server_version", "16.0"), ("server_version_num", "160000"), ("server_encoding", "UTF8"), ("client_encoding", "UTF8"),
    ("standard_conforming_strings", "on"), ("integer_datetimes", "on"), ("TimeZone", "UTC"), ("DateStyle", "ISO, MDY"), ("IntervalStyle", "postgres"),
    ("max_identifier_length", "63"), ("default_transaction_isolation", "read committed"), ("transaction_isolation", "read committed"), ("search_path", "\"$user\", public"),
    ("is_superuser", "off"), ("application_name", ""), ("default_transaction_read_only", "off"), ("lc_collate", "C.UTF-8"), ("lc_ctype", "C.UTF-8")];

/// The SQL standard's `information_schema`, Postgres's way: its types by Postgres's names.
fn information_schema(l: &Lakes, want: &dyn Fn(&str) -> bool) -> Result<Vec<(&'static str, Arc<MemTable>)>> {
    let db = || s(l.database.clone());
    let yes = |v: bool| s(if v { "YES" } else { "NO" });
    let mut out = vec![];
    // (only the tables the statement names are built: a catalog query pays for what it reads)
    macro_rules! add {
        ($name:expr, $columns:expr, $rows:expr) => {
            if want($name) {
                out.push(($name, table($columns, $rows)?));
            }
        };
    }
    add!("schemata", &[("catalog_name", TEXT), ("schema_name", TEXT), ("schema_owner", TEXT), ("default_character_set_catalog", TEXT), ("default_character_set_schema", TEXT),
        ("default_character_set_name", TEXT), ("sql_path", TEXT)], l.schemas.iter().map(|(_, sc)| vec![db(), s(sc.clone()), s(l.user.clone()), n(), n(), n(), n()]).collect());
    add!("tables", &[("table_catalog", TEXT), ("table_schema", TEXT), ("table_name", TEXT), ("table_type", TEXT), ("self_referencing_column_name", TEXT), ("reference_generation", TEXT),
        ("user_defined_type_catalog", TEXT), ("user_defined_type_schema", TEXT), ("user_defined_type_name", TEXT), ("is_insertable_into", TEXT), ("is_typed", TEXT), ("commit_action", TEXT)],
        l.rels.iter().map(|r| vec![db(), s(r.schema.clone()), s(r.name.clone()), s(if r.kind == 'v' { "VIEW" } else { "BASE TABLE" }), n(), n(), n(), n(), n(), yes(r.kind == 'r'), s("NO"), n()]).collect());
    let mut cols = vec![];
    for r in &l.rels {
        for (i, (name, t, not_null, default)) in r.columns.iter().enumerate() {
            let (typ, typmod, _) = r.column_type(name, t);
            let array = arrays().find(|a| a.0 == typ);
            let data_type = match (r.typed.get(name).is_some_and(|t| l.enum_type(t.0).is_some()), array) {
                (true, _) => "USER-DEFINED".to_string(), // (an enum: its type's schema and name, below)
                (false, Some(_)) => "ARRAY".to_string(),
                (false, None) => l.format_type(typ, -1).unwrap_or_else(|| "text".into()),
            };
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
            let (udt_schema, udt) = match r.typed.get(name).and_then(|t| l.enum_type(t.0)) {
                Some(e) => (e.schema.clone(), e.name.clone()),
                None => ("pg_catalog".to_string(), type_name(typ).unwrap_or_else(|| "text".into())),
            };
            // An identity column's options, from its sequence (the standard's identity_* columns).
            let identity = r.identity.get(name);
            let shape = identity.and_then(|id| l.sequences.iter().find(|q| q.lake_name == id.sequence)).map(|q| &q.shape);
            let (generation, start, increment, maximum, minimum, cycle) = match (identity, shape) {
                (Some(id), Some(sh)) => (s(if id.always { "ALWAYS" } else { "BY DEFAULT" }), s(sh.start.to_string()), s(sh.increment.to_string()), s(sh.max.to_string()), s(sh.min.to_string()), yes(sh.cycle)),
                _ => (n(), n(), n(), n(), n(), n()),
            };
            // (a VARCHAR(n)'s length, and its bytes at UTF-8's four a character, as Postgres says; text's 1 GB)
            let (chars, octets) = match (typ, typmod) {
                (1042 | 1043, m) if m >= 4 => (i4(m - 4), i4((m - 4) * 4)),
                (25 | 1043, _) => (n(), i4(1_073_741_824)),
                _ => (n(), n()),
            };
            cols.push(vec![db(), s(r.schema.clone()), s(r.name.clone()), s(name.clone()), i4(i as i32 + 1), default.clone().map_or(n(), s), yes(!(*not_null || r.key.contains(name))), s(data_type),
                chars, octets, precision, radix, scale, datetime, n(), n(), n(), n(), n(), n(), n(), n(), n(), n(), n(), db(), s(udt_schema), s(udt),
                n(), n(), n(), n(), s((i + 1).to_string()), s("NO"), yes(identity.is_some()), generation, start, increment, maximum, minimum, cycle, s("NEVER"), n(), yes(r.kind == 'r')]);
        }
    }
    add!("columns", &[("table_catalog", TEXT), ("table_schema", TEXT), ("table_name", TEXT), ("column_name", TEXT), ("ordinal_position", INT4), ("column_default", TEXT),
        ("is_nullable", TEXT), ("data_type", TEXT), ("character_maximum_length", INT4), ("character_octet_length", INT4), ("numeric_precision", INT4), ("numeric_precision_radix", INT4),
        ("numeric_scale", INT4), ("datetime_precision", INT4), ("interval_type", TEXT), ("interval_precision", INT4), ("character_set_catalog", TEXT), ("character_set_schema", TEXT),
        ("character_set_name", TEXT), ("collation_catalog", TEXT), ("collation_schema", TEXT), ("collation_name", TEXT), ("domain_catalog", TEXT), ("domain_schema", TEXT), ("domain_name", TEXT),
        ("udt_catalog", TEXT), ("udt_schema", TEXT), ("udt_name", TEXT), ("scope_catalog", TEXT), ("scope_schema", TEXT), ("scope_name", TEXT), ("maximum_cardinality", INT4),
        ("dtd_identifier", TEXT), ("is_self_referencing", TEXT), ("is_identity", TEXT), ("identity_generation", TEXT), ("identity_start", TEXT), ("identity_increment", TEXT),
        ("identity_maximum", TEXT), ("identity_minimum", TEXT), ("identity_cycle", TEXT), ("is_generated", TEXT), ("generation_expression", TEXT), ("is_updatable", TEXT)], cols);
    add!("views", &[("table_catalog", TEXT), ("table_schema", TEXT), ("table_name", TEXT), ("view_definition", TEXT), ("check_option", TEXT), ("is_updatable", TEXT),
        ("is_insertable_into", TEXT), ("is_trigger_updatable", TEXT), ("is_trigger_deletable", TEXT), ("is_trigger_insertable_into", TEXT)],
        l.rels.iter().filter(|r| r.kind == 'v').map(|r| vec![db(), s(r.schema.clone()), s(r.name.clone()), s(r.sql.clone().unwrap_or_default()), s("NONE"), s("NO"), s("NO"), s("NO"), s("NO"), s("NO")]).collect());
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
    for r in &l.rels {
        for (name, _) in &r.checks {
            table_constraints.push(vec![db(), s(r.schema.clone()), s(name.clone()), db(), s(r.schema.clone()), s(r.name.clone()), s("CHECK"), s("NO"), s("NO"), s("YES"), n()]);
        }
    }
    add!("table_constraints", &[("constraint_catalog", TEXT), ("constraint_schema", TEXT), ("constraint_name", TEXT), ("table_catalog", TEXT), ("table_schema", TEXT), ("table_name", TEXT),
        ("constraint_type", TEXT), ("is_deferrable", TEXT), ("initially_deferred", TEXT), ("enforced", TEXT), ("nulls_distinct", TEXT)], table_constraints);
    add!("key_column_usage", &[("constraint_catalog", TEXT), ("constraint_schema", TEXT), ("constraint_name", TEXT), ("table_catalog", TEXT), ("table_schema", TEXT), ("table_name", TEXT),
        ("column_name", TEXT), ("ordinal_position", INT4), ("position_in_unique_constraint", INT4)], key_columns);
    add!("routines", &[("specific_catalog", TEXT), ("specific_schema", TEXT), ("specific_name", TEXT), ("routine_catalog", TEXT), ("routine_schema", TEXT), ("routine_name", TEXT),
        ("routine_type", TEXT), ("data_type", TEXT), ("routine_body", TEXT), ("routine_definition", TEXT), ("external_language", TEXT), ("is_deterministic", TEXT)],
        l.routines.iter().map(|(oid, schema, name, r)| {
            let procedure = r.kind == crate::expander::Kind::Procedure;
            vec![db(), s(schema.clone()), s(format!("{name}_{oid}")), db(), s(schema.clone()), s(name.clone()), s(if procedure { "PROCEDURE" } else { "FUNCTION" }), r.returns.clone().map_or(n(), s),
                s(if r.language == "python" { "EXTERNAL" } else { "SQL" }), if crate::auth::shows_code() { s(r.body.clone()) } else { n() }, s(if r.language.is_empty() { "SQL".to_string() } else { r.language.to_uppercase() }), s("NO")]
        }).collect());
    add!("sequences", &[("sequence_catalog", TEXT), ("sequence_schema", TEXT), ("sequence_name", TEXT), ("data_type", TEXT), ("numeric_precision", INT4),
        ("numeric_precision_radix", INT4), ("numeric_scale", INT4), ("start_value", TEXT), ("minimum_value", TEXT), ("maximum_value", TEXT), ("increment", TEXT), ("cycle_option", TEXT)],
        l.sequences.iter().map(|q| {
            let (data_type, bits) = match q.shape.as_type { "SMALLINT" => ("smallint", 16), "INTEGER" => ("integer", 32), _ => ("bigint", 64) };
            vec![db(), s(q.schema.clone()), s(q.name.clone()), s(data_type), i4(bits), i4(2), i4(0), s(q.shape.start.to_string()), s(q.shape.min.to_string()),
                s(q.shape.max.to_string()), s(q.shape.increment.to_string()), yes(q.shape.cycle)]
        }).collect());
    add!("referential_constraints", &[("constraint_catalog", TEXT), ("constraint_schema", TEXT), ("constraint_name", TEXT), ("unique_constraint_catalog", TEXT),
        ("unique_constraint_schema", TEXT), ("unique_constraint_name", TEXT), ("match_option", TEXT), ("update_rule", TEXT), ("delete_rule", TEXT)], referential);
    add!("constraint_column_usage", &[("table_catalog", TEXT), ("table_schema", TEXT), ("table_name", TEXT), ("column_name", TEXT), ("constraint_catalog", TEXT),
        ("constraint_schema", TEXT), ("constraint_name", TEXT)], used);
    // Privileges, as each grantee holds them (`users.rs`): SELECT, INSERT, UPDATE and DELETE on a table
    // name it, on a schema (every table of it) or on the lake; columns only with SELECT on a table. A
    // table granted twice is one row. Routines have no privileges here.
    let (mut tables_granted, mut columns_granted) = (BTreeSet::new(), BTreeSet::new()); // (grantee, schema, table, privilege or column)
    for m in &l.members {
        for g in m.grants.iter().filter(|g| ["select", "insert", "update", "delete"].contains(&g.privilege.as_str())) {
            let relations: Vec<&Rel> = match &g.on {
                On::Table(t) => {
                    let (schema, table) = crate::text::split(t);
                    l.rels.iter().filter(|r| r.schema == schema && r.name == table).collect()
                }
                On::Schema(schema) => l.rels.iter().filter(|r| r.schema == *schema).collect(),
                On::Lake => l.rels.iter().collect(),
                _ => vec![],
            };
            for r in relations.into_iter().filter(|r| r.schema != TEMP) {
                if g.columns.is_empty() {
                    tables_granted.insert((m.name.clone(), r.schema.clone(), r.name.clone(), g.privilege.to_uppercase()));
                } else {
                    columns_granted.extend(g.columns.iter().map(|c| (m.name.clone(), r.schema.clone(), r.name.clone(), c.clone())));
                }
            }
        }
    }
    add!("table_privileges", &[("grantor", TEXT), ("grantee", TEXT), ("table_catalog", TEXT), ("table_schema", TEXT), ("table_name", TEXT), ("privilege_type", TEXT),
        ("is_grantable", TEXT), ("with_hierarchy", TEXT)],
        tables_granted.iter().map(|(grantee, schema, name, privilege)| vec![s(l.user.clone()), s(grantee.clone()), db(), s(schema.clone()), s(name.clone()), s(privilege.clone()), s("NO"), s("NO")]).collect());
    add!("column_privileges", &[("grantor", TEXT), ("grantee", TEXT), ("table_catalog", TEXT), ("table_schema", TEXT), ("table_name", TEXT), ("column_name", TEXT),
        ("privilege_type", TEXT), ("is_grantable", TEXT)],
        columns_granted.iter().map(|(grantee, schema, name, column)| vec![s(l.user.clone()), s(grantee.clone()), db(), s(schema.clone()), s(name.clone()), s(column.clone()), s("SELECT"), s("NO")]).collect());
    add!("routine_privileges", &[("grantor", TEXT), ("grantee", TEXT), ("specific_catalog", TEXT), ("specific_schema", TEXT), ("specific_name", TEXT), ("routine_catalog", TEXT),
        ("routine_schema", TEXT), ("routine_name", TEXT), ("privilege_type", TEXT), ("is_grantable", TEXT)], vec![]);
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

fn udf(name: &'static str, returns: fn(&[DataType]) -> DataType, body: impl Fn(&[ArrayRef], usize) -> datafusion::error::Result<ArrayRef> + Send + Sync + 'static) -> ScalarUDF {
    crate::pg_functions::function(name, Signature::one_of(vec![TypeSignature::Nullary, TypeSignature::VariadicAny], Volatility::Stable), returns, body)
}

/// `udf`, whose answers are JSON text: the column they make is marked json, so the port sends it as json.
fn json_udf(name: &'static str, body: impl Fn(&[ArrayRef], usize) -> datafusion::error::Result<ArrayRef> + Send + Sync + 'static) -> ScalarUDF {
    let signature = Signature::one_of(vec![TypeSignature::Nullary, TypeSignature::VariadicAny], Volatility::Stable);
    crate::pg_functions::marked(name, signature, |_| DataType::Utf8, Some("json"), body)
}

/// The sequence a table's identity column counts by, named as Postgres names it (`schema.name`);
/// None for a column that has none, or a name that isn't a table.
fn serial_sequence(l: &Lakes, table: &str, column: &str) -> Option<String> {
    let table = table.trim_matches('"');
    let (schema, name) = table.rsplit_once('.').map_or((None, table), |(s, t)| (Some(s.trim_matches('"')), t.trim_matches('"')));
    let rel = l.rels.iter().find(|r| r.kind != 'v' && r.name == name && schema.is_none_or(|s| s == r.schema))?;
    let id = rel.identity.get(column)?;
    let seq = l.sequences.iter().find(|q| q.lake_name == id.sequence)?;
    Some(format!("{}.{}", seq.schema, seq.name))
}

/// A column's identity options as the JSON Postgres's query gives (`identity_of`): its sequence's
/// options, or None for a column that isn't an identity column (`attidentity` blank).
fn identity_options(l: &Lakes, relid: u32, column: &str, kind: &str) -> Option<String> {
    if kind.is_empty() {
        return None;
    }
    let rel = l.rels.iter().find(|r| r.oid == relid && r.kind != 'v')?;
    let id = rel.identity.get(column)?;
    let shape = &l.sequences.iter().find(|q| q.lake_name == id.sequence)?.shape;
    Some(serde_json::json!({"always": id.always, "start": shape.start, "increment": shape.increment, "minvalue": shape.min,
        "maxvalue": shape.max, "cache": shape.cache, "cycle": shape.cycle}).to_string())
}

fn register_functions(ctx: &SessionContext, l: Arc<Lakes>) {
    let to_text = |_: &[DataType]| DataType::Utf8;
    let to_bool = |_: &[DataType]| DataType::Boolean;
    let yes = |n: usize| -> ArrayRef { Arc::new(datafusion::arrow::array::BooleanArray::from(vec![true; n])) };
    let nulls = |n: usize| -> ArrayRef { new_null_array(&DataType::Utf8, n) };
    let lk = l.clone();
    ctx.register_udf(udf("format_type", to_text, move |a, n| {
        let (oids, mods) = (int_arg(&a[0])?, a.get(1).map(int_arg).transpose()?);
        Ok(texts_out((0..n).map(|i| lk.format_type(oids[i]? as u32, mods.as_ref().and_then(|m| m[i]).unwrap_or(-1) as i32))))
    }));
    ctx.register_udf(udf("pg_get_expr", to_text, |a, _| Ok(Arc::new(datafusion::arrow::compute::cast(&a[0], &DataType::Utf8)?))));
    // Visible: on the search path, which is `public` unless the session set one (and a session's
    // temporary schema, and the catalog's own), as in Postgres: `\dt` and SQLAlchemy's default
    // schema list only those.
    let off_path = |schema: &str| l.off_path(schema);
    let hidden: Arc<std::collections::HashSet<u32>> = Arc::new(l.rels.iter().filter(|r| off_path(&r.schema)).flat_map(|r| [r.oid, oid("i", &format!("{}.{}", r.schema, r.name))])
        .chain(l.routines.iter().filter(|(_, schema, ..)| off_path(schema)).map(|(o, ..)| *o)).collect());
    for name in ["pg_table_is_visible", "pg_function_is_visible"] {
        let hidden = hidden.clone();
        ctx.register_udf(udf(name, to_bool, move |a, n| {
            let oids = int_arg(&a[0])?;
            Ok(Arc::new((0..n).map(|i| oids[i].map(|o| !hidden.contains(&(o as u32)))).collect::<datafusion::arrow::array::BooleanArray>()))
        }));
    }
    // An enum type off the search path is hidden the same way (`\dT` lists the visible ones).
    let hidden_types: Arc<std::collections::HashSet<u32>> = Arc::new(l.types.iter().filter(|t| off_path(&t.schema)).flat_map(|t| [t.oid, t.array]).collect());
    ctx.register_udf(udf("pg_type_is_visible", to_bool, move |a, n| {
        let oids = int_arg(&a[0])?;
        Ok(Arc::new((0..n).map(|i| oids[i].map(|o| !hidden_types.contains(&(o as u32)))).collect::<datafusion::arrow::array::BooleanArray>()))
    }));
    for name in ["has_table_privilege", "has_schema_privilege", "has_database_privilege",
        "has_column_privilege", "has_any_column_privilege", "has_function_privilege"] {
        ctx.register_udf(udf(name, to_bool, move |_, n| Ok(yes(n))));
    }
    for name in ["pg_is_in_recovery", "pg_is_other_temp_schema"] {
        ctx.register_udf(udf(name, to_bool, move |_, n| Ok(Arc::new(datafusion::arrow::array::BooleanArray::from(vec![false; n.max(1)])))));
    }
    // COMMENT ON, read by `obj_description(oid, 'pg_class')` and `col_description(oid, attnum)` (psql's `\d+`
    // asks them for each row): a relation's by its oid, a column's by its number.
    let by_oid: Arc<BTreeMap<u32, (Option<String>, Vec<Option<String>>)>> = Arc::new(l.rels.iter().map(|r| {
        (r.oid, (r.comment.clone(), r.columns.iter().map(|(c, ..)| r.comments.get(c).cloned()).collect()))
    }).collect());
    let relations = by_oid.clone();
    ctx.register_udf(udf("obj_description", to_text, move |a, n| {
        let oids = int_arg(&a[0])?;
        let classes = match a.get(1) {
            Some(c) => text_arg(c)?,
            None => vec![Some("pg_class".to_string()); n],
        };
        Ok(texts_out((0..n).map(|i| {
            if classes[i].as_deref() != Some("pg_class") {
                return None;
            }
            relations.get(&(oids[i]? as u32))?.0.clone()
        })))
    }));
    let columns = by_oid.clone();
    ctx.register_udf(udf("col_description", to_text, move |a, n| {
        let (oids, nums) = (int_arg(&a[0])?, int_arg(&a[1])?);
        Ok(texts_out((0..n).map(|i| {
            let (_, notes) = columns.get(&(oids[i]? as u32))?;
            notes.get((nums[i]? as usize).checked_sub(1)?)?.clone()
        })))
    }));
    for name in ["shobj_description", "pg_get_triggerdef", "pg_get_ruledef", "pg_get_partkeydef", "pg_get_statisticsobjdef"] {
        ctx.register_udf(udf(name, to_text, move |_, n| Ok(nulls(n))));
    }
    // A role's oid names it. 10 is the session's user (every object's owner here, as before), and any
    // other oid Postgres's "unknown".
    let who = l.user.clone();
    let names: BTreeMap<u32, String> = role_rows(&l).into_iter().map(|(id, name, ..)| (id, name)).collect();
    ctx.register_udf(udf("pg_get_userbyid", to_text, move |a, n| {
        let ids = int_arg(&a[0])?;
        Ok(texts_out((0..n).map(|i| ids[i].map(|id| match id {
            10 => who.clone(),
            id => names.get(&(id as u32)).cloned().unwrap_or_else(|| format!("unknown (OID={id})")),
        }))))
    }));
    ctx.register_udf(udf("pg_encoding_to_char", to_text, |_, n| Ok(texts_out((0..n).map(|_| Some("UTF8".to_string()))))));
    ctx.register_udf(udf("pg_backend_pid", |_| DataType::Int32, |_, n| Ok(Arc::new(datafusion::arrow::array::Int32Array::from(vec![std::process::id() as i32; n.max(1)])))));
    ctx.register_udf(udf("current_setting", to_text, |a, n| {
        let names = text_arg(&a[0])?;
        Ok(texts_out((0..n).map(|i| SETTINGS.iter().find(|(k, _)| names[i].as_deref().is_some_and(|x| x.eq_ignore_ascii_case(k))).map(|(_, v)| v.to_string()))))
    }));
    let on_path: Vec<ScalarValue> = std::iter::once(s("pg_catalog")).chain(l.visible.iter().map(|v| s(v.as_str()))).collect();
    ctx.register_udf(udf("current_schemas", |_| list(DataType::Utf8), move |_, n| {
        let one = ScalarValue::List(ScalarValue::new_list_nullable(&on_path, &DataType::Utf8));
        Ok(ScalarValue::iter_to_array(std::iter::repeat_n(one, n.max(1)))?)
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
    let rels: Arc<Vec<(u32, String, String)>> = Arc::new(l.rels.iter().map(|r| (r.oid, r.schema.clone(), r.name.clone()))
        .chain(l.sequences.iter().map(|q| (q.oid, q.schema.clone(), q.name.clone())))
        .chain(CATALOG.iter().map(|(n, o)| (*o, "pg_catalog".to_string(), n.to_string()))).collect());
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
    let lk = l.clone();
    for (name, strict) in [("pondra_regtype", true), ("to_regtype", false)] {
        let lk = lk.clone();
        ctx.register_udf(udf(name, |t| if t.first().is_some_and(|t| t.is_integer()) { DataType::Utf8 } else { DataType::UInt32 }, move |a, n| {
            if a[0].data_type().is_integer() {
                let v = int_arg(&a[0])?;
                return Ok(texts_out((0..n).map(|i| v[i].and_then(|o| lk.format_type(o as u32, -1)))));
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
    let declared: BTreeMap<u32, String> = l.rels.iter().flat_map(|r| {
        let constraints = r.constraints.iter().map(move |c| (constraint_oid(r, c), c.def(&|n| crate::objects::ident(n))));
        let checks = r.checks.iter().map(move |(name, cond)| (check_oid(r, name), format!("CHECK (({cond}))")));
        constraints.chain(checks)
    }).collect();
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
            crate::expander::Kind::Procedure => String::new(),
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
    // A database's size is its relations' bytes, for this database by oid or name and NULL for another.
    // Pondra counts no scans, and every table is in the default tablespace.
    let (here, bytes) = (l.database.clone(), l.rels.iter().map(|r| r.bytes as i64).sum::<i64>());
    ctx.register_udf(udf("pg_database_size", |_| DataType::Int64, move |a, n| {
        let this: Vec<bool> = match a[0].data_type().is_integer() {
            true => int_arg(&a[0])?.iter().map(|id| *id == Some(16384)).collect(),
            false => text_arg(&a[0])?.iter().map(|name| name.as_deref() == Some(here.as_str())).collect(),
        };
        Ok(Arc::new((0..n).map(|i| this[i].then_some(bytes)).collect::<datafusion::arrow::array::Int64Array>()))
    }));
    ctx.register_udf(udf("pg_stat_get_numscans", |_| DataType::Int64, |_, n| Ok(Arc::new(datafusion::arrow::array::Int64Array::from(vec![0; n])))));
    ctx.register_udf(udf("pg_tablespace_location", to_text, |_, n| Ok(texts_out((0..n).map(|_| Some(String::new()))))));
    // A routine's CREATE statement, as `SHOW CREATE` writes it. A caller who may not see code gets NULL.
    let code = crate::auth::shows_code();
    let lk = l.clone();
    ctx.register_udf(udf("pg_get_functiondef", to_text, move |a, n| {
        let ids = int_arg(&a[0])?;
        Ok(texts_out((0..n).map(|i| {
            let (_, schema, name, r) = lk.routines.iter().find(|(id, ..)| Some(*id as i64) == ids[i])?;
            let lake_name = if schema == crate::text::PUBLIC { name.clone() } else { format!("{schema}.{name}") };
            code.then(|| crate::objects::routine_sql(&crate::objects::name_sql(&lake_name), r))
        })))
    }));
    // SQLAlchemy's identity query: the sequence a column counts by (its options are `identity_options`, below).
    let lk = l.clone();
    ctx.register_udf(udf("pg_get_serial_sequence", to_text, move |a, n| {
        let (tables, columns) = (text_arg(&a[0])?, text_arg(&a[1])?);
        Ok(texts_out((0..n).map(|i| serial_sequence(&lk, tables[i].as_deref()?, columns[i].as_deref()?))))
    }));
    // The identity options `identity_of` puts in place of SQLAlchemy's subquery: (attrelid, attname, attidentity).
    ctx.register_udf(json_udf("identity_options", move |a, n| {
        let (relids, columns, kinds) = (int_arg(&a[0])?, text_arg(&a[1])?, text_arg(&a[2])?);
        Ok(texts_out((0..n).map(|i| identity_options(&l, relids[i]? as u32, columns[i].as_deref()?, kinds[i].as_deref()?))))
    }));
}

/// Postgres's catalog, its functions and its `information_schema` in `ctx`, for a client logged in as `user`.
pub async fn register(ctx: &SessionContext, lake: &Lake, user: &str, sql: &str) -> Result<()> {
    let lower = sql.to_lowercase();
    // (a view's columns: its `attnum`s and a key's `conkey`, which name them, need them too)
    let columns = ["pg_attribute", "pg_constraint", "information_schema", "columns"].iter().any(|w| lower.contains(w));
    // (and the log's rows only for SQL that sizes tables: they cost a read of the log tail's entries)
    let counts = ["n_live_tup", "reltuples", "pg_stat"].iter().any(|w| lower.contains(w));
    let l = Arc::new(lakes(lake, user, columns, counts).await?);
    let catalog = ctx.catalog(&crate::ddl::lake_name(lake)).expect("the lake's catalog");
    let pg = Arc::new(MemorySchemaProvider::new());
    let want = |t: &str| lower.contains(t); // (a table's name, as the rewritten statement has it)
    for (name, t) in tables(&l, &want)? {
        pg.register_table(name.to_string(), t)?;
    }
    catalog.register_schema("pg_catalog", pg)?;
    let info = Arc::new(MemorySchemaProvider::new());
    for (name, t) in information_schema(&l, &want)? {
        info.register_table(name.to_string(), t)?;
    }
    catalog.register_schema(PG_INFO, info)?;
    register_functions(ctx, l);
    Ok(())
}

const PG_INFO: &str = "pg_information_schema"; // (DataFusion keeps `information_schema` to itself)
const INFO_TABLES: &[&str] = &["schemata", "tables", "columns", "views", "table_constraints", "key_column_usage", "routines", "referential_constraints", "constraint_column_usage", "sequences",
    "table_privileges", "column_privileges", "routine_privileges"];

// ---------------------------------------------------------------- Postgres's SQL, said DataFusion's way

/// `sql` with what only Postgres says said so DataFusion takes it: catalog tables by their
/// schema, `OPERATOR(pg_catalog.~)` as `~`, `::regclass` and its kin as functions, `COLLATE`
/// dropped, `current_user` as the user. Unchanged if it doesn't parse as Postgres's.
pub fn rewrite(sql: &str, user: &str, schema: &str) -> String {
    use datafusion::sql::sqlparser::{dialect::PostgreSqlDialect, parser::Parser};
    let Ok(mut stmts) = Parser::parse_sql(&PostgreSqlDialect {}, sql) else { return sql.to_string() };
    let mut r = Rewriter { user: if user.is_empty() { "pondra" } else { user }, schema, expanded: vec![] };
    for s in stmts.iter_mut() {
        // (a table a statement makes or writes keeps its name: Postgres makes `CREATE TABLE
        // pg_stat_user_tables (…)` in the current schema; only reads look in pg_catalog first)
        let written = target(s).cloned();
        let _ = s.visit(&mut r);
        if let (Some(name), Some(t)) = (written, target(s)) {
            *t = name;
        }
    }
    stmts.iter().map(crate::text::sql).collect::<Vec<_>>().join("; ")
}

/// The table a statement makes, alters or inserts into.
fn target(s: &mut ast::Statement) -> Option<&mut ast::ObjectName> {
    match s {
        ast::Statement::CreateTable(c) => Some(&mut c.name),
        ast::Statement::AlterTable(a) => Some(&mut a.name),
        ast::Statement::Insert(i) => match &mut i.table {
            ast::TableObject::TableName(n) => Some(n),
            _ => None,
        },
        _ => None,
    }
}

struct Rewriter<'a> {
    user: &'a str,
    schema: &'a str, // (the session's current schema: `current_schema()`)
    expanded: Vec<String>, // the names of columns that are `_pg_expandarray` elements (pgjdbc's `(keys).x`)
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
    matches!(name.as_str(), "typinput" | "typoutput" | "typreceive" | "typsend" | "typmodin" | "typmodout" | "typanalyze" | "typsubscript" | "aggfnoid" | "oprcode")
}

fn boolean(e: &ast::Expr) -> bool {
    static NAMES: std::sync::LazyLock<std::collections::HashSet<String>> = std::sync::LazyLock::new(|| {
        let empty = Lakes { database: String::new(), databases: vec![], user: String::new(), schemas: vec![], rels: vec![], sequences: vec![], types: vec![], routines: vec![], indexes: vec![], visible: vec![], members: vec![] };
        tables(&empty, &|_| true).unwrap_or_default().iter().flat_map(|(_, t)| datafusion::datasource::TableProvider::schema(t.as_ref()).fields().iter().filter(|f| *f.data_type() == DataType::Boolean).map(|f| f.name().clone()).collect::<Vec<_>>()).collect()
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

/// A column's name, lower-cased: the last part of `t.col`, or `col`.
fn column_name(e: &ast::Expr) -> Option<String> {
    match e {
        ast::Expr::Identifier(i) => Some(i.value.to_lowercase()),
        ast::Expr::CompoundIdentifier(p) => p.last().map(|i| i.value.to_lowercase()),
        _ => None,
    }
}

/// A projected int2vector or oidvector column as Postgres prints it: its elements, space-separated
/// (`1 2`). SQLAlchemy splits that text to read an index's keys, which a list can't give it. Only a
/// column projected alone changes; inside an expression it stays a list.
fn vector_as_text(item: &mut ast::SelectItem) {
    let text = |e: &ast::Expr| expr(&format!("array_to_string({e}, ' ')"));
    let new = match item {
        ast::SelectItem::UnnamedExpr(e) if vector(e) => match (text(e), column_name(e)) {
            (Some(t), Some(n)) => ast::SelectItem::ExprWithAlias { expr: t, alias: ast::Ident::new(n) },
            _ => return,
        },
        ast::SelectItem::ExprWithAlias { expr: e, alias } if vector(e) => match text(e) {
            Some(t) => ast::SelectItem::ExprWithAlias { expr: t, alias: alias.clone() },
            None => return,
        },
        _ => return,
    };
    *item = new;
}

/// Catalog tables that are always empty here: a subquery that joins one gives no row.
const EMPTY: &[&str] = &["pg_publication_namespace", "pg_publication_rel", "pg_shdescription", "pg_depend", "pg_rewrite", "pg_inherits", "pg_collation", "pg_extension", "pg_range", "pg_trigger",
    "pg_foreign_table", "pg_partitioned_table", "pg_policy", "pg_statistic_ext", "pg_publication", "pg_event_trigger", "pg_cast", "pg_opclass", "pg_stat_activity", "pg_policies",
    "pg_default_acl", "pg_db_role_setting", "pg_aggregate", "pg_conversion", "pg_foreign_data_wrapper", "pg_foreign_server", "pg_available_extensions"];

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

/// SQLAlchemy's identity query for a column: a scalar subquery of `pg_sequence` correlated through
/// `pg_get_serial_sequence`, which DataFusion can't decorrelate. It becomes `identity_options` over the
/// same outer row (the same JSON, or NULL), so no subquery is left to plan.
fn identity_of(q: &ast::Query) -> Option<ast::Expr> {
    let ast::SetExpr::Select(sel) = q.body.as_ref() else { return None };
    let [t] = &sel.from[..] else { return None };
    let ast::TableFactor::Table { name, .. } = &t.relation else { return None };
    if !name.0.last().and_then(|p| p.as_ident()).is_some_and(|i| i.value.eq_ignore_ascii_case("pg_sequence")) {
        return None;
    }
    let mut serial = false;
    let mut outer = None; // the table the column is read from: `pg_catalog.pg_attribute` for `…pg_attribute.attidentity`
    let _ = ast::visit_expressions(sel.selection.as_ref()?, |x| {
        match x {
            ast::Expr::Function(f) if f.name.to_string().eq_ignore_ascii_case("pg_get_serial_sequence") => serial = true,
            ast::Expr::CompoundIdentifier(p) if p.len() > 1 && p.last().is_some_and(|i| i.value.eq_ignore_ascii_case("attidentity")) => {
                outer = Some(p[..p.len() - 1].iter().map(|i| i.value.clone()).collect::<Vec<_>>().join("."));
            }
            _ => {}
        }
        ControlFlow::<()>::Continue(())
    });
    if !serial {
        return None;
    }
    let from = outer?;
    expr(&format!("identity_options({from}.attrelid, {from}.attname, {from}.attidentity)"))
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

/// One relation of a FROM, from its text (`(SELECT …) AS a`).
fn factor(sql: &str) -> Option<ast::TableFactor> {
    use datafusion::sql::sqlparser::{dialect::PostgreSqlDialect, parser::Parser};
    let q = Parser::new(&PostgreSqlDialect {}).try_with_sql(&format!("SELECT * FROM {sql}")).ok()?.parse_query().ok()?;
    match *q.body {
        ast::SetExpr::Select(s) => s.from.into_iter().next().map(|t| t.relation),
        _ => None,
    }
}

/// Scalar functions clients call in a FROM, as Postgres lets them: one row, its value a column named
/// for the alias (Grafana lists tables with `FROM …, string_to_array(current_setting('search_path'), ',') s`).
const SCALAR_IN_FROM: &[&str] = &["string_to_array", "regexp_split_to_array", "current_schemas", "current_setting"];

/// Table functions of one column, which Postgres names for its alias: `generate_series(1, 3) AS i`
/// gives a column `i` (DataFusion's is `value`).
const ONE_COLUMN: &[&str] = &["generate_series"];

/// A function's last name, lower-cased (`pg_catalog.aclexplode` is `aclexplode`).
fn last_name(name: &ast::ObjectName) -> String { name.0.last().and_then(|p| p.as_ident()).map(|i| i.value.to_lowercase()).unwrap_or_default() }

/// `aclexplode(acl)`'s rows, of which there are none: every ACL in Pondra's catalog is NULL (its
/// grants are in `pondra.grants` and `information_schema.table_privileges`).
const NO_ACL: &str = "SELECT CAST(NULL AS INT UNSIGNED) AS grantor, CAST(NULL AS INT UNSIGNED) AS grantee, CAST(NULL AS TEXT) AS privilege_type, CAST(NULL AS BOOLEAN) AS is_grantable WHERE false";

/// `ARRAY(SELECT x FROM … WHERE … ORDER BY k)` as `(SELECT array_agg(x ORDER BY k) FROM … WHERE …)`.
/// DataFusion can't plan a derived table of such a query when its filter uses the outer row (psql's
/// `\dT+` filters `pg_enum` on the outer `t.oid`: "No field named __a.enumtypid"), and it plans the
/// aggregate as it does `string_agg`. Only a plain query of one column is changed. The aggregate of no
/// rows is NULL, so it is coalesced to an empty list: Postgres's `{}`.
fn array_of_rows(q: &ast::Query) -> Option<ast::Expr> {
    let ast::SetExpr::Select(select) = q.body.as_ref() else { return None };
    let item = match select.projection.as_slice() {
        [ast::SelectItem::UnnamedExpr(item) | ast::SelectItem::ExprWithAlias { expr: item, .. }] => item,
        _ => return None,
    };
    let plain = select.distinct.is_none() && select.having.is_none() && select.qualify.is_none() && select.named_window.is_empty()
        && matches!(&select.group_by, ast::GroupByExpr::Expressions(by, mods) if by.is_empty() && mods.is_empty());
    if !plain || q.limit_clause.is_some() || q.fetch.is_some() {
        return None;
    }
    let order = q.order_by.as_ref().map(|o| format!(" {o}")).unwrap_or_default();
    let agg = expr(&format!("array_agg({item}{order})"))?;
    let mut out = q.clone();
    out.order_by = None;
    if let ast::SetExpr::Select(s) = out.body.as_mut() {
        s.projection = vec![ast::SelectItem::UnnamedExpr(agg)];
    }
    expr(&format!("COALESCE(({out}), make_array())"))
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

    fn pre_visit_table_factor(&mut self, f: &mut ast::TableFactor) -> ControlFlow<()> {
        let replaced = match f {
            ast::TableFactor::Table { name, args: Some(args), alias, .. } => match last_name(name).as_str() {
                n if ONE_COLUMN.contains(&n) => {
                    if let Some(a) = alias.as_mut().filter(|a| a.columns.is_empty()) {
                        a.columns = vec![ast::TableAliasColumnDef::from_name(a.name.value.clone())];
                    }
                    None
                }
                n if SCALAR_IN_FROM.contains(&n) => {
                    let named = alias.as_ref().map_or_else(|| ast::Ident::new(n), |a| a.name.clone());
                    let column = alias.as_ref().and_then(|a| a.columns.first()).map_or_else(|| named.clone(), |c| c.name.clone());
                    let given = args.args.iter().map(|a| a.to_string()).collect::<Vec<_>>().join(", ");
                    factor(&format!("(SELECT {name}({given}) AS {column}) AS {named}"))
                }
                "aclexplode" => factor(&format!("({NO_ACL}) AS {}", alias.as_ref().map_or("aclexplode".into(), |a| a.name.to_string()))),
                _ => None,
            },
            // (pgjdbc's and DBeaver's `CROSS JOIN LATERAL aclexplode(c.relacl) AS acl`)
            ast::TableFactor::Function { name, alias, .. } if last_name(name) == "aclexplode" => {
                factor(&format!("({NO_ACL}) AS {}", alias.as_ref().map_or("aclexplode".into(), |a| a.name.to_string())))
            }
            _ => None,
        };
        if let Some(r) = replaced {
            *f = r;
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_select(&mut self, select: &mut ast::Select) -> ControlFlow<()> {
        for item in select.projection.iter_mut() {
            if let ast::SelectItem::ExprWithAlias { expr: ast::Expr::Function(f), alias } = item {
                if f.name.to_string().to_lowercase().ends_with("_pg_expandarray") {
                    self.expanded.push(alias.value.to_lowercase());
                }
            }
            vector_as_text(item);
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
        // `.f` as written, or the quoted name `friendly.rs` made of it where SQL runs (a subscript).
        let field = match access_chain.as_slice() {
            [ast::AccessExpr::Dot(ast::Expr::Identifier(f))] => f.value.to_lowercase(),
            [ast::AccessExpr::Subscript(ast::Subscript::Index { index: ast::Expr::Value(v) })] => match &v.value {
                ast::Value::SingleQuotedString(f) => f.to_lowercase(),
                _ => return ControlFlow::Continue(()),
            },
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
            // (DBeaver's `(aclexplode(relacl)).grantee`: no ACL, so no grantee, as `NO_ACL` says)
            ast::Expr::Function(f) if last_name(&f.name) == "aclexplode" => match field.as_str() {
                "grantor" | "grantee" => expr("CAST(NULL AS INT UNSIGNED)"),
                "is_grantable" => expr("CAST(NULL AS BOOLEAN)"),
                _ => expr("CAST(NULL AS TEXT)"),
            },
            // (pgjdbc's `(result.keys).x`: the element itself, for a column `_pg_expandarray` made; any
            // other `(x).f` is a struct's field, as friendly.rs has it)
            x @ (ast::Expr::CompoundIdentifier(_) | ast::Expr::Identifier(_)) if field == "x" && column_name(x).is_some_and(|n| self.expanded.contains(&n)) => Some(x.clone()),
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
            // `CASE WHEN x IN (SELECT …)` (Grafana's tables on the search path): DataFusion takes an IN over
            // a subquery only in a filter, so here it asks whether the subquery's values, gathered, hold x.
            // Only in a WHEN, where NULL and false alike go on to the next branch.
            ast::Expr::Case { conditions, .. } => {
                for w in conditions.iter_mut() {
                    if let ast::Expr::InSubquery { expr: x, subquery, negated: false } = &w.condition {
                        if let Some(held) = expr(&format!("array_has((SELECT array_agg(pondra_in) FROM ({subquery}) AS pondra_in_q (pondra_in)), {x})")) {
                            w.condition = held;
                        }
                    }
                }
                None
            }
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
            // that way), none: Pondra's objects have no options. Otherwise the aggregate form.
            ast::Expr::Function(f) if f.name.to_string().eq_ignore_ascii_case("array") && matches!(f.args, ast::FunctionArguments::Subquery(_)) => {
                let ast::FunctionArguments::Subquery(q) = &f.args else { unreachable!() };
                let text = q.to_string();
                match (text.to_lowercase().contains("unnest("), q.body.as_ref()) {
                    (true, _) => expr("CAST(NULL AS VARCHAR[])"),
                    (false, ast::SetExpr::Select(_)) => array_of_rows(q),
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
                    Some("current_user" | "session_user" | "current_role" | "user") if bare => Some(ast::Expr::Value(ast::Value::SingleQuotedString(self.user.to_string()).into())),
                    // (beside `unnest(a)` in a select list, each element's place: zipped, as Postgres does)
                    Some("generate_subscripts") if !args.is_empty() => expr(&format!("unnest(generate_series(1, cardinality({})))", args[0])),
                    Some("current_schema") => Some(ast::Expr::Value(ast::Value::SingleQuotedString(self.schema.to_string()).into())),
                    Some("current_database" | "current_catalog") => None,
                    _ => None,
                }
            }
            // A scalar subquery Postgres takes as one row (psql's, over the catalog): DataFusion takes
            // it once it's aggregated, and here it holds one row or none.
            ast::Expr::Subquery(q) => {
                if let Some(replacement) = identity_of(&**q).or_else(|| settle(q)) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::users::{Grant, User};
    use datafusion::arrow::util::display::array_value_to_string;

    #[test]
    fn postgres_from_said_datafusion_way() {
        let r = |s: &str| rewrite(s, "ann", "public");
        assert_eq!(r("SELECT user, current_user"), "SELECT 'ann', 'ann'");
        // a table made or written under a catalog table's name is the current schema's, as in Postgres
        assert_eq!(r("CREATE TABLE pg_stat_user_tables (relid BIGINT)"), "CREATE TABLE pg_stat_user_tables (relid BIGINT)");
        let i = r("INSERT INTO pg_stat_user_tables SELECT oid FROM pg_class");
        assert!(i.starts_with("INSERT INTO pg_stat_user_tables SELECT") && i.contains("FROM pg_catalog.pg_class"), "{i}");
        // Grafana's search path: a one-column function named for its alias, a scalar function in FROM
        let g = r("SELECT trim(s[i]) FROM generate_series(1, 2) AS i, string_to_array(current_setting('search_path'), ',') s");
        assert!(g.contains("generate_series(1, 2) AS i (i)"), "{g}");
        assert!(g.contains("(SELECT string_to_array(current_setting('search_path'), ',') AS s) AS s"), "{g}");
        assert!(r("SELECT * FROM generate_series(1, 2) AS s(r)").contains("AS s (r)"));
        assert!(r("SELECT x FROM current_schemas(false) AS t(x)").contains("(SELECT current_schemas(false) AS x) AS t"));
        // DBeaver's ACLs: none, so no rows and no grantee
        let a = r("SELECT acl.grantee FROM pg_class c CROSS JOIN LATERAL pg_catalog.aclexplode(c.relacl) AS acl");
        assert!(a.contains("WHERE false) AS acl") && !a.contains("aclexplode"), "{a}");
        let c = r("SELECT CASE WHEN table_schema IN (SELECT 'public') THEN 0 ELSE 1 END FROM information_schema.tables");
        assert!(c.contains("WHEN array_has((SELECT array_agg(pondra_in) FROM (SELECT 'public') AS pondra_in_q (pondra_in)), table_schema)"), "{c}");
        let e = r("SELECT (aclexplode(relacl)).grantee AS g FROM pg_class");
        assert!(e.contains("CAST(NULL AS INT UNSIGNED) AS g") && !e.contains("aclexplode"), "{e}");
    }

    /// A relation of the sample lake, as `lakes()` makes one from the catalog.
    fn rel(name: &str, kind: char, rows: u64) -> Rel {
        let (schema, table) = crate::text::split(name);
        Rel {
            oid: oid("r", name), schema: schema.into(), name: table.into(), kind,
            columns: vec![("id".into(), DataType::Int64, true, None), ("region".into(), DataType::Utf8, false, None)],
            key: vec![], sql: None, identity: BTreeMap::new(), constraints: vec![], checks: vec![], typed: BTreeMap::new(),
            comment: None, comments: BTreeMap::new(), rows, bytes: if kind == 'v' { 0 } else { 4096 },
        }
    }

    /// A lake of three relations and a sequence. `analyst` is a role with SELECT on `sales` and on schema
    /// `crm`; `u` is a user in `analyst` with INSERT on `sales` and SELECT on its `region`; `pondra` is
    /// the lake's first user, listed once (as the bootstrap role).
    fn sample() -> Lakes {
        let grant = |privilege: &str, on: On, columns: &[&str]| Grant { privilege: privilege.into(), on, columns: columns.iter().map(|c| c.to_string()).collect() };
        let users = vec![
            ("u/pondra".to_string(), User { login: true, superuser: true, ..Default::default() }),
            ("u/analyst".to_string(), User { grants: vec![grant("select", On::Table("sales".into()), &[]), grant("select", On::Schema("crm".into()), &[])], ..Default::default() }),
            ("u/u".to_string(), User { login: true, roles: vec!["analyst".into()], grants: vec![grant("insert", On::Table("sales".into()), &[]), grant("select", On::Table("sales".into()), &["region"])], ..Default::default() }),
        ];
        let sequence = SeqRel {
            oid: oid("sq", "sales_seq"), lake_name: "sales_seq".into(), schema: "public".into(), name: "sales_seq".into(),
            shape: crate::seq::Shape { start: 1, increment: 1, min: 1, max: i64::MAX, cache: 1, cycle: false, as_type: "BIGINT" },
        };
        Lakes {
            indexes: vec![], database: "lake".into(), databases: vec!["lake".into()], user: "u".into(),
            schemas: vec![(2200, "public".into()), (oid("n", "crm"), "crm".into())],
            rels: vec![rel("sales", 'r', 20000), rel("crm.customers", 'r', 2), rel("sales_by_region", 'v', 0), rel("mv", 'm', 4)],
            sequences: vec![sequence], types: vec![], routines: vec![], visible: vec!["public".into()], members: members_of(&users, "u"),
        }
    }

    /// A session with every catalog table and function of the sample lake registered.
    async fn catalog() -> SessionContext {
        let l = Arc::new(sample());
        let ctx = SessionContext::new();
        for (name, t) in tables(&l, &|_| true).unwrap().into_iter().chain(information_schema(&l, &|_| true).unwrap()) {
            ctx.register_table(name, t).unwrap();
        }
        register_functions(&ctx, l);
        ctx
    }

    /// The first column of `sql`'s rows as text (NULL as `NULL`).
    async fn column(ctx: &SessionContext, sql: &str) -> Vec<String> {
        let mut out = vec![];
        for batch in ctx.sql(sql).await.expect(sql).collect().await.expect(sql) {
            let col = batch.column(0);
            for i in 0..batch.num_rows() {
                out.push(if col.is_null(i) { "NULL".to_string() } else { array_value_to_string(col, i).unwrap() });
            }
        }
        out
    }

    #[tokio::test]
    async fn statistics_list_tables_and_materialized_views() {
        let ctx = catalog().await;
        assert_eq!(column(&ctx, "SELECT relname FROM pg_stat_user_tables WHERE n_live_tup > 0 ORDER BY relname").await, vec!["customers", "mv", "sales"]);
        assert_eq!(column(&ctx, "SELECT n_live_tup FROM pg_stat_all_tables WHERE relname = 'sales'").await, vec!["20000"]);
        assert_eq!(column(&ctx, "SELECT count(*) FROM pg_stat_all_tables").await, vec!["3"]);
        assert_eq!(column(&ctx, "SELECT data_type FROM pg_sequences").await, vec!["bigint"]);
    }

    #[tokio::test]
    async fn roles_and_memberships() {
        let ctx = catalog().await;
        // The lake's first user is the bootstrap role's row, and only that.
        assert_eq!(column(&ctx, "SELECT count(*) FROM pg_roles WHERE rolname = 'pondra'").await, vec!["1"]);
        assert_eq!(column(&ctx, "SELECT rolcanlogin FROM pg_roles WHERE rolname = 'analyst'").await, vec!["false"]);
        assert_eq!(column(&ctx, "SELECT usename FROM pg_user ORDER BY usename").await, vec!["pondra", "u"]);
        let (analyst, u) = (oid("u", "analyst"), oid("u", "u"));
        assert_eq!(column(&ctx, &format!("SELECT count(*) FROM pg_auth_members WHERE roleid = {analyst} AND member = {u}")).await, vec!["1"]);
        assert_eq!(column(&ctx, &format!("SELECT pg_get_userbyid({analyst})")).await, vec!["analyst"]);
        assert_eq!(column(&ctx, "SELECT pg_get_userbyid(999)").await, vec!["unknown (OID=999)"]);
        assert_eq!(column(&ctx, "SELECT pg_get_userbyid(10)").await, vec!["u"]);
    }

    #[tokio::test]
    async fn privileges_of_tables_and_columns() {
        let ctx = catalog().await;
        let mut tables = column(&ctx, "SELECT concat_ws(':', grantee, table_schema, table_name, privilege_type) FROM table_privileges").await;
        tables.sort();
        assert_eq!(tables, vec!["analyst:crm:customers:SELECT", "analyst:public:sales:SELECT", "u:public:sales:INSERT"]);
        assert_eq!(column(&ctx, "SELECT concat_ws(':', grantee, table_name, column_name, privilege_type) FROM column_privileges").await, vec!["u:sales:region:SELECT"]);
        assert_eq!(column(&ctx, "SELECT count(*) FROM routine_privileges").await, vec!["0"]);
    }

    #[tokio::test]
    async fn sizes_and_settings() {
        let ctx = catalog().await;
        let bytes: u64 = sample().rels.iter().map(|r| r.bytes).sum();
        assert_eq!(column(&ctx, "SELECT pg_database_size('lake')").await, vec![bytes.to_string()]);
        assert_eq!(column(&ctx, "SELECT pg_database_size('other')").await, vec!["NULL"]);
        assert_eq!(column(&ctx, "SELECT reset_val FROM pg_settings WHERE name = 'search_path'").await, vec!["\"$user\", public"]);
    }

    #[tokio::test]
    async fn every_catalog_table_builds() {
        // (`table` refuses a row of the wrong width: this builds every table with rows in it)
        let l = sample();
        assert!(tables(&l, &|_| true).is_ok() && information_schema(&l, &|_| true).is_ok());
    }
}
