//! Postgres's type oids and names, as the server has them: `pg_typeof` answers from these in every form (`pg_casts.rs`),
//! and the catalog (`pg_catalog.rs`) lists them.

use datafusion::arrow::datatypes::DataType;

/// A Postgres type: oid, name, how `format_type` says it, length, category, its array's oid (or,
/// for an array, its element's).
pub struct PgType(pub u32, pub &'static str, pub &'static str, pub i16, pub char, pub u32);

pub const TYPES: &[PgType] = &[
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

pub fn arrays() -> impl Iterator<Item = (u32, String, String, u32)> {
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

/// `pg_typeof(x)`: the name of the Postgres type a value of this Arrow type is sent as.
pub fn type_of(t: &DataType) -> String {
    match t {
        DataType::Null => "unknown".into(), // (an untyped NULL, as Postgres says of one)
        t => format_type(pg_of(t).0, -1).unwrap_or_else(|| "text".into()),
    }
}

/// `format_type(oid, typmod)`.
pub fn format_type(oid: u32, typmod: i32) -> Option<String> {
    let name = TYPES.iter().find(|t| t.0 == oid).map(|t| t.2.to_string()).or_else(|| arrays().find(|a| a.0 == oid).map(|a| a.2))?;
    Some(match (oid, typmod) {
        (1700, m) if m >= 4 => format!("numeric({},{})", (m - 4) >> 16, (m - 4) & 0xffff),
        (1043, m) if m >= 4 => format!("character varying({})", m - 4), // (a VARCHAR(n) column: n + 4, as Postgres keeps it)
        (1042, m) if m >= 4 => format!("character({})", m - 4),
        _ => name,
    })
}
