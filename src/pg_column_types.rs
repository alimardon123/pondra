//! Postgres's column types that the lake holds as text, as `column_types` entries (step 5, E1): `uuid` one way, lower case
//! with hyphens; `json` as written, once it parses; `jsonb` as jsonb writes it. Registered by the postgres extension, and
//! what a write refuses of each is said here (`rules::typed` asks the entry).
use crate::column_types::ColumnType;
use datafusion::arrow::datatypes::DataType;

fn text(_: &[u64]) -> DataType { DataType::Utf8 }

pub const UUID: ColumnType = ColumnType { name: "uuid", extension: "pondra.uuid", storage: text, refusal: Some(uuid) };
pub const JSON: ColumnType = ColumnType { name: "json", extension: "arrow.json", storage: text, refusal: Some(json) };
pub const JSONB: ColumnType = ColumnType { name: "jsonb", extension: "pondra.jsonb", storage: text, refusal: Some(jsonb) };

/// Not a uuid, or one written another way than the column holds (22P02).
fn uuid(column: &str, v: &str) -> Option<(String, &'static str)> {
    if crate::text::is_uuid(v) {
        return None;
    }
    let said = match crate::pg_casts::uuid_of(v) {
        Ok(u) => format!("invalid input syntax for type uuid: \"{v}\" (column {column} holds a uuid one way, lower case with hyphens: write '{u}', or cast it, '{v}'::uuid)"),
        Err(_) => format!("invalid input syntax for type uuid: \"{v}\" (column {column} holds uuids)"),
    };
    Some((said, "22P02"))
}

fn json(column: &str, v: &str) -> Option<(String, &'static str)> { held_json("json", column, v) }

fn jsonb(column: &str, v: &str) -> Option<(String, &'static str)> { held_json("jsonb", column, v) }

/// Not JSON (22P02), or nested past the levels a reader takes (54001): the reader the functions use decides.
fn held_json(t: &str, column: &str, v: &str) -> Option<(String, &'static str)> {
    crate::pg_json_make::refused(v).map(|code| match code {
        "54001" => (format!("stack depth limit exceeded (column {column} holds {t}: a document nested past {} levels)", crate::pg_json_make::MAX_DEPTH), code),
        _ => (format!("{} (column {column} holds {t})", crate::pg_json::json_refusal(v)), code),
    })
}
