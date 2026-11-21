//! FILE (the every-data-kind review's E3; L6's words, 11-14): a file a row points at, the bytes staying where they are. A
//! value is its path, its version and its content type, held as a struct of the three (`path` never NULL), so a JSON
//! answer shows it as its object, an Arrow answer as its struct, and `photo.path`, `photo.content_type` read its parts.
//! Its text, what a CSV answer and `x::text` show and the Postgres door's `jsonb` is, is that object as Postgres writes
//! a jsonb: `{"path": "photos/a.jpg", "version": null, "content_type": "image/jpeg"}`.
//!
//! It is read from its text, that object (`version` and `content_type` may be left out), or from a path alone
//! (`'photos/a.jpg'::FILE`, `path::FILE FROM files('photos/')`), its content type then known by its extension, as `GET
//! /files` serves it (`lake_files::content_type`). A version left out is NULL, the file as it is when read: pinning the
//! version a path has as the value is made belongs to the stores work (E3).
use super::ColumnType;
use datafusion::arrow::array::{Array, ArrayRef, AsArray, StringArray, StructArray};
use datafusion::arrow::buffer::NullBuffer;
use datafusion::arrow::datatypes::{DataType, Field, Fields};
use std::sync::Arc;

pub const FILE: ColumnType = ColumnType { name: "file", extension: "pondra.file", storage, refusal: None, parse: Some(parse), show: Some(show) };

/// Its parts, in the order its text names them (Postgres's jsonb's: shorter keys first).
const PARTS: [&str; 3] = ["path", "version", "content_type"];

fn fields() -> Fields { Fields::from(PARTS.map(|p| Field::new(p, DataType::Utf8, p != "path")).to_vec()) }

fn storage(_: &[u64]) -> DataType { DataType::Struct(fields()) }

/// A value's parts, from its text: the object its text is, or a path alone. What is neither is refused as Postgres refuses
/// a value of a type (22P02), and so is an object with no path or with a key that isn't one of its parts.
pub fn parts(text: &str) -> anyhow::Result<[Option<String>; 3]> {
    let refused = || crate::codes::coded("22P02", format!("invalid input syntax for type file: \"{text}\""));
    if text.is_empty() {
        return Err(refused());
    }
    if !text.trim_start().starts_with('{') {
        return Ok([Some(text.to_string()), None, Some(crate::lake_files::content_type(text).to_string())]);
    }
    let serde_json::Value::Object(given) = serde_json::from_str(text).map_err(|_| refused())? else { return Err(refused()) };
    if given.keys().any(|k| !PARTS.contains(&k.as_str())) {
        return Err(refused());
    }
    let part = |k: &str| match given.get(k) {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(serde_json::Value::String(s)) => Ok(Some(s.clone())),
        Some(_) => Err(refused()),
    };
    let path = part("path")?.filter(|p| !p.is_empty()).ok_or_else(refused)?;
    let content_type = match given.contains_key("content_type") {
        true => part("content_type")?,
        false => Some(crate::lake_files::content_type(&path).to_string()),
    };
    Ok([Some(path), part("version")?, content_type])
}

/// A value's text: its object as Postgres writes a jsonb, each part named, a NULL one as `null`.
pub fn text(parts: [Option<&str>; 3]) -> String {
    let each = PARTS.iter().zip(parts).map(|(k, v)| format!("\"{k}\": {}", v.map_or("null".to_string(), |v| serde_json::Value::from(v).to_string())));
    format!("{{{}}}", each.collect::<Vec<_>>().join(", "))
}

/// Each text (Utf8) as a value, or the first that isn't one refused.
fn parse(c: &ArrayRef) -> anyhow::Result<ArrayRef> {
    let read = c.as_string::<i32>().iter().map(|v| v.map(parts).transpose()).collect::<anyhow::Result<Vec<_>>>()?;
    let nulls = NullBuffer::from(read.iter().map(Option::is_some).collect::<Vec<_>>());
    let part = |i: usize| -> ArrayRef { Arc::new(read.iter().map(|v| v.as_ref().and_then(|p| p[i].clone())).collect::<StringArray>()) };
    Ok(Arc::new(StructArray::try_new(fields(), (0..PARTS.len()).map(part).collect(), Some(nulls).filter(|n| n.null_count() > 0))?))
}

/// Each value as its text.
fn show(c: &ArrayRef) -> anyhow::Result<ArrayRef> {
    let c = match c.data_type() {
        DataType::Struct(_) => datafusion::arrow::compute::cast(c, &storage(&[]))?, // (its parts as text, a file's views read as Utf8View too)
        t => anyhow::bail!("a FILE's values are its parts, not {t}"),
    };
    let s = c.as_struct();
    let columns = PARTS.map(|p| s.column_by_name(p).and_then(|c| c.as_string_opt::<i32>()));
    let [Some(path), Some(version), Some(content_type)] = columns else { anyhow::bail!("a FILE's values are its parts, not {}", c.data_type()) };
    fn at(c: &StringArray, i: usize) -> Option<&str> { c.is_valid(i).then(|| c.value(i)) }
    Ok(Arc::new((0..s.len()).map(|i| s.is_valid(i).then(|| text([at(path, i), at(version, i), at(content_type, i)]))).collect::<StringArray>()))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A path alone, or its object; its text is the object, as Postgres writes a jsonb; what is neither is refused.
    #[test]
    fn a_file_is_read_from_a_path_or_its_object_and_shown_as_its_object() {
        let some = |p: [Option<&str>; 3]| p.map(|v| v.map(str::to_string));
        assert_eq!(parts("photos/a.jpg").unwrap(), some([Some("photos/a.jpg"), None, Some("image/jpeg")]), "by its extension");
        assert_eq!(parts("notes/a").unwrap()[2].as_deref(), Some("application/octet-stream"));
        assert_eq!(parts(r#"{"path": "a.png", "version": "1.x"}"#).unwrap(), some([Some("a.png"), Some("1.x"), Some("image/png")]));
        assert_eq!(parts(r#"{"content_type": null, "path": "a.png"}"#).unwrap(), some([Some("a.png"), None, None]), "a NULL said is kept");
        let shown = text([Some("photos/\"a\".jpg"), None, Some("image/jpeg")]);
        assert_eq!(shown, r#"{"path": "photos/\"a\".jpg", "version": null, "content_type": "image/jpeg"}"#);
        assert_eq!(parts(&shown).unwrap(), some([Some("photos/\"a\".jpg"), None, Some("image/jpeg")]), "its text read back");
        assert_eq!(parts("[1]").unwrap()[0].as_deref(), Some("[1]"), "a name, not an object, is a path");
        for bad in ["", "{", "{}", r#"{"version": "1"}"#, r#"{"path": ""}"#, r#"{"path": 1}"#, r#"{"path": "a", "size": 3}"#] {
            let e = parts(bad).unwrap_err();
            assert_eq!((crate::codes::of(&e), e.to_string()), ("22P02", format!("invalid input syntax for type file: \"{bad}\"")), "{bad}");
        }
    }

    /// A batch read into its struct, NULLs kept, and shown back; the struct's type reads back from its name, as a column's
    /// record keeps it.
    #[test]
    fn a_batch_is_read_and_shown_with_its_nulls() {
        let c: ArrayRef = Arc::new(StringArray::from(vec![Some("a.pdf"), None, Some(r#"{"path": "b", "version": "7"}"#)]));
        let read = parse(&c).unwrap();
        assert_eq!((read.data_type(), read.null_count()), (&storage(&[]), 1));
        let back = show(&read).unwrap();
        assert_eq!(back.as_string::<i32>().iter().collect::<Vec<_>>(), vec![
            Some(r#"{"path": "a.pdf", "version": null, "content_type": "application/pdf"}"#),
            None,
            Some(r#"{"path": "b", "version": "7", "content_type": "application/octet-stream"}"#),
        ]);
        assert_eq!(storage(&[]).to_string().parse::<DataType>().unwrap(), storage(&[]));
    }
}
