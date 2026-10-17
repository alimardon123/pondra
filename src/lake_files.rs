//! A lake's own files (`files/…`): the rules every form keeps for them, whatever holds the bytes. The node's
//! `PUT`, `GET` and `DELETE /files/…` (`server.rs`) and a page's (`page.rs`) answer with these words and these types,
//! so a client written for one speaks to the other (ADR-061's request contract). No lake here: a page builds it.

/// A path as `PUT /files/…` named it (`photos/a.jpg`), or as `files()` lists it (`files/photos/a.jpg`):
/// the object's place in the lake either way.
pub fn under_files(path: &str) -> String {
    let path = path.trim_start_matches('/');
    if path.starts_with("files/") { path.to_string() } else { format!("files/{path}") }
}

/// What a file replaced since it was read says: `store::Changed`'s `Display` writes it.
pub const CHANGED: &str = "the file changed since it was opened: open it again, or save under another name";

/// A `PUT` without `If-Match` to a path that is there already (409).
pub fn there_already(path: &str) -> String {
    format!("{path} is there already: send If-Match with the version you read to replace it, or put it under another name")
}

/// A `PUT` with `If-Match` to a file replaced since the version it named (412).
pub fn changed(path: &str) -> String {
    format!("{path}: {CHANGED}")
}

/// A `GET` or `DELETE` of a path no file is at (404).
pub fn not_found(path: &str) -> String {
    format!("no such file: {path}")
}

/// The content type a file is served as, by its extension.
pub fn content_type(path: &str) -> &'static str {
    let ext = path.rsplit('.').next().unwrap_or_default().to_ascii_lowercase();
    match ext.as_str() {
        "csv" => "text/csv; charset=utf-8",
        "tsv" => "text/tab-separated-values; charset=utf-8",
        "json" | "ipynb" => "application/json",
        "jsonl" | "ndjson" => "application/x-ndjson",
        "sql" | "py" | "md" | "txt" => "text/plain; charset=utf-8",
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "svg" => "image/svg+xml",
        "pdf" => "application/pdf",
        _ => "application/octet-stream",
    }
}
