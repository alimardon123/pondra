//! A lake's own files (`files/…`): the rules every form keeps for them, whatever holds the bytes. The node's
//! `PUT`, `GET` and `DELETE /files/…` (`server.rs`) and a page's (`page.rs`) answer with these words and these types,
//! so a client written for one speaks to the other (ADR-061's request contract). No lake here: a page builds it.

/// A path as `PUT /files/…` named it (`photos/a.jpg`), or as `files()` lists it (`files/photos/a.jpg`):
/// the object's place in the lake either way. A path whose first folder is `~` or `Shared` (with or without a leading `/`)
/// is the workspace's in 1.1 (ADR-054), beside the lakes, so it is refused here: a lake file can't take one.
pub fn under_files(path: &str) -> anyhow::Result<String> {
    let rel = path.trim_start_matches('/');
    if matches!(rel.split('/').next(), Some("~" | "Shared")) {
        return Err(crate::codes::coded("0A000", format!("{path}: paths like this are kept for the workspace each user gets in 1.1 (ADR-054), so put the file under another folder for now")));
    }
    Ok(if rel.starts_with("files/") { rel.to_string() } else { format!("files/{rel}") })
}

/// Does a name leave the folder it is read from? A `.` or `..` segment does, and so does its escaped form (`%2e`, in
/// either case): the url crate and DataFusion collapse both, so a name with either reaches outside `files/`. A backslash
/// separates segments too, as a Windows path writes them.
pub fn leaves(path: &str) -> bool {
    path.replace("%2e", ".").replace("%2E", ".").split(['/', '\\']).any(|p| p == "." || p == "..")
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

#[cfg(test)]
mod tests {
    use super::under_files;

    #[test]
    fn a_first_folder_of_the_workspace_is_refused_with_or_without_a_slash() {
        for path in ["~", "~/x", "/~/x", "Shared", "Shared/x", "/Shared/x", "//Shared/x"] {
            assert_eq!(crate::codes::of(&under_files(path).unwrap_err()), "0A000", "{path}");
        }
    }

    #[test]
    fn the_other_paths_are_lake_files_as_before() {
        assert_eq!(under_files("files/~/x").unwrap(), "files/~/x");
        assert_eq!(under_files("etl/Shared/x").unwrap(), "files/etl/Shared/x");
        assert_eq!(under_files("Sharedx/a").unwrap(), "files/Sharedx/a");
        assert_eq!(under_files("/etl/a.sql").unwrap(), "files/etl/a.sql");
    }
}
