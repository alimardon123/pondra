//! What a lake and its objects may be called (the catalog's): a table's name is its folder's, and a lake's is its own
//! folder's (`ddl.rs` reads both, and the statements that name them).
use crate::store::Lake;
use anyhow::Result;

/// What a schema, table or view may be called: letters, digits, `_` and `-`. A dot separates the
/// parts of a name; quotes, slashes and spaces would get in the way of paths and SQL.
pub fn check(part: &str) -> Result<()> {
    let ok = |c: char| c.is_alphanumeric() || c == '_' || c == '-';
    if !part.is_empty() && part.chars().all(ok) {
        return Ok(());
    }
    // (a table's name is its folder's name, and Windows refuses a quote or a slash in a file name: the
    // name to write is the same one with each such character as `_`)
    let fixed: String = part.chars().map(|c| if ok(c) { c } else { '_' }).collect();
    let hint = if fixed.is_empty() { String::new() } else { format!(": a table's name is its folder's, and Windows refuses \" and / in file names; name it {fixed}") };
    Err(crate::codes::coded("42602", format!("'{part}': names are letters, digits, _ and - (a dot separates schema and table){hint}")))
}

/// This lake's own name in three-part names: its folder's (or prefix's) last part (`mylake` of
/// `D:\data\mylake` too), and `memory` for a lake held in this process's memory, as DuckDB names its own.
pub fn lake_name(lake: &Lake) -> String {
    name_of(&lake.url)
}

/// `lake_name` of a lake at `url`. A lake in memory is `memory://<id>`, whose id would show in every name and error.
fn name_of(url: &str) -> String {
    if crate::memory_lake::in_memory(url) {
        return "memory".into();
    }
    url.trim_end_matches(['/', '\\']).rsplit(['/', '\\']).next().unwrap_or("lake").to_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A lake's name is its folder's or prefix's last part, in lower case; a lake in memory is `memory`, not its id.
    #[test]
    fn a_lakes_name_is_its_folders_and_memory_for_one_in_memory() {
        assert_eq!(name_of("/data/MyLake/"), "mylake");
        assert_eq!(name_of("D:\\data\\mylake"), "mylake");
        assert_eq!(name_of("s3://bucket/shop"), "shop");
        assert_eq!(name_of("memory://2965062e0e8442ce87cf27d8a2c4f13e"), "memory");
    }
}
