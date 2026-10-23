//! A table's data file as the catalog records it, and what reading it needs: its partition values,
//! deletes and lineage. In the core because a page reads files too.
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Column -> (min, max), as text parsed with the column's type.
pub type Stats = BTreeMap<String, (String, String)>;

#[derive(Serialize, Deserialize, Clone, Default)]
pub struct DataFile {
    pub path: String,
    pub rows: u64,
    #[serde(default)]
    pub bytes: u64,
    /// Keyed tables: the last log segment this file covers. A row in a file with a higher `ord`
    /// is a newer version of its key, so several files can hold the same key (LSM-style) and
    /// tiering doesn't have to rewrite the whole table every time.
    #[serde(default)]
    pub ord: u64,
    /// A compaction output: the whole table, one row per key, no delete markers.
    #[serde(default)]
    pub whole: bool,
    /// Append tables: each column's min and max, so queries skip files without opening them.
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub stats: crate::data_file::Stats,
    /// Partitioned tables: the one partition value this file holds.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub part: String,
    /// Its rows carry their system columns (`sys.rs`); files written before round 19 don't.
    #[serde(default)]
    pub sys: bool,
    /// Append tables: the columns (of those with `stats`) that hold a NULL. None: not known (files
    /// written before round 15).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nulls: Option<Vec<String>>,
    /// A new file's distinct-value sketches (`sketch.rs`), on their way into its table's.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub sketch: BTreeMap<String, String>,
    /// Another engine's table's file (`scan.rs`): its partition values and deletes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outside: Option<Box<crate::scan::Outside>>,
    /// A file recorded as another engine wrote it (ADR-029 §1): its rows' system columns, which it
    /// doesn't hold, from here (`lake_scan::adopted`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lineage: Option<Lineage>,
    /// Rows deleted from it by position (ADR-029 §4), in Iceberg's position-delete files (paths in
    /// the lake) that name it: another engine's merge-on-read change, or Pondra's own changes
    /// (`tier::positions`). Reads skip them without decoding them (`lake_scan::lake_gone`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub deletes: Vec<crate::scan::Delete>,
    /// How many of its rows those delete (a file mostly deleted is rewritten: `tier::maintain`).
    #[serde(default, skip_serializing_if = "is_zero")]
    pub deleted: u64,
}

fn is_zero(n: &u64) -> bool { *n == 0 }

impl DataFile {
    /// The position-delete files that name it (paths in the lake).
    pub fn delete_files(&self) -> impl Iterator<Item = &String> {
        self.deletes.iter().filter_map(|d| match d {
            crate::scan::Delete::Positions { path, .. } => Some(path),
            _ => None,
        })
    }
}

/// Where a file's rows' system columns come from when it doesn't hold them: its first row's id
/// (the next rows' follow, by their place in the file), and the commit that recorded it (their
/// `_version`) and its time (`_created_at`, `_updated_at`). Iceberg v3's row lineage and Delta's
/// row tracking give a row its id the same way.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq)]
pub struct Lineage {
    pub first: i64,
    pub version: u64,
    pub ms: u64,
}
