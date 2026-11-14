//! Other engines' table formats (step 5, the Iceberg review's I5 and I9; designs/redesign/redesign.md §6): each is an
//! entry that says what Pondra does with a table of it, so a door asks the entry and names no format. Iceberg's is the
//! `iceberg` extension's (`read_iceberg::FORMAT`), Delta's the `delta` extension's (`read_delta::FORMAT`). A table of
//! a catalog attached `(TYPE iceberg)` and a file table (`delta_scan('…')`) go through the same entry. What an entry
//! doesn't declare is refused naming the format and what it does take (`refused`).
use crate::store::{Lake, TableMeta};
use crate::table_record::Spec;
use anyhow::Result;
use futures::future::BoxFuture;

/// What Pondra does with a format's tables. One an entry doesn't declare is refused by name.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Ability {
    Read,
    /// `AT (…)` and the format's own version options: a read of its past.
    AsOf,
    Insert,
    CopyTo,
    Delete,
    Update,
    Merge,
    /// Its columns and partitioning changed. (This and the two below: declared so a refusal names them; no format takes
    /// them yet, step 9 and step 11 do.)
    #[allow(dead_code)]
    Alter,
    /// `RESTORE`: a write of its past (`AsOf` is a read).
    #[allow(dead_code)]
    Restore,
    /// Its own maintenance: `VACUUM`, `OPTIMIZE`.
    #[allow(dead_code)]
    Maintain,
}

impl Ability {
    /// As a refusal lists it.
    pub fn said(self) -> &'static str {
        match self {
            Ability::Read => "reads",
            Ability::AsOf => "AT",
            Ability::Insert => "INSERT",
            Ability::CopyTo => "COPY … TO",
            Ability::Delete => "DELETE",
            Ability::Update => "UPDATE",
            Ability::Merge => "MERGE",
            Ability::Alter => "ALTER",
            Ability::Restore => "RESTORE",
            Ability::Maintain => "VACUUM and OPTIMIZE",
        }
    }
}

/// A table's files as a format's current version lists them (`TableFormat::resolve`).
pub struct Resolved {
    pub meta: TableMeta,
    /// A folder whose files its catalog vouched for (a table's location, read with the catalog's credentials): its files
    /// need no secret of the caller's (`ext::check_files`).
    pub vouched: Option<String>,
    /// Done to the table once its files are checked: its files named as the store that reads them names them.
    pub then: Option<Box<dyn FnOnce(&mut TableMeta) + Send>>,
}

/// A table format: what a door asks instead of naming it.
pub struct TableFormat {
    /// As a spec's `format` names it (`ATTACH … (TYPE iceberg)`, `iceberg_scan`), lower case.
    pub name: &'static str,
    /// What Pondra does with its tables.
    pub abilities: &'static [Ability],
    /// The options a table of it takes (`version`, …).
    pub options: &'static [&'static str],
    /// Its current files, with their columns.
    pub resolve: for<'a> fn(&'a Lake, &'a Spec, &'a crate::rights::Scope) -> BoxFuture<'a, Result<Resolved>>,
    /// Before a node reads its share of a spread query: what it needs that the coordinator's list doesn't carry (an
    /// attached catalog's delegated store). None: nothing.
    pub prime: Option<for<'a> fn(&'a Lake, &'a Spec, &'a TableMeta, &'a crate::rights::Scope) -> BoxFuture<'a, Result<()>>>,
}

impl TableFormat {
    pub fn takes(&self, a: Ability) -> bool { self.abilities.contains(&a) }
}

/// The registry's table formats section.
#[derive(Default)]
pub struct TableFormats(Vec<&'static TableFormat>);

impl crate::registry::Section for TableFormats {
    const NAME: &'static str = "table formats";

    /// A format's name is lower case and no other's, and it reads: every other ability is of a table it can read.
    fn check(&self, _: &crate::registry::Registry) -> Result<(), String> {
        let mut seen = std::collections::HashSet::new();
        for f in &self.0 {
            if f.name != f.name.to_ascii_lowercase() || !seen.insert(f.name) {
                return Err(format!("{}: a table format's name is lower case and no other format's", f.name));
            }
            if !f.takes(Ability::Read) {
                return Err(format!("{}: a table format reads its tables", f.name));
            }
        }
        Ok(())
    }
}

impl TableFormats {
    pub fn add(&mut self, f: &'static TableFormat) { self.0.push(f) }
}

/// The table format a spec's `format` names, if one does.
pub fn named(format: &str) -> Option<&'static TableFormat> {
    crate::registry::get().find::<TableFormats>()?.0.iter().copied().find(|f| f.name == format)
}

/// The table formats, as a refusal lists them: `delta, iceberg`.
pub fn names() -> String {
    let mut n: Vec<&str> = crate::registry::get().find::<TableFormats>().map(|t| t.0.iter().map(|f| f.name).collect()).unwrap_or_default();
    n.sort_unstable();
    n.join(", ")
}

/// Is `format` a table format's (not a file format's, a topic's or a share's)?
pub fn is(format: &str) -> bool { named(format).is_some() }

/// `table`'s format may do `a`, or the refusal that says what it takes.
pub fn may(table: &str, format: &str, a: Ability) -> Result<&'static TableFormat> {
    match named(format) {
        Some(f) if f.takes(a) => Ok(f),
        Some(f) => Err(refused(table, f, a)),
        None => Err(anyhow::anyhow!("{table}: not another engine's table ({format})")),
    }
}

/// Why `f`'s tables can't do `a`: one sentence, from what the entry declares, so a door never words it.
pub fn refused(table: &str, f: &TableFormat, a: Ability) -> anyhow::Error {
    let takes: Vec<&str> = f.abilities.iter().map(|a| a.said()).collect();
    crate::codes::coded("0A000", format!("{table}: another engine's table ({}), of which Pondra takes {}; {}: not yet", f.name, takes.join(", "), a.said()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::{BuiltIn, Registry};

    fn none<'a>(_: &'a Lake, _: &'a Spec, _: &'a crate::rights::Scope) -> BoxFuture<'a, Result<Resolved>> { Box::pin(async { anyhow::bail!("none") }) }

    #[test]
    fn a_format_is_named_once_and_reads() {
        static A: TableFormat = TableFormat { name: "a", abilities: &[Ability::Read], options: &[], resolve: none, prime: None };
        static UPPER: TableFormat = TableFormat { name: "B", abilities: &[Ability::Read], options: &[], resolve: none, prime: None };
        static BLIND: TableFormat = TableFormat { name: "c", abilities: &[Ability::Insert], options: &[], resolve: none, prime: None };
        for (two, said) in [([&A, &A], "a: a table format's name"), ([&A, &UPPER], "B: a table format's name"), ([&A, &BLIND], "c: a table format reads")] {
            let built = std::panic::catch_unwind(|| {
                static TWO: std::sync::Mutex<Vec<&'static TableFormat>> = std::sync::Mutex::new(vec![]);
                *TWO.lock().unwrap() = two.to_vec();
                Registry::build(&[&BuiltIn { name: "x", requires: &[], register: |r| TWO.lock().unwrap().iter().for_each(|f| r.section_mut::<TableFormats>().add(*f)) }])
            });
            let e = built.err().and_then(|e| e.downcast_ref::<String>().cloned()).unwrap_or_default();
            assert!(e.contains(said), "{said}: {e}");
        }
    }

    #[test]
    #[cfg(feature = "lake")]
    fn the_builds_formats_and_a_refusal_says_what_one_takes() {
        assert!(is("iceberg") && is("delta") && !is("parquet") && !is("kafka"));
        let e = may("t", "iceberg", Ability::Update).err().map(|e| format!("{e:#}")).unwrap_or_default();
        assert!(e.contains("t: another engine's table (iceberg), of which Pondra takes reads, AT, INSERT, COPY … TO; UPDATE: not yet"), "{e}");
        assert!(may("t", "iceberg", Ability::Insert).is_ok());
    }
}
