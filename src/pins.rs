//! Who still reads a lake's old versions (redesign step 5, piece 13): one list, by kind of reader. A branch made from the
//! lake reads its files as they were then (`branch`, its `pn/` pins); a consumer behind, a share's recipient, a long query
//! and an outside snapshot (Iceberg's and Delta's published versions) are kinds that can join it. Expiry
//! (`tier::expire`), the orphan sweep and a backup's garbage keep what the oldest reader still reads (`held`), each after
//! every kind lets go of its readers that are gone (`lapse`, each round; `sweep`, hourly).
use crate::registry::Section;
use crate::store::Lake;
use anyhow::Result;
use futures::future::BoxFuture;

/// The oldest and the newest version (commit time, ms) a kind's readers still read in a lake: None when none does.
pub type Held = for<'a> fn(&'a Lake) -> BoxFuture<'a, Result<Option<(u64, u64)>>>;

/// A kind's readers that are gone let go, before the rest are counted.
pub type Lapse = for<'a> fn(&'a Lake) -> BoxFuture<'a, Result<()>>;

/// A kind of reader that keeps a lake's old files.
pub struct Pin {
    pub name: &'static str,
    pub held: Held,
    /// Each retention round: its readers that stopped saying they read (a lease not renewed).
    pub lapse: Option<Lapse>,
    /// Hourly, with the orphan sweep: its readers that are gone, found by looking (a branch's lake deleted).
    pub sweep: Option<Lapse>,
}

/// Every kind, as the extensions add them.
#[derive(Default)]
pub struct Pins(Vec<Pin>);

impl Pins {
    pub fn add(&mut self, p: Pin) { self.0.push(p) }
    pub fn names(&self) -> impl Iterator<Item = &'static str> + '_ { self.0.iter().map(|p| p.name) }
}

impl Section for Pins {
    const NAME: &'static str = "pins";
    fn check(&self, _: &crate::registry::Registry) -> Result<(), String> {
        let mut seen = std::collections::BTreeSet::new();
        match self.0.iter().find(|p| !seen.insert(p.name)) {
            Some(p) => Err(format!("two kinds of reader are named {}", p.name)),
            None => Ok(()),
        }
    }
}

fn all() -> &'static [Pin] { crate::registry::get().find::<Pins>().map_or(&[], |p| &p.0) }

/// Each round, before `held`: every kind lets go of its readers that stopped saying they read.
pub async fn lapse(lake: &Lake) -> Result<()> {
    for f in all().iter().filter_map(|p| p.lapse) {
        f(lake).await?;
    }
    Ok(())
}

/// Hourly, with the orphan sweep: every kind lets go of its readers that are gone.
pub async fn sweep(lake: &Lake) -> Result<()> {
    for f in all().iter().filter_map(|p| p.sweep) {
        f(lake).await?;
    }
    Ok(())
}

/// The oldest and the newest version any reader still reads in this lake (None: none does). Files that were live then
/// stay (`tier::expire`), and so do objects older than the newest (the orphan sweep).
pub async fn held(lake: &Lake) -> Result<Option<(u64, u64)>> {
    let mut each = vec![];
    for p in all() {
        each.push((p.held)(lake).await?);
    }
    Ok(span(each))
}

/// The span the kinds' spans make together.
fn span(each: impl IntoIterator<Item = Option<(u64, u64)>>) -> Option<(u64, u64)> {
    each.into_iter().flatten().reduce(|(a, b), (c, d)| (a.min(c), b.max(d)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_oldest_reader_of_any_kind_holds_and_none_holds_nothing() {
        assert_eq!(span([None, None]), None);
        assert_eq!(span([Some((5, 9)), None, Some((3, 7))]), Some((3, 9)));
        assert!(crate::registry::get().section::<Pins>().names().any(|n| n == "branches"), "a branch is a kind of reader");
    }
}
