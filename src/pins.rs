//! Who still reads a lake's old versions (redesign step 5, piece 13): one list, by kind of reader. A branch made from the
//! lake reads its files as they were then (`branch`, its `pn/` pins); a streaming task, a view run again and a Kafka
//! consumer group read the log (`tasks`, `rerun`, `kafka`); a statement still running reads the files its view lists
//! (`readings`, a node's and its followers'); a share's recipient and an outside snapshot (Iceberg's and Delta's published
//! versions) are kinds that can join them. Expiry (`tier::expire`), the orphan sweep and
//! a backup's garbage keep what the oldest reader still reads (`held`), each after every kind lets go of its readers that
//! are gone (`lapse`, each round; `sweep`, hourly), and expiry keeps the log's segments its readers still read (`floor`).
use crate::registry::Section;
use crate::store::Lake;
use anyhow::Result;
use futures::future::BoxFuture;

/// The oldest and the newest version (commit time, ms) a kind's readers still read in a lake: None when none does. Files
/// let go of since the oldest stay; objects older than the newest stay from the orphan sweep, since a reader may read one
/// nothing lists any more (a branch's), and a kind whose readers read only what the catalog lists says 0.
pub type Held = for<'a> fn(&'a Lake) -> BoxFuture<'a, Result<Option<(u64, u64)>>>;

/// A kind's readers that are gone let go, before the rest are counted.
pub type Lapse = for<'a> fn(&'a Lake) -> BoxFuture<'a, Result<()>>;

/// The log's segments a kind's readers still read: given the floor so far (segments at or below it go), the lowest
/// segment none of them still has to read, at most that floor.
pub type Floor = for<'a> fn(&'a Lake, u64) -> BoxFuture<'a, Result<u64>>;

/// A kind of reader that keeps a lake's old files.
pub struct Pin {
    pub name: &'static str,
    pub held: Held,
    /// Each retention round: its readers that stopped saying they read (a lease not renewed).
    pub lapse: Option<Lapse>,
    /// Hourly, with the orphan sweep: its readers that are gone, found by looking (a branch's lake deleted).
    pub sweep: Option<Lapse>,
    /// Readers of the log itself (a task's, a view run again, a consumer group's): the segments they still read stay,
    /// and so do the files those segments name.
    pub floor: Option<Floor>,
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

/// The log's floor: the segments at or below it go, as no reader of any kind still reads them (`floor`, the tables'
/// tiered mark first).
pub async fn floor(lake: &Lake, mut floor: u64) -> Result<u64> {
    for f in all().iter().filter_map(|p| p.floor) {
        floor = floor.min(f(lake, floor).await?);
    }
    Ok(floor)
}

/// A reader of the log that has read up to segment `done`: the floor it holds, `done` when a segment after it, up to
/// `floor`, has rows it reads (`reads`), and `floor` otherwise (a reader with nothing new holds nothing back).
pub async fn behind(lake: &Lake, done: u64, floor: u64, reads: impl Fn(&crate::store::Segment) -> bool) -> Result<u64> {
    use crate::store::seg_key;
    if done >= floor {
        return Ok(floor);
    }
    let unread = lake.cat.segments(&seg_key(done + 1), &seg_key(floor + 1)).await?.iter().any(|(_, s)| reads(s));
    Ok(if unread { done } else { floor })
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
        let names: Vec<_> = crate::registry::get().section::<Pins>().names().collect();
        for kind in ["branches", "tasks", "views run again", #[cfg(feature = "doors")] "consumer groups"] {
            assert!(names.contains(&kind), "{kind} are a kind of reader: {names:?}");
        }
    }
}
