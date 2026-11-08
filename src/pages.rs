//! An answer's other pages (ADR-034, round 29). The console is sent an answer's first 10,000 rows
//! (`answer::typed`), and the answer is kept here a while, under the id it is sent with, so its
//! other pages, and a download of every row, come from it: the same rows, in the same order,
//! without running it again.
//! Answers are kept within `PONDRA_PAGES_MB` (256) in all, for 20 minutes after they were last
//! read, the least lately read going first. A page of one no longer kept answers 410: the console
//! then runs the query again for that page.
//! Only whoever asked for an answer reads its pages: the id is a key to nothing else (S31), and
//! another's request for it answers 410 as if it were gone.
//! They are the state of the lake the answer came from (`Pages`, `lake_state.rs`): a page of it is asked of that lake's node.
use crate::ledger::ByteLru;
use crate::store::Lake;
use datafusion::arrow::record_batch::RecordBatch;
use std::sync::{Arc, Mutex};
use datafusion::common::instant::Instant;
use std::time::Duration;

struct Kept {
    holder: String, // (who asked: `auth::holder`)
    batches: Vec<RecordBatch>,
    used: Instant,
}

/// The answers a lake kept for their pages, by id, each charged its rows' memory: the lake's state (`Lake::state`), on its
/// line as `cache_bytes{cache="pages"}`. Each answer keeps who asked for it (its holder), which is what it is checked
/// against, never who is asking now.
pub struct Pages(Mutex<ByteLru<String, Kept>>);

impl crate::lake_state::LakeState for Pages {
    const NAME: &'static str = "pages";
    const ABOUT: &'static str = "answers the console was sent, kept a while for their later pages and downloads, each with who asked for it";

    fn make() -> Self { Pages(Mutex::new(ByteLru::new(budget()))) }

    fn cache(self: Arc<Self>) -> Option<Arc<dyn crate::ledger::Charged>> { Some(self) }
}

impl crate::ledger::Charged for Pages {
    fn held(&self) -> usize { self.0.lock().unwrap_or_else(|e| e.into_inner()).bytes() }

    fn give_back(&self, bytes: usize) -> usize { crate::ledger::give_back_from(&self.0, |c| c, bytes) }

    fn gives_back(&self) -> bool { true }

    /// Inside the whole line, unless `PONDRA_PAGES_MB` gives them a budget of their own.
    fn inside(&self) -> bool { crate::env::get("PONDRA_PAGES_MB").is_none() }
}

const IDLE: Duration = Duration::from_secs(20 * 60);

fn budget() -> usize { crate::env::value::<usize>("PONDRA_PAGES_MB") << 20 }

/// Keep an answer of `lake`'s: its id, or none when it alone is bigger than the budget.
pub fn keep(lake: &Lake, batches: &[RecordBatch]) -> Option<String> {
    let (bytes, budget) = (batches.iter().map(|b| b.get_array_memory_size()).sum::<usize>(), budget());
    if bytes > budget {
        return None;
    }
    let id = uuid::Uuid::new_v4().simple().to_string(); // (random: an id is known only to whoever was sent the answer)
    let holder = crate::auth::holder();
    let pages = lake.state::<Pages>().ok()?;
    let mut kept = pages.0.lock().unwrap();
    let mut left = kept.retain(|_, k| k.used.elapsed() < IDLE);
    left.extend(kept.set_most(budget));
    left.extend(kept.put(id.clone(), Kept { holder, batches: batches.to_vec(), used: Instant::now() }, bytes));
    drop(kept); // (what left is let go of outside the lock)
    Some(id)
}

/// Rows `from..from + n` of an answer `lake` kept (read now, so it is kept the longest), or none if it's gone,
/// or was asked for by someone else.
pub fn page(lake: &Lake, id: &str, from: usize, n: usize) -> Option<Vec<RecordBatch>> {
    let who = crate::auth::holder();
    let pages = lake.state::<Pages>().ok()?;
    let mut kept = pages.0.lock().unwrap();
    if !kept.peek(id).is_some_and(|k| k.holder == who && k.used.elapsed() < IDLE) {
        return None; // (another's answer is left as it is: as if gone)
    }
    let k = kept.get_mut(id)?;
    k.used = Instant::now();
    let (mut skip, mut left) = (from, n);
    let mut out: Vec<RecordBatch> = k.batches.first().map(|b| b.slice(0, 0)).into_iter().collect(); // (the columns, even past the end)
    for b in &k.batches {
        if left == 0 {
            break;
        }
        if skip >= b.num_rows() {
            skip -= b.num_rows();
            continue;
        }
        let take = left.min(b.num_rows() - skip);
        out.push(b.slice(skip, take));
        (skip, left) = (0, left - take);
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{ArrayRef, Int64Array};
    use std::sync::Arc;

    fn batch(from: i64, n: i64) -> RecordBatch { RecordBatch::try_from_iter(vec![("x", Arc::new(Int64Array::from_iter_values(from..from + n)) as ArrayRef)]).unwrap() }
    fn xs(bs: &[RecordBatch]) -> Vec<i64> { bs.iter().flat_map(|b| b.column(0).as_any().downcast_ref::<Int64Array>().unwrap().values().to_vec()).collect() }

    async fn lake() -> Arc<Lake> {
        Lake::open(&format!("memory://pages-{}", uuid::Uuid::new_v4().simple()), true, false, crate::lifecycle::unhosted()).await.expect("a lake in memory")
    }

    #[tokio::test]
    async fn pages_across_batches() {
        let lake = lake().await;
        let id = keep(&lake, &[batch(0, 5), batch(5, 5), batch(10, 3)]).unwrap();
        assert_eq!(xs(&page(&lake, &id, 0, 4).unwrap()), vec![0, 1, 2, 3]);
        assert_eq!(xs(&page(&lake, &id, 4, 4).unwrap()), vec![4, 5, 6, 7]);
        assert_eq!(xs(&page(&lake, &id, 12, 4).unwrap()), vec![12]);
        let past = page(&lake, &id, 20, 4).unwrap();
        assert_eq!((past[0].num_columns(), xs(&past).len()), (1, 0)); // (its columns, no rows)
        assert!(page(&lake, "gone", 0, 4).is_none());
        let other = super::tests::lake().await;
        assert!(page(&other, &id, 0, 4).is_none(), "another lake's answer is none of this one's");
    }

    #[tokio::test]
    async fn only_whoever_asked_reads_pages() {
        use crate::auth::{Principal, Role, WHO};
        let lake = lake().await;
        let id = WHO.sync_scope(Principal::of(Role::Read), || keep(&lake, &[batch(0, 3)])).unwrap();
        assert!(WHO.sync_scope(Principal::of(Role::Read), || page(&lake, &id, 0, 3)).is_some()); // (the same asker)
        assert!(WHO.sync_scope(Principal::of(Role::Admin), || page(&lake, &id, 0, 3)).is_none()); // (another's: as if gone)
        assert!(page(&lake, &id, 0, 3).is_none()); // (the node's own work is no asker of it)
    }
}
