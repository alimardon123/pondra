//! An answer's other pages (ADR-034, round 29). The console is sent an answer's first 10,000 rows
//! (`answer::typed`), and the answer is kept here a while, under the id it is sent with, so its
//! other pages, and a download of every row, come from it: the same rows, in the same order,
//! without running it again.
//! Answers are kept within `PONDRA_PAGES_MB` (256) in all, for 20 minutes after they were last
//! read, the least lately read going first. A page of one no longer kept answers 410: the console
//! then runs the query again for that page.
//! Only whoever asked for an answer reads its pages: the id is a key to nothing else (S31), and
//! another's request for it answers 410 as if it were gone.
use datafusion::arrow::record_batch::RecordBatch;
use std::collections::VecDeque;
use std::sync::Mutex;
use std::time::{Duration, Instant};

struct Kept {
    id: String,
    holder: String, // (who asked: `auth::holder`)
    batches: Vec<RecordBatch>,
    bytes: usize,
    used: Instant,
}

crate::per_node!(static KEPT: Mutex<VecDeque<Kept>> = Default::default());
const IDLE: Duration = Duration::from_secs(20 * 60);

fn budget() -> usize { crate::env::or::<usize>("PONDRA_PAGES_MB", 256) << 20 }

/// Keep an answer: its id, or none when it alone is bigger than the budget.
pub fn keep(batches: &[RecordBatch]) -> Option<String> {
    let (bytes, budget) = (batches.iter().map(|b| b.get_array_memory_size()).sum::<usize>(), budget());
    if bytes > budget {
        return None;
    }
    let id = uuid::Uuid::new_v4().simple().to_string(); // (random: an id is known only to whoever was sent the answer)
    let holder = crate::auth::holder();
    let mut kept = KEPT.lock().unwrap();
    kept.retain(|k| k.used.elapsed() < IDLE);
    let mut total: usize = kept.iter().map(|k| k.bytes).sum();
    while total + bytes > budget {
        let Some(k) = kept.pop_front() else { break };
        total -= k.bytes;
    }
    kept.push_back(Kept { id: id.clone(), holder, batches: batches.to_vec(), bytes, used: Instant::now() });
    Some(id)
}

/// Rows `from..from + n` of a kept answer (read now, so it is kept the longest), or none if it's gone,
/// or was asked for by someone else.
pub fn page(id: &str, from: usize, n: usize) -> Option<Vec<RecordBatch>> {
    let who = crate::auth::holder();
    let mut kept = KEPT.lock().unwrap();
    let i = kept.iter().position(|k| k.id == id && k.holder == who && k.used.elapsed() < IDLE)?;
    let mut k = kept.remove(i)?;
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
    kept.push_back(k);
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{ArrayRef, Int64Array};
    use std::sync::Arc;

    fn batch(from: i64, n: i64) -> RecordBatch { RecordBatch::try_from_iter(vec![("x", Arc::new(Int64Array::from_iter_values(from..from + n)) as ArrayRef)]).unwrap() }
    fn xs(bs: &[RecordBatch]) -> Vec<i64> { bs.iter().flat_map(|b| b.column(0).as_any().downcast_ref::<Int64Array>().unwrap().values().to_vec()).collect() }

    #[test]
    fn pages_across_batches() {
        let id = keep(&[batch(0, 5), batch(5, 5), batch(10, 3)]).unwrap();
        assert_eq!(xs(&page(&id, 0, 4).unwrap()), vec![0, 1, 2, 3]);
        assert_eq!(xs(&page(&id, 4, 4).unwrap()), vec![4, 5, 6, 7]);
        assert_eq!(xs(&page(&id, 12, 4).unwrap()), vec![12]);
        let past = page(&id, 20, 4).unwrap();
        assert_eq!((past[0].num_columns(), xs(&past).len()), (1, 0)); // (its columns, no rows)
        assert!(page("gone", 0, 4).is_none());
    }

    #[test]
    fn only_whoever_asked_reads_pages() {
        use crate::auth::{Principal, Role, WHO};
        let id = WHO.sync_scope(Principal::of(Role::Read), || keep(&[batch(0, 3)])).unwrap();
        assert!(WHO.sync_scope(Principal::of(Role::Read), || page(&id, 0, 3)).is_some()); // (the same asker)
        assert!(WHO.sync_scope(Principal::of(Role::Admin), || page(&id, 0, 3)).is_none()); // (another's: as if gone)
        assert!(page(&id, 0, 3).is_none()); // (the node's own work is no asker of it)
    }
}
