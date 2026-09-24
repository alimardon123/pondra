//! Hot keys in a shuffled join: a partition too big for one node is shared out.
//!
//! A join shuffled by its key sends every row of a key to one node, so a key that holds much of
//! a table is one node's work while the others wait (`pondra_shuffle_skew`). Once both sides of
//! such a join have been hashed, the coordinator knows how big every node's partition of it is
//! (each node reports what it sent where). A partition much bigger than the average is shared
//! out: its rows on one side stay on the node that hashed them — each node joins its own share —
//! and its rows on the other side go to every node, so every row still meets every row it
//! matches, once. (Spark's adaptive skew join, without a driver.)
//!
//! Only where that is right: an inner join may share out either side, a left, semi or anti join
//! only its left side (the one it keeps), a right one its right; and nothing above the join in
//! its step may need a key's rows on one node — an aggregation by its key, another join on it.
use datafusion::physical_plan::joins::HashJoinExec;
use datafusion::physical_plan::ExecutionPlan;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// A shuffled join that may share out a partition: its exchanges (left, right), those of them
/// whose rows may be split, and the step it runs in.
pub struct Join {
    pub sides: [usize; 2],
    pub split: Vec<usize>,
    pub step: usize,
}

/// A partition shared out: on `exchange`'s side, node `node`'s partition `part` stays where it
/// was hashed (each node joins its own share of it); on `with`'s side, every node gets all of it.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Split {
    pub exchange: usize,
    pub with: usize,
    pub node: usize,
    pub part: usize,
}

/// Partitions bigger than this (and than twice the average) are shared out (`PONDRA_SKEW_MB`, 64).
fn skew_bytes() -> u64 { std::env::var("PONDRA_SKEW_MB").ok().and_then(|v| v.parse().ok()).unwrap_or(64) << 20 }

/// The joins below `region` (what the nodes run: the plan below the coordinator's gather) a
/// shuffle may share out partitions of. `exchange(p)`: which exchange `p` is, if it is one, and
/// whether its rows may be split (a hash exchange, not an `own` one).
pub fn joins(region: &Arc<dyn ExecutionPlan>, exchange: &dyn Fn(&Arc<dyn ExecutionPlan>) -> Option<(usize, bool)>, last: usize) -> Vec<Join> {
    type Ex<'a> = &'a dyn Fn(&Arc<dyn ExecutionPlan>) -> Option<(usize, bool)>;
    fn walk(p: &Arc<dyn ExecutionPlan>, above: &mut Vec<Arc<dyn ExecutionPlan>>, exchange: Ex, last: usize, out: &mut Vec<Join>) {
        let side = |c: &Arc<dyn ExecutionPlan>| exchange(c).filter(|x| x.1).map(|x| x.0);
        if let Some(j) = p.downcast_ref::<HashJoinExec>() {
            if let (Some(l), Some(r)) = (side(j.left()), side(j.right())) {
                use datafusion::common::JoinType::*;
                // What uses its rows in its step, up to the next exchange (whose step it runs
                // in) or the coordinator: none of it may need a key's rows on one node.
                let (mut step, mut loose) = (last, true);
                for a in above.iter().rev() {
                    if let Some((k, _)) = exchange(a) {
                        step = k;
                        break;
                    }
                    loose &= !keyed(a);
                }
                let split = match j.join_type() {
                    _ if j.null_aware => vec![],
                    Inner => vec![l, r],
                    Left | LeftSemi | LeftAnti | LeftMark => vec![l],
                    Right | RightSemi | RightAnti | RightMark => vec![r],
                    _ => vec![],
                };
                if loose && !split.is_empty() {
                    out.push(Join { sides: [l, r], split, step });
                }
            }
        }
        above.push(p.clone());
        for c in p.children() {
            walk(c, above, exchange, last, out);
        }
        above.pop();
    }
    let mut out = vec![];
    walk(region, &mut vec![], exchange, last, &mut out);
    out
}

/// Whether an operator needs every row of a key on one node (as its input was hashed).
fn keyed(p: &Arc<dyn ExecutionPlan>) -> bool {
    let line = datafusion::physical_plan::displayable(p.as_ref()).one_line().to_string();
    match p.name() {
        "AggregateExec" => !line.contains("mode=Partial,"),
        "HashJoinExec" => line.contains("mode=Partitioned,"),
        "SortMergeJoinExec" | "SortMergeJoin" | "SymmetricHashJoinExec" | "InterleaveExec" | "BoundedWindowAggExec" | "WindowAggExec" => true,
        _ => false,
    }
}

/// The partitions to share out in a step: `sizes[exchange][from][to][part]`, the bytes each node
/// hashed to each node's partition.
pub fn splits(joins: &[Join], step: usize, sizes: &std::collections::HashMap<usize, Vec<Vec<Vec<u64>>>>) -> Vec<Split> {
    let mut out = vec![];
    for j in joins.iter().filter(|j| j.step == step) {
        let (Some(l), Some(r)) = (sizes.get(&j.sides[0]), sizes.get(&j.sides[1])) else { continue };
        // bytes arriving at (node, part), per side
        let into = |s: &Vec<Vec<Vec<u64>>>, to: usize, part: usize| s.iter().map(|from| from.get(to).and_then(|t| t.get(part)).copied().unwrap_or(0)).sum::<u64>();
        let (nodes, parts) = (l.first().map_or(0, |f| f.len()), l.first().and_then(|f| f.first()).map_or(0, |t| t.len()));
        let cells: Vec<(usize, usize, u64, u64)> = (0..nodes).flat_map(|to| (0..parts).map(move |p| (to, p))).map(|(to, p)| (to, p, into(l, to, p), into(r, to, p))).collect();
        let mean = cells.iter().map(|c| c.2 + c.3).sum::<u64>() / cells.len().max(1) as u64;
        for (node, part, lb, rb) in cells {
            if lb + rb <= skew_bytes().max(2 * mean) {
                continue;
            }
            // Share out the bigger side where the join allows it; the other goes to every node.
            let (big, small) = if lb >= rb { (j.sides[0], j.sides[1]) } else { (j.sides[1], j.sides[0]) };
            let (exchange, with) = if j.split.contains(&big) { (big, small) } else { (small, big) };
            if j.split.contains(&exchange) {
                out.push(Split { exchange, with, node, part });
            }
        }
    }
    out
}
