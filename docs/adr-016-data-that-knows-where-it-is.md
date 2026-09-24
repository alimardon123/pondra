# ADR-016: Data that knows where it is — tables split by key ranges, hot keys shared out, distinct values counted

**Status:** Accepted, built and tested (round 15) · **Date:** 2026-09-25 · **Builds on:** ADR-014, ADR-015

## Context

After round 14 every TPC-H query ran across the nodes, but three things still cost more than
they should once the data is spread over machines:

- **Big tables that share a key were shuffled for every join on it.** `orders` and `lineitem`
  meet on the order key in a third of TPC-H. Both were sliced by size, so every such join hashed
  both tables across the network. The data was in order-key order when it was loaded, but an
  `INSERT` interleaved its partitions as it wrote. So every file held the whole range of keys, and
  there was nothing to split by.
- **A hot key was one node's work.** A shuffle sends every row of a key to one node. If one key
  holds half a table, one node does half the work. Round 13 measured this (`pondra_shuffle_skew`)
  but did nothing about it.
- **The join order guessed distinct values from a column's range.** That says nothing about a
  string. For a date it was wrong by five orders of magnitude: the range was counted in seconds,
  giving 213 million distinct shipping dates where there are 2,526.

## Decisions

### 1. Rows keep the order they arrive in

An `INSERT` now writes each partition of its query to its own files, in the order its rows come,
with round-robin repartitioning off (`write::write_files`). A merge of small files reads them one
after another, in the order they were written (`tier::run_job`, `Merge`). Data that arrives in
order, by time or by an id handed out in sequence, lands in files that each hold a narrow range of
it: TPC-H's `lineitem` becomes seven files of consecutive order keys where it was six files that
each spanned all of them. Loading TPC-H SF1 also got faster, **30.6 s → 7.2 s**, because the
partitions now write side by side.

Every file also records which of its columns hold a NULL (`DataFile::nulls`).

### 2. Big tables sliced by the ranges of a key they share (`ranges.rs`)

Before a shuffle, a new way is tried first (`How::Ranged`):

- **The key.** A column of the query's biggest table that the query names, and whose files hold
  narrow ranges of it. "Narrow" means slicing it costs at most one extra piece read per cut; files
  that each held every key would be read by every node.
- **The cuts.** They divide that table's bytes evenly over the nodes. A cut falls where a file
  starts, or, for whole-number keys, inside a file too big to leave whole.
- **Other tables.** Every other table the query reads, small ones included, is cut by the same
  ranges of a column of the same type, if that reads it at most about twice over and leaves no
  node much more than its share. Otherwise it is sliced by size or read whole, as before.
- **Each node's slice.** A node reads the files and manifests that overlap its range, plus the
  whole log tail, and keeps only the rows inside its range (`Pruned::range`, pushed down to the
  Parquet reader). The first range also holds the NULLs, so a file that holds NULLs is read by
  the first node too.

So the ranges split every table exactly, whatever its files hold. They are only chosen where that
is cheap.

The analysis gets a fourth state, `Spread::Ranged`: each node holds its own rows, and every row
with a given value of the key is on one node, whichever table it came from. `by_range` traces a
column back through projections, filters, aggregations' groups and joins to the slice that cut
it. That turns three operators local:

- a hash repartition on the key stays on the node (no exchange);
- an aggregation grouped by the key runs where its rows are;
- a join whose keys pair the key on both sides runs in place, collected or partitioned alike.

Everything else treats `Ranged` as `Split`. If the plan still doesn't split correctly, the ways
from round 14 follow.

TPC-H on three nodes: **13 of 22 queries run by ranges, 14 with every table sliced**. q21, three
copies of `lineitem` meeting on the order key, needs no shuffle except for its final grouping.

### 3. What a node happens to hold doesn't change its plan

A slice now reports no ordering and no constant columns (`ShareExec::props`). The tests found
why: a node whose files all held one value of a column saw it as constant, dropped it from a sort
and planned the query differently from the others.

### 4. Hot keys shared out (`skew.rs`)

Every node now reports how much it sent to each node's partitions. Once both sides of a shuffled
join have been hashed, the coordinator knows how big each partition of the join is:

- **What counts as hot.** A partition bigger than twice the average, and bigger than
  `PONDRA_SKEW_MB` (64).
- **Sharing it out.** On one side, each node keeps the rows it hashed there itself, and nothing
  moves. On the other side, every node gets all of that partition's rows. Every row still meets
  every row it matches, exactly once. This is Spark's adaptive skew join, without a driver.
- **Which side is split.** An inner join shares out whichever side is bigger. A left, semi or
  anti join can only share out its left side; a right join, its right.
- **When it is refused.** Nothing above the join in its step may need a key's rows on one node,
  such as an aggregation by the join key or another join on it.

`tools/skew_check.py` joins a table whose key is 0 in half its rows. The busiest of three nodes
read **1.95×** the average without sharing and **1.27×** with it, and every answer equals one
node's.

### 5. NOT IN across the nodes

A NULL anywhere in a `NOT IN` subquery empties the answer. So the subquery's rows, usually few,
are now sent to every node (an all-gather of that side: `Exchange::whole`), and each node keeps
its own share of the other side. TPC-H q16 now runs across the nodes with every table sliced:
**22 of 22 both ways**.

### 6. Distinct values counted, not guessed (`sketch.rs`)

- **The sketch.** Every file written gets a HyperLogLog sketch of each column that could be a
  key: whole numbers, strings, dates and decimals. That is 256 registers, about 6% off.
- **Kept per table, not per file.** The leader folds the file's sketches into its table's as it
  commits the file (`TableMeta::sketch`). A table's catalog entry lists up to 128 files, and each
  carrying a sketch of every column would be too much.
- **Used for statistics.** A column's statistics say the smallest of what its sketch saw, what
  its range could hold, and the table's rows.

On TPC-H the estimates came out within about 15%:

| Column | Estimate | Actual |
|---|---|---|
| `o_custkey` | 97,175 | 99,996 |
| `o_clerk` | 1,032 | 1,000 |
| `l_shipdate` | 2,326 | 2,526 (was 213 M from the range) |

The join order (`optimize::JoinOrder`) now catches two badly written queries it missed. In the
round-14 run the snowflake one took 0.169 s against 0.121 s written well; now it takes 0.118 s
against 0.116 s. The filtered-dimension one went the same way: 0.166 s against 0.119 s before,
0.110 s against 0.112 s now. Badly written queries as a whole take **1.55 s** with the rule
against 1.72 s without it; in round 14 it was 1.74 s against 1.89 s.

### 7. Tried and left out

- **Telling DataFusion the files are in order.** Declaring each file's sort order, with files
  grouped by statistics so the order survives, made TPC-H *slower*:
  - one node from Parquet: 3.30 s → 3.85 s (grouping by statistics put a table's ordered files in
    one partition);
  - from memory: 1.84 s → 2.04 s (order-keeping plans cost more than they saved).

  Reverted. The files are still written in order; only the declaration is gone.
- **Smaller row groups** (128k or 256k rows instead of 1M). From memory: 1.79 s and 1.94 s
  against 1.92 s. From Parquet: 3.62 s and 3.42 s against 3.32 s. Mixed, so left as it was.

## What it measures

The three-node rows ran as three processes sharing one 2-vCPU box. They say what is correct and
what moves less, not how fast a real cluster is (`logs/round15/`).

| | Round 14 | Round 15 |
|---|---|---|
| TPC-H SF1 on 3 nodes, all 22 (small tables whole) | 5.85 s | **4.81 s** (13 by ranges) |
| …every table sliced | 7.66 s | **5.70 s** (14 by ranges) |
| q21 on 3 nodes (whole / sliced) | 0.96 s / 1.97 s | **0.50 s / 0.45 s** |
| q18 on 3 nodes (whole / sliced) | 0.74 s / 0.57 s | **0.31 s / 0.34 s** |
| Queries across the nodes, every table sliced | 21 of 22 | **22 of 22** |
| Busiest node's share of a join with a hot key | 1.95× the average | **1.27×** |
| TPC-H SF1 on one node, from memory / from Parquet (DuckDB 3.38 s) | 1.84 s / 3.30 s | **1.69 s / 3.07 s** |
| Loading TPC-H SF1 (one INSERT per table) | 30.6 s | **7.2 s** |
| Badly written queries, join-order rule on vs off | 1.74 s vs 1.89 s | **1.55 s** vs 1.72 s |

## What this costs

- **Slicing by ranges.** One more planning pass is tried first; when it doesn't split, the next
  way is tried as before. Each node reads the whole log tail through its range, and a file at a
  cut is read by both nodes.
- **File metadata.** Every file written computes its NULL columns and sketches: one hash per value
  of up to 32 columns. A table's catalog entry grows by about 350 bytes per key-like column.
- **Sharing out hot partitions.** Every node reports its partition sizes after each exchange, a
  few bytes per partition. A hot partition's other side is sent to every node.
- **Old files.** Files written before round 15 don't say whether they hold NULLs. Those are read
  by the first node of a ranged slice too, which usually rules ranges out until they are rewritten.

## What is still open

- **The multi-machine run itself.** `.github/workflows/cluster-bench.yml` is ready, and the
  round-15 binary is in the bucket (`bench-bin/pondra`).
- **Ranges are chosen from what the files hold, not declared.** A table written out of order,
  such as late rows mixed into merged files, isn't sliced by ranges. `cluster_by` could make it so.
- **Joins with a hot key on both sides** share out only one side.
- **Still on one node:** a `LIMIT` inside a subquery over sliced data, and a shuffle that must keep
  order.
- **Sketches only grow.** Rows deleted from keyed tables stay counted; for append tables that
  doesn't matter.
- **TPC-H q15 over DOUBLE money on one node.** It compares a sum with the max of the same sums,
  and DataFusion adds them up in an order that varies from run to run, so in round 15's
  benchmark the Parquet run's answer differed from DuckDB's. Across the nodes it adds up in node
  order and is right every time; with DECIMAL money (TPC-H's own types) it always is.
