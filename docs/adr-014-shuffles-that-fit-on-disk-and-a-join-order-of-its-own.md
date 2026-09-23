# ADR-014: Shuffles that fit on disk, steps that can be retried, and a join order of Pondra's own

**Status:** Accepted, built and tested (round 13) · **Date:** 2026-09-23 · **Builds on:** ADR-003, ADR-012, ADR-013

## Context

Round 12 made one node fast (ADR-013). What it left behind was the other half of the promise:
queries that run across nodes were still a demo.

- **Everything a shuffle moved lived in memory.** A node ran its stage, collected every row it
  produced, split them by hash into one bucket per node, and held all of it. The coordinator then
  held every node's results while it finished the query. A shuffle bigger than a node's memory
  killed the node.
- **A failed step failed the query.** A node restarting mid-shuffle — a deploy, an OOM, a
  machine going away — ended the whole query, which then ran on one node.
- **Work was dealt by count, not by size.** Files of very different sizes went round-robin, so a
  node could be handed twice the bytes of its neighbour and everyone waited for it.

And ADR-013's own "what is still open" list began with join order: DataFusion joins tables in the
order the query names them, and Pondra had nothing to say about it, though its catalog already
holds each table's row count and each column's range.

## Decisions

### 1. A bucket is pieces, and a piece can be on disk

`spill.rs`. A bucket (`Spill`) is a list of *pieces*: Arrow IPC blobs of `PONDRA_SPILL_MB`
(64 MB by default). Rows are pushed into a bucket as they are produced; when the piece being
filled passes the size, it is written to this node's scratch folder and a new one starts. What a
shuffle can move is therefore bounded by disk, not by memory.

A piece is the unit everywhere, which is what keeps this small: it is what is written, what
crosses the wire (length-prefixed, `Spill::framed` out and `Spill::take` back), and what one
partition of the next stage reads (`Spill::pieces`, one `PartitionStream` each). Nothing ever
holds a whole bucket.

Two things follow that are easy to get wrong and are now tested:

- **The scratch folder has the node's own id in its path.** Two nodes on one machine share a
  cache directory and name their buckets the same way; without the id they wrote over each
  other's rows and the shuffled answer was quietly wrong.
- **Everything a job spilled goes when the job does** — when it ends, when the coordinator gives
  up on it (`GET /cluster/shuffle?drop=1`), when the `Job` is dropped, and, for anything a dead
  node left behind, in a sweep an hour later.

### 2. Stages write as they run, and send as they are read

Nothing is collected and then written any more.

- A step that feeds another step runs each of its partitions and splits every batch into the
  buckets straight away (`spmd::scatter`). The partitions run at once, each filling buckets of
  its own, and those are joined in partition order at the end, so every node splits the same rows
  the same way whatever order they finish in.
- A step whose output goes to the coordinator writes it into buckets too (`spmd::drain`) and the
  reply streams them: the plan's shape, then how many buckets follow, then each bucket's pieces
  (`spmd::reply`). The coordinator reads them onto *its* disk piece by piece and finishes the
  query over a `StreamingTableExec` across those pieces. The rows a query gathers no longer have
  to fit in the coordinator's memory.
- What a gather spilled is freed on the node that sent it as soon as it has been sent, by a guard
  the response stream holds — so it goes whether the stream finished or the reader walked away. A
  shuffle keeps its buckets instead, because a step may have to run again.

### 3. A step that fails is tried again; a node that fails is dropped

A step is the same work every time — the buckets it reads are kept, not taken — so it can simply
be run again. A node that fails a step gets one more try after 250 ms. If it fails twice the
shuffle gives up on *it*, not on the query: the coordinator tells the others to forget that
shuffle, drops the node from the list and runs the whole shuffle again without it (at most three
times). Below three live nodes it stops trying and the query runs on one node, which is always
correct, just slower.

### 4. Work dealt by size

`spmd::deal` hands each manifest and each file to whichever node has the fewest bytes so far,
oldest first. Ties keep the old round-robin, so a time range still spreads across every node.
In the spill test this alone evened the buckets out: 193 MB / 97 MB spilled became 97 MB / 97 MB.

Skew that is left is the kind hashing causes — one key holding much of the table — and
`pondra_shuffle_skew` (the biggest bucket against the average, 1 = even) is where it shows.
`GROUP BY` mostly avoids it already, because every node aggregates its own rows before the
exchange, so a hot key crosses as one row per node. A hot *join* key still lands on one node;
it answers (a bucket is bounded by disk), it just doesn't answer in parallel.

### 5. A join order of Pondra's own

`optimize::JoinOrder`, and `query::Pruned::statistics`.

The catalog already knows, for every append table, how many rows and bytes it holds and what
range each column covers — the file entries and the sealed manifests carry both, and reading
them opens nothing. `Pruned` now reports that as DataFusion statistics, including an upper bound
on a column's distinct values taken from its range (`nationkey` runs 0–24, so 25 values however
many rows there are). The ranges are worked out once per version of a table, not once per query.

The rule flattens a tree of inner joins into its inputs, its equi-keys and its conditions, and
builds it again left-deep: the smallest input first, then each time the input that leaves the
fewest rows. A join's rows are estimated the textbook way — `rows(a) × rows(b) / distinct(key)` —
which is what catches the joins that *expand*: TPC-H q5 relates customers to suppliers by nation,
25 values, so every customer meets four hundred suppliers.

Two rules keep it honest, and they matter more than the search:

- **The order the query wrote is costed the same way, as the tree it is** (which may be deeper
  than left-deep), and kept unless the new one is *a good deal* cheaper — twice, by default.
  These are bounds, not counts, so a small difference between two orders is not a reason to
  overrule the one the query asked for. A query written well is left alone.
- **Nothing is reordered unless every input's size is known and every step joins on a key.** A
  tree with a cross join in it, or an input nothing can size, is one these estimates say nothing
  useful about.

`PONDRA_JOIN_ORDER` is that margin; `0` turns the rule off.

## What it measures

**A shuffle bigger than memory, and one that loses a node** (`tools/shuffle_spill.py`): 3 nodes,
4 million rows with 4 million distinct keys, `PONDRA_SPILL_MB=1` so every bucket spills, a 1 GB
memory budget each.

| | |
|---|---|
| shuffled answer == one-node answer | identical, row for row |
| spilled to disk | 97 MB + 97 MB |
| scratch after the query | 0 bytes |
| peak memory | 943 / 376 / 351 MB (budget 1 GB) |
| bucket skew | 1.0 |
| a node killed just before the query | same answer, 0.3 s |

It passes on local disk and on the R2 simulator.

**Join order** (`tools/join_order.py`): ten queries, each written well and written badly — the
same query with its tables named the other way round, biggest first. Every badly written query
answers exactly as the well-written one does; the ten of them together cost **1.75–1.83 s** with
the rule against **1.87–1.95 s** without it, and the worst of them falls from about 2.1× its
well-written form to about 1.9×. None of the twenty, well or badly written, is slower with the
rule on.

TPC-H's own 22 answers still match DuckDB's and its SF1 total is unchanged (3.35 s either way,
twice over): those queries name their tables well already, so the rule looks and leaves them
alone — which is the point of costing the order they asked for.

That is a modest win, and worth saying why. DataFusion's physical planner already has exact row
counts from the Parquet footers and uses them to choose which side of each join builds the hash
table, and dynamic filters cut the probe side; on two cores at SF1, that absorbs much of what a
better logical order would have bought. What the rule buys is the case those don't reach — a tree
whose *shape* carries far more rows than it needs to — and it is guarded so that it never pays
for itself twice.

The binary is 96.1 MB (32.7 MB gzipped), 0.2 MB more than round 12 — the pieces, the retries, the
statistics and the join-order rule together. The Rust source is about 11,000 lines.

## What this costs

- A shuffle now writes to the node's scratch disk. `PONDRA_SPILL_MB` decides when: higher keeps
  more in memory, lower spills sooner. Nothing is written while a bucket is under the size.
- Statistics cost a little planning time per query — bounded by caching each table's ranges per
  version — and buy nothing for a query that was already written in a good order.
- The cost model has no idea how many distinct values a wide-ranged column really holds (a
  foreign key's range is its parent's), so it can only bound it. That is why the query's own
  order is the baseline and has to be beaten.
- Retrying a step assumes the buckets it reads are still there, which is why they are kept for
  the life of the job rather than freed as they are read: a shuffle costs its scratch until it
  ends.

## What is still open

- **A multi-machine run.** Everything here is measured with three nodes on one box.
- **Skew is measured, not corrected.** A hot join key is still one node's work.
- **A query's own result** still passes through the coordinator's memory once, because an HTTP
  answer is one body and identical queries share it (invariant 14). What the nodes *send* no
  longer does.
- **Sorted data.** Files written in key order still aren't declared as sorted, so an aggregation
  on that key hashes instead of streaming.
- Distributed queries are still one SELECT with inner joins over append tables.
