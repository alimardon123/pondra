# ADR-015: Any query across the nodes — and the same answer every time

**Status:** Accepted, built and tested (round 14) · **Date:** 2026-09-24 · **Builds on:** ADR-003, ADR-012, ADR-014

## Context

Round 13 (ADR-014) made a shuffle bounded by disk and able to lose a node. But what could run
across the nodes at all was decided by a gate on the SQL text: one plain SELECT, inner joins only,
over append tables. Anything with an outer join, a subquery, a CTE, a UNION or a keyed table ran
on one node — quietly. Counted on TPC-H, **8 of the 22 queries** spread. A cluster that runs a
third of a standard benchmark in parallel is not yet a cluster anyone can plan on, however good
its shuffles are.

The gate was also redundant. Whether a split is correct was already being decided, operator by
operator, on the physical plan (`spmd::spread`); the SQL gate only stood in front of it.

## Decisions

### 1. The plan decides, not the SQL

`spmd::tables` walks the whole statement — joins, subqueries, CTEs, unions — for every table it
reads (CTE names left out), and the query is sliced on its **biggest append table**, not the first
one named. Keyed tables are read whole on every node: a key's versions are spread over the files,
so a share of the files isn't a share of the rows. Then the plan is analysed; whatever it can't
split correctly runs on one node, and `PONDRA_DEBUG_SPREAD=1` prints which operator said no.

### 2. Every kind of join, by one rule

Whatever a join emits for a row by looking at **all** of the other side needs that other side
whole on every node, or both sides shuffled by the key. Rows it emits only on a match are right
either way.

| join | its kept side may be sliced when | never |
|---|---|---|
| inner | the other side is whole, or both are shuffled by the key | — |
| left / semi / anti / mark | the right side is whole, or both are shuffled | a whole left side against a sliced right one |
| right / … | mirror image | mirror image |
| full | both shuffled by the key (or both whole) | a broadcast |
| NOT IN (null-aware anti) | the right side is whole | shuffled: a NULL anywhere empties the answer, and only one node would see it |

Unions pass through what their inputs are, as long as none of them is a copy read whole (which
every node would repeat).

### 3. Three ways to try a shuffle

Cheapest first (`How`): small tables read whole and joins as DataFusion plans them; then every
join shuffled on both sides by its key (two big tables meeting); then every append table sliced
(a small table on the kept side of an outer join). Each is a planning pass on the coordinator;
the first whose plan splits correctly runs.

### 4. Three kinds of exchange

- **Hash** — each row to the node its key belongs to (as before).
- **Own** — a table read whole on every node that has to meet a sliced one by key, on the kept
  side of an outer, semi or anti join: every node already has all its rows and simply keeps its
  own keys'. Nothing moves. `b LEFT JOIN a` with `b` small and `a` sliced runs this way.
- **All-gather** — a final aggregate over rows spread across the nodes (a scalar subquery's
  `avg`, a `max` over groups): what reaches it is partial aggregates, a few rows per node, so
  every node is sent all of them and computes the same answer. And the side a join collects, when
  it comes from sliced rows (a small table that was sliced, a big one filtered down): every node
  is sent all of it and joins its own share of the other side against it — a broadcast of a
  result rather than of a table (`collected`).

### 4b. A join that doesn't split moves what it needs, not the whole query

Pondra's own planning rule runs a semi join on the table it filters, and DataFusion then collects
that side on every node and streams the other side past it. Across nodes that is correct only
when the streamed side is whole. When it isn't — `EXISTS` over `lineitem` in q4, q17, q20, q21 —
the first way failed and the query went to the second, where *every* join is shuffled on both
sides: q21 moved `lineitem` three times over for the sake of one join. Now just that join is
rewritten (`by_key`): both sides hashed by its key, a side read whole keeping its own keys'
rows. Only a join that fails as planned is touched, so every other plan is unchanged. Seven
TPC-H queries went this way; across three nodes q17 went from 1.11 s to 0.21 s, q21 from 2.12 s
to 0.95 s, q2 from 0.28 s to 0.12 s, and all 22 from 8.0 s to 5.8 s.

An inner join whose collected side is sliced gets that side all-gathered instead (`collected`,
above): DataFusion collects the side it expects to be small, so sending it to every node costs
less than shuffling the big side it meets. That is what a bigger lake looks like — at SF10 and
up, `part`, `customer` and `partsupp` are past `PONDRA_BROADCAST_MB` and sliced — and with every
table sliced the 22 queries went from 12.1 s to 7.7 s (q5 0.95 → 0.24 s, q9 1.49 → 0.56 s).

### 5. Scalar subqueries answered on the way

DataFusion 55 runs an uncorrelated scalar subquery inside an operator (`ScalarSubqueryExec`) that
answers it when *its* step runs — but what uses the answer may sit below a shuffle, in an earlier
step (TPC-H q22 filters customers by an `avg` before shuffling them). A shuffle now takes those
operators out of the plan (`hoist`) and answers the subqueries itself, on every node, as soon as
the exchanges they need are done; the expressions that use them hold the same answer slots and
find the answers in whichever step they run.

### 6. The same answer every time

A shuffle used to deliver a node's rows as they arrived and re-partition them locally, so the same
query added up its floating-point sums in a different order each run. Now each exchange hashes
**once**, into `nodes × partitions` buckets: bucket `i` goes to node `i / partitions` and lands in
partition `i % partitions` — the partition DataFusion itself would put the row in, so there is no
second pass on the receiving side — and a partition reads every node's bucket for it **in node
order** (`spill::chain`, `Received`). The coordinator reads what the nodes send the same way.

This is not cosmetic. TPC-H q15 over the benchmark copy's DOUBLE columns asks for
`total_revenue = (SELECT max(total_revenue) …)` — two sums of the same rows that must come out
bit for bit equal. On one node, DataFusion's threads finish in whatever order they finish, and
the query returned **no rows in one run of three**. Across three nodes it now returns the same
row every time. (With DECIMAL columns, as TPC-H specifies, sums are exact and neither varies.)

### 7. Tables read whole, at one snapshot

A table read whole used to be read by each node from its own copy of the catalog — possibly a
commit apart, and with different files in memory. Now the coordinator sends every table it doesn't
slice along with the slices, as it saw it, and how far into the log (`Slice::whole`, `upto`): every
node reads the very same rows, waiting a moment to catch up if it must. Plan shapes, which every
node compares, no longer count how a node happens to gather a table's partitions — that depends
on what it holds decoded in memory (`hot.rs`), not on the query.

The same rows are not the same plan, though, and a real bucket showed it where one box didn't:
one node had a small table decoded in memory and another read it from Parquet, and DataFusion,
which picks a join's sides by size and a scan's partitions by how it reads, planned the same
query two ways on two nodes — twice in 23 shapes on R2, each then run on one node. So a table
read whole now reports its size from the catalog, like a slice does (`WholeExec`); what is
inside a whole read — the joins over a keyed table's versions — is no longer compared; and every
node plans with the coordinator's number of partitions (`Slice::partitions`), which also lets
machines with different core counts share a query.

### 8. What the new tests found

- **Strings in a shuffle were copied whole into every piece.** A batch cut into a piece per node
  and partition kept sharing its `Utf8View` data buffers, and each piece wrote all of them:
  TPC-H q21 wrote **2.5 GB per node** and filled the disk. Pieces now copy out only the strings
  they hold (`spill::compact`): q21 across three nodes went from 62 s (10–16 s in round 13,
  when a node had a third as many pieces) to **2.2 s**, spilling 12 MB instead.
- **Spill sizes were counted by buffer, not by row.** 145 MB counted for 10 MB on disk, and a
  piece written every few batches (588 files for one shuffle, now 112).
- **Abandoning a shuffle never worked.** The coordinator asked nodes to forget it with
  `?drop=1`; the handler wanted `true` and refused, so an abandoned shuffle's scratch waited ten
  minutes for the sweep. And a step already on its way could open the shuffle again after it was
  forgotten; a forgotten shuffle is now remembered as such.
- **A coordinator that failed its own step dropped itself** from the shuffle and then found it
  was "not a member". It now runs the query alone instead.

## What it measures

On one 2-vCPU box, three nodes sharing its cores — so the numbers say what is correct, not what
is fast (`logs/round14/`).

| | round 13 | round 14 |
|---|---|---|
| TPC-H SF1, of 22 queries: run across 3 nodes | 8 | **22** (19 shuffled, 3 gathered) |
| …every answer equal to one node's | yes | **yes** |
| …with every table sliced (`PONDRA_BROADCAST_MB=0`) | — | 21 (q16's NOT IN over a sliced subquery stays on one node) |
| `harness.py scale` query shapes spread | 14 of 14 kinds it allowed | **23 of 23**, 9 new kinds |
| q21 across 3 nodes | 10–16 s | **0.95 s** (2.2 s before `by_key`) |
| all 22 across 3 nodes (one box) | — | **5.8 s** (8.0 s before `by_key`) |
| …with every table sliced | — | **7.7 s** (12.1 s before `by_key` and `collected`) |
| TPC-H SF1 on one node (20 queries) | 3.35 s | 3.30 s |

The nine new shapes: a LEFT JOIN keeping unmatched rows, a small table LEFT JOIN a big one, a FULL
JOIN, `IN`, `NOT EXISTS`, `NOT IN`, a scalar subquery over the sliced table, a CTE with `UNION ALL`,
and a keyed table on the right of a LEFT JOIN.

`tools/spread_tpch.py` is the test: all 22 queries on N nodes against one node, how each ran, and
why when it didn't.

## A cluster on separate machines, ready to run

The owner has no machines of their own; an agent can't reach any from its sandbox. So the
multi-machine run is packaged for the owner to start: `.github/workflows/cluster-bench.yml` runs
nodes on GitHub-hosted machines joined by Tailscale, the lake in the owner's R2 bucket, and
`tools/cloud/actions/driver.py` loads TPC-H and times every query on one node and across all of
them. The results land in the bucket (`bench-results/<run>/`), where the next agent can read
them. `tools/cloud/actions/README.md` has the secrets it needs and what it costs.

## What this costs

- A shuffle makes `nodes × partitions` buckets per step on every node — small ones stay in
  memory (below `PONDRA_SPILL_MB`), but a small shuffle now writes more, smaller pieces.
- A partition reads the nodes' buckets one after another, not at once: the price of the same
  answer every time.
- Up to three planning passes before a shuffle starts, when the cheap ways don't split correctly.
- A node that is behind the coordinator's snapshot waits up to 10 s for it; past that its step
  fails and is retried.

## What is still open

- **The multi-machine run itself**: everything above is three processes on one box.
- **Skew is measured, not corrected.**
- `NOT IN` over a sliced subquery, a `LIMIT` inside a subquery over sliced data, a window over all
  rows, and order-preserving shuffles run on one node.
- A query's own answer still passes through the coordinator's memory once.
- Sorted files declared as sorted.
