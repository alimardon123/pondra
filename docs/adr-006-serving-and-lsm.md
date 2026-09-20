# ADR-006: Keyed tables as an LSM, and serving reads as a first-class path

**Status:** Accepted, built and tested (round 5) · **Date:** 2026-09-21 · **Builds on:** ADR-005

## Context

Round 4 made every node ingest and made reads milliseconds fresh. Two things were still shaped for
analytics only:

1. **Every tiering round rewrote a keyed (upsert or merge) table in full.** Cost grew with the
   size of the table, not with the number of new rows, and it ran on one node.
2. **Every read of a keyed table paid a window function over the whole table** to pick the newest
   version per key — even a lookup of one key, and even when the table had just been compacted.

Meanwhile the product this competes with grew a serving tier: Databricks Lakehouse//RT answers
point lookups on Delta/Iceberg in ~10 ms at high concurrency, and Fluss serves primary-key lookups
in milliseconds. A lake that is fresh but can't answer "give me this one row" quickly still needs
a Postgres or Redis in front of it — which is exactly the stack this project exists to remove.

## Decisions

### 1. A keyed table is a small LSM, not a single rewritten file

- Each tiering round folds only the **log tail** into a new file, one segment range per node, the
  same way append tables already worked.
- Every file carries `ord`, the last log segment it covers. A read gives a file's rows
  `_ord = ord << 32`, and log rows `(segment << 32) + position`, so the newest version of a key
  wins no matter which file it came from.
- Files are compacted into one only when **8 pile up**, and that job also folds in the log.
- A fold keeps delete markers (they still shadow older files); a full compaction drops them.
- Append tables' small-file merges skip anything already 64 MB or 4 M rows (what one merge writes
  at most), so rows are never merged twice. Keyed tables use the 8-file rule above instead.

The result: tiering costs what the new rows cost. Durable ingest went from 2.1 M to 3.8 M events/s
through a 3-node cluster, and from 1.5 M to 5.5 M events/s on a single node without a view.

### 2. Newest-version-per-key is an aggregate, and often not needed at all

- The dedupe is now `first_value(col ORDER BY _ord DESC) … GROUP BY key` — a hash aggregate
  instead of a window's sort. Keyed-table scans got 3.5x faster.
- When a table has one file and nothing newer in the log, it already holds one row per key, so the
  dedupe is skipped entirely: a compacted table reads like plain Parquet.

### 3. Files are written to be looked up, not only scanned

Keyed tables are written sorted by key, with a bloom filter per key column and 256k-row row groups
(a Parquet default row group is 1 M rows). A lookup touches one row group instead of a whole file.

### 4. `GET /lookup/{table}/{key}`

A lookup-shaped plan (filter, newest wins, limit 1) on a single-partition session, returning JSON.
It gives serving clients a stable API that doesn't depend on SQL planning. Today it is not faster
than the equivalent SQL — 8.9 ms vs 7.7 ms p50 — because the cost is planning and Parquet
decoding, not the plan's shape; that is what a prepared-plan cache would fix.

Measured on 2 M keys, one node, two cores: **7.7 ms p50 / 11 ms p99** with one client, 265
lookups/s with eight (30 ms p50). On a real R2 bucket, 500k keys: 21 ms p50 and 311 lookups/s with
eight clients once the file is cached; a cold key costs one GET.

### 5. Read-only nodes follow the commit stream

A `--reader` node now mirrors the leader's commit stream like a follower (it never votes, leads or
heartbeats; it watches for a leadership change and restarts onto the new leader). Freshness on a
serving node went from 163 ms to **7 ms** p50.

## What this cost, and what it didn't

- A keyed table now holds up to 8 files, so a scan of one opens more files than before. The
  compaction threshold is the knob.
- Nothing in the write path, the commit stream or the consistency rules changed: latency, crash
  behaviour, failover and the 64-writer consistency test are unchanged.

## A lesson worth writing down

The first version of decision 1 stopped tiering an append table once it had 8+ files. Every
correctness test still passed — reads were right, the log just stopped draining — while sustained
ingest halved. Benchmarks caught it, not tests. There is now a `tiering` test (the log must drain,
the file count must stay bounded) and a note in `AGENTS.md`: a tiering failure shows up as
throughput, not as a red test.

## Rejected

| Option | Why not |
|---|---|
| Key-range partitioned files (each file owns a range) | Needs min/max key bookkeeping per file and split-point selection; the LSM gets most of the win with none of it |
| A separate serving process or cache tier | The thing this project exists to avoid; a read-only node with the same binary already serves |
| An in-memory row cache per key | Duplicates what the page cache and the log tail already hold; revisit only if measurement says so |
