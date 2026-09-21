# ADR-007: A lake other engines can read, and sub-second first reads on object storage

**Status:** Accepted, built and tested (round 6) · **Date:** 2026-09-21 · **Builds on:** ADR-005, ADR-006

## Context

Three things were asked of round 6, all measured against Apache Fluss:

1. **Any new user reads in under a second**, on object storage too. Fluss serves reads from its
   tablet servers' local disks in milliseconds. Pondra's first query on R2 took **3.0 s**, and a
   steady-state query **~0.5 s**: every query read the catalog and the Parquet files straight
   from the bucket, where one GET costs ~380 ms from this sandbox.
2. **The lake itself should be available sooner than Fluss's.** Fluss's union read covers the
   gap in Fluss, but its lake tables (Paimon/Iceberg) lag by `table.datalake.freshness`,
   3 minutes by default. Pondra's lake could not be read without Pondra at all: the Parquet
   files were in the bucket, but the only list of live files was in Pondra's own catalog.
3. **Other systems should be able to read the lake without Pondra.**

## Decisions

### 1. Publish every table as a Delta Lake table, next to its files

- Each table's folder `data/{table}/` gets a `_delta_log/`: one JSON commit per change to the
  table's file list, a Parquet checkpoint every 10 commits (as Delta itself does, so a reader
  replays at most 9 JSON files), `_last_checkpoint`, and the last 1,000 versions kept.
- Publishing is part of every tiering round, right after the new files are committed.
- The catalog stays the source of truth. A Delta commit is derived only from committed catalog
  state and written put-if-absent. If a crash leaves a written commit unrecorded, the next round
  finds it and adopts it. At worst the Delta log is one step behind; it is never wrong.
- **Append tables** are published every round. **Keyed tables** (upsert, merge, GROUP BY views)
  hold several versions of a key between compactions, so they are published whenever one file
  holds exactly one row per key: after a compaction (at most 8 rounds apart), or after a first
  fold that has no delete markers.
- **Why Delta, not Iceberg or DuckLake:** it needs no catalog service (the log is plain objects,
  put-if-absent is the only concurrency control), and it has the widest set of readers today:
  Spark, Databricks, DuckDB, Polars, delta-rs, Trino and Athena.
- Iceberg metadata could be written the same way later. DuckLake needs a SQL catalog database,
  which conflicts with "object storage is the only state".

Checked with three independent readers — **delta-rs 1.6.4, Polars 1.44.2 and DuckDB 1.5.5**
(delta extension) — on local disk, on simulated R2 and on real R2. Their row counts equal
Pondra's across append tables, an upsert table, a GROUP BY view and a bulk insert, including
after 1,200+ versions with checkpoints and log cleanup.

### 2. Tiering starts when rows commit, and fresh rows never wait for maintenance

What an outside engine sees is only as fresh as the last tiering round, so the round was taken
apart and put back together around one question: how many storage round trips stand between
a commit and a Delta version that has it?

- **Event-driven:** a round starts as soon as new rows commit, at most once every
  `--tier-secs` (default 2; fractions allowed, e.g. 0.25), and at least every 5 × that for bulk
  inserts, compaction and retention. The first write after a quiet spell is tiered at once.
- **Fold, commit, publish — then maintenance.** A round folds each table's log tail into new
  files and commits them, then writes the Delta commits. Only after that does it merge small
  files and compact keyed tables, in a separate commit that is published too. Fresh rows
  never wait for a merge.
- **Tables side by side:** up to 4 tables are tiered at once, and all their Delta commits are
  written at once. What was published is recorded in one catalog write, which is not awaited:
  if the next round sees the older record, it finds its Delta commit already there and adopts it.
- **Nothing is committed for nothing:** a table with no new rows is skipped. Retention moves its
  mark along instead, every 10 s, and deletes expired objects 16 at a time.
- **The leader keeps the catalog's own files on local disk.** This uses SlateDB's disk cache:
  on flush, on compaction, next to the SSD tier. Its reads during a round don't wait on the
  bucket.

That leaves three storage round trips on the path: the Parquet file, the catalog commit and the
Delta commit.

### 3. A local SSD tier on every node

- Lakes on object storage get a per-node disk cache: `--cache-dir` (default
  `<temp>/pondra-cache/`) and `--cache-gb` (default 20; 0 turns it off). It holds whole objects,
  with LRU eviction.
- **Write-through:** what a node writes (log segments, Parquet files) it keeps.
- **Read-through:** a query that misses reads the range from the bucket and queues the whole
  object for download.
- **Prefetch from the commit stream:** every commit names the new objects (segments other nodes
  wrote, new Parquet files), so every node downloads them in the background. The leader does the
  same from its own commits.
- **Warm at start:** a node that starts queues the log tail and the newest table files, up to
  half the tier.
- No invalidation is needed: objects are immutable. The one exception, `_delta_log/_last_checkpoint`,
  never goes through the tier.

### 4. Followers keep the whole catalog in memory

- A follower or read-only node seeds an in-memory copy of the catalog from its own view. It
  does this in the background, one attempt per refresh: a single scan, taken as consistent if
  the view didn't move during it, or if the streamed commits cover every change it might have
  picked up. From then on the commit stream keeps the copy current.
- Reads are answered from memory as of the last streamed commit. Inline segment data (`d/`) is
  the exception: it is still read from the view unless it is recent.
- After a gap in the stream, the copy is dropped. Reads fall back to the view plus the overlay
  (ADR-005's rules) until the view has caught up, and then the copy is seeded again.

### 5. Catalog flushes follow commits, not tiering

- Every tiering call used to flush SlateDB's memtable, plus a separate 2-second flush loop while
  busy. At 2-second tiering that meant 1–3 level-0 files per second.
- The compactor couldn't keep up, so writes hit SlateDB's per-key level-0 limit: **a single
  flush took 9 s**, and so did every tiering round.
- Now there is one flush loop, every 5 s, and it flushes only when something was committed. A
  new leader also flushes what it inherited right away, so every follower's view covers the
  previous term.

## Results

| | Before (round 5) | Round 6 |
|---|---|---|
| First query of a new client, read-only node, R2 (1 M rows) | 2,999 ms | **20–30 ms** |
| Steady query, same | ~495 ms p50 | **17–31 ms** p50 |
| First query on a node that just joined (empty SSD tier), R2 | — | **949 ms** first, 20–35 ms once warm |
| Write on node B → visible on node C, R2 | 1,999 ms p50 | **660 ms** p50 (≈ one PUT) |
| Write → new Delta version (outside Pondra), local disk | not possible | **17 ms** p50 (delta-rs returns it at 31 ms) |
| Write → new Delta version, real R2 from this sandbox | not possible | **4.8 s** p50 (ack 0.7 s; then 3 round trips of 0.6–1.4 s) |
| Lake readable by Spark / DuckDB / Polars / delta-rs | no | **yes**, all tables |

Against Fluss:

- **Unified read:** Pondra's is as fresh as the ack, on every node. A write is readable the
  moment it is durable: 4–7 ms on local disk, one PUT on object storage.
- **The open lake:** 17 ms behind on local disk and ~5 s on a far-away R2 bucket, for append
  tables; under a steady stream, add up to `--tier-secs`. Fluss's lake is 3 min behind by
  default. Keyed tables are at most 8 tiering rounds behind.
- **Where Fluss still wins:** a durable ack on object storage costs one PUT (0.2–0.8 s),
  where Fluss acks after replicating to local disks. S3 Express One Zone (~5 ms PUTs) is the way
  to close that without adding a disk-replication layer. It has not been measured yet.

## What the tests caught

- **The in-memory catalog lost commits when the node's view got ahead of its stream.** The view
  pruning marked those commits "already seen", so they were skipped. But the copy never reads
  the view, so reads missed batches. Tiering jobs that ran on such a node then wrote files
  without those rows, and the leader committed them.
  - `cluster.py users` found it in 4 of 5 runs.
  - Fixed: the view no longer moves the skip mark while the copy is on.
  - Belt and braces: every tiering job now carries the leader's row count for its log range.
    A node that sees a different count refuses the job, and the next round retries.
- **The catalog flush storm** above: found by a 1,250-round Delta test that slowed to one round
  per 9 s.
- **A stale follower after a leader change** (`harness.py fence`): its view lacked commits the
  old leader never flushed. Fixed by flushing on takeover.
- **A restarted node that never came up** (`cluster.py failover --s3`, simulated R2). Seeding
  retried its whole-catalog scan until the view stood still, and did it before the node started
  serving. On a busy lake with 100 ms GETs, the view never stood still. Now it's one try per
  refresh, in the background, and a scan during which the view moved still counts when the
  streamed commits cover the difference.
- **SSD tier:**
  - A panic on the one-character catalog key `c` (it killed every node in the first R2 run).
  - Temporary files that two writers of the same object could share. They are now unique.

## Consequences

- Each node uses up to 20 GB of local disk for lakes on object storage (configurable).
- Delta readers see the latest version and the last 1,000 versions of history. Files replaced by
  compaction are deleted after `--retain-secs` (60 s), so Delta time travel to older versions
  fails after that. Delta readers must not write.
- Types without a Delta equivalent (e.g. timestamps with time zones, nested types) leave that
  table unpublished. Nothing is partitioned yet.
- Tiering every 2 s costs no measurable write throughput. Without readers, 64 writers did
  175k events/s at 2 s against 163k/s at 10 s on this box. More, smaller files are merged more
  often.
