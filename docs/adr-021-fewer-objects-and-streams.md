# ADR-021: Fewer objects, keyed tables that take any layout, COPY, and streams joined as they arrive

**Status:** Accepted, built and tested (round 20) · **Date:** 2026-09-26 · **Builds on:** ADR-012 (toward petabytes), ADR-017 (streams on their own time), ADR-019 (a database you can shape), ADR-020 (change any row)

## Context

After round 19 the owner asked a run of questions, and then for this round:

- **"If the row ids slow things down, maybe we don't need them."** Round 19 measured ingest,
  queries and planning unchanged, but tiering 8 M rows took 1.11–1.17 s against round 18's
  0.75–0.78 s.
- **"What is the difference between PRIMARY KEY and cluster_by?"** Their `CREATE TABLE t (id BIGINT
  PRIMARY KEY, user text, ts timestamp) WITH (publish = 'delta,iceberg', cluster_by = 'user',
  partition_by = 'day(ts)')` was refused: a keyed table kept its files sorted by the key, so it took
  neither option. They want "a normal database experience" with every streaming and batch
  capability underneath.
- **"Is cluster_by like Databricks' liquid clustering (Hilbert)?"** It was a plain sort.
- **"Is it true streaming, like Flink?"** And "can the Postgres protocol go faster, or use Arrow
  underneath?"
- **"Many files in the folders — think about S3's limits."** Measured: one table publishing Delta
  and Iceberg, 20 one-row INSERTs a second for 2 minutes, wrote 9,207 objects (77 a second) and
  left 5,894.
- They pushed round 19 and ran the cluster bench on it by hand, for this round to read.

## Decisions

### 1. Fewer objects: what a bucket bills and rate-limits

Three causes, measured (`logs/round20/objects.txt`; `harness.py files` keeps them fixed):

- **Every SQL `INSERT` wrote its own Parquet file.** That path is for bulk `INSERT … SELECT`; a
  one-row `INSERT … VALUES` got a one-row file. Now `INSERT … VALUES` goes through the log, like
  streamed rows, and tiering writes one file per table per round (`write::through_log`).
- **The catalog's write-ahead log was cleared every 10 minutes.** Every commit is a WAL object;
  SlateDB's cleaner ran every 10 minutes for objects 5 minutes old. Now every minute, for objects
  a minute old (`PONDRA_GC_SECS`); what a reader's checkpoint still needs stays.
- **Tiering ran every 2 s,** each run a Parquet file, a Delta version and an Iceberg snapshot per
  busy table. Now at most every 10 s by default (`--tier-secs`), and a table with a million rows
  waiting is still tiered within a second. Pondra's own reads see every commit at once either way;
  Delta and Iceberg readers see a table up to 10 s later. Delta keeps 100 versions, not 1,000.

`/metrics` now counts what the lake's store was asked to do: `pondra_object_requests_total{op}`
for writes, lists and deletes (`store::Counted`).

### 2. System columns cost tiering a third of what they did

Round 19's tiering overhead was taken apart stage by stage: encoding the four system columns cost
about 0.08 s of CPU per 8 M rows (`_row_id` alone, delta-encoded, next to nothing:
`logs/round20/syscols.rs`), and the second pass over every column for its min and max cost more
(0.037 s per 8 M values, each column). File statistics now come from the Parquet footer the writer
builds anyway (`manifest::stats`, floats aside: Parquet leaves NaN out of them), so no column is
read twice, system column or not; and building the system columns skips what the rows already
carry whole (`sys::derive`).

Deriving the system columns from per-file metadata (Iceberg v3's row lineage, Delta's row
tracking) would save the rest, but every reader would have to rebuild them from row positions.
Not worth it for what is left; the design stays open (below).

### 3. A PRIMARY KEY with partition_by and cluster_by

The key is what a row is; `partition_by` which files it goes into; `cluster_by` how a file is
sorted. They now combine. A keyed table's tiering round writes its newest rows as one
*generation*: a file per partition, sorted by `cluster_by` then the key. Reads already let a newer
generation's row for a key shadow an older one's (`query::upsert_view`, an anti-join by key per
generation), wherever its partition, so a row that moves to another day has one current version.
Compaction counts generations, not files, and merges whole ones (`tier::run`). Delta and Iceberg
see a keyed table as of its last full compaction, now possibly several files (one per
partition); `CHECKPOINT` compacts a published keyed table so they see it as it is now.

### 4. cluster_by over two or more columns: a Hilbert curve

Sorted by (a, b), a file's row groups each span all of b, so a filter on b skips nothing. With two
or more `cluster_by` columns, rows are ordered along a Hilbert curve through the columns' ranks
(`hilbert.rs`, Skilling's algorithm), as Databricks' liquid clustering does: each row group then
holds a narrow part of every column. Like liquid clustering it is incremental (each file as it is
written or merged) and the columns can change (`ALTER TABLE … SET (cluster_by = …)`).

### 5. COPY over the Postgres protocol

The Postgres protocol sends rows, not columns, and every client expects it that way: Arrow can't
travel inside it. Two things make it faster where it can be:

- **Rows are encoded a batch at a time,** as the client takes them, not all before the first.
- **`COPY … TO STDOUT` (text, CSV, binary) and `COPY … FROM STDIN` (text, CSV).** psql's `\copy`,
  psycopg's `cursor.copy`, and the ADBC Postgres driver, which reads every result as
  `COPY (query) TO STDOUT (FORMAT binary)` and hands the client Arrow. The catalog views drivers
  ask for gained what that driver reads (`pg_type`'s receive functions, `pg_attribute`).

The fast way in stays Arrow Flight SQL (ADBC, JDBC); the Postgres protocol is for everything that
already speaks it.

### 6. Streams joined as they arrive; sliding windows

Pondra's streaming is continuous: a batch per flush, a flush as soon as the last one committed, a
view's rows in the same commit (5 ms from an event to a view row on another node). What it lacked
from Flink was operators. Two of them now:

- **A join of two streams** (`WITH (join = 'streams', time = 'ts', within_secs = 600)`): a row of
  either table pairs with the other's rows when it arrives and with those that arrive after it,
  each pair once. The leader keeps the view up to date right after commits: over the commits since
  its last run, `Δa ⋈ b(now) ∪ a(before) ⋈ Δb`, with `_version` (round 19's system column: the
  commit that wrote a row) telling new rows from old in the files and the log alike. The pairs and
  the progress commit together, so none is lost or doubled across restarts. `within_secs` bounds
  what a new row is paired against: the other table's rows that close in time, so only their files
  are read (the state Flink keeps in RocksDB is the lake's own tables here). Rows written to either
  table go through the log while such a view follows it.
- **Sliding windows** (`slide_secs`): 5-minute windows every minute. The view keeps 1-minute
  panes; each window emitted combines the panes it covers (counts and sums added, min of mins,
  max of maxes), so a row is added once, not once per window.

### 7. The cluster bench on round 19, and what it found

The owner's run of `cluster-bench.yml` on round 19 (3 GitHub runners, TPC-H, links 23–65 ms and
43–116 MB/s, `logs/round20/cluster-bench-round19-run.json`):

| | Round 18 | Round 19 |
|---|---|---|
| One node | 23.1 s | 15.7 s |
| As the cluster decides | 46.6 s (22 queries spread) | **18.0 s** (5 spread) |
| Spread anyway | — | 39.5 s |
| Moved between nodes | 938 MB | 80 MB |
| Answers the same | yes | yes |
| Loading TPC-H (`pondra sql` INSERTs) | 108 s | 442 s |

- **The guard works:** the cluster is within 14% of one node on a network where spreading costs
  twice as much. Of the 5 queries it still spread, 4 were slower spread (2.3 s in all). A query
  that has run both ways is now decided by what each took (`guard::ran_spread`); the model only
  guesses the first time.
- **Loading was 4× slower:** a bulk `INSERT` from `pondra sql` wrote its files without row ids,
  and the leader rewrote every one to add them. Now `pondra sql` takes a block of ids from the
  leader first, when it can reach it, and the files are recorded as written (6 M rows locally:
  2.7 s → 1.3 s).

## What it costs

On one 2-vCPU box (`logs/round20/`):

| | Before | After |
|---|---|---|
| Objects a trickle of one-row INSERTs writes (20/s for 2 min, Delta + Iceberg) | 9,244 (3.9 per INSERT), 5,962 left | 2,667 (1.1 per INSERT), 217 left |
| Tiering 8 M rows (dist builds, side by side) | round 18 0.88–0.98 s, round 19 1.45–1.55 s | 1.10–1.16 s |
| A `pondra sql` bulk INSERT of 6 M rows, leader running | 2.7 s | 1.3 s |
| A stream join's pair, after the second row's ack | — | 14 ms p50, 24 ms at worst |
| 1 M rows × 4 columns out: psycopg / ADBC over Postgres / Flight SQL / HTTP Arrow | — | 0.72 / 0.67 / 0.10 / 0.05 s |
| A filter on the 2nd of two `cluster_by` columns, 4 M rows from Parquet: none / 1st only / Hilbert | — | 21.9 / 23.3 / 11.4 ms |
| A filter on the 1st column: none / 1st only / Hilbert | — | 39.9 / 5.5 / 14.4 ms |

- **Delta and Iceberg readers see a table up to 10 s after a write** by default, not 2 s
  (`--tier-secs` sets it); a published keyed table, as of its last compaction (8 tiering rounds,
  or `CHECKPOINT`).
- **Hilbert order gives each column about half** of what sorting by that column alone gives; a
  table filtered almost always by one column is better clustered by that one.

## What is still open

- **More of Flink:** deduplication and Top-N per key by event time, timers, pattern matching
  (`MATCH_RECOGNIZE`), a watermark per partition, and a Nexmark run against Flink.
- **Stream joins run on the leader alone.** Enough for what one machine joins; sharded by key
  across the nodes, as tasks are, later.
- **Pondra's reads of a keyed table don't prune by partition** (a newer generation in another
  partition may shadow a row); outside engines, reading the compacted table, do.
- **System columns from metadata** (Iceberg v3 style) if tiering ever needs the last few percent.
- **A bulk INSERT from a machine that can't reach the leader** (through the bucket inbox, or
  leading itself) still has its files rewritten to stamp row ids.

## Tests

- `harness.py files`: 200 one-row INSERTs make one Parquet file, one object write each; the
  catalog's WAL is cleared; a `pondra sql` INSERT's files are recorded as written.
- `harness.py layouts`: the owner's CREATE TABLE; rows moving between days over 9 rounds against a
  model; every file one day, sorted by user; a lookup; delta-rs sees what Pondra sees.
- `harness.py clusters`: two-column cluster_by gives narrow row groups in both columns, for an
  append and a keyed table.
- `harness.py copies`: COPY in (text, CSV) and out (text, CSV, binary); the ADBC Postgres driver.
- `harness.py streams`: a stream join over two nodes and a leader restart, against a model;
  sliding windows against a model.
- `harness.py guard`: a query that ran both ways goes the faster way.
