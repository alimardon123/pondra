# ADR-020: Change any row — UPDATE, DELETE and MERGE on every table, system columns, streaming that follows, and a cluster never slower than one node

**Status:** Accepted, built and tested (round 19) · **Date:** 2026-09-26 · **Builds on:** ADR-005 (every node writes), ADR-015 (any query across the nodes), ADR-017 (views that emit), ADR-019 (a database you can shape)

## Context

The owner asked for two things in round 18 and chose them as this round ("Change any row +
guard"):

- **`UPDATE`, `DELETE` and `MERGE` on every table, with system columns.** Until now only keyed
  (upsert) tables took `UPDATE` and `DELETE`, as new versions of their keys. An append table's
  rows had no identity to change them by. The owner wanted a row id like Postgres's `ctid`, when a
  row was written, and a version, and "streaming has to keep working through all of it": views,
  the change feed, Kafka consumers, Delta and Iceberg readers.
- **A cluster that is never slower than one node.** The first cluster bench that completed (three
  GitHub runners, TPC-H SF10, `logs/round18/cluster-bench-3-nodes.json`) was right in every
  answer and twice as slow: 46.6 s against 23.1 s. The runners reach each other over the public
  internet (17–54 ms, 51–150 MB/s). The ten queries that move under 1 MB took what one node takes;
  the twelve that shuffle moved 936 MB and cost about 0.7 s plus 15 ms a MB. And the leader's
  merges (19–36 s) ran under the first queries.

While the round was being built, the owner tried the shell on Windows again and hit five more
things: `ATTACH` of a folder with no lake in it failed, `CREATE DATABASE` didn't exist, the lake's
name was its whole Windows path (so `mylake.dbo.t` wasn't found), the shell couldn't read a CSV on
the laptop (`SELECT * FROM 'D:\Downloads\AMEX_SPY, 1(1).csv'`), and `CHECKPOINT` wasn't known.
They are in this round too (decision 7).

## Decisions

### 1. Every row has system columns

| Column | What it is | Where it comes from |
|---|---|---|
| `_row_id` (BIGINT) | which row it is, from its INSERT on; an UPDATE or MERGE keeps it | stamped as the row enters the log (the node that packs it), or as a bulk INSERT writes its files |
| `_version` (BIGINT) | the commit that wrote this version of the row | the log segment's number; a bulk INSERT's reserved commit number |
| `_created_at` (TIMESTAMP UTC) | when its first version was committed | the segment's commit time; kept through changes |
| `_updated_at` (TIMESTAMP UTC) | when this version was | the segment's commit time |

- **Row ids never repeat, and nobody coordinates per row.** A node takes a *block* of ids from the
  leader: a commit number it reserves (`Flush::reserve`: the sequencer skips it, so no segment
  ever has it). A block holds 2³² ids, `(block << 32) + n`, and the node counts through it
  (`sys::Ids`). A bulk INSERT reserves one number for itself; its partitions share it through an
  atomic counter. That is Iceberg v3's row lineage (a first-row id per file, then positions), made
  to work with many writers.
- **Stored once, derived when cheap.** In the log, a batch's ids that run on from one — fresh
  ones, as a flush stamps them — are that one number in the batch's metadata (`sys::compact`: a
  column of them cost a third of ingest throughput); ids an UPDATE carries stay a column. Versions
  and times come from the segment when a row is read (`sys::derive`). Tiering writes all four into
  the Parquet files (ids delta-encoded, versions and times plain): the files come out the size
  they were without them. Rows logged before this round get `(segment << 32) + position`.
- **`SELECT *` leaves them out; naming one brings them in.** `SELECT _row_id, * FROM t` shows the
  table's columns and `_row_id` once (`sys::hide` adds `EXCLUDE` to a one-table `SELECT *` in a
  query that names a system column). `CREATE TABLE` refuses their names, and so does a view whose
  output would be named like one. Delta and Iceberg don't list them either: the files hold them,
  and Iceberg's name mapping names them (with ids of their own), so a reader that maps every
  column of a file by name — PyIceberg — accepts them.
- **Tables made before 0.19** have rows without ids (`TableMeta::ids` false). Their keyed tables
  change as before; an append table is changed after a copy (`CREATE TABLE t2 AS SELECT * FROM
  t`), which the error says.

### 2. An append table's rows change by version

`UPDATE … SET … WHERE`, `DELETE … WHERE` and `MERGE INTO t USING s ON … WHEN MATCHED [AND …]
THEN UPDATE/DELETE WHEN NOT MATCHED [AND …] THEN INSERT WHEN NOT MATCHED BY SOURCE … THEN
UPDATE/DELETE` on an append table:

- **Nothing is rewritten when a row changes.** The new version goes into the table like any new
  row, keeping its `_row_id` and `_created_at`. The version it replaces goes into the table's
  hidden companion `{t}$deleted`, its `_version` kept as `_old_version`. Every read of the table
  leaves out the rows whose (`_row_id`, `_version`) that table holds: an anti-join, with the
  (few) replaced rows as its build side (`query::current`). A DELETE is only the second half.
  (`$`: no name a user gives has one, and object stores take it as it is; round 19's first try,
  `~`, came back from object_store as `%7E` and the old rows reappeared after tiering.)
- **The leader carries out a change, under the lake's lock, from one snapshot, in one commit.**
  Its queries all read as of one commit (`session_at(upto)`); the new versions, the old ones and
  what views derive from both go in one flush. So a change applies all or nothing, two changes of
  the same rows from different nodes at once don't lose either (invariant 56), and a change
  streams like any other write. A follower hands the statement to the leader
  (`/cluster/change`), and so does `pondra sql` (or leads for the moment, as for any write). A
  retried job id changes nothing twice (its producers are `sql:{job}` and `sql:{job}:deleted`).
- **`MERGE` does what the SQL standard says.** A row takes the first `WHEN` of its kind whose
  condition holds; a target row that matches two source rows is an error, not a guess (checked
  first, `GROUP BY _row_id HAVING count(*) > 1`); `NOT MATCHED` inserts, `NOT MATCHED BY SOURCE`
  updates or deletes target rows no source row matches. `INSERT` without a column list takes the
  table's columns in order. It answers `{"rows", "updated", "deleted", "inserted"}`.
- **Keyed tables** change as they always did — upserts and delete markers through the log — and
  now keep the row's `_row_id` through an `UPDATE` (`rows_sql` carries it). `MERGE` into a keyed
  table is the leader's too: new versions, and delete markers only for rows no new version
  replaces.

### 3. Streaming follows every change

- **Views take the old rows back** (`views::derive`, in the same flush as the change). A view
  that adds up (sum and count, grouped) subtracts the old rows' aggregates; a group whose count
  reaches 0 is gone (`query::live`). A row-by-row view (filters and projections over its source
  alone: `View::ids`, decided when it is made) carries each source row's `_row_id` and
  `_created_at` through its query (`carry`: a column appended to every projection of its plan),
  and drops its rows of old versions into `{view}$deleted` — so it is changed exactly as its
  source is. Everything else can't take a row back: a view that emits windows or sessions once, one
  that keeps a min or max, one that reads another table (it would find that table as it is now,
  not as it was), and streaming tasks. While one of them follows a table, a change of the table
  is refused, with the reason (`views::can_follow`).
- **The change feed** (`change::feed`) is Delta's change data feed: each row with its system
  columns and `_change_type` — `insert`, `update_preimage`, `update_postimage`, `delete` (a keyed
  table: `upsert`, `delete`) — commit by commit, old versions first. `GET /watch/{t}?changes=true`
  pushes it as NDJSON; MCP's `changes` tool gives it a page at a time (it now returns every row
  of a page: it used to cut a page at 1,000 rows and move its position past the rest).
- **Delta and Iceberg readers** see a change once a purge has rewritten the files (decision 4):
  for a published table, in the same tiering round, before it is published.
- **Kafka consumers** read a topic's log: they see an UPDATE's new rows, not its deletes. Left as
  it is (a tombstone per deleted row would need a key; ADR-011's topics have none).

### 4. Purges: files without the old rows

A changed row's old version stays in its file until a purge rewrites the file without it
(`tier::purge`, one job per group of files, dealt to the nodes like merges):

- **When:** every tiering round for a table that is published (Delta and Iceberg readers can't
  anti-join); otherwise once `PONDRA_PURGE_ROWS` (100,000) old rows wait, or a tenth of the table.
- **Which changes:** those whose old rows are all in files and whose tombstones are too — commits
  up to both tables' `tiered` (invariant 58).
- **Which files:** those whose `_row_id` and `_version` ranges hold an old row. Every file's
  system columns now have min/max statistics, beyond the first 32 columns too, and so do
  manifests; a sealed file to rewrite takes its manifest apart (`manifest::unseal`) and `seal`
  puts the rest away again.
- **How reads know:** the table's entry records the commits purged (`TableMeta::purges`); a read
  skips `{t}$deleted` rows up to the mark of the entry it read, and its files by their ranges. A
  spread query's slices get the coordinator's mark (`Part::purged`). A `{t}$deleted` file whose
  changes were all purged goes once every reader has passed that purge (`retain_ms` later).

### 5. A query spreads only when it pays

Before a query goes across the nodes, the node it came to weighs the two (`guard.rs`):

- **The cost:** the bytes its plan would move — DataFusion's estimates at each exchange (a hash
  exchange's rows but the share each node keeps, an all-gather's to every other node, a whole
  table's own keys nothing) and what reaches the coordinator — at the slowest link's speed,
  compressed a third as the wire carries them, plus a round trip or three per step.
- **The link:** measured from each node to the others (a few empty requests for the round trip,
  8 MB of noise for the rate), every 10 minutes; or given (`PONDRA_LINK=ms,MB/s`).
- **The saving:** what the query takes on one node, less a node's share: the time it took here
  when last asked (the same text, comments aside), else the bytes of the tables it reads at the
  rate this node has read tables lately. A query nothing is known about runs here, and teaches it.
- `?spread=1` spreads anyway (the tests); `?spread=0` never.

By the last bench's own numbers, a shuffle of a few tens of MB on that network costs more than a
second, against under a second saved, so the twelve queries that shuffled should stay on one node
and the cluster take what one node takes; in one data centre (1–2 GB/s, under a millisecond) the
same queries spread. The next cluster bench measures both (decision 6).

### 6. The bench waits for a settled lake

`driver.py` now asks the leader to tier until a round finds nothing to do (every row tiered,
merges and sealing done) before it times anything, and runs each query three ways: on one node,
as the cluster decides (the guard), and spread anyway. The workflows use actions on Node 24
(`checkout@v5`, `setup-python@v6`, `upload-artifact@v6`, `download-artifact@v7`, `setup-node@v5`):
GitHub is retiring Node 20.

### 7. What the owner's second Windows session found

- **`CREATE DATABASE l2`** makes a new lake in a folder beside this one (`LOCATION '…'` for
  another place) and attaches it; **`ATTACH` of a folder with nothing in it** makes the lake there,
  as DuckDB's `ATTACH` makes a new file (the answer says `"created": true`). A node never leads
  another lake in its own process: that lake's catalog writer would stay open in it, and the
  lake's next leader would fence it. A `pondra sql` of its own does it and ends
  (`inbox::lead_once`) — for a write to an attached lake nobody leads, too.
- **The lake's name on Windows** is its folder's (`mylake` of `D:\…\mylake`): the path was split
  at `/` only.
- **The shell reads files on its own machine**, as DuckDB's does: `SELECT * FROM 'D:\…\x.csv'`,
  `CREATE TABLE t AS SELECT * FROM 'x.parquet'`, `MERGE INTO t USING 'new.csv' s ON …`. The node the
  shell starts gets a key only the shell knows (`PONDRA_OWNER_KEY`); requests with it may read
  files, and nothing else may (invariant 21 holds for everyone else). `local()` in Python and
  JavaScript does the same.
- **`CHECKPOINT`** tiers every table's log into Parquet now and writes the catalog down.
- **`ALTER TABLE t SET (publish = …, cluster_by = …, ttl = …)`** changes a table's options;
  `partition_by` can't change. `CREATE TABLE` with a key and `cluster_by`/`partition_by` says
  which to drop and why.

## What it costs

On the usual 2-vCPU box, a table of 10 M rows in Parquet files (`tools/change_bench.py`,
`logs/round19/change-bench.txt`; reads: `logs/round19/change-reads.txt`, hot columns off):

| Change | Rows | Time |
|---|---|---|
| `UPDATE … WHERE id = 12345` | 1 | 0.024 s |
| `UPDATE … WHERE id % 100 = 1` | 100,000 | 0.26 s |
| `DELETE … WHERE id % 100 = 2` | 100,000 | 0.13–0.21 s |
| `MERGE` of 100,000 source rows (98,000 matched, 2,000 new) | 100,000 | 0.9–1.2 s |
| `UPDATE` of 10% | 1,000,000 | 0.47–0.54 s |

| `SELECT count(*), sum(v)` over the table | From Parquet | With hot columns |
|---|---|---|
| never changed | 0.07–0.08 s | 0.015–0.026 s |
| one row changed (waiting, then tiered) | 0.08–0.09 s | 0.03 s |
| 1% changed, in every file, until the purge | 0.17–0.18 s | 0.09–0.13 s |
| after the purge | 0.07–0.08 s | 0.056 s (the rewritten files' columns not yet hot) |

- **A change costs what reading its rows costs**, plus one commit: nothing is rewritten.
- **A read of a changed table** holds the old rows waiting for a purge as a list, and only the
  files whose `_row_id` and `_version` ranges can hold one go through the anti-join; so a point
  change costs a read almost nothing, and a change spread over every file doubles a scan until
  the purge. With none waiting, a changed table reads like any other.
- **A purge** rewrote the 10 M rows' files in 2.6 s (`CHECKPOINT`).
- **Ingest** is where it was: a flush stamps its rows' ids from a block held in memory (a new
  block is one small request to the leader per 4 billion rows), and the log keeps them as one
  number per batch. Arrow over HTTP, 8 M rows: 11.1–12.3 M rows/s, round 18's 12.1–12.6 M; Arrow
  Flight: 19.8–20.7 M rows/s in, 10–12 M out, round 18's 17.6–21.0 M and 11.3–12.9 M
  (`logs/round19/ingest-and-tiering.txt`, both glibc 2.17 builds). **Tiering** writes four more
  columns: 8 M rows took 1.11–1.17 s against 0.75–0.78 s, into files 2.5% bigger (36.1 MB); it
  is dealt out to the nodes like any job. Query planning costs what it did
  (`logs/round19/query-latency.txt`).
- **The code:** `change.rs` 380 lines, `sys.rs` 220, `guard.rs` 120; the rest is small changes
  across `views.rs`, `query.rs`, `tier.rs`, `write.rs` and `log.rs`: about 15,300 lines of Rust.

## What is still open

- **The rest of `ALTER TABLE`:** renaming a table or a column, dropping a column, widening a type.
  Files and the log match columns by name, so this needs an id per column that the files carry
  (Iceberg's field ids): round 20.
- **Deletion vectors** (Delta) and position deletes (Iceberg) instead of rewriting files: a purge
  of a big table under constant changes rewrites a lot.
- **More views following changes:** views over joins (they'd need the other table as of the
  change), min/max (they need the group's other rows), windows already emitted (retractions to
  `_final`).
- **A change waits for a tiering round in progress:** they share the lake's lock, and on object
  storage a round with a purge and a Delta publish in it takes seconds (on R2, with a purge every
  round, a change once waited over 30 s). A lock of the changes' own, with the few catalog writes
  a change makes kept under the big one, would let them run alongside.
- **Kafka consumers seeing deletes.**
- **The guard's model is coarse:** DataFusion's estimates after joins can be far off, and the
  one-node rate is an average over queries that differ. The cluster bench says how far; a query's
  own history (asked again) is exact.
- **Materialized views filled from the rows already there** (E7): round 20.

## Tests

- `harness.py changes`: 60 random INSERT/UPDATE/DELETE/MERGE statements on three nodes against a
  model, checked on every node while tiering and purging every round; row ids kept; a retried job
  and `pondra sql`; three nodes changing the same rows at once; the change feed replayed to the
  table; `/watch?changes=true`; Delta readers after the purge; spread queries over the changed
  tables equal to one node; refusals; keyed tables; a restart.
- `harness.py guard`: the same queries through a node that sees a slow network (40 ms, 60 MB/s)
  stay on it when they would shuffle (one that moves nothing may pay even there: on R2 it did),
  through one that sees a fast one spread, `?spread=1` spreads, a query nothing is
  known about stays, the answers are the same every way.
- `smoke.py` (Linux, Windows, macOS in CI): the shell reads a file named like a download,
  `CREATE DATABASE`, a node refusing files to anyone else, the lake named after its folder, an
  UPDATE keeping its rows.
- `tools/change_bench.py`: what changes and reads of a changed table cost.
