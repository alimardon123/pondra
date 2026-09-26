# ADR-022: Columns that change, views that start full, the latest by event time, and a DataFrame API

**Status:** Accepted, built and tested (round 21; the DataFrame API is designed here and built in
round 22) · **Date:** 2026-09-27 · **Builds on:** ADR-017 (streams on their own time), ADR-019
(a database you can shape), ADR-020 (change any row), ADR-021 (fewer objects and streams)

## Context

Round 21 was planned as "shape it further, and more of Flink" (roadmap): the rest of `ALTER
TABLE`, materialized views filled from the rows already there, deduplication by event time, and
Nexmark against Flink. The owner added:

- **"Think about a Python DataFrame API, as easy as Polars, or directly like PySpark for easy
  migration, with SQL converged in it."** Asked whether to build it now, they chose design only
  this round.
- **"How secure, correct, easy and fast are the nodes' connections?"**, and how to publish to pip
  and npm. They then **licensed Pondra MIT OR Apache-2.0**; a managed cloud service may come
  later.
- **Round 20's cluster bench,** run on GitHub, for this round to read.
- **"Is Pondra in-process, single node, distributed and client–server, as one binary?"** and
  **"How does it compare with Sail?"** (answered in chat; Sail in the comparison doc).

## Decisions

### 1. Rename, drop and widen columns: the catalog's names, never the files'

Files are immutable (invariant 6), and a table can hold millions of them. So a column keeps the
name it was first written under, in every file and log segment, and the catalog says what SQL
calls it now:

- `TableMeta::columns` stays the **stored** columns (names and types), in the order they came.
- `names` maps a stored name to its SQL name (`RENAME COLUMN`); `dropped` lists stored columns
  nothing reads again (`DROP COLUMN`). A dropped column's data stays in older files, unread,
  until compaction rewrites them.
- A column added under a name an older (renamed or dropped) column is stored under is stored as
  `name~2`, `name~3`…, so the two never meet.
- `TableMeta::logical()` is the table as SQL sees it. Reads wrap each table's provider in a
  projection that aliases stored names to SQL names and leaves dropped columns out
  (`query::named`); filters pass through it, so files are still pruned by their min and max.
  Everything that takes rows from users (INSERT, UPDATE, MERGE, HTTP, Flight, Kafka, COPY) works
  in SQL names; the log keeps stored names (`log::pack` renames, `TableMeta::to_stored`), and a
  field under a name SQL no longer knows — a writer from before a rename — is left out, never
  taken for the column now stored under that name.
- **`ALTER COLUMN … TYPE` only widens** (a smaller integer to a bigger one, FLOAT to DOUBLE, a
  DECIMAL to more digits at the same scale): every file already written reads as the new type.
- **Refused:** key, partition, TTL and `order_by` columns can't be dropped; system columns can't
  be touched; a table a view, stored view or task reads can't change its columns (they name
  them); `ALTER TABLE … RENAME TO` says how to do it instead (`CREATE TABLE new AS SELECT * FROM
  old; DROP TABLE old`). A table's name is where its files, log rows, Delta and Iceberg copies
  and Kafka topic live, so renaming a table is a bigger change, left for later.
- **Delta and Iceberg follow.** Iceberg: each column's field id is its place among the stored
  columns, so a renamed column keeps its id and the name mapping names the files' column; a
  dropped one leaves the schema. Delta: column mapping by name (`delta.columnMapping.mode =
  name`, each field's id and physical name), declared as the `columnMapping` table feature
  (reader 3, writer 7). A reader that can't map columns then refuses the table instead of reading
  columns by the wrong names: delta-rs's pyarrow reader and Polars refuse, while delta-rs's
  DataFusion reader, DuckDB (Delta and Iceberg) and PyIceberg read it right, widened types
  included (`harness.py columns`).

### 2. Materialized views filled from the rows already there, every row once

A view used to follow "rows written from now on", and even that edge was loose: a node that
packed a flush before it saw a new view committed that flush without the view's rows.

- **The sequencer holds every flush to the views** (`views::Inline`, checked in `log::commit`).
  A flush carrying rows of a table that views follow must carry a part for each of them (empty
  when a view derived nothing), and none for a view the table doesn't have. A flush packed with
  other views goes back to its node, which packs it again (`Outcome::Retry` with nothing
  settled). So from one commit on, every flush derives a view's rows, and none before it does.
- **That commit is where a view's filling ends.** A new view carries `fill: {id, upto: None}`;
  the first commit that holds flushes to it writes `upto` (its own number less one) in the same
  catalog write (`views::bound`). The leader then runs the view's SQL over its source as it was
  at `upto` — the rows with `_version ≤ upto`, and its changes as of then — and appends the
  result with producer `fill:{view}`, sequence 1, so a retry or a new leader fills it once
  (`views::fill_all`, under the lake's lock). `CREATE MATERIALIZED VIEW` returns once the view
  is filled.
- A bulk INSERT whose writer didn't know of a new view is sent again, through the log
  (`write::record` refuses its files); UPDATE, DELETE and MERGE of a table are refused, with a
  message to try again, while a view of it is filling (the fill reads the table as it was).
- **A dropped view's producers are forgotten.** The sequencer caches producers' last sequence
  numbers; a view dropped and made again under its name would have found its old `emit:`,
  `join:` and `fill:` numbers there and had its first commits taken for duplicates
  (`log::forget_producers`).

Views that are sessions or stream joins read their sources as rows commit and start from their
creation, as before.

### 3. The latest row by event time: `order_by`

`CREATE TABLE t (k BIGINT PRIMARY KEY, ts TIMESTAMP, …) WITH (order_by = 'ts')`: of a key's
rows, the one with the greatest `ts` is current, not the one that came last — Flink's
deduplication "keep last row by event time", as a table. A late row doesn't replace a newer one;
a delete marker carries its row's time, so a late row older than the delete stays deleted, a
newer one brings the key back.

Reads of such a table group every generation's rows by key (`first_value(… ORDER BY ts DESC,
_ord DESC)`) instead of letting newer generations shadow older ones; compaction merges the same
way, so every file still holds one row per key (invariant 13). Lookups and point queries go
through SQL. `ALTER TABLE … SET (order_by = …)` sets it later.

### 4. `SELECT *` on a keyed table shows its own columns

A keyed table's `_deleted` column (what `DELETE` marks) is left out of reads unless the query
names it: reads never return deleted rows, so it was always false or null. `SELECT *, _deleted`
shows it once; lookups and `COPY` leave it out too.

### 5. Nexmark against Flink

A Nexmark subset (`tools/bench/nexmark.py`): the auction site's bids, five of its queries —
q1 currency conversion (row by row), q2 selection, q5 bids per auction in 10 s windows every
2 s, q7 the top bid of each 10 s window, q11 bids per bidder session (3 s gap). Bids are a pure
function of their number, so both engines see the same stream. Flink 2.3 (PyFlink, a local
MiniCluster, parallelism 2, mini-batches) makes them in its own process and writes to blackhole
sinks; Pondra takes them over HTTP from a client as Arrow batches and writes every view to the
lake. Pondra's answers equal DuckDB's over the same bids.

| 4 M bids, all five queries | Pondra | Flink 2.3 |
|---|---|---|
| From the first bid to every query's last row (two runs) | **3.8–5.1 s (0.8–1.1 M bids/s)** | 11.0–11.2 s (0.36 M bids/s) |
| Ingest | over HTTP, from another process | none (generated inside) |
| Output | five views in the lake, durable, queryable | blackhole |
| Each query alone | — | q1 4.0, q2 2.9, q5 6.6, q7 3.8, q11 7.2 s |

With 10 M bids: Pondra 8.7–10.9 s (0.9–1.2 M bids/s), Flink 24.3–25.0 s (0.4 M bids/s), the same
answers as DuckDB again (`logs/round21/nexmark.txt`). Flink's JVM and cluster start (7 s) are outside its times. On one 2-vCPU
box; Flink's strengths — state far bigger than memory, parallelism across many machines — are
not what this measures.

### 6. A DataFrame API: designed, built next round

`docs/dataframe-api.md` has the design. In short:

- **One engine:** a DataFrame compiles to one SQL statement (CTEs for its steps), so it runs
  through every door, spreads across the nodes, and shows its SQL (`frame.sql`).
- **Two dialects on one tree:** `pondra.frame`, a Polars-style lazy API (`col`, `filter`,
  `with_columns`, `group_by().agg()`, `join`, `join_asof`, `collect()` to Arrow, Polars or pandas);
  and `pondra.spark`, PySpark's `SparkSession`, `DataFrame`, `functions` and `Window`, so a job
  moves by changing its imports. Spark's semantics (nulls first, integer division, names
  regardless of case) are written out in the SQL; what isn't covered says so.
- **SQL and Python used interchangeably** (the owner's point): `con.sql(…)` gives a lazy frame;
  SQL names frames, pandas, Polars and Arrow data by their Python names (as DuckDB does, or by
  keyword as PySpark does); frame methods take SQL snippets; `.sql` files and Python share the
  lake's names (`con.run("file.sql")`, `to_view`); `%%sql` notebook cells; later, pipelines of
  `.sql` and `.py` model files (`pondra run models/`).
- **Writes and streams are SQL's:** `write_table` (CTAS, INSERT), `update`, `delete`, `merge`
  with Delta Lake's builder names, `to_view(materialized=True, window=…)`, `watch()`.
- **Tested differentially** against Polars and PySpark (local), and TPC-H written as frames.
- **Later:** the same builder in JavaScript; Spark Connect served by the binary if Scala or Java
  Spark jobs need to move.

### 7. The cluster bench on round 20

The owner's run (`logs/round21/cluster-bench-round20-run.json`; 3 GitHub runners, links
41–53 ms, 53–69 MB/s): loading TPC-H 105.9 s (442 s on round 19: fixed); one node 24.5 s, as
the cluster decides 24.8 s (8 queries spread: 0.22 s lost, 0.15 s won), spread anyway 34.7 s;
every answer the same. The guard now keeps a cluster on a slow network at one node's speed. It
also showed tiering merges of small files taking 16–28 s on R2 right after a bulk load (left
open).

### 8. The nodes' connections, and publishing

- **Nodes talk plain HTTP.** Correctness doesn't depend on the network (the bucket is the truth,
  writers are fenced, every write is exactly once), and it's easy (one port, found through the
  bucket). But there is no TLS: an admin token set for the nodes' calls crosses in the clear, and
  so do shuffles. Until TLS and mutual TLS between nodes land (roadmap E3), run a cluster inside a
  private network, a VPC or Tailscale.
- **MIT OR Apache-2.0** (`LICENSE-MIT`, `LICENSE-APACHE`), as Rust's own crates are. The wheel
  (metadata 2.4, `License-Expression`) and npm packages carry both files and a `repository`
  (npm's provenance checks it); the do-not-upload marks are gone.
- **The release workflow:** the Intel macOS build moved to `macos-15-intel` (GitHub retired
  `macos-13`); the publish job runs in the `pypi` environment PyPI's trusted publisher names; npm
  publishes with a token the first time and by trusted publishing (OIDC, npm 11.5.1+) after.

### 9. What the tests caught

- **Two publishing rounds wrote one Iceberg version** (`harness.py columns` on real R2): a
  `CHECKPOINT` published while a tiering round's record of its own publish wasn't committed yet,
  both took the next version, and the second failed. Publishing now runs one round at a time and
  commits what it published before the next reads it; an Iceberg version found already written
  (an attempt that crashed) is skipped, as Delta's is adopted.
- **A view made again under a dropped one's name never filled** (`fills`): the sequencer still
  remembered the old `fill:` producer's sequence and took the new fill for a duplicate
  (invariant 78).
- **The sequencer's set of views could be overwritten with an older one** (`fills`, the first
  runs hung): a round that bounded a view put its cache back after a view was made meanwhile,
  and every flush of the new view's rows was refused (invariant 77).
- **Reading by event time was slow** (the cost run): an ordered aggregate per column, 2.3 s on
  200 k keys; now two hash aggregates and two joins, 0.14 s.
- **Two checks were stale:** `schemas` expected a view to start empty, and `anywhere_check`'s
  notebook check hadn't run since round 18 and still expected round 18's numbers.

## What it costs

On one 2-vCPU box (`logs/round21/`):

| | |
|---|---|
| A filter and GROUP BY over 10 M rows, before and after a rename and a drop | 0.033 s / 0.034 s |
| `CREATE MATERIALIZED VIEW` over 10 M rows, filled: a GROUP BY / row by row | 2.6 s / 2.3 s |
| A keyed table of 200 k keys (1.5 M rows: 5 generations of files and the log), read by arrival / by event time | 0.026 s / 0.143 s |

- Reading by event time groups every generation by key (two hash aggregates and two joins),
  where reading by arrival anti-joins each generation against the newer keys: 5–6× the time
  until compaction folds the generations into one. (The first version, one ordered aggregate
  per column, took 2.3 s.)

- A table that was never renamed or dropped reads exactly as before: no projection is added.
- A view over a big table takes the fill's time to create, on the leader, in one go (its rows in
  memory): a view over a billion rows would need the fill done in parts (open).

## What is still open

- **Renaming a table** (its name is where its files and copies live); **narrowing a type**
  (needs files rewritten); a dropped column's bytes stay in older files until they're compacted.
- **Top-N per key by event time**, timers, `MATCH_RECOGNIZE`, a watermark per partition; more of
  Nexmark (q3, q4, q8: joins of persons and auctions), and on more machines.
- **A view's fill in parts** for very large sources; views that are sessions or stream joins
  still start from their creation.
- **TLS and mutual TLS between nodes** (E3), then per-table grants.
- **Tiering merges after a bulk load on R2** (16–28 s rounds in the bench).
- **Sail** (LakeSail, the other DataFusion engine: a Spark Connect server) on the same box in
  `bench/singlenode.py`; what Pondra can take from it is in the comparison doc.

## Tests

- `harness.py columns`: rename, drop, a dropped name added again and a widened type while rows
  stream into two nodes (and one writer keeps an old name), against a model: every node, a query
  spread over three nodes, a bulk INSERT, UPDATE, a keyed table and its lookups; DuckDB (Delta,
  Iceberg), PyIceberg and delta-rs read the same; the refusals.
- `harness.py fills`: views made while two producers stream into two nodes and a bulk INSERT
  writes files, dropped and made again, one made as the leader is killed: each equals its query.
  Without the sequencer's check every view misses rows (12 of 12 checks fail).
- `harness.py dedup`: out-of-order rows over two nodes and tiering rounds against a model;
  lookups, point queries, a DELETE and late rows, an UPDATE, compaction, DuckDB over Delta;
  `SELECT *` without `_deleted`.
- `tools/bench/nexmark.py`: Nexmark's q1, q2, q5, q7, q11 on Pondra and Flink; Pondra's answers
  equal DuckDB's.
