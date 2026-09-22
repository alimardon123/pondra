# ADR-012: Toward petabytes: table metadata that stays small, partitions, memory limits, shuffles, Arrow Flight

**Status:** Accepted, built and tested (round 11) · **Date:** 2026-09-22 · **Builds on:** ADR-003, ADR-005, ADR-011

## Context

After round 10 we listed what stood between the prototype and petabyte tables:

1. **Every file was listed in the table's catalog entry.** Every commit to the table rewrote that
   list. A petabyte in 1 GB files is a million files, so each commit would write ~300 MB.
2. **No partitions, and no statistics outside the Parquet files.** A query read the footer of
   every file to decide which ones to skip.
3. **No memory limit.** One big sort or join could take a node down.
4. **Distributed queries gathered everything at one node.** Every node's partial result went
   to the node that received the query, which then did the rest alone. So high-cardinality
   GROUP BYs and joins of two big tables didn't get faster with more nodes.
5. **No Arrow Flight.** Arrow went in and out over HTTP; ADBC and Flight SQL drivers couldn't
   connect.
6. **No metrics, and no multi-machine numbers.**

Round 11 takes the first five, adds `/metrics`, and ships a kit for the multi-machine numbers.

## Decisions

### 1. Table metadata that stays small (`manifest.rs`)

- **Statistics.**
  - Every Parquet file of an append table carries the min and max of each of its first 32
    columns (`DataFile.stats`), as Delta does, so a wide table's entry stays small.
  - They're kept as text that casts back exactly; timestamps are ISO 8601.
  - Strings longer than 64 characters get no range.
- **Manifests.**
  - A table's catalog entry lists at most `INLINE` = 128 files.
  - Beyond that, all but the newest 64 are **sealed** into immutable *manifest* objects: their
    `DataFile`s as zstd JSON, up to 4,096 per manifest, grouped by partition, stored under
    `data/{table}/_manifests/`.
  - The entry keeps one *manifest list* object: each manifest's path, file count, rows, bytes
    and column ranges, plus the totals.
  - Small manifests are merged 16 at a time, so a list has about one entry per 4,096 files.
  - It's Iceberg's layout, kept by Pondra's leader, so a commit writes the same ~20 KB however
    big the table grows.
- **Reading.**
  1. A query reads the list.
  2. It skips every manifest whose ranges can't match its filters.
  3. It loads the rest, 32 at a time, through a 256 MB in-memory cache and the SSD tier.
  4. It skips files the same way.
  5. It opens only what's left.

  Pruning is DataFusion's own `PruningPredicate` over these statistics, so it works for any
  filter DataFusion can prune on: ranges, equality, `IN`, `LIKE 'x%'`, and ANDs and ORs of them.
- **Writing.**
  - A big INSERT's files are sealed as soon as they're recorded.
  - Maintenance merges a partition's small files before sealing them, because a manifest never
    changes.
  - Maintenance now also runs for tables that have files to merge or seal but no log traffic.
    Before, tables written only by INSERT never had their small files merged.
- **Everything else follows the sealed files.**
  - Distributed queries deal out manifests, not files, when there are many.
  - Delta and Iceberg publish the sealed files too. A table unchanged since its last publish
    is skipped without loading its manifests.
  - Replaced lists and merged manifests go to the table's garbage, like replaced Parquet files.
  - Orphan collection keeps every manifest's files.

### 2. Partitions (`partition_by`)

- **Declaring.** `CREATE TABLE … WITH (partition_by = 'day(ts)')`, or `"partition_by"` in a JSON
  definition.
  - A partition is a column, or year / month / day / hour of a timestamp or date column.
  - Append tables only. It's fixed at creation: a re-sent definition may repeat it, not change it.
- **Writing.** Every flush, INSERT and merge splits its rows by partition, so each file holds one
  value (`DataFile.part`). Merges stay within a partition.
- **Reading.** Queries prune partitions through the file statistics; no separate partition logic
  is needed.
- **Layout.** Files stay in the table's folder (no Hive-style paths). To Delta and Iceberg the
  table is published unpartitioned, which is correct, just without partition pruning there.

### 3. Memory limits and spilling

- **The limit.** `serve --memory-gb N` (also `PONDRA_MEMORY_GB`; default: half the machine's
  memory) is one `FairSpillPool` shared by all queries. Sorts, aggregations and sort-merge joins
  that need more spill to the temp directory.
- **Joins.** Hash joins can't spill. A query that runs out of memory runs once more *frugally*:
  sort-merge joins, and 1 MB kept aside per sort instead of 10.
- **Beyond that,** the query fails with "Resources exhausted" and the node stays up.
- **One case that can't spill:** DataFusion's TopK with a huge `LIMIT` or `OFFSET`. It fails
  cleanly.

### 4. Shuffles (`spmd.rs`)

A distributed query still starts from DataFusion's own parallel plan, and every node plans the
same SQL.

**The gather way.** When the plan's first exchange gathers — a global aggregate, a sort — it's
the ADR-005 path:

- the main table is sliced;
- the other tables are read whole by every node;
- each node runs up to the exchange;
- the coordinator finishes.

**Otherwise, a shuffle.**

1. **Slices.**
   - First try: the main table and every table of 64 MB or more are sliced; smaller tables are
     read whole by every node, so joins broadcast them.
   - If that plan can't be split: every table is sliced, and joins shuffle both sides.
2. **Stages.** Each hash exchange (`RepartitionExec` by hash) becomes a shuffle, and the plan is
   cut there into stages, run bottom-up. For each step, on every node at once:
   1. The node fetches its bucket of each exchange below the stage from every node
      (`GET /cluster/shuffle`).
   2. It runs the stage.
   3. It splits the output by the exchange's hash into one bucket per node, and keeps them.

   The hash is DataFusion's own, with a fixed seed, so two sides of a join land alike.
3. **The last stage** runs on every node over its buckets, up to the plan's first gather, and
   its output goes to the coordinator, which runs the rest (merge, final sort, limit).

**Which plans qualify.** Every operator's output is classified by how its rows are spread over
the nodes:

- **whole:** every node has all of them — a table read whole;
- **split:** each node has its own share — a sliced table;
- **keyed:** split, and all of a key's rows are on one node — after a shuffle.

The rules:

- A shuffle needs a split input. It makes the output keyed.
- A hash exchange over a whole input stays inside the node. Shuffled, each row would arrive once
  from every node.
- A final or partitioned aggregate, or a window partitioned by key, needs keyed input.
- A join needs keyed × keyed, or whole on at least one side.
- A sort, limit or window over all rows needs whole input.
- Anything with only whole inputs is whole.
- What reaches the coordinator must be split or keyed.

A plan that breaks a rule runs the gather way, or on one node. Among the queries this covers:

- GROUP BY with many groups;
- DISTINCT and `count(DISTINCT)` per group;
- HAVING;
- joins, then GROUP BY on another key;
- windows `PARTITION BY` a key;
- self-joins;
- broadcast joins with keyed tables.

**Agreeing on the plan.**

- A node's slice is scanned through `ShareExec`, which reports the *whole* table's size. So every
  node picks the same join order and build side, whatever its slice holds; before, a node with
  an empty slice planned differently.
- Filters still push down through it to the Parquet scan.
- Every step, each node returns the shape of its plan above the scans. The coordinator checks
  they're identical, and falls back to one node if not.

**Limits.**

- Buckets are held in memory, not spilled.
- A step that fails fails the query, which then runs on one node.
- Only one SELECT with inner joins, as before.

### 5. Arrow Flight and Flight SQL (`flight.rs`, `serve --flight 0.0.0.0:8815`)

- **Flight SQL**, for ADBC, JDBC and other Flight SQL drivers:
  - queries;
  - writes (`CommandStatementUpdate`, or a write sent as a query, as DB-API's `execute` does);
  - bulk ingest (`CommandStatementIngest`: ADBC's `adbc_ingest`, which creates the table from the
    Arrow schema if it's new);
  - catalogs, schemas and tables with their Arrow schemas;
  - `GetSqlInfo`.
- **Plain Flight**, for pyarrow and any Flight client:
  - **`DoPut`.** The path is `[table]`, or `[table, producer, first seq]` for exactly-once:
    batch i is seq `first + i`, so a retried stream is applied once. Batches are queued into the
    log as they arrive, and each is answered with its ack as JSON as soon as it's committed.
  - **`DoGet`**, with a JSON ticket: `{"sql": …}`, or a table's **log as a columnar stream**,
    `{"table": "events", "after": N, "columns": [...], "follow": true}`:
    - only those columns (Fluss serves its log with column pruning the same way);
    - with `follow`, every new commit as it lands;
    - after each commit, a metadata-only message `{"after": N}` saying where to resume.
  - `GetFlightInfo` for the same JSON, and `ListFlights`, which lists the tables.
- **Tokens:** `authorization: Bearer`, or the basic-auth handshake with the token as password
  (ADBC's `username/password`, pyarrow's `authenticate_basic_token`).
- **Pipelined batches of one producer now commit in order.**
  - Before, a node encoded up to 4 flushes at once, and they could reach the sequencer in any
    order. A producer's later batch could then arrive first and be refused as out of order. Kafka
    clients resend such a batch; a Flight stream can't.
  - Now flushes are still encoded side by side, but reach the leader's sequencer in the order
    they were cut.
  - Over HTTP from a follower they can still, rarely, overtake each other. The Flight door
    re-queues such a batch once its predecessor is committed.

### 6. `GET /metrics` (Prometheus)

- **The node:** its role, live nodes, and the newest visible segment.
- **Counters:**
  - rows taken in;
  - queries: count, errors and seconds;
  - queries spread, and queries shuffled;
  - files scanned and files skipped by min/max.
- **Memory:** the query memory limit and what's reserved, and resident memory.
- **On the leader:** untiered rows and commit latency.
- **Per table:** files inline and sealed, rows, bytes, and the size of the catalog entry.
- **Access:** the read token.

### 7. Also

- **Delta publishes `TIMESTAMP` columns** (no time zone) as `timestamp_ntz`, with its table
  feature (reader 3 / writer 7). Before, tables with timestamps weren't published to Delta at
  all. delta-rs, Polars and DuckDB read them.
- **`tools/cloud/`**: `cluster.sh` starts and stops a cluster over ssh, on one bucket.
  `bench.py` loads through Flight from many writers, waits until everything is tiered, runs a
  query suite on one node and on the cluster, and saves every node's `/metrics`.

## Results

Local disk, 2 vCPU; the full numbers and runs are in `prototype-status.md` and `logs/round11/`.

- **`metadata_bench.py`.** One table gets 1,000,000 registered files: 1 PiB and 10 trillion rows
  on paper, stats spread over 2000–2019. The files don't exist, so opening one fails the query.
  Then 70k real rows arrive through the log.

  | | before | with 1 M more files |
  |---|---|---|
  | catalog entry | 2.9 KB | 20.4 KB |
  | tiering commit | 10.6 ms | 11.3 ms |
  | INSERT | 5.0 ms | 4.4 ms |
  | append ack | 7.2 ms | 6.6 ms |
  | query over today | 5.0 ms | 12.9 ms |

  - The query over today opened 7 files and skipped 1,000,006.
  - Planning a query over one day of 2010 picks its 138 files in 29 ms.
  - A restarted node answers in 35 ms, and 3 nodes dealing out the manifests give the same
    answer in 35 ms.
  - Registering the million files took 15.5 s, in 20 calls.
- **`harness.py scale`: 9 checks.**
  - 23,100 rows over 139 daily partitions.
  - Every Parquet file holds one day; a day's query reads 2 of 237 files.
  - The files were sealed into manifests.
  - 3 nodes give the same answers.
  - 14 queries against one node: all equal, 11 shuffled.
  - 6 outside readers (Delta and Iceberg) see the sealed files.
  - Under a 50 MB limit: a 2 M-group aggregation, a 3 M-row window sort and a 3 M × 3 M join.
- **`cluster.py spread`:** 9 queries on 4 M rows, spread over 3 nodes, identical to one node,
  with no fallbacks.
- **`harness.py flight`: 13 checks.**
  - pyarrow `DoPut` of 10 batches: a retried stream applied once; through a follower too.
  - A read token refused.
  - `DoGet` SQL, `GetFlightInfo`, `ListFlights`.
  - The log with two columns, and following new commits.
  - The basic-auth handshake.
  - ADBC: a query, an INSERT sent as a query, `adbc_ingest` into a new table, `adbc_get_objects`.
- **`flight_bench.py`** (1 node, 4 pyarrow writers of 10k-row batches):
  - **15.7 M rows/s (437 MB/s) in, exactly-once**;
  - `SELECT *` out at **9.1 M rows/s (255 MB/s)**; it varies from run to run (9–15 M rows/s);
  - a log subscriber gets each new commit in **2.6 ms p50, 5.1 ms p99**.
- **Everything from earlier rounds** passes unchanged (`harness.py all`).

## Consequences

- **New invariants** (AGENTS.md):
  - 25: an append table's entry lists ≤ 128 files; the rest are in immutable manifests reached
    through one list object.
  - 26: a partitioned table's files each hold one partition value.
  - 27: a node's slice reports the whole table's size.
  - 28: a shuffle's plan is identical on every node, checked every step.
  - 29: flushes reach the sequencer in the order they were cut.
- **New dependencies:** `arrow-flight` with `tonic` and `prost` (gRPC; hyper and h2 were
  already built). `base64` is a direct dependency now; it was already built.
- **Size:** the binary grew by 2.0 MB, to 95.0 MB (32.0 MB gzip, 18.0 MB xz). Idle memory with
  `--kafka --flight --pg` is 42 MB.
- **Still between Pondra and petabytes,** most important first:
  1. Numbers from several machines. The kit is ready.
  2. Shuffle buckets and big query results held in memory. They should stream and spill.
  3. Publishing a huge table to Delta or Iceberg. It rewrites one Iceberg manifest of every
     file, and the publish state lists every file. It should reuse Pondra's manifests.
  4. One sequencer per lake. To scale writes today, attach lakes (ADR-010).
  5. Kafka topics with one partition.
  6. Orphan collection lists the table folders every hour.
  7. Sealed files are never merged again (they're merged before sealing).
