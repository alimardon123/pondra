# Prototype status: Pondra, a streamhouse in one binary

**Date:** 2026-09-22 (round 10) · **Plan:** ADR-002 to ADR-011 · **Code:** `pondra.zip` / `pondra.bundle` (≈7,300 lines of Rust, plus a Python client and test and benchmark tools)
**Name:** the prototype formerly called `lh` is now **Pondra**. The name is free on crates.io, PyPI and npm. A small personal-finance app uses it (pondra.app), a different category; run a trademark search before a public launch.

## Where it stands

One Rust binary replaces the Kafka + Flink + Spark + metastore + ZooKeeper stack for the common jobs:

- stream ingest, exactly-once;
- streaming SQL: views with no lag, and stateful tasks;
- push to clients;
- batch ELT and SQL, distributed across nodes;
- upsert and merge tables.

Start more copies on the same bucket to scale out. The only state is object storage. There's no JVM, no database server and no coordination service.

**Round 10 opened the doors other systems use** (ADR-011):

1. **The Kafka protocol.**
   - Kafka producers write to tables:
     - JSON values become rows;
     - idempotent producers are exactly-once;
     - Debezium change events and tombstones become upserts and deletes.
   - Consumers and consumer groups read the log back.
   - Measured on one box with 3 nodes, via librdkafka:
     - **~0.8 M events/s**, exactly-once;
     - ack **1 ms p50**, and a consumer on another node gets the event 1 ms later;
     - on real R2: 461k events/s, ack 1 ms p50 (replicated).
2. **An Iceberg REST catalog.** PyIceberg and DuckDB attach a node by URL.
3. **`ALTER TABLE … ADD COLUMN`**, under load, with Delta and Iceberg following.
4. **Event-time windows that close:** each window is emitted once, final, past a watermark.
5. **JSON functions** (`json_get`, `->>`) over text.

**Round 9 let every machine write and every tool connect** (ADR-010):

1. **Writes from anywhere, with the same capabilities.**
   - A machine that can reach the bucket but not the leader writes through the bucket inbox
     (1.0 s locally, exactly-once).
   - Several clusters share one bucket, each leading its own lake and attaching the others
     (cross-lake joins; writes recorded by the owning leader).
2. **SQL writes everywhere:** `CREATE TABLE`, `INSERT`, `UPDATE`, `DELETE`.
   - They run on any node, over the **Postgres protocol** (psql, psycopg 2/3, asyncpg,
     SQLAlchemy tested), over **MCP** for AI agents, or from `pondra sql` on any machine.
   - A **Python client** reads into pandas, Polars and Arrow.
3. **What competitors are building next, answered where it was cheap.**
   - Fluss 1.0 lists the Postgres protocol and MCP as future work; Pondra has both now.
   - Vector search in SQL (`cosine_distance`), as Flink 2.2 added `VECTOR_SEARCH`.
   - Read / write / admin tokens on every door. SQL sent to a node can no longer touch the
     node's disk.
4. **Keyed tables got cheaper and more capable:**
   - size-tiered compaction (3x fewer bytes written);
   - row TTL;
   - the log as a replayable change feed.

   Replicated acks gained `--fsync` and were tested with 3 replicas.

**Round 8 made the native lake the default and writes fast on any storage** (ADR-009):

1. **Millisecond writes without S3 Express.** With `--ack replicated`, a write is acknowledged once
   two nodes hold it and reaches the bucket a moment later. On real R2: **4 ms p50** (was 299 ms),
   and 64 writers went from 6,400 to **87,200 events/s**. A new leader recovers what followers
   held — up to 279 commits in one failover — with 0 lost and 0 duplicated.
2. **Native first; Delta and Iceberg on request.** Pondra's readers use the catalog directly:
   nodes see a write 10–15 ms after the ack, on local disk or R2. Tables that ask are also
   published as Delta and, new, **Iceberg**. Six outside readers (delta-rs, Polars, DuckDB × 2,
   PyIceberg) match Pondra row for row, on local disk and real R2.
3. **Writes from any machine.** `pondra sql "INSERT INTO t SELECT …"` runs the query where it's
   typed and has the leader record the files — or records them itself when nobody runs. A node
   starting on an idle lake leads at once (was a 30 s wait).
4. **`cluster_by` for append tables:** selective filters 6–11x faster.
5. **Freshness compared like for like** with Fluss (memory/SSD vs memory/SSD, object storage vs
   object storage): see `docs/comparison-spark-flink-fluss.md`.

**Round 7 made it a serving engine at Lakehouse//RT speed, and measured it against Spark on TPC-H:**

1. **Point reads skip SQL:** 0.14–0.22 ms, and **20,000–36,000 lookups/s on two cores** (was
   ~200/s). SQL point queries take the same path.
2. **Repeated dashboards come from a result cache** that is exact until the next commit: about
   20,000–38,000/s. `?stale_ms=` trades exactness for throughput when tables change every few
   milliseconds.
3. **Keyed tables read as anti-joins** instead of grouping every row by key: dashboards on them
   went from 420 ms to 6–42 ms.
4. **TPC-H SF1: all 22 queries in 5.9 s**, against 58–65 s for Spark 4.2 on the same machine.
   The answers are the same; DuckDB, the single-node reference, takes 3.8 s.

The honest comparison with Spark, Flink, Fluss and Lakehouse//RT — where Pondra wins today, where
it doesn't yet, and the plan — is `docs/comparison-spark-flink-fluss.md`.

**Round 6 opened the lake to other engines and made first reads on object storage fast:**

1. **Every table is also a Delta Lake table.** Spark, Databricks, DuckDB, Polars, delta-rs,
   Trino and Athena read `<lake>/data/<table>` without Pondra. Three independent readers were
   checked row for row.
   - **Local disk:** a Delta version with a new row exists **17 ms** after the ack, and delta-rs
     reads it at **30 ms** (p50).
   - **Real R2** (from this sandbox): 4.8 s — three object-store round trips of 0.6–1.4 s each.
   - **Fluss, for comparison:** its lake lags 3 minutes by default.
2. **A new user's first query on R2 takes 20–30 ms** on a serving node (it was 3.0 s), and
   **≈0.95 s on a node that just joined** with an empty disk. Each node keeps recent objects on a
   local SSD tier and the whole catalog in memory.
3. **The local folder and the bucket use exactly the same layout** (`docs/lake-format.md`).

**Round 4 removed the two limits round 3 left, and round 5 made it a serving engine too:**

1. **Writes no longer go through one machine.** Every node ingests: it encodes, stores and aggregates the data it receives. The leader only hands out the order of commits, and even the tiering work is dealt out to all nodes.
2. **Reactions take milliseconds.** An event written to one node shows up, aggregated, on a client watching another node 7 ms later (p50, local disk; round 3: ~0.8 s), and on a read-only serving node just as fast. On object storage it costs one storage write — on a real R2 bucket, 1.0 s from this sandbox, where a bare PUT is 1.07 s.
3. **Serving reads are a first-class path** (round 5): one key out of 2 M in 7 ms, and keyed tables no longer rewrite themselves on every tiering round — which also took durable ingest from 2.1 M to 3.8 M events/s.

**R2: now measured for real.** The whole suite ran against a Cloudflare R2 bucket — see "On real
R2" below. Everything also still runs on local disk and on a local S3 server with R2-like latency
(PUT p50 197 ms, GET p50 100 ms); `tools/r2_test.sh` runs the main tests against any bucket.

## The old limit, in plain words

Round 3's report said "one leader commits all writes, so write capacity grows with a bigger machine, not with more machines."

- **Before:** imagine a shop where one cashier also packs every bag. More doors (nodes) let more customers in, but all of them still queued at that one cashier.
- **Now:** every node packs its own bags. It parses the batch, compresses it, writes it to the bucket and updates the views. The leader only hands out numbered tickets: "your batch is number 1,042". A ticket is a few bytes of metadata, so one leader can keep many nodes busy.

**Measured** (all three nodes share one 2-vCPU VM):

- 8 producers wrote 3.8 M events/s through the two followers (2.1 M before round 5's tiering work).
- The leader used 31 % of the cluster's CPU, below its one-third share.
- Before the tiering work was also dealt out, the leader's share was 70 %.

## Round 4: every node writes

| Feature | Replaces | Notes |
|---|---|---|
| **Every node ingests** | Kafka brokers | Nodes batch, encode (Arrow IPC + ZSTD) and store their own writes. Small flushes (≤64 KB) ride inside the commit request; the leader dedupes, numbers and commits. 4 flushes in flight per node, 4 commits in flight at the leader; no fixed flush window |
| **Commit stream** | ZooKeeper watches, Kafka replication | The leader pushes each durable commit to every follower over one HTTP response. Followers lay it over their own catalog view, so every node sees a commit within milliseconds |
| **Inline views** (`POST /views/{name}`) | Flink SQL jobs + keyed state | Run on every flush, on the node that received it, and commit with their input: never behind, exactly-once for free. GROUP BY views become **merge tables** (sum / count / min / max partials, merged on read) that any number of nodes update at once |
| **Push** (`GET /watch/{table}`) | Kafka consumers | New rows as NDJSON the moment they commit, on any node |
| **Event-driven tasks** | Flink jobs | Stateful tasks run as soon as rows commit (not every second) |
| **SPMD queries** | Trino / Spark SQL | Every node runs the same plan over its slice of files up to DataFusion's first exchange; the receiving node finishes it |
| **Distributed maintenance** | Spark OPTIMIZE jobs | The leader decides; converting the log to Parquet, merging small files and compaction run as jobs dealt to all nodes |
| **Backpressure** | — | While 10 M rows wait to be tiered, commits pause; tiering starts as soon as 1 M rows wait, in chunks of ≤4 M |

### What the tests found, and what was fixed

- **Lost task rows in about 1 failover run in 8.** A follower combines its own catalog view with the commits streamed to it, and three races could make a read see less than the node thought it had:
  - the view moving past the stream while a read was in flight;
  - streamed copies dropped on a 5-second timer while a slow read still needed them;
  - the "how many segments exist" mark being read from a *newer* view than the one a read was held to, so a streaming task read a range of the log that its own view couldn't fully see yet, and committed progress past the rows it missed.

  Now: a read checks its view before and after reading and decides under one lock, it pins what it needs, and the segment count always comes from a view at most as new as the read's. Twenty failover runs have passed since.
- **The leader did 70 % of the cluster's work**, because tiering, file merges and compaction ran there. They're now jobs dealt out to all nodes; the leader's share is 30 %.
- **A tiering job on a node whose view lagged could have written an incomplete file.** A job now names its inputs exactly and waits until the node sees the last log segment; otherwise it fails and the next round retries.
- **Catalog writes stalled ~30 s on simulated R2** when SlateDB's default limit of 8 level-0 files was reached. Raised to 64.
- **A follower that misses commits now restarts** instead of serving reads that could go back in time.

## Round 5: serving reads, and keyed tables that don't rewrite themselves

Two things were wrong for a system that wants to replace the serving tier as well as the lake:
every tiering round rewrote a keyed table in full, and every read of one paid a window function
over the whole table. Fixing them also made ingest much faster.

| Change | Effect |
|---|---|
| **Keyed tables are LSM-like.** Each round folds the log tail into a *new* file (a range per node, like append tables); files are compacted into one only when 8 pile up. Each file carries the last segment it covers, so a newer file's row wins for a key | Tiering costs what the new rows cost, not what the table costs |
| **Files that are already 64 MB or 4 M rows are left alone** by small-file merges | Rows are written once, not merged over and over |
| **Files are written for lookups:** sorted by key, a bloom filter per key column, 256k-row row groups | A lookup reads one row group, not a whole file |
| **`GET /lookup/{table}/{key}`** plans a lookup (single thread, no window) instead of a scan | A serving API that doesn't depend on SQL planning; today it costs the same as the equivalent SQL, not less |
| **"Newest version per key" is a grouped aggregate, not a window** | Scans of keyed tables 3.5x faster (2 M rows: 1,234 ms → 351 ms) |
| **Compacted tables skip deduplication entirely** (one file, nothing newer in the log = one row per key already) | A served table reads like plain Parquet |
| **Read-only nodes follow the leader's commit stream** instead of polling their own catalog view | Freshness on a serving node: 163 ms → **7 ms** p50 |

What it did to throughput (same box, same tests, round 4 → round 5):

| | Round 4 | Round 5 |
|---|---|---|
| Durable ingest, 3 nodes, producers writing to the followers | 2.1 M events/s | **3.8 M events/s** (leader 31 % of cluster CPU) |
| Saturated ingest through an aggregating view, one node | 1.23 M events/s | **2.84 M events/s** |
| 10 M events, ingest + keyed aggregation, one node | 3.97 s | 4.08 s |
| Batch: write 20 M rows + 6 queries | 3.08 s + 0.04–0.46 s | 3.15 s + 0.04–0.48 s |

Serving, measured with `tools/serve_bench.py` (2 M keys, one node, 2 vCPUs, local disk):

| Query | Result |
|---|---|
| Point lookup, 1 client | **7.7 ms** p50 (`/lookup`: 8.9 ms), 11 ms p99 |
| Point lookup, 8 clients | 30 ms p50, 46 ms p99, **265 lookups/s** (the two cores are the ceiling) |
| Point lookup, 8 clients, while 200-row batches keep arriving | 44 ms p50, 99 ms p99 |
| Dashboard aggregate over all 2 M rows | 351 ms (1,234 ms before this round) |
| Freshness on a read-only node (write → visible) | 7 ms p50, 84 ms p99 |

The ceiling of ~265 lookups/s is this box's two cores, not the design: each lookup costs ~7 ms of
CPU, most of it planning and Parquet decoding. Caching prepared plans per table version is the
obvious next step, and read-only nodes already scale the rest horizontally.

**What the tests missed.** The first version of this tiering rewrite stopped tiering an append
table once it had 8+ files. Reads stayed correct — the log just kept growing — so every
correctness test passed while sustained ingest fell by half. It was found by benchmarking, not by
testing. There is now a `tiering` test (rounds of writes + `/tier`: the log must drain and the
file count stay bounded) and a note in `AGENTS.md` that this failure mode shows up as throughput,
not as a red test.

## Round 10: the Kafka protocol, an Iceberg REST catalog, schema evolution, windows that close

**What's new** (ADR-011):

| | What | Measured |
|---|---|---|
| Kafka protocol | `--kafka`: a topic is a table. Producers: JSON values → rows (`_key`, `_timestamp`, raw `_value` columns), idempotent producers exactly-once, all 4 codecs, Debezium events and tombstones → upserts and deletes. Consumers read the log; consumer groups coordinated by the leader; SASL/PLAIN tokens | 3 nodes, 4 librdkafka producers, one 2-vCPU box: **776k events/s** durable, **796k** replicated, every event exactly once. Ack **1 ms p50 / 2 ms p99** (replicated), 3 / 21 ms (durable); a consumer on another node gets it 1 / 2 ms after the send (replicated) |
| Iceberg REST catalog | `GET /v1/…` on every node: engines attach by URL | PyIceberg and DuckDB attach it; 8 outside readers in `open_check.py` equal Pondra |
| Publishing | Timestamps kept in µs, so tables with timestamps publish; a keyed table's first file publishes at once; published keyed tables compact fully | timestamps read back right by PyIceberg and DuckDB |
| `ALTER TABLE … ADD COLUMN` | Any node, Postgres, MCP, `pondra sql`; old rows read null; writes that don't know the column keep working; Delta and Iceberg follow | under load (65–72k rows written during the change): 8 checks, 6 outside readers |
| Windows that close | `?window=w&size_secs=&lateness_secs=` on a `date_bin` GROUP BY view: `{view}_final` gets each window once, final, past the watermark | each window once; a late row updates the view only; a leader restart emits nothing twice |
| JSON functions | `json_get…`, `json_contains`, `json_length`, `->`, `->>` | over HTTP and Postgres; Kafka raw values queried as JSON |

**Regression, local disk** (`logs/round10/local-regression.txt`):

- `harness.py all`: every test passes, now with `kafka` (9 checks), `alter` (8) and `windows` (3).
- `crash --size 50000`: 3 runs × 9 M events, 0 lost, 0 duplicated, views exact.
- `users`: 3 runs, 129.5–138.1k events/s, ack p50 36–38 ms, 0 torn reads, 0 lost or duplicated batches.
- `failover`: 3 durable and 2 replicated runs, back in 4.3–5.1 s, state == view == model.
- race, isolate, split (4.97 M events/s), spread (identical): pass.
- latency: event → view row on another node 6 ms p50, 9 ms p99.
- `open_check.py`: all 8 outside readers (the REST catalog's two included) equal Pondra.
- `keyed_bench.py`: 17.9 MB written vs 53.4 MB for full rewrites (unchanged from round 9).

**Simulated R2** (`logs/round10/sim-r2.txt`):

- `kafka`: 9 of 9, the same checks as locally.
- `alter` (8 of 8) and `windows` (3 of 3): pass.
- `open_check.py`: all 8 outside readers, the REST catalog's included, equal Pondra.
- `kafka_bench.py`, 3 nodes, 1 M events:
  - replicated: 683k events/s, ack 1 ms p50 / 5 ms p99;
  - durable (one PUT per commit): 84.6k events/s, ack 231 / 764 ms.

**Real R2** (`ponderabucket-us`, `logs/round10/r2.txt`):

- `kafka`: 9 of 9 — the same producers, Debezium, consumers, groups and tokens, on R2.
- `alter` (8 of 8) and `windows` (3 of 3): pass.
- `open_check.py`: all 8 outside readers equal Pondra, the REST catalog's included.
- `kafka_bench.py`, 3 nodes, 400k events:
  - replicated: **461k events/s**, ack **1 ms p50** / 20 ms p99, a consumer on another node
    1 / 20 ms;
  - durable: 68.7k events/s, ack 245 / 606 ms (one PUT per commit).
- Every test lake was deleted when its test finished; the buckets hold only the three kept
  lakes.

### What the tests caught this round

- **Offsets across segment boundaries.** The first group test expected a consumer to resume at
  the last committed offset + 1. Offsets are `_ord`, so after a segment's last row the next
  offset is the next segment's first; the check (not the code) was wrong.
- **Keyed tables created in SQL stopped publishing** since round 9 added `_deleted` to them:
  they waited for a full compaction, which size-tiered merging made rare. Now a first file
  publishes at once, and published keyed tables compact fully.
- **Tables with timestamp columns never published** (nanoseconds, which Iceberg v2 can't hold).
  SQL timestamps are now microseconds.
- **Arrow appends missing a column were refused**, which would have broken every producer after
  an ALTER. Missing columns are now null, as in JSON.

## Round 9: everyone writes, everything speaks SQL

**What's new** (ADR-010):

| | What | Measured |
|---|---|---|
| Bucket inbox | A machine that can reach the bucket but not the leader leaves its write in `inbox/`; the leader answers within a second | `pondra sql` INSERT through the inbox: 1.0 s locally, 6.1 s on simulated R2, 7.3 s on real R2 (the whole process, catalog open included); a retry is a duplicate |
| Attached lakes | `--attach sales=…`: another lake's tables read as `sales.orders`, joins across lakes, writes recorded by that lake's leader | cross-lake join after a write through the other leader: exact |
| SQL writes | `CREATE TABLE … PRIMARY KEY … WITH (publish, cluster_by, merge, ttl)`, `INSERT`, `UPDATE`, `DELETE`, from any node, Postgres, MCP or `pondra sql` | upsert/delete model exact; a keyed table declared without `_deleted` takes DELETE |
| Postgres protocol | `--pg`: simple and extended protocol, text and binary, typed and array parameters, a small `pg_catalog` | psql, psycopg 3 (text and binary), psycopg2, asyncpg, SQLAlchemy + pandas |
| Python client | `import pondra`: SQL → pandas / Polars / Arrow, exactly-once appends, lookups, the change feed | pandas, Polars and list appends; replay with a delete |
| MCP | `POST /mcp`: `list_tables`, `query`, `write`, `changes`, under the same tokens | raw JSON-RPC in `harness.py clients`; the official MCP Python SDK (`logs/round9/mcp-sdk.txt`) |
| Vector search | `FLOAT[]` embeddings, `ORDER BY cosine_distance(emb, [...]) LIMIT k` | same neighbours by SQL and by a Postgres array parameter |
| Tokens | read / write / admin on HTTP, Postgres and MCP | missing, weaker and wrong tokens refused |
| SQL can't touch a node's disk | `COPY … TO` and `CREATE EXTERNAL TABLE` refused on nodes | refused |
| Size-tiered keyed compaction | merge the newest run of similar files; rewrite the table only when the run reaches the oldest | 2 M keys, 60 rounds × 20k updates: **17.9 MB written vs 53.4 MB** for full rewrites; 7 files; tier call 19 ms median; every key right |
| TTL, change feed | `ttl = 'ts:secs'` on keyed tables; `--changelog-secs` keeps the log replayable | expired rows hidden in SQL and lookups |
| Replicated acks, hardened | `--fsync`; `--replicas 3` | ack 4 ms p50 / 9 ms p99 with fsync; 3 replicas: 4 / 7 ms, `users` 114,804 events/s with 0 inconsistent, 3 × `failover` exact |

**Regression, local disk** (`logs/round9/local-regression.txt`, `harness-all-final.txt`,
`final-binary-cluster.txt`):

- `harness.py all`: every test passes, twice (the final run's `clients` has 18 of 18 checks; the
  first ran before the vector, file-access and MCP checks existed, with 15).
- `crash --size 50000`: 3 runs × 9 M events, 0 lost, 0 duplicated, views exact (the log keeps
  the last two runs' lines; a failing run stops the test).
- `users`: 4 runs, 114.5–116.8k events/s, ack p50 44–45 ms, 0 torn reads, 0 lost or duplicated
  batches.
- `failover`: 3 durable and 3 replicated runs, back in 4.5–5.3 s, state == view == model.
- race, isolate, split (3.6 M events/s), spread (identical results): pass.
- latency: event → view row on another node 5–6 ms p50, 8–10 ms p99.
- `open_check.py`: all six outside readers equal Pondra.
- `clustering.py` (8 M rows): one user 276 → 6 ms, a range 174 → 2.4 ms, 100 users 221 →
  101 ms, full scans 122 → 136 ms.

**Simulated R2** (`logs/round9/sim-r2.txt`):

- `harness.py clients`: 18 of 18 (the inbox write takes 6.1 s, opening the catalog included).
- `serverless`: an INSERT with nobody running 5.8 s, through a running leader 1.7 s, 8,000 rows
  and no duplicates.
- `crash`: 45,000 events through 7 kill -9s and 75 injected crashes, exact. (A second run hit
  the time limit.)
- `failover`, replicated: back in 12.8 s and 10.4 s, exactly-once, state == view == model.
- `users`, replicated: 83.6k events/s, ack p50 62 ms, 0 torn reads.
- `latency`, replicated: ack 4 ms p50 / 9 ms p99; a view row on another node 3 / 8 ms.
- Test lakes deleted at exit: 0 objects left in the bucket.

**Real R2** (`ponderabucket-us`, `logs/round9/r2.txt`):

- `harness.py clients`: 18 of 18 on two R2 lakes: Postgres drivers, MCP, vectors, tokens,
  the cross-lake join. The inbox write takes 7.3 s for the whole `pondra sql` process,
  including 3–6 s to open the catalog.
- `serverless`:
  - an INSERT on a brand-new lake with nobody running: 11.9 s (it creates the catalog);
  - through a running leader: 4.0 s;
  - right after the leader is killed: 42 s (waits for its mark to go stale);
  - 8,000 rows, no duplicates.
- `latency`, replicated: ack **3 ms p50 / 10 ms p99**; a view row on another node 3 / 7 ms.
- `failover`, replicated: back in 16.9 s and 15.0 s, exactly-once, state == view == model.
- Every round-9 test lake was deleted when its test finished. The buckets hold only the three
  kept lakes.

### What the tests caught this round

- **The test harness passed `tier_secs=1` as a bare flag** (Python treats `1 == True`), so the
  crash test's node never started, and the restart loop recursed until Python gave up. Flags are
  now bare only for a real `True`, and a node that keeps dying on start fails after 20 tries.
- **SQL could reach a node's own disk.** Any token that could run a query could also run `COPY …
  TO '/path'` or `CREATE EXTERNAL TABLE … LOCATION '/etc/…'` through DataFusion, and an INSERT
  into an attached lake could read local files. Queries on nodes are now read-only
  (invariant 21).
- **DELETE failed on keyed tables created in SQL** unless they declared `_deleted` themselves.
  They get the column now; INSERTs and Arrow appends may leave it out.
- **`cluster.py` runs on object storage left their lakes behind.** The cleanup at exit created
  its S3 client during interpreter shutdown, when boto3 can no longer start threads. On real R2
  that would have filled the 10 GB free tier. The client is now made when the first lake is,
  and a simulated-R2 run ends with 0 objects left.
- **Postgres array parameters** (a Python list for an embedding) arrived as text. They're bound
  as arrays now.
- **`SHOW TABLES` listed internal helper tables**, and SQLAlchemy and asyncpg needed `pg_type`,
  a parseable `version()` and typed parameters. All fixed while building the protocol.

## Round 8: native first, fast writes on any storage, writes from anywhere

**Write latency and throughput** (3 nodes on one 2-vCPU box; `cluster.py latency | users`):

| | Local disk | Simulated R2 | Real R2, Eastern NA bucket (PUT ~290 ms) | Real R2, the far bucket (PUT ~670 ms) |
|---|---|---|---|---|
| Ack, `--ack durable` | 5 ms | 255 ms p50, 757 ms p99 | 299 ms p50, 732 ms p99 | 818 ms p50, 2.5 s p99 |
| Ack, `--ack replicated` | 4 ms | **4 ms p50, 7 ms p99** | **4 ms p50, 5 ms p99** | **4 ms p50, 8 ms p99** |
| 64 writers + 16 readers, durable | 125k events/s, ack p50 39 ms | — | 6,400 events/s, ack p50 937 ms | — |
| 64 writers + 16 readers, replicated | 129k events/s, ack p50 38 ms | 89k events/s, ack p50 58 ms | **87,200 events/s, ack p50 60 ms** | — |

Replicated mode passed every consistency test it was given:

- `users`: 0 torn reads, 0 lost or duplicated batches, 4 runs locally, 2 on simulated R2 and 1 on
  real R2.
- `failover`: exactly-once and task state == model, 11 runs locally, 4 on simulated R2 and
  2 on real R2 (14–16 s from kill to writes acked again after the fix).
- Its leaders were killed with up to 279 acknowledged commits not yet in the bucket; every one
  was recovered.

**Freshness, head to head:** the table and what it means are in
`docs/comparison-spark-flink-fluss.md`. In short:

- **Nodes:** 10–15 ms after the ack, on local disk and on R2.
- **A new `pondra sql` process:** 34 ms on local disk; ~3 s on R2, where opening the catalog
  costs ~20 requests.
- **Delta / Iceberg readers:** ~30 ms on local disk; 3–4 s on the near R2 bucket (worst 5–8 s);
  7–10 s on the far one.

**Writes from any machine** (`harness.py serverless`; local disk / real R2):

| | Local disk | Real R2 |
|---|---|---|
| `pondra sql` INSERT on a brand-new lake, nobody running | 0.03 s | 11.3 s (it creates the catalog) |
| Same job id again | `{"duplicate": true}` | same |
| 4 INSERTs at once, nobody running | all 4 recorded, in turn | same |
| A node starting on the idle lake | leads at once (0.02 s) | leads, up in 5.7 s |
| INSERT while that node runs | 0.02 s (the leader records the files) | 3.5 s |
| INSERT right after the leader is killed (no followers) | 30 s (waits for the leader's mark to go stale) | 30.6 s |
| Rows at the end | 8,000, no duplicates | same |

**Open formats:** `open_check.py`, 4 tables (append, upsert, GROUP BY view, bulk insert):

- **Local disk, 120 rounds:** past a Delta checkpoint and Iceberg's 100-snapshot history. All six
  readers equal Pondra.
- **Real R2, 30 rounds:** all six equal Pondra. PyIceberg needs its fsspec file IO there: its
  default PyArrow S3 client got 403s from R2 in this sandbox.

**Clustering** (`clustering.py`, 8 M rows, 100k users, local disk):

- one user: 130 → 20 ms;
- a range of users: 73 → 6.5 ms;
- 100 users (`IN` list): 103 → 79 ms;
- full scans: 52 → 91 ms (sorting scatters columns that were in arrival order, which then
  compress worse).

### What the tests caught this round

- **A PUT on real R2 hung for its full 30 s timeout.** It slowed a tiering round to 26 s and
  failed a `/tier` call. In replicated mode, it let 279 commits be acknowledged ahead of the
  bucket. Two fixes:
  - idle bucket connections are dropped after 15 s instead of being reused;
  - at most 256 commits may be acknowledged ahead of the bucket.
- **Real R2 slowed down when test pollers hammered it** from this sandbox. The freshness tool now
  polls the bucket every 0.2 s.
- **`pondra sql` INSERT on a brand-new lake** can't open a catalog that doesn't exist yet: it now
  writes its files after it has claimed the lake.
- **A node that could never reach the leader would take over after the 30 s startup lease.** A
  laptop outside the cluster's network could have deposed a healthy leader. Now only members
  that lost a leader they were talking to use the lease; everyone else needs the leader's mark
  in the bucket to be stale.
- **Readers of other formats:**
  - DuckDB's iceberg extension needs its avro extension installed next to it;
  - PyIceberg's default PyArrow S3 client got 403s from R2 here; use fsspec;
  - Polars reads Iceberg on R2 when given the PyIceberg table.

## Round 7: serving reads, TPC-H, and the result cache

The design is in ADR-008. All numbers are on one 2-vCPU box with the load generator
(`tools/loadgen.go`) on the same box, 2 M keys (500k on R2).

| | Round 6 | Round 7 |
|---|---|---|
| `/lookup`, one client | 11.2 ms | **0.14–0.22 ms** |
| `/lookup`, 64 clients | ~200/s | **20,000/s** (leader), **35,600/s** (read-only node), **31,800/s** on R2 (32 clients) |
| SQL point query, 64 clients | ~280/s | **15,800–21,100/s** |
| Dashboard aggregate, a new query each time | ~420 ms | **6–43 ms** |
| Same dashboard, 32 clients | ~2/s | **21,000–23,000/s** (≤ 5 ms p99) |
| Same, while writes land every few ms: exact / `stale_ms=1000` | 15/s | **230–842/s / 10,900–12,400/s** |
| 64 writers + 16 readers + 2 serverless, 3 nodes | 30k events/s | **~103k events/s**: identical reader queries now share one computation |
| TPC-H SF1, 22 queries | — | **5.9 s** (Spark 4.2: 58–65 s; DuckDB: 3.8 s) |

Also this round:

- `POST /sql?format=arrow` returns Arrow IPC.
- The `upsert` test checks `/lookup` and SQL point queries against the model after every batch:
  3,400 lookups per run, on local disk, simulated R2 and real R2, with 0 wrong.
- Everything else still passes on the round-7 binary:
  - all, crash, tiering, race, isolate, latency and spread;
  - 5 of 5 `users` runs and 8 of 8 `failover` runs;
  - Delta and new-user tests;
  - simulated R2: users, failover ×2 and fence;
  - real R2: users, upsert, serving, new-user and Delta.

## Round 6: an open lake, and fast first reads on object storage

The design is in ADR-007; the on-disk layout and how to read it without Pondra are in
`docs/lake-format.md`.

| Change | Effect |
|---|---|
| **Delta Lake publishing:** each tiering round writes each table's `_delta_log/` (JSON commit, checkpoint every 10 versions, last 1,000 versions kept), derived from committed catalog state, put-if-absent | Any Delta reader sees the lake. Checked against delta-rs 1.6.4, Polars 1.44.2 and DuckDB 1.5.5 on local disk, simulated R2 and real R2, across 1,250 versions with checkpoints and log cleanup |
| **Tiering starts when rows commit** (at most every `--tier-secs`, default 2, fractions allowed), commits fresh rows before merges and compactions, and does up to 4 tables at once | New row → Delta version: **17 ms** on local disk (was 10 s + a round) |
| **SSD tier per node** (`--cache-dir`, `--cache-gb 20`): write-through, read-through, prefetched from the commit stream, warmed at start | Queries on R2 read local disk, not the bucket |
| **Followers and serving nodes hold the whole catalog in memory**, kept current by the commit stream | No catalog reads from the bucket on the query path |
| **The leader keeps the catalog's SST files on local disk** (SlateDB's disk cache) | Tiering rounds don't wait on catalog reads |
| **Catalog flushes only every 5 s, and only when something changed** (and once on takeover) | A tiering round went from 9 s back to milliseconds; the 1,250-round Delta test from 15+ min to 2.5 min |

A new user, measured with `tools/newuser_bench.py` (3 nodes: leader, follower, read-only node; 1 M
rows; the client has never queried before):

| | Real R2, round 5 | Real R2, round 6 | Simulated R2 | Local disk |
|---|---|---|---|---|
| First query on the read-only node (count+sum / top 10 / one user) | 2,999 ms | **20 / 30 / 22 ms** | 20 / 25 / 19 ms | 17 / 24 / 17 ms |
| Same queries, steady (p50) | ~495 ms | **17 / 31 / 22 ms** | 13 / 23 / 18 ms | 12 / 23 / 18 ms |
| First queries on a node that just joined, empty SSD tier | — | **949 / 372 / 23 ms** | 452 / 387 / 21 ms | 20 / 26 / 20 ms |
| Write on node B → visible on node C | 1,999 ms | **660 ms** (≈ one PUT) | 262 ms | 7 ms |

On R2, a node that just joined pays for its first reads from the bucket: one GET takes ~0.4 s from
this sandbox. They are still under a second, and 20–35 ms once its SSD tier has warmed up (5 s
later).

Open lake freshness, measured with `tools/delta_check.py`. A probe row is written after a quiet
spell; the table shows how long until a Delta version containing it exists, and until delta-rs,
reading on its own, returns it:

| | Local disk | Simulated R2 | Real R2 (from this sandbox) |
|---|---|---|---|
| Write acknowledged | 7 ms | 256 ms | 663 ms |
| Delta version with the row exists | **17 ms** | 1.6 s | 4.8 s |
| delta-rs returns the row | **31 ms** | 3.7 s | 10.5 s (one delta-rs load takes ~4.7 s from here) |
| Fluss's lake, for comparison | — | — | 3 min by default |

Under a steady stream of writes, add up to `--tier-secs` (2 s by default): tiering starts as
soon as rows commit, but at most that often.

### What the tests caught this round

- **Lost and torn reads from the in-memory catalog (fixed).** When a node's own catalog view got
  ahead of its commit stream, the commits in between were marked "seen" and skipped. But the
  in-memory copy never reads the view, so reads missed batches. Tiering jobs that ran on such a
  node then wrote files without those rows, and the leader committed them.
  - `cluster.py users` failed 4 runs in 5; every run since has passed: 20+ local runs, plus
    simulated and real R2.
  - Every tiering job now also checks the leader's row count for its log range, so a node that
    sees less refuses the job instead of writing a short file.
- **A 9-second stall per tiering round (fixed).** Flushing the catalog's memtable on every
  tiering call piled up level-0 files faster than SlateDB compacted them, and writes waited on
  the limit.
- **A follower that came back after a leader change read a stale catalog (fixed).** Its view
  lacked commits the old leader hadn't flushed; a new leader now flushes what it inherited.
- **A restarted node never came up on simulated R2 (fixed).** Loading the in-memory catalog
  retried until the catalog stood still, which a busy lake on slow storage never does. It now
  loads in the background, one try at a time.
- **SSD tier:** a panic on a one-character catalog key (it killed all nodes in the first R2
  run), and temporary file names two writers could share.

## On real R2 (round 5 table; round 6's R2 numbers are in the round 6 section)

Round 6 re-ran the core tests on R2, and all passed:

- 64 writers + 16 readers: 91.8k events, 0 inconsistent, snapshot query p50 21 ms (was 12 ms).
- Failover: 26.0 / 22.3 s, exact.
- Crash: 18k events with 8 kill -9s and 41 injected crashes, exact.
- Latency: 0.7–1.2 s p50, depending on the hour.

Same tests, against a Cloudflare R2 bucket, from this sandbox. **A bare 64 KB PUT from here takes
810 ms p50 (1,048 ms p90) and a GET 379 ms** — that round trip dominates every number below, and
it is why a durable ack costs 1.2 s: the ack is essentially one object-store write. A deployment
near its bucket would see far less; treat these as a worst case, not a floor.

| | Real R2 | Simulated R2 (PUT p50 197 ms) | Local disk |
|---|---|---|---|
| Write acknowledged (durable), idle | **1,243** / 1,699 ms | 309 / 919 ms | 7 / 9 ms |
| Event → view row on another node | 1,336 / 2,553 ms | 333 / 1,072 ms | 7 / 10 ms |
| 64 writers + 16 readers + 2 serverless, 3 nodes | 91k events (2.5k/s), ack p50 2.0 s, snapshot query p50 12 ms, **0 inconsistent** | 276k events, 0 inconsistent | 1.47 M events, 0 inconsistent |
| Crash / exactly-once (kill -9 + injected crashes) | 30k events, 34 kill -9s + injected crashes: exact (plus 2 × 30k in the previous round) | 3 × 45k events, exact | 3 × 9 M events, exact |
| Leader failover (2 kills) | 23.1 / 22.9 s, state caught up 8.2 s, exactly once | 10.0 / 11.8 s | 4.5–4.9 s |
| Split brain (frozen leader) | 18.5 s; stale write rejected; rejoined | 10.2 s | 6.0 s |
| Distributed query, 3 nodes vs 1 | identical results; q9 1,221 → 541 ms, q8 1,308 → 1,126 ms | 8 of 9 queries 1.2–4.3x faster | no gain on one machine |
| Upsert compaction (12,000 upserts, 6 compactions) | 3.7–7.1 s each | 1.5–3.2 s each | milliseconds |
| Serving: point lookup, 500k keys, 8 clients | 29 ms p50 warm (276 ms cold: one GET), 311/s | — | 30 ms p50, 265/s (2 M keys) |
| Serving: dashboard aggregate on a compacted table | 3.1 ms (cached) | — | 351 ms (2 M rows) |

Nothing failed: 0 lost, 0 duplicated, 0 inconsistent reads, exactly-once through leader kills, on
real object storage.

## Many users at once

- **64 independent writers + 16 SQL readers + 2 serverless `pondra sql` processes, on 3 nodes:**
  - local disk: 1.52 M events in 31 s (49.2k/s), ack p50 88 ms / p99 638 ms, snapshot query p50 404 ms;
  - real R2: 91k events in 37 s (2.5k/s), ack p50 2.0 s / p99 3.9 s, snapshot query p50 12 ms;
  - simulated R2: 276k events in 35 s (7.9k/s), ack p50 691 ms / p99 1,537 ms;
  - **0 inconsistent reads** (every read saw each producer's batches as a gap-free prefix) and **0 lost or duplicated batches** in both.
- **Writers don't lock each other.** Each node batches its own writers; the leader folds every node's flush into one commit.
- **Readers never block writers or each other.** They read immutable objects plus a catalog snapshot.
- **Two writer processes on one bucket** form a cluster instead of fighting. A leader that was replaced is fenced by the catalog and rejoins as a follower.

Limits: producer names must be unique per client; there is no auth or per-user quota yet.

## Tests

Every test runs on local disk, on a local S3 server with R2-like latency, and (this round) against
a real Cloudflare R2 bucket. All of them pass on all three.

| Test | Local disk | Real R2 |
|---|---|---|
| Crash / exactly-once (kill -9 + injected crashes at 3 points) | 3 runs × 9 M events: 0 lost, 0 duplicated, views exact | 30k events, 34 kill -9s: exact |
| 64 writers + 16 readers + serverless, 3 nodes | 1.52 M events (49.2k/s), ack p50 88 ms, 0 inconsistent | 91k events (2.5k/s), ack p50 2.0 s, 0 inconsistent |
| Failover: 3 nodes, 6 task shards, 2 leader kills + 1 follower kill | 4 runs: writes back after 4.5–4.9 s, caught up 0.1–6.4 s, state == view == model | 23.1 / 22.9 s, caught up 8.2 s, exact |
| 5 nodes start at once | 1 leader | 1 leader |
| Follower cut off from a healthy leader | doesn't take over; takes over 5.1 s after the leader dies | 18.1 s |
| Split brain (leader frozen, then resumes) | takeover 6.0 s; stale write rejected; rejoined | 18.5 s; same |
| Upsert vs model (12,000 upserts/deletes, compactions, restart) | pass | pass |
| Tiering keeps up (18 rounds of writes + `/tier`, 10.8 M rows) | log drained, files bounded, rows exact | pass (1 M rows) |
| Distributed query == single-node query | identical, 2 M rows | identical |
| Latency (event → view row on another node) | 7 ms p50 / 10 ms p99; 54 / 290 ms under load | 1.34 s p50 / 2.55 s p99 |
| Freshness on a read-only node | 7 ms p50 / 84 ms p99 | — |
| Serving (2 M keys: lookups and a dashboard query) | 7 ms p50, 265 lookups/s, 351 ms aggregate | 29 ms p50 warm, 311 lookups/s |

## Sizes

| What | Round 3 | Round 5 | Round 8 | Round 9 | Round 10 |
|---|---|---|---|---|---|
| Binary (stripped) | 88.3 MB (30 MB gzip, 17 MB xz) | 89.2 MB (29.9 MB gzip, 16.8 MB xz) | 90.0 MB (30.5 MB gzip, 18.7 MB xz) | 90.7 MB (30.5 MB gzip, 17.2 MB xz), with the Postgres protocol and MCP | 93.0 MB (31.4 MB gzip, 17.6 MB xz), with the Kafka protocol and JSON functions |
| Idle memory | 18 MB | 41 MB (mimalloc reserves more up front) | 42 MB | 44 MB | 49 MB (with `--kafka`) |
| Peak memory under full load | 455 MB | 1.8 GB at 2.84 M events/s sustained (279–586 MB in the batch and streaming benchmarks) | not re-measured | not re-measured | not re-measured |
| Storage per event (user, event, amount, ts) | NDJSON 77.9 B → log 12.8 B → Parquet 6.8 B | NDJSON 77.9 B → log 12.8 B → Parquet 6.8 B (unchanged) | unchanged | unchanged | unchanged |

## Memory is a knob, not a mystery

Under saturation, memory is bounded by how many rows may wait in the log for tiering
(`--backlog`, default 10 M rows). One node, 8 producers, 60 s, a 100,000-key aggregating view:

| `--backlog` | Throughput | Ack → visible in the view (p50 / p99) | Peak memory |
|---|---|---|---|
| 10 M (default) | 2.84 M events/s | 205 ms / 730 ms | 1.8 GB |
| 2 M | 784k events/s | 49 ms / 234 ms | 1.4 GB |
| 500k | 33k events/s | 18 ms / 36 ms | 0.35 GB |

Lower it for less memory and lower latency, raise it to absorb longer bursts. Plain durable
ingest without a view reaches 5.5 M events/s at 1.3 GB, and the batch and streaming benchmarks
peak at 279–586 MB.

## Against the kill criteria (ADR-002 / 003)

| Criterion | Status |
|---|---|
| Freshness p99 ≤5 s at ≥50k events/s | **Pass:** 10 ms p99 idle, 290 ms p99 under load (local disk); 2.6 s p99 on real R2 |
| 0 lost / 0 duplicated under crashes | **Pass,** including leader failover and split brain |
| Binary ≤150 MB, idle memory ≤200 MB | **Pass** |
| Queries within 2x of DuckDB | **Pass** (round 2) |
| Catalog commit p95 ≤250 ms on object storage | **Measured, and it depends on the bucket's distance:** a durable ack is essentially one object-store write — 1.24 s p50 against R2 from this sandbox (where a bare PUT is 0.81 s), 309 ms on the R2-latency simulator, 7 ms on local disk |
| First query after idle ≤3 s | Readers pass; a writer restart is a few seconds on simulated R2 |
| Multi-node at ≥2x one node, recovery | **Partly:** ingest, views, tasks, queries and tiering all spread over nodes, and failover works. Speed-ups can't be shown on one machine: on simulated R2, where I/O latency dominates, 3 nodes ran 8 of the 9 queries 1.2–4.3x faster than one |

## What remains

- One sequencer per lake orders commits (metadata only). Past its capacity, split tables across
  attached lakes (round 9); there are no transactions across lakes.
- A *durable* acknowledgement costs one object-store write (0.25–0.7 s on R2). `--ack replicated`
  makes it milliseconds, but a write in that window survives only as long as one of its holders
  does (with `--fsync`, power loss included; not the leader and every holder at once).
- Kafka: one partition per topic, no transactions, sparse offsets; consumer groups live in the
  leader's memory. `ALTER TABLE` only adds columns. Windows take their watermark from window
  starts; no session windows.
- A one-off `pondra sql` on far-away object storage spends 2–3 s opening the catalog.
- Open-format versions trail the ack by a few sequential bucket round trips (3–4 s near, 7–10 s
  far, at `--tier-secs 2`); Iceberg costs two more round trips than Delta.
- A *new* analytical query costs what its scan costs (TPC-H SF1: 60–600 ms per query on two
  cores). Repeated ones and key lookups are served from caches in about a millisecond.
- Compaction of a keyed table is one job on one node. Size-tiered merging (round 9) means the
  whole table is rewritten only when the newer data has grown to about half of it; partitioned
  compaction (a key range per node) is the next step.
- Merge tables only support decomposable aggregates (sum, count, min, max).
- Distributed queries have one stage: no shuffles, so big-to-big joins run on one node.
- Under sustained overload, commits pause until tiering catches up (`--backlog`); a client with a
  short timeout will see it as a slow ack.
- Tokens per role only (round 9): no TLS, per-table grants, quotas or multi-tenancy.
- Other engines read Delta and Iceberg (opt-in per table), unpartitioned, and keyed tables only
  as of their last compaction (at most 8 tiering rounds behind). Time travel reaches back only as
  far as `--retain-secs` keeps replaced files.
- Bucket credentials still decide who can use `pondra sql` and the inbox directly.

## Next

From the plan in `docs/comparison-spark-flink-fluss.md`, in order:

1. **A multi-machine run** (3–10 VMs on S3/R2), then **shuffles** for big-to-big joins.
2. **AI functions in SQL** and an approximate vector index.
3. **Kafka partitions** and transactions; the Java client and Kafka Connect verified.
4. **TLS, per-table grants, an audit log, quotas.**
5. **Session windows**, a watermark from event time, point-in-time joins.
