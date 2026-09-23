# Pondra vs Spark, Flink, Fluss, Databricks Lakehouse//RT — and the single-node engines (round 12)

**Date:** 2026-09-23 · **Machine:** one 2-vCPU, 7 GB sandbox VM, local disk (plus a real Cloudflare R2 bucket where marked); every engine ran alone
**Versions:**
- Pondra (this prototype: Rust, Apache DataFusion 55)
- Spark 4.2.0 (PySpark, `local[*]`)
- Flink 2.3.0 (PyFlink, local MiniCluster, parallelism 2)
- DuckDB 1.5.5 (as a reference)
- DuckDB 1.5.5, Polars 1.44.2, Daft and Bodo (the single-node engines, new this round; each runs
  its own published TPC-H code)
- Fluss (1.0, released 2026-09-21) and Lakehouse//RT are compared from their published numbers and
  docs; neither can run here.

**Reproduce:**
- `tools/bench/singlenode.py` (TPC-H against DuckDB, Polars, Daft and Bodo), `tools/bench/tpch.py` (against Spark)
- `tools/bench/run.py` (batch / streaming / ETL, same data and SQL for every engine)
- `tools/serve_bench.py` (serving)
- `tools/cluster.py latency | split | spread`
- `tools/freshness.py` (freshness, head to head), `tools/open_check.py` (outside readers)

**Where the numbers come from:**
- TPC-H against DuckDB, Polars, Daft and Bodo, and everything multimodal: round 12.
- Table metadata at a million files, partitions, memory limits, shuffles and Arrow Flight: round 11.
- The Kafka protocol, the Iceberg REST catalog, schema evolution and windows: round 10.
- Clients, protocols, MCP, vectors and the roadmaps: round 9.
- Freshness, write latency and open formats: round 8.
- Spark's TPC-H and the serving numbers: round 7.
- Spark's and Flink's batch and streaming numbers: round 3, same machine and scripts.

## The short answer

**Per machine: yes, in the main areas, measured.**

- **Batch SQL:** TPC-H SF1 runs **~10x faster than Spark** (5.9 s vs 58–65 s for all 22
  queries, same answers).
- **Stateful streaming:** about **3x Flink's** throughput.
- **Latency:** a change shows up on another node in **5 ms**, where Flink's exactly-once
  results wait for a checkpoint. Writes are acknowledged in **2–4 ms on R2** with
  `--ack replicated`, without S3 Express.
- **Freshness, like for like** (below): Pondra's own readers see a write milliseconds after the
  ack, as Fluss's do. Its Delta/Iceberg tables follow 3–10 s later on R2, depending on the
  bucket's distance, where Fluss's lake tables lag 3 minutes by default.
- **Serving:** **20,000–36,000 lookups/s on two cores**, where Lakehouse//RT publishes 12,000
  QPS.

All of that comes from one 95 MB binary, with no JVM, ZooKeeper, Kafka or separate tiering job.

**At cluster scale and in breadth: not yet.**

- **Scale:** Spark and Flink are proven on thousands of machines, with shuffles, spilling and
  skew handling.
  - Since round 11, Pondra has shuffles, spilling, partitions, and table metadata that stays
    small: a table of a million files commits as fast as one of ten.
  - But its shuffle buckets live in memory, it has no skew handling, and it has only been tested
    as several processes on one machine.
- **Streaming features:** Flink has event time, watermarks, timers and very large state; Pondra
  has none of these yet.
- **APIs and ecosystem:** Spark has DataFrame APIs in four languages and hundreds of connectors.
  Pondra has SQL (reads and writes) over HTTP, the Postgres protocol, Arrow Flight SQL (ADBC,
  JDBC) and MCP, a Python client, the Kafka protocol and an Iceberg REST catalog; few connectors
  beyond those.
- **Maturity:** Pondra is a prototype.

**So the realistic claim:** Pondra can beat them for the common case — small to mid-size
clusters, up to terabytes a day, SQL-shaped work — and do it with far less to run. The gaps are
engineering work with a known shape (see the plan at the end), not changes to the design. The
biggest open risk is scale-out, and only a multi-machine benchmark can retire it.

## Scorecard

✓ = leads in that area today, on the evidence below. "—" = not what that product does.

| Area | Pondra | Spark | Flink | Fluss | Lakehouse//RT |
|---|---|---|---|---|---|
| Batch SQL per core (TPC-H) | ✓ 10x Spark | | (batch isn't its focus) | — | (not published) |
| Stateful streaming throughput per core | ✓ ~3x Flink | | | — | — |
| Change → visible elsewhere, exactly-once | ✓ 5 ms (local disk) | seconds (micro-batches); ms with Databricks RTM | ms per record, but visible at checkpoint | ✓ ms | — |
| Write ack on standard object storage | ✓ 2–4 ms replicated (`--ack replicated`); one PUT (0.25–0.7 s) durable | — | — | ✓ ms (replicated to TabletServer disks) | — |
| Own readers' freshness (memory/SSD path) | ✓ 10–15 ms, on local disk and on R2 | per micro-batch | per checkpoint | ✓ ms / sub-second | reads the lake |
| Open-format freshness (lake tables other engines read) | ✓ ~30 ms local, 3–10 s on R2 (Delta + Iceberg) | per micro-batch | per checkpoint | 3 min default (+ up to 2 rounds) | reads the lake |
| Serving: point reads, repeated dashboards | ✓ 0.1–3 ms, 20–36k/s on 2 cores | | | ms lookups | 10 ms, 12k QPS (cluster) |
| Serving: new analytical queries on big data | 35–600 ms (single node) | | | — | ✓ sub-100 ms (claimed) |
| Scale-out to 100s of machines | unproven: shuffles and spilling since round 11, tested on one box only | ✓ | ✓ | ✓ | ✓ |
| Streaming semantics (event time, windows, CEP, huge state) | decomposable aggregates, SQL tasks, event-time windows emitted once past a watermark | good | ✓ | storage only | — |
| APIs & usability | SQL reads and writes over HTTP, the Postgres protocol and Arrow Flight SQL (ADBC, JDBC); Python client (pandas, Polars, Arrow) | ✓ SQL + DataFrames (Python/Scala/Java/R), notebooks | SQL + DataStream API | clients (Java, Rust, Python, C++); REST gateway; Postgres protocol planned | ✓ Databricks SQL |
| Batch SQL on one machine (TPC-H) | ✓ fastest of Pondra, DuckDB, Polars, Daft and Bodo from files, at SF1 and SF10 | | | — | — |
| AI agents and vectors | ✓ MCP server built in; `ai_complete`/`ai_embed` against any OpenAI-compatible endpoint; your own functions on an Arrow Flight server; exact vector search in SQL | AI functions on Databricks only | `ML_PREDICT`, `VECTOR_SEARCH`; Flink Agents (0.2) | MCP and vector columns planned | ✓ Agent Bricks, Genie |
| Unstructured and multimodal | ✓ files in the lake (`files('…')`, `file_read`), `BINARY` with hashing and base64, `VARIANT`, `Float32[]` vectors — published as Delta arrays and Iceberg lists | — | — | blob and variant types planned | ✓ Databricks file types, `ai_query` |
| Connectors & ecosystem | Kafka protocol in and out (any Kafka client, Debezium), Postgres, HTTP; Delta + Iceberg out, Iceberg REST catalog | ✓ huge | ✓ huge | Flink/Spark connectors | ✓ Databricks |
| Operations & footprint | ✓ 1 binary, 44–49 MB idle, 0.02 s start | JVM cluster | JVM cluster + checkpoints | JVM + ZooKeeper + Flink tiering job | managed |
| Governance & security | read / write / admin tokens (HTTP, Postgres, MCP); no TLS or per-table grants yet | via platforms | via platforms | SASL users (1.0); TLS planned | ✓ Unity Catalog |
| Maturity | prototype | ✓ | ✓ | 1.0, a top-level Apache project | beta |

## Processing power

### TPC-H against the single-node engines (new this round)

The 22 queries at SF1 (6 M lineitems) and SF10 (60 M), on one 2-vCPU, 8 GB machine, best of three
runs, every answer checked against DuckDB's. Each engine runs its own published TPC-H code (Polars'
`pola-rs/tpch`, Daft's `benchmarking/tpch`, Bodo's `benchmarks/tpch`; DataFusion's SQL for Pondra
and DuckDB) and reads the same Parquet files — except Pondra, which reads its own lake, loaded with
one INSERT per table. `tools/bench/singlenode.py` runs all of it.

**From files, every query** (nothing loaded into memory first):

| | SF1 | SF10 |
|---|---|---|
| **Pondra** (`PONDRA_HOT_GB=0`) | **3.19 s** | **38.0 s** |
| DuckDB 1.5.5 | 3.36 s | 39.8 s |
| Polars 1.44.2 (in-memory engine) | 3.78 s | q9 out of memory |
| Polars (streaming engine) | 3.18 s | 42.8 s |
| Daft | 6.11 s | 89.0 s |
| Bodo | 8 of 21 answers differ from DuckDB's, one query crashes, minutes per query | not run |

**From memory** (the load is not in the times):

| | SF1 | SF10 |
|---|---|---|
| **Pondra**, hot columns (1.5 GB) | **1.96 s** | **35.9 s** |
| DuckDB, native tables | 1.80 s | needs ~7 GB of temporary space beyond this machine's memory: no room |

Pondra is the fastest of the five reading Parquet, at both scales, and the only one that also
ingests, serves and scales out. DuckDB stays ~9% ahead when both hold the data in memory at SF1;
at SF10 it can't hold it here at all, while Pondra's cache takes what fits and gives it back when
queries need the memory. Bodo's numbers are its own published code at this scale on two cores; its
answers differ often enough that they aren't a comparison.

What changed since round 11 (SF1 went from 6.45 s to 3.19 s, DuckDB from 4.18 to 3.36 on a quieter
machine): strings read as Arrow views, LZ4 instead of ZSTD for Pondra's own files, SQL-standard
decimal literals, three planning rules of Pondra's own and one physical rule (ADR-013).

### TPC-H SF1 against Spark (round 7)

The 22 queries on the same Parquet files (`tpchgen-cli -s 1`: 6 M lineitems), best of two runs
each. Pondra's runs go over HTTP and return JSON, and each run is a new query, so the result
cache never answers. All three engines return the same row counts for every query.

| | Pondra | Spark 4.2 | DuckDB 1.5.5 (reference) |
|---|---|---|---|
| All 22 queries | **5.9 s** | 58.5–65.2 s | 3.8 s |
| Fastest / slowest query | 0.06 / 0.60 s | 0.6 / 6.8 s | 0.04 / 0.47 s |
| Per query vs Spark | **3x–23x faster** (median ~11x) | 1x | — |

DuckDB, the single-machine specialist, is 1.5x faster still. Pondra runs every query through the
lake (Parquet + log tail, one consistent snapshot) and HTTP. Flink couldn't take part: PyFlink
ships no Parquet reader, and Maven Central is blocked here.

### Batch: 20 M rows, write then 6 queries (seconds; Pondra's first run, uncached)

| | Pondra | Spark | Flink ¹ | Pondra vs Spark |
|---|---|---|---|---|
| Generate + write 20 M rows | **3.3** (Parquet ZSTD) | 7.3 | 10.5 (CSV) | 2.2x |
| Q1 `count, sum, avg` | **0.05** | 0.70 | 5.2 | 14x |
| Q2 filter + group by 1,000 | **0.14** | 0.93 | 6.5 | 6.5x |
| Q3 group by 1 M users, top 10 | **0.59** | 2.20 | 8.9 | 3.8x |
| Q4 `count(DISTINCT user)` | **0.51** | 1.75 | 7.5 | 3.4x |
| Q5 join with a dimension | **0.26** | 2.30 | 9.4 | 8.8x |
| Q6 time buckets | **0.25** | 0.52 | 4.7 | 2.1x |
| Peak memory | **293 MB** | 1.5 GB | 3.1 GB | |

Asked a second time, each of these queries now returns in about 1 ms from the result cache
(round 7) until the table changes.

¹ Flink wrote and read CSV (no Parquet format jar here), so its batch numbers are pessimistic.

### Streaming: 10 M events

| Workload | Pondra | Spark Structured Streaming | Flink |
|---|---|---|---|
| Keyed running aggregation (100k keys) | **4.2 s end to end (2.4 M/s)**: over HTTP, stored durably, aggregated by an inline view, queryable | 3.9 s from an in-memory backlog, `noop` sink | 13.3 s (0.75 M/s), in-memory source, `blackhole` sink |
| Stateless ETL (project + filter) | **4.1 s end to end (2.4 M/s)**, output durable | 5.2 s, `noop` sink | 4.0 s, `blackhole` sink |
| Sustained 60 s, durable, 8 producers, one node | **2.7 M events/s**, acked → visible in the aggregate p50 0.23 s | needs Kafka/Fluss + checkpoints | same |
| 3-node cluster, producers writing to followers | **4.3–4.6 M events/s** durable, exactly-once (leader 26–28 % of CPU) | — | — |

Spark and Flink generated input in memory and discarded output; Pondra received its input over
HTTP and committed the results durably. The comparison is tilted against Pondra.

## Latency and freshness, head to head

Round 7 put Pondra's 17 ms (a Delta version on local disk) next to Fluss's 3 minutes (its lake
tables' default freshness). That compared different paths. These tables compare like with like:
memory/SSD paths with memory/SSD paths, and object storage with object storage.

**Fluss, from its documentation:**

- Writes and streaming reads go through its TabletServers, which replicate to their local disks:
  milliseconds.
- Its lake tables (Paimon, Iceberg, Lance) are written by a tiering service every
  `table.datalake.freshness` (default 3 minutes). Peak lag is roughly freshness + 2 × the
  tiering round.
- *Union read* combines both for "sub-second freshness".

**Pondra, measured** (`tools/freshness.py`):

- **Method:** one row per probe, written after a quiet spell; every reader polls at once, each in
  its own process. Time is from the ack to the first read that sees the row.
- **Setup:** 3 processes on one 2-vCPU box, with two real R2 buckets: a near one (Eastern North
  America, ~290 ms per PUT from here) and a far one (~670 ms).
- **Numbers:** p50, with the worst of 8–10 probes in brackets. On R2 each cell shows `durable` /
  `--ack replicated`.

| Path | Local disk | Real R2, near | Real R2, far | Fluss |
|---|---|---|---|---|
| **Write acknowledged** | 4 ms | 300 ms (458) / **2 ms (5)** | 723 ms (1,000) / **2 ms (4)** | ms |
| **Memory/SSD path:** another node | 15 ms | 10 ms (56) / 13 ms (16) | 17 ms (28) / 9 ms (27) | ms (TabletServer reads) |
| read-only node | 15 ms | 11 ms (56) / 13 ms (16) | 13 ms (32) / 14 ms (29) | — |
| **Serverless:** a new `pondra sql` process | 34 ms | 3.1 s / 2.9 s | 5.8 s / 5.5 s | — |
| **Lake tables for other engines:** Delta (delta-rs) | 36 ms | 3.3 s (4.3) / 3.6 s (5.1) | 7.1 s (9.4) / 7.2 s (9.1) | — |
| Iceberg (PyIceberg) | 31 ms | 3.9 s (5.8) / 4.0 s (7.7) | 10.0 s (11.3) / 9.7 s (12.2) | 3 min default + up to 2 rounds |

What this says:

- **Like for like, the fast paths match.** Pondra's nodes see a write 10–15 ms after the ack,
  on local disk or R2 (with the pollers competing for two cores; alone it's ~5 ms), as Fluss's
  TabletServers serve theirs in milliseconds. With `--ack replicated`, the write itself is also
  acknowledged in milliseconds, the way Fluss's is, without S3 Express.
- **Like for like, the lake tables don't.** A Pondra table's Delta and Iceberg versions trail
  the ack by ~3–4 s on the near bucket and 7–10 s on the far one, at the default
  `--tier-secs 2` (tens of ms on local disk). That is a handful of sequential bucket round trips.
  Iceberg needs two more than Delta: the manifest, then the metadata file, then the version hint.
  Fluss's lake tables trail by minutes by default. Fluss can be set lower, at the cost of more
  lake commits, and so can Pondra (`--tier-secs 0.5`).
- **Serverless reads are fresh, not fast, on far-away storage.** A new `pondra sql` process sees
  every write already in the bucket, with nothing to wait for. But it spends ~2–3 s opening the
  catalog over R2 from here, a little less than Delta readers wait for a new version. For fast
  *and* fresh, join with `pondra serve --reader`.
- **The first R2 runs had 13–34 s worst cases for the open formats.** The cause was a PUT hanging
  for its full 30 s timeout on a reused idle connection. Idle connections are now dropped after
  15 s; the table shows the runs after that fix.

## Serving vs Databricks Lakehouse//RT

Lakehouse//RT's published claims: "as low as 10 ms on smaller datasets", "sub-100 millisecond
latency at 12,000 queries per second on standard analytical benchmarks", read-only beta. Pondra,
with 2 M keys (500k on R2), on one 2-vCPU box, with the load generator running on the same box:

| | Local disk, leader | Local disk, read-only node | Real R2, read-only node |
|---|---|---|---|
| `/lookup`, 1 client | 0.22 ms p50 | **0.14 ms** p50 | **0.14 ms** p50 |
| `/lookup`, 32–64 clients | 20,000/s, p99 7.4 ms | **35,600/s**, p99 6.4 ms | **31,800/s**, p99 4.0 ms |
| SQL point query, 64 clients | 15,800/s | 21,100/s | 18,400/s (32 clients) |
| Dashboard aggregate, a new query each time | 6–42 ms | 43 ms | 17 ms |
| Same dashboard, 32 clients | 23,300/s, p99 5 ms | 21,300/s | 19,500/s |
| Same, while writes land every few ms (exact) | 230/s | 842/s | 12,400/s |
| Same, with `stale_ms=1000` | 10,900/s | 12,400/s | 17,100/s |

Where each side stands:

- **Pondra is ahead** on point reads, repeated dashboards and freshness: it serves rows
  milliseconds old, and it takes writes itself.
- **Lakehouse//RT is ahead** on new, heavy analytical queries at high concurrency: it claims
  sub-100 ms at 12k QPS on TPC-H/TPC-DS-style queries on a cluster. Pondra runs a new TPC-H
  query in 60–600 ms on two cores.
- **To close that:** more read-only nodes (each has its own caches), shuffles and partitioned
  tables (both in round 11), and measurements on real machines — the plan below.

## Fluss 1.0, item by item

Fluss released 1.0 on 2026-09-21, a month after becoming a top-level Apache project. What it
added, next to where Pondra stands after round 11:

| Fluss 1.0 | Pondra (round 11) |
|---|---|
| REST gateway in Rust (metadata, tables, batch writes) | HTTP API from the start (`/sql`, `/append`, `/lookup`, `/watch`) |
| Postgres protocol, gRPC and MCP for the gateway: on its roadmap | **Postgres protocol and MCP: built** (tested with psql, psycopg 2 and 3, asyncpg, SQLAlchemy; MCP with the official SDK) |
| Python, C++ and Rust clients on one Rust core | a Python client (pure Python over HTTP and Arrow), and any Postgres driver; no C++ client |
| Row-level TTL for primary-key tables; log TTL | TTL on keyed tables; `--changelog-secs` for how long the log is kept |
| Aggregation merge engine | merge tables (`merge = 'total:sum'`), since round 4 |
| Batch `$changelog` / `$binlog` reads | `/watch?after=` replays every change, deletes included; MCP `changes` |
| UPDATE / DELETE without the full primary key (for GDPR erasure) | UPDATE / DELETE with any WHERE |
| Predicate pushdown via column statistics; server-side primary-key scans | every file's column ranges prune manifests and files before any Parquet is opened (round 11); DataFusion prunes row groups; `cluster_by` sorts files; `partition_by` |
| Arrow-based columnar log with column pruning (its own RPC) | the log as an Arrow Flight stream with chosen columns, following new commits in 2.6 ms (round 11); plus the Kafka protocol |
| Bucket rescaling | no buckets to rescale: every node writes, one sequencer orders commits (ADR-005) |
| Coordinator HA, multiple disks, a health API, a Helm chart | leader election on the bucket (failover 4.5–5.3 s locally); `/stats` for health, `/metrics` for Prometheus; no Helm chart yet |
| Hudi tiering, next to Iceberg, Paimon and Lance | Delta and Iceberg publishing, per table |
| Spark union read; time-range incremental reads | outside engines read the published Delta/Iceberg tables (3–10 s behind on R2); Pondra's own readers see the log |
| SASL/PLAIN user management | read / write / admin tokens; no per-user accounts yet |

## What they're building next, and Pondra's answer

Collected on 2026-09-22 from each project's roadmap, release notes and summit announcements
(sources at the end). Status labels for the newest Databricks and Snowflake items come partly
from third-party summit recaps; check them before relying on them.

**Fluss** (the roadmap page after 1.0; no dates):

- **Zero Disks** (writes straight to S3, diskless servers) and **ZooKeeper removal.**
  - This moves Fluss toward the design Pondra started with: object storage only, disposable
    nodes, leader election by conditional writes on the bucket.
  - Pondra already acks in 2–4 ms on R2 through replicated acks, without S3 Express.
- **Postgres protocol, gRPC and MCP on the gateway.** Pondra has the Postgres protocol and MCP
  as of this round.
- **A real-time feature store** (point-in-time correctness), **multimodal data** (vectors, variant,
  images), and a Python SDK for PyTorch, Ray and pandas.
  - Pondra has vector columns with exact nearest-neighbour search in SQL, `VARIANT` columns,
    `BINARY` with hashing and base64, and files in the lake that a query reads by path
    (`files('photos/')`, `file_read`) — round 12.
  - Point-in-time joins are in the plan below; a shredded variant waits for Arrow to have one.
- **A global secondary index** for non-key lookups. Pondra: `cluster_by` and Parquet pruning; no
  index.
- **Lake integration:** Iceberg v3; Delta; an "in-place lakehouse" (Fluss tables defined over
  existing lake tables); deletion vectors.
  - Pondra publishes Delta and Iceberg.
  - Reading existing lake tables in place, and deletion vectors, are in the plan.
- **Union read in Trino and StarRocks.** Pondra doesn't need a connector for this: both engines
  already read the Delta/Iceberg tables Pondra publishes.
- **Also:** full schema evolution, a cost-based optimizer fed by table statistics, (m)TLS.

**Flink** (2.0 in March 2025 through 2.3 in June 2026; its roadmap page hasn't been updated since
2023):

- **Disaggregated state** (ForSt, 2.0): state on object storage. Flink is converging on
  Pondra's model too.
- **Materialized tables** with a declared freshness. Pondra's views are the same idea, kept
  current on every commit rather than on a freshness schedule.
- **AI in SQL:**
  - model DDL and `ML_PREDICT` (2.1);
  - `VECTOR_SEARCH` (2.2);
  - Flink Agents (0.2, with MCP support), a separate framework for event-driven agents.
- **Joins with little state:** the delta join and multi-way join (2.1–2.2), built to read from
  Fluss.
- **SQL and types:** VARIANT (2.1), process table functions, `FROM_CHANGELOG` / `TO_CHANGELOG`
  (2.3). Pondra has `VARIANT` as JSON text with `json_get` and `->>`.
- **The threat:** Flink plus Fluss is becoming a streamhouse stack of two mature projects —
  log, keyed tables, lake and low-state joins. Pondra's answer is to be that stack in one
  binary: faster to adopt and cheaper to run.

**Spark** (4.0, 4.1, and 4.2 in 2026):

- **Real-Time Mode** for Structured Streaming: millisecond latency without a second engine.
  - Generally available in 4.1, for stateless Scala queries first.
  - Pondra's millisecond path is exactly-once and stateful (views, merge tables), not stateless.
- **Declarative pipelines** (from Databricks' Delta Live Tables). Pondra's views and tasks are
  declared in SQL too, and run incrementally.
- **Types and SQL:** VARIANT with shredding, SQL scripting, pipe syntax.
- **Clients and UDFs:** Spark Connect clients (Python, Go, Swift) and a JDBC driver;
  Arrow-native Python UDFs; the Python data source API. Pondra's answer to UDFs is a function
  that lives on an Arrow Flight server of yours: it gets a batch of arguments and returns a
  column, so the model or library runs in that process and a slow one can't take a node down.

**Databricks** (Data + AI Summit 2025 and 2026):

- **Lakebase**, a serverless Postgres (from the Neon acquisition), and **LTAP**, transactions and
  analytics on one store (announced).
  - Pondra answers from the other side: the Postgres protocol straight onto the lake, with
    millisecond upserts. No separate OLTP database to sync.
- **Lakehouse//RT** (10 ms SQL on Delta/Iceberg, beta). Compared in its own section above.
- **Zerobus**, direct ingestion, reported as Kafka-compatible in 2026. Kafka-protocol ingest is
  the top item in Pondra's plan.
- **Unity Catalog:** an Iceberg REST catalog, attribute-based access control (ABAC), metrics.
- **Also:** Agent Bricks, Genie, Databricks Apps, and an expanded Free Edition.

**Snowflake** (Summit 2025 and 2026):

- **Snowflake Postgres** (from Crunchy Data), generally available in February 2026.
- **Openflow** for ingestion; **Snowpipe Streaming's high-performance architecture**, generally
  available on all three clouds.
- **Cortex AISQL** (`AI_COMPLETE`, `AI_FILTER`, `AI_AGG` in SQL).
- **Iceberg v3** and a managed Iceberg catalog; **dynamic tables.**

**Where they are all heading, and where Pondra stands:**

| Direction | Who | Pondra |
|---|---|---|
| Postgres everywhere | Lakebase, Snowflake Postgres, Fluss gateway (planned) | ✓ the Postgres protocol over the lake (round 9) |
| Agents: MCP, AI functions in SQL | Fluss (planned), Flink Agents, Databricks, Cortex AISQL | ✓ MCP (round 9); AI functions in SQL: plan |
| Vectors and multimodal data | Flink `VECTOR_SEARCH`, Fluss (planned), Lakebase search | ✓ exact k-NN in SQL (round 9); an ANN index: plan |
| State and storage on object storage, no local disks | Flink ForSt, Fluss Zero Disks | ✓ the design since round 1 |
| Millisecond streaming inside the main engine | Spark Real-Time Mode, Lakehouse//RT | ✓ 5 ms change → another node, exactly-once |
| Declarative, incremental pipelines | Spark declarative pipelines, Flink materialized tables, dynamic tables | ✓ views and tasks in SQL |
| Kafka-compatible ingestion | Zerobus (reported), Fluss log agents | ✓ the Kafka protocol on every node (round 10) |
| Open catalogs (the Iceberg REST catalog API) | Unity Catalog, Polaris, Snowflake | ✓ read-only REST catalog on every node (round 10) |
| A VARIANT type | Spark, Flink, Delta, Iceberg v3 | JSON functions and `->` / `->>` (round 10); VARIANT when DataFusion has it |
| Arrow-native clients (ADBC, Flight SQL), columnar logs | Dremio, InfluxDB 3, Fluss's Arrow log, Databricks ADBC | ✓ Arrow Flight and Flight SQL on every node; the log as a columnar stream with chosen columns (round 11) |
| Petabyte tables: manifests, partitions, file skipping | Iceberg, Delta, Snowflake micro-partitions | ✓ per-file statistics, manifests, `partition_by` (round 11) |
| Access control | Unity Catalog ABAC, Fluss SASL + TLS | tokens (round 9); grants and TLS: plan |

## Footprint and operations

| | Pondra | Spark 4.2 | Flink 2.3 | Fluss |
|---|---|---|---|---|
| What you install | one binary, 93.0 MB (31.4 MB gzip, 17.6 MB xz) | 485 MB PySpark + a JVM | 353 MB PyFlink + a JVM | CoordinatorServer + TabletServers + ZooKeeper + a Flink tiering job, JVM |
| Start to first query | **0.02–0.07 s** | 4.1 s | 5.2–5.4 s | — |
| Idle memory | **44–49 MB** | — | — | — |
| Peak memory in these runs | **293–609 MB** (1.3 GB at 2.7 M events/s sustained) | 0.7–1.5 GB | 1.2–3.1 GB | — |
| State | object storage only; nodes are disposable (SSD tier = a cache) | + Kafka/Fluss + checkpoints | + Kafka/Fluss + checkpoints | TabletServer disks (replicated) + object storage |

## Where the JVM engines still win, and the plan

In rough order: what closes the most ground per unit of work comes first.

| Gap | Why it matters | Plan | What proves it |
|---|---|---|---|
| **Multi-machine evidence** | Everything above is one box | Run the suite and benchmarks on 3–20 cloud VMs against S3/R2 | Near-linear ingest and query scaling, failover times |
| **Scale-out beyond one stage** | Spark's core strength; TPC-H at SF100+ needs it | Round 11: hash exchanges become shuffles between nodes, small tables broadcast, spilling under `--memory-gb`; 14 query shapes on 3 nodes equal one node. Next: spill and stream shuffle buckets, skew handling, retry a failed step instead of the query | TPC-H SF100 on 3–10 real machines vs Spark, same hardware (`tools/cloud/`) |
| **Petabyte tables** | Big tables mean millions of files | Round 11: per-file column ranges, manifests behind one list object (Iceberg's layout), partitions: a million files commit a 20 KB entry, and a query over today skips them all in 13 ms. Next: publishing big tables to Delta/Iceberg by reusing the manifests; merging files after sealing | 1 PB-scale table on real storage with steady commits |
| **Streaming semantics** | Flink's core strength | Rounds 9–10: event-time tumbling windows (a GROUP BY `date_bin` view), updated incrementally, and emitted once, final, past a watermark with allowed lateness. Next: session windows, a watermark from the source's event time, point-in-time (temporal) joins | Nexmark queries vs Flink |
| **Kafka beyond one partition** | Kafka clients scale reads by partitions | Round 10: produce, consume, consumer groups, one partition per topic. Next: key-hashed partitions (each a slice of the table), transactions for Kafka Streams / Flink exactly-once sinks, the Java client verified | Kafka Connect and Flink's Kafka source against Pondra |
| **Schema evolution** | Tables change; Fluss 1.0 lists it as a gap too | Round 10: `ALTER TABLE … ADD COLUMN` (old rows read null; Delta and Iceberg follow). Next: renames, defaults, type widening | ✓ adding a column under load (`harness.py alter`) |
| **AI in SQL** | Flink `ML_PREDICT`, Snowflake Cortex AISQL, Databricks AI functions | `ai_complete()` / `embed()` against any OpenAI-compatible endpoint, batched per Arrow batch; an ANN index for vector columns | A RAG demo: embed on insert, nearest neighbours in SQL, answered through MCP |
| **Governance** | Enterprise requirement | Round 9: read / write / admin tokens on HTTP, Postgres and MCP. Next: TLS, per-table grants, an audit log (the change feed of a system table), quotas | Multi-tenant test |
| **APIs** | Usability for data teams | Round 9: Postgres protocol, Python client, MCP. Round 11: Arrow Flight SQL (ADBC tested: queries, writes, ingest, catalog) and plain Flight (15.7 M rows/s in, exactly-once; the log as a columnar stream). Next: JDBC / BI tools verified (DBeaver, Tableau, Power BI), Python UDFs over Arrow | Tableau / Power BI connect; notebook demo |
| **Open-format lag** on object storage | Other engines see a table 3–10 s after the ack on R2 (a few sequential round trips) | Next: overlap publishing with the next fold, fewer sequential writes for Iceberg, a lower default `--tier-secs` when the bucket is close | p99 < 3 s on a nearby bucket |
| **Table layout** (Delta liquid clustering, Iceberg sort orders, partitions) | Big tables with selective filters | `cluster_by` (round 8: 6–11x on selective filters) → partitions and file-level pruning (round 11) → clustering across files → deletion vectors | TPC-H SF100 with partition + clustering pruning |
| **Heavy new analytical queries at high concurrency** | Lakehouse//RT's edge | Prepared-plan cache, partitioned tables (pruning), per-node caches on many read-only nodes | TPC-H SF10 at 1k+ QPS mixed, p99 < 100 ms on N nodes |
| **Types** | VARIANT (Spark, Flink, Delta, Iceberg v3) for semi-structured data | Round 10: JSON functions over string columns (`json_get`, `->>`). Next: DataFusion's variant type when it lands | Semi-structured events queried without a schema up front |
| ~~Durable ack in ms on object storage~~ | Fluss's edge | **Done in rounds 8–9:** `--ack replicated`, 2–4 ms on R2; `--fsync`; 3 replicas tested | ✓ ack p50 2 ms on R2 |
| ~~Kafka-protocol ingest~~ | How most event data travels | **Done in round 10:** producers (exactly-once when idempotent), Debezium, consumers, groups, SASL | ✓ ~0.7 M events/s via librdkafka on one box |
| ~~Open catalogs~~ | Engines attach by URL | **Done in round 10:** the Iceberg REST catalog (read-only) | ✓ PyIceberg and DuckDB attach it |
| **Maturity** | Trust | Chaos tests on real clusters, fuzzing, long soak runs, versioned upgrades | Months of soak without data loss |

Sources:

- [Fluss roadmap](https://fluss.apache.org/roadmap/)
- [Fluss 1.0 release](https://fluss.apache.org/blog/releases/1.0/)
- [Flink 2.3.0 release](https://flink.apache.org/2026/06/25/apache-flink-2.3.0-release-announcement/)
- [Flink 2.2.0 release](https://flink.apache.org/2025/12/04/apache-flink-2.2.0-advancing-real-time-data--ai-and-empowering-stream-processing-for-the-ai-era/)
- [Flink 2.1.0 release](https://flink.apache.org/2025/07/31/apache-flink-2.1.0-ushers-in-a-new-era-of-unified-real-time-data--ai-with-comprehensive-upgrades/)
- [Flink Agents 0.2.0](https://flink.apache.org/2026/02/06/apache-flink-agents-0.2.0-release-announcement/)
- [Flink community update, April 2026](https://flink.apache.org/2026/04/13/flink-community-update-for-april-2026/)
- [Spark 4.1.0 release notes](https://spark.apache.org/releases/spark-release-4.1.0.html)
- [Databricks: introducing Apache Spark 4.1](https://www.databricks.com/blog/introducing-apache-sparkr-41)
- [Spark 4.2.0 preview 2](https://spark.apache.org/news/spark-4-2-0-preview2-released.html)
- [Databricks: what's new in Unity Catalog (Summit 2026)](https://www.databricks.com/blog/whats-new-unity-catalog-data-ai-summit-2026)
- [Flexera: Databricks Data + AI Summit 2026 recap](https://www.flexera.com/blog/perspectives/databricks-data-ai-summit-2026/) (third-party)
- [Delta Lake 4.0](https://delta.io/blog/2025-09-25-delta-lake-40/)
- [Snowflake Postgres GA (2026-02-24)](https://docs.snowflake.com/en/release-notes/2026/other/2026-02-24-snowflake-postgres-ga)
- [Snowflake Cortex AISQL operators GA](https://docs.snowflake.com/en/release-notes/2025/other/2025-11-04-cortex-aisql-operators-ga)
- [Snowpipe Streaming high-performance architecture](https://docs.snowflake.com/en/release-notes/2025/other/2025-09-23-snowpipe-streaming-high-performance-architecture)
- [SELECT: Snowflake Summit 2026, what shipped](https://select.dev/posts/snowflake-summit-2026-what-actually-shipped-and-what-it-means) (third-party)
- [Databricks: Introducing Lakehouse//RT](https://www.databricks.com/blog/introducing-lakehousert-real-time-performance-unified-lakehouse)
- [Databricks press release (June 2026)](https://www.databricks.com/company/newsroom/press-releases/databricks-launches-lakehousert-bring-real-time-analytics-directly)
- [StarTree: Lakehouse//RT vs StarTree](https://startree.ai/resources/databricks-lakehouse-rt-vs-startree/)
- [Spark Real-Time Mode vs Flink (2026)](https://sparkingscala.com/latest/2026/05/23/spark-rtm-vs-flink/)
- [Databricks: Real-Time Mode in Spark Structured Streaming](https://www.databricks.com/blog/introducing-real-time-mode-apache-sparktm-structured-streaming)
- [Fluss releases](https://fluss.apache.org/blog/tags/releases/)
- [Fluss 1.0 roadmap](https://github.com/apache/fluss/discussions/2684)
- [Fluss architecture](https://fluss.apache.org/docs/next/concepts/architecture/)
- [Fluss tiering service](https://fluss.apache.org/docs/1.0/streaming-lakehouse/tiering-service/)
- [Fluss tiering service deep dive, part 2: tuning (default freshness 3 min; peak ≈ freshness + 2 rounds)](https://fluss.apache.org/blog/fluss-tiering-service-deep-dive-part2/)
- [Fluss union read ("sub-second freshness")](https://fluss.apache.org/docs/next/streaming-lakehouse/union-read/)
- [Fluss 0.8 release (Iceberg, Lance)](https://fluss.apache.org/blog/releases/0.8/)
- [Jack Vanlightly: Understanding Apache Fluss](https://jack-vanlightly.com/blog/2025/9/2/understanding-apache-fluss)
- [Flink: end-to-end exactly-once](https://flink.apache.org/2018/02/28/an-overview-of-end-to-end-exactly-once-processing-in-apache-flink-with-apache-kafka-too/)
- [Confluent: delivery guarantees and latency in Flink](https://docs.confluent.io/cloud/current/flink/concepts/delivery-guarantees.html)
