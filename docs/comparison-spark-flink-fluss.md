# Pondra vs Spark, Flink, Fluss and Databricks Lakehouse//RT (round 8)

**Date:** 2026-09-21 · **Machine:** one 2-vCPU, 7 GB sandbox VM, local disk (plus a real Cloudflare R2 bucket where marked); every engine ran alone
**Versions:**
- Pondra (this prototype: Rust, Apache DataFusion 55)
- Spark 4.2.0 (PySpark, `local[*]`)
- Flink 2.3.0 (PyFlink, local MiniCluster, parallelism 2)
- DuckDB 1.5.5 (as a reference)
- Fluss 0.9 and Lakehouse//RT are compared from their published numbers; neither can run here.

**Reproduce:**
- `tools/bench/tpch.py` (TPC-H)
- `tools/bench/run.py` (batch / streaming / ETL, same data and SQL for every engine)
- `tools/serve_bench.py` (serving)
- `tools/cluster.py latency | split | spread`
- `tools/freshness.py` (freshness, head to head), `tools/open_check.py` (outside readers)

**Where the numbers come from:**
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

All of that comes from one 90 MB binary, with no JVM, ZooKeeper, Kafka or separate tiering job.

**At cluster scale and in breadth: not yet.**

- **Scale:** Spark and Flink are proven on thousands of machines, with shuffles, spilling and
  skew handling. Pondra's distributed queries are one stage, and it has only been tested as
  several processes on one machine.
- **Streaming features:** Flink has event time, watermarks, timers and very large state; Pondra
  has none of these yet.
- **APIs and ecosystem:** Spark has DataFrame APIs in four languages and hundreds of connectors.
  Pondra has SQL over HTTP.
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
| Scale-out to 100s of machines | unproven; no shuffles | ✓ | ✓ | ✓ | ✓ |
| Streaming semantics (event time, windows, CEP, huge state) | decomposable aggregates + SQL tasks | good | ✓ | storage only | — |
| APIs & usability | SQL over HTTP, JSON/Arrow out, one command | ✓ SQL + DataFrames (Python/Scala/Java/R), notebooks | SQL + DataStream API | clients (Java, Rust, Python) | ✓ Databricks SQL |
| Connectors & ecosystem | HTTP in; Delta + Iceberg out | ✓ huge | ✓ huge | Flink/Spark connectors | ✓ Databricks |
| Operations & footprint | ✓ 1 binary, 39 MB idle, 0.02 s start | JVM cluster | JVM cluster + checkpoints | JVM + ZooKeeper + Flink tiering job | managed |
| Governance & security | none yet | via platforms | via platforms | TLS/Kerberos (1.0) | ✓ Unity Catalog |
| Maturity | prototype | ✓ | ✓ | incubating | beta |

## Processing power

### TPC-H SF1 (new this round)

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
- **To close that:** more read-only nodes (each has its own caches), shuffles, and partitioned
  tables — all in the plan below.

## Footprint and operations

| | Pondra | Spark 4.2 | Flink 2.3 | Fluss |
|---|---|---|---|---|
| What you install | one binary, 90.0 MB (30.5 MB gzip, 18.7 MB xz) | 485 MB PySpark + a JVM | 353 MB PyFlink + a JVM | CoordinatorServer + TabletServers + ZooKeeper + a Flink tiering job, JVM |
| Start to first query | **0.02–0.07 s** | 4.1 s | 5.2–5.4 s | — |
| Idle memory | **39 MB** | — | — | — |
| Peak memory in these runs | **293–609 MB** (1.3 GB at 2.7 M events/s sustained) | 0.7–1.5 GB | 1.2–3.1 GB | — |
| State | object storage only; nodes are disposable (SSD tier = a cache) | + Kafka/Fluss + checkpoints | + Kafka/Fluss + checkpoints | TabletServer disks (replicated) + object storage |

## Where the JVM engines still win, and the plan

| Gap | Why it matters | Plan | What proves it |
|---|---|---|---|
| **Scale-out beyond one stage** (no shuffles; big-to-big joins on one node) | Spark's core strength; TPC-H at SF100+ needs it | Shuffle through the job-dealing mechanism tiering already uses: hash-partitioned exchange between nodes, spill to local SSD | TPC-H SF100 on 3–10 real machines vs Spark, same hardware |
| **Multi-machine evidence** | Everything above is one box | Run the suite and benchmarks on 3–20 cloud VMs against S3/R2 | Near-linear ingest and query scaling, failover times |
| **Streaming semantics** (event time, watermarks, windows, timers, CEP) | Flink's core strength | Tumbling/sliding/session windows with watermarks in views and tasks, on the same exactly-once commit path | Nexmark queries vs Flink |
| ~~Durable ack in ms on object storage~~ | Fluss's edge | **Done in round 8:** `--ack replicated`, 2–4 ms on R2, recovery tested by failovers. Next: `fsync` option, 3-replica tests on real machines | ✓ ack p50 2 ms on R2 |
| **Open-format lag** on object storage | Other engines see a table 3–10 s after the ack on R2 (a few sequential round trips) | Hung idle connections fixed (round 8); next: overlap publishing with the next fold, fewer sequential writes for Iceberg, a lower default `--tier-secs` when the bucket is close | p99 < 3 s on a nearby bucket |
| **APIs** | Usability for data teams | Arrow Flight SQL / Postgres wire (BI tools, JDBC), a Python client with DataFrame-style calls, Python UDFs | Tableau / Power BI / psql connect; notebook demo |
| **Connectors** | Getting data in and out | Kafka-protocol ingest endpoint, CDC from Postgres/MySQL (Iceberg and Delta output: done in round 8) | Debezium → Pondra → Spark reading Iceberg |
| **Table layout** (Delta liquid clustering, Iceberg sort orders, partitions) | Big tables with selective filters | `cluster_by` (round 8: 6–11x on selective filters) → partitions → clustering across files → deletion vectors | TPC-H SF100 with partition + clustering pruning |
| **Heavy new analytical queries at high concurrency** | Lakehouse//RT's edge | Prepared-plan cache, partitioned tables (pruning), per-node caches on many read-only nodes | TPC-H SF10 at 1k+ QPS mixed, p99 < 100 ms on N nodes |
| **Governance** | Enterprise requirement | Auth (tokens, per-table grants), audit log, quotas | Multi-tenant test |
| **Maturity** | Trust | Chaos tests on real clusters, fuzzing, long soak runs, versioned upgrades | Months of soak without data loss |

Sources:

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
