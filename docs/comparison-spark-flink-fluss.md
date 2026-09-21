# Pondra vs Spark, Flink, Fluss and Databricks Lakehouse//RT (round 7)

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
- `tools/delta_check.py` (open-lake freshness)

**Where the numbers come from:**
- Spark's TPC-H and every Pondra number: this round.
- Spark's and Flink's batch and streaming numbers: round 3, same machine and scripts.

## The short answer

**Per machine: yes, in the main areas, measured.**

- **Batch SQL:** TPC-H SF1 runs **~10x faster than Spark** (5.9 s vs 58–65 s for all 22
  queries, same answers).
- **Stateful streaming:** about **3x Flink's** throughput.
- **Latency:** a change shows up on another node in **5 ms**, where Flink's exactly-once
  results wait for a checkpoint.
- **The open lake:** fresh in **17 ms**, where Fluss's lags 3 minutes.
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
| Durable ack on standard object storage | one PUT (0.3–0.7 s) | — | — | ✓ ms (local-disk replication) | — |
| Open lake freshness | ✓ 17 ms (Delta) | per micro-batch | per checkpoint | 3 min default | reads the lake |
| Serving: point reads, repeated dashboards | ✓ 0.1–3 ms, 20–36k/s on 2 cores | | | ms lookups | 10 ms, 12k QPS (cluster) |
| Serving: new analytical queries on big data | 35–600 ms (single node) | | | — | ✓ sub-100 ms (claimed) |
| Scale-out to 100s of machines | unproven; no shuffles | ✓ | ✓ | ✓ | ✓ |
| Streaming semantics (event time, windows, CEP, huge state) | decomposable aggregates + SQL tasks | good | ✓ | storage only | — |
| APIs & usability | SQL over HTTP, JSON/Arrow out, one command | ✓ SQL + DataFrames (Python/Scala/Java/R), notebooks | SQL + DataStream API | clients (Java, Rust, Python) | ✓ Databricks SQL |
| Connectors & ecosystem | HTTP in, Delta out | ✓ huge | ✓ huge | Flink/Spark connectors | ✓ Databricks |
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

## Latency and freshness

| | Pondra (local disk) | Pondra (R2 from this far-away sandbox) | Flink | Spark | Fluss |
|---|---|---|---|---|---|
| Write acked, durable | **5 ms** p50 / 8 ms p99 | 0.66–1.1 s (one PUT) | at checkpoint | per micro-batch | ms |
| Event → updated aggregate row on another node | **5 ms** p50 / 7 ms p99; 38–77 ms p50 under load | ≈ the ack | ms, exactly-once only at checkpoint | RTM: single-digit ms stateless, open-source at-least-once | ms |
| New row readable by other engines (open lake) | **17 ms** (a Delta version), 31 ms via delta-rs | ~5 s (3 round trips) | per checkpoint | per micro-batch | 3 min default |
| A new user's first query | 17–24 ms | **20–30 ms** on a warm node; ≈1 s on a node that just joined | — | — | ms |

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
| What you install | one binary, 89.6 MB (30.1 MB gzip, 16.9 MB xz) | 485 MB PySpark + a JVM | 353 MB PyFlink + a JVM | CoordinatorServer + TabletServers + ZooKeeper + a Flink tiering job, JVM |
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
| **Durable ack in ms on object storage** | Fluss's edge | S3 Express One Zone as the log bucket (measure first); optional ack after replication to 2 nodes' memory/SSD | Ack p50 < 20 ms on S3 Express |
| **APIs** | Usability for data teams | Arrow Flight SQL / Postgres wire (BI tools, JDBC), a Python client with DataFrame-style calls, Python UDFs | Tableau / Power BI / psql connect; notebook demo |
| **Connectors** | Getting data in and out | Kafka-protocol ingest endpoint, CDC from Postgres/MySQL, Iceberg metadata beside Delta | Debezium → Pondra → Spark reading Iceberg |
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
- [Jack Vanlightly: Understanding Apache Fluss](https://jack-vanlightly.com/blog/2025/9/2/understanding-apache-fluss)
- [Flink: end-to-end exactly-once](https://flink.apache.org/2018/02/28/an-overview-of-end-to-end-exactly-once-processing-in-apache-flink-with-apache-kafka-too/)
- [Confluent: delivery guarantees and latency in Flink](https://docs.confluent.io/cloud/current/flink/concepts/delivery-guarantees.html)
