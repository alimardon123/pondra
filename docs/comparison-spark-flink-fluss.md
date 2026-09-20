# Pondra vs Spark, Flink, Fluss and Databricks: batch, streaming, serving (round 5)

**Date:** 2026-09-21 · **Machine:** one 2-vCPU, 7 GB sandbox VM, local disk; every engine ran alone, one at a time
**Versions:** Pondra (this prototype, DataFusion 55) · Spark 4.2.0 (PySpark, `local[*]`) · Flink 2.3.0 (PyFlink, local MiniCluster, parallelism 2) · Fluss 0.9 (not runnable here; see the last section)
**Reproduce:** `tools/bench/run.py` (same generated data and same SQL for every engine; results agree across engines), `tools/cluster.py latency | split | spread`.
Spark and Flink numbers are from round 3 (same machine, same scripts); Pondra's were re-measured on the round-5 binary.

## Footprint

| | Pondra | Spark 4.2 | Flink 2.3 |
|---|---|---|---|
| What you install | one binary, **89.2 MB** (29.9 MB gzip, 16.8 MB xz), no runtime | 485 MB PySpark + a 286 MB JVM | 353 MB PyFlink + a 286 MB JVM |
| Start to first query | **0.03 s** | 4.1 s | 5.2–5.4 s |
| Idle memory | **41 MB** | — | — |
| Peak memory in these runs | **279–586 MB** (1.8 GB saturated for 60 s) | 0.7–1.5 GB | 1.2–3.1 GB |
| Durable streaming storage | built in (log in the bucket, exactly-once) | needs Kafka / Fluss + checkpoint storage | needs Kafka / Fluss + checkpoint storage |

## Batch: 20M rows, write then 6 queries (seconds, best of two runs)

| | Pondra | Spark | Flink ¹ | Pondra vs Spark |
|---|---|---|---|---|
| Generate + write 20 M rows | **3.1** (Parquet ZSTD, 79 MB) | 7.3 (Parquet Snappy, 209 MB) | 10.5 (CSV, 603 MB) | 2.3x |
| Q1 `count, sum, avg` | **0.04** | 0.70 | 5.2 | 16x |
| Q2 filter + group by 1,000 categories | **0.13** | 0.93 | 6.5 | 7x |
| Q3 group by 1 M users, top 10 | **0.43** | 2.20 | 8.9 | 5x |
| Q4 `count(DISTINCT user)` | **0.33** | 1.75 | 7.5 | 5x |
| Q5 join with a dimension table | **0.25** | 2.30 | 9.4 | 9x |
| Q6 time buckets | **0.25** | 0.52 | 4.7 | 2x |
| Peak memory | **279 MB** | 1.5 GB | 3.1 GB |  |

¹ Flink's Parquet format jar isn't bundled with PyFlink, and Maven Central is blocked in this sandbox, so Flink wrote and read CSV. Its batch numbers are therefore pessimistic. Batch isn't Flink's focus anyway.

Round 3 ran these queries 10–25 % faster (e.g. Q1 0.034 s, Q3 0.35 s): the round-4 binary spends a little more per query on the extra bookkeeping (commit numbers, pinned reads) and on checking whether to spread the query across nodes.

## Streaming: 10M events

Every engine computes the same running aggregation (count and sum per key, 100,000 keys) or the same stateless filter/projection.

| Workload | Pondra | Spark Structured Streaming | Flink (streaming mode) |
|---|---|---|---|
| **Stateful:** keyed running aggregation | **4.0 s end to end (2.5 M events/s)**: 10 M events sent over HTTP by 4 client threads, stored durably, aggregated by an inline view, with the result queryable. Ingest alone: 2.7 s | 3.9 s (2.5 M/s) reading a 10 M-row in-memory backlog in one micro-batch, `noop` sink; 12.8 s in 500k-row micro-batches | 13.3 s (0.75 M/s), in-memory source, `blackhole` sink |
| **Stateless ETL:** project 4 columns + filter | **3.9 s end to end (2.6 M events/s)**, output stored durably | 5.2 s (2.0 M/s), `noop` sink | 4.0 s (2.5 M/s), `blackhole` sink |
| **Sustained, durable, end to end:** producers → HTTP (Arrow) → log in storage → aggregated view → query | **2.8 M events/s for 60 s on one node; acked → visible in the aggregate p50 0.21 s, p99 0.73 s** | not measurable here: needs Kafka or Fluss plus checkpointing | same |
| 3-node cluster, producers writing to followers only | **3.8 M events/s**, durable, exactly-once | — | — |

**How to read these rows:**

- **Sources and sinks differ.** Spark and Flink generated their input in memory (`rate`, `datagen`) and threw the output away. Pondra received its input over HTTP, stored it durably, and committed the results. The comparison is tilted *against* Pondra.
- **Round 3 for reference:** ingest took 6.95 s (1.44 M/s) and the stateful pass another 1.2 s. Round 4 does both at once in 4.0 s, because views run during ingest on every node.
- **Engine vs engine.** On 2 vCPUs, JVM and scheduling overhead dominate for Spark and Flink; large clusters would narrow the gap per core, not close it.

## Latency: how fast does a change show up?

Measured with `tools/cluster.py latency`: 3 nodes. A client writes one event at a time to one follower; another client, subscribed with `GET /watch/totals` on the *other* follower, times each event until the updated row of an aggregating view (`GROUP BY user`) arrives.

| | Local disk, idle | Local disk, 4 busy producers | Simulated R2, idle | Simulated R2, 4 busy producers |
|---|---|---|---|---|
| Write acknowledged (durable) | **6 ms** p50 / 10 ms p99 | 24 / 107 ms | 309 / 919 ms | 453 / 998 ms |
| View row pushed to a client on another node | **6 ms** p50 / 10 ms p99 | 65 / 461 ms | 333 / 1,072 ms | 602 / 1,146 ms |

What the numbers mean:

- **Pondra acknowledges only once a write is durable in storage.** On local disk that's a few milliseconds. On object storage it's one catalog write (simulated R2: PUT p50 197 ms, GET p50 100 ms, long tail), so ~0.3–0.5 s. The view update and the push to other nodes add almost nothing: the view is computed before the write and committed with it.
- **Round 3** took ~0.76 s p50 (1.8 s p99) from ack to a stateful result on local disk. Tasks polled every second and writes waited for a 250 ms group-commit window.
- **Flink** processes each event in memory within milliseconds. Its exactly-once results reach readers only when a checkpoint commits the sink's transaction: every few seconds to minutes (Confluent Cloud: "roughly one minute").
- **Fluss** acknowledges after replicating a write to other TabletServers' local disks, so its writes and streaming reads take milliseconds on any object store.
- **Where Pondra stands:**
  - On fast storage (local NVMe, or S3 Express One Zone, not measured here), Pondra reacts in milliseconds with exactly-once results, which Flink only gets at checkpoint time.
  - On standard object storage, Fluss is faster, by one object-store round trip. The reason: Pondra keeps no local disks and no replication protocol, only the bucket.

## Scaling out

One machine can't show multi-machine speed-ups, so these are shape checks, not scale-up numbers:

| Check | Result |
|---|---|
| Where the write work lands (`split`, 3 nodes, producers writing to the 2 followers) | 3.8 M events/s; the leader used **31 %** of the cluster's CPU — its one-third share. Before the tiering work was dealt out too, it was 70 % |
| Distributed query correctness (`spread`, 2 M rows, 3 nodes) | identical results to a single node for all 9 queries, no fallbacks |
| Distributed query speed-up, one CPU per node (2 nodes, 20 M rows) | scan/filter/join/time-bucket **1.7–1.95x**; high-cardinality group-by 1.16x and count-distinct 1.01x (their final merge dominates) |
| Distributed query speed-up on simulated R2 (3 nodes, I/O-latency bound) | 1.2–4.3x faster than one node on 8 of 9 queries (q1 921→472 ms, q9 760→176 ms), identical results |

## Serving: Databricks Lakehouse//RT, and where Pondra stands

Databricks' Lakehouse//RT (engine codename Reyden) is a *serving* layer over Delta/Iceberg:
"response times as low as 10 ms on smaller datasets and sub-100 ms performance on larger ones",
"sub-100 millisecond latency at 12,000 queries per second", in beta for read-only workloads.
It doesn't ingest; the write path stays Spark/DLT.

| | Pondra (one 2-vCPU node) | Lakehouse//RT (published) |
|---|---|---|
| Point lookup by key, 2 M keys, one client | **7.7 ms** p50, 11 ms p99 | 10 ms on small datasets, sub-100 ms on large |
| Lookups per second, 8 clients | 265/s on two cores (at 30 ms p50) | 12,000/s (cluster size not stated) |
| Dashboard aggregate over the whole table | 351 ms for 2 M rows | sub-100 ms |
| Freshness of what it serves | the log tail is in every read: 7 ms after the write (p50, local disk) | reads the lake; freshness is whatever wrote it |
| Writes | ingest, views, tasks, exactly-once, in the same binary | none (read-only beta) |
| What you run | one binary on your own machines | Databricks compute, Unity Catalog, their pricing |

Honest read: on raw serving latency at concurrency, a purpose-built serving engine on a cluster
wins today, and Pondra's 265 lookups/s per two cores needs prepared-plan caching and more nodes
to get interesting. On freshness, footprint and "the same system also ingests", Pondra is the
only one of the two that does the whole job.

## Fluss (unified streaming storage)

Fluss can't run in this sandbox (Apache mirrors and Maven Central are blocked), so this comparison uses its architecture and published numbers only.

| | Pondra | Apache Fluss 0.9 |
|---|---|---|
| Processes to run | 1 binary, N copies | CoordinatorServer + TabletServers + ZooKeeper + a Flink job for lake tiering, plus Paimon/Iceberg and object storage. All JVM |
| Where the durable data lives | object storage only; nodes are stateless | TabletServer local disks, replicated Kafka-style, tiered to object storage later |
| Write scale-out | every node ingests (encodes, stores, runs views); one leader only orders the commits | writes spread over buckets and TabletServers |
| Keyed aggregation in storage | merge tables (inline GROUP BY views): sum / count / min / max, merged on read and folded when tiered | aggregation merge engine (sum, min, max, …) on primary-key tables |
| Primary-key / upsert tables | merge-on-read + compaction to Parquet | KV tablets in RocksDB with a changelog; point lookups in milliseconds |
| Write → readable, durable | ~6 ms on local disk; ~0.3–0.5 s on object storage (simulated R2) | milliseconds (replicated to TabletServer disks) |
| Lake freshness | **no gap at all**: every query reads the log tail with the Parquet, so what is acked is queryable. Tiering to Parquet runs every 10 s, or as soon as 1 M rows wait | union read of Fluss + lake; the lake itself lags by `table.datalake.freshness`, 3 min by default |
| Freshness on a read-only serving node | 7 ms p50 (it follows the leader's commit stream) | milliseconds (reads from TabletServers) |
| Published throughput | this report: 3.8 M events/s through 3 nodes on 2 vCPUs, durable | community benchmark (Fluss 0.9.1, docker-compose, one TaskManager): 88.7k records/s vs Kafka's 98.6k. Rednote in production: ~1B records and 10 TB per day on one table; write CPU −30 %, write traffic −50 % after moving from Kafka |

**Verdict:**

- **Fluss still wins** on:
  - millisecond durability on standard object storage (it replicates to local disks instead);
  - point lookups on primary keys;
  - production proof at Alibaba / Rednote scale.
- **Pondra wins** on:
  - operational simplicity (no ZooKeeper, no disks to replicate, no JVM, no separate tiering job);
  - cost (object storage only);
  - freshness of the lake itself (the log tail is part of every query);
  - one engine for streaming and batch SQL.
- **Now matched:**
  - write scale-out;
  - keyed aggregation in storage.
- **For a company growing into enterprise scale:** Pondra covers streaming, batch and serving with one binary. A workload that needs millisecond durability on S3 itself can use S3 Express One Zone as the bucket. Only if that isn't enough does it need a Fluss-like tier.

Sources: [Fluss architecture](https://fluss.apache.org/docs/next/concepts/architecture/), [Fluss aggregation merge engine](https://fluss.apache.org/docs/table-design/merge-engines/aggregation/), [Fluss tiering service](https://fluss.apache.org/docs/1.0/streaming-lakehouse/tiering-service/), [Fluss 0.9 release](https://fluss.apache.org/blog/releases/0.9/), [Jack Vanlightly: Understanding Apache Fluss](https://jack-vanlightly.com/blog/2025/9/2/understanding-apache-fluss), [community Fluss vs Kafka benchmark](https://github.com/fmorillo7694/fluss-kafka-bench/blob/main/bench/results/README.md), [Rednote: Kafka to Fluss](https://fluss.apache.org/blog/rednote-kafka-to-fluss-real-time-indexing/), [Flink: end-to-end exactly-once with Kafka](https://flink.apache.org/2018/02/28/an-overview-of-end-to-end-exactly-once-processing-in-apache-flink-with-apache-kafka-too/), [Confluent: delivery guarantees and latency in Flink](https://docs.confluent.io/cloud/current/flink/concepts/delivery-guarantees.html).
