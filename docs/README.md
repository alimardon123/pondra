# Docs

Read in this order:

| File | What it is |
|---|---|
| `prototype-status.md` | Where the prototype stands: what it does, every measured number, what's missing (round 7, the current one) |
| `lake-format.md` | The lake on disk and in the bucket (same layout), its formats, and how Spark, DuckDB, Polars & co. read it without Pondra |
| `comparison-spark-flink-fluss.md` | Head-to-head with Spark, Flink, Fluss and Lakehouse//RT: TPC-H, streaming, latency, serving, a scorecard, and the plan for the gaps |
| `adr-008-serving-reads.md` | Current design: key lookups without SQL, keyed tables read as anti-joins, the version-exact result cache |
| `adr-007-open-lake-and-first-reads.md` | Delta Lake publishing, event-driven tiering, the SSD tier, the in-memory catalog on followers |
| `adr-006-serving-and-lsm.md` | Keyed tables as an LSM, serving reads, read-only nodes on the commit stream |
| `adr-005-every-node-writes.md` | The round-4 design: every node ingests, the leader only orders commits, inline views, push, SPMD queries and maintenance |
| `adr-004-cluster.md` | Clustering: leader election through the bucket, fencing, heartbeats, distributed task state |
| `adr-003-serverless-spmd.md` | Why serverless and SPMD (Bodo-style) instead of driver/executor |
| `adr-002-streamhouse-single-binary.md` | Why one binary instead of Fluss + Flink + Kafka; the streamhouse design |
| `adr-001-lakehouse.md` | The first study: DuckLake, published snapshots, alternatives (pg_lake, Iceberg, Delta) |

`../logs/round4/` to `../logs/round7/` hold the raw output of the test runs these reports quote.
