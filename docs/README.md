# Docs

Read in this order:

| File | What it is |
|---|---|
| `prototype-status.md` | Where the prototype stands: what it does, every measured number, what's missing (round 4, the current one) |
| `comparison-spark-flink-fluss.md` | Head-to-head with Spark, Flink and Fluss: batch, streaming, latency, footprint |
| `adr-005-every-node-writes.md` | Current design: every node ingests, the leader only orders commits, inline views, push, SPMD queries and maintenance |
| `adr-004-cluster.md` | Clustering: leader election through the bucket, fencing, heartbeats, distributed task state |
| `adr-003-serverless-spmd.md` | Why serverless and SPMD (Bodo-style) instead of driver/executor |
| `adr-002-streamhouse-single-binary.md` | Why one binary instead of Fluss + Flink + Kafka; the streamhouse design |
| `adr-001-lakehouse.md` | The first study: DuckLake, published snapshots, alternatives (pg_lake, Iceberg, Delta) |

`../logs/round4/` holds the raw output of the test runs these reports quote.
