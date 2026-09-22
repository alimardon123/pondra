# Docs

Read in this order:

| File | What it is |
|---|---|
| `prototype-status.md` | Where the prototype stands: what it does, every measured number, what's missing (round 11, the current one) |
| `lake-format.md` | The lake on disk and in the bucket (same layout): the native format, who can read and write it how, small files and compaction, and the optional Delta/Iceberg metadata other engines read |
| `comparison-spark-flink-fluss.md` | Head-to-head with Spark, Flink, Fluss and Lakehouse//RT: TPC-H, streaming, freshness like for like, serving, a scorecard, Fluss 1.0 item by item, what Fluss, Flink, Spark, Databricks and Snowflake are building next, and the plan for the gaps |
| `adr-012-toward-petabytes.md` | Current design: table metadata that stays small (per-file statistics, manifests, a million files), partitions, memory limits and spilling, shuffles between nodes, Arrow Flight and Flight SQL, `/metrics` |
| `adr-011-open-doors.md` | Round-10 design: the Kafka protocol (producers, Debezium, consumers, groups), the Iceberg REST catalog, `ALTER TABLE … ADD COLUMN`, event-time windows that emit once, JSON functions, timestamps in µs |
| `adr-010-anywhere.md` | Round-9 design: the bucket inbox (writes from any network), attached lakes (several leaders on one bucket), SQL writes, the Postgres protocol, the Python client, MCP for AI agents, vector search, tokens, size-tiered keyed compaction, TTL, the change feed |
| `adr-009-native-first.md` | Round-8 design: replicated acks without S3 Express, native format first with Delta/Iceberg on request, writes from any machine, `cluster_by` |
| `adr-008-serving-reads.md` | Key lookups without SQL, keyed tables read as anti-joins, the version-exact result cache |
| `adr-007-open-lake-and-first-reads.md` | Delta Lake publishing, event-driven tiering, the SSD tier, the in-memory catalog on followers |
| `adr-006-serving-and-lsm.md` | Keyed tables as an LSM, serving reads, read-only nodes on the commit stream |
| `adr-005-every-node-writes.md` | The round-4 design: every node ingests, the leader only orders commits, inline views, push, SPMD queries and maintenance |
| `adr-004-cluster.md` | Clustering: leader election through the bucket, fencing, heartbeats, distributed task state |
| `adr-003-serverless-spmd.md` | Why serverless and SPMD (Bodo-style) instead of driver/executor |
| `adr-002-streamhouse-single-binary.md` | Why one binary instead of Fluss + Flink + Kafka; the streamhouse design |
| `adr-001-lakehouse.md` | The first study: DuckLake, published snapshots, alternatives (pg_lake, Iceberg, Delta) |

`../logs/round4/` to `../logs/round11/` hold the raw output of the test runs these reports quote.
