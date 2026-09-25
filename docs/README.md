# Docs

Read in this order:

| File | What it is |
|---|---|
| `prototype-status.md` | Where the prototype stands: what it does, every measured number, what's missing (round 17, the current one) |
| `roadmap.md` | What's left and in what order after round 16 (with round 17's progress): six tracks (runs anywhere, in-process and in the browser, proof at scale, correctness, ready for a team, depth), lessons from DuckDB-WASM and PGlite, rounds 17–23, and the decisions only the owner can make |
| `lake-format.md` | The lake on disk and in the bucket (same layout): the native format, who can read and write it how, small files and compaction, and the optional Delta/Iceberg metadata other engines read |
| `comparison-spark-flink-fluss.md` | Head-to-head with Spark, Flink, Fluss, Lakehouse//RT and the single-node engines (DuckDB, Polars, Daft, Bodo): TPC-H, streaming, freshness like for like, serving, a scorecard, Fluss 1.0 item by item, what Fluss, Flink, Spark, Databricks and Snowflake are building next, and the plan for the gaps |
| `adr-018-install-anywhere.md` | Current design: a Linux binary for glibc 2.17, `pip install pondra` and `npm install pondra` with `local()`, a SQL shell, nodes that stop when whoever started them does, small-machine and proxy defaults, `sum` over DOUBLE the same in any order, a lite build measured and rejected, and a release workflow for five platforms |
| `adr-017-streams-on-their-own-time.md` | Round-16 design: the watermark taken from the stream's own event time (windows closed when the data is past them), session windows emitted once, whole, and point-in-time joins (`ASOF JOIN`) ad hoc, across the nodes and in views over a stream — against DuckDB's answers |
| `adr-016-data-that-knows-where-it-is.md` | Round-15 design: rows kept in the order they arrive, big tables split across the nodes by the ranges of a key they share (joins and aggregations on it without a shuffle), hot keys' partitions shared out, NOT IN across the nodes, distinct values sketched for the join order — and what was tried and left out |
| `adr-015-any-query-across-the-nodes.md` | Round-14 design: the plan, not the SQL, decides what runs across the nodes — every join type, subqueries, CTEs, unions, keyed tables (TPC-H 22 of 22); scalar subqueries answered between steps; exchanges that give the same answer every time; tables read whole at one snapshot; a GitHub Actions cluster benchmark ready to run |
| `adr-014-shuffles-that-fit-on-disk-and-a-join-order-of-its-own.md` | Round-13 design: a shuffle bounded by disk rather than memory (buckets and gathered results in pieces), a step retried and a node dropped mid-query, work dealt by size, skew measured, and inner joins ordered from the catalog's statistics |
| `adr-013-one-node-first-and-anything-in-a-column.md` | Round-12 design: what made one node fast (strings as views, LZ4, four planning rules, the hot columns), files, bytes, `VARIANT`, vectors and models in SQL, functions of your own over Arrow Flight, and publishing that costs what changed |
| `adr-012-toward-petabytes.md` | Round-11 design: table metadata that stays small (per-file statistics, manifests, a million files), partitions, memory limits and spilling, shuffles between nodes, Arrow Flight and Flight SQL, `/metrics` |
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

`../logs/round4/` to `../logs/round17/` hold the raw output of the test runs these reports quote.
