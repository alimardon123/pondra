# ADR-011: Open doors — the Kafka protocol, an Iceberg REST catalog, schema evolution, JSON, event-time windows

**Status:** Accepted, built and tested (round 10) · **Date:** 2026-09-22 · **Builds on:** ADR-010

## Context

The round-9 study of what Fluss, Flink, Spark, Databricks and Snowflake plan next put a few
items at the top of Pondra's plan:

- **A Kafka door.** It's how most event data travels:
  - Databricks' Zerobus is reported as Kafka-compatible;
  - Fluss plans log agents;
  - Flink and Spark read Kafka natively.
- **An open catalog.** Engines find tables through the Iceberg REST catalog API (Unity, Polaris,
  Snowflake), not through metadata file paths.
- **Schema evolution** (Fluss 1.0 lists it as a gap too).
- **Watermarks:** windows that close and emit once — Flink's core strength.
- **Semi-structured data:** VARIANT arrived in Spark, Flink, Delta and Iceberg v3.

Round 10 takes all five, in that order.

## Decisions

### 1. The Kafka protocol (`--kafka 0.0.0.0:9092`, `kafka.rs`)

Any node speaks enough of the Kafka protocol for producers, consumers and consumer groups. A
topic is a table, with one partition. Every node lists itself as that partition's leader, so
clients spread over nodes by the address they start from. Every node accepts every write
(ADR-005), so a client that moves from node to node loses nothing.

**Producing:**

- **Record → row.** Each record's value is a JSON row, decoded straight into Arrow. On top of that:
  - a table with `_key` / `_timestamp` columns also gets the record's key and timestamp;
  - a table with a `_value` column takes values raw, in any format (text, Avro, Protobuf bytes);
    JSON functions (§5) query them.
- **Change data.**
  - Debezium change events are unwrapped, with or without Kafka Connect's schema wrapper:
    `after` is the new row, and a delete marks `before` deleted.
  - A null value (a tombstone) deletes its key from a keyed table. The key is a JSON object of
    the key columns, or a plain value for a one-column key.
  - So Debezium → Pondra keeps a Postgres or MySQL table's current state, with nothing in
    between.
- **Exactly-once.** An idempotent producer's `(producer id, partition)` becomes a Pondra
  producer (`kafka:{id}:{topic}`), and its batch sequence becomes the producer's seq:
  - `seq = base sequence + record count`;
  - `prev = base sequence` (the last batch's seq).

  A retried batch is a duplicate: success, applied once. A batch that overtook its predecessor
  gets `OUT_OF_ORDER_SEQUENCE_NUMBER` and is resent. Producers that aren't idempotent are
  at-least-once, as in Kafka: an empty producer name skips the check.
- **Pipelining.** A connection's requests are decoded and queued in the log in arrival order,
  and answered in that order as their acks come back (`Log::queue`). A client keeps many
  batches in flight, as Kafka allows.
- **Also supported:**
  - gzip, snappy (raw and Java-framed), lz4 and zstd batches;
  - acks 0, 1 and all (1 and all both mean Pondra's ack: durable, or replicated).

**Consuming:**

- **What a topic reads.** A topic reads the table's log, as far back as it's kept
  (`--changelog-secs`).
- **Offsets.** A record's offset is its `_ord`: (segment << 32) + row. That's the position
  Pondra already uses for "newest version wins", and every node computes the same one.
  - Offsets increase but aren't dense: lag in records isn't the high watermark minus the
    offset.
  - `ListOffsets` finds the earliest segment still kept, the next one to be written, and the
    first at or after a time (binary search over segments).
- **Records.**
  - Values are JSON rows; a keyed table's key is `{"id": …}`.
  - Deletes arrive as tombstones.
  - Each segment's rows come as one uncompressed record batch, timestamped with the segment's
    commit time.
- **Long polling.** Fetch waits for the next commit, up to the client's max wait.

**Consumer groups** are coordinated by the leader, in memory:

- `FindCoordinator` on a follower points at the leader's Kafka address (`GET /cluster/kafka`).
- Join / Sync / Heartbeat / Leave follow Kafka's classic protocol: generations, the group
  leader member assigns partitions, and a member that joins, leaves or goes silent past its
  session timeout starts a new generation.
- Committed offsets are a producer's progress in the catalog (`kafka-group:{group}:{topic}`
  = offset + 1), written through the log like any append. They survive a new leader; members
  then just join again.
- A commit never moves an offset back.

**Tokens:** SASL/PLAIN. The user name picks the role (`reader`, `writer`, `admin`), and the
password is its token. A read token's produce gets `TOPIC_AUTHORIZATION_FAILED`.

**Not supported:**

- transactions (`transactional.id`: refused);
- more than one partition per topic;
- the protocol's "flexible" versions. We advertise the versions before them, which current
  clients still speak: Kafka 4's clients talk to brokers back to Kafka 2.1. The Java client
  itself is untested here (no jars in this sandbox); librdkafka and kafka-python are tested.

### 2. An Iceberg REST catalog (`GET /v1/…`, in `iceberg.rs`)

The read side of the Iceberg REST catalog API, over the Iceberg metadata Pondra already
publishes:

- `/v1/config`;
- namespaces: `default` for this lake, plus one per attached lake;
- the tables that publish Iceberg;
- load table: the current metadata file and its contents.

Engines attach a node by URL instead of pointing at metadata files. PyIceberg and DuckDB are
tested; Spark, Trino and Snowflake speak the same API, untested here. Tokens work as elsewhere (`Authorization: Bearer`). Commits through the catalog
aren't supported: Pondra writes, other engines read.

**Publishing got better at the same time:**

- **Timestamps.** SQL `TIMESTAMP` columns are now kept in microseconds, as Iceberg, Delta,
  Spark and Postgres keep them. So tables with timestamps publish:
  - to Iceberg: `timestamp`, and `timestamptz` for time-zone-aware columns;
  - to Delta: `timestamp`, time-zone-aware columns only.

  Before, any timestamp column stopped a table from publishing.
- **A keyed table's first file** has nothing older to shadow. It drops delete markers like a
  full compaction and is published at once. Since round 9, every keyed table created in SQL has
  `_deleted`, and those waited for a full compaction.
- **Keyed tables that publish compact fully** once 8 files pile up, as before round 9. Other
  engines see a keyed table as of its last full compaction, so those tables trade the round-9
  write savings for freshness; tables that don't publish keep the size-tiered merges.

### 3. `ALTER TABLE … ADD COLUMN` (`write.rs`, `query.rs`)

- **Where it runs:** from SQL on any node, over Postgres or MCP, and from `pondra sql`.
- **How it's recorded.** The new column is added at the end, as the table's definition sent
  again: the same path as CREATE TABLE (HTTP, inbox, or leading for a moment). The leader
  accepts a definition whose columns extend the table's; a re-sent original definition is
  fine, and anything else is refused.
- **Old data needs no rewrite.**
  - Parquet files read the new column as null (DataFusion's schema adapter).
  - Log rows written before the change are conformed to the current columns by name when read
    (`query::conform`): SQL, views, tasks, Kafka consumers and tiering all see the same.
- **Writes that don't know the column** keep working:
  - JSON and Arrow appends that leave out a column get nulls;
  - an INSERT with fewer values fills the first columns.
- **Other engines follow.** Delta gets a `metaData` action with the new schema. Iceberg's next
  snapshot carries it; columns are mapped by name, so old files read as null.
- `IF NOT EXISTS` is supported. Only adding is: no drop, rename or type change yet.

### 4. Event-time windows that emit once (`views.rs`)

`POST /views/{name}?window=w&size_secs=60&lateness_secs=10` with a GROUP BY view over
`date_bin(INTERVAL '1 minute', ts) AS w`:

- **The view** stays what it was: open windows, updated on every flush, late rows included.
- **`{name}_final`**, a new append table, gets each window once, final, when the watermark
  passes it:
  - the watermark is the newest window started;
  - a window closes once the watermark is past its end plus the allowed lateness.
- **Downstream.** It's a stream of closed windows: views, tasks and Kafka consumers can follow
  it, like Flink's windowed output.
- **Exactly-once.** The leader emits, every 0.5 s. The emission's progress is a producer's seq
  (`emit:{view}` = the watermark in microseconds, `prev` = the last one), committed with the
  rows. A leader that dies half-way leaves nothing behind twice.
- **Late rows** — after their window was emitted — still update the view, but not what was
  emitted.

**Not yet:** session windows, and a watermark from the source's event time rather than the
window starts. Ours lags that by up to one window.

### 5. JSON functions (`datafusion-functions-json`)

In every session:

- `json_get(col, 'a', 0, 'b')`, `json_get_str`, `json_get_int`, `json_get_float`,
  `json_get_bool`;
- `json_contains`, `json_length`, `json_as_text`;
- the operators `->` and `->>`.

They work over JSON kept as text: Kafka's raw `_value`, and semi-structured columns. This is
what VARIANT gives Spark and Flink, until DataFusion has VARIANT itself. One small dependency
(`jiter`), built for DataFusion 55.

## Results

Local disk, simulated R2 and a real R2 bucket; the full numbers are in `prototype-status.md`.

- **`harness.py kafka`: 9 checks, on all three.**
  - librdkafka producers, idempotent, with each codec; kafka-python producers.
  - Exactly-once retries: a hand-built idempotent batch sent twice is applied once, and one
    that skips ahead is refused.
  - Debezium events with the schema wrapper, and tombstones.
  - Raw values, queried as JSON.
  - kafka-python and librdkafka consumers, deletes arriving as tombstones.
  - Consumer groups: a member commits and leaves, and the next one — starting from a follower
    node — resumes exactly there; with two live members, exactly one holds the partition.
  - A wrong password and a read token are refused.
- **`kafka_bench.py`** (3 nodes and 4 librdkafka producers on one 2-vCPU box):

  | Storage | Ack mode | Events/s | Ack p50 / p99 |
  |---|---|---|---|
  | Local disk | durable | 776k | 3 / 21 ms |
  | Local disk | replicated | 796k | 1 / 2 ms |
  | Real R2 | replicated | 461k | 1 / 20 ms |
  | Real R2 | durable | 68.7k | 245 / 606 ms |

  Every event arrived exactly once. A consumer on another node gets an event 1–3 ms after it
  was sent (local disk).
- **`harness.py alter`: 8 checks.** 65–72k rows were written by an old producer while columns
  were added. Old rows read null and new ones carry the column. The keyed table, the view, bulk
  and Arrow appends that leave the column out, and 6 outside readers all follow.
- **`harness.py windows`: 3 checks.**
  - only closed windows are emitted;
  - each window once, final;
  - a late row updates the view, not the emitted window;
  - nothing is emitted twice across a leader restart.
- **`open_check.py`: 8 outside readers**, the REST catalog through PyIceberg and DuckDB
  included. All equal Pondra, locally and on R2.

## Consequences

- **New invariants** (AGENTS.md):
  - 22: a Kafka batch's seq and `prev` come from its producer id and sequence; producers with
    no name are never checked.
  - 23: columns only grow, at the end, and reads conform older rows to the current columns.
  - 24: a window is emitted once, with its progress as the `emit:{view}` producer's seq.
- **New dependencies:**
  - `datafusion-functions-json` (and `jiter`);
  - `flate2`, `snap`, `lz4_flex`, `zstd` and `crc-fast` as direct dependencies. All five were
    already built for Parquet, Arrow and object_store.
- **The Kafka door and the REST catalog are features of every node.** Nothing new to run. The
  binary grew by 2.3 MB, to 93.0 MB (31.4 MB gzip, 17.6 MB xz); idle memory with `--kafka` is
  49 MB.
