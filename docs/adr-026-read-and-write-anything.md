# ADR-026: Read and write anything (round 23)

**Date:** 2026-09-28 · **Status:** accepted and built (round 23; the owner: "go ahead with building next round") · **Follows:** ADR-019 (attached lakes), ADR-020, ADR-025

## Context

The owner, 2026-09-28: Pondra is becoming a processing engine too, and should read and write as
many sources and targets as its competitors; the order is the agent's to choose.

What Pondra reads today: its own lakes (a folder or S3-compatible storage; other lakes attached
as databases), files on the owner's own machine (Parquet, CSV, JSON), rows pushed in (HTTP, the
Kafka protocol, Flight, Postgres `COPY`, `INSERT`), and Python data. What it writes: its own
tables, published as Delta and Iceberg for other engines, and answers to clients. What it can't:

| Competitor | Reads and writes that Pondra lacks |
|---|---|
| DuckDB | `read_parquet/csv/json('s3://…')` on S3, GCS, Azure and HTTPS; `delta_scan`, `iceberg_scan`; Postgres and MySQL attached; `COPY … TO 's3://…'` |
| Spark / Databricks | data sources for every format and store; JDBC; Delta and Iceberg both ways; Lakehouse Federation (Postgres, MySQL, Snowflake, BigQuery…); Auto Loader (new files as a stream) |
| Snowflake | external stages and tables on S3, GCS, Azure; Iceberg tables in other catalogs; Snowpipe and Kafka ingest; `COPY INTO` a stage |
| Flink | connectors: Kafka (both ways), files, JDBC, CDC from databases |
| Polars, Daft | `scan_parquet/csv/ndjson/delta/iceberg` and `sink_*` anywhere |

## Decision

### 1. One shape for every source: a URL, or a name attached

A file, a folder or a glob is a table wherever SQL takes one, with DuckDB's names:

```sql
SELECT * FROM 's3://sales/2026/*.parquet';                       -- by extension
SELECT * FROM read_csv('gs://drop/orders_*.csv', header => true, delim => ';');
SELECT * FROM read_json('https://example.com/feed.ndjson');
SELECT day, sum(amount) FROM read_parquet('az://lake/events/', hive_partitioning => true) GROUP BY day;
SELECT * FROM delta_scan('s3://other/warehouse/orders');         -- another engine's table
SELECT * FROM iceberg_scan('s3://other/warehouse/db/orders');
```

A catalog of tables is attached once, as Pondra's own lakes are (ADR-019), and read as
`name.schema.table`:

```sql
ATTACH 's3://other/warehouse' AS spark_lake (TYPE delta);        -- a folder of Delta tables
ATTACH 'https://polaris.example.com/api/catalog' AS pol (TYPE iceberg, SECRET polaris);
SELECT * FROM pol.sales.orders o JOIN orders p ON o.id = p.id;   -- joined with Pondra's
```

- **Schemes:** `s3://` (AWS, R2, MinIO, anything S3-compatible), `gs://`, `az://` and
  `abfss://`, `https://`, and paths on the owner's own machine (only for the program that started
  the node: invariants 21 and 61 stand).
- **Spread:** a glob's files are the slices, dealt to the nodes by bytes, as a table's files are;
  their footers give statistics for pruning and join order. A Delta or Iceberg table's files come
  from its log or manifests, pruned by their statistics and partitions.
- **Formats' own rules, read natively:** the Delta log (JSON commits, Parquet checkpoints,
  deletion vectors, column mapping by name and id) and Iceberg metadata (v1–v3 manifests,
  position and equality deletes, field ids, partition transforms). Pondra already writes both;
  reading them is its own code over DataFusion's Parquet reader, not delta-rs or iceberg-rust
  (each brings a DataFusion of another version: a second engine in the binary, and upgrades held
  back to theirs). A feature Pondra doesn't read yet is refused by name, never read wrongly.

### 2. One shape for every target: `COPY … TO`, and `INSERT` into what is attached

```sql
COPY (SELECT * FROM orders WHERE day = '2026-09-27') TO 's3://exports/orders/' (FORMAT parquet, PARTITION_BY (region));
COPY orders TO 'gs://drop/orders.csv' (FORMAT csv, HEADER true);
INSERT INTO spark_lake.sales.orders SELECT * FROM orders WHERE day = '2026-09-27';   -- a Delta commit
INSERT INTO pol.sales.orders SELECT …;                                               -- an Iceberg commit through its catalog
```

- `COPY … TO` runs where the rows are: each node writes its slice's files, the coordinator
  collects their names; `PARTITION_BY` writes Hive-style folders. On a node it needs the admin
  role (it writes outside the lake); from the shell it may write the owner's own files.
- `INSERT` into an attached Delta or Iceberg table appends through that format's protocol (a
  put-if-absent log entry; a REST catalog commit), exactly-once with a job id; `UPDATE`,
  `DELETE` and `MERGE` into them come after (copy-on-write first).

### 3. Credentials: `CREATE SECRET`, never in SQL text

```sql
CREATE SECRET sales_s3 (TYPE s3, KEY_ID '…', SECRET '…', REGION 'eu-west-1', SCOPE 's3://sales');
CREATE SECRET polaris (TYPE iceberg, CLIENT_ID '…', CLIENT_SECRET '…');
CREATE SECRET smtp (TYPE generic, HOST 'smtp.example.com', USER '…', PASSWORD '…');   -- for procedures (ADR-027)
```

DuckDB's statement. Secrets live in the catalog encrypted with a key only the nodes hold
(`PONDRA_SECRET_KEY`, or a file beside the node), so the bucket never holds one in the clear. SQL
can list their names, types and scopes, never read their values back; a URL takes the secret
whose scope is its longest prefix, then the node's environment (`AWS_*`, Google's and Azure's
own). Only an admin makes or drops one. The same secrets serve databases, Kafka clusters and
Python procedures (`pondra.secret("smtp")`, ADR-027).

### 4. Every store for lakes too

`object_store`'s GCS and Azure support is switched on: `pondra serve --dir gs://…` or `az://…`
works as `s3://` does, with the same tests (their emulators: fake-gcs-server, Azurite).

### 5. Kafka clusters, both ways

```sql
ATTACH 'kafka://broker1:9092,broker2:9092' AS k (TYPE kafka, SECRET kafka_prod);
SELECT * FROM k.orders LIMIT 10;                                   -- what the topic holds now
CREATE MATERIALIZED VIEW orders_in AS SELECT … FROM k.orders;      -- a feed, kept up to date
COPY (SELECT …) TO 'kafka://broker1:9092/alerts' (FORMAT json);     -- rows out, once
```

A feed is a streaming task: each topic partition is a shard, dealt to the nodes; the offsets it
has read commit in the catalog with its rows, so it is exactly-once without Kafka's transactions.
Out, the same the other way (and a view or task kept in step with a topic is G7, later). The
client is pure Rust (`rskafka`), not librdkafka: nothing to link, and the glibc 2.17 build stays.

### 6. The same from Python and JavaScript

Frames follow Polars' names (ADR-022), so: `db.scan_parquet(url)`, `scan_csv`, `scan_ndjson`,
`scan_delta`, `scan_iceberg` give frames, and `frame.sink_parquet(url)`, `sink_csv`,
`sink_ndjson` are `COPY … TO`; `pondra.spark` has `spark.read.parquet(url)` and
`df.write.parquet(url)`. Each is the SQL above, so every door gets the same behaviour.

### What it costs

- Every connector is Rust inside the one binary, and costs nothing until a query uses it: no
  background work unless a feed exists (principle 6: no memory when unused).
- The binary grows by the GCS and Azure clients, the Kafka client and the formats' readers:
  measured and reported, as round 17 did.

## Rejected

- **delta-rs and iceberg-rust:** a second DataFusion in the binary, and ours held to their pace.
- **A plugin process or a JVM for connectors** (Spark's and Flink's way): principles 1 and 4.
- **librdkafka:** C to build for five platforms and glibc 2.17; `rskafka` is enough for fetch
  and produce, and the offsets live in Pondra's catalog anyway.

## Order within the round

1. `CREATE SECRET`; files on S3, GCS, Azure and HTTPS (read, spread, `COPY … TO`); GCS and Azure lakes.
2. Delta and Iceberg read (`delta_scan`, `iceberg_scan`, `ATTACH … (TYPE delta | iceberg)`, REST catalogs), deletes included.
3. `INSERT` into attached Delta and Iceberg tables.
4. Kafka clusters in and out.

Postgres and MySQL attached (G6) follow in round 25, with the console.

## Tests (the plan)

- Each format × each store (MinIO, fake-gcs-server, Azurite, a local HTTPS server): answers equal
  DuckDB's reading the same files; one node == three nodes.
- Delta tables written by delta-rs and by Spark 4 (deletion vectors, column mapping, schema
  changes) and Iceberg tables by PyIceberg and Spark (position and equality deletes, partition
  evolution): Pondra's reads equal theirs; a table with a feature not yet read is refused.
- `COPY … TO` in each format read back by DuckDB and Polars; `INSERT` into Delta and Iceberg read
  back by delta-rs and PyIceberg.
- A Kafka feed through a node killed and a leader failover: every record once (Redpanda or the
  Apache Kafka image).
- A secret never appears in a query's text, a log, the catalog in the clear, or an error.
- Performance: TPC-H SF1 read from Parquet files on S3 as fast as from Pondra's own tables (within 10%).

## As built (round 23, 2026-09-28)

Everything in the decision is built, from every door (SQL over HTTP and Postgres, Python's
connection and frames, `pondra.spark`, JavaScript and MCP by SQL):

| | Read | Write |
|---|---|---|
| Files: Parquet, CSV/TSV, JSON lines | `'s3://…/*.parquet'`, `read_parquet/csv/json(…)`: globs, folders, lists; Hive folders typed, NULL's folder as NULL; S3/R2/MinIO, GCS, Azure, HTTP(S), the owner's machine | `COPY … TO` a file or folder: `PARTITION_BY`, `OVERWRITE`/`APPEND`, Parquet's compression and row groups, CSV's header and delimiter |
| Delta Lake | `delta_scan(url [, version =>])`, `ATTACH … (TYPE delta)`: JSON commits, checkpoints (classic, in parts, v2 with sidecars), deletion vectors, column mapping by name and id, partitions | `INSERT` into an attached table: a put-if-absent commit with a `txn` for the job |
| Iceberg | `iceberg_scan(url [, version, snapshot_from_id, snapshot_from_timestamp, allow_moved_paths])`; REST catalogs with OAuth: v1–v3, position and equality deletes, deletion vectors, field ids, partitions | `INSERT` into a v2 table (a folder's or a REST catalog's), exactly-once by job |
| Kafka | `'kafka://brokers/topic'`, `ATTACH … (TYPE kafka)`: `_partition, _offset, _timestamp, key, value`, spread by partition; SASL PLAIN, SCRAM-SHA-256/512, TLS | `COPY … TO 'kafka://…'` with Kafka's key partitioning; a materialized view over a topic is a feed, every record once |
| Lakes | on GCS and Azure too (`serve --dir gs://…`, `az://…`) | |

Where it went beyond the decision, or differs from it:

- **Kafka's client is Pondra's own**, over the wire code its Kafka port already had (`kafka.rs`),
  not `rskafka`: the same encoders both ways, no new crate; SCRAM through aws-lc-rs, TLS through
  rustls (both already in the binary). SCRAM-SHA-512 and TLS are built, not yet tested against
  a broker (SCRAM-SHA-256 and PLAIN are).
- **The GCS emulator is `tools/sim_gcs.py`** (the XML API `object_store` speaks):
  fake-gcs-server refused its uploads and gcp-storage-emulator its listings. Azure's is Azurite.
- **Files are fresh and still fast.** Every statement lists them again (a file added or changed
  under its name is read as it is); byte ranges are kept in memory only as the version that
  statement listed (the e-tag, fetched with `If-Match`); the listing is the only look at a file
  before it is read; each file's rows and ranges reach the planner. Found on the way: DataFusion
  55 keeps folder listings forever by default (turned off: invariant 95).
- **`COPY … TO` a folder is written by every node** when the query's rows split over them as they
  are (`spmd::copy`); anything else is written from one node.
- **Not yet:** Iceberg v1 and v3 tables are read, not written (refused by name); `UPDATE`,
  `DELETE` and `MERGE` into another engine's table (as decided: later); Postgres and MySQL
  attached (round 25).

Measured (`logs/round23/`):

- **TPC-H SF1 from files is as fast as from the lake's own tables** (`tools/bench/files_tpch.py`,
  best of 5, two cores): on this machine's disk 3.67 s against 3.73 s, on a local S3 3.51 s
  against 3.48 s, every answer equal. From tpchgen's own files (Snappy, small row groups) 4.16 s
  against 3.44 s: the encoding, not the path. Before this round's last fixes, files on S3 took
  214 s (no cache: every read went to the bucket), then 4.69 s (a second look at each file, and
  no statistics for the join order).
- **The binary** grew by 3.3 MB, to 101.2 MB (34.4 MB gzipped; 0.22.2's is 97.9 MB): GCS and
  Azure's clients, the Kafka client, both formats' readers and writers, Avro. The code grew by
  4,650 lines, to 22,200.
- **Tests:** `harness.py outside` (27 checks), `clouds` (10), `kafkas` (9), `formats_check.py`
  (50: Spark 4, delta-rs and PyIceberg's tables, attached, inserted, spread), `frames_check.py`
  section 6, `spark_check.py`'s file pipelines. Invariants 95, 96, 98, 100, 101, 104 and 106
  were each seen to fail their tests without their code; 97, 99, 102 and 103 have tests that run
  them; 105 can't be shown on one machine.
