# Pondra: a streamhouse in one binary

One Rust binary (~10,100 lines) that ingests streams, stores them as a lakehouse (Parquet files
plus a catalog, on object storage; Delta Lake and Iceberg metadata for other engines on request),
keeps SQL views and streaming state up to date, answers SQL, and scales out by starting more
copies of itself on the same bucket. Object storage is the only state: no Postgres, no
ZooKeeper, no Kafka, no JVM. Runs on a local directory or any S3-compatible store (S3,
Cloudflare R2, MinIO).

## Run it

```bash
cargo build --release

# Local directory
./target/release/pondra serve --dir ./lake

# A cluster: the same command on each machine, same bucket. Every node takes writes and
# queries; one of them (elected through the bucket) orders the commits; any can take over.
export AWS_ACCESS_KEY_ID=… AWS_SECRET_ACCESS_KEY=… AWS_REGION=auto
export AWS_ENDPOINT=https://<account>.r2.cloudflarestorage.com
./target/release/pondra serve --dir s3://my-bucket/lake --addr 10.0.0.1:8080
./target/release/pondra serve --dir s3://my-bucket/lake --addr 10.0.0.2:8080
./target/release/pondra serve --dir s3://my-bucket/lake --addr 10.0.0.3:8080 --reader   # SQL only

# Serverless, from any machine with the binary and bucket credentials — no node needed:
./target/release/pondra sql --dir s3://my-bucket/lake "SELECT count(*) FROM events"
./target/release/pondra sql --dir s3://my-bucket/lake "INSERT INTO sales SELECT * FROM 'jan.parquet'"
./target/release/pondra sql --dir s3://my-bucket/lake "UPDATE users SET plan = 'pro' WHERE id = 7"

# SQL from anything that speaks Postgres, and from Python:
./target/release/pondra serve --dir ./lake --pg 0.0.0.0:5432      # psql -h localhost, psycopg, SQLAlchemy, BI tools
pip install ./python && python -c "import pondra; print(pondra.connect('http://127.0.0.1:8080').sql('SELECT 1').to_pandas())"

# Kafka producers and consumers (a topic is a table), and engines attaching the lake by URL:
./target/release/pondra serve --dir ./lake --kafka 0.0.0.0:9092   # bootstrap.servers=host:9092
#   PyIceberg / DuckDB / Spark: an Iceberg REST catalog at http://host:8080 (namespace "default")

# Arrow Flight and Flight SQL: ADBC / JDBC drivers and pyarrow, Arrow in and out
./target/release/pondra serve --dir ./lake --flight 0.0.0.0:8815  # adbc_driver_flightsql.dbapi.connect("grpc://host:8815")

# AI agents over MCP (Claude Code, Claude Desktop, Cursor, …): every node serves POST /mcp
claude mcp add --transport http pondra http://127.0.0.1:8080/mcp   # add --header "Authorization: Bearer $TOKEN" with tokens on
```

`--addr` must be reachable by the other nodes. Read-only nodes and `pondra sql` queries write
nothing, so read-only bucket credentials are enough for them.

**How machines share one lake.** The bucket is the platform; the binary is compute anyone
brings. Commits need one process to put them in order: whichever node leads, or — when nobody
runs — the `pondra sql` INSERT itself, for a moment.

- **Nobody running.** `pondra sql` reads the bucket directly. Its INSERT runs the query on that
  machine, writes the Parquet, and records the files itself.
- **A cluster running.** Nodes see each write milliseconds after it's acknowledged.
  - Laptops can join with `pondra serve` (or `--reader`) and leave again.
  - A laptop's `pondra sql` INSERT still does its own work; the leader only records the files.
- **After a full shutdown**, the first node started on the lake leads at once.
- **A machine that can't reach the leader** (another network, another company) still writes:
  its request goes through the bucket (`inbox/`), and the leader records it within a second or so.
- **Several clusters, one bucket:** each cluster leads its own lake and attaches the others
  (`--attach sales=s3://my-bucket/sales`): it reads `sales.orders`, and its writes to it are
  recorded by that lake's leader.

Useful `serve` flags (give every node the same ones: any of them may lead):

- `--ack durable|replicated`: when a write is acknowledged.
  - `durable` (default): once it is in the bucket. That is one object-store write: a millisecond
    on local disk, 0.25–0.7 s on S3/R2.
  - `replicated`: once `--replicas` nodes hold it (default 2: the leader in memory, a follower
    on local disk). That takes **~4 ms on any storage, no S3 Express needed**; the bucket gets
    the write a moment later. It survives any one node dying, but not the leader and every
    holder dying within that moment (see ADR-009).
- `--publish delta,iceberg`: new tables are also published as Delta Lake and/or Iceberg for other
  engines (default: none; per table: `"publish"`).
- `--tier-secs 2`: new rows become Parquet (and new Delta/Iceberg versions) as soon as they
  commit, at most this often. Fractions are fine (`0.25`).
- `--pg 0.0.0.0:5432`: also speak the Postgres protocol.
- `--kafka 0.0.0.0:9092`: also speak the Kafka protocol (`--kafka-advertise host:port` if clients
  must reach this node at another address than `--addr`'s host).
- `--flight 0.0.0.0:8815`: also speak Arrow Flight and Flight SQL.
- `--memory-gb 24`: memory for queries (default: a third of the machine's); sorts, aggregations
  and joins that need more spill to the temp directory. The columns kept decoded in memory
  (`PONDRA_HOT_GB`, a quarter of it by default) come out of the same budget and give way to
  queries; `PONDRA_HOT_GB=0` turns them off.
- `PONDRA_CODEC=lz4|zstd|snappy|none`: how Parquet files are compressed. LZ4 by default — it
  decodes fastest, so scans are CPU-cheap; `zstd` where storage or bandwidth costs more than CPU.
- `--read-token`, `--write-token`, `--admin-token`: access control (none set = open). Over
  Postgres the user name picks the role (`reader`, `writer`, `admin`) and the password is its
  token. Whatever the token, SQL sent to a node never touches the node's own disk (no `COPY …
  TO`, no `CREATE EXTERNAL TABLE`); only `pondra sql` reads local files, on its own machine.
- `--attach name=dir`: read (and write through its leader) another lake as `name.table`.
- `--changelog-secs 86400`: keep the log as a replayable change feed (`/watch/{t}?after=…`).
- `--fsync` (with `--ack replicated`): followers flush each copy to disk before acknowledging.
- `--cache-dir`, `--cache-gb 20`: the local SSD tier for lakes on object storage. 0 turns it off.
- `--retain-secs 60`: how long replaced files and consumed log segments are kept.
- `--backlog 10000000`: rows allowed to wait for tiering before commits pause.

## Read the lake with other engines

Tables created with `"publish": ["delta", "iceberg"]` (or on a node started with `--publish`)
are also a **Delta Lake table** and an **Iceberg table** at `<lake>/data/<table>`. Spark,
Databricks, DuckDB, Polars, delta-rs, PyIceberg, Trino and Athena read them directly:

```sql
SELECT user, sum(amount) FROM delta_scan('s3://my-bucket/lake/data/events') GROUP BY user;  -- DuckDB
SELECT count(*) FROM iceberg_scan('s3://my-bucket/lake/data/events/metadata/v42.metadata.json');
```

Other engines read the table as of the last tiering round, and read-only:

- ~30 ms after the ack on local disk;
- 3–10 s on R2 at the default `--tier-secs 2`, depending on how far away the bucket is.

Pondra's own readers (nodes, `pondra sql`) see every write sooner, and can write. The local
folder and the bucket use the same layout: see `docs/lake-format.md`.

## Windows, macOS, Linux

The code is portable Rust; nothing in it is Linux-specific. Three ways to run it on Windows:

- **WSL2** (what I'd test with first): `wsl --install`, then `cargo build --release` and run the
  Linux binary as above. This is the combination the tests were run on.
- **Native build:** install [rustup](https://rustup.rs) and the Visual Studio Build Tools (C++),
  then `cargo build --release` → `target\release\pondra.exe`. Untested so far; the one Unix-only
  piece (restart-in-place after a leader change) has a Windows path that spawns the replacement
  process instead.
- **From CI:** `.github/workflows/build.yml` builds Linux, Windows and macOS binaries on every
  push; download `pondra-windows-x86_64.exe` from the run's artifacts.

Paths on Windows work either way, but `--dir s3://bucket/lake` (R2, S3, MinIO) avoids local-path
differences entirely.

## What it does

| Need | How (HTTP API, on any node) | Replaces |
|---|---|---|
| Stream ingest, exactly-once | `POST /append/{t}?producer=&seq=` with NDJSON or an Arrow IPC stream | Kafka / Fluss |
| Tables | SQL `CREATE TABLE t (id BIGINT PRIMARY KEY, …) WITH (publish = 'delta,iceberg', cluster_by = 'user', partition_by = 'day(ts)', merge = 'total:sum', ttl = 'ts:86400')`, or `POST /tables/{t}` with the same as JSON. A key = upsert table; `merge` = merge table; `cluster_by` sorts an append table's files for fast filters; `partition_by` (a column, or year/month/day/hour of a timestamp) keeps one partition per file; `ttl` expires a keyed table's rows. Every file's column ranges are kept, and past 128 files a table's file list goes into manifests: a table of a million files commits as fast as one of ten, and queries open only the files their filters can match | Delta/Iceberg MERGE, partitioning, liquid clustering, Fluss PK tables with TTL |
| SQL writes | `INSERT … SELECT/VALUES`, `UPDATE … SET … WHERE`, `DELETE … WHERE` (keyed tables) on any node, over Postgres, or with `pondra sql` on any machine | Spark SQL DML, Fluss 1.0's UPDATE/DELETE by condition |
| Postgres protocol | `--pg`: psql, psycopg 2/3, asyncpg, SQLAlchemy + pandas (tested); JDBC/BI tools by the same protocol | a Postgres-compatible serving layer |
| Python | `import pondra`: `sql()` → pandas / Polars / Arrow, `append()` exactly-once, `watch()`, `lookup()` | PySpark / PyFlink clients for the common jobs |
| Kafka | `--kafka`: producers write to tables (a topic is a table; JSON values; `_key`/`_timestamp`/`_value` columns; idempotent producers exactly-once; gzip/snappy/lz4/zstd), Debezium change events and tombstones become upserts and deletes; consumers and consumer groups read the log (offsets = `_ord`); SASL/PLAIN with the tokens. Tested: librdkafka (confluent-kafka), kafka-python | Kafka / Fluss ingest, Debezium sinks |
| Schema evolution | `ALTER TABLE t ADD COLUMN c TYPE` (any node, Postgres, `pondra sql`); old rows read it as null; Delta and Iceberg follow | Delta/Iceberg schema evolution |
| Event-time windows | `POST /views/{v}?window=w&size_secs=60&lateness_secs=10` over `GROUP BY date_bin(…) AS w`: the view updates live; `{v}_final` gets each window once, final, when the watermark passes it | Flink tumbling windows with watermarks |
| JSON | `json_get(col, 'a', 0)`, `json_get_str/int/float/bool`, `json_contains`, `json_length`, `->`, `->>` | VARIANT / JSON functions |
| Arrow Flight | `--flight`: Flight SQL for ADBC and JDBC drivers (queries, writes, `adbc_ingest`, catalog); pyarrow `DoPut` to `[table, producer, first seq]` (exactly-once, acks as batches commit), `DoGet` with `{"sql": …}`, or a table's log as a columnar stream with only the columns asked for (`{"table": t, "after": N, "columns": [...], "follow": true}`) | Arrow Flight SQL servers (Dremio, InfluxDB 3), Fluss's columnar log |
| AI agents | `POST /mcp` (the Model Context Protocol): tools `list_tables`, `query`, `write`, `changes`, under the same tokens | an MCP server in front of the warehouse |
| Vector search | `FLOAT[]` embedding columns (`Float32[]`, published as a Delta `array` and an Iceberg `list`); `ORDER BY cosine_similarity(emb, [...]) DESC LIMIT k` (also `l2_distance`, `dot_product`, `cosine_distance`, `inner_product`, `array_distance`), exact, over the log and the files; Postgres array parameters work | a vector database next to the lake; Flink `VECTOR_SEARCH` |
| Files (images, PDFs, audio) | `PUT /files/<path>` and `GET /files/<path>` put objects in the lake next to the tables; `SELECT * FROM files('photos/')` lists them (path, size, written); `file_read(path)` reads one where a query needs it. `BINARY` columns hold bytes, with `byte_length`, `sha256`, `md5`, `encode(…, 'base64')`, `decode`, `substr` | Databricks file types, Hudi blobs, a blob store beside the warehouse |
| Semi-structured | `VARIANT` columns (JSON text): `json_get(col, 'a', 0)`, `json_get_str/int/float/bool`, `json_contains`, `json_length`, `->`, `->>` | VARIANT / JSON functions |
| Models in SQL | `ai_complete(prompt [, model])` and `ai_embed(text [, model])` call an OpenAI-compatible endpoint (`PONDRA_AI_URL`: your own vLLM or Ollama, or a hosted one), eight rows in flight, a failed row null | Databricks `ai_query` / `ai_forecast`, Snowflake Cortex |
| Your own functions | `POST /functions/{name}` `{"flight": "http://host:port", "args": ["Binary"], "returns": "Utf8"}`: an Arrow Flight server of yours gets the rows as one Arrow batch and returns one column, so a model, a GPU or any Python library runs in that process and not in the node (`tools/udf_server.py` is one in forty lines) | Python/Pandas UDFs, Databricks model serving, Daft UDFs |
| Streaming SQL with no lag | `POST /views/{name}` with SQL. Runs on every flush of new rows, commits with them. With GROUP BY it keeps per-key aggregates (sum/count/min/max) that any number of nodes update at once | Flink SQL jobs + keyed state |
| General stateful streaming | `POST /tasks/{name}` `{"source","target","sql"[, "key","shards","shard_by"]}`: runs as soon as rows commit, exactly-once, shards spread over nodes | Flink jobs |
| Push and change feeds | `GET /watch/{t}`: new rows as NDJSON the moment they commit (upserts and deletes of keyed tables included); `?after=N` replays from N, as far back as `--changelog-secs` keeps the log | Kafka consumers, Fluss `$changelog` |
| SQL | `POST /sql[?format=json\|table\|arrow][&after=<seg>][&stale_ms=N]`: files ∪ log tail, one snapshot. Large tables run SPMD across all nodes, with shuffles for many-group aggregations and big joins (`&spread=1` forces, `0` disables). Queries beyond `--memory-gb` spill. Repeated queries are answered from a result cache until the next commit (`stale_ms`: accept one up to N ms old) | Trino / Spark SQL / Databricks SQL |
| Metrics | `GET /metrics` (Prometheus): rows in, queries and their time, spread and shuffled queries, files scanned and skipped, memory, commit latency, per-table files, rows and bytes | a metrics exporter |
| Serving reads | `GET /lookup/{t}/{key}` (or SQL `SELECT … WHERE key = …`): the current row of one key without SQL planning — log tail, then the files newest-first, each narrowed to one cached, key-sorted row group: ~0.2 ms, ~20k/s on two cores | Redis / Postgres / Lakehouse//RT in front of the lake |
| Batch ELT, exactly-once | `POST /insert/{t}?job=` with a `SELECT` (the receiving node does the work), or `pondra sql "INSERT INTO t SELECT …"` from any machine: straight to Parquet; a retried job is a no-op | Spark batch jobs |
| Maintenance | automatic and spread over the nodes: tiering to Parquet, compaction, retention, orphan cleanup, backpressure | Spark OPTIMIZE / VACUUM |
| Open formats | tables that ask are published as Delta Lake (`data/{t}/_delta_log`) and Iceberg (`data/{t}/metadata`) each tiering round, for engines that don't know Pondra; an Iceberg REST catalog (`/v1/…`) lets them attach by URL | a separate Delta/Iceberg writer and catalog |

## How it works

| File | Role |
|---|---|
| `log.rs` | Every node batches its writes (Arrow IPC + ZSTD) and runs the views on them; big flushes it writes to storage itself. The leader's sequencer only orders them: dedupes producer retries and commits every flush as a log segment in one catalog write, pipelined |
| `store.rs` | The lake: object store + catalog (SlateDB, inside the bucket). The leader commits in order and streams every change and commit to the other nodes. They keep the whole catalog in memory from it (seeded from their own view; after a gap they fall back to the view, checked before and after every read, so a read never goes back in time): every node sees a commit within milliseconds, without asking the bucket |
| `replica.rs` | `--ack replicated`: followers keep the changes the bucket doesn't have yet in local files and acknowledge them; a new leader collects and re-commits them before taking writes |
| `cluster.rs` | Leader election through the bucket (put-if-absent `cluster/term/{n}`), HTTP heartbeats, takeover after 5 s if no peer still hears the leader; a replaced leader is fenced by the catalog and rejoins. A liveness mark in the bucket lets a node on an idle lake lead at once |
| `views.rs` | Inline views; GROUP BY views become merge tables |
| `tasks.rs` | Streaming tasks: output + progress commit together, only if progress is unchanged (compare-and-swap) |
| `spmd.rs` | Distributed queries: every node runs the same plan over its slice; small tables are read whole (broadcast). Up to the first gather, or through shuffles: each hash exchange becomes a step in which every node splits its output by hash, one bucket per node, and fetches its own bucket from every node. The receiving node finishes the plan |
| `manifest.rs` | Table metadata that stays small: per-file column ranges, the oldest files sealed into immutable manifests behind one list object, pruning of manifests and files by a query's filters |
| `flight.rs` | Arrow Flight and Flight SQL: exactly-once `DoPut`, SQL and the log as columnar streams, ADBC's statements, ingest and catalog |
| `metrics.rs` | `GET /metrics` in Prometheus' format |
| `tier.rs` | Tiering, merging small files and compaction: the leader decides and commits, the data work is dealt to the nodes as jobs. Keyed tables are LSM-like — each round folds the log tail into a new file, and files are compacted once 8 pile up. Retention and orphan cleanup |
| `query.rs` | Hot+cold snapshot per query (DataFusion); strings are read as views |
| `hot.rs` | The columns queries read lately, decoded, in memory, per file: a scan that finds them all there skips reading and decoding Parquet. Files never change, so nothing goes stale. Filled in the background, only for a file a second scan came back to, and never at the expense of a running query |
| `optimize.rs` | The engine settings Pondra starts from, and four planning rules of its own: a semi join runs on the table it filters, a grouped subquery groups only the keys the join keeps, a filter's conditions run cheapest first, and the few groups a HAVING keeps make the hash table |
| `files.rs`, `ai.rs`, `udf.rs` | Files in the lake (`files('…')`, `file_read`), models in SQL (`ai_complete`, `ai_embed`) and vector maths, and functions of your own on an Arrow Flight server |
| `cache.rs` | For lakes on object storage: an in-memory read cache and a local SSD tier (write-through, read-through, prefetched from the commit stream, warmed at start) |
| `serve.rs` | Serving reads: key lookups without SQL (tail, then files newest-first, cached key-sorted row groups, binary search), and SQL point queries routed to them |
| `delta.rs`, `iceberg.rs` | Open formats, per table: a Delta JSON commit / an Iceberg v2 snapshot (hand-written Avro manifests) per change to a table's files; crash-safe (derived from durable catalog state, put-if-absent); the Iceberg REST catalog |
| `write.rs` | Writes in SQL from anywhere (CREATE TABLE, INSERT, UPDATE, DELETE): the work runs where the statement runs; the leader records it — over HTTP, through the bucket inbox, or the statement leads for a moment when nobody does. Attached lakes' writes go to their own leaders |
| `inbox.rs` | The bucket inbox: writers that can't reach the leader leave requests in the bucket; the leader answers them |
| `pg.rs` | The Postgres wire protocol (queries and writes, text and binary results, typed `$1` parameters, a small `pg_catalog`) |
| `auth.rs` | Read / write / admin tokens, over HTTP, Postgres and MCP |
| `mcp.rs` | MCP for AI agents: JSON-RPC over HTTP, four tools |
| `kafka.rs` | The Kafka protocol: produce (record batches → rows, exactly-once), fetch, offsets, consumer groups, SASL/PLAIN |
| `server.rs`, `main.rs` | HTTP API (axum) and CLI |

**Producer contract:** each producer has its own name, sends batches in order with increasing
`seq`, one request in flight, to any node, retrying (on any node) until acknowledged. Retries of
committed batches come back as `"duplicate": true`.

## Tests

```bash
python3 tools/harness.py all [--s3]             # upsert, fence (split brain), insert, serverless, clients, reader, crash, load
python3 tools/harness.py clients                # SQL writes, Python client, Postgres drivers, tokens, inbox, attached lakes, vectors, MCP
python3 tools/mcp_client.py --url http://127.0.0.1:8080/mcp   # the official MCP SDK against a node (pip install mcp)
python3 tools/harness.py kafka | alter | windows   # Kafka clients, ALTER TABLE under load, windows emitted once
python3 tools/harness.py scale | flight         # partitions, manifests, shuffles, memory limits; Arrow Flight + ADBC
python3 tools/metadata_bench.py [--files 1000000]   # a table with a million files: commits, pruning, 3 nodes
python3 tools/flight_bench.py                   # Arrow Flight in, out, and the log as a stream
python3 tools/cloud/bench.py --nodes …          # a cluster on several machines (tools/cloud/README.md)
python3 tools/kafka_bench.py                    # Kafka ingest throughput and latency on 3 nodes
python3 tools/keyed_bench.py                    # keyed-table compaction: bytes written, correctness
python3 tools/harness.py crash --runs 20        # kill -9 + injected crashes (PONDRA_CRASH=point:prob)
python3 tools/cluster.py users                  # 64 writers + 16 readers + serverless reads, 3 nodes
python3 tools/cluster.py failover               # views + sharded task state; leader killed twice
python3 tools/cluster.py latency [--load 4]     # event -> view row pushed to another node (add --flag ack=replicated)
python3 tools/cluster.py spread                 # distributed queries == single-node results
python3 tools/cluster.py race | isolate | split # elections, cut-off follower, where the CPU goes
python3 tools/bench/run.py batch 20000000       # vs Spark and Flink (ENGINES=pondra,spark,flink)
python3 tools/serve_bench.py --keys 2000000     # point lookups and dashboard queries, p50/p99/QPS (uses tools/loadgen.go if Go is installed)
python3 tools/bench/tpch.py --data sf1          # TPC-H (tpchgen-cli) on Pondra, DuckDB and Spark
python3 tools/sizes.py                          # storage bytes per event
python3 tools/open_check.py [--s3]              # 8 readers (Delta: delta-rs/Polars/DuckDB; Iceberg: PyIceberg/Polars/DuckDB, and both through the REST catalog) == Pondra
python3 tools/freshness.py [--s3] [--flag ack=replicated]  # head to head: nodes, pondra sql, Delta, Iceberg
python3 tools/clustering.py                     # what cluster_by buys
python3 tools/newuser_bench.py [--s3]           # a new client's first query, a new node's, write→visible
python3 tools/demo_lake.py --dir <folder|s3://…> # one of everything, then the folder tree
tools/r2_test.sh                                # the main tests against a real bucket
python3 tools/sim_r2.py --port 9000             # local S3 server with R2-like latency (moto)
```

`--s3` uses `s3://$PONDRA_BUCKET/$PONDRA_TEST_PREFIX` + `test-…` with the `AWS_*` variables. Add
`--flag tier-secs=10` to `cluster.py` to pass a serve flag to every node. Test runs delete their
lakes when they finish (`--keep` or `PONDRA_KEEP=1` keeps them); `tools/clean_bucket.py` trims a
bucket to its newest lakes.

## Not yet

- Shuffle buckets and big results are held in memory (not streamed or spilled); distributed
  queries are one SELECT with inner joins. Nothing has run on several machines yet
  (`tools/cloud/` is the kit).
- Publishing a huge table to Delta/Iceberg rewrites a manifest of every file each time.
- Clustering across files; copy-on-write DELETE for append tables.
- Per-table grants, quotas and TLS (tokens are per role; put a TLS proxy in front); JDBC and BI
  tools untested here.
- Kafka: one partition per topic, no transactions; offsets are positions in the log (increasing,
  not dense). Consumer groups live in the leader's memory (members rejoin after a failover).
- `ALTER TABLE` only adds columns; session windows; AI functions in SQL; an approximate vector
  index (see the plan in `docs/comparison-spark-flink-fluss.md`).
- On object storage a *durable* ack costs one PUT; `--ack replicated` trades a small window
  (the leader and every holder dying before that PUT) for milliseconds.
- A one-off `pondra sql` on far-away object storage spends 1–3 s opening the catalog; join
  (`pondra serve --reader`) for millisecond reads.
