# Pondra: a streamhouse in one binary

One Rust binary (~4,550 lines) that ingests streams, stores them as a lakehouse (Parquet files
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
| Tables | `POST /tables/{t}` `[["user","Utf8"],…]`; `{"columns":[…],"key":["id"]}` = upsert table; add `"merge":{"total":"sum"}` = merge table; `"cluster_by":["user"]` sorts an append table's files for fast filters; `"publish":["delta","iceberg"]` | Delta/Iceberg MERGE, liquid clustering |
| Streaming SQL with no lag | `POST /views/{name}` with SQL. Runs on every flush of new rows, commits with them. With GROUP BY it keeps per-key aggregates (sum/count/min/max) that any number of nodes update at once | Flink SQL jobs + keyed state |
| General stateful streaming | `POST /tasks/{name}` `{"source","target","sql"[, "key","shards","shard_by"]}`: runs as soon as rows commit, exactly-once, shards spread over nodes | Flink jobs |
| Push | `GET /watch/{t}`: new rows as NDJSON the moment they commit | Kafka consumers |
| SQL | `POST /sql[?format=json\|table\|arrow][&after=<seg>][&stale_ms=N]`: files ∪ log tail, one snapshot. Large tables run SPMD across all nodes (`&spread=1` forces, `0` disables). Repeated queries are answered from a result cache until the next commit (`stale_ms`: accept one up to N ms old) | Trino / Spark SQL / Databricks SQL |
| Serving reads | `GET /lookup/{t}/{key}` (or SQL `SELECT … WHERE key = …`): the current row of one key without SQL planning — log tail, then the files newest-first, each narrowed to one cached, key-sorted row group: ~0.2 ms, ~20k/s on two cores | Redis / Postgres / Lakehouse//RT in front of the lake |
| Batch ELT, exactly-once | `POST /insert/{t}?job=` with a `SELECT` (the receiving node does the work), or `pondra sql "INSERT INTO t SELECT …"` from any machine: straight to Parquet; a retried job is a no-op | Spark batch jobs |
| Maintenance | automatic and spread over the nodes: tiering to Parquet, compaction, retention, orphan cleanup, backpressure | Spark OPTIMIZE / VACUUM |
| Open formats | tables that ask are published as Delta Lake (`data/{t}/_delta_log`) and Iceberg (`data/{t}/metadata`) each tiering round, for engines that don't know Pondra | a separate Delta/Iceberg writer |

## How it works

| File | Role |
|---|---|
| `log.rs` | Every node batches its writes (Arrow IPC + ZSTD) and runs the views on them; big flushes it writes to storage itself. The leader's sequencer only orders them: dedupes producer retries and commits every flush as a log segment in one catalog write, pipelined |
| `store.rs` | The lake: object store + catalog (SlateDB, inside the bucket). The leader commits in order and streams every change and commit to the other nodes. They keep the whole catalog in memory from it (seeded from their own view; after a gap they fall back to the view, checked before and after every read, so a read never goes back in time): every node sees a commit within milliseconds, without asking the bucket |
| `replica.rs` | `--ack replicated`: followers keep the changes the bucket doesn't have yet in local files and acknowledge them; a new leader collects and re-commits them before taking writes |
| `cluster.rs` | Leader election through the bucket (put-if-absent `cluster/term/{n}`), HTTP heartbeats, takeover after 5 s if no peer still hears the leader; a replaced leader is fenced by the catalog and rejoins. A liveness mark in the bucket lets a node on an idle lake lead at once |
| `views.rs` | Inline views; GROUP BY views become merge tables |
| `tasks.rs` | Streaming tasks: output + progress commit together, only if progress is unchanged (compare-and-swap) |
| `spmd.rs` | Distributed queries: every node runs the same plan over its slice up to the first exchange; the receiving node finishes it |
| `tier.rs` | Tiering, merging small files and compaction: the leader decides and commits, the data work is dealt to the nodes as jobs. Keyed tables are LSM-like — each round folds the log tail into a new file, and files are compacted once 8 pile up. Retention and orphan cleanup |
| `query.rs` | Hot+cold snapshot per query (DataFusion) |
| `cache.rs` | For lakes on object storage: an in-memory read cache and a local SSD tier (write-through, read-through, prefetched from the commit stream, warmed at start) |
| `serve.rs` | Serving reads: key lookups without SQL (tail, then files newest-first, cached key-sorted row groups, binary search), and SQL point queries routed to them |
| `delta.rs`, `iceberg.rs` | Open formats, per table: a Delta JSON commit / an Iceberg v2 snapshot (hand-written Avro manifests) per change to a table's files; crash-safe (derived from durable catalog state, put-if-absent) |
| `insert.rs` | Bulk INSERT from any node or any machine: the work runs where the statement runs; the leader (or the statement itself, when nobody leads) records the files |
| `server.rs`, `main.rs` | HTTP API (axum) and CLI |

**Producer contract:** each producer has its own name, sends batches in order with increasing
`seq`, one request in flight, to any node, retrying (on any node) until acknowledged. Retries of
committed batches come back as `"duplicate": true`.

## Tests

```bash
python3 tools/harness.py all [--s3]             # upsert, fence (split brain), insert, serverless, reader, crash, load
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
python3 tools/open_check.py [--s3]              # 6 readers (Delta: delta-rs/Polars/DuckDB; Iceberg: PyIceberg/Polars/DuckDB) == Pondra
python3 tools/freshness.py [--s3] [--flag ack=replicated]  # head to head: nodes, pondra sql, Delta, Iceberg
python3 tools/clustering.py                     # what cluster_by buys
python3 tools/newuser_bench.py [--s3]           # a new client's first query, a new node's, write→visible
python3 tools/demo_lake.py --dir <folder|s3://…> # one of everything, then the folder tree
tools/r2_test.sh                                # the main tests against a real bucket
python3 tools/sim_r2.py --port 9000             # local S3 server with R2-like latency (moto)
```

`--s3` uses `s3://$PONDRA_BUCKET/$PONDRA_TEST_PREFIX` + `test-…` with the `AWS_*` variables. Add
`--flag tier-secs=10` to `cluster.py` to pass a serve flag to every node.

## Not yet

- Shuffles in distributed queries: big-to-big joins run on one node.
- Partitioned tables; compaction of big keyed tables without a full rewrite; clustering across
  files.
- Auth and quotas.
- On object storage a *durable* ack costs one PUT; `--ack replicated` trades a small window
  (the leader and every holder dying before that PUT) for milliseconds.
- A one-off `pondra sql` on far-away object storage spends 1–3 s opening the catalog; join
  (`pondra serve --reader`) for millisecond reads.
