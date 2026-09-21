# AGENTS.md — working on Pondra

Read this first, then `README.md` (what it does) and `docs/adr-005-every-node-writes.md` (why it
works this way). `docs/prototype-status.md` has the measured numbers and what's left.

## What this is

**Pondra** is one Rust binary that is a streaming store, a lakehouse and a SQL engine at once.
Object storage (a local directory, S3, R2, MinIO) holds *all* the state: there is no Postgres, no
ZooKeeper, no Kafka, no JVM. Start the same binary on several machines pointed at the same bucket
and they form a cluster. Goal: replace Kafka + Flink + Spark + a metastore for the common jobs,
and compete with Databricks / Snowflake / Fluss on simplicity and cost.

The owner's design principles, which every change must respect:

1. **One small binary that runs anywhere**, DuckDB-like, and joins a cluster with almost no setup.
2. **As serverless as possible**: no always-on services besides the nodes themselves.
3. **SPMD, not driver/executor** (Bodo-style): every node runs the same code on its slice.
4. **No JVM, no Spark, no Flink, no Fluss needed.**
5. **Short, simple, readable code** — without losing functionality. ~3,650 lines of Rust total.
   If a change makes a file much longer, look for the simpler shape first.

## Layout

```
src/      3,650 lines of Rust, one file per concern (see the table in README.md)
tools/    harness.py, cluster.py (tests), delta_check.py (outside readers == Pondra),
          newuser_bench.py (first reads, new nodes), demo_lake.py (one of everything + the tree),
          serve_bench.py + loadgen.go (serving), bench/tpch.py (TPC-H vs DuckDB and Spark),
          sizes.py, sim_r2.py (local S3 with R2 latency),
          r2_test.sh (run the suite against a real bucket), bench/ (vs Spark and Flink)
docs/     ADRs and reports; lake-format.md is the on-disk layout
```

## The model in one page

- **Tables** are Parquet files in the bucket plus a **log tail**. Every query reads files ∪ tail,
  so data is queryable the moment it commits.
- **The catalog** is a SlateDB key-value store inside the same bucket: `t/` tables, `s/` segments,
  `d/` inline segment data, `p/` producer progress, `v/` views, `k/` tasks, `x/` Delta publish
  state, `n` next segment, `c` commit number. One process (the leader) writes it; everyone reads it.
- **Writes:** a client POSTs a batch to *any* node. That node encodes it (Arrow IPC + ZSTD), runs
  the inline views on it, writes it to the bucket if it's over 64 KB, and asks the leader to
  sequence it. The leader dedupes `(producer, seq)`, numbers the segments and commits — one
  catalog write for every node's flush in that round. It never touches the data itself.
- **Exactly-once:** producers send `(producer, seq)` in order, one request in flight, retrying on
  any node. Retries of committed batches come back `"duplicate": true`. Streaming tasks use the
  same mechanism with a compare-and-swap (`prev`), so output and progress commit together.
- **Followers** get every durable commit streamed over `GET /cluster/log`. They seed an in-memory
  copy of the whole catalog from their own view and keep it current from the stream (the
  "mirror"), so they see a commit within milliseconds and never ask the bucket for metadata.
  After a gap in the stream they fall back to their view plus the streamed commits (the ADR-005
  rules) until they can seed again.
- **Open lake:** every tiering round (default every 2 s) also writes each table's Delta Lake log
  (`data/{table}/_delta_log/`, `src/delta.rs`), so other engines read the lake without Pondra.
- **SSD tier** (lakes on object storage): each node keeps immutable objects on local disk —
  written through, read through, prefetched from the commit stream, warmed at start (`cache.rs`).
- **Leader election** is a put-if-absent object `cluster/term/{n}`; SlateDB fencing stops an old
  leader from writing. HTTP heartbeats decide liveness and who runs which task shard.
- **Maintenance** (log → Parquet, merging small files, compaction, retention) is decided by the
  leader and dealt out to all nodes as jobs.

## Invariants — break these and data goes missing

1. **A read never goes back in time.** A follower combines its catalog view with streamed commits
   only when it can prove the result is a prefix of the lake: it reads its view's commit number
   before *and* after the data, decides under the overlay lock, and pins what it needs so pruning
   can't drop it mid-read (`Catalog::scan`, `refresh`, `apply` in `src/store.rs`).
2. **"How far can I read" comes from the same view as the read** (`Lake::visible`), never from the
   `hwm` watch (which only wakes readers and may be ahead on a follower). A task that reads
   `(done, hwm]` while its view can't see all of it would commit progress past rows it never read.
   On the leader `visible()` is the durable high-water mark, *not* `last_n`, which counts
   in-flight commits.
3. **Only durable data is visible.** Leader reads use SlateDB's `DurabilityLevel::Remote`;
   followers only ever see commits the leader already made durable.
4. **A job names its inputs.** Tiering jobs carry the file list and segment range from the leader
   and refuse to run until the node can see the last segment; otherwise a lagging node would write
   an incomplete file that the leader then commits.
5. **A keyed table's files are versions, not a set.** Each file carries `ord`, the last log segment
   it covers, and a row in a higher-`ord` file is a newer version of its key (`_ord` in a read is
   `ord << 32` for file rows, `(segment << 32) + position` for log rows). Two rules follow: a file
   written by a fold keeps delete markers (they shadow older files; only a full compaction drops
   them), and two files of the same `ord` must never cover the same segments.
6. **Segments and files are immutable.** Nothing is ever overwritten (`PutMode::Create`);
   replaced files become `garbage` and are deleted after the retention period.
7. **Expire only what everyone has consumed**, using the floor as of `retain_secs` ago, so a query
   that started earlier still finds its segments.
8. **While the mirror is on, commits reach it only through the stream.** Its reads never consult
   the view, so the view must not mark commits as "already seen" (`pruned`) — that skipped them
   and lost rows (round 6). Gaps are judged against what the mirror holds (`streamed`), not the
   view. Seeding is one attempt per refresh, off the startup path — never a retry loop (a busy
   lake on slow storage kept one from ever finishing, so a restarted node never came up).
9. **A tiering job checks the leader's row count** for its log range before it writes anything
   (`caught_up` in `tier.rs`). A node whose catalog disagrees refuses the job; the next round
   retries. This turns any future "a node saw less than the leader" bug into a retry, not a loss.
10. **The Delta log is derived, never authoritative.** A Delta commit is computed from committed
   catalog state only and written put-if-absent; an unrecorded one found later is adopted (which
   is also why the catalog write recording it isn't awaited). Only `_last_checkpoint` is ever
   overwritten, and nothing in `_delta_log/` goes through the SSD tier.
11. **Catalog memtable flushes are rationed:** one loop, every 5 s, only if something committed,
   plus one when a leader takes over (followers' views read no WAL and need it). Each flush is a
   level-0 file; flushing on every tiering call stalled writes for 9 s at a time (round 6).
12. **A tiering round is: fold + commit, publish, then maintain.** Merges and compactions come
   after the fresh rows are committed and published, in their own commit. A table with no new
   rows is skipped (no empty commit); `expire` moves its `tiered` mark along every 10 s — don't
   write code that assumes `tiered` advances every round.
13. **Every keyed file holds one row per key** (folds and compactions both write that way). Upsert
   reads rely on it: they anti-join each file against the keys of newer sources instead of
   grouping every row by key (`register_upsert` in `query.rs`), and `/lookup` stops at the first
   file that has the key. A writer that breaks it (say, appending raw rows to a keyed file)
   makes reads return duplicates.
14. **A cached result is valid for exactly one catalog version** (`Catalog::version`), which only
   exists where every read reflects exactly that version: the leader (last durable commit) and
   nodes with the in-memory catalog (last streamed commit). Identical queries in flight share one
   computation that covers only requests which arrived before it started. `stale_ms` is the one,
   opt-in, exception.

## Tests: run these before and after any change

```bash
cargo build --release
python3 tools/harness.py all            # upsert, fence/split-brain, bulk insert, reader, crash, load
python3 tools/harness.py crash --runs 3 --batches 60 --size 50000   # kill -9 + injected crashes, 9M events
python3 tools/cluster.py users --secs 30      # 64 writers + 16 readers: 0 torn reads, 0 lost
python3 tools/cluster.py failover --secs 45   # 2 leader kills; task state == inline view == model
python3 tools/cluster.py latency [--load 4]   # event -> view row on another node
python3 tools/cluster.py race | isolate | split | spread
python3 tools/bench/run.py batch 20000000     # ENGINES=pondra,spark,flink
python3 tools/serve_bench.py --keys 2000000   # serving: point lookups and dashboard queries
```

Add `--s3` to any of them with a simulated-R2 bucket to see the object-storage behaviour:

```bash
python3 tools/sim_r2.py --port 9000 &   # moto + R2-like latency (PUT p50 197 ms, GET p50 100 ms)
export AWS_ENDPOINT=http://127.0.0.1:9000 AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
       AWS_REGION=auto AWS_ALLOW_HTTP=true PONDRA_BUCKET=testbucket
python3 tools/harness.py crash --runs 3 --batches 150 --s3
```

**A tiering failure is silent in the correctness tests** — reads stay correct, the log just stops
draining — so it shows up as a throughput drop in `tools/bench/run.py live` (and as
`background job failed:` on the leader's stderr), not as a test failure. `harness.py tiering`
checks the log drains and the file count stays bounded; watch the live benchmark for the rest.

**`failover` and `users` are the tests that catch read-consistency bugs.** `failover` fails about
1 run in 8 when something is wrong — run it 15–20 times before believing a fix. `users` caught the
round-6 mirror bug in 4 of 5 runs; run it at least 5 times after touching `store.rs`.
`crash --size 50000` is the one that catches "the leader can see its own in-flight writes" bugs.
`delta_check.py --rounds 1250` is the one that catches slow tiering rounds (and Delta log cleanup).

Practical notes for an agent working here:

- Never rebuild the binary while a test suite is running (tests exec `argv[0]` when a node restarts).
- Kill leftover nodes with `pgrep -x pondra` (never `pkill -f` or `pgrep -f <script name>`: it
  matches your own shell) and clean `/tmp/pondra-*/` afterwards, or the disk fills up. The SSD
  tier's default folder `/tmp/pondra-cache/` goes with it.
- Node stderr goes to `/tmp/pondra-<port>-<id>.stderr`; that's where "restarting to rejoin",
  "slow tiering" and panics show up.

## State of the work (2026-09-21, round 7)

Everything in `docs/prototype-status.md` passes on local disk, on simulated R2 and on a real
Cloudflare R2 bucket.

Headline numbers, all on one 2-vCPU box:

- **Durable ingest:** 4.3–4.6 M events/s through a 3-node cluster.
- **TPC-H SF1:** all 22 queries in 5.9 s (Spark 4.2: 58–65 s).
- **Write → aggregated row on another node:** 5 ms on local disk; one PUT on object storage.
- **An open lake:** every table is also a Delta Lake table, readable 17 ms after the ack on
  local disk and ~5 s on R2 from here.
- **Serving:** 0.14 ms key lookups and 20–36k lookups/s; repeated dashboards from a
  version-exact result cache at ~20k/s.
- **Consistency:** 0 torn reads and 0 lost batches with 64 concurrent writers, and clean
  failover runs.

The honest comparison with Spark, Flink, Fluss and Lakehouse//RT, with the plan for the gaps,
is `docs/comparison-spark-flink-fluss.md`.

Known limits, in the order they matter:

1. **Durable acks cost one object-store write** (0.3–0.7 s on R2 from this sandbox). Fluss and
   Databricks RTM get milliseconds by replicating to disks/memory first. S3 Express One Zone
   would close most of the gap and is untested here.
2. **One sequencer per lake** orders commits (metadata only). Several lakes past that.
3. **No shuffles** in distributed queries: big-to-big joins run on one node.
4. **Keyed-table compaction is one job on one node**; partitioned compaction is next. Other
   engines see keyed tables as of their last compaction.
5. **No auth, quotas or multi-tenancy.** Delta only (no Iceberg metadata), unpartitioned.

Good next moves, in order:

1. Shuffles reusing the job-dealing mechanism, plus a multi-machine TPC-H run against Spark.
2. Event-time windows and watermarks.
3. Arrow Flight SQL or the Postgres wire protocol.
4. Kafka-protocol ingest and CDC.
5. Iceberg metadata beside the Delta log.
6. Partitioned tables.
7. Auth.

The plan table at the end of the comparison doc has the evidence each should produce.

## Conventions

- Comments explain *why*, in plain English; the code shows *what*. Keep functions small.
- No new dependencies without a real reason; no new always-on services, ever.
- Every new invariant gets a test in `tools/` that would fail without it.
- Docs live in `docs/`; a design change means a new ADR, not an edit to an old one.
