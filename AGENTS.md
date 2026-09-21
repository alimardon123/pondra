# AGENTS.md — working on Pondra

Read this first, then `README.md` (what it does), `docs/adr-005-every-node-writes.md` (why it
works this way) and `docs/adr-010-anywhere.md` (the current round). `docs/prototype-status.md`
has the measured numbers and what's left.

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
5. **Short, simple, readable code** — without losing functionality. ~5,800 lines of Rust total.
   If a change makes a file much longer, look for the simpler shape first.

## Layout

```
src/      5,800 lines of Rust, one file per concern (see the table in README.md)
python/   the Python client (pure Python, HTTP + Arrow)
tools/    harness.py, cluster.py (tests), open_check.py (Delta + Iceberg readers == Pondra),
          keyed_bench.py (compaction cost), clean_bucket.py (keep a bucket to its newest lakes),
          freshness.py (head-to-head freshness), clustering.py (what cluster_by buys),
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
  `d/` inline segment data, `p/` producer progress, `v/` views, `k/` tasks, `x/` Delta and `i/`
  Iceberg publish state, `m` members (replicated acks), `n` next segment, `c` commit number. One
  process (the leader) writes it; everyone reads it.
- **Writes:** a client POSTs a batch to *any* node. That node encodes it (Arrow IPC + ZSTD), runs
  the inline views on it, writes it to the bucket if it's over 64 KB (1 MB with replicated
  acks), and asks the leader to sequence it. The leader dedupes `(producer, seq)`, numbers the
  segments and commits — one catalog write for every node's flush in that round. It never
  touches the data itself.
- **Committed** means acknowledged and visible. By default (`--ack durable`) a write commits
  once it's in the bucket. With `--ack replicated` it commits once `--replicas` nodes hold it:
  the leader in memory, followers in local replica files (`replica.rs`). The bucket gets it a
  moment later either way.
- **Exactly-once:** producers send `(producer, seq)` in order, one request in flight, retrying on
  any node. Retries of committed batches come back `"duplicate": true`. Streaming tasks use the
  same mechanism with a compare-and-swap (`prev`), so output and progress commit together.
- **Followers** get every change and commit streamed over `GET /cluster/log` (frames: `Start`,
  `Change`, `Committed`, `Durable`; a change takes effect at its `Committed`). They seed an in-memory
  copy of the whole catalog from their own view and keep it current from the stream (the
  "mirror"), so they see a commit within milliseconds and never ask the bucket for metadata.
  After a gap in the stream they fall back to their view plus the streamed commits (the ADR-005
  rules) until they can seed again.
- **Native first, open formats on request:** Pondra's readers use the catalog directly. Tables
  with `publish` get a Delta log (`data/{table}/_delta_log/`, `delta.rs`) and/or Iceberg metadata
  (`data/{table}/metadata/`, `iceberg.rs`) every tiering round, for engines that don't know
  Pondra.
- **Writes from anywhere** (`write.rs`): `CREATE TABLE`, `INSERT`, `UPDATE`, `DELETE` in SQL on
  any node, over Postgres (`pg.rs`) or from `pondra sql` on any machine. The work runs where the
  statement runs; the leader records it — over HTTP, through the bucket inbox (`inbox.rs`) when
  it can't be reached, or the statement leads for a moment itself when nobody does.
- **Attached lakes** (`--attach name=dir`): other lakes read as `name.table`; writes to them are
  recorded by their own leaders. Several clusters share one bucket this way.
- **Tokens** (`auth.rs`): read / write / admin; none set = open. The same tokens guard HTTP,
  Postgres and MCP (`mcp.rs`, `POST /mcp`: tools `list_tables`, `query`, `write`, `changes`).
- **SSD tier** (lakes on object storage): each node keeps immutable objects on local disk —
  written through, read through, prefetched from the commit stream, warmed at start (`cache.rs`).
- **Leader election** is a put-if-absent object `cluster/term/{n}`; SlateDB fencing stops an old
  leader from writing. HTTP heartbeats decide liveness and who runs which task shard. The leader
  also rewrites `cluster/alive/{n}` every 10 s, so machines outside the cluster can tell a live
  leader from a dead one.
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
   On the leader `visible()` is the committed high-water mark, *not* `last_n`, which counts
   in-flight commits.
3. **Only committed data is visible.** The leader reads its in-memory catalog, which only ever
   holds committed writes (applied in `Lake::commits`), never ones in flight. Followers apply a
   change only at the `Committed` frame that covers it. Committed = durable by default; with
   `--ack replicated`, held by `replicas − 1` member followers or durable (invariant 15).
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
   exists where every read reflects exactly that version: the leader (last committed write) and
   nodes with the in-memory catalog (last streamed commit). Identical queries in flight share one
   computation that covers only requests which arrived before it started. `stale_ms` is the one,
   opt-in, exception.
15. **Replicated commits** (`--ack replicated`, `replica.rs`). Break one of these and acked writes
   vanish in a failover:
   - A follower acks only a run of *consecutive* changes it holds, and never for a term older
     than the newest it has heard of (a new leader's `/cluster/replica` fetch raises that mark).
   - Only members listed in the catalog (`m`), durably, count. A follower is listed before it
     counts; it stops counting, and everything is made durable, before it leaves the list.
   - A new leader recovers before it takes writes (`replica::recover`, before `Sequencer::start`
     and before its HTTP server): it asks every member (20 s), re-commits the longest chain
     (terms never going down), and waits until it's durable.
   - At most `AHEAD` (256) commits are acknowledged ahead of the bucket; past that, acks wait for
     it (a hung PUT on real R2 once let 279 pile up).
   - `failover --flag ack=replicated` on simulated R2 is the test that exercises recovery: its
     leaders die with commits the bucket doesn't have yet.
16. **Only durable state leaves the catalog.** Delta/Iceberg publishing and deleting objects
   (retention) first wait for everything committed so far to be durable
   (`Catalog::wait_durable`). With replicated acks, a commit that recovery can't find must never
   have reached another engine or deleted a file.
17. **Nobody deposes a live leader from outside the cluster.** A node joining, or a `pondra sql`
   INSERT, claims a new term only if the latest term's `cluster/alive` mark is over 30 s old. A
   follower that has never reached its leader (`Cluster::heard`) waits for that too; only
   members that lost a leader they were talking to use the 5 s lease.
   A one-off writer claims with an empty address, keeps its mark fresh while it works, and
   deletes it when done. Nodes and other writers wait for it; they never follow it.
18. **The inbox is just another door to the leader.** Its requests carry the same exactly-once
   keys as HTTP (a bulk INSERT's job id, a flush's `(producer, seq)`), and only a process that
   leads answers them (`inbox::drain`, from the leader loop or a one-off writer that leads). A
   writer that gives up waiting withdraws its request first; a late answer is then a duplicate.
19. **Keyed compaction merges consecutive runs only** (`tier::run`). Files are versions in `ord`
   order; merging around a file would let an older version overtake a newer one. A partial
   merge (`Squash`) keeps delete markers and expired rows; only a full compaction (the run
   reaches the oldest file) drops them.
20. **A write to an attached lake is recorded by that lake's leader** (`write::deliver`), never
   by ours: our catalog never lists another lake's files.
21. **SQL from users never touches a node's disk.** Every query that arrives over HTTP, Postgres
   or MCP runs with `query::read_only()` (no `COPY … TO`, no `CREATE EXTERNAL TABLE`, no session
   DDL); writes go through `write.rs`. Local files (`enable_url_table`) are for `pondra sql` on
   its user's own machine only (`prepare(…, files: true)`). `harness.py clients` checks both.

## Tests: run these before and after any change

```bash
cargo build --release
python3 tools/harness.py all            # upsert, fence/split-brain, bulk insert, reader, crash, load
python3 tools/harness.py crash --runs 3 --batches 60 --size 50000   # kill -9 + injected crashes, 9M events
python3 tools/cluster.py users --secs 30      # 64 writers + 16 readers: 0 torn reads, 0 lost
python3 tools/cluster.py failover --secs 45   # 2 leader kills; task state == inline view == model
python3 tools/cluster.py latency [--load 4]   # event -> view row on another node
python3 tools/harness.py serverless            # pondra sql INSERT with and without a leader, 4 at once, a retry
python3 tools/open_check.py                    # Delta + Iceberg: 6 outside readers == Pondra
python3 tools/freshness.py [--flag ack=replicated]  # head to head: nodes, pondra sql, Delta, Iceberg
python3 tools/harness.py clients               # SQL writes, Python client, Postgres drivers, tokens, inbox, attach, vectors, MCP
python3 tools/mcp_client.py --url http://127.0.0.1:8080/mcp   # the official MCP SDK (pip install mcp) against a node
python3 tools/keyed_bench.py                   # keyed-table compaction: bytes written, correctness
python3 tools/cluster.py race | isolate | split | spread
python3 tools/bench/run.py batch 20000000     # ENGINES=pondra,spark,flink
python3 tools/serve_bench.py --keys 2000000   # serving: point lookups and dashboard queries
```

Add `--s3` to any of them with a simulated-R2 bucket to see the object-storage behaviour:

```bash
python3 tools/sim_r2.py --port 9000 &   # moto + R2-like latency (PUT p50 197 ms, GET p50 100 ms)
export AWS_ENDPOINT=http://127.0.0.1:9000 AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
       AWS_REGION=auto AWS_ALLOW_HTTP=true PONDRA_BUCKET=testbucket
python3 -c "import boto3; boto3.client('s3', endpoint_url='http://127.0.0.1:9000', region_name='us-east-1').create_bucket(Bucket='testbucket')"
python3 tools/harness.py crash --runs 3 --batches 150 --s3
python3 tools/cluster.py failover --s3 --flag ack=replicated   # recovery of acked-but-not-durable commits
```

**A tiering failure is silent in the correctness tests** — reads stay correct, the log just stops
draining — so it shows up as a throughput drop in `tools/bench/run.py live` (and as
`background job failed:` on the leader's stderr), not as a test failure. `harness.py tiering`
checks the log drains and the file count stays bounded; watch the live benchmark for the rest.

**`failover` and `users` are the tests that catch read-consistency bugs.** `failover` fails about
1 run in 8 when something is wrong — run it 15–20 times before believing a fix. `users` caught the
round-6 mirror bug in 4 of 5 runs; run it at least 5 times after touching `store.rs`.
`crash --size 50000` is the one that catches "the leader can see its own in-flight writes" bugs.
`open_check.py --rounds 1250` is the one that catches slow tiering rounds (and Delta log and
Iceberg snapshot cleanup). Any change to replication or recovery: `users` and `failover` with
`--flag ack=replicated`, locally and with `--s3` on simulated R2, several runs each.

Practical notes for an agent working here:

- Never rebuild the binary while a test suite is running (tests exec `argv[0]` when a node restarts).
- Test runs delete their lakes when they exit (`harness.new_lake`; `--keep` or `PONDRA_KEEP=1`
  keeps them). The owner's R2 free tier is 10 GB: after R2 runs, `tools/clean_bucket.py --bucket
  … --newest 3` leaves only the newest three lakes.
- Kill leftover nodes with `pgrep -x pondra` (never `pkill -f` or `pgrep -f <script name>`: it
  matches your own shell) and clean `/tmp/pondra-*/` afterwards, or the disk fills up. The SSD
  tier's default folder `/tmp/pondra-cache/` goes with it.
- Node stderr goes to `/tmp/pondra-<port>-<id>.stderr`; that's where "restarting to rejoin",
  "slow tiering" and panics show up.

## State of the work (2026-09-22, round 9)

Everything in `docs/prototype-status.md` passes on local disk and on simulated R2. The round-9
additions (inbox, SQL writes, Postgres, MCP, vectors, tokens) also ran against real R2.

**R2 test buckets.** There are two:

- `ponderabucket-us` (Eastern North America, ~290 ms per PUT from the sandbox; the default);
- `pondbucket` (~670 ms per PUT).

**The owner's R2 free tier is 10 GB.** Test runs delete their lakes; keep at most three lakes in
all. After R2 runs: `tools/clean_bucket.py --bucket ponderabucket-us --bucket pondbucket --newest
3 --dry-run`, then without `--dry-run`. The three kept now:

- `ponderabucket-us/round8/test-180091d0` (open formats);
- `ponderabucket-us/round8/test-a263452c` (replicated failover);
- `pondbucket/pondra-demo`.

Headline numbers, all on one 2-vCPU box:

- **Writes on R2:** acked in 4 ms with `--ack replicated` (299 ms durable); 87k events/s from 64
  writers, replicated. Locally, `--fsync` and `--replicas 3` cost nothing measurable (4 ms p50).
- **Writes from anywhere:** a `pondra sql` INSERT through the bucket inbox takes 1.0 s locally
  and 7.3 s on real R2, including the 3–6 s it takes to open the catalog.
- **Clients:** 18 checks pass: SQL writes, the Python client, four Postgres drivers, MCP,
  vectors, tokens, attached lakes, the inbox.
- **Freshness, like for like:** nodes see a write 10–15 ms after the ack (local and R2); Delta
  and Iceberg readers ~30 ms (local) / 3–4 s (near R2) / 7–10 s (far R2).
- **Keyed compaction:** 3x fewer bytes written than full rewrites (size-tiered).
- **TPC-H SF1:** all 22 queries in 5.9 s (Spark 4.2: 58–65 s). **Serving:** 0.14 ms key lookups,
  20–36k/s.
- **Consistency:** 0 torn reads, 0 lost batches, clean failovers, in both ack modes.

The comparison with Spark, Flink, Fluss and Lakehouse//RT — including Fluss 1.0 item by item,
what each competitor is building next, and the plan for the gaps — is
`docs/comparison-spark-flink-fluss.md`.

Known limits, in the order they matter:

1. **Replicated acks' window.** An acked write survives any one node dying (with `--fsync`,
   followers' power loss too), but not the leader and every holder dying before the bucket has
   it. Recovery waits up to 20 s for unreachable members.
2. **One sequencer per lake** orders commits. Attached lakes split the load across leaders, but
   there are no transactions across lakes.
3. **No shuffles** in distributed queries: big-to-big joins run on one node. Everything has run
   as processes on one machine; no multi-machine run yet.
4. **Missing doors:** no Kafka protocol, no Iceberg REST catalog, no `ALTER TABLE`.
5. **Streaming:** event-time windows update on late rows and expire by TTL, but there are no
   watermarks that close a window and emit it once.
6. **Security:** tokens per role only; no TLS (use a proxy), no per-table grants or quotas.
7. **Latency of one-off writers on far storage:** `pondra sql` spends 2–3 s opening the
   catalog on far object storage, and an inbox write adds a second or two.
8. **`_deleted` shows** in `SELECT *` of keyed tables (null or false for live rows).

Good next moves, in order. The plan table in the comparison doc has the evidence each should
produce.

1. **Kafka-protocol ingest** (produce, plus a fetch subset). Every competitor has a Kafka door.
2. **A multi-machine run** (3–10 VMs on S3/R2), then **shuffles** through the job-dealing
   mechanism.
3. **An Iceberg REST catalog endpoint** over the Iceberg tables Pondra already publishes.
4. **`ALTER TABLE … ADD COLUMN`**, then renames and defaults.
5. **Watermarks** and append-only window output; point-in-time joins.
6. **AI functions in SQL** (`ai_complete`, `embed` against an OpenAI-compatible endpoint) and an
   approximate vector index.
7. **TLS, per-table grants, an audit log, quotas.**

## Conventions

- Comments explain *why*, in plain English; the code shows *what*. Keep functions small.
- No new dependencies without a real reason; no new always-on services, ever.
- Every new invariant gets a test in `tools/` that would fail without it.
- Docs live in `docs/`; a design change means a new ADR, not an edit to an old one.
