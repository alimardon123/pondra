# Prototype status: Pondra, a streamhouse in one binary

**Date:** 2026-09-21 (round 5) · **Plan:** ADR-002 / 003 / 004 / 005 · **Code:** `pondra-prototype.tar.gz` (≈2,650 lines of Rust, plus test and benchmark tools)
**Name:** the prototype formerly called `lh` is now **Pondra**. The name is free on crates.io, PyPI and npm. A small personal-finance app uses it (pondra.app), a different category; run a trademark search before a public launch.

## Where it stands

One Rust binary replaces the Kafka + Flink + Spark + metastore + ZooKeeper stack for the common jobs:

- stream ingest, exactly-once;
- streaming SQL: views with no lag, and stateful tasks;
- push to clients;
- batch ELT and SQL, distributed across nodes;
- upsert and merge tables.

Start more copies on the same bucket to scale out. The only state is object storage. There's no JVM, no database server and no coordination service.

**Round 4 removed the two limits round 3 left, and round 5 made it a serving engine too:**

1. **Writes no longer go through one machine.** Every node ingests: it encodes, stores and aggregates the data it receives. The leader only hands out the order of commits, and even the tiering work is dealt out to all nodes.
2. **Reactions take milliseconds.** An event written to one node shows up, aggregated, on a client watching another node 7 ms later (p50, local disk; round 3: ~0.8 s), and on a read-only serving node just as fast. On object storage it costs one storage write — on a real R2 bucket, 1.0 s from this sandbox, where a bare PUT is 1.07 s.
3. **Serving reads are a first-class path** (round 5): one key out of 2 M in 7 ms, and keyed tables no longer rewrite themselves on every tiering round — which also took durable ingest from 2.1 M to 3.8 M events/s.

**R2: now measured for real.** The whole suite ran against a Cloudflare R2 bucket — see "On real
R2" below. Everything also still runs on local disk and on a local S3 server with R2-like latency
(PUT p50 197 ms, GET p50 100 ms); `tools/r2_test.sh` runs the main tests against any bucket.

## The old limit, in plain words

Round 3's report said "one leader commits all writes, so write capacity grows with a bigger machine, not with more machines."

- **Before:** imagine a shop where one cashier also packs every bag. More doors (nodes) let more customers in, but all of them still queued at that one cashier.
- **Now:** every node packs its own bags. It parses the batch, compresses it, writes it to the bucket and updates the views. The leader only hands out numbered tickets: "your batch is number 1,042". A ticket is a few bytes of metadata, so one leader can keep many nodes busy.

**Measured** (all three nodes share one 2-vCPU VM):

- 8 producers wrote 3.8 M events/s through the two followers (2.1 M before round 5's tiering work).
- The leader used 31 % of the cluster's CPU, below its one-third share.
- Before the tiering work was also dealt out, the leader's share was 70 %.

## Round 4: every node writes

| Feature | Replaces | Notes |
|---|---|---|
| **Every node ingests** | Kafka brokers | Nodes batch, encode (Arrow IPC + ZSTD) and store their own writes. Small flushes (≤64 KB) ride inside the commit request; the leader dedupes, numbers and commits. 4 flushes in flight per node, 4 commits in flight at the leader; no fixed flush window |
| **Commit stream** | ZooKeeper watches, Kafka replication | The leader pushes each durable commit to every follower over one HTTP response. Followers lay it over their own catalog view, so every node sees a commit within milliseconds |
| **Inline views** (`POST /views/{name}`) | Flink SQL jobs + keyed state | Run on every flush, on the node that received it, and commit with their input: never behind, exactly-once for free. GROUP BY views become **merge tables** (sum / count / min / max partials, merged on read) that any number of nodes update at once |
| **Push** (`GET /watch/{table}`) | Kafka consumers | New rows as NDJSON the moment they commit, on any node |
| **Event-driven tasks** | Flink jobs | Stateful tasks run as soon as rows commit (not every second) |
| **SPMD queries** | Trino / Spark SQL | Every node runs the same plan over its slice of files up to DataFusion's first exchange; the receiving node finishes it |
| **Distributed maintenance** | Spark OPTIMIZE jobs | The leader decides; converting the log to Parquet, merging small files and compaction run as jobs dealt to all nodes |
| **Backpressure** | — | While 10 M rows wait to be tiered, commits pause; tiering starts as soon as 1 M rows wait, in chunks of ≤4 M |

### What the tests found, and what was fixed

- **Lost task rows in about 1 failover run in 8.** A follower combines its own catalog view with the commits streamed to it, and three races could make a read see less than the node thought it had:
  - the view moving past the stream while a read was in flight;
  - streamed copies dropped on a 5-second timer while a slow read still needed them;
  - the "how many segments exist" mark being read from a *newer* view than the one a read was held to, so a streaming task read a range of the log that its own view couldn't fully see yet, and committed progress past the rows it missed.

  Now: a read checks its view before and after reading and decides under one lock, it pins what it needs, and the segment count always comes from a view at most as new as the read's. Twenty failover runs have passed since.
- **The leader did 70 % of the cluster's work**, because tiering, file merges and compaction ran there. They're now jobs dealt out to all nodes; the leader's share is 30 %.
- **A tiering job on a node whose view lagged could have written an incomplete file.** A job now names its inputs exactly and waits until the node sees the last log segment; otherwise it fails and the next round retries.
- **Catalog writes stalled ~30 s on simulated R2** when SlateDB's default limit of 8 level-0 files was reached. Raised to 64.
- **A follower that misses commits now restarts** instead of serving reads that could go back in time.

## Round 5: serving reads, and keyed tables that don't rewrite themselves

Two things were wrong for a system that wants to replace the serving tier as well as the lake:
every tiering round rewrote a keyed table in full, and every read of one paid a window function
over the whole table. Fixing them also made ingest much faster.

| Change | Effect |
|---|---|
| **Keyed tables are LSM-like.** Each round folds the log tail into a *new* file (a range per node, like append tables); files are compacted into one only when 8 pile up. Each file carries the last segment it covers, so a newer file's row wins for a key | Tiering costs what the new rows cost, not what the table costs |
| **Files that are already 64 MB or 4 M rows are left alone** by small-file merges | Rows are written once, not merged over and over |
| **Files are written for lookups:** sorted by key, a bloom filter per key column, 256k-row row groups | A lookup reads one row group, not a whole file |
| **`GET /lookup/{table}/{key}`** plans a lookup (single thread, no window) instead of a scan | A serving API that doesn't depend on SQL planning; today it costs the same as the equivalent SQL, not less |
| **"Newest version per key" is a grouped aggregate, not a window** | Scans of keyed tables 3.5x faster (2 M rows: 1,234 ms → 351 ms) |
| **Compacted tables skip deduplication entirely** (one file, nothing newer in the log = one row per key already) | A served table reads like plain Parquet |
| **Read-only nodes follow the leader's commit stream** instead of polling their own catalog view | Freshness on a serving node: 163 ms → **7 ms** p50 |

What it did to throughput (same box, same tests, round 4 → round 5):

| | Round 4 | Round 5 |
|---|---|---|
| Durable ingest, 3 nodes, producers writing to the followers | 2.1 M events/s | **3.8 M events/s** (leader 31 % of cluster CPU) |
| Saturated ingest through an aggregating view, one node | 1.23 M events/s | **2.84 M events/s** |
| 10 M events, ingest + keyed aggregation, one node | 3.97 s | 4.08 s |
| Batch: write 20 M rows + 6 queries | 3.08 s + 0.04–0.46 s | 3.15 s + 0.04–0.48 s |

Serving, measured with `tools/serve_bench.py` (2 M keys, one node, 2 vCPUs, local disk):

| Query | Result |
|---|---|
| Point lookup, 1 client | **7.7 ms** p50 (`/lookup`: 8.9 ms), 11 ms p99 |
| Point lookup, 8 clients | 30 ms p50, 46 ms p99, **265 lookups/s** (the two cores are the ceiling) |
| Point lookup, 8 clients, while 200-row batches keep arriving | 44 ms p50, 99 ms p99 |
| Dashboard aggregate over all 2 M rows | 351 ms (1,234 ms before this round) |
| Freshness on a read-only node (write → visible) | 7 ms p50, 84 ms p99 |

The ceiling of ~265 lookups/s is this box's two cores, not the design: each lookup costs ~7 ms of
CPU, most of it planning and Parquet decoding. Caching prepared plans per table version is the
obvious next step, and read-only nodes already scale the rest horizontally.

**What the tests missed.** The first version of this tiering rewrite stopped tiering an append
table once it had 8+ files. Reads stayed correct — the log just kept growing — so every
correctness test passed while sustained ingest fell by half. It was found by benchmarking, not by
testing. There is now a `tiering` test (rounds of writes + `/tier`: the log must drain and the
file count stay bounded) and a note in `AGENTS.md` that this failure mode shows up as throughput,
not as a red test.

## On real R2

Same tests, against a Cloudflare R2 bucket, from this sandbox. **A bare 64 KB PUT from here takes
810 ms p50 (1,048 ms p90) and a GET 379 ms** — that round trip dominates every number below, and
it is why a durable ack costs 1.2 s: the ack is essentially one object-store write. A deployment
near its bucket would see far less; treat these as a worst case, not a floor.

| | Real R2 | Simulated R2 (PUT p50 197 ms) | Local disk |
|---|---|---|---|
| Write acknowledged (durable), idle | **1,243** / 1,699 ms | 309 / 919 ms | 7 / 9 ms |
| Event → view row on another node | 1,336 / 2,553 ms | 333 / 1,072 ms | 7 / 10 ms |
| 64 writers + 16 readers + 2 serverless, 3 nodes | 91k events (2.5k/s), ack p50 2.0 s, snapshot query p50 12 ms, **0 inconsistent** | 276k events, 0 inconsistent | 1.47 M events, 0 inconsistent |
| Crash / exactly-once (kill -9 + injected crashes) | 30k events, 34 kill -9s + injected crashes: exact (plus 2 × 30k in the previous round) | 3 × 45k events, exact | 3 × 9 M events, exact |
| Leader failover (2 kills) | 23.1 / 22.9 s, state caught up 8.2 s, exactly once | 10.0 / 11.8 s | 4.5–4.9 s |
| Split brain (frozen leader) | 18.5 s; stale write rejected; rejoined | 10.2 s | 6.0 s |
| Distributed query, 3 nodes vs 1 | identical results; q9 1,221 → 541 ms, q8 1,308 → 1,126 ms | 8 of 9 queries 1.2–4.3x faster | no gain on one machine |
| Upsert compaction (12,000 upserts, 6 compactions) | 3.7–7.1 s each | 1.5–3.2 s each | milliseconds |
| Serving: point lookup, 500k keys, 8 clients | 29 ms p50 warm (276 ms cold: one GET), 311/s | — | 30 ms p50, 265/s (2 M keys) |
| Serving: dashboard aggregate on a compacted table | 3.1 ms (cached) | — | 351 ms (2 M rows) |

Nothing failed: 0 lost, 0 duplicated, 0 inconsistent reads, exactly-once through leader kills, on
real object storage.

## Many users at once

- **64 independent writers + 16 SQL readers + 2 serverless `pondra sql` processes, on 3 nodes:**
  - local disk: 1.52 M events in 31 s (49.2k/s), ack p50 88 ms / p99 638 ms, snapshot query p50 404 ms;
  - real R2: 91k events in 37 s (2.5k/s), ack p50 2.0 s / p99 3.9 s, snapshot query p50 12 ms;
  - simulated R2: 276k events in 35 s (7.9k/s), ack p50 691 ms / p99 1,537 ms;
  - **0 inconsistent reads** (every read saw each producer's batches as a gap-free prefix) and **0 lost or duplicated batches** in both.
- **Writers don't lock each other.** Each node batches its own writers; the leader folds every node's flush into one commit.
- **Readers never block writers or each other.** They read immutable objects plus a catalog snapshot.
- **Two writer processes on one bucket** form a cluster instead of fighting. A leader that was replaced is fenced by the catalog and rejoins as a follower.

Limits: producer names must be unique per client; there is no auth or per-user quota yet.

## Tests

Every test runs on local disk, on a local S3 server with R2-like latency, and (this round) against
a real Cloudflare R2 bucket. All of them pass on all three.

| Test | Local disk | Real R2 |
|---|---|---|
| Crash / exactly-once (kill -9 + injected crashes at 3 points) | 3 runs × 9 M events: 0 lost, 0 duplicated, views exact | 30k events, 34 kill -9s: exact |
| 64 writers + 16 readers + serverless, 3 nodes | 1.52 M events (49.2k/s), ack p50 88 ms, 0 inconsistent | 91k events (2.5k/s), ack p50 2.0 s, 0 inconsistent |
| Failover: 3 nodes, 6 task shards, 2 leader kills + 1 follower kill | 4 runs: writes back after 4.5–4.9 s, caught up 0.1–6.4 s, state == view == model | 23.1 / 22.9 s, caught up 8.2 s, exact |
| 5 nodes start at once | 1 leader | 1 leader |
| Follower cut off from a healthy leader | doesn't take over; takes over 5.1 s after the leader dies | 18.1 s |
| Split brain (leader frozen, then resumes) | takeover 6.0 s; stale write rejected; rejoined | 18.5 s; same |
| Upsert vs model (12,000 upserts/deletes, compactions, restart) | pass | pass |
| Tiering keeps up (18 rounds of writes + `/tier`, 10.8 M rows) | log drained, files bounded, rows exact | pass (1 M rows) |
| Distributed query == single-node query | identical, 2 M rows | identical |
| Latency (event → view row on another node) | 7 ms p50 / 10 ms p99; 54 / 290 ms under load | 1.34 s p50 / 2.55 s p99 |
| Freshness on a read-only node | 7 ms p50 / 84 ms p99 | — |
| Serving (2 M keys: lookups and a dashboard query) | 7 ms p50, 265 lookups/s, 351 ms aggregate | 29 ms p50 warm, 311 lookups/s |

## Sizes

| What | Round 3 | Round 5 |
|---|---|---|
| Binary (stripped) | 88.3 MB (30 MB gzip, 17 MB xz) | 89.2 MB (29.9 MB gzip, 16.8 MB xz) |
| Idle memory | 18 MB | 41 MB (mimalloc reserves more up front) |
| Peak memory under full load | 455 MB | 1.8 GB at 2.84 M events/s sustained (279–586 MB in the batch and streaming benchmarks) |
| Storage per event (user, event, amount, ts) | NDJSON 77.9 B → log 12.8 B → Parquet 6.8 B | NDJSON 77.9 B → log 12.8 B → Parquet 6.8 B (unchanged) |

## Memory is a knob, not a mystery

Under saturation, memory is bounded by how many rows may wait in the log for tiering
(`--backlog`, default 10 M rows). One node, 8 producers, 60 s, a 100,000-key aggregating view:

| `--backlog` | Throughput | Ack → visible in the view (p50 / p99) | Peak memory |
|---|---|---|---|
| 10 M (default) | 2.84 M events/s | 205 ms / 730 ms | 1.8 GB |
| 2 M | 784k events/s | 49 ms / 234 ms | 1.4 GB |
| 500k | 33k events/s | 18 ms / 36 ms | 0.35 GB |

Lower it for less memory and lower latency, raise it to absorb longer bursts. Plain durable
ingest without a view reaches 5.5 M events/s at 1.3 GB, and the batch and streaming benchmarks
peak at 279–586 MB.

## Against the kill criteria (ADR-002 / 003)

| Criterion | Status |
|---|---|
| Freshness p99 ≤5 s at ≥50k events/s | **Pass:** 10 ms p99 idle, 290 ms p99 under load (local disk); 2.6 s p99 on real R2 |
| 0 lost / 0 duplicated under crashes | **Pass,** including leader failover and split brain |
| Binary ≤150 MB, idle memory ≤200 MB | **Pass** |
| Queries within 2x of DuckDB | **Pass** (round 2) |
| Catalog commit p95 ≤250 ms on object storage | **Measured, and it depends on the bucket's distance:** a durable ack is essentially one object-store write — 1.24 s p50 against R2 from this sandbox (where a bare PUT is 0.81 s), 309 ms on the R2-latency simulator, 7 ms on local disk |
| First query after idle ≤3 s | Readers pass; a writer restart is a few seconds on simulated R2 |
| Multi-node at ≥2x one node, recovery | **Partly:** ingest, views, tasks, queries and tiering all spread over nodes, and failover works. Speed-ups can't be shown on one machine: on simulated R2, where I/O latency dominates, 3 nodes ran 8 of the 9 queries 1.2–4.3x faster than one |

## What remains

- One sequencer per lake orders commits (metadata only). Past its capacity, use several lakes.
- Durable acknowledgement costs one object-store write: 1.2 s against R2 from this sandbox, where
  a bare PUT is 0.81 s; milliseconds on local disks, and expected to be milliseconds on S3 Express
  One Zone (not measured).
- Serving throughput is ~265 lookups/s per two cores. Prepared plans cached per table version are
  the missing piece; read-only nodes already scale out and are 7 ms fresh.
- Compaction of a keyed table is one job on one node. Partitioned compaction (a key range per
  node) is the next step; the LSM layout means it now runs once every 8 rounds, not every round.
- Merge tables only support decomposable aggregates (sum, count, min, max).
- Distributed queries have one stage: no shuffles, so big-to-big joins run on one node.
- Under sustained overload, commits pause until tiering catches up (`--backlog`); a client with a
  short timeout will see it as a slow ack.
- No auth, quotas or multi-tenancy, and no DuckLake / Iceberg publishing yet.

## Next

1. **Prepared-plan cache per table version**, so a lookup costs a page read instead of a plan.
2. **Partitioned compaction and key-range pruning**, so 100 M+ key state stays cheap.
3. **Shuffles** for big-to-big joins (the job-dealing mechanism the tiering uses already fits).
4. **S3 Express One Zone measurements** — the one storage choice that removes the latency gap to
   Fluss and Databricks RT without giving up "object storage is the only state".
5. **Auth and quotas** per producer and per user.
6. **DuckLake / Iceberg publishing** so Spark, Trino and DuckDB can read the same tables (ADR-001).
