# Prototype status: Pondra, a streamhouse in one binary

**Date:** 2026-09-20 (round 4) · **Plan:** ADR-002 / 003 / 004 / 005 · **Code:** `pondra-prototype.tar.gz` (≈2,500 lines of Rust, plus test and benchmark tools)
**Name:** the prototype formerly called `lh` is now **Pondra**. The name is free on crates.io, PyPI and npm. A small personal-finance app uses it (pondra.app), a different category; run a trademark search before a public launch.

## Where it stands

One Rust binary replaces the Kafka + Flink + Spark + metastore + ZooKeeper stack for the common jobs:

- stream ingest, exactly-once;
- streaming SQL: views with no lag, and stateful tasks;
- push to clients;
- batch ELT and SQL, distributed across nodes;
- upsert and merge tables.

Start more copies on the same bucket to scale out. The only state is object storage. There's no JVM, no database server and no coordination service.

**Round 4 removed the two limits round 3 left:**

1. **Writes no longer go through one machine.** Every node ingests: it encodes, stores and aggregates the data it receives. The leader only hands out the order of commits, and even the tiering work is dealt out to all nodes.
2. **Reactions take milliseconds.** An event written to one node shows up, aggregated, on a client watching another node 6 ms later (p50, local disk; round 3: ~0.8 s). On object storage it's one storage write, ~0.3–0.5 s on simulated R2.

**R2:** still blocked from this sandbox (organization network policy). Everything ran on local disk and on a local S3 server that adds R2-like latency (PUT p50 197 ms, GET p50 100 ms). `tools/r2_test.sh` runs the main tests against a real bucket from any machine.

## The old limit, in plain words

Round 3's report said "one leader commits all writes, so write capacity grows with a bigger machine, not with more machines."

- **Before:** imagine a shop where one cashier also packs every bag. More doors (nodes) let more customers in, but all of them still queued at that one cashier.
- **Now:** every node packs its own bags. It parses the batch, compresses it, writes it to the bucket and updates the views. The leader only hands out numbered tickets: "your batch is number 1,042". A ticket is a few bytes of metadata, so one leader can keep many nodes busy.

**Measured** (all three nodes share one 2-vCPU VM):

- 8 producers wrote 2.1–2.3 M events/s through the two followers.
- The leader used 34 % of the cluster's CPU: its one-third share.
- Before the tiering work was also dealt out, the leader's share was 70 %.

## What was added this round

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

## What the tests found, and what was fixed

- **Lost task rows in about 1 failover run in 8.** A follower combines its own catalog view with the commits streamed to it, and three races could make a read see less than the node thought it had:
  - the view moving past the stream while a read was in flight;
  - streamed copies dropped on a 5-second timer while a slow read still needed them;
  - the "how many segments exist" mark being read from a *newer* view than the one a read was held to, so a streaming task read a range of the log that its own view couldn't fully see yet, and committed progress past the rows it missed.

  Now: a read checks its view before and after reading and decides under one lock, it pins what it needs, and the segment count always comes from a view at most as new as the read's. Twenty failover runs have passed since.
- **The leader did 70 % of the cluster's work**, because tiering, file merges and compaction ran there. They're now jobs dealt out to all nodes; the leader's share is 34 %.
- **A tiering job on a node whose view lagged could have written an incomplete file.** A job now names its inputs exactly and waits until the node sees the last log segment; otherwise it fails and the next round retries.
- **Catalog writes stalled ~30 s on simulated R2** when SlateDB's default limit of 8 level-0 files was reached. Raised to 64.
- **A follower that misses commits now restarts** instead of serving reads that could go back in time.

## Many users at once

- **64 independent writers + 16 SQL readers + 2 serverless `pondra sql` processes, on 3 nodes:**
  - local disk: 1.47 M events in 31 s (47.8k/s), ack p50 109 ms / p99 461 ms, snapshot query p50 386 ms;
  - simulated R2: 276k events in 35 s (7.9k/s), ack p50 691 ms / p99 1,537 ms, snapshot query p50 26 ms;
  - **0 inconsistent reads** (every read saw each producer's batches as a gap-free prefix) and **0 lost or duplicated batches** in both.
- **Writers don't lock each other.** Each node batches its own writers; the leader folds every node's flush into one commit.
- **Readers never block writers or each other.** They read immutable objects plus a catalog snapshot.
- **Two writer processes on one bucket** form a cluster instead of fighting. A leader that was replaced is fenced by the catalog and rejoins as a follower.

Limits: producer names must be unique per client; there is no auth or per-user quota yet.

## Tests

| Test | Local disk | Simulated R2 |
|---|---|---|
| Crash / exactly-once (kill -9 + injected crashes at 3 points) | 3 runs × 9 M events: 0 lost, 0 duplicated, views exact | 3 runs × 45k events, 12–13 kill -9 plus injected crashes each: 0 lost, 0 duplicated, views exact |
| 64 writers + 16 readers + serverless, 3 nodes | 1.47 M events (47.8k/s), 0 inconsistent | 276k events (7.9k/s), 0 inconsistent |
| Failover: 3 nodes, 6 task shards, 8 producers, 2 leader kills + 1 follower kill | 20 runs: writes back after 4.5–4.9 s, state caught up 0.1–0.3 s after the last write, task state == inline view == model, events exactly once | writes back after 10.0 s and 11.8 s, state caught up 4.2 s after the last write, exactly once, state == view == model |
| 5 nodes start at once | 1 leader | 1 leader |
| Follower cut off from a healthy leader | Doesn't take over; takes over 5.1 s after the leader really dies | Doesn't take over; takes over 9.7 s after the leader dies |
| Split brain (leader frozen, then resumes) | takeover 6.0 s; stale write rejected; rejoined as follower | takeover 10.2 s; stale write rejected; rejoined as follower |
| Upsert vs model (12,000 upserts/deletes, compactions, restart) | Pass | Pass |
| Distributed query == single-node query | Identical results, 2 M rows | Identical results |
| Latency (event → view row on another node) | idle 6 ms p50 / 10 ms p99; under 4 background producers 65 / 461 ms | idle 333 ms p50 / 1,072 ms p99; under 4 background producers 602 / 1,146 ms |

## Sizes

| What | Round 3 | Round 4 |
|---|---|---|
| Binary (stripped) | 88.3 MB (30 MB gzip, 17 MB xz) | 89.2 MB (30 MB gzip, 16.8 MB xz) |
| Idle memory | 18 MB | 37 MB (mimalloc reserves more up front) |
| Peak memory under full load | 455 MB | 2.0 GB at 1.2 M events/s sustained (280–613 MB in the batch and streaming benchmarks) |
| Storage per event (user, event, amount, ts) | NDJSON 77.9 B → log 12.8 B → Parquet 6.8 B | NDJSON 77.9 B → log 12.8 B → Parquet 6.8 B (unchanged) |

## Memory is a knob, not a mystery

Under saturation, memory is bounded by how many rows may wait in the log for tiering (`--backlog`, default 10 M rows). One node, 8 producers, 60 s, a 100,000-key aggregating view:

| `--backlog` | Throughput | Ack → visible in the view (p50 / p99) | Peak memory |
|---|---|---|---|
| 10 M (default) | 1.22 M events/s | 162 ms / 1.06 s | 2.0 GB |
| 2 M | 554k events/s | 53 ms / 232 ms | 1.3 GB |
| 500k | 34k events/s | 18 ms / 35 ms | 0.4 GB |

On this 2-vCPU box tiering competes with ingest for CPU, which is why a small backlog costs so much throughput; with more cores the knee moves. Without the view (plain durable ingest) the same run peaks at 0.87 GB, and the batch and streaming benchmarks peak at 258–639 MB.

## Against the kill criteria (ADR-002 / 003)

| Criterion | Status |
|---|---|
| Freshness p99 ≤5 s at ≥50k events/s | **Pass:** 10 ms p99 idle, 461 ms p99 under load (local disk); ~1.0–1.7 s p99 on simulated R2 |
| 0 lost / 0 duplicated under crashes | **Pass,** including leader failover and split brain |
| Binary ≤150 MB, idle memory ≤200 MB | **Pass** |
| Queries within 2x of DuckDB | **Pass** (round 2) |
| Catalog commit p95 ≤250 ms on object storage | **Open:** needs a real R2 measurement (simulated: a durable ack takes 309 ms p50 / 919 ms p99 when idle) |
| First query after idle ≤3 s | Readers pass; a writer restart is a few seconds on simulated R2 |
| Multi-node at ≥2x one node, recovery | **Partly:** ingest, views, tasks, queries and tiering all spread over nodes, and failover works. Speed-ups can't be shown on one machine: on simulated R2, where I/O latency dominates, 3 nodes ran 8 of the 9 queries 1.2–4.3x faster than one |

## What remains

- One sequencer per lake orders commits (metadata only). Past its capacity, use several lakes.
- Durable acknowledgement costs one object-store write: ~0.3–0.5 s on R2-like storage, milliseconds on local disks or S3 Express One Zone (not measured).
- Merge tables only support decomposable aggregates (sum, count, min, max).
- Distributed queries have one stage: no shuffles, so big-to-big joins run on one node.
- Upsert compaction rewrites the whole table (on any node now, not just the leader).
- No auth, quotas or multi-tenancy.
- No DuckLake / Iceberg publishing yet, so outside engines can't read the tables.

## Next

1. **Real R2**: run `tools/r2_test.sh` from a machine that can reach the bucket, then rotate the keys shared in chat.
2. **Partitioned upsert compaction and key-range pruning**, so 100 M+ key state stays cheap.
3. **Shuffles** for big-to-big joins (the same job-dealing mechanism the tiering uses).
4. **Auth and quotas** per producer and per user.
5. **DuckLake / Iceberg publishing** so Spark, Trino and DuckDB can read the same tables (ADR-001).
