# ADR-005: No single write path, millisecond reactions, one engine across nodes

**Status:** Accepted, built and tested (round 4) · **Date:** 2026-09-20 · **Builds on:** ADR-002, ADR-003, ADR-004
**Product name:** the prototype called `lh` is now **Pondra** (binary `pondra`). The name is free on crates.io, PyPI and npm. The only other product using it is a small personal-finance app (pondra.app), a different category; run a trademark search before a public launch.

## Context

Round 3 left two limits the project owner wants gone:

1. **One machine carried every write.** Followers forwarded writes to the leader, which parsed, encoded and stored all the data. Adding machines didn't add write capacity.
2. **Reactions took about a second.** Writes waited for a 250 ms group-commit window, tasks polled every second, and followers polled the catalog. Flink reacts in milliseconds and Fluss serves reads in milliseconds.

A third limit carried over from ADR-003: a single query still ran on one node.

The design principles still apply: one small binary, object storage as the only state, no JVM or coordination service, SPMD rather than driver/executor, and short, readable code.

## Decisions

### 1. Every node ingests; the leader only puts writes in order

The leader used to do all the work. Now it only hands out the order: a ticket counter, not the whole shop.

- Every node batches the writes it receives. It encodes each batch as Arrow IPC (ZSTD) and runs the inline views on it (section 3).
- Big flushes (over 64 KB) are written to object storage by that node itself. Small ones travel inside the commit request.
- The leader's sequencer receives only metadata: which producer, which sequence number, where the bytes are. It rejects retries (exactly-once), numbers the flushes as log segments and commits them all in one catalog write.
- Writes are pipelined:
  - up to 4 flushes in flight per node;
  - up to 4 catalog commits in flight at the leader;
  - commits become durable in order.

  There is no fixed flush window any more. An idle node flushes at once; a busy one batches whatever queued up while the previous flush was in flight.
- **Backpressure:** while more than 10 M rows are waiting to be tiered, commits pause. Producers slow down to what the cluster sustains, instead of memory and latency growing without limit.
- **Measured** (`cluster.py split`: 8 producers writing only to the 2 followers of a 3-node cluster, all on one 2-vCPU VM):
  - 2.1 M events/s;
  - the leader used 30 % of the cluster's CPU, below its one-third share, including its share of the tiering work (section 5).

  Before the tiering work was dealt out too, the leader's share was 70 %.

### 2. The leader streams every commit to the followers

When a catalog write becomes durable, the leader pushes it over one long-lived HTTP response (`/cluster/log`) to every follower. Every node therefore sees every commit within milliseconds, and the bucket stays the only source of truth.

- **Commit numbers:** every commit writes its number (`c`).
- **Two layers on a follower:**
  - Its own catalog view follows only the leader's checkpoints (every 2 s), which is cheap.
  - The streamed commits are laid over that view. They're dropped once the view has them and no read in progress still needs them: each scan pins the view it started from.
- **When a read uses the streamed commits:** a scan notes its view's commit number before and after reading.
  - If both are within what the stream holds, it adds the streamed commits: the result is the lake as of the last streamed commit.
  - If the view is wholly ahead of the stream, the view alone is newer, and the read uses just the view.
  - If the view passed the stream mid-read, the read runs again.
  - After a gap at the start of a stream, the read uses its view alone until the view has caught up past the gap.
- **How far a node can read:** the number of log segments a node treats as readable comes from the same view its reads are held to, never a newer one. Tasks and watchers use it instead of the "a commit happened" signal, so a task never reads a range of the log its own view can't fully see.
- **How we got here:**
  - The 64-user test found torn reads on followers under heavy load with two simpler rules.
  - The failover test then lost task rows in about 1 run in 8, from three races: a read whose view moved past the stream while it was reading, streamed copies dropped on a timer while a slow read still needed them, and a segment count read from a newer view than the read itself.
  - With these rules: 0 inconsistent reads and 20 clean failover runs.
- **Inline segment data never changes**, so a streamed copy of it is always safe to use.
- **If the stream breaks,** the follower reconnects and receives the last 30 s of commits again. If it missed more than that, it restarts rather than serve reads that could go back in time.

### 3. Inline views: stream processing with no lag

`POST /views/{name}` with a SQL body registers a view.

- **Where it runs:** on the node that received the rows, on every flush of new rows from the view's source table (the first table in FROM).
- **Commit:** the view's output is committed in the same catalog write as its input. It is never behind its source, there's no progress to track, and it's exactly-once without extra machinery.
- **Without GROUP BY:** rows are appended. Use it to filter, reshape, or enrich by joining any table.
- **With GROUP BY:** the view is a merge table.
  - Each flush adds partial aggregates per key.
  - Reads combine them (sum, min, max; counts are summed).
  - Tiering folds them together.
  - Many nodes can add to the same key at once without coordinating, the way Fluss 0.9's aggregation merge engine and Paimon's merge engines work.

General stateful SQL (joins against a task's own state, anything not decomposable) stays in streaming tasks. Those now run as soon as rows commit, not on a timer.

### 4. Push to clients

`GET /watch/{table}` streams new rows as NDJSON the moment they commit, including a view's rows, on any node.

### 5. SPMD execution across nodes

- **Distributed queries:**
  - Every live node plans the same SQL, with the query's main table standing for only its own slice of files (dealt round-robin). The log tail is one more slice.
  - Each node runs the plan up to DataFusion's first exchange; for an aggregation, that's the partial aggregate. The receiving node merges the partial results and finishes the plan.
  - There is no shuffle, and every node reads its files straight from the bucket. Other tables are read whole on each node, so star joins work.
  - Because the cut comes from DataFusion's own parallel plan, every aggregate DataFusion can split works (avg, count distinct, …).
  - This applies to one-SELECT, inner-join queries over append tables of 256 MB or more (`?spread=1` forces it, `?spread=0` turns it off). Anything else runs on one node.
- **Distributed maintenance.** The leader decides what to tier and commits the result; all the data work is dealt out as jobs to the live nodes, round-robin:
  - converting ranges of log segments to Parquet (one range per node);
  - merging small files;
  - upsert and merge-table compaction.

  Each job names its inputs exactly (the file list as the leader sees it), so a node whose view lags can't work from an older file list. A node only starts once it sees the job's last log segment; otherwise the job fails and the next round retries it.

### 6. Operational fixes found by the tests

- **Faster restarts:** the catalog's memtable is persisted every 2 s while busy, so a restart, new leader or one-shot reader replays only a few seconds of catalog WAL instead of everything since the last compaction. Before this, opening a lake on simulated R2 could take tens of seconds.
- **No catalog stalls:** SlateDB's default limit of 8 level-0 files paused catalog flushes for about 30 s on simulated R2 while compaction caught up. Pondra now allows 64 (32 per key).
- **Tiering in chunks:** tiering runs as soon as a table has 1 M rows waiting, in chunks of at most 4 M rows, so memory stays bounded.
- **Failover fixes:**
  - A follower now restarts when the leader at its address changes term.
  - A peer vouches for the leader only if they agree on the term.
  - Before either fix, a restarted follower could keep following a stale term.
- **Fewer segments:** all small flushes in one commit share one log segment, so the log stays short.
- **Memory:**
  - Decoded log segments are cached per node (256 MB LRU) instead of raw bytes.
  - mimalloc returns freed memory promptly.
  - Commit-latency samples are capped.
- **Cleanup:** orphaned objects (flushes whose commit never happened, files from crashed jobs) are deleted after a day.

## What remains, stated plainly

- **Ordering goes through one leader,** but that is now metadata only. A single lake's commits are still one sequence. Beyond one sequencer's capacity (many thousands of flushes per second), the answer is several lakes.
- **Durable latency equals your storage's write latency:**
  - measured: ~6 ms on local disk; ~0.3–0.5 s on simulated R2;
  - expected but not measured here: low milliseconds on S3 Express One Zone.

  Fluss gets milliseconds on any storage by acknowledging after replicating to other servers' disks. Pondra keeps object storage as the only durable state, so its floor is one object-store write. Replicating to peers before acking would trade that simplicity away; it isn't built.
- **Merge tables need decomposable aggregates** (sum, count, min, max; avg as sum/count). Anything else goes in a task.
- **SPMD queries have one stage** (no shuffles). Big-to-big joins still run on one node.
- **Upsert compaction rewrites the whole table** (now on any node, not only the leader). Partitioned compaction is next.
- **Scale-out can't be timed on one machine.** Every test ran on one 2-vCPU VM, so the distributed paths show correctness and where the work goes, not multi-machine speed-ups.

## Alternatives rejected

| Option | Why not |
|---|---|
| One writer per partition with its own catalog (Kafka-style) | Global order and exactly-once across partitions get complicated; queries would read N catalogs |
| Acknowledge after replicating to peer memory/disks | Brings back the disk-and-replication stack Pondra exists to avoid |
| Flink-style keyed state with network shuffles | Needs state migration on rescale; merge tables need no shuffle and no state moves |
| Run the full plan on each node and union the results | Wrong for aggregates that don't combine (avg, count distinct); cutting at DataFusion's exchange is always right |
