# ADR-009: Native first — millisecond writes without S3 Express, open formats on request, writes from any machine

**Status:** Accepted, built and tested (round 8) · **Date:** 2026-09-21 · **Builds on:** ADR-005, ADR-007, ADR-008

## Context

Round 8 started from five questions about round 7:

1. **Delta conversion takes time and has limits.** Other engines can read the Delta log, but they
   can't write to it, and they only see a table after a tiering round. So what is Pondra's own
   format? Does it work anywhere? And can someone with the Pondra binary read and write the
   lake natively, faster and fresher than through Delta? If so, Delta — and Iceberg — should
   be something a table opts into, not something every table pays for.
2. **Fast writes without S3 Express.** A durable ack cost one object-store write: 0.3–0.7 s on
   R2, and more from far away. S3 Express One Zone would help, but most users don't have it.
3. **Small files, compaction, optimisation and indexes.** How are they handled, on local disk
   and on object storage? Is that enough?
4. **An honest freshness comparison.** Round 7 compared Pondra's 17 ms (local disk) with Fluss's
   3 minutes (its lake tables). That compared different things. Local/memory/SSD should be
   compared with local/memory/SSD, and object storage with object storage.
5. **Serverless, both ways.** Any laptop with the binary should be able to read *and write*,
   with or without a cluster running.

## Decisions

### 1. The native format is the lake itself; open formats are opt-in per table

The native format is what was always there:

- the Parquet files under `data/<table>/`;
- the log tail (recent rows not yet in Parquet);
- the catalog, a SlateDB store in `catalog/`.

All of it lives in the same folder or bucket, and it works wherever the binary runs: a local or
network directory, AWS S3, Cloudflare R2, MinIO — any S3-compatible store with conditional
writes. Readers with the binary use it directly and see more, sooner:

| Reader | What it reads | Sees | Can write |
|---|---|---|---|
| A node in the cluster (`pondra serve`, `--reader`) | the catalog in memory, kept current by the commit stream | every committed write, milliseconds after the ack | yes (`--reader`: no) |
| `pondra sql` on any machine, no server | the catalog, the log tail and Parquet, straight from the bucket | every write already in the bucket (in the default mode: every acknowledged write) | `INSERT … SELECT` (decision 4) |
| Another engine through Delta or Iceberg | the table's Delta log or Iceberg metadata | the table as of the last tiering round | no |

So tables are no longer published by default:

- `"publish": ["delta", "iceberg"]` when a table is created opts in; sending it again for an
  existing table changes it.
- `--publish delta,iceberg` makes that the default for new tables on a node.
- Turning a format off deletes its metadata folder, so no engine reads a stale copy.

### 2. Iceberg next to Delta (`src/iceberg.rs`)

- **Layout:** Iceberg format v2, in `data/<table>/metadata/`. Each change writes a
  `v{N}.metadata.json`, the manifest list and the manifest it names, and updates
  `version-hint.text`.
- **Avro by hand:** the manifests are Avro files written directly (about 100 lines, no new
  dependency). A manifest costs a few dozen bytes per file.
- **Same rules as Delta:**
  - it is derived from committed, durable catalog state only;
  - metadata files are written put-if-absent;
  - a version that a crashed attempt wrote but never recorded is skipped, never rewritten;
  - keyed tables are published only when one file holds one row per key.
- **Snapshots:** version N is snapshot N and sequence number N. Files carried over keep the
  sequence number they were added with. The last 100 snapshots are kept.
- **Column mapping:** Pondra's Parquet files carry no Iceberg field ids, so the metadata maps
  columns by name (`schema.name-mapping.default`).
- **Types:** Delta and Iceberg both gained `Decimal128`.

Checked with six independent readers — delta-rs, Polars and DuckDB for Delta; PyIceberg 0.12,
Polars and DuckDB 1.5.5 for Iceberg — on an append table, an upsert table, a GROUP BY view and a
bulk insert. That covers 120 rounds on local disk (past a Delta checkpoint and past Iceberg's
100-snapshot history) and real R2 (`tools/open_check.py`). Every reader's row count equals
Pondra's.

### 3. Replicated acks: milliseconds on any storage (`--ack replicated`)

A write is acknowledged once `--replicas` nodes hold it (default 2: the leader and one
follower). It reaches the bucket a moment later, in the same catalog write as before. No
S3 Express is needed.

**What the leader streams.** The commit stream (`GET /cluster/log`) now carries four kinds of
frame:

- `Start {term, replicated}`
- `Change` (a write)
- `Committed(id)` (apply everything up to here)
- `Durable(id)` (everything up to here is in the bucket)

**Default mode (`durable`).** The leader streams `Change` and `Committed` once the bucket has the
write, exactly as before.

**Replicated mode.**

1. The leader streams `Change` first.
2. Each follower appends the change to a local replica file and acknowledges the run of
   *consecutive* changes it holds. It never claims one it missed.
3. The write commits once `replicas − 1` member followers hold it, or once it is durable,
   whichever comes first. Committing means: acknowledged, visible on the leader, and streamed
   as `Committed`.
4. `Durable` frames let followers delete their copies.

Small flushes ride inside the commit up to 1 MB in this mode (64 KB otherwise), so a typical
write never waits on a data PUT either.

**Safety rules** (AGENTS.md invariant 15):

- **Members are listed before they count.** The followers whose acks count are listed in the
  catalog (`m`) and in the bucket *before* they count. A departing follower stops counting, and
  everything it helped commit is made durable, *before* it leaves the list. So a new leader
  always knows everyone to ask.
- **A new leader recovers first.** Before it takes writes, it:
  1. asks every listed member (waiting up to 20 s) for what they hold beyond the bucket;
  2. takes the longest run of consecutive changes, never one from an older term after one from
     a newer term;
  3. commits them again and waits until they are durable.
- **Followers never help an old leader.** A follower never acknowledges a change from a term
  older than the newest it has heard of, and a new leader's request raises that mark. A deposed
  leader therefore can't collect acks for writes the new one never saw.
- **Durable-only work waits for the bucket.** Delta/Iceberg publishing, deleting objects
  (retention) and the members list all wait until the state they build on is durable.
- **The lead over the bucket is bounded.** At most 256 commits are acknowledged ahead of the
  bucket. If the bucket stalls, acks wait for it. This bounds what a failover has to recover,
  or could lose.

**What it survives, and what it doesn't:**

- It survives any single node dying, the leader included, like Kafka with `acks=all`. Like
  Kafka's default, it doesn't `fsync`: the copy is in the follower's page cache, which survives
  a process crash but not a power loss.
- It does **not** survive losing the leader *and* every follower holding a write in the same
  window of one bucket write (0.3–0.7 s on R2). Nor does it survive a network partition where
  the new leader can't reach a holder within 20 s.
- Those losses are why it is opt-in. `durable` stays the default.

**Measured on real R2** (Eastern North America bucket, ~290 ms per PUT from here):

| | `--ack durable` | `--ack replicated` |
|---|---|---|
| Write ack, one writer (`cluster.py latency`) | 299 ms p50, 732 ms p99 (far bucket: 818 ms, 2.5 s) | **4 ms p50, 5 ms p99** (far bucket: 4 ms, 8 ms) |
| 64 writers + 16 readers, 3 nodes (`cluster.py users`) | 6,400 events/s, ack p50 937 ms | **87,200 events/s, ack p50 60 ms** |
| Torn reads, lost or duplicated batches | 0 | 0 |

- **Failover on real R2, replicated mode:** exactly-once held. The new leaders recovered 279 and
  18 commits that followers held but the bucket didn't have yet.
- **The 279 exposed a stall:** a PUT that hung for its full 30 s timeout let the lead over the
  bucket grow. That led to two changes:
  - the 256-commit bound above;
  - idle bucket connections are now dropped after 15 s instead of being reused (a proxy or NAT
    that silently forgets idle connections leaves a reused one hanging).

**The cost:**

- A failover waits for recovery. On simulated R2, 25–40 commits were recovered per failover, and
  kill → writes acked again took 10–13 s (10.9 s in durable mode). On real R2 it took 14–16 s,
  plus one 43 s outlier caused by the hung PUT, before the fix.
- Each follower writes every change to local disk once.
- `pondra sql` (serverless) reads the bucket, so in this mode it can be up to one bucket write
  behind the acks.

### 4. Writes from any machine, with or without a cluster (`pondra sql "INSERT …"`)

- **The work runs where the statement runs.** `pondra sql --dir … "INSERT INTO t SELECT …"`
  runs the query on the machine it is started on — local files too:
  `SELECT * FROM 'jan.parquet'`. That machine writes the Parquet into the bucket. The lake only
  has to *record* the new files: one commit.
  - **A leader is running:** the files go to it (`POST /cluster/files`).
  - **Nobody leads:** the process claims a term like a node would, records the files itself,
    and releases the term. A node starting meanwhile waits for it, and four writers at once take
    turns.
  - **A retried job is applied once** (`PONDRA_JOB`, and the job id is kept across a restart).
- **`POST /insert` on any node** now also computes on that node and only forwards the file list
  to the leader. Before, the leader did all the work.
- **A liveness mark in the bucket.** The leader writes `cluster/alive/<term>` every 10 s: one
  PUT, only while a leader runs. It serves machines outside the cluster:
  - **Idle lake:** a node starting on a lake whose leader is gone (the mark is older than 30 s)
    leads at once. It used to wait out a 30 s lease.
  - **Live leader:** a `pondra sql` INSERT never deposes a leader whose mark is fresh. If it
    can't reach that leader — say, a laptop outside the cluster's private network — it fails
    with a clear message instead.
  - **Joining nodes:** a node that has never reached the leader can't take over either while the
    mark is fresh. Members that lost a leader they had been talking to still take over after
    the 5 s lease, as before.

### 5. Small files, compaction and indexes: what exists, and `cluster_by`

What already keeps the file count down, on local disk and object storage alike:

| Where small things come from | What Pondra does |
|---|---|
| Many small writes | A node batches everything it received into one flush per round. Flushes up to 64 KB (1 MB replicated) ride *inside* the catalog commit: no object at all. |
| Log → Parquet every `--tier-secs` | One file per busy table per round. |
| Append tables | Once 8 files are small, they are merged, 8 → 1, up to 4 M rows / 64 MB. So a table has at most ~8 small files plus big ones. |
| Keyed tables (upsert, merge, views) | An LSM: each round writes a file of the newest version per key. At 8 files, a compaction folds them into one. |
| Replaced files, consumed log objects | Deleted after `--retain-secs`; objects no commit ever referenced are deleted after a day. |
| The catalog | SlateDB compacts its own SSTs; the leader flushes its memtable at most every 5 s. |

What makes reads skip data:

- Parquet min/max statistics per row group and per page.
- Keyed tables: sorted by key, bloom filters on the key, small row groups, row-group binary
  search for lookups (ADR-008).
- Caches: footers and decoded row groups in memory; objects on the SSD tier.

**New: `"cluster_by": ["user"]`** for append tables. Folds and merges sort every file by those
columns, with small row groups and bloom filters on them. A filter on them then reads a sliver of
each file, in Pondra and in any engine reading the Parquet. Measured with 8 M rows and 100k users
on local disk (`tools/clustering.py`):

| Query | Plain | Clustered by `user` |
|---|---|---|
| One user | 130 ms | **20 ms** (6.4x) |
| A range of users | 73 ms | **6.5 ms** (11x) |
| 100 users (`IN` list) | 103 ms | 79 ms (1.3x) |
| Full scan | **52 ms** | 91 ms (0.6x): columns that were in arrival order compress worse once rows are sorted by user |

**Still missing, in order:**

1. Partitioned tables.
2. Keyed-table compaction that doesn't rewrite the whole table (tiered or partitioned).
3. Clustering *across* files: Z-order or Hilbert curves, like liquid clustering.
4. Deletion vectors, so Delta and Iceberg readers see keyed tables between compactions.

## Results: freshness, head to head

Each probe is one row written after a quiet spell; every reader polls at once, each in its own
process (`tools/freshness.py`). Times are from the ack to the first read that sees the row. The
node and read-only-node times include the pollers competing for the same two cores; alone, a
node sees a write in ~5 ms (`cluster.py latency`).

**Local disk (memory / SSD against memory / SSD):**

| | Pondra, durable | Fluss (published) |
|---|---|---|
| Write acknowledged | **4 ms** | ms (replicated to TabletServers) |
| Another node / a read-only node | **15 ms** | ms: streaming reads from TabletServers |
| A new `pondra sql` process | **34 ms** (including process start) | — |
| Delta / Iceberg reader (open format) | 36 / 31 ms p50, up to 0.4–0.7 s | its lake tables: `table.datalake.freshness` (default 3 min) + up to 2 tiering rounds |

**Object storage:**

| | Pondra, durable (sim R2) | Pondra, replicated (sim R2) | Pondra on real R2 | Fluss |
|---|---|---|---|---|
| Write acknowledged | 240 ms | **3 ms** | 300 ms durable, **2 ms** replicated (far bucket: 723 / **2 ms**) | ms (TabletServer disks; the bucket is only for tiered data) |
| Another node / a read-only node | **10–14 ms** | **11–14 ms** | **10–13 ms** (either mode; far bucket 9–17 ms) | sub-second (union read: TabletServers + lake) |
| A new `pondra sql` process | 1.9 s | 2.1 s | 2.9–3.1 s (far: 5.5–5.8 s) | — |
| Delta (delta-rs) / Iceberg (PyIceberg) | 2.4 / 3.0 s | 2.2 / 3.4 s | 3.3–3.6 s / 3.9–4.0 s, worst 5.1 / 7.7 s (far: 7.1 / 10 s) | lake tables: 3 min default + up to 2 rounds |

**Reading the tables:**

- **Like for like.** Fluss's own fast path (writes and reads served by its TabletServers from
  local disk) matches Pondra's cluster path: both take milliseconds. Fluss's lake tables match
  Pondra's Delta/Iceberg tables. There Pondra is ~2–3 s behind the ack at the default
  `--tier-secs 2`, against Fluss's 3-minute default. (Fluss can be configured lower, at the cost
  of more lake commits.)
- **The honest caveat.** Pondra's durable ack on object storage is one bucket write. Fluss
  replicates to disks first. `--ack replicated` gives Pondra the same shape, at the same kind of
  cost.
- **`pondra sql` on object storage is not faster than Delta for a one-off query.** Opening the
  catalog costs ~20 sequential requests: 2–3 s on R2 from here; the second run is faster, with the
  catalog cached on local disk. But what it sees is fresher: every write in the bucket, with no
  tiering round to wait for. For fast *and* fresh, join (`pondra serve --reader`), which keeps
  the catalog in memory.
- **Before the idle-connection fix, the open formats' worst cases on real R2 were 13–34 s.**
  They came from tiering rounds that took 8–26 s, where a PUT hung for its full 30 s timeout on a
  reused idle connection (decision 3). With idle connections dropped after 15 s, the worst of 10
  probes was 5.1 s (Delta) and 7.7 s (Iceberg).

## Consequences

- **New AGENTS.md invariants:**
  - 15: replicated commits;
  - 16: open formats and deletes only from durable state;
  - 17: the liveness mark, and one-off writers never deposing a live leader;
  - 3 now reads "only committed data is visible".
- **New tests:**
  - `harness.py serverless`;
  - `cluster.py latency | users | failover --flag ack=replicated`;
  - `tools/open_check.py` (formerly `delta_check.py`, now also Iceberg);
  - `tools/freshness.py`;
  - `tools/clustering.py`.
- **New catalog keys:** `i/<table>` (Iceberg state) and `m` (members). **New bucket objects:**
  `cluster/alive/<term>` and `data/<table>/metadata/`. Nodes keep replica files next to their
  SSD tier (`<cache dir>/<lake>.replica/<addr>/`); they are safe to delete when the node is
  stopped and the lake has a live leader.
- **A leader writes one PUT every 10 s while it runs** (the liveness mark): about 260k requests
  a month, ~$1 at S3 prices.
