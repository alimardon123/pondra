# ADR-008: Serving reads at Lakehouse//RT speed, on the same binary

**Status:** Accepted, built and tested (round 7) · **Date:** 2026-09-21 · **Builds on:** ADR-006, ADR-007

## Context

Databricks' Lakehouse//RT (engine "Reyden", beta since June 2026) is a serving layer over
Delta/Iceberg. Its published claims:

- "response times as low as 10 ms on smaller datasets and sub-100 ms performance on larger ones";
- "sub-100 millisecond latency at 12,000 queries per second on standard analytical benchmarks";
- the workloads are dashboards, app serving, observability and agents: joins, aggregations and
  window functions, not only point lookups.

Pondra, measured the same way on one 2-vCPU box at the end of round 6:

| Workload | Round 6 |
|---|---|
| Point lookup (2 M keys) | 9–11 ms, ~200 lookups/s at most, all planning and Parquet decoding |
| Dashboard aggregate over a keyed table that wasn't compacted into a single file | ~420 ms |
| Many clients asking the same dashboard question | each request recomputed it |

## Decisions

### 1. Point lookups don't use SQL (`serve.rs`)

An upsert table is an LSM, so a lookup reads it the way an LSM does:

1. The log tail, newest row first (decoded segments are already cached per node).
2. Then the files, newest first. Parquet statistics narrow each file to the row group that can
   hold the key.
3. That row group is decoded once and kept in memory, up to half the read cache.
4. Since files are written sorted by key, a binary search finds the row. Sortedness is checked
   once when the group is loaded; older files fall back to a scan of the group.

`GET /lookup/{table}/{key}` takes this path. So does a SQL query of exactly the shape
`SELECT … FROM <upsert table> WHERE <key> = <literal>`. The query is recognised by parsing it
and checking that it prints back as nothing but those parts. Anything else runs as SQL.

### 2. Upsert tables read as anti-joins, not a group-by over every row

- Every file holds one row per key.
- So the current table is: the newest source (the log tail, deduplicated), then each older
  source minus the keys any newer source has (`LEFT ANTI JOIN` on the key).
- The newer sources are usually small, so each older file is streamed past a small hash table.
- The old plan grouped all rows by key with `first_value(… ORDER BY _ord)`: 650 ms of hashing
  for 2 M keys, on every query.

### 3. Results are reused while nothing changed (`server.rs`, `Results`)

- **What counts as "nothing changed":** every catalog commit moves a version. On the leader it
  is the last durable commit; on a node holding the catalog in memory it is the last streamed
  commit.
- **Reuse:** a result is reused for the same query text at the same version. Queries that ask
  for the time or randomness are never reused.
- **Many clients, one computation:** identical queries share one computation at a time. It covers
  every request that arrived before it started, so nobody gets an answer older than the lake
  they arrived at. The rest queue for the next one.
- **Opt-in staleness:** with `?stale_ms=N`, a caller accepts a result up to N ms old. While one
  request recomputes an expired result, the others keep getting the previous one. This is for
  dashboards over tables that change every few milliseconds. The default stays exact.

### 4. Arrow out

`POST /sql?format=arrow` returns an Arrow IPC stream, which pandas, Polars and DuckDB read
without parsing JSON.

## Results (one 2-vCPU box, 2 M keys, load generator on the same box; `serve_bench.py`)

| | Round 6 | Round 7, leader | Round 7, read-only node | Round 7, read-only node on R2 (500k keys) |
|---|---|---|---|---|
| `/lookup`, 1 client | 11.2 ms p50 | 0.22 ms | **0.14 ms** | **0.14 ms** |
| `/lookup`, 32–64 clients | ~200/s | 20,000/s, p99 7.4 ms | **35,600/s**, p99 6.4 ms | **31,800/s**, p99 4.0 ms |
| SQL point query, 32–64 clients | ~280/s | 15,800/s | 21,100/s | 18,400/s |
| Dashboard aggregate, a new query each time | 419 ms | 6–42 ms | 43 ms | 17 ms |
| Same dashboard, 32 clients | ~2/s | 23,300/s, p99 5 ms | 21,300/s | 19,500/s |
| Same, while writes land every few ms: exact | 15/s, p50 2.3 s | 230/s | 842/s | 12,400/s |
| Same, with `stale_ms=1000` | — | 10,900/s | 12,400/s | 17,100/s |

Against Lakehouse//RT's published numbers:

- **Latency:** Pondra's point reads (0.2–3 ms) and repeated dashboards (sub-ms) beat "10 ms on
  small data".
- **Throughput:** 20–36k requests/s on two cores beats 12k QPS. Their cluster size isn't stated.
- **Freshness:** Pondra serves the log tail in every read, milliseconds after the write.
- **Where Pondra is still behind:** a *new* analytical query over a big table costs what the
  scan costs (35–42 ms for 2 M rows here; TPC-H SF1 queries 60–600 ms). RT's "sub-100 ms at
  12k QPS on analytical benchmarks" implies a much larger engine, a lot of caching, or both.
  Scaling that is read-only nodes (each has the cache), not a bigger single node.

## Consequences

- Row-group and footer caches cost memory: decoded row groups take up to half of
  `PONDRA_CACHE_MB` (1,024 MB by default, so 512 MB).
- The anti-join plan relies on "one row per key per file". Folds and compactions guarantee it
  (see AGENTS.md invariant 5), and the harness `upsert` test checks SQL and `/lookup` against a
  model after every batch.
- `stale_ms` is opt-in. It trades the "never older than you've seen" rule for throughput, per
  request.
