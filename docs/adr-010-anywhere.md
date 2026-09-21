# ADR-010: Everyone writes, everything speaks SQL — inbox, attached lakes, SQL writes, Postgres, Python, MCP, tokens

**Status:** Accepted, built and tested (round 9) · **Date:** 2026-09-22 · **Builds on:** ADR-005, ADR-009

## Context

Round 8 left Pondra with one ordering point per lake: every commit goes through the leader. Any
machine could write, but only if it could reach the leader over the network. Round 9 took up:

- The owner's question: can any node, laptop or cluster write to the same backend, with the
  same capabilities?
- The remaining limits in AGENTS.md, as far as possible.
- Where Spark, Flink and Fluss (1.0 shipped on 2026-09-21) still lead: SQL writes, Python,
  standard protocols, access control, compaction cost, change feeds.
- What Fluss, Flink, Spark, Databricks and Snowflake are building next (collected in
  `comparison-spark-flink-fluss.md`). Three directions they share are cheap for Pondra to take
  now: the Postgres protocol, MCP for AI agents, and vectors in SQL.
- Keeping R2 within its 10 GB free tier: test runs now delete their lakes.

## Decisions

### 1. The bucket inbox: write without reaching the leader (`inbox.rs`)

A machine that can reach the bucket but not the leader — another network, another company:

1. leaves its request in `inbox/<id>.<kind>`;
2. touches `inbox/bell`;
3. waits for `inbox/<id>.out`.

The leader looks at the bell once a second: one HEAD request, a cheap request class on S3 and
R2. When the bell has rung, the leader records every request and writes each answer. Three kinds
of request go through the inbox, the same ones HTTP carries:

- the files of a bulk INSERT;
- a log flush (appends, upserts, deletes);
- a new table.

Exactly-once holds as everywhere else: an INSERT is recorded once per job id, a flush once per
`(producer, seq)`. If the leader dies while a writer waits, the writer withdraws its request and,
once no leader is alive, records the write itself. Answers nobody collected are deleted after
an hour.

**What it costs:** ~1 s per write locally (the bell is checked once a second), and a HEAD per
second while a leader runs. **What it gives:** every machine with bucket credentials writes with
the same capabilities as a node, whatever its network.

### 2. Attached lakes: several leaders on one bucket (`--attach name=dir`)

A node or a `pondra sql` attaches other lakes. Their tables read as `name.table`: joins across
lakes, views over them, and `SHOW TABLES` all work. A write to `name.table` is done where it's
issued — the query, the Parquet or the log segment — and recorded by *that* lake's leader: over
HTTP, through its inbox, or by leading it for a moment.

A node keeps an attached lake fresh as a read-only node would: through its leader's commit stream
when that answers, and its own catalog view otherwise.

**The effect is split leadership.** Each cluster leads its own lake (a namespace), every cluster
reads all of them, and every commit still has exactly one ordering point, so millisecond writes
stay millisecond. It also lifts the one-sequencer-per-lake limit: split tables that need more
commits per second across lakes. There is no transaction across lakes; each commit is atomic
within its lake.

### 3. Writes in SQL, anywhere (`write.rs`)

`CREATE TABLE`, `INSERT`, `UPDATE` and `DELETE` run the same way from three places:

- `POST /sql` on any node;
- the Postgres protocol;
- `pondra sql` on any machine.

What each statement does:

- **`CREATE TABLE t (id BIGINT PRIMARY KEY, …) WITH (publish = 'delta,iceberg', cluster_by = 'user', merge = 'total:sum', ttl = 'ts:86400')`**
  - SQL types map to Arrow the way DataFusion maps them.
  - `PRIMARY KEY` makes an upsert table; `merge` makes a merge table.
- **`PRIMARY KEY` tables get a `_deleted` column** (unless merge, or already declared), so DELETE
  works. Writes may leave it out; reads show it as null or false.
- **`INSERT … SELECT` / `VALUES`** (local files too, from `pondra sql`: `SELECT * FROM 'jan.parquet'`).
  - Into an append table nothing streams from, it goes straight to Parquet (bulk).
  - Into a keyed table, or one a view or task follows, it goes through the log. Views and tasks
    see every row either way.
- **`UPDATE t SET … WHERE …` / `DELETE FROM t WHERE …`** on keyed tables:
  - the matching rows are computed where the statement runs, as new versions or delete markers;
  - one log flush commits them;
  - keys can't change; merge tables take only INSERTs.
- **Append tables** take only INSERTs. Copy-on-write DELETE for them is future work.

### 4. The Postgres wire protocol (`--pg 0.0.0.0:5432`, `pg.rs`)

Built on `pgwire` (a protocol library), with Pondra's own Arrow-to-Postgres encoding.

- **Queries and writes** run exactly as `POST /sql` runs them.
- **Protocols:** simple and extended.
- **Results** are text or binary per column, as the client asks.
- **Parameters:** `$1` values are bound as literals, typed from the query plan
  (`WHERE id = $1` makes `$1` a BIGINT).
- **What drivers ask for on connect is answered:**
  - `SET`, `SHOW`, `BEGIN`/`COMMIT`;
  - `version()`;
  - a small `pg_catalog` (`pg_type`, `pg_namespace`, `pg_class`, `pg_database`).

**Tested with** psql 16 (`logs/round9/psql.txt`), psycopg 3 (text and binary), psycopg2, asyncpg, and SQLAlchemy +
`pandas.read_sql`. JDBC, DBeaver and Tableau are untested here: their jars can't be downloaded
in this sandbox. Fluss 1.0 lists the Postgres protocol as future work.

**Nodes never let SQL touch their own disk.** Queries from HTTP, Postgres and MCP run with
DataFusion's read-only options: no `COPY … TO`, no `CREATE EXTERNAL TABLE`, no session DDL. Local
files are read only by `pondra sql`, on its user's own machine.

### 5. A Python client (`python/pondra`)

Pure Python over the HTTP API and Arrow:

- `db.sql(…)` returns `.to_pandas()`, `.to_polars()`, `.to_arrow()` or `.rows()`, or a write's
  outcome;
- `db.append(table, rows | pandas | Polars | Arrow)`, exactly-once: it keeps a producer name and
  sequence, and retries safely;
- `db.lookup(table, key)`;
- `db.watch(table, after=…)`, the change feed, resumable.

### 6. Tokens (`--read-token`, `--write-token`, `--admin-token`, `auth.rs`)

- **Roles:**
  - reading needs any token;
  - appending, INSERT, UPDATE and DELETE need write or admin;
  - tables, views, tasks, `/tier` and the nodes' internal calls need admin.
- **Nodes** call each other with the admin token.
- **`pondra sql` writers** send `PONDRA_TOKEN`.
- **Postgres:** the user name picks the role (`reader`, `writer`, `admin`); the password is that
  role's token.
- **No tokens set** means no checks, as before.
- **Still to come:** per-table grants and quotas.

### 7. MCP for AI agents (`POST /mcp`, `mcp.rs`)

The Model Context Protocol, spoken by Claude, Cursor and most agent frameworks. It uses MCP's
streamable-HTTP transport, answering each call with plain JSON: every tool answers at once, so
there's no event stream. Four tools:

- **`list_tables`:** each table's columns, kind (append, upsert, merge) and key, and a view's SQL.
- **`query`:** one SQL query; the first 1,000 rows as JSON, and the total.
- **`write`:** CREATE TABLE / INSERT / UPDATE / DELETE, checked against the caller's token. An
  optional `job` id makes a retry apply once.
- **`changes`:** what was committed to a table after a position, and the next position. An agent
  can follow a table without holding a stream open.

The same tokens apply as over HTTP. Tested with raw JSON-RPC in `harness.py clients` and with the
official MCP Python SDK (`tools/mcp_client.py`). Fluss lists MCP as future work; Flink Agents,
Databricks and Snowflake offer MCP on their own platforms.

Connect Claude Code with:
`claude mcp add --transport http pondra http://host:8080/mcp --header "Authorization: Bearer $TOKEN"`

### 8. Vector search in SQL

An embedding is a `FLOAT[]` (or `DOUBLE[]`) column. DataFusion's `cosine_distance`,
`inner_product` and `array_distance` give an exact nearest-neighbour search:

```sql
SELECT id, title FROM docs ORDER BY cosine_distance(emb, [0.1, 0.3, …]) LIMIT 10
```

- It works over the log and Parquet alike, and in upsert tables (an embedding per key, replaced
  on update).
- Over Postgres, a driver's array parameter (`%s` with a Python list) becomes the literal.

This is a scan, fine up to millions of vectors per query. Flink 2.2 added `VECTOR_SEARCH`; Fluss
plans vector columns. An approximate index (HNSW or IVF) is future work.

### 9. Keyed tables: compaction without full rewrites, TTL, a change feed

- **Size-tiered compaction** (`tier::run`). Once 8 files pile up, the newest run of files of
  similar size is merged, going back while each older file is at most twice what's newer:
  - the run is consecutive in `ord`, so no older version jumps a newer one;
  - delete markers are kept.

  The whole table is rewritten only when the run reaches the oldest file, i.e. when the newer
  data has grown to about half the base. Measured with 2 M keys and 60 rounds of 20k updates:
  **17.9 MB written, against 53.4 MB for a full rewrite every 8 files** (3x less), 7 live files,
  every key right.
- **TTL:** `ttl = 'column:seconds'` on a keyed table.
  - Reads (SQL and `/lookup`) hide rows whose timestamp or date column is older than that.
  - Full compactions drop them.
  - With a GROUP BY view keyed by `date_bin(...)`, old windows expire. (Cached results of such
    a table can show an expired row until the next commit.)
- **A change feed:** the log is one.
  - `--changelog-secs N` keeps it N seconds.
  - `/watch/{t}?after=K&marks=true` replays every change since K, upserts and deletes included,
    then follows live, marking positions to resume from.

### 10. Replicated acks, hardened

- **`--fsync`:** followers flush each copy to disk before acknowledging it, so an acked write
  also survives a follower's power loss. It cost nothing measurable here: ack 4 ms p50 / 9 ms
  p99 locally.
- **`--replicas 3`** tested: ack 4 / 7 ms, `users` clean, 3 × `failover` with fsync all exact.

### 11. Event-time windows, as far as they go

A GROUP BY view keyed by `date_bin(INTERVAL '1 minute', ts)` is an event-time tumbling window,
updated incrementally on every flush. Late rows update their window, as Flink's upsert output
does. TTL drops old windows.

Not yet: watermarks that close a window and emit it once (append-only output), session windows,
and `MATCH_RECOGNIZE`.

## Results

`harness.py clients` passes 18 checks against a 2-lake setup with tokens on:

- tokens refuse missing and weaker tokens;
- SQL writes;
- pandas, Polars and list appends;
- lookup;
- change-feed replay with a delete in it;
- psycopg 3 text and binary, psycopg2, SQLAlchemy + pandas, asyncpg;
- wrong password refused;
- a cross-lake join after writing through the attached lake's leader;
- an inbox write (1.0 s), retried as a duplicate;
- vector search, by SQL and by a Postgres array parameter, after a DELETE on a table declared
  without `_deleted`;
- `COPY … TO` and `CREATE EXTERNAL TABLE` refused;
- MCP: handshake, the four tools, a write refused to a read token, a retried write applied once,
  and the change feed with the delete in it.

The full round-9 numbers are in `prototype-status.md`.

## Consequences

- **New invariants** (AGENTS.md):
  - 18: inbox requests carry the same exactly-once keys as HTTP; only a leader answers them.
  - 19: squash merges only consecutive runs and keeps delete markers; only a full compaction
    drops them and expired rows.
  - 20: a write to an attached lake is recorded by that lake's leader, never ours.
- **New dependency:** `pgwire` (server API only, no TLS). MCP needed none. The binary grew by
  0.7 MB, to 90.7 MB (30.5 MB gzip, 17.2 MB xz); idle memory is 44 MB.
- **Test runs delete their lakes when they finish** (`harness.new_lake`). `tools/clean_bucket.py`
  keeps a bucket down to its newest N lakes.
