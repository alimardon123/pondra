# ADR-019: A database you can shape — schemas, three-part names, and DDL in SQL

**Status:** Accepted, built and tested (round 18) · **Date:** 2026-09-28 · **Builds on:** ADR-011 (open doors: `--attach`), ADR-017 (views that emit), ADR-018 (the shell)

## Context

The owner tried round 17's shell on Windows and found SQL a database user expects that Pondra
didn't take:

- **No schemas.** `CREATE TABLE dbo.t2 …` failed with "no lake dbo": a two-part name could only
  mean an attached lake. Every table lived in one flat namespace.
- **No `CREATE VIEW`.** Views existed only as streaming views, made with `POST /views/{v}`.
- **No `DROP`** of anything, and no `CREATE TABLE … AS SELECT`.
- **`UPDATE` on an append table** was refused (keyed tables only).

They asked for three things: schemas, with the lake as the database, so that a table is
`lake.schema.table` and an attached lake is one more database; `UPDATE`, `DELETE` and `MERGE` on
every table; and system columns (a row id like Postgres's `ctid`, the time a row was written, a
version). Streaming has to keep working through all of it. And, from Postgres's worst habit:
queries across databases, as SQL Server allows on one server.

This round does the first, and the DDL that goes with it. `UPDATE`/`DELETE`/`MERGE` on every
table and the system columns are next round's, together (see "What is still open": the row id is
what both need).

## Decisions

### 1. A lake is a database; its tables live in schemas

A table belongs to a schema: `public` unless one is named. Inside the lake, a table of schema
`s` is called `s.t`, and one of `public` just `t`, as before schemas existed.

- **Everything keyed by a table's name works unchanged.** The catalog (`t/{name}`), the log, the
  files (`data/{name}/…`), Delta and Iceberg, Kafka topics, the HTTP API (`/append/dbo.t`), Flight
  paths and the change feed all take the name inside the lake. Round-17 lakes open as they are:
  every table they hold is in `public`.
- **A schema is a catalog entry** (`ns/{schema}`). `CREATE SCHEMA [IF NOT EXISTS] s`; `DROP
  SCHEMA [IF EXISTS] s [CASCADE]` refuses a schema that holds anything unless `CASCADE`, which
  drops its views, then its tables. `public` can't be dropped.
- **Its name.** The lake's own name, for three-part names, is its folder's (or prefix's) last
  part, lower-cased: `./lake` is `lake`, `s3://bucket/sales` is `sales`. DataFusion's default
  catalog is now called that (it was `datafusion`), so `SHOW TABLES` and `information_schema`
  show it.

### 2. How SQL names resolve

Names are read as SQL reads them: unquoted parts in lower case, quoted ones as written.

- `t` is `public.t`.
- `s.t` is schema `s` of this lake; if no schema here has that name, it is attached lake `s`'s
  `public.t`. That keeps round 11's `sales.orders` (a table of the lake attached as `sales`)
  meaning what it meant.
- `l.s.t` is lake `l`: this one, by its name, or an attached one.
- **Attached lakes are catalogs.** `--attach Sales=…` is `sales` (lower-cased, as SQL would
  read it). A lake can't be attached under this lake's own name.
- **What a name may hold.** Letters, digits, `_` and `-`. Dots separate the parts; quotes,
  slashes and spaces would get in the way of paths and SQL. `"bad name"`, `a.b.c.d` and `"x.y"`
  are refused.

### 3. Other lakes, attached in SQL: cross-database queries

`--attach name=dir` attached another lake when a node started, and nothing else could; the shell
couldn't at all. Now `ATTACH 'dir' AS name` and `DETACH name` do it from SQL, DuckDB-style,
from any client.

- **An attachment is a catalog entry** (`a/{name}`: the lake's full path or bucket URL). So every
  node of the cluster attaches it within a second, a node that restarts attaches it again, and
  `pondra sql` reads it too. `--attach` still works, and its lakes can't be detached.
- **Checked when made:** a lake must be there (a catalog in it); the name can't be this lake's,
  or a schema's here; a folder is kept as its full path.
- **Reads and writes across lakes.** `SELECT … FROM sales.eu.orders JOIN mylake.public.customers
  …` plans as one query. `INSERT INTO sales.t SELECT … FROM t` goes to that lake's leader, from a
  node or from `pondra sql`. A write to two lakes is two commits, not one transaction.
- **A remembered answer depends on the attached lakes it reads.** The result cache answered a
  repeated query until *this* lake's next commit. A query over an attached lake could get an
  answer from before a write there, or from before a `DETACH` (the check that found it). Its key
  now also holds the versions of the attached lakes the query (or a view it reads) names; a lake
  attached without a commit stream has no version, and queries over it aren't remembered.
- The shell's `.databases` lists this lake and the attached ones.

### 4. DDL is the leader's, sent from anywhere

`CREATE`/`DROP SCHEMA`, `DROP TABLE`, `CREATE VIEW`, `CREATE MATERIALIZED VIEW`, `DROP VIEW`,
`ATTACH` and `DETACH` are one message, `Ddl`, that the leader carries out under the lake's lock, like `CREATE TABLE`.
A follower forwards it (`POST /cluster/ddl`); a machine that can't reach the leader puts it in
the bucket's inbox (`pondra sql`); Postgres, Flight SQL and MCP send SQL down the same path. All
of it needs the admin token.

### 5. `DROP TABLE`

- **The table leaves the catalog at once**, and its Delta and Iceberg copies are unpublished.
  Its files are then orphans: the hourly orphan collection deletes those more than a day old, as
  it does files of crashed jobs.
- **Refused while something uses it:** a view (stored or materialized) or a task that reads it,
  or when it is a materialized view's own table (`DROP MATERIALIZED VIEW` drops those). Which
  tables a view reads comes from parsing its SQL; if that fails, a word match stands in, so a
  drop is refused rather than a view broken.
- **A new table of the same name starts empty.** A table's log rows are the ones after its
  `tiered` position. A new table used to start at 0, so after `DROP TABLE t; CREATE TABLE t`,
  rows of the old `t` still in the log came back. A new table now starts at the log's current
  end (`lake.visible()`).

### 6. `CREATE TABLE … AS SELECT`

The columns are the query's (planned on the lake, types as a view would get them); then the rows
go in as an `INSERT … SELECT`. It is two steps: if the insert fails, the empty table stays.

### 7. Views: `CREATE VIEW` stores a query; `CREATE MATERIALIZED VIEW` is the streaming view

- **`CREATE [OR REPLACE] VIEW v AS …`** keeps the query (`q/{v}`). A query that names it gets it
  as DataFusion's `ViewTable`, planned over the tables as they are at that moment, and views of
  views work. It is planned once when created, so a view that doesn't plan is refused. Its name
  can't be a table's or a materialized view's.
- **A query over a view spreads over the nodes like any other.** The coordinator counts the
  view's tables among the query's, and each node plans the view again after putting its share of
  the tables in place, so the view reads that share. Without the second step the view read
  whole tables on every node: `SELECT count(*) FROM v` spread over three nodes counted each row
  three times.
- **`CREATE MATERIALIZED VIEW v [WITH (window = 'w', size_secs = 60, lateness_secs = 10)] AS …`**
  is `POST /views/{v}?window=w&…` in SQL: the streaming view of ADR-002 and ADR-017, updated with
  every flush, with windows or sessions. Options it doesn't know are refused (they were ignored
  over HTTP). **It follows the rows written from then on**, as the HTTP one always did; the reply
  says so. Rows already in the source are not in it (see "What is still open").

### 8. Every door lists the schemas

- **Postgres:** `pg_namespace` has the schemas, and `pg_class` their tables and views, for
  drivers that look there; `current_database()` is the lake's name.
- **Flight SQL:** catalogs (the lake), schemas, and tables and views in them; ADBC's
  `adbc_ingest(…, db_schema_name="dbo")` writes into that schema.
- **Iceberg REST:** namespace `default` is `public` (as before), each other schema is its own,
  an attached lake `l` is `l`, and its schema `s` is `["l", "s"]`.
- **MCP** lists views too; **the shell's** `.tables` lists lake, schema, name and kind.

### 9. A follower that finds no catalog waits for its leader

The owner's second 3-node cluster-bench run on R2 ended with no results: one node exited at
start with "failed to find latest transactional object (e.g. manifest) version". All three
started on a new lake at once; one won the lead, and another opened the catalog to follow it
before the leader had made it.

A follower that finds no catalog now waits a second and starts over (`cluster::restart`): by
then its leader has made the catalog, or, if that leader died before making it, its mark goes
stale (30 s) and this node takes the lead. `cluster.py race` now gives no node a second start
(the harness used to restart a node that died while starting, which hid this), and puts a
leader's mark on a new lake with no catalog behind it: the node leads after 30 s, and exited at
once before.

### 10. Memory on Windows and macOS

The node read the machine's memory and its own from `/proc`, which only Linux has. On Windows the
query memory limit fell back to a fixed 4 GiB, and `/metrics` reported 0 bytes resident. Where
there is no `/proc`, both now come from the OS (the `sysinfo` crate, already built for SlateDB).
`tools/smoke.py` runs on each OS in the build workflow: the shell, SQL with a schema and a view,
and both memory figures.

### 11. The cluster bench measures what crosses the network

Round 17's first 3-node run on GitHub's runners gave every answer right, but took 33.2 s against
one node's 18.6 s. Before changing anything, the bench now measures why:

- `pondra_wire_bytes_total` (bytes nodes sent each other) and
  `pondra_shuffle_wait_seconds_total` (time spent waiting for other nodes' pieces), per query in
  `results.json`;
- the network between the runners (Tailscale ping, and a 256 MB copy);
- the reason for every node restart, in its log.

### 12. Two failures on GitHub's runners

- **The test job: sorts on a wider machine than the sandbox.** `harness.py scale` gives a node 50
  MB for queries, so a sort of 3 M rows must spill. On GitHub's 4-core runner it failed with
  "Resources exhausted … SortPreservingMergeExec". Each of the 4 partitions' sorts keeps
  DataFusion's 10 MB aside to merge what it spilled, which left almost nothing for the merge of
  the partitions above them. Smaller reserves don't help: then a sort can't merge its own spills
  ("Not enough memory to continue external sort"). So a node now runs at most one partition per
  24 MB of query memory (and at least 2). Nothing changes at ordinary limits: a third of 8 GB
  allows 111. `PONDRA_CORES` makes a node plan as if it had that many cores, and the check runs
  as 4 cores on any machine (and passes as 16).
- **The bench's driver gave up on a node that was starting.** It asked the first node for the
  member list and stopped at its first refused connection. It now asks every node that answers,
  and waits up to ten minutes for all of them.

## What it measures

One 2-vCPU box (`logs/round18/`).

| | |
|---|---|
| `harness.py schemas` (new, 14 checks: names, attached lakes, CTAS, views, spreads over views, materialized views, drops, clients, `ATTACH`/`DETACH` on a cluster) | all pass, on local disk, the R2 simulator and real R2; fails on round 17 (no `CREATE SCHEMA`), and each invariant it holds fails without its fix |
| `cluster.py race` (strict, plus a leader that never made the catalog) | 1 leader of 3; the next node leads 30 s after the dead leader's mark (41 s on real R2); round 17's code exits at once |
| `harness.py scale`'s memory check as a 4- and a 16-core machine | passes; with 10 MB sort reserves on 4 partitions (round 17), "Resources exhausted", as on GitHub's runner |
| Query latency, one node, round 17 → 18 (p50: a point lookup; `count(*)` over 200 k rows; a scan of 20 k; `SELECT i`) | 0.25 → 0.25 ms; 3.1–3.4 → 2.9 ms; 1.6–1.7 → 1.5–1.8 ms; 0.61–0.63 → 0.60–0.65 ms: the same |
| The shell on Windows (the owner's laptop, round 17's build) | `SELECT 1` 84 ms the first time, 1–2 ms after |
| The rest of the suite, local disk (`harness.py all`, users ×2, failover ×3, race, isolate, spread, latency, open formats, as-of, skew, shuffle spill, stream, freshness, crash at 9 M events, smoke) | all pass: users 86.8–88.2 k events/s, 0 inconsistent reads; failovers 4.4–5.0 s, state == view == model; event → view row on another node 6 ms p50 |
| The R2 simulator (before `ATTACH` and the partition cap; `schemas` and `race` again on real R2 after) | schemas, race, clients, windows, failover and users with replicated acks, crash (3 × 45 k events, 0 lost or duplicated), Flight: all pass |
| The portable binary | 97.0 MB (32.8 MB gzip), glibc 2.17 at most |

## What this costs

- **Two more catalog scans per query** (schemas and stored views), next to the one for tables.
  Query latency didn't move (above).
- **Every node looks for new attachments once a second** (one catalog scan).
- **Fewer partitions under a small memory limit**: a partition per 24 MB, so only a limit
  under 24 MB per core changes anything.
- **A view is planned for every query that names it**, and again on each node of a spread query.

## What is still open

- **`UPDATE`, `DELETE` and `MERGE` on every table, and system columns** — next round. An append
  table has no key, so changing a row means knowing which row it is: a row id assigned when the
  row commits (as Iceberg v3's row lineage does), which is also the `ctid`-like column the owner
  asked for. The commit time and a version come with it. Streaming has to see an update as a
  change (the change feed, views, Kafka consumers), which is the design work.
- **Materialized views start empty.** Filling one from the rows already there, exactly once
  alongside the rows arriving, needs to know which commits the view has seen; followers pack
  views' rows with the catalog they have, so the boundary isn't one commit. Until then, create
  the view before the data, or `CREATE TABLE … AS` for the rows so far.
- **`ALTER … RENAME`**, `ALTER SCHEMA`, grants per schema, and `search_path`.
- **Speed across machines.** The fixes wait for the measurements of a run that completes.
