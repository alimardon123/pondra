# Pondra: the shortest path to 1.0

**Date:** 2026-10-03 (round 34 begins; rewritten from the 2026-09-28 plan, whose history is in git,
`prototype-status.md` and the ADRs) · **Status:** the order is my recommendation; the decisions at
the end are the owner's.

**The bar** (the owner, 2026-10-03): reach 1.0 faster and more productively, with everything before
it high quality, stable, workable, fully featured and true to the design principles (AGENTS.md).
So from here: no new surface unless a bar below needs it, every round leaves every angle better
(principle 8), and each part merges to main as soon as CI is green; a version is cut only when its
round is complete.

## Where Pondra stands

Rounds 17 to 32 are released (0.32.0); round 33 is built but for the 24-hour soak and environments.
One binary is a streaming store, a lakehouse and a SQL engine: exactly-once ingest from eight doors,
Parquet in your bucket, SQL spread over any number of nodes, flows of views, windows and as-of
joins, other engines reading and writing its tables, users and grants, TLS, transactions, a
console, a workspace of files and tasks, time travel, a query history. Measured numbers are in
`prototype-status.md`; what is promised is below.

## The scorecard

| Angle | Today (measured) | The bar for 1.0 | Round |
|---|---|---|---|
| **Right answers** | sqllogictest 98.0% of records not named an exception (every exception named); TPC-H 22/22 on 1, 3 and 6 nodes; TPC-DS 99/99 equal to DuckDB on one node, and on three; 100,000 random queries: one node == three == DuckDB, but for 26 that DataFusion refuses; 69 of 73 everyday features of DuckDB, Snowflake, Postgres and BigQuery (the 4 left wait on the SQL registry: decision 2) | the everyday features all there | 34 |
| **Never loses data** | kill -9, injected crashes and leader kills: every event once; every release's lake since 0.22 opens; a rolling upgrade; `UNDROP`, `AT (…)`, `RESTORE`, `CLONE` | also a 24-hour soak on R2: 0 lost, memory flat | 33 |
| **Safe** | users and grants to a column, TLS on every door and between nodes, an audit log, quotas, every door fuzzed, a request's panic an error | also a security review | 37 |
| **The same from every door** | the doors matrix; `BEGIN`…`COMMIT` everywhere; Postgres's error codes | holds | every round |
| **Fast: analytics** | TPC-H SF1 (4 cores): from files 2.18 s (DuckDB 1.5 2.19 s), from memory 1.22 s (DuckDB's own tables 1.05 s); ClickBench (10 M rows) from memory 8.22 s (DuckDB 7.18 s) | from memory ≤ DuckDB's own tables at SF1 and SF10; ClickBench published | 34 |
| **Fast: points and writes** | key lookup 0.19 ms (`/lookup`), 0.29 ms (Postgres port); pgbench's balances right | holds | every round |
| **Scales out** | GitHub's runners over the internet: 3 nodes 8% faster than one; never in one data centre | TPC-H SF100 time falls 1 → 3 → 6 machines, faster than Spark on the same VMs | 34 |
| **Streaming** | Nexmark q1, q2, q5, q7, q11: 2.3× Flink 2.3 on 2 vCPUs | all of Nexmark against Flink; Fluss head to head | 34 |
| **Easy to run** | image, compose, Helm, `pondra service`, drain, `pondra.history`, slow-query log, traces | also an upgrade guide | 37 |
| **Fits in** | psql, dbt, SQLAlchemy, JDBC, ODBC, ADBC, Npgsql; Spark and PyIceberg write | also Metabase, Superset, Grafana, Tableau, DBeaver checked; Postgres and MySQL attached with their changes streamed in; sinks; Kafka partitions | 36 |
| **Runs anywhere** | Linux, macOS, Windows; pip, npm, installers, a container | also in-process in Python and Node, and in the browser; Homebrew, winget; signed binaries | 35, 37 |

## The rounds left

| Round | Theme | What you'd see at the end |
|---|---|---|
| 33 (closing) | Run it for years | **Left:** the 24-hour soak on R2 (the owner's machine, `tools/soak.py --hours 24 --s3`); environments (ADR-047, approved 2026-10-03, built in its own thread: a whole database cloned without a copy, with pins and `REFRESH`, then `pondra plan` and `deploy`); the console's History reading `pondra.history` (the console's thread). 0.33.0 when all three are in. |
| 34 | SQL as people write it, and scale proven | **SQL parity** (from `designs/scripting-design-review.md`; rewrites where SQL comes in, no engine change). **Built** (`friendly.rs`, invariant 225): `PIVOT`/`UNPIVOT` (both spellings), `COLUMNS('re')`, `SELECT * RENAME`, `ORDER BY ALL`; list comprehensions, lambdas (`x -> x + 1`), struct field access; `arg_max`/`max_by`, `list()`, `string_split`, `::json` and `json_extract`; DuckDB's `ASOF JOIN … ON a.t >= b.t`, `SUMMARIZE`, `USING SAMPLE` and a `TABLESAMPLE` that samples, `FETCH FIRST`, a select alias reused in `WHERE`. **Left:** `COMMENT ON` comes with the statement registry (`pondra.objects`, `SHOW CREATE`, `CREATE OR ALTER`: the SQL language review's next PR, picked 2026-10-03); `CREATE TYPE … AS ENUM`, `CREATE SEQUENCE` (identity columns) and `CREATE INDEX` (a no-op) build on it; `UNIQUE`. **Proof:** done: 100,000 random queries (one node == three == DuckDB but 26 refusals DataFusion makes; the five bugs they found fixed: invariant 226), TPC-DS 99/99 on three nodes; left: `INSERT … SELECT` and `CREATE TABLE AS` written by every node at once. **Scale** (machines: decision 1): 1 → 3 → 6 machines in one data centre, SF100 against Spark, all of Nexmark against Flink, Fluss head to head, ClickBench's 100 M rows on its standard machine submitted and ClickHouse beside DuckDB in `singlenode.py` (`clickhouse-local`; `designs/realtime-olap-comparison.md`), hundreds of clients on dashboards while writes land (`serve_bench.py`) with the cheap paths that shows (a plan kept per statement shape, small aggregates on the key-lookup path), and what they find fixed. |
| 35 | In-process and in the browser (the owner, 2026-10-03: before 1.0, built to a high bar) | One engine, three places, through the core split (B1, in a window with no side branches open): `pondra.open(…)` in Python and Node without a server, Arrow straight into pandas and Polars (B2); **in the browser** (B4): DataFusion in WebAssembly, on Web Workers where the page is cross-origin isolated; a lake read straight from its bucket at a commit a node publishes (its files and log tail), answers equal to a node's; files dropped on the page (Parquet, CSV, JSON, Arrow) and tables kept in the browser's own storage (OPFS) between visits; a local table joined with a lake's, the lake's part run on a node (B5); the console served as a static page, working with no node at all; JavaScript frames with the Python frames' names; live queries; one npm package for the page and Node. **Gates:** TPC-H SF1 in a browser against DuckDB-WASM, the download's size, the console's budget (invariant 149). |
| 36 | Fits in | Postgres and MySQL attached and their changes streamed in (G6: a feed whose place, the replication slot's commit LSN and transaction id, is the exactly-once `(producer, seq)` as Kafka feeds' offsets are; a table that meets a change it can't apply is set aside alone, its changes kept to replay once it is synced again, while the others flow; `designs/embrasure-flow-findings.md`); a keyed table published to Delta or Iceberg finds the older versions of a round's few new keys as an `IN` list, so its files' key filters skip most of them (`tier::shadow`; a key index only if that isn't enough, measured first); sinks (G7); Glue and S3 Tables attached (AWS's signed requests) and the credentials a REST catalog vends (Polaris, Unity, Snowflake's); Kafka partitions; measures in views (E13, its first phase: one definition of every number for BI tools and agents); Metabase, Superset, Grafana, Tableau and DBeaver checked; a SQLAlchemy dialect and a dbt adapter packaged. |
| 37 | 1.0 | **First, everything reviewed again** (the owner, 2026-10-03), before anything is promised: the known limits (AGENTS.md's list, `prototype-status.md`), every mistake and bug found so far (each round's "found" lists, the reviews in the project's files, the issues) checked for its kind elsewhere, and every door and mode against what the docs say it does; each one fixed or improved, or kept as a limit with its reason written down. **And every way in tried as a new user would** (the owner, 2026-10-03): the shell (a short banner, colour, answers drawn as DuckDB's are), the command line, the console, the Python and JavaScript clients, the HTTP API, Postgres and the error messages, each made beautiful, ergonomic and simple from the first try. **Then** what stays stable (the lake format, SQL, the HTTP API, the clients, the command line) and how things are deprecated; a security review; signed binaries, Homebrew and winget (ADR-041, waiting on the owner's accounts and certificates); an upgrade guide; the docs complete and reorganized (each thing said once, a full reference per API, guides with a tab per way, current console pictures); every document, the site and the code reviewed against what was decided. |

**Every round:** CI on five platforms; the gates (`tools/gates.py`: sqllogictest, TPC-H SF1 against
DuckDB, Postgres, Nexmark) hold or improve; an engine or read-path change keeps TPC-H's 22 and
TPC-DS's 99 answers and is no slower; a new feature works from SQL, Python, JavaScript, Postgres and
MCP alike or is refused by name; its ADR when it is a design.

**Why this order.** SQL first because it needs nothing from the owner and every later promise is
about the finished SQL; the scale runs start the moment machines exist and their fixes gather in
34. In-process and the browser wait for the format and APIs to settle, and share the core split;
"fits in" follows, so what BI tools and databases meet is the finished engine. 1.0 last: the promises cover what the rounds before proved.

## After 1.0

Depth by evidence, then the platform on top, each a part behind a small surface (principle 9):

- **F, streaming and flows:** timers, `MATCH_RECOGNIZE`, Top-N per key, a watermark per partition,
  as-of joins in views that wait for the looked-up table, a view's state split by key ranges;
  `avg`, `count(DISTINCT)`, outer joins kept up in flows, and views refreshed whole when they can't
  be (`WITH (refresh = …)`).
- **F, distributed and storage:** adaptive stages, raced stragglers, a `LIMIT` in a subquery,
  order-keeping shuffles, hot keys on both sides, keyed compaction by key range.
- **I1:** `INSTALL`/`LOAD` extensions as WebAssembly components (ADR-031, proposed).
- **J1:** the server's catalog (ADR-032 §9). **J3, J6:** reports and dashboards of Pondra's own,
  snappy at any scale. **J4:** connections (`CREATE CONNECTION`). **J5, J7:** a workspace exported
  whole, and ETL as code, a canvas and YAML over one definition, as plugins.
- **E12:** sharing with other companies (ADR-046, proposed). **E13**'s rollups, before J6.
- **From the OLAP engines** (`designs/realtime-olap-comparison.md`): a query over a table answered from a
  materialized view that covers it, without naming it; HLL and quantiles as merge states for merge tables
  and adding-up views; `VARIANT` as a real type with its paths as columns (Parquet's shredding) and a
  text index.
- A vector index, a managed service.

**One ecosystem (the owner, 2026-10-03).** Every object SQL makes is a catalog entry that every
other part (the console, the clients, the HTTP API, the tools still to come) reads through the same
surface, so a new tool plugs into what is there instead of keeping its own copy.

## The tracks' open items

The IDs other documents cite. Done items are gone from this list (their ADRs and
`prototype-status.md` keep them).

| # | Item | Round |
|---|---|---|
| A5 | A measured lite build (Cargo features for Kafka, Flight, Postgres, AI, the Iceberg catalog) | after 1.0 |
| B1, B2 | `pondra-core` split from the server; `pondra.open("s3://…/lake")` in process | 35 |
| B4, B5 | A browser Pondra; a local table joined with a lake's, the lake's part on a node | 35 |
| C1, C2, C3 | 1 → 3 → 6 machines; SF100 against Spark; all of Nexmark against Flink | 34 |
| C4 | The 24-hour soak on R2 | 33 |
| D2 | Random queries: one node == three == DuckDB, 100,000 of them (done: `random_sql.py`) | 34 |
| E2 | BI tools checked (Power BI Desktop, Tableau, DBeaver, Metabase, Superset, Grafana) | 36 |
| E12 | Sharing (ADR-046, proposed) | after 1.0 |
| E13 | Semantic models as measures in views (approved 2026-10-03; `designs/semantic-models-and-dremio.md`): `sum(x) AS MEASURE m` in a `CREATE VIEW`, queried as `MEASURE(m)`, right at any grain; BI tools' `sum(m)` gives the measure; MCP `list_metrics`; Apache Ossie YAML out and in. Then rollups kept in their source's commit and read where they give the same answer; then rollups proposed from `pondra.history`. Proof: `harness.py measures` | 36 (then after 1.0, before J6) |
| F | Depth by evidence (above) | after 1.0 |
| G6, G7 | Databases attached with their changes streamed in; sinks | 36 |
| I1 | Extensions (ADR-031, proposed) | after 1.0 |
| J1, J3–J7 | The platform on top (above) | after 1.0 |

## What only the owner can decide

1. **Machines for round 34** (the owner will try, 2026-10-03): six 8-vCPU VMs and a client in one zone for a few hours at a time
   (`tools/cloud/gcp.sh up 6 --local-ssd`; a thread drives them through Remote Control on the client).
2. **The SQL registry** (the SQL language review's card): `CREATE TYPE`, `CREATE SEQUENCE`,
   `CREATE INDEX` and `COMMENT ON` wait for it, so they are built as registry entries if it says so.
3. **Publishing:** Homebrew, winget and signing wait on the owner's accounts and certificates.
4. **An `INSERT` of a key that exists:** it replaces the row today (an upsert, as Fluss and Paimon
   do). My recommendation: keep it, and add `WITH (on_duplicate = 'error')` for Postgres's
   behaviour, with `ON CONFLICT` working in both.

## What not to do yet

- No new surface before 1.0 that no bar needs; new ideas go to "After 1.0".
- No fully static musl build (its allocator slows multi-threaded Rust); the old-glibc build reaches
  as far.
- No DuckLake as Pondra's catalog: it needs a database, and Pondra needs none.
- No enterprise governance beyond users, grants, TLS and the audit log until someone asks.
