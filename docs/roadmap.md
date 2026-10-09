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

## The promises scorecard

The owner's goal (2026-10-09): the product itself proves every promise, as a working alternative
to Spark, Flink, Fluss and DuckDB, without flaws. Each row is a promise, what it answers, what
proves it today (a check, a benchmark or a mode, with its log), and what doesn't yet. **1.0 ships
when the "not proven" column is empty**; moving a row past 1.0 is the owner's call. Numbers are the
newest logged; a round's run replaces them.

| Promise | Against | Proven today | Not proven yet | Round |
|---|---|---|---|---|
| **Scales out** across machines | Spark | TPC-H 22/22 spread == one node; SF10 on GitHub's runners over the internet: 25.1 s on one node, 23.9 s on 3, 21.1 s on 6 (`logs/round29/cluster-bench-0.27.0-*`); 9,997 random queries spread, 0 wrong (`logs/round34/random-100k.json`); one machine SF1 4.5 s vs Spark 56.1 s (`logs/round30/tpch-sf1-vs-spark.json`) | machines in one data centre; SF100 falling 1 → 3 → 6 machines, faster than Spark on the same VMs | 34 (his VMs) |
| **In-memory speed** on one machine | DuckDB, Polars | TPC-H SF1 from files 2.24 s (DuckDB 2.12), from memory 1.14 s (DuckDB's own tables 1.07) (`logs/gates/2026-10-03-tpch-sf1.txt`); ClickBench 10 M rows from memory 6.73 s (DuckDB's tables 6.69), from files 9.86 s (10.71) (`logs/round34/singlenode-clickbench-10m-clickhouse.json`) | from memory at or under DuckDB's own tables (7–24% behind at SF1 across runs); SF10 not run since round 12; ClickBench's 100 M rows | 34 |
| **Lightweight** | Spark, Flink (JVMs) | 118 MB installed, first answer 0.14 s, 109 MB idle; Spark 6.9 s and 549 MB, Flink 6.4 s and 585 MB (`logs/round32/footprint-warm.txt`) | idle memory twice DuckDB's (50 MB); a cold first answer of 5.1 s | 35 |
| **In-process**, a library in Python and Node | DuckDB, Polars | `local()` starts a node beside the program | `pondra.open(…)` in the process: not built | 35 |
| **In the browser** | DuckDB-Wasm | — | not built | 35 |
| **Streaming SQL** | Flink | Nexmark q1, q2, q5, q7, q11 over 10 M bids: 7.06 s vs Flink 2.3 16.16 s, answers == DuckDB's (`logs/round32/nexmark-10m.txt`); windows, sessions, as-of joins and stream joins, each row once through kills | all of Nexmark, on the same machines as Flink; timers, CEP, a watermark per partition (after 1.0, the owner's call) | 34 |
| **A log you query as tables** | Fluss, Kafka | Kafka's protocol, exactly once: 0.99 M events/s on one node, 1.03 M on 3, ack 3 ms p50; Apache Kafka 4.3 1.14 M, 1 ms (`logs/round32/kafka-*.txt`) | Fluss head to head; a topic's partitions; Kafka's own pace and ack | 34, 36 |
| **A lakehouse in your bucket** | Databricks, Snowflake | Delta and Iceberg by Spark 4, delta-rs and PyIceberg == Pondra's reads on 55 of 55 tables, one node and three; Spark's appends, DELETE, UPDATE, MERGE through Pondra's catalog (`logs/round28/`) | Glue, S3 Tables, vended credentials; Iceberg v3 row lineage (after 1.0) | 36 |
| **Serverless**: the bucket is the only state | Spark + a metastore, Kafka + ZooKeeper | an INSERT with no node running 0.05 s; a new lake serving after 4.3 s and 28 bucket requests (`logs/round29/`) | a lake whose only node was killed makes the next writer wait 30 s (invariant 17's margin) | 35 |
| **Within the bucket's limits** at any size | S3's and R2's request limits | one request budget per node; 240 INSERTs at once into a bucket taking 10 writes a second (`c5_check.py`, on the simulator) | a logged run on real R2 at scale | 34 |
| **Never loses data** | Kafka, Flink | kill -9 and injected crashes, every event once; failovers in both ack modes; every release's lake since 0.22 opens; 64 writers and 16 readers, 0 torn reads | the 24-hour soak on R2, logged | 33 |
| **Right answers** | every engine | sqllogictest 98% of records not named an exception; TPC-H 22/22 on 1, 3 and 6 nodes; TPC-DS 99/99 == DuckDB on one node and three; 100,000 random queries, one node == three == DuckDB | — | holds |
| **Fast points and writes** | Postgres for serving | lookups 0.13 ms over HTTP, 0.22 ms over the Postgres port (Postgres 0.08) (`logs/gates/2026-10-03-postgres.txt`); a writer's 19,900 rows a second beside 400 readers, acks 5 ms (`logs/round34/users-writes.json`); another node sees a write 7 ms later | pgbench 198 transactions a second on one client but 168 on four (Postgres 1,793); a one-row INSERT 2.4 ms (Postgres 0.24) | 36 |
| **Fits in** | the Postgres ecosystem | dbt == Postgres 16's rows; psql, SQLAlchemy, pgjdbc, psqlODBC, ADBC, Npgsql (`clients_check.py`); Spark and PyIceberg write | Metabase, Superset, Grafana, Tableau, DBeaver, Power BI checked; Postgres and MySQL attached with changes streamed in | 36 |
| **Easy to change and extend** (principle 9) | closed platforms | one registry of every kind of object; the console's `register.*` and extensions; one check at every door | `INSTALL` / `LOAD` extensions (ADR-031, proposed) | the owner's call |
| **Safe** | — | users and grants to a column, TLS everywhere, an audit log, quotas, every door fuzzed | a security review | 37 |
| **Runs anywhere** | — | Linux (glibc 2.17), macOS, Windows; pip, npm, installers, a container, Helm, `pondra service` | Homebrew, winget, signed binaries; an upgrade guide | 37 |

## The rounds left

| Round | Theme | What you'd see at the end |
|---|---|---|
| 33 (closing) | Run it for years | **Left:** the 24-hour soak on R2 (the owner's machine, `tools/soak.py --hours 24 --s3`); environments (ADR-047, approved 2026-10-03, built in its own thread: a whole database cloned without a copy, with pins and `REFRESH`, then `pondra plan` and `deploy`); the console's History reading `pondra.history` (the console's thread). 0.33.0 when all three are in. |
| 34 | SQL as people write it, and scale proven | **SQL parity** (from `designs/scripting-design-review.md`; rewrites where SQL comes in, no engine change). **Built** (`friendly.rs`, invariant 225): `PIVOT`/`UNPIVOT` (both spellings), `COLUMNS('re')`, `SELECT * RENAME`, `ORDER BY ALL`; list comprehensions, lambdas (`x -> x + 1`), struct field access; `arg_max`/`max_by`, `list()`, `string_split`, `::json` and `json_extract`; DuckDB's `ASOF JOIN … ON a.t >= b.t`, `SUMMARIZE`, `USING SAMPLE` and a `TABLESAMPLE` that samples, `FETCH FIRST`, a select alias reused in `WHERE`. The statement registry (`objects.rs`, invariant 227): `pondra.objects`, `SHOW CREATE`, `COMMENT ON`, `CREATE OR ALTER TABLE`. Sequences and identity columns (`seq.rs`, invariant 234): `CREATE`/`ALTER`/`DROP SEQUENCE`, `nextval`/`currval`/`setval`, `GENERATED { ALWAYS \| BY DEFAULT } AS IDENTITY`, `SERIAL`, `AUTO_INCREMENT`, `IDENTITY(1, 1)`; values from blocks the sequencer hands each node, every one once through a leader's kill. `CREATE`/`ALTER`/`DROP INDEX` as a registry kind (`index.rs`, invariant 235): kept, shown in `pondra.objects`, `SHOW CREATE` and `pg_indexes`, following renames and drops, a notice that nothing is built; `UNIQUE` and vector indexes refused by name. Constraints (`constraints.rs`, ADR-057, invariant 236): `UNIQUE` checked by the leader for every SQL write (23505, from three nodes at once), `PRIMARY KEY`, `UNIQUE` and `FOREIGN KEY` said `NOT ENFORCED` kept as facts for the tools that read them, `ALTER TABLE … ADD | DROP CONSTRAINT`. Enum types (`types.rs`, invariant 242): `CREATE`/`ALTER`/`DROP TYPE … AS ENUM` and `ENUM('a', 'b')` columns, text in the files, labels checked at every door (22P02), casts and `enum_range`. **Proof:** done: 100,000 random queries (one node == three == DuckDB but 26 refusals DataFusion makes; the five bugs they found fixed: invariant 226), TPC-DS 99/99 on three nodes, `INSERT … SELECT` and `CREATE TABLE AS` written by every node at once (invariant 228; a lineitem copy 5.9 s on one node, 3.6 s on three sharing one box); hundreds of dashboard clients while writes land (`serve_bench.py --users`), and the cheap paths it showed: a table's log tail kept between queries, merges by size, unchanged tables planned straight from their tail and files (invariants 229–230; 760 → 870 queries a second at 50 clients, 749 → 934 at 200, `logs/round34/users.json`). A plan kept per statement shape was not one: planning is a quarter of such a query, and its leaves are each commit's rows. **Scale** (machines: decision 1): 1 → 3 → 6 machines in one data centre, SF100 against Spark, all of Nexmark against Flink, Fluss head to head, ClickBench's 100 M rows on its standard machine submitted (ClickHouse is beside DuckDB in `singlenode.py` now: `logs/round34/singlenode-*-clickhouse.json`), and what they find fixed. **Built:** writes that keep their pace under hundreds of readers (queries on a runtime of their own: a writer lands 19,900 rows a second beside 400 dashboard clients, 2,860 before; invariants 231–232) and hot columns that let go of merged files at once (233). **From the promises scorecard:** the bucket's limits measured on real R2 at scale, logged. |
| 35 | In-process and in the browser (the owner, 2026-10-03: before 1.0, built to a high bar) | One engine, three places, through the core split (B1, in a window with no side branches open): `pondra.open(…)` in Python and Node without a server, Arrow straight into pandas and Polars (B2); **in the browser** (B4): DataFusion in WebAssembly, on Web Workers where the page is cross-origin isolated; a lake read straight from its bucket at a commit a node publishes (its files and log tail), answers equal to a node's; files dropped on the page (Parquet, CSV, JSON, Arrow) and tables kept in the browser's own storage (OPFS) between visits; a local table joined with a lake's, the lake's part run on a node (B5); the console served as a static page, working with no node at all; JavaScript frames with the Python frames' names; live queries; one npm package for the page and Node. **Gates:** TPC-H SF1 in a browser against DuckDB-WASM, the download's size, the console's budget (invariant 149). **From the promises scorecard:** a node's idle memory nearer DuckDB's, a cold first answer under a second, and a lake whose only node was killed writable at once by the next writer, without giving up invariant 17. |
| 36 | Fits in | Postgres and MySQL attached and their changes streamed in (G6: a feed whose place, the replication slot's commit LSN and transaction id, is the exactly-once `(producer, seq)` as Kafka feeds' offsets are; a table that meets a change it can't apply is set aside alone, its changes kept to replay once it is synced again, while the others flow; `designs/embrasure-flow-findings.md`); a keyed table published to Delta or Iceberg finds the older versions of a round's few new keys as an `IN` list, so its files' key filters skip most of them (`tier::shadow`; a key index only if that isn't enough, measured first); sinks (G7); Glue and S3 Tables attached (AWS's signed requests) and the credentials a REST catalog vends (Polaris, Unity, Snowflake's); Kafka partitions; measures in views (E13, its first phase: one definition of every number for BI tools and agents); Metabase, Superset, Grafana, Tableau and DBeaver checked; a SQLAlchemy dialect and a dbt adapter packaged. **AI and files** (ADR-051 phase 1, the owner's choice 2026-10-04): models as catalog objects (`CREATE MODEL`, keys as secrets, grants, quotas); `ai_complete`, `ai_embed`, `ai_classify`, `ai_extract` (typed STRUCT), `ai_filter`, `ai_agg`; volumes (`CREATE VOLUME`, a table of their files, grants per volume, `/volumes/…` paths) and a `FILE` column (a version of a file); pictures and PDFs into models (`ai_parse`, `text_chunks`); pgvector's names on the Postgres port (LangChain, LlamaIndex); the same from SQL, frames (`.ai`), JavaScript and MCP. **From the promises scorecard:** the Postgres port's writes scaling with clients (pgbench at four clients, a one-row INSERT). |
| 37 | 1.0 | **It ships when every row of the promises scorecard is proven** (or the owner moves a row past 1.0). **The order** (the owner, 2026-10-03, after trying 0.32): the platform without its UI comes first, and rounds 34 to 36 hold it to the principles (stable, simple and pleasant to use, performant, lightweight, powerful, scalable in any form factor); the console's fixes (bugs he met in 0.32's console), any API still stale and the docs' review come here, together, shaped by what is built by then. **First, everything reviewed again** (the owner, 2026-10-03), before anything is promised: the known limits (AGENTS.md's list, `prototype-status.md`), every mistake and bug found so far (each round's "found" lists, the reviews in the project's files, the issues) checked for its kind elsewhere, and every door and mode against what the docs say it does; each one fixed or improved, or kept as a limit with its reason written down. **And every way in tried as a new user would** (the owner, 2026-10-03): the shell (a short banner, colour, answers drawn as DuckDB's are), the command line, the console, the Python and JavaScript clients, the HTTP API, Postgres and the error messages, each made beautiful, ergonomic and simple from the first try. **Then** what stays stable (the lake format, SQL, the HTTP API, the clients, the command line) and how things are deprecated; a security review; signed binaries, Homebrew and winget (ADR-041, waiting on the owner's accounts and certificates); an upgrade guide; the docs complete and reorganized (each thing said once, a full reference per API, guides with a tab per way, current console pictures); every document, the site and the code reviewed against what was decided. |

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
- ADR-051 phase 2 right after 1.0: vector and text indexes (`CREATE INDEX … USING hnsw | bm25`, `search()`), `VARIANT` stored shredded, `MAP`; a managed service.

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
