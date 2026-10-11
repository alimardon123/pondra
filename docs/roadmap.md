# Pondra: the shortest path to 1.0

**Date:** 2026-10-11 (one month to 1.0; the 2026-10-03 plan of rounds is in git) · **Status:** the
month is the owner's (2026-10-11), the lanes my plan for it; the decisions at the end are his.

**The bar** (the owner, 2026-10-03): reach 1.0 faster and more productively, with everything before
it high quality, stable, workable, fully featured and true to the design principles (AGENTS.md).
So from here: no new surface unless a bar below needs it, every round leaves every angle better
(principle 8), and each part merges to main as soon as CI is green; a version is cut only when its
round is complete.

## Where Pondra stands

Rounds 17 to 33 are released (0.33.0).
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

| Promise | Against | Proven today | Not proven yet | Lane |
|---|---|---|---|---|
| **Scales out** across machines | Spark | TPC-H 22/22 spread == one node; SF10 on GitHub's runners over the internet: 25.1 s on one node, 23.9 s on 3, 21.1 s on 6 (`logs/round29/cluster-bench-0.27.0-*`); 9,997 random queries spread, 0 wrong (`logs/round34/random-100k.json`); one machine SF1 4.5 s vs Spark 56.1 s (`logs/round30/tpch-sf1-vs-spark.json`) | machines in one data centre; SF100 falling 1 → 3 → 6 machines, faster than Spark on the same VMs | L1 (his VMs) |
| **In-memory speed** on one machine | DuckDB, Polars | TPC-H SF1 from files 1.94 s (DuckDB 2.04), from memory 1.07 s (DuckDB's own tables 1.03) (`logs/round34/singlenode-tpch-sf1-implied-keys.json`); SF10 on a GitHub runner from memory 12.95 s (DuckDB's tables 15.28), from files 19.50 s (23.70) (`logs/round34/singlenode-tpch-sf10-github.json`); ClickBench 10 M rows from memory 6.73 s (DuckDB's tables 6.69), from files 9.86 s (10.71) (`logs/round34/singlenode-clickbench-10m-clickhouse.json`) | from memory at or under DuckDB's own tables at SF1 (4% behind: q1, q8, q7 and q3 the most); ClickBench's 100 M rows | L1 |
| **Lightweight** | Spark, Flink (JVMs) | 118 MB installed, first answer 0.14 s, 109 MB idle; Spark 6.9 s and 549 MB, Flink 6.4 s and 585 MB (`logs/round32/footprint-warm.txt`); a top-10 of every column of a 105-column table answered in a 1.6 GB container, where the node had been killed (row groups of 64 MB: `logs/round34/wide-row-groups.json`) | idle memory twice DuckDB's (50 MB); that top-10 peaking at 1.04 GB to DuckDB's 0.33 (the top-N keeps whole batches of string views); a cold first answer of 5.1 s; a node's own memory grows 2–5 MB a minute under steady writes, with readers or none, until it restarts (the 24-hour soak's leg 1 on R2, 2026-10-09; 0.32 grew faster) | L2, L7 |
| **In-process**, a library in Python and Node | DuckDB, Polars | `local()` starts a node beside the program | `pondra.open(…)` in the process: not built | L2 |
| **In the browser** | DuckDB-Wasm | — | not built | L2 |
| **Streaming SQL** | Flink | Nexmark's 20 queries over 10 M events on one 4-core VM, answers == DuckDB's: all 20 at once as views, the last answer 80 s after the first event, each event sent over HTTP and committed (median of 3, 8.0 GB at most); Flink 2.3 685 s with RocksDB state (9.0 GB), and with its state on a 10 GB heap it failed after 18 minutes; each query alone, Pondra faster than Flink's best on 10 of 20, its times counting all 10 M events taken in (`logs/round34/nexmark-10m-flink.json`); windows, sessions, as-of joins and stream joins, each row once through kills | q18 (each bidder's latest bid on an auction) answers 22–31 s after the stream, and alone takes 42.6 s to Flink's 31.7; machines in one data centre; timers, CEP, a watermark per partition (after 1.0, the owner's call) | L1 |
| **A log you query as tables** | Fluss, Kafka | Kafka's protocol, exactly once: 0.99 M events/s on one node, 1.03 M on 3, ack 3 ms p50; Apache Kafka 4.3 1.14 M, 1 ms (`logs/round32/kafka-*.txt`) | Fluss head to head; a topic's partitions; Kafka's own pace and ack | L1, L4 |
| **A lakehouse in your bucket** | Databricks, Snowflake | Delta and Iceberg by Spark 4, delta-rs and PyIceberg == Pondra's reads on 55 of 55 tables, one node and three; Spark's appends, DELETE, UPDATE, MERGE through Pondra's catalog (`logs/round28/`) | Glue, S3 Tables, vended credentials; Iceberg v3 row lineage (after 1.0) | L4 |
| **Serverless**: the bucket is the only state | Spark + a metastore, Kafka + ZooKeeper | an INSERT with no node running 0.05 s; a new lake serving after 4.3 s and 28 bucket requests (`logs/round29/`) | a lake whose only node was killed makes the next writer wait 30 s (invariant 17's margin) | L7 |
| **Within the bucket's limits** at any size | S3's and R2's request limits | one request budget per node; 240 INSERTs at once into a bucket taking 10 writes a second (`c5_check.py`, on the simulator) | a logged run on real R2 at scale | L1 |
| **Never loses data** | Kafka, Flink | kill -9 and injected crashes, every event once; failovers in both ack modes; every release's lake since 0.22 opens; 64 writers and 16 readers, 0 torn reads; the 24-hour soak's first leg on R2 (4.8 hours, 8 nodes stopped or killed): 43,138 batches, none lost or twice, 0 torn reads of 423,085 | the soak's other legs, logged, and its memory check (Lightweight) | L7 |
| **Right answers** | every engine | sqllogictest 98% of records not named an exception; TPC-H 22/22 on 1, 3 and 6 nodes; TPC-DS 99/99 == DuckDB on one node and three; 100,000 random queries, one node == three == DuckDB | — | holds (L6) |
| **Fast points and writes** | Postgres for serving | lookups 0.13 ms over HTTP, 0.22 ms over the Postgres port (Postgres 0.08) (`logs/gates/2026-10-03-postgres.txt`); a writer's 19,900 rows a second beside 400 readers, acks 5 ms (`logs/round34/users-writes.json`); another node sees a write 7 ms later | pgbench 198 transactions a second on one client but 168 on four (Postgres 1,793); a one-row INSERT 2.4 ms (Postgres 0.24) | L3 |
| **Fits in** | the Postgres ecosystem | dbt == Postgres 16's rows; psql, SQLAlchemy, pgjdbc, psqlODBC, ADBC, Npgsql (`clients_check.py`); Spark and PyIceberg write | Metabase, Superset, Grafana, Tableau, DBeaver, Power BI checked; Postgres and MySQL attached with changes streamed in | L3, L4, L5 |
| **Every feature through every door** (principle 10) | engines with one or two ways in | the doors matrix (`harness.py doors`): one list of features through SQL over HTTP, Python, the Postgres port, Flight SQL, JavaScript and MCP, each cell right or refused by name | JavaScript has no frames, `write_table` or `read_*` (35); a table with an enforced `UNIQUE` is refused by the loading doors (appends, Kafka, Flight, `COPY`) and Flight SQL and MCP hold no transaction (36); Kafka topics have one partition and no transactions (36); enum types are text in Postgres's catalog (36); the command line and the console in the matrix (37) | L2, L3, L8 |
| **Easy to change and extend** (principle 9) | closed platforms | one registry of every kind of object; the console's `register.*` and extensions; one check at every door | `INSTALL` / `LOAD` extensions (ADR-031, proposed) | the owner's call |
| **Safe** | — | users and grants to a column, TLS everywhere, an audit log, quotas, every door fuzzed | a security review | L7 |
| **Runs anywhere** | — | Linux (glibc 2.17), macOS, Windows; pip, npm, installers, a container, Helm, `pondra service` | Homebrew, winget, signed binaries; an upgrade guide | L7 |

## One month to 1.0

The owner, 2026-10-11: 1.0 in a month, finished and polished, built by many threads at once, each
led by one that designs, reviews and answers for its result. So rounds 35 to 37 run side by side as
**lanes**, one thread each, every lane owning its files (the list, with each lane's reserved numbers,
is `designs/one-month-to-1-0.md` in the project's files). Rounds 17 to 33 are released; round 34
(SQL as people write it, the proof runs) is merged and ships as 0.34.0.

| Date | |
|---|---|
| Wednesdays (10-14, 10-21, 10-28) | 0.34.0, 0.35.0, 0.36.0: whatever is green on main |
| **10-31** | **Feature freeze**: fixes, reviews, measurements and docs only from here |
| 11-01 to 11-06 | The 1.0 review: every area reviewed again, every door tried as a new user would, the security review, the docs finished, the console's last fixes |
| 11-06 | 1.0.0-rc.1, then the 24-hour soak on R2, the cluster bench, the gates and the upgrade check |
| **11-11** | **1.0.0** |

| Lane | Thread | By the freeze |
|---|---|---|
| L1 | Engine, speed and proof (the main thread) | the Fluss head to head; TPC-H from memory at DuckDB's own tables; ClickBench's 100 M rows; Nexmark's q18; the bucket's limits on real R2; the cold-tier fixes and keyed tables' incremental steps; 1 → 3 → 6 machines once they exist; every release |
| L2 | In-process and in the browser | `pondra.open(…)` in Python and Node (B1, B2); the browser build against DuckDB-WASM (B4); JavaScript frames, `write_table` and `read_*`; idle memory and a cold first answer; then B5 |
| L3 | The Postgres port and the tools on it | pgbench scaling with clients and a one-row INSERT nearer Postgres; the BI tools checked, a SQLAlchemy dialect and a dbt adapter; the doors' gaps |
| L4 | Sources and outside catalogs | Postgres's changes streamed in (G6); `COPY INTO` (G11); `read_xml` (G10); Glue, S3 Tables and vended credentials; Kafka partitions; then sinks (G7) and MySQL |
| L5 | Semantic layer and AI | `pondra.parts` and `pondra.find`; measures in views (E13); the ontology's core (E14); `CREATE MODEL` and the six `ai_*` functions (ADR-051 phase 1); then volumes, the `FILE` column, pictures and PDFs |
| L6 | SQL language | every new spelling reviewed; sqllogictest and the random queries held; refusals that say what to write; then the SQL 1.0 keeps stable |
| L7 | Durability, safety and shipping | CI's capacity; the soak's other legs; a lake whose only node died writable at once; the security review; signing, Homebrew and winget; an upgrade guide; what 1.0 keeps stable |
| L8 | Console and shell (from 10-18) | the console's backlog; the shell as a new user meets it; then views over what L3 to L5 landed, and the Ontology view |
| L9 | Docs that show everything | a map of every feature with examples; the site reorganized around it, each thing said once |

**How the lanes work.** A lane that finishes early goes straight on to its next step. Nothing ships
with a flaw, drawback or limit that would push people away (the owner, 2026-10-11): a review thread
keeps that list, each finding goes to the lane owning its files, and a limit kept is written where a
user meets it. On 10-31 every row not proven and every finding still open is listed for the owner.

**Every pull request:** its lane's files only (shared files take additions, not edits); `ci` while
iterating and `full-ci` once before merging; the gates (`tools/gates.py`) hold or improve; an engine
or read-path change keeps TPC-H's 22 and TPC-DS's 99 answers and is no slower; a new feature works
from every door or is refused by name; a one-page review; its ADR when it is a design.

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
- ADR-051 phase 2 right after 1.0: vector and text indexes (`CREATE INDEX … USING hnsw | bm25`, `search()`), `VARIANT` stored shredded, `MAP`; trained models in SQL (E15); a managed service.

**One ecosystem (the owner, 2026-10-03).** Every object SQL makes is a catalog entry that every
other part (the console, the clients, the HTTP API, the tools still to come) reads through the same
surface, so a new tool plugs into what is there instead of keeping its own copy.

## The tracks' open items

The IDs other documents cite. Done items are gone from this list (their ADRs and
`prototype-status.md` keep them).

| # | Item | Lane |
|---|---|---|
| A5 | A measured lite build (Cargo features for Kafka, Flight, Postgres, AI, the Iceberg catalog) | after 1.0 |
| B1, B2 | `pondra-core` split from the server; `pondra.open("s3://…/lake")` in process | L2 |
| B4, B5 | A browser Pondra; a local table joined with a lake's, the lake's part on a node | L2 |
| C1, C2, C3 | 1 → 3 → 6 machines; SF100 against Spark; all of Nexmark against Flink (done on one machine: `logs/round34/nexmark-10m-flink.json`) | L1 |
| C4 | The 24-hour soak on R2 | L7 |
| D2 | Random queries: one node == three == DuckDB, 100,000 of them (done: `random_sql.py`) | done (L6 keeps it) |
| E2 | BI tools checked (Power BI Desktop, Tableau, DBeaver, Metabase, Superset, Grafana) | L3 |
| E12 | Sharing (ADR-046, proposed) | after 1.0 |
| E13 | Semantic models as measures in views (approved 2026-10-03; `designs/semantic-models-and-dremio.md`): `sum(x) AS MEASURE m` in a `CREATE VIEW`, queried as `MEASURE(m)`, right at any grain; BI tools' `sum(m)` gives the measure; MCP `list_metrics`; Apache Ossie YAML out and in. Then rollups kept in their source's commit and read where they give the same answer; then rollups proposed from `pondra.history`. Proof: `harness.py measures` | L5 (then after 1.0, before J6) |
| E14 | An ontology as a schema (approved 2026-10-10; `designs/ontology-as-a-schema.md`): an ontology is a schema whose tables and views are object types, whose `FOREIGN KEY … NOT ENFORCED` facts are links, whose measures (E13) are metrics and whose procedures are actions. Nothing is stored apart. New: key facts on views and materialized views (`ALTER VIEW v ADD CONSTRAINT … NOT ENFORCED`) and `COMMENT ON COLUMN` on their columns (done in round 34); `pondra.ontology('s')`; `pondra.related(object, key)`; `pondra.suggest_ontology(sources, into => …)`, a deterministic read that returns statements with evidence (keys, links, one thing in several systems as a view whose match is SQL, measures, tests); the clients' verbs and MCP's `ontology`, `suggest_ontology` and `related`. Then the console's Ontology view (suggest, a picture, Looks right, Home from the measures, an Explorer from `related`). Proof: `harness.py ontology` | L5 (core), L8 (console) |
| E15 | Machine learning models trained in SQL (approved 2026-10-10; `designs/models-and-names.md`). ADR-051's `CREATE MODEL` gains `AS SELECT …` with a trainer (`USING 'sklearn/GradientBoostingClassifier'`, the library's own class, or `FUNCTION f`; `WITH (target = …)`; a registry of trainers), trained on a Python worker as a run. Each version keeps its object, the commit it read, its metrics and who made it. `predict('m', …)` and `predict_proba('m', …)` are vectorized functions spread with the query, alongside `pondra.evaluate('m' [, (query)])`, `pondra.model_versions('m')`, `ALTER MODEL m ADD VERSION` and `SET (default_version = n)`. Features are views kept current; `ASOF JOIN` builds training sets as of each label's time; keyed lookups serve features online. Proof: `harness.py models` | right after 1.0 (with ADR-051 phase 2) |
| F | Depth by evidence (above) | after 1.0 |
| G6, G7 | Databases attached with their changes streamed in; sinks | L4 |
| G10 | XML files as tables: `read_xml('…')`, one more `read_*` format beside `read_json`, through every door that reads files | L4 |
| G11 | Files loaded once each as they land (what Snowpipe and Auto Loader do): `COPY INTO t FROM 's3://landing/…'` remembers the files it loaded, so running it again (or as a task) loads only new ones, each once through a failover; its spelling goes past the SQL review first | L4 |
| I1 | Extensions (ADR-031, proposed) | after 1.0 |
| J1, J3–J7 | The platform on top (above) | after 1.0 |

## What only the owner can decide

1. **Machines, by 10-24:** six 8-vCPU VMs and a client in one zone for a few hours at a time
   (`tools/cloud/gcp.sh up 6 --local-ssd`; the Google Cloud trial caps at 8 CPUs and the run needs
   52). Without them "Scales out" is proven on GitHub's runners over the internet only.
2. **Publishing, by 11-01:** Homebrew, winget and signing wait on the owner's accounts and
   certificates (ADR-041 §9); without them they follow 1.0 in 1.0.1.
3. **CI's capacity:** two pull requests' runs at once today; nine lanes need four.
4. **An `INSERT` of a key that exists:** it replaces the row today (an upsert, as Fluss and Paimon
   do). My recommendation: keep it, and add `WITH (on_duplicate = 'error')` for Postgres's
   behaviour, with `ON CONFLICT` working in both.

## What not to do yet

- No new surface before 1.0 that no bar needs; new ideas go to "After 1.0".
- No fully static musl build (its allocator slows multi-threaded Rust); the old-glibc build reaches
  as far.
- No DuckLake as Pondra's catalog: it needs a database, and Pondra needs none.
- No enterprise governance beyond users, grants, TLS and the audit log until someone asks.
