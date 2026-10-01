# Pondra: what's left, and in what order (after round 28)

**Date:** 2026-09-28, the road to 1.0 added 2026-09-30 · **Status:** proposed; the order in "The
rounds" is what I recommend, the decisions in "What only you can decide" are yours · **Builds on:** ADR-002 to ADR-017,
`prototype-status.md`, `comparison-spark-flink-fluss.md`

**Progress (2026-10-01):** rounds 17 to 30 are done (ADR-018 to ADR-036; round 29, the owner's
console lists, then users, grants, secrets, TLS, an audit log, quotas, files with versions and C5,
is 0.28.0; round 30: pipelines with expectations, transactions, error codes, CHECK constraints,
the point path from every door, the doors matrix and history per key; identity columns and UNIQUE
(decision 10) are left). Round 31's first part is 0.29.0: session settings and prepared
statements, Spark's functions, Arrow files, `CREATE TABLE` as Postgres has it, interval
arithmetic; sqllogictest 98.0% with every exception named (92.9% of every record); TPC-DS's 99
queries equal to DuckDB's; a random-query tester (which found a wrong answer in DataFusion's `IN`
lists, worked around); Polars' and PySpark's coverage published.

- **Round 17** made Pondra install anywhere: a glibc 2.17 Linux binary, pip and npm packages
  (built, not published), a SQL shell, and both flaky tests fixed.
- **Round 18** took the owner's first session on Windows as its list. A lake is now a database of
  schemas: `lake.schema.table`, attached lakes as databases too, `CREATE`/`DROP SCHEMA`, `DROP
  TABLE`, `CREATE TABLE … AS`, stored views (`CREATE VIEW`), `CREATE MATERIALIZED VIEW`, and
  `ATTACH … AS …` for queries across lakes, as SQL Server allows across databases. A7
  is done: the owner ran the Windows build, and its memory figures, which came from Linux's
  `/proc`, now come from the OS; CI runs a smoke test on Windows, macOS and Linux. C1 started:
  the first 3-node run on GitHub's runners was right but slower than one node; the second lost a
  node at start to a race (fixed), and the bench now measures what crosses the network.

- **Round 19** (ADR-020) made every row changeable: `UPDATE`, `DELETE` and `MERGE` on every
  table, system columns (`_row_id`, `_version`, `_created_at`, `_updated_at`), views and a change
  feed that follow every change, and files rewritten without changed rows so Delta and Iceberg
  see them. C1's second finding became a guard: a query spreads only when what it would move
  costs less than the work it shares, so a cluster on a slow network is no slower than one node.
  The owner's second Windows session added `CREATE DATABASE`, `ATTACH` of a new folder,
  `CHECKPOINT`, `ALTER TABLE … SET`, local files in the shell, and fixed the lake's name on Windows.

- **Round 20** (ADR-021) took the owner's questions after round 19 as its list. Lakes write far
  fewer objects (a trickle of one-row INSERTs: 3.9 objects each down to 1.1, and 217 left
  instead of 5,962); system columns cost tiering almost nothing (file statistics from the Parquet footer); a
  PRIMARY KEY goes with `partition_by` and `cluster_by`; `cluster_by` over two or more columns
  orders along a Hilbert curve; the Postgres protocol has `COPY` (and the ADBC Postgres driver
  works); two streams join as their rows arrive; sliding windows. C1: the owner's run on round 19
  showed the guard works (18.0 s against one node's 15.7 s, from 46.6 s) and found loading 4×
  slower (fixed).

- **Round 21** (ADR-022): `RENAME COLUMN`, `DROP COLUMN` and widening types with no file
  rewritten (Delta column mapping, Iceberg field ids); materialized views filled from the rows
  already there, every row once (the sequencer now holds every flush to the views);
  deduplication by event time (`order_by`); Nexmark q1, q2, q5, q7, q11 against Flink 2.3 (10 M
  bids: 8.7–10.9 s against 24.3–25.0 s, the answers equal to DuckDB's). The DataFrame API was designed
  (`dataframe-api.md`). C1: the owner's run on round 20 showed loading fixed (106 s from 442 s)
  and the cluster at one node's speed (24.8 s against 24.5 s). Pondra is now MIT OR Apache-2.0.

- **Round 22** (ADR-023): the DataFrame API, built: `pondra.frame` (Polars' names) and
  `pondra.spark` (PySpark's), each a CTE per step of one SQL statement — as fast as the SQL (10 M
  rows: 0.063 s against 0.065 s); SQL and Python either way round (SQL names Python frames and
  pandas data, `.sql` files with parameters, `%%sql` cells); all 22 TPC-H queries the same as
  SQL, as frames and as PySpark code, and 44 PySpark pipelines equal to PySpark 4's answers and
  column names. The owner's idea came with it: **macros and procedures** kept in the catalog, SQL
  or Python (a Python procedure runs beside the node, lent its caller's rights), called from SQL,
  Postgres, Python, JavaScript and as MCP tools; scripts with `$name` parameters; `pondra run`.

- **Round 23** (ADR-026): read and write anything. Files on S3, GCS, Azure, HTTPS and the
  owner's machine are tables wherever SQL takes one (DuckDB's names: `'s3://…/*.parquet'`,
  `read_csv`, Hive folders), spread over the nodes and read fresh each statement; Delta and
  Iceberg tables read natively (checkpoints, deletion vectors, column mapping, position and
  equality deletes, REST catalogs), attached, and written by `INSERT`; `COPY … TO` files anywhere
  (a big folder written by every node) or a Kafka topic; other Kafka clusters read as tables and
  fed into views, every record once; `CREATE SECRET`; lakes on GCS and Azure. Read from files,
  TPC-H SF1 is as fast as from the lake's own tables (3.67 s against 3.73 s on disk, 3.51 s
  against 3.48 s on S3). D1 is set up: DataFusion's own sqllogictest files pass 67.2% on one node
  and on three (72.6% without its Spark-function files); it found `INSERT INTO t (columns)` and
  `CREATE TABLE t (columns) AS VALUES` wrong, both fixed.

Still waiting:

- **C1 in one data centre:** machines under a millisecond apart, where spreading should pay.
- **Publishing:** 0.22.1 (ADR-024: one-line installers, `python -m pondra`, rows without
  pyarrow) is on PyPI and npm; `v0.22.2` (tagged) adds the fix below. 0.23.0 makes the clients'
  names SQL's (ADR-025: `db.view` is a stored query unless `materialized=True`) and is the first
  release that publishes the build workflow's packages instead of building again.

0.22.2 fixes a bug found while testing 0.22.1: on a cluster, a keyed table's first tiering round
could bring deleted keys back, and an adding-up view could lose part of an UPDATE (every job of
the round took its file for the table's first; invariant 93, `harness.py deal`).

Two gaps the owner met in the shell went with round 25, and are closed: `UPDATE`/`DELETE`/`MERGE`
on an attached lake from any node, and temporary tables.

**Round 24** (ADR-027) made **SQL and Python one** (track H, H1–H6):

- **Functions:** `CREATE FUNCTION` in Postgres's forms, in SQL and in Python. Python functions are
  per row, vectorized or tables, run on warm workers beside every node, and spread with their
  queries.
- **Procedures** send mail from a SQL cell: they run as their caller, print notices back through
  every door, and read secrets that never show.
- **Tasks** on a schedule, each tick once through a failover, and a run log (`pondra.runs`).
- **Decorators** take a notebook's function as it is.
- **Speed:** a warm `CALL` in 2.4 ms.

**Round 25** (ADR-028, the owner's split: the engine first):

- **One vocabulary:** `read_*` / `write_*` in SQL, Python, PySpark and JavaScript, the tools' names
  as fallbacks with the same answers; `COPY … TO` (and `write_delta`, `write_iceberg`) makes,
  appends to or overwrites Delta and Iceberg tables in a folder.
- **Other engines append to Pondra's tables** through its own Iceberg REST catalog: Spark 4,
  PyIceberg and another Pondra, each commit once, the rows the table's own.
- **Live queries** push an answer within milliseconds of a commit that changes it.
- **Function answers reused** for a lifetime (`WITH (cache = '10 minutes')`).
- **Temporary tables and views**, a session's own; changes to attached lakes from any node.

**Round 26 is done** (ADR-030, built):

- **A documentation website** (Starlight, GitHub Pages): 49 pages, 456 examples, every one run in
  CI. Writing it found 37 bugs, and all are fixed with checks.
- **The console at `/`:** SQL, Python and Markdown cells, live answers, and notebooks saved in the lake
  as `.ipynb`, with Jupyter's keys.
- **`pondra server`:** a folder of lakes as databases, each started on use and stopped when idle.
- **Postgres's catalog:** dbt gives the same rows as Postgres 16; psql, SQLAlchemy, pgjdbc (DBeaver,
  Metabase), psqlODBC (Tableau, Excel), Npgsql (Power BI) and ADBC work. `ALTER TABLE | VIEW … RENAME TO`, `ON
  CONFLICT`, `UPDATE … FROM`, `DELETE … USING`, `TRUNCATE`, `NOT NULL` and `DEFAULT`.

**Round 26, continued** (ADR-032, built before the tag): `CREATE EXTERNAL TABLE` as a view of
files and `to_timestamp` as DataFusion's (sqllogictest 64.9% to 74.5%, with DataFusion's test data
in place); a page's
Python cells sharing a worker, with figures and a Variables tab; one serve command (`pondra serve
PATH`, `--lake`, `--lakes`) over a local folder or a bucket prefix, `pondra server` gone; the logo
and colours in `brand/`; the console rebuilt as a core with an extension API, a details panel,
profiles, completion and an outline.

Left from it: Power BI Desktop itself on Windows (its drivers, Npgsql and psqlODBC, are tested);
charts of SQL answers without Python.

**Backward compatibility** (the owner, 2026-09-29): promised from the production-ready release
(1.0) on: the lake's format, SQL, the HTTP API, the clients and the command line stay compatible,
and a breaking change needs a deprecation release first. Until 1.0, names and formats may change
when that makes the product better, each change in its ADR and the release notes.

**Proposed, 2026-09-29: anyone's compute, one catalog** (ADR-029, the owner's direction: "total
serverless and compute/storage separation"). It takes two rules from the first step one step
further, from Pondra's own processes and from other engines' reads to other engines' writes:
ADR-001's "files in place, never copied" and ADR-003's "your compute, the leader's commit". Other
engines would write Pondra's tables with their own compute, and Pondra would only commit:

- files taken as written, with their row ids from each file's first id plus position (Iceberg v3's
  row lineage, Delta's row tracking);
- deletes, overwrites and upserts from Spark and PyIceberg;
- the table's layout published for writers to follow;
- views fed from the files.

It is G9 below, in three phases: rounds 27 and 28 (the owner, 2026-09-29), after the console and the docs.

## Where Pondra stands

One Rust binary does streaming ingest (exactly-once), a lake of Parquet files in your bucket,
SQL that spreads over any number of nodes, views that update as rows arrive, windows and
sessions, point-in-time joins, and Postgres, Kafka, Flight and MCP front doors. It passes its
suite on local disk, on an R2 simulator and on real R2. On one machine it is a little faster
than DuckDB on TPC-H (SF1 and SF10).

Two things hold it back more than any missing feature:

- **It hasn't been proven on several machines.** Every distributed number comes from three
  processes sharing one 2-core box. The answers are shown to be right; the speed-up is not.
- **It doesn't run anywhere yet.** The Linux binary needs glibc 2.38, so it won't start on
  Ubuntu 22.04 images, which many cloud notebooks use. It is 97 MB (33 MB compressed), a server
  only, with no `pip install` and nothing for a browser. DuckDB and PGlite show what "runs
  anywhere" really takes.

## The owner's principles, checked

| Principle | Where it stands | Gap |
|---|---|---|
| Small binary that runs anywhere, like DuckDB | One file, no JVM, 41 MB of memory idle, 159 MB after loading and querying 1 M rows | glibc 2.38; 97 MB; no pip/npm package, in-process mode or browser build; runs as a server only |
| Serverless: no always-on services | Object storage holds all state; `pondra sql` works with no server; an idle lake is taken over at once | — |
| Bodo-style SPMD, no driver | Every node plans the same SQL; any node coordinates | Not yet run on separate machines |
| No need for Spark, Flink, Fluss | Batch SQL, streaming SQL, the Kafka protocol, open lake formats | No head-to-head with Flink on a standard streaming benchmark |
| Small to petabyte workloads | A million files per table, shuffles bounded by disk | Largest run: TPC-H SF10, one machine |
| Short, readable code | 12,800 lines of Rust | `spmd.rs` is 1,300 lines; worth a tidy before it grows |

## What DuckDB-WASM and PGlite teach

Researched 2026-09-25; sources at the end.

**What they are.** DuckDB-WASM is DuckDB compiled to WebAssembly: an analytical engine in a web
page. PGlite is Postgres compiled to WebAssembly, under 3 MB compressed, running in the browser,
Node, Bun and Deno. Both are the same engine as their native versions, packaged for new places.

**Seven lessons for Pondra.**

1. **The engine is a library; the server is optional.** `pip install duckdb` runs the engine
   inside Python, and PGlite runs inside a JavaScript program. Pondra today is a server you
   start and then talk to. An in-process mode would let a notebook, a script or a test use a
   Pondra lake with no port, no process and no copy of the data. The lake's catalog already
   lives in the bucket, and `pondra sql` already opens it with no server.
2. **Packaging is part of the product.** Neither is a single download for one OS. ruff and uv
   ship a Rust binary inside per-platform Python wheels (maturin's "bin" mode). esbuild and
   biome ship per-platform npm packages. For Linux, building against an old glibc (2.17, with
   `cargo zigbuild`) runs everywhere without the allocator slowdown a fully static musl build
   brings for multi-threaded Rust.
3. **Say what the small build leaves out.** DuckDB-WASM drops extensions such as httpfs,
   Iceberg and Delta in the browser, runs single-threaded unless the page is cross-origin
   isolated, and stops at wasm32's 4 GB. It says so plainly. A browser Pondra would read a lake
   and answer queries; it wouldn't lead a cluster.
4. **Queries that keep themselves current.** PGlite's live queries push a new answer when the
   data changes, and ElectricSQL syncs Postgres into it. Pondra already knows when each table
   changes (the result cache and `/watch`), so a "live query" endpoint for dashboards is a short
   step.
5. **Speak an existing protocol from the embedded engine too.** `pglite-socket` lets psql and
   ORMs use an in-browser Postgres. Pondra already speaks Postgres, Flight SQL and Kafka; the
   in-process mode should keep that door.
6. **A built-in console wins users.** `duckdb -ui` opens a local web UI (notebooks, table
   browser, column profiles) from the same binary. Pondra's HTTP server could serve one page at
   `/`: a SQL editor, the tables, and live results.
7. **Split a plan between the viewer and the cloud.** MotherDuck runs the part of a query over
   local data in the browser's DuckDB, the rest in the cloud, and passes rows across. Pondra's
   SPMD already treats every process as a peer. A notebook or browser could be one more peer,
   working on the data it holds.

**Worth watching, not copying now.** DuckLake reached 1.0 in April 2026. It keeps a lake's
metadata in a SQL database (Postgres, SQLite or DuckDB) and has DuckDB, DataFusion, Spark and
Trino clients. Pondra keeps its metadata in the bucket itself (SlateDB), which needs no database
at all and fits "serverless" better. Publishing DuckLake metadata as a third open format, next
to Delta and Iceberg, could come later.

**The engine side.** DataFusion compiles to WebAssembly (`wasm32-unknown-unknown`). In a browser
it runs one partition at a time, because its parallel execution needs a Tokio runtime
(DataFusion issue #15599). SlateDB, which holds Pondra's catalog, has no browser build. So a
browser Pondra would read a published snapshot of the lake (the list of files and log segments),
not the catalog.

## Everything still open, in ten tracks

Size: **S** = part of a round, **M** = about one round, **L** = more than one.

### A. Runs anywhere

| # | Item | Why | Size | Proof |
|---|---|---|---|---|
| A1 | Linux binary built against glibc 2.17 (`cargo zigbuild`), keeping the current allocator | Starts on any Linux from 2014 on: Colab, SageMaker, old servers | S | Starts on Ubuntu 18.04, 20.04, 22.04 and 24.04 containers |
| A2 | `pip install pondra`: the binary in per-platform wheels (maturin "bin"), and `pondra.local()` starting a node in the background from Python | A notebook runs Pondra in two lines | S–M | A fresh Ubuntu 22.04 notebook: install, create, append, query, in under a minute |
| A3 | `npm install pondra`: per-platform packages and a small JavaScript client | Node and web developers | S | A Node script writes and reads a lake |
| A4 | A built-in web console at `/`: SQL editor, tables and their columns, live results | The first five minutes of a new user | M | Open a browser at the node, no other install |
| A5 | Cargo features for Kafka, Flight, Postgres, AI and the Iceberg REST catalog, and a measured "lite" build | Smaller downloads where they matter | S–M | Sizes of the full and lite builds, side by side |
| A6 | Defaults for small machines: the disk cache sized to the free disk, and `pondra` with no arguments starting a lake in `./lake` | Sandboxes with little disk | S | The notebook test above, on a 10 GB disk |
| A7 ✓ | Windows checked on a real Windows machine | Your laptop runs Windows | S | The `.exe` runs on your laptop (round 18); CI runs `smoke.py` on Windows and macOS |

### B. In-process and in the browser

| # | Item | Why | Size | Proof |
|---|---|---|---|---|
| B1 | Split a `pondra-core` library (lake, log, query) from the server | Everything below builds on it | M | The server is a thin layer over the library; every test still passes |
| B2 | An in-process Python module: `pondra.open("s3://…/lake")`, answers as Arrow straight into pandas and Polars | The DuckDB experience, on a lake a cluster also uses | M | The same lake written by a cluster and read in a notebook with no server |
| B3 ✓ | Live queries: `GET /live?sql=…` pushes a new answer whenever a commit touches the query's tables; Python and JS clients subscribe | Dashboards that keep themselves current (PGlite's lesson) | S–M | A dashboard updates within 50 ms of a write, locally |
| B4 | A browser Pondra, read-only: DataFusion in WebAssembly reading a per-commit snapshot of the lake (Parquet files and log segments) straight from the bucket | Viewers do the work; no node in the query path | L | A 1–10 GB lake queried in a browser, answers equal to a node's |
| B5 | A browser or notebook as one more SPMD peer, working on its own data (MotherDuck's lesson) | Local data joined with the lake without uploading it | L | Later; after B2 and B4 |

Before B4, a cheap check: can DuckDB-WASM already read the Parquet files Pondra publishes, from
a list of URLs? If it can, that gives a browser read path at no cost.

### C. Proof at scale

| # | Item | Why | Size | Proof |
|---|---|---|---|---|
| C1 | TPC-H SF10 at 1, 3 and 6 nodes on separate machines (GitHub runners); ingest scaling over Flight and Kafka | The claim nothing proves yet | M | Time drops as nodes are added; answers equal one node's |
| C2 | TPC-H SF100 against Spark on the same VMs | The petabyte road starts at 100 GB | M–L | Pondra vs Spark, like for like |
| C3 | Nexmark against Flink (the standard streaming benchmark) | Round 16 made streaming Flink-like; this measures it | M | Throughput and latency, query by query, both engines |
| C4 | A 24-hour soak on R2: steady ingest, views, tiering, failovers | Leaks and slow drifts show only over time | M | Memory flat, log drained, 0 lost, 0 duplicated |
| C5 | Fast cold starts, within the bucket's limits at any size (the owner, 2026-09-30: "at PB scale we should not hit rate limits of S3"): the start's requests overlapped, one request budget per node, keys that spread, no scheduled whole-bucket listing (below) | A cold node takes 6.5 to 8 s to serve, its requests one after another; and at petabytes every unbounded fan-out, time-ordered key or full listing meets the store's limits | M | Below |

**C5 in detail** (measured 2026-09-30 with `tools/cold_trace.sh`, `logs/round29/cold-before.txt`).

The store's limits:

- **S3:** at least 3,500 writes and 5,500 reads a second per partitioned prefix. It splits a busy
  prefix by itself, gradually, answering 503 Slow Down meanwhile.
- **GCS:** about 1,000 writes and 5,000 reads a second per bucket at first. Grow no faster than
  double every 20 minutes, and avoid names in time order.
- **Azure:** 20,000 to 40,000 requests a second per account, and 503 Server Busy above that.
- **R2:** one write a second to the same key; more gets 429.

Where Pondra stands:

- **The cold start is short on requests but slow.** On the simulator at the near R2 bucket's
  latency, a node serves after 6.5 to 8 s. It sends about 45 requests on a new lake and 80 on an
  existing one, nearly all one after another:
  - up to 1.3 s is the lease (three listings, then its writes);
  - 5 to 7 s is SlateDB opening the catalog. The writer fences, then the compactor starts, and
    every manifest or compaction write is followed by a read of SlateDB's GC boundary. All of it
    happens before the node serves.

  So overlapping the start sends no more requests. It only stops them waiting on each other.
- **Fan-outs pick their own width.** Deletes go 16 at a time and maintenance 4. A few places send
  one request per file at once:
  - unpublishing a format's folder;
  - discarding a refused commit's files;
  - writing a table's manifests.

  object_store retries 429 and 5xx with jittered backoff (10 tries within 3 minutes), but each
  request retries on its own, so a storm across nodes keeps its load.
- **Log segments are named by time** (`log/<ms>-<uuid>.seg`). Every node's flushes land at the
  same end of the key range, which is the pattern S3 and GCS warn against. Data files are already
  spread (`data/<table>/<uuid>.parquet`).
- **The hourly orphan sweep lists all of `log/` and `data/` and reads every manifest.** At 100
  million files that is 100,000 LIST calls in one stream: hours of work at R2's latency, started
  every hour.
- **One key is written by many writers:** `inbox/bell`, touched by every writer that reaches the
  bucket but not the leader (the bucket inbox). On R2, many at once get 429s.
- **Queries of the lake's tables never list the bucket;** their files come from the catalog and
  the manifests. That stays a rule.

**Done (2026-10-01):**

- **Step 1, in part:** the catalog's compactor and garbage collector start once the node serves
  (SlateDB's standalone ones, beside the writer); the catalog's first checkpoint, the leader's mark
  and the lake's keys go beside serving; a node stopping writes its memtable out. On the simulator
  at the near R2 bucket's latency (`tools/cold_trace.sh`, which now prints each step's time as the
  node saw it), a node serves after 3.1–4.4 s (was 6.5–7.7 s), with 28–44 requests (were 46–78).
  What is left is SlateDB's writer opening, 2–3 s: it reads the unflushed WAL twice (fencing finds
  its last object, then replay). A newer SlateDB, or one read, is the step to a third.
- **Step 2:** `budget.rs`, in object_store's HTTP layer, so it sees every attempt: at most
  `PONDRA_BUCKET_REQUESTS` (256) requests at once per bucket and node, halved on a 503 or 429 (once a
  second at most), one more after each run of answers without one.
- **Step 3:** log segments are `log/<uuid>-<ms>.seg`; old names still read (the catalog holds each
  path).
- **Step 4:** the orphan sweep takes one part at a time (`log/`, a table's folder, a dropped
  table's), each once a day at most, a day's worth of parts an hour; a path compared as the store
  lists it.
- **Step 5:** a ring of `inbox/bell` refused counts as rung, and the leader looks at the inbox every
  30 s anyway.
- **Proof** (`tools/c5_check.py`, `sim_r2.py --writes-per-sec --key-writes-per-sec`): 240 INSERTs at
  once into a bucket that takes 10 writes a second, every one succeeding while the node slows down
  (13 refusals; 12–16 writes a second sent after the first); four writers through the inbox at once,
  79 refused rings, each answered. Not yet run: six nodes starting at once, a 50,000-file drop.

The plan (round 29):

1. **Serve as soon as the catalog is open for writing.** The compactor and the other background
   work start after, and steps that don't depend on each other overlap: the lease's listing
   with the catalog's check, and a follower's reader with its lease. Check whether a newer SlateDB
   reads the GC boundary less. The target is under a third of today's time.
2. **One request budget per node and bucket, in the HTTP layer** (object_store's HTTP connector),
   so it sees every attempt, retries included. It caps the requests in flight, halves the cap on
   a 503 or 429, and grows it back by one per round of successes: AIMD, as TCP and the AWS SDK's
   adaptive retries do. Every fan-out goes through it, so code can fan out freely and the node
   still backs off within a second of the store asking.
3. **Keys with a random first part** wherever many writers add keys: log segments become
   `log/<uuid>.seg`, with their time kept in the catalog, as it already is. Old names still read,
   since the catalog holds each path.
4. **No scheduled whole-bucket listing.** The orphan sweep becomes incremental. Either writers
   record what they are about to write, so orphans are found without listing, or the sweep lists
   one table's folder at a time, each at most daily. The ADR picks one.
5. **No key written by many writers faster than once a second.** A 429 on the bell means it just
   rang, so it counts as rung, with no retry.

Proof:

- `sim_r2.py` answers 503 above a set rate per prefix, and 429 above one write a second per key.
- With the rate set low (200 writes a second), six nodes start at once, a 50,000-file table is
  dropped and three nodes scan a big table. Every statement succeeds, and the node backs off
  within a second.
- The cold start is timed on the simulator and on the near R2 bucket, against today's 6.5 to 8 s
  to serve and 14 s to a new database's first write. The requests per statement (`GET /metrics`) do
  not grow.

### D. Correctness you can trust

| # | Item | Why | Size | Proof |
|---|---|---|---|---|
| D1 | Run DataFusion's own SQL test files (sqllogictest) through Pondra: one node, and spread across three | Thousands of SQL cases beyond TPC-H | M | Pass rate, with every failure understood |
| D2 | Random queries on random data: one node vs three vs DuckDB (`asof_check.py`, generalised) | Finds the join or aggregation shape nobody thought of | M | N thousand queries, 0 differences |
| D3 | The known flakes: the Kafka earliest-offset loop; q15's float order on one node | Tests that sometimes fail get ignored | S | Both pass 20 runs in a row |

### E. Ready for a team

| # | Item | Why | Size | Proof |
|---|---|---|---|---|
| E1 | dbt through the Postgres protocol (dbt-postgres) | How teams build models | S–M | A dbt project runs: seeds, models, tests |
| E2 | BI tools on Windows: Power BI Desktop, DBeaver, Tableau, over Postgres ODBC/JDBC and Flight SQL | How teams look at data | S | Each connects and refreshes a report |
| E3 | Security: TLS built in, per-table grants, an audit log (a system table fed by the change feed), quotas | Before anyone else's data goes in | M–L | A second user sees only what they are granted; every write audited |
| E4 | Schema changes beyond ADD COLUMN: rename, drop, widen a type, defaults | Tables change | M | Each under load, with Delta and Iceberg readers following |
| E5 ✓ | Schemas and three-part names, DDL in SQL (`DROP`, `CREATE TABLE … AS`, `CREATE VIEW`, `CREATE MATERIALIZED VIEW`) | What a database user types first (the owner, on Windows) | M | `harness.py schemas` (round 18) |
| E6 | `UPDATE`, `DELETE` and `MERGE` on every table; system columns: a row id, when a row was written, its version | The owner's request; changing an append table's rows needs to know which row is which | M–L | Each on append and keyed tables, under streaming ingest, with views, the change feed, Kafka consumers, Delta and Iceberg readers following |
| E7 | Materialized views filled from the rows already there | A view created on a table with data starts empty today | M | A view created mid-stream equals the query over the whole table |
| E9 ✓ | Pondra's own names, the same everywhere (the owner, 2026-09-28: "in the end we will be independent"): reading is `read_parquet`, `read_csv`, `read_json`, `read_delta`, `read_iceberg` and `table(name)` in SQL and every client; writing is `write_parquet`, `write_csv`, `write_json`, `write_delta`, `write_iceberg` and `write_table(name)` (SQL: `COPY … TO`). The tools' names stay as fallbacks: Polars' `scan_*`/`sink_*`/`ndjson`, DuckDB's `delta_scan`/`iceberg_scan`, PySpark's `spark.read`/`df.write`. The docs lead with Pondra's names, the others in a column beside them | One vocabulary to learn; nothing breaks for someone arriving from Polars, PySpark or DuckDB | S–M | Each fallback equal to its standard name's answer; one table of every operation in SQL, frames, Spark and JavaScript, checked by a test |
| E10 ✓ | Function results reused (from query.farm's HTTP caching for DuckDB's remote functions): `WITH (cache = '5 minutes')` on a function or table function, keyed by the function's version, its arguments and the caller, kept only when the call succeeded (an argument's distinct values per batch are already sent once: round 24) | An API or model called again for the same arguments costs a round trip each time | S–M | A second query within the lifetime makes no call; a changed function or another caller doesn't reuse it |
| E11 | A documentation website (the owner, 2026-09-29: "even I can't know exactly what things we have"): what Pondra does and how to use all of it. Getting started, guides by task, a reference page per feature with every example in SQL, Python, PySpark and JavaScript side by side, operations and concepts. Starlight on GitHub Pages, published by a workflow; every example runs in CI | Users (and the owner) can find and use what's built | M | Every page's examples pass in CI; the site builds and publishes on each release |
| E8 | The rest of `ALTER TABLE`: rename a table, rename and drop columns, widen a column's type | The owner's third Windows session. Files and the log match columns by name, so a renamed column would lose its values and a dropped one come back with a new column of its name: every column needs an id that the files carry (Iceberg's field ids) | M | Each under streaming ingest, with views and Delta/Iceberg readers following; old files read by id |

### F. Depth, ordered by what the tracks above show

- **Streaming:**
  - as-of joins in views that wait for the looked-up table to catch up to the event's time;
  - a watermark per partition or per node;
  - late rows to a side table;
  - Top-N per key by event time (deduplication is done: `order_by`, round 21);
  - timers and `MATCH_RECOGNIZE`;
  - stream joins sharded across the nodes;
  - a view's state split by key ranges over the nodes, each keeping its own keys' (Flink's keyed
    state, without a JobManager: every node knows its ranges, as queries' shuffles do).
- **Distributed:**
  - the stages after a shuffle planned again from what the ones before found (Spark's adaptive
    execution: sizes, skew, a join switched to a broadcast), SPMD's way: every node gets the same
    few numbers and plans the same;
  - a slow step raced on another node, the first to finish kept (Spark's speculative execution:
    the straggler's cost, not a failure's);
  - a `LIMIT` inside a subquery;
  - shuffles that keep order;
  - hot keys on both sides of a join;
  - ranges declared by `cluster_by` across files;
  - a query's answer streamed instead of held once in memory.
- **Storage:**
  - merging files inside sealed manifests;
  - publishing big tables to Delta and Iceberg from the manifests;
  - keyed compaction split by key range;
  - deletion vectors (Delta) and position deletes (Iceberg) instead of rewriting files on a purge.
- **Kafka:** partitions, transactions, the Java client and Kafka Connect.
- **Other:** an approximate vector index, `VARIANT` as a real type, and statistics of what a
  filter keeps.

### G. Read and write anything (the owner, 2026-09-28)

Every connector is Rust inside the one binary (no JVM, no plugin process), costs nothing until a
query uses it, and runs where the work is: files and partitions are dealt out to the nodes like a
table's slices.

| # | Item | Why | Size | Proof |
|---|---|---|---|---|
| G1 | Files anywhere, from SQL: Parquet, CSV, JSON (and Avro) on S3, GCS, Azure and HTTPS (`FROM 's3://b/x/*.parquet'`, `read_csv(…)`), spread across the nodes | The first thing a new user tries: their data where it is | M | Each format × each store, one node == three; DuckDB's answers |
| G2 | Other lakes' tables: Delta (`delta_scan`), Iceberg (`iceberg_scan`, and REST catalogs: Polaris, Unity, Glue) as tables to read and join | Where most companies' data already is | M | delta-rs's and PyIceberg's tables read right, with deletes and schema changes |
| G3 | Files out: `COPY (query) TO 's3://…' (FORMAT parquet/csv/json, PARTITION_BY …)`, from any node, spread | Exports and hand-offs | S–M | Round trip through each format; other engines read them |
| G4 | GCS and Azure for lakes themselves | Not everyone is on S3 | S | The suite on each (emulators) |
| G5 | Kafka both ways: a table fed from an existing Kafka cluster (exactly-once, offsets in the catalog), and a table's or view's changes produced to one | Joining a company's streams | M | librdkafka's cluster in, out; a restart in each |
| G6 | Databases: `ATTACH 'postgres://…'` / MySQL as a database to read (filters pushed down) and write; their changes streamed in natively (logical replication, binlog) | CDC without Debezium | L | Tables equal the source's under changes; a restart |
| G7 | Sinks: a materialized view or task kept in step in an outside target (Kafka, Postgres upsert, files) | The other half of ETL | M | Exactly-once through failovers |
| G8 ✓ (appends) | Outside engines write Pondra's tables through its Iceberg REST catalog (the owner, 2026-09-28: others should write too; Polaris and Unity Catalog are JVM services and would have to be the source of truth, so Pondra plays their part itself): appends to append tables first, committed by the leader as its own (schema checked, exactly-once, views and the change feed following); keyed tables later (equality deletes as upserts); Delta through catalog-managed commits when Delta has them. Never by writing the published files behind Pondra's back: its catalog is the truth, and a commit it didn't see would break views, row ids and the next publish | Spark, Trino, Flink, PyIceberg, Snowflake and DuckDB write through a REST catalog, as with Polaris and Unity | M | Spark and PyIceberg append; Pondra's views, change feed and Delta copy follow; a retried commit applied once |
| G9 (phases 1–2 ✓) | Other engines' compute for their writes (ADR-029; phase 1 built in round 27, phase 2 in round 28). Files are taken as written: the leader records them and reads only their footers. Each file's rows get ids from its first id plus position (Iceberg v3's row lineage, Delta's row tracking). Deletes are kept as positions, and Pondra publishes its own deletes the same way (purges become maintenance). Keyed tables take upserts. Partition spec, sort order and key are published for writers. Views are fed from the files. The Iceberg REST catalog becomes complete (create, alter, transactions, vended credentials, scan planning). Also fixes the row-id and Kafka-offset limits (ADR-029 decision 11) | Batch writes cost Pondra nothing but the commit, which makes compute and storage separate | L (3 phases) | An outside 1 GB append costs the node its footers and one commit; Spark's `DELETE`/`MERGE` equal Pondra's; views and the change feed follow |

### H. SQL and Python as one (the owner, 2026-09-28; ADR-027)

| # | Item | Why | Size | Proof |
|---|---|---|---|---|
| H1 ✓ | `CREATE FUNCTION` in Postgres's forms (macros become SQL functions; `CREATE MACRO` stays) | The word Postgres users know | S–M | Postgres 17's answers for its forms (done: `harness.py functions`) |
| H2 ✓ | Python functions: per row, vectorized, table; spread over the nodes | Python where SQL can't: text, PDFs, images, APIs | M | One node == three; a worker killed mid-query (done) |
| H3 ✓ | Warm Python workers per node, packages per routine | A `CALL` in milliseconds; the same libraries on every node | M | 2.4 ms warm (done) |
| H4 ✓ | Procedures without limits: `pondra.sql` as the caller, notices back, secrets, no answer needed | "Send an email from a SQL cell" | S–M | Mail to a local SMTP server from HTTP, psql, MCP, JavaScript and the shell (done) |
| H5 ✓ | Decorators that take a notebook's function as it is (imports, helpers, constants) | Python users write Python, not wrappers | S–M | The same function runs in the notebook and on the node (done); PySpark's `udf` too |
| H6 ✓ | Schedules (`CREATE TASK … SCHEDULE`) and the run log (`pondra.runs`) | Jobs that run by themselves, and what they did | M | Every tick's writes once through a failover (done) |
| H7 | Notebooks in the catalog: `.ipynb` in the lake, run as a procedure, on a schedule | The platform on top | M–L | Later: after the console (round 26) |
| H8 ✓ | Spark SQL where PySpark code sends it (the owner, 2026-10-01; built in round 31, part 2: `spark_sql('…')`, `sparksql.rs`, `harness.py sparksql`): `spark.sql(…)` read with Spark's grammar and functions, one statement at a time, and turned into Pondra's own SQL before frames build on it; Pondra's SQL everywhere else; a session's `SET datafusion.sql_parser.dialect = 'spark'` for whole sessions | A PySpark job keeps its SQL as written, with one engine and one SQL underneath | M | Spark's `"text"` literals, backticks, `LATERAL VIEW explode`, and shared names (`floor`, `substring`) answer as Spark does inside `spark.sql`, and frames built on them still compose |

### I. Extensions (the owner, 2026-09-29; ADR-031, proposed)

| # | Item | Why | Size | Proof |
|---|---|---|---|---|
| I1 | DuckDB-style `INSTALL name; LOAD name;`: third parties add functions, table functions, file formats and connectors without rebuilding Pondra, as WebAssembly components (sandboxed, one build for every platform), from a registry or a file | A small binary that still grows with its users' needs | L | An extension written outside the repo adds a function and a format, loads on all five platforms, spreads with queries, and can't reach what it wasn't granted |

### J. The platform on top (the owner, 2026-09-29; proposed, after the base binary)

The owner's order: the base binary first (parity with what it is compared with, SQL, Python and
JavaScript as one, the streamhouse, and the comparisons with Spark, Flink, Fluss and DuckDB), and
the platform after.

| # | Item | Why | Size | Proof |
|---|---|---|---|---|
| J1 | The server's catalog (ADR-032 §9): databases anywhere by name, attachments, secrets, users and extensions for every database of a server | Users live above the databases (round 29 needs it); no listing per connection | M | Databases in two buckets served as one server; listing costs no request; a user made once reaches every database |
| J2 ✓ | A workspace (ADR-033, built 2026-09-30): `.sql`, `.py` and notebooks as versioned files, edited in the console, run with parameters from every door (`CALL run(…)`), recorded in the run log, scheduled | ETL without another tool; SQL and Python calling each other | M–L | A SQL file and a Python file chained with parameters, from SQL, Python, JavaScript and the console; a schedule runs a notebook; the run log names each version |
| J3 | Dashboards and reports: a notebook with parameters shown read-only | What a team shares | M | A report with inputs, refreshed by a schedule |
| J4 | Connections, for ETL in code and on a canvas (the owner, 2026-10-01; proposed, an ADR first): a named, typed connection to a source or target system that holds its details and points to a secret for its credentials | A graphical or code ETL tool has to keep many systems' logins safely, and a pipeline should never hold a password | M | A pipeline in SQL, Python and the console reads and writes through one connection; nobody sees the password, not even an admin; one `ALTER SECRET` rotates it for every pipeline; every use is in `pondra.audit` |

**J4 in brief (as proposed to the owner on 2026-10-01).** Secrets stay the one place for
credentials. They already are sealed by the master key or a KMS command, never shown back, granted
with `GRANT USAGE ON SECRET`, audited, and have a per-session temporary form (ADR-035). A
connection adds the wiring, in the catalog, in plain sight:

- `CREATE CONNECTION crm (TYPE postgres, HOST 'db.local', PORT 5432, DATABASE 'sales', SECRET crm_login)`:
  the type, host, port, database and options are visible and editable (the console's form for a
  connection shows them); the credentials are only named, by the secret.
- A pipeline names the connection, never the secret or the password: `ATTACH CONNECTION crm`,
  `read_table(crm, 'public.orders')`, `COPY … TO CONNECTION warehouse`, a canvas step's
  "source: crm". The same object serves graphical and code ETL.
- `GRANT USAGE ON CONNECTION crm TO etl` checks the secret's grant as well; `TEST CONNECTION crm`
  reaches the system and says what failed; each use is a row in `pondra.audit`.
- Rotation is one `ALTER SECRET crm_login …`; every connection and pipeline using it follows.
- Later, a secret can be a pointer to an outside vault (AWS Secrets Manager, Azure Key Vault,
  HashiCorp Vault, GCP Secret Manager), fetched when used and kept in memory a short while;
  OAuth sources (Salesforce, Google) as a secret type whose refresh token the node renews.
- It builds on G6 (databases attached), G7 (sinks) and the workspace (J2).

## The road to 1.0 (proposed 2026-09-30, after round 28)

Twenty-eight rounds made Pondra broad: a streaming store, a lakehouse and a SQL engine in one
binary, eight doors (HTTP, Postgres, Flight SQL, Kafka, MCP, an Iceberg catalog, the Python and
JavaScript clients), other engines reading and writing its tables, a console, a docs site. What
stands between it and *mature* is depth, not more breadth. This one session found:

- **doors that differ:** the console can't read files on the node's machine and the shell can;
  one key lookup takes 0.17 ms through `/lookup` and 6.5 ms through the Postgres port; `BEGIN`
  and `COMMIT` are accepted on the Postgres port and do nothing;
- **a node stopped by its input:** a delete file whose column came back as a string view panicked
  the node, and every panic stops a node (`panic = "abort"`);
- **the gates not run, and a regression missed:** rounds 27 and 28 didn't re-measure
  sqllogictest or TPC-H. Measured on 2026-09-30: sqllogictest held (two records fewer: three
  answers in an order SQL leaves open, one record newly passing), but TPC-H SF1 from memory went
  from 2.34 s (round 26) to 3.48 s. Files that carry a lineage (written without a leader, as
  `pondra sql` does, or while something follows the table: round 27) and an append table's files
  with deleted rows (round 28's positions) were read from Parquet every time, never through the
  hot columns. Fixed the same day: 2.03 s, faster than round 26;
- **the main claim unproven:** on GitHub's runners, over the internet, three nodes ran TPC-H SF10
  in 23.3 s and six in 23.4 s, against one node's 25.3–25.5 s (v0.26.0). Shuffles over links of up
  to 67 ms and as slow as 69 MB/s lose, so the guard keeps most queries on one node. Nothing has
  run in one data centre.

**The proposal: from round 29 to 1.0, no new surface unless a bar below needs it.** Each round
takes one angle to its bar; every round re-measures all of them.

### The scorecard

| Angle | Today (measured) | The bar for 1.0 | Round |
|---|---|---|---|
| **Right answers** | ~~sqllogictest 74.5%~~ round 31: sqllogictest 98.0% of the records not named an exception (23,032 of 23,493; 92.9% of all 24,783), each exception named with its reason; TPC-H 22 of 22 on 1, 3 and 6 nodes; TPC-DS SF1 99 of 99 equal to DuckDB on one node (98 on three: q72 ran out of this box's memory); random queries against DuckDB and three nodes (`tools/random_sql.py`) | sqllogictest ≥ 95%, every exception named; TPC-DS's 99 equal to DuckDB's; 100,000 random queries: one node == three == DuckDB | 31 |
| **Never loses data** | kill -9 and injected crashes, leader kills: every event once, views exact | also a 24-hour soak (0 lost, memory flat); every release's lake opens in the next; a rolling upgrade; time travel and `UNDROP` within a retention you set (today 60 s) | 33 |
| **Safe** | ~~read, write and admin tokens; plain HTTP between nodes; any panic stops the node~~ round 29: users and grants to a column, TLS everywhere, mutual TLS, an audit log, quotas, doors fuzzed, a request's panic an error | TLS on every door and between nodes; users and grants down to a column; an audit log; quotas; every wire parser fuzzed; no request can stop a node | 29 |
| **The same from every door** | ~~local files for the shell only; the fast lookup on `/lookup` only; no transactions on the Postgres port~~ round 30: the doors matrix (9 features × 6 doors, each right or refused by name); `BEGIN`…`COMMIT` on Postgres, HTTP and the clients; Postgres's error codes on every door | every feature × every door in one test; `BEGIN`…`COMMIT` everywhere; Postgres's error codes; the console signed in as the shell is | 29, 30 |
| **Fast: analytics** | TPC-H SF1 from files 3.60 s, DuckDB 3.70 s (Polars 3.79 s, the same day); from memory 2.03 s against DuckDB's own tables 1.74 s; Q1 10x Postgres | from memory ≤ DuckDB's own tables at SF1 and SF10; ClickBench published | every round, 32, 34 |
| **Fast: points and writes** | lookup p50 0.19 ms (`/lookup`), ~~6.5~~ **0.29 ms** (Postgres port, psycopg, round 30; Postgres 0.09 ms); one-row insert 2.0 ms (Postgres 0.33 ms); one-key UPDATE 2.5 ms; pgbench runs, balances right: 134 tps on 1 client (Postgres 997), 95 on 4 (Postgres 1,793) | ≤ 0.5 ms from every door; `pgbench` runs, balances right | 30 |
| **Fast from cold** | 6.5–8 s before a node serves, on R2-like storage | a third of that; no burst above the bucket's limits (C5) | 29 |
| **Scales out** | never run in one data centre; GitHub's runners (above): 3 nodes 8% faster than one | TPC-H SF100: time falls 1 → 3 → 6 machines; faster than Spark on the same VMs | 34 |
| **Streaming** | Nexmark's q1, q2, q5, q7, q11: 2.1x Flink 2.3 on 2 vCPUs (10 M bids 14.3 s against 30.4 s, 2026-10-01; 2.2–2.9x in round 21); pipelines of views in one commit (round 30); Fluss compared from its docs | all of Nexmark against Flink; Fluss run head to head | 34 |
| **Easy to run** | `/metrics`, `/stats`, node logs | a query history table, a query's plan and time across the nodes, a slow-query log; a container image, a compose cluster, a Helm chart; drain before stop; an upgrade guide | 33 |
| **Fits in** | psql, dbt, SQLAlchemy, JDBC, ODBC, ADBC, Npgsql (Power BI's drivers; Power BI itself not yet run); Spark and PyIceberg write | also Metabase, Superset, Grafana, Tableau, DBeaver; Postgres and MySQL attached; sinks; Kafka partitions | 36 |
| **Runs anywhere** | Linux, macOS, Windows binaries; pip; npm | also in-process, in the browser; Homebrew, winget, a container; signed binaries | 35, 37 |

### How the work changes

- **Gates as one command** (round 29's first step): `tools/gates.py` runs sqllogictest, TPC-H
  SF1 against DuckDB, `vs_postgres.py` and a Nexmark subset, and appends a line to
  `logs/gates/README.md` (its first lines are 2026-09-30's, by hand). A release is tagged only
  when no gate dropped (a drop explained, as an order SQL leaves open, is recorded as such).
- **The doors matrix** (round 30): one list of features, run from SQL over HTTP, Postgres, Flight,
  Python (connection and frames), JavaScript and MCP. A door that can't do one refuses it by name.
- **A real workload:** one real dataset and job of the owner's, run on Pondra every round. The
  owner's Windows sessions found more than any test this month.
- **Scale runs start as soon as there are machines:** round 34 is where their fixes are gathered,
  not when they start.

## The rounds

Each round is about one session like the last sixteen, ending with tests on local disk,
simulated R2 and real R2, an ADR, and a bundle.

| Round | Theme | Items | What you'd see at the end |
|---|---|---|---|
| 17 ✓ | Install anywhere (done: ADR-018) | A1, A2, A3, A5 (measure), A6, D3 | `pip install pondra` works in a fresh Ubuntu 22.04 notebook; both flakes fixed |
| 18 ✓ | A database you can shape (done: ADR-019) | E5, A7, C1 (measuring) | `lake.schema.table`, DDL and views in SQL; the `.exe` on your laptop; the cluster bench measures the network |
| 19 ✓ | Change any row (done: ADR-020) | E6, C1 (the guard) | `UPDATE`/`DELETE`/`MERGE` on every table with system columns, streaming following every change; a cluster never slower than one node |
| 20 ✓ | Fewer objects, any layout, streams joined (done: ADR-021) | the owner's questions, C1 (round 19's run) | a trickle of INSERTs writes under a third of the objects; `PRIMARY KEY` with `partition_by`/`cluster_by` (Hilbert); `COPY`; stream joins and sliding windows |
| 21 ✓ | Shape it further, and more of Flink (done: ADR-022) | E8, E7, C3, F (streaming) | `ALTER TABLE … RENAME/DROP COLUMN`, widening; views filled from existing rows; dedup by event time; Nexmark against Flink; the DataFrame API designed |
| 22 ✓ | Frames and procedures (done: ADR-023) | the DataFrame API (`dataframe-api.md`), the owner's macros and procedures | `pondra.frame` and `pondra.spark` over SQL, equal to Polars and PySpark; SQL and Python mixed every way; macros and procedures (SQL, Python) in the catalog |
| 23 ✓ | Read and write anything (done: ADR-026) | G1–G5, secrets; D1 set up and measured | files, Delta and Iceberg anywhere read, joined and written, spread; GCS and Azure lakes; Kafka clusters in and out; `CREATE SECRET` |
| 24 ✓ | SQL and Python as one (done: ADR-027) | H1–H6 | `CREATE FUNCTION` in SQL and Python; procedures that send mail from a SQL cell; decorators that take a notebook's function; schedules and a run log |
| 25 ✓ | One vocabulary, open writes, live answers (done: ADR-028) | E9, E10, G8, B3, temporary tables, changes to attached lakes | `read_*`/`write_*` everywhere (the tools' names as fallbacks); Spark and PyIceberg append to Pondra's tables through its Iceberg catalog; live queries; function results reused; `CREATE TEMP TABLE`; `UPDATE`/`MERGE` on attached lakes from any node |
| 26 ✓ | The console, the server and the docs (done: ADR-030, ADR-032, ADR-034) | A4, E1, E2, the server, E11 | a console at `/` with SQL, Python and text cells, then tabs of notebooks, SQL, Python and data files edited in place (ADR-034), built to be extended; a folder of lakes (local or in a bucket) served as databases (`pondra serve --lakes`); dbt and BI tools through Postgres's catalog; a documentation website on GitHub Pages covering everything, each example tested |
| 26+ ✓ | The workspace (done: ADR-033, moved up by the owner) | J2 | `.sql`, `.py` and notebook files of the lake's run with parameters from every door (`CALL run(…)`), as jobs and on schedules, each run logged by the file's version |
| 27 ✓ | Anyone's compute, phase 1 (done: ADR-029) | G9: appends as written, the id limit | other engines' appends cost the node only its footers and a commit (a million rows: 0.01 s of CPU against 0.24 s copied); layout published for writers; tables made, renamed and dropped through the catalog; row ids and log places that can't wrap |
| 28 ✓ | Anyone's compute, phase 2 (done: ADR-029) | G9: changes as written, and what phase 1 moved on | Spark's and PyIceberg's `DELETE`, `UPDATE`, `MERGE` and overwrites on Pondra's tables, copy-on-write and merge-on-read; deletes published as positions (Iceberg delete files, Delta deletion vectors), purges as maintenance; changes carried over Pondra's merges by row id; multi-table transactions; keyed tables published every tier round and taking other engines' upserts and deletes; followers fed from the files in one commit; `/watch`, the change feed and Kafka topics carrying file commits |
| 29 ✓ | The owner's console list, then safe to share (as approved, and robust) (done: ADR-034, ADR-035; 0.28.0) | the console (part 1), E3 (part 2), C5 (part 3); no request can stop a node | **part 1**, the owner's list from 0.26 on Windows: Python found with a time limit and chosen in the console, Stop interrupting a Python cell, the grid's menus and filters, copies and downloads in every form, Messages and Runs worth reading, the plan as a graph with a profile, charts to choose and save, Format for SQL and Python (right-click, Shift+Alt+F), calmer toolbars, the right pane and Settings reworked, colours, settings kept on the machine, light by default, the page's first load back under its 70 KB budget, uv checked in CI, the docs site building from a clean checkout; the owner's second list (2026-10-01): Markdown cells drawn as GitHub does, a SQL cell's Chart and Plan kept with its notebook, tabs that scroll and pin, pages of rows kept on the node (`pages.rs`), Jobs apart from History (schedules now, pipelines as a section), a clearer Data tree, Format selection and file, Settings as sections (`register.setting`); the owner's third list (2026-10-01): rows a page, a quieter pager, the Run ▾ in the editor's right-click with Create as, Data profile and Query profile named apart, a right-click menu for every kind of object with Script as in SQL or Python (`register.objectKind`), a cell made Python or SQL, `pondra.tables` so the shell's `.tables` says what a materialized view is, cells added between cells, `SELECT ts::date, *` named as other engines name it; **part 2 built (2026-10-01):** users and roles with grants down to a column, one check at every door, passwords and tokens kept as hashes, secrets sealed by a master key or a KMS command with `GRANT USAGE ON SECRET` and `CREATE TEMPORARY SECRET`, the console signed in as the shell is, TLS on every door (plain only from the node's machine), mutual TLS between nodes, `pondra.audit`, quotas (`MAX_QUERIES`, `STATEMENT_TIMEOUT`), every door fuzzed, a panic in a request answered as an error (ADR-035 "Built"), `tools/gates.py`; **then, after security** (the owner, 2026-10-01): **built:** every file keeps its versions (ADR-035 §8: each save kept with who and when, **Versions…** shows what changed and restores one, notebooks one file each, any notebook a job); a run whose node stopped under it marked `stopped` in `pondra.runs`; the cold start about 40% shorter (C5: 3.1–4.4 s against 6.5–7.7 s), a request budget per bucket and node that backs off on 503 and 429, log keys with a random first part, the orphan sweep one part at a time, the bell's 429 counted as rung; **left:** the last of C5's step 1 (SlateDB reading the WAL once) and its bigger proof (six nodes at once, a 50,000-file drop) |
| 30 | Behaves like a database, from every door | transactions, errors, the point path, constraints | `BEGIN`…`COMMIT`/`ROLLBACK` on every door as one commit (reads from one snapshot, a conflict as Postgres's serialization error); Postgres's error codes everywhere; the key lookup's fast path from every door and prepared statements' plans kept; one-row inserts faster; `UNIQUE`, `CHECK` and identity columns; the doors matrix; pipelines as Databricks' DLT (now Lakeflow pipelines) has them (the owner, 2026-10-01: round 30): pipelines of materialized views, each following the one before in the same commit (bronze → silver → gold; until then a view of a view is refused, never left empty), expectations (`CHECK … ON VIOLATION DROP ROW | FAIL`, the rows dropped counted), history kept per key (SCD type 2) from a change feed, and the pipeline's graph in the console. Gates: `pgbench`'s own script runs with the balances right; a lookup through the Postgres port ≤ 0.5 ms |
| 31 (part 1 ✓, 0.29.0; part 2 under way) | Correct SQL, proven | D1 to its end, D2, TPC-DS | **built (2026-10-01):** sqllogictest 98.0%, every exception named (`slt_check.py`: plan text, a write explained, what the runner makes in Rust, the node's memory, microseconds, an order no query asked for, interval arithmetic); `SET`/`RESET`/`SHOW`, `PREPARE`/`EXECUTE`/`DEALLOCATE` per session (a script its own), Spark's functions (DataFusion's names untouched; Spark's for all in the `spark` dialect), Arrow files, DataFusion's writer options through `COPY`, `CREATE TABLE` refusing a table that is there (42P07), `SELECT … INTO`, MERGE's mistakes refused, `n * INTERVAL` as Postgres; TPC-DS's 99 equal to DuckDB (`tools/tpcds_check.py`); `tools/random_sql.py`; `tools/api_coverage.py` (Polars 17–29% of each part by name, PySpark's DataFrame 45%, functions 39% by name in SQL); **part 2 (the owner, 2026-10-01), built:** `CREATE OR REPLACE` and `IF NOT EXISTS` for every kind of object (both together refused; `IF NOT EXISTS` only where replacing would drop data or permissions: schemas, databases, users, roles); a notebook's SQL cell named (`→ df`, saved as `%%sql df <<`) is a frame in the page's Python, and a SQL cell reads the page's Python tables by name, in the console and in a notebook's run; `ALTER MATERIALIZED VIEW v DETACH` (the flow stops, the rows stay a table, what follows it keeps following that table; a GROUP BY view's stays a merge table; a view keeping windows refused; one fed by a topic stops reading it); **Versions…** showing a notebook's changes cell by cell, each cell's lines highlighted as its language, and SQL and Python files highlighted; the Windows build starting again (an 8 MB stack for the work). **Still to build:** `spark.sql(…)` in Spark's grammar and functions, a statement at a time, turned into Pondra's SQL before frames build on it; chained materialized views renamed from "pipelines" to a name of Pondra's own (the owner's choice), so a future ETL tool's pipelines aren't confused with them; TPC-DS on three nodes in full, 100,000 random queries, Avro files, identity and UNIQUE. (q72's join order: round 32.) **The bar:** sqllogictest ≥ 95%, every exception named; Postgres's interval arithmetic (`n * INTERVAL '37 seconds'`, refused today: found by the console review); TPC-DS's 99 queries == DuckDB, one node and three; 100,000 random queries (SQLancer's oracles) one node == three == DuckDB; Polars and PySpark coverage published |
| 32 | Lean and fast (the owner, 2026-10-01: a round only for optimizing) | sizes, speed, the tests' time, stale docs | the binary's size back down (what each crate adds, measured; features off by default that most don't use), start-up and idle memory, a session's making cheaper; one node: TPC-DS's slow ones (q72's join order: 81 s against DuckDB's 1 s), TPC-H and ClickBench from memory against DuckDB's own tables; several nodes: what the cluster bench shows (shuffles, the guard's choices); the suite, the gates and the benchmarks fast enough to run whole every round (they have grown slow: parallel suites, smaller data where it proves the same); every stale doc brought up to date (the site, ADRs marked superseded where they are). The gate: every number holds or improves, and the binary and the suite's time fall |
| 33 | Run it for years | upgrades, recovery, observability, deployment; C4 | a lake format version, and a lake of every release since 0.22 opening in the new one (in CI); a rolling upgrade of a mixed-version cluster; time travel in SQL (`AT`), retention per table, `UNDROP`, zero-copy `CLONE`, `RESTORE`; a query history table, plans and times across the nodes (the console's History showing each statement's plan and profile from it), a slow-query log, traces; a container image, a compose cluster, a Helm chart; **environments** (the owner, 2026-10-01, a note to weigh in this round): dev, staging and prod for the workspace's files, jobs, tasks and materialized views, each with its own settings, secrets and connections, and promotion from one to the next in CI (as Databricks' asset bundles do), so a change is tried before it reaches production; `pondra service install` (a node that starts with the machine and restarts if it stops, on the same lake and port: a systemd unit, a launchd agent, a Windows service; the owner, 2026-10-01, optional); drain before stop; a 24-hour soak on R2 |
| 34 | Scale, proven (machines: the owner's decision 9) | C1, C2, C3, ClickBench, burst | 1 → 3 → 6 machines in one data centre, TPC-H SF100 time falling, against Spark on the same VMs; all of Nexmark against Flink; Fluss head to head; ClickBench submitted; what they find fixed (six nodes' 246 s settle first; the guard spreading a query that reads much and sends little: on 0.27.0's SF10 runs it kept q1, q3, q12 and q19 on one node, 21.1 s where 19.4 s was there on 6 runners); the cluster benchmark's data made in parts on every runner, so SF100 fits on runners of 14 GB; `INSERT … SELECT` and `CREATE TABLE AS` computed and written by every node at once, each its share's files, recorded in one commit (as `COPY … TO` already is); serverless bursts for a big query (an ADR of its own) |
| 35 | In-process and in the browser | B1, B2, B4 | `pondra.open(…)` in Python without a server, Arrow straight into pandas and Polars; a lake queried in a web page (WebAssembly), against DuckDB; the console's editor drawing only the lines in sight (typing in a 1,000-line file: 28 ms a key today, under 8 the aim)-WASM |
| 36 | Fits in | G6, G7, E2, Kafka's partitions | Postgres and MySQL attached and their changes streamed in; sinks; Kafka partitions and transactions; Metabase, Superset, Grafana, Tableau and DBeaver checked; a SQLAlchemy dialect and a dbt adapter packaged |
| 37 | 1.0 | the promises | what stays stable (the lake format, SQL, the APIs) and how things are deprecated; a security review; signed binaries for Windows and macOS; Homebrew, winget and a container image; the docs complete (every feature from every door, limits, troubleshooting, upgrading); a benchmarks page one command re-runs; **a review of every document, the site and the code** against what was decided (the owner, 2026-09-30): Pondra's own names first, Polars', PySpark's and DuckDB's shown as optional; what was superseded marked or gone |
| after 1.0 | Depth, then the platform | F by evidence, I1, J1, J3 | streaming depth (timers, CEP, Top-N, a watermark per partition), keyed compaction by key range, adaptive stages and raced stragglers (the owner, 2026-09-30, on Spark's DAGs), tasks that run after others (`CREATE TASK b AFTER a`: a DAG of jobs, as Airflow's, in the catalog; the same from Python (`@pondra.task(after=…)`) as from SQL; drawn and edited as a graph in the console: the owner's idea, 2026-09-30), a vector index, `INSTALL`/`LOAD` extensions, the server's catalog, reports, lineage and entity-relationship diagrams (the owner, 2026-09-30), a managed service |

**Every round, whatever its theme** (the owner's rules: nothing half-done, performance only goes
up, scale-out is the point):

- the suite on local disk, simulated R2 and real R2, and CI on five platforms;
- performance gates: TPC-H SF1 and SF10 on one node against DuckDB, the in-memory run, and the
  cluster bench at 3 and 6 nodes (the owner starts it, `binary: ci`): each holds or improves;
- from round 23, the sqllogictest pass rate (D1), which never drops and climbs every round;
- a new feature works from SQL, Python (connection and frames), JavaScript, Postgres and MCP
  alike (ADR-025), or is refused by name;
- an ADR, and a release (a tag publishes what the build tested).

Why this order:

- **Round 17 comes first** because it needs nothing from you but decisions. It also makes every
  later demo possible: a notebook, a laptop, a CI runner.
- **Round 19 is the owner's request**, and it needs the row identity that `MERGE`, CDC and the
  system columns all rest on; it is design work before code.
- **Proof at scale needs machines,** and runs on GitHub's runners whenever the owner starts the
  bench: its fixes can land in any round, as the numbers come in.
- **Reading and writing anything comes first** (round 23) because it is what a new user tries
  first ("point it at my data"), and its `CREATE SECRET` is what round 24's procedures need for
  mail, APIs and databases.
- **SQL and Python as one comes before the console** (round 24) because the console's Python
  cells, schedules and notebooks run on its workers and run log.
- **The engine before the console** (round 25, the owner's choice): the names the console shows,
  the writes other tools make and the live answers it draws all come first, and each is testable
  here without a browser or Windows.
- **The console, the server and the docs come together** (round 26, the owner's choice,
  2026-09-29). A folder of lakes served as databases is what the console lists and what dbt and
  BI tools connect to. The docs site is the owner's ask: "even I can't know exactly what things we
  have".
- **Anyone's compute next** (rounds 27 and 28, the owner's choice): ADR-029's two phases, after
  the docs and the console.
- **Security after that** (round 29): a server others connect to needs users, grants and TLS
  before anyone else's data goes in. Outside writers (ADR-029) make grants matter more. A request
  that can stop a node is a security hole too, so fuzzing and panics answered as errors go with it.
- **A database's behaviour before conformance** (round 30): the doors must agree, and
  transactions exist, before the tests that measure them are finished.
- **Correct SQL before scale and before promises** (round 31): what is proven at scale and
  promised stable should be the finished SQL.
- **Lean and fast before running it for years** (round 32, the owner, 2026-10-01): the binary,
  the tests and the benchmarks had grown; one round optimizes and nothing else, so what is proven
  at scale and promised is also small and quick.
- **Upgrades, recovery and operations before the scale runs** (round 33): a soak and a rolling
  upgrade are what a data-centre run should exercise.
- **Scale as soon as there are machines** (round 34 gathers its fixes): it is the main claim, and
  the one only machines can settle.
- **The in-process library and the browser after the promises** (round 35): the library exposes
  the lake format and the APIs, which should settle first.

## What only you can decide

1. **The repository and the license: decided.** `alimardon123/pondra` is public and, since round
   21, MIT OR Apache-2.0. A managed cloud service may come later, on top.
2. **Linking your laptop.** It would help with:
   - runs bigger than this 2-core sandbox allows: Flink for Nexmark, TPC-H SF10 to SF100 if the
     disk allows;
   - pushing to GitHub with your own git setup, when you ask me to, instead of you pushing
     bundles.

   Tell me its cores, memory and free disk. Through the link I work in a Linux environment on
   the laptop. So Windows-only checks, the `.exe` and Power BI, are for you to run, with my
   scripts, or for GitHub's Windows runners.
3. **Publishing.** PyPI's trusted publisher is set and 0.22.0 is there. npm: the `NPM_TOKEN` secret
   publishes the first release (0.22.1, as npm refused 0.22.0's paths), then each package's
   Trusted Publisher set to `release.yml` and the token removed. Tag `v0.22.1` next. winget and
   Homebrew when users ask. crates.io can wait (`publish = false`).

4. **Decided, 2026-09-29:**
   - round 26 is the console, the server and the docs site together (Starlight, on GitHub
     Pages);
   - ADR-029 comes after them, in rounds 27 and 28;
   - "our extension framework" means DuckDB-style `INSTALL`/`LOAD`: ADR-031, proposed.
5. **When extensions come (ADR-031).** Their round is still open. They could go after anyone's
   compute (round 29, pushing security to 30), or with depth (33+).
6. **The plural's name.** `pondra serve --lakes` is built. "Lake hub" (or another word) could name
   the mode in the docs and the console; the flag can stay.
7. **The server's catalog (J1).** Proposed in ADR-032 §9. The owner put the base binary first; J1
   may need to come before round 29, since users live above the databases. (The workspace, J2, is
   built: ADR-033.)

8. **A freeze on new surface until 1.0** (proposed 2026-09-30): new features only where a bar of
   the scorecard needs them; new ideas go to "after 1.0".
9. **Machines for round 34.** Three to six VMs in one region for a few hours at a time
   (`tools/cloud/` sets them up; a cloud trial's credit covers it). The sooner the better: the
   scale runs can begin during any round.
10. **An `INSERT` of a key that exists.** Today it replaces the row (an upsert), as Fluss's and
    Paimon's key tables do; Postgres raises an error instead, and Snowflake's and Databricks'
    ordinary tables don't enforce keys at all. My recommendation: keep the upsert, and add `WITH (on_duplicate =
    'error')` for tables that want Postgres's behaviour, `ON CONFLICT` working in both.
11. **Postgres and MySQL attached (G6) move from round 29 to 35**, so round 29 is security alone.

## What not to do yet

- **No fully static musl build:** its allocator slows multi-threaded Rust badly. An old-glibc
  build gets the same reach.
- **No browser engine before the pip package, the console and live queries.** Those reach more
  people, sooner.
- **No chase after Flink's full list (timers, CEP)** until Nexmark shows which gaps cost the most
  (round 21's five queries all run; q3, q4 and q8, over persons and auctions, are next).
- **No enterprise governance suite beyond TLS, grants and an audit log** until someone other than
  us uses it.
- **No DuckLake as Pondra's own catalog:** it needs a database, and Pondra needs none.

## Sources

- DuckDB-WASM:
  - [overview](https://duckdb.org/docs/lts/clients/wasm/overview)
  - [extensions in the browser](https://duckdb.org/docs/lts/clients/wasm/extensions)
  - [OPFS persistence](https://duckdb.org/2026/09/18/opfs-wasm)
  - [limits](https://duckdb.org/docs/current/operations_manual/limits)
  - [the VLDB 2022 paper](https://db.in.tum.de/~kohn/papers/duckdb-wasm-vldb22.pdf)
- MotherDuck: [hybrid queries](https://motherduck.com/docs/key-tasks/running-hybrid-queries/)
- DuckDB:
  - [local UI](https://duckdb.org/2025/03/12/duckdb-ui)
  - [Pyodide](https://duckdb.org/2024/10/02/pyodide)
  - [Python build](https://duckdb.org/docs/lts/dev/building/python)
- DuckLake: [1.0 announcement](https://ducklake.select/2026/04/13/ducklake-10/)
- PGlite:
  - [pglite.dev](https://pglite.dev/)
  - [about](https://pglite.dev/docs/about)
  - [pglite-socket](https://pglite.dev/docs/pglite-socket)
  - [repository](https://github.com/electric-sql/pglite)
  - [Electric sync](https://electric.ax/sync/pglite)
- DataFusion and WebAssembly:
  - [issue #177](https://github.com/apache/datafusion/issues/177)
  - [issue #15599](https://github.com/apache/datafusion/issues/15599)
  - [wasm playground](https://github.com/datafusion-contrib/datafusion-wasm-playground)
- Shipping Rust binaries:
  - [maturin bindings](https://www.maturin.rs/bindings)
  - [maturin distribution](https://www.maturin.rs/distribution.html)
  - [cargo-zigbuild](https://github.com/rust-cross/cargo-zigbuild)
  - [binaries on npm](https://blog.sentry.io/publishing-binaries-on-npm/)
  - [musl and mimalloc](https://www.tweag.io/blog/2023-08-10-rust-static-link-with-mimalloc/)
