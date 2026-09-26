# Pondra: what's left, and in what order (after round 19)

**Date:** 2026-09-25 · **Status:** proposed; the order in "The rounds" is what I recommend, the
decisions in "What only you can decide" are yours · **Builds on:** ADR-002 to ADR-017,
`prototype-status.md`, `comparison-spark-flink-fluss.md`

**Progress (2026-09-26):** rounds 17, 18 and 19 are done (ADR-018, ADR-019, ADR-020).

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

Still waiting:

- **C1's numbers:** a 3-node run on round 19's code (the guard on), then machines in one data centre.
- **Publishing:** the package names and the repository decision below.

Round 20 is next: the rest of `ALTER TABLE` (rename and drop columns and tables, widen types:
column ids in the files), materialized views filled from the rows already there, and C1 on
machines in one data centre.

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

## Everything still open, in six tracks

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
| B3 | Live queries: `GET /live?sql=…` pushes a new answer whenever a commit touches the query's tables; Python and JS clients subscribe | Dashboards that keep themselves current (PGlite's lesson) | S–M | A dashboard updates within 50 ms of a write, locally |
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
| E8 | The rest of `ALTER TABLE`: rename a table, rename and drop columns, widen a column's type | The owner's third Windows session. Files and the log match columns by name, so a renamed column would lose its values and a dropped one come back with a new column of its name: every column needs an id that the files carry (Iceberg's field ids) | M | Each under streaming ingest, with views and Delta/Iceberg readers following; old files read by id |

### F. Depth, ordered by what the tracks above show

- **Streaming:**
  - as-of joins in views that wait for the looked-up table to catch up to the event's time;
  - a watermark per partition or per node;
  - sliding windows;
  - late rows to a side table;
  - timers.
- **Distributed:**
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

## The rounds

Each round is about one session like the last sixteen, ending with tests on local disk,
simulated R2 and real R2, an ADR, and a bundle.

| Round | Theme | Items | What you'd see at the end |
|---|---|---|---|
| 17 ✓ | Install anywhere (done: ADR-018) | A1, A2, A3, A5 (measure), A6, D3 | `pip install pondra` works in a fresh Ubuntu 22.04 notebook; both flakes fixed |
| 18 ✓ | A database you can shape (done: ADR-019) | E5, A7, C1 (measuring) | `lake.schema.table`, DDL and views in SQL; the `.exe` on your laptop; the cluster bench measures the network |
| 19 ✓ | Change any row (done: ADR-020) | E6, C1 (the guard) | `UPDATE`/`DELETE`/`MERGE` on every table with system columns, streaming following every change; a cluster never slower than one node |
| 20 | Shape it further, and proof at scale | E8, E7, C1, C3, D1 (start) | `ALTER TABLE … RENAME/DROP COLUMN`; TPC-H SF10 at 1/3/6 machines in one data centre; Nexmark against Flink; the SQL test files' pass rate |
| 21 | Use it from anything | A4, B3, E1, E2 | A console at `/`, live queries, dbt and Power BI working |
| 22 | In-process | B1, B2 | `pondra.open(…)` in a notebook reads and writes a cluster's lake, no server |
| 23 | Safe to share | E3, D2 | TLS, grants, audit; random-query checks against DuckDB |
| 24 | In the browser | B4 (after the DuckDB-WASM check) | A lake queried in a web page, straight from the bucket |
| 25+ | Depth | C2, C4, E4, then F by evidence | Whatever the scale runs and first users show matters most |

Why this order:

- **Round 17 comes first** because it needs nothing from you but decisions. It also makes every
  later demo possible: a notebook, a laptop, a CI runner.
- **Round 19 is the owner's request**, and it needs the row identity that `MERGE`, CDC and the
  system columns all rest on; it is design work before code.
- **Proof at scale needs machines,** and runs on GitHub's runners whenever the owner starts the
  bench: its fixes can land in any round, as the numbers come in.
- **"Use it from anything" comes before the in-process and browser work** because a console and
  live queries are short steps on what exists. The library split is the bigger change.
- **Security comes before the browser.** It matters as soon as anyone else's data goes in.

## What only you can decide

1. **The repository.** Three choices:
   - **Make `alimardon123/pondra` public.** This gives free, unlimited GitHub Actions with
     4-vCPU Linux runners; private repos get 2-vCPU runners and a monthly allowance. It also
     means CI can publish releases, wheels and npm packages, and people can find the project.
     It needs a license first. Apache-2.0 (as DataFusion) or MIT (as DuckDB) gives the widest
     use; a source-available license (BSL, ELv2) keeps others from selling it as a service; AGPL
     sits between. Every round's history has been scanned for the bucket's keys and account; a
     last full scan with a dedicated tool belongs just before going public. Check the name too:
     a small app already uses pondra.app.
   - **Keep it private, and add a small public `pondra-bench` repo** holding only the benchmark
     workflow, which pulls the binary from your bucket. This gets the bigger free runners for
     the multi-machine runs and exposes no source.
   - **Stay private for now.** Smaller runners with a monthly allowance, and no public releases.

   My recommendation: the bench repo now, and the main repo public once round 17 makes
   `pip install pondra` work and you've picked a license. A first impression that installs in
   one line is worth waiting for.
2. **Linking your laptop.** It would help with:
   - runs bigger than this 2-core sandbox allows: Flink for Nexmark, TPC-H SF10 to SF100 if the
     disk allows;
   - pushing to GitHub with your own git setup, when you ask me to, instead of you pushing
     bundles.

   Tell me its cores, memory and free disk. Through the link I work in a Linux environment on
   the laptop. So Windows-only checks, the `.exe` and Power BI, are for you to run, with my
   scripts, or for GitHub's Windows runners.
3. **Package names.** Reserve `pondra` on PyPI, npm and crates.io. Publishing can then go
   through GitHub's trusted publishing, so no tokens pass through me.

## What not to do yet

- **No fully static musl build:** its allocator slows multi-threaded Rust badly. An old-glibc
  build gets the same reach.
- **No browser engine before the pip package, the console and live queries.** Those reach more
  people, sooner.
- **No chase after Flink's full list (timers, CEP)** until Nexmark shows which gaps cost the most.
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
