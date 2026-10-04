# AGENTS.md — working on Pondra

Read this first, then `README.md` (what it does), `docs/adr-005-every-node-writes.md` (why it
works this way) and `docs/adr-028-one-vocabulary-and-open-writes.md` (the latest round;
`docs/adr-029-anyones-compute-one-catalog.md`, phase 1 built in round 27, phase 2 in round 28).
`docs/prototype-status.md` has the measured numbers and what's left.

## What this is

**Pondra** is one Rust binary that is a streaming store, a lakehouse and a SQL engine at once.
Object storage (a local directory, S3, R2, MinIO) holds *all* the state: there is no Postgres, no
ZooKeeper, no Kafka, no JVM. Start the same binary on several machines pointed at the same bucket
and they form a cluster. Goal: replace Kafka + Flink + Spark + a metastore for the common jobs,
and compete with Databricks / Snowflake / Fluss on simplicity and cost.

The owner's design principles, which every change must respect:

1. **One small binary that runs anywhere**, DuckDB-like, and joins a cluster with almost no setup.
2. **As serverless as possible**: no always-on services besides the nodes themselves.
3. **SPMD, not driver/executor** (Bodo-style): every node runs the same code on its slice.
4. **No JVM, no Spark, no Flink, no Fluss needed.**
5. **Short, simple, readable code** — without losing functionality. ~30,700 lines of Rust total (the Kafka protocol is 1,300 of them; other engines' formats, Kafka's client side and files anywhere, round 23, 4,650; Python functions, procedures on workers, the run log and tasks, round 24, 1,400; outside appends, live queries, temporary tables, answers kept and changes across lakes, round 25, 1,200; other engines' changes as written, round 28, 1,300).
   If a change makes a file much longer, look for the simpler shape first.
6. **Scale-out is the point** (the owner, 2026-09-27): running across machines is what sets
   Pondra apart from single-node engines (DuckDB, Polars, Daft, Bodo) and makes it leaner than
   Spark and Flink. No feature may slow a cluster down, add a single node everything depends on,
   or cost memory when unused; the cluster bench (3 and 6 nodes) runs every round and must hold or
   improve.
7. **Within the bucket's limits at any size** (the owner, 2026-09-30: "at PB scale we should not
   hit rate limits of S3"). Nothing may count on a bucket taking more requests than it allows:
   - fan-outs are bounded (round 29 puts them all through one request budget per node);
   - keys that many writers add start with a random part, not a time or a counter;
   - nothing lists a whole table or the bucket on a schedule;
   - no key is written more than once a second.

   Round 29 builds the budget and fixes what breaks these today: log segments named by time,
   the hourly orphan sweep's full listing, and the inbox bell (C5: invariant 182).
8. **Every round leaves it better on every angle** (the owner, 2026-09-30): faster, more
   performant, simpler, easier to use, more functional, versatile, scalable and powerful — while
   staying lightweight and efficient. The gates hold each round to it (`logs/gates/`: speed and
   SQL never drop), and the console's budget keeps the page light.
9. **Easy to change, replace and extend** (the owner, 2026-10-01): the platform will grow tools of
   its own (ETL on a canvas, AI agents, reports, GPUs) and parts will be swapped. Every feature is
   a part with one job behind a small surface — a registry entry (`register.*` in the console, a
   kind of object, a door, a format), not a branch threaded through other code — and works the
   same from the UI, SQL, the clients and the HTTP API. Prefer a shape a new tool can plug into
   over one that has to be edited to admit it.

## Layout

```
src/      28,600 lines of Rust, one file per concern (see the table in README.md); round 25 added
          live.rs (live queries) and temp.rs (a session's temporary tables and views); round 31 vars.rs
          (SQL variables and a file's declared parameters, ADR-037, ADR-044) and script.rs (a script's
          blocks, branches, loops and handlers, ADR-045); round 26
          pg_catalog.rs (Postgres's catalog, for dbt and BI tools), dbserver.rs (`pondra serve
          --lakes`: a folder of lakes as databases), defaults.rs (NOT NULL and DEFAULT), ext.rs
          (files read by name: `read_*`, `CREATE EXTERNAL TABLE`) and console.rs + console/ (the
          console at /, ADR-032, ADR-034: index.html, console.css, and its modules — core.js the
          API, state and node; editor.js; grid.js; notebook.js; files.js the Workspace and the
          file tabs; console.js the shell and `window.pondra`; loaded when first used: more.js
          (Runs, Variables, Settings, search, choosing Python), data.js (data files), chart.js,
          plan.js, details.js and more.css, sqlfile.js (a SQL file), rename.js (renaming a file),
          tabs.js (the tabs' and the panes' menus), live.js (live queries)), xlsx.rs (a download as an Excel workbook); round 32 fresh.rs (a view's plan kept from one write to
          the next); round 33 format.rs (the lake's format, ADR-039), drain.rs (stopping without
          dropping work), service.rs (`pondra service`: systemd, launchd, a Windows service;
          ADR-041), past.rs (a table's past: `AT (…)`, `RESTORE`, ADR-043) and history.rs (every
          statement a row of `pondra.history`, slow ones with plans and traces, ADR-048); round 34
          friendly.rs (DuckDB's spellings, rewritten where SQL comes in: invariant 225)
brand/    the logo (mark.svg), colours (colors.css) and fonts (fonts/: Geist and Geist Mono, SIL
          OFL): the only copies; tools/brand_check.py
site/     the documentation website (Starlight; ADR-030): site/STYLE.md says how pages are written,
          site/PAGES.md what each covers; every example runs (tools/docs_check.py);
          .github/workflows/pages.yml publishes it to GitHub Pages
python/   the Python client (pure Python, HTTP + Arrow; `local()` starts a node): `client.py`, frames
          (`frame.py`, Polars' names), `spark/` (PySpark's names), `worker.py` (a node's warm
          Python worker: functions' batches and procedures' calls, ADR-027), `plpy.py` (PL/Python's
          `plpy`), `magic.py` (`%%sql`), `__main__.py` (`python -m pondra`, and `--add-to-path`);
          without pyarrow, rows come as JSON (ADR-024)
install.sh, install.ps1   the one-line installers each release carries (ADR-024)
deploy/   the container image (docker/Dockerfile; tools/image.py lays out its context), a compose
          cluster (compose/) and the Helm chart (helm/pondra); ADR-041
js/       the JavaScript client and the `pondra` npm package's files
examples/ quickstart.ipynb (pip install to an as-of join, in the owner's notebook style),
          console-extension.js (what a console extension adds: a section, a tab, a view, an action)
tools/    harness.py, cluster.py (tests), open_check.py (Delta + Iceberg readers == Pondra),
          keyed_bench.py (compaction cost), clean_bucket.py (keep a bucket to its newest lakes),
          kafka_bench.py (Kafka clients: throughput, latency), mcp_client.py (the MCP SDK),
          freshness.py (head-to-head freshness), clustering.py (what cluster_by buys),
          newuser_bench.py (first reads, new nodes), demo_lake.py (one of everything + the tree),
          serve_bench.py + loadgen.go (serving), bench/tpch.py (TPC-H vs DuckDB and Spark),
          sizes.py, sim_r2.py (local S3 with R2 latency), udf_server.py (a function of your own,
          in Python, over Arrow Flight), bench/singlenode.py (TPC-H and ClickBench vs DuckDB, Polars, Daft, Bodo; bench/clickbench_ties.py: its answers that differ are ties),
          metadata_bench.py (a table with a million files), flight_bench.py (Arrow Flight),
          shuffle_spill.py (a shuffle bigger than memory, and one that loses a node),
          join_order.py (the same queries written badly: same answers, no slower),
          spread_tpch.py (all 22 TPC-H queries across N nodes == one node, and why not),
          skew_check.py (a hot join key: same answers, the work shared out),
          formats_check.py (Delta and Iceberg tables by Spark 4, delta-rs and PyIceberg == Pondra's reads; attached,
          written, spread), sim_gcs.py and sim_iceberg_rest.py (a local GCS and an Iceberg REST catalog),
          bench/files_tpch.py (TPC-H from files outside the lake against the lake's tables),
          files_s3_check.py (files in a real bucket), slt_check.py (DataFusion's sqllogictest files: D1),
          bench/nexmark.py (five Nexmark queries, Pondra and Flink), frames_check.py (frames == Polars; SQL and
          Python mixed every way), spark_check.py (pondra.spark == PySpark), bench/tpch_frames.py (TPC-H as
          SQL, frames and PySpark code), asof_check.py (ASOF JOIN == DuckDB's, one node and three), stream_check.py (windows,
          sessions and an as-of view over one stream: every click once; rates and delays),
          cloud/ (a cluster on several machines; cloud/actions/ + .github/workflows/: on GitHub runners),
          bench/tpch-queries/ (the 22 TPC-H queries),
          r2_test.sh (run the suite against a real bucket), bench/ (vs Spark and Flink),
          clients_check.py (dbt against Postgres 16, psql, SQLAlchemy, pgjdbc, psqlODBC, ADBC and
          Npgsql on the Postgres port), console_check.py (the console in headless Chromium, in parts:
          node, files, grid, layout, budget, tokens, server, extensions), console_shots.py (the
          website's pictures of it), docs_check.py (every
          example on the website), package.py (wheels, npm packages and the binary alone from a binary), try_packages.sh
          (them installed and tried as CI does on each OS: pip without and with pyarrow, npm, the
          installer; try_install.ps1 is Windows's), npm_publish.sh (the release's npm publish; CI dry-runs it), anywhere_check.py (the shell, local(),
          the packages, the notebook; old Linux in docker), bench/repeat.py (one query many times),
          cloud/gcp.sh, cloud/tpch_parts.sh, cloud/spark.sh and cloud/scale.py (VMs on Google Cloud,
          SF100 in parts, Spark beside, 1/3/6 nodes into scale.json), deploy_check.py (the image,
          compose, the chart on kind, pondra service), ci_artifact.sh (a platform's packages from a
          commit's build run), distribute.py (Homebrew's formula, Scoop's and winget's manifests from
          a release), sign.py (macOS and Windows signing once the certificates are set),
          upgrade_check.py (every release's lake opens, formats, the drain, rolling upgrades),
          soak.py (C4's soak), faulty_s3.py (an S3 proxy that errs, loses replies, slows, holds and
          goes down), resilience_check.py (every mode under failure)
docs/     ADRs and reports; lake-format.md is the on-disk layout
```

## The model in one page

- **Tables** are Parquet files in the bucket plus a **log tail**. Every query reads files ∪ tail,
  so data is queryable the moment it commits.
- **The catalog** is a SlateDB key-value store inside the same bucket: `t/` tables, `s/` segments,
  `d/` inline segment data, `p/` producer progress (also Kafka producers, consumer-group offsets
  and window emission), `v/` views, `w/` session views' bounds, `k/` tasks, `x/` Delta and `i/`
  Iceberg publish state, `a/` lakes attached, `f/` functions, `r/` macros and procedures, `e/`
  secrets (sealed), `o/` catalogs attached from outside and `fd/` feeds (round 23), `m` members
  (replicated acks), `n` next segment, `c` commit number. One process (the leader) writes it;
  everyone reads it.
- **Writes:** a client POSTs a batch to *any* node. That node encodes it (Arrow IPC + ZSTD), runs
  the inline views on it, writes it to the bucket if it's over 64 KB (1 MB with replicated
  acks), and asks the leader to sequence it. The leader dedupes `(producer, seq)`, numbers the
  segments and commits — one catalog write for every node's flush in that round. It never
  touches the data itself.
- **Committed** means acknowledged and visible. By default (`--ack durable`) a write commits
  once it's in the bucket. With `--ack replicated` it commits once `--replicas` nodes hold it:
  the leader in memory, followers in local replica files (`replica.rs`). The bucket gets it a
  moment later either way.
- **Exactly-once:** producers send `(producer, seq)` in order, one request in flight, retrying on
  any node. Retries of committed batches come back `"duplicate": true`. Streaming tasks use the
  same mechanism with a compare-and-swap (`prev`), so output and progress commit together.
- **Followers** get every change and commit streamed over `GET /cluster/log` (frames: `Start`,
  `Change`, `Committed`, `Durable`; a change takes effect at its `Committed`). They seed an in-memory
  copy of the whole catalog from their own view and keep it current from the stream (the
  "mirror"), so they see a commit within milliseconds and never ask the bucket for metadata.
  After a gap in the stream they fall back to their view plus the streamed commits (the ADR-005
  rules) until they can seed again.
- **Native first, open formats on request:** Pondra's readers use the catalog directly. Tables
  with `publish` get a Delta log (`data/{table}/_delta_log/`, `delta.rs`) and/or Iceberg metadata
  (`data/{table}/metadata/`, `iceberg.rs`) every tiering round, for engines that don't know
  Pondra.
- **Writes from anywhere** (`write.rs`): `CREATE TABLE`, `INSERT`, `UPDATE`, `DELETE` in SQL on
  any node, over Postgres (`pg.rs`) or from `pondra sql` on any machine. The work runs where the
  statement runs; the leader records it — over HTTP, through the bucket inbox (`inbox.rs`) when
  it can't be reached, or the statement leads for a moment itself when nobody does.
- **Attached lakes** (`--attach name=dir`): other lakes read as `name.table`; writes to them are
  recorded by their own leaders. Several clusters share one bucket this way.
- **Tokens** (`auth.rs`): read / write / admin; none set = open. The same tokens guard HTTP,
  Postgres, MCP (`mcp.rs`, `POST /mcp`: tools `list_tables`, `query`, `write`, `changes`),
  Kafka (SASL/PLAIN) and the Iceberg REST catalog.
- **The Kafka protocol** (`kafka.rs`, `--kafka`): a topic is a table with one partition; every
  node takes producers (idempotent ones exactly-once) and consumers (offsets are `_ord`); the
  leader coordinates consumer groups in memory. JSON values become rows; Debezium events and
  tombstones become upserts and deletes.
- **The Iceberg REST catalog** (`GET /v1/…`, `iceberg.rs`): engines attach a node by URL.
- **Schema evolution:** `ALTER TABLE … ADD COLUMN`; reads conform older rows (`query::conform`).
  `RENAME COLUMN`, `DROP COLUMN` and widening `ALTER COLUMN … TYPE` (round 21, ADR-022) change the
  catalog, never the files: `TableMeta::columns` are the stored names, `names` and `dropped` say
  what SQL sees (`TableMeta::logical`); reads alias them (`query::named`), the log keeps stored
  names (`log::pack`: `to_stored`), Delta maps columns by name and Iceberg by field id.
- **Window views that emit once** (`views.rs`, `?window=w&size_secs=&lateness_secs=`): closed
  windows go to `{view}_final`, emitted by the leader.
- **SSD tier** (lakes on object storage): each node keeps immutable objects on local disk —
  written through, read through, prefetched from the commit stream, warmed at start (`cache.rs`).
- **Leader election** is a put-if-absent object `cluster/term/{n}`; SlateDB fencing stops an old
  leader from writing. HTTP heartbeats decide liveness and who runs which task shard. The leader
  also rewrites `cluster/alive/{n}` every 10 s, so machines outside the cluster can tell a live
  leader from a dead one.
- **Maintenance** (log → Parquet, merging small files, compaction, retention) is decided by the
  leader and dealt out to all nodes as jobs.
- **Table metadata that stays small** (`manifest.rs`, append tables): every file carries its
  columns' min/max; past 128 files, all but the newest 64 are sealed into immutable manifests
  (`data/{t}/_manifests/`) behind one list object (`TableMeta.sealed`). Queries (`query::Pruned`)
  prune manifests, then files, by their filters. `partition_by` keeps one partition per file.
- **Distributed queries** (`spmd.rs`): any statement — joins of every type, subqueries, CTEs,
  unions. `tables()` finds every table it reads; it is sliced on the biggest append table, and
  small and keyed tables are read whole, at the coordinator's snapshot (`Slice::whole`, `upto`).
  Then the physical plan decides (`spread`: states Whole/Split/Keyed): gather (first exchange is a
  gather: the coordinator merges partial results) or shuffle (each exchange becomes a step: every
  node splits its output into `nodes × partitions` buckets and reads its own from every node, in
  node order). Exchanges are Hash, Own (a whole table keeps its own keys' rows) or All-gather (a
  final aggregate over partial ones). Three ways are tried (`How`: broadcast, both sides
  partitioned, every table sliced); a left/semi/anti join that fails as planned is first rewritten
  to shuffle both sides by its key (`by_key`), and a join's collected side that is spread across
  the nodes is all-gathered (`collected`). Scalar subqueries are hoisted out of the plan and answered
  between steps (`hoist`, `Subquery`). A node's slice (`ShareExec`) reports the whole table's
  size; only plans that the spread analysis proves correct run spread (`PONDRA_DEBUG_SPREAD=1`
  says which operator refused). Work is dealt by bytes, a step that fails is
  retried once and then the shuffle runs again without that node, and below three live nodes the
  query falls back to one.
- **Data that knows where it is** (round 15). INSERTs write each partition's rows to their own
  files and merges read files one after another, so rows keep the order they arrived in and each
  file holds a narrow range of a key that came in order. Every file says which columns hold
  NULLs (`DataFile::nulls`) and carries a HyperLogLog sketch per key-like column until the leader
  folds it into its table's (`sketch.rs`, `TableMeta::sketch`; statistics' distinct counts).
- **Key ranges** (`ranges.rs`, `How::Ranged`, tried first): the query's biggest table is cut into
  ranges of a column its files hold narrowly, by bytes, and every other table with a same-typed
  column the query names is cut the same way where that's cheap (small ones too). Each node reads
  the pieces overlapping its range plus the whole log tail and keeps its rows (`Pruned::range`;
  NULLs in the first range). `Spread::Ranged`: `by_range`/`ranged` trace a column back to the
  slice that cut it; repartitions on it stay local, aggregations grouped by it and joins pairing
  it on both sides (`meets`) run where the rows are.
- **Hot keys** (`skew.rs`): exchange steps report the bytes they sent to each node's partitions;
  before a shuffled join's step the coordinator shares out partitions far above the average
  (`PONDRA_SKEW_MB`): one side stays where it was hashed, the other goes to every node
  (`Shuffle::splits`, `received`). NOT IN all-gathers its subquery side (`Exchange::whole`).
- **What a shuffle moves lives in pieces** (`spill.rs`): a bucket is Arrow IPC pieces of
  `PONDRA_SPILL_MB` (64 MB), held in memory while small and written to the node's scratch folder
  beyond. A piece is what is written, what crosses the wire (length-prefixed) and what one
  partition of the next stage reads, so nothing holds a whole bucket — including the coordinator,
  which reads each node's results onto its own disk and finishes the query over them.
- **Join order from the catalog** (`optimize::JoinOrder`, `query::Pruned::statistics`): rows,
  bytes and each column's range (bounding its distinct values) become DataFusion statistics;
  inner joins are rebuilt greedily, from each input in turn, when that costs less than the order the query wrote,
  which is costed the same way as the tree it is. A filter keeps its share by distinct counts and
  the columns' spans (`kept`: a month of dates, one year of 201), a join keeps only the key values
  both sides have (`joined`), and a subquery's semi join goes onto the smaller side first
  (`SemiJoinDown`). `PONDRA_JOIN_ORDER=0` turns the order off; `PONDRA_DEBUG_JOIN_ORDER=1` prints
  each choice.
- **Streams on their own time** (round 16, `views.rs`): the watermark of a window or session
  view is its source's newest event time less the lateness (`views::newest`: file ranges, then
  each new log segment once, in the leader's memory). Window views emit each window to
  `{v}_final` when it passes the window's end; session views (`?session=ts&gap_secs=…`) cut each
  key's rows at gaps in SQL each round, over the rows from the earliest open session's start (a
  lower bound under `w/{v}`), leave out rows inside a session already emitted, and append the
  closed sessions to `{v}`. Both commit their progress as a producer's seq (`emit:{v}`).
- **`ASOF JOIN`** (`asof.rs`): `rewrite` turns it into a LEFT JOIN whose condition carries the
  marker `pondra_asof(l op r)` wherever SQL comes in; the physical rule `asof::Rule` (right after
  `join_selection`) replaces the hash / sort-merge / nested-loop join carrying it with
  `AsOfJoinExec` (children: kept side, looked-up side; `Mode::Collected` one lookup table,
  `Partitioned` one per partition when both sides are hashed by the key, `Keys` the small kept
  side first and only its keys' rows of the other). `KeepOuter` keeps the join outer so a WHERE
  isn't pushed into the lookup. `spmd::asof`: looked-up side whole, both hashed, or sent whole to
  every node.
- **Arrow Flight / Flight SQL** (`flight.rs`, `--flight`): ADBC/JDBC statements and ingest,
  pyarrow `DoPut` exactly-once, `DoGet` SQL or a table's log as a columnar stream.
- **Installed anywhere** (round 17, ADR-018). The Linux release binary is built for glibc 2.17
  (`cargo zigbuild --profile dist --target x86_64-unknown-linux-gnu.2.17`). `tools/package.py`
  puts a binary in a wheel (as a script, like maturin's bin wheels) and in npm packages
  (esbuild's pattern: `pondra` + optional `pondra-<platform>`). `pondra [lake]` with no command
  is a SQL shell (`shell.rs`) over a node it starts (with the other lakes in its folder attached:
  ADR-024); Python's and JavaScript's `local()` start one too. All three start it with `--stop-with-stdin`: the node stops, and a leader gives up its
  term, when its standard input closes.
- **`sum` over DOUBLE is order-independent** (`fsum.rs`): it replaces DataFusion's `sum` in every
  session; Float64 sums carry a second double with the rounding errors (state: two columns),
  other types go to DataFusion's.
- **Rows that change** (round 19, ADR-020, `change.rs`, `sys.rs`). Every row has system columns:
  `_row_id` (stamped as it enters the log or a bulk INSERT's files, from blocks of ids the leader
  hands out as commit numbers: `Ids`, `Flush::reserve`), `_version` (its commit), `_created_at`,
  `_updated_at`; tiering writes them into the files, `SELECT *` hides them (`sys::hide`).
  `UPDATE`/`DELETE`/`MERGE` on an append table are the leader's, under the lock, from one
  snapshot, in one commit: new versions (their `_row_id` kept) into `t`, old ones into the hidden
  `{t}$deleted` (`_version` → `_old_version`); reads anti-join on (`_row_id`, `_version`)
  (`query::current`). Keyed tables' changes are upserts and delete markers, ids kept. Views take
  the old rows back (`views::derive`): adding-up views subtract them, row-by-row views carry their
  source's ids and drop old versions into `{view}$deleted`. `change::feed` is the change feed
  (`_change_type`, Delta CDF's names). `tier::purge` rewrites files without the old rows (every
  round for published tables) and moves `TableMeta::purges` on.
- **Spread only when it pays** (`guard.rs`): a query goes across the nodes when what its plan
  would move (DataFusion's estimates at each exchange), at the slowest link's measured speed, plus
  a few round trips a step, costs less than what it saves on one node (the time it took here when
  last asked, else its tables' bytes at this node's rate). Nothing known yet: it runs here; run
  both ways, the faster way wins. `?spread=1` forces; `PONDRA_LINK=ms,MB/s` states a network.
- **Few objects** (round 20): `INSERT … VALUES` through the log, the catalog's WAL cleared every
  minute, tiering at most every 10 s by default; `store::Counted` counts writes, lists and deletes.
  A keyed table's tiering round is a generation (a file per partition, sorted by `cluster_by` then
  the key); two or more `cluster_by` columns order rows along a Hilbert curve (`hilbert.rs`).
- **Stream joins and sliding windows** (round 20, `views.rs`): `join = 'streams'` views run on the
  leader after commits (Δa ⋈ b ∪ a ⋈ Δb by `_version`); `slide_secs` windows combine panes.
- **The Postgres port** (`pg.rs`): rows a batch at a time, `COPY` in (text, CSV) and out (text,
  CSV, binary), DECIMAL as NUMERIC.
- **Views that start full** (round 21, `views.rs`): the sequencer holds every flush to the inline
  views (`views::Inline`, checked in `log::commit`: a flush with a table's rows carries a part per
  view of it, and none for a view it doesn't have, or its node packs it again); a new view's
  filling ends at the first commit that holds flushes to it (`Fill::upto`, set by
  `views::bound` in that commit) and the leader fills it from the rows up to there once
  (`fill_all`, producer `fill:{view}`).
- **The latest row by event time** (round 21): a keyed table's `order_by` column decides which of
  a key's rows is current (`query::latest_sql`: the greatest, then the last to come); such tables
  read by grouping every generation, not by shadowing. A keyed table's `_deleted` is left out of
  reads unless a query names it (`query::named`).
- **Frames** (round 22, ADR-023, `python/pondra/`; one set of names with SQL and the connection:
  ADR-025): `pondra.frame` (Polars' names) and
  `pondra.spark` (PySpark's) build one SQL statement, a CTE per step; the engine sees only SQL.
  `con.sql` gives a frame; a name the lake lacks is looked up among the caller's Python names (a
  frame goes into the query's `WITH`, pandas / Polars / Arrow data travel with the request as its
  own tables: `query::SENT`); a write that names a frame sends it as a view of the request
  (`routines::Expander` puts its query in place). A frame's sort is put in every step that keeps it.
- **Functions, procedures, scripts, tasks** (rounds 22 and 24, ADR-023 and ADR-027,
  `routines.rs`, catalog `r/`). `CREATE FUNCTION` / `CREATE PROCEDURE` in Postgres's forms,
  `LANGUAGE sql | python`; DuckDB's `CREATE MACRO` is a SQL function too.
  - **SQL functions** are replaced by their bodies in the syntax tree where SQL comes in
    (`routines::expand`: the doors, a materialized view when made, stored views as read), the
    arguments cast to the parameters' types.
  - **Python functions** are DataFusion async functions (`pyfn.rs`) whose batches go to the
    node's warm workers (`python.rs`: `python -m pondra.worker` under `--python`, framed Arrow
    over its standard input and output).
  - **`CALL`** runs a procedure as its caller (`routines::one`), arguments worked out once. A
    Python one runs on a worker with a connection back, lent the caller's rights
    (`auth::lend`); what it prints goes back as notices (`routines::with_notices`), and its
    secrets are blanked out of what it says.
  - **Every call** is a row of `pondra.runs` (`runs.rs`: the hidden keyed table `pondra$runs`).
    `SELECT pondra.start(…)` runs one without waiting. Tasks (`CREATE TASK … SCHEDULE`, catalog
    `j/`, ticks `jt/`) run on the leader, each tick once.
  - **`POST /sql`** takes several statements (`routines::statements`) and `$name` parameters
    (`routines::bind`); MCP lists every procedure as a tool.
  - **Other engines' writes** (ADR-029, `adopt.rs`, `iceberg.rs`): an append through the REST
    catalog is recorded as written (footers checked, lineage given), or copied when the table has
    renamed columns; their changes too — files taken out (copy-on-write), position deletes
    (merge-on-read), several tables at once — as one commit through the log that what follows the
    table follows; a keyed table's as upserts and delete markers through the log. The catalog makes,
    renames and drops tables, and publishes each table's layout for writers.
  - **The workspace** (ADR-033, `workspace.rs`): `CALL run('etl/orders.sql', day =>
    …)` runs a file of the lake's as its caller: a `.sql` file through `routines::script`, a `.py`
    file and a notebook's Python cells in a worker of the run's own (`python::ask_session`, the
    values as variables), `%%sql` cells as scripts. `routines::one` and `start` hand `run` here;
    the run is logged as `files/<path>@<version>`.
- **Files and other engines' tables, anywhere** (round 23, ADR-026). A file, folder or glob
  (`'s3://b/*.parquet'`, `read_csv(…)`), another engine's table (`delta_scan`, `iceberg_scan`), a
  topic (`'kafka://brokers/topic'`) or a name under a catalog attached with `ATTACH … (TYPE delta
  | iceberg | kafka)` is rewritten where SQL comes in to a quoted `"ext:<base64url JSON Spec>"`
  (`routines::expand`, `ext::table`), which `ext::meta` resolves to a TableMeta of its files:
  listed once a statement (`ext::scope`/`listing`), sent with a spread query's slices
  (`Slice::ext`). Plain files are read by DataFusion's listing table (`ext::read`), Delta and
  Iceberg files by `scan.rs` (partition values, deletion vectors and position deletes as a row
  selection, equality deletes as an anti join by sequence number, columns by field id), a topic
  by `kafka_client.rs`. A bucket's store is registered with its secret (`ext::register`;
  `CachedStore::files` keeps byte ranges by the version listed). `CREATE SECRET` (`ext::create`,
  catalog `e/`) seals credentials with `PONDRA_SECRET_KEY`. `COPY … TO` (`copy.rs`) writes files
  anywhere or a topic; a folder from a big table is written by every node (`spmd::copy`).
  `INSERT` into an attached Delta or Iceberg table commits through the format
  (`write_outside.rs`; Iceberg's Avro is `avro.rs`). A materialized view over a topic is a feed
  (`feeds.rs`, catalog `fd/`): a shard per partition, rows and offset committed together. Lakes
  may be on GCS and Azure too (`store::open_store`).
- **Memory:** one spill pool per node (`--memory-gb`); a query out of memory runs again with
  sort-merge joins. **`GET /metrics`** (Prometheus) for everything else.

## Invariants — break these and data goes missing

1. **A read never goes back in time.** A follower combines its catalog view with streamed commits
   only when it can prove the result is a prefix of the lake: it reads its view's commit number
   before *and* after the data, decides under the overlay lock, and pins what it needs so pruning
   can't drop it mid-read (`Catalog::scan`, `refresh`, `apply` in `src/store.rs`).
2. **"How far can I read" comes from the same view as the read** (`Lake::visible`), never from the
   `hwm` watch (which only wakes readers and may be ahead on a follower). A task that reads
   `(done, hwm]` while its view can't see all of it would commit progress past rows it never read.
   On the leader `visible()` is the committed high-water mark, *not* `last_n`, which counts
   in-flight commits.
3. **Only committed data is visible.** The leader reads its in-memory catalog, which only ever
   holds committed writes (applied in `Lake::commits`), never ones in flight. Followers apply a
   change only at the `Committed` frame that covers it. Committed = durable by default; with
   `--ack replicated`, held by `replicas − 1` member followers or durable (invariant 15).
4. **A job names its inputs.** Tiering jobs carry the file list and segment range from the leader
   and refuse to run until the node can see the last segment; otherwise a lagging node would write
   an incomplete file that the leader then commits.
5. **A keyed table's files are versions, not a set.** Each file carries `ord`, the last log segment
   it covers, and a row in a higher-`ord` file is a newer version of its key (`_ord` in a read is
   `ord << 32` for file rows, `(segment << 32) + position` for log rows). Two rules follow: a file
   written by a fold keeps delete markers (they shadow older files; only a full compaction drops
   them), and two files of the same `ord` must never cover the same segments.
6. **Segments and files are immutable.** Nothing is ever overwritten (`PutMode::Create`);
   replaced files become `garbage` and are deleted after the retention period.
7. **Expire only what everyone has consumed**, using the floor as of `retain_secs` ago, so a query
   that started earlier still finds its segments.
8. **While the mirror is on, commits reach it only through the stream.** Its reads never consult
   the view, so the view must not mark commits as "already seen" (`pruned`) — that skipped them
   and lost rows (round 6). Gaps are judged against what the mirror holds (`streamed`), not the
   view. Seeding is one attempt per refresh, off the startup path — never a retry loop (a busy
   lake on slow storage kept one from ever finishing, so a restarted node never came up).
9. **A tiering job checks the leader's row count** for its log range before it writes anything
   (`caught_up` in `tier.rs`). A node whose catalog disagrees refuses the job; the next round
   retries. This turns any future "a node saw less than the leader" bug into a retry, not a loss.
10. **The Delta log is derived, never authoritative.** A Delta commit is computed from committed
   catalog state only and written put-if-absent; an unrecorded one found later is adopted (an
   Iceberg version already there is skipped: its next number is taken). Publishing runs one round
   at a time, each committing what it published before the next reads it (`delta::publish_all`):
   a `CHECKPOINT` and a tiering round publishing at once wrote the same Iceberg version on R2.
   Only `_last_checkpoint` is ever overwritten, and nothing in `_delta_log/` goes through the SSD
   tier.
11. **Catalog memtable flushes are rationed:** one loop, every 5 s, only if something committed,
   plus one when a leader takes over (followers' views read no WAL and need it). Each flush is a
   level-0 file; flushing on every tiering call stalled writes for 9 s at a time (round 6).
12. **A tiering round is: fold + commit, publish, then maintain.** Merges and compactions come
   after the fresh rows are committed and published, in their own commit. A table with no new
   rows is skipped (no empty commit); `expire` moves its `tiered` mark along every 10 s — don't
   write code that assumes `tiered` advances every round.
13. **Every keyed file holds one row per key** (folds and compactions both write that way). Upsert
   reads rely on it: they anti-join each file against the keys of newer sources instead of
   grouping every row by key (`register_upsert` in `query.rs`), and `/lookup` stops at the first
   file that has the key. A writer that breaks it (say, appending raw rows to a keyed file)
   makes reads return duplicates.
14. **A cached result is valid for exactly one catalog version** (`Catalog::version`), which only
   exists where every read reflects exactly that version: the leader (last committed write) and
   nodes with the in-memory catalog (last streamed commit). Identical queries in flight share one
   computation that covers only requests which arrived before it started. `stale_ms` is the one,
   opt-in, exception.
15. **Replicated commits** (`--ack replicated`, `replica.rs`). Break one of these and acked writes
   vanish in a failover:
   - A follower acks only a run of *consecutive* changes it holds, and never for a term older
     than the newest it has heard of (a new leader's `/cluster/replica` fetch raises that mark).
   - Only members listed in the catalog (`m`), durably, count. A follower is listed before it
     counts; it stops counting, and everything is made durable, before it leaves the list.
   - A new leader recovers before it takes writes (`replica::recover`, before `Sequencer::start`
     and before its HTTP server): it asks every member (20 s), re-commits the longest chain
     (terms never going down), and waits until it's durable.
   - At most `AHEAD` (256) commits are acknowledged ahead of the bucket; past that, acks wait for
     it (a hung PUT on real R2 once let 279 pile up).
   - `failover --flag ack=replicated` on simulated R2 is the test that exercises recovery: its
     leaders die with commits the bucket doesn't have yet.
16. **Only durable state leaves the catalog.** Delta/Iceberg publishing and deleting objects
   (retention) first wait for everything committed so far to be durable
   (`Catalog::wait_durable`). With replicated acks, a commit that recovery can't find must never
   have reached another engine or deleted a file.
17. **Nobody deposes a live leader from outside the cluster.** A node joining, or a `pondra sql`
   INSERT, claims a new term only if the latest term's `cluster/alive` mark is over 30 s old. A
   follower that has never reached its leader (`Cluster::heard`) waits for that too; only
   members that lost a leader they were talking to use the 5 s lease.
   A one-off writer claims with an empty address, keeps its mark fresh while it works, and
   deletes it when done. Nodes and other writers wait for it; they never follow it.
18. **The inbox is just another door to the leader.** Its requests carry the same exactly-once
   keys as HTTP (a bulk INSERT's job id, a flush's `(producer, seq)`), and only a process that
   leads answers them (`inbox::drain`, from the leader loop or a one-off writer that leads). A
   writer that gives up waiting withdraws its request first; a late answer is then a duplicate.
19. **Keyed compaction merges consecutive runs only** (`tier::run`). Files are versions in `ord`
   order; merging around a file would let an older version overtake a newer one. A partial
   merge (`Squash`) keeps delete markers and expired rows; only a full compaction (the run
   reaches the oldest file) drops them — or a keyed table's first file, which has nothing older
   to shadow. Tables that publish Delta/Iceberg always compact fully.
20. **A write to an attached lake is recorded by that lake's leader** (`write::deliver`), never
   by ours: our catalog never lists another lake's files.
21. **SQL from users never touches a node's disk.** Every query that arrives over HTTP, Postgres
   or MCP runs with `query::read_only()` (no `COPY … TO`, no `CREATE EXTERNAL TABLE`, no session
   DDL); writes go through `write.rs`. Local files (`enable_url_table`) are for `pondra sql` on
   its user's own machine only (`prepare(…, files: true)`). `harness.py clients` checks both.
22. **A Kafka batch's seq comes from its producer id and sequence** (`kafka::queue`): seq = base
   sequence + record count, `prev` = base sequence, producer `kafka:{id}:{topic}`, or
   `kafka:{id}/{epoch}:{topic}` once its epoch isn't 0 (an epoch is a producer of its own:
   librdkafka numbers from 0 again after bumping it). Producers
   without a name (non-idempotent Kafka producers) are never checked (`log::commit`); nothing
   else may use an empty name.
23. **Stored columns only grow, at the end** (`write::create_table`), and every read of log rows
   goes through `query::conform` (by name; missing → null). Never read segment rows with the table
   schema without conforming them: rows written before an ALTER have fewer columns. A rename or
   drop changes `names`/`dropped`, never `columns` (invariant 74).
24. **A window is emitted once** (`views::emit`): the rows and the `emit:{view}` producer's seq
   (the watermark, µs) commit together, with `prev` = the last watermark.
25. **An append table's entry lists at most 128 files** (`manifest::seal`, called from
   `tier::maintain` and `write::record`); the rest are in manifests, which never change. Anything
   that needs every file goes through `manifest::all` / `manifest::pruned`, never `meta.files`
   alone (publishing, orphan collection, distributed queries).
26. **A partitioned table's files each hold one partition value** (`tier::split` on every write,
   merges grouped by `part`). Never merge files of different partitions.
27. **Every node plans a distributed query alike.** A slice is scanned through `ShareExec`, which
   reports the whole table's size, and has 2+ partitions; a table read whole through `WholeExec`,
   which reports its size from the catalog (not from what the node holds decoded or in its log);
   every node plans with the coordinator's partition count (`Slice::partitions`). The coordinator
   compares each node's plan shape at every step and falls back to one node on any difference.
28. **A shuffle never carries a whole copy** (`spmd::spread`): a hash exchange over rows every
   node has in full stays inside the node, and what reaches the coordinator is split. New
   operators are refused until the analysis knows them.
29. **Flushes reach the sequencer in the order they were cut** (`log::send`'s turn), so a
   producer's pipelined batches commit in order. Over HTTP from a follower they may still
   overtake each other; a door that pipelines (Flight) re-queues a batch refused as out of order.
30. **A shuffle's scratch folder has the node's own id in it** (`spill::dir`). Two nodes on one
   machine share a cache directory and name their buckets the same way; without the id they write
   over each other's rows and the answer is quietly wrong. This is what `tools/shuffle_spill.py`
   caught when the id wasn't there.
31. **A step's buckets are kept, not taken** (`spmd::fetch`, `bucket`): reading one clones it, so
   a step that has to be run again reads the same rows. Everything a job spilled goes when the job
   ends (`Drop for Job`, `gc`, `?drop=true`), and a dead node's is swept an hour later. A gather's
   spill is freed by the guard its response stream holds instead, since nothing retries it.
32. **Every node splits the same rows the same way.** `spmd::scatter` runs a stage's partitions at
   once, each into buckets of its own, and joins them in partition order at the end; the hash is
   DataFusion's `BatchPartitioner` over the exchange's own expressions. Anything that made the
   split depend on arrival order would send a key to two nodes.
33. **Every exchange adds up in the same order** (`spmd::scatter`, `spill::chain`, `Received`):
   rows are hashed once into `nodes × partitions` buckets (bucket `i` → node `i / parts`,
   partition `i % parts`, which is DataFusion's own `hash % parts`), and a partition reads every
   node's bucket for it in node order; the coordinator reads the nodes' results the same way.
   Reading them as they arrive made float sums differ run to run (TPC-H q15's `= max(...)`).
34. **Tables not sliced are read at the coordinator's snapshot** (`Slice::whole`, `upto`,
   `query::table_view(.., Some(upto))`): every node reads the very same rows of a small or keyed
   table, waiting (10 s) for its log to reach `upto`. Reading each node's own catalog let two
   nodes see a commit apart. Plan shapes don't count `CoalescePartitionsExec` (`shape`): whether a
   table's partitions are gathered depends on what `hot.rs` holds decoded, not on the query; nor an
   aggregate's `ordering_mode`, which follows from the orders a node's reading of a table gives it
   (TPC-DS q66 and q75 fell back to one node).
35. **A scalar subquery is answered before anything that uses it runs** (`spmd::hoist`): a shuffle
   takes the `ScalarSubqueryExec`s out of the plan, and `step()` fills their shared answer slots
   as soon as the exchanges they read are done — on every node, from the same all-gathered rows.
   A shuffle's pieces are compacted (`query::compact`, invariant 202) before they are counted or written: a
   `Utf8View` slice otherwise carries every string of the batch it was cut from.
36. **A key range holds its NULLs once.** The first range holds every NULL of the key: a piece
   that may hold one (`DataFile::nulls` lists the column, or doesn't say) is read by the first
   node too, and every node filters to its range (`ranges::overlaps`, `Range::expr`). A tiered
   file holding NULL keys went only to the node owning its range, and the NULLs were lost.
37. **`Ranged` means co-located by value, and only a column traced unchanged from its slice is a
   range key** (`spmd::ranged`): through projections, filters, aggregations' groups and joins — but
   never the side an outer join pads with NULLs, whose NULLs sit wherever the unmatched rows are
   (`harness.py scale`'s "grouped by a padded key" returns a different answer without it).
38. **A slice reports no orderings or constant columns** (`ShareExec::props`,
   `maintains_input_order` false). A node whose files all held one value of a column planned the
   query differently from the others.
39. **A hot partition is shared out only where the join allows it** (`skew::joins`): an inner join
   either side, a left/semi/anti join its left, a right join its right, never a full or NOT IN
   one; and nothing above it in its step may need a key's rows on one node (an aggregation by
   the key, another shuffled join). The split side stays where it was hashed; the other side goes
   to every node, into the partition of the same number (a join only pairs equal keys).
40. **A file's sketch travels only until its commit** (`sketch::add` in `tier_table` and
   `write::record`; `replace` drops merged files' ones). A table's entry lists 128 files; each
   with a sketch of every column would be too big.
41. **A watermark only grows, and comes from the source's own rows** (`views::newest`): the
   newest event time in its files' ranges and in each log segment after them. Read the view (or
   the source's rows) only after working it out: views commit with their rows, so what is read
   then includes every row the watermark counted.
42. **A row inside a session already emitted is late** (`views::sessions`: the `_last` join, per
   key, against the view's own rows). Without it a late row re-emits its session, longer
   (`harness.py sessions` fails). The `w/{v}` bound is a lower bound: written after the append,
   stale is only slower.
43. **An as-of join is never run as a filter.** The marker errs if executed; a plan shape
   `asof::Rule` doesn't know must fail loudly, not return every earlier row. And no WHERE may be
   pushed into its looked-up side (`KeepOuter`; `asof_check.py`'s "filtered after the join"
   differs from DuckDB without it).
44. **An as-of join sees all of a key's rows on its looked-up side** (`spmd::asof`): that side
   whole, both sides hashed by the key (a node's partition *i* holds the keys of a whole copy's
   partition *i*: invariant 33), or all-gathered.
45. **A view's or task's rows go into its table by position, cast** (`query::cast_as`): strings
   a query reads from files are views, the table holds plain ones. `with_schema` refused them, and
   a view that took a string from a table it joins failed every flush (`harness.py asof`'s
   `venue`).
46. **A node started by another program lives as long as its standard input** (`--stop-with-stdin`).
   Whatever spawns one (the shell, `local()` in Python and JavaScript) keeps the pipe open for
   the node's life and stops it by closing the pipe, never by killing it first: closing lets a
   leader release its term, so the next process on the lake leads at once. A parent killed
   outright closes the pipe too. `anywhere_check.py`'s "second shell starts at once" and "the
   lake reopens at once… for writes" fail when a node is killed instead.
47. **Float sums keep their error term everywhere** (`fsum.rs`). A sum of DOUBLEs is a pair (sum,
   error) in every state: partial aggregates, what crosses the nodes, windows. An operator that
   added partial sums as plain doubles would bring back order-dependent answers (TPC-H q15's
   `= max(...)`); `harness.py sums` compares with `math.fsum` on every node.
48. **The Linux release binary needs nothing newer than glibc 2.17.** A dependency that links a
   newer glibc symbol stops the wheel from installing on older systems.
   `anywhere_check.py --docker` runs the binary on CentOS 7 and Ubuntu 22.04; check `objdump -T
   <pondra> | grep -o 'GLIBC_[0-9.]*' | sort -V | tail -1` after adding one.
49. **A table's name inside the lake is `schema.table`, and just `table` in `public`** (`ddl.rs`).
   Everything keyed by a name (the catalog, the log, `data/…`, Delta, Iceberg, Kafka topics, the
   HTTP API) takes that form; SQL names are resolved to it once (`ddl::resolve`, `ddl::local`),
   unquoted parts lower-cased. A second form of the same table's name anywhere would make two
   tables of one.
50. **A new table starts at the log's end** (`create_table`: `tiered = lake.visible()`). A table's
   log rows are those after `tiered`; starting at 0, a table re-created after `DROP TABLE` read
   the dropped one's rows still in the log (`harness.py schemas`: "a new table of its name is
   empty" fails without it).
51. **A node plans stored views after its shares are in place** (`spmd::plan`:
   `query::register_views` again). A `ViewTable` keeps the table it was planned over; planned
   over whole tables, a spread query over a view counted every row once per node
   (`harness.py schemas`: "queries over views spread" fails without it).
52. **A follower that finds no catalog starts over** (`main.rs`: `cluster::restart`). A new lake's
   leader may not have made it yet, or may have died first; the restart waits for the one or
   takes over from the other when its mark is stale. Exiting instead lost a node of the R2
   cluster bench (`cluster.py race`: "a leader that never made the catalog").
53. **A node runs at most one partition per 24 MB of query memory** (`store::partitions`). Each
   partition's sort keeps 10 MB aside to merge its spills; on 4 cores with 50 MB the reserves
   took the budget and the merge above them failed, and smaller reserves can't merge at all
   (`harness.py scale`'s memory check runs as `PONDRA_CORES=4` and fails without it; GitHub's
   4-core runner found it).
54. **A remembered answer is keyed by every lake it reads** (`Lake::version_for`): this lake's
   catalog version and those of the attached lakes the query, or a view it reads, names. Keyed
   by this lake's alone, a query over an attached lake kept its answer after a write there or a
   `DETACH` (`harness.py schemas`' `ATTACH` check failed on one node without it).

55. **A changed row keeps its `_row_id`, and its old version leaves in the same commit**
   (`change::commit`): the new versions go into the table, the old ones into `{t}$deleted`, in
   one flush, and every read leaves out the (`_row_id`, `_version`) pairs that table holds. Ids
   are never reused: a block is a commit number the leader reserved (`Flush::reserve`), so no
   segment ever has it. `harness.py changes`: "an UPDATE or MERGE keeps a row's _row_id; ids are
   unique".
56. **A change is the leader's, under the lake's lock, from one snapshot** (`change::run`: every
   query of it `session_at(upto)`). Two nodes changing the same rows at once would each start
   from the other's old version. `harness.py changes`: "three nodes changing the same rows at
   once: every change counts".
57. **What follows a table follows its changes, or the change is refused** (`views::can_follow`,
   `views::derive`). A view that adds up subtracts the old rows (and needs a count, so a group a
   change empties goes); a row-by-row view over the table alone carries each source row's
   `_row_id` and `_created_at` (`View::ids`, `carry`) and drops its rows of old versions into
   `{view}$deleted`. Windows and sessions emitted once, min/max, joins and tasks can't take a row
   back: refused. `harness.py changes`: the view checks and "refused: …".
58. **A purge covers only changes whose old rows are in files, and a read skips only what the
   entry it read says is purged** (`tier::purge`: up to both tables' `tiered`; `TableMeta::purges`;
   a slice gets the coordinator's mark, `Part::purged`). A `{t}$deleted` file goes only once every
   reader has passed the purge that covered it (`retain_ms`). `harness.py changes` purges every
   round (`PONDRA_PURGE_ROWS=1`) while it changes rows: "== the model, every node, while tiering
   and purging".
59. **A changed table's `{t}$deleted` is read as of the query's snapshot** (`Pruned::at`), also in
   a slice of a spread query that reads none of the table's log (`upto` 0): its files hold rows
   changed since. Read as of the slice's `upto`, a spread query counted deleted rows and both
   versions of updated ones (`harness.py changes`: "spread over three nodes == one node"). Every
   slice waits for its node to see the coordinator's snapshot first.
60. **A query spreads only when it pays, unless asked to** (`guard.rs`). A node that knows nothing
   about a query yet runs it itself, and learns from it. `harness.py guard`: "over a slow
   network, queries that would shuffle stay on one node", "over a fast one, they spread", "?spread=1 spreads anyway".
61. **Only the program that started a node reads files through it** (`server::owner`: the key in
   `PONDRA_OWNER_KEY`, which the shell and `local()` make and send). Invariant 21 holds for
   everyone else. `smoke.py`: "the shell reads a file on this machine…" and "a node doesn't read
   this machine's files for whoever asks".
62. **A node never leads another lake in its own process** (`inbox::lead_once`): a write to an
   attached lake nobody leads, `CREATE DATABASE` and `ATTACH` of a new folder go through a `pondra
   sql` of their own, which leads for a moment and ends. That lake's catalog writer would
   otherwise stay open in the node, and its next leader would fence it. (No test catches the
   stray writer yet; `smoke.py`'s `CREATE DATABASE` and `harness.py changes` run the path.)
63. **DataFusion settings that have their own switches are changed through `set`**
   (`enable_dynamic_filter_pushdown`): assigning the umbrella field left the joins' own switch on,
   and a changed table's anti-join, planned with its dynamic filter built, failed when the query
   around it swapped its sides (`harness.py changes`' spread query fell back to one node).
64. **`INSERT … VALUES` goes through the log** (`write::through_log`): a Parquet file per INSERT
   piled up (200 one-row INSERTs, 215 files), and a bucket bills and rate-limits every object.
   Bulk `INSERT … SELECT` still writes Parquet itself (`harness.py files`).
65. **A file's min and max come from its Parquet footer** (`manifest::stats`), except floats
   (Parquet leaves NaN out, and a NaN must not be pruned away): a second pass over every column
   cost more than the system columns' encoding. Only exact statistics count: a row group whose
   min or max is missing or cut short (strings over 64 bytes) leaves the column without a range.
66. **A keyed table's files come in generations** (one `ord` per tiering round: a file per
   partition). Compaction takes whole generations, newest back (`tier::run`), and counts them,
   not files; a keyed table is published only as one generation (`delta::publishable`); reads let
   a newer generation's row for a key shadow an older one's in any partition, so Pondra doesn't
   prune a keyed table by partition (`harness.py layouts`).
67. **A table a stream join follows takes every write through the log** (`View::follows`): its
   rows' `_version` must be the commit that made them visible. A bulk INSERT's files carry the
   commit number reserved before they were written, older than the commit that records them, so
   a join run in between would miss them.
68. **A stream join's pairs and its progress commit together** (`views::join`, producer
   `join:{view}`): over commits (done, now], Δa ⋈ b(≤ now) ∪ a(≤ done) ⋈ Δb, each pair once; a run
   with nothing new on either side commits nothing (`harness.py streams`, a leader restart in it).
69. **A sliding window is its panes, combined** (`slide_secs` divides `size_secs`): the view keeps
   `slide_secs` panes and each emitted window adds counts and sums, takes the min of mins and max
   of maxes — a row is added once, not once per window.
70. **`pondra sql` takes a block of row ids from a reachable leader before a bulk INSERT writes**
   (`write::reserve`): unstamped files are rewritten by the leader to add them, which made loading
   TPC-H 4× slower on round 19 (`harness.py files`: the leader writes nothing but its commits).
71. **COPY is the Postgres port's, not DataFusion's** (`pg::Copy`): described with no columns, never
   planned; `COPY … FROM STDIN` gathers per connection and loads through the log 32 MB at a time,
   split at a line's end (for CSV, outside quotes).
72. **A query that ran both ways goes the way that was faster** (`guard::ran_spread`), unless the
   network is pretended (`PONDRA_LINK`: the model alone, as the tests need). One spread run slower
   than here doesn't decide alone (`guard::spread_runs`: the other nodes saw the query cold), so
   the model decides until a second agrees; one slow first run kept the cluster bench's q1 on one
   node for good (`harness.py guard`: "one spread run slower than here doesn't decide alone").
73. **`pondra_object_requests_total` counts what the store was asked to do** (`store::Counted`), not
   what SlateDB tried: a local disk refuses SlateDB's tagged PUT before writing, and it tries again
   untagged; the refusal isn't counted.
74. **A column keeps the name it was first written under** (`TableMeta::columns`); SQL's names are
   `names` and `dropped` over it (`TableMeta::logical`). Everything users write goes in by SQL's
   names and into the log by stored names (`log::pack` → `to_stored`), and a name SQL no longer
   knows is left out — never taken for the column now stored under it (`harness.py columns`: the
   writer from before the rename). Reads alias stored to SQL names in one place
   (`query::named`, also on every node of a spread query); key ranges and file statistics stay
   in stored names (`ranges::named`).
75. **A table's type only widens** (`ddl::widens`): every file already written must read as the
   new type. Delta readers learn renamed columns from column mapping declared as the
   `columnMapping` table feature (reader 3), never reader version 2 alone: delta-rs's pyarrow
   reader ignored version 2 and read renamed columns as null (`harness.py columns`).
76. **The sequencer holds every flush to the inline views** (`log::commit`, `views::Inline`): a
   flush with rows of a table views follow carries a part for each (empty if none derived), and no
   part for a view the table doesn't have; otherwise it goes back to be packed again. A view's
   filling ends at the first commit that holds flushes to it, written in that commit
   (`views::bound`); the fill covers `_version ≤ upto`, as of `upto`. `harness.py fills`: with the
   check off, all 12 checks fail. Making a view waits for its fill, which commits: a crash there
   drops the call, and asking again (the same view) is the answer — `harness.py crash`'s setup
   does, or CI fails now and then.
77. **The inline views the sequencer holds flushes to are reloaded after every view made or
   dropped** (`views::forget`), and a cache that changed meanwhile is never overwritten with an
   older one (`views::bound` checks it's the same). A stale set refuses every flush of the new
   view's rows forever.
78. **A producer whose progress leaves the catalog leaves the sequencer's memory too**
   (`log::forget_producers`, from `drop_view`): a view dropped and made again under its name had
   its fill taken for a duplicate of the old one's, and never filled (`harness.py fills`: made
   again).
79. **A keyed table with `order_by` is never read by shadowing** (`query::table_view`): a newer
   generation may hold an older event. Reads, folds and compactions take each key's greatest
   `order_by` then the last `_ord` (`latest_sql`); lookups and point queries go through SQL
   (`harness.py dedup`).
80. **A keyed table's `_deleted` is shown only to a query that names it, decided alike on every
   node** (`query::names_deleted` over the query and the stored views it reads, in `session_at` and
   `spmd::plan`): nodes that disagreed would plan the query differently.
81. **A frame puts its sort in every step that keeps order** (`Frame._order`, `_step`, and
   `trailing_order` for SQL given to `con.sql`): DataFusion drops the `ORDER BY` of a CTE or a
   subquery, so `sort().limit(5)` as two CTEs returned rows in scan order (`frames_check.py`'s
   "sort, then limit: the top rows" fails without it). A step that drops a sort column, groups or
   joins ends the sort, as in Polars.
82. **A macro is expanded where SQL comes in, before anything plans it** (`routines::expand`: every
   door, a materialized view's SQL when made, stored views as read — `query::stored_views`), and
   a spread query's coordinator sends the expanded statement. After that it is plain SQL. A
   follower waits to see a macro it made before answering (`ddl::settle`). `harness.py procedures`:
   "a macro made on a follower is used there at once", "spread over three nodes == one node".
83. **A procedure's arguments are worked out once, as its caller** (`routines::call`: one `SELECT
   CAST((arg) AS type)`, bound as `arrow_cast` values): `harness.py procedures`' "random() is one
   value" fails when the argument's text is put in each statement instead.
84. **What a procedure runs, runs with its caller's rights and no more**: each statement is
   checked as the caller's (`routines::one`), and a Python procedure's connection back uses a token
   lent the caller's role and file access (`auth::lend`) that ends with the call. `harness.py
   procedures`: "…it may not write for a reader" fails with a lease of more rights; "its lent
   token dies with it". Procedures and macros call each other 16 deep at most.
85. **Code runs on a node only if its owner allowed it and an admin stored it** (`--python`; a node
   without tokens takes it only on 127.0.0.1). Invariant 21 stands for SQL: a Python procedure is
   the admin's code, run with the caller's rights. `harness.py procedures`: "--python without
   tokens only on 127.0.0.1".
86. **Statements split outside strings, `$tag$` bodies and comments, in one place**
   (`routines::statements`: `POST /sql`, the shell, the Postgres port, procedure bodies): the
   Postgres port split at every `;`. `harness.py procedures`' setup fails without the `$$` rule.
87. **Rows sent with a request are its own, on that node only** (`query::SENT`, a task-local: its
   sessions register them; `App::query_as` never spreads a query that has them; no result cache).
88. **An answer with no rows keeps its columns** (`App::query_as`: one empty batch with the
   plan's schema): a frame learns its columns from `LIMIT 0` (`frames_check.py` fails without it).
89. **`pip install pondra` alone answers queries** (ADR-024): without pyarrow, rows come as the
   node's JSON with nulls put back as `None`; only tables (`collect()`, pandas, Polars) need it,
   and say so. `try_packages.sh` runs `package_check.py` before installing pyarrow (0.22.0's
   client fails it).
90. **After any install, pondra runs with nothing set up** (ADR-024): `python -m pondra` finds the
   binary wherever pip put it; the installers leave their folder on the user's PATH (and on
   Windows on this terminal's). `try_packages.sh` checks `command -v pondra` in a clean shell,
   and `Get-Command pondra` plus the user's `Path` on CI's Windows (both fail without the PATH step).
91. **The shell's lakes found beside it are the session's, never the catalog's** (ADR-024,
   `ddl::attach_found`, `serve --attach-found`): attached in memory like `--attach`, skipping a
   name the catalog attaches, and a lake that won't open never stops the node. `smoke.py`: "the
   shell attaches the lakes beside it", "…for that session only: nothing saved in the shell's lake".
92. **`FROM t` is `SELECT * FROM t` wherever SQL comes in** (`routines::expand`, `select_star`,
   `FROM_FIRST`): DataFusion plans a FROM-first select as no columns. `harness.py schemas` and
   `smoke.py` fail without it.
93. **Only a keyed table's first file drops delete markers** (`tier::run_job`: `first`): the fold
   job whose range starts where the table's files end, when it has none. A round deals a job per
   node, and the others' older rows are in that job's file; they keep their markers, expired rows
   and a view's emptied groups, as every later fold does (invariants 5, 19). `harness.py deal`: a
   first round on three nodes, deleted keys stay deleted and a view keeps an UPDATE of a total;
   both fail without it.
94. **A client's names mean what SQL's do, from the connection and from a frame** (ADR-025):
   `view` is a stored query (`CREATE VIEW`) unless `materialized=True` (`CREATE MATERIALIZED
   VIEW`), in Python's `db.view` and `to_view` and JavaScript's `view`; a connection's verb takes
   what the frame's is called on (`db.view(name, frame)` is `frame.to_view(name)`, `db.write_table`
   is `frame.write_table`); a materialized view's options without `materialized` are refused.
   `frames_check.py` section 5 fails with 0.22's client.

95. **Files outside the lakes are listed once a statement, and never kept longer** (`ext::LISTED`
   through `ext::scope`/`listing` at every door; a spread query's nodes get the coordinator's list:
   `Slice::ext`, `ext::prime`; DataFusion's own list-files cache is off, `Lake::open`). A file added
   to a folder or changed under its name is read as it is by the next statement. `harness.py
   outside`: "a file changed under its name, or one more in a folder" fails with the list kept
   across statements, and with DataFusion's cache on (it never expires).
96. **A file outside the lakes is cached only as the version its statement listed** (`cache::Outside`:
   the e-tag, time and size a listing or HEAD gave; fetched with `if_match`; a reply of another
   version is read, not kept). Only a lake's own `data/` goes through its caches and SSD tier as it
   is. `harness.py outside` (a Parquet file rewritten at the same size) fails with the version out
   of the key; `clouds`' "a file beside the lake, changed under its name" fails with a lake's
   bucket cached whole.
97. **Who reads a file is checked, and an `ext:` name carries no trust** (`ext::check`,
   `check_files`): a URL needs a secret whose scope covers it, or the node's owner (the shell,
   `local()`, a node running a share its coordinator checked); a path on the node's machine only
   its owner; every file another engine's log names is checked, not just its folder. Anyone can
   write an `ext:` name by hand, so a Spec never says which secret to use or who may read.
   `harness.py outside`: the refusals.
98. **A bucket is read with one secret** (`ext::create`): a second scope in a bucket, or the same
   scope twice, is refused, since a bucket's store is registered once (`ext::register`).
   `harness.py outside`: "one secret a bucket".
99. **Nothing writes into a lake but its own catalog's writers** (`copy::copy_to` refuses a target
   in this lake or an attached one; `CachedStore::outside` and `delete_stream` refuse writes and
   deletes under a lake's prefix, whoever asks). `harness.py outside`: "…never into a lake, not
   even by the node's owner"; `clouds`: "COPY … TO into the lake: refused".
100. **A `COPY … TO` a folder spreads only when every row is written by one node from its own
   share** (`spmd::copy`: the plan is `Split` with no exchange and no sort on top; the coordinator
   writes its share and the log tail). A spread that fails takes away what it wrote, then one node
   writes it all. `harness.py outside`: "…an aggregate or a LIMIT… from one node" and "…a lake
   table's files and its log tail" fail without it.
101. **Iceberg deletes follow sequence numbers and written paths** (`read_iceberg`): an equality
   delete removes rows only from files with an older data sequence number (with equality deletes
   about, every data file carries its own: `Outside::seq`); a position delete names the path a file
   was written at, matched before `allow_moved_paths` moves it. `formats_check.py`'s
   `iceberg:equality` (a row inserted after the delete) fails without the first.
102. **A commit to another engine's table is put-if-absent and names its job** (`write_outside`:
   Delta's `{v:020}.json` with a `txn` action `pondra:{job}`; Iceberg's `v{N+1}.metadata.json` or
   a REST commit asserting the snapshot it read, summary `pondra.job`). A lost race reads again; a
   retried job that committed writes nothing. `formats_check.py`: "INSERT retried with its job:
   applied once".
103. **A feed's rows and its offset commit together** (`feeds::shard`: producer
   `feed:{view}:{partition}`, seq = the next offset, `prev` = the offset it read from): every
   record lands once whichever node runs the partition; a refused append starts again from what
   was committed. `harness.py kafkas`: "a view fed by a topic on three nodes, one killed, then the
   leader: every record once".
104. **Each kind of catalog entry has a prefix of its own** (round 23: `o/` catalogs attached from
   outside, `fd/` feeds, `e/` secrets; `f/` is the functions'). A scan of one prefix must never see
   another kind: feeds under `f/` broke `/functions` ("missing field flight"). `harness.py kafkas`:
   "…its feed kept apart from the lake's functions".
105. **A query that reads files on the node's machine runs there alone** (`spmd::sliced`:
   `ext::local`): other machines don't have them. One machine can't show it (every node there
   has the files), so no test fails without it; `outside` checks such a query's answer.
106. **A file outside the lake tells the planner what its listing found, and nothing it didn't**
   (`scan::statistics`, `Files::scan`): rows exact from a Parquet footer (an estimate from
   another engine's log), column ranges only from footers, and no statistics at all for a file
   in a NULL's Hive folder — DataFusion adds partition columns' statistics with no NULLs, so
   `count(k)` counted the NULL folder's rows. `harness.py outside`: "Hive-style folders… (NULL's
   folder too)" fails without it. (Without statistics the join order went wrong: TPC-H over files
   took 20% longer.)
107. **Python never runs inside a node** (ADR-027, `python.rs`). Functions and procedures run on
   workers, `python -m pondra.worker`, beside the node. A worker that dies, runs past its time or
   grows past `PONDRA_WORKER_MB` is stopped and replaced; only its request fails, with why.
   `harness.py functions`: "a worker killed mid-query…" and "a batch past its time limit…" fail
   with the Python inside, or with a worker left serving after its time.
108. **A worker costs nothing when unused** (principle 6). Workers start when first asked for,
   and stop after `PONDRA_WORKER_IDLE_SECS` idle (60); the reaper ends when none is left.
   `harness.py functions`: "idle workers are gone…" (`/stats` `python_workers`).
109. **A procedure called by a procedure takes no slot** (`python::Use::Procedure { nested }`): it
   would wait for its caller's slot, which waits for it. Functions' batches have slots of their
   own, so a procedure's query using a Python function never waits for a procedure's.
   `harness.py functions`: "procedures calling procedures take no slot…" hangs without it
   (`PONDRA_PROCEDURES=2`).
110. **A function has no connection to the lake.** A query may run it over millions of rows on
   every node, and a lent token belongs to a procedure's caller. `pondra.sql` in a function is
   refused (`client._inside`), and a worker forgets the connection after each call
   (`_current`, `_last`). `harness.py functions`: "a function has no connection…".
111. **A secret goes only to a procedure's own code, and never out of it.** `GET /secrets/{name}`
   answers only a lent token. What it hands out is replaced by `***` in that procedure's notices,
   its error and the run log (`Lease::redact`), and so is the token. `harness.py functions`: "a
   secret read by a procedure never shows…" fails without either.
112. **A task's tick is committed before it runs, and marked done after its line is in the run
   log** (`runs::due`, `jt/`; `Run::end` answers when written). A leader that finds a tick claimed
   and not done runs it again, with the same job (`task:{name}:{tick}`) and run id, so its writes
   land once and the log has it. `harness.py functions`: "a task through a leader failover: each
   tick's writes once" (ticks == distinct jobs: a leader killed between a tick's end and its
   line's write left one uncounted before the wait).
113. **A query calling a volatile Python function is never answered from the result cache**
   (`routines::volatile`; a Python function is volatile unless IMMUTABLE or STABLE). A query
   reading a Python table function runs on one node (`routines::pinned`): each node would call
   it. `harness.py functions`: "a volatile Python function isn't answered from the result
   cache…" and "…a Python table function runs on one node" fail without them.
114. **An async function works wherever SQL's own do** (`optimize::AsyncBelow`): in a GROUP BY,
   an ORDER BY, a window or a COUNT(DISTINCT), a projection below computes it. A spread plan
   lets `AsyncFuncExec` (`async_func`) split, so each node runs its own rows. `harness.py
   functions`: "a Python function anywhere a SQL one goes…" and "…each node runs its rows
   through its own workers" fail without them.
115. **Procedures calling procedures don't grow the stack by a query each** (`App::query_as` and
   `write::on_node_as` make their futures on the heap, `#[inline(never)]`). A query's future is
   over 100 KB, and 16 nested SQL procedures overflowed a worker thread. `harness.py procedures`:
   "procedures calling procedures stop 16 deep" killed the node without it.
116. **What a procedure prints reaches its caller through every door**: the `x-pondra-notices`
   header (and the Python and JavaScript clients, the shell, `pondra run`), a Postgres NOTICE,
   and MCP's `notices`. `harness.py functions`: HTTP, Postgres, MCP, JavaScript and the shell,
   each "sent to …".

117. **A deterministic function's work is its distinct arguments** (`pyfn::distinct`): an
   IMMUTABLE or STABLE Python function called per row gets each distinct argument tuple of a
   batch once, and its answers are spread back to the rows; a volatile one, or a vectorized one
   (it may look across rows), gets every row. `harness.py functions`: "an IMMUTABLE function gets
   each distinct argument once a batch…" takes 1,000 s without it (past the batch's time limit).
   (107's error names the worker's end because `python::ask` waits up to a second for its exit
   status: its pipes close a moment before the OS reports it gone. CI's busy runner lost that race
   once in a while, round 24's second fix.)

118. **Another engine's append is recorded once, and only on the version it was written on**
   (ADR-028, `iceberg::record`): the leader checks `assert-table-uuid` and
   `assert-ref-snapshot-id` under the lake's lock, against the version last published, and
   answers 409 otherwise (the writer retries on top). A snapshot id already recorded (the job
   `iceberg:{table}:{snapshot}`) is answered as done, before any requirement is checked.
   `harness.py writes`: "two writers at once…" (a lost append without the check) and "a commit
   sent twice…" (a duplicate without the job).
119. **An outside append's rows are the table's own, never the writer's files** (`iceberg::update`):
   they are read and written again, as a bulk INSERT's are (row ids, the table's layout), or go
   through the log when views or tasks follow the table (`write::follows`). The writer's data files
   go once recorded; its manifests go with the table's replaced files, after the retention period
   (Iceberg 1.10 reads its own manifest list once more after a commit). `harness.py writes`: "…the
   rows are the table's own (row ids), the writer's files gone" and "a view of the table follows".
120. **The version published for an outside commit carries the writer's snapshot id**
   (`iceberg::publish`'s `Named`): PyIceberg and Java look their commit up by it after a retry.
   So a version's number (its file `v{N}.metadata.json`, its sequence number) and its snapshot id
   are two things now; only the number names files. `harness.py writes`: "…its snapshot found by
   its id".
121. **Only files in the table's own `data/` folder are taken, and nothing else is deleted**
   (`iceberg::parse`, `inside`): a manifest naming a file anywhere else — another table's, a path
   with `..` — is refused before anything is read. `harness.py writes`: "files outside the table's
   folder" (refused, and the file still there).
122. **A cached function answer is reused only for the same definition and arguments, within its
   lifetime, and only after success** (`pyfn::Answers`: keyed by the routine's JSON and the
   argument row). `harness.py answers`: each check fails without its part.
123. **A live query sends an answer only when one of its tables changed and the answer with it, and
   nothing runs once its client goes** (`live.rs`: a print of its tables' definitions and the
   commits touching them; a keep-alive line every 15 s notices a gone client). `harness.py live`:
   "…none for other tables or the same answer" and "closed: nothing runs…" (`/stats`
   `live_queries`). A page's live queries share one connection (`POST /live` with `queries`, each
   line naming its own: `live.js`): a stream each, six Live cells held all six of a browser's
   connections to the node, and every other request waited. `harness.py live`: "several on one
   connection…"; `console_check.py` (`work`): seven live cells, then Python.
124. **A temporary table is its session's alone, and a query reading one runs on its node and is
   never answered from the result cache** (`temp.rs`; `temp::mentioned` in `App::query_as`,
   `server::query`). `harness.py temps`: "another session doesn't see them", "a query spread over
   three nodes reading one runs on its node, with the same answer" (the other nodes have no such
   table), "not answered from the result cache".
125. **A change to another lake's table is its leader's, with everything it reads that the leader
   can't read sent along** (`change::for_leader`: this lake's tables, files here, the session's
   temporary tables, as `__sent_N`). `harness.py across`: that lake has tables of the same names
   with other rows, so a relation not sent reads the wrong rows.
126. **Each fallback name is its standard name** (ADR-028): DuckDB's, Polars' and PySpark's names
   map to the same code, never a second one. `harness.py names` compares their answers and runs
   every name in `dataframe-api.md`'s table.
127. **A folder named relatively is where the node runs, made absolute** (`ddl::full`: canonical if
   it exists, otherwise absolute with `.` and `..` removed, since object stores refuse them).
   Delta and Iceberg tables written to one (`write_delta("out/x")`) and read back
   (`read_delta`, `read_iceberg`, `delta_scan`), and an Iceberg `location`, are always absolute.
   Tested by `harness.py names`.
128. **A frame shows itself in a notebook** (`_repr_html_`). In `frame.py`, `all`, `len` and friends
   are Polars' expressions, so Python's own must be reached as `builtins.all`, `builtins.len` and
   so on. `package_check.py` calls the display. `anywhere_check.py` fails on any error a notebook
   shows, because IPython turns a failed display into text and the cell still passes.
129. **A table keeps its folder through a rename** (`TableMeta.folder`, `store::folder`): its files,
   and its Delta and Iceberg copies, stay where they are, so other engines keep reading them. A new
   table under a used name gets a folder of its own, `name__N` (not `~`: object_store
   percent-encodes it, and the files would be written where no reader looks). `harness.py renames`.
   The log's rows are kept under a table's name, so a rename sends the table's and its
   `{t}$deleted`'s to files first (`ddl::rename`): left in the log, an `UPDATE`'s old versions came
   back (`harness.py columns`, now and then).
130. **NOT NULL and DEFAULT hold on every door** (`defaults.rs`): `check` in the log's `queue`, in
   `change.rs`, `kafka.rs` and Flight; `checked` for a bulk INSERT's stream; and in
   `write::prepare` for `pondra sql`, which writes its rows to the log itself. `harness.py found`:
   "NOT NULL refused by name on every door…" and "pondra sql: NOT NULL refused…".
131. **The Postgres port's catalog is the lake's** (`pg_catalog.rs`), with stable oids (a hash of
   the name), `pg_type`'s functions under Postgres's own names (ADBC picks a type's binary format
   by `typreceive`) and listed in `pg_proc`, a `regproc` column compared with `0` compared with
   `-`, and joined to a function's `oid` joined by name (Npgsql learns the types that way).
   Visible (`pg_table_is_visible`, `pg_function_is_visible`) means on the search path: `public`,
   the session's temporary schema and `pg_catalog`, so `\dt` and SQLAlchemy's default schema list
   only those, as on Postgres.
   Registered only for SQL that names the catalog (`wanted`), so other queries pay nothing.
   `clients_check.py`: dbt's rows equal Postgres 16's; psql, SQLAlchemy, pgjdbc, psqlODBC, ADBC
   and Npgsql 4.0 and 8.
132. **A lake's table is a `BASE TABLE`** to `information_schema` (`query::Table`), and nothing
   else changes: it passes every call through, its plan included, so DataFusion plans it as
   before. `harness.py found`.
133. **A time without seconds is a timestamp** (`query::seconds`): the `Seconds` optimizer rule
   gives a literal DataFusion would cast its `:00` before it is folded, and `query::strict` does
   the same for rows being written. `harness.py found`.
134. **The console's answers are exact** (`server::typed`): rows as lists, so a join's repeated
   column names survive; decimals, and integers past 2^53, as text. `harness.py found`,
   `console_check.py` (a live answer's decimals to their scale).
135. **A `files()` listing is never a remembered answer** (`server::query`'s volatile words): files
   change without a catalog commit. `harness.py found`.
136. **`DO` is an admin's, and its errors count lines from the code's first** (`routines::do_of`
   drops the newline after `$$`; a DO block's errors carry no routine name). `harness.py
   procedures`.
137. **A folder of lakes' server holds no lake.** Each database is a `pondra serve` child
   (`--advertise host:port/db/name`, `--attach-found <folder>`, `--stop-with-stdin`,
   `PONDRA_SERVER_URL`); the server routes Postgres by the startup message and HTTP by
   `/db/{name}`. `harness.py server` (local, `--s3`).
138. **The console asks only the node that served it** (no CDN or other hosts; its page, modules
   and style sheet are `include_str!`ed and its fonts `include_bytes!`ed from `brand/fonts/`;
   extensions are the node's own files). `console_check.py`: "every request went to the node", in
   every part, with and without an extension.
139. **A view of files is a view** (`StoredView.external`, `ext.rs`): `CREATE EXTERNAL TABLE` stores
   `SELECT … FROM read_*(…)`, copies nothing, and `INSERT` into one over a folder is `COPY … TO
   folder/ (APPEND true, …)`, from a node and from `pondra sql` (`write::from_cli` calls
   `view_write` too); an `INSERT` into any other view is refused, never a table made. A folder's
   keys are declared (`hive_types`), so an empty folder has its columns. The `ext:` tables such
   views read are deregistered in listing sessions. `harness.py external`.
140. **`to_timestamp` answers a TIMESTAMP with no zone** (`optimize::register_zoned` re-makes
   DataFusion's function with `naive()` config, and again in `with_updated_config`), whatever the
   session's zone; text naming a zone is converted to UTC. `harness.py found`.
141. **A session's `DO` blocks share one worker** (`python::KERNELS`): only a `DO` with a session at
   depth 1 goes to it (`routines::python`); it ends with the session (`temp::end`), idle after
   `PONDRA_SESSION_IDLE_SECS`, or past `PONDRA_WORKER_MB`; another session, and a block with none,
   share nothing. Its variables (`GET`) and restart (`DELETE /sessions/{id}/python`) are an
   admin's. `harness.py procedures`; `console_check.py` (Variables, Restart, the admin check).
142. **`pondra serve PATH` guesses only the unambiguous** (`main.rs`): a folder holding other things
   is never made a lake, the current folder only when named; `--lake` on a folder of lakes and
   `--lakes` on a lake are refused with the command meant; one lake's options (`--flight`,
   `--kafka`, `--attach`, `--advertise`, `--attach-found`) are refused with `--lakes`, and every
   other reaches each database's node (`Options.node`). `harness.py server`.
143. **A database's node isn't stopped while in use** (`dbserver::Busy`): an open Postgres
   connection or an HTTP request (until its answer's last byte) holds it; idle time counts from
   when the last one ended; `reap` checks again under the lock before it stops one. `harness.py
   server`: "a database with a connection open isn't stopped…" (on simulated R2, `CREATE DATABASE`
   through psql took longer than the idle time and lost its node before this).
144. **The brand has one source** (`brand/`): nothing else draws the mark or is a logo or favicon;
   the console and the site take it from there. `tools/brand_check.py` (the repository, `--node`,
   `--site`).
145. **Everything the console shows is registered** (`core.js`: `register.*`), the core's own
   parts as an extension's: its views (Data, Workspace, Details, Variables, Runs: `register.view`,
   either side), its kinds of file (`register.doc`), cells, answers' views and actions;
   registrations made while the page starts are drawn with the core's (`started` is set after the
   first drawing). `console_check.py`: the extension's section, tab, view and action; views moved.
146. **The console's files are tagged by their contents** (`console::FILES`: a hash, `no-cache`,
   `304` on a match), so a new binary's console is never a stale one; gzipped when the browser
   takes it; the console's own scripts and style sheet served without whole-line comments,
   blank lines and indentation (`console::lean`: a comment after code too, when no quote or `/`
   follows its `//`), so they hold no string or template literal over several lines. `console_check.py` (every part runs the served code; `budget`: the 304s).
147. **A lake's own files are readable by its readers** (`ext::own_file`), and `GET /objects` reads
   the catalog only (no query). `harness.py external`, `console_check.py` (a file read as a
   table).
148. **A lake's own files are replaced only by their version** (`store::replace`, ADR-034 §4):
   `files/` alone may be overwritten, and only by a `PUT` whose `If-Match` is the version a `GET`
   gave (`412` if it changed: `store::Changed`; `409` with no `If-Match` on a path that exists).
   Everything else in a lake is still never overwritten, which is what lets it be cached; so
   `files/` is never cached: `store::replaceable` keeps it out of the SSD tier (`put` and
   `object`), and DataFusion keeps no list of files (`with_list_files_cache_limit(0)`), so every
   node reads a replaced file at its next statement. `harness.py external` (both nodes read the
   new rows; `--s3`: nothing under `files/` in the tier).
149. **The console keeps its budget** (ADR-034 §7): its scripts and style sheet at most 70 KB
   gzipped as served, first paint under 400 ms, a key under 8 ms in a 1,000-line file, a 10,000-row
   scroll's p95 frame under 20 ms; and axe finds nothing (WCAG 2.1 AA, contrast included), light
   and dark. A change that breaks one makes room first (the ADR's measures: a line highlighted at
   a time, the width in steps, rows drawn in sight). `console_check.py` (`budget`, `layout`).
150. **The editor's highlighting is the whole text's** (`editor.js` `LINE`): each line highlighted
   from the state the one before left (a block comment, a quote, a triple-quoted string), and
   again only as far as a change moves that state, which is what opened a run (`/*`, a quote),
   never a word (an alias `c` once opened a comment). `console_check.py` (`budget`: the lines drawn
   equal to the whole text highlighted, after edits that open and close comments and quotes;
   `files`: an alias `c`).
151. **`run` is Pondra's own procedure** (`workspace.rs`, ADR-033): `CALL run('path', name =>
   value…)` runs a file of the lake's (`.sql`, `.py`, `.ipynb`, or `notebooks/<name>`: its newest
   version), never a procedure of that name (`CREATE PROCEDURE run` is refused). Its values are
   worked out once, as the caller, and bound, never pasted in (`routines::bind`: every `$name`
   missing is named). A file's run is a row of `pondra.runs` named `files/<path>@<version>`, the
   version that ran; its statements get the job's parts, so a retried run writes once. `harness.py
   workspace`.
152. **A file that runs Python needs an admin** (`workspace::run`): a `.py` file, or a notebook with
   a Python cell, runs code on the node, as `DO` does; a writer runs `.sql` files with its own
   rights for each statement, a reader none. Runs inside runs stop 16 deep. `harness.py workspace`.
153. **A file is a file, whoever wrote it** (ADR-029 §1, `adopt.rs`): another engine's appended
   file is recorded where it was written, never copied, once its footer shows it holds the
   manifest's rows, the table's types (by field id, else name), no NULL in a NOT NULL column and
   one partition value; its `lineage` (a first row id, the commit that recorded it, its time)
   gives its rows' system columns, `_row_id` by the row's place in the file (Parquet's row number:
   `scan::adopted`), in every read (`query::read_files`, `files_once`: merges, purges); a merge
   writes them out. Copied instead (round 25's path) when the table has a renamed or dropped column
   (Delta readers go by name: `adopt::fits_as_written`). Pondra's
   own files written without a leader take lineage too, never a rewrite. `harness.py adopted`,
   `writes`.
154. **A table's layout is published for writers** (`iceberg::Layout`): `partition_by` as partition
   spec 1 (spec 0, none, stays for manifests written before), every file's partition record in its
   manifest entry; a `cluster_by` on one column as the sort order; a key as identifier fields
   (required). A new version is published when the schema or layout changes, not only the files
   (`Published::shape`), and at once after `ALTER TABLE … COLUMN`; a renamed column is in the name
   mapping by both names. `harness.py adopted`.
155. **Row ids and log places can't wrap** (ADR-029 §11): row-id blocks come from their own counter
   (catalog `b`, moving only when a node or a bulk write takes a block: `log::To::block`,
   `reserve`), never from commit numbers; a log row's place is `log::ord` (segment << 24, then its
   row), and a segment with more of a table's rows than that takes the numbers after it
   (`log::span`), which a Kafka fetch looks back over. `harness.py ids`.
156. **The REST catalog makes, renames and drops tables as the SQL does** (`iceberg::create`,
   `drop_table`, `rename`): each is the statement, run as the caller (DDL: an admin's); what
   Pondra's tables can't be (a type, a spec of two fields or by bucket, a descending order, a
   staged create) is refused by name, and a table made answers its first published version.
   `harness.py adopted`.
157. **Another engine's change is made against the table as Pondra has it** (ADR-029 §2–3,
   `iceberg::parse`, `adopt::record`, `adopt::stale`): a commit's snapshots (one after another, the
   last made main) may add files and take the table's files out (copy-on-write `DELETE`, `UPDATE`,
   `MERGE`, overwrite), in one catalog commit, sealed files too (their manifests unsealed); a file
   taken out must still be the table's (409); a commit that takes files out while the table has
   rows in the log or changes not yet purged gets 409 once they are tiered, purged and published
   (`iceberg::up_to_date`). Position-delete files (merge-on-read) are taken as written, each data
   file they name the table's (`iceberg::deleted`, `adopt::mark`); a delete file taken out must name
   only files taken out too (`iceberg::still_deleting`). Refused by name: equality deletes on an
   append table, `replace`, changes to renamed tables and to tables followed by what can't take
   rows back (`views::can_follow`). `harness.py rewrites`, `formats_check.py --spark … --only commits`.
158. **Another engine's schema change is `ALTER TABLE`** (`iceberg::schema_sql`): a commit of
   `add-schema` and `set-current-schema` alone (the name mapping's `set-properties` taken and
   dropped: Pondra publishes its own) is diffed by field id against the published schema into
   DROP, RENAME, widening ALTER COLUMN TYPE and ADD COLUMN statements, run as the caller; a new
   required column, a reorder, a changed key or a narrower type is refused before any runs.
   `harness.py rewrites`, `formats_check.py --spark … --only commits` (Spark's ADD COLUMN).
159. **A file commit is a commit through the log** (ADR-029 §7, `adopt::file`, `log::Filing`,
   `Segment::files`): a bulk INSERT's files and other engines' commits go through the sequencer
   with the views' derived rows and the jobs' marks, in one catalog write (`t/` under the lake's
   lock); the views derive from the files' rows (and take back the rows taken out or deleted:
   `{t}$deleted` rows passed to `derive` only). The change feed, `/watch`, Kafka topics and tasks
   read a file commit's rows from its files (`Lake::filed_rows`, `deleted_rows`, `query::tail_of`),
   a big one a piece at a time. A bulk INSERT stamps its files' system columns only when nothing
   follows the table (`write::stamp`); files stamped under a reserved commit meeting a table
   followed since go again without them (`write::AGAIN`). `harness.py followers`.
160. **Deleted rows are positions** (ADR-029 §4, `DataFile::deletes`, `deleted`): cold reads skip
   them by Parquet row selections (`scan::with_deletes`, `placed`, `adopted_in`); the hot columns
   hold an append table's file without them, under a key naming its deletes (`hot::key`), and a
   file with a lineage with its system columns from it (`hot::column`): every file of a table is
   read through `hot::HotFiles` (round 28's fix: they had been left out, TPC-H SF1 from memory
   3.48 s instead of 2.03 s); Pondra's purge writes one position-delete file per partition (`tier::purge`,
   `write_positions`) instead of rewriting files; maintenance rewrites a file a tenth deleted
   (`tier::mostly_deleted`). A replaced file's delete files go with it after the retention period,
   unless another file still names them (`TableMeta::garbage_deletes`, `tier::named_deletes`).
   Published as Iceberg delete manifests and Delta deletion vectors (inline, reader 3 and writer 7).
   `harness.py changes`, `rewrites`.
161. **A published version never looks like a change it isn't** (`iceberg::publish`,
   `delta::publish`): a file listed again is an existing entry with the sequence number and snapshot
   that first added it (`Published::since`, and the manifests a version drops, read); a version whose
   rows didn't change (`TableMeta::rows_at`, the last purge) only rewrote files: Iceberg's `replace`,
   Delta's `dataChange: false`. Writers' own conflict checks pass over Pondra's merges.
   `formats_check.py --spark … --only commits`.
162. **A writer's change to files Pondra rewrote since is carried over** (`adopt::carry`): a file it
   takes out or deletes rows of that a merge replaced (`TableMeta::replaced`, kept while the file
   is) has its rows found where they are now by row id, and deleted there by position; a row changed
   since is a 409. `harness.py transactions`.
163. **A transaction is one commit** (`iceberg::transaction`, `record` over several commits): every
   table's change is checked, then all go in one `adopt::file`, or none; a keyed table's changes go
   a table at a time. `harness.py transactions`.
164. **An upsert table that others read publishes every round** (`tier::shadow`, `TableMeta::shadows`):
   the older versions a round's files replace and its delete markers become positions, found by
   key; once a table held one generation (made, or compacted) every generation is published
   (`delta::publishable`). Its `_deleted` is never published (`TableMeta::marker`: Iceberg reserves
   the name). Pondra's own reads of keyed files pass over those positions (`query::files_once`).
   Another engine's change to a keyed table is rows for the log (`iceberg::keyed`, `adopt::upserts`):
   its rows upserts, the keys it deletes (files taken out, positions, equality deletes by the key)
   markers before them; tiered and published before the answer. `order_by` and merge tables refuse
   it. `harness.py upserts`, `formats_check.py --spark … --only commits`.
165. **Data files carry no Arrow schema in their footers** (`tier::plain`): Parquet's own types only,
   so other engines' Arrow reads strings as strings, not views (PyArrow can't yet take rows of a
   `string_view`, which PyIceberg does to apply a position delete). `harness.py upserts`.
166. **Finding Python never waits on one Python** (`python::candidates`, `probe`): every Python the
   machine has is tried at once, each for `PONDRA_PYTHON_PROBE_SECS` at most; a worker must say
   hello (`worker.py` `hello`) within `PONDRA_WORKER_START_SECS`, and a worker's error carries the
   last lines it wrote. The Python chosen in the console (`PUT /python`, loopback and admin only) is
   kept in `python.txt` beside the console's settings and tried first next time. `tools/package_check.py`
   (`--python auto` with a Python that hangs on the PATH).
167. **A session's worker is never out of step with its cells** (`python::ask_session`): a cell the
   caller stopped waiting for is drained before the next one is sent (or the worker restarted, with
   a notice, if it doesn't end); an interrupt (`POST /sessions/{id}/python`) is SIGINT, raised only
   while a request runs (`worker.py` `_running`), so the variables stay; Windows stops the worker.
   `harness.py procedures`.
168. **The console's settings are the machine's** (`console::settings`, `save_settings`): read by
   anyone, written only from loopback, one JSON object of at most 64 KB, written whole (a temporary
   file renamed); a page on another machine keeps its own in its browser. `console_check.py` (`layout`).
169. **A download is every row, as the file says** (`server::render`, `xlsx.rs`): `?format=csv|tsv|ndjson|
   parquet|xlsx` runs the statement again and writes all its rows, not the 10,000 the console
   shows; a workbook over Excel's 1,048,575 rows is refused with the way out (CSV, Parquet), never
   cut. `harness.py clients` (each format read back: pandas, pyarrow, openpyxl).
170. **What the page needs later loads later** (ADR-034 §7, round 29): Runs, Variables, Settings,
   search and choosing the Python (`more.js`), a table's, a file's or an answer's details and a
   table's profile (`details.js`), a data file (`data.js`), charts
   (`chart.js`), plans (`plan.js`) and their style (`more.css`) load the first time they are used,
   through `R.helpers` (no import of `console.js`); each at most 8 KB gzipped. The first load stays
   within 149's 70 KB. Markdown (`md.js`), Jobs (`jobs.js`), Settings, the key list and the
   sign-in dialog (`settings.js`), a SQL file's Messages and its answers' numbers in the gutter
   (`stmts.js`), and the files' menus and a cell's ⋯ (`more.js`) too. A SQL file's editor and
   answers (`sqlfile.js`, as `pyfile.js` is a Python file's), renaming a file (`rename.js`), the
   tabs' and the panes' menus (`tabs.js`), live queries (`live.js`), the database pill's menu and
   New database (`objects.js`), the grid's editing, menus and filter dialog (`gridmore.js`), the
   Data tree's other objects (`groups.js`), uploads (`upload.js`), users, roles and who has access
   (`access.js`), and a table's own tab (`table.js`) too; the lake's summary loads with Details
   (`details.js`). `console_check.py` (`budget`).
171. **A page of an answer is that answer's rows** (`pages.rs`, `server::page`): every answer the
   console gets is kept whole, under a random id, and a page is a slice of it: the same rows in the
   same order, never the query run again while it is kept; a download (`/sql/pages/{id}?format=`)
   writes every row of it, and only once it is gone does the console run the statement again. Kept within
   `PONDRA_PAGES_MB`, 20 minutes after it was last read; a page of one gone is `410`, never another
   answer's rows. `harness.py found`, `console_check.py` (`grid`).
172. **A notebook's Markdown runs nothing** (`md.js`): the text's HTML is escaped but for tags that
   can't run script or load anything (`br`, `kbd`, `sub`, `sup`, `u`, `mark`, …, no attributes); a
   link is followed only to `http(s):`, `mailto:`, a place in the page or a lake file; a picture is
   `http(s):`, a `data:image/…` or a lake file read with the page's token. `console_check.py` (`work`).
173. **One listing of what a lake holds** (`ddl::listed`): `pondra.tables` (the shell's `.tables`,
   `SHOW VIEWS`, `SHOW MATERIALIZED VIEWS`) and the console's `/objects` are built from it, so a
   table, a view, a materialized view (and its `_final` table) and an external table are called the
   same everywhere; `information_schema.tables` stays as the standard has it (a materialized view a
   `BASE TABLE`), for the tools that read it. `harness.py external`.
174. **The console's actions are SQL (or its Python)** (`objects.js`, ADR-034's third list): every
   menu on an object runs or opens the statement it stands for, one that can't be undone asked
   first; a kind of object is `register.objectKind`, an action `register.objectAction`, so new
   objects (users, grants, flows, an extension's) add a kind, not a tree. `console_check.py` (`work`).
175. **An answer's columns are named apart** (`routines::output_names`): a query DataFusion would
   refuse for two columns of one name (`SELECT ts::date, *`, `SELECT id, *`) runs, a cast named as
   its column or, beside another of that name, as written, a column named twice `id_1`; a query
   that needs no name given is sent as written. `harness.py names`.
176. **One check at every door** (`auth::WHO`, ADR-035): HTTP, Postgres, Kafka and Flight each work
   out a `Principal` (a token's role, or a user's grants) and run the request inside `WHO.scope`;
   anything that reads tables for it (`query::guarded`), writes (`auth::allows`), streams rows
   (`auth::check_all`) or uses a secret (`ext::usable`) asks it. Work no door started (tasks, the
   leader's own) runs as the node. A door that can't tell who it is refuses (no principal is never
   "everything"). `harness.py users`.
177. **A password is kept as SCRAM's verifier, a token as its hash, a secret sealed by a key the master
   key wraps** (`users.rs`, `ext.rs`): the catalog gives none of them away, and nothing is written in
   the clear. `harness.py users`, `harness.py secrets`.
178. **A request can't stop a node; the node's own work still can** (`panics.rs`): panics unwind.
   Start every loop the node can't do without with `panics::spawn` (a panic in it aborts the
   process, as before); a request's work runs in `panics::door` (HTTP's guard, each Postgres
   statement) or in its connection's own task (Kafka, Flight). Never `panic = "abort"` again; never
   a plain `tokio::spawn` for a loop that commits, tiers or follows. `harness.py safety`,
   `tools/fuzz_doors.py`.
179. **With a certificate, nothing crosses a network in the clear** (`tls.rs`): every door takes TLS
   on its own port; a plain connection only from the node's own machine (unless
   `PONDRA_TLS=optional`); a call to another node is `tls::url(…)`, never `format!("http://…")`;
   with `PONDRA_TLS_CA`, the nodes' key only over a connection with the authority's certificate.
180. **Every statement a door is sent goes through `audit::statement`** (`audit.rs`): it is where a
   user's quota (`users::Quota`) is taken and where the audit log is written; a new door or a new
   statement path calls it, once (a nested call is its caller's). A statement that makes a user, a
   token or a secret is kept with its values as `'***'`; `pondra.audit` is a superuser's and is
   never answered from the result cache.
181. **Every save of a lake file is kept** (`files::keep`, ADR-035 §8): a new way to write
   `files/` from a door calls it after the write succeeds; `files/.versions/` is never listed by
   `files()`, and `?version=` reads only that file's own versions. Notebooks are one file each.
182. **A cold start waits only for what serving needs, and the bucket is never stormed** (C5): the
   catalog's compactor and garbage collector start at `store::serving()`; work that can go beside
   serving (a checkpoint, the lake's keys, the leader's mark) does. Every request to a bucket goes
   through its `budget.rs` turns (a new store builder gets `.with_http_connector(Budget::of(…))`);
   keys many writers add have a random first part; nothing lists the whole bucket on a schedule;
   no key is written by many writers faster than once a second. `tools/cold_trace.sh`,
   `tools/c5_check.py`.
183. **A flow moves in one commit** (ADR-036 §1, which calls it a pipeline; renamed by the owner, 2026-10-01): `views::derive` runs views in `in_order`, each
   taking what the views before it derived in the same flush; the sequencer owes a view's rows to
   the views that follow it (any part with rows, not only a producer's). A view of a GROUP BY view
   is a rollup (`views::merges` with `up`) or refused; a flow is dropped from its end.
   `can_follow` and `row_views` follow the chain. A history view (SCD type 2) is an append view
   whose table has `TableMeta::history`: every read of it goes through `views::history_view`
   (`__start_at`, `__end_at`), so nothing may follow it inline. `tools/harness.py flows`.
184. **Expectations count with the rows, and a refusal is the writer's alone** (ADR-036 §2): a
   view's new rows go through `views::expected` (taken-back rows through `let_in`); counts are a
   part of `pondra$expectations` in the same flush; a `Violation` from packing a group sends
   `log::send` back to check each append alone. Errors that are a row refused are `Violation`
   (23514), tables' CHECKs included (`defaults::check`).
185. **Every error has Postgres's code** (`codes.rs`, ADR-036 §4): a new kind of error a client
   should tell apart gets a typed `Coded` or words `codes::by_words` knows, and a test in
   `codes::tests`. Postgres sends it, HTTP's `x-pondra-sqlstate`, Flight's metadata, the clients'
   `sqlstate`.
186. **A transaction is a session's, kept where the session is, and one commit** (`txn.rs`, ADR-036
   §5): every read in it goes through `query::session_at`, which takes its snapshot and its overlay
   (`txn::overlaid`); a new path that reads tables for a session must too, or refuse inside a
   transaction. Writes are kept by `txn::keep`, and `COMMIT` is `Request::Txn` → `commit_here`
   under the lake's lock: the conflict check (40001), then `change::appends` for every table, one
   `change::submit`. `tools/harness.py begin`; `tools/bench/pgbench.py`.
187. **A key lookup isn't planned, on any door** (ADR-036 §6): `serve::point` recognises one (a
   literal or a parameter); Postgres answers it before anything else in `pg::run`, its `Describe`
   and parameter types from it too; in a transaction `txn::point_read`; a one-key UPDATE of a keyed
   table is `txn::point_change`, in a transaction or not.
188. **A session's settings are checked as they are set, and travel with its queries only**
   (`settings.rs`, round 31): `SET datafusion.*` is validated against DataFusion's options and
   applied in `query::session_at` (and `write::declared`); a session that has any runs its queries
   on its own node and never from `Results`. Postgres's undotted names are kept for `SHOW`;
   `datafusion.runtime.*` is refused. A script sent without a session gets one of its own
   (`temp::of_script`, `#` in its id: no client can name it), ended with the script.
189. **SQL's CREATE TABLE refuses a table that is there** (`write::there`, 42P07), unless `IF NOT
   EXISTS` (left as it is: no rows added) or `OR REPLACE` (dropped first). Only `POST
   /tables/{name}` may be sent again (it may add columns at the end).
190. **Spark's functions never change DataFusion's answers by default** (`store::spark`): only
   names DataFusion doesn't have are registered. Spark's versions of shared names come only with
   `datafusion.sql_parser.dialect = 'spark' | 'databricks'` (`settings::apply`), or under names of
   their own (`spark_floor`, `sparksql::renamed`), which only `spark_sql('…')` writes for them.
191. **sqllogictest exceptions are named, never a bucket** (`tools/slt_check.py` `EXCEPTIONS`): a
   failure left out of the pass rate matches a rule with its reason (plan text, a write explained,
   what the runner makes in Rust, the node's memory, microseconds, an order no query asked for).
   A new kind gets a name and a reason, or it is a failure.
192. **`IF NOT EXISTS` and `OR REPLACE` mean one thing for every kind of object** (round 31): a
   kind without its own handling is wrapped (`Ddl::Unless`: nothing if a relation, routine or task
   of the name is there; `Ddl::Replacing`: a materialized view, or one fed by a topic, dropped
   first, refused while another follows it). Both together are refused. Schemas, databases, users
   and roles take only `IF NOT EXISTS`: replacing one would drop what it holds. `harness.py objects`.
193. **`ALTER MATERIALIZED VIEW v DETACH` keeps the table and nothing of the view** (`ddl::detach_view`):
   the view's entry, watermark and producers go, the table stays as it is (a merge table stays
   one); what follows it keeps following the table. A view with a `_final` table is refused. A view
   fed by a topic stops reading it: its feed and offsets go, from the sequencer's memory too
   (invariant 78), as when one is dropped, and a shard running it looks at its feed after every
   fetch, ending when it changed or went (`feeds::shard`), so one made again under its name fills
   again with its own query. `harness.py objects` (a Pondra node's Kafka port as the topic).
194. **A notebook's SQL and Python see each other the same way in the console and in a run**
   (`console.js` `sqlCell`, `workspace::cells`): a SQL query naming a table the session's Python
   holds runs through Python (`db.sql`, which sends it along); a SQL cell named (`%%sql df <<`, the
   console's **→ name**) leaves `df = db.sql(…)` in Python, a frame, not a copy. Only a single query
   is named: a write never runs twice.
195. **The work runs on an 8 MB stack on every OS** (`main.rs`): Windows gives its main thread 1 MB;
   a session's making and DataFusion's planning need more there than Linux's main thread lets on.
   Tokio's workers get 8 MB too (`thread_stack_size`): at its 2 MB, the dist build's
   "procedures calling procedures stop 16 deep" (`harness.py procedures`) overflowed one and
   killed the node (CI, PR #2); 4 MB passed.
196. **Spark SQL is turned into Pondra's where SQL comes in, and nowhere else** (`sparksql::inline`,
   from `routines::expand` and `routines::bind`): `spark_sql('…')` in a FROM becomes a subquery in
   Pondra's SQL (Spark's grammar read by `SparkSqlDialect`; `"text"`, `DIV`, `<=>`, `!`, `RLIKE`,
   `LATERAL VIEW`, `explode`, and the names both have as `spark_<name>`), so a frame built on it,
   a view of it, a spread query or a macro sees plain SQL; it takes a query only. A query that is
   `SELECT * FROM spark_sql('…')` alone becomes that query itself, so its sort holds (DataFusion
   drops a subquery's, and Spark sorts by columns it leaves out); a frame's later steps sort again
   only by columns it kept. PySpark's `spark.sql` sends its queries this way, its writes as
   Pondra's SQL. `harness.py sparksql` (its "by a column it leaves out" fails without the first).
197. **A node that just started answers only once it holds what its leader had** (`cluster::catch_up`,
   `Lake::caught_up`, in `query::session_at` and `write::on_node_as`; 10 s at most): restarted after
   a failover, its catalog view reads no WAL, and what the new leader took over from the old one's
   is flushed a moment after it leads, so a table made just before the kill was "not found" there.
   A Flight log stream that follows a table skips the commits to other tables instead of sending
   them as empty chunks. The website's `guides/clusters.mdx` (a node killed and restarted, then
   asked for that table) failed one run in three under load without it; `cluster.py failover`.

198. **A variable's value is bound, never pasted, and lives where its statements do** (`vars.rs`,
   ADR-037): `DECLARE $day DATE = …` and `$day = …` (DuckDB's `SET VARIABLE`, `RESET VARIABLE`,
   `getvariable` the same) work their value out once, as the caller, cast to the declared type,
   and every `$day` after is a literal in the syntax tree (`routines::bind`). A session holds its
   variables (`temp::Session.variables`); a procedure and a file run hold their own (`vars::own`),
   lent to their Python's connection (`auth::lend`); with no session a `DECLARE` is refused, never
   kept where nothing reads it. A run's given values replace its `DECLARE PARAMETER`s' defaults,
   cast to their types; a value given for a plain `DECLARE` (the script's own) is refused, and a
   file run refuses a name that isn't one of its parameters (`workspace::parameters`: a `.py`
   file's `# %% tags=["parameters"]` cell, a notebook's tagged cell; ADR-044). `pondra.variables` and `pondra.parameters('file')` are never remembered answers
   and run on their node. `harness.py variables`. In the console every tab is a session of its own
   (`core::sessionOf`): a SQL file's variables, a notebook's Python, never another tab's.
199. **A query's session is a copy, and only its functions are shared** (`Lake::session_with`):
   the functions, planners and rules every session has are made once a partition count and
   copied; each copy gets catalogs of its own, so the tables, views and temporary tables a query
   registers are its alone, and what a session changes (settings, its functions) changes its
   copy. Nothing that holds the lake goes into what is kept (`files()`, `file_read`, `secrets()`
   are registered on each copy), or the lake would never be dropped. With the catalogs shared,
   `harness.py temps` fails at its first `CREATE TEMP TABLE … FROM orders` ("the table orders
   already exists": the last query's tables were still registered).
200. **A view's plan is kept from one write to the next only when nothing of that write stays in it**
   (`fresh.rs`): a view or streaming task whose SQL reads the new rows alone (its session registered
   no other table) and calls no function that may answer otherwise next time (`now()` is folded as
   it is planned) keeps its physical plan, keyed by its SQL, the rows' columns, the partitions and
   the lake's functions (`f/`). The rows are a table with no statistics (DataFusion answers a
   `count(*)` from exact ones), and each write resets every operator's state, as DataFusion runs a
   recursive query's plan again, with that write's rows in the leaf. `harness.py flows`: "a kept
   plan keeps no write's count…" (1, 1, 1, 1, 1 with exact statistics) and "…nor its time" fail
   without them.
201. **An inner join runs before a LEFT JOIN only when it reads nothing of the LEFT JOIN's padded side**
   (`optimize::OuterLast`): `(a LEFT JOIN b) JOIN c ON a.x = c.y` is `(a JOIN c) LEFT JOIN b`, never
   when the inner join's keys or condition name a column of `b` (its NULL-padded rows would meet
   it), and never past an as-of join (`asof::marked`: its plan has a shape of its own). The order
   rule then sees the inner joins together; it counts an equality in a join's condition as a key
   (`optimize::equalities`), since filters pushed into joins become keys only a pass later, when
   projections already sit between them. `join_order.py`: "past an outer join, written badly, joins
   the returns last" and "under an exists, written badly, starts from the nation" fail without
   them; `tpcds_check.py`: 99 of 99 the same as DuckDB.
202. **An answer carries only its own rows' strings** (`query::compact`): a string or binary view
   points into a buffer it may share with every other row of the page or batch it came from, and
   Arrow IPC sends every buffer a view points into. So an answer (`App::query_as`), every IPC stream
   (`query::ipc`, `log::encode_ipc`), Flight's streams and a shuffle's pieces copy out views whose
   buffers are mostly other rows'. Without it a `LIMIT 5` of a 50,000-row table was 3.2 MB as
   Arrow, a 10-row ClickBench answer 220 MB (and too big for the result cache).
   `harness.py found`: "a few rows of a table's, sent as Arrow (HTTP, Flight), carry only their own
   strings".
203. **A hot batch is skipped only when its ranges rule it out, and its filters stay above**
   (`hot::HotSource`, `Skip`): every hot column of an ordered type (integers, dates, times,
   decimals; not floats or strings) keeps each 8,192-row batch's least and greatest value, NULLs
   and rows, and a scan skips the batches its pushed filters can't match, as a Parquet scan skips
   row groups (`PruningPredicate`), while saying `PushedDown::No`, so every row is still filtered
   above. A fetch turns skipping off. A dynamic filter is looked at again as it moves, soon at
   first and then ever later (a top-N moves its bound after every batch, and each look costs about
   a batch). A top-N reads the batches in its first key's order (`TopFirst`, through filters,
   projections and exchanges; DataFusion's own sort pushdown stops at a filter), never a scan with
   a fetch, and its sort stays above: only the batches' order changes. The source never shows its
   predicate (`apply_expressions`): a join builds its dynamic filter only for a plan that shows it, and
   building them made TPC-H from memory a tenth slower. `harness.py hot`: a range of the time, a
   top-N either way (through a filter too) and a key skip batches, and NULL-sensitive filters over
   a batch of NULLs answer as the model does (fails on a build without it; the newest rows skip
   nothing without `TopFirst`).
204. **A global min/max skips only rows that can't change any of its answers**
   (`optimize::MinMaxBounds`): DataFusion's filter for one (`a < least so far OR b > greatest so
   far`), which the scans skip row groups and hot batches by, leaves out a min or max of anything
   but a column, and one whose column has been NULL so far; nothing above the scan checks those
   rows again, so `min(a), max(b + 1)` came back too low and `min(a), max(c)` NULL (0.30 and 0.31.0
   too). Unless every bound fills at once (one aggregate; several of one column, or of columns never
   NULL, and none with a FILTER), the aggregate keeps a filter of its own and the scans keep theirs,
   which never moves from `true`. `harness.py minmax`: three of its seven checks fail without it.
205. **A node's memory is its container's** (`store::ram`): the smaller of the machine's and its
   cgroup's limit (v2 `memory.max`, v1 `memory.limit_in_bytes`, the smallest over its ancestors).
   With the machine's, a node in a 1 GB container planned a third of the host's memory for
   queries, more than the container may use. `deploy_check.py image`: "its queries are sized by
   the container's memory, not the host's".
206. **A service's node stops the way a node stops** (`service.rs`): systemd and launchd send
   SIGTERM; on Windows the supervisor closes the child's standard input (invariant 46), and a
   node that must restart to rejoin exits 75 for the supervisor to start it again
   (`cluster::restart` under `PONDRA_SUPERVISED`), never a process the service manager doesn't
   know. `deploy_check.py service` on all three (deploy.yml): "killed, its manager starts it again,
   with its rows", "installed again with other options: stopped, started on its new port, with its
   rows".
207. **A lake newer than this build is refused before its tables are read** (`format::check` in
   `Lake::open`), by name, at every door; a node that would lead it gives the term back first, and
   a follower whose lake moves past it stops (`format::watch`). `upgrade_check.py format`.
208. **The lake's format moves only to what every live node knows** (`format::raise`: followers'
   heartbeats and the commit streams' `x-pondra-format`, after two leases; a node from before
   formats is 0), except a lake the process made, which is its build's at once. A change an older
   release would read wrongly is written only at its format. `upgrade_check.py format` ("…only to
   the newest format every node knows").
209. **Every release's lake since 0.22 opens in this build and answers as it did**
   (`upgrade_check.py lakes`, `upgrade.yml` on every pull request): row ids and versions included,
   then written on.
210. **A node told to stop drains** (`drain.rs`): `/ready` 503, new requests 503 with `Retry-After`
   (the cluster's own calls and the probes still answered), a new statement on an open Postgres
   connection 57P01; the requests in flight when it was told to stop, and only those, finish within
   `PONDRA_DRAIN_SECS` (a leader's followers keep sending it flushes, and on a bucket one is always
   in flight: waiting for none held a leader the whole 30 s); a leader then waits for durability,
   checkpoints and steps down (`cluster/left/{n}`), and a follower whose leader stopped answering
   and stepped down claims the next term without the lease. `upgrade_check.py drain` ("…sooner than
   when it is killed"; with `--s3`, "…exits within 10 s, though its followers keep sending").
211. **A key lookup reads `_deleted` only where it is valid** (`serve::lookup`): a NULL's value bit
   means nothing. `upgrade_check.py`'s "a key looked up (GET /lookup, a point query) as SQL reads it".
212. **A hot column's batches share one allocation per file column, and a buffer several batches
   share stays shared** (`hot::whole`): each distinct buffer is copied into it once, and each
   batch's buffers are pieces of it that say their own size. In the decoder's buffers the columns
   kept its short-lived ones' pages from being given back (the process held twice what they
   count); copying each batch's strings out made a join on a low-cardinality string column carry
   and count a copy per batch (TPC-H q12's build 235 MB instead of 68, and slower); slices of one
   array would count a whole file per batch. `harness.py hot`; TPC-H q12's `EXPLAIN ANALYZE` from
   memory.
213. **A lookup's index of the log tail holds for one `tiered` mark and the key's types, and the row
   a hash names is checked** (`serve::Tail`): it is extended with only the segments after the last
   it holds, in order, a later row winning; when the table's `tiered` moves it is made again (the
   segments before it may be gone), and a row whose key isn't the one asked for (two keys of one
   hash) sends the lookup back to scanning the tail. `harness.py upsert` (3,400 lookups against a
   model through compactions and a restart), `begin`, `dedup`, `layouts`.
214. **A leader cut off from its bucket steps aside, and only then** (`cluster::keep_alive`,
   `budget::answered`): cut off is 15 s with no mark written and no answer from the bucket but
   server errors (slow, or asking to slow down, is still there). It answers heartbeats
   `x-pondra-cut-off` and turns requests away (503, 57P03) while another node is there; a follower
   takes the next term once it has reached the bucket for 3 s, so a bucket down for everyone keeps
   its leader. `resilience_check.py` storage and cutoff.
215. **A client goes to another node only with what can't apply twice** (`client.py`, `index.js`):
   a call no node ran, a query, an append, or one INSERT/UPDATE/DELETE/MERGE sent with a job. A
   session a node holds (`x-pondra-session: held`) stays there; its loss is 08006.
   `resilience_check.py` clients.
216. **A dropped table is kept whole, and only its own entry lets it go** (`ddl::drop_table`,
   `undrop`, ADR-043). Its log rows go to files first (the log moves on). Its entries, and its
   `{t}$deleted`'s, move to `dt/{name}/{ms}` in one commit. While one is kept, its folder is taken
   (`ddl::free_folder`) and its files are in use for the orphan sweep (`tier::collect_orphans`).
   `UNDROP` reads the log from its end (invariant 50). `tools/history_check.py`.
217. **`t AT (…)` reads only a past the table still holds, or says so** (`past.rs`, `tier::settle`).
   A purge drops `{t}$deleted` files only past the table's retention, and moves
   `TableMeta::past_from` to the newest it dropped. A moment before that, and any keyed table, is
   refused by name, never answered with fewer rows. `tools/history_check.py`: "AT refused by name…".
218. **A file in a folder a clone shares is deleted only by the orphan sweep, once no table lists it**
   (`TableMeta::shares`, `tier::expire`, `collect_orphans`): never as one table's garbage, since the
   source's merges, the clone's merges and a `DROP` of either would take files the other reads. The
   sweep counts every table that lists files in a folder (kept dropped tables too). A clone is never
   published (Delta and Iceberg name files under a table's own folder). `tools/history_check.py`:
   "a clone and its source change apart…".
219. **A script's block is one statement on every door** (`script::joined`, `routines::statements`,
   the console's `sqlfile.js` `statements`): a block word opens one only where a statement starts,
   every `CASE` and `END` counts, `BEGIN;` and `BEGIN TRANSACTION|WORK|ISOLATION|READ` are
   transactions, and `if(` and `repeat(` are functions. A script's statements write once per job:
   each one's part is its place and its loop's pass (`{job}:{path}`). A block's `DECLARE`s, a
   loop's row and a handler's `$error` end with them (`vars::local`, `Runner::unwind`); a lone
   `DECLARE` with no session is still refused. A `PARALLEL` pass and an `ASYNC` statement run in
   the script's own task (never spawned: the caller, session and notices hold), each with a copy of
   the variables (`vars::snapshot`). The `ASYNC` statements' state is shared with every step run
   beside them (`Runner::beside`, `Started`), so an `AWAIT` in a block sees them. Their statements
   keep their places in the job, and a script's end waits for what it started. A script that is
   one block has that block's top as its top (`DECLARE PARAMETER`; `script::inside` for a file's
   parameters). `harness.py scripts`.
220. **A lake on local disk lets its catalog's garbage go within a minute** (`store::Catalog::writer`,
   `unpin`): the write-ahead log goes every 5 s, and a compaction's replaced files a minute on, once
   the compactor's 15-minute checkpoint is let go. A small disk the catalog filled otherwise had no
   room to flush, so nothing could clear it. A bucket keeps its minute (C5). `tools/resilience_check.py
   disk` on 64 MB.
221. **An INSERT's own `VALUES` go through the log as `VALUES`** (`routines::values_apart` leaves
   them, and `write::rows` sets apart a subquery's rows), so a subquery in a row never turns a
   one-row INSERT into a Parquet file. `harness.py scripts`, `files`.
222. **A task graph runs once per tick of its first task, and only forward** (`runs::due`): a task is
   due when every task it follows passed (`ok` or `skipped`) at the same tick and it hasn't run at
   it. Its tick (`jt/`) is committed before it runs and marked done, with its status and result,
   after its line is in the run log, so a new leader runs an unfinished one again with its job.
   `CREATE TASK` refuses a graph with two starts or a loop; `DROP TASK` one that others follow.
   Every pass of a script's loop yields (`Runner::pass`), or a task's `timeout` never fires.
   `harness.py tasks`.
223. **Every statement a door was sent is one row of `pondra.history`, written off its path**
   (`history::ended`, from `audit::statement` only, ADR-048): a node's writer appends a second's
   rows through the log (one producer, a seq a batch), at most `PONDRA_HISTORY_RATE` a second, the
   rest of the fast, good ones counted in a `skipped` row; slow (`PONDRA_SLOW_MS`) and failed ones
   always, a slow one with its plan (`history::planned`) and each node's share (`spmd::timed`). An
   admin reads every row, anyone else their own (`history::visible`); a query naming it is never a
   remembered answer. Nothing a statement does may wait on it. `harness.py history`.
224. **A quiet commit doesn't move the version remembered answers are keyed by** (`store::quiet`,
   `Catalog::loud`): one that only adds history rows, moves its producers or its table's entry, or
   lets segments go and moves marks (`tier::expire`'s `QUIET`). Anything else a read could see is
   loud: a new kind of periodic commit must be quiet, or every remembered answer is forgotten at its
   pace (history's commits every second made a repeated query 12× slower on a lake only read). The
   leader's version is the smaller of `loud` and `committed` (`apply` comes first).
225. **DuckDB's spellings are rewritten in one place, and a text with none goes on as it was**
   (`friendly.rs`, from `routines::expand`, ahead of every door, a view as it is read and a
   materialized view when made): what the parser can't read is turned in the text first (`text`:
   `PIVOT t ON …`, comprehensions, `LAMBDA x:`, DuckDB's `ASOF … ON`, `USING SAMPLE`); the rest
   in the tree after the macros (`rewrite`), and a statement it changed nothing in is sent as it
   came (`as_written`). What needs the data (a `PIVOT`'s values, a `FROM`'s columns for `COLUMNS`,
   `RENAME`, an alias in a `WHERE`, `ORDER BY ALL` over `*`, `SUMMARIZE`) is asked with a query of
   its own, as the caller, under the `WITH`s around it: one pass asks, the next takes the answers
   in the same order, so every node of a spread query gets the same text. A lambda stays `x -> …`
   in the text (the generic dialect reads it as JSON's arrow) and becomes one where a query is
   planned (`query::sql`, at every planning site). `harness.py friendly`: 30 forms == DuckDB's
   answers, spread == one node, over Postgres, in a view and a materialized view.
226. **What DataFusion answers wrong is mended where it goes wrong, and the query that showed it
   stays a check** (`tools/random_sql.py`, D2): an IN list that isn't all values (a column, a NULL,
   an expression) is ORs before DataFusion's simplifier sees it (`optimize::InListOfRows`, the
   first logical rule: two lists of one column were intersected as sets of values, and `x NOT IN
   (NULL)` was dropped); `ProjectionPushdown` leaves a projection on a filter that has one of its
   own where it is (`optimize::GuardedPushdown`: DataFusion swapped them as if the filter's own
   weren't there, and the columns pointed at others); a statement written back as text goes through
   `routines::sql` (sqlparser writes `- -3` as `--3`, a comment); DataFusion's aggregate schema
   check is off (its two analyses of a CASE's nullability disagree; the rows are the same).
   `harness.py friendly`'s last five checks fail without them.
227. **An `INSERT … SELECT` or `CREATE TABLE AS` is written by every node only when its rows split as
   they are, under one reserved commit** (`spmd::insert`, `writers`, `/cluster/insert`): the query's
   biggest append table sliced, the rest read whole, no exchange and no sort on top (a `GROUP BY`, a
   join that shuffles, an `ORDER BY` or a `LIMIT` is written from one node), and this node writes its
   share and the log tail. Every share stamps its rows under the one `Reserved` version with a block
   of row ids of its own (`To::block`), or not at all when something follows the table (invariant
   159); the shares' columns must agree, and the leader records every file in one commit with the
   job's mark, so a retried job writes nothing. A share that fails leaves its files to the orphan
   sweep, and this node writes it all. A spread query that names a system column gets them in its
   tables, as `session_at` gives them (`spmd::shared`: it fell back to one node). `harness.py
   insert`: "every node writes its share" (the followers' object writes), the rows, ids and one
   version, "a GROUP BY is written by one node", "nothing fell back… (a spread query naming _row_id
   too)".

## Tests: run these before and after any change

```bash
cargo build --release
python3 tools/harness.py all            # upsert, fence/split-brain, bulk insert, reader, crash, load
python3 tools/gates.py [--prepare]      # the gates (sqllogictest, TPC-H SF1 vs DuckDB, vs Postgres, Nexmark): a row in logs/gates/README.md; exit 1 on a drop
python3 tools/harness.py safety         # panics answered as errors, TLS at every door, mutual TLS, the audit log, quotas
python3 tools/fuzz_doors.py --secs 60   # malformed input at HTTP, SQL, Postgres, Kafka and Flight: the node stays up
python3 tools/resilience_check.py [storage cutoff clients doors disk cache server cli]   # every mode under failure: a failing bucket (faulty_s3.py), a cut-off leader, clients and doors through kills, full disks, pondra sql killed
python3 tools/upgrade_check.py [lakes|format|drain|rolling|all] [--s3]   # every release's lake since 0.22 opens and answers as it did; newer formats refused; drains (a leader on a bucket with --s3); a rolling upgrade under load
python3 tools/soak.py --minutes 10 [--hours 24] [--s3]                   # C4: steady ingest, nodes stopped and killed, memory, the log, commits on a timeline
python3 tools/history_check.py   # DROP/UNDROP, retention, PURGE, Delta; AT (VERSION | TIMESTAMP | OFFSET) == a model of 13 states; RESTORE; CLONE (no copy, apart, merges and drops); refusals
python3 tools/deploy_check.py                  # the image and compose; add python, chart, helm (kind), service: deploy.yml runs them all
python3 tools/harness.py versions       # every file keeps its versions: listed, read, restored, after a delete, retention, old notebooks
python3 tools/harness.py stopped        # a run whose node was killed under it: stopped, not running for good
python3 tools/harness.py scripts        # IF, CASE, loops, handlers, RETURN, EXECUTE IMMEDIATE: errors at their line, scopes, a job run twice writing once, Postgres's protocols
python3 tools/harness.py variables      # DECLARE $x, $x = …, SET VARIABLE, getvariable: sessions, Postgres, procedures, file runs, db.vars, pondra.parameters
python3 tools/harness.py hot            # hot columns skip batches by their ranges (a time range, a top-N either way, a key); NULL filters == the model
python3 tools/harness.py minmax         # a global min/max over 24 files skips no row its other answers need (an expression, NULLs so far, FILTER); a wide top-N's answer
python3 tools/harness.py history        # pondra.history: every door's statements, slow ones' plans and three nodes' traces, the rate, off, who reads what
python3 tools/harness.py friendly       # DuckDB's spellings (PIVOT, COLUMNS, lambdas, ASOF … ON, SUMMARIZE, samples, …) == DuckDB's answers; spread, Postgres, views
python3 tools/random_sql.py --queries 100000 # random queries: one node == DuckDB, every tenth == three nodes, each split three ways by a condition (TLP)
python3 tools/harness.py tasks          # task graphs on three nodes: AFTER, WHEN, pondra.result, retries, timeouts, SUSPEND, refusals, a failover
python3 tools/harness.py sparksql       # spark.sql / spark_sql('…') in Spark's grammar: literals, LATERAL VIEW, Spark's floor and substring, frames on top, refusals
python3 tools/harness.py flows          # views of views in one commit, rollups, expectations (keep, drop, fail), changes down the flow
python3 tools/harness.py begin          # BEGIN … COMMIT from every door, read-your-writes, 40001 and retries, 25P02, SQLSTATEs
python3 tools/bench/pgbench.py          # pgbench's own TPC-B script, 1 and 4 clients: the balances agree (--postgres: Postgres too)
python3 tools/bench/flow.py             # what a flow of 0 to 3 views costs ingest; every stage right when acknowledged
python3 tools/bench/footprint.py        # install size, start to first answer, idle memory: Pondra, DuckDB, Polars, Spark, Flink (a venv with pyspark, apache-flink)
FLINK_PYTHON=/tmp/engines/bin/python python3 tools/bench/nexmark.py --bids 10000000   # Nexmark q1 q2 q5 q7 q11 against Flink 2.3
python3 tools/bench/tpch.py --data ~/tpch/sf1 --queries tools/bench/tpch-queries --engines pondra,duckdb,spark --spark-python /tmp/engines/bin/python   # Spark alone if memory is short
bash tools/cold_trace.sh                # a cold start's requests on the simulator at R2's latency, and the node's own steps
python3 tools/c5_check.py               # the bucket's limits: a 10 writes/s bucket, 240 INSERTs at once, the inbox's bell
python3 tools/harness.py crash --runs 3 --batches 60 --size 50000   # kill -9 + injected crashes, 9M events
python3 tools/cluster.py users --secs 30      # 64 writers + 16 readers: 0 torn reads, 0 lost
python3 tools/cluster.py failover --secs 45   # 2 leader kills; task state == inline view == model
python3 tools/cluster.py latency [--load 4]   # event -> view row on another node
python3 tools/harness.py serverless            # pondra sql INSERT with and without a leader, 4 at once, a retry
python3 tools/open_check.py                    # Delta + Iceberg: 8 outside readers (REST catalog included) == Pondra
python3 tools/freshness.py [--flag ack=replicated]  # head to head: nodes, pondra sql, Delta, Iceberg
python3 tools/harness.py clients               # SQL writes, Python client, Postgres drivers, tokens, inbox, attach, vectors, MCP
python3 tools/harness.py kafka                 # Kafka producers/consumers/groups (librdkafka, kafka-python), Debezium, SASL
python3 tools/harness.py alter                 # ALTER TABLE ADD COLUMN under load, 6 outside readers follow
python3 tools/harness.py windows               # event-time windows closed by the data's time, emitted once, late rows, a leader restart
python3 tools/harness.py sessions              # session windows emitted once, whole; late rows; a leader restart
python3 tools/harness.py asof                  # ASOF JOIN over a stream (a view), ad hoc, over Postgres; refusals
python3 tools/harness.py sums                  # sum(DOUBLE) == math.fsum, whole, grouped, windowed, on every node
python3 tools/harness.py schemas               # schemas, three-part names, attached lakes, DDL, stored and materialized views, drops
python3 tools/harness.py changes               # UPDATE/DELETE/MERGE vs a model on 3 nodes: row ids, views, change feed, purges, Delta, spread
python3 tools/harness.py deal                  # a keyed table's and a view's first tiering round on 3 nodes: deleted keys stay deleted
python3 tools/harness.py guard                 # a query spreads only when it pays (PONDRA_LINK: a slow link keeps it on one node); measured times decide after
python3 tools/harness.py files                 # one-row INSERTs: one Parquet file, one object write each; the catalog's WAL cleared; pondra sql INSERTs not rewritten
python3 tools/harness.py layouts               # PRIMARY KEY + partition_by + cluster_by: rows moving between days vs a model; delta-rs
python3 tools/harness.py clusters              # cluster_by over two columns: row groups narrow in both (Hilbert order)
python3 tools/harness.py copies                # COPY FROM STDIN (text, CSV), TO STDOUT (text, CSV, binary); the ADBC Postgres driver
python3 tools/harness.py streams               # a stream join over two nodes and a restart vs a model; sliding windows vs a model
python3 tools/harness.py columns               # RENAME/DROP COLUMN, a name added again, widened types under streaming vs a model; 4 outside readers
python3 tools/harness.py fills                 # views filled from existing rows while rows stream in, made again, through a leader restart
python3 tools/harness.py dedup                 # a keyed table deduplicated by event time (order_by) vs a model; SELECT * without _deleted
python3 tools/harness.py procedures            # macros, SQL and Python procedures, scripts, parameters, sent rows: rights, depth, 3 nodes, Postgres, MCP, pondra run
python3 tools/harness.py functions             # CREATE FUNCTION (SQL, Python), workers, spread, notices at every door, mail (aiosmtpd), secrets, run log, tasks through failover, speed
python3 tools/harness.py names                 # read_*/write_* names == the tools' fallbacks; dataframe-api.md's table runs; Delta/Iceberg folders (delta-rs, PyIceberg)
python3 tools/harness.py answers               # WITH (cache = '…'): reused within the lifetime, by definition and arguments, only after success
python3 tools/harness.py writes                # PyIceberg (and Pondra) append through the Iceberg REST catalog: once, row ids, 409 + retry, views, refusals
python3 tools/harness.py live                  # live queries: an answer per change of its tables, none for others, gone when closed; JavaScript
python3 tools/harness.py temps                 # TEMP tables and views on 3 nodes: every statement, sessions (HTTP, Postgres, procedures), spread, idle end
python3 tools/harness.py across                # UPDATE/DELETE/MERGE on an attached lake from a follower of another: sent rows, a file, a temp table, no leader
python3 tools/harness.py outside               # files on S3 and HTTP (moto): globs, CSV, JSON, Hive folders, spread, COPY … TO (spread too), secrets, who may read
python3 tools/harness.py clouds                # GCS (sim_gcs.py) and Azure (Azurite) lakes and files: failover, COPY, a file changed beside a lake
python3 tools/harness.py kafkas                # Apache Kafka 4 (~/kafka_2.13-*) and a Pondra node's port: topics as tables, COPY to a topic, feeds through kills, SASL
python3 tools/formats_check.py --spark ~/venv-spark/bin/python   # Delta/Iceberg by Spark 4, delta-rs, PyIceberg == Pondra; attached, REST, INSERT, spread; Spark appending through Pondra's catalog
python3 tools/bench/files_tpch.py --data ~/tpch/sf1-bench        # TPC-H from files (local, S3) against the lake's own tables
python3 tools/files_s3_check.py                # files in a real bucket (R2): a glob, the cache by version, a file changed, COPY there
python3 tools/slt_check.py --slt <datafusion>/datafusion/sqllogictest/test_files [--nodes 3]   # DataFusion's sqllogictest: pass rate, failures grouped (D1)
python3 tools/frames_check.py                  # pondra.frame == Polars (26 pipelines); one question asked 10 ways; a sort kept through steps
~/venv-spark/bin/python tools/spark_check.py   # pondra.spark == PySpark 4.0.1 (55 pipelines, files and UDFs among them: values and column names)
python3 tools/bench/tpch_frames.py --data ~/tpch/sf1-bench   # TPC-H: SQL == frames == PySpark code, 22 of 22
python3 tools/bench/nexmark.py [--bids 4000000] # Nexmark q1, q2, q5, q7, q11: Pondra (== DuckDB) and Flink 2.3 (venv-flink)
python3 tools/smoke.py target/release/pondra   # what CI runs on Windows, macOS and Linux (stdlib only)
python3 tools/anywhere_check.py --bin <pondra> --dist dist [--docker]   # shell, local(), kill -9, wheel, npm, notebook; glibc 2.17 + Ubuntu 22.04
python3 tools/bench/repeat.py --data ~/tpch/sf1-bench --query 15 --runs 20 [--hot]   # one query many times vs DuckDB
python3 tools/bench/outside_append.py            # what another engine's append costs the node (ADR-028's copy; ADR-029's before/after)
python3 tools/asof_check.py                    # ASOF JOIN == DuckDB's: 4 directions and more, one node and 3, 4 ways of planning
python3 tools/stream_check.py                  # one stream, window + session + as-of views: every click once; clicks/s; emission delay
python3 tools/harness.py scale                 # partitions, manifests, 29 spread query shapes (joins of every kind, subqueries, CTEs, key ranges) == one node, memory limits
python3 tools/shuffle_spill.py                 # a shuffle bigger than memory, a node killed mid-query, the scratch freed
python3 tools/join_order.py --lake <tpch lake> # the same queries written badly: same answers, no slower
python3 tools/spread_tpch.py --expect 22 [--broadcast-mb 0]  # TPC-H SF1 on 3 nodes == one node; 22 of 22 spread either way, 13-14 by key ranges
python3 tools/skew_check.py                     # a hot join key: same answers, the busiest node's work shared out
python3 tools/harness.py flight                # Arrow Flight (pyarrow) and Flight SQL (ADBC): exactly-once DoPut, SQL, the log stream
python3 tools/metadata_bench.py [--files 1000000]   # a million files: commits, pruning, a restart, 3 nodes
python3 tools/flight_bench.py                  # Flight in, out, and the log as a stream
python3 tools/kafka_bench.py [--flag ack=replicated] [--kafka ~/kafka_2.13-4.3.1]   # Kafka ingest throughput and latency, 3 nodes; --kafka: the same producers into an Apache Kafka broker
python3 tools/mcp_client.py --url http://127.0.0.1:8080/mcp   # the official MCP SDK (pip install mcp) against a node
python3 tools/keyed_bench.py                   # keyed-table compaction: bytes written, correctness
python3 tools/cluster.py race | isolate | split | spread
python3 tools/bench/run.py batch 20000000     # ENGINES=pondra,spark,flink
python3 tools/bench/singlenode.py prepare --data ~/tpch/sf1   # tpchgen-cli output -> the bench copy
python3 tools/bench/singlenode.py run --data ~/tpch/sf1-bench --sf 1   # vs DuckDB, Polars, Daft, Bodo
python3 tools/serve_bench.py --keys 2000000   # serving: point lookups and dashboard queries
```

The Python tools need `pip install -r tools/requirements.txt` (Python 3.11; the versions the
suite last passed with). `.github/workflows/build.yml` builds on every push what a release ships:
the dist profile for all five platforms (Linux with zig, for glibc 2.17), each tried there
(`smoke.py`), packaged and its packages tried (`tools/try_packages.sh`; the Linux wheel on CentOS
7 and Ubuntu 22.04 too); the Linux job then runs `harness.py all`, `frames_check.py` and a
failover on its own binary, and dry-runs the release's npm publish (`tools/npm_publish.sh
--dry-run`, newest npm). A tag's `release.yml` builds nothing: it waits for that commit's build
run, refuses one that failed or a tag that isn't Cargo.toml's version, and publishes the run's
packages (a minute or two). `cluster-bench.yml`'s default binary (`ci`) is that run's too.
A pull request's push runs CI only when the pull request is labelled: `ci` builds linux-x64 with
the suite, `full-ci` all five platforms with deploy.yml; add `full-ci` once before merging, and
never turn on auto-merge before that run starts. Main builds all five on every push.

Add `--s3` to any of them with a simulated-R2 bucket to see the object-storage behaviour:

```bash
python3 tools/sim_r2.py --port 9000 &   # moto + R2-like latency (PUT p50 197 ms, GET p50 100 ms)
export AWS_ENDPOINT=http://127.0.0.1:9000 AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
       AWS_REGION=auto AWS_ALLOW_HTTP=true PONDRA_BUCKET=testbucket
python3 -c "import boto3; boto3.client('s3', endpoint_url='http://127.0.0.1:9000', region_name='us-east-1').create_bucket(Bucket='testbucket')"
python3 tools/harness.py crash --runs 3 --batches 150 --s3
python3 tools/cluster.py failover --s3 --flag ack=replicated   # recovery of acked-but-not-durable commits
```

**A tiering failure is silent in the correctness tests** — reads stay correct, the log just stops
draining — so it shows up as a throughput drop in `tools/bench/run.py live` (and as
`background job failed:` on the leader's stderr), not as a test failure. `harness.py tiering`
checks the log drains and the file count stays bounded; watch the live benchmark for the rest.

**`failover` and `users` are the tests that catch read-consistency bugs.** `failover` fails about
1 run in 8 when something is wrong — run it 15–20 times before believing a fix. `users` caught the
round-6 mirror bug in 4 of 5 runs; run it at least 5 times after touching `store.rs`.
`crash --size 50000` is the one that catches "the leader can see its own in-flight writes" bugs.
`open_check.py --rounds 1250` is the one that catches slow tiering rounds (and Delta log and
Iceberg snapshot cleanup). Any change to replication or recovery: `users` and `failover` with
`--flag ack=replicated`, locally and with `--s3` on simulated R2, several runs each.

Practical notes for an agent working here:

- Never rebuild the binary while a test suite is running (tests exec `argv[0]` when a node restarts).
- Test runs delete their lakes when they exit (`harness.new_lake`; `--keep` or `PONDRA_KEEP=1`
  keeps them). The owner's R2 free tier is 10 GB: after R2 runs, `tools/clean_bucket.py --bucket
  … --newest 3` leaves only the newest three lakes.
- Kill leftover nodes with `pgrep -x pondra` (never `pkill -f` or `pgrep -f <script name>`: it
  matches your own shell) and clean `/tmp/pondra-*/` afterwards, or the disk fills up. The SSD
  tier's default folder `/tmp/pondra-cache/` goes with it.
- Node stderr goes to `/tmp/pondra-<port>-<id>.stderr`; that's where "restarting to rejoin",
  "slow tiering" and panics show up.

## State of the work (2026-10-03, round 32 complete: 0.32.0)

Everything in `docs/prototype-status.md` passes on local disk and on simulated R2. The round-11
additions (manifests, partitions, shuffles, memory limits, Arrow Flight) also ran against real
R2; round 12's are in `logs/round12/`, round 13's in `logs/round13/`, round 14's in
`logs/round14/`, round 15's in `logs/round15/`, round 16's in `logs/round16/`, round 17's in `logs/round17/`,
round 18's in `logs/round18/`, round 19's in `logs/round19/`, round 20's in `logs/round20/`, round 21's in `logs/round21/`, round 22's in `logs/round22/`, round 23's in `logs/round23/`, round 24's in `logs/round24/`, round 25's in `logs/round25/`, round 26's in `logs/round26/`, round 27's in `logs/round27/`, round 28's in `logs/round28/`, round 29's in `logs/round29/`, round 30's in `logs/round30/`, round 31's in `logs/round31/` and round 32's in `logs/round32/`.

**Round 32 (after 0.30.0; 0.31.0, 0.31.1 untagged, 0.32.0): lean and fast.** The join order from
every input (TPC-DS q72 81 s → 0.17 s; the 99 18.2 → 12.1 s), planning and small queries cheaper
(199), flows within 5% of ingest (200), joins under `EXISTS` and past `LEFT JOIN`s (201), answers
carrying only their own strings (202), hot batches skipped by their ranges (203), a global min/max
that skipped rows it needed fixed (204, wrong since before 0.30), a wide top-N filtering as it
decodes Parquet, in-memory columns holding what they count (212), key lookups indexed over the log
tail (213). Measured against DuckDB 1.5.5 and 2.0's preview, Flink 2.3, Apache Kafka 4 and the
single-node engines (`prototype-status.md`, round 32; the site's performance and comparison
pages). In the same release, round 33's first parts from the side threads: the lake format and
upgrades (ADR-039, 207–211), deployment (ADR-041, 205–206), every mode under failure and the five
faults it found (214–215), a full local disk that recovers on its own (220), a table's past (`UNDROP`, retention per table, time travel `AT (…)`,
`RESTORE`, zero-copy `CLONE`: ADR-043, 216–218), `CREATE VIEW v (a, b)`, `DECLARE PARAMETER`
(ADR-044), scripts that branch, loop and handle errors (ADR-045, 219) and the console's batches. After the
release: scripts' `PARALLEL`, `ASYNC` and `AWAIT` and task graphs (ADR-045, 219, 221–222), and
observability: every statement a row of `pondra.history`, slow ones with their plans and each
node's share, a slow-query log (ADR-048, 223–224). Left of 33: the 24-hour R2 soak (the owner's
machine); environments, branching data and the team's workflow are designed in their own thread.

**Round 29, part 1 (ADR-034, after 0.27): the owner's console list.** The grid's outline, header
card and menus, typed filters, Copy and Download in every form (a download is every row:
`?format=csv|tsv|ndjson|parquet|xlsx`, 169); Messages and Runs that say what ran (a `DO` block logs
its code); the plan as a graph with a profile; charts to choose and save; Format for SQL and Python
(`POST /python/format`); calmer toolbars (Save only when there is something to save); Settings as a
dialog, light first, colours per theme, kept on the machine (168); Python found without waiting on
a broken one, chosen in the console, interrupted by Stop, never out of step (166, 167); the first
load back under 70 KB, the rest loaded when first used (170). The Docs workflow builds from a clean
checkout again. **The owner's second list:** Markdown cells drawn as GitHub does (172), a SQL cell's
Chart and Plan kept with its notebook, tabs that scroll and pin, pages of rows kept on the node (171),
Jobs apart from History (`register.jobKind`), a clearer Data tree, the header's card above the
pointer, Format selection and Format file, Settings as sections with a search (`register.setting`).
**The owner's third list:** rows a page as a setting and in the pager (`?rows=`), a quieter pager and
notebook footer, the Run ▾'s items in the editor's right-click with **Create as table or view**,
**Data profile** and **Query profile** as two names for two things, a right-click menu for every kind
of object in the Data tree with **Script as** in SQL or Python (174), a cell made Python or SQL, and
`pondra.tables` for the shell's `.tables` (173), a cell added between two cells, and `SELECT ts::date, *`
running as other engines run it (175).

**Round 28 (ADR-029 phase 2): other engines' changes as written.** Spark's `DELETE`, `UPDATE` and
`MERGE` copy-on-write and merge-on-read, PyIceberg's `delete` and `overwrite`, through the catalog,
against the table as Pondra has it (invariant 157); schema and property changes as `ALTER TABLE`
(158); every file commit through the log, followed by views, tasks, the feed and Kafka in the same
commit (159); deleted rows as positions, Pondra's own too, published as delete files and deletion
vectors (160); versions that don't look like changes they aren't, and changes carried over Pondra's
merges (161, 162); multi-table transactions (163); keyed tables published every round, and other
engines' upserts and deletes into them (164). The console's Workspace got folders and the unsaved
dot, after the owner tried 0.26 on Windows.

**Round 27 (ADR-029 phase 1): other engines' appends as written.** An append through the Iceberg
catalog costs the node its files' footers and a commit, not a copy (a million rows: 0.01 s of its
CPU against 0.24 s); the files keep their lineage for their rows' system columns until a merge
writes them out; tables made, renamed and dropped through the catalog; the layout published
(partition spec, sort order, identifier fields); row ids from blocks of their own and log places
that can't wrap (invariants 153–156). Followers fed from the files and the feed seeing file
commits moved to round 28.

**The workspace (ADR-033), after round 26:** the lake's files run as jobs, `CALL run('etl/orders.sql',
day => …)` from every door (HTTP, Postgres, MCP, Python's and JavaScript's `db.run`,
`pondra.run` in a file, `pondra.start('run', …)`, tasks); `.sql` files with `$name`s bound, `.py`
files with their values as variables, notebooks with a `parameters` cell (papermill's) and a saved
one by name; files running files, 16 deep at most; each run a row of `pondra.runs` named
`files/<path>@<version>`. In the console: a SQL file's parameters bar, a file's Run as a job and
Schedule…, and Runs listing the node's runs and schedules (invariants 151, 152).

**Round 26, continued (ADR-034): the console as the canvas drew it.** Tabs of notebooks, SQL,
Python, data and text files; Data and Workspace on the left (a filter; either view moves to the
right as a tab), Details, Variables and Runs on the right; a SQL file's statements each with its
answer (or, by Settings, the last one's); a Python file's console; CSV and JSON edited in a grid and
saved in place (`If-Match`, invariant 148); one grid everywhere (coloured type marks, a header's
card, a spreadsheet's selection, copy, filter and sort); Geist fonts, the charcoal dark theme,
Settings, Sign in, pane edges, narrow windows; the budget and axe checked (invariants 149, 150).

**Round 26, continued (ADR-032), before the tag:** `CREATE EXTERNAL TABLE` as a view of files;
`to_timestamp` as DataFusion answers it; a page's Python cells sharing a worker (variables,
figures, restart); `pondra serve PATH` / `--lake` / `--lakes` in place of `pondra server`, over a
local folder or a bucket prefix, a database kept up while in use; the brand in `brand/`; the
console rebuilt as a core with an extension API (`window.pondra`, `PONDRA_CONSOLE_EXTENSIONS`),
a details panel, profiles, a virtual grid, completion, variables, figures, an outline. Proposed:
the server's catalog (ADR-032 §9) and a workspace of files, runs and parameters (ADR-033).
Backward compatibility is promised from the production-ready release (1.0) on, not before
(ADR-032 §8).

**Round 26 (ADR-030): the console, the server, dbt and BI, and the documentation website.**

- **The website** (`site/`, Starlight, GitHub Pages through `pages.yml`): 49 pages for users —
  start, guides, reference, concepts — every example run by `tools/docs_check.py` (456 of them,
  in CI too). The owner must turn Pages on once (Settings → Pages → Source: GitHub Actions).
- **Writing it found 37 bugs**, each fixed with a check (most in `harness.py found`): NOT NULL and
  DEFAULT enforced, `INSERT … ON CONFLICT`, `UPDATE … FROM`, `DELETE … USING`, `TRUNCATE`, the
  byte and JSON types, unknown options refused, uncastable values refused, TIMESTAMPTZ in UTC,
  Iceberg field ids in Parquet, read-your-writes on followers, and the guard's estimate of what an
  aggregation moves (the 6-node bench's q1/q3/q4/q12).
- **Postgres's catalog** (`pg_catalog.rs`): dbt (seeds, models of every kind, snapshots, tests,
  docs, run twice) gives the same rows as Postgres 16; psql's backslash commands, SQLAlchemy,
  pgjdbc, psqlODBC, ADBC and Npgsql 4.0 and 8 (Power BI's driver) work. `ALTER TABLE | VIEW …
  RENAME TO` (dbt's swap).
- **A folder of lakes as databases** (`dbserver.rs`; `pondra server` then, `pondra serve --lakes`
  now): each a node started on use and stopped when idle; `CREATE/DROP DATABASE`; queries across
  databases.
- **The console at `/`** (then `console.html`, one file, no CDN): a tree with row counts, SQL, Python
  (`DO LANGUAGE python`) and text cells, live answers, notebooks as `.ipynb` versions in the lake,
  Jupyter's keys, light and dark. `console_check.py` drives it in Chromium.
- **Found at the end:** `pondra sql` didn't check NOT NULL; ADBC's Postgres driver couldn't read
  `pg_type`; Npgsql (Power BI's driver) knew none of the types; SQLAlchemy's default schema listed
  every schema's tables; a node on a lake whose first leader hadn't made its catalog stopped
  instead of waiting (round 26's "holds no lake yet", caught by `cluster.py race`: the wait now
  matches it, `Lake::NO_LAKE`); a time without seconds wasn't a timestamp; tables listed as views in
  `information_schema`; a `files()` listing could be a cached answer. All fixed (invariants
  129–138).
- **Not in this round:** TLS. (A folder in a bucket, completion in the console and Python cells
  sharing variables came in its continuation, ADR-032.)


**Round 23 (ADR-026) read and wrote everything else:** files on S3, GCS, Azure, HTTPS and the
owner's machine as tables (listed each statement, cached only by version, spread, their footers'
statistics in the join order); Delta and Iceberg tables read natively and `INSERT`ed into;
`COPY … TO` files (a big folder by every node) and topics; other Kafka clusters as tables and
feeds; `CREATE SECRET`; lakes on GCS and Azure. TPC-H SF1 from files runs as fast as from the
lake's own tables (`logs/round23/files-tpch-sf1.txt`). D1 began: DataFusion's own sqllogictest
files through Pondra (`tools/slt_check.py`, `logs/round23/slt-*.json`), which found `INSERT INTO t
(columns)` missing and `CREATE TABLE t (a INT) AS VALUES` ignoring its names (both fixed).

**Round 24 (ADR-027) made SQL and Python one.**

- **Functions.** `CREATE FUNCTION` takes Postgres's forms, in SQL (expanded in place) or Python:
  per row, vectorized, or a table. Python functions run on warm workers beside each node, spread
  with their queries, and work in GROUP BY, ORDER BY and windows.
- **Procedures** do anything Python can: mail, HTTP, files. They run as their caller, print
  notices back through every door, read `CREATE SECRET`s that never show, and are logged in
  `pondra.runs`.
- **Tasks** run a statement on a schedule (`CREATE TASK … SCHEDULE`), each tick once through a
  failover.
- **The clients.** `@db.function` and `@db.procedure` take a notebook's function as it is: its
  imports, helpers and constants go along. `pondra.sql` is the current connection. PySpark's
  `udf`, `pandas_udf` and `udf.register` work.
- **Speed.** A warm `CALL` takes 2.4 ms (0.15 s before). A Python function runs at 15M rows/s
  vectorized and 6.6M rows/s per row, on this 2-vCPU sandbox.
- **What the building found:**
  - DataFusion can't run an async function in a GROUP BY, an ORDER BY or a window (a rule now
    moves it below);
  - spread plans refused `AsyncFuncExec`;
  - nested SQL procedures overflowed the stack (a query's future is 120 KB; now on the heap).
- **CI.** Round 23's build run failed after 53 minutes (`logs/round24/ci-round23.txt`):
  - the suite's S3 simulator needs `moto[server]`, which the sandbox had and CI didn't;
  - Apache Kafka from archive.apache.org took most of half an hour (now dlcdn, and cached);
  - rust-cache saved nothing after a failing job, so the Linux job compiled all 394 crates
    (now `cache-on-failure`).

  The pondra crate itself takes about 9 minutes in the dist profile on every push.
- **Round 24's CI** failed three times on the Linux job after the round was pushed; each fix is
  its own commit on top of it (the tag `v0.24.0` goes on the last):
  - two function checks were too tight for the runner (9672068);
  - a dead worker's error lost its reason when the OS reported the exit a moment late (7bf651a);
  - the spread guard chose a query's way by its last run, so one slow run flipped it (e123acf).

**Round 25 (ADR-028): one vocabulary, open writes, live answers.** The owner split the planned
round: the engine first (this), then the console and `--server` (round 26).

- **E9, one vocabulary:** `read_*` / `write_*` in SQL, Python and PySpark; the tools' names are the
  same functions. `dataframe-api.md` has the table, and `harness.py names` runs every name in it.
  `COPY … TO` makes, appends to and overwrites Delta and Iceberg tables in a folder.
- **G8, other engines append** through the node's Iceberg REST catalog: Spark 4, PyIceberg and
  another Pondra. Their rows become the table's own, each commit once (invariants 118–121). The
  owner's reason for not using Polaris or Unity: JVM services, and they'd be the truth, not Pondra.
- **B3, live queries;** **E10, answers kept** (`WITH (cache = …)`); **temporary tables and views**;
  **changes to attached lakes from any node** (invariants 122–126).
- **The owner's questions after it:**
  - Do other engines use Pondra's compute? Reading, no: they read the files. An append costs
    the receiving node a read and a write of the appended rows (the price of row ids). Taking the
    writer's files as they are, with ids by position, would be a design change: ask first.
  - MERGE/DELETE into other engines' tables? Not yet: Pondra appends to Delta and Iceberg
    (INSERT, and `COPY … (FORMAT delta | iceberg, APPEND | OVERWRITE)`), and refuses the rest by
    name. Databases are G6, planned after the console round. Writing Delta's removes and deletion
    vectors and Iceberg's overwrites and position deletes is within reach (Pondra reads all of
    them), when the owner wants it.
- **The owner's direction, 2026-09-29:** "total serverless and compute/storage separation":
  other engines should read *and write* with their own compute. That is **ADR-029, proposed**
  (G9 in the roadmap, in three phases). Its round is the owner's call: before or after the
  console.
  - It **builds on the first step's decisions** (ADR-001: files in place, never copied; ADR-002:
    the streamhouse R1–R6 and the Fluss verdict; ADR-003: serverless, "your compute, the
    leader's commit").
  - It does not reopen them. The owner pointed out that a restated comparison with Flink, Fluss,
    Spark and DuckDB had already been done at the start. **Before writing an ADR, read ADR-001 to
    ADR-003 and cite them instead of re-deriving.**
- **Found while designing ADR-029:** row ids are `(commit << 32) + n` and Kafka offsets `(segment
  << 32) + row`, both 64-bit signed. At about 500 commits a second (a trickle on local disk),
  offsets turn negative after about 50 days and ids repeat after about 100. It is a known limit,
  fixed in ADR-029's phase 1 (blocks from a counter of their own).
- **Found at the end of round 25,** by running the notebook on the wheel:
  - a relatively named Delta or Iceberg folder couldn't be read back (invariant 127);
  - a frame's notebook display had failed since 0.22.1 (invariant 128);
  - on R2, PyIceberg's writes need its fsspec file IO.
- **CI:** `v0.24.0` was released from e123acf (run #21 green on all five platforms). The Kafka
  cache step moved to `actions/cache@v5` (Node 24). The linux-x64 build and the release job are
  pinned to `ubuntu-24.04`, because `ubuntu-latest` becomes 26.04 on 2026-10-19; move them on
  purpose, after a green run there.
- **The cluster bench:** the owner runs it on `main` (v0.24.0) at 3 and then 6 nodes, as round
  25's baseline, and again once round 25 is pushed.
- **The owner's decisions after round 25 (2026-09-29):**
  - Round 26 is the console, `--server`, dbt and BI **and a documentation website**, together.
    The site uses Starlight on GitHub Pages, with every example tested. The owner: "even I can't
    know exactly what things we have and how to use the product full power."
  - ADR-029 comes after that: phase 1 in round 27, phase 2 in round 28. Security moves to round
    29.
  - "Our extension framework" means DuckDB-style `INSTALL`/`LOAD`. Nothing was designed before;
    ADR-031 (proposed) designs it, and its round is still open.
  - pyarrow stays optional ("we might make it required in the future, but not now").

**R2 test buckets.** There are two:

- `ponderabucket-us` (Eastern North America, ~290 ms per PUT from the sandbox; the default);
- `pondbucket` (~670 ms per PUT).

Set `PONDRA_BUCKET=ponderabucket-us` for test runs: `LH_BUCKET` in `~/.r2env` names `pondbucket`,
and runs there take two to three times as long (round 27's `server` and `serverless` missed their
time limits on it, and passed on the near one).

**The owner's R2 free tier is 10 GB.** Test runs delete their lakes; keep at most three lakes in
all. After R2 runs: `tools/clean_bucket.py --bucket ponderabucket-us --bucket pondbucket --newest
3 --dry-run`, then without `--dry-run`.

**The repository (2026-09-27).** `alimardon123/pondra` on GitHub holds the code, pushed by the
owner from the bundles (the sandbox can't push). It is public and, since round 21, licensed
**MIT OR Apache-2.0** (`LICENSE-MIT`, `LICENSE-APACHE`); the wheel and npm packages carry both.
The owner is setting up PyPI (trusted publisher: `release.yml`, environment `pypi`) and npm (a
token for the first release, trusted publishing after); a `v*` tag then builds, tries and
publishes (`.github/workflows/release.yml`); `publish = false` keeps the crate off crates.io.
The first tag, `v0.21.0`, failed on Windows when packaging (`npm` is `npm.cmd` there, which
`subprocess` doesn't look for), so nothing was published (the other four platforms passed); fixed after round 22, with checkouts
kept at `\n` line endings everywhere (`.gitattributes`) and `pondra`'s own npm package
uploaded from Linux only. `v0.22.0` (b70033e) made the GitHub release and put all five wheels
on PyPI, then npm refused its first package: the newest npm (12) reads `dist/x.tgz` as the GitHub
repository "dist/x.tgz" and won't fetch git. `tools/npm_publish.sh` passes `./dist/…`; the npm
packages are published by starting `release.yml` by hand with publish ticked (the GitHub release
is made only on a tag; PyPI skips what it has). The owner installed 0.22.0 from PyPI on Windows:
it worked from Python, but `pondra` wasn't found (a user install: pip's folder isn't on PATH) and
`pip install pondra` without pyarrow couldn't answer a query. 0.22.1 (ADR-024) fixes both, with
one-line installers on every release, and makes the shell's folder of lakes its databases.
npm refused the release's first publish (a token needing a two-factor code; npm doesn't take a
package's first version through trusted publishing), so the owner published 0.22.0's six npm
packages by hand (`npm.cmd`: PowerShell's scripts are off there) and set each one's trusted
publisher (`release.yml`, environment `pypi`, "Allow npm publish"); from 0.22.1 on, the tag
publishes PyPI and npm with no token. 0.22.1 (the tag at 3c59e6a) is on PyPI and npm with its
installers on the release; from the sandbox, the Linux one-liner put `pondra` on PATH in a clean
shell and the PyPI wheel answered `FROM t` without pyarrow (`logs/round22/0.22.1-published.txt`).
`v0.22.2` (the fix of invariant 93) was tagged at c47e2c7, which still builds on the tag; from
0.23.0 on, a tag publishes what the build workflow made and tested. Its history was rewritten once, before it went public, to
put the owner's GitHub noreply address on the four commits that had their email; commit IDs from
before then (in older bundles) differ. Each round the owner downloads the new bundle and, in
their clone, runs `git pull <bundle> main` and `git push`; GitHub then builds it on Linux,
Windows and macOS (`.github/workflows/build.yml`).

**The cluster bench so far (round 18).** The owner has run `cluster-bench.yml` on GitHub's
4-vCPU runners, TPC-H SF10 (60 M lineitems), four times: one node (18.6 s in all); three nodes
(every answer right, all 22 queries spread, but 33.2 s); three nodes again, which lost a node at
start to the follower-before-catalog race (invariant 52, fixed); and three nodes on round 18's
code (`logs/round18/cluster-bench-3-nodes.json`): every answer right, the race hit and handled
(a node restarted once), one node 23.1 s, three nodes 46.6 s. The runners reach each other over
the public internet through Tailscale: 17–54 ms round trips, 51–150 MB/s. The ten queries that
move under 1 MB between nodes take about what one node does (9.8 s against 10.7 s); the twelve
that shuffle move 936 MB and take 36.0 s against 13.3 s: about 0.7 s plus 15 ms per MB (about
68 MB/s). So on this network a shuffle costs more than it saves, and even picking the faster way
per query would give 22.4 s against one node's 23.1 s. Round 19 made that the rule (`guard.rs`,
invariant 60): a query spreads only when the bytes it would move, at the measured speed of the
slowest link, cost less than the work it shares out. `driver.py` now waits for a settled lake and
times each query on one node, as the cluster decides, and spread anyway. Next: that run on round
19's code, then scale-out where machines share a data centre (the owner's Google Cloud trial: one
zone, well under 1 ms, 1–2 GB/s).

**The cluster bench on round 19** (the owner's run, `logs/round20/cluster-bench-round19-run.json`,
links 23–65 ms and 43–116 MB/s): one node 15.7 s, as the cluster decides 18.0 s (5 queries
spread, 4 of them slower for it: 2.3 s in all), spread anyway 39.5 s; 80 MB moved instead of
938; every answer the same. Loading took 442 s against 108 s: a `pondra sql` bulk INSERT's files
were rewritten by the leader to stamp row ids (fixed: invariant 70). A query that has run both
ways now goes the faster way (invariant 72). Next: machines in one data centre.

**The cluster bench on round 20** (the owner's run, `logs/round21/cluster-bench-round20-run.json`,
links 41–53 ms and 53–69 MB/s): loading 105.9 s (442 s on round 19: fixed); one node 24.5 s, as
the cluster decides 24.8 s (8 queries spread: 0.22 s lost, 0.15 s won), spread anyway 34.7 s;
every answer the same. The runners vary: compare within a run. Right after the load the leader's
tiering merged small files for 16–28 s a round on R2 (open).

**The cluster bench on round 21** (`logs/round21/cluster-bench-round21-run.json`, links 34–66 ms,
40–84 MB/s): loading 100.5 s; one node 21.7 s, as the cluster decides **21.5 s** (3 queries
spread, 9 MB moved), spread anyway 38.9 s; every answer the same. The cluster is now no slower
than one node over the internet; the tiering merges after the load took 13–32 s a round again.

**Where the multi-machine run will happen (the owner's plan, 2026-09-23).** The owner has no VMs
of their own. They will run the multi-machine tests themselves, later, on one of:

- **GitHub Actions** — free runners joined into one network with Tailscale's free plan, the lake
  in their R2 bucket. A private repo gets 2-vCPU / 8 GB runners and a monthly minute allowance; a
  small *public* bench repo holding only the workflow gets 4-vCPU / 16 GB runners, free and
  unlimited, while the binary stays in R2 and the source stays private. Jobs last at most 6 hours;
  runners have 14 GB of disk (SF10 fits, SF100 doesn't) and are shared, so compare shapes (1 → 3 →
  6 nodes), not headline numbers. `.github/workflows/cluster-bench.yml` and `tools/cloud/actions/`
  are the workflow; `bench-bin/pondra` in `ponderabucket-us` holds the portable binary
  (Linux x86-64, glibc 2.17) for its `binary: r2` input (round 22's since this round). Rebuild and re-upload it when the code
  changes, and read results from `bench-results/<run id>/results.json`.
- **A Google Cloud VM trial** ($300 for 90 days, no charge unless they upgrade) for dedicated
  machines, SF100 and Spark on the same VMs, with `tools/cloud/cluster.sh`.

An agent can't reach VMs from its sandbox (outbound HTTPS only, through a proxy: no SSH, nothing
inbound) and must not push to GitHub. So the pattern is: the agent prepares the workflow or
scripts, the owner runs them, and the runs write their results under `bench-results/` in the R2
bucket, which the agent can read with the credentials in `/home/claude/.r2env`. If the owner
links a session to their computer, an agent can drive VMs from there instead.

Headline numbers, all on one 2-vCPU box:

- **Frames, macros and procedures** (round 22, ADR-023): `pondra.frame` (Polars' names) and
  `pondra.spark` (PySpark's), one SQL statement each, as fast as the SQL (10 M rows: 0.063 s
  against 0.065 s); 26 pipelines equal to Polars, 44 to PySpark 4.0.1 (values and column names),
  the 22 TPC-H queries equal as SQL, frames and PySpark code; SQL and Python mixed ten ways, one
  answer. Macros cost a small query 0.07 ms; a SQL `CALL` 2.9 ms; a Python procedure a process
  start (0.15 s).

- **Columns that change, views that start full** (round 21, ADR-022): `RENAME COLUMN`, `DROP
  COLUMN`, widening `ALTER COLUMN … TYPE` with no file rewritten (a scan of 10 M rows 0.033 s
  before, 0.034 s after), and Delta (column mapping) and Iceberg (field ids) readers following;
  `CREATE MATERIALIZED VIEW` filled from the rows already there, every row once (10 M rows:
  2.3–2.6 s); keyed tables deduplicated by event time (`order_by`). Nexmark q1, q2, q5, q7, q11
  over 10 M bids: 8.7–10.9 s (Flink 2.3: 24.3–25.0 s), the same answers as DuckDB.

- **Fewer objects, any layout, streams joined** (round 20, ADR-021): a trickle of one-row INSERTs
  writes 1.1 objects each (3.9 before) and leaves 217 (5,962); tiering with system columns 1.10–1.16 s
  per 8 M rows (round 19 1.45–1.55, round 18 0.88–0.98, side by side); a PRIMARY KEY with
  `partition_by` and `cluster_by`; `cluster_by` over two columns along a Hilbert curve (a filter on
  the second 2× faster); `COPY` over Postgres, the ADBC Postgres driver; joins of two streams (a
  pair 14 ms after its second row) and sliding windows.
- **Change any row** (round 19, ADR-020): `UPDATE`/`DELETE`/`MERGE` on every table, system
  columns (`_row_id`, `_version`, `_created_at`, `_updated_at`), views and a Delta-style change
  feed that follow every change, purges so Delta and Iceberg see it. On 10 M rows: an UPDATE of
  100,000 rows 0.26 s, a MERGE of 100,000 0.9–1.2 s; a scan 0.07 s unchanged, 0.08 s with a row
  changed, 0.17 s with 1% changed in every file until the purge (2.6 s). A query spreads only when
  it pays. `CREATE DATABASE`, `CHECKPOINT`, `ALTER TABLE … SET`, local files in the shell.
- **A database you can shape** (round 18, ADR-019): schemas and `lake.schema.table`, other lakes
  attached in SQL (`ATTACH … AS …`) and queried and written across, `CREATE`/`DROP SCHEMA`, `DROP TABLE`, CTAS, stored views that spread over
  the nodes, `CREATE MATERIALIZED VIEW`; the schemas listed over Postgres, Flight SQL, Iceberg
  REST and MCP. Query planning costs what it did. Memory figures on Windows and macOS; a smoke
  test on all three OSes in CI.
- **Installs anywhere** (round 17, ADR-018): a glibc 2.17 binary (CentOS 7, Ubuntu 22.04), a
  wheel and npm packages built by `tools/package.py` and tried in fresh environments (not yet
  published), `pondra` as a shell (a session in 0.14–0.44 s), `pondra.local()` in a notebook,
  and a node that stops, handing the lake on, when whoever started it dies (the lake reopens for
  writes 0.2 s after `kill -9`). `sum(DOUBLE)` gives the same answer in any order (TPC-H q15: 0
  of 20 runs wrong, 8 of 20 before).
- **Any query across the nodes:** all 22 TPC-H queries run on 3 nodes, each answer equal to one
  node's, the same every run, with small tables whole or every table sliced (ADR-015). 13 of them
  run by key ranges, `orders` and `lineitem` meeting on the order key without a shuffle: 4.81 s
  for all 22 on three nodes sharing one box, 5.70 s with every table sliced (round 14: 5.85 /
  7.66 s). A hot key's partition is shared out (busiest node 1.27× the average, not 1.95×)
  (ADR-016).

- **Streaming on event time** (round 16, ADR-017): windows closed by the data's own time, session
  windows, `ASOF JOIN` (1 M trades × 200 k quotes in 0.18–0.32 s on one node, DuckDB 0.24 s; the same
  answers as DuckDB's every way, on one node and three). One stream with window, session and
  as-of views: 0.37 M clicks/s in (2.4 M with none), every click once, windows out 0.5 s after
  the click that closes them.
- **TPC-H on one machine, from Parquet:** SF1 **3.19 s** (round 12; 3.07 s in round 15's run, 2.99–3.17 s in round 16's, DuckDB 3.18–3.38 s), SF10 **38.0 s** — ahead of DuckDB
  (3.36 / 39.8), Polars (3.78 / out of memory), Polars streaming (3.18 / 42.8) and Daft
  (6.11 / 89.0). With the columns in memory: **1.96 s** / **35.9 s** (DuckDB's native tables:
  1.80 s at SF1; SF10 doesn't fit on this machine). Every answer is checked against DuckDB's.
- **Petabyte-shaped metadata:** a table given a million files commits a 20 KB entry, and,
  published as Delta and Iceberg, keeps a 24 KB / 70 KB state and publishes in 17 ms.
- **Arrow Flight:** 15.7 M rows/s in (exactly-once), 9.1 M rows/s out, the log as a stream in
  2.6 ms p50.
- **Writes on R2:** acked in 4 ms with `--ack replicated` (299 ms durable); 87k events/s from 64
  writers, replicated.
- **Kafka:** ~0.8 M events/s exactly-once into 3 nodes, ack 1 ms p50 (replicated).
- **Freshness, like for like:** nodes see a write 10–15 ms after the ack (local and R2); Delta
  and Iceberg readers ~30 ms (local) / 3–4 s (near R2) / 7–10 s (far R2).
- **Serving:** 0.14 ms key lookups, 20–36k/s.
- **Consistency:** 0 torn reads, 0 lost batches, clean failovers, in both ack modes.

The comparison with Spark, Flink, Fluss, Lakehouse//RT and the single-node engines — item by
item, with what each is building next and the plan for the gaps — is
`docs/comparison-spark-flink-fluss.md`.

**Fixed in 0.22.2: deleted keys that came back** (found 2026-09-27 as `harness.py changes`
failing now and then on release builds). A keyed table's first tiering round on a cluster deals a
job per node, and every one of them took its file for the table's first (the table had no files
yet), so each dropped its delete markers and a view's emptied groups. The jobs after the first one
have older rows to shadow, in the first job's file: keys deleted in their part of the log came
back, and an adding-up view lost the part of an UPDATE that changed a total but not a count. Only
multi-node clusters, only the first round (or the first after a compaction left no rows), and
only what that round's later jobs held, hence "now and then". Found by keeping a failing run's
lake and comparing each file with the log segments it covers (the log was right; one file lacked
a group). `harness.py deal` does it on purpose: fails without the fix, every run (invariant 93,
`logs/round22/0.22.2-first-round-dealt.txt`).

Known limits, in the order they matter:

1. **Multi-machine runs only over the internet so far** (GitHub's runners, where a shuffle costs
   more than it saves: the guard keeps such queries on one node). `.github/workflows/cluster-bench.yml`
   and `tools/cloud/` are the kits; the owner starts them.
2. **What follows a table and can't take a row back** (windows emitted once, min/max views, views
   over joins, streaming tasks) makes a change of it refused; Kafka consumers see an UPDATE's new
   rows, not its deletes; a purge rewrites whole files (no deletion vectors yet); `ALTER TABLE`
   renames a table only by copying it (`CREATE TABLE … AS`, `DROP TABLE`), and never narrows a type.
3. **Distributed edges:** a `LIMIT` inside a subquery over sliced data and order-preserving
   shuffles run on one node; a join with a hot key on both sides shares out only one side; key
   ranges are found from the files, not declared (a table written out of order isn't sliced by
   them); a query's own answer still passes through the coordinator's memory once.
4. **Join order is only as good as its statistics.** Distinct values come from per-table
   sketches (about 6% off; rows deleted from keyed tables stay counted), and the order the query
   wrote is the baseline to beat. Declaring files' order to DataFusion made TPC-H slower (round
   15), so an aggregation on a sorted key still hashes.
5. **Replicated acks' window.** An acked write survives any one node dying (with `--fsync`,
   followers' power loss too), but not the leader and every holder dying before the bucket has
   it.
6. **One sequencer per lake** orders commits. Attached lakes split the load across leaders, but
   there are no transactions across lakes, and `UPDATE`/`DELETE`/`MERGE` on an attached lake's
   table run only on a node of that lake (an `INSERT` works from anywhere; round 23).
7. **Memory is bounded by budgets, not by accounting.** What DataFusion counts is the big hash
   tables and sort buffers; Parquet decoding and the batches in flight are not counted, so the
   query budget defaults to a third of RAM and the hot columns watch the process's own memory.
   A lake on a local disk needs room for about four minutes of its catalog's inline writes (a few
   hundred MB under a stream of small batches); it no longer grows past that (invariant 220).
8. **Kafka's edges:** one partition per topic, no transactions, sparse offsets, consumer groups
   in the leader's memory.
9. **Streaming:** one watermark per source (not per partition or node), held by a quiet source;
   no timers or CEP, no Top-N per key by event time yet; a view's fill runs in one go on the
   leader (rows in memory); an as-of join in a view joins what the table has when the
   event arrives (Flink's temporal join waits for the table's watermark); keyed tables keep only
   their latest row, so as-of joins need a table's history kept as rows.
10. **Security:** tokens per role only; no TLS (use a proxy) — the nodes' own calls to each other
    are plain HTTP too, so run a cluster in a private network, a VPC or Tailscale — no per-table
    grants or quotas.
11. **`VARIANT` is JSON text**, not a shredded variant; `ai_*` and Flight functions call out of
    the process, so their latency is the endpoint's.
12. **Packages:** 0.22.1 is on PyPI and npm (all five platforms). Outside CI, only the owner's
    Windows machine (0.22.0 from PyPI) and the sandbox's Linux (0.22.1: the installer, the wheel)
    have run a published package; no winget or Homebrew package. The node's JSON leaves out nulls (the Python
    client puts them back; the JavaScript client doesn't yet). Only `sum` over DOUBLE is
    order-independent (not `avg`, `stddev`, …).
13. **Frames and procedures** (round 22): a Python procedure starts a process per call (a warm
    pool would take the 0.15 s away) and doesn't run on a schedule yet; the JavaScript client has
    no frame builder; `pondra run models/` (a folder of `.sql` and `.py` models in order of what
    reads what) is next; a `MERGE` from rows sent with a request needs the leader to receive it.
    `con.sql(query)` is lazy since round 22: it runs when its rows are asked for, each time.
14. **Outside the lake** (round 23): another engine's table takes `INSERT`, not `UPDATE`,
    `DELETE` or `MERGE`; Iceberg is written as v2 only; Kafka's SCRAM-SHA-512 and TLS are built
    but untested against a broker; `CREATE EXTERNAL TABLE` isn't taken (a view over files is the
    way: `CREATE VIEW t AS SELECT * FROM 's3://…'`); Delta and Iceberg data files aren't kept in
    memory between statements (files named by URL, a glob or a folder are, by version).
15. **SQL conformance (D1):** 67% of DataFusion's sqllogictest records pass on one node (73%
    without its Spark-function files). The rest, grouped in `logs/round23/slt-1-node.json`: session
    `SET`/`RESET`/`PREPARE` (each request is its own session), `CREATE EXTERNAL TABLE` and the
    tables it would have made, Spark's function library, EXPLAIN's text (Pondra plans its own
    way), number literals typed DECIMAL (as Postgres and DuckDB do), strings read as `Utf8View`.

Good next moves: `docs/roadmap.md` (2026-10-03) is the shortest path to 1.0, at the owner's bar
("high quality, stable, workable, fully featured"): round 33 closes with the soak and environments;
34 is SQL as people write it and scale on machines; 35 in-process and in the browser; 36 fitting
in (databases attached, BI tools, measures in views); 37 is 1.0 (the promises, a security review,
signed packages, the docs).

## Conventions

- Comments explain *why*, in plain English; the code shows *what*. Keep functions small.
- No new dependencies without a real reason; no new always-on services, ever.
- Every new invariant gets a test in `tools/` that would fail without it.
- Docs live in `docs/`; a design change means a new ADR, not an edit to an old one.
- Backward compatibility is promised from the production-ready release (1.0) on (ADR-032 §8): the
  lake's format, SQL, the HTTP API, the clients and the command line. Before 1.0, change what makes
  the product better, and write each change in its ADR and the release notes.
- The owner's priorities, for every change: performance, simplicity, ease of use, ergonomics,
  a beautiful result, scalability, power, and versatility.
- The logo and colours come from `brand/` only; the console is extended through its registry
  (`window.pondra`), never by editing a copy of it.
