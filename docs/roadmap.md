# Pondra: the shortest path to 1.0

**Date:** 2026-10-11 (one month to 1.0; the 2026-10-03 plan of rounds is in git) · **Status:** the
month is the owner's (2026-10-11), the lanes my plan for it; the decisions at the end are his.

**No release until the redesign is done** (the owner, 2026-11-07): 0.80.0-rc.1 and 0.80.0 are not
tagged on 11-06 and 11-11 as planned. The redesign on main comes first, until its architecture is
modular, flexible, reusable and easy to extend, and proven to work; then L1 recommends the version,
and new features ship on it. `release/0.80` keeps 0.80 as it stands (its candidate passed every gate
on 11-07), so the owner may still tag it himself later, once the gates run again on its head. On
11-02 he had named the month's release 0.80.0, not 1.0, so a clean 1.0 has room to mature. 1.0
follows the redesign, once every row below is proven; until then no release promises compatibility
(the stability page's 1.x promises start at 1.0). "After 1.0" below still means after 1.0.

**The bar** (the owner, 2026-10-03): reach 1.0 faster and more productively, with everything before
it high quality, stable, workable, fully featured and true to the design principles (AGENTS.md).
So from here: no new surface unless a bar below needs it, every round leaves every angle better
(principle 8), and each part merges to main as soon as CI is green; a version is cut only when its
round is complete.

## Where Pondra stands

Rounds 17 to 34 are released (0.34.0).
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
| **Scales out** across machines | Spark | TPC-H 22/22 spread == one node; SF10 on GitHub's runners over the internet: 25.1 s on one node, 23.9 s on 3, 21.1 s on 6 (`logs/round29/cluster-bench-0.27.0-*`); 9,997 random queries spread, 0 wrong (`logs/round34/random-100k.json`); one machine SF1 4.5 s vs Spark 56.1 s (`logs/round30/tpch-sf1-vs-spark.json`) | machines in one data centre; SF100 falling 1 → 3 → 6 machines, faster than Spark on the same VMs; a lake in memory or in a laptop's folder that other machines join, and members with no address of their own (a page, a laptop behind NAT, a CI runner: ADR-072 §8) | L1 (his VMs) |
| **In-memory speed** on one machine | DuckDB 2.0.0, Polars 2.0 | TPC-H SF1, every engine on one 4-core machine (`logs/round35/singlenode-tpch-sf1-duckdb-2.0.0.json`): from memory 0.99 s (DuckDB 2.0.0's own tables 0.81; Polars 2.0 2.06, streaming 1.56), from files 2.04 s (DuckDB 2.0.0 1.55); 0.34.0's gates on a 4-core machine, the median of three: 1.08 s from memory (DuckDB 1.5.5's own tables 1.15), 2.06 s from files (2.19) (`logs/gates/`); SF10 on a GitHub runner from memory 12.95 s (DuckDB 1.5's tables 15.28), from files 19.50 s (23.70) (`logs/round34/singlenode-tpch-sf10-github.json`); ClickBench 10 M rows from memory 7.79 s (DuckDB 2.0.0's tables 6.50); with the hot columns within half of RAM (11-01), 6.99 s, of which q29 1.45 s, where DuckDB's own took 3.05 s in round 35 (DuckDB's other 42: 3.71 s, Pondra's 5.54) (`logs/round36/singlenode-clickbench-10m-half-ram.json`), from files 11.03 s (9.96) (`logs/round35/singlenode-clickbench-10m-duckdb-2.0.0.json`); loading: TPC-H SF1 5.2 s (DuckDB's tables 1.4), ClickBench 25.7 s (6.1) | from memory at SF1, 22% behind DuckDB 2.0.0's own tables; from files, 32% behind DuckDB 2.0.0 and Polars 2.0's streaming engine; ClickBench from memory: within half of RAM the hot columns hold 7.1 GB of hits' 7.3 GB on a 15 GB machine (DuckDB's tables 7.9 GB), and the queries a third of RAM held from Parquet are near DuckDB (q25 8 ms against 5, q37–q43 2× at 7–80 ms); q24 (`SELECT *` top 10) 4× and q23 3.4×, URL group-bys (q34, q35) 1.6×; loading, 4× DuckDB's time; ClickBench's 100 M rows | L1 |
| **Bigger than memory**: past memory, an answer or a refusal by name, never a killed node | DuckDB 2.0.0, Polars 2.0 | TPC-H SF10's lineitem under a hard 2 GB cap: 16 of 19 heavy queries answered (DuckDB 2.0.0 17, Polars 2.0 4 on 10-16), every answer both gave the same; a spilling GROUP BY merges in one pass again, and GROUP BYs and joins run 15-41% faster than before that fix, side by side; a window beside a spilling sort still gets memory (invariant 423); `string_agg` over 3 huge groups, which killed the node (DuckDB's too), and the median beside it answer 53200 eight times back to back, the node alive after (`logs/round36/bigger-than-memory/`); a whole-partition `avg() OVER` under 2 GB 2.5 s with nothing spilled, from 47 s and 4.3 GB (#344; DuckDB 2.0 s); spill written as LZ4 frames, 1.8 to 6 times smaller, no query slower (#357); the median over 3 groups of 60 M DECIMALs under 2 GB in 0.8-1.2 s on its first try, from 19-40 s on its frugal retry, DuckDB 2.0.0 2.8-3.3 s on the same box (`logs/round36/median-3-groups-2gb.json`: rows made like lineitem's flag and price) | `string_agg` over a huge group is refused by its name only when the group passes its cap before its partial sends it on, else by the memory watch's words | L1 |
| **Lightweight** | Spark, Flink (JVMs) | 118 MB installed, first answer 0.14 s, 109 MB idle; Spark 6.9 s and 549 MB, Flink 6.4 s and 585 MB (`logs/round32/footprint-warm.txt`); a cold first answer (the library out of the page cache) 0.13-0.17 s to DuckDB 2.0.0's 0.16-0.17, in-process 0.17 s to 0.20 (`reviews/l2-lightweight-1102.md`); a top-10 of every column of a 105-column table (ClickBench q24) peaks at 0.36-0.38 GB to DuckDB's 0.37, in 0.41-0.47 s: its other columns are read only for the rows it keeps (#445; it was 0.92-0.96 GB) | idle memory in a Python program 92 MB to DuckDB's 78–83 (0.34.0, both holding pyarrow's ~28 MB: L2, 10-25), and a node's 98-105 MB to DuckDB's 60; a node's memory flat for a day on R2 (a 5-hour heap leg passes the memory check since jemalloc stopped taking huge pages, #185: `reviews/soak-heap-d9c739f.md`) | L2, L7 |
| **In-process**, a library in Python and Node | DuckDB, Polars | a node inside the program, no server (#98: first answer 0.21 s, DuckDB 0.12); `connect()` the one way to start, from 0.34.0 (the owner, 10-12); closed in 6 ms (#205) | a lake in memory as fast as DuckDB's `:memory:`. Main e68a042 against DuckDB 2.0.0 in Python (L2, 11-02, `tools/bench/memory.py --tpch sf1`, medians of 3: `reviews/l2-in-process-1102.md`): a first answer 0.18 s to 0.20 and a close 13 ms to 125, but 10 M rows from a series in 2.86 s to 0.56, a warm aggregate 57 ms to 25, TPC-H SF1 loaded in 3.29 s to 1.48, memory after its queries 3,039 MB to 1,770; its queries fully warm (their hot columns loaded) on main 5bef730, 21 of them without q15: 1.31 s to DuckDB 2.0.0's 1.05 (1.24x), level or faster on 9, 1.4-2x on q1, q3, q6, q7, q12, q18, q19, q21 (`logs/round36/inprocess-tpch-sf1-duckdb-2026-11-02.txt`) (on 10-25 the queries were 2.4x DuckDB 1.5.5's and the load 3.8x); the gaps are the engine's | L2, L1 |
| **In the browser** | DuckDB-Wasm | the page (#143–#176): the core engine as wasm in Node and Chromium, a node's requests and answers (ADR-061), files put and read by SQL, a lake's published Delta tables read over the web at any version, files skipped by their ranges; TPC-H SF1 in Chromium, 22 queries in 3.79 s against DuckDB-WASM 1.5.4's 4.06 s, every answer the same, each query's median of 3 in one run (`logs/round36/page-tpch-sf1-duckdb-wasm-1101.json`, `reviews/l2-p4-gates-1101.md`); tables kept across reloads (OPFS); files dropped on the page (#389); a member of a cluster with no address of its own (#346, #349), which writes (#377) and whose statements its node records (#397); the module 8.60 MB brotli, within its 9.2 MB budget; `web_check` Node 137 of 137, Chromium 185 of 185 | loading TPC-H SF1's 8 tables 7.23 s against DuckDB-WASM's 7.04 s, both single-threaded (#409: a `CREATE TABLE … AS FROM` a Parquet file keeps its row groups; written as before it took 19.3 s), its 22 queries then 3.86 s to 4.09 | L2 (the page), L1 (the cluster's side) |
| **Streaming SQL** | Flink | Nexmark's 20 queries over 10 M events on one 4-core VM, answers == DuckDB's: all 20 at once as views, the last answer 80 s after the first event, each event sent over HTTP and committed (median of 3, 8.0 GB at most); Flink 2.3 685 s with RocksDB state (9.0 GB), and with its state on a 10 GB heap it failed after 18 minutes; each query alone, Pondra faster than Flink's best on 10 of 20, its times counting all 10 M events taken in (`logs/round34/nexmark-10m-flink.json`); q18 (each bidder's latest bid on an auction) alone 30.8 s, Flink 2.3 37.5 s with its state on the heap, the two run in turn on the same box (`logs/round36/nexmark-q18-10m.json`); windows, sessions, as-of joins and stream joins, each row once through kills; `MATCH_RECOGNIZE` (ADR-073), as a query or a view that writes each match once (`EMIT FINAL`), finding the same matches as Flink 2.3's CEP over 1,000 tickers: 1 M rows in 1.68 s as a query and 3.26 s as a view, Flink 5.87 s; 10 M rows in 19.8 s as a query and 25.8 s as a view holding 1.8 GB at most, Flink 49.2 s holding 3.7 GB with its state on the heap (`logs/round36/cep-vs-flink-*.json`) | q18 answers 22–31 s after the stream when all 20 run at once; machines in one data centre; views' runs, stream joins and windows' emission dealt to every node, not only the leader (Nexmark on 3 and 6 nodes); a watermark per partition, after 1.0 with its seam kept now (the owner, 10-31); timers after 1.0 | L1, L6 (`MATCH_RECOGNIZE`), L4 (partitions) |
| **A log you query as tables** | Fluss, Kafka | Kafka's protocol, exactly once: 0.99 M events/s on one node, 1.03 M on 3, ack 3 ms p50; Apache Kafka 4.3 1.14 M, 1 ms (`logs/round32/kafka-*.txt`). Against Fluss 1.0 on one machine (`logs/round34/fluss-1.0-head-to-head.json`, medians): starts in 0.08 s (3.3), 120 MB idle in one process (514 in three), appends 4.6 M rows/s (4.7 M), every row read back in 0.34 s (0.48), upserts 11 M rows/s (0.30 M), lookups 0.28 ms (102), a row seen 2.4 ms after its write (5.4), 463 MB 15 s after the run (1,054), every answer right (Fluss loses upserts at its default batch size) | a single row's ack 1.6 ms against 1.03; 2.0 GB against 1.0 GB in the moment after the run, while the allocator keeps freed pages 10 s for the next query; a topic's partitions; Kafka's own pace and ack | L1, L4 |
| **A lakehouse in your bucket** | Databricks, Snowflake | Delta and Iceberg by Spark 4, delta-rs and PyIceberg == Pondra's reads on 55 of 55 tables, one node and three; Spark's appends, DELETE, UPDATE, MERGE through Pondra's catalog (`logs/round28/`) | Glue, S3 Tables, vended credentials; Iceberg v3 row lineage (after 1.0) | L4 |
| **An engine on any Iceberg catalog** (the owner, 2026-11-09): another catalog's Iceberg tables, and our own catalog's (`CREATE TABLE … USING iceberg`), read and written as Spark 4 does them, before a team moves its truth | Spark 4, Trino | Iceberg tables by Spark 4 and PyIceberg read as they do, and Spark's appends, DELETE, UPDATE and MERGE through Pondra's own catalog (the row above) | through the one `iceberg` table-format entry (`ATTACH … (TYPE iceberg)`, `USING iceberg`), a built-in extension on the table-format contract, each checked against Spark 4 in `formats_check.py`: DELETE, UPDATE and MERGE as position deletes; ALTER of columns and partition specs; `AT` and `RESTORE`; CREATE, CTAS, CREATE OR REPLACE, DROP and RENAME in their catalog; their table properties honored on write (copy-on-write or merge-on-read, target file size, compression, sort order, every partition transform); the metadata tables (`$snapshots`, `$files`, `$history`, `$partitions`, `$refs`); VACUUM and OPTIMIZE as Iceberg's maintenance; tables whose data files are ORC, read and written through an ORC file-format entry shared with `read_orc` (today `read_iceberg.rs` refuses them; Avro data files after). Our own catalog's Iceberg tables also get what the catalog gives every table: one commit across tables, maintenance as jobs (compaction, snapshot expiry by retention, orphan cleanup), rights, tags, EXPECT, lineage, audit, UNDROP and branches, the change feed, streams and live views, and service to Spark and Trino through our REST door. An ability the entry lacks is refused naming the format. The rest is 1.1 (After 1.0) | L1 (the entry, redesign step 5), L3 (writes, step 9), L6 (words) |
| **Other engines on our tables** (the owner, 2026-11-09; the mirror of the row above): Spark, Trino, PyIceberg and DuckDB read and write `pondra` tables through our REST catalog door | Spark 4, Trino | outside engines' appends, DELETE, UPDATE and MERGE through our catalog taken as written (A lakehouse in your bucket); an engine's catalog calls taken as Pondra's own statements on the real table, each through PyIceberg as Spark sends it (`harness.py writes`): a rollback as `RESTORE` to that snapshot's time, a staged create (Spark's `CREATE TABLE … AS SELECT`) as `CREATE TABLE` with the engine's rows, a replace (`CREATE OR REPLACE TABLE … AS SELECT`) as `CREATE OR REPLACE TABLE`, keeping the table's uuid | the same Spark checklist with each engine as a client of the door: reads from the catalog's own records with no copied files (redesign step 14); honest history; a change of partitioning (refused by name until step 5's partition specs) and an expiry of snapshots (refused, naming `--retain-secs`, until VACUUM: Spark's deletes the files itself right after its commit); Spark, Trino and DuckDB themselves as the clients, not PyIceberg alone; what Iceberg can't carry stays ours (millisecond freshness, which they see per tier round; their appends to keyed tables are upserts) | L2 (steps 13 and 14), L7 (the gate in CI) |
| **Serverless**: the bucket is the only state | Spark + a metastore, Kafka + ZooKeeper | an INSERT with no node running 0.05 s; a new lake serving after 4.3 s and 28 bucket requests (`logs/round29/`); after its only node is killed, the next writer on its machine writes in 0.05 s, and on another machine in 5.1 s (it rings the lake's bell, no mark comes; 30 s before: `harness.py serverless`) | another machine's writer still waits the bell's 5 s | L7 |
| **Within the bucket's limits** at any size | S3's and R2's request limits | one request budget per node; 240 INSERTs at once into a bucket taking 10 writes a second (`c5_check.py`, on the simulator) | a logged run on real R2 at scale | L1 |
| **Many tables in one lake** | DuckLake (every commit re-read every table's statistics), a Hive metastore | 100,000 tables on a folder, one leader and one follower (`tools/bench/catalog_scale.py`, 11-09 on 83ddca55, `logs/catalog-scale/2026-11-09-83ddca55.json`): an INSERT reads 10 catalog records and writes 5, a CREATE TABLE 8 and 2, a SELECT 3, the same at 1,000 tables; lookups 0.11 ms p50 on the leader | the leader holds 1.4 GB at 100,000 tables (222 MB at 1,000); a follower starts in 12.9 s (0.23 s at 1,000) and looks up in 0.28 ms; after any write, the leader's next retention round reads and rewrites every table's entry, 5 to 9 s a round at 50,000 to 100,000 tables, so tables are made 68 a second past 50,000 (210 before); on a bucket (MinIO) | L1 |
| **Reads a bucket cold**: a new process, nothing cached | DuckDB 2.0.0 over files, DuckLake 1.1 | On real R2 (limits-r2's nightly `cold` job, TPC-H SF1's 22 queries, each engine started fresh, 11-02 on 13538a0: `reviews/cold-r2-13538a0.md`): the lake 4.33 s cold and 2.36 s warm, Pondra over the same Parquet files 14.09 s and 4.25 s, DuckDB 2.0.0 over them 15.38 s and 9.25 s, every Pondra answer the same. Each query from a new process on a simulated bucket (`logs/round36/cold-bucket-sf1-duckdb-ducklake.md`): the lake 56.4 s and 1,386 reads, DuckLake 1.1 239.9 s; warm, the lake's first pass 3.45 s to DuckLake's 60.4 s | Pondra over plain Parquet files on R2 cold is still slower on 5 of 22 (q1 2.05 s to DuckDB's 0.95, q3 2.92 to 1.16, q4 1.46 to 0.81, q17 0.62 to 0.42, q6 0.15 to 0.12); before #387 read them in 2 MB blocks, 16 at once, kept for later queries, it was 19.55 s to 11.86 s and slower on 18. The lake's own q1 cold on R2, 2.10 s to DuckDB's 0.95 on the files: a long range of a lake's file not yet on the node's disk is now fetched in 2 MB pieces side by side too; on a local bucket paced like R2 (`tools/bench/files_tpch.py --stores paced`), q1 cold 1.04 s to 0.75 s and q3 0.79 s to 0.53 s (the mean of two runs each), the 22 queries' total within the runs' noise; a partition still reads a file's row groups one after another. A first answer 1.66 s and 33 requests from a new process (DuckLake 0.34 s and none: its catalog is a file on the machine, not in the bucket); loading SF1 48.4 s and 141 writes (DuckLake 14.5 s and 35); 500 one-row INSERTs from one process with no node, 251 ms each at p50 and 708 at p99, about 3 requests each (DuckLake 12.7 ms with none: it keeps small writes in that local catalog) | L1 |
| **Cheap to run**: what a stream costs the bucket | Kafka, WarpStream, Buffer | On a simulated bucket with R2's latency (one 4-core VM, `logs/round34/cost-steady.json`): one node taking 13 MiB/s of 1 KB values through the Kafka port made 8.7 writes and 6.3 reads a second, $121 a month in requests at S3's list prices ($109 at R2's); three nodes at 16 MiB/s $247 ($222); one node at S3's latency, 26 MiB/s, $216 ($194). Requests come per commit round, not per byte. OpenData puts Buffer at about $91 a month at 30 MB/s, WarpStream $1,300 and self-hosted Kafka $8,500 | 30 MiB/s itself, on real R2 (`limits-r2.yml`'s Sunday cost job: this VM's cores went to the producers and the simulator); fewer writes a round (a segment per node and a catalog write per round); followers reading other nodes' segments from the commit stream instead of the bucket; bytes between the nodes, on Google Cloud | L1 |
| **Never loses data** | Kafka, Flink | kill -9 and injected crashes, every event once; failovers in both ack modes; every release's lake since 0.22 opens; 64 writers and 16 readers, 0 torn reads; the 24-hour soak's first leg on R2 (4.8 hours, 8 nodes stopped or killed): 43,138 batches, none lost or twice, 0 torn reads of 423,085 | the soak's other legs, logged, and its memory check (Lightweight) | L7 |
| **Right answers** | every engine | sqllogictest 98% of records not named an exception; TPC-H 22/22 on 1, 3 and 6 nodes; TPC-DS 99/99 == DuckDB on one node and three; 100,000 random queries, one node == three == DuckDB | — | holds (L6) |
| **Fast points and writes** | Postgres for serving | lookups 0.10 ms over HTTP, 0.31 ms over the Postgres port (Postgres 0.08) (`logs/gates/2026-10-13-postgres.txt`); a writer's 19,900 rows a second beside 400 readers, acks 5 ms (`logs/round34/users-writes.json`); another node sees a write 7 ms later; TPC-H SF1's statements repeating as a dashboard's do, on one 4-core node: 11,775 a second at 100 clients and 11,149 at 400 over HTTP, p99 under 100 ms, 7,983 at 100 over the Postgres port (`logs/round34/serve-vs-reyden.json`) | every statement new: 38 Q6 a second, one Q6 twice DuckDB's time; pgbench 141 transactions a second on one client and 221 on four (`logs/gates/`, 10-13; Postgres 997 and 1,793); a one-row INSERT 2.4 ms (Postgres 0.24) | L3 |
| **Fast over the wire**: answers and loads through the doors | Hardcharger 2.0.0 (ADBC) | narrow rows over HTTP within 1.1 to 1.3 times of Hardcharger (SF5, one 4-core box, `reviews/door-race-sf5-2026-10-26.md`) | fetching a big table 2.2 to 3.3 times slower, and loading it 1.6 to 4 times slower, plain and TLS (same run) | L3 |
| **Fits in** | the Postgres ecosystem | dbt == Postgres 16's rows; psql, SQLAlchemy, pgjdbc, psqlODBC, ADBC, Npgsql (`clients_check.py`); Spark and PyIceberg write | Metabase, Superset, Grafana, Tableau, DBeaver, Power BI checked; Postgres's and MySQL's changes streamed in through Debezium into the Kafka port (the owner, 10-24; attached natively in 1.1) | L3, L4, L5 |
| **Every feature through every door** (principle 10) | engines with one or two ways in | the doors matrix (`harness.py doors`): one list of features through SQL over HTTP, Python, the Postgres port, Flight SQL, JavaScript and MCP, each cell right or refused by name | JavaScript has no frames, `write_table` or `read_*` (35); a table with an enforced `UNIQUE` is refused by the loading doors (appends, Kafka, Flight, `COPY`) and Flight SQL and MCP hold no transaction (36); Kafka topics have one partition and no transactions (36); enum types are text in Postgres's catalog (36); the command line and the console in the matrix (37) | L2, L3, L8 |
| **Easy to change and extend** (principle 9) | closed platforms | one registry of every kind of object; the console's `register.*` and extensions; one check at every door | `INSTALL` / `LOAD` extensions (ADR-075, accepted; replaces ADR-031 and ADR-035 §6) | the owner's call |
| **Safe** | — | users and grants to a column, TLS everywhere, an audit log, quotas, every door fuzzed; single sign-on through an OIDC provider at every door (ADR-074, `harness.py sso`) | the security review from scratch (11-01 to 11-06); row filters and column masks after 1.0 | L7 |
| **Agents can rely on it** (the owner, 11-07): an agent gets right answers in few steps and few tokens, and can't do harm | DuckDB, Postgres; Snowflake and Databricks once there are accounts | the MCP door in the doors matrix; users, grants and quotas (Safe) | the agent benchmark, run like TPC-H: tasks of the Spider 2.0 and BIRD kind plus our own with writes, scored on success, steps, tokens and dollars; agent mode for `pondra sql` and the MCP tools (a compact table, errors as JSON, a row cap); checks that no call lifts a limit and that an agent's writes land on its own branch (the guardrails as a role's options, redesign step 6); the agent guide generated from the registry (step 12) and a small model given only it completing the benchmark's tasks | the redesign (the benchmark, steps 6 and 12) |
| **Runs anywhere** | — | Linux (glibc 2.17), macOS, Windows; pip, npm, installers, a container, Helm, `pondra service` | Homebrew, winget; an upgrade guide; signed binaries after 1.0, the release pipeline ready for the step (the owner, 10-31) | L7 |

## One month to 1.0

The owner, 2026-10-11: 1.0 in a month, finished and polished, built by many threads at once, each
led by one that designs, reviews and answers for its result. So rounds 35 to 37 run side by side as
**lanes**, one thread each, every lane owning its files (the list, with each lane's reserved numbers,
is `designs/one-month-to-1-0.md` in the project's files). Rounds 17 to 34 are released; round 34
(SQL as people write it, the proof runs) shipped as 0.34.0 on 10-14.

| Date | |
|---|---|
| Wednesdays (10-14, 10-21, 10-28) | 0.34.0, 0.35.0, 0.36.0: whatever is green on main |
| 10-31 to 11-03 | Features kept landing as they fit by rc.1, High fixes first (the owner, 10-31: the freeze waited for his call) |
| **11-03** | **Feature freeze** (the owner, 11-03 10:25 UTC). `release/0.80` was cut at main 20c2732 (his pick, 12:06). It took only fixes and rc.1 blockers, and each fix is ported to main. Main takes the redesign's steps in order (AGENTS.md, "Two branches") |
| 11-03 on | The review, once every feature works (above all one core with its forms and APIs: ADR-072), from scratch (the owner, 10-16): architecture, design, security and the code reviewed again, and simplified so the parts work together: modular, flexible, one reusable part in every place over copies, easy to add, swap or remove for maintainers and customers alike, still one small binary; every door tried as a new user would; the docs finished; the console's last fixes |
| 11-06 | 0.80.0-rc.1 was planned, then the 24-hour soak on R2, the cluster bench, the gates and the upgrade check. Its candidate passed the gates on 11-07 |
| 11-07 | **No release for now** (the owner, 11-07): rc.1 and 0.80.0 (planned for 11-11, and as 1.0.0 until 11-02) wait. The redesign comes first; a version follows once its architecture is done and proven |

| Lane | Thread | By the freeze |
|---|---|---|
| L1 | Engine, speed and proof (the main thread) | the Fluss head to head; TPC-H from memory at DuckDB's own tables; ClickBench's 100 M rows; Nexmark's q18; the bucket's limits on real R2; the cold-tier fixes and keyed tables' incremental steps; 1 → 3 → 6 machines once they exist; every release |
| L2 | In-process and in the browser | `pondra.open(…)` in Python and Node (B1, B2); the browser build against DuckDB-WASM (B4); JavaScript frames, `write_table` and `read_*`; idle memory and a cold first answer; then B5 |
| L3 | The Postgres port and the tools on it | pgbench scaling with clients and a one-row INSERT nearer Postgres; the BI tools checked (the SQLAlchemy dialect and the dbt adapter are built); Debezium's changes through the Kafka port, checked and written up; the doors' gaps |
| L4 | Sources and outside catalogs | `read_xml` (G10); Glue, S3 Tables and vended credentials; Kafka partitions; then sinks (G7) and MySQL |
| L5 | Semantic layer and AI | `pondra.parts` and `pondra.find` (done: #76); measures in views and key facts on views (E13 phase 1, done: #83; phases 2 and 3 after the freeze unless asked sooner); the ontology's core (E14); `CREATE MODEL` and the six `ai_*` functions (ADR-051 phase 1); then volumes, the `FILE` column, pictures and PDFs |
| L6 | SQL language | every new spelling reviewed; sqllogictest and the random queries held; refusals that say what to write; then the SQL 1.0 keeps stable |
| L7 | Durability, safety and shipping | CI's capacity; the soak's other legs; a lake whose only node died writable at once; the security review; signing, Homebrew and winget; an upgrade guide; what 1.0 keeps stable |
| L8 | Console and shell (from 10-18) | the console's backlog; the shell as a new user meets it; then views over what L3 to L5 landed, and the Ontology view |
| L9 | Docs that show everything | a map of every feature with examples; the site reorganized around it, each thing said once |

**How the lanes work.** A lane that finishes early goes straight on to its next step. Nothing ships
with a flaw, drawback or limit that would push people away (the owner, 2026-10-11): a review thread
keeps that list, each finding goes to the lane owning its files, and a limit kept is written where a
user meets it. On 10-31 every row not proven and every finding still open is listed for the owner.

**Every pull request:** its lane's files only (shared files take additions, not edits); `ci` while
iterating, and it merges once that passes (main builds the other platforms, and a red one there is
its lane's to fix at once); the gates (`tools/gates.py`) hold or improve; an engine
or read-path change keeps TPC-H's 22 and TPC-DS's 99 answers and is no slower; a new feature works
from every door or is refused by name; a one-page review; its ADR when it is a design.

## After 1.0

Depth by evidence, then the platform on top, each a part behind a small surface (principle 9):

- **F, streaming and flows:** timers, Top-N per key,
  as-of joins in views that wait for the looked-up table, a view's state split by key ranges across nodes, a range
  moved to another node in one commit with no state copied (the state is in the lake already; Arroyo's key-range
  rescaling is the reference);
  `avg`, `count(DISTINCT)`, outer joins kept up in flows, and views refreshed whole when they can't
  be (`WITH (refresh = …)`); a pattern view that resumes a search held open from its saved threads, so a run
  reads the new rows only, not the search's held rows again.
- **F, distributed and storage:** adaptive stages, raced stragglers, stage-level recovery (ADR-003: a long job writes
  each stage's output to object storage, so a node lost mid-shuffle costs one stage, not the job; Ballista's shuffle-file
  retry is the reference), a `LIMIT` in a subquery,
  order-keeping shuffles, hot keys on both sides, keyed compaction by key range.
- **F, sharing a cluster's time** (the owner, 2026-11-02, after 0.80.0; `reviews/yunikorn-2026-11-02.md`, from
  Apache YuniKorn):
  - A spread query is admitted with the cluster's load in view. Today a node's limit counts only its own queries,
    since remote stages take no turn, so 3 nodes that each admit 16 can all run 48 queries' stages at once. The
    membership will carry each node's running count. The coordinator spreads a query only to nodes that report room;
    otherwise it runs on fewer nodes or waits its turn. Stages still never wait, so no deadlock is possible.
  - Turns are fair by usage. One first-come line and a per-user `MAX_QUERIES` become shares per class (interactive,
    batch, agents), and the least-using user goes next. A quota on a role, which its members inherit, has a
    guaranteed share and a max. This is also the per-agent limit that agents depend on.
  - Background work (a view catching up, compaction, tiering) yields to queries at a batch boundary. It is never
    killed, since killed work is wasted.
  - Key-range views place their slots by load: each slot's owner is picked by its measured rows a second, moves are
    delayed and capped a minute, and a node that is leaving hands its slots away before it stops.
- **F, commits on a local folder** (`reviews/writes-vs-postgres-2026-11-02.md`; the flush of each write, added 10-31,
  stays): the folder's log appended to one file it keeps open and preallocated, flushed once a commit (`fdatasync`), as
  Postgres's WAL is, instead of a new file a commit (three changes to the folder and two flushes each); buckets keep one
  object a flush. The new file is also why a write slows as the lake grows: on disk a one-row write goes from about 3 ms
  to 4-5 ms over 16 K rows, because creating the file waits on the file system's journal behind the node's own
  background flushes (compaction, manifests, tiering, history), which an open file never does
  (`reviews/pgbench-statements-2026-11-03.md`, L3 11-03). Then group commit: a transaction's COMMIT, and an autocommitted UPDATE or DELETE, checks and queues its flush
  under the lake's lock and waits for it outside, so commits of different rows share one flush, as plain INSERTs share
  one in the sequencer already. A check reads only what is visible, so it also checks the commits still in flight: their
  keys, and their tables' UNIQUE values (ADR-057). pgbench at scale 1 gains nothing from it (every transaction updates
  the one branch row and holds its lock through its flush, as in Postgres): measured at a scale of one branch a client.
- **I1:** extensions (ADR-075, accepted; replaces ADR-031 and ADR-035 §6): one `INSTALL` line,
  WebAssembly components built once for every version and form. Before 1.0 (the owner, 10-31;
  `designs/extensions-before-1-0.md`): `INSTALL name FROM` a file or URL, scalar and table functions in
  Rust, sandboxed and kept in the lake, at every node form. After 1.0: the index and signing, aggregates,
  network and secret powers, deep hooks, the console API, whole tools, and extensions in the page.
- **J1:** the server's catalog (ADR-032 §9). **J3, J6:** reports and dashboards of Pondra's own,
  snappy at any scale. **J4:** database attaches (`designs/etl-and-bi-tools.md` §3.7): `ATTACH 'url' AS name (TYPE postgres,
  mysql or sqlserver, SECRET s, MAX_PARALLEL n)` makes a database read and written as ours are; a secret, if
  any, says who reaches it (one with `PROVIDER credential_chain` is the machine's own identity, chosen and
  granted, never a fallback). Connections (the owner, 10-31): one `CONNECTION` kind for every outside place,
  `postgres`, `mysql`, `sqlserver`, `s3`, `gcs`, `azure`, `sftp`, `http`, its secret by name and its paths
  `'@name/path'`; `ATTACH` is its one-line form for tables, over the same records 1.0's attaches keep. Drivers
  (proposed 10-31): each database type on an ADBC driver through one driver manager, each driver an ADR-075 package
  fetched on first use and run in a helper process, with Pondra still pushing work down as the database's own SQL;
  rows per second against each database's native client decide it before it is chosen. 1.0
  refuses `CREATE CONNECTION` showing the `ATTACH` and the `CREATE SECRET … SCOPE` to write. Row filters
  and column masks come before the dashboards (J6), whose tiles run as their viewer (§4.9). **J5, J7:** a workspace exported whole, and ETL as code, a canvas and YAML
  over one definition, as plugins.
- **Iceberg engine extras and Delta tables, 1.1** (the owner, 2026-11-09; `reviews/iceberg-engine-compat-2026-11-09.md` I8, I9): on
  another catalog's tables, branches and tags with write-audit-publish; incremental reads between snapshots, which feed
  streams, live views and the change feed; Puffin statistics read for planning and written; Iceberg views; v3 types
  (variant, geometry, nanosecond timestamps, defaults, row lineage) and v3 writes; REST scan planning as a client.
  Each is an ability of the `iceberg` table-format entry, shared with our own format where the meaning is the same.
  And Delta tables in our own catalog (`CREATE TABLE … USING delta`) on the same table-format contract, with the same
  checklist as Iceberg's 1.0 row, checked against Spark and delta-rs.
- **E12:** sharing with other companies (ADR-046, proposed). **E13**'s rollups, before J6.
- **Governance** (the owner, 2026-10-13, after single sign-on by the freeze; `reviews/platform-comparison.md`'s
  governance row): row filters and column masks, as Databricks' Unity Catalog and Snowflake's Horizon have them.
- **From the OLAP engines** (`designs/realtime-olap-comparison.md`): a query over a table answered from a
  materialized view that covers it, without naming it; HLL and quantiles as merge states for merge tables
  and adding-up views; `VARIANT` as a real type with its paths as columns (Parquet's shredding) and a
  text index.
- ADR-051 phase 2 right after 1.0: vector and text indexes (`CREATE INDEX … USING hnsw | bm25`, `search()`), `VARIANT` stored shredded, `MAP`; trained models in SQL (E15); a managed service.
- **Agents as a door** (`reviews/linkedin-posts-2026-10-30.md` A6; the owner, 2026-10-31: after 1.0, brought into
  1.0 on 11-07 with the scorecard's agent row): an agent
  mode for `pondra sql` and the MCP tools, as DuckDB 2.0's command line has one: a compact table, errors as JSON, a
  row cap, and an estimate before a long query from ADR-050's plans. Then (the owner, 2026-11-02, from
  `reviews/pgx-2026-11-02.md` A3; the AI lane, after the redesign): `pondra agent exec -- <cmd>` and an Agent Skill or
  Claude Code plugin from a small repo of its own: each session and subagent gets its own branch, its URL set,
  extended while it is active.
- **Branches that expire** (the same review, A1; L1 with L7, after 0.80): branches stay permanent by default; `ttl`
  or `idle_ttl` is an option a branch is given and never inherits, and an agent's branch goes retired, then a grace
  period, then dropped. The sweeper touches only branches with a TTL record. With it, one measurement (A8): an idle
  branch's and an idle connection's memory, published as one number each.

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
| C5 | Two more engines beside ours on the same hardware, after the freeze (the owner, 2026-11-02): open-source ClickHouse on ClickBench's 43 queries, with DuckDB's and ours; Arroyo on Nexmark's 20, with Flink's. ClickHouse Cloud, with the cost row in dollars, in 1.1 | L1 |
| D2 | Random queries: one node == three == DuckDB, 100,000 of them (done: `random_sql.py`) | done (L6 keeps it) |
| E2 | BI tools checked (Power BI Desktop, Tableau, DBeaver, Metabase, Superset, Grafana) | L3 |
| E12 | Sharing (ADR-046, proposed) | after 1.0 |
| E13 | Semantic models as measures in views (approved 2026-10-03; `designs/semantic-models-and-dremio.md`): `sum(x) AS MEASURE m` in a `CREATE VIEW`, queried as `MEASURE(m)`, right at any grain; BI tools' `sum(m)` gives the measure (phase 1, measures in views, and key facts on views: done, #83); MCP `list_metrics`; Apache Ossie YAML out and in. Then rollups kept in their source's commit and read where they give the same answer; then rollups proposed from `pondra.history`. Proof: `harness.py measures` | L5 (then after 1.0, before J6) |
| E14 | An ontology as a schema (approved 2026-10-10; `designs/ontology-as-a-schema.md`): an ontology is a schema whose tables and views are object types, whose `FOREIGN KEY … NOT ENFORCED` facts are links, whose measures (E13) are metrics and whose procedures are actions. Nothing is stored apart. New: key facts on views and materialized views (`ALTER VIEW v ADD CONSTRAINT … NOT ENFORCED`) and `COMMENT ON COLUMN` on their columns (done in round 34); `pondra.ontology('s')`; `pondra.related(object, key)`; `pondra.suggest_ontology(sources, into => …)`, a deterministic read that returns statements with evidence (keys, links, one thing in several systems as a view whose match is SQL, measures, tests); the clients' verbs and MCP's `ontology`, `suggest_ontology` and `related`. Then the console's Ontology view (suggest, a picture, Looks right, Home from the measures, an Explorer from `related`). Proof: `harness.py ontology` | L5 (core), L8 (console) |
| E15 | Machine learning models trained in SQL (approved 2026-10-10; `designs/models-and-names.md`). ADR-051's `CREATE MODEL` gains `AS SELECT …` with a trainer (`USING 'sklearn/GradientBoostingClassifier'`, the library's own class, or `FUNCTION f`; `WITH (target = …)`; a registry of trainers), trained on a Python worker as a run. Each version keeps its object, the commit it read, its metrics and who made it. `predict('m', …)` and `predict_proba('m', …)` are vectorized functions spread with the query, alongside `pondra.evaluate('m' [, (query)])`, `pondra.model_versions('m')`, `ALTER MODEL m ADD VERSION` and `SET (default_version = n)`. Features are views kept current; `ASOF JOIN` builds training sets as of each label's time; keyed lookups serve features online. Proof: `harness.py models` | right after 1.0 (with ADR-051 phase 2) |
| F | Depth by evidence (above) | after 1.0 |
| G6 | Databases attached natively with their changes streamed in (1.0 takes them through Debezium into the Kafka port: L3) | 1.1 |
| G7 | Sinks | L4 |
| G10 | XML files as tables: `read_xml('…')`, one more `read_*` format beside `read_json`, through every door that reads files | L4 |
| I1 | Extensions (ADR-075, accepted; replaces ADR-031 and ADR-035 §6) | from 1.1 |
| J1, J3–J7 | The platform on top (above) | after 1.0 |

## What only the owner can decide

1. **Machines, by 10-24:** six 8-vCPU VMs and a client in one zone for a few hours at a time
   (`tools/cloud/gcp.sh up 6 --local-ssd`; the Google Cloud trial caps at 8 CPUs and the run needs
   52). Without them "Scales out" is proven on GitHub's runners over the internet only.
2. **Publishing, by 11-01:** Homebrew, winget and signing wait on the owner's accounts and
   certificates (ADR-041 §9); without them they follow 0.80 in 0.80.1.
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
