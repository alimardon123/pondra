# Prototype status: Pondra, a streamhouse in one binary

**Date:** 2026-10-01 (the workspace, rounds 27 and 28, round 29 (the owner's lists, users, grants, secrets, TLS, audit, quotas, files with versions, C5), round 30: pipelines, transactions, error codes, the point path; round 31: correct SQL, every kind of object alike, SQL and Python in one notebook) · **Plan:** ADR-002 to ADR-035, `roadmap.md` · **Code:** `pondra.zip` / `pondra.bundle` (≈30,700 lines of Rust, plus Python and JavaScript clients, a documentation website, packaging, and test and benchmark tools)
**Name:** the prototype formerly called `lh` is now **Pondra**. The name is free on crates.io, PyPI and npm. A small personal-finance app uses it (pondra.app), a different category; run a trademark search before a public launch.

## Where it stands

One Rust binary replaces the Kafka + Flink + Spark + metastore + ZooKeeper stack for the common jobs:

- stream ingest, exactly-once;
- streaming SQL: views with no lag, and stateful tasks;
- push to clients;
- batch ELT and SQL, distributed across nodes;
- upsert and merge tables.

Start more copies on the same bucket to scale out. The only state is object storage. There's no JVM, no database server and no coordination service.

**Round 33 complete (2026-10-10): 0.33.0.** Each database read and written under its own sign-in;
branches (`CREATE DATABASE dev CLONE prod`, zero copy, `REFRESH`) and projects planned, deployed and
tested (`pondra plan`, `pondra deploy`); sharing with other companies over Delta Sharing; every
materialized view kept current whatever its query (partial rows finished as read, by key, or run
whole); windows as SQL with `EMIT FINAL`; `INSERT … SELECT` written by every node; plans that learn
(`EXPLAIN`'s expected rows, `pondra.learned`); vector search as fast as DuckDB's; task graphs; every
statement in `pondra.history`. The 24-hour soak's first leg on R2 (4.8 hours, eight nodes stopped or
killed): 43,138 batches each once, 0 torn reads of 423,085; a node's memory grew 2–5 MB a minute
under steady writes, which round 35 takes. The gates found three bugs before the tag, all fixed: a
`WITH RECURSIVE` whose friendly probe asked itself until the node died; a table with an `INTERVAL`
column that never left the log and held every other table's tiering with it (invariants 257–258);
and `EXPLAIN (ANALYZE, FORMAT PGJSON)` refused. They also found pgbench a sixth slower at four clients
than 0.32.0's, measured side by side: every statement was read again by each kind of the registry, and
went to the queries' runtime whenever another was running. Both fixed (invariants 231, 232): 186–204
and 160–167 transactions a second against 0.32.0's 187–210 and 165–175, and a writer's acks beside 64
querying clients still 8 ms. Gates: `logs/gates/README.md`, 2026-10-10. What a user
sees is in `.github/release.md`; each change's checks in its pull request.

**Begun (2026-10-03, round 34): SQL as people write it.** DuckDB's spellings, rewritten where SQL
comes in (`friendly.rs`, invariant 225): `PIVOT` and `UNPIVOT` (DuckDB's statements and the
standard's), `COLUMNS(…)`, `* RENAME`, `ORDER BY ALL`, `FETCH FIRST`, list comprehensions and
lambdas, `({…}).a`, `max_by`/`arg_max`/`min_by`/`arg_min`, `list()`, `string_split`, `::JSON` and
`json_extract`, DuckDB's `ASOF [LEFT] JOIN … ON`, a select's alias in its `WHERE`, `SUMMARIZE`,
`USING SAMPLE`, and a `TABLESAMPLE` that samples (DataFusion ignored it and returned every row).
`harness.py friendly`: 30 forms answer as DuckDB 1.5.5 does over the same 2,000 rows, and the same
spread over three nodes, over Postgres and in views. 70 of the 73 everyday features probed on
2026-10-03 now work (`COMMENT ON` came with the statement registry, invariant 227). On the
registry since: sequences and identity columns (`harness.py sequences`: three nodes taking values at
once, a leader killed, every value once), `CREATE INDEX` kept as an object (nothing built), and
`UNIQUE` checked by the leader for every SQL write (`harness.py constraints`: three nodes inserting
the same 40 values at once, 40 go in, 80 refused with 23505), with `NOT ENFORCED` keys kept as
facts; and enum types, `CREATE TYPE … AS ENUM` and `ENUM('a', 'b')` columns, held as text with their
labels checked at every door (`harness.py enums`: 8 of 8).

**100,000 random queries** (`tools/random_sql.py --seed 3434`, D2): each against DuckDB 1.5.5 on
one node, every tenth spread over three, 42,826 split three ways by a condition. The first run
found two wrong answers, both DataFusion 55's: two IN lists of one column intersected as sets of
values (`x IN (x, '') AND x IN ('abc', 'a')` became false), and `x NOT IN (NULL)` dropped beside
another list; and three refusals: `- -3` written back as a comment, a DISTINCT over a CASE failing
DataFusion's schema check, and a projection pushed through a filter with the wrong columns (it
could have swapped two columns of one name and type). All five are fixed (invariant 226,
`harness.py friendly`). Run again: 99,974 agree, no wrong answer, every spread answer the same as
one node's; the 26 left are refusals DataFusion makes and DuckDB doesn't (`NULL || NULL`, as
Postgres, and two decimal types too wide) (`logs/round34/random-100k.json`). **TPC-DS on three
nodes**: 99 of 99 the same as one node, all spread (q66 and q75 had fallen back over an
aggregate's `ordering_mode`: invariant 34).

**Big writes on every node** (invariant 228): an `INSERT … SELECT` or `CREATE TABLE AS` whose rows
split as they are is written by every node from its own share, recorded in one commit, row ids
unique, a retried job writing nothing. Three nodes sharing one 4-core box: a copy of TPC-H SF1's
lineitem (6,001,215 rows) 5.9 s on one node, 3.6 s on three; a filtered copy (3,426,687 rows) 2.8 s
against 1.5 s (`logs/round34/insert-spread.json`). Building it found two read bugs on spread
queries, both fixed: a query naming `_row_id` fell back to one node, and a history view (SCD type
2) spread over the nodes counted its deleted versions (98,970 rows read as 100,000), since round
33.

**Dashboards while writes land** (`serve_bench.py --users`, invariants 229–230): five dashboard
statements from Go clients against one node on 4 cores, while an append table takes 1,000 rows and a
keyed one 100 upserts every 50 ms. Each query had conformed every log segment again and run a batch
a segment, a streamed table's merges rewrote it every minute or so (sending reads back to cold
Parquet), and each scan planned a small query of its own. Now a table's log tail is kept between
queries and extended by new segments, small files merge with files of their size, and a table that
never changed is planned straight from its tail and files. Queries a second: 760 → 870 at 50
clients, 826 → 956 at 100, 749 → 934 at 200, 751 → 861 at 400; p99 at 200 clients 1.54 → 0.83 s
(`logs/round34/users.json`). TPC-H and TPC-DS from files and from memory are no slower. What it also
showed: the writer, a request at a time, landed 9,770 rows a second beside 50 readers and 2,860
beside 400, its acks waiting behind the queries' tasks on the one runtime. Now a request's queries
run on a runtime of their own once another is running (invariant 231), and a door boxes a
statement's future before wrapping it (232): the writer lands 19,800–19,900 rows a second at every
count of readers, acks 5–6 ms at p50 (80–240 ms before), and the readers keep 760–860 queries a
second (`logs/round34/users-writes.json`). A point lookup takes 0.16 ms over Postgres (0.35) and
0.19 ms over HTTP (0.31); pgbench's one client 210 transactions a second (150).

**ClickHouse beside DuckDB** (`singlenode.py`, ClickHouse 26.9.9.28 as `clickhouse local`, 4 cores,
best of three; `logs/round34/singlenode-*-clickhouse.json`). TPC-H SF1: DuckDB 1.5.5's tables
0.97 s, Pondra from memory 1.20 s, DuckDB over Parquet 2.02 s, Pondra from files 2.17 s, ClickHouse's
MergeTree tables 2.56 s, ClickHouse over Parquet 4.98 s. ClickBench's first 10 million rows:
ClickHouse's MergeTree 4.63 s, DuckDB's tables 6.69 s, Pondra from memory 6.73 s, ClickHouse over
Parquet 7.89 s, Pondra from files 9.86 s, DuckDB over Parquet 10.71 s. Where ClickHouse's tables are
far ahead: q29's `REGEXP_REPLACE` (0.46 s against 1.29 s), q28 (0.04 s, its `length` counting bytes
and its table sorted by the grouped key), q24's wide top-N (0.20 s against 0.50 s), q23, q19. Its
answers that differ are its own: `0.06 - 0.01` a float (TPC-H q6), sums of doubles in any order
(q15, now and then), `length` in bytes (q28, q29), `avg` of a BIGINT wrapping (q4).

**A small build looked up in an array** (`optimize::config`). A join on one integer key whose build
side's keys span under 262,144 values now looks them up in an array of that span (at most 1 MB),
however few they are: DataFusion's perfect hash join, which by default takes only a span under 1,024
or keys at least 15% dense. TPC-H q17's 204 parts, spread over 200,000 part keys, had stayed in a hash
table, and its 6 million probes took 2.4 times as long. Measured as `SET` in the same request,
interleaved, best of 10–30 rounds (`logs/round34/perfect-hash-join-ab.json`): TPC-H SF1 from memory
1.142 s → 1.038 s, from files 2.088 s → 1.962 s (q17 58 → 24 ms and 101 → 70 ms), TPC-DS SF1 11.06 s
and 11.17 s (no query apart beyond this box's noise at 15 rounds). With it, the single-node bench
(`logs/round34/singlenode-tpch-sf1-perfect-hash.json`): Pondra from memory 1.16 s against DuckDB's
tables 0.97 s (the last gate's 1.26 s), from files 2.00 s against DuckDB over Parquet 2.04 s; 22 of 22
answers equal. TPC-DS 99 of 99 equal to DuckDB's from files and from memory, `join_order.py`,
`spread_tpch.py --expect 22`, `harness.py scale`, `hot` and `minmax` pass.

**Keys a query implies** (`optimize::implied`, `Equal`). The join order sees what a query's keys say
together: two columns of one type that each equal a third are equal. TPC-H q5's customers and
suppliers each name a nation, so customers may join the nation at once, and the order starts from the
region (Asia) instead of a year of orders. A join takes only the keys those it already has don't
say (`Equal::said`): given q17's implied key beside its own, its last join hashed two columns and lost
the array lookup above, twice as slow (`join_order.py`: "TPC-H q17: no join takes a key the others
say"). Two nodes on copies of one lake, interleaved, both ways round, best of 20
(`logs/round34/implied-keys-ab.json`): TPC-H q5 0.74×, TPC-DS q64 0.76× and q72 0.82×, the rest
within this box's noise. TPC-H SF1 from memory 1.07 s against DuckDB's tables 1.03 s, from files
1.94 s against DuckDB over Parquet 2.04 s, 22 of 22 equal
(`logs/round34/singlenode-tpch-sf1-implied-keys.json`); TPC-DS 99 of 99 equal to DuckDB's from
files and from memory; `join_order.py`, `spread_tpch.py --expect 22`, `harness.py scale` and 10,000
random queries (the same known differences, no new one) pass.

**What runs learn, measured** (the owner asked, 2026-10-10, whether the planner's use of them pays).
Planned with them, TPC-H SF1 changed one plan (q17's) and TPC-DS SF1 none, and no query ran faster.
They cost nothing measurable either: a `SELECT 1` 1.43 ms against 1.42, a three-way join 11.6 ms
against 11.7, and a refresh's read of the history 4 ms every 10 s while queries are planned
(`logs/round34/learned-facts.json`). So, as the owner's rule says of what doesn't pay its way, the
planner uses them only with `PONDRA_LEARN=on`; every run still learns them and `pondra.learned`
lists them, for people to read and for the planner where a filter's columns go together (the case
`harness.py learn` builds).

**On GitHub's runners** (`.github/workflows/singlenode-bench.yml`): TPC-H SF10 and all 100 M rows
of ClickBench, Pondra from memory and from files against DuckDB over Parquet and in its own tables,
every answer checked; weekly on main, by hand, and on a pull request labelled `bench`. Its first
run (before implied keys, `logs/round34/singlenode-tpch-sf10-github.json`): TPC-H SF10 from memory
12.95 s against DuckDB's tables 15.28 s, from files 19.50 s against DuckDB over Parquet 23.70 s, 22
of 22 equal; furthest behind DuckDB's tables, q18 (1.69 s against 1.16), q20 and q12.

**Now (2026-10-03, round 33, toward 0.33.0): run it for years.**

1. **Every statement remembered** (ADR-048, `history.rs`): `SELECT * FROM pondra.history` has a row
   per statement from every door (who, which door and address, node, session, class, the
   statement, how it ended, ms, rows, nodes), written a second later off the statement's path, at
   most 500 rows a second a node (the rest counted). A statement over `PONDRA_SLOW_MS` (1 s) keeps
   the plan that ran with each operator's rows and time, and its trace: each node's share when it
   ran across three nodes, a step at a time for a shuffle; it writes a line to the node's log too.
   An admin reads every row, a user their own. Point lookups over Postgres: 0.414 ms off, 0.418 ms
   on (three runs each). Writing it found that any commit, history's every second included, made
   every remembered answer stale: history's commits are quiet now (invariant 224), and a repeated
   query on a lake only read stays at 0.30 ms instead of 3.6 ms. `harness.py history`: 9 of 9.
2. **Scripts and tasks** (ADR-045, the console's thread, PRs #17 and #19): `FOR … PARALLEL n`,
   `ASYNC`, `AWAIT ALL`, `AWAIT $h` and `AWAIT 'id'`; `VALUES` with a subquery; task graphs
   (`AFTER`, `WHEN`, `pondra.result`, retries, timeouts, `on_failure`, `EXECUTE TASK`, `SUSPEND`).

**Then (2026-10-02, round 32, complete: 0.31.0, 0.31.1 and 0.32.0): lean and fast** (the owner, 2026-10-01: a round only for
optimizing; performance first):

1. **The join order from every input in turn** (`optimize::JoinOrder`): the greedy order is built
   from each input, the cheapest taken, where it started from the smallest alone. TPC-DS q72 began
   at its 5 warehouses, then joined all of the inventory: 81 s, now 0.17 s (DuckDB 0.31 s). A join
   on several keys counts the one that spreads its rows most, not their product (TPC-H q9 would
   otherwise start with partsupp joined to all of lineitem, as if 2,400 rows came out: 6 million
   do). `tools/join_order.py` has q72's shape ("far from the filters": its plan must start from a
   filtered table; the round 31 build starts from nation): the badly written queries 1.22 → 1.01 s,
   the worst 1.72× its well written twin, now 1.15×. TPC-H SF1 from memory 1.38 → 1.29 s on this
   machine; TPC-DS's 99 still equal DuckDB's (`logs/round32/`).
2. **Planning that costs less:** orders are costed over which inputs each key reads (a bit an
   input), not over schemas built at every step, and a table's statistics are worked out once a
   query (its sketches' estimates were made again for every input costed). TPC-DS q64 plans in
   0.16 s, 0.22 s before (0.71 s with the first try at the order above).
3. **Small queries cost half what they did** (invariant 199): a query's session is a copy of one
   made once per lake (its functions, planners and rules: registering them again was a third of
   `SELECT 1`), with catalogs of its own; only the table entries a query names are decoded (every
   table's was, about half of a small query's time), stored views are expanded only when named,
   and the cores are counted once. Over HTTP, on TPC-DS's lake: `SELECT 1` 1.64 → 0.86 ms, a count
   of a small table 3.2 → 2.2 ms, a three-table join that finds nothing 10.6 → 8.7 ms.
4. **Flows within 5% of ingest** (invariant 200; the bar was 10%): a view that reads a write's new
   rows alone keeps its physical plan from one write to the next, and each write puts its rows in
   it (planning was 60% of what a view cost a write: a GROUP BY of 4,000 rows, 0.9 ms planning and
   0.4 ms running), one partition for a write's rows. `tools/bench/flow.py`, 4 producers of 1,000
   rows: three stages cost 15.4% of ingest before, 5.0% now (191K rows/s against 201K with no
   view); the node's CPU for the three 130% → 82% (53% with none); `logs/round32/flow.json`.
5. **CI the same every push:** `harness.py functions`' killed-worker check waits for the workers
   it killed to be gone (the next query could take one the node still saw running); fuzzed nodes
   run in their test folder (`ATTACH 'nope'` had left two lakes in the repository).
6. **Joins the order rule couldn't see** (invariant 201): under an `EXISTS` or `IN`, comma joins'
   equalities were still conditions when the rule looked (keys only a pass later, with projections
   between the joins by then), so TPC-H q21 joined all of lineitem to its suppliers before its one
   nation cut them: from memory 0.18 → 0.09 s, from files 0.23 → 0.19 s; q17 0.065 → 0.047 s. And
   an inner join that reads nothing of a `LEFT JOIN`'s padded side now runs before it (TPC-DS's
   sales `LEFT JOIN` returns, then dates and items): q80 3.4 → 0.08 s (DuckDB 0.07 s), q40 0.45 →
   0.03 s (0.02 s); TPC-DS's 99 18.2 → 15.2 s (DuckDB 7.6 s), all the same as DuckDB's.
   `tools/join_order.py` has both shapes, their plans checked.
7. **Row estimates the order rule can trust** (`optimize::kept`, `joined`, `SemiJoinDown`): a
   filter keeps its share by the column's distinct values and its span (a month of 200 years of
   dates is a month, not a third), a join keeps only the key values both sides have, and a
   subquery's semi join goes onto the smaller side before the order is chosen. 52 of TPC-DS's 99
   plans change, none slower (each re-timed both ways, alternating): q98 0.056 → 0.036 s, q72
   0.163 → 0.119 s, q50 0.076 → 0.057 s, q62, q58, q61 about a fifth faster; the 99 in one run
   15.7 → 13.0 s against DuckDB's 7.8–8.1 s, measured side by side. TPC-H's 22 plans don't change.
   q72 first went 10× slower: inventory joined to every date kept all 10,436 weeks, so a later
   join on the week looked like a cut (unit tests in `optimize.rs`).
8. **The cluster bench on 0.30 (932cbad, the owner's runs #15 and #16, SF10 on GitHub's 4-vCPU
   runners over Tailscale):** every answer the same as one node's; 3 nodes 22.4 s as the cluster
   decides against 23.8 s on one node (34.5 s spread anyway), 6 nodes 24.2 s against 26.2 s (33.3
   s). Two things kept time on the table. One slow first spread run (the other nodes' caches cold)
   decided for good: q1 spread in 2.1 s the first time and 1.0 s after, yet went back to 2.4 s on
   one node; now a second run must agree (invariant 72). And the guard's 20 round trips a step
   keep queries that move nothing on one node (6 nodes: q12 0.50 s spread against 0.98 s, q19
   0.68 s against 1.07 s, q3 0.67 s against 0.95 s); its next runs log each decision
   (`PONDRA_DEBUG_SPREAD`, the nodes' logs kept), to set that from what the steps take.
9. **Answers carry only their own strings** (invariant 202): a string read from a file is a view
   into a buffer the whole page shares, and Arrow IPC sends every buffer a view points into, so a
   few rows took their pages with them. An answer, every IPC stream, Flight's streams and a
   shuffle's pieces now copy out views whose buffers are mostly other rows'. A `LIMIT 5` of a
   50,000-row table was 3.2 MB as Arrow, now under 16 KB; ClickBench's 10-row answers were up to
   220 MB, too big for the result cache.
10. **Hot batches skipped by their ranges, a top-N read in its key's order** (invariant 203,
   `hot::HotSource`, `TopFirst`): each 8,192-row batch of a hot column of an ordered type keeps its
   least and greatest value, NULLs and rows, and a scan skips the batches its filters rule out, as a
   Parquet scan skips row groups. A top-N's bound skips them too, and it reads the batches in its
   first key's order, so the bound is tight after the first few. The filters stay above the scan,
   so every row is still checked. With every column hot and the build before alternated, 10 runs
   each: ClickBench's two top-N by time (q25, q27) 0.029 → 0.007 s and 0.027 → 0.006 s, where
   DuckDB's own tables take 0.007 and 0.006 s; the 43 about 3% faster in all; TPC-H and TPC-DS
   from memory the same within noise (the queries one pass showed slower, re-timed alone: TPC-DS
   q66, q78, q93, TPC-H q19, q12, q7; q7 with skipping on and off in one build, 30 runs each six
   times over, 0.051 against 0.050 s, not a significant difference). Looking costs about 2 µs a
   batch when nothing can be skipped: 0.3 ms per partition of `lineitem` at SF1, before its first
   batch. The first version looked at a moving bound again after every
   batch, which made those two top-N slower than before; it now looks soon at first and then ever
   later. `harness.py hot` checks the answers against a model (NULLs, `IS DISTINCT FROM`, `IN`, a
   top-N either way, through a filter) and that batches were skipped.
11. **ClickBench, measured** (`tools/bench/singlenode.py --suite clickbench`: the first 10 million
   rows of `hits`, 43 queries, best of 3, this 4-core, 15 GB machine; `logs/round32/`): Pondra
   from memory 8.22 s, from files 12.25 s; DuckDB 1.5.5 over the Parquet file 11.11 s, in its own
   tables 7.18 s; DuckDB 2.0's preview 10.89 s and 6.22 s. The 7 answers that differ from DuckDB's
   are ties a `LIMIT` cuts through (`tools/bench/clickbench_ties.py`: the same row count, every row
   a real group, the sort key's values the same). Pondra is ahead on the regular expression (q29:
   1.49 s against 3.12 s) and the `LIKE` counts (q21, q22); DuckDB's own tables are far ahead where
   a filter keeps few rows of many columns: q24 (`SELECT *` of a top-N, 1.27 s against 0.11 s:
   DuckDB fetches the other columns only for the rows it keeps), q23, and the pages of one counter
   (q37 to q43, 3 to 5×). Those are next.
12. **TPC-H SF1, the same day** (`logs/round32/tpch-sf1*.json`): Pondra from memory 1.22 s, from
   files 2.18 s; DuckDB 1.5.5 over Parquet 2.19 s, its own tables 1.05 s; DuckDB 2.0's preview
   1.98 s and 0.92 s; Polars 1.44 1.92 s. Every answer equal to DuckDB's.
13. **A wrong answer fixed (0.31.1; invariant 204):** a global min and max of two things,
   `SELECT min(a), max(b + 1)`, came back too low when the table's rows were in several files, and
   `min(a), max(c)` NULL when `c` was NULL in the files read first. DataFusion's filter for a global
   min/max, which the scans skip row groups (and hot batches) by, leaves out a max of an expression
   and one with no value yet. Found by reviewing the next change to the hot scan; 0.30 had it too.
   Where a bound can be left out, the aggregate now keeps the filter to itself.
   `harness.py minmax` checks seven shapes against a model, three times each.
14. **A top-N of many columns decodes its Parquet files filtering as it goes**
   (`optimize::WideTopN`): under a `LIMIT`ed sort, through projections, filters and exchanges, a
   scan of 16 columns or more reads the filters' and the sort key's columns first and the others
   only for the rows they keep (DataFusion's own row filter; the filters still run above). ClickBench
   from files 11.39 → 10.17 s, q24 (`SELECT *` of a top-N) 2.26 → 0.80 s; TPC-H from files 2.20 →
   2.15 s, no plan of it changed; every query that looked slower re-timed alone, both ways. Turned
   on for every scan instead, the same row filter made q24 0.57 s but TPC-H from files a third
   slower (q6 0.06 → 0.20 s), so only this shape gets it. Two other ideas were timed on and off in one
   build and dropped: dropping rows at the in-memory scan by a top-N's bound (ClickBench's top-N by
   time 0.006 → 0.04 s) and copying the in-memory columns into buffers of their own (TPC-H q12 from
   memory 0.05 → 0.11 s), until it kept shared strings shared (item 16).
15. **A transaction's UPDATE then INSERT of one table commits:** COMMIT sent the table's rows as one
   Arrow stream under its first batch's schema, and an UPDATE's rows carry `_created_at` where an
   INSERT's don't, so the leader couldn't read it (found by the console thread). `harness.py begin`
   checks an append and a keyed table.
16. **The in-memory columns hold what they count** (`hot::whole`): once a file's column is decoded,
   its batches are copied into one allocation of their own, and a buffer several batches share (the
   Parquet page their strings point into, a dictionary's values) is copied once and stays shared.
   Left in the decoder's buffers, the columns sat among its short-lived ones in the allocator's
   pages and the process held about twice what they count, so the node trimmed them early. Now
   ClickBench's columns hold 7.2 GB at 7.9 GB of the process's memory (about 5 GB before, at the
   same limit). Side by side, from memory: ClickBench 7.66 → 6.86 s (q23 0.95 → 0.44 s), TPC-H
   1.006 → 1.004 s; every query that looked slower re-timed alone, both ways. In `singlenode.py`'s
   runs ClickBench from memory is 6.58 s (8.22 s in item 11; DuckDB 1.5.5 over the file 11.10 s) and
   TPC-H 1.24 s, every answer as DuckDB's but the same 7 ties (`logs/round32/*-arena.json`); TPC-DS
   99 of 99. The first try copied each batch's strings out, which made a join on a low-cardinality
   string column carry and count a copy per batch: TPC-H q12's build 235 MB instead of 68.
17. **Key lookups while writes land, 16× more a second** (`serve::Tail`): a lookup looked for its key
   in every log segment since the table's last tiering round, newest first, and a table taking 250
   small commits a second had thousands of them; now each table's log tail is indexed by the hash
   of its key, kept per node until the next round and brought up to date with only the segments
   committed since (the row a hash names is checked; another key of the same hash scans as before).
   `serve_bench.py`, 8 clients while a writer upserts 200 rows per commit: 1,676 → 27,600 lookups a
   second, p50 4.6 → 0.22 ms; without writes 38,900 both ways. The same dashboard aggregate while
   writing looked slower after it (300 → 180 a second), because the writer, no longer starved by
   the lookups before it, had written twice as many rows by then: run without the lookups first,
   both builds give 175.
18. **The gates on 0.32.0** (`logs/gates/`, 2026-10-03): TPC-H SF1 from memory 1.14 s and from
   files 2.24 s (DuckDB 1.07 / 2.12 s), every answer right; Nexmark 2 M bids 2.23 s; pgbench 198 and
   168 transactions a second, balances right; sqllogictest 23,049 of 24,783 as before, once two
   `CREATE DATABASE` records stopped meeting folders an earlier run left in `/tmp` (`slt_check.py`
   now gives each file's lake a folder of its own).

**Beside it, in the same release (2026-10-02, round 33's first parts):**

- **Run it for years** (ADR-039, PR #6): lakes have a format version, refused by a build that
  doesn't know it; every release's lake since 0.22 (8 of them) opens and answers row for row as
  before, then takes writes, tiering, merges and a kill (`upgrade_check.py`, `upgrade.yml` on every
  PR); a 0.30.0 cluster under load upgraded a node at a time, both orders, every batch once; a node
  stopped with SIGTERM drains (`/ready` 503, Postgres 57P01, requests in flight finish) and a leader
  steps down durable, so writers wait about 5 s for the next one (13 s when it is killed); a key
  lookup that could miss a live key (0.22 to 0.30) fixed; `tools/soak.py` for the 24-hour soak.
- **Deployed and distributed** (ADR-041, PR #5): a container image (amd64, arm64, and one with
  Python), a node sized to its container's memory, a three-node compose cluster, a Helm chart,
  `pondra service install` (systemd, launchd, Windows), Homebrew, Scoop and winget manifests made
  from each release, signing ready for certificates, provenance attestations; each tried on every
  PR with the build's own binary. Publishing waits on the owner's accounts.
- **Every mode under failure** (PR #10, `tools/resilience_check.py`, 75 checks on every PR): a
  failing bucket (errors, lost replies, held requests, down 40 s), a leader cut off alone, clients
  and doors through kills, full disks, `pondra sql` killed; every acknowledged write there once on
  every node, no read torn. It found five faults, all fixed: an idempotent Kafka producer that gave
  up on a batch lost acknowledged records (12,000 of 50,800 across a leader kill: each epoch is now
  a producer of its own); a leader cut off from its bucket held every write (now another node takes
  over after about 25 s); the Python and JavaScript clients failed while their node was down (they
  take every node's address now); `pondra sql` waited for ever on a missing bucket; a slow bucket
  was taken for a lost one. A PR's push runs CI only when labelled (`ci`, `full-ci`).
- **A full local disk recovers on its own** (PR #18, `store.rs`): the catalog's write-ahead log
  goes every 5 s on a local disk (95 MB at 90 s of small writes before, about 6 MB now) and a
  compaction's replaced files a minute on (600 MB at six minutes and growing before, about 250 MB
  level now); a full disk given room acks again within a second. `resilience_check.py disk` passes
  on 64 MB and 32 MB.
- **A table's past** (ADR-043, PR #13, `tools/history_check.py` 12 of 12): a dropped table is kept
  for its retention (a day unless the table says otherwise) and `UNDROP TABLE` brings it back;
  `SELECT … FROM t AT (VERSION => n | TIMESTAMP => '…' | OFFSET => -3600)` reads an append table as
  it was, anywhere a table goes, equal to a model of 13 states; `RESTORE TABLE t TO VERSION AS OF n`
  puts it back in one change (ids kept, undone by another); `CREATE TABLE c CLONE t` copies no file.
  The past is the rows' system columns and `{t}$deleted`: nothing in the catalog until it's read.
- **`CREATE VIEW v (a, b)`** names the view's columns (PR #12; the list was ignored).
- **`DECLARE PARAMETER`** (ADR-044, PR #15): only marked variables are a file's parameters; a plain
  `DECLARE` is the file's own, and a run given a name that isn't a parameter is refused; a `.py`
  file's parameters are its `# %% tags=["parameters"]` cell.
- **Scripts that decide** (ADR-045 phase 1, PR #16, `harness.py scripts` 9 of 9): `IF … ELSEIF`,
  `CASE`, `WHILE`, `REPEAT … UNTIL`, `LOOP`, `FOR r IN (query)`, labels with `LEAVE` and `ITERATE`,
  `BEGIN … EXCEPTION WHEN … END`, `RETURN`, `RAISE`, `PRINT`, `ASSERT`, `EXECUTE IMMEDIATE … INTO …
  USING`, `CALL … INTO` and `IDENTIFIER(…)`, the same from every door; each statement runs as it
  would alone (spread, or written through the leader), and a script run again with its job writes
  once, loops included. A loop's pass over variables alone takes 0.4 ms (no query).
- **The console** (PRs #7 and #8, the owner's lists of 2026-10-02): every Live cell over one
  connection (seven of them took the browser's six and hung Python), one grid everywhere with undo,
  paste and filters as pills, the Data tree's details, drags and uploads, a schedule editor, users,
  roles and who has access, and a table's own tab whose edits save as one transaction.

**Then (2026-10-01, round 31, second part so far; 0.29.0): every kind of object alike, SQL and
Python in one notebook** (the owner's list after the first part):

1. **`CREATE OR REPLACE` and `IF NOT EXISTS` mean one thing everywhere** (invariant 192): tables,
   views, materialized views (one fed by a topic too), functions, procedures, macros, tasks,
   secrets, external tables and a session's temporary tables, views and secrets take both; both
   together are refused; schemas, databases, users and roles take only `IF NOT EXISTS`, and `OR
   REPLACE` there is refused with what it would drop.
2. **`ALTER MATERIALIZED VIEW v DETACH`** (invariant 193): the flow stops and the rows stay, a table
   that takes `INSERT`s; what follows it keeps following that table (a `GROUP BY` view's stays a
   merge table); a view keeping windows is refused; one fed by a topic stops reading it.
3. **A notebook's SQL and Python see each other** (invariant 194), in the console and in a run
   (`CALL run('nb.ipynb')`): a SQL cell's **→ name** (saved as `%%sql df <<`) is a frame in the
   page's Python; a SQL cell naming a pandas, Polars or Arrow table, or a frame, of the page's
   Python reads it (through `db.sql`, which sends it along).
4. **Versions…** shows a notebook's changes cell by cell (a cell the same folded, one added or gone
   whole), each cell's lines highlighted as its language; SQL and Python files highlighted too.
5. **Spark SQL in `spark.sql`** (H8, invariant 196): PySpark's `spark.sql` reads its query with
   Spark's grammar (backticks, `"text"`, `!`, `DIV`, `<=>`, `RLIKE`, `LATERAL VIEW [OUTER]
   explode`, `explode` named `col`) and Spark's functions where both have a name and differ
   (`floor` a `BIGINT`, `substring` from a negative start), turned into Pondra's SQL where SQL
   comes in: `spark_sql('…')` in any FROM, from any door; Spark's versions of shared names are
   `spark_floor`, `spark_substring`, … in every session, DataFusion's answers unchanged.
6. **The build of `0253f18`**: Windows' node never started (its main thread has 1 MB of stack, and
   round 31 made a whole session state there): the work runs on an 8 MB thread on every OS
   (invariant 195), and Spark's functions are found against the first session; the columns suite
   waits for a renamed table's streamed rows. The pull request's own build then found tokio's 2 MB
   workers too small for 16 procedures calling each other in the release build: they get 8 MB.
7. **Found on the way:** a view fed by a topic made again under its name kept running its old query
   from where it was, and never filled: a shard now ends when its feed changes or goes, and a feed
   dropped leaves the sequencer's memory (invariant 78's rule for views). And a table renamed soon
   after an `UPDATE` could show the changed rows' old versions again, for good: the rename sent the
   table's log rows to files first, but not its `{t}$deleted`'s, which were left under the old name
   (`harness.py columns` failed on it now and then; the rename now does both, invariant 129).
8. **The owner's list of 19:50** (the console, after trying the second part):
   - **Flows:** chains of materialized views are *flows* now (`pondra.flows`, `guides/flows`,
     `harness.py flows`, `tools/bench/flow.py`); "pipeline" stays free for the ETL tools to come (J4).
   - **A SQL file's answers numbered:** a strip of numbers with what each did (`2 · 3 rows`), scrolled
     when there are many; each number is also in the file's gutter beside its statement, and the
     one shown (or pointed at) has its lines tinted. Messages lists every statement in full.
   - **Explain** (Ctrl+Shift+E, in the Run ▾ and the editor's right-click, a SQL cell's ⋯ too): the
     plan of the selection or the statement the caret is in, not run. A plan copies or downloads as
     text, SVG or PNG.
   - **A SQL cell's frame name** reads *Result in Python: [name it]*, shown on hover until named.
   - **The Data tree by schema:** functions, procedures and schedules under the schema that holds
     them (secrets stay the lake's), empty schemas listed; a schema's ⋯ makes any of them there,
     makes a table from a file and copies its tables' names; a table's ⋯ imports rows from a file
     and copies its columns' names.
   - **Planned:** a workspace exported and imported whole, with or without data, for CI/CD (J5);
     flows' cost and more SQL kept up incrementally (F, round 32). AGENTS.md principle 9: easy to
     change, replace and extend.
   - **The first load** stays within its budget (71,335 bytes of 71,680): the sign-in dialog, a SQL
     file's Messages and gutter numbers (`stmts.js`, new) and a cell's ⋯ load when first used.
9. **SQL variables and a file's parameters declared** (ADR-037, invariant 198, the owner's choice
   of 2026-10-01): `DECLARE $day DATE = current_date - 1;` (type and value optional) and `$day =
   $day + 1;`, with DuckDB's `SET VARIABLE`, `RESET VARIABLE` and `getvariable` as other names. A
   value is worked out once and bound as a typed literal wherever `$day` is used; a session holds
   its variables (Postgres, `x-pondra-session`, a console tab, a script), a procedure and a file
   run their own, shared with their Python (`db.vars.day` is `$day`). In a file a `DECLARE` is a
   parameter with its default, its description the comment above:
   `pondra.parameters('etl/orders.sql')`, `db.parameters(…)`; `pondra.variables` lists what is set.
   A column using a variable is named as written (`$day + 1`). In the console a SQL file's bar shows
   each parameter's type, default (a date picker for a `DATE`) and comment; `$name`s are highlighted,
   completed after `$` and say their value on hover; the Variables view lists SQL's too; Ctrl+Shift+
   Enter runs the statement at the caret (just after its `;` too), and Settings says what Ctrl+Enter
   runs. JavaScript's `db.vars()`, `getVariable`, `setVariable`, `resetVariable`, `parameters`;
   `pondra run daily.sql --help` lists a file's parameters. `harness.py variables` (12 checks),
   `console_check.py` (`files`). Found on the way: PL/Python's `plpy.execute(plan, [v])` lost its
   `$1` once a run's values became its run's (`harness.py functions`).
10. **CI builds a commit once:** a push to main whose code a pull request's run built and tested
   (the branch was up to date) builds nothing; the release and the cluster bench take that run's
   packages (`tools/ci_build.sh`, by the commit's tree).
11. **Tests:** `harness.py objects` (new, 13 checks, a Pondra node's Kafka port as the topic);
   `harness.py sparksql` (new); `harness.py flows` (renamed);
   `console_check.py` in every part (axe clean, light and dark); the changed docs pages' 142
   examples; `harness.py` workspace, procedures, temps, external, schemas, secrets,
   versions, columns (4 runs of 4 with the rename's fix), renames, changes, found, functions,
   kafkas, outside, doors, begin and users; `smoke.py`.

**Then (2026-10-01, round 31, first part; 0.29.0): correct SQL, proven**:

1. **sqllogictest 74.5% → 98.0%** of DataFusion 55.1's records that aren't named exceptions
   (23,032 of 23,493; 92.9% of all 24,783). `slt_check.py` runs each file in a session of its own,
   with DataFusion's runner's two choices (4 partitions, `1.5` a DOUBLE), shows times, durations
   and intervals as Arrow does, and leaves out only failures that match a named rule, each with
   its reason: EXPLAIN's text (987), a write explained (37), what the runner makes in Rust (196),
   the node's memory settings (41), microseconds (23), an order no query asked for (6).
2. **Session settings and prepared statements** (`settings.rs`): `SET`, `RESET`, `SHOW`, `SET TIME
   ZONE`, `PREPARE`, `EXECUTE`, `DEALLOCATE`, a Postgres connection's, a client's session's, or a
   script's own. DataFusion's options are checked as they are set and used by the session's
   queries (on its node, never from the result cache); Postgres's names are kept for `SHOW`.
3. **Spark's functions**: `format_string`, `pmod`, `parse_url`, `sha2` and the rest of
   `datafusion-spark`'s names DataFusion lacks, everywhere; in the `spark` or `databricks` dialect,
   Spark's versions of shared names too. `pondra.spark.functions` reaches any of them by name.
4. **Files and DDL**: Arrow IPC files read (`read_arrow`, `STORED AS ARROW`) and written (`COPY …
   TO` as arrow); DataFusion's writer options through `COPY` (`'format.compression' 'zstd(10)'`);
   `CREATE TABLE` of a table that is there refused (42P07) unless `IF NOT EXISTS` (left alone, no
   rows added) or `OR REPLACE` (it used to append); `SELECT … INTO`; DuckDB's `STRUCT(a INT)` in
   the `duckdb` dialect; a MERGE with no WHEN, a column set twice, a SET of the source's column or a
   source named as the target refused; `n * INTERVAL '37 seconds'` and `INTERVAL … / n` as Postgres
   computes them (`intervals.rs`).
5. **TPC-DS** (`tools/tpcds_check.py`, DuckDB's data and queries at SF1): all 99 queries answer
   as DuckDB does on one node. Pondra 123.5 s in all against DuckDB's 22.2 s, 81 s of it q72 (a join
   order to fix); on three nodes 98, q72 running out of this 8 GB box's memory beside a build.
6. **Random queries** (`tools/random_sql.py`): each query on one node against DuckDB, every tenth
   spread over three, and split three ways by a random condition (ternary logic partitioning). It
   found DataFusion 55.1 answering `x IN (CASE WHEN … END, 1)` wrong (the list taken for a
   constant after a try on an empty batch); `optimize::InListOfRows` writes such lists as ORs.
7. **Coverage published** (`tools/api_coverage.py`, `logs/round31/api-coverage.json`): Polars
   1.44's DataFrame and LazyFrame 23%, Expr 17%, `str` 29%, `dt` 28% by name; PySpark 4.2's
   DataFrame 45%, Column 64%, reader 64%, writer 61%, its functions 12% defined and 39% by name in
   Pondra's SQL.
8. **Tests**: every harness suite passes (`versions` once its tie in a millisecond was ordered);
   `flows`, `begin`, `doors`; the changed docs pages' 59 examples.

**Then (2026-10-01, round 30): flows (ADR-036 calls them pipelines), and a database's behaviour from every door** (ADR-036):

1. **Flows** (the owner's ask, as Databricks' DLT has them): a materialized view may follow
   another, and every stage moves in the commit that wrote the rows that started it (bronze →
   silver → gold, no lag between stages). A view of a GROUP BY view must be a rollup (its keys,
   `sum` of sums and counts, `min` of mins, `max` of maxes), else it is refused saying why; changes
   (`UPDATE`, `DELETE`) flow down the chain in one commit; a flow is dropped from its end.
   `pondra.flows`, and the console's details panel draws each table's flow. **History per
   key** (SCD type 2): `WITH (history = 'id', sequence_by = 'at', delete_when = '…')` keeps every
   version, each with its `__start_at` and `__end_at` worked out when read, late versions in their
   place, a delete ending its key.
2. **Expectations**: `CONSTRAINT c CHECK (…) [ON VIOLATION DROP ROW | FAIL]` and `EXPECT (…)` on a
   materialized view, counted exactly in `pondra$expectations` with the rows (`pondra.expectations`).
   A row a `FAIL` one refuses fails its own INSERT only: the flush it was packed in is checked
   producer by producer and the others go on.
3. **CHECK constraints on tables**, every door, `23514` as Postgres sends it.
4. **Postgres's error codes everywhere** (`codes.rs`): the Postgres port, HTTP's
   `x-pondra-sqlstate`, Flight SQL's metadata, MCP's text, `PondraError.sqlstate` in Python and
   `err.sqlstate` in JavaScript.
5. **Transactions**: `BEGIN` … `COMMIT` is one commit with snapshot isolation (Postgres's REPEATABLE
   READ): reads see the transaction's own writes over its snapshot; a row another commit changed
   first refuses it with 40001; a failed statement fails it (25P02). The Postgres port (drivers'
   transactions, psycopg's status), HTTP and the clients (a session; `with con.transaction():`).
   **pgbench's own TPC-B script runs, its balances right** (the roadmap's gate): 1 client 133 tps
   (7.5 ms a transaction; 28 tps when it first ran this round), 4 clients 91 tps with most
   retrying on the one branch row; Postgres on the same machine 997 and 1,793 tps
   (`tools/bench/pgbench.py --postgres`, a gate in `gates.py`).
6. **The point path from every door**: a key lookup through the Postgres port is answered from the
   serving path before anything is planned, its `Describe` and parameter types too, each worked out
   once per catalog version: with psycopg (`vs_postgres.py`), p50 6.5 ms → **0.29 ms** (Postgres
   0.09 ms), the roadmap's ≤ 0.5 ms. The log's segments are decoded once per node, a lookup reads
   only those committed since the last (`Lake::segments_after`), and a big log batch not yet
   tiered is looked up through an index of its keys made once (0.93 → 0.53 ms for a row in a
   100,000-row INSERT). In a transaction a lookup reads its own version or its snapshot's
   (6.2 → 0.4 ms); a one-key `UPDATE` of a keyed table is worked out without planning, in a
   transaction (6.5 → 0.7 ms) or not (10.8 → 2.5 ms).
7. **What a flow costs** (`tools/bench/flow.py`, 4 producers of 1,000-row batches over
   HTTP on this 2-vCPU machine): no view 183,000 rows/s; with silver (two expectations) 156,000;
   with gold 135,000; with platinum 131,000 (−29% for three stages), the append p50 16 → 24 ms;
   at the moment the producers stop, every stage equals its query over the source.
8. **The doors matrix** (`harness.py doors`): nine features through HTTP, Python, Postgres, Flight
   SQL, JavaScript and MCP; every cell right, or refused by name (a transaction on Flight SQL and
   MCP, which have no session). It found three gaps, fixed: Flight SQL ran no `CALL` and lost its
   errors' codes, MCP's errors had none.
9. **The comparisons, run again** (2026-10-01; `docs/comparison-spark-flink-fluss.md`): TPC-H SF1
   4.5 s against Spark 4.2's 56.1 s (DuckDB 3.5 s); Nexmark's five queries over 10 M bids 14.3 s
   against Flink 2.3's 30.4 s (slower than round 21's 8.7–10.9 s; the gates' 2 M-bid run has held
   at 2.75–2.98 s); a row in the Delta and Iceberg tables 0.42–0.46 s after its ack on local disk;
   and the form factor (`tools/bench/footprint.py`): first answer 0.07 s after start (Spark 9.7 s,
   Flink 7.4 s), 42 MB of memory of its own idle (Spark and Flink over 550 MB).
10. **Found and fixed:** the new key-lookup work, inline in the statement loop procedures recurse
   through, overflowed a worker's stack 16 procedures deep: put on the heap, as the rest is.
11. **Tests**: `harness.py flows`, `begin`, `doors`, and every other suite; `tools/bench/pgbench.py`,
    `tools/bench/flow.py`, `tools/bench/footprint.py`.

**Then (2026-10-01): every file keeps its versions, a stopped run says so, a faster cold start**
(ADR-035 §8, round 29 part 3):

1. **Versions**: each save of a lake file kept with who made it and when (`files/.versions/`, the
   newest 50 for 90 days); **Versions…** in every file's ⋯ shows what changed since, line by line,
   and restores one; a deleted file keeps them. Notebooks are one file each (`notebooks/<name>.ipynb`;
   saved before as `notebooks/<name>/<time>.ipynb`, they still open, and those saves are among their
   versions), and any notebook can be a job or a schedule.
2. **`stopped`**: a run whose node was killed, restarted or left the cluster is marked so in
   `pondra.runs`, saying whose node, instead of `running` for good.
3. **Within the bucket's limits, and a faster cold start** (C5): the catalog's compactor and
   garbage collector start once the node serves, and the first checkpoint, the leader's mark and
   the lake's keys go beside serving: on the simulator at R2's latency a node serves after 3.1–4.4 s
   (was 6.5–7.7 s). One request budget per bucket and node, in the HTTP layer so retries count
   too, halves on a 503 or 429 and grows back (`budget.rs`); log segments are named with a random
   first part; the orphan sweep takes one part of the lake at a time, not the whole bucket each
   hour; a ring of the inbox's bell refused (R2's one write a second to a key) counts as rung.
   `tools/c5_check.py`: 240 INSERTs into a bucket that takes 10 writes a second all succeed as the
   node slows down; four writers through the inbox at once all answered.
4. **Found and fixed:** with the lake's keys made beside serving, a request could make them at the
   same moment as the leader, two keys, and a follower's calls refused: they are made once now.
5. **Tests**: `harness.py versions` and `stopped`, `tools/c5_check.py`, `tools/cold_trace.sh`,
   `console_check.py` (94: Versions… shows the change and restores), the docs' examples,
   `harness.py all` again.

**Round 29, part 2 made a lake safe to share: users, grants, sealed secrets, TLS, an audit log, quotas, and no request that stops a node** (ADR-035 §2, §3, §5):

1. **Users and roles** in SQL: `CREATE USER ana PASSWORD '…'`, `CREATE ROLE`, `GRANT SELECT (id,
   amount) ON orders TO ana`, `GRANT INSERT ON SCHEMA sales` (its later tables too), `GRANT USAGE ON
   SECRET`, `CREATE TOKEN … FOR USER … EXPIRES IN '30 days'`; `pondra.users` and `pondra.grants`.
   The tokens of before stay, as the built-in roles `admin`, `writer` and `reader`.
2. **One check at every door**: HTTP (Basic, a token or a session), Postgres (SCRAM-SHA-256 or a
   password), Kafka (SASL PLAIN), Flight (its handshake gives a session), the Iceberg catalog and
   MCP all sign in the same users. A column a user may not read is refused in its query, its
   filters included; a stream of rows needs every column.
3. **Nothing readable kept**: a password as SCRAM's verifier, a token as its SHA-256, sessions
   signed with a key the lake keeps.
4. **Secrets sealed twice**: each with a data key of its own, wrapped by the master key or by a key
   service (`PONDRA_KMS_COMMAND`, any KMS by a short script); a new master key rewraps them all;
   `CREATE TEMPORARY SECRET` lives in the session's memory.
5. **The console signs in as the shell is** (the shell prints a link with a key, as Jupyter does)
   or as a user (Sign in as someone else…).
6. **TLS on every door, on its own port** (`--tls-cert`, `--tls-key`): HTTPS, Postgres's
   `sslmode=require`, Kafka's SSL, Flight's `grpc+tls`; a plain connection only from the node's
   own machine; nodes call each other over HTTPS, and with an authority (`--tls-ca`) show their
   certificates to each other (mutual TLS): the nodes' key alone opens nothing.
7. **`pondra.audit`**: refused sign-ins and requests at every door, and every change to users,
   grants, secrets and what exists (pgaudit's classes, `PONDRA_AUDIT`), values as `'***'`; a
   superuser's only.
8. **Quotas**: `MAX_QUERIES` (the rest wait their turn) and `STATEMENT_TIMEOUT` per user, on each
   node.
9. **No request stops a node** (`panics.rs`): a panic in a request is answered as an error (HTTP
   500, `XX000`) or ends that connection; the node's own loops still stop it. `tools/fuzz_doors.py`
   threw malformed input at all four doors and generated SQL: against the build before, it stopped
   the node through `/cluster/commit` (fixed) and through pgwire's decoding of `Bind`, `Parse` and
   `CopyData` cut short (now that connection's alone); the new build stayed up through every door.
   The cost: the binary is about a fifth bigger (unwinding tables).
10. **Found and fixed:** a Postgres client could sign in as `reader` with an empty password when no
   read token was set; the AI functions sent the nodes' admin token to the AI endpoint; a docs
   example claimed a refusal for a query it never ran (the client runs a query when its rows are
   asked for).
11. **Tests** (`logs/round29/p2-*`): `harness.py users`, `secrets` and `safety` (every door, a column
   refused, a revoke seen at once on a follower, a rotated master key, a KMS command, panics, TLS
   from another address, mutual TLS, the audit log, quotas), `tools/fuzz_doors.py`, the whole
   suite again, `console_check.py` (94), the docs' examples. `tools/gates.py` runs the gates as one
   command and appends a row to `logs/gates/README.md`.

**Round 29, part 1 took the owner's list from 0.26 on Windows** (ADR-034, "Changed after 0.27"):

1. **Python that starts.** `--python auto` tries every Python the machine has at once (the PATH,
   Windows' `py` launcher, Anaconda and Miniconda and their environments), each for 15 s at most,
   so a broken install no longer holds a cell forever; a worker must say hello within 60 s, and
   its error carries what it printed. **Choose the Python…** lists them and keeps the choice. Stop
   interrupts a running cell (its variables stay; Windows restarts Python), and a cell the page
   stopped waiting for never answers the next one.
2. **The grid and its answers:** a whole selection outline, a smaller header card, a header's own
   menu, typed filters (contains, =, <, is NULL, …) that open again to change, a SQL list of every
   column selected; Copy and Download as split buttons: tab-separated, CSV, JSON, Markdown, SQL
   `VALUES` or a list; every row as CSV, TSV, JSON lines, Parquet or Excel (`xlsx.rs`, no crate).
3. **What ran:** Messages lists each statement; Runs shows each run on a line, a click to see it
   and open, copy, run again or see its plan; a `DO` block logs its code, so a cell run on the node
   is named by its first line.
4. **Plans and charts:** the Plan tab draws the steps as a graph (the ones moving rows between
   nodes dashed); Profile runs `EXPLAIN ANALYZE` and colours the costliest steps. Charts: bars,
   lines, areas, points, a pie, saved as PNG or SVG.
5. **Editing:** a right-click menu in files and cells, **Format** (Shift+Alt+F) for SQL in the page
   and for Python by the node's ruff or black (`POST /python/format`), leaving what can't be
   formatted as it was.
6. **Calmer:** Save shows only when there is something to save; Settings is a gear and a dialog,
   light first, a background and accent colour per theme, **kept on the machine** for every lake
   and session (`/console/settings`); the right pane's tabs reorder; a results panel at the right
   of a narrow file keeps its buttons in reach.
7. **Lighter:** the page's first load had grown to 76.5 KB gzipped; Runs, Variables, Settings,
   search, choosing Python, a table's profile and data files now load when first used: 68.5 KB,
   each later part under 8 KB.
8. **Reviewed before shipping** (the owner asked if it is production-ready; ADR-034): every
   workflow on a realistic lake at 1440, 1024 and 760 px, light and dark. Fixed: the SQL
   highlighter taking an alias `c` for a comment; Profile failing on `DOUBLE` columns; the details
   pane covering Run between 760 and 1180 px; the tab in front pushed out of sight; Details stuck
   on a table; completion after `o.`; a JSON document failing to open; notebook answers without
   the Copy and Download menus; scrolling 10,000 rows (p95 27.7 → 17–20 ms). Measured: page ready
   in 0.2 s, 10,000 rows drawn in 0.11 s, a 40-cell notebook run in 1.05 s; typing in a
   1,000-line file 28 ms a key (a new editor: round 35).
9. **The owner's second list** (from screenshots of the reviewed console; ADR-034): **Markdown**
   cells drawn as GitHub does (tables, pictures from the lake, task lists, links that open lake
   files, SQL highlighted; no HTML that can run), and a `.md` file's Preview; a SQL cell's **Chart**
   and **Plan** under its answer, kept with the notebook; **tabs** that scroll (a thin bar, the
   wheel, ⌄ for all) and **pin**; **pages of rows** for answers over 10,000 (`‹ 1 2 3 ›`), the
   node keeping the answer (`pages.rs`, `GET /sql/pages/{id}`) so a page is the same rows without
   running the query again; **Jobs** apart from History, a card a schedule with its last runs, room
   for pipelines (`register.jobKind`); the Data tree's columns a level in, with guides; the header
   card above the pointer; **Format selection** and **Format file** in every menu; **Settings** as
   sections with a search, Keys one of them, room for users, tokens and audit
   (`register.setting`). The first load stays under 70 KB (69.5).
10. **The owner's third list** (ADR-034): **rows a page** in Settings and the pager (`/sql?rows=N`);
   one quiet pager line and a single footer line under a cell's answer; the Run ▾'s items (Create
   as table or view, Run as a job, Schedule, Save as) in the editor's right-click; **Data profile**
   (the columns) and **Query profile** (`EXPLAIN ANALYZE`, in Plan) named apart; **right-click on
   everything in the Data tree**: tables, views, materialized views, columns, schemas, the lake,
   functions, procedures, schedules and secrets, each action the SQL it runs, **Script as** any
   statement in SQL or Python (`objects.js`, `register.objectKind`); **Make it Python / SQL** on a
   cell; and `pondra.tables`, so the shell's `.tables` says *materialized view* (the owner found it
   said `BASE TABLE`), with `SHOW VIEWS` and `SHOW MATERIALIZED VIEWS`; **+ SQL, + Python, +
   Markdown between cells**; and `SELECT ts::date, *` (or `SELECT id, *`), which DataFusion refused
   for two columns of one name, runs and names them as Postgres, Snowflake and DuckDB do.
10. **Found and fixed:** the Docs workflow failed on a clean checkout since round 26 (`site/public/`
   didn't exist); a `pop` dialog showed "null"; a menu under a button on the left opened off to its
   left; clicking a statement's answer selected it in the editor, so the next Run ran it alone.
10. **Tests** (`logs/round29/`): `console_check.py` (81, axe light and dark included), `harness.py
   procedures` (Stop, a cell abandoned, `/python`, Format, the run log's code) and `clients` (every
   download read back, Excel by openpyxl), `harness.py all`; the owner's cluster benchmarks on
   0.27.0 (SF10, 3 and 6 runners): every answer the same, the cluster spreading 4 (3 nodes) and 2 (6 nodes) of 22 queries
   and faster than one node (23.9 s against 25.1 s; 21.1 against 22.7), spreading all 22 slower
   (40.0 s, 33.0 s) on a network of 40–85 MB/s.

**Round 28 took other engines' changes as written** (ADR-029 phase 2):

1. **Changes through the catalog, both ways.** Spark's `DELETE`, `UPDATE` and `MERGE`
   copy-on-write (files taken out and added) and merge-on-read (position-delete files, taken as
   written), PyIceberg's `delete` and `overwrite`, against the table as Pondra has it (a change
   made while rows waited in the log gets 409 once they are in the files). Schema and property
   changes (`ADD COLUMN`, `SET TBLPROPERTIES`) are `ALTER TABLE`s. Several tables in one
   transaction (`/v1/transactions/commit`): all or none.
2. **Followed in the same commit.** Every file commit — another engine's, a bulk `INSERT`'s — goes
   through the log: views derive from the files' rows (and take back the rows deleted), and the
   change feed, `/watch`, Kafka topics and tasks read the rows from the files.
3. **Deleted rows are positions.** Pondra's own `UPDATE`, `DELETE` and `MERGE` are purged into
   position-delete files, not rewritten files; a file a tenth deleted is rewritten by maintenance.
   Published as Iceberg delete files and Delta deletion vectors.
4. **Pondra's upkeep never fails a writer.** A version that only merged files is published as a
   `replace` (Delta: `dataChange: false`), with the files it lists again as existing, so Spark's
   conflict checks pass it; the writer's change is then carried over to the merged files by row id.
   Before this, Spark's second statement failed whenever Pondra merged between two.
5. **Keyed tables every tier round.** Their older versions and delete markers are positions, so
   PyIceberg, DuckDB, Polars and Spark read one row per key after every round, not only after a
   compaction. **Measured:** on a 2M-row keyed table, a round of 100 upserts takes 52 ms published
   (6 ms unpublished), of 10,000 upserts 104 ms (48 ms). Other engines' appends to them are upserts, their deletes (copy-on-write,
   merge-on-read, equality deletes on the key) deletes of keys.
6. **Found and fixed:** PyArrow read Pondra's strings as `string_view` (from the Arrow schema in the
   files' footers) and couldn't apply a position delete to them, so PyIceberg failed on any
   deleted row; an `ALTER` through the catalog answered 400 after ten seconds; Spark's metadata
   tables (`t.files`) failed; a delete file's names read as string views panicked the node.
7. **Tests** (`logs/round28/`): `harness.py followers` (6), `transactions` (4), `upserts` (6),
   `rewrites`, `writes` and `changes` updated; `formats_check.py` with Spark 4 (55 of 55, the
   commits part 5 of 5: copy-on-write, merge-on-read, a keyed table); `harness.py all`,
   `console_check.py` (78), locally; the new tests on simulated and real R2. Tests read Delta
   through delta-rs's `QueryBuilder`: its `to_pyarrow_table` refuses deletion vectors.
8. **Found after the round, fixed the same day** (`logs/gates/`, CI on 443e607):
   - TPC-H SF1 from memory had gone from 2.34 s (round 26) to 3.48 s: files with a lineage
     (rounds 27–28) and an append table's files with deleted rows (round 28) never reached the hot
     columns. The hot columns now hold them (system columns from the lineage, deleted rows left
     out): 2.03 s. The rounds' gates hadn't been run; `roadmap.md`'s road to 1.0 makes them one
     command;
   - on Windows, a file of an attached lake, read by its path (`C:\…`), was taken for a URL of scheme
     `c` (`smoke.py` on CI's Windows runner);
   - CI lacked `pyiceberg-core`, which PyIceberg needs to write a partitioned table.
9. **The console's Workspace** (after the owner tried 0.26 on Windows): folders, a ⋯ on every row,
   new files in the tree with the unsaved dot, notebooks in any folder, dialogs that close on a
   click outside, and a Data tree that lists what it can when a view reads a local file.

**Round 27 made other engines' appends cost Pondra a commit, not a copy** (ADR-029 phase 1):

1. **Appends as written.** An append through the Iceberg REST catalog is recorded where the writer
   put its files: the node reads each file's footer once and checks it (the manifest's rows, the
   table's types, no NULL in a NOT NULL column, one partition value), and never reads or writes the
   rows. **Measured:** a million rows from PyIceberg cost the two nodes 0.01 s of CPU, against 0.24 s
   to copy them as round 25 did. Tables that views or tasks follow, or that have a renamed column,
   still copy (decided by Claude, in the ADR).
2. **Lineage per file:** a first row id, the commit and its time give the rows' system columns
   (`_row_id` by Parquet's row number), in every read; `UPDATE` keeps an adopted row's id, and a
   merge writes the columns out, every row keeping its id and version.
3. **The layout published for writers:** `partition_by` as the partition spec (PyIceberg then
   writes a file per day, and Pondra takes them as written; a file of two days is refused), a
   `cluster_by` column as the sort order, a key as identifier fields.
4. **Tables made, renamed and dropped through the catalog**, as the SQL does.
5. **Row ids and log places that can't wrap:** blocks of ids from a counter of their own, and
   `_ord`/Kafka offsets as the segment shifted by 24 bits (34 years at 1,000 commits a second),
   a big segment taking the numbers after it.
6. **Found and fixed:** a column rename reached the published Iceberg schema only with the next
   files, and PyIceberg refused appends to a renamed table (the name mapping had only the stored
   name).
7. **Moved to round 28:** followers fed from the files in one commit, and `/watch`, the change
   feed and Kafka topics carrying file commits (bulk `INSERT`s' too); adopted files are read from
   Parquet, not the hot columns, until merged.
8. **Tests** (`logs/round27/`): `harness.py adopted` (10 checks: spread over three nodes too) and `ids` (2), `writes` updated,
   `harness.py all`, locally, on simulated R2 and on real R2.

**The workspace (ADR-033), after round 26:** the lake's `.sql`, `.py` and notebook files run as
jobs, `CALL run('etl/orders.sql', day => DATE '2026-09-29')`, from every door (HTTP, Postgres, MCP,
Python's and JavaScript's `db.run`, `pondra.run` inside a file, `pondra.start('run', …)`, tasks),
each run a row of `pondra.runs` named `files/<path>@<version>`. The console gives a SQL file's
`$name`s inputs, runs a file as a job or on a schedule, and lists the node's runs and schedules.
Tested (`logs/workspace/`): `harness.py workspace` (13 checks) locally, on simulated R2 and on real
R2; `harness.py all` locally; `console_check.py` (61 checks; the budget 71.6 of 71.7 KB), and the
workspace guide's examples. Real R2 also passed `fence` again; `server` and `serverless` missed
their time limits there only because the runs used the far test bucket (`pondbucket`: PUT p50
≈660 ms, GET ≈440 ms), and passed with the original limits on the near one (`ponderabucket-us`:
PUT ≈250–300 ms, GET ≈115–165 ms; a new database's first write 14 s, a node leading an idle lake
in 5 s: `logs/round28/r2-near-*.txt`).

**Round 26's continuation** (ADR-032), before the tag, took the owner's asks after seeing the
round:

1. **Files by name:** `CREATE EXTERNAL TABLE` is a stored view of files (CSV, Parquet, JSON; a
   folder's partition keys declared). `INSERT` into a view of a folder writes a new file there.
   `to_timestamp` answers a zoneless `TIMESTAMP`, as DataFusion's does. With the test data
   DataFusion's files read now in place, **DataFusion's sqllogictest went from 16,090 to 18,462 of
   24,783 records (64.9% to 74.5%)**. No file lost a record. The most gained:
   - `timestamps` (561 to 764 of 831);
   - `sort_pushdown` (124 to 326);
   - `aggregate` (1,104 to 1,257);
   - `push_down_filter_parquet` (2 to 133);
   - `window` (292 to 401).
2. **A page's Python cells share a worker**, as a notebook's kernel:
   - variables, imports and frames carry from cell to cell;
   - figures (matplotlib, seaborn, Pillow) come back as pictures;
   - `GET`/`DELETE /sessions/{id}/python` list the variables and restart it.
3. **One serve command.**
   - `pondra serve PATH` serves a lake, or a folder whose lakes are databases.
   - `--lake` and `--lakes` say exactly which, for services, and refuse the other.
   - `pondra server` is gone (never released).
   - The folder can be a bucket prefix (`CREATE`/`DROP DATABASE` there too).
   - A database's node stays up while a connection is open or a request is in flight (a 14 s
     `CREATE DATABASE` on simulated R2 found this).
4. **The brand in one place:** `brand/mark.svg` and `brand/colors.css`, used by the console and the
   docs site, and `tools/brand_check.py` fails on any copy.
5. **The console, rebuilt as a core to build on** (`src/console/`: 115 KB, 35 KB compressed, no
   framework).
   - **Everything it shows is registered** through `window.pondra`: sections, panel tabs, kinds
     of cell, views of answers, actions, a rail, keys, events, and how it reaches the node. The
     core's own parts go through it too. Extensions load from `PONDRA_CONSOLE_EXTENSIONS`, and
     `examples/console-extension.js` shows all four kinds.
   - **Left side:**
     - the tree, with an icon for each kind and a glyph for each column's type;
     - the lake's files;
     - an outline;
     - notebooks.
   - **Details panel:**
     - a table's facts, a view's definition, a file;
     - profiles over the whole table;
     - an answer's columns;
     - the Python variables, with Restart.
   - **Answers:** a grid that draws only the rows in sight.
   - **Jupyter's working set:**
     - Tab completion (tables, the named tables' columns first, functions, Python's variables);
     - run above and below;
     - hide or clear an output;
     - a kernel badge.
6. **Tests** (`logs/round26/`):
   - `harness.py all` (403 checks, with `external` and the server's new ones),
     `console_check.py` (30), `docs_check.py` (465 examples on 48 pages), `frames_check.py`,
     failover, race, `brand_check.py`, and sqllogictest, locally;
   - `server`, `external`, `procedures` and `found` on simulated R2;
   - `server` and `external` on real R2.
7. **Proposed, not built:** the server's catalog (ADR-032 §9). **Decided:** backward
   compatibility from 1.0 on, not before. (The workspace, ADR-033, is built: below.)

**Round 26, continued again (ADR-034): the console as the owner's canvas drew it.**

1. **Tabs:** notebooks, SQL files (Results, Messages, Chart, Plan; each statement its answer, or
   by Settings the last one's), Python files (a console on the page's Python, and a `>>>` line),
   data files (CSV and JSON edited in a grid, Parquet read-only) and text.
2. **Around them:** Data and Workspace on the left (a filter; the notebook's outline under it), Details,
   Variables and Runs on the right; any view moves to the other side; pane edges; Settings (theme,
   order, font, statements); Sign in; narrow windows as drawers.
3. **Files saved where they are:** `PUT /files` with `If-Match` replaces a lake's file, never over
   someone else's change (`412`); `DELETE`; `files/` kept out of every cache, so every node reads
   the new bytes at once.
4. **One grid:** coloured type marks, a header's card, a spreadsheet's selection (the row lit),
   Ctrl+C as cells, filter and sort from the menu.
5. **Measured** (`console_check.py budget`): the code 69 KB gzipped as served (70 KB allowed),
   first paint about 70 ms, a key 3 ms in a 1,000-line file (33 ms before the line-at-a-time
   highlighting), a 10,000-row scroll's p95 frame 18 ms; axe finds nothing, light and dark.
6. **Tests:** `console_check.py` (60 checks in 8 parts), `harness.py external` (locally and on
   simulated R2: replace, 409, 412, delete, two nodes, the SSD tier), `docs_check.py`.

**Round 26 made Pondra something people can find their way around** (ADR-030): a documentation
website, a console in the browser, a server of databases, and the Postgres catalog that dbt and BI
tools read.

1. **The documentation website** (`site/`, Starlight on GitHub Pages, published with each release
   tag): 49 pages for users — start, guides by task, a reference page per door and feature,
   concepts. **Every example runs in CI**: `tools/docs_check.py` ran 456 examples on 48 pages, each
   page against a fresh node.
2. **Writing it found 37 bugs**, and all are fixed, each with a check that fails without it:
   - `NOT NULL` and `DEFAULT` are enforced on every door (they were accepted and ignored);
   - `INSERT … ON CONFLICT`, `UPDATE … FROM`, `DELETE … USING` and `TRUNCATE` work;
   - unknown `WITH` options, uncastable values and `CREATE EXTERNAL TABLE` are refused by name
     (they were ignored, stored as NULL, or made an empty table);
   - `TIMESTAMPTZ` is an instant in UTC; Parquet files carry Iceberg field ids (Polars'
     `scan_iceberg`); reads on a follower see its own writes; a reader follows a new leader
     within a second;
   - the spread guard estimates what an aggregation moves by its groups, not its input rows. The
     6-node bench had kept q1, q3, q4 and q12 on one node although spreading them was 1.5–2.6×
     faster.
3. **The console at `/`**, in the binary (one HTML file, no CDN):
   - a tree of databases, schemas, tables (with row counts), views and columns;
   - SQL cells with types and exact numbers; Python cells run on the node (`DO LANGUAGE python`);
     text cells;
   - a live switch, redrawn on each commit that changes the answer;
   - notebooks saved in the lake as `.ipynb` versions, opened, downloaded and uploaded;
   - Jupyter's keys, light and dark. `console_check.py` drives it in Chromium: 15 checks.
4. **`pondra server`:** a folder of lakes as databases. Postgres clients pick one by name, HTTP by
   `/db/{name}`. Each database is a node of its own, started on first use (about 70 ms here) and
   stopped when idle. `CREATE DATABASE`, `DROP DATABASE`, and queries across databases.
5. **dbt and BI tools** (`pg_catalog.rs`): dbt (seeds, views, tables, incremental models of three
   kinds, a snapshot, tests, docs; run twice) gives the same rows as Postgres 16. psql's backslash
   commands, SQLAlchemy, pgjdbc (DBeaver, Metabase), psqlODBC (Tableau, Excel), ADBC, and Npgsql
   4.0 and 8 (Power BI's driver) work. `ALTER TABLE | VIEW … RENAME TO` keeps a table's files where
   they are, so other engines keep reading them.
6. **Found at the end,** by the docs sweep and the suite, and fixed:
   - `pondra sql` didn't check `NOT NULL`;
   - ADBC's Postgres driver couldn't read `pg_type`; Npgsql knew none of the types;
   - SQLAlchemy's default schema listed every schema's tables;
   - a time without seconds (`'2024-05-01 10:30'`) wasn't a timestamp;
   - `to_timestamp` over a column failed once the session's time zone was set;
   - tables were views to `information_schema`; a `files()` listing could be a remembered answer;
   - a node on a lake whose first leader hadn't made its catalog stopped instead of waiting.
7. **Tests** (`logs/round26/`):
   - **Locally:** `harness.py all` (42 sections and a load run, with the new `found`, `renames` and `server`),
     `frames_check.py`, `spark_check.py` (55 of 55), `formats_check.py` (52 tables),
     `tpch_frames.py`, `clients_check.py` (24 checks: dbt against Postgres 16, psql,
     SQLAlchemy, pgjdbc, psqlODBC, ADBC, Npgsql), `console_check.py` (15), `docs_check.py` (456
     examples), `cluster.py` failover ×3, users, race and spread, `open_check.py`,
     `asof_check.py`, `stream_check.py`, `spread_tpch.py` (22 of 22), `skew_check.py`,
     `smoke.py`.
   - **On simulated R2:** `renames`, `columns`, `found`, `procedures`, `schemas`, the crash run
     (2 × 30,000 events, kill -9 and injected crashes: every event once, views exact), failover
     (writes back 11.6 s after a leader kill), users (0 inconsistent reads) and race.
   - **On real R2:** `renames`, `columns`, `procedures` and race.
   - **DataFusion's sqllogictest:** 16,090 of 24,783 records (64.9%; round 25: 67.2%). The
     difference is two decisions of this round, not regressions: `CREATE EXTERNAL TABLE` is
     refused instead of making an empty Pondra table (about 380 records in files that went on to
     use such a table), and `to_timestamp` returns `TIMESTAMPTZ`, as Postgres's does (about 200
     records expect DataFusion's zoneless answer). Reading `CREATE EXTERNAL TABLE` as a named view
     of the files would win the first back; it belongs to round 30's conformance work.
   - **Single-node TPC-H SF1, 2 vCPUs** (this run's machine was slower for every engine):

     | Engine | Total | Round 25 |
     |---|---|---|
     | Pondra, hot columns | **2.34 s** | 2.11 s |
     | DuckDB, its own tables | 1.87 s | 1.65 s |
     | Pondra, from files | 3.62 s | 3.46 s |
     | Polars, streaming | 3.72 s | 3.36 s |
     | DuckDB, from files | 4.18 s | 3.45 s |

     Against DuckDB from the same files, Pondra took 0.56 of its time (round 25: 0.61), and from
     its files 0.87 (1.00).

**Round 25 gave Pondra one vocabulary and opened its tables to other engines' writes** (ADR-028):
the names a user already knows work everywhere, and Spark or PyIceberg can append to a Pondra
table.

1. **One vocabulary:**
   - `read_*` and `write_*` in SQL, Python, PySpark and JavaScript.
   - The tools' own names are fallbacks with the same answers: Polars' `scan_*` and `sink_*`,
     DuckDB's `delta_scan` and `parquet_scan`, PySpark's `spark.read` and `df.write`.
   - `dataframe-api.md` lists 69 names, and a test runs each one.
   - `COPY … TO '<folder>' (FORMAT delta | iceberg)`, `write_delta` and `write_iceberg` make a
     table in a folder, append to one or overwrite it. delta-rs and PyIceberg read the result.
2. **Other engines append through the node's Iceberg REST catalog:**
   - tested with Spark 4 (Iceberg 1.10), PyIceberg and another Pondra;
   - each commit lands once: a stale one gets 409 and a retry, a repeated one is answered as done;
   - the rows become the table's own, with row ids, and views and the Delta copy follow;
   - deletes, schema changes, keyed tables and new tables are refused by name.
   
   The node copies the rows: 1.1 CPU-seconds for 4 M rows (`tools/bench/outside_append.py`).
   ADR-029 (proposed) is the design that removes that copy.
3. **Live queries:** `GET /live`, `db.live(…)` in Python and JavaScript. You get an answer now,
   and a new one after each commit that changes it: 9–19 ms after the statement on local disk,
   0.4–0.5 s on R2 (the commit's own round trip included). Nothing runs once the client closes.
4. **Function answers kept:** `WITH (cache = '10 minutes')`. Each node keeps them in an LRU of
   up to 256 MB.
5. **Temporary tables and views:** they belong to a Postgres connection or a client's session.
   Every statement works on them, and they are gone at `close()`, on disconnect or after an
   hour idle.
6. **`UPDATE`, `DELETE` and `MERGE` on an attached lake, from any node.** What the statement reads
   is sent along. When nobody leads that lake, the node leads it for the moment.
7. **What building it found:**
   - Spark reads its own manifest list right after committing, so the writer's manifests now go
     later, with the table's replaced files.
   - A retried commit got 409; "already done" is now checked first.
   - A Delta or Iceberg folder named relatively couldn't be read back. The notebook found it.
   - A frame's display in a notebook had failed since 0.22.1 (`all` in `frame.py` is Polars'
     `all()`). IPython fell back to text, so no test saw it. `package_check.py` checks the
     display now, and `anywhere_check.py` fails on any error a notebook shows.
   - On R2, PyIceberg's own writes need its fsspec file IO. The docs say so.
   - **A known limit, found while designing ADR-029.** Row ids and Kafka offsets are built on
     commit numbers shifted by 32 bits. With a steady trickle of writes on local disk (about 500
     commits a second), Kafka offsets turn negative after about 50 days, and row ids repeat after
     about 100. On R2 this is years away. ADR-029's phase 1 fixes it.
8. **Tests** (`logs/round25/`):
   - **Locally:**
     - `harness.py all`: 40 tests, with the new `names` (7 checks), `answers` (7), `writes` (7),
       `live` (4), `temps` (8) and `across` (3);
     - `frames_check.py`: 26 pipelines, one question 11 ways;
     - `spark_check.py`: 55 of 55;
     - `formats_check.py`: 52 tables, with Spark appending through Pondra's catalog;
     - `tpch_frames.py`: 22 of 22, three ways;
     - `cluster.py`: failover 3 times, users, race and spread; the spread guard;
     - `open_check.py`, `asof_check.py`, `stream_check.py`, `smoke.py`, `package_check.py`,
       `anywhere_check.py` (the wheel, npm, the notebook top to bottom);
     - `spread_tpch.py`: 22 of 22 spread.
   - **DataFusion's sqllogictest:** 16,646 of 24,783 records (67.2%, one more than round 24).
   - **Single-node TPC-H SF1, 2 vCPUs:**

     | Engine | Time |
     |---|---|
     | Pondra | 2.11 s (a recheck: 2.06 s) |
     | DuckDB over the same Parquet | 3.45 s (recheck: 3.35 s) |
     | DuckDB in its own format | 1.65 s |
     | Polars | 3.80 s |
     | Daft | 6.98 s |

     Pondra's ratio to DuckDB is the same as round 24's (0.61 against 0.60), on a machine that
     ran DuckDB 6–9% slower that day. Most of Pondra's difference is q13, whose time here is
     bimodal: 25 runs on a warm node ranged from 0.108 to 0.236 s. Round 24's 0.094 was a best of
     3.
   - **On simulated R2:** `writes`, `temps`, `across`, `live`, `names`, `answers`, `schemas`,
     failover and users with replicated acks, and two crash runs.
   - **On real R2:** `writes`, `temps`, `across` and `live`.

**Round 24 made SQL and Python one** (ADR-027): someone who works only in SQL or only in Python
can do anything the other can.

1. **`CREATE FUNCTION` in Postgres's forms:** `RETURN expr`, `LANGUAGE sql AS $$ SELECT … $$` (a
   value, a scalar subquery, or `RETURNS TABLE` / `SETOF`), `$1`, typed arguments cast as
   Postgres casts them, `DEFAULT`, `STRICT`, `IMMUTABLE`; DuckDB's `CREATE MACRO` is the same
   thing. SQL functions are expanded where SQL comes in, so their queries spread as any other.
2. **Python functions:** per row, vectorized (`WITH (vectorized = true)`: pyarrow in and out) or a
   table (`RETURNS TABLE`), with `WITH (packages = …)` installed once per node, PL/Python's
   `plpy`, a time limit per batch and no connection back. They run on warm workers beside every
   node (one per core at most, gone when idle), anywhere SQL's own functions go (GROUP BY, ORDER
   BY, windows), and a spread query runs each node's rows through its own workers. An IMMUTABLE
   one gets each distinct argument once a batch (a thousand calls, not a million). A worker
   killed mid-query fails that query with why, and the next one runs.
3. **Procedures that do anything Python can:** send mail, call APIs, read files, as their caller
   (`pondra.sql` is the caller's connection, lent for the call). `pondra.secret('smtp')` reads a
   `CREATE SECRET` that never shows in notices, errors or the log. What a procedure prints comes
   back as notices through every door: the shell, psql (NOTICE), HTTP, the Python and JavaScript
   clients, MCP. `return` is optional.
4. **A run log and schedules:** every call is a row of `pondra.runs` (caller, arguments, outcome,
   notices, error); `SELECT pondra.start(…)` / `db.call(…, wait=False)` start one without
   waiting; `CREATE TASK … SCHEDULE 'cron 0 2 * * * UTC' | '5 minutes' AS CALL …` runs on the
   leader, each tick's writes once through a leader failover.
5. **A notebook's function, as it is:** `@db.function` and `@db.procedure` take along the imports,
   helpers and constants it uses, refuse a DataFrame with the fix, and hand the function back
   unchanged; `pondra.fn.slug(col("title"))` in frames; PySpark's `udf`, `pandas_udf` and
   `spark.udf.register` equal PySpark's (4 more `spark_check.py` pipelines).
6. **Fast:** a warm `CALL` of a Python procedure takes 2.4 ms over HTTP (a Python process per
   call took 0.15 s); a Python function runs over 1M rows at 15M rows/s vectorized and 6.6M rows/s
   per row on this 2-vCPU sandbox.
7. **What building it found:** DataFusion can't run an async function in a GROUP BY, ORDER BY,
   window or COUNT(DISTINCT) (a planning rule now moves it below); spread plans refused async
   functions (now they split); and SQL procedures calling procedures 16 deep overflowed a
   thread's stack, a query's future being 120 KB (now made on the heap).
8. **Tests** (`logs/round24/`):
   - **Locally:**
     - `harness.py all`: 34 tests, with the new `functions` (31 checks);
     - `frames_check.py`: 26 pipelines, one question 11 ways;
     - `spark_check.py`: 55 of 55, 4 of them UDFs;
     - `formats_check.py`: 50 tables;
     - `tpch_frames.py`: 22 of 22, three ways;
     - `cluster.py`: failover 4 times, users, race and spread;
     - `open_check.py`, `asof_check.py`, `stream_check.py`, `smoke.py`, `package_check.py`;
     - `spread_tpch.py`: 22 of 22 spread.
   - **DataFusion's sqllogictest:** 16,645 of 24,783 records (67.2%, as in round 23). Taking
     `SHOW FUNCTIONS` for the lake's own and accepting `RETURN $2` with one parameter each cost
     a record; both were changed to keep them.
   - **Single-node TPC-H SF1, 2 vCPUs:**

     | Engine | Time |
     |---|---|
     | Pondra | 1.91 s |
     | DuckDB over the same Parquet | 3.17 s |
     | DuckDB in its own format | 1.59 s |
     | Polars | 3.72 s |
     | Daft | 6.64 s |

   - **On simulated R2:** `functions`, `procedures`, failover and users with replicated acks, and
     two crash runs.
   - **On real R2:** `functions` (a warm `CALL` 2.2 ms there too) and `procedures`.

**Round 23 read and wrote everything else** (ADR-026): Pondra is a processing engine for data
that isn't in its lake too.

1. **Files anywhere are tables.** `SELECT … FROM 's3://sales/2026/*.parquet'`, `read_csv(…)`,
   `read_json(…)`: globs, folders and lists on S3/R2/MinIO, GCS, Azure, HTTPS and the owner's
   machine; Hive folders become typed columns (NULL's folder as NULL). Files are listed again by
   every statement, so a file added or changed under its name is read as it is at once; their
   byte ranges stay in memory only as the version that statement listed (fetched with
   `If-Match`), and their footers' rows and ranges reach the join order. Spread over the nodes
   like a table's files.
2. **Credentials in secrets:** `CREATE SECRET` (s3, r2, gcs, azure, http, iceberg, kafka,
   generic), sealed with the nodes' key; one secret per bucket; `secrets()` lists names and
   scopes, never values. A URL no secret covers is refused, except for the program that started
   the node.
3. **Other engines' tables, read natively:** `delta_scan` (JSON commits, every kind of
   checkpoint, deletion vectors, column mapping, old versions) and `iceberg_scan` (v1–v3,
   position and equality deletes, deletion vectors, field ids, snapshots), folders of them and
   Iceberg REST catalogs attached (`ATTACH … (TYPE delta | iceberg)`), each equal to what Spark
   4, delta-rs and PyIceberg read of them (50 checks, `formats_check.py`); a feature Pondra doesn't read
   is refused by name. `INSERT` into them commits through the format, exactly-once by job.
4. **`COPY … TO`** Parquet, CSV or JSON files anywhere (`PARTITION_BY`, `OVERWRITE`, `APPEND`),
   from every door, Postgres included; a folder from a big table is written by every node at
   once, each its own share's files.
5. **Other Kafka clusters:** a topic is a table (`'kafka://brokers/topic'` or `ATTACH … (TYPE
   kafka)`), spread by partition; `COPY … TO` a topic with Kafka's own key partitioning; a
   materialized view over a topic is a feed, every record once through a node killed and then
   the leader (Apache Kafka 4.3.1; SASL PLAIN and SCRAM).
6. **Lakes on GCS and Azure** as on S3, failover included (on their emulators).
7. **As fast as its own tables:** TPC-H SF1 from Parquet files takes 3.67 s against the lake's
   3.73 s on local disk and 3.51 s against 3.48 s on a local S3 (the same data written as the lake
   writes it; `tools/bench/files_tpch.py`, `logs/round23/files-tpch-sf1.txt`). tpchgen's own files
   take 4.16 s against 3.44 s: their Snappy and small row groups. The first version took 214 s on
   S3 (nothing kept between queries) and then 4.69 s (a second look at every file, and no
   statistics for the join order), both fixed this round. On real R2, a glob's second read took
   0.11 s after 1.37 s cold (`files_s3_check.py`).
8. **SQL conformance measured (D1):** DataFusion 55.1's own 504 sqllogictest files run through
   Pondra (`tools/slt_check.py`): 16,644 of 24,783 records (67.2%) pass on one node, 72.6%
   without the files of its optional Spark function library. On three nodes, every query spread,
   16,643 pass: the few records that differ return rows in another order, where the query sets
   none. They found two gaps, fixed:
   `INSERT INTO t (b, a) …` wasn't taken, and `CREATE TABLE t (a INT) AS VALUES …` ignored its
   column names. What else fails is grouped in `logs/round23/slt-1-node.json`.
9. **Tests** (`logs/round23/`): locally, `harness.py all` (with `outside`, 27 checks; `clouds`,
   GCS and Azure, 10; `kafkas`, 9; `schemas` with INSERT's columns and CTAS's names),
   `formats_check` (50 of 50), `frames_check` (26 pipelines, 11 ways, the file section),
   `spark_check` (51 of 51), `tpch_frames` (22 of 22), failover ×3, users, race, spread,
   `open_check`, `asof_check`, `stream_check`, `spread_tpch` (22 of 22), `smoke`, the sqllogictest
   run on one node and three; on real R2 `files_s3_check`. TPC-H SF1 on one node: 2.60 s from
   memory and 4.18 s from Parquet against DuckDB's 4.47 s — a slower day for this machine (DuckDB
   took 3.2–3.4 s in earlier rounds), and the same standing: 0.94 of DuckDB's time from Parquet
   (0.93–0.97 before), 0.58 from memory (0.6). Invariants 95, 96, 98, 100, 101, 104 and 106 were each seen to fail
   their tests without their code.

**Round 22 built the DataFrame API, and macros and procedures with it** (ADR-023):

1. **Frames, with Polars' names or PySpark's.** `pondra.frame` is Polars' lazy API over a lake;
   `pondra.spark` gives PySpark's names over the same frames, so a PySpark job moves by changing
   its imports. A frame is one SQL statement, a CTE per step (`frame.sql`): it runs, spreads and
   is remembered as SQL does, as fast (10 M rows: 0.063 s against 0.065 s for the same SQL
   written by hand; a small table's round trip 2.35 ms against 2.2 ms). Where Polars' or
   PySpark's meaning differs from SQL's (the order of nulls, `/`, rounding, column names), the
   SQL says theirs; a frame's sort is carried through the steps after it (DataFusion drops a
   CTE's `ORDER BY`).
2. **SQL and Python, either way round.** `con.sql(…)` is a frame; SQL names Python frames and
   pandas, Polars and Arrow data by their variable names (data travels with the request: a
   million pandas rows sent and summed in 0.06 s); frame methods take SQL snippets; `to_view`
   makes a frame a view every client reads; `.sql` files run with `$name` parameters
   (`con.run`, `pondra run`); `%%sql` cells in notebooks. One question asked ten ways gives one
   answer.
3. **Checked against the real thing:** 26 pipelines equal to Polars value by value; 44 PySpark
   pipelines, written once, give PySpark 4.0.1's answers and column names; all 22 TPC-H
   queries give SQL's answers written as frames and as PySpark code (SQL 3.6 s, frames 5.3 s,
   PySpark code 5.1 s for all 22: the frame versions are written as Polars' own TPC-H is, and
   q21's two aggregations over lineitem cost more than SQL's `EXISTS`).
4. **Macros** (DuckDB's `CREATE MACRO`, scalar and table), kept in the lake and replaced by
   their bodies where SQL comes in, so queries that use them spread like any other (a small
   query 2.64 ms with a macro, 2.57 ms written out).
5. **Procedures in SQL or Python** (the owner's idea): `CREATE PROCEDURE … LANGUAGE sql|python
   AS $$ … $$`, `CALL` from SQL, Postgres, Python (`con.call`, `@con.procedure`), JavaScript and
   as MCP tools. Arguments worked out once; every statement with the caller's rights; a job makes
   a call exactly-once. A Python procedure runs beside a node started with `--python`, never in
   it, with a token lent the caller's rights for as long as it runs. A SQL `CALL` costs 2.9 ms; a
   Python one starts a process (about 0.15 s).
6. **Scripts:** `POST /sql` takes several statements and `$name` parameters (bound by the node).
7. **Tests** (`logs/round22/`): locally, `harness.py all` with the new `procedures` (29 checks
   on three nodes with tokens: macros spread, a follower's new macro used at once, stored and
   materialized views, rights, arguments once, a job's retry, 16 deep, Python procedures and
   their lent tokens, `$$` scripts, parameters, rows sent with a request, Postgres, MCP tools,
   `--python`'s refusal, `pondra run`), `frames_check` (26 pipelines equal to Polars; one question
   eleven ways; a sort kept; writes and Delta's merge builders), `spark_check` (44 of 44 equal to
   PySpark 4.0.1, values and names), `tpch_frames` (22 of 22, both ways), users ×2, failover ×3,
   race, spread, `open_check`, `asof_check`, `stream_check`, `spread_tpch` (22 of 22), the big
   crash run, `smoke`, and `anywhere_check` on the glibc 2.17 build (the wheel runs a frame and a
   Python procedure in a fresh virtualenv; npm calls a procedure; the notebook's new cells run);
   on simulated R2 eleven tests (procedures, fills, changes, schemas, streams, windows, alter,
   clients, failover and users with replicated acks, the crash run); on real R2 the procedures
   test. Each new
   rule was checked to fail without it (invariants 81, 83, 84, 86, 88).

**Published, then 0.22.1: nothing to set up** (ADR-024). 0.22.0 went to PyPI for all five
platforms (npm refused the release's paths; fixed). The owner installed it on Windows, where
`pondra` wasn't found: a user install puts it in a folder that isn't on PATH, and pip can't
change PATH. And `pip install pondra` without pyarrow couldn't answer a query. 0.22.1 adds:

- one-line installers on every release (`irm …/install.ps1 | iex`, `curl …/install.sh | sh`), which
  put the binary in the user's own folder and that folder on PATH (on Windows, this terminal's
  too);
- `python -m pondra`, which works wherever pip put the binary, and `--add-to-path` for the short
  name;
- rows as JSON when pyarrow isn't installed;
- in the shell, the other lakes in its folder attached as its databases for the session, `FROM t`
  read as `SELECT * FROM t` (it answered with no columns), and a change to an attached lake's
  table saying which lake and where it runs.

`tools/try_packages.sh` and `smoke.py` try all of it on Linux, Windows and macOS on every push
(invariants 89 to 92, each checked to fail without its fix).

**0.22.2: deleted keys stay deleted on a cluster.** A keyed table's first tiering round deals a
job per node, and each took its file for the table's first, dropping its delete markers; the keys
deleted in the later jobs' part of the log came back, and an adding-up view lost the part of an
UPDATE that changed a total but not a count. Only the job starting where the table's files end
writes the first file now (invariant 93); `harness.py deal` fails without it on every run.

**0.23.0: one name, one meaning** (ADR-025). Python's `db.view` made a materialized view while
SQL's `CREATE VIEW` and a frame's `to_view` make a stored query. Now every client uses SQL's
words: `view` is a stored query unless `materialized=True`, from the connection (SQL or a frame)
and from a frame alike, and in JavaScript; `db.write_table` is a frame's `write_table`; JavaScript
calls a procedure with `call`, as Python does. 0.22's `db.view(…, window=…)` still works, with a
warning. `frames_check.py` checks it (section 5).

**Round 21 shaped tables further and took on more of Flink** (ADR-022):

1. **Columns that change without a file rewritten.** `RENAME COLUMN`, `DROP COLUMN`, a dropped
   name added again, and widening `ALTER COLUMN … TYPE`, on any node while rows stream in. The
   catalog keeps each column's stored name and what SQL calls it; reads alias one to the other,
   so a scan costs the same (10 M rows: 0.033 s before, 0.034 s after). A writer still using an
   old name has it left out, never mistaken for the column now stored under it. DuckDB (Delta and
   Iceberg), PyIceberg and delta-rs's DataFusion reader read renamed, dropped and widened columns
   right (Delta: column mapping; Iceberg: field ids); delta-rs's pyarrow reader and Polars say
   they can't map columns instead of guessing.
2. **Materialized views filled from the rows already there, every row once**, even with rows
   streaming into several nodes as the view is made, or the leader killed meanwhile. The
   sequencer now holds every flush to the views: a flush packed with other views than its tables
   have goes back to its node. Without that check every view in the test missed rows. A GROUP BY
   view over 10 M rows is filled in 2.6 s.
3. **Deduplication by event time:** `order_by = 'ts'` on a keyed table keeps each key's latest
   event, not its last arrival; late rows, deletes and compaction follow it. 200 k keys over five
   generations of files and the log read in 0.14 s (by arrival 0.03 s).
4. **`SELECT *` on a keyed table** leaves `_deleted` out unless a query names it.
5. **Nexmark against Flink** (q1, q2, q5, q7, q11 over bids; `logs/round21/nexmark-*.jsonl`):
   10 M bids in 8.7–10.9 s on Pondra, taken over HTTP and written to the lake, the answers equal
   to DuckDB's; Flink 2.3 (PyFlink MiniCluster, generating the bids itself, blackhole sinks)
   24.3–25.0 s. 4 M bids: 3.8–5.1 s against 11.0–11.2 s.
6. **The owner's cluster benches:** on round 20, loading TPC-H 105.9 s (442 s on round 19), one
   node 24.5 s, as the cluster decides 24.8 s, spread anyway 34.7 s; on round 21, loading 100.5 s,
   one node 21.7 s, as the cluster decides 21.5 s (3 queries spread, 9 MB moved), spread anyway
   38.9 s. Every answer the same both times.
7. **A DataFrame API, designed** (`docs/dataframe-api.md`): `pondra.frame` (Polars-style) and
   `pondra.spark` (PySpark's names) on one expression tree that compiles to SQL; built next round.
8. **MIT OR Apache-2.0.** The packages carry both licenses; the release workflow publishes to
   PyPI (trusted publishing) and npm once the owner's accounts are set up.
9. **Tests** (`logs/round21/`): the local suite with the new `columns`, `fills` and `dedup`
   passes — `harness.py all`, failover ×4, users ×3, race, isolate, spread, latency,
   `open_check`, `asof_check`, `skew_check`, `shuffle_spill`, `stream_check` (ingest with views as
   round 20's), freshness, the big crash run, `smoke`, and `anywhere_check` on the glibc 2.17
   build (the new wheel, npm packages and notebook); 12 tests on simulated R2 (the three new
   ones, `changes`, `schemas`, `streams`, `windows`, `alter`, `clients`, failover and users with
   replicated acks, the crash run) and the three new ones on real R2. Real R2 caught two
   publishing rounds writing one Iceberg version (a `CHECKPOINT` beside a tiering round):
   publishing now runs one round at a time.

**Round 20 took the owner's questions after round 19 as its list** (ADR-021):

1. **Far fewer objects.** A bucket bills, and rate-limits, every request. A trickle of one-row
   INSERTs (20 a second for 2 minutes, a table publishing Delta and Iceberg) wrote 9,244 objects
   and left 5,962 on round 19; now 2,667 and 217 (`logs/round20/objects.txt`): `INSERT … VALUES`
   goes through the log instead of a Parquet file each, the catalog's write-ahead log is cleared
   every minute instead of every 10, and tiering runs at most every 10 s by default (a million
   rows waiting: within a second). What remains is about one write per acknowledged commit.
   `/metrics` counts the store's writes, lists and deletes.
2. **System columns cost tiering a third of what they did.** Tiering 8 M rows, dist builds side by
   side (`logs/round20/tiering-r18-r19-r20.txt`): round 18 0.88–0.98 s, round 19 1.45–1.55 s,
   round 20 1.10–1.16 s; ingest the same. File statistics now come from the Parquet footer the
   writer builds anyway, not a second pass over every column — that pass cost more than encoding
   the system columns (`_row_id` alone, delta-encoded, costs next to nothing:
   `logs/round20/syscols.rs`).
3. **A PRIMARY KEY goes with `partition_by` and `cluster_by`** — the owner's `CREATE TABLE`, as
   written. Each tiering round's newest rows go into a file per partition, sorted by the cluster
   columns then the key; a newer round's row for a key shadows an older one's in any partition, so
   a row that moves to another day has one current version. `CHECKPOINT` compacts a published
   keyed table so Delta and Iceberg see it as it is.
4. **`cluster_by` over two or more columns orders rows along a Hilbert curve,** as Databricks'
   liquid clustering does. 4 M rows read from Parquet (`logs/round20/clustering-two-columns.txt`):
   a range of the second column 21.9 ms unclustered, 23.3 ms sorted by the first alone, 11.4 ms
   Hilbert; one value of the first column 39.9 / 5.5 / 14.4 ms. Each column gets about half of
   what sorting by it alone gives, and the second column is no longer left out.
5. **`COPY` over the Postgres protocol:** `COPY … TO STDOUT` (text, CSV, binary) and `COPY … FROM
   STDIN` (text, CSV); rows encoded a batch at a time; DECIMAL sent as NUMERIC in text and binary;
   the ADBC Postgres driver works. 1 M rows × 4 columns (`logs/round20/pg-bench.txt`): psycopg
   0.72 s, the ADBC Postgres driver (COPY binary → Arrow) 0.67 s, Flight SQL through ADBC 0.10 s,
   HTTP Arrow 0.05 s — Postgres for everything that speaks it, Flight for speed.
6. **Streams joined as they arrive** (`join = 'streams'`): a row of either table pairs with the
   other's when it arrives and with those that come after, each pair once, exactly-once through a
   leader restart; `within_secs` bounds what is read. The leader runs it right after commits: a
   payment's pair shows 14 ms after its ack (p50; 24 ms at worst, `logs/round20/stream-join-latency.txt`).
   **Sliding windows** (`slide_secs`): the view keeps panes, each window combines the ones it covers.
7. **The owner's cluster bench on round 19** (3 GitHub runners, TPC-H): one node 15.7 s, as the
   cluster decides 18.0 s (round 18: 46.6 s), spread anyway 39.5 s, 80 MB moved instead of 938,
   every answer the same. It found loading 4× slower (442 s against 108 s): a `pondra sql` bulk
   INSERT's files were rewritten by the leader to add row ids. Now `pondra sql` takes a block of
   ids from the leader first (6 M rows locally: 2.7 s → 1.3 s). And a query that has run both
   ways goes the faster way next time.
8. **Tests** (`logs/round20/`): the local suite with the new `files`, `layouts`, `clusters`,
   `copies` and `streams` passes, with `open_check`, failover ×3, users, race, isolate, spread,
   latency, `asof_check`, `skew_check`, `shuffle_spill`, `stream_check`, freshness, the big crash
   run and `smoke`; 15 tests on simulated R2 (the five new ones, `changes`, `schemas`, failover
   and users with replicated acks, the crash run among them) and the five new ones on real R2. On
   R2, 200 one-row INSERTs took 179 s (a durable ack is a PUT): one Parquet file per tiering round
   (20), 2.5 object writes per INSERT counting what the node writes meanwhile anyway.

**Round 19 made every row changeable** (ADR-020), the owner's request, and kept a cluster from
being slower than one node:

1. **`UPDATE`, `DELETE` and `MERGE` on every table**, from any node, Postgres, `pondra sql` or the
   shell. On an append table a row changes by version: the new one goes in like any row, the old
   one into the hidden `{t}$deleted`, and every read leaves it out; nothing is rewritten when a row
   changes. The leader carries a change out from one snapshot, in one commit, exactly once per
   job id. `MERGE` takes `WHEN MATCHED [AND …]`, `WHEN NOT MATCHED`, `WHEN NOT MATCHED BY SOURCE`,
   and refuses a row that two source rows match.
2. **System columns on every row:** `_row_id` (kept through an UPDATE or MERGE; ids handed out in
   blocks, so no one coordinates per row), `_version` (the commit), `_created_at`, `_updated_at`.
   `SELECT _row_id, * FROM t`; `SELECT *` leaves them out. Ingest costs what it did (the log
   keeps a batch's fresh ids as one number); tiering writes the four columns, about half again
   the CPU per row, into files 2.5% bigger (`logs/round19/ingest-and-tiering.txt`).
3. **Streaming follows every change.** Views that add up subtract the old rows (a group a change
   empties goes); row-by-row views carry their source rows' ids and change as their source does.
   `/watch/{t}?changes=true` and MCP's `changes` give Delta-style change rows (`insert`,
   `update_preimage`, `update_postimage`, `delete`). A view that can't take a row back (windows
   emitted once, min/max, joins) makes a change refuse, with the reason.
4. **Purges** rewrite the files holding changed rows without them: every round for published
   tables, so Delta and Iceberg readers see the change; otherwise once 100,000 changed rows wait,
   or `CHECKPOINT`. On 10 M rows: an UPDATE of 100,000 rows 0.26 s, a MERGE of 100,000 0.9–1.2 s;
   a scan 0.07 s unchanged, 0.08 s with a row changed, 0.17 s with 1% changed in every file until
   the purge (2.6 s), 0.07 s after.
5. **A query spreads only when it pays** (`guard.rs`): what its plan would move, at the measured
   speed of the slowest link, against what it saves on one node. By the last cluster bench's own
   numbers, the twelve TPC-H queries that shuffled (about 78 MB each over 50–150 MB/s, against
   under a second saved) would stay on one node; the next run measures it. `?spread=1` still
   forces a spread.
   The cluster bench now waits for a settled lake and times each query three ways.
6. **From the owner's second Windows session:** `CREATE DATABASE`, `ATTACH` of an empty folder
   (a new lake), the lake named after its folder on Windows (`mylake.dbo.t`), the shell reading
   local files (`SELECT * FROM 'D:\…\x.csv'`, `MERGE … USING 'new.csv'`), `CHECKPOINT`, and `ALTER
   TABLE … SET (publish, cluster_by, ttl)`. Found on the way: a spread query over a changed table
   read the changes as of the slice, not the query (fixed, invariant 59), and a DataFusion switch
   set the wrong way left joins' dynamic filters on inside shuffles (invariant 63).
7. **Tests** (`logs/round19/`): the local suite (`harness.py all` with the new `changes` and
   `guard`, `open_check`, failover ×3, users, race, isolate, spread, latency, `asof_check`,
   `skew_check`, `shuffle_spill`, `stream_check`, freshness, the big crash run, `smoke`), 9 tests
   on simulated R2 (`changes`, `schemas`, failover and users with replicated acks, the crash run
   among them) and `changes`, `schemas` and `guard` on real R2 all pass. On R2, `changes` needed
   a longer statement timeout: a change waits for a tiering round in progress, which with a purge
   every round took over 30 s once (ADR-020, "What is still open").

**Round 18 made it a database you can shape** (ADR-019), from the owner's first session on
Windows:

1. **Schemas and three-part names.** A lake is a database: `CREATE SCHEMA sales`, then
   `sales.orders`; a plain name is schema `public`, so older lakes read as before. A table is
   `t`, `schema.t` or `lake.schema.t`, where the lake is this one (its folder's name) or one
   attached with `--attach`, which is now a database of its own (`other.eu.sales`; `other.sales`
   still means its `public.sales`). `ATTACH 'dir' AS name` does that from SQL, for every node
   of the cluster, kept in the lake: queries and writes across databases, from any client.
2. **DDL in SQL**, from any node, Postgres, Flight SQL, MCP or `pondra sql`: `CREATE`/`DROP
   SCHEMA [CASCADE]`, `DROP TABLE` (refused while a view or task reads the table),
   `CREATE TABLE … AS SELECT`, `CREATE [OR REPLACE] VIEW` (a stored query) and `CREATE
   MATERIALIZED VIEW … [WITH (window = …)]` (the streaming view). A query over a stored view
   spreads over the nodes, each node's view reading its share.
3. **Every door lists the schemas:** Postgres (`pg_namespace`, `pg_class`), Flight SQL (and ADBC
   ingest into a schema), the Iceberg REST catalog (a namespace per schema and per attached
   lake), MCP, and the shell's `.tables`.
4. **Two bugs found on the way:** a table re-created after `DROP TABLE` read the dropped one's
   rows still in the log; and a follower that opened a new lake before its leader had made the
   catalog exited, which is how the owner's second 3-node run on GitHub's runners lost a node.
   Both are fixed, each with a test that fails without the fix.
5. **Windows and macOS:** the node's memory limit and resident memory came from Linux's `/proc`
   (Windows got a fixed 4 GiB and 0 bytes); they come from the OS now, and every push runs
   `tools/smoke.py` on Windows, macOS and Linux.
6. **The cluster bench measures the network** (bytes sent between nodes, time waiting for them,
   the runners' ping and bandwidth). On round 18's code, 3 GitHub runners, TPC-H SF10: every
   answer right, but 46.6 s against one node's 23.1 s. The runners talk over the public internet
   (17–54 ms, 51–150 MB/s): queries that move little take what one node does, and the 12 that
   shuffle 936 MB pay about 15 ms per MB. Planning queries costs what it did
   (`logs/round18/query-latency.txt`).

**Round 17 made it install anywhere** (ADR-018):

1. **A Linux binary for glibc 2.17** (`cargo zigbuild`). It runs on any Linux from 2014 on:
   tested in CentOS 7 and Ubuntu 22.04 containers, where round 16's needed glibc 2.38 and failed.
   TPC-H runs as fast with it (1.78 s from memory, 2.83 s from Parquet at SF1).
2. **`pip install pondra` and `npm install pondra`**, built by `tools/package.py`: the binary in
   a wheel (as a script, like maturin's bin wheels) and in npm packages (esbuild's pattern).
   `pondra.local("lake")` in Python, or `await local("lake")` in JavaScript, starts a node in the
   background and returns a client. Each package was tried in a fresh environment: a virtualenv,
   an npm project, CentOS 7, and Ubuntu 22.04 with Python from apt. So was
   `examples/quickstart.ipynb`, from its own `%pip install` cell. Not published yet: the names
   and the repository are the owner's call; `.github/workflows/release.yml` builds all five
   platforms and publishes on a version tag.
3. **A shell.** `pondra` (or `pondra <folder | s3://…>`) opens a SQL shell, DuckDB-style, over a
   node it runs on the lake. A statement ends at a `;` outside strings and comments, and errors
   don't end the session. A session on a new local lake takes 0.14–0.44 s from start to stop;
   on the R2 simulator, two sessions (create, insert, query; then insert, query) take 12 s.
4. **A node lives as long as whoever started it** (`--stop-with-stdin`). When the shell, Python
   or Node.js exits, or is killed with `kill -9`, the node's input closes and it stops, giving up
   leadership at once. A second shell on the lake writes 0.10 s later, and Python reopens the lake
   for writes 0.21–0.24 s after it was killed. Before, a killed leader left a 5 s lease to wait
   out.
5. **Small machines, and proxies.**
   - The SSD tier takes a quarter of the free disk, at most 20 GB, not a fixed 20 GB.
   - A missing CA bundle is a warning, not a crash (Ubuntu's minimal image).
   - The shell and the clients reach their own node directly even when the environment names a
     proxy. Many companies' notebooks do; there the shell waited for its node until it gave up,
     and `local()` failed.
6. **Both flaky tests fixed.**
   - A Kafka fetch from before the oldest segment kept now reads from it, as Kafka does. Before,
     librdkafka looped on a cached earliest offset.
   - `sum` over DOUBLE now gives the same answer in any order, on any number of nodes
     (`fsum.rs`: each addition's rounding error kept in a second double). TPC-H q15 was wrong in
     8 of 20 runs before and 0 of 20 now. Sums equal Python's `math.fsum` on every node, and
     `[1e16, 1, -1e16]` adds up to 1 (it was 0). TPC-H SF1 totals didn't move beyond run-to-run
     noise: 1.79–1.89 s / 2.79–2.98 s against 1.78–1.88 s / 2.95 s without it, with DuckDB at
     2.99 s. q18, a sum over 1.5 M groups, went from 0.13–0.14 s to 0.15–0.19 s.
7. **Measured and left out: a "lite" build.** The Kafka, Flight, Postgres and MCP front doors'
   libraries are under 1% of the binary. Its bytes are the SQL engine's: the SQL parser 17 MB,
   generic code 23 MB, Arrow and DataFusion ~25 MB.
8. **What the tests and the review found:**
   - The wheel's binary lost its executable bit, until the zip entries said they were regular
     files.
   - Ubuntu's minimal image has no CA certificates, and the HTTP client panicked on it.
   - A `;` inside a string ended the shell's statement.
   - Asking again for a view with other options was silently ignored; now it is refused.
   - The JavaScript client's `close()` didn't wait for the node.

**Round 16 put streams on their own time: watermarks from the data, session windows,
point-in-time joins** (ADR-017):

1. **The watermark is the newest event time, less the lateness** (as Flink's bounded
   out-of-orderness). It comes from the source's own event-time column: what its files' ranges
   say, then each new log segment, once. A window now closes as soon as event time is past its
   end by the lateness, not when a window after the next one starts; rows that far out of order
   still count, and each window is still emitted once.
2. **Session windows** (`POST /views/{v}?session=ts&gap_secs=30&lateness_secs=5` over a plain
   `SELECT user, count(*) … GROUP BY user`): each user's rows with no gap of 30 s between them
   are a session, emitted once, whole, with `session_start` and `session_end`, when the watermark
   passes its last row plus the gap. A session spanning many rounds is emitted once; a row out of
   order within the lateness joins its session; a late row inside a session already emitted is
   left out (without that, the session came out twice).
3. **`ASOF JOIN`** (`asof.rs`): `trades t ASOF JOIN quotes q MATCH_CONDITION (t.ts >= q.ts) ON
   t.sym = q.sym` gives each trade the quote of its moment (also `>`, `<=`, `<`; NULL if none).
   DataFusion plans it as a LEFT JOIN carrying a marker, which a join of Pondra's replaces: the
   looked-up side per key in time order, a binary search per row, in one table or one per
   partition. 1 M trades × 200 k quotes: **0.18–0.32 s** on one node over four runs (DuckDB
   0.24–0.26 s), 0.34–0.39 s on three (one box). Every answer equals DuckDB's in all eight shapes
   tested, on one node and three,
   planned four ways. In a view over a stream, each event gets the table's row of its own time;
   for a few rows (a flush) only their keys' rows are looked up.
4. **One stream, three views** (`tools/stream_check.py`): 2 M clicks from 4 producers, with a
   window, a session and an as-of view: 0.37 M clicks/s in (2.4 M with no views; 1.03 / 1.08 /
   0.58 M with each alone), every click in exactly one window and one session, each enriched
   with its tier of that moment, and the last window and session out 0.51 s and 0.73 s after
   the click that closed them (on real R2: 2.5 s and 3.0 s, with every durable ack a PUT).
   TPC-H on one node is unchanged: 1.90–2.01 s from memory and 2.99–3.17 s from Parquet, with
   round 15's binary at 1.91 / 2.97 s run beside it (DuckDB 3.18–3.26 s).
5. **What the tests found:**
   - A WHERE on the quote was pushed into the lookup — the latest quote that passes the filter
     instead of the latest quote, filtered — because DataFusion turns such an outer join inner
     first; as-of joins now stay outer (`asof_check.py` against DuckDB caught it).
   - A view that took a string from a table it joins failed every flush: strings read from files
     are views, and the view's rows were fitted to its table without a cast. Views and tasks now
     cast their rows (`stream_check.py` found it).
   - A review found the Postgres extended protocol's Describe and sort-merge join plans (the
     out-of-memory retry) missed the rewrite; both covered now.

**Round 15 made the data know where it is: tables split by the ranges of a key they share,
hot keys shared out, distinct values counted** (ADR-016):

1. **Rows keep the order they arrive in.** An INSERT writes each partition of its query to its
   own files, and merges read files one after another, so data that arrives in order (by time,
   by an id handed out in sequence) lands in files that each hold a narrow range of it. Loading
   TPC-H SF1 went from 30.6 s to 7.2 s on the way.
2. **Big tables sliced by the ranges of a key they share** (`ranges.rs`). The biggest table of a
   query is cut into ranges of a column its files hold narrowly, even by bytes, and every other
   table with a matching column is cut by the same ranges (small ones too). Each node keeps the
   rows in its range, with NULLs in the first. A join, `EXISTS` or GROUP BY on that key then runs
   where the rows are, with no shuffle (`Spread::Ranged`). On three nodes, 13 of TPC-H's 22
   queries now run this way, and all 22 took **4.81 s** (round 14: 5.85 s). With every table
   sliced: **5.70 s** (7.66 s). q21 went from 0.96 s to 0.50 s, and from 1.97 s to 0.45 s sliced.
3. **Hot keys shared out** (`skew.rs`). After both sides of a shuffled join are hashed, a
   partition much bigger than the rest stays split where it was hashed, and its other side goes
   to every node. With a key that holds half a table, the busiest of three nodes read 1.27× the
   average instead of 1.95×. The answers didn't change (`tools/skew_check.py`).
4. **NOT IN across the nodes**: the subquery's rows go to every node. TPC-H is now 22 of 22 even
   with every table sliced.
5. **Distinct values counted** (`sketch.rs`). Every file gets a small HyperLogLog sketch of each
   key-like column, folded into its table's as the leader commits it. The join order divides by
   these counts rather than by a column's range. The range said nothing about strings, and gave
   213 M distinct shipping dates where there are 2,526. Two badly written queries the rule used to
   miss now cost what their well-written forms do. One node: TPC-H SF1 **1.69 s** from memory and
   **3.07 s** from Parquet (1.84 / 3.30; DuckDB 3.38 s in the same run), every answer checked
   against DuckDB's (q15's DOUBLE comparison, which flips from run to run on one node, aside).
6. **Tried and left out:** telling DataFusion the files are in order (TPC-H got slower: 3.30 s →
   3.85 s from Parquet), and smaller row groups (mixed). ADR-016 has the numbers.
7. **What the tests found:**
   - A node whose files all held one value of a column saw it as constant and planned the query
     differently from the others; slices now report no order.
   - A tiered file holding NULL keys went only to the node owning its range, so the NULLs were
     lost; files now say which columns hold NULLs.

**Round 14 let any query run across the nodes, with the same answer every time** (ADR-015):

1. **The plan decides what spreads, not the SQL.** Round 13 let one plain SELECT of inner joins
   over append tables run across the nodes — 8 of TPC-H's 22 queries. Now every table a query
   reads is found (joins, subqueries, CTEs, unions), it is sliced on its biggest append table, and
   keyed tables are read whole; then the physical plan is checked operator by operator. Every
   join type has one rule: whatever a join emits by looking at *all* of the other side needs that
   side whole, or both sides shuffled by the key. **All 22 TPC-H queries now run across three
   nodes (19 shuffled, 3 gathered), every answer equal to one node's**; with every table sliced
   (`PONDRA_BROADCAST_MB=0`), 21. `harness.py scale` spreads 23 of 23 query shapes, nine of them
   new (LEFT, FULL, `IN`, `NOT EXISTS`, `NOT IN`, a scalar subquery, a CTE with `UNION ALL`, a
   keyed table on the right of a LEFT JOIN).
2. **Two new exchanges.** *Own*: a table every node read whole that must meet a sliced one on the
   kept side of an outer join — each node keeps its own keys' rows, and nothing moves.
   *All-gather*: a final aggregate over partial ones (a subquery's `avg`) — every node gets the few
   partial rows and computes the same answer.
3. **A join that doesn't split moves what it needs**, not the whole query: a semi or anti
   join planned to stream a sliced table past a collected one is rewritten into one that hashes
   both sides by its key (`by_key`), where before the whole query fell to the plan that shuffles
   every join; and an inner join whose collected side is sliced gets that side sent to every node
   (`collected`) instead of both sides shuffled. Across three nodes on one box, q17 went from
   1.11 s to 0.21 s, q21 from 2.12 s to 0.95 s, and all 22 queries from 8.0 s to 5.8 s — with every
   table sliced, as a bigger lake would have them, from 12.1 s to 7.7 s.
4. **Scalar subqueries answered between steps.** What uses a subquery's answer can sit below a
   shuffle (q22 filters customers by an `avg` before shuffling them), so a shuffle answers the
   subqueries itself, on every node, as soon as their exchanges are done.
5. **The same answer every time.** Each exchange now hashes once into `nodes × partitions`
   buckets — the partition DataFusion itself would choose, so nothing is re-partitioned on arrival
   — and a partition reads the nodes' buckets in node order. TPC-H q15 over DOUBLE columns
   (`total_revenue = max(total_revenue)`) returned no rows in one run of three on a single node;
   across three nodes it returns the same row every time.
6. **Whole tables at one snapshot.** The coordinator sends the tables it doesn't slice along with
   the slices, as of the log position it planned at; a node that is behind waits for it.
7. **What the new tests found:** a shuffle's pieces kept every string of the batch they were cut
   from (`Utf8View` buffers), so q21 wrote 2.5 GB per node and filled the disk — now 12 MB, and
   62 s became 2.2 s; spill sizes were counted by buffer (145 MB for 10 MB); abandoning a shuffle
   never cleaned up (`?drop=1` against a handler that wanted `true`); a coordinator that failed
   its own step dropped itself from the shuffle; and, only on real R2, a small table decoded in
   memory on one node and read from Parquet on another made two nodes plan the same query two
   ways (now a table read whole reports its size from the catalog, and every node plans with the
   coordinator's partition count — machines with different core counts can share a query too).
8. **A cluster benchmark anyone can start.** `.github/workflows/cluster-bench.yml` runs N nodes on
   GitHub-hosted runners joined by Tailscale against an R2 bucket, loads TPC-H and times each query
   on one node and across the cluster (`tools/cloud/actions/`). Nothing has run on several machines
   yet; this is the kit for it.

**Round 13 made queries across nodes something you can rely on, and gave Pondra a join order of
its own** (ADR-014):

1. **A shuffle is bounded by disk, not by memory.** Rows going from one node to another are held
   in pieces (`PONDRA_SPILL_MB`, 64 MB); a piece past that size is written to the node's scratch
   folder, sent length-prefixed, and read back a piece at a time by whoever needs it. Nothing —
   the stage that makes it, the wire, the coordinator that finishes the query — ever holds a whole
   bucket. Three nodes shuffling 4 million distinct keys with the piece size set to 1 MB: the
   answer is identical to one node's, 97 MB went to each node's disk, the scratch is empty
   afterwards, and no node passed its 1 GB budget.
2. **A step that fails is run again; a node that fails is dropped.** A step reads buckets that are
   kept, not taken, so it is the same work every time: one retry after 250 ms, and if that fails
   too, the whole shuffle runs again without that node. A node killed just before a query changed
   nothing about the answer.
3. **Work dealt by size.** Manifests and files go to whichever node holds the fewest bytes so far
   (ties keep the old round-robin, so a time range still spreads). In the spill test this alone
   turned 193 MB / 97 MB of spilling into 97 MB / 97 MB. What skew is left —
   a key that holds much of the table — is measured as `pondra_shuffle_skew`, not corrected.
4. **A join order of Pondra's own** (`PONDRA_JOIN_ORDER=0` to turn it off). The catalog already
   knows each table's rows and each column's range, so `Pruned` reports them as statistics,
   including a bound on a column's distinct values. Inner joins are then rebuilt smallest-first,
   costing each step as `rows(a) × rows(b) / distinct(key)` — which is what catches the joins that
   *expand* (TPC-H q5 relates customers to suppliers by nation: 25 values, 400 suppliers each).
   The order the query wrote is costed the same way and kept unless the new one is cheaper.
   Ten queries written badly — the same query with its tables named biggest-first — answer the
   same and cost 1.75–1.83 s together with the rule against 1.87–1.95 s without it, with none of
   them slower; TPC-H's own 22 answers and its 3.35 s SF1 total are unchanged, because those
   queries name their tables well already. A modest win, and ADR-014 says why: DataFusion's
   physical planner already picks build sides from exact Parquet row counts.

**Round 12 made one node fast, gave columns something other than numbers to hold, and made
publishing cost what changed** (ADR-013):

1. **TPC-H on one machine, against the single-node engines** (SF1 and SF10, 2 cores, 8 GB, best of
   three, every answer checked against DuckDB's):

   | From Parquet files | SF1 | SF10 | | From memory | SF1 | SF10 |
   |---|---|---|---|---|---|---|
   | **Pondra** | **3.19 s** | **38.0 s** | | **Pondra** (hot columns) | **1.96 s** | **35.9 s** |
   | DuckDB | 3.36 s | 39.8 s | | DuckDB (native tables) | 1.80 s | no room on this machine |
   | Polars | 3.78 s | q9 out of memory | | | | |
   | Polars (streaming) | 3.18 s | 42.8 s | | | | |
   | Daft | 6.11 s | 89.0 s | | | | |
   | Bodo | 8 queries differ, 1 crash, minutes each | — | | | | |

   Round 11 ran SF1 in 6.45 s against DuckDB's 4.18 s on the same files. What closed the gap:
   strings read as views, LZ4 instead of ZSTD, decimal literals, three planning rules of Pondra's
   own (a semi join runs on the table it filters; a grouped subquery groups only the keys the join
   keeps; a filter's conditions run cheapest first) and one physical rule (the groups a HAVING
   keeps make the hash table). The "from memory" column is the columns queries read lately, kept
   decoded in memory (`hot.rs`) — DuckDB's own trick, reported separately.
2. **Anything in a column.** Files in the lake (`PUT /files/…`, `files('photos/')`,
   `file_read(path)`), `BINARY` with `byte_length`/`sha256`/`md5`/`encode`, `VARIANT` for
   semi-structured text, `Float32[]` vectors with `cosine_similarity`/`l2_distance`/`dot_product`
   (published as a Delta array and an Iceberg list; delta-rs, DuckDB, Polars and PyIceberg read
   them back), `ai_complete`/`ai_embed` against any OpenAI-compatible endpoint, and functions of
   your own on an Arrow Flight server (`tools/udf_server.py`: forty lines of Python).
3. **Publishing that costs what changed.** Delta and Iceberg metadata is now derived from Pondra's
   immutable manifests: one Iceberg manifest per Pondra manifest, and a Delta state that names
   manifests instead of files. A table of **a million files** published in both formats keeps a
   20 KB catalog entry, a 24 KB Delta state and a 70 KB Iceberg state, and a publish round takes
   **17 ms**; at 20,000 files the first publish (20,001 adds) takes 0.33 s and the next 1 ms.
4. **Memory that holds.** The hot columns come out of the query budget and are given back when the
   node's own memory runs high; merge jobs are bounded in size and number; the query budget
   defaults to a third of RAM (it was half) because Parquet decoding and the batches in flight
   are not counted in it. Before these, TPC-H SF10 could take a node down.
5. **Orphan collection in a few megabytes** (a Bloom filter of the paths in use, ten bits each),
   and a node stopped with Ctrl-C or SIGTERM hands leadership over at once instead of leaving the
   next one to wait out the lease.

**Round 11 took on what stood between it and petabyte tables** (ADR-012):

1. **Table metadata that stays small.**
   - Every file's column ranges are kept.
   - Past 128 files, a table's file list goes into immutable manifests behind one list object.
   - A table given a **million files** (1 PiB on paper) commits a 20 KB catalog entry: tiering
     commits 11 ms, INSERTs 4 ms, as with a handful of files.
   - A query over today skips the million without opening one: 13 ms.
2. **Partitions:** `partition_by = 'day(ts)'` (or a column). Every file holds one partition, and a
   day's query read 2 files of 237.
3. **Memory limits:** `--memory-gb`. Aggregations, sorts and joins bigger than memory spill, or
   switch to a join that can: a 2 M-group aggregation, a 3 M-row window sort and a 3 M × 3 M join
   ran within 50 MB.
4. **Shuffles.** Many-group aggregations, joins of big tables, DISTINCT and windows by key now
   run on every node in stages; small tables are broadcast. 14 query shapes on 3 nodes: all
   equal one node, 11 shuffled.
5. **Arrow Flight and Flight SQL.**
   - ADBC and JDBC drivers and pyarrow connect.
   - **15.7 M rows/s in, exactly-once**, and 9.1 M rows/s out, on one node.
   - A table's log is a columnar stream: a subscriber has each new commit in **2.6 ms**.
6. **`GET /metrics`** for Prometheus, and a kit to benchmark a cluster of several machines
   (`tools/cloud/`).

**Round 10 opened the doors other systems use** (ADR-011):

1. **The Kafka protocol.**
   - Kafka producers write to tables:
     - JSON values become rows;
     - idempotent producers are exactly-once;
     - Debezium change events and tombstones become upserts and deletes.
   - Consumers and consumer groups read the log back.
   - Measured on one box with 3 nodes, via librdkafka:
     - **~0.8 M events/s**, exactly-once;
     - ack **1 ms p50**, and a consumer on another node gets the event 1 ms later;
     - on real R2: 461k events/s, ack 1 ms p50 (replicated).
2. **An Iceberg REST catalog.** PyIceberg and DuckDB attach a node by URL.
3. **`ALTER TABLE … ADD COLUMN`**, under load, with Delta and Iceberg following.
4. **Event-time windows that close:** each window is emitted once, final, past a watermark.
5. **JSON functions** (`json_get`, `->>`) over text.

**Round 9 let every machine write and every tool connect** (ADR-010):

1. **Writes from anywhere, with the same capabilities.**
   - A machine that can reach the bucket but not the leader writes through the bucket inbox
     (1.0 s locally, exactly-once).
   - Several clusters share one bucket, each leading its own lake and attaching the others
     (cross-lake joins; writes recorded by the owning leader).
2. **SQL writes everywhere:** `CREATE TABLE`, `INSERT`, `UPDATE`, `DELETE`.
   - They run on any node, over the **Postgres protocol** (psql, psycopg 2/3, asyncpg,
     SQLAlchemy tested), over **MCP** for AI agents, or from `pondra sql` on any machine.
   - A **Python client** reads into pandas, Polars and Arrow.
3. **What competitors are building next, answered where it was cheap.**
   - Fluss 1.0 lists the Postgres protocol and MCP as future work; Pondra has both now.
   - Vector search in SQL (`cosine_distance`), as Flink 2.2 added `VECTOR_SEARCH`.
   - Read / write / admin tokens on every door. SQL sent to a node can no longer touch the
     node's disk.
4. **Keyed tables got cheaper and more capable:**
   - size-tiered compaction (3x fewer bytes written);
   - row TTL;
   - the log as a replayable change feed.

   Replicated acks gained `--fsync` and were tested with 3 replicas.

**Round 8 made the native lake the default and writes fast on any storage** (ADR-009):

1. **Millisecond writes without S3 Express.** With `--ack replicated`, a write is acknowledged once
   two nodes hold it and reaches the bucket a moment later. On real R2: **4 ms p50** (was 299 ms),
   and 64 writers went from 6,400 to **87,200 events/s**. A new leader recovers what followers
   held — up to 279 commits in one failover — with 0 lost and 0 duplicated.
2. **Native first; Delta and Iceberg on request.** Pondra's readers use the catalog directly:
   nodes see a write 10–15 ms after the ack, on local disk or R2. Tables that ask are also
   published as Delta and, new, **Iceberg**. Six outside readers (delta-rs, Polars, DuckDB × 2,
   PyIceberg) match Pondra row for row, on local disk and real R2.
3. **Writes from any machine.** `pondra sql "INSERT INTO t SELECT …"` runs the query where it's
   typed and has the leader record the files — or records them itself when nobody runs. A node
   starting on an idle lake leads at once (was a 30 s wait).
4. **`cluster_by` for append tables:** selective filters 6–11x faster.
5. **Freshness compared like for like** with Fluss (memory/SSD vs memory/SSD, object storage vs
   object storage): see `docs/comparison-spark-flink-fluss.md`.

**Round 7 made it a serving engine at Lakehouse//RT speed, and measured it against Spark on TPC-H:**

1. **Point reads skip SQL:** 0.14–0.22 ms, and **20,000–36,000 lookups/s on two cores** (was
   ~200/s). SQL point queries take the same path.
2. **Repeated dashboards come from a result cache** that is exact until the next commit: about
   20,000–38,000/s. `?stale_ms=` trades exactness for throughput when tables change every few
   milliseconds.
3. **Keyed tables read as anti-joins** instead of grouping every row by key: dashboards on them
   went from 420 ms to 6–42 ms.
4. **TPC-H SF1: all 22 queries in 5.9 s**, against 58–65 s for Spark 4.2 on the same machine.
   The answers are the same; DuckDB, the single-node reference, takes 3.8 s.

The honest comparison with Spark, Flink, Fluss and Lakehouse//RT — where Pondra wins today, where
it doesn't yet, and the plan — is `docs/comparison-spark-flink-fluss.md`.

**Round 6 opened the lake to other engines and made first reads on object storage fast:**

1. **Every table is also a Delta Lake table.** Spark, Databricks, DuckDB, Polars, delta-rs,
   Trino and Athena read `<lake>/data/<table>` without Pondra. Three independent readers were
   checked row for row.
   - **Local disk:** a Delta version with a new row exists **17 ms** after the ack, and delta-rs
     reads it at **30 ms** (p50).
   - **Real R2** (from this sandbox): 4.8 s — three object-store round trips of 0.6–1.4 s each.
   - **Fluss, for comparison:** its lake lags 3 minutes by default.
2. **A new user's first query on R2 takes 20–30 ms** on a serving node (it was 3.0 s), and
   **≈0.95 s on a node that just joined** with an empty disk. Each node keeps recent objects on a
   local SSD tier and the whole catalog in memory.
3. **The local folder and the bucket use exactly the same layout** (`docs/lake-format.md`).

**Round 4 removed the two limits round 3 left, and round 5 made it a serving engine too:**

1. **Writes no longer go through one machine.** Every node ingests: it encodes, stores and aggregates the data it receives. The leader only hands out the order of commits, and even the tiering work is dealt out to all nodes.
2. **Reactions take milliseconds.** An event written to one node shows up, aggregated, on a client watching another node 7 ms later (p50, local disk; round 3: ~0.8 s), and on a read-only serving node just as fast. On object storage it costs one storage write — on a real R2 bucket, 1.0 s from this sandbox, where a bare PUT is 1.07 s.
3. **Serving reads are a first-class path** (round 5): one key out of 2 M in 7 ms, and keyed tables no longer rewrite themselves on every tiering round — which also took durable ingest from 2.1 M to 3.8 M events/s.

**R2: now measured for real.** The whole suite ran against a Cloudflare R2 bucket — see "On real
R2" below. Everything also still runs on local disk and on a local S3 server with R2-like latency
(PUT p50 197 ms, GET p50 100 ms); `tools/r2_test.sh` runs the main tests against any bucket.

## The old limit, in plain words

Round 3's report said "one leader commits all writes, so write capacity grows with a bigger machine, not with more machines."

- **Before:** imagine a shop where one cashier also packs every bag. More doors (nodes) let more customers in, but all of them still queued at that one cashier.
- **Now:** every node packs its own bags. It parses the batch, compresses it, writes it to the bucket and updates the views. The leader only hands out numbered tickets: "your batch is number 1,042". A ticket is a few bytes of metadata, so one leader can keep many nodes busy.

**Measured** (all three nodes share one 2-vCPU VM):

- 8 producers wrote 3.8 M events/s through the two followers (2.1 M before round 5's tiering work).
- The leader used 31 % of the cluster's CPU, below its one-third share.
- Before the tiering work was also dealt out, the leader's share was 70 %.

## Round 4: every node writes

| Feature | Replaces | Notes |
|---|---|---|
| **Every node ingests** | Kafka brokers | Nodes batch, encode (Arrow IPC + ZSTD) and store their own writes. Small flushes (≤64 KB) ride inside the commit request; the leader dedupes, numbers and commits. 4 flushes in flight per node, 4 commits in flight at the leader; no fixed flush window |
| **Commit stream** | ZooKeeper watches, Kafka replication | The leader pushes each durable commit to every follower over one HTTP response. Followers lay it over their own catalog view, so every node sees a commit within milliseconds |
| **Inline views** (`POST /views/{name}`) | Flink SQL jobs + keyed state | Run on every flush, on the node that received it, and commit with their input: never behind, exactly-once for free. GROUP BY views become **merge tables** (sum / count / min / max partials, merged on read) that any number of nodes update at once |
| **Push** (`GET /watch/{table}`) | Kafka consumers | New rows as NDJSON the moment they commit, on any node |
| **Event-driven tasks** | Flink jobs | Stateful tasks run as soon as rows commit (not every second) |
| **SPMD queries** | Trino / Spark SQL | Every node runs the same plan over its slice of files up to DataFusion's first exchange; the receiving node finishes it |
| **Distributed maintenance** | Spark OPTIMIZE jobs | The leader decides; converting the log to Parquet, merging small files and compaction run as jobs dealt to all nodes |
| **Backpressure** | — | While 10 M rows wait to be tiered, commits pause; tiering starts as soon as 1 M rows wait, in chunks of ≤4 M |

### What the tests found, and what was fixed

- **Lost task rows in about 1 failover run in 8.** A follower combines its own catalog view with the commits streamed to it, and three races could make a read see less than the node thought it had:
  - the view moving past the stream while a read was in flight;
  - streamed copies dropped on a 5-second timer while a slow read still needed them;
  - the "how many segments exist" mark being read from a *newer* view than the one a read was held to, so a streaming task read a range of the log that its own view couldn't fully see yet, and committed progress past the rows it missed.

  Now: a read checks its view before and after reading and decides under one lock, it pins what it needs, and the segment count always comes from a view at most as new as the read's. Twenty failover runs have passed since.
- **The leader did 70 % of the cluster's work**, because tiering, file merges and compaction ran there. They're now jobs dealt out to all nodes; the leader's share is 30 %.
- **A tiering job on a node whose view lagged could have written an incomplete file.** A job now names its inputs exactly and waits until the node sees the last log segment; otherwise it fails and the next round retries.
- **Catalog writes stalled ~30 s on simulated R2** when SlateDB's default limit of 8 level-0 files was reached. Raised to 64.
- **A follower that misses commits now restarts** instead of serving reads that could go back in time.

## Round 5: serving reads, and keyed tables that don't rewrite themselves

Two things were wrong for a system that wants to replace the serving tier as well as the lake:
every tiering round rewrote a keyed table in full, and every read of one paid a window function
over the whole table. Fixing them also made ingest much faster.

| Change | Effect |
|---|---|
| **Keyed tables are LSM-like.** Each round folds the log tail into a *new* file (a range per node, like append tables); files are compacted into one only when 8 pile up. Each file carries the last segment it covers, so a newer file's row wins for a key | Tiering costs what the new rows cost, not what the table costs |
| **Files that are already 64 MB or 4 M rows are left alone** by small-file merges | Rows are written once, not merged over and over |
| **Files are written for lookups:** sorted by key, a bloom filter per key column, 256k-row row groups | A lookup reads one row group, not a whole file |
| **`GET /lookup/{table}/{key}`** plans a lookup (single thread, no window) instead of a scan | A serving API that doesn't depend on SQL planning; today it costs the same as the equivalent SQL, not less |
| **"Newest version per key" is a grouped aggregate, not a window** | Scans of keyed tables 3.5x faster (2 M rows: 1,234 ms → 351 ms) |
| **Compacted tables skip deduplication entirely** (one file, nothing newer in the log = one row per key already) | A served table reads like plain Parquet |
| **Read-only nodes follow the leader's commit stream** instead of polling their own catalog view | Freshness on a serving node: 163 ms → **7 ms** p50 |

What it did to throughput (same box, same tests, round 4 → round 5):

| | Round 4 | Round 5 |
|---|---|---|
| Durable ingest, 3 nodes, producers writing to the followers | 2.1 M events/s | **3.8 M events/s** (leader 31 % of cluster CPU) |
| Saturated ingest through an aggregating view, one node | 1.23 M events/s | **2.84 M events/s** |
| 10 M events, ingest + keyed aggregation, one node | 3.97 s | 4.08 s |
| Batch: write 20 M rows + 6 queries | 3.08 s + 0.04–0.46 s | 3.15 s + 0.04–0.48 s |

Serving, measured with `tools/serve_bench.py` (2 M keys, one node, 2 vCPUs, local disk):

| Query | Result |
|---|---|
| Point lookup, 1 client | **7.7 ms** p50 (`/lookup`: 8.9 ms), 11 ms p99 |
| Point lookup, 8 clients | 30 ms p50, 46 ms p99, **265 lookups/s** (the two cores are the ceiling) |
| Point lookup, 8 clients, while 200-row batches keep arriving | 44 ms p50, 99 ms p99 |
| Dashboard aggregate over all 2 M rows | 351 ms (1,234 ms before this round) |
| Freshness on a read-only node (write → visible) | 7 ms p50, 84 ms p99 |

The ceiling of ~265 lookups/s is this box's two cores, not the design: each lookup costs ~7 ms of
CPU, most of it planning and Parquet decoding. Caching prepared plans per table version is the
obvious next step, and read-only nodes already scale the rest horizontally.

**What the tests missed.** The first version of this tiering rewrite stopped tiering an append
table once it had 8+ files. Reads stayed correct — the log just kept growing — so every
correctness test passed while sustained ingest fell by half. It was found by benchmarking, not by
testing. There is now a `tiering` test (rounds of writes + `/tier`: the log must drain and the
file count stay bounded) and a note in `AGENTS.md` that this failure mode shows up as throughput,
not as a red test.

## Round 11: toward petabytes: metadata, partitions, memory limits, shuffles, Arrow Flight

**What's new** (ADR-012):

| | What | Measured (local disk, one 2-vCPU box) |
|---|---|---|
| Table metadata that stays small | Every append-table file's column ranges. Past 128 files, the rest sealed into immutable manifests (≤4,096 files each, by partition) behind one list object; queries prune manifests, then files, before opening any Parquet | `metadata_bench.py`: 1,000,000 files registered (1 PiB and 10 T rows on paper). Catalog entry 2.9 → 20.4 KB; tiering commit 10.6 → 11.3 ms; INSERT 5.0 → 4.4 ms; append ack 7.2 → 6.6 ms; a query over today 5.0 → 12.9 ms, opening 7 files and skipping 1,000,006; one day of 2010 plans its 138 files in 29 ms; a restarted node answers in 35 ms; 3 nodes dealing out manifests agree |
| Partitions | `partition_by = 'day(ts)'` (a column, or year/month/day/hour of a timestamp); one partition per file; merges within a partition; a partition's small files merged before they're sealed | `harness.py scale`: 23,100 rows over 139 days, every Parquet file one day, a day's query reads 2 of 237 files |
| Memory limits | `--memory-gb` (default half of RAM): one spill pool for all queries; out of memory → again with sort-merge joins | under 50 MB: a 2 M-group aggregation, a 3 M-row window sort, a 3 M × 3 M join |
| Shuffles | Hash exchanges become shuffles between nodes, step by step; small tables broadcast; a node's slice reports the whole table's size; only plans whose operators stay correct split | 14 query shapes on 3 nodes == one node, 11 shuffled; `cluster.py spread` (9 queries, 4 M rows) identical, no fallbacks |
| Arrow Flight / Flight SQL | `--flight`: ADBC and JDBC drivers (queries, writes, `adbc_ingest`, catalog), pyarrow `DoPut` exactly-once with pipelined acks, `DoGet` SQL, a table's log as a columnar stream with chosen columns | `flight_bench.py`, 4 writers: **15.7 M rows/s (437 MB/s) in**, exactly-once; `SELECT *` out at 9.1 M rows/s; a log subscriber has each commit in **2.6 ms p50 / 5.1 ms p99**. `harness.py flight`: 13 checks |
| Metrics | `GET /metrics`: rows in, queries, spread and shuffled, files scanned and skipped, memory, commit latency, per-table files, rows, bytes, entry size | used by the tests above |
| Delta and timestamps | `TIMESTAMP` columns publish to Delta as `timestamp_ntz` | delta-rs, Polars and DuckDB read them (`harness.py scale`) |

**Regression, local disk** (`logs/round11/`):

- `harness.py all`: every test passes, now with `scale` (9 checks) and `flight` (13).
  - `clients` 18, `kafka` 9, `alter` 8, `windows` 3;
  - `crash`: 5 runs, 0 lost, 0 duplicated;
  - `reader` 10 ms p50.
- `failover`: 3 runs, writes back after 4.5–5.1 s, state == view == model.
- `users`: 2.95 M events (95k/s), 0 inconsistent reads, 0 lost or duplicated batches.
- `open_check.py`: all 8 outside readers equal Pondra.
- `cluster.py spread`: identical on 3 nodes.

  All three nodes share two cores here, so it checks correctness, not speed. The high-cardinality
  GROUP BY took 0.64–1.5 s spread against 0.65–1.3 s on one node, from run to run.

**Simulated R2** (`logs/round11/sim-r2.txt`; PUT p50 197 ms, GET p50 100 ms):

- `scale`: all 9 checks: 214 files (132 sealed), every file one day, a day's query reads 2
  files, 14 query shapes on 3 nodes equal one node (11 shuffled), 6 outside readers, the 50 MB
  limit. The first run failed in the test itself: a replaced file was deleted between listing
  the folder and reading it. The check now skips such files.
- `flight`: 13 of 13; a log subscriber has each commit 111 ms after it's sent (durable acks: one
  PUT each).
- `metadata_bench.py --files 100000`:
  - catalog entry 1.9 → 19.0 KB;
  - tiering commit 781 → 529 ms, INSERT 476 → 348 ms, append ack 225 → 275 ms (each about one
    PUT);
  - a query over today 4 → 14 ms, opening 2 files and skipping 100,006;
  - a restarted node 128 ms; 3 nodes 38 ms, same answer.
- `flight_bench.py` (durable, 4 writers): 1.5 M rows/s in, 13.8 M rows/s out, a subscriber 359 ms
  p50.
- `crash --runs 3 --batches 150`: 3 runs × 45k events, 0 lost, 0 duplicated, views exact,
  through 251 injected crashes and 12 kill -9s. This covers the new flush ordering.
- `kafka`: 9 of 9.

**Real R2** (`ponderabucket-us`, `logs/round11/r2.txt`):

- `scale`: all 9 checks: 214 files (131 sealed), every file one day, a day's query reads 2
  files, 14 query shapes on 3 nodes equal one node (11 shuffled), 6 outside readers equal Pondra,
  the 50 MB limit.
- `flight`: 13 of 13; a log subscriber has each commit 434 ms after it's sent (durable).
- `metadata_bench.py --files 100000`:
  - catalog entry 2.5 → 19.2 KB;
  - append ack 289 → 290 ms, INSERT 830 → 548 ms, tiering commit 842 → 1,011 ms (one or two
    PUTs each);
  - a query over today 5 → 17 ms, opening 3 files and skipping 100,008;
  - a restarted node 125 ms; 3 nodes 47 ms, same answer.
- `flight_bench.py` (durable, 4 writers, while a release build ran on the same two cores):
  1.0 M rows/s in, 8.5 M rows/s out, a subscriber 525 ms p50.
- Every test lake was deleted when its test finished; the buckets hold only the three kept
  lakes.

### What the tests caught this round

- **Statistics that didn't prune.** File ranges were first written with the values' display
  form: a timestamp came out as a raw integer that didn't parse back as a timestamp. So pruning
  on timestamps silently kept every file. They're now text that casts back exactly (ISO 8601),
  checked per column type.
- **Spreading quietly stopped when a node's slice pruned to nothing.** A single-partition plan
  merges its aggregate into one step, so there was no place to cut. Every slice now has at least
  two partitions.
- **Nodes with empty slices planned joins differently** (build side chosen by local size). The
  coordinator caught it and fell back to one node. Now a slice reports the whole table's size.
- **A shuffle ended too early.** A node that finished its last step dropped its buckets while
  others were still fetching them ("shuffle expired"). Finished shuffles are now kept for a
  minute.
- **A keyed table's view plans a hash repartition over data every node has whole.** Treated as a
  shuffle, each row would have arrived once per node. The spread analysis keeps such a
  repartition inside the node.
- **Pipelined batches from one producer overtook each other.** 756 of 800 Flight batches came
  back "out of order": a node's flushes reached the sequencer in any order. Flushes now reach it
  in the order they were cut, and the Flight door re-queues the rare batch that still arrives
  early (over HTTP from a follower).
- **Tables written only by INSERT never merged their small files.** Maintenance ran only for
  tables with log traffic. Small files also got sealed into manifests while small (338 of 402);
  a partition's small files are now merged first (114–152 of ~240).
- **Delta didn't publish tables with a `TIMESTAMP` column.** They now publish as `timestamp_ntz`.
- **A Kafka consumer-group check allowed a rebalance only 15 s**; it now waits up to 45 s.

## Round 10: the Kafka protocol, an Iceberg REST catalog, schema evolution, windows that close

**What's new** (ADR-011):

| | What | Measured |
|---|---|---|
| Kafka protocol | `--kafka`: a topic is a table. Producers: JSON values → rows (`_key`, `_timestamp`, raw `_value` columns), idempotent producers exactly-once, all 4 codecs, Debezium events and tombstones → upserts and deletes. Consumers read the log; consumer groups coordinated by the leader; SASL/PLAIN tokens | 3 nodes, 4 librdkafka producers, one 2-vCPU box: **776k events/s** durable, **796k** replicated, every event exactly once. Ack **1 ms p50 / 2 ms p99** (replicated), 3 / 21 ms (durable); a consumer on another node gets it 1 / 2 ms after the send (replicated) |
| Iceberg REST catalog | `GET /v1/…` on every node: engines attach by URL | PyIceberg and DuckDB attach it; 8 outside readers in `open_check.py` equal Pondra |
| Publishing | Timestamps kept in µs, so tables with timestamps publish; a keyed table's first file publishes at once; published keyed tables compact fully | timestamps read back right by PyIceberg and DuckDB |
| `ALTER TABLE … ADD COLUMN` | Any node, Postgres, MCP, `pondra sql`; old rows read null; writes that don't know the column keep working; Delta and Iceberg follow | under load (65–72k rows written during the change): 8 checks, 6 outside readers |
| Windows that close | `?window=w&size_secs=&lateness_secs=` on a `date_bin` GROUP BY view: `{view}_final` gets each window once, final, past the watermark | each window once; a late row updates the view only; a leader restart emits nothing twice |
| JSON functions | `json_get…`, `json_contains`, `json_length`, `->`, `->>` | over HTTP and Postgres; Kafka raw values queried as JSON |

**Regression, local disk** (`logs/round10/local-regression.txt`):

- `harness.py all`: every test passes, now with `kafka` (9 checks), `alter` (8) and `windows` (3).
- `crash --size 50000`: 3 runs × 9 M events, 0 lost, 0 duplicated, views exact.
- `users`: 3 runs, 129.5–138.1k events/s, ack p50 36–38 ms, 0 torn reads, 0 lost or duplicated batches.
- `failover`: 3 durable and 2 replicated runs, back in 4.3–5.1 s, state == view == model.
- race, isolate, split (4.97 M events/s), spread (identical): pass.
- latency: event → view row on another node 6 ms p50, 9 ms p99.
- `open_check.py`: all 8 outside readers (the REST catalog's two included) equal Pondra.
- `keyed_bench.py`: 17.9 MB written vs 53.4 MB for full rewrites (unchanged from round 9).

**Simulated R2** (`logs/round10/sim-r2.txt`):

- `kafka`: 9 of 9, the same checks as locally.
- `alter` (8 of 8) and `windows` (3 of 3): pass.
- `open_check.py`: all 8 outside readers, the REST catalog's included, equal Pondra.
- `kafka_bench.py`, 3 nodes, 1 M events:
  - replicated: 683k events/s, ack 1 ms p50 / 5 ms p99;
  - durable (one PUT per commit): 84.6k events/s, ack 231 / 764 ms.

**Real R2** (`ponderabucket-us`, `logs/round10/r2.txt`):

- `kafka`: 9 of 9 — the same producers, Debezium, consumers, groups and tokens, on R2.
- `alter` (8 of 8) and `windows` (3 of 3): pass.
- `open_check.py`: all 8 outside readers equal Pondra, the REST catalog's included.
- `kafka_bench.py`, 3 nodes, 400k events:
  - replicated: **461k events/s**, ack **1 ms p50** / 20 ms p99, a consumer on another node
    1 / 20 ms;
  - durable: 68.7k events/s, ack 245 / 606 ms (one PUT per commit).
- Every test lake was deleted when its test finished; the buckets hold only the three kept
  lakes.

### What the tests caught this round

- **Offsets across segment boundaries.** The first group test expected a consumer to resume at
  the last committed offset + 1. Offsets are `_ord`, so after a segment's last row the next
  offset is the next segment's first; the check (not the code) was wrong.
- **Keyed tables created in SQL stopped publishing** since round 9 added `_deleted` to them:
  they waited for a full compaction, which size-tiered merging made rare. Now a first file
  publishes at once, and published keyed tables compact fully.
- **Tables with timestamp columns never published** (nanoseconds, which Iceberg v2 can't hold).
  SQL timestamps are now microseconds.
- **Arrow appends missing a column were refused**, which would have broken every producer after
  an ALTER. Missing columns are now null, as in JSON.

## Round 9: everyone writes, everything speaks SQL

**What's new** (ADR-010):

| | What | Measured |
|---|---|---|
| Bucket inbox | A machine that can reach the bucket but not the leader leaves its write in `inbox/`; the leader answers within a second | `pondra sql` INSERT through the inbox: 1.0 s locally, 6.1 s on simulated R2, 7.3 s on real R2 (the whole process, catalog open included); a retry is a duplicate |
| Attached lakes | `--attach sales=…`: another lake's tables read as `sales.orders`, joins across lakes, writes recorded by that lake's leader | cross-lake join after a write through the other leader: exact |
| SQL writes | `CREATE TABLE … PRIMARY KEY … WITH (publish, cluster_by, merge, ttl)`, `INSERT`, `UPDATE`, `DELETE`, from any node, Postgres, MCP or `pondra sql` | upsert/delete model exact; a keyed table declared without `_deleted` takes DELETE |
| Postgres protocol | `--pg`: simple and extended protocol, text and binary, typed and array parameters, a small `pg_catalog` | psql, psycopg 3 (text and binary), psycopg2, asyncpg, SQLAlchemy + pandas |
| Python client | `import pondra`: SQL → pandas / Polars / Arrow, exactly-once appends, lookups, the change feed | pandas, Polars and list appends; replay with a delete |
| MCP | `POST /mcp`: `list_tables`, `query`, `write`, `changes`, under the same tokens | raw JSON-RPC in `harness.py clients`; the official MCP Python SDK (`logs/round9/mcp-sdk.txt`) |
| Vector search | `FLOAT[]` embeddings, `ORDER BY cosine_distance(emb, [...]) LIMIT k` | same neighbours by SQL and by a Postgres array parameter |
| Tokens | read / write / admin on HTTP, Postgres and MCP | missing, weaker and wrong tokens refused |
| SQL can't touch a node's disk | `COPY … TO` and `CREATE EXTERNAL TABLE` refused on nodes | refused |
| Size-tiered keyed compaction | merge the newest run of similar files; rewrite the table only when the run reaches the oldest | 2 M keys, 60 rounds × 20k updates: **17.9 MB written vs 53.4 MB** for full rewrites; 7 files; tier call 19 ms median; every key right |
| TTL, change feed | `ttl = 'ts:secs'` on keyed tables; `--changelog-secs` keeps the log replayable | expired rows hidden in SQL and lookups |
| Replicated acks, hardened | `--fsync`; `--replicas 3` | ack 4 ms p50 / 9 ms p99 with fsync; 3 replicas: 4 / 7 ms, `users` 114,804 events/s with 0 inconsistent, 3 × `failover` exact |

**Regression, local disk** (`logs/round9/local-regression.txt`, `harness-all-final.txt`,
`final-binary-cluster.txt`):

- `harness.py all`: every test passes, twice (the final run's `clients` has 18 of 18 checks; the
  first ran before the vector, file-access and MCP checks existed, with 15).
- `crash --size 50000`: 3 runs × 9 M events, 0 lost, 0 duplicated, views exact (the log keeps
  the last two runs' lines; a failing run stops the test).
- `users`: 4 runs, 114.5–116.8k events/s, ack p50 44–45 ms, 0 torn reads, 0 lost or duplicated
  batches.
- `failover`: 3 durable and 3 replicated runs, back in 4.5–5.3 s, state == view == model.
- race, isolate, split (3.6 M events/s), spread (identical results): pass.
- latency: event → view row on another node 5–6 ms p50, 8–10 ms p99.
- `open_check.py`: all six outside readers equal Pondra.
- `clustering.py` (8 M rows): one user 276 → 6 ms, a range 174 → 2.4 ms, 100 users 221 →
  101 ms, full scans 122 → 136 ms.

**Simulated R2** (`logs/round9/sim-r2.txt`):

- `harness.py clients`: 18 of 18 (the inbox write takes 6.1 s, opening the catalog included).
- `serverless`: an INSERT with nobody running 5.8 s, through a running leader 1.7 s, 8,000 rows
  and no duplicates.
- `crash`: 45,000 events through 7 kill -9s and 75 injected crashes, exact. (A second run hit
  the time limit.)
- `failover`, replicated: back in 12.8 s and 10.4 s, exactly-once, state == view == model.
- `users`, replicated: 83.6k events/s, ack p50 62 ms, 0 torn reads.
- `latency`, replicated: ack 4 ms p50 / 9 ms p99; a view row on another node 3 / 8 ms.
- Test lakes deleted at exit: 0 objects left in the bucket.

**Real R2** (`ponderabucket-us`, `logs/round9/r2.txt`):

- `harness.py clients`: 18 of 18 on two R2 lakes: Postgres drivers, MCP, vectors, tokens,
  the cross-lake join. The inbox write takes 7.3 s for the whole `pondra sql` process,
  including 3–6 s to open the catalog.
- `serverless`:
  - an INSERT on a brand-new lake with nobody running: 11.9 s (it creates the catalog);
  - through a running leader: 4.0 s;
  - right after the leader is killed: 42 s (waits for its mark to go stale);
  - 8,000 rows, no duplicates.
- `latency`, replicated: ack **3 ms p50 / 10 ms p99**; a view row on another node 3 / 7 ms.
- `failover`, replicated: back in 16.9 s and 15.0 s, exactly-once, state == view == model.
- Every round-9 test lake was deleted when its test finished. The buckets hold only the three
  kept lakes.

### What the tests caught this round

- **The test harness passed `tier_secs=1` as a bare flag** (Python treats `1 == True`), so the
  crash test's node never started, and the restart loop recursed until Python gave up. Flags are
  now bare only for a real `True`, and a node that keeps dying on start fails after 20 tries.
- **SQL could reach a node's own disk.** Any token that could run a query could also run `COPY …
  TO '/path'` or `CREATE EXTERNAL TABLE … LOCATION '/etc/…'` through DataFusion, and an INSERT
  into an attached lake could read local files. Queries on nodes are now read-only
  (invariant 21).
- **DELETE failed on keyed tables created in SQL** unless they declared `_deleted` themselves.
  They get the column now; INSERTs and Arrow appends may leave it out.
- **`cluster.py` runs on object storage left their lakes behind.** The cleanup at exit created
  its S3 client during interpreter shutdown, when boto3 can no longer start threads. On real R2
  that would have filled the 10 GB free tier. The client is now made when the first lake is,
  and a simulated-R2 run ends with 0 objects left.
- **Postgres array parameters** (a Python list for an embedding) arrived as text. They're bound
  as arrays now.
- **`SHOW TABLES` listed internal helper tables**, and SQLAlchemy and asyncpg needed `pg_type`,
  a parseable `version()` and typed parameters. All fixed while building the protocol.

## Round 8: native first, fast writes on any storage, writes from anywhere

**Write latency and throughput** (3 nodes on one 2-vCPU box; `cluster.py latency | users`):

| | Local disk | Simulated R2 | Real R2, Eastern NA bucket (PUT ~290 ms) | Real R2, the far bucket (PUT ~670 ms) |
|---|---|---|---|---|
| Ack, `--ack durable` | 5 ms | 255 ms p50, 757 ms p99 | 299 ms p50, 732 ms p99 | 818 ms p50, 2.5 s p99 |
| Ack, `--ack replicated` | 4 ms | **4 ms p50, 7 ms p99** | **4 ms p50, 5 ms p99** | **4 ms p50, 8 ms p99** |
| 64 writers + 16 readers, durable | 125k events/s, ack p50 39 ms | — | 6,400 events/s, ack p50 937 ms | — |
| 64 writers + 16 readers, replicated | 129k events/s, ack p50 38 ms | 89k events/s, ack p50 58 ms | **87,200 events/s, ack p50 60 ms** | — |

Replicated mode passed every consistency test it was given:

- `users`: 0 torn reads, 0 lost or duplicated batches, 4 runs locally, 2 on simulated R2 and 1 on
  real R2.
- `failover`: exactly-once and task state == model, 11 runs locally, 4 on simulated R2 and
  2 on real R2 (14–16 s from kill to writes acked again after the fix).
- Its leaders were killed with up to 279 acknowledged commits not yet in the bucket; every one
  was recovered.

**Freshness, head to head:** the table and what it means are in
`docs/comparison-spark-flink-fluss.md`. In short:

- **Nodes:** 10–15 ms after the ack, on local disk and on R2.
- **A new `pondra sql` process:** 34 ms on local disk; ~3 s on R2, where opening the catalog
  costs ~20 requests.
- **Delta / Iceberg readers:** ~30 ms on local disk; 3–4 s on the near R2 bucket (worst 5–8 s);
  7–10 s on the far one.

**Writes from any machine** (`harness.py serverless`; local disk / real R2):

| | Local disk | Real R2 |
|---|---|---|
| `pondra sql` INSERT on a brand-new lake, nobody running | 0.03 s | 11.3 s (it creates the catalog) |
| Same job id again | `{"duplicate": true}` | same |
| 4 INSERTs at once, nobody running | all 4 recorded, in turn | same |
| A node starting on the idle lake | leads at once (0.02 s) | leads, up in 5.7 s |
| INSERT while that node runs | 0.02 s (the leader records the files) | 3.5 s |
| INSERT right after the leader is killed (no followers) | 30 s (waits for the leader's mark to go stale) | 30.6 s |
| Rows at the end | 8,000, no duplicates | same |

**Open formats:** `open_check.py`, 4 tables (append, upsert, GROUP BY view, bulk insert):

- **Local disk, 120 rounds:** past a Delta checkpoint and Iceberg's 100-snapshot history. All six
  readers equal Pondra.
- **Real R2, 30 rounds:** all six equal Pondra. PyIceberg needs its fsspec file IO there: its
  default PyArrow S3 client got 403s from R2 in this sandbox.

**Clustering** (`clustering.py`, 8 M rows, 100k users, local disk):

- one user: 130 → 20 ms;
- a range of users: 73 → 6.5 ms;
- 100 users (`IN` list): 103 → 79 ms;
- full scans: 52 → 91 ms (sorting scatters columns that were in arrival order, which then
  compress worse).

### What the tests caught this round

- **A PUT on real R2 hung for its full 30 s timeout.** It slowed a tiering round to 26 s and
  failed a `/tier` call. In replicated mode, it let 279 commits be acknowledged ahead of the
  bucket. Two fixes:
  - idle bucket connections are dropped after 15 s instead of being reused;
  - at most 256 commits may be acknowledged ahead of the bucket.
- **Real R2 slowed down when test pollers hammered it** from this sandbox. The freshness tool now
  polls the bucket every 0.2 s.
- **`pondra sql` INSERT on a brand-new lake** can't open a catalog that doesn't exist yet: it now
  writes its files after it has claimed the lake.
- **A node that could never reach the leader would take over after the 30 s startup lease.** A
  laptop outside the cluster's network could have deposed a healthy leader. Now only members
  that lost a leader they were talking to use the lease; everyone else needs the leader's mark
  in the bucket to be stale.
- **Readers of other formats:**
  - DuckDB's iceberg extension needs its avro extension installed next to it;
  - PyIceberg's default PyArrow S3 client got 403s from R2 here; use fsspec;
  - Polars reads Iceberg on R2 when given the PyIceberg table.

## Round 7: serving reads, TPC-H, and the result cache

The design is in ADR-008. All numbers are on one 2-vCPU box with the load generator
(`tools/loadgen.go`) on the same box, 2 M keys (500k on R2).

| | Round 6 | Round 7 |
|---|---|---|
| `/lookup`, one client | 11.2 ms | **0.14–0.22 ms** |
| `/lookup`, 64 clients | ~200/s | **20,000/s** (leader), **35,600/s** (read-only node), **31,800/s** on R2 (32 clients) |
| SQL point query, 64 clients | ~280/s | **15,800–21,100/s** |
| Dashboard aggregate, a new query each time | ~420 ms | **6–43 ms** |
| Same dashboard, 32 clients | ~2/s | **21,000–23,000/s** (≤ 5 ms p99) |
| Same, while writes land every few ms: exact / `stale_ms=1000` | 15/s | **230–842/s / 10,900–12,400/s** |
| 64 writers + 16 readers + 2 serverless, 3 nodes | 30k events/s | **~103k events/s**: identical reader queries now share one computation |
| TPC-H SF1, 22 queries | — | **5.9 s** (Spark 4.2: 58–65 s; DuckDB: 3.8 s) |

Also this round:

- `POST /sql?format=arrow` returns Arrow IPC.
- The `upsert` test checks `/lookup` and SQL point queries against the model after every batch:
  3,400 lookups per run, on local disk, simulated R2 and real R2, with 0 wrong.
- Everything else still passes on the round-7 binary:
  - all, crash, tiering, race, isolate, latency and spread;
  - 5 of 5 `users` runs and 8 of 8 `failover` runs;
  - Delta and new-user tests;
  - simulated R2: users, failover ×2 and fence;
  - real R2: users, upsert, serving, new-user and Delta.

## Round 6: an open lake, and fast first reads on object storage

The design is in ADR-007; the on-disk layout and how to read it without Pondra are in
`docs/lake-format.md`.

| Change | Effect |
|---|---|
| **Delta Lake publishing:** each tiering round writes each table's `_delta_log/` (JSON commit, checkpoint every 10 versions, last 1,000 versions kept), derived from committed catalog state, put-if-absent | Any Delta reader sees the lake. Checked against delta-rs 1.6.4, Polars 1.44.2 and DuckDB 1.5.5 on local disk, simulated R2 and real R2, across 1,250 versions with checkpoints and log cleanup |
| **Tiering starts when rows commit** (at most every `--tier-secs`, default 2, fractions allowed), commits fresh rows before merges and compactions, and does up to 4 tables at once | New row → Delta version: **17 ms** on local disk (was 10 s + a round) |
| **SSD tier per node** (`--cache-dir`, `--cache-gb 20`): write-through, read-through, prefetched from the commit stream, warmed at start | Queries on R2 read local disk, not the bucket |
| **Followers and serving nodes hold the whole catalog in memory**, kept current by the commit stream | No catalog reads from the bucket on the query path |
| **The leader keeps the catalog's SST files on local disk** (SlateDB's disk cache) | Tiering rounds don't wait on catalog reads |
| **Catalog flushes only every 5 s, and only when something changed** (and once on takeover) | A tiering round went from 9 s back to milliseconds; the 1,250-round Delta test from 15+ min to 2.5 min |

A new user, measured with `tools/newuser_bench.py` (3 nodes: leader, follower, read-only node; 1 M
rows; the client has never queried before):

| | Real R2, round 5 | Real R2, round 6 | Simulated R2 | Local disk |
|---|---|---|---|---|
| First query on the read-only node (count+sum / top 10 / one user) | 2,999 ms | **20 / 30 / 22 ms** | 20 / 25 / 19 ms | 17 / 24 / 17 ms |
| Same queries, steady (p50) | ~495 ms | **17 / 31 / 22 ms** | 13 / 23 / 18 ms | 12 / 23 / 18 ms |
| First queries on a node that just joined, empty SSD tier | — | **949 / 372 / 23 ms** | 452 / 387 / 21 ms | 20 / 26 / 20 ms |
| Write on node B → visible on node C | 1,999 ms | **660 ms** (≈ one PUT) | 262 ms | 7 ms |

On R2, a node that just joined pays for its first reads from the bucket: one GET takes ~0.4 s from
this sandbox. They are still under a second, and 20–35 ms once its SSD tier has warmed up (5 s
later).

Open lake freshness, measured with `tools/delta_check.py`. A probe row is written after a quiet
spell; the table shows how long until a Delta version containing it exists, and until delta-rs,
reading on its own, returns it:

| | Local disk | Simulated R2 | Real R2 (from this sandbox) |
|---|---|---|---|
| Write acknowledged | 7 ms | 256 ms | 663 ms |
| Delta version with the row exists | **17 ms** | 1.6 s | 4.8 s |
| delta-rs returns the row | **31 ms** | 3.7 s | 10.5 s (one delta-rs load takes ~4.7 s from here) |
| Fluss's lake, for comparison | — | — | 3 min by default |

Under a steady stream of writes, add up to `--tier-secs` (2 s by default): tiering starts as
soon as rows commit, but at most that often.

### What the tests caught this round

- **Lost and torn reads from the in-memory catalog (fixed).** When a node's own catalog view got
  ahead of its commit stream, the commits in between were marked "seen" and skipped. But the
  in-memory copy never reads the view, so reads missed batches. Tiering jobs that ran on such a
  node then wrote files without those rows, and the leader committed them.
  - `cluster.py users` failed 4 runs in 5; every run since has passed: 20+ local runs, plus
    simulated and real R2.
  - Every tiering job now also checks the leader's row count for its log range, so a node that
    sees less refuses the job instead of writing a short file.
- **A 9-second stall per tiering round (fixed).** Flushing the catalog's memtable on every
  tiering call piled up level-0 files faster than SlateDB compacted them, and writes waited on
  the limit.
- **A follower that came back after a leader change read a stale catalog (fixed).** Its view
  lacked commits the old leader hadn't flushed; a new leader now flushes what it inherited.
- **A restarted node never came up on simulated R2 (fixed).** Loading the in-memory catalog
  retried until the catalog stood still, which a busy lake on slow storage never does. It now
  loads in the background, one try at a time.
- **SSD tier:** a panic on a one-character catalog key (it killed all nodes in the first R2
  run), and temporary file names two writers could share.

## On real R2 (round 5 table; round 6's R2 numbers are in the round 6 section)

Round 6 re-ran the core tests on R2, and all passed:

- 64 writers + 16 readers: 91.8k events, 0 inconsistent, snapshot query p50 21 ms (was 12 ms).
- Failover: 26.0 / 22.3 s, exact.
- Crash: 18k events with 8 kill -9s and 41 injected crashes, exact.
- Latency: 0.7–1.2 s p50, depending on the hour.

Same tests, against a Cloudflare R2 bucket, from this sandbox. **A bare 64 KB PUT from here takes
810 ms p50 (1,048 ms p90) and a GET 379 ms** — that round trip dominates every number below, and
it is why a durable ack costs 1.2 s: the ack is essentially one object-store write. A deployment
near its bucket would see far less; treat these as a worst case, not a floor.

| | Real R2 | Simulated R2 (PUT p50 197 ms) | Local disk |
|---|---|---|---|
| Write acknowledged (durable), idle | **1,243** / 1,699 ms | 309 / 919 ms | 7 / 9 ms |
| Event → view row on another node | 1,336 / 2,553 ms | 333 / 1,072 ms | 7 / 10 ms |
| 64 writers + 16 readers + 2 serverless, 3 nodes | 91k events (2.5k/s), ack p50 2.0 s, snapshot query p50 12 ms, **0 inconsistent** | 276k events, 0 inconsistent | 1.47 M events, 0 inconsistent |
| Crash / exactly-once (kill -9 + injected crashes) | 30k events, 34 kill -9s + injected crashes: exact (plus 2 × 30k in the previous round) | 3 × 45k events, exact | 3 × 9 M events, exact |
| Leader failover (2 kills) | 23.1 / 22.9 s, state caught up 8.2 s, exactly once | 10.0 / 11.8 s | 4.5–4.9 s |
| Split brain (frozen leader) | 18.5 s; stale write rejected; rejoined | 10.2 s | 6.0 s |
| Distributed query, 3 nodes vs 1 | identical results; q9 1,221 → 541 ms, q8 1,308 → 1,126 ms | 8 of 9 queries 1.2–4.3x faster | no gain on one machine |
| Upsert compaction (12,000 upserts, 6 compactions) | 3.7–7.1 s each | 1.5–3.2 s each | milliseconds |
| Serving: point lookup, 500k keys, 8 clients | 29 ms p50 warm (276 ms cold: one GET), 311/s | — | 30 ms p50, 265/s (2 M keys) |
| Serving: dashboard aggregate on a compacted table | 3.1 ms (cached) | — | 351 ms (2 M rows) |

Nothing failed: 0 lost, 0 duplicated, 0 inconsistent reads, exactly-once through leader kills, on
real object storage.

## Many users at once

- **64 independent writers + 16 SQL readers + 2 serverless `pondra sql` processes, on 3 nodes:**
  - local disk: 1.52 M events in 31 s (49.2k/s), ack p50 88 ms / p99 638 ms, snapshot query p50 404 ms;
  - real R2: 91k events in 37 s (2.5k/s), ack p50 2.0 s / p99 3.9 s, snapshot query p50 12 ms;
  - simulated R2: 276k events in 35 s (7.9k/s), ack p50 691 ms / p99 1,537 ms;
  - **0 inconsistent reads** (every read saw each producer's batches as a gap-free prefix) and **0 lost or duplicated batches** in both.
- **Writers don't lock each other.** Each node batches its own writers; the leader folds every node's flush into one commit.
- **Readers never block writers or each other.** They read immutable objects plus a catalog snapshot.
- **Two writer processes on one bucket** form a cluster instead of fighting. A leader that was replaced is fenced by the catalog and rejoins as a follower.

Limits: producer names must be unique per client; there is no auth or per-user quota yet.

## Tests

Every test runs on local disk, on a local S3 server with R2-like latency, and against a real
Cloudflare R2 bucket. All of them pass on all three.

Round 18's runs are in `logs/round18/` (the suite on local disk, `local-*.txt`; the R2
simulator, `sim-r2-*.txt`; real R2, `r2-*.txt`; query latency before and after). Round 17's runs are in `logs/round17/` (the install checks, the suite with both builds, q15 twenty times, TPC-H before and after the exact sum, on local disk, the R2 simulator and real R2). Round 16's runs are in `logs/round16/` (the suite, `asof_check.py` and `stream_check.py` on
local disk, the streaming tests on the R2 simulator and on real R2). Round 15's runs are in `logs/round15/`: on local disk (`local.txt`: the suite, TPC-H on three
nodes with small tables whole and with every table sliced, hot keys, shuffles bigger than memory,
the GitHub workflow's driver, join order and the single-node benchmark), on the R2 simulator
(`sim-r2.txt`) and on a real R2 bucket (`r2.txt`), plus the binary's size (`sizes.txt`). Round
12's (`logs/round12/`) have TPC-H against DuckDB, Polars, Daft and Bodo (`tpch-sf1.json`,
`tpch-sf10.json`).

| Test | Local disk | Real R2 |
|---|---|---|
| Crash / exactly-once (kill -9 + injected crashes at 3 points) | 3 runs × 9 M events: 0 lost, 0 duplicated, views exact | 30k events, 34 kill -9s: exact |
| 64 writers + 16 readers + serverless, 3 nodes | 1.52 M events (49.2k/s), ack p50 88 ms, 0 inconsistent | 91k events (2.5k/s), ack p50 2.0 s, 0 inconsistent |
| Failover: 3 nodes, 6 task shards, 2 leader kills + 1 follower kill | 4 runs: writes back after 4.5–4.9 s, caught up 0.1–6.4 s, state == view == model | 23.1 / 22.9 s, caught up 8.2 s, exact |
| 5 nodes start at once | 1 leader | 1 leader |
| Follower cut off from a healthy leader | doesn't take over; takes over 5.1 s after the leader dies | 18.1 s |
| Split brain (leader frozen, then resumes) | takeover 6.0 s; stale write rejected; rejoined | 18.5 s; same |
| Upsert vs model (12,000 upserts/deletes, compactions, restart) | pass | pass |
| Tiering keeps up (18 rounds of writes + `/tier`, 10.8 M rows) | log drained, files bounded, rows exact | pass (1 M rows) |
| Distributed query == single-node query | identical, 2 M rows | identical |
| Latency (event → view row on another node) | 7 ms p50 / 10 ms p99; 54 / 290 ms under load | 1.34 s p50 / 2.55 s p99 |
| Freshness on a read-only node | 7 ms p50 / 84 ms p99 | — |
| Serving (2 M keys: lookups and a dashboard query) | 7 ms p50, 265 lookups/s, 351 ms aggregate | 29 ms p50 warm, 311 lookups/s |

## Sizes

| What | Round 3 | Round 5 | Round 8 | Round 9 | Round 10 | Round 11 | Round 12 | Round 14 | Round 15 | Round 16 | Round 17 | Round 18 | Round 23 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| Binary (stripped) | 88.3 MB (30 MB gzip, 17 MB xz) | 89.2 MB (29.9 MB gzip, 16.8 MB xz) | 90.0 MB (30.5 MB gzip, 18.7 MB xz) | 90.7 MB (30.5 MB gzip, 17.2 MB xz), with the Postgres protocol and MCP | 93.0 MB (31.4 MB gzip, 17.6 MB xz), with the Kafka protocol and JSON functions | 95.0 MB (32.0 MB gzip, 18.0 MB xz), with Arrow Flight (gRPC) | 95.9 MB (32.4 MB gzip, 18.2 MB xz), with hashing, base64, files, vectors and AI functions | 96.3 MB (32.8 MB gzip), with shuffles that spill, the join order and any query across the nodes | 96.5 MB (32.9 MB gzip), with tables split by key ranges, hot keys shared out and distinct values sketched | 96.7 MB (33.0 MB gzip), with `ASOF JOIN`, session windows and watermarks from event time | 96.7 MB, built for glibc 2.17 (the wheel 32.8 MB, the npm platform package 33.3 MB), with the shell and exact float sums | 97.0 MB (32.8 MB gzip), glibc 2.17, with schemas, DDL, stored views and `ATTACH` | 101.2 MB (34.4 MB gzip, 21.6 MB xz), with files anywhere, Delta and Iceberg read and written, GCS and Azure, Kafka's client side (0.22.2: 97.9 MB) |
| Idle memory | 18 MB | 41 MB (mimalloc reserves more up front) | 42 MB | 44 MB | 49 MB (with `--kafka`) | 42 MB (with `--kafka --flight --pg`) | 39 MB (with `--kafka --flight --pg`) | not re-measured | not re-measured | not re-measured | not re-measured | not re-measured | not re-measured |
| Peak memory under full load | 455 MB | 1.8 GB at 2.84 M events/s sustained (279–586 MB in the batch and streaming benchmarks) | not re-measured | not re-measured | not re-measured | bounded for queries by `--memory-gb` | as before, plus the decoded columns (`PONDRA_HOT_GB`, a quarter of the query budget), which are given back when the node's own memory runs high | as before; a shuffle's buckets past `PONDRA_SPILL_MB` go to the node's disk | as before | as before; an `ASOF JOIN`'s lookup table counts against the query budget | as before | as before; a node runs at most one partition per 24 MB of query memory | not re-measured |
| Storage per event (user, event, amount, ts) | NDJSON 77.9 B → log 12.8 B → Parquet 6.8 B | NDJSON 77.9 B → log 12.8 B → Parquet 6.8 B (unchanged) | unchanged | unchanged | unchanged | unchanged | Parquet files are LZ4 now, about a third bigger than ZSTD's and much cheaper to read (`PONDRA_CODEC=zstd` to go back) | unchanged | unchanged; a table's entry carries a ~350-byte sketch per key-like column | unchanged | unchanged | unchanged | unchanged |

## Memory is a knob, not a mystery

Under saturation, memory is bounded by how many rows may wait in the log for tiering
(`--backlog`, default 10 M rows). One node, 8 producers, 60 s, a 100,000-key aggregating view:

| `--backlog` | Throughput | Ack → visible in the view (p50 / p99) | Peak memory |
|---|---|---|---|
| 10 M (default) | 2.84 M events/s | 205 ms / 730 ms | 1.8 GB |
| 2 M | 784k events/s | 49 ms / 234 ms | 1.4 GB |
| 500k | 33k events/s | 18 ms / 36 ms | 0.35 GB |

Lower it for less memory and lower latency, raise it to absorb longer bursts. Plain durable
ingest without a view reaches 5.5 M events/s at 1.3 GB, and the batch and streaming benchmarks
peak at 279–586 MB.

## Against the kill criteria (ADR-002 / 003)

| Criterion | Status |
|---|---|
| Freshness p99 ≤5 s at ≥50k events/s | **Pass:** 10 ms p99 idle, 290 ms p99 under load (local disk); 2.6 s p99 on real R2 |
| 0 lost / 0 duplicated under crashes | **Pass,** including leader failover and split brain |
| Binary ≤150 MB, idle memory ≤200 MB | **Pass** |
| Queries within 2x of DuckDB | **Pass** (round 2) |
| Catalog commit p95 ≤250 ms on object storage | **Measured, and it depends on the bucket's distance:** a durable ack is essentially one object-store write — 1.24 s p50 against R2 from this sandbox (where a bare PUT is 0.81 s), 309 ms on the R2-latency simulator, 7 ms on local disk |
| First query after idle ≤3 s | Readers pass; a writer restart is a few seconds on simulated R2 |
| Multi-node at ≥2x one node, recovery | **Partly:** ingest, views, tasks, queries and tiering all spread over nodes, and failover works. Speed-ups can't be shown on one machine: on simulated R2, where I/O latency dominates, 3 nodes ran 8 of the 9 queries 1.2–4.3x faster than one |

## What remains

- One sequencer per lake orders commits (metadata only). Past its capacity, split tables across
  attached lakes (round 9); there are no transactions across lakes.
- A *durable* acknowledgement costs one object-store write (0.25–0.7 s on R2). `--ack replicated`
  makes it milliseconds, but a write in that window survives only as long as one of its holders
  does (with `--fsync`, power loss included; not the leader and every holder at once).
- Kafka: one partition per topic, no transactions, sparse offsets; consumer groups live in the
  leader's memory. `ALTER TABLE` renames, drops and widens columns (round 21), but renames a
  table only by copying it, and never narrows a type.
- Streaming: one watermark per source, not per partition or node, and a quiet source holds it;
  no timers, CEP or Top-N by event time (sliding windows: round 20; deduplication by event time:
  round 21). A view's fill runs on the leader in one go. An as-of join in a view joins what the table has when the
  event arrives (Flink's temporal join waits for the table's watermark), and against a keyed
  table it sees only the latest row per key.
- A one-off `pondra sql` on far-away object storage spends 2–3 s opening the catalog.
- Open-format versions trail the ack by a few sequential bucket round trips (3–4 s near, 7–10 s
  far, at `--tier-secs 2`); Iceberg costs two more round trips than Delta.
- A *new* analytical query costs what its scan costs (TPC-H SF1: 60–600 ms per query on two
  cores). Repeated ones and key lookups are served from caches in about a millisecond.
- Compaction of a keyed table is one job on one node. Size-tiered merging (round 9) means the
  whole table is rewritten only when the newer data has grown to about half of it; partitioned
  compaction (a key range per node) is the next step.
- Merge tables only support decomposable aggregates (sum, count, min, max).
- Any query can run across the nodes (all 22 TPC-H queries, with small tables whole or every
  table sliced). Buckets spill to disk and results stream, a failed step is retried and then run
  without that node, big tables that share a key are split by its ranges, and a hot key's
  partition is shared out (round 15). Still on one node: a `LIMIT` inside a subquery over sliced
  data, and a shuffle that must keep order. Nothing has run on several machines yet;
  `tools/cloud/` and the GitHub Actions workflow are the kits.
- A query's own answer still passes through the coordinator's memory once, because an HTTP answer
  is one body that identical queries share (invariant 14). What the nodes send no longer does.
- Join order is chosen from the catalog's statistics (round 13), but only when it clearly beats
  the order the query wrote. Distinct values come from per-table sketches (round 15, about 6%
  off); rows deleted from keyed tables stay counted in them.
- Files written in key order aren't declared as sorted to DataFusion: declaring it made TPC-H
  slower (round 15). They are used to split tables by ranges instead.
- The columns kept decoded in memory (`hot.rs`) are a cache of what was read, not a policy: no
  pinning a table in memory, and nothing is loaded before a second read asks for it.
- `VARIANT` is JSON text (`json_get` parses at read time), not a shredded variant type.
- Under sustained overload, commits pause until tiering catches up (`--backlog`); a client with a
  short timeout will see it as a slow ack.
- Tokens per role only (round 9): no TLS, per-table grants, quotas or multi-tenancy. Nodes call
  each other over plain HTTP: keep a cluster on a private network (a VPC, Tailscale).
- Other engines read Delta and Iceberg (opt-in per table); keyed tables with `order_by` or merge
  functions only as of their last compaction (at most 8 tiering rounds behind; plain upsert tables
  every round since round 28). Time travel reaches back only as far as `--retain-secs` keeps
  replaced files. A keyed table's changes from other engines are copied through the log, not
  recorded as written; Iceberg v3 (row ids through other engines' updates), vended credentials and
  scan planning are ADR-029's phase 3.
- Bucket credentials still decide who can use `pondra sql` and the inbox directly.

## Next

From the plan in `docs/comparison-spark-flink-fluss.md`, in order:

0. **The DataFrame API** (`dataframe-api.md`, round 22): `pondra.frame` and `pondra.spark` over SQL.
1. **A multi-machine run in one data centre** (`.github/workflows/cluster-bench.yml` has run on
   GitHub's runners over the internet, rounds 18–20): `tools/cloud/` on VMs (e.g. a Google Cloud
   trial), TPC-H SF10–SF100 against Spark and Sail.
2. **Ranges declared, not only found**: `cluster_by` columns kept in order through merges, so a
   table written out of order can still be split by key; and a `LIMIT` in a subquery across the
   nodes (each node's top rows, then the top of those).
3. **Streaming aggregation and merge joins** where DataFusion gains from an order it knows
   (declaring it everywhere made TPC-H slower in round 15).
4. **Kafka partitions** and transactions; the Java client and Kafka Connect verified.
5. **An approximate vector index**, and `VARIANT` as a real type once Arrow has one.
6. **TLS, per-table grants, an audit log, quotas.**
7. **Streaming, next:** Nexmark against Flink; as-of joins in views that wait for the table's
   watermark; sliding windows; a side table for late rows.
