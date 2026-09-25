# AGENTS.md — working on Pondra

Read this first, then `README.md` (what it does), `docs/adr-005-every-node-writes.md` (why it
works this way) and `docs/adr-018-install-anywhere.md` (the current round).
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
5. **Short, simple, readable code** — without losing functionality. ~13,200 lines of Rust total (the Kafka protocol is 1,300 of them).
   If a change makes a file much longer, look for the simpler shape first.

## Layout

```
src/      13,700 lines of Rust, one file per concern (see the table in README.md)
python/   the Python client (pure Python, HTTP + Arrow; `local()` starts a node)
js/       the JavaScript client and the `pondra` npm package's files
examples/ quickstart.ipynb (pip install to an as-of join, in the owner's notebook style)
tools/    harness.py, cluster.py (tests), open_check.py (Delta + Iceberg readers == Pondra),
          keyed_bench.py (compaction cost), clean_bucket.py (keep a bucket to its newest lakes),
          kafka_bench.py (Kafka clients: throughput, latency), mcp_client.py (the MCP SDK),
          freshness.py (head-to-head freshness), clustering.py (what cluster_by buys),
          newuser_bench.py (first reads, new nodes), demo_lake.py (one of everything + the tree),
          serve_bench.py + loadgen.go (serving), bench/tpch.py (TPC-H vs DuckDB and Spark),
          sizes.py, sim_r2.py (local S3 with R2 latency), udf_server.py (a function of your own,
          in Python, over Arrow Flight), bench/singlenode.py (TPC-H vs DuckDB, Polars, Daft, Bodo),
          metadata_bench.py (a table with a million files), flight_bench.py (Arrow Flight),
          shuffle_spill.py (a shuffle bigger than memory, and one that loses a node),
          join_order.py (the same queries written badly: same answers, no slower),
          spread_tpch.py (all 22 TPC-H queries across N nodes == one node, and why not),
          skew_check.py (a hot join key: same answers, the work shared out),
          asof_check.py (ASOF JOIN == DuckDB's, one node and three), stream_check.py (windows,
          sessions and an as-of view over one stream: every click once; rates and delays),
          cloud/ (a cluster on several machines; cloud/actions/ + .github/workflows/: on GitHub runners),
          bench/tpch-queries/ (the 22 TPC-H queries),
          r2_test.sh (run the suite against a real bucket), bench/ (vs Spark and Flink),
          package.py (wheels and npm packages from a binary), anywhere_check.py (the shell, local(),
          the packages, the notebook; old Linux in docker), bench/repeat.py (one query many times)
docs/     ADRs and reports; lake-format.md is the on-disk layout
```

## The model in one page

- **Tables** are Parquet files in the bucket plus a **log tail**. Every query reads files ∪ tail,
  so data is queryable the moment it commits.
- **The catalog** is a SlateDB key-value store inside the same bucket: `t/` tables, `s/` segments,
  `d/` inline segment data, `p/` producer progress (also Kafka producers, consumer-group offsets
  and window emission), `v/` views, `w/` session views' bounds, `k/` tasks, `x/` Delta and `i/`
  Iceberg publish state, `m` members (replicated acks), `n` next segment, `c` commit number. One
  process (the leader) writes it; everyone reads it.
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
  inner joins are rebuilt smallest-first when that costs less than the order the query wrote,
  which is costed the same way as the tree it is. `PONDRA_JOIN_ORDER=0` turns it off.
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
  is a SQL shell (`shell.rs`) over a node it starts; Python's and JavaScript's `local()` start
  one too. All three start it with `--stop-with-stdin`: the node stops, and a leader gives up its
  term, when its standard input closes.
- **`sum` over DOUBLE is order-independent** (`fsum.rs`): it replaces DataFusion's `sum` in every
  session; Float64 sums carry a second double with the rounding errors (state: two columns),
  other types go to DataFusion's.
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
   catalog state only and written put-if-absent; an unrecorded one found later is adopted (which
   is also why the catalog write recording it isn't awaited). Only `_last_checkpoint` is ever
   overwritten, and nothing in `_delta_log/` goes through the SSD tier.
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
   sequence + record count, `prev` = base sequence, producer `kafka:{id}:{topic}`. Producers
   without a name (non-idempotent Kafka producers) are never checked (`log::commit`); nothing
   else may use an empty name.
23. **Columns only grow, at the end** (`write::create_table`), and every read of log rows goes
   through `query::conform` (by name; missing → null). Never read segment rows with the table
   schema without conforming them: rows written before an ALTER have fewer columns.
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
   table's partitions are gathered depends on what `hot.rs` holds decoded, not on the query.
35. **A scalar subquery is answered before anything that uses it runs** (`spmd::hoist`): a shuffle
   takes the `ScalarSubqueryExec`s out of the plan, and `step()` fills their shared answer slots
   as soon as the exchanges they read are done — on every node, from the same all-gathered rows.
   A shuffle's pieces are compacted (`spill::compact`) before they are counted or written: a
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

## Tests: run these before and after any change

```bash
cargo build --release
python3 tools/harness.py all            # upsert, fence/split-brain, bulk insert, reader, crash, load
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
python3 tools/smoke.py target/release/pondra   # what CI runs on Windows, macOS and Linux (stdlib only)
python3 tools/anywhere_check.py --bin <pondra> --dist dist [--docker]   # shell, local(), kill -9, wheel, npm, notebook; glibc 2.17 + Ubuntu 22.04
python3 tools/bench/repeat.py --data ~/tpch/sf1-bench --query 15 --runs 20 [--hot]   # one query many times vs DuckDB
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
python3 tools/kafka_bench.py [--flag ack=replicated]   # Kafka ingest throughput and latency, 3 nodes
python3 tools/mcp_client.py --url http://127.0.0.1:8080/mcp   # the official MCP SDK (pip install mcp) against a node
python3 tools/keyed_bench.py                   # keyed-table compaction: bytes written, correctness
python3 tools/cluster.py race | isolate | split | spread
python3 tools/bench/run.py batch 20000000     # ENGINES=pondra,spark,flink
python3 tools/bench/singlenode.py prepare --data ~/tpch/sf1   # tpchgen-cli output -> the bench copy
python3 tools/bench/singlenode.py run --data ~/tpch/sf1-bench --sf 1   # vs DuckDB, Polars, Daft, Bodo
python3 tools/serve_bench.py --keys 2000000   # serving: point lookups and dashboard queries
```

The Python tools need `pip install -r tools/requirements.txt` (Python 3.11; the versions the
suite last passed with). `.github/workflows/build.yml` runs `harness.py all` and a failover on
every push with them.

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

## State of the work (2026-09-28, round 18)

Everything in `docs/prototype-status.md` passes on local disk and on simulated R2. The round-11
additions (manifests, partitions, shuffles, memory limits, Arrow Flight) also ran against real
R2; round 12's are in `logs/round12/`, round 13's in `logs/round13/`, round 14's in
`logs/round14/`, round 15's in `logs/round15/`, round 16's in `logs/round16/`, round 17's in `logs/round17/`
and round 18's in `logs/round18/`.

**R2 test buckets.** There are two:

- `ponderabucket-us` (Eastern North America, ~290 ms per PUT from the sandbox; the default);
- `pondbucket` (~670 ms per PUT).

**The owner's R2 free tier is 10 GB.** Test runs delete their lakes; keep at most three lakes in
all. After R2 runs: `tools/clean_bucket.py --bucket ponderabucket-us --bucket pondbucket --newest
3 --dry-run`, then without `--dry-run`.

**The repository (2026-09-27).** `alimardon123/pondra` on GitHub holds the code, pushed by the
owner from the bundles (the sandbox can't push). It is public but **all rights reserved**
(`LICENSE`): nobody may reuse it, and the packages are marked so PyPI, npm and crates.io refuse
them until the owner picks a license. Its history was rewritten once, before it went public, to
put the owner's GitHub noreply address on the four commits that had their email; commit IDs from
before then (in older bundles) differ. Each round the owner downloads the new bundle and, in
their clone, runs `git pull <bundle> main` and `git push`; GitHub then builds it on Linux,
Windows and macOS (`.github/workflows/build.yml`).

**The cluster bench so far (round 18).** The owner has run `cluster-bench.yml` on GitHub's
runners three times: one node (TPC-H 18.6 s in all); three nodes (every answer right, all 22
queries spread, but 33.2 s); three nodes again, with the network measured (commit 1092999),
which lost a node at start to the follower-before-catalog race (invariant 52, fixed in round
18) and wrote no `results.json`. Next: the same run with round 18's code, then read
`wire_mb`, `wait_s` and `network` in its results before changing how queries spread.

**Where the multi-machine run will happen (the owner's plan, 2026-09-23).** The owner has no VMs
of their own. They will run the multi-machine tests themselves, later, on one of:

- **GitHub Actions** — free runners joined into one network with Tailscale's free plan, the lake
  in their R2 bucket. A private repo gets 2-vCPU / 8 GB runners and a monthly minute allowance; a
  small *public* bench repo holding only the workflow gets 4-vCPU / 16 GB runners, free and
  unlimited, while the binary stays in R2 and the source stays private. Jobs last at most 6 hours;
  runners have 14 GB of disk (SF10 fits, SF100 doesn't) and are shared, so compare shapes (1 → 3 →
  6 nodes), not headline numbers. `.github/workflows/cluster-bench.yml` and `tools/cloud/actions/`
  are the workflow; `bench-bin/pondra` in `ponderabucket-us` holds the round-17 portable binary
  (Linux x86-64, glibc 2.17) for its `binary: r2` input. Rebuild and re-upload it when the code
  changes, and read results from `bench-results/<run id>/results.json`.
- **A Google Cloud VM trial** ($300 for 90 days, no charge unless they upgrade) for dedicated
  machines, SF100 and Spark on the same VMs, with `tools/cloud/cluster.sh`.

An agent can't reach VMs from its sandbox (outbound HTTPS only, through a proxy: no SSH, nothing
inbound) and must not push to GitHub. So the pattern is: the agent prepares the workflow or
scripts, the owner runs them, and the runs write their results under `bench-results/` in the R2
bucket, which the agent can read with the credentials in `/home/claude/.r2env`. If the owner
links a session to their computer, an agent can drive VMs from there instead.

Headline numbers, all on one 2-vCPU box:

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

Known limits, in the order they matter:

1. **No multi-machine run yet.** `.github/workflows/cluster-bench.yml` (GitHub runners +
   Tailscale + R2, results in `bench-results/`) and `tools/cloud/` (`cluster.sh` over ssh,
   `bench.py` from a client VM) are the kits; the owner starts them.
2. **Distributed edges:** a `LIMIT` inside a subquery over sliced data and order-preserving
   shuffles run on one node; a join with a hot key on both sides shares out only one side; key
   ranges are found from the files, not declared (a table written out of order isn't sliced by
   them); a query's own answer still passes through the coordinator's memory once.
3. **Join order is only as good as its statistics.** Distinct values come from per-table
   sketches (about 6% off; rows deleted from keyed tables stay counted), and the order the query
   wrote is the baseline to beat. Declaring files' order to DataFusion made TPC-H slower (round
   15), so an aggregation on a sorted key still hashes.
4. **Replicated acks' window.** An acked write survives any one node dying (with `--fsync`,
   followers' power loss too), but not the leader and every holder dying before the bucket has
   it.
5. **One sequencer per lake** orders commits. Attached lakes split the load across leaders, but
   there are no transactions across lakes.
6. **Memory is bounded by budgets, not by accounting.** What DataFusion counts is the big hash
   tables and sort buffers; Parquet decoding and the batches in flight are not counted, so the
   query budget defaults to a third of RAM and the hot columns watch the process's own memory.
7. **Kafka's edges:** one partition per topic, no transactions, sparse offsets, consumer groups
   in the leader's memory.
8. **Streaming:** one watermark per source (not per partition or node), held by a quiet source;
   no sliding windows, timers or CEP; an as-of join in a view joins what the table has when the
   event arrives (Flink's temporal join waits for the table's watermark); keyed tables keep only
   their latest row, so as-of joins need a table's history kept as rows.
9. **Security:** tokens per role only; no TLS (use a proxy), no per-table grants or quotas.
10. **`VARIANT` is JSON text**, not a shredded variant; `ai_*` and Flight functions call out of
    the process, so their latency is the endpoint's.
11. **Packages built, not published.** PyPI and npm names and the repository's visibility are
    the owner's call; the macOS, Windows and ARM Linux builds exist only in the release
    workflow, which hasn't run yet. Only `sum` over DOUBLE is order-independent (not `avg`,
    `stddev`, …).

Good next moves: `docs/roadmap.md` (2026-09-28, after round 18) is the plan, with the reasons.
Rounds 17 (install anywhere) and 18 (a database you can shape) are done except what needs the
owner: publishing the packages, and a cluster-bench run that completes. In short:

1. **Round 19, change any row (the owner's request):** `UPDATE`, `DELETE` and `MERGE` on append
   tables too, with system columns — a row id assigned at commit (Iceberg v3's row lineage is
   the model), the commit time, a version — and streaming following every change (views, the
   change feed, Kafka consumers, Delta and Iceberg readers). Design first: an ADR before code.
   Also: materialized views filled from the rows already there.
2. **The cluster bench:** once the owner reruns `cluster-bench.yml` with 3 nodes on round 18's
   code (`bench-bin/pondra` holds it), read `wire_mb`, `wait_s` and `network` in
   `bench-results/<run id>/results.json` before changing how queries spread; then 6 nodes.
3. **Publish:** once the owner reserves `pondra` on PyPI and npm and picks a license, tag
   `v0.18.0` and let `.github/workflows/release.yml` build, try and publish.
4. **Then:** proof at scale (TPC-H SF10 on 1/3/6 machines, Nexmark, sqllogictest), a web console
   and live queries, the in-process module, TLS and grants, the browser (roadmap rounds 20–24).

The owner decides whether the repo goes public (or a public bench repo holds only the
workflow), whether to link their Windows laptop, and the package names on PyPI, npm and crates.io.

## Conventions

- Comments explain *why*, in plain English; the code shows *what*. Keep functions small.
- No new dependencies without a real reason; no new always-on services, ever.
- Every new invariant gets a test in `tools/` that would fail without it.
- Docs live in `docs/`; a design change means a new ADR, not an edit to an old one.
