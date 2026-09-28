# ADR-028: One vocabulary, open writes, live answers (round 25)

**Date:** 2026-09-28 · **Status:** accepted and built (round 25, 0.25.0) · **Follows:** ADR-025 (one name, one meaning), ADR-026 (read and write anything), ADR-027 (SQL and Python as one)

## Context

The owner, 2026-09-28, after round 24:

- **Names.** Reading is `read_*` in some places and `scan_*` in others, writing `sink_*` or
  `write_*`. "I would go for standard names everywhere, but still having the older function names
  as fallback for compatibility with Polars, Spark, DuckDB. Because in the end we will be
  independent. So docs should show our standard names as main and the others as an option too."
- **Others writing Pondra's tables.** Pondra reads and writes other engines' Delta and Iceberg
  tables, but other engines can only read Pondra's. "Would be better if others are able to write
  too. We can't use Polaris and Unity as our catalog, right? I think they are JVM and heavier."
- **query.farm's HTTP caching for DuckDB's remote functions:** round 24 sends each distinct
  argument once a batch; a result reused for a while was left for later.
- **Scope of this round** (the owner's choice, "split: engine first"): round 25 is the engine
  (E9, E10, G8, B3, temporary tables, changes to attached lakes); round 26 the console,
  `--server`, dbt and BI tools; Postgres and MySQL attached (G6) after that.

Two gaps from the owner's sessions in the shell come with it:

- `UPDATE`, `DELETE` and `MERGE` on an attached lake run only on that lake's own node, while
  `INSERT` works from any node.
- `CREATE TEMP TABLE` makes an ordinary table.

## Decision

### 1. One vocabulary: Pondra's names first, the tools' names as fallbacks (E9)

Reading is `read_<format>`, writing `write_<format>`, in SQL and in every client:

| | SQL | Python (connection, frames) | PySpark (`pondra.spark`) | JavaScript |
|---|---|---|---|---|
| read files | `read_parquet`, `read_csv`, `read_json` | `db.read_parquet(…)`, `pondra.read_parquet(…)` | `spark.read.parquet(…)` | `db.sql("… read_parquet(…)")` |
| read a Delta / Iceberg table | `read_delta`, `read_iceberg` | `db.read_delta(…)`, `db.read_iceberg(…)` | `spark.read.format("delta").load(…)` | as SQL |
| a lake's table | `t`, `lake.schema.t` | `db.table("t")` | `spark.table("t")` | as SQL |
| write files | `COPY (…) TO 'x/' (FORMAT parquet)` | `frame.write_parquet(…)`, `write_csv`, `write_json` | `df.write.parquet(…)` | as SQL |
| write a Delta / Iceberg table | `COPY (…) TO 'x/' (FORMAT delta)` | `frame.write_delta(…)`, `write_iceberg` | `df.write.format("delta").save(…)` | as SQL |
| into a lake's table | `INSERT INTO t …`, `CREATE TABLE t AS …` | `frame.write_table("t", mode=…)` | `df.write.saveAsTable("t")` | as SQL |

- **The fallbacks stay, and equal their standard names:** Polars' `scan_parquet`, `scan_csv`,
  `scan_ndjson`, `scan_delta`, `scan_iceberg`, `read_ndjson`, `sink_parquet`, `sink_csv`,
  `sink_ndjson`, `write_ndjson`; DuckDB's `parquet_scan`, `read_csv_auto`, `read_json_auto`,
  `read_ndjson`, `delta_scan`, `iceberg_scan`; PySpark's `spark.read` and `df.write`, which are
  the Spark layer's whole point.
- **The docs lead with Pondra's names**, the others in a column beside them.
- **Delta and Iceberg are written as files are**, and `COPY … TO` takes them. An empty folder gets
  a new table; one that holds a table takes `APPEND` or `OVERWRITE` (Spark's and Polars' modes),
  as a folder of Parquet files does. That makes `write_delta` and `write_iceberg` whole: before,
  an existing table had to be attached first.
- **JavaScript** has no frames: SQL is its API, and the names are SQL's.

### 2. Function results reused for a while (E10)

```sql
CREATE FUNCTION geocode(city VARCHAR) RETURNS VARCHAR LANGUAGE python
  WITH (cache = '10 minutes') AS $$ return requests.get(f"https://…?q={city}").json()["id"] $$;
```

- `cache = '<duration>'` on a Python function or table function: an answer is reused for that
  long. The key is the function's definition (a changed function never reuses an old answer) and
  its argument values. Only a call that succeeded is kept.
- Each node keeps its own answers in memory, least recently used out first, within
  `PONDRA_FUNCTION_CACHE_MB` (256). Nothing is kept for a function without `cache`.
- The caller is not part of the key: a function sees only its arguments (no connection, no
  secrets, no user). Once round 27 gives functions users, the user joins the key.
- `cache` makes the function's answers reusable whatever its volatility says (the user asked for
  it). It is refused on SQL functions (they are expanded into the query, whose answers the result
  cache already keeps) and on procedures (their effects are what they are for).

### 3. Other engines write Pondra's tables through its Iceberg REST catalog (G8)

Pondra already serves the Iceberg REST catalog protocol at `/v1` for reading (round 10). It now
takes commits there too:

```python
catalog = pyiceberg.catalog.load_catalog("pondra", uri="http://node:8080", type="rest")
catalog.load_table("default.events").append(arrow_table)       # PyIceberg
spark.sql("INSERT INTO pondra.default.events SELECT …")          # Spark with Iceberg's REST catalog
```

Polaris and Unity Catalog are JVM services that would have to become the source of truth. Pondra
plays their part itself; its catalog stays the truth.

- **An outside append is an `INSERT`.** The writer writes Parquet files under the table's
  `data/` folder and commits a snapshot. The node takes the files that snapshot adds, reads them,
  and puts their rows into the table as a bulk `INSERT` would: files with their rows' system
  columns (row ids, versions, times), partitioned and sized as the table says; or through the log
  when views or streaming tasks follow the table. The leader records it and publishes the table's
  next Iceberg version at once, under the writer's snapshot id, so the writer finds its commit.
  The writer's own files are deleted after that.
- **Iceberg's rules hold.** Requirements are checked atomically under the leader's lock:
  `assert-table-uuid`, and `assert-ref-snapshot-id` against the version last published. A
  conflict answers 409, and PyIceberg and Spark retry on top of the new version. A snapshot id
  already recorded is answered as done, so a retried commit is applied once.
- **What is taken:** `append` snapshots into append tables of this lake or an attached one, with
  data files in Parquet under the table's `data/` folder. Everything else is refused by name
  (400): deletes and overwrites (they go through Pondra's SQL: `UPDATE`, `DELETE`, `MERGE`),
  schema and property changes (`ALTER TABLE`), keyed tables (next: equality deletes as upserts),
  creating tables through the catalog, and files outside the table's folder.
- **Views and the Delta copy follow** as they do for an `INSERT`. A table publishing Iceberg now
  publishes its first version when it is made, so a writer finds an empty table.
- **Cost:** the rows are read and written once more by the node, as a bulk `INSERT`'s are.
  This buys row ids, the table's layout and partitioning, and everything that follows the table.

### 4. Live queries (B3)

```python
for rows in db.live("SELECT region, sum(amount) FROM orders GROUP BY region"):
    redraw(rows)                        # the answer now, then each time it changes
```

- `GET /live?sql=…` answers a query now, and again whenever a commit changes a table it reads
  (its own, through views, or an attached lake's), as JSON lines: `{"at": commit, "rows": […]}`.
- Answers that come out the same are not sent again. A busy table is queried at most once per
  `every_ms` (100 by default).
- Nothing runs when nobody listens: a live query is its open connection, and closing it ends it.
- Python: `db.live(sql, **params)` iterates answers (as the frames' rows). JavaScript:
  `for await (const rows of db.live(sql))`. Postgres has no way to push an answer, so the pg
  protocol gets none.

### 5. Temporary tables and views

```sql
CREATE TEMP TABLE picked AS SELECT id FROM orders WHERE flagged;
DELETE FROM picked WHERE id IN (SELECT id FROM refunds);
SELECT * FROM orders JOIN picked USING (id);
```

- `CREATE TEMP[ORARY] TABLE` and `CREATE TEMP VIEW` are the session's own, as in Postgres. Nobody
  else sees them. They shadow a lake table of the same name, and they are gone when the session
  ends.
- A temporary table lives in the memory of the node the session talks to, within
  `PONDRA_TEMP_MB` (1024) per node. `INSERT`, `UPDATE`, `DELETE`, `MERGE`, `DROP` and queries work
  as on a lake table. A query reading one runs on that node.
- **Sessions:** a Postgres connection is one. The Python and JavaScript clients send a session id
  (`x-pondra-session`) with each request, and end the session on `close()`. A session idle for an
  hour (`PONDRA_SESSION_IDLE_SECS`) ends by itself. A request with no session is refused a
  temporary table by name.

### 6. `UPDATE`, `DELETE` and `MERGE` on an attached lake, from any node

- The statement goes to that lake's leader, which carries it out as it does its own: one
  snapshot, under its lock, all or nothing, once (the job id).
- Tables of other lakes it reads (a `MERGE`'s source in this lake, a file on this machine) are
  read here and sent along with it. The other lake needs nothing from this one.

## Rejected

- **Polaris or Unity Catalog as the catalog:** JVM services, always on, and the source of truth.
  Pondra would become their client.
- **Letting outside writers' files stand as they are:** their rows would have no row ids, so
  `UPDATE`, `DELETE` and the change feed couldn't follow them. Views and streaming tasks would
  miss them too.
- **Answering a conflict by rebasing on the server** (taking an append whatever version it was
  written on): simple, but not Iceberg's contract. PyIceberg and Spark already retry.
- **Temporary tables as hidden lake tables:** objects in the bucket, a commit per write, and
  leftovers when a client dies. A session's scratch belongs in memory.
- **Renaming the old names away:** the fallbacks cost a line each, and nothing breaks for someone
  arriving from Polars, PySpark or DuckDB.

## Tests (the plan)

- **E9:** each fallback's answer equals its standard name's, in SQL, frames and Spark. The table
  of operations in `dataframe-api.md` is checked by a test: every name in it exists and runs.
  Delta and Iceberg tables written to a folder (new, `APPEND`, `OVERWRITE`) are read back by
  Pondra, and by delta-rs and PyIceberg.
- **E10:** a second query within the lifetime calls nothing; after it, it calls again; a changed
  function doesn't reuse; a failed call isn't kept.
- **G8:** PyIceberg and Spark 4 (Iceberg 1.10) append through `/v1`, and so does Pondra's own
  `INSERT` into another Pondra's table. The rows equal what was written, with row ids. A view of
  the table and its Delta copy follow. A stale snapshot gets 409 and the retry lands once; a
  retried commit is applied once. A delete, a schema change and a keyed table are each refused
  by name.
- **B3:** a live query answers within 100 ms of a commit that changes its table, and not for
  commits to other tables. It stops working when closed.
- **Temporary tables:** each statement works on them. Another session doesn't see them. They are
  gone after `close()` and after a Postgres disconnect. A spread query reading one runs on one
  node.
- **Attached lakes:** `UPDATE`, `DELETE` and `MERGE` from a node of another lake, with a source in
  this lake, equal the same statements run on the lake's own node; a retried job applies once.
- **Every new invariant** gets a test in `tools/` that fails without it.

## As built (round 25)

About 1,200 lines of Rust (25,090 in all): `live.rs` (150) and `temp.rs` (300) are new;
`iceberg.rs` takes commits, `change.rs` sends a change to another leader, `pyfn.rs` keeps answers,
`write_outside.rs` makes Delta and Iceberg tables in a folder.

1. **One vocabulary.**
   - SQL takes `read_delta` and `read_iceberg`. `COPY … TO '<folder>' (FORMAT delta | iceberg)`
     makes a table there (a first commit with the query's schema), adds to one with `APPEND`, or
     replaces its rows with `OVERWRITE`: Delta's removes carry each file's deletion vector;
     Iceberg's snapshot names only the new manifest.
   - Python has `db.read_*`, `pondra.read_*` and `frame.write_*`. The Polars names are the same
     functions, not copies.
   - `pondra.spark` has `df.write.format("delta" | "iceberg").save(path)` and `df.write.delta`.
   - `dataframe-api.md`'s table has 69 names; `harness.py names` runs each one.
2. **Answers kept.** `WITH (cache = '<duration>')` (the task schedules' parser) on a Python
   function or table function. `pyfn::Answers` keys an answer by the routine's JSON and the
   argument row (arrow's row format); kept answers are interleaved with fresh ones. A cached
   function gets each distinct argument once a batch even when vectorized. Two partitions of one
   query may both miss the same argument (the first query over 50 cities made 100 calls here);
   the next query makes none.
3. **Outside appends** (`iceberg::update`, `parse`, `check`, `record`).
   - The catalog now lists its endpoints in `/v1/config`, so PyIceberg refuses what isn't there
     (creating a table) before asking.
   - A table that publishes Iceberg is published at `CREATE TABLE`, so a writer finds it empty.
   - The node that takes a commit rewrites the writer's files (a follower too: it reserves row ids
     from the leader). The leader re-checks and records under the lake's lock, then publishes that
     one version under the writer's snapshot id, with the writer's summary keys, so Pondra's own
     `pondra.job` works against another Pondra.
   - Versions and snapshot ids are separate now: a version's number names its files, and its
     snapshot id is the writer's or the number.
   - Two findings while testing:
     - A commit sent again must be answered as done *before* its requirements are checked:
       otherwise the retry gets 409, since main moved to its own snapshot.
     - Iceberg 1.10's Spark reads its own manifest list after the commit (`committedSnapshot
       .allManifests`), so the writer's manifests go with the table's replaced files after the
       retention period, not at once. Deleting them at once cost nothing but a warning with a
       stack trace on every append.
4. **Live queries** (`live.rs`).
   - `GET /live?sql=` or `POST /live` with `/sql`'s body.
   - Each commit is looked at against a print of the query's tables: their catalog entries, and
     whether a new segment has rows for them (or their `$deleted`). The answer is sent only if it
     changed.
   - A newline every 15 s idle notices a client gone. `/stats` counts `live_queries`.
   - Python `db.live(sql_or_frame)`; JavaScript `db.live(sql)`, an async generator.
5. **Temporary tables** (`temp.rs`).
   - Batches in memory with their own system columns: `UPDATE`, `DELETE` and `MERGE` are worked
     out by `change::rows_of`, the same code as a lake table's, then applied here.
   - `session_at` registers them (and temporary views) over the lake's names.
   - `App::query_as` keeps a query reading one on its node, and `server::query` skips the result
     cache and the point-lookup path.
   - Sessions:
     - a Postgres connection (ended when it closes);
     - `x-pondra-session` (the Python and JavaScript clients; `close()` sends `DELETE
       /sessions/{id}`);
     - a lent token's caller: a procedure sees its caller's.
   - A reaper runs only while sessions exist. Any role may keep temporary tables
     (`auth::allows`).
6. **Changes across lakes** (`change::for_leader`).
   - The statement's relations are sorted out in the syntax tree. The target is named as its
     lake names it. Tables of this lake or a third, files (by then `ext:` names), function calls
     and the session's temporary tables are read here and sent as `__sent_N`, under their old
     names as aliases. Tables that are the target lake's own stay as they are.
   - The leader runs the change with them in `query::SENT`, as a request's own tables. A follower
     of the target's own lake uses the same path, so its files and temporary tables reach the
     leader too.

**Found and fixed in round 24's CI** (the three commits before round 25):

- two checks too tight for GitHub's runner;
- a race: a dead worker's exit status read before the OS had it;
- the spread guard choosing by a query's last time, not its best recent one.

**Tests** (`logs/round25/`):

- **New `harness.py` sections,** also run by CI:
  - `names` (7 checks);
  - `answers` (7);
  - `writes` (7: PyIceberg through a follower, 409 and retry, a commit sent twice, a view and the
    Delta copy, an attached lake, Pondra writing to Pondra, five refusals);
  - `live` (4, JavaScript among them);
  - `temps` (8, three nodes, Postgres);
  - `across` (3).
- `schemas`' attached-lake check now expects the change to be carried out.
- `formats_check.py` has Spark 4 appending through Pondra's catalog (`INSERT` and
  `writeTo().append()`), and its `DELETE` and `ALTER TABLE` refused.

**Found at the end of the round**, by running the quick-start notebook on the wheel
(`anywhere_check.py`), and fixed:

- **A Delta or Iceberg folder named relatively** (`write_delta("out/orders")`, as a notebook
  writes one) couldn't be read back: its files were named relatively, and Iceberg's `location`
  was not absolute. A relative folder is now where the node runs, made absolute with `.` and `..`
  taken out (object stores refuse them). `names` checks it.
- **A frame's display in a notebook** had failed since 0.22.1. `_repr_html_` called `all(…)`, and
  in `frame.py` that is Polars' `all()`. IPython printed the error, and the frame showed as text.
  `package_check.py` checks the display now. `anywhere_check.py` fails on any error output a
  notebook shows, not only on a failed cell.
- **The notebook's outside-append cell** used a table name an earlier cell had taken with other
  columns. It is `gauges` now.
- **On R2, PyIceberg's own writes need its fsspec file IO** (`py-io-impl`), as `lake-format.md`
  says for reads: pyarrow's multipart upload was refused. `harness.py writes` sets it, and the
  README says so.
- **`across`' last check** read the lake from another node before that node had seen the commit
  (on the R2 simulator). It waits for it now, up to 30 s.

**Results** (`prototype-status.md` has them all): every suite passes on local disk, the R2
simulator and real R2. The sqllogictest count is 16,646 of 24,783 (+1). TPC-H SF1 is 2.06–2.11 s
on one node against DuckDB's 3.35–3.45 s over the same Parquet, the same ratio as round 24 on a
slower day. Live answers arrive 9–19 ms after the statement on local disk, and 0.4–0.5 s on R2
(the commit's own round trip included). The outside-append copy costs the node 1.1 CPU-seconds
for 4 M rows (`tools/bench/outside_append.py`); ADR-029 proposes removing it.
