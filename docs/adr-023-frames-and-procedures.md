# ADR-023: Frames, macros and procedures — SQL and Python as one language

**Status:** Accepted, built and tested (round 22) · **Date:** 2026-09-27 · **Builds on:** ADR-019
(a database you can shape), ADR-020 (change any row), ADR-022 (the DataFrame API's design,
`docs/dataframe-api.md`)

## Context

Round 22 was planned as the DataFrame API designed in round 21 (`dataframe-api.md`), with a web
console and live queries. The owner added one idea:

> "Maybe in the future, maybe we can have stored procs/macros on the catalog. And maybe on that
> we might have python language or running python file option inside those procs alongside the
> sql. It is just my idea, you can ignore it if you want. I just prefer flexibility."

A Python procedure is a frame program kept in the catalog under a name, so the two belong
together. The owner chose **frames and procedures** for this round; the console, live queries
and dbt move to round 23.

## Decisions

### 1. Frames compile to one SQL statement, a CTE per step

`pondra.frame` is Polars' lazy API over a lake: `con.table("orders").filter(col("amount") >
100).group_by("user").agg(col("amount").sum())`. Each method returns a new frame whose step is a
CTE; `frame.sql` is the statement (readable, pasteable into any SQL client), and `collect()`
sends it to the node. A frame is therefore exactly as fast as its SQL, spreads over the nodes
like any query, is remembered like any query, and every door (HTTP, Postgres, Flight SQL, MCP)
runs the very same thing. Nothing about frames is in the engine.

Where Polars and SQL mean different things, the SQL says Polars':

| | Polars | SQL | frames write |
|---|---|---|---|
| a sort's nulls | first, either direction | last ascending, first descending | `NULLS FIRST` unless `nulls_last=True` |
| `/` | a float | integer division for integers | `CAST(a AS DOUBLE) / b` |
| `round` | half to even | half away from zero | a `CASE` for the exact halves |
| `clip` of a null | null | `greatest()` skips nulls | `CASE WHEN x IS NULL` |
| `n_unique` | counts null as a value | `count(DISTINCT)` doesn't | `+ max(x IS NULL)` |
| `0.9` | a float | Pondra reads it as DECIMAL (TPC-H) | `CAST('0.9' AS DOUBLE)` |
| a column's name | the first column in the expression | DataFusion's text | `AS "name"` |

**A sort survives the steps after it.** DataFusion drops the `ORDER BY` of a CTE or a subquery
(SQL allows it), so `sort().limit(5)` written as two CTEs returned five rows in scan order. A frame
keeps its sort (`Frame._order`) and puts it in every step that keeps order — filters, column
changes, limits — until a step drops a column it sorts by, groups, or joins (as Polars' order
ends there too). SQL text given to `con.sql` that ends in `ORDER BY` columns is read the same way
(`trailing_order`), so `con.sql("… ORDER BY total DESC").limit(10)` is the top ten.

### 2. SQL and Python, either way round

The six rules of `dataframe-api.md` are built as designed:

1. `con.sql(query)` is a lazy frame; other statements (writes, DDL, `CALL`, several at once) run
   at once and return their outcome, as before.
2. **SQL reads Python by name.** When the node answers that a table isn't there, the client looks
   the name up where the SQL was written — this connection's temporary views, the caller's local
   and global names — and asks again: a Pondra frame becomes a CTE of the same statement (in the
   query's own `WITH`, so its `ORDER BY` stays on top); pandas, Polars and Arrow data travel with
   the request (`application/vnd.pondra.request`: the JSON's length, the JSON, then each table as
   its length and Arrow IPC) and are that request's own tables, on that node only. A lake table
   wins over a Python name, as DuckDB's does. `{name}` and keywords are the explicit form;
   other keywords are `$name` parameters.
3. Frame methods take SQL snippets: `filter("amount > 100")`, `with_columns("qty * price AS
   total")`, `agg("sum(total) AS revenue")`.
4. One set of names: `to_view` makes a frame a view (or, `materialized=True`, a live one) that
   `.sql` files and every client read; `con.run("model.sql", since=…)` runs a file's statements
   with parameters; `pondra run model.sql lake --since …` does the same from the command line.
5. `%load_ext pondra` gives notebooks `%%sql` cells (`%%sql top <<` keeps the frame).
6. One result type (`collect()`: a pyarrow Table; `to_pandas`, `to_polars`, `rows`).

A write that names a frame (`INSERT INTO t SELECT * FROM recent`) can't take a CTE in front of
it, so the request carries the frame as a *view of its own*: the node puts the frame's query in
place of the name in the statement's syntax tree (`routines::Expander`), wherever it appears.

### 3. `pondra.spark`: PySpark's names over the same frames

`from pondra.spark import SparkSession, functions as F, Window` instead of `pyspark.sql`, and a
job runs unchanged: a DataFrame is a frame with PySpark's names and meanings — ascending sorts
put nulls first, `/` is a double, columns are named as PySpark names them (`sum(amount)`,
`count(1)`, `(a + 1)`), a join by names has its keys once (coalesced for a full join), names
match regardless of case, `drop` of an unknown column is no error, `F.concat` with a null is null.
Joins by a condition keep the join's aliases for the step after it (`df.alias("a")` …
`F.col("a.x")`), and a column taken from a DataFrame joined earlier (`orders.o_orderkey`) means the
column of the side that holds it now. `DeltaTable.forName(…).merge(…)` builds a `MERGE` as Delta
on Spark does. RDDs, Python UDFs on the nodes and Structured Streaming raise
`NotImplementedError` naming the way round.

### 4. Macros: SQL with parameters, expanded where SQL comes in

DuckDB's `CREATE MACRO`: an expression (`CREATE MACRO net(x, rate := 0.2) AS x * (1 - rate)`)
or a query (`CREATE MACRO recent(days) AS TABLE SELECT …`). They live in the catalog (`r/`), so
every node has them. A call is replaced by its body, arguments in place of parameters (by
position, by name with `:=` or `=>`, or the default), in the syntax tree, where SQL comes in: every
door, a materialized view's SQL when it is made, stored views as they are read. After that it is
plain SQL — it plans, spreads, and is remembered like any other, and the coordinator of a spread
query sends the expanded statement, so every node runs the same one.

- A stored view reads macros as they are now; a materialized view keeps them as they were when it
  was made (it has been adding up rows since).
- Macros may call macros, 16 deep (a loop is an error, not a hang). A macro may not take the name
  of one of SQL's own functions.
- The list is read once per catalog version (`routines::listed`); a statement that names none
  isn't parsed at all.

Why not DataFusion UDFs with `simplify`? A scalar function's return type must be known before
the body is placed, and the body would miss type coercion; a table function can't plan SQL
(it is synchronous). Replacing the text is exact and short.

### 5. Procedures: SQL or Python, with the caller's rights

```sql
CREATE [OR REPLACE] PROCEDURE load_day(day DATE, source VARCHAR DEFAULT 'web')
LANGUAGE sql AS $$
  DELETE FROM daily WHERE d = $day AND src = $source;
  INSERT INTO daily SELECT … WHERE ts::DATE = $day;
$$;
CALL load_day(DATE '2026-09-27');
```

Postgres's form (sqlparser reads MSSQL's; Pondra parses this one itself, `routines::statement`).
`LANGUAGE python` bodies are Python. `CALL` works over HTTP, Postgres and MCP, from Python
(`con.call`) and JavaScript (`callProcedure`); a Python function becomes one with
`@con.procedure`, its parameters' types from its annotations, and a Python file with
`con.create_procedure("rollup", file="jobs/rollup.py", params={"day": "DATE"})` — the file's
text is kept in the lake, so every node runs the same code, whatever is on its disk.

- **Arguments are worked out once**, as the caller, cast to their declared types
  (`SELECT CAST((arg) AS type)`), then bound as exact values (`arrow_cast('…', 'type')`): `CALL
  p(random())` is one value in every statement, and a subquery argument reads the lake once.
- **Each statement runs as if the caller had sent it** (`routines::one`): a reader's `CALL` of a
  procedure that writes is refused at the write. Making a procedure or macro needs the admin
  token, as every DDL does.
- **A job makes a `CALL` or a script exactly-once**: each statement gets `{job}:{i}`; a Python
  procedure's connection numbers its own writes under the caller's job.
- **The answer** is the last statement's (rows, or a write's outcome).
- **16 deep**: procedures calling procedures stop there, SQL or Python.

**Python procedures run beside the node, not in it.** The binary stays small and has no Python in
it (principle 1): a node started with `--python <python>` runs `python -m pondra.procedure` for
each call, hands it the body, the arguments (one Arrow row) and a connection back to the node,
and reads the answer. The connection's token is **lent** (`auth::lend`): it has the caller's
role, and whether the caller may read this machine's files, and it dies when the procedure ends.
The body runs like a notebook cell — `con`, `pondra` and the parameters are names, and its last
line's value is the answer: a frame (its SQL then runs on the node: it may spread, nothing crosses
twice), a table (pyarrow, pandas, Polars, rows), a value, or nothing. What it prints goes to the
node's log; an exception comes back as the error.

A Python procedure can do anything on the machine that runs it. So:

- only an admin token makes one;
- a node runs them only with `--python`, and a node without tokens takes `--python` only when it
  listens on 127.0.0.1 (`pondra.local()` passes its own Python, so a notebook's procedures just
  work);
- **SQL from users still never touches a node's disk** (invariant 21): a Python procedure is code
  the lake's admin wrote, run with the rights of whoever calls it.

### 6. Scripts: several statements, `$name` parameters

`POST /sql` takes several statements (split outside strings, `$$` bodies and comments:
`routines::statements`, which the shell and the Postgres port now use too), with `$name`
parameters in JSON (`{"sql": …, "params": {"day": "2026-09-27", "n": 3, "at": {"sql": "now()"}}}`),
bound in the syntax tree — values are quoted by the node, so a parameter can't become SQL. The
answer is the last statement's. A single query with nothing attached still goes the old way (a key
lookup without planning, a remembered answer).

### 7. MCP: every procedure is a tool

An agent's `tools/list` shows each stored procedure as a tool, its parameters the tool's input
schema (and the body's first comment its description); `tools/call` runs `CALL` with the agent's
token. What the lake's owner wrote for a job becomes something an agent can do, with no more
rights than its token has.

## What the tests caught

- **The sort dropped in a CTE** (above): `sort().limit()` returned rows in scan order.
- **A Python float became a DECIMAL** (Pondra parses `0.9` as DECIMAL for TPC-H): TPC-H q15 as a
  frame compared a DOUBLE with the decimal of its own maximum and found nothing. Floats are
  written `CAST('…' AS DOUBLE)`.
- **An empty answer lost its columns** (no batches, so an empty schema): a frame's schema — asked
  with `LIMIT 0` — was empty. A query with no rows now returns one empty batch with its columns.
- **A name bound into SQL that ends with `ORDER BY`** wrapped that SQL in a CTE and lost the
  order; frames now go into the query's own `WITH`.
- **PySpark's `df.col` references across several joins** pointed at an alias that no longer
  existed; the join now maps every DataFrame joined before to the side that holds it.
- **A select after a join by names** read the raw join again, where the key was ambiguous.
- **Polars' rounding and `clip`** differed at exact halves and nulls.
- **A nested Python procedure's error** was the whole traceback, sixteen times over; the error
  line comes back now, the traceback goes to the log.

## What it costs

About 530 lines of Rust in all (`routines.rs` is 630; the shell lost its own statement splitter)
and 2,400 of Python (`frame.py` 850, `spark/` 1,180, `client.py` 400, the procedure runner and the
notebook magic 100). No change to the engine: a frame is only SQL, so it is exactly as fast.

`con.sql(query)` is lazy now: it runs when its rows are asked for, and each time they are (a
query kept in a variable answers with the lake as it is then, not as it was).

## What is still open

- A frame over a raw `con.sql` whose sort is by an expression (not a column) loses it after the
  next step; sort in the frame instead.
- `pondra run models/` (a folder of `.sql` and `.py` models in order of what reads what) is the
  next step of rule 4.
- The JavaScript client has parameters, `run` and `callProcedure`, not the frame builder yet.
- Python procedures run one process per call (about 0.1–0.2 s to start, most of it importing
  pyarrow); a pool of warm workers would take that away.
- A `MERGE` from rows sent with a request runs only where the leader is (a follower forwards the
  statement, not the rows).
- Procedures don't run on a schedule yet (a task that `CALL`s one).

## Tests

- `tools/frames_check.py`: 26 pipelines in `pondra.frame` and Polars, equal value by value; one
  question asked ten ways (SQL, frames, both mixed, pandas in SQL, a `.sql` file, a view from a
  frame, a `%%sql` cell, a Python procedure called from SQL and Python) with one answer; a sort
  kept through the steps after it.
- `tools/spark_check.py`: 44 pipelines written once and run on PySpark 4.0.1 and on
  `pondra.spark`: equal values and equal column names.
- `tools/bench/tpch_frames.py`: the 22 TPC-H queries as SQL, as frames and as PySpark code:
  equal answers.
- `harness.py procedures`: 29 checks of macros and procedures on three nodes with tokens (see
  invariants 81–88 in `AGENTS.md`), also on simulated and real R2.
- Each new rule was built out once to see its test fail: arguments put in as text (random() two
  values), a lease with more than the caller's rights (a reader's procedure wrote), a splitter
  without `$$` (the procedure couldn't be made), an empty answer without columns (frames couldn't
  learn their schema), frames without their sort (sort-then-limit returned other rows).
