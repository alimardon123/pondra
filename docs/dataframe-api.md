# Pondra's DataFrame API: a design (round 21, built in round 22)

**Status:** built in round 22 (ADR-023; what differs from this design is listed at the end) ·
**Date:** 2026-09-27 · **Part of:** ADR-022 · **Asked for by the owner:**
"a Python DataFrame API, as easy as Polars or directly like PySpark for easy migration, with the
SQL side converged in it and in any other API" — and then, to be precise: "an easy way to use
both SQL and Python DataFrames that feels natural and native, using them interchangeably in
Python and in SQL files."

## What it has to be

- **One engine, one meaning.** A DataFrame is another way to write a query, not another engine.
  Whatever SQL can do on a lake — spread across nodes, the guard, remembered answers, system
  columns, changed rows, attached lakes — a DataFrame does the same way,
  because it becomes the same query.
- **Two dialects, one tree.** A Polars-style lazy API (`pondra.frame`) for new code, and a
  PySpark-shaped one (`pondra.spark`) so a PySpark job moves by changing its imports. Both build
  the same expression tree.
- **SQL and Python are one language, either way round** (the next section): SQL gives a frame,
  SQL names frames and pandas/Polars/Arrow data by their Python names, frame methods take SQL
  snippets, `.sql` files and Python share one set of names, and a notebook cell can be SQL.
  Every frame shows the SQL it runs (`frame.sql`).
- **Small and dependency-free.** Pure Python over the existing client: `pyarrow` for results,
  nothing else. The JavaScript client gets the same builder later, from the same design.
- **Streaming is the same API.** A frame that is written as a materialized view keeps up with its
  sources; `watch()` streams its rows.

## SQL and Python, used interchangeably

The test of "natural" is that nobody has to decide up front: a pipeline starts in whichever is
easier and moves to the other at any step, with no conversion, copy or export in between. Six
rules give that:

**1. SQL gives a frame, and a frame keeps going in Python.** `con.sql(…)` doesn't run anything:
it returns a lazy frame like `con.table(…)`, so Python can go on from it.

```python
big = con.sql("SELECT * FROM orders WHERE amount > 100")        # a frame, not rows
top = big.group_by("user").agg(col("amount").sum().alias("total")).sort("total", descending=True)
top.collect()                                                   # one SQL statement runs, now
```

**2. SQL reads Python by name.** A name in SQL that isn't a lake table is looked up among the
caller's Python variables (as DuckDB does): a Pondra frame becomes a CTE of the same statement
(nothing moves, it still runs on the cluster); a pandas, Polars or Arrow table is sent along with
the query as Arrow, a temporary table only that query sees. Passing them by keyword is the
explicit form, as PySpark's `spark.sql("… {df} …", df=df)`.

```python
recent = orders.filter(col("ts") >= "2026-09-01")               # a Pondra frame
targets = pd.read_csv("targets.csv")                            # pandas, on this machine
con.sql("""SELECT r.user, sum(r.amount) AS spent, t.target
           FROM recent r JOIN targets t USING (user) GROUP BY r.user, t.target""")
con.sql("SELECT * FROM {r} WHERE amount > $min", r=recent, min=100)   # explicit, with a parameter
```

**3. Frame methods take SQL where SQL is shorter.** Any expression can be a SQL snippet
(Polars' `sql_expr`, PySpark's `expr`/`selectExpr`), so no one rewrites a `CASE` or a date
function into method calls.

```python
orders.filter("amount > 100 AND item LIKE 'tea%'") \
      .with_columns("qty * price AS total", size="CASE WHEN qty > 10 THEN 'big' ELSE 'small' END") \
      .group_by("item").agg("sum(total) AS revenue", "count(*) AS n")
```

**4. One set of names for `.sql` files and Python.** Tables, views and stored views are the
lake's, seen the same way from both. A `.sql` file's `CREATE VIEW` is a frame in Python
(`con.table("clean_orders")`); a frame saved with `to_view("…")` is a view a `.sql` file reads
(`to_view(temporary=True)` keeps it to the session, `materialized=True` keeps it up to date).
The connection has the same verbs, taking the frame, SQL or Python data as the second argument:
`con.view(name, frame_or_sql, materialized=…)` is `frame.to_view(name, materialized=…)`, and
`con.write_table(name, data, mode)` is `frame.write_table(name, mode)` (ADR-025).
Python runs `.sql` files, with parameters and the Python names of rule 2:

```sql
-- models/clean_orders.sql
CREATE OR REPLACE VIEW clean_orders AS
SELECT * FROM orders WHERE qty > 0 AND ts >= $since;
```

```python
con.run("models/clean_orders.sql", since="2026-01-01")   # every statement, in order
by_user = con.table("clean_orders").group_by("user").agg(pondra.len().alias("n"))
by_user.to_view("orders_by_user")                        # now SQL (a .sql file, psql, BI) sees it
```

From the command line the same file runs with `pondra run models/clean_orders.sql --since
2026-01-01`, or `pondra sql < file.sql` as today.

**5. A notebook cell can be SQL.** `%load_ext pondra` adds `%%sql` cells (and `%sql` lines) that
see Python names as in rule 2 and hand their result back as a frame:

```python
%%sql top_users <<
SELECT user, sum(amount) AS total FROM recent GROUP BY user ORDER BY total DESC LIMIT 10
```

`top_users` is then a Python frame (`top_users.to_pandas()`, `top_users.join(…)`).

**6. One result type, one meaning.** Whichever way a query was written, it is one SQL statement
on the engine — the same plan, speed and answer — and its result is the same frame, with
`to_arrow()`, `to_pandas()`, `to_polars()`, `rows()` and `watch()`. Functions cross too: a SQL
expression is a frame expression (rule 3), and a Python function registered with
`@pondra.function` is callable from SQL (it runs out of the node, over Arrow Flight, as
`udf_server.py` does today).

**Pipelines of both kinds of file (after the frames):** a folder of `.sql` and `.py` models run
in order of what reads what (`pondra run models/`): each `.sql` file defines a view or table,
each `.py` file a frame (`@pondra.model("name")`) saved the same way — so a pipeline is written
file by file in whichever language suits that step, as dbt and SQLMesh allow, without their
separate runtime.

The JavaScript client follows the same rules later, with a tagged template for rule 2:
``con.sql`SELECT * FROM ${frame} WHERE amount > ${min}` ``.

## Why frames compile to SQL text

Three ways were weighed:

| | Frames become… | For | Against |
|---|---|---|---|
| **A. SQL text** (chosen) | one SQL statement, CTEs for the steps | every door (HTTP, Flight SQL, Postgres, the shell) already takes it; `frame.sql` is readable and pasteable; nothing new on the server | a builder must quote names and map functions carefully |
| B. Substrait plans | a protobuf plan DataFusion reads | exact types, no text round trip | a second way into the engine to keep correct; unreadable for users; few clients speak it |
| C. Spark Connect | Spark's own gRPC protocol, served by the binary (as Sail does on DataFusion) | the real `pyspark` client, and Scala and Java Spark Connect clients, unchanged | a large protocol (hundreds of message types) and Spark SQL's semantics to match; only worth it for full fidelity |

A is round 22. C stays the option for later: if users bring Spark jobs in Scala or Java, or need
PySpark behaviour the shim can't give, a Spark Connect server in the binary is the answer.

## `pondra.frame`: the Polars-style API

```python
import pondra
from pondra import col, lit, when

con = pondra.connect("http://node:8080")        # or pondra.local("lake")
orders = con.table("sales.orders")               # a LazyFrame: nothing runs yet

top = (orders
       .filter(col("amount") > 100)
       .with_columns((col("amount") * 0.9).alias("net"))
       .group_by("user")
       .agg(col("net").sum().alias("total"), pondra.len().alias("n"))
       .sort("total", descending=True)
       .limit(10))

top.sql                  # the SQL it becomes
top.collect()            # a pyarrow.Table (also .to_polars(), .to_pandas(), .rows())
top.explain()            # Pondra's plan, and whether it would spread
```

| Area | API | Becomes |
|---|---|---|
| Sources | `con.table(name)`, `con.sql(query, **frames_and_params)`, `con.run("file.sql", **params)`, `con.from_arrow(t)` / `from_pandas` / `from_polars` | the table; `(query)` with frames as CTEs; a file's statements; the rows sent with the query (Arrow, one query's temporary table) |
| SQL inside | `filter("…")`, `with_columns("… AS x", y="…")`, `agg("sum(x) AS s")`, `pondra.sql_expr("…")` | the snippet, as written, in its place |
| Back to SQL | `to_view(name, temporary=, materialized=)` (or `con.view(name, frame, …)`), `frame.sql` | `CREATE [OR REPLACE] VIEW`, session-only, or `CREATE MATERIALIZED VIEW` (kept up to date); the statement itself |
| Rows | `filter`, `head`/`limit`, `sort(…, descending=, nulls_last=)`, `unique(subset=)`, `sample` | `WHERE`, `LIMIT`, `ORDER BY`, `DISTINCT ON`-style `ROW_NUMBER() = 1`, `TABLESAMPLE` |
| Columns | `select`, `with_columns`, `drop`, `rename`, `cast` | the projection |
| Expressions | `col`, `lit`, `+ - * / // %`, comparisons, `& \| ~`, `is_null`, `is_in`, `between`, `when().then().otherwise()`, `.str.*`, `.dt.*`, `.alias`, `.over(…)` | SQL expressions; `CASE`; window functions |
| Aggregation | `group_by(…).agg(…)`, `sum/mean/min/max/count/n_unique/first/last/quantile`, `pondra.len()` | `GROUP BY` (`sum` over DOUBLE stays order-independent: `fsum.rs`) |
| Joins | `join(other, on=, left_on=, right_on=, how="inner/left/right/full/semi/anti/cross")`, `join_asof(other, on=, by=, strategy="backward/forward")` | `JOIN`; `ASOF JOIN` (`asof.rs`) |
| Sets | `pondra.concat([a, b])`, `union`, `intersect`, `except_` | `UNION ALL` / `UNION` / `INTERSECT` / `EXCEPT` |
| Time | `con.table(name, at=commit)` | **not built yet** (see "As built" below): it would read the table as of that commit |

Writes and streams are the same verbs SQL has, so they keep SQL's guarantees (the leader
records them; a job id makes a retry a no-op):

```python
top.write_table("reports.top_users", mode="create")    # CREATE TABLE … AS SELECT
orders.filter(col("day") == "2026-09-27").write_table("archive.orders", mode="append")   # INSERT … SELECT
con.table("users").update({"score": col("score") + 1}, where=col("id") == 7)             # UPDATE
con.table("users").delete(where=col("active").not_())                                    # DELETE
(con.table("users").merge(updates, on="id")                                              # MERGE
    .when_matched_update({"score": col("s.score")})
    .when_not_matched_insert()
    .run())
per_minute = (con.table("clicks")
              .group_by(col("ts").dt.truncate("1m").alias("w"), "user")
              .agg(pondra.len().alias("n")))
per_minute.to_view("clicks_per_minute", materialized=True, window="w", size_secs=60)    # CREATE MATERIALIZED VIEW … WITH (…)
for rows in con.table("clicks_per_minute_final").watch():                              # the view's rows as they come
    ...
```

`merge` takes Delta Lake's builder names (`when_matched_update`, `when_not_matched_insert`,
`when_not_matched_by_source_delete`), so code written for delta-rs or Delta on Spark reads the
same.

## `pondra.spark`: PySpark, moved by its imports

```python
from pondra.spark import SparkSession, functions as F, Window   # was: from pyspark.sql import …

spark = SparkSession.builder.remote("pondra://node:8080").getOrCreate()   # or .master("local[*]"): a local lake
df = (spark.table("sales.orders")
      .where(F.col("amount") > 100)
      .withColumn("net", F.col("amount") * 0.9)
      .groupBy("user").agg(F.sum("net").alias("total"), F.count("*").alias("n"))
      .orderBy(F.desc("total")).limit(10))
df.show(); df.toPandas()
df.write.mode("overwrite").saveAsTable("reports.top_users")
spark.sql("SELECT * FROM reports.top_users").show()
```

The shim is a thin layer over `pondra.frame`: the same tree, PySpark's names and defaults.

- **Covered first** (what most jobs use):
  - `SparkSession`: `sql`, `table`, `createDataFrame`, `read.parquet/csv/json` (from the lake's
    files or the caller's own machine, as the shell reads them), `catalog.listTables`.
  - `DataFrame`: `select`, `selectExpr`, `filter`/`where`, `withColumn(s)`,
    `withColumnRenamed`, `drop`, `groupBy().agg/count/sum/avg/min/max`, `orderBy`/`sort`,
    `limit`, `join`, `union`/`unionByName`, `distinct`, `dropDuplicates`, `na.fill/drop`,
    `collect`, `toPandas`, `toArrow`, `show`, `count`, `first`, `take`, `createOrReplaceTempView`,
    `cache` (a no-op: the lake is the cache), `explain`.
  - `write`: `mode`, `saveAsTable`, `insertInto`.
  - `functions`: `col`, `lit`, `expr`, `when/otherwise`, arithmetic, `sum/avg/count/countDistinct/min/max/first/last`,
    `date_trunc/to_date/to_timestamp/year/month`, `concat/lower/upper/substring/regexp_replace`,
    `coalesce`, `row_number/rank/dense_rank/lag/lead` with `Window.partitionBy().orderBy()`.
  - `delta.tables.DeltaTable.forName(spark, name).merge(…)`: `MERGE`.
- **Spark's semantics where they differ from DataFusion's** are written out in the SQL:
  - `NULLS FIRST` in ascending sorts;
  - Spark's integer division, which gives a double;
  - column names that match regardless of case.
- **What it doesn't cover** raises `NotImplementedError` naming the method and the way around it
  (usually `spark.sql`), never a silently different answer. RDDs, UDFs in Python on the nodes
  and Structured Streaming's `readStream` are outside it. Streaming goes through materialized views
  instead.
- **`createOrReplaceTempView`** keeps the frame in the session (client side), expanded as a CTE
  in later `spark.sql` text, as Spark scopes temp views to the session.

## How it is tested

- **Differential tests:** the same pipelines run in Polars (local, over the same Parquet) and in
  PySpark (local mode), and on Pondra through `pondra.frame` and `pondra.spark`. The answers are
  compared value by value, as `asof_check.py` does with DuckDB: about 60 pipelines covering every
  row of the tables above, one node and three.
- **Mixed pipelines:** the same pipeline written as SQL alone, as frames alone, and mixed every
  way (SQL then frames, frames named in SQL, a `.sql` file then Python, a `%%sql` cell between
  Python cells, pandas data joined in SQL) gives one answer.
- **TPC-H written as frames:** all 22 queries as `pondra.frame` code and as `pondra.spark` code,
  equal to the SQL answers (`bench/tpch-queries`).
- **Every SQL door stays one engine:** a frame's `sql` runs over Postgres and Flight SQL too, and
  gives the same rows.

## What it costs

- About 1,500 lines of Python in `python/pondra/` (`frame.py`, `spark.py`, and the name lookup,
  `.sql` file runner and notebook magic), and a small server addition: a query's own Arrow rows,
  sent with it, as a temporary table only it sees. `pondra run` in the binary runs `.sql` files
  with parameters.
- No change to the engine. A frame is only as fast as its SQL, so it is exactly as fast.

## Later

- **The same builder in JavaScript**, and in Rust via DataFusion's own `DataFrame` for the
  in-process library (round 23).
- **Spark Connect in the binary** (option C) if Scala or Java Spark jobs need to move.
- **Python UDFs on the nodes** through the Arrow Flight function server (`udf_server.py`), not in
  the node's process. (Built instead in round 24: warm Python workers beside each node, below.)

## As built (round 22)

Everything above is built (`python/pondra/frame.py`, `client.py`, `spark/`, `magic.py`) and
tested (`tools/frames_check.py`, `tools/spark_check.py`, `tools/bench/tpch_frames.py`), except:

- **`con.table(name, at=commit)`** (time travel) and **`@pondra.function`** (a Python function
  callable from SQL) aren't built. Functions of your own are Arrow Flight servers (`POST
  /functions`), as before; a stored Python *procedure* (`@con.procedure`, ADR-023) covers the
  jobs. (`@db.function` came in round 24, below.)
- **Pipelines of `.sql` and `.py` files** (`pondra run models/`) aren't built; `pondra run
  file.sql` and `con.run("file.sql", …)` are.
- **The JavaScript client** has `$name` parameters, `run`, `call` and `view`, not the builder.
- **`sample(n)`** is `ORDER BY random() LIMIT n` (DataFusion ignores `TABLESAMPLE`). With `seed=` it
  orders by a hash of the seed and each row's values, so the same rows come every time.
- **Rows sent with a query** go in the request itself (`application/vnd.pondra.request`), and a
  write that names a frame sends it as a view the node puts in place.
- **Frames learn their columns** (for `with_columns`, `rename`, joins) with one `LIMIT 0` query,
  about a millisecond, remembered per frame.
- **A sort is kept** through the steps after it (DataFusion drops a CTE's `ORDER BY`): a frame
  puts it in each step that keeps order.
- Beyond the design: **macros and procedures** (SQL and Python) kept in the catalog, callable from
  every client and as MCP tools (ADR-023).


## Round 23: files in and out (ADR-026)

Frames read and write what is outside the lake with Polars' names, and `pondra.spark` with
PySpark's; each is the SQL of ADR-026, so every client does the same:

- `db.scan_parquet(url)`, `scan_csv`, `scan_ndjson`, `scan_delta(url, version=…)`,
  `scan_iceberg(url, …)`: a frame over files, a folder, a glob or another engine's table
  (`read_parquet`, `read_csv`, `read_json`, `delta_scan`, `iceberg_scan`). Since round 25,
  `db.read_parquet(url)` and the rest are the names, and these the fallbacks.
- `frame.sink_parquet(url, partition_by=…)`, `sink_csv`, `sink_ndjson`: `COPY (frame) TO url`
  (since round 25: `frame.write_parquet(url)` and the rest).
- `spark.read.option(…).parquet/csv/json(url)`, `spark.read.format("delta" | "iceberg").load(url)`,
  and `df.write.mode("overwrite" | "append" | "error" | "ignore").partitionBy(…).parquet/csv/json(url)`
  (Spark's modes as `COPY … TO`'s `OVERWRITE` and `APPEND`).

`frames_check.py` section 6 and `spark_check.py`'s seven file pipelines compare them with Polars
and PySpark reading and writing the same files.

## Round 25: one vocabulary (ADR-028)

Pondra's own names come first everywhere: reading is `read_<format>`, writing `write_<format>`.
The names Polars, DuckDB and PySpark users know stay as fallbacks, with the same answers. This
table is checked by `harness.py names`: every name in it exists, runs, and each fallback equals its
standard name.

<!-- vocabulary -->
| Operation | SQL | Python (connection, module, frames) | PySpark (`pondra.spark`) | Fallbacks |
|---|---|---|---|---|
| Parquet files | `read_parquet` | `db.read_parquet`, `pondra.read_parquet` | `spark.read.parquet` | `parquet_scan`, `db.scan_parquet`, `pondra.scan_parquet` |
| CSV files | `read_csv` | `db.read_csv`, `pondra.read_csv` | `spark.read.csv` | `read_csv_auto`, `db.scan_csv`, `pondra.scan_csv` |
| JSON lines | `read_json` | `db.read_json`, `pondra.read_json` | `spark.read.json` | `read_json_auto`, `read_ndjson`, `db.scan_ndjson`, `db.read_ndjson`, `pondra.scan_ndjson`, `pondra.read_ndjson` |
| A Delta table | `read_delta` | `db.read_delta`, `pondra.read_delta` | `spark.read.format("delta").load` | `delta_scan`, `db.scan_delta`, `pondra.scan_delta` |
| An Iceberg table | `read_iceberg` | `db.read_iceberg`, `pondra.read_iceberg` | `spark.read.format("iceberg").load` | `iceberg_scan`, `db.scan_iceberg`, `pondra.scan_iceberg` |
| A lake's table | `FROM t` | `db.table`, `pondra.table` | `spark.table`, `spark.read.table` | |
| Parquet files out | `COPY … TO (FORMAT parquet)` | `frame.write_parquet` | `df.write.parquet` | `frame.sink_parquet` |
| CSV files out | `COPY … TO (FORMAT csv)` | `frame.write_csv` | `df.write.csv` | `frame.sink_csv` |
| JSON lines out | `COPY … TO (FORMAT json)` | `frame.write_json` | `df.write.json` | `frame.sink_ndjson`, `frame.write_ndjson` |
| A Delta table out | `COPY … TO (FORMAT delta)` | `frame.write_delta` | `df.write.format("delta").save`, `df.write.delta` | |
| An Iceberg table out | `COPY … TO (FORMAT iceberg)` | `frame.write_iceberg` | `df.write.format("iceberg").save` | |
| Into a lake's table | `INSERT INTO t …`, `CREATE TABLE t AS …` | `frame.write_table`, `db.write_table` | `df.write.saveAsTable`, `df.write.insertInto` | |
<!-- /vocabulary -->

- **Writing Delta and Iceberg tables to a folder:** the folder gets a new table if it holds
  none. One that holds a table takes `mode="append"` or `"overwrite"` (SQL's `APPEND`,
  `OVERWRITE`), as a folder of files does; `"error"` (the default) refuses, `"ignore"` leaves it.
  delta-rs, PyIceberg and DuckDB read what is written.
- **JavaScript** has no frames: SQL is its API (`db.sql("SELECT * FROM read_parquet('…')")`), so
  its names are SQL's.
- **Live answers:** `db.live(sql_or_frame)` yields a query's rows now and each time a commit
  changes them (`for await (const rows of db.live(sql))` in JavaScript).
- **Temporary tables and views** are the connection's own (`CREATE TEMP TABLE`, and a frame's
  `to_view(name, temporary=True)`); `db.close()` ends them.
- **Function answers reused:** `@db.function(cache="10 minutes")`.

## Round 24: functions and procedures from Python (ADR-027)

A notebook's function becomes the lake's, and runs on every node beside the data. The same code
still runs in the notebook:

```python
import re, pondra
from datetime import date
STOP = {"the", "a"}

def words(t):
    return [w for w in re.findall(r"[a-z]+", t.lower()) if w not in STOP]

@db.function                                    # SELECT slug(title) FROM posts, on every node
def slug(title: str) -> str:
    return "-".join(words(title))

@db.procedure                                   # CALL weekly(DATE '2026-09-27'), as its caller
def weekly(day: date, top: int = 10):
    rows = pondra.sql("SELECT user, count(*) AS n FROM orders GROUP BY user ORDER BY n DESC LIMIT $k", k=top).rows()
    print(f"week of {day}: {len(rows)} users")   # a notice to whoever called it
    return rows

posts.with_columns(s=pondra.fn.slug(pondra.col("title")))   # a lake function in a frame
db.call("weekly", date.today())                              # on the node; db.notices has what it printed
db.call("weekly", date.today(), wait=False).wait()           # started, then waited for (pondra.runs)
```

- **What goes along:** the imports the function uses (`re`), the helpers it calls (`words`,
  and theirs), and the constants it reads (`STOP`: numbers, strings, lists, dicts and sets of
  them). The stored body is readable Python (`SHOW USER FUNCTIONS`, `pondra.routines`).
- **What is refused,** when it's made, with the fix: anything else from the notebook (a
  DataFrame, a connection) and a closure's variables.
- **Types come from the annotations:**

  | Python | SQL |
  |---|---|
  | `str` | VARCHAR |
  | `int` | BIGINT |
  | `float` | DOUBLE |
  | `bool` | BOOLEAN |
  | `date` | DATE |
  | `datetime` | TIMESTAMP |
  | `bytes` | BYTEA |
  | `Decimal` | DECIMAL |
  | `list[T]` | `T[]` |
  | `dict` | VARIANT |

  A parameter without one takes any value as it comes (`ANY`). A function needs its result's
  type: an annotation, or `returns=` (a SQL type, or `{"col": type, …}` for a table function).
- **More options:** `@db.function(vectorized=True)` is called once a batch with pyarrow arrays.
  `strict=True` answers NULL for a NULL argument without calling. `volatility="immutable"` lets
  its queries be remembered, `packages="…"` installs packages on each node, and `timeout=` sets
  the seconds a batch may take.
- **`db.create_function(name, body | file=…, params=…, returns=…)`** and
  `db.create_procedure(…)` take code as text.
- **`pondra.sql`, `pondra.table`, `pondra.call`, `pondra.secret`** use the current connection:
  in a procedure, the one lent its caller; in a notebook, the newest one made. So code moves
  between the two unchanged. `con` as a procedure's first parameter still gets the connection
  (0.22's `@con.procedure`).
- **PySpark's names** (`pondra.spark`): `F.udf(f, "int")` / `@F.udf("int")`,
  `@F.pandas_udf("double")` (pandas Series in and out, once a batch), and `spark.udf.register(name,
  f, returnType)` (then `spark.sql("SELECT name(…)")`). Each is a lake function, made when first
  used; `spark_check.py`'s four UDF pipelines compare them with PySpark 4.0.1.
