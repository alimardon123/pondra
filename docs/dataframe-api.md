# Pondra's DataFrame API: a design (round 21, built in round 22)

**Status:** proposed · **Date:** 2026-09-27 · **Part of:** ADR-022 · **Asked for by the owner:**
"a Python DataFrame API, as easy as Polars or directly like PySpark for easy migration, with the
SQL side converged in it and in any other API."

## What it has to be

- **One engine, one meaning.** A DataFrame is another way to write a query, not another engine.
  Whatever SQL can do on a lake — spread across nodes, the guard, remembered answers, system
  columns, changed rows, attached lakes, time travel with `upto` — a DataFrame does the same way,
  because it becomes the same query.
- **Two dialects, one tree.** A Polars-style lazy API (`pondra.frame`) for new code, and a
  PySpark-shaped one (`pondra.spark`) so a PySpark job moves by changing its imports. Both build
  the same expression tree.
- **SQL and frames mix freely.** A frame can come from SQL (`con.sql("SELECT …")`), SQL can read
  a frame (`frame.to_view("v")`, then `SELECT … FROM v`), and every frame shows the SQL it runs
  (`frame.sql`).
- **Small and dependency-free.** Pure Python over the existing client: `pyarrow` for results,
  nothing else. The JavaScript client gets the same builder later, from the same design.
- **Streaming is the same API.** A frame that is written as a materialized view keeps up with its
  sources; `watch()` streams its rows.

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
| Sources | `con.table(name)`, `con.sql(query)`, `con.from_arrow(t)` / `from_pandas` / `from_polars` | the table; `(query)`; the rows sent with the query (Arrow, one query's temporary table) |
| Rows | `filter`, `head`/`limit`, `sort(…, descending=, nulls_last=)`, `unique(subset=)`, `sample` | `WHERE`, `LIMIT`, `ORDER BY`, `DISTINCT ON`-style `ROW_NUMBER() = 1`, `TABLESAMPLE` |
| Columns | `select`, `with_columns`, `drop`, `rename`, `cast` | the projection |
| Expressions | `col`, `lit`, `+ - * / // %`, comparisons, `& \| ~`, `is_null`, `is_in`, `between`, `when().then().otherwise()`, `.str.*`, `.dt.*`, `.alias`, `.over(…)` | SQL expressions; `CASE`; window functions |
| Aggregation | `group_by(…).agg(…)`, `sum/mean/min/max/count/n_unique/first/last/quantile`, `pondra.len()` | `GROUP BY` (`sum` over DOUBLE stays order-independent: `fsum.rs`) |
| Joins | `join(other, on=, left_on=, right_on=, how="inner/left/right/full/semi/anti/cross")`, `join_asof(other, on=, by=, strategy="backward/forward")` | `JOIN`; `ASOF JOIN` (`asof.rs`) |
| Sets | `pondra.concat([a, b])`, `union`, `intersect`, `except_` | `UNION ALL` / `UNION` / `INTERSECT` / `EXCEPT` |
| Time | `con.table(name, at=commit)` | the table as of that commit (`upto`) |

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
- **TPC-H written as frames:** all 22 queries as `pondra.frame` code and as `pondra.spark` code,
  equal to the SQL answers (`bench/tpch-queries`).
- **Every SQL door stays one engine:** a frame's `sql` runs over Postgres and Flight SQL too, and
  gives the same rows.

## What it costs

- About 1,200 lines of Python in `python/pondra/` (`frame.py`, `spark.py`), and a small server
  addition: a query's own Arrow rows, sent with it, as a temporary table only it sees.
- No change to the engine. A frame is only as fast as its SQL, so it is exactly as fast.

## Later

- **The same builder in JavaScript**, and in Rust via DataFusion's own `DataFrame` for the
  in-process library (round 23).
- **Spark Connect in the binary** (option C) if Scala or Java Spark jobs need to move.
- **Python UDFs on the nodes** through the Arrow Flight function server (`udf_server.py`), not in
  the node's process.
