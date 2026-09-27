#!/usr/bin/env python3
"""A differential test: `pondra.spark` against real PySpark 4.0.1, local mode (round 22).

  /home/claude/venv-spark/bin/python tools/spark_check.py [--only name1,name2] [--port 8781]

Builds ~2,000 orders and ~50 users (seeded, some nulls), loads the same rows into a fresh Pondra
lake wrapped as `pondra.spark.SparkSession` and into real PySpark (local[2]), then runs about 40
pipelines written ONCE as `f(spark, F, Window)` — the same code runs against both, which is the
point: a PySpark job migrates by changing its imports. Prints one JSON line per pipeline
`{"name", "equal", "names_equal", "detail"?}` and a summary line, and cleans up its lake, Spark
session and node.

PySpark lives only in /home/claude/venv-spark; run this with its Python. Java 21 is required.
"""
import argparse
import datetime
import json
import math
import os
import random
import shutil
import sys
import tempfile
import time

# The sandbox's own TZ (Asia/Tashkent, +05) leaks into the JVM's default zone, which some Spark
# functions (date_trunc on a TIMESTAMP result) consult instead of spark.sql.session.timeZone; pin
# it to UTC before the JVM is ever spawned so results depend only on the data, not the host clock.
os.environ["TZ"] = "UTC"
if hasattr(time, "tzset"):
    time.tzset()

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "python"))  # the pondra client
os.environ.setdefault("PONDRA_BIN", os.path.join(HERE, "..", "target", "release", "pondra"))

import pyarrow as pa

import pondra
from pondra.spark import SparkSession as PondraSession
from pondra.spark import functions as PF
from pondra.spark import Window as PWindow

from pyspark.sql import SparkSession as RealSession
from pyspark.sql import functions as RF
from pyspark.sql import Window as RWindow


# ================================================================== data

def build_data(seed=20260927, n_orders=2000, n_users=50):
    """~2,000 orders (some null qty/price/region) and ~50 users, as pyarrow tables."""
    rng = random.Random(seed)
    users = [f"u{idx:03d}" for idx in range(n_users)]
    tiers = ["gold", "silver", "bronze"]
    join_start = datetime.date(2022, 1, 1)
    users_t = pa.table({
        "user": pa.array(users, pa.string()),
        "tier": pa.array([tiers[idx % 3] for idx in range(n_users)], pa.string()),
        "joined": pa.array([join_start + datetime.timedelta(days=rng.randint(0, 900)) for _ in range(n_users)], pa.date32()),
    })

    items = ["tea", "coffee", "widget", "gadget", "bolt", "screw", "cable", "mug"]
    regions = ["US", "EU", "APAC"]
    ts_start = datetime.datetime(2024, 1, 1)
    ids, us, its, qtys, prices, tss, days, regs = [], [], [], [], [], [], [], []
    for i in range(1, n_orders + 1):
        ids.append(i)
        us.append(rng.choice(users))
        its.append(rng.choice(items))
        qtys.append(None if rng.random() < 0.05 else rng.randint(1, 20))
        prices.append(None if rng.random() < 0.05 else round(rng.uniform(1.5, 499.5), 2))
        ts = ts_start + datetime.timedelta(seconds=rng.randint(0, 180 * 86400))
        tss.append(ts)
        days.append(ts.date())
        regs.append(None if rng.random() < 0.1 else rng.choice(regions))
    orders_t = pa.table({
        "id": pa.array(ids, pa.int64()),
        "user": pa.array(us, pa.string()),
        "item": pa.array(its, pa.string()),
        "qty": pa.array(qtys, pa.int64()),
        "price": pa.array(prices, pa.float64()),
        "ts": pa.array(tss, pa.timestamp("us")),
        "day": pa.array(days, pa.date32()),
        "region": pa.array(regs, pa.string()),
    })
    return orders_t, users_t


def load_pondra(con, orders_t, users_t):
    con.sql("CREATE TABLE orders (id BIGINT, \"user\" VARCHAR, item VARCHAR, qty BIGINT, price DOUBLE, ts TIMESTAMP, day DATE, region VARCHAR)")
    con.append("orders", orders_t)
    con.sql("CREATE TABLE users (\"user\" VARCHAR, tier VARCHAR, joined DATE)")
    con.append("users", users_t)


def load_spark(spark, orders_t, users_t):
    # An explicit schema over a list of dicts (not to_pandas()): pandas promotes a nullable int
    # column to float64 and turns its nulls into NaN, which is NOT SQL NULL (it poisons sums and
    # sorts as a value), so it would corrupt qty's nulls before Spark ever sees them.
    # ts is TIMESTAMP_NTZ, not TIMESTAMP: the sandbox's TZ is Asia/Tashkent (+05), and plain
    # TIMESTAMP would have Spark reinterpret each naive Python datetime as wall-clock time in the
    # JVM's default zone and convert it to a UTC instant, shifting every hour/day-boundary value
    # by 5h relative to Pondra's (which reads the naive timestamp literally, no zone involved).
    orders_schema = "id long, user string, item string, qty long, price double, ts timestamp_ntz, day date, region string"
    users_schema = "user string, tier string, joined date"
    spark.createDataFrame(orders_t.to_pylist(), schema=orders_schema).createOrReplaceTempView("orders")
    spark.createDataFrame(users_t.to_pylist(), schema=users_schema).createOrReplaceTempView("users")


# ================================================================== comparing answers

def _iso(v):
    return v.isoformat() if isinstance(v, (datetime.datetime, datetime.date)) else v


def _cell_equal(a, b, rel=1e-9):
    """None == None, NaN == NaN, floats within a relative tolerance, dates/timestamps as ISO text."""
    if a is None or b is None:
        return a is None and b is None
    if isinstance(a, float) or isinstance(b, float):
        try:
            fa, fb = float(a), float(b)
        except (TypeError, ValueError):
            return a == b
        if math.isnan(fa) and math.isnan(fb):
            return True
        return fa == fb or abs(fa - fb) <= rel * max(abs(fa), abs(fb), 1e-12)
    if isinstance(a, (datetime.datetime, datetime.date)) or isinstance(b, (datetime.datetime, datetime.date)):
        return _iso(a) == _iso(b)
    return a == b


def _cell_key(v):
    """A sort key for one cell: None first, then (kind, value) so mixed-type columns never compare
    across rows (only same-typed values are ever compared past the first element)."""
    if v is None:
        return (0,)
    if isinstance(v, (datetime.datetime, datetime.date)):
        return (1, v.isoformat())
    if isinstance(v, bool):
        return (1, int(v))
    if isinstance(v, (int, float)):
        return (1, v)
    return (1, str(v))


def _row_key(row):
    return tuple(_cell_key(v) for v in row)


def _deep_equal(a, b):
    """A structural equality for plain Python values (counts, Rows, lists of them)."""
    if isinstance(a, dict) and isinstance(b, dict):
        return set(a) == set(b) and all(_deep_equal(a[k], b[k]) for k in a)
    if isinstance(a, (list, tuple)) and isinstance(b, (list, tuple)):
        return len(a) == len(b) and all(_deep_equal(x, y) for x, y in zip(a, b))
    return _cell_equal(a, b)


def compare_frames(ordered, real_names, real_rows, pon_names, pon_rows):
    names_equal = real_names == pon_names
    if not ordered:
        real_rows = sorted(real_rows, key=_row_key)
        pon_rows = sorted(pon_rows, key=_row_key)
    if len(real_rows) != len(pon_rows):
        return False, names_equal, f"{len(real_rows)} rows vs {len(pon_rows)}"
    for i, (a, b) in enumerate(zip(real_rows, pon_rows)):
        if len(a) != len(b) or not all(_cell_equal(x, y) for x, y in zip(a, b)):
            return False, names_equal, f"row {i}: {a} vs {b}"
    return True, names_equal, None


# ================================================================== pipelines
#
# Each pipeline is (name, ordered, kind, fn). `fn(spark, F, Window)` is the SAME code run against
# real PySpark's (spark, functions, Window) and pondra.spark's — that identity is the whole point.
# `ordered`: the pipeline has its own ORDER BY, so rows are compared position by position (else
# both sides are sorted first, since row order is otherwise not guaranteed to match).
# `kind`: "frame" (fn returns a DataFrame) or "value" (fn already collects to a plain Python value).

def p_select(spark, F, Window):
    return spark.table("orders").select("id", "user", "item")


def p_select_expr(spark, F, Window):
    return spark.table("orders").selectExpr("id", "qty * price AS total", "upper(item) AS item_upper")


def p_with_column(spark, F, Window):
    return spark.table("orders").withColumn("total", F.col("qty") * F.col("price")).select("id", "total")


def p_with_columns(spark, F, Window):
    return (spark.table("orders")
            .withColumns({"total": F.col("qty") * F.col("price"), "is_big": F.when(F.col("qty") > 10, True).otherwise(False)})
            .select("id", "total", "is_big"))


def p_with_column_renamed(spark, F, Window):
    return spark.table("orders").withColumnRenamed("qty", "quantity").select("id", "quantity")


def p_drop_unknown(spark, F, Window):
    return spark.table("orders").drop("region", "does_not_exist")


def p_filter_column(spark, F, Window):
    return spark.table("orders").filter(F.col("price") > 100).select("id", "price")


def p_where_sql_string(spark, F, Window):
    return spark.table("orders").where("price > 100 AND item = 'tea'").select("id", "item", "price")


def p_group_agg(spark, F, Window):
    return (spark.table("orders").groupBy("item")
            .agg(F.sum("price"), F.count("*"), F.avg("qty").alias("avg_qty"),
                 F.countDistinct("user").alias("distinct_users"),
                 F.min("price").alias("min_price"), F.max("price").alias("max_price")))


def p_group_count(spark, F, Window):
    return spark.table("orders").groupBy("region").count()


def p_order_nulls_limit(spark, F, Window):
    return (spark.table("orders")
            .orderBy(F.asc("region"), F.desc("price"), F.asc("id"))
            .limit(30).select("id", "region", "price"))


def p_distinct(spark, F, Window):
    return spark.table("orders").select("item", "region").distinct()


def p_drop_duplicates(spark, F, Window):
    # only the subset column is safe to compare: which OTHER columns survive per group is
    # unspecified (each engine may keep a different row of the group).
    return spark.table("orders").dropDuplicates(["region"]).select("region")


def p_union(spark, F, Window):
    orders = spark.table("orders")
    a = orders.filter(F.col("region") == "US").select("id", "region")
    b = orders.filter(F.col("region") == "EU").select("id", "region")
    return a.union(b)


def p_union_by_name(spark, F, Window):
    orders = spark.table("orders")
    a = orders.filter(F.col("region") == "US").select("id", "region")
    b = orders.filter(F.col("region") == "EU").select("region", "id")  # reversed order
    return a.unionByName(b)


def p_join_on_name(spark, F, Window):
    return spark.table("orders").join(spark.table("users"), "user").select("id", "user", "tier")


def p_join_on_names_list(spark, F, Window):
    orders = spark.table("orders")
    stats = orders.groupBy("user", "item").agg(F.sum("qty").alias("item_qty"))
    return orders.join(stats, ["user", "item"]).select("id", "user", "item", "item_qty")


def p_join_left(spark, F, Window):
    return spark.table("orders").join(spark.table("users"), "user", "left").select("id", "user", "tier")


def p_join_right(spark, F, Window):
    return spark.table("orders").join(spark.table("users"), "user", "right").select("user", "tier", "id")


def p_join_full(spark, F, Window):
    return spark.table("orders").join(spark.table("users"), "user", "full").select("user", "tier", "id")


def p_join_condition_alias(spark, F, Window):
    o, u = spark.table("orders").alias("o"), spark.table("users").alias("u")
    return o.join(u, o["user"] == u["user"], "inner").select(F.col("o.id"), F.col("u.tier"))


def p_join_semi(spark, F, Window):
    gold = spark.table("users").filter(F.col("tier") == "gold")
    return spark.table("orders").join(gold, "user", "left_semi").select("id", "user")


def p_join_anti(spark, F, Window):
    gold = spark.table("users").filter(F.col("tier") == "gold")
    return spark.table("orders").join(gold, "user", "left_anti").select("id", "user")


def p_cross_join(spark, F, Window):
    tiers = spark.createDataFrame([("gold",), ("silver",), ("bronze",)], "tier string")
    regions = spark.createDataFrame([("US",), ("EU",), ("APAC",)], "region string")
    return tiers.crossJoin(regions)


def p_na_fill(spark, F, Window):
    return spark.table("orders").na.fill({"qty": 0, "price": 0.0, "region": "UNKNOWN"}).select("id", "qty", "price", "region")


def p_na_drop(spark, F, Window):
    return spark.table("orders").na.drop(subset=["qty", "price"]).select("id", "qty", "price")


def p_when_otherwise(spark, F, Window):
    size = F.when(F.col("qty") > 10, "big").when(F.col("qty") > 0, "small").otherwise("none").alias("size")
    return spark.table("orders").select("id", size)


def p_coalesce(spark, F, Window):
    return spark.table("orders").select("id", F.coalesce(F.col("region"), F.lit("NONE")).alias("region2"))


def p_predicates(spark, F, Window):
    orders = spark.table("orders")
    return orders.select("id",
                          F.col("region").isNull().alias("region_null"),
                          F.col("item").isin("tea", "coffee").alias("is_hot_drink"),
                          F.col("qty").between(5, 10).alias("mid_qty"),
                          F.col("item").like("t%").alias("like_t"),
                          F.col("item").rlike("^c.*").alias("rlike_c"))


def p_concat(spark, F, Window):
    # null if any part is null (PySpark's concat, unlike SQL's plain concat())
    return spark.table("orders").select("id", F.concat(F.col("item"), F.lit("-"), F.col("region")).alias("tag"))


def p_concat_ws(spark, F, Window):
    return spark.table("orders").select("id", F.concat_ws("-", F.col("item"), F.col("region")).alias("tag"))


def p_string_funcs(spark, F, Window):
    return spark.table("orders").select("id",
                                         F.upper(F.col("item")).alias("u"),
                                         F.lower(F.col("item")).alias("l"),
                                         F.length(F.col("item")).alias("len"),
                                         F.trim(F.col("item")).alias("t"))


def p_regexp_replace(spark, F, Window):
    return spark.table("orders").select("id", F.regexp_replace(F.col("item"), "e", "3").alias("r"))


def p_substring(spark, F, Window):
    return spark.table("orders").select("id", F.substring(F.col("item"), 1, 3).alias("sub"))


def p_date_parts(spark, F, Window):
    return spark.table("orders").select("id",
                                         F.year("ts").alias("y"), F.month("ts").alias("m"),
                                         F.dayofmonth("ts").alias("d"), F.dayofweek("ts").alias("dow"),
                                         F.hour("ts").alias("h"))


def p_date_trunc(spark, F, Window):
    return spark.table("orders").select("id", F.date_trunc("month", F.col("ts")).alias("m0"))


def p_date_math(spark, F, Window):
    orders = spark.table("orders")
    return orders.select("id",
                          F.datediff(F.col("day"), F.to_date(F.lit("2024-01-01"))).alias("dd"),
                          F.date_add(F.col("day"), 7).alias("plus7"),
                          F.to_date(F.col("ts")).alias("d2"))


def p_window_ranks(spark, F, Window):
    w = Window.partitionBy("user").orderBy(F.desc("price"), F.asc("id"))
    lag_w = Window.partitionBy("user").orderBy(F.asc("id"))
    return (spark.table("orders").select("id", "user", "price",
                                          F.row_number().over(w).alias("rn"),
                                          F.rank().over(w).alias("rk"),
                                          F.dense_rank().over(w).alias("drk"),
                                          F.lag("price", 1).over(lag_w).alias("lag1"),
                                          F.lead("price", 1).over(lag_w).alias("lead1")))


def p_window_rows_between(spark, F, Window):
    w = Window.partitionBy("user").orderBy("id").rowsBetween(-1, 1)
    return spark.table("orders").select("id", "user", "price", F.sum("price").over(w).alias("roll"))


def p_int_division_round(spark, F, Window):
    return spark.table("orders").select("id", (F.col("qty") / F.lit(3)).alias("div"), F.round(F.col("price"), 1).alias("rp"))


def p_spark_sql(spark, F, Window):
    spark.table("orders").createOrReplaceTempView("orders_tv")
    return spark.sql("SELECT region, count(*) AS n FROM orders_tv GROUP BY region")


def p_create_dataframe_tuples(spark, F, Window):
    return spark.createDataFrame([(1, "a"), (2, "b"), (3, None)], "num int, tag string")


def p_write_then_read(spark, F, Window):
    sub = spark.table("orders").filter(F.col("region") == "US").select("id", "user")
    sub.write.mode("overwrite").saveAsTable("us_orders_check")
    return spark.table("us_orders_check")


def p_count_first_collect(spark, F, Window):
    orders = spark.table("orders").orderBy("id")
    n = orders.count()
    first_row = orders.first()
    top3 = [(r["id"], r.item) for r in orders.limit(3).collect()]  # dict- and attribute-style access
    return {"count": n, "first_id": first_row["id"], "top3": top3}


PIPELINES = [
    ("select", False, "frame", p_select),
    ("selectExpr", False, "frame", p_select_expr),
    ("withColumn", False, "frame", p_with_column),
    ("withColumns", False, "frame", p_with_columns),
    ("withColumnRenamed", False, "frame", p_with_column_renamed),
    ("drop_unknown", False, "frame", p_drop_unknown),
    ("filter_column", False, "frame", p_filter_column),
    ("where_sql_string", False, "frame", p_where_sql_string),
    ("groupBy_agg", False, "frame", p_group_agg),
    ("groupBy_count", False, "frame", p_group_count),
    ("orderBy_nulls_limit", True, "frame", p_order_nulls_limit),
    ("distinct", False, "frame", p_distinct),
    ("dropDuplicates_subset", False, "frame", p_drop_duplicates),
    ("union", False, "frame", p_union),
    ("unionByName", False, "frame", p_union_by_name),
    ("join_on_name", False, "frame", p_join_on_name),
    ("join_on_names_list", False, "frame", p_join_on_names_list),
    ("join_left", False, "frame", p_join_left),
    ("join_right", False, "frame", p_join_right),
    ("join_full_outer", False, "frame", p_join_full),
    ("join_condition_alias", False, "frame", p_join_condition_alias),
    ("join_left_semi", False, "frame", p_join_semi),
    ("join_left_anti", False, "frame", p_join_anti),
    ("cross_join", False, "frame", p_cross_join),
    ("na_fill", False, "frame", p_na_fill),
    ("na_drop", False, "frame", p_na_drop),
    ("when_otherwise", False, "frame", p_when_otherwise),
    ("coalesce", False, "frame", p_coalesce),
    ("predicates_isnull_isin_between_like_rlike", False, "frame", p_predicates),
    ("concat_null_propagates", False, "frame", p_concat),
    ("concat_ws", False, "frame", p_concat_ws),
    ("string_funcs", False, "frame", p_string_funcs),
    ("regexp_replace", False, "frame", p_regexp_replace),
    ("substring", False, "frame", p_substring),
    ("date_parts", False, "frame", p_date_parts),
    ("date_trunc", False, "frame", p_date_trunc),
    ("date_math", False, "frame", p_date_math),
    ("window_ranks_lag_lead", False, "frame", p_window_ranks),
    ("window_rows_between", False, "frame", p_window_rows_between),
    ("int_division_round", False, "frame", p_int_division_round),
    ("spark_sql_tempview", False, "frame", p_spark_sql),
    ("createDataFrame_tuples_schema", False, "frame", p_create_dataframe_tuples),
    ("write_saveAsTable_then_table", False, "frame", p_write_then_read),
    ("count_first_collect_row_access", None, "value", p_count_first_collect),
]


# ================================================================== driver

def _run_side(fn, spark, F, Window, kind):
    val = fn(spark, F, Window)
    if kind == "frame":
        return list(val.columns), [list(r) for r in val.collect()]
    return None, val


def run_pipeline(name, ordered, kind, fn, real_spark, pon_spark):
    out = {"name": name}
    try:
        real_names, real_data = _run_side(fn, real_spark, RF, RWindow, kind)
    except Exception as e:  # noqa: BLE001 (the failure itself is the report)
        out["equal"], out["names_equal"], out["detail"] = False, False, f"pyspark error: {type(e).__name__}: {e}"
        return out
    try:
        pon_names, pon_data = _run_side(fn, pon_spark, PF, PWindow, kind)
    except Exception as e:  # noqa: BLE001
        out["equal"], out["names_equal"], out["detail"] = False, False, f"pondra error: {type(e).__name__}: {e}"
        return out
    if kind == "frame":
        ok, names_equal, detail = compare_frames(ordered, real_names, real_data, pon_names, pon_data)
    else:
        ok, names_equal, detail = _deep_equal(real_data, pon_data), True, (None if _deep_equal(real_data, pon_data) else f"{real_data!r} vs {pon_data!r}")
    out["equal"], out["names_equal"] = ok, names_equal
    if detail:
        out["detail"] = detail
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", help="comma-separated pipeline names to run (default: all)")
    ap.add_argument("--port", type=int, default=8781)
    args = ap.parse_args()
    wanted = set(args.only.split(",")) if args.only else None
    pipelines = [p for p in PIPELINES if wanted is None or p[0] in wanted]
    if wanted:
        missing = wanted - {p[0] for p in pipelines}
        if missing:
            print(f"unknown pipeline names: {sorted(missing)}", file=sys.stderr)

    lake = tempfile.mkdtemp(prefix="pondra-sparkcheck-")
    warehouse = tempfile.mkdtemp(prefix="spark-warehouse-")
    con = real_spark = None
    try:
        orders_t, users_t = build_data()

        con = pondra.local(lake, port=args.port)
        load_pondra(con, orders_t, users_t)
        pon_spark = PondraSession(con)

        real_spark = (RealSession.builder.master("local[2]").appName("spark_check")
                      .config("spark.sql.shuffle.partitions", "4")
                      .config("spark.sql.session.timeZone", "UTC")
                      .config("spark.sql.warehouse.dir", warehouse)
                      .config("spark.ui.enabled", "false")
                      .config("spark.ui.showConsoleProgress", "false")
                      .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
                      .getOrCreate())
        real_spark.sparkContext.setLogLevel("WARN")
        load_spark(real_spark, orders_t, users_t)

        results = [run_pipeline(name, ordered, kind, fn, real_spark, pon_spark) for name, ordered, kind, fn in pipelines]
        for r in results:
            print(json.dumps(r), flush=True)
        summary = {"equal": sum(r["equal"] for r in results), "names_equal": sum(r["names_equal"] for r in results), "total": len(results)}
        print(json.dumps(summary), flush=True)
    finally:
        if real_spark is not None:
            real_spark.stop()
        if con is not None:
            con.close()
        shutil.rmtree(lake, ignore_errors=True)
        shutil.rmtree(warehouse, ignore_errors=True)
        shutil.rmtree("spark-warehouse", ignore_errors=True)  # (created in cwd if the config were ever ignored)


if __name__ == "__main__":
    main()
