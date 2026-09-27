#!/usr/bin/env python3
"""pondra.frame == Polars, and SQL and Python used interchangeably (ADR-023).

1. The same pipelines in pondra.frame and in Polars (lazy), over the same rows: equal values and
   column names (rows sorted unless the pipeline sorts; floats to a relative 1e-9).
2. One question asked every way — SQL alone, frames alone, SQL then frames, frames named in SQL,
   pandas data named in SQL, a `.sql` file then Python, a view made from a frame read by SQL, a
   `%%sql` cell, a Python procedure CALLed from SQL — gives one answer.
3. A frame's sort survives the steps after it (SQL drops a CTE's ORDER BY: a sort then a limit
   gave the wrong rows before the frame put it in each step again).
4. Frames write: CREATE TABLE … AS, INSERT, UPDATE, DELETE and MERGE with Delta's builder names.
5. One name, one meaning: `db.view` and a frame's `to_view` make a stored query unless
   `materialized=True`; `db.write_table` is a frame's `write_table` (ADR-025).

  frames_check.py [--only name,…] [--port 8830]      (pondra.spark vs PySpark: spark_check.py)
"""
import argparse, datetime as dt, json, math, os, random, shutil, sys, tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "python"))
os.environ.setdefault("PONDRA_BIN", os.path.join(HERE, "..", "target", "release", "pondra"))

import pandas as pd
import polars as pl
import pyarrow as pa

import pondra
from pondra import col, lit, when


def data(n=3000, seed=11):
    """Orders and users, with nulls in places, as Arrow."""
    r = random.Random(seed)
    t0 = dt.datetime(2026, 9, 1)
    orders = pa.table({
        "id": list(range(n)),
        "user": [f"u{r.randrange(40)}" for _ in range(n)],
        "item": [r.choice(["tea", "cake", "coffee", "bun", "Tea Pot", "juice"]) for _ in range(n)],
        "qty": [None if r.random() < 0.05 else r.randrange(10) for _ in range(n)],
        "price": [None if r.random() < 0.05 else round(r.uniform(0.5, 40), 2) for _ in range(n)],
        "ts": [t0 + dt.timedelta(seconds=r.randrange(30 * 86400)) for _ in range(n)],
        "region": [r.choice(["north", "south", None]) for _ in range(n)],
    })
    users = pa.table({"user": [f"u{i}" for i in range(45)], "tier": [["gold", "plain", "new"][i % 3] for i in range(45)],
                      "joined": [dt.date(2026, 1, 1) + dt.timedelta(days=5 * i) for i in range(45)]})
    quotes = pa.table({"item": [r.choice(["tea", "cake", "coffee"]) for _ in range(400)],
                       "at": sorted(t0 + dt.timedelta(seconds=r.randrange(30 * 86400)) for _ in range(400)),
                       "px": [round(r.uniform(1, 9), 2) for _ in range(400)]})
    return {"orders": orders, "users": users, "quotes": quotes}


# ---------------------------------------------------------------- 1. frames == Polars

def pipelines():
    """name -> (pondra (con, o, u, q) -> frame, polars (o, u, q) -> LazyFrame, ordered)."""
    P = {}

    def add(name, ours, theirs, ordered=False):
        P[name] = (ours, theirs, ordered)

    add("filter + select", lambda c, o, u, q: o.filter(col("qty") > 3).select("id", "user", "qty"),
        lambda o, u, q: o.filter(pl.col("qty") > 3).select("id", "user", "qty"))
    add("with_columns: new and replaced", lambda c, o, u, q: o.with_columns((col("qty") * col("price")).alias("total"), (col("price") * 2).alias("price")),
        lambda o, u, q: o.with_columns((pl.col("qty") * pl.col("price")).alias("total"), (pl.col("price") * 2).alias("price")))
    add("group_by: sum mean min max count n_unique len",
        lambda c, o, u, q: o.group_by("user").agg(col("qty").sum().alias("s"), col("price").mean().alias("m"), col("price").min().alias("lo"), col("price").max().alias("hi"),
                                                   col("qty").count().alias("c"), col("item").n_unique().alias("items"), pondra.len()),
        lambda o, u, q: o.group_by("user").agg(pl.col("qty").sum().alias("s"), pl.col("price").mean().alias("m"), pl.col("price").min().alias("lo"), pl.col("price").max().alias("hi"),
                                                pl.col("qty").count().alias("c"), pl.col("item").n_unique().alias("items"), pl.len()))
    add("group_by two keys, std and var", lambda c, o, u, q: o.group_by("user", "item").agg(col("price").std().alias("sd"), col("price").var().alias("v")),
        lambda o, u, q: o.group_by("user", "item").agg(pl.col("price").std().alias("sd"), pl.col("price").var().alias("v")))
    add("sort desc (nulls first) + limit", lambda c, o, u, q: o.sort("price", "id", descending=[True, False]).limit(15),
        lambda o, u, q: o.sort("price", "id", descending=[True, False]).limit(15), True)
    add("sort nulls_last, then filter and select (the sort kept)", lambda c, o, u, q: o.sort("qty", "id", nulls_last=True).filter(col("price") > 20).select("id", "qty").head(20),
        lambda o, u, q: o.sort("qty", "id", nulls_last=True).filter(pl.col("price") > 20).select("id", "qty").head(20), True)
    add("join inner", lambda c, o, u, q: o.join(u, on="user"), lambda o, u, q: o.join(u, on="user"))
    add("join left", lambda c, o, u, q: o.join(u, on="user", how="left"), lambda o, u, q: o.join(u, on="user", how="left"))
    add("join full", lambda c, o, u, q: u.join(o.select("user", "id"), on="user", how="full"), lambda o, u, q: u.join(o.select("user", "id"), on="user", how="full"))
    add("join semi / anti", lambda c, o, u, q: pondra.concat([u.join(o.filter(col("qty") == 9), on="user", how="semi"), u.join(o, on="user", how="anti")]),
        lambda o, u, q: pl.concat([u.join(o.filter(pl.col("qty") == 9), on="user", how="semi"), u.join(o, on="user", how="anti")]))
    add("join left_on/right_on, a clashing name", lambda c, o, u, q: o.select("id", "user", "item").join(u.with_columns(col("tier").alias("item")), left_on="user", right_on="user"),
        lambda o, u, q: o.select("id", "user", "item").join(u.with_columns(pl.col("tier").alias("item")), left_on="user", right_on="user"))
    add("join cross (small)", lambda c, o, u, q: u.filter(col("tier") == "gold").select("user").join(u.filter(col("tier") == "new").select("user"), how="cross"),
        lambda o, u, q: u.filter(pl.col("tier") == "gold").select("user").join(u.filter(pl.col("tier") == "new").select("user"), how="cross"))
    add("join_asof backward by", lambda c, o, u, q: o.filter(col("item").is_in(["tea", "cake"])).sort("ts").join_asof(q.sort("at"), left_on="ts", right_on="at", by="item").select("id", "px"),
        lambda o, u, q: o.filter(pl.col("item").is_in(["tea", "cake"])).sort("ts").join_asof(q.sort("at"), left_on="ts", right_on="at", by="item").select("id", "px"))
    add("unique", lambda c, o, u, q: o.select("user", "item").unique(), lambda o, u, q: o.select("user", "item").unique())
    add("drop, rename, cast", lambda c, o, u, q: o.drop("ts", "region").rename({"qty": "n"}).cast({"n": pl.Float64}),
        lambda o, u, q: o.drop("ts", "region").rename({"qty": "n"}).cast({"n": pl.Float64}))
    add("when / then / otherwise", lambda c, o, u, q: o.select("id", when(col("qty") > 6).then(lit("big")).when(col("qty") > 2).then(lit("mid")).otherwise(lit("small")).alias("size")),
        lambda o, u, q: o.select("id", pl.when(pl.col("qty") > 6).then(pl.lit("big")).when(pl.col("qty") > 2).then(pl.lit("mid")).otherwise(pl.lit("small")).alias("size")))
    add("is_in, is_between, is_null, fill_null", lambda c, o, u, q: o.select("id", col("item").is_in(["tea", "bun"]).alias("a"), col("price").is_between(5, 10).alias("b"), col("qty").is_null().alias("c"), col("qty").fill_null(-1).alias("d")),
        lambda o, u, q: o.select("id", pl.col("item").is_in(["tea", "bun"]).alias("a"), pl.col("price").is_between(5, 10).alias("b"), pl.col("qty").is_null().alias("c"), pl.col("qty").fill_null(-1).alias("d")))
    add("fill_null over the frame, drop_nulls", lambda c, o, u, q: o.fill_null(0).drop_nulls("region"),
        lambda o, u, q: o.fill_null(0).drop_nulls("region"))
    add("str: case, contains, starts_with, len, slice, replace_all", lambda c, o, u, q: o.select("id", col("item").str.to_uppercase().alias("up"), col("item").str.contains("ea").alias("ea"),
        col("item").str.starts_with("co").alias("co"), col("item").str.len_chars().alias("n"), col("item").str.slice(1, 2).alias("s"), col("item").str.replace_all("e", "E").alias("r")),
        lambda o, u, q: o.select("id", pl.col("item").str.to_uppercase().alias("up"), pl.col("item").str.contains("ea").alias("ea"),
        pl.col("item").str.starts_with("co").alias("co"), pl.col("item").str.len_chars().cast(pl.Int64).alias("n"), pl.col("item").str.slice(1, 2).alias("s"), pl.col("item").str.replace_all("e", "E").alias("r")))
    add("dt: year month day weekday hour, truncate, date", lambda c, o, u, q: o.select("id", col("ts").dt.year().alias("y"), col("ts").dt.month().alias("m"), col("ts").dt.day().alias("d"), col("ts").dt.weekday().alias("wd"),
        col("ts").dt.hour().alias("h"), col("ts").dt.truncate("1d").alias("day"), col("ts").dt.truncate("6h").alias("q"), col("ts").dt.date().alias("date")),
        lambda o, u, q: o.select("id", pl.col("ts").dt.year().cast(pl.Int64).alias("y"), pl.col("ts").dt.month().cast(pl.Int64).alias("m"), pl.col("ts").dt.day().cast(pl.Int64).alias("d"),
        pl.col("ts").dt.weekday().cast(pl.Int64).alias("wd"), pl.col("ts").dt.hour().cast(pl.Int64).alias("h"), pl.col("ts").dt.truncate("1d").alias("day"), pl.col("ts").dt.truncate("6h").alias("q"), pl.col("ts").dt.date().alias("date")))
    add("over: sum, rank dense, shift, cum_sum", lambda c, o, u, q: o.filter(col("qty").is_not_null()).select("id", col("qty").sum().over("user").alias("s"), col("qty").rank("dense").over("user").alias("r"),
        col("qty").shift(1).over("user", order_by="id").alias("prev"), col("qty").cum_sum().over("user", order_by="id").alias("run")),
        lambda o, u, q: o.filter(pl.col("qty").is_not_null()).select("id", pl.col("qty").sum().over("user").alias("s"), pl.col("qty").rank("dense").over("user").cast(pl.Int64).alias("r"),
        pl.col("qty").shift(1).over("user", order_by="id").alias("prev"), pl.col("qty").cum_sum().over("user", order_by="id").alias("run")))
    add("rank average", lambda c, o, u, q: o.filter(col("user") == "u3").select("id", col("qty").rank().alias("r")),
        lambda o, u, q: o.filter(pl.col("user") == "u3").select("id", pl.col("qty").rank().alias("r")))
    add("concat vertical and diagonal", lambda c, o, u, q: pondra.concat([o.select("id", "user").limit(0), pondra.concat([u.select("user"), o.select("user", "id").filter(col("id") < 5)], how="diagonal")], how="diagonal"),
        lambda o, u, q: pl.concat([o.select("id", "user").limit(0), pl.concat([u.select("user"), o.select("user", "id").filter(pl.col("id") < 5)], how="diagonal")], how="diagonal"))
    add("division, floor division, modulo", lambda c, o, u, q: o.filter(col("qty") > 0).select("id", (col("id") / col("qty")).alias("d"), (col("id") % col("qty")).alias("m"), (col("price") // 3).alias("f")),
        lambda o, u, q: o.filter(pl.col("qty") > 0).select("id", (pl.col("id") / pl.col("qty")).alias("d"), (pl.col("id") % pl.col("qty")).alias("m"), (pl.col("price") // 3).alias("f")))
    add("round, abs, sqrt, clip", lambda c, o, u, q: o.select("id", col("price").round(1).alias("r"), (-col("price")).abs().alias("a"), col("price").sqrt().alias("s"), col("price").clip(5, 30).alias("c")),
        lambda o, u, q: o.select("id", pl.col("price").round(1).alias("r"), (-pl.col("price")).abs().alias("a"), pl.col("price").sqrt().alias("s"), pl.col("price").clip(5, 30).alias("c")))
    add("SQL snippets inside frame methods", lambda c, o, u, q: o.filter("qty > 2 AND item LIKE 't%'").with_columns("qty * price AS total").group_by("item").agg("sum(total) AS revenue", "count(*) AS n"),
        lambda o, u, q: o.filter((pl.col("qty") > 2) & pl.col("item").str.starts_with("t")).with_columns((pl.col("qty") * pl.col("price")).alias("total")).group_by("item").agg(pl.col("total").sum().alias("revenue"), pl.len().cast(pl.Int64).alias("n")))
    return P


def compare(ours, theirs, ordered):
    """Column names, then values: rows in order if `ordered`, else sorted."""
    a, b = ours.to_pylist(), theirs.to_pylist()
    if ours.column_names != theirs.column_names:
        return False, f"columns {ours.column_names} vs {theirs.column_names}"
    key = lambda r: tuple((v is None, str(v)) for v in r.values())
    if not ordered:
        a, b = sorted(a, key=key), sorted(b, key=key)
    if len(a) != len(b):
        return False, f"{len(a)} rows vs {len(b)}"
    for i, (x, y) in enumerate(zip(a, b)):
        if not all(same(x[k], y[k]) for k in x):
            return False, f"row {i}: {x} vs {y}"
    return True, None


def same(x, y):
    if isinstance(x, float) or isinstance(y, float):
        if x is None or y is None:
            return x is None and y is None
        return (math.isnan(x) and math.isnan(y)) or math.isclose(x, y, rel_tol=1e-9, abs_tol=1e-12)
    if isinstance(x, dt.datetime) and isinstance(y, dt.datetime):
        return x.replace(tzinfo=None) == y.replace(tzinfo=None)
    return x == y


# ---------------------------------------------------------------- 2. one question, every way

def every_way(con, tmp):
    """Revenue of tea-like items per user tier, over orders with qty > 0, asked nine ways."""
    want = con.sql("""SELECT u.tier, round(sum(o.qty * o.price), 4) AS revenue FROM orders o JOIN users u USING (user)
                      WHERE o.qty > 0 AND o.item LIKE 't%' GROUP BY u.tier ORDER BY u.tier""").rows()
    rev = lambda f: f.group_by("tier").agg((col("qty") * col("price")).sum().round(4).alias("revenue")).sort("tier")
    o, u = con.table("orders"), con.table("users")
    ways = {}
    ways["frames alone"] = rev(o.filter((col("qty") > 0) & col("item").str.starts_with("t")).join(u, on="user")).rows()
    ways["SQL, then frames"] = rev(con.sql("SELECT * FROM orders WHERE qty > 0 AND item LIKE 't%'").join(u, on="user")).rows()
    teaish = o.filter("qty > 0 AND item LIKE 't%'")  # (a frame, named in the SQL below by its Python name)
    ways["a frame named in SQL"] = con.sql("SELECT u.tier, round(sum(t.qty * t.price), 4) AS revenue FROM teaish t JOIN users u USING (user) GROUP BY u.tier ORDER BY u.tier").rows()
    tiers = con.table("users").to_pandas()  # (pandas data here, joined in SQL there)
    ways["pandas named in SQL"] = con.sql("SELECT t.tier, round(sum(o.qty * o.price), 4) AS revenue FROM orders o JOIN tiers t USING (user) WHERE o.qty > 0 AND o.item LIKE 't%' GROUP BY t.tier ORDER BY t.tier").rows()
    ways["keywords: {frame}, $param"] = con.sql("SELECT u.tier, round(sum(t.qty * t.price), 4) AS revenue FROM {t} t JOIN users u USING (user) WHERE t.qty > $min GROUP BY u.tier ORDER BY u.tier", t=teaish, min=0).rows()
    path = os.path.join(tmp, "teaish.sql")
    open(path, "w").write("-- a model, as a .sql file\nCREATE OR REPLACE VIEW teaish_v AS\nSELECT * FROM orders WHERE qty > $min AND item LIKE $pattern;\n")
    con.run(path, min=0, pattern="t%")
    ways["a .sql file, then Python"] = rev(con.table("teaish_v").join(u, on="user")).rows()
    teaish.join(u, on="user").to_view("teaish_tiers")
    ways["a view made from a frame, read by SQL"] = con.sql("SELECT tier, round(sum(qty * price), 4) AS revenue FROM teaish_tiers GROUP BY tier ORDER BY tier").rows()
    ways["a %%sql cell"] = cell(con, "SELECT tier, round(sum(qty * price), 4) AS revenue FROM teaish_tiers GROUP BY tier ORDER BY tier")

    @con.procedure
    def revenue_by_tier(con, pattern: str = "t%"):
        from pondra import col
        o = con.table("orders").filter((col("qty") > 0) & col("item").str.contains("^" + pattern.rstrip("%")))
        return o.join(con.table("users"), on="user").group_by("tier").agg((col("qty") * col("price")).sum().round(4).alias("revenue")).sort("tier")

    ways["a Python procedure CALLed from SQL"] = con.sql("CALL revenue_by_tier('t%')").rows()
    ways["…and from Python"] = con.call("revenue_by_tier").rows()
    job = os.path.join(tmp, "revenue_job.py")  # (a Python file kept in the lake as a procedure)
    open(job, "w").write("from pondra import col\nt = con.table('teaish_tiers')\n"
                         "t.group_by('tier').agg((col('qty') * col('price')).sum().round(4).alias('revenue')).sort('tier')\n")
    con.create_procedure("revenue_job", file=job)
    ways["…and a Python file kept as one"] = con.call("revenue_job").rows()
    return {k: v == want or v for k, v in ways.items()}, want


def cell(con, text):
    """A `%%sql` cell, run the way IPython runs it (the notebook's names are the cell's)."""
    from IPython.core.interactiveshell import InteractiveShell
    shell = InteractiveShell.instance()
    shell.run_line_magic("load_ext", "pondra")
    shell.user_ns["con"] = con
    shell.run_cell_magic("sql", "answer <<", text)
    return shell.user_ns["answer"].rows()


# ---------------------------------------------------------------- 3. a sort kept

def sorts(con):
    """Sort, then limit: the top rows, not any rows. SQL drops a CTE's ORDER BY; a frame puts its
    sort in every step that keeps it (without, this returns the first rows the scan meets)."""
    o = con.table("big")
    top = o.sort("v", descending=True).limit(5).rows()
    after = o.sort("v", descending=True).filter(col("v") % 2 == 0).select("v").limit(3).rows()
    return {"sort, then limit: the top rows": [r["v"] for r in top] == [99999, 99998, 99997, 99996, 99995],
            "sort, filter, select, limit": [r["v"] for r in after] == [99998, 99996, 99994]}


# ---------------------------------------------------------------- 4. writes from frames

def writes(con):
    """Frames and PySpark's names write as SQL does: CREATE TABLE … AS, INSERT, UPDATE, DELETE, and
    MERGE built with Delta Lake's names (delta-rs's and Delta on Spark's), against a model."""
    from pondra.spark import DeltaTable, SparkSession, functions as F
    o = con.table("orders").filter(col("id") < 100).select("id", "user", "qty")
    model = {r["id"]: dict(r) for r in o.rows()}
    o.write_table("w")
    o.filter(col("id") < 10).write_table("w", mode="append")  # (ids 0-9 twice now)
    w = con.table("w")
    checks = {"write_table: create, then append": w.select(pondra.len()).item() == 110}
    w.delete(where=col("id") >= 90)
    w.update({"qty": coalesce_(col("qty"), 0) + 100}, where="id < 5")
    got = {r["id"]: r for r in w.filter(col("id") < 5).rows()}
    checks["update, delete"] = w.select(pondra.len()).item() == 100 and all(got[i]["qty"] == (model[i]["qty"] or 0) + 100 for i in got)
    con.sql("DELETE FROM w WHERE id < 10")  # (both copies of ids 0-9: 80 rows left)
    src = con.from_rows([{"id": 20, "user": "zz", "qty": -1}, {"id": 5000, "user": "new", "qty": 7}])
    w.merge(src, on="id").when_matched_update({"user": col("s.user"), "qty": col("s.qty")}).when_not_matched_insert().run()
    after = {r["id"]: r for r in w.rows()}
    checks["merge (delta-rs's names): matched updated, new inserted"] = after[20]["user"] == "zz" and after[5000]["qty"] == 7 and len(after) == 81
    spark = SparkSession(con)
    upd = spark.createDataFrame([(30, "yy", 3), (6000, "brand", 1)], "id bigint, user string, qty bigint")
    DeltaTable.forName(spark, "w").alias("t").merge(upd.alias("s"), "t.id = s.id") \
        .whenMatchedUpdate(set={"user": "s.user"}).whenNotMatchedInsertAll().execute()
    after = {r["id"]: r for r in w.rows()}
    checks["merge (Delta on Spark's names)"] = after[30]["user"] == "yy" and after[6000]["user"] == "brand" and len(after) == 82
    spark.table("w").where(F.col("id") > 5000).write.mode("append").saveAsTable("w")
    checks["saveAsTable append"] = w.filter(col("id") > 5000).select(pondra.len()).item() == 2
    return checks


# ---------------------------------------------------------------- 5. one name, one meaning

def names(con):
    """A connection and a frame make things by the same names, as SQL does (ADR-025): `db.view`
    and `to_view` a stored query unless `materialized=True`; `db.write_table` and a frame's
    `write_table` a table. (0.22's `db.view` made every view a materialized one.)"""
    import warnings
    per_user = "SELECT user, count(*) AS n FROM orders GROUP BY user"
    kind = lambda v: "materialized" if _made_again(con, v) else "stored"
    con.view("n_plain", per_user)
    con.view("n_live", con.sql(per_user), materialized=True)
    con.table("orders").group_by("user").agg(pondra.len().alias("n")).to_view("n_frame")
    want = con.sql(per_user).sort("user").rows()
    checks = {"db.view is a stored query, as CREATE VIEW and to_view make": (kind("n_plain"), kind("n_frame")) == ("stored", "stored"),
              "…materialized=True keeps it up to date, from SQL or a frame": kind("n_live") == "materialized",
              "…all three answer alike": all(con.table(v).sort("user").rows() == want for v in ("n_plain", "n_live", "n_frame"))}
    try:
        con.table("orders").to_view("n_bad", window="w")
        checks["options without materialized=True are refused"] = False
    except ValueError:
        checks["options without materialized=True are refused"] = True
    with warnings.catch_warnings(record=True) as said:
        warnings.simplefilter("always")
        con.view("n_old", "SELECT date_bin(INTERVAL '1 day', ts) AS w, count(*) AS n FROM orders GROUP BY w", window="w", size_secs=86400)
    checks["0.22's db.view(…, window=…): still materialized, with a warning"] = kind("n_old") == "materialized" and any(issubclass(w.category, DeprecationWarning) for w in said)
    con.write_table("w_rows", pd.DataFrame({"id": [1, 2, 3]}))
    con.write_table("w_rows", con.table("w_rows").filter(col("id") > 2), mode="append")
    checks["db.write_table: pandas data, then a frame, as a frame's write_table"] = con.table("w_rows").sort("id").rows() == [{"id": 1}, {"id": 2}, {"id": 3}, {"id": 3}]
    return checks


def _made_again(con, name):
    """Is `name` a materialized view? Making another under its name says it exists already; a
    stored view of that name is refused as one ("is a (stored) view")."""
    try:
        con.sql(f"CREATE MATERIALIZED VIEW {name} AS SELECT id FROM orders")
    except RuntimeError as e:
        return "already exists" in str(e)
    raise AssertionError(f"there was no view {name}")


def coalesce_(*xs):
    return pondra.coalesce(*xs)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", default="")
    ap.add_argument("--port", type=int, default=8830)
    A = ap.parse_args()
    tmp = tempfile.mkdtemp(prefix="pondra-frames-")
    con = pondra.local(os.path.join(tmp, "lake"), port=A.port)
    try:
        d = data()
        for name, t in d.items():
            cols = ", ".join(f'"{f.name}" {({"int64": "BIGINT", "string": "VARCHAR", "double": "DOUBLE", "timestamp[us]": "TIMESTAMP", "date32[day]": "DATE"})[str(f.type)]}' for f in t.schema)
            con.sql(f"CREATE TABLE {name} ({cols})")
            con.append(name, t)
        con.sql("CREATE TABLE big AS SELECT value AS v FROM generate_series(0, 99999) ORDER BY random()")
        ours = {k: con.table(k) for k in d}
        theirs = {k: pl.from_arrow(v).lazy() for k, v in d.items()}
        results, only = {}, set(filter(None, A.only.split(",")))
        for name, (f, g, ordered) in pipelines().items():
            if only and name not in only:
                continue
            try:
                ok, why = compare(f(con, ours["orders"], ours["users"], ours["quotes"]).collect(), g(theirs["orders"], theirs["users"], theirs["quotes"]).collect().to_arrow(), ordered)
            except Exception as e:  # noqa: BLE001 (reported, not fatal)
                ok, why = False, f"{type(e).__name__}: {str(e)[:400]}"
            results[name] = ok
            print(json.dumps({"pipeline": name, "equal": ok, **({"why": why} if why else {})}), flush=True)
        ways, want = every_way(con, tmp)
        print(json.dumps({"one question, every way": ways, "answer": want}, default=str), flush=True)
        kept = sorts(con)
        print(json.dumps({"sorts": kept}), flush=True)
        wrote = writes(con)
        print(json.dumps({"writes": wrote}), flush=True)
        named = names(con)
        print(json.dumps({"names": named}, ensure_ascii=False), flush=True)
        kept.update(wrote)
        kept.update(named)
        ok = all(results.values()) and all(v is True for v in ways.values()) and all(kept.values())
        print(json.dumps({"pipelines_equal": sum(results.values()), "pipelines": len(results), "ways_equal": sum(v is True for v in ways.values()), "ways": len(ways), "ok": ok}))
        sys.exit(0 if ok else 1)
    finally:
        con.close()
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    main()
