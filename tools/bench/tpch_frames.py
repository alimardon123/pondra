#!/usr/bin/env python3
"""TPC-H, three ways: SQL (tpch-queries/q1..q22.sql), pondra.frame (Polars-style) and pondra.spark
(PySpark-style), checked equal.

  tpch_frames.py [--queries 1,2,3] [--data ~/tpch/sf1-bench] [--port 8741]

Loads SF1 Parquet into a fresh lake, runs each query the three ways (best of 2), compares the
frame and spark answers to the SQL answer (row/column counts; values in order when the query has
an ORDER BY, floats to a relative 1e-6), and prints one JSON line per query plus a summary line.
"""
import argparse, datetime, decimal, json, os, shutil, sys, tempfile, time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "..", "python"))  # the pondra client
sys.path.insert(0, HERE)  # tpch.py, for its q15-as-CTE rewrite
os.environ.setdefault("PONDRA_BIN", os.path.join(HERE, "..", "..", "target", "release", "pondra"))

import pyarrow as pa
import pyarrow.parquet as pq

import pondra
from pondra import col, lit, when
from pondra import len as p_len
from pondra.spark import SparkSession
from pondra.spark import functions as F

from tpch import queries as sql_query_texts  # q15's CREATE VIEW + query -> one CTE statement

TABLES = ["region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem"]
SQL_TYPE = {"int64": "BIGINT", "int32": "INT", "string": "VARCHAR", "double": "DOUBLE", "date32[day]": "DATE", "bool": "BOOLEAN"}

# Queries with no top-level ORDER BY (q6, q14, q17, q19) each return exactly one row: order never
# matters for them. Every other query's frame/spark translation keeps the same ORDER BY columns
# as the SQL, so ties break the same way on all three (same engine, same data).
ORDERED = {i: i not in (6, 14, 17, 19) for i in range(1, 23)}


# ---------------------------------------------------------------- loading

def load(con, data):
    """SF1 Parquet -> a fresh lake: one CREATE TABLE per table, appended in batches (lineitem is
    6M rows; one append per file would be a single huge request)."""
    for t in TABLES:
        pf = pq.ParquetFile(os.path.join(data, f"{t}.parquet"))
        cols = ", ".join(f'"{f.name}" {SQL_TYPE[str(f.type)]}' for f in pf.schema_arrow)
        con.sql(f"CREATE TABLE {t} ({cols})")
        for batch in pf.iter_batches(batch_size=500_000):
            con.append(t, batch)
    con._call("POST", "/tier")  # Parquet, not just the log tail: realistic query times


# ---------------------------------------------------------------- comparing answers

def _cell(v):
    """A pyarrow scalar's Python value, normalized for comparison (dates as their ISO day,
    Decimal as float — this dataset has none, but singlenode.py's benches keep the same rule)."""
    if isinstance(v, (datetime.date, datetime.datetime)):
        return str(v)[:10]
    if isinstance(v, decimal.Decimal):
        return float(v)
    return v


def _equal(a, b, rel=1e-6):
    a, b = _cell(a), _cell(b)
    if isinstance(a, float) or isinstance(b, float):
        try:
            fa, fb = float(a), float(b)
        except (TypeError, ValueError):
            return a == b
        return fa == fb or abs(fa - fb) <= rel * max(abs(fa), abs(fb), 1e-12)
    return a == b


def compare(ref, other, ordered):
    """`ref` (the SQL answer) against `other` (frame or spark), by position: same row and column
    counts, values equal in ORDER BY order (or the query's one row). Column names may differ."""
    if ref.num_rows != other.num_rows:
        return False, f"{ref.num_rows} rows vs {other.num_rows}"
    if ref.num_columns != other.num_columns:
        return False, f"{ref.num_columns} columns vs {other.num_columns}"
    ref_rows, other_rows = ref.to_pylist(), other.to_pylist()
    ref_cols, other_cols = ref.column_names, other.column_names
    for i in range(ref.num_rows):
        a = [ref_rows[i][c] for c in ref_cols]
        b = [other_rows[i][c] for c in other_cols]
        if not all(_equal(x, y) for x, y in zip(a, b)):
            return False, f"row {i}: {a} vs {b}"
    return True, None


def best_of(fn, n=2):
    """The best of `n` runs; each run's text differs (`fn(k)`), or the node's remembered answer
    would time nothing."""
    best, out = None, None
    for k in range(n):
        t0 = time.time()
        out = fn(k)
        best = (time.time() - t0) if best is None else min(best, time.time() - t0)
    return out, best


def fresh(frame, k):
    """The same frame, its text told apart by a comment."""
    return frame._with(f"{frame._query}\n-- run {k}", frame._ctes)


# ==================================================================
# pondra.frame: one function per query, Polars-style
# ==================================================================

def frame_q1(con):
    li = con.table("lineitem")
    disc_price = col("l_extendedprice") * (1 - col("l_discount"))
    charge = disc_price * (1 + col("l_tax"))
    return (li.filter(col("l_shipdate") <= datetime.date(1998, 9, 2))
              .group_by("l_returnflag", "l_linestatus")
              .agg(col("l_quantity").sum().alias("sum_qty"),
                   col("l_extendedprice").sum().alias("sum_base_price"),
                   disc_price.sum().alias("sum_disc_price"),
                   charge.sum().alias("sum_charge"),
                   col("l_quantity").mean().alias("avg_qty"),
                   col("l_extendedprice").mean().alias("avg_price"),
                   col("l_discount").mean().alias("avg_disc"),
                   p_len().alias("count_order"))
              .sort("l_returnflag", "l_linestatus"))


def frame_q2(con):
    part, supplier, partsupp, nation, region = (con.table(t) for t in ("part", "supplier", "partsupp", "nation", "region"))
    euro = (partsupp.join(supplier, left_on="ps_suppkey", right_on="s_suppkey")
                     .join(nation, left_on="s_nationkey", right_on="n_nationkey")
                     .join(region, left_on="n_regionkey", right_on="r_regionkey")
                     .filter(col("r_name") == "EUROPE"))
    cheapest = euro.group_by("ps_partkey").agg(col("ps_supplycost").min().alias("min_supplycost"))
    parts = part.filter((col("p_size") == 15) & col("p_type").str.ends_with("BRASS"))
    return (parts.join(euro, left_on="p_partkey", right_on="ps_partkey")
                 .join(cheapest, left_on="p_partkey", right_on="ps_partkey")  # ps_partkey didn't survive the first join (see report: dropped join keys)
                 .filter(col("ps_supplycost") == col("min_supplycost"))
                 .select("s_acctbal", "s_name", "n_name", "p_partkey", "p_mfgr", "s_address", "s_phone", "s_comment")
                 .sort(["s_acctbal", "n_name", "s_name", "p_partkey"], descending=[True, False, False, False])
                 .limit(100))


def frame_q3(con):
    customer, orders, lineitem = con.table("customer"), con.table("orders"), con.table("lineitem")
    revenue = (col("l_extendedprice") * (1 - col("l_discount"))).sum().alias("revenue")
    return (customer.filter(col("c_mktsegment") == "BUILDING")
                     .join(orders, left_on="c_custkey", right_on="o_custkey")
                     .filter(col("o_orderdate") < datetime.date(1995, 3, 15))
                     .join(lineitem, left_on="o_orderkey", right_on="l_orderkey")
                     .filter(col("l_shipdate") > datetime.date(1995, 3, 15))
                     .group_by("o_orderkey", "o_orderdate", "o_shippriority")  # l_orderkey didn't survive the lineitem join (dropped join key)
                     .agg(revenue)
                     .select("o_orderkey", "revenue", "o_orderdate", "o_shippriority")  # group_by puts keys before aggs; reorder to match the SQL's column order
                     .sort(["revenue", "o_orderdate"], descending=[True, False])
                     .limit(10))


def frame_q4(con):
    orders, lineitem = con.table("orders"), con.table("lineitem")
    late = lineitem.filter(col("l_commitdate") < col("l_receiptdate"))
    return (orders.filter(col("o_orderdate").is_between(datetime.date(1993, 7, 1), datetime.date(1993, 9, 30)))
                  .join(late, left_on="o_orderkey", right_on="l_orderkey", how="semi")
                  .group_by("o_orderpriority")
                  .agg(p_len().alias("order_count"))
                  .sort("o_orderpriority"))


def frame_q5(con):
    customer, orders, lineitem, supplier, nation, region = (con.table(t) for t in ("customer", "orders", "lineitem", "supplier", "nation", "region"))
    revenue = (col("l_extendedprice") * (1 - col("l_discount"))).sum().alias("revenue")
    return (customer.join(orders, left_on="c_custkey", right_on="o_custkey")
                     .filter(col("o_orderdate").is_between(datetime.date(1994, 1, 1), datetime.date(1994, 12, 31)))
                     .join(lineitem, left_on="o_orderkey", right_on="l_orderkey")
                     .join(supplier, left_on=["l_suppkey", "c_nationkey"], right_on=["s_suppkey", "s_nationkey"])
                     .join(nation, left_on="c_nationkey", right_on="n_nationkey")
                     .join(region, left_on="n_regionkey", right_on="r_regionkey")
                     .filter(col("r_name") == "ASIA")
                     .group_by("n_name")
                     .agg(revenue)
                     .sort("revenue", descending=True))


def frame_q6(con):
    li = con.table("lineitem")
    return (li.filter(col("l_shipdate").is_between(datetime.date(1994, 1, 1), datetime.date(1994, 12, 31))
                       & col("l_discount").is_between(0.05, 0.07) & (col("l_quantity") < 24))
              .select((col("l_extendedprice") * col("l_discount")).sum().alias("revenue")))


def frame_q7(con):
    supplier, lineitem, orders, customer = con.table("supplier"), con.table("lineitem"), con.table("orders"), con.table("customer")
    n1, n2 = con.table("nation"), con.table("nation")
    shipping = (supplier.join(lineitem, left_on="s_suppkey", right_on="l_suppkey")
                         .join(orders, left_on="l_orderkey", right_on="o_orderkey")
                         .join(customer, left_on="o_custkey", right_on="c_custkey")
                         .join(n1, left_on="s_nationkey", right_on="n_nationkey")
                         .join(n2, left_on="c_nationkey", right_on="n_nationkey", suffix="_cust")
                         .filter(col("l_shipdate").is_between(datetime.date(1995, 1, 1), datetime.date(1996, 12, 31))
                                 & (((col("n_name") == "FRANCE") & (col("n_name_cust") == "GERMANY"))
                                    | ((col("n_name") == "GERMANY") & (col("n_name_cust") == "FRANCE"))))
                         .select(col("n_name").alias("supp_nation"), col("n_name_cust").alias("cust_nation"),
                                 col("l_shipdate").dt.year().alias("l_year"),
                                 (col("l_extendedprice") * (1 - col("l_discount"))).alias("volume")))
    return (shipping.group_by("supp_nation", "cust_nation", "l_year")
                     .agg(col("volume").sum().alias("revenue"))
                     .sort("supp_nation", "cust_nation", "l_year"))


def frame_q8(con):
    part, supplier, lineitem, orders, customer = (con.table(t) for t in ("part", "supplier", "lineitem", "orders", "customer"))
    n1, n2, region = con.table("nation"), con.table("nation"), con.table("region")
    all_nations = (part.filter(col("p_type") == "ECONOMY ANODIZED STEEL")
                        .join(lineitem, left_on="p_partkey", right_on="l_partkey")
                        .join(supplier, left_on="l_suppkey", right_on="s_suppkey")
                        .join(orders, left_on="l_orderkey", right_on="o_orderkey")
                        .filter(col("o_orderdate").is_between(datetime.date(1995, 1, 1), datetime.date(1996, 12, 31)))
                        .join(customer, left_on="o_custkey", right_on="c_custkey")
                        .join(n1, left_on="c_nationkey", right_on="n_nationkey")
                        .join(region, left_on="n_regionkey", right_on="r_regionkey")
                        .filter(col("r_name") == "AMERICA")
                        .join(n2, left_on="s_nationkey", right_on="n_nationkey", suffix="_supp")
                        .select(col("o_orderdate").dt.year().alias("o_year"),
                                (col("l_extendedprice") * (1 - col("l_discount"))).alias("volume"),
                                col("n_name_supp").alias("nation")))
    return (all_nations.group_by("o_year")
                        .agg((when(col("nation") == "BRAZIL").then(col("volume")).otherwise(0).sum()
                              / col("volume").sum()).alias("mkt_share"))
                        .sort("o_year"))


def frame_q9(con):
    part, supplier, lineitem, partsupp, orders, nation = (con.table(t) for t in ("part", "supplier", "lineitem", "partsupp", "orders", "nation"))
    profit = (part.filter(col("p_name").str.contains("green", literal=True))
                   .join(lineitem, left_on="p_partkey", right_on="l_partkey")
                   .join(supplier, left_on="l_suppkey", right_on="s_suppkey")
                   .join(partsupp, left_on=["p_partkey", "l_suppkey"], right_on=["ps_partkey", "ps_suppkey"])  # l_partkey didn't survive the part join (dropped join key); p_partkey holds the same values
                   .join(orders, left_on="l_orderkey", right_on="o_orderkey")
                   .join(nation, left_on="s_nationkey", right_on="n_nationkey")
                   .select(col("n_name").alias("nation"), col("o_orderdate").dt.year().alias("o_year"),
                           (col("l_extendedprice") * (1 - col("l_discount")) - col("ps_supplycost") * col("l_quantity")).alias("amount")))
    return (profit.group_by("nation", "o_year")
                  .agg(col("amount").sum().alias("sum_profit"))
                  .sort(["nation", "o_year"], descending=[False, True]))


def frame_q10(con):
    customer, orders, lineitem, nation = con.table("customer"), con.table("orders"), con.table("lineitem"), con.table("nation")
    revenue = (col("l_extendedprice") * (1 - col("l_discount"))).sum().alias("revenue")
    return (customer.join(orders, left_on="c_custkey", right_on="o_custkey")
                     .filter(col("o_orderdate").is_between(datetime.date(1993, 10, 1), datetime.date(1993, 12, 31)))
                     .join(lineitem, left_on="o_orderkey", right_on="l_orderkey")
                     .filter(col("l_returnflag") == "R")
                     .join(nation, left_on="c_nationkey", right_on="n_nationkey")
                     .group_by("c_custkey", "c_name", "c_acctbal", "c_phone", "n_name", "c_address", "c_comment")
                     .agg(revenue)
                     .select("c_custkey", "c_name", "revenue", "c_acctbal", "n_name", "c_address", "c_phone", "c_comment")  # reorder to match the SQL's column order
                     .sort("revenue", descending=True)
                     .limit(20))


def frame_q11(con):
    partsupp, supplier, nation = con.table("partsupp"), con.table("supplier"), con.table("nation")
    german = (partsupp.join(supplier, left_on="ps_suppkey", right_on="s_suppkey")
                       .join(nation, left_on="s_nationkey", right_on="n_nationkey")
                       .filter(col("n_name") == "GERMANY"))
    value = (col("ps_supplycost") * col("ps_availqty"))
    threshold = german.select((value.sum() * 0.0001).alias("threshold")).item()
    return (german.group_by("ps_partkey")
                  .agg(value.sum().alias("value"))
                  .filter(col("value") > threshold)
                  .sort("value", descending=True))


def frame_q12(con):
    lineitem, orders = con.table("lineitem"), con.table("orders")
    return (lineitem.filter(col("l_shipmode").is_in(["MAIL", "SHIP"])
                             & (col("l_commitdate") < col("l_receiptdate")) & (col("l_shipdate") < col("l_commitdate"))
                             & col("l_receiptdate").is_between(datetime.date(1994, 1, 1), datetime.date(1994, 12, 31)))
                     .join(orders, left_on="l_orderkey", right_on="o_orderkey")
                     .group_by("l_shipmode")
                     .agg(when(col("o_orderpriority").is_in(["1-URGENT", "2-HIGH"])).then(1).otherwise(0).sum().alias("high_line_count"),
                          when(~col("o_orderpriority").is_in(["1-URGENT", "2-HIGH"])).then(1).otherwise(0).sum().alias("low_line_count"))
                     .sort("l_shipmode"))


def frame_q13(con):
    customer, orders = con.table("customer"), con.table("orders")
    not_special = orders.filter(~col("o_comment").str.contains("special.*requests"))
    c_orders = (customer.join(not_special, left_on="c_custkey", right_on="o_custkey", how="left")
                         .group_by("c_custkey")
                         .agg(col("o_orderkey").count().alias("c_count")))
    return (c_orders.group_by("c_count")
                     .agg(p_len().alias("custdist"))
                     .sort(["custdist", "c_count"], descending=[True, True]))


def frame_q14(con):
    lineitem, part = con.table("lineitem"), con.table("part")
    disc_price = col("l_extendedprice") * (1 - col("l_discount"))
    return (lineitem.filter(col("l_shipdate").is_between(datetime.date(1995, 9, 1), datetime.date(1995, 9, 30)))
                     .join(part, left_on="l_partkey", right_on="p_partkey")
                     .select((100.00 * when(col("p_type").str.starts_with("PROMO")).then(disc_price).otherwise(0).sum()
                              / disc_price.sum()).alias("promo_revenue")))


def frame_q15(con):
    lineitem, supplier = con.table("lineitem"), con.table("supplier")
    revenue0 = (lineitem.filter(col("l_shipdate").is_between(datetime.date(1996, 1, 1), datetime.date(1996, 3, 31)))
                         .group_by("l_suppkey")
                         .agg((col("l_extendedprice") * (1 - col("l_discount"))).sum().alias("total_revenue"))
                         .rename({"l_suppkey": "supplier_no"}))
    top = revenue0.select(col("total_revenue").max()).item()
    return (supplier.join(revenue0, left_on="s_suppkey", right_on="supplier_no")
                     .filter(col("total_revenue") == top)
                     .select("s_suppkey", "s_name", "s_address", "s_phone", "total_revenue")
                     .sort("s_suppkey"))


def frame_q16(con):
    partsupp, part, supplier = con.table("partsupp"), con.table("part"), con.table("supplier")
    complainers = supplier.filter(col("s_comment").str.contains("Customer.*Complaints")).select("s_suppkey")
    return (partsupp.join(part, left_on="ps_partkey", right_on="p_partkey")
                     .filter((col("p_brand") != "Brand#45") & ~col("p_type").str.starts_with("MEDIUM POLISHED")
                             & col("p_size").is_in([49, 14, 23, 45, 19, 3, 36, 9]))
                     .join(complainers, left_on="ps_suppkey", right_on="s_suppkey", how="anti")
                     .group_by("p_brand", "p_type", "p_size")
                     .agg(col("ps_suppkey").n_unique().alias("supplier_cnt"))
                     .sort(["supplier_cnt", "p_brand", "p_type", "p_size"], descending=[True, False, False, False]))


def frame_q17(con):
    lineitem, part = con.table("lineitem"), con.table("part")
    parts = part.filter((col("p_brand") == "Brand#23") & (col("p_container") == "MED BOX"))
    avg_qty = (lineitem.join(parts, left_on="l_partkey", right_on="p_partkey")
                        .group_by("l_partkey")
                        .agg((col("l_quantity").mean() * 0.2).alias("small_qty")))
    return (lineitem.join(parts, left_on="l_partkey", right_on="p_partkey")
                     .join(avg_qty, on="l_partkey")
                     .filter(col("l_quantity") < col("small_qty"))
                     .select((col("l_extendedprice").sum() / 7.0).alias("avg_yearly")))


def frame_q18(con):
    customer, orders, lineitem = con.table("customer"), con.table("orders"), con.table("lineitem")
    big = (lineitem.group_by("l_orderkey")
                    .agg(col("l_quantity").sum().alias("total_qty"))
                    .filter(col("total_qty") > 300))
    return (customer.join(orders, left_on="c_custkey", right_on="o_custkey")
                     .join(lineitem, left_on="o_orderkey", right_on="l_orderkey")
                     .join(big, left_on="o_orderkey", right_on="l_orderkey", how="semi")  # l_orderkey didn't survive the lineitem join (dropped join key)
                     .group_by("c_name", "c_custkey", "o_orderkey", "o_orderdate", "o_totalprice")
                     .agg(col("l_quantity").sum())
                     .sort(["o_totalprice", "o_orderdate"], descending=[True, False])
                     .limit(100))


def frame_q19(con):
    lineitem, part = con.table("lineitem"), con.table("part")
    j = lineitem.join(part, left_on="l_partkey", right_on="p_partkey")
    small = ((col("p_brand") == "Brand#12") & col("p_container").is_in(["SM CASE", "SM BOX", "SM PACK", "SM PKG"])
              & col("l_quantity").is_between(1, 11) & col("p_size").is_between(1, 5))
    medium = ((col("p_brand") == "Brand#23") & col("p_container").is_in(["MED BAG", "MED BOX", "MED PKG", "MED PACK"])
              & col("l_quantity").is_between(10, 20) & col("p_size").is_between(1, 10))
    large = ((col("p_brand") == "Brand#34") & col("p_container").is_in(["LG CASE", "LG BOX", "LG PACK", "LG PKG"])
             & col("l_quantity").is_between(20, 30) & col("p_size").is_between(1, 15))
    air = col("l_shipmode").is_in(["AIR", "AIR REG"]) & (col("l_shipinstruct") == "DELIVER IN PERSON")
    return (j.filter(air & (small | medium | large))
              .select((col("l_extendedprice") * (1 - col("l_discount"))).sum().alias("revenue")))


def frame_q20(con):
    supplier, nation, partsupp, part, lineitem = (con.table(t) for t in ("supplier", "nation", "partsupp", "part", "lineitem"))
    forest_parts = part.filter(col("p_name").str.starts_with("forest")).select("p_partkey")
    used = (lineitem.filter(col("l_shipdate").is_between(datetime.date(1994, 1, 1), datetime.date(1994, 12, 31)))
                     .group_by("l_partkey", "l_suppkey")
                     .agg((col("l_quantity").sum() * 0.5).alias("half_qty")))
    qualifying = (partsupp.join(forest_parts, left_on="ps_partkey", right_on="p_partkey", how="semi")
                           .join(used, left_on=["ps_partkey", "ps_suppkey"], right_on=["l_partkey", "l_suppkey"])
                           .filter(col("ps_availqty") > col("half_qty"))
                           .select("ps_suppkey"))
    return (supplier.join(nation, left_on="s_nationkey", right_on="n_nationkey")
                     .filter(col("n_name") == "CANADA")
                     .join(qualifying, left_on="s_suppkey", right_on="ps_suppkey", how="semi")
                     .select("s_name", "s_address")
                     .sort("s_name"))


def frame_q21(con):
    lineitem, orders, supplier, nation = con.table("lineitem"), con.table("orders"), con.table("supplier"), con.table("nation")
    per_order = lineitem.group_by("l_orderkey").agg(col("l_suppkey").n_unique().alias("n_suppliers"))
    late = lineitem.filter(col("l_receiptdate") > col("l_commitdate"))
    late_per_order = late.group_by("l_orderkey").agg(col("l_suppkey").n_unique().alias("n_late_suppliers"),
                                                      col("l_suppkey").max().alias("the_late_supplier"))
    exists_other_supplier = col("n_suppliers") > 1
    no_other_late_supplier = col("n_late_suppliers").fill_null(0) == 0
    only_late_supplier_is_this_one = (col("n_late_suppliers") == 1) & (col("the_late_supplier") == col("l_suppkey"))
    return (late.join(orders, left_on="l_orderkey", right_on="o_orderkey")
                .filter(col("o_orderstatus") == "F")
                .join(supplier, left_on="l_suppkey", right_on="s_suppkey")
                .join(nation, left_on="s_nationkey", right_on="n_nationkey")
                .filter(col("n_name") == "SAUDI ARABIA")
                .join(per_order, on="l_orderkey")
                .join(late_per_order, on="l_orderkey")
                .filter(exists_other_supplier & (no_other_late_supplier | only_late_supplier_is_this_one))
                .group_by("s_name")
                .agg(p_len().alias("numwait"))
                .sort(["numwait", "s_name"], descending=[True, False])
                .limit(100))


def frame_q22(con):
    customer, orders = con.table("customer"), con.table("orders")
    codes = ["13", "31", "23", "29", "30", "18", "17"]
    with_code = customer.with_columns(col("c_phone").str.slice(0, 2).alias("cntrycode"))
    avg_bal = with_code.filter((col("c_acctbal") > 0) & col("cntrycode").is_in(codes)).select(col("c_acctbal").mean()).item()
    return (with_code.filter(col("cntrycode").is_in(codes) & (col("c_acctbal") > avg_bal))
                      .join(orders, left_on="c_custkey", right_on="o_custkey", how="anti")
                      .group_by("cntrycode")
                      .agg(p_len().alias("numcust"), col("c_acctbal").sum().alias("totacctbal"))
                      .sort("cntrycode"))


FRAME_QUERIES = {i: globals()[f"frame_q{i}"] for i in range(1, 23)}


# ==================================================================
# pondra.spark: one function per query, PySpark-style
# ==================================================================

def spark_q1(spark):
    li = spark.table("lineitem")
    disc_price = F.col("l_extendedprice") * (1 - F.col("l_discount"))
    charge = disc_price * (1 + F.col("l_tax"))
    return (li.where(F.col("l_shipdate") <= datetime.date(1998, 9, 2))
              .groupBy("l_returnflag", "l_linestatus")
              .agg(F.sum("l_quantity").alias("sum_qty"), F.sum("l_extendedprice").alias("sum_base_price"),
                   F.sum(disc_price).alias("sum_disc_price"), F.sum(charge).alias("sum_charge"),
                   F.avg("l_quantity").alias("avg_qty"), F.avg("l_extendedprice").alias("avg_price"),
                   F.avg("l_discount").alias("avg_disc"), F.count("*").alias("count_order"))
              .orderBy("l_returnflag", "l_linestatus"))


def spark_q2(spark):
    part, supplier, partsupp = spark.table("part"), spark.table("supplier"), spark.table("partsupp")
    nation, region = spark.table("nation"), spark.table("region")
    euro = (partsupp.join(supplier, partsupp.ps_suppkey == supplier.s_suppkey)
                     .join(nation, F.col("s_nationkey") == nation.n_nationkey)
                     .join(region, F.col("n_regionkey") == region.r_regionkey)
                     .where(F.col("r_name") == "EUROPE"))
    cheapest = euro.groupBy("ps_partkey").agg(F.min("ps_supplycost").alias("min_supplycost"))
    parts = part.where((F.col("p_size") == 15) & F.col("p_type").like("%BRASS"))
    return (parts.join(euro, parts.p_partkey == euro.ps_partkey)
                 .join(cheapest, "ps_partkey")
                 .where(F.col("ps_supplycost") == F.col("min_supplycost"))
                 .select("s_acctbal", "s_name", "n_name", "p_partkey", "p_mfgr", "s_address", "s_phone", "s_comment")
                 .orderBy(F.desc("s_acctbal"), "n_name", "s_name", "p_partkey")
                 .limit(100))


def spark_q3(spark):
    # Every join condition below uses unqualified F.col(), never `table.column`: a qualified
    # reference only resolves against the table's OWN original alias, which a later join (it
    # re-wraps the accumulated side into a fresh CTE) leaves invisible — see report, "a qualified
    # Column stops resolving after the DataFrame it names is joined again".
    customer, orders, lineitem = spark.table("customer"), spark.table("orders"), spark.table("lineitem")
    revenue = F.sum(F.col("l_extendedprice") * (1 - F.col("l_discount"))).alias("revenue")
    return (customer.join(orders, F.col("c_custkey") == F.col("o_custkey"))
                     .where((F.col("c_mktsegment") == "BUILDING") & (F.col("o_orderdate") < datetime.date(1995, 3, 15)))
                     .join(lineitem, F.col("o_orderkey") == F.col("l_orderkey"))
                     .where(F.col("l_shipdate") > datetime.date(1995, 3, 15))
                     .groupBy("o_orderkey", "o_orderdate", "o_shippriority")
                     .agg(revenue)
                     .select("o_orderkey", "revenue", "o_orderdate", "o_shippriority")  # groupBy puts keys before aggs; reorder to match the SQL
                     .orderBy(F.desc("revenue"), "o_orderdate")
                     .limit(10))


def spark_q4(spark):
    orders, lineitem = spark.table("orders"), spark.table("lineitem")
    late = lineitem.where(F.col("l_commitdate") < F.col("l_receiptdate"))
    return (orders.where(F.col("o_orderdate").between(datetime.date(1993, 7, 1), datetime.date(1993, 9, 30)))
                  .join(late, F.col("o_orderkey") == F.col("l_orderkey"), "leftsemi")
                  .groupBy("o_orderpriority")
                  .agg(F.count("*").alias("order_count"))
                  .orderBy("o_orderpriority"))


def spark_q5(spark):
    customer, orders, lineitem = spark.table("customer"), spark.table("orders"), spark.table("lineitem")
    supplier, nation, region = spark.table("supplier"), spark.table("nation"), spark.table("region")
    revenue = F.sum(F.col("l_extendedprice") * (1 - F.col("l_discount"))).alias("revenue")
    return (customer.join(orders, F.col("c_custkey") == F.col("o_custkey"))
                     .where(F.col("o_orderdate").between(datetime.date(1994, 1, 1), datetime.date(1994, 12, 31)))
                     .join(lineitem, F.col("o_orderkey") == F.col("l_orderkey"))
                     .join(supplier, (F.col("l_suppkey") == F.col("s_suppkey")) & (F.col("c_nationkey") == F.col("s_nationkey")))
                     .join(nation, F.col("c_nationkey") == F.col("n_nationkey"))
                     .join(region, F.col("n_regionkey") == F.col("r_regionkey"))
                     .where(F.col("r_name") == "ASIA")
                     .groupBy("n_name")
                     .agg(revenue)
                     .orderBy(F.desc("revenue")))


def spark_q6(spark):
    li = spark.table("lineitem")
    return (li.where(F.col("l_shipdate").between(datetime.date(1994, 1, 1), datetime.date(1994, 12, 31))
                     & F.col("l_discount").between(0.05, 0.07) & (F.col("l_quantity") < 24))
              .select(F.sum(F.col("l_extendedprice") * F.col("l_discount")).alias("revenue")))


def spark_q7(spark):
    supplier, lineitem, orders, customer = spark.table("supplier"), spark.table("lineitem"), spark.table("orders"), spark.table("customer")
    # nation is joined in twice; rename its columns right after loading each copy so every later
    # reference can stay a plain, unqualified F.col() (see spark_q3's note).
    n1 = spark.table("nation").select(F.col("n_nationkey").alias("n1_key"), F.col("n_name").alias("supp_nation"))
    n2 = spark.table("nation").select(F.col("n_nationkey").alias("n2_key"), F.col("n_name").alias("cust_nation"))
    shipping = (supplier.join(lineitem, F.col("s_suppkey") == F.col("l_suppkey"))
                         .join(orders, F.col("l_orderkey") == F.col("o_orderkey"))
                         .join(customer, F.col("o_custkey") == F.col("c_custkey"))
                         .join(n1, F.col("s_nationkey") == F.col("n1_key"))
                         .join(n2, F.col("c_nationkey") == F.col("n2_key"))
                         .where(F.col("l_shipdate").between(datetime.date(1995, 1, 1), datetime.date(1996, 12, 31))
                                & (((F.col("supp_nation") == "FRANCE") & (F.col("cust_nation") == "GERMANY"))
                                   | ((F.col("supp_nation") == "GERMANY") & (F.col("cust_nation") == "FRANCE"))))
                         .select("supp_nation", "cust_nation", F.year("l_shipdate").alias("l_year"),
                                 (F.col("l_extendedprice") * (1 - F.col("l_discount"))).alias("volume")))
    return (shipping.groupBy("supp_nation", "cust_nation", "l_year")
                     .agg(F.sum("volume").alias("revenue"))
                     .orderBy("supp_nation", "cust_nation", "l_year"))


def spark_q8(spark):
    part, supplier, lineitem, orders, customer = (spark.table(t) for t in ("part", "supplier", "lineitem", "orders", "customer"))
    n1 = spark.table("nation").select(F.col("n_nationkey").alias("n1_key"), F.col("n_regionkey").alias("n1_region"))
    n2 = spark.table("nation").select(F.col("n_nationkey").alias("n2_key"), F.col("n_name").alias("nation"))
    region = spark.table("region")
    all_nations = (part.where(F.col("p_type") == "ECONOMY ANODIZED STEEL")
                        .join(lineitem, F.col("p_partkey") == F.col("l_partkey"))
                        .join(supplier, F.col("l_suppkey") == F.col("s_suppkey"))
                        .join(orders, F.col("l_orderkey") == F.col("o_orderkey"))
                        .where(F.col("o_orderdate").between(datetime.date(1995, 1, 1), datetime.date(1996, 12, 31)))
                        .join(customer, F.col("o_custkey") == F.col("c_custkey"))
                        .join(n1, F.col("c_nationkey") == F.col("n1_key"))
                        .join(region, F.col("n1_region") == F.col("r_regionkey"))
                        .where(F.col("r_name") == "AMERICA")
                        .join(n2, F.col("s_nationkey") == F.col("n2_key"))
                        .select(F.year("o_orderdate").alias("o_year"),
                                (F.col("l_extendedprice") * (1 - F.col("l_discount"))).alias("volume"),
                                "nation"))
    return (all_nations.groupBy("o_year")
                        .agg((F.sum(F.when(F.col("nation") == "BRAZIL", F.col("volume")).otherwise(0)) / F.sum("volume")).alias("mkt_share"))
                        .orderBy("o_year"))


def spark_q9(spark):
    part, supplier, lineitem, partsupp, orders, nation = (spark.table(t) for t in ("part", "supplier", "lineitem", "partsupp", "orders", "nation"))
    profit = (part.where(F.col("p_name").contains("green"))
                   .join(lineitem, F.col("p_partkey") == F.col("l_partkey"))
                   .join(supplier, F.col("l_suppkey") == F.col("s_suppkey"))
                   .join(partsupp, (F.col("l_partkey") == F.col("ps_partkey")) & (F.col("l_suppkey") == F.col("ps_suppkey")))
                   .join(orders, F.col("l_orderkey") == F.col("o_orderkey"))
                   .join(nation, F.col("s_nationkey") == F.col("n_nationkey"))
                   .select(F.col("n_name").alias("nation"), F.year("o_orderdate").alias("o_year"),
                           (F.col("l_extendedprice") * (1 - F.col("l_discount")) - F.col("ps_supplycost") * F.col("l_quantity")).alias("amount")))
    return (profit.groupBy("nation", "o_year")
                  .agg(F.sum("amount").alias("sum_profit"))
                  .orderBy("nation", F.desc("o_year")))


def spark_q10(spark):
    customer, orders, lineitem, nation = spark.table("customer"), spark.table("orders"), spark.table("lineitem"), spark.table("nation")
    revenue = F.sum(F.col("l_extendedprice") * (1 - F.col("l_discount"))).alias("revenue")
    return (customer.join(orders, F.col("c_custkey") == F.col("o_custkey"))
                     .where(F.col("o_orderdate").between(datetime.date(1993, 10, 1), datetime.date(1993, 12, 31)))
                     .join(lineitem, F.col("o_orderkey") == F.col("l_orderkey"))
                     .where(F.col("l_returnflag") == "R")
                     .join(nation, F.col("c_nationkey") == F.col("n_nationkey"))
                     .groupBy("c_custkey", "c_name", "c_acctbal", "c_phone", "n_name", "c_address", "c_comment")
                     .agg(revenue)
                     .select("c_custkey", "c_name", "revenue", "c_acctbal", "n_name", "c_address", "c_phone", "c_comment")
                     .orderBy(F.desc("revenue"))
                     .limit(20))


def spark_q11(spark):
    partsupp, supplier, nation = spark.table("partsupp"), spark.table("supplier"), spark.table("nation")
    german = (partsupp.join(supplier, F.col("ps_suppkey") == F.col("s_suppkey"))
                       .join(nation, F.col("s_nationkey") == F.col("n_nationkey"))
                       .where(F.col("n_name") == "GERMANY"))
    value = F.col("ps_supplycost") * F.col("ps_availqty")
    threshold = german.select((F.sum(value) * 0.0001).alias("t")).collect()[0][0]
    return (german.groupBy("ps_partkey")
                  .agg(F.sum(value).alias("value"))
                  .where(F.col("value") > threshold)
                  .orderBy(F.desc("value")))


def spark_q12(spark):
    lineitem, orders = spark.table("lineitem"), spark.table("orders")
    return (lineitem.where(F.col("l_shipmode").isin("MAIL", "SHIP")
                            & (F.col("l_commitdate") < F.col("l_receiptdate")) & (F.col("l_shipdate") < F.col("l_commitdate"))
                            & F.col("l_receiptdate").between(datetime.date(1994, 1, 1), datetime.date(1994, 12, 31)))
                     .join(orders, F.col("l_orderkey") == F.col("o_orderkey"))
                     .groupBy("l_shipmode")
                     .agg(F.sum(F.when(F.col("o_orderpriority").isin("1-URGENT", "2-HIGH"), 1).otherwise(0)).alias("high_line_count"),
                          F.sum(F.when(~F.col("o_orderpriority").isin("1-URGENT", "2-HIGH"), 1).otherwise(0)).alias("low_line_count"))
                     .orderBy("l_shipmode"))


def spark_q13(spark):
    customer, orders = spark.table("customer"), spark.table("orders")
    not_special = orders.where(~F.col("o_comment").rlike("special.*requests"))
    c_orders = (customer.join(not_special, customer.c_custkey == not_special.o_custkey, "left")
                         .groupBy("c_custkey")
                         .agg(F.count("o_orderkey").alias("c_count")))
    return (c_orders.groupBy("c_count")
                     .agg(F.count("*").alias("custdist"))
                     .orderBy(F.desc("custdist"), F.desc("c_count")))


def spark_q14(spark):
    lineitem, part = spark.table("lineitem"), spark.table("part")
    disc_price = F.col("l_extendedprice") * (1 - F.col("l_discount"))
    return (lineitem.where(F.col("l_shipdate").between(datetime.date(1995, 9, 1), datetime.date(1995, 9, 30)))
                     .join(part, F.col("l_partkey") == F.col("p_partkey"))
                     .select((100.00 * F.sum(F.when(F.col("p_type").like("PROMO%"), disc_price).otherwise(0))
                              / F.sum(disc_price)).alias("promo_revenue")))


def spark_q15(spark):
    lineitem, supplier = spark.table("lineitem"), spark.table("supplier")
    revenue0 = (lineitem.where(F.col("l_shipdate").between(datetime.date(1996, 1, 1), datetime.date(1996, 3, 31)))
                         .groupBy("l_suppkey")
                         .agg(F.sum(F.col("l_extendedprice") * (1 - F.col("l_discount"))).alias("total_revenue"))
                         .withColumnRenamed("l_suppkey", "supplier_no"))
    top = revenue0.select(F.max("total_revenue")).collect()[0][0]
    return (supplier.join(revenue0, F.col("s_suppkey") == F.col("supplier_no"))
                     .where(F.col("total_revenue") == top)
                     .select("s_suppkey", "s_name", "s_address", "s_phone", "total_revenue")
                     .orderBy("s_suppkey"))


def spark_q16(spark):
    partsupp, part, supplier = spark.table("partsupp"), spark.table("part"), spark.table("supplier")
    complainers = supplier.where(F.col("s_comment").rlike("Customer.*Complaints")).select("s_suppkey")
    return (partsupp.join(part, F.col("ps_partkey") == F.col("p_partkey"))
                     .where((F.col("p_brand") != "Brand#45") & ~F.col("p_type").like("MEDIUM POLISHED%")
                            & F.col("p_size").isin(49, 14, 23, 45, 19, 3, 36, 9))
                     .join(complainers, F.col("ps_suppkey") == F.col("s_suppkey"), "leftanti")
                     .groupBy("p_brand", "p_type", "p_size")
                     .agg(F.countDistinct("ps_suppkey").alias("supplier_cnt"))
                     .orderBy(F.desc("supplier_cnt"), "p_brand", "p_type", "p_size"))


def spark_q17(spark):
    lineitem, part = spark.table("lineitem"), spark.table("part")
    parts = part.where((F.col("p_brand") == "Brand#23") & (F.col("p_container") == "MED BOX"))
    avg_qty = (lineitem.join(parts, lineitem.l_partkey == parts.p_partkey)
                        .groupBy("l_partkey")
                        .agg((F.avg("l_quantity") * 0.2).alias("small_qty")))
    return (lineitem.join(parts, lineitem.l_partkey == parts.p_partkey)
                     .join(avg_qty, "l_partkey")
                     .where(F.col("l_quantity") < F.col("small_qty"))
                     .select((F.sum("l_extendedprice") / 7.0).alias("avg_yearly")))


def spark_q18(spark):
    customer, orders, lineitem = spark.table("customer"), spark.table("orders"), spark.table("lineitem")
    big = (lineitem.groupBy("l_orderkey")
                    .agg(F.sum("l_quantity").alias("total_qty"))
                    .where(F.col("total_qty") > 300)
                    .withColumnRenamed("l_orderkey", "big_orderkey"))  # l_orderkey would else clash with the lineitem already joined below
    return (customer.join(orders, F.col("c_custkey") == F.col("o_custkey"))
                     .join(lineitem, F.col("o_orderkey") == F.col("l_orderkey"))
                     .join(big, F.col("o_orderkey") == F.col("big_orderkey"), "leftsemi")
                     .groupBy("c_name", "c_custkey", "o_orderkey", "o_orderdate", "o_totalprice")
                     .agg(F.sum("l_quantity"))
                     .orderBy(F.desc("o_totalprice"), "o_orderdate")
                     .limit(100))


def spark_q19(spark):
    lineitem, part = spark.table("lineitem"), spark.table("part")
    j = lineitem.join(part, lineitem.l_partkey == part.p_partkey)
    small = ((F.col("p_brand") == "Brand#12") & F.col("p_container").isin("SM CASE", "SM BOX", "SM PACK", "SM PKG")
              & F.col("l_quantity").between(1, 11) & F.col("p_size").between(1, 5))
    medium = ((F.col("p_brand") == "Brand#23") & F.col("p_container").isin("MED BAG", "MED BOX", "MED PKG", "MED PACK")
              & F.col("l_quantity").between(10, 20) & F.col("p_size").between(1, 10))
    large = ((F.col("p_brand") == "Brand#34") & F.col("p_container").isin("LG CASE", "LG BOX", "LG PACK", "LG PKG")
             & F.col("l_quantity").between(20, 30) & F.col("p_size").between(1, 15))
    air = F.col("l_shipmode").isin("AIR", "AIR REG") & (F.col("l_shipinstruct") == "DELIVER IN PERSON")
    return (j.where(air & (small | medium | large))
              .select(F.sum(F.col("l_extendedprice") * (1 - F.col("l_discount"))).alias("revenue")))


def spark_q20(spark):
    supplier, nation, partsupp, part, lineitem = (spark.table(t) for t in ("supplier", "nation", "partsupp", "part", "lineitem"))
    forest_parts = part.where(F.col("p_name").like("forest%")).select("p_partkey")
    used = (lineitem.where(F.col("l_shipdate").between(datetime.date(1994, 1, 1), datetime.date(1994, 12, 31)))
                     .groupBy("l_partkey", "l_suppkey")
                     .agg((F.sum("l_quantity") * 0.5).alias("half_qty")))
    qualifying = (partsupp.join(forest_parts, F.col("ps_partkey") == F.col("p_partkey"), "leftsemi")
                           .join(used, (F.col("ps_partkey") == F.col("l_partkey")) & (F.col("ps_suppkey") == F.col("l_suppkey")))
                           .where(F.col("ps_availqty") > F.col("half_qty"))
                           .select("ps_suppkey"))
    return (supplier.join(nation, F.col("s_nationkey") == F.col("n_nationkey"))
                     .where(F.col("n_name") == "CANADA")
                     .join(qualifying, F.col("s_suppkey") == F.col("ps_suppkey"), "leftsemi")
                     .select("s_name", "s_address")
                     .orderBy("s_name"))


def spark_q21(spark):
    lineitem, orders, supplier, nation = spark.table("lineitem"), spark.table("orders"), spark.table("supplier"), spark.table("nation")
    per_order = lineitem.groupBy("l_orderkey").agg(F.countDistinct("l_suppkey").alias("n_suppliers"))
    late = lineitem.where(F.col("l_receiptdate") > F.col("l_commitdate"))
    late_per_order = late.groupBy("l_orderkey").agg(F.countDistinct("l_suppkey").alias("n_late_suppliers"),
                                                     F.max("l_suppkey").alias("the_late_supplier"))
    exists_other_supplier = F.col("n_suppliers") > 1
    no_other_late_supplier = F.coalesce(F.col("n_late_suppliers"), F.lit(0)) == 0
    only_late_supplier_is_this_one = (F.col("n_late_suppliers") == 1) & (F.col("the_late_supplier") == F.col("l_suppkey"))
    return (late.join(orders, F.col("l_orderkey") == F.col("o_orderkey"))
                .where(F.col("o_orderstatus") == "F")
                .join(supplier, F.col("l_suppkey") == F.col("s_suppkey"))
                .join(nation, F.col("s_nationkey") == F.col("n_nationkey"))
                .where(F.col("n_name") == "SAUDI ARABIA")
                .join(per_order, "l_orderkey")
                .join(late_per_order, "l_orderkey")
                .where(exists_other_supplier & (no_other_late_supplier | only_late_supplier_is_this_one))
                .groupBy("s_name")
                .agg(F.count("*").alias("numwait"))
                .orderBy(F.desc("numwait"), "s_name")
                .limit(100))


def spark_q22(spark):
    customer, orders = spark.table("customer"), spark.table("orders")
    codes = ["13", "31", "23", "29", "30", "18", "17"]
    with_code = customer.withColumn("cntrycode", F.substring("c_phone", 1, 2))
    avg_bal = with_code.where((F.col("c_acctbal") > 0) & F.col("cntrycode").isin(*codes)).select(F.avg("c_acctbal")).collect()[0][0]
    return (with_code.where(F.col("cntrycode").isin(*codes) & (F.col("c_acctbal") > avg_bal))
                      .join(orders, F.col("c_custkey") == F.col("o_custkey"), "leftanti")
                      .groupBy("cntrycode")
                      .agg(F.count("*").alias("numcust"), F.sum("c_acctbal").alias("totacctbal"))
                      .orderBy("cntrycode"))


SPARK_QUERIES = {i: globals()[f"spark_q{i}"] for i in range(1, 23)}


# ---------------------------------------------------------------- driver

def run_query(con, spark, i, sql_text):
    out = {"query": i}
    ref, out["sql_s"] = best_of(lambda k: con.sql(f"{sql_text}\n-- run {k}").collect())
    for label, build, run in (("frames", FRAME_QUERIES.get(i), lambda f, k: fresh(f, k).collect()),
                               ("spark", SPARK_QUERIES.get(i), lambda f, k: fresh(f._f, k).collect())):
        if build is None:
            continue
        try:
            made = build(con if label == "frames" else spark)
            answer, secs = best_of(lambda k: run(made, k))
            ok, detail = compare(ref, answer, ORDERED[i])
            out[f"{label}_equal"], out[f"{label}_s"] = ok, secs
            if not ok:
                out[f"{label}_diff"] = detail
        except Exception as e:  # noqa: BLE001 (a bug in the query goes in the report, not a crash)
            out[f"{label}_equal"], out[f"{label}_error"] = False, f"{type(e).__name__}: {e}"
    print(json.dumps(out), flush=True)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--queries", default=",".join(map(str, range(1, 23))))
    ap.add_argument("--data", default=os.path.expanduser("~/tpch/sf1-bench"))
    ap.add_argument("--port", type=int, default=8741)
    A = ap.parse_args()
    qnums = [int(x) for x in A.queries.split(",")]

    lake = tempfile.mkdtemp(prefix="pondra-tpch-frames-")
    con = pondra.local(lake, port=A.port)
    try:
        load(con, A.data)
        spark = SparkSession(con)
        sql_texts = sql_query_texts(os.path.join(HERE, "tpch-queries"))
        results = [run_query(con, spark, i, sql_texts[i]) for i in qnums]
        summary = {"frames_equal": sum(r.get("frames_equal") is True for r in results),
                   "spark_equal": sum(r.get("spark_equal") is True for r in results),
                   "n": len(results),
                   "sql_s": round(sum(r["sql_s"] for r in results), 3),
                   "frames_s": round(sum(r.get("frames_s", 0) for r in results), 3),
                   "spark_s": round(sum(r.get("spark_s", 0) for r in results), 3)}
        print(json.dumps(summary), flush=True)
    finally:
        con.close()
        shutil.rmtree(lake, ignore_errors=True)


if __name__ == "__main__":
    main()
