#!/usr/bin/env python3
"""The 22 TPC-H queries on Spark, over the Parquet in the bucket (run by `spark.sh submit`).

  spark.sh submit hosts.txt spark_tpch.py --data gs://bucket/tpch-sf100 --queries /tmp/tpch-queries.json --runs 3

Reads each table's folder (`<data>/<table>/*.parquet`, as tpch_parts.sh copied them) afresh for every
query, as tools/bench/tpch.py does, best of --runs; the last line printed is one JSON object, the
time and the row count of each query. `--queries` is scale.py's JSON of {number: SQL}, q15's view
already a CTE (driver.queries).
"""
import argparse, json, time

TABLES = ["region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem"]

ap = argparse.ArgumentParser()
ap.add_argument("--data", required=True)
ap.add_argument("--queries", required=True)
ap.add_argument("--runs", type=int, default=3)
A = ap.parse_args()

from pyspark.sql import SparkSession  # (spark-submit brings pyspark; nothing is pip-installed)

spark = SparkSession.builder.appName("tpch").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
for t in TABLES:
    spark.read.parquet(f"{A.data}/{t}").createOrReplaceTempView(t)
times, rows = {}, {}
for i, q in json.load(open(A.queries)).items():
    best = None
    for _ in range(A.runs):
        t = time.time()
        res = spark.sql(q).collect()
        best = min(best or 1e9, time.time() - t)
    times[i], rows[i] = round(best, 3), len(res)
    print(f"q{i} {times[i]}s {rows[i]} rows", flush=True)
print(json.dumps({"times": times, "rows": rows, "version": spark.version, "cores": spark.sparkContext.defaultParallelism}))
spark.stop()
