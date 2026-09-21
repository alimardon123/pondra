#!/usr/bin/env python3
"""TPC-H: the 22 queries on the same Parquet data in Pondra, DuckDB and Spark (best of 2 runs each,
results compared by row count). Data: `tpchgen-cli -s 1 --format=parquet --output-dir sf1`; queries:
the DataFusion benchmark's q1..q22.sql (q15's view written as a CTE).
  tpch.py --data sf1 --queries queries [--engines pondra,duckdb,spark] [--spark-python venv/bin/python]"""
import argparse, io, json, os, re, subprocess, sys, tempfile, time
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import harness
from harness import Node, call

TABLES = ["region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem"]


def queries(folder):
    out = {}
    for i in range(1, 23):
        q = open(os.path.join(folder, f"q{i}.sql")).read().strip().rstrip(";")
        if i == 15:  # a view + a query -> one statement
            m = re.match(r"create view revenue0 \(supplier_no, total_revenue\) as\s*select\s*l_suppkey,\s*(sum\(.*?\))\s*from(.*?);\s*(select.*)", q, re.S | re.I)
            q = f"with revenue0 as (select l_suppkey as supplier_no, {m.group(1)} as total_revenue from{m.group(2)}) {m.group(3)}"
            q = re.split(r";\s*drop view", q, flags=re.I)[0]
        out[i] = q
    return out


def pondra(data, qs):
    import pyarrow as pa, pyarrow.ipc, pyarrow.parquet as pq
    lake = tempfile.mkdtemp(prefix="pondra-tpch-")
    node = Node(lake, A.port).start()
    t0 = time.time()
    for t in TABLES:
        table = pq.read_table(os.path.join(data, f"{t}.parquet"))
        cols = [[f.name, str(f.type).replace("decimal128", "Decimal128").replace("date32[day]", "Date32").replace("int64", "Int64")
                 .replace("int32", "Int32").replace("string", "Utf8")] for f in table.schema]
        call(A.port, "POST", f"/tables/{t}", json.dumps(cols).encode())
        for seq, batch in enumerate(table.to_batches(max_chunksize=500_000), 1):
            buf = io.BytesIO()
            with pa.ipc.new_stream(buf, batch.schema) as w:
                w.write_batch(batch)
            call(A.port, "POST", f"/append/{t}?producer=load-{t}&seq={seq}", buf.getvalue(), headers={"content-type": "application/vnd.apache.arrow.stream"}, timeout=3600)
    call(A.port, "POST", "/tier", timeout=3600)
    load = time.time() - t0
    times, rows = {}, {}
    for i, q in qs.items():
        best = None
        for run in range(2):  # (a comment makes each run a new query: no result cache)
            t = time.time()
            res = call(A.port, "POST", "/sql", f"{q} -- run {run}".encode(), timeout=3600)
            best = min(best or 1e9, time.time() - t)
        times[i], rows[i] = round(best, 3), len(res)
    if not A.keep:
        node.kill()
    return {"load_s": round(load, 1), "times": times, "rows": rows}


def duckdb(data, qs):
    import duckdb
    con = duckdb.connect()
    for t in TABLES:
        con.execute(f"CREATE VIEW {t} AS SELECT * FROM read_parquet('{os.path.join(data, t)}.parquet')")
    times, rows = {}, {}
    for i, q in qs.items():
        best = None
        for _ in range(2):
            t = time.time()
            res = con.execute(q).fetchall()
            best = min(best or 1e9, time.time() - t)
        times[i], rows[i] = round(best, 3), len(res)
    return {"times": times, "rows": rows}


SPARK = """
import json, sys, time
from pyspark.sql import SparkSession
data, qs = sys.argv[1], json.load(open(sys.argv[2]))
spark = SparkSession.builder.master("local[*]").config("spark.driver.memory", "4g").config("spark.ui.enabled", "false").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
for t in %r:
    spark.read.parquet(f"{data}/{t}.parquet").createOrReplaceTempView(t)
times, rows = {}, {}
for i, q in qs.items():
    best = None
    for _ in range(2):
        t = time.time(); res = spark.sql(q).collect(); best = min(best or 1e9, time.time() - t)
    times[i], rows[i] = round(best, 3), len(res)
print(json.dumps({"times": times, "rows": rows}))
""" % (TABLES,)


def spark(data, qs):
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
        json.dump(qs, f)
    with tempfile.NamedTemporaryFile("w", suffix=".py", delete=False) as s:
        s.write(SPARK)
    out = subprocess.run([A.spark_python, s.name, os.path.abspath(data), f.name], capture_output=True, text=True, timeout=7200)
    if not out.stdout.strip():
        raise RuntimeError("spark failed: " + out.stderr[-2000:])
    r = json.loads(out.stdout.strip().splitlines()[-1])
    return {"times": {int(k): v for k, v in r["times"].items()}, "rows": {int(k): v for k, v in r["rows"].items()}}


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default="sf1")
    ap.add_argument("--queries", default="queries")
    ap.add_argument("--engines", default="pondra,duckdb,spark")
    ap.add_argument("--spark-python", default="python3")
    ap.add_argument("--port", type=int, default=18450)
    ap.add_argument("--keep", action="store_true", help="leave the Pondra node running")
    A = harness.A = ap.parse_args()
    qs = queries(A.queries)
    results = {}
    for e in A.engines.split(","):
        results[e] = {"pondra": pondra, "duckdb": duckdb, "spark": spark}[e](A.data, qs)
        print(json.dumps({e: results[e]}), flush=True)
    ref = next(iter(results.values()))["rows"]
    agree = {e: all(r["rows"][i] == ref[i] for i in qs) for e, r in results.items()}
    total = {e: round(sum(r["times"].values()), 2) for e, r in results.items()}
    print(json.dumps({"total_s": total, "same_row_counts": agree}))
