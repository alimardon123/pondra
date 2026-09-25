#!/usr/bin/env python3
"""One TPC-H query run many times on one node, every answer checked against DuckDB's.

  repeat.py --data ~/tpch/sf1-bench --query 15 --runs 20 [--hot]

A query whose answer changes from run to run shows up as wrong runs. TPC-H q15 did, on DOUBLE
money: it compares each supplier's revenue with the max of the same revenues, both sums added up
in a different order each run, so the top supplier matched itself only some of the time. The
lake is loaded as `singlenode.py` loads it (one INSERT per table); `--hot` holds its columns in
memory, as singlenode's `pondra` does, and without it every run reads the Parquet files.
"""
import argparse, json, os, shutil, sys, time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path[:0] = [HERE, os.path.join(HERE, "..")]
import duckdb, harness, singlenode
from tpch import queries

ap = argparse.ArgumentParser()
ap.add_argument("--data", required=True)
ap.add_argument("--queries", default=os.path.expanduser("~/tpch/queries"))
ap.add_argument("--query", type=int, default=15)
ap.add_argument("--runs", type=int, default=20)
ap.add_argument("--hot", action="store_true")
ap.add_argument("--port", type=int, default=8150)
A = singlenode.A = ap.parse_args()
A.hot_gb, A.flag = 3, []

sql = queries(A.queries)[A.query]
con = duckdb.connect()
for t in singlenode.TABLES:
    con.execute(f"CREATE VIEW {t} AS SELECT * FROM '{os.path.join(A.data, t)}.parquet'")
want = [tuple(r) for r in con.execute(sql).fetchall()]

lake, _ = singlenode.pondra_lake()
node, _ = singlenode.pondra_up(lake, A.hot, {A.query: sql})
answers, times = [], []
for run in range(A.runs):
    t = time.time()
    rows = harness.call(A.port, "POST", "/sql", (sql + f" -- run {run}").encode(), timeout=3600)  # (a new text: the result cache never answers)
    times.append(time.time() - t)
    answers.append([tuple(r.values()) for r in rows])
node.kill()
shutil.rmtree(lake, ignore_errors=True)
wrong = sum(not singlenode.same(a, want) for a in answers)
print(json.dumps({"query": A.query, "runs": A.runs, "hot": A.hot, "wrong": wrong, "rows": len(want),
                  "best_s": round(min(times), 3), "ok": wrong == 0}))
sys.exit(0 if wrong == 0 else 1)
