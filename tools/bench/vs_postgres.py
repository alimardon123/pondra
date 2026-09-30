#!/usr/bin/env python3
"""Pondra against Postgres, one client, local disk: point lookups by key (Postgres; Pondra through its
Postgres port and through GET /lookup), single-row inserts (each its own commit), loading TPC-H SF1's
lineitem (6M rows) and its Q1 and Q6. Needs a Postgres running with trust auth on port 5499
(user postgres), the lineitem Parquet file (tools/bench/tpch.py makes it) and psycopg.

  vs_postgres.py [--bin target/release/pondra] [--lineitem /path/lineitem.parquet] [--work /tmp/vs-postgres]"""
import json, os, random, statistics, subprocess, time, http.client
import argparse, psycopg, pyarrow.parquet as pq, pyarrow.csv as pc
HERE = os.path.dirname(os.path.abspath(__file__))
ap = argparse.ArgumentParser()
ap.add_argument("--bin", default=os.path.join(HERE, "..", "..", "target", "release", "pondra"))
ap.add_argument("--lineitem", default=os.path.expanduser("~/tpch/sf1-raw/lineitem.parquet"))
ap.add_argument("--work", default="/tmp/vs-postgres")
A = ap.parse_args()
os.makedirs(A.work, exist_ok=True)
OWNER = "benchmark-owner-key-0123456789"
PG = "host=127.0.0.1 port=5499 user=postgres dbname=postgres"
PD = "host=127.0.0.1 port=5498 user=u dbname=lake"
LINEITEM = A.lineitem
Q = {n: open(os.path.join(HERE, "tpch-queries", f"{n}.sql")).read().strip().rstrip(";") for n in ("q1", "q6")}
lake = os.path.join(A.work, "lake")
subprocess.run(["rm", "-rf", lake])
node = subprocess.Popen([A.bin, "serve", lake, "--addr", "127.0.0.1:8820", "--pg", "127.0.0.1:5498"],
                        env={**os.environ, "PONDRA_OWNER_KEY": OWNER}, stdout=subprocess.DEVNULL, stderr=open(os.path.join(A.work, "node.log"), "w"))
time.sleep(3)
def http_sql(s):
    c = http.client.HTTPConnection("127.0.0.1", 8820, timeout=3600); c.request("POST", "/sql", s.encode(), {"x-pondra-owner": OWNER}); r = c.getresponse(); d = r.read()
    if r.status != 200: raise RuntimeError(d[:500])
    return d
def lat(f, n):
    ts = []
    for i in range(n):
        t0 = time.perf_counter(); f(i); ts.append((time.perf_counter() - t0) * 1000)
    ts.sort()
    return {"p50_ms": round(statistics.median(ts), 3), "p99_ms": round(ts[int(len(ts) * 0.99)], 3)}
out = {}
import atexit; atexit.register(lambda: print(json.dumps(out, indent=1)))
N = 1_000_000
# --- point lookups
with psycopg.connect(PG, autocommit=True) as c:
    c.execute("DROP TABLE IF EXISTS kv, ins, lineitem")
    c.execute("CREATE TABLE kv (k BIGINT PRIMARY KEY, v TEXT)")
    c.execute(f"INSERT INTO kv SELECT g, 'value ' || g FROM generate_series(1, {N}) g")
    c.execute("VACUUM ANALYZE kv")
http_sql(f"CREATE TABLE kv (k BIGINT PRIMARY KEY, v VARCHAR)")
http_sql(f"INSERT INTO kv SELECT value, 'value ' || value FROM generate_series(1, {N})")
http_sql("CHECKPOINT")
random.seed(1)
keys = [random.randint(1, N) for _ in range(3000)]
with psycopg.connect(PG, autocommit=True) as c:
    out["lookup: Postgres (psycopg)"] = lat(lambda i: c.execute("SELECT v FROM kv WHERE k = %s", (keys[i],)).fetchone(), 3000)
with psycopg.connect(PD, autocommit=True) as c:
    out["lookup: Pondra, its Postgres port (psycopg)"] = lat(lambda i: c.execute("SELECT v FROM kv WHERE k = %s", (keys[i],)).fetchone(), 3000)
h = http.client.HTTPConnection("127.0.0.1", 8820, timeout=60)
def lookup(i):
    h.request("GET", f"/lookup/kv/{keys[i]}"); h.getresponse().read()
out["lookup: Pondra, GET /lookup (HTTP keep-alive)"] = lat(lookup, 3000)
# --- single-row inserts, each its own commit
with psycopg.connect(PG, autocommit=True) as c:
    c.execute("CREATE TABLE ins (k BIGINT PRIMARY KEY, v TEXT)")
    out["insert one row: Postgres (fsync on)"] = lat(lambda i: c.execute("INSERT INTO ins VALUES (%s, %s)", (i, "x")), 1000)
http_sql("CREATE TABLE ins (k BIGINT PRIMARY KEY, v VARCHAR)")
http_sql("CREATE TABLE app (k BIGINT, v VARCHAR)")
with psycopg.connect(PD, autocommit=True) as c:
    out["insert one row: Pondra keyed table (Postgres port)"] = lat(lambda i: c.execute("INSERT INTO ins VALUES (%s, %s)", (i, "x")), 1000)
    out["insert one row: Pondra append table (Postgres port)"] = lat(lambda i: c.execute("INSERT INTO app VALUES (%s, %s)", (i, "x")), 1000)
# --- analytics: TPC-H SF1 lineitem
t0 = time.time()
pc.write_csv(pq.read_table(LINEITEM), os.path.join(A.work, "lineitem.csv"), pc.WriteOptions(include_header=False))
with psycopg.connect(PG, autocommit=True) as c:
    c.execute("""CREATE TABLE lineitem (l_orderkey BIGINT, l_partkey BIGINT, l_suppkey BIGINT, l_linenumber INT, l_quantity NUMERIC(15,2), l_extendedprice NUMERIC(15,2),
                 l_discount NUMERIC(15,2), l_tax NUMERIC(15,2), l_returnflag TEXT, l_linestatus TEXT, l_shipdate DATE, l_commitdate DATE, l_receiptdate DATE,
                 l_shipinstruct TEXT, l_shipmode TEXT, l_comment TEXT)""")
    with c.cursor().copy("COPY lineitem FROM STDIN (FORMAT csv)") as cp, open(os.path.join(A.work, "lineitem.csv"), "rb") as f:
        while chunk := f.read(1 << 20):
            cp.write(chunk)
    c.execute("VACUUM ANALYZE lineitem")
pg_load = time.time() - t0
t0 = time.time()
http_sql(f"CREATE TABLE lineitem AS SELECT * FROM '{LINEITEM}'")
http_sql("CHECKPOINT")
pd_load = time.time() - t0
out["load lineitem (6M rows)"] = {"Postgres (CSV COPY, then VACUUM ANALYZE), s": round(pg_load, 1), "Pondra (CREATE TABLE AS from the Parquet file), s": round(pd_load, 1)}
def best(f):
    ts = []
    for _ in range(3):
        t0 = time.time(); f(); ts.append(time.time() - t0)
    return round(min(ts), 3)
for n, q in Q.items():
    with psycopg.connect(PG, autocommit=True) as c:
        pg_s = best(lambda: c.execute(q).fetchall())
    with psycopg.connect(PD, autocommit=True) as c:
        pd_s = best(lambda: c.execute(q).fetchall())
    out[f"TPC-H {n.upper()} on SF1 lineitem, best of 3 (s)"] = {"Postgres": pg_s, "Pondra": pd_s}
node.terminate(); node.wait()
