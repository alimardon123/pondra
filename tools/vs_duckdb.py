#!/usr/bin/env python3
"""Same queries on the same Parquet files: Pondra (DataFusion, via its SQL endpoint) vs DuckDB.
Usage: vs_duckdb.py <lake url or dir> <pondra port>   (s3:// lakes use AWS_* env vars, e.g. the simulator)"""
import os
import http.client, json, statistics, sys, time
import duckdb

DIR, PORT = sys.argv[1], int(sys.argv[2])
QUERIES = {
    "count": "SELECT count(*) AS n FROM events",
    "group_by": "SELECT producer, count(*) AS n, sum(i) AS s, avg(ts) AS a FROM events GROUP BY producer ORDER BY producer",
    "filter_distinct": "SELECT count(DISTINCT seq) AS d FROM events WHERE i < 100",
    "top_k": "SELECT producer, seq, max(i) AS m FROM events GROUP BY producer, seq ORDER BY m DESC, producer, seq LIMIT 5",
}

def pondra(q):
    c = http.client.HTTPConnection("127.0.0.1", PORT, timeout=120)
    c.request("POST", "/sql", q.encode())
    return json.loads(c.getresponse().read())

con = duckdb.connect()
if DIR.startswith("s3://"):
    con.sql("LOAD httpfs")
    ep = os.environ["AWS_ENDPOINT"].split("://")
    con.sql(f"""CREATE SECRET (TYPE s3, KEY_ID '{os.environ["AWS_ACCESS_KEY_ID"]}', SECRET '{os.environ["AWS_SECRET_ACCESS_KEY"]}',
                REGION '{os.environ["AWS_REGION"]}', ENDPOINT '{ep[1]}', URL_STYLE 'path', USE_SSL {str(ep[0] == "https").lower()})""")
if os.environ.get("DUCK_CACHE"):  # give DuckDB its metadata cache too, for a fair warm comparison
    con.sql("SET enable_http_metadata_cache = true")
con.sql(f"CREATE VIEW events AS SELECT * FROM read_parquet('{DIR}/data/events/*.parquet')")
def duck(q):
    cur = con.execute(q)  # execute once, then read column names + rows from the same cursor
    return [dict(zip([d[0] for d in cur.description], r)) for r in cur.fetchall()]

def timed(f, q, n=5):
    f(q)  # warm-up
    ts = []
    for _ in range(n):
        t = time.perf_counter(); f(q); ts.append(time.perf_counter() - t)
    return statistics.median(ts) * 1000

print(f"{'query':16} {'pondra ms':>10} {'duckdb ms':>10} {'ratio':>6}  same result")
for name, q in QUERIES.items():
    a, b = pondra(q), duck(q)
    same = json.dumps(a, sort_keys=True, default=str) == json.dumps(json.loads(json.dumps(b, default=str)), sort_keys=True, default=str)
    if not same:  # compare numerically (float formatting differs between engines)
        same = all(abs(float(x[k]) - float(y[k])) < 1e-6 * max(1, abs(float(y[k]))) if isinstance(y[k], (int, float)) else x[k] == y[k]
                   for x, y in zip(a, b) for k in y) and len(a) == len(b)
    tl, td = timed(pondra, q), timed(duck, q)
    print(f"{name:16} {tl:8.0f} {td:10.0f} {tl / td:6.2f}  {same}")
