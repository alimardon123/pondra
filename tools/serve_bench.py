#!/usr/bin/env python3
"""Serving benchmark: how fast can Pondra answer point lookups and small dashboard queries,
and how many per second, while new rows keep arriving?
  serve_bench.py [--keys 2000000] [--nodes 1] [--secs 5] [--threads 1,8,32]
Prints one JSON line per measurement."""
import argparse, io, json, os, random, statistics, sys, threading, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import harness
from harness import Node, call, sql

A = None


def measure(port, make_query, threads, secs):
    """Run queries from `threads` clients for `secs`; return p50/p99/max ms and queries per second.
    `make_query` returns either SQL, or ("GET", path) for the /lookup fast path."""
    stop, lat, errs = threading.Event(), [], [0]

    def run():
        mine = []
        while not stop.is_set():
            q = make_query()
            t = time.time()
            try:
                call(port, "GET", q[1]) if isinstance(q, tuple) else sql(port, q)
            except Exception:
                errs[0] += 1
            mine.append((time.time() - t) * 1000)
        lat.extend(mine)

    ts = [threading.Thread(target=run, daemon=True) for _ in range(threads)]
    [t.start() for t in ts]
    time.sleep(secs)
    stop.set()
    [t.join(10) for t in ts]
    return {"threads": threads, "queries": len(lat), "qps": round(len(lat) / secs), "errors": errs[0],
            "p50_ms": round(statistics.median(lat), 1), "p99_ms": round(sorted(lat)[int(len(lat) * .99)], 1), "max_ms": round(max(lat), 1)}


def main():
    lake = harness.new_lake()
    nodes = [Node(lake, A.port + i, reader=(i > 0), tier_secs=10).start() for i in range(A.nodes)]
    p = nodes[0].port
    call(p, "POST", "/tables/kv", json.dumps({"columns": [["id", "Int64"], ["name", "Utf8"], ["amount", "Int64"], ["ts", "Int64"]], "key": ["id"]}).encode())
    import pyarrow as pa, pyarrow.ipc
    t, batch = time.time(), 250_000
    for seq, lo in enumerate(range(0, A.keys, batch), 1):  # keyed table: rows go through the log
        ids = list(range(lo, min(lo + batch, A.keys)))
        tbl = pa.table({"id": pa.array(ids, pa.int64()), "name": pa.array([f"user{i}" for i in ids]),
                        "amount": pa.array([i % 1000 for i in ids], pa.int64()),
                        "ts": pa.array([1760000000000 + i for i in ids], pa.int64())})
        buf = io.BytesIO()
        with pa.ipc.new_stream(buf, tbl.schema) as w:
            w.write_table(tbl)
        call(p, "POST", f"/append/kv?producer=load&seq={seq}", buf.getvalue(),
             headers={"content-type": "application/vnd.apache.arrow.stream"}, timeout=3600)
    call(p, "POST", "/tier", timeout=3600)
    load_s = round(time.time() - t, 1)
    files = call(p, "GET", "/stats")
    out = {"keys": A.keys, "nodes": A.nodes, "load_s": load_s}
    print(json.dumps({**out, "stats": {k: files[k] for k in ("role", "hwm") if k in files}}), flush=True)

    point = lambda: f"SELECT id, name, amount FROM kv WHERE id = {random.randrange(A.keys)}"
    fast = lambda: ("GET", f"/lookup/kv/{random.randrange(A.keys)}")
    dash = lambda: "SELECT count(*) AS n, sum(amount) AS total FROM kv WHERE amount > 900"
    for threads in [int(x) for x in A.threads.split(",")]:
        for name, q in (("point_lookup_sql", point), ("point_lookup", fast)):
            r = measure(nodes[-1].port, q, threads, A.secs)
            print(json.dumps({**out, "query": name, **r}), flush=True)
    r = measure(nodes[-1].port, dash, 1, A.secs)
    print(json.dumps({**out, "query": "dashboard_agg", **r}), flush=True)

    # …and the same lookups while writes keep landing (the log tail is never empty)
    stop = threading.Event()

    def writer():
        seq = 0
        while not stop.is_set():
            seq += 1
            rows = "".join(json.dumps({"id": random.randrange(A.keys), "name": "x", "amount": 1, "ts": 0}) + "\n" for _ in range(200))
            try:
                call(p, "POST", f"/append/kv?producer=w&seq={seq}", rows.encode())
            except Exception:
                pass

    w = threading.Thread(target=writer, daemon=True)
    w.start()
    time.sleep(2)
    r = measure(nodes[-1].port, fast, 8, A.secs)
    print(json.dumps({**out, "query": "point_lookup_while_writing", **r}), flush=True)
    stop.set()
    [nd.kill() for nd in nodes]


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--keys", type=int, default=2_000_000)
    ap.add_argument("--nodes", type=int, default=1)
    ap.add_argument("--secs", type=float, default=5)
    ap.add_argument("--threads", default="1,8,32")
    ap.add_argument("--port", type=int, default=18300)
    ap.add_argument("--s3", action="store_true")
    A = harness.A = ap.parse_args()
    main()
