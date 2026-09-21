#!/usr/bin/env python3
"""Serving benchmark: how fast can Pondra answer point lookups and small dashboard queries,
and how many per second, while new rows keep arriving?
  serve_bench.py [--keys 2000000] [--nodes 1] [--secs 5] [--threads 1,8,32]
Prints one JSON line per measurement."""
import argparse, io, json, os, random, shutil, statistics, subprocess, sys, tempfile, threading, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import harness
from harness import Node, call, sql

A = None


def loadgen():
    """The Go load generator (tools/loadgen.go), built once if Go is installed; else None."""
    exe = os.path.join(tempfile.gettempdir(), "pondra-loadgen")
    if not os.path.exists(exe) and shutil.which("go"):
        subprocess.run(["go", "build", "-o", exe, os.path.join(os.path.dirname(os.path.abspath(__file__)), "loadgen.go")], check=True)
    return exe if os.path.exists(exe) else None


def measure(port, make_query, threads, secs, path="/sql"):
    """Run queries from `threads` clients for `secs`; return p50/p99/max ms and queries per second.
    `make_query` returns either SQL, or ("GET", path) for the /lookup fast path. With Go available
    the clients are tools/loadgen.go (the Python client tops out near 2k requests/s); then a
    `{k}` in the query becomes a random key."""
    exe = loadgen()
    if exe:
        q = make_query()
        args = ["-url", f"http://127.0.0.1:{port}{q[1]}"] if isinstance(q, tuple) else ["-url", f"http://127.0.0.1:{port}{path}", "-body", q]
        out = subprocess.run([exe, *args, "-keys", str(A.keys), "-c", str(threads), "-secs", str(secs)], capture_output=True, text=True, check=True)
        r = json.loads(out.stdout)
        return {"threads": threads, "queries": r["requests"], "qps": r["qps"], "errors": r["errors"], "p50_ms": round(r["p50_ms"], 2), "p99_ms": round(r["p99_ms"], 2), "max_ms": round(r["max_ms"], 1)}
    stop, lat, errs = threading.Event(), [], [0]

    def run():
        mine = []
        while not stop.is_set():
            q = make_query()
            t = time.time()
            try:
                call(port, "GET", q[1]) if isinstance(q, tuple) else call(port, "POST", path, q.encode())
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
    nodes = [Node(lake, A.port + i, reader=(i > 0)).start() for i in range(A.nodes)]
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

    go = loadgen() is not None  # ({k}: a random key per request, drawn by the Go client)
    key = lambda: "{k}" if go else str(random.randrange(A.keys))
    point = lambda: f"SELECT id, name, amount FROM kv WHERE id = {key()}"
    fast = lambda: ("GET", f"/lookup/kv/{key()}")
    dash = lambda: "SELECT count(*) AS n, sum(amount) AS total FROM kv WHERE amount > 900"
    fresh_dash = lambda: f"SELECT count(*) AS n, sum(amount) AS total FROM kv WHERE amount > ({key()} % 1000)"  # a new query each time
    for threads in [int(x) for x in A.threads.split(",")]:
        for name, q in (("point_lookup_sql", point), ("point_lookup", fast)):
            r = measure(nodes[-1].port, q, threads, A.secs)
            print(json.dumps({**out, "query": name, **r}), flush=True)
    for name, q, threads in (("dashboard_agg_uncached", fresh_dash, 1), ("dashboard_agg_uncached", fresh_dash, 8), ("dashboard_agg_repeated", dash, 32)):
        r = measure(nodes[-1].port, q, threads, A.secs)
        print(json.dumps({**out, "query": name, **r}), flush=True)

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
    r = measure(nodes[-1].port, dash, 32, A.secs)
    print(json.dumps({**out, "query": "dashboard_agg_repeated_while_writing", **r}), flush=True)
    r = measure(nodes[-1].port, dash, 32, A.secs, path="/sql?stale_ms=1000")
    print(json.dumps({**out, "query": "dashboard_agg_repeated_while_writing_stale_1s", **r}), flush=True)
    stop.set()
    [nd.kill() for nd in nodes]


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--keys", type=int, default=2_000_000)
    ap.add_argument("--nodes", type=int, default=1)
    ap.add_argument("--secs", type=float, default=5)
    ap.add_argument("--threads", default="1,8,32,64")
    ap.add_argument("--port", type=int, default=18300)
    ap.add_argument("--s3", action="store_true")
    A = harness.A = ap.parse_args()
    main()
