#!/usr/bin/env python3
"""What does a brand-new user see? A 3-node cluster (leader, follower, read-only node) holds a
table; a client that has never queried before asks the read-only node. Measures that first query,
the steady state, how soon a row written on another node is visible there, and the first queries
on a node that joins afterwards with an empty SSD tier (--cache-gb 0 turns the tier off).
  newuser_bench.py [--s3] [--cache-gb 20] [--rows 2000000]"""
import argparse, io, json, os, statistics, sys, tempfile, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import harness
from harness import Node, call, sql

A = None
QUERIES = {
    "count_sum": "SELECT count(*) AS n, sum(amount) AS total FROM events",
    "top_users": "SELECT user, sum(amount) AS total FROM events GROUP BY user ORDER BY total DESC LIMIT 10",
    "one_user": "SELECT count(*) AS n, max(amount) AS m FROM events WHERE user = 'u42'",
}


def timed(port, q):
    t = time.time()
    sql(port, q)
    return round((time.time() - t) * 1000)


def main():
    import pyarrow as pa, pyarrow.ipc
    lake = harness.new_lake()
    caches = [tempfile.mkdtemp(prefix=f"pondra-ssd-{i}-") for i in range(3)]
    flags = dict(tier_secs=2, cache_gb=A.cache_gb)
    nodes = [Node(lake, A.port + i, reader=(i == 2), cache_dir=caches[i], **flags).start() for i in range(3)]
    time.sleep(3)
    a, b, c = (nd.port for nd in nodes)
    call(a, "POST", "/tables/events", json.dumps([["user", "Utf8"], ["amount", "Int64"]]).encode())
    per = 100_000
    for seq, lo in enumerate(range(0, A.rows, per), 1):
        ids = range(lo, min(lo + per, A.rows))
        t = pa.table({"user": [f"u{i % 1000}" for i in ids], "amount": pa.array(list(ids), pa.int64())})
        buf = io.BytesIO()
        with pa.ipc.new_stream(buf, t.schema) as w:
            w.write_table(t)
        call(a, "POST", f"/append/events?producer=load&seq={seq}", buf.getvalue(), headers={"content-type": "application/vnd.apache.arrow.stream"}, timeout=600)
    call(a, "POST", "/tier", timeout=1200)
    time.sleep(8)  # let the other nodes see it (and, with the SSD tier, prefetch it)
    out = {"lake": "s3" if A.s3 else "local", "cache_gb": A.cache_gb, "rows": A.rows}
    for name, q in QUERIES.items():
        first = timed(c, q)
        rest = [timed(c, q) for _ in range(9)]
        out[name] = {"first_ms": first, "p50_ms": statistics.median(rest)}
    # freshness: write on the follower, read on the read-only node
    lags = []
    for k in range(10):
        n0 = sql(c, "SELECT count(*) AS n FROM events")[0]["n"]
        t = time.time()
        call(b, "POST", f"/append/events?producer=probe&seq={k + 1}", json.dumps({"user": "probe", "amount": 1}).encode())
        while sql(c, "SELECT count(*) AS n FROM events")[0]["n"] <= n0:
            time.sleep(0.01)
        lags.append(round((time.time() - t) * 1000))
    out["write_on_B_visible_on_C_ms"] = {"p50": statistics.median(lags), "max": max(lags)}
    # A brand-new node (empty SSD tier) joins now: its very first queries, then after it warmed up.
    caches.append(tempfile.mkdtemp(prefix="pondra-ssd-new-"))
    nodes.append(Node(lake, A.port + 3, reader=True, cache_dir=caches[-1], **flags).start())
    out["new_node_first_ms"] = {name: timed(nodes[-1].port, q) for name, q in QUERIES.items()}
    time.sleep(5)
    out["new_node_after_5s_ms"] = {name: timed(nodes[-1].port, q) for name, q in QUERIES.items()}
    out["ssd_tier_bytes_per_node"] = [sum(os.path.getsize(os.path.join(r, f)) for r, _, fs in os.walk(d) for f in fs) for d in caches]
    print(json.dumps(out))
    [nd.kill() for nd in nodes]


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=2_000_000)
    ap.add_argument("--cache-gb", type=int, default=20)
    ap.add_argument("--port", type=int, default=18700)
    ap.add_argument("--s3", action="store_true")
    A = harness.A = ap.parse_args()
    main()
