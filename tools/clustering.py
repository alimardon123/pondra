#!/usr/bin/env python3
"""What `cluster_by` buys: the same rows in two append tables, one clustered by `user`, then the
same selective queries on both (a new query text each time, so no result cache).
  clustering.py [--rows 8000000] [--users 100000] [--s3]"""
import argparse, io, json, os, random, statistics, sys, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import harness
from harness import Node, call, sql

A = None


def main():
    import pyarrow as pa
    lake = harness.new_lake()
    node = Node(lake, A.port).start()
    cols = [["user", "Utf8"], ["amount", "Int64"], ["ts", "Int64"]]
    call(A.port, "POST", "/tables/plain", json.dumps(cols).encode())
    call(A.port, "POST", "/tables/sorted", json.dumps({"columns": cols, "cluster_by": ["user"]}).encode())
    rng, batch, t0 = random.Random(7), 500_000, time.time()
    for seq, start in enumerate(range(0, A.rows, batch), 1):
        n = min(batch, A.rows - start)
        users = [f"u{rng.randrange(A.users)}" for _ in range(n)]
        rb = pa.record_batch([pa.array(users), pa.array(range(start, start + n), pa.int64()), pa.array(range(start, start + n), pa.int64())], names=["user", "amount", "ts"])
        buf = io.BytesIO()
        with pa.ipc.new_stream(buf, rb.schema) as w:
            w.write_batch(rb)
        for t in ("plain", "sorted"):
            call(A.port, "POST", f"/append/{t}?producer=load-{t}&seq={seq}", buf.getvalue(), headers={"content-type": "application/vnd.apache.arrow.stream"}, timeout=600)
    for _ in range(4):  # fold everything, then let merges run
        call(A.port, "POST", "/tier", timeout=3600)
    load_s = time.time() - t0
    files = {t: json.loads(harness.subprocess.run([harness.BIN, "catalog", "--dir", lake, f"t/{t}"], capture_output=True, text=True).stdout.split(" ", 1)[1])["files"] for t in ("plain", "sorted")}
    out = {"rows": A.rows, "users": A.users, "load_and_tier_s": round(load_s, 1), "files": {t: len(f) for t, f in files.items()}}
    for name, q in [("one user", "SELECT count(*) AS n, sum(amount) AS s FROM {t} WHERE user = '{u}'"),
                    ("100 users (IN list)", "SELECT count(*) AS n FROM {t} WHERE user IN ({us})"),
                    ("a range of users", "SELECT count(*) AS n FROM {t} WHERE user BETWEEN '{u}' AND '{u}5'"),
                    ("full scan (unaffected)", "SELECT count(*) AS n, sum(amount) AS s FROM {t}")]:
        res = {}
        for t in ("plain", "sorted"):
            lat, rows = [], []
            for i in range(A.queries):
                u = f"u{rng.randrange(A.users)}"
                us = ",".join(f"'u{rng.randrange(A.users)}'" for _ in range(100))
                s = time.time()
                r = sql(A.port, q.format(t=t, u=u, us=us) + f" -- {i} {t}")
                lat.append(time.time() - s)
                rows.append(r[0]["n"])
            res[t] = round(1000 * statistics.median(lat), 1)
        res["speedup"] = round(res["plain"] / max(res["sorted"], 0.01), 1)
        out[f"{name} (median ms)"] = res
    same = sql(A.port, "SELECT (SELECT count(*) FROM plain) AS a, (SELECT count(*) FROM sorted) AS b")[0]
    out["same_rows"] = same["a"] == same["b"] == A.rows
    print(json.dumps(out, indent=1))
    node.kill()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=8_000_000)
    ap.add_argument("--users", type=int, default=100_000)
    ap.add_argument("--queries", type=int, default=15)
    ap.add_argument("--port", type=int, default=18900)
    ap.add_argument("--s3", action="store_true")
    A = harness.A = ap.parse_args()
    main()
