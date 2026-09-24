#!/usr/bin/env python3
"""Hot keys in a shuffled join: the answers don't change, and the work is shared out.

  skew_check.py [--rows 2000000] [--nodes 3] [--s3]

Three nodes; a fact table whose key is 0 in half its rows (the rest spread over 100,000 keys), and
a dimension table with every key (0 three times). Both are sliced (`PONDRA_BROADCAST_MB=0`) and
DataFusion is told never to collect a side, so the joins are shuffled by key and one node gets
key 0. The same queries run twice: with hot partitions shared out (`PONDRA_SKEW_MB=1`) and
without (a threshold nothing reaches). What this proves:

- every answer equals one node's, both ways: an inner join, a LEFT JOIN keeping the fact rows, a
  semi join (IN) and an aggregation by the join key (whose partitions must not be shared out);
- with sharing, hot partitions were shared out (`pondra_skew_splits_total`), and the busiest node
  read much less than without (`pondra_shuffle_received_bytes_total`: busiest over average).
"""
import argparse, itertools, json, os, sys, time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import harness
from harness import Node, call
from shuffle_spill import metrics

RUNS = itertools.count()
QUERIES = {
    "inner join": "SELECT count(*) AS n, sum(f.v * d.w) AS s FROM f JOIN d ON f.k = d.k",
    "LEFT JOIN": "SELECT count(*) AS n, count(d.w) AS m FROM f LEFT JOIN d ON f.k = d.k AND d.w < 50",
    "IN": "SELECT count(*) AS n FROM f WHERE k IN (SELECT k FROM d WHERE w % 2 = 0)",
    "grouped by the key": "SELECT f.k, count(*) AS n FROM f JOIN d ON f.k = d.k GROUP BY f.k ORDER BY n DESC, f.k LIMIT 5",
}


def run(lake, skew_mb, first):
    scratch = os.path.join(harness.tempfile.gettempdir(), f"pondra-skew-{os.getpid()}")
    env = lambda i: {"PONDRA_SKEW_MB": str(skew_mb), "PONDRA_BROADCAST_MB": "0", "PONDRA_CACHE_DIR": f"{scratch}/{i}",
                     "PONDRA_SQL_OPTIONS": "datafusion.optimizer.hash_join_single_partition_threshold=0,datafusion.optimizer.hash_join_single_partition_threshold_rows=0"}
    nodes = [Node(lake, A.port + i, env=env(i), memory_gb=1.5, tier_secs=0).start() for i in range(A.nodes)]
    port = A.port
    deadline = time.time() + 90
    while len(call(port, "GET", "/stats").get("nodes", [])) < A.nodes and time.time() < deadline:
        time.sleep(0.5)
    if first:
        call(port, "POST", "/sql", b"CREATE TABLE f (id BIGINT, k BIGINT, v DOUBLE)")
        call(port, "POST", "/sql", b"CREATE TABLE d (k BIGINT, w BIGINT)")
        for i in range(0, A.rows, 500_000):
            call(port, "POST", "/sql", f"INSERT INTO f SELECT value + {i}, CASE WHEN value % 2 = 0 THEN 0 ELSE (value * 7919) % 100000 + 1 END, (value % 13) * 1.0 FROM generate_series(1, 500000)".encode(), timeout=3600)
        call(port, "POST", "/sql", b"INSERT INTO d SELECT value, value % 97 FROM generate_series(0, 100000)")
        call(port, "POST", "/sql", b"INSERT INTO d VALUES (0, 1), (0, 2)")
        call(port, "POST", "/tier", timeout=3600)
    before = [metrics(A.port + i) for i in range(A.nodes)]
    out = {}
    for name, sql in QUERIES.items():
        once = lambda: (sql + f" -- {next(RUNS)}").encode()
        one, many = call(port, "POST", "/sql?spread=0", once(), timeout=3600), call(port, "POST", "/sql?spread=1", once(), timeout=3600)
        out[name] = one == many if "ORDER BY" in sql else sorted(map(json.dumps, one)) == sorted(map(json.dumps, many))
    after = [metrics(A.port + i) for i in range(A.nodes)]
    got = lambda m: [a.get(f"pondra_{m}", 0) - b.get(f"pondra_{m}", 0) for a, b in zip(after, before)]
    read = got("shuffle_received_bytes_total")
    for n in nodes:
        n.kill()
    harness.subprocess.run(["rm", "-rf", scratch])
    return {"same": out, "splits": sum(got("skew_splits_total")), "read_mb": [round(r / 1e6, 1) for r in read],
            "busiest_over_average": round(max(read) * len(read) / max(sum(read), 1), 2)}


def main():
    lake = harness.new_lake()
    without = run(lake, 1 << 20, True)
    shared = run(lake, 1, False)
    checks = {
        "every answer equals one node's, without sharing": all(without["same"].values()),
        "every answer equals one node's, with hot partitions shared out": all(shared["same"].values()),
        "hot partitions were shared out": shared["splits"] > 0 and without["splits"] == 0,
        "the busiest node read much less": shared["busiest_over_average"] < without["busiest_over_average"] - 0.3,
    }
    result = {"rows": A.rows, "without": without, "shared_out": shared, "checks": checks, "ok": all(checks.values())}
    print(json.dumps(result, indent=1))
    sys.exit(0 if result["ok"] else 1)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=2_000_000)
    ap.add_argument("--nodes", type=int, default=3)
    ap.add_argument("--port", type=int, default=8170)
    ap.add_argument("--s3", action="store_true")
    ap.add_argument("--keep", action="store_true")
    A = ap.parse_args()
    harness.A = A
    main()
