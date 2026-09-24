#!/usr/bin/env python3
"""TPC-H across nodes: every query answers on N nodes exactly as it does on one.

  spread_tpch.py --lake DIR [--queries ~/tpch/queries] [--nodes 3] [--broadcast-mb 64]
  spread_tpch.py --data ~/tpch/sf1-bench            # (builds a lake from the Parquet files first)

Starts N nodes on a lake holding the TPC-H tables and runs each of the 22 queries twice: on one
node (`?spread=0`) and across the nodes (`?spread=1`). For each it reports how it ran — shuffled,
gathered, or on one node after all, and if so why (`PONDRA_DEBUG_SPREAD`) — and whether the two
answers are the same. `--broadcast-mb 0` slices every table instead of reading the small ones
whole on every node, which is the harder case for the joins.

What this proves: every answer is the same, and how many of the 22 actually ran across the nodes.
The nodes share this machine's cores, so the timings say nothing about speed. (With the benchmark
copy's DOUBLE columns, q15's answer on one node changes from run to run — its `total_revenue =
max(total_revenue)` compares two sums added up in whatever order the threads finish. The cluster
adds up in node order and gives the same answer every time; it only has to be one of one node's.)
"""
import argparse, itertools, json, os, re, subprocess, sys, time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "bench"))
import harness
from harness import Node, call
from join_order import same

TABLES = ["region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem"]
RUNS = itertools.count()


def lake_of():
    """A lake holding the TPC-H tables (`--lake` to reuse one)."""
    if A.lake:
        return A.lake
    lake = harness.tempfile.mkdtemp(prefix="pondra-spreadtpch-")
    harness.LAKES.append(lake)
    for t in TABLES:
        subprocess.run([harness.BIN, "sql", "--dir", lake, f"INSERT INTO {t} SELECT * FROM '{os.path.join(A.data, t)}.parquet'"], check=True, capture_output=True)
    return lake


def counters(port):
    out = {}
    for line in call(port, "GET", "/metrics").decode().splitlines():
        if line.startswith("pondra_") and " " in line:
            name, v = line.rsplit(" ", 1)
            out[name] = float(v)
    return out


def why(node, since):
    """The last reason the node gave for running a query on its own, after byte `since` of its log."""
    text = open(node.log, errors="replace").read()[since:]
    found = re.findall(r"(?:spread: not spread: |distributed query failed, running it here: |shuffle: )(.*)", text)
    return " | ".join(dict.fromkeys(f[:200] for f in found))  # (every reason, first to last: the operator, then the verdict)


def main():
    from tpch import queries
    lake = lake_of()
    scratch = os.path.join(harness.tempfile.gettempdir(), f"pondra-spreadtpch-{os.getpid()}")
    env = lambda i: {"PONDRA_CACHE_DIR": f"{scratch}/{i}", "PONDRA_HOT_GB": "0", "PONDRA_DEBUG_SPREAD": "1", "PONDRA_BROADCAST_MB": str(A.broadcast_mb)}
    nodes = [Node(lake, A.port + i, env=env(i), memory_gb=A.memory_gb, tier_secs=0).start() for i in range(A.nodes)]
    port, out = A.port, {}
    deadline = time.time() + 90  # (a lake led a moment ago waits out its old leader's mark first)
    while len(call(port, "GET", "/stats").get("nodes", [])) < A.nodes and time.time() < deadline:
        time.sleep(0.5)
    for i, sql in queries(A.queries).items():
        once = lambda: (sql + f" -- {next(RUNS)}").encode()
        one = call(port, "POST", "/sql?spread=0", once(), timeout=3600)
        before, since = counters(port), os.path.getsize(nodes[0].log)
        t0 = time.time()
        try:
            spread, error = call(port, "POST", "/sql?spread=1", once(), timeout=3600), ""
        except Exception as e:
            spread, error = None, str(e)[:200]
        took = round(time.time() - t0, 2)
        after = counters(port)
        ran = lambda m: after.get(f"pondra_{m}_queries_total", 0) > before.get(f"pondra_{m}_queries_total", 0)
        how = ("ranged, " if ran("ranged") else "") + ("shuffled" if ran("shuffled") else "gathered" if ran("spread") else "one node")
        agree = spread is not None and same(one, spread)
        # A query whose answer turns on floating-point sums matching exactly (TPC-H q15 compares a
        # sum with the max of the same sums, over DOUBLE columns) can give different answers from
        # run to run on one node alone. Then the cluster's answer only has to be one of them.
        unstable = False
        for _ in range(0 if agree or spread is None else 6):
            again = call(port, "POST", "/sql?spread=0", once(), timeout=3600)
            unstable |= not same(one, again)
            if same(again, spread):
                agree = True
                break
        out[f"q{i}"] = {"how": how, "same": agree, "s": took, **({"one node varies": True} if unstable else {}),
                        **({"why": why(nodes[0], since)} if how == "one node" else {}), **({"error": error} if error else {})}
        print(f"q{i:<3} {how:<17} same={out[f'q{i}']['same']!s:<5} {took:>6.2f}s  {out[f'q{i}'].get('why', '')}", file=sys.stderr, flush=True)
    for n in nodes:
        n.kill()
    subprocess.run(["rm", "-rf", scratch])
    spread = [q for q, v in out.items() if v["how"] != "one node"]
    ranged = [q for q, v in out.items() if v["how"].startswith("ranged")]
    checks = {
        "every answer across the nodes equals one node's": all(v["same"] for v in out.values()),
        f"at least {A.expect} of the 22 ran across the nodes": len(spread) >= A.expect,
    }
    result = {"nodes": A.nodes, "broadcast_mb": A.broadcast_mb, "spread": len(spread), "shuffled": sum(v["how"].endswith("shuffled") for v in out.values()), "ranged": len(ranged),
              "queries": out, "checks": checks, "ok": all(checks.values())}
    print(json.dumps(result, indent=1))
    sys.exit(0 if result["ok"] else 1)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--lake", help="a lake that already holds the TPC-H tables")
    ap.add_argument("--data", default=os.path.expanduser("~/tpch/sf1-bench"), help="the Parquet files to load when there is no --lake")
    ap.add_argument("--queries", default=os.path.expanduser("~/tpch/queries"))
    ap.add_argument("--nodes", type=int, default=3)
    ap.add_argument("--broadcast-mb", type=int, default=64, help="PONDRA_BROADCAST_MB: tables under this are read whole by every node (0: slice them all)")
    ap.add_argument("--expect", type=int, default=0, help="fail unless at least this many queries ran across the nodes")
    ap.add_argument("--memory-gb", type=float, default=1.5)
    ap.add_argument("--port", type=int, default=8150)
    ap.add_argument("--s3", action="store_true")
    ap.add_argument("--keep", action="store_true")
    A = ap.parse_args()
    harness.A = A
    main()
