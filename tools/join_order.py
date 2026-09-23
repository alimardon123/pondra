#!/usr/bin/env python3
"""The same query written badly runs as fast as the one written well.

  join_order.py [--data ~/tpch/sf1-bench] [--lake DIR] [--runs 3]

A query names its tables in some order; most engines join them in that order. TPC-H's queries
name theirs well, so the order they ask for is already a good one. This writes each of them
badly — the same query with the `FROM` list reversed, biggest table first — and checks that:

- every reversed query gives the same answer as the one it came from;
- with Pondra's `join_order` rule on, the reversed queries cost about what the originals do;
- with it off (`PONDRA_JOIN_ORDER=0`), at least one of them costs far more, which is the evidence
  that the rule is what closed the gap and not the machine being kind.
"""
import argparse, json, os, re, subprocess, sys, time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import harness
from harness import Node, call

TABLES = ["region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem"]
FROM = re.compile(r"(\n\s*from\s*\n)((?:\s*\w+\s*,\s*\n)+\s*\w+\s*\n)(\s*where\b)", re.I)

# Queries of the shape people write every day — a big table joined to dimensions, one of them
# filtered hard — written both ways round. The second of each pair names the biggest table first,
# which is the order most engines then join in.
SHAPES = {
    "star": ("""select sum(l_extendedprice * (1 - l_discount)) as revenue from {}
                where r_name = 'ASIA' and n_regionkey = r_regionkey and c_nationkey = n_nationkey
                  and o_custkey = c_custkey and l_orderkey = o_orderkey""",
             "region, nation, customer, orders, lineitem"),
    "snowflake": ("""select sum(l_quantity) as items from {}
                     where r_name = 'EUROPE' and n_regionkey = r_regionkey and s_nationkey = n_nationkey
                       and l_suppkey = s_suppkey and o_orderkey = l_orderkey and o_orderstatus = 'F'""",
                  "region, nation, supplier, lineitem, orders"),
    "filtered dimension": ("""select count(*) as n from {}
                              where p_size = 15 and p_container = 'JUMBO BAG' and l_partkey = p_partkey
                                and o_orderkey = l_orderkey and o_orderdate >= date '1995-01-01'""",
                           "part, lineitem, orders"),
}


def shapes():
    """Each shape written well and written badly (the same query, tables named the other way)."""
    out = {}
    for name, (sql, order) in SHAPES.items():
        names = [t.strip() for t in order.split(",")]
        out[name] = (sql.format(", ".join(names)), sql.format(", ".join(reversed(names))))
    return out


def same(a, b):
    """The same answer, allowing for sums adding up in a different order."""
    if type(a) is not type(b):
        return False
    if isinstance(a, float) or isinstance(b, float):
        return abs(a - b) <= 1e-9 * max(abs(a), abs(b), 1.0)
    if isinstance(a, list):
        return len(a) == len(b) and all(same(x, y) for x, y in zip(a, b))
    if isinstance(a, dict):
        return a.keys() == b.keys() and all(same(a[k], b[k]) for k in a)
    return a == b


def reversed_from(sql):
    """The same query with its FROM list reversed (None if it doesn't have a plain one)."""
    m = FROM.search(sql)
    if not m:
        return None
    names = [n.strip() for n in m.group(2).split(",")]
    if len(names) < 3 or not all(n in TABLES for n in names):
        return None
    listed = "".join(f"    {n},\n" for n in reversed(names))[:-2] + "\n"
    return sql[:m.start()] + m.group(1) + listed + m.group(3) + sql[m.end():]


def lake_of(data):
    """A lake holding the TPC-H tables (`--lake` to reuse one)."""
    if A.lake:
        return A.lake
    lake = harness.tempfile.mkdtemp(prefix="pondra-joinorder-")
    harness.LAKES.append(lake)
    for t in TABLES:
        subprocess.run([harness.BIN, "sql", "--dir", lake, f"INSERT INTO {t} SELECT * FROM '{os.path.join(data, t)}.parquet'"], check=True, capture_output=True)
    return lake


RUN = iter(range(1 << 30))


def timed(port, sql, runs):
    """The best of `runs` runs, and the answer. Each run's text is its own, so none of them is
    answered from the cache a query of the same text would hit."""
    best, out = 1e9, None
    for _ in range(runs):
        t0 = time.time()
        out = call(port, "POST", "/sql", (sql + f" -- {next(RUN)}").encode(), timeout=3600)
        best = min(best, time.time() - t0)
    return round(best, 3), out


def measure(lake, queries, on):
    """Every query, written well and written badly, with the rule on or off. The whole list runs
    `--passes` times and the best of each is kept: a 2-vCPU box is noisy."""
    node = Node(lake, A.port, env={"PONDRA_JOIN_ORDER": "1" if on else "0", "PONDRA_HOT_GB": "0"}, memory_gb=A.memory_gb, tier_secs=0).start()
    call(A.port, "POST", "/tier", timeout=3600)
    out = {q: {"well": 1e9, "badly": 1e9, "same": True} for q in queries}
    for _ in range(A.passes):
        for q, (good, bad) in queries.items():
            (well, answer) = timed(A.port, good, A.runs)
            (badly, other) = timed(A.port, bad, A.runs)
            out[q] = {"well": min(well, out[q]["well"]), "badly": min(badly, out[q]["badly"]), "same": out[q]["same"] and same(answer, other)}
    node.kill()
    return out


def main():
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "bench"))
    from tpch import queries as tpch_queries
    lake = lake_of(A.data)
    queries = shapes()
    for i, sql in tpch_queries(A.queries).items():
        bad = reversed_from(sql)
        if bad:
            queries[f"tpch-q{i}"] = (sql, bad)
    on = measure(lake, queries, True)
    off = measure(lake, queries, False)
    worst = lambda m: max(v["badly"] / max(v["well"], 1e-6) for v in m.values())
    total = lambda m, k: round(sum(v[k] for v in m.values()), 3)
    # A query counts as slower only if it is both a good deal slower and slower by something worth
    # measuring: a 45 ms query on a 2-vCPU box wobbles by more than 15% on its own.
    slower = lambda k: [q for q in queries if on[q][k] > off[q][k] * 1.15 and on[q][k] > off[q][k] + 0.03]
    checks = {
        "the badly written queries answer the same": all(v["same"] for v in on.values()) and all(v["same"] for v in off.values()),
        "the badly written queries cost less with the rule": total(on, "badly") < total(off, "badly"),
        "no badly written query is slower with the rule": not slower("badly"),
        "no well written query is slower with the rule": not slower("well"),
    }
    out = {"queries": len(queries), "rule_on": {"well_s": total(on, "well"), "badly_s": total(on, "badly"), "worst_ratio": round(worst(on), 2)},
           "rule_off": {"well_s": total(off, "well"), "badly_s": total(off, "badly"), "worst_ratio": round(worst(off), 2)},
           "slower_with_the_rule": {k: slower(k) for k in ("well", "badly")},
           "per_query": {q: {"on": on[q], "off": off[q]} for q in queries}, "checks": checks, "ok": all(checks.values())}
    print(json.dumps(out, indent=1))
    sys.exit(0 if out["ok"] else 1)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default=os.path.expanduser("~/tpch/sf1-bench"), help="the TPC-H parquet files to load")
    ap.add_argument("--lake", help="a lake that already holds them (skips loading)")
    ap.add_argument("--queries", default=os.path.expanduser("~/tpch/queries"))
    ap.add_argument("--runs", type=int, default=3)
    ap.add_argument("--passes", type=int, default=2, help="times through the whole list (the best of each is kept)")
    ap.add_argument("--memory-gb", type=float, default=2)
    ap.add_argument("--port", type=int, default=8140)
    ap.add_argument("--s3", action="store_true")
    ap.add_argument("--keep", action="store_true")
    A = ap.parse_args()
    harness.A = A
    main()
