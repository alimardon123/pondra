#!/usr/bin/env python3
"""The benchmark of a cluster on separate machines (`.github/workflows/cluster-bench.yml`).

  driver.py --nodes 100.64.0.1:8080,100.64.0.2:8080,100.64.0.3:8080 --lake s3://bucket/bench-7 \
            --data tpch-sf10 --queries tools/bench/tpch-queries --bin ./pondra --out results.json

Waits until every node is in the cluster, loads TPC-H with one `pondra sql` INSERT per table
(Parquet straight into the bucket), then runs each of the 22 queries on one node (`?spread=0`)
and across the cluster (`?spread=1`), best of `--runs`, and checks that the two answers agree.
results.json has, per query, both times and how it ran (shuffled, gathered, one node), plus the
load time and every node's /metrics. Standalone on purpose: a bench repo needs only this file,
the queries and the workflow.
"""
import argparse, http.client, itertools, json, os, re, subprocess, sys, time

TABLES = ["region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem"]
RUNS = itertools.count()


def call(node, method, path, body=b"", timeout=3600):
    host, port = node.rsplit(":", 1)
    c = http.client.HTTPConnection(host, int(port), timeout=timeout)
    c.request(method, path, body)
    r = c.getresponse()
    data = r.read()
    if r.status != 200:
        raise RuntimeError(f"{node} {path}: {r.status} {data[:300]!r}")
    return json.loads(data) if data[:1] in (b"{", b"[") else data


def metrics(node):
    out = {}
    for line in call(node, "GET", "/metrics").decode().splitlines():
        if line.startswith("pondra_") and " " in line:
            name, v = line.rsplit(" ", 1)
            out[name] = float(v)
    return out


def queries(folder):
    """The 22 queries; q15's view becomes a CTE, so each is one statement."""
    out = {}
    for i in range(1, 23):
        q = open(os.path.join(folder, f"q{i}.sql")).read().strip().rstrip(";")
        if i == 15:
            m = re.match(r"create view revenue0 \(supplier_no, total_revenue\) as\s*select\s*l_suppkey,\s*(sum\(.*?\))\s*from(.*?);\s*(select.*)", q, re.S | re.I)
            q = f"with revenue0 as (select l_suppkey as supplier_no, {m.group(1)} as total_revenue from{m.group(2)}) {m.group(3)}"
            q = re.split(r";\s*drop view", q, flags=re.I)[0]
        out[i] = q
    return out


def same(a, b):
    """The same rows, in any order an ORDER BY leaves open; floats equal to 1e-9 of their size
    (a sum of DOUBLEs depends on the order it is added up in)."""
    close = lambda x, y: x == y or isinstance(x, (int, float)) and isinstance(y, (int, float)) and abs(x - y) <= 1e-9 * max(abs(x), abs(y), 1)
    key = lambda r: json.dumps({k: float(f"{v:.8g}") if isinstance(v, float) else v for k, v in r.items()}, sort_keys=True, default=str)
    pairs = zip(sorted(a, key=key), sorted(b, key=key))
    return len(a) == len(b) and all(r.keys() == s.keys() and all(close(r[k], s[k]) for k in r) for r, s in pairs)


def best(node, sql, spread, runs):
    times, out = [], None
    for _ in range(runs):
        t0 = time.time()
        out = call(node, "POST", f"/sql?spread={spread}", (sql + f" -- {next(RUNS)}").encode())
        times.append(time.time() - t0)
    return round(min(times), 3), out


def main():
    nodes = A.nodes.split(",")
    head = nodes[0]
    deadline = time.time() + 600
    while len(call(head, "GET", "/stats").get("nodes", [])) < len(nodes):
        if time.time() > deadline:
            sys.exit(f"only {call(head, 'GET', '/stats').get('nodes')} of {len(nodes)} nodes joined")
        time.sleep(2)
    t0 = time.time()
    for t in TABLES:
        subprocess.run([A.bin, "sql", "--dir", A.lake, f"INSERT INTO {t} SELECT * FROM '{os.path.join(A.data, t)}.parquet'"], check=True)
    load_s = round(time.time() - t0, 1)
    for n in nodes:  # (merges and sealing done before timing; only the leader acts on it)
        try:
            call(n, "POST", "/tier")
        except Exception:
            pass
    results = {}
    for i, sql in queries(A.queries).items():
        before = metrics(head)
        one_s, one = best(head, sql, 0, A.runs)
        many_s, many = best(head, sql, 1, A.runs)
        after = metrics(head)
        ran = lambda m: after.get(f"pondra_{m}_queries_total", 0) > before.get(f"pondra_{m}_queries_total", 0)
        results[f"q{i}"] = {"one_node_s": one_s, "cluster_s": many_s, "how": "shuffled" if ran("shuffled") else "gathered" if ran("spread") else "one node", "same": same(one, many)}
        print(f"q{i:<3} one node {one_s:>7.2f}s  {len(nodes)} nodes {many_s:>7.2f}s  {results[f'q{i}']['how']:<9} same={results[f'q{i}']['same']}", flush=True)
    total = lambda k: round(sum(r[k] for r in results.values()), 2)
    out = {"nodes": len(nodes), "lake": A.lake, "data": A.data, "load_s": load_s, "one_node_s": total("one_node_s"), "cluster_s": total("cluster_s"),
           "spread": sum(r["how"] != "one node" for r in results.values()), "all_same": all(r["same"] for r in results.values()),
           "cpus": os.cpu_count(), "queries": results, "metrics": {n: metrics(n) for n in nodes}}
    json.dump(out, open(A.out, "w"), indent=1)
    print(json.dumps({k: v for k, v in out.items() if k not in ("queries", "metrics")}, indent=1))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--nodes", required=True, help="host:port of every node, comma-separated")
    ap.add_argument("--lake", required=True)
    ap.add_argument("--data", required=True, help="a folder of TPC-H Parquet files (tpchgen-cli)")
    ap.add_argument("--queries", default="tools/bench/tpch-queries")
    ap.add_argument("--bin", default="./pondra")
    ap.add_argument("--runs", type=int, default=3)
    ap.add_argument("--out", default="results.json")
    A = ap.parse_args()
    main()
