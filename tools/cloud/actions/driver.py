#!/usr/bin/env python3
"""The benchmark of a cluster on separate machines (`.github/workflows/cluster-bench.yml`).

  driver.py --nodes 100.64.0.1:8080,100.64.0.2:8080,100.64.0.3:8080 --lake s3://bucket/bench-7 \
            --data tpch-sf10 --queries tools/bench/tpch-queries --bin ./pondra --out results.json

Waits until every node is in the cluster, measures the network to each node (Tailscale's path,
direct or relayed, and a 256 MB download), loads TPC-H with one `pondra sql` INSERT per table
(Parquet straight into the bucket), waits until the lake is settled (every row tiered, merges
done: they would otherwise run under the first queries), then runs each of the 22 queries on one
node (`?spread=0`), as the cluster decides (the default: spread only when it pays, `guard.rs`)
and spread anyway (`?spread=1`), best of `--runs`, and checks that the answers agree.
results.json has, per query, the three times, how each ran (shuffled, gathered, one node), what
the nodes sent each other and how long their steps waited for it; plus the network, the load
time and every node's /metrics. Standalone on purpose: a bench repo needs only this file,
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


def network(nodes):
    """How the machines reach each other: Tailscale's path to each node (a direct link, or relayed
    through a DERP server, and its latency) and a 256 MB download from it (nodes serve one on 8081)."""
    out = {}
    for n in nodes:
        host = n.rsplit(":", 1)[0]
        def run(*cmd):
            try:
                return subprocess.run(cmd, capture_output=True, text=True, timeout=600).stdout.strip()
            except (OSError, subprocess.SubprocessError):
                return ""
        ping = run("sudo", "tailscale", "ping", "-c", "3", host).splitlines()
        speed = run("curl", "-s", "-o", "/dev/null", "-w", "%{speed_download}", f"http://{host}:8081/blob")
        out[n] = {"path": ping[-1] if ping else "", "mb_s": round(float(speed or 0) / 1e6, 1)}
    return out


def moved(nodes):
    """Bytes the nodes have sent each other (compressed, as sent) and seconds their steps have
    waited for them, summed over the nodes."""
    ms = [metrics(n) for n in nodes]
    return sum(m.get("pondra_wire_bytes_total", 0) for m in ms), sum(m.get("pondra_shuffle_wait_seconds_total", 0) for m in ms)


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


def settle(nodes):
    """Wait until the lake is settled — every row tiered, merges and sealing done — so none of it
    runs under the queries being timed: the leader's /tier until a round finds nothing to do."""
    t0 = time.time()
    while time.time() - t0 < 1800:
        leader = next((n for n in nodes if (answers(n) or {}).get("role") == "leader"), nodes[0])
        t = time.time()
        try:
            tiered = call(leader, "POST", "/tier").get("rows_tiered", 0)
        except Exception:
            tiered = -1
        if tiered == 0 and time.time() - t < 1 and (answers(leader) or {}).get("untiered_rows", 1) == 0:
            break
        time.sleep(1)
    return round(time.time() - t0, 1)


def best(node, sql, spread, runs):
    times, out = [], None
    for _ in range(runs):
        t0 = time.time()
        out = call(node, "POST", "/sql" + (f"?spread={spread}" if spread != "" else ""), (sql + f" -- {next(RUNS)}").encode())
        times.append(time.time() - t0)
    return round(min(times), 3), out


def answers(node):
    try:
        return call(node, "GET", "/stats", timeout=5)
    except OSError:
        return None


def joined(nodes):
    """The members the cluster lists, asked of every node that answers."""
    return max((set(s.get("nodes", [])) for s in map(answers, nodes) if s), key=len, default=set())


def main():
    nodes = A.nodes.split(",")
    head = nodes[0]
    deadline = time.time() + 600
    while len(joined(nodes)) < len(nodes):  # (a node may still be starting, or restarting to rejoin)
        if time.time() > deadline:
            sys.exit(f"only {joined(nodes)} of {nodes} joined; not answering: {[n for n in nodes if not answers(n)]}")
        time.sleep(2)
    net = network(nodes)
    print(json.dumps({"network": net}, indent=1), flush=True)
    t0 = time.time()
    for t in TABLES:
        subprocess.run([A.bin, "sql", "--dir", A.lake, f"INSERT INTO {t} SELECT * FROM '{os.path.join(A.data, t)}.parquet'"], check=True)
    load_s = round(time.time() - t0, 1)
    settle_s = settle(nodes)
    results = {}
    for i, sql in queries(A.queries).items():
        one_s, one = best(head, sql, 0, A.runs)
        runs = {}
        for name, spread in (("cluster", ""), ("forced", 1)):  # (as the cluster decides; spread anyway)
            before, (wire0, wait0) = metrics(head), moved(nodes)
            took, rows = best(head, sql, spread, A.runs)
            after, (wire1, wait1) = metrics(head), moved(nodes)
            ran = lambda m: after.get(f"pondra_{m}_queries_total", 0) > before.get(f"pondra_{m}_queries_total", 0)
            runs[name] = {"s": took, "how": "shuffled" if ran("shuffled") else "gathered" if ran("spread") else "one node", "same": same(one, rows),
                          "wire_mb": round((wire1 - wire0) / 1e6 / A.runs, 1), "wait_s": round((wait1 - wait0) / A.runs, 3)}  # (per run; wait summed over the nodes)
        c, f = runs["cluster"], runs["forced"]
        r = results[f"q{i}"] = {"one_node_s": one_s, "cluster_s": c["s"], "how": c["how"], "forced_s": f["s"], "forced_how": f["how"], "same": c["same"] and f["same"],
                                "wire_mb": c["wire_mb"], "wait_s": c["wait_s"], "forced_wire_mb": f["wire_mb"], "forced_wait_s": f["wait_s"]}
        print(f"q{i:<3} one node {one_s:>7.2f}s  {len(nodes)} nodes {c['s']:>7.2f}s {c['how']:<9} (spread anyway {f['s']:>7.2f}s {f['how']:<9})  same={r['same']}  "
              f"sent {c['wire_mb']} MB ({f['wire_mb']} MB), waited {c['wait_s']} s ({f['wait_s']} s)", flush=True)
    total = lambda k: round(sum(r[k] for r in results.values()), 2)
    out = {"nodes": len(nodes), "lake": A.lake, "data": A.data, "network": net, "load_s": load_s, "settle_s": settle_s, "one_node_s": total("one_node_s"), "cluster_s": total("cluster_s"),
           "forced_s": total("forced_s"), "spread": sum(r["how"] != "one node" for r in results.values()), "forced_spread": sum(r["forced_how"] != "one node" for r in results.values()),
           "all_same": all(r["same"] for r in results.values()), "cpus": os.cpu_count(), "queries": results, "metrics": {n: metrics(n) for n in nodes}}
    out["wire_mb"], out["wait_s"] = round(sum(r["wire_mb"] for r in results.values()), 1), round(sum(r["wait_s"] for r in results.values()), 2)
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
