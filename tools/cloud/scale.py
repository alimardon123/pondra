#!/usr/bin/env python3
"""Scale, proven (round 34): the same TPC-H queries on 1, 3 and 6 nodes of one cluster, and
optionally on Spark on the same machines, into one scale.json.

  scale.py --hosts hosts.internal.txt --env env.sh --sf 100 [--spark] [--ks 1,3,6] [--runs 3]
  scale.py --local [--nodes 3] [--data DIR]        the kit's plumbing on this machine: nodes on 127.0.0.1
  scale.py … --dry-run                             print the plan and every command; run nothing

Run it on the client VM (gcp.sh sync), which reaches the nodes' private addresses. The data and
the lake are there already (tpch_parts.sh). For each k, the first k hosts are started by
cluster.sh on the one lake, the run waits until all k are in the cluster and one leads and until
the lake is settled, then times each query with driver.py's own measures (actions/driver.py:
`best`, `settle`, `same`, …): on one node when k = 1, else as the cluster decides and spread
anyway, best of --runs. Every answer is compared with the first size's. Spark runs with the
Pondra nodes stopped, so the two never share the CPUs: `spark.sh up hosts k` starts k workers,
and spark_tpch.py times the same 22 queries over the same Parquet in the bucket.

--local with no --data makes a small stand-in (a lineitem and an orders table, written by Pondra
itself) and runs three queries over it: that checks the plumbing, it is not a benchmark.
"""
import argparse, json, os, re, shlex, shutil, subprocess, sys, tempfile, time

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, os.path.join(HERE, "actions"))
import driver  # (queries, best, same, settle, joined, answers, metrics, moved: nothing runs on import)

LINEITEM = """SELECT value % {orders} + 1 AS l_orderkey, value % 7 + 1 AS l_linenumber,
  CAST(value % 50 + 1 AS DECIMAL(15,2)) AS l_quantity, CAST(value % 1000 + 100 AS DECIMAL(15,2)) AS l_extendedprice,
  CAST(value % 11 AS DECIMAL(15,2)) / 100 AS l_discount,
  CASE value % 3 WHEN 0 THEN 'A' WHEN 1 THEN 'N' ELSE 'R' END AS l_returnflag, CASE value % 2 WHEN 0 THEN 'O' ELSE 'F' END AS l_linestatus
  FROM generate_series({lo}, {hi})"""
ORDERS = "SELECT value AS o_orderkey, value % 1000 AS o_custkey, CAST(value % 5 AS VARCHAR) AS o_orderpriority FROM generate_series({lo}, {hi})"
STANDIN = {  # (TPC-H's q1, q3 and q6 in the stand-in's columns: a group-by, a join, a filtered sum)
    1: "SELECT l_returnflag, l_linestatus, sum(l_quantity) AS sum_qty, sum(l_extendedprice) AS sum_price, count(*) AS n FROM lineitem GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus",
    3: "SELECT o_orderpriority, count(*) AS n, sum(l_extendedprice) AS revenue FROM orders JOIN lineitem ON l_orderkey = o_orderkey WHERE l_discount > 0.05 GROUP BY o_orderpriority ORDER BY o_orderpriority",
    6: "SELECT sum(l_extendedprice * l_discount) AS revenue FROM lineitem WHERE l_quantity < 24",
}


def sh(*cmd, capture=False):
    """Run a command (a dry run only prints it)."""
    if A.dry_run:
        print("+", shlex.join(cmd), flush=True)
        return ""
    return subprocess.run(cmd, check=True, text=True, stdout=subprocess.PIPE if capture else None).stdout


def note(text):
    if A.dry_run:
        print("  #", text, flush=True)


def wait(cond, secs, what):
    t0 = time.time()
    while time.time() - t0 < secs:
        if (v := cond()):
            return v
        time.sleep(0.5)
    sys.exit(f"gave up waiting for {what}")


def alive(node):
    try:
        return driver.answers(node)
    except Exception:  # (a half-open connection raises more than OSError)
        return None


def form(nodes):
    """Until every node is in the cluster and exactly one leads: the leader's address."""
    def ready():
        stats = {n: alive(n) or {} for n in nodes}
        leaders = [n for n in nodes if stats[n].get("role") == "leader"]
        members = max((set(s.get("nodes", [])) for s in stats.values()), key=len)  # (what driver.joined reads)
        return leaders[0] if set(nodes) <= members and len(leaders) == 1 else None
    return wait(ready, 600, f"{len(nodes)} nodes in one cluster with one leader")


class Cloud:
    """The nodes on hosts.txt's machines, started and stopped by cluster.sh."""
    def __init__(self):
        self.hosts = [l.split()[:2] for l in open(A.hosts) if l.strip() and not l.startswith("#")]
        self.all = [f"{ip}:8080" for _, ip in self.hosts]
        self.dir = None

    def start(self, k):
        self.dir = self.dir or (tempfile.gettempdir() if A.dry_run else tempfile.mkdtemp(prefix="scale-"))
        part = os.path.join(self.dir, f"hosts-{k}.txt")  # (the first k hosts: cluster.sh starts what its file lists)
        if not A.dry_run:
            open(part, "w").write("".join(f"{login} {ip}\n" for login, ip in self.hosts[:k]))
        sh("bash", os.path.join(HERE, "cluster.sh"), "start", part, A.env, *shlex.split(A.serve_flags))
        return self.all[:k]

    def stop(self):
        sh("bash", os.path.join(HERE, "cluster.sh"), "stop", A.hosts)

    def remove(self):
        if self.dir and not A.dry_run:
            shutil.rmtree(self.dir, ignore_errors=True)


class Local:
    """Nodes on 127.0.0.1, one lake folder, started one at a time so the first leads."""
    def __init__(self, work):
        self.lake = os.path.join(work, "lake")
        self.work, self.procs = work, []
        self.all = [f"127.0.0.1:{A.port + i}" for i in range(A.nodes)]

    def start(self, k):
        for i in range(k):
            if A.dry_run:
                print("+", shlex.join([A.bin, "serve", "--lake", self.lake, "--addr", self.all[i]]), flush=True)
                continue
            if alive(self.all[i]):
                sys.exit(f"{self.all[i]} already answers: another node is using the port (--port)")
            log = open(os.path.join(self.work, f"node-{i}.log"), "a")
            self.procs.append(subprocess.Popen([A.bin, "serve", "--lake", self.lake, "--addr", self.all[i]], stdout=subprocess.DEVNULL, stderr=log))
            wait(lambda: alive(self.all[i]), 120, f"node {i} to answer")
        return self.all[:k]

    def stop(self):
        for p in self.procs:
            p.terminate()  # (SIGTERM: a leader gives up its term)
        for p in self.procs:
            try:
                p.wait(30)
            except subprocess.TimeoutExpired:
                p.kill()
                p.wait()
        self.procs = []


def measure(nodes, qs, base):
    """driver.py's timing at this size: one node when k = 1, else as the cluster decides and spread
    anyway; every answer compared with the first size's (`base`)."""
    head = nodes[0]
    modes = [("one", 0)] if len(nodes) == 1 else [("cluster", "")] + ([] if A.no_forced else [("forced", 1)])
    results = {}
    for i, sql in qs.items():
        r = {}
        for name, spread in modes:
            before, (wire0, _) = driver.metrics(head), driver.moved(nodes)
            took, rows = driver.best(head, sql, spread, A.runs)
            after, (wire1, _) = driver.metrics(head), driver.moved(nodes)
            ran = lambda m: after.get(f"pondra_{m}_queries_total", 0) > before.get(f"pondra_{m}_queries_total", 0)
            base.setdefault(i, rows)
            r[name] = {"s": took, "how": "shuffled" if ran("shuffled") else "gathered" if ran("spread") else "one node",
                       "same": driver.same(base[i], rows), "wire_mb": round((wire1 - wire0) / 1e6 / A.runs, 1)}  # (sent between the nodes, per run)
        first = r[modes[0][0]]
        row = results[f"q{i}"] = {"s": first["s"], "how": first["how"], "wire_mb": first["wire_mb"], "same": all(m["same"] for m in r.values())}
        if "forced" in r:
            row.update(forced_s=r["forced"]["s"], forced_how=r["forced"]["how"], forced_wire_mb=r["forced"]["wire_mb"])
        print(f"q{i:<3} k={len(nodes)} {row['s']:>8.2f}s {row['how']:<9}" + (f" (spread anyway {row['forced_s']:>8.2f}s {row['forced_how']})" if "forced" in r else "")
              + f"  same={row['same']}  sent {row['wire_mb']} MB", flush=True)
    return results


def pondra_at(cluster, k, qs, base):
    cluster.stop()  # (so that exactly k nodes run)
    if not A.dry_run:
        wait(lambda: not any(alive(n) for n in cluster.all), 120, "the old nodes to stop")
    nodes = cluster.start(k)
    if A.dry_run:
        note(f"wait until {', '.join(nodes)} are in one cluster with one leader, and the lake is settled")
        note(f"time q{min(qs)}..q{max(qs)} on {nodes[0]}: " + ("one node" if k == 1 else "as the cluster decides" + ("" if A.no_forced else ", then spread anyway (?spread=1)"))
             + f", best of {A.runs}; compare each answer with the first size's")
        return {}
    leader = form(nodes)
    settle_s = driver.settle(nodes)
    results = measure(nodes, qs, base)
    total = lambda key: round(sum(r[key] for r in results.values() if key in r), 2)
    out = {"nodes": nodes, "leader": leader, "settle_s": settle_s, "total_s": total("s"), "spread": sum(r["how"] != "one node" for r in results.values()),
           "all_same": all(r["same"] for r in results.values()), "queries": results}
    if not A.no_forced and k > 1:
        out.update(forced_s=total("forced_s"), forced_spread=sum(r["forced_how"] != "one node" for r in results.values()))
    return out


def spark_at(k, qs, base, login):
    """k Spark workers on the first k hosts, the same queries over the same Parquet in the bucket."""
    spark = os.path.join(HERE, "spark.sh")
    qfile = "/tmp/tpch-queries.json" if A.dry_run else tempfile.NamedTemporaryFile("w", suffix=".json", delete=False).name
    if not A.dry_run:
        json.dump({str(i): q for i, q in qs.items()}, open(qfile, "w"))
    sh("scp", "-q", qfile, f"{login}:/tmp/tpch-queries.json")
    if not A.dry_run:
        os.unlink(qfile)
    sh("bash", spark, "up", A.hosts, str(k))
    out = sh("bash", spark, "submit", A.hosts, os.path.join(HERE, "spark_tpch.py"), "--data", A.spark_data, "--queries", "/tmp/tpch-queries.json", "--runs", str(A.runs), capture=True)
    sh("bash", spark, "down", A.hosts)
    if A.dry_run:
        return {}
    r = json.loads(out.strip().splitlines()[-1])
    return {"total_s": round(sum(r["times"].values()), 2), "spark": r.get("version"), "cores": r.get("cores"),
            "rows_same": all(r["rows"][str(i)] == len(base[i]) for i in qs),  # (the row counts, as tools/bench/tpch.py compares them)
            "queries": {f"q{i}": r["times"][str(i)] for i in qs}}


def summary(out):
    """What the round asks: does time fall as nodes are added, and by how much?"""
    p = out["pondra"]
    ks = sorted(p, key=int)
    if len(ks) > 1:
        t = [p[k]["total_s"] for k in ks]
        out["speedup"] = {k: round(t[0] / p[k]["total_s"], 2) for k in ks}
        out["time_falls"] = all(a > b for a, b in zip(t, t[1:]))
    out["all_same"] = all(p[k]["all_same"] for k in ks)
    if out["spark"]:
        out["spark_over_pondra"] = {k: round(out["spark"][k]["total_s"] / p[k]["total_s"], 2) for k in ks if k in out["spark"]}  # (above 1: Pondra is faster)
        out["spark_rows_same"] = all(v["rows_same"] for v in out["spark"].values())
    else:
        del out["spark"]


def read_env(path):
    out = {}
    for line in open(path):
        if (m := re.match(r"\s*(?:export\s+)?(\w+)=(.*)", line)):
            out[m[1]] = m[2].strip().strip("'\"")
    return out


def pondra(*args):
    return sh(A.bin, "sql", "--lake", *args)


def standin(work):
    """Two small tables written as Parquet by Pondra itself, in parts as tpch_parts.sh lays them out."""
    d, n = os.path.join(work, "data"), A.rows
    orders = max(n // 4, 1)
    for table, part, lo, hi in (("lineitem", 1, 1, n // 2), ("lineitem", 2, n // 2 + 1, n), ("orders", 1, 1, orders)):
        os.makedirs(f"{d}/{table}", exist_ok=True)
        select = " ".join((LINEITEM if table == "lineitem" else ORDERS).format(lo=lo, hi=hi, orders=orders).split())
        pondra(os.path.join(work, "scratch"), f"COPY ({select}) TO '{d}/{table}/{table}.{part}.parquet'")
    return d, ["orders", "lineitem"], dict(STANDIN)


def main():
    local = A.local
    work = tempfile.mkdtemp(prefix="scale-local-", dir=A.work) if local else None
    cluster = Local(work) if local else Cloud()
    if local:
        if A.data:
            tables = [t for t in driver.TABLES if os.path.isdir(f"{A.data}/{t}") or os.path.exists(f"{A.data}/{t}.parquet")]
            data, qs = A.data, driver.queries(A.queries)
        else:
            data, tables, qs = standin(work)
    else:
        env = read_env(A.env)
        A.spark_data = A.spark_data or f"{env.get('BUCKET', '')}/tpch-sf{A.sf}"
        qs = driver.queries(A.queries)
    qs = {i: q for i, q in qs.items() if not A.only or i in A.only}
    ks = [k for k in A.ks if k <= len(cluster.all)]
    out = {"mode": "local" if local else "cloud", "sf": None if local else A.sf, "runs": A.runs, "ks": ks, "queries": len(qs),
           "serve_flags": None if local else A.serve_flags, "when": time.strftime("%Y-%m-%d %H:%M:%S"), "pondra": {}, "spark": {}}
    save = lambda: None if A.dry_run else json.dump(out, open(A.out, "w"), indent=1)
    print(f"plan: {'local, ' if local else ''}k = {ks} of {len(cluster.all)} hosts, q{min(qs)}..q{max(qs)} ({len(qs)} queries), best of {A.runs}"
          + (", then Spark" if A.spark else "") + f" -> {A.out}", flush=True)
    base = {}
    try:
        if local:
            for t in tables:  # (the first node isn't up yet: this process leads the lake for a moment)
                src = f"{data}/{t}/*.parquet" if os.path.isdir(f"{data}/{t}") else f"{data}/{t}.parquet"
                pondra(cluster.lake, f"INSERT INTO {t} SELECT * FROM read_parquet('{src}')")
        for k in ks:
            print(f"== Pondra, {k} node{'s' * (k > 1)}", flush=True)
            out["pondra"][str(k)] = pondra_at(cluster, k, qs, base)
            save()
        if A.spark and not local:
            cluster.stop()  # (Pondra's nodes off: Spark gets the CPUs)
            for k in ks:
                print(f"== Spark, {k} worker{'s' * (k > 1)}", flush=True)
                out["spark"][str(k)] = spark_at(k, qs, base, cluster.hosts[0][0])
                save()
        if not A.dry_run:
            summary(out)
            save()
            print(json.dumps({k: v for k, v in out.items() if k not in ("pondra", "spark")}, indent=1))
            print(json.dumps({k: {n: v for n, v in r.items() if n in ("total_s", "forced_s", "spread", "all_same")} for k, r in out["pondra"].items()}))
    finally:
        cluster.stop()
        if A.spark and not local:
            sh("bash", os.path.join(HERE, "spark.sh"), "down", A.hosts)
        if not local:
            cluster.remove()
        elif not A.keep:
            shutil.rmtree(work, ignore_errors=True)
        else:
            print("kept", work)


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--hosts", help="hosts.txt: `user@host private_ip` per line (hosts.internal.txt, on the client VM)")
    ap.add_argument("--env", default="env.sh", help="LAKE and BUCKET, as gcp.sh writes them; cluster.sh sources it on the nodes")
    ap.add_argument("--sf", default="100", help="the scale factor tpch_parts.sh was given (it names the data in the bucket)")
    ap.add_argument("--ks", default="1,3,6", help="cluster sizes (those no larger than the hosts given)")
    ap.add_argument("--runs", type=int, default=3, help="best of")
    ap.add_argument("--only", help="queries to run, e.g. 1,3,6 (default: all)")
    ap.add_argument("--no-forced", action="store_true", help="skip the spread-anyway timings (half the time at k > 1)")
    ap.add_argument("--queries", default=os.path.join(ROOT, "tools", "bench", "tpch-queries"))
    ap.add_argument("--serve-flags", default=os.environ.get("PONDRA_SERVE_FLAGS", "--memory-gb 22 --cache-dir /mnt/ssd/pondra-cache"), help="for each node")
    ap.add_argument("--spark", action="store_true", help="also time Spark on the same machines (spark.sh)")
    ap.add_argument("--spark-data", help="the Parquet Spark reads (default: $BUCKET/tpch-sf<SF>, where tpch_parts.sh put it)")
    ap.add_argument("--out", default="scale.json")
    ap.add_argument("--dry-run", action="store_true", help="print the plan and the commands; run nothing")
    ap.add_argument("--local", action="store_true", help="nodes on 127.0.0.1, one lake folder (no cloud)")
    ap.add_argument("--nodes", type=int, default=3, help="--local: how many nodes")
    ap.add_argument("--data", help="--local: a folder of TPC-H Parquet (<table>/*.parquet or <table>.parquet); default: a small stand-in")
    ap.add_argument("--rows", type=int, default=1_000_000, help="--local stand-in: lineitem rows")
    ap.add_argument("--bin", default=os.path.join(ROOT, "target", "release", "pondra"), help="--local: the binary")
    ap.add_argument("--port", type=int, default=18700, help="--local: the first node's port")
    ap.add_argument("--work", default=tempfile.gettempdir(), help="--local: where the lake and the stand-in go")
    ap.add_argument("--keep", action="store_true", help="--local: keep them")
    A = ap.parse_args()
    A.ks = [int(k) for k in A.ks.split(",")]
    A.data = os.path.abspath(A.data) if A.data else None
    A.only = {int(q) for q in A.only.split(",")} if A.only else None
    if not A.local and not A.hosts:
        ap.error("--hosts (or --local)")
    if not A.local and not os.path.exists(A.hosts):
        ap.error(f"{A.hosts} not found (gcp.sh up writes it)")
    main()
