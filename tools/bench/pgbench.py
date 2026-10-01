#!/usr/bin/env python3
"""pgbench's own TPC-B script against Pondra's Postgres port (ADR-036 §5, the round-30 gate).

The tables are made as `pgbench -i` makes them, but by SQL Pondra takes (its `-i` asks for
`fillfactor` and `VACUUM`): branches, tellers and accounts keyed, history an append table. Then
pgbench runs its builtin `tpcb-like` script (`BEGIN`, three `UPDATE`s, a `SELECT`, an `INSERT`,
`END`) with retries on serialization failures (40001), and the balances are checked: the sums of
accounts, tellers, branches and history's deltas agree, and history has a row per transaction.

  pgbench.py [--bin target/release/pondra] [--scale 1] [--clients 1,4] [--secs 15] [--postgres]

`--postgres` runs the same against a Postgres this starts for it (initdb, `pgbench -i`), for
comparison (as `postgres` here when run as root).
"""
import argparse, json, os, re, shutil, subprocess, sys, tempfile, time, urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
ap = argparse.ArgumentParser()
ap.add_argument("--bin", default=os.environ.get("PONDRA_BIN", os.path.join(HERE, "..", "..", "target", "release", "pondra")))
ap.add_argument("--scale", type=int, default=1)
ap.add_argument("--clients", default="1,4")
ap.add_argument("--secs", type=int, default=15)
ap.add_argument("--port", type=int, default=9620)
ap.add_argument("--postgres", action="store_true")
A = ap.parse_args()


def bench(port, user, db):
    """pgbench's builtin script at each number of clients, retried on 40001."""
    runs = []
    for c in [int(x) for x in A.clients.split(",")]:
        r = subprocess.run(["pgbench", "-n", "-h", "127.0.0.1", "-p", str(port), "-U", user, "-c", str(c), "-j", str(min(c, 2)), "-T", str(A.secs), "--max-tries=100", db],
                           capture_output=True, text=True, timeout=A.secs + 300)
        got = lambda pat: (m := re.search(pat, r.stdout)) and float(m.group(1))
        runs.append({"clients": c, "tps": got(r"tps = ([\d.]+)"), "latency_ms": got(r"latency average = ([\d.]+) ms"), "transactions": got(r"actually processed: (\d+)"),
                     "failed": got(r"failed transactions: (\d+)"), "retried": got(r"transactions retried: (\d+)"), "error": r.stderr[-300:] if r.returncode else None})
    return runs


def postgres():
    """The same against Postgres: initdb, `pgbench -i`, the runs (a Postgres of its own, stopped after)."""
    bins = sorted(d for d in [f"/usr/lib/postgresql/{v}/bin" for v in range(20, 12, -1)] if os.path.exists(os.path.join(d, "initdb")))
    if not bins:
        return {"skipped": "no Postgres here"}
    pg, data = bins[-1], tempfile.mkdtemp(prefix="pondra-pgbench-pg-")
    user = [] if os.geteuid() else ["runuser", "-u", "postgres", "--"]
    if user:
        shutil.chown(data, "postgres")
    subprocess.run([*user, os.path.join(pg, "initdb"), "-D", data, "-U", "postgres", "--auth=trust"], check=True, stdout=subprocess.DEVNULL)
    subprocess.run([*user, os.path.join(pg, "pg_ctl"), "-D", data, "-o", "-p 5497 -k /tmp", "-l", os.path.join(data, "log"), "-w", "start"], check=True, stdout=subprocess.DEVNULL)
    try:
        subprocess.run(["pgbench", "-i", "-q", "-s", str(A.scale), "-h", "127.0.0.1", "-p", "5497", "-U", "postgres", "postgres"], check=True, capture_output=True)
        return {"runs": bench(5497, "postgres", "postgres")}
    finally:
        subprocess.run([*user, os.path.join(pg, "pg_ctl"), "-D", data, "-m", "fast", "stop"], capture_output=True)
        shutil.rmtree(data, ignore_errors=True)


HTTP, PG = A.port, A.port + 1


def q(sql):
    return json.loads(urllib.request.urlopen(urllib.request.Request(f"http://127.0.0.1:{HTTP}/sql", data=sql.encode()), timeout=600).read())


lake = tempfile.mkdtemp(prefix="pondra-pgbench-")
node = subprocess.Popen([A.bin, "serve", "--dir", lake, "--addr", f"127.0.0.1:{HTTP}", "--pg", f"127.0.0.1:{PG}"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
out = {"scale": A.scale, "runs": []}
try:
    for _ in range(300):
        try:
            urllib.request.urlopen(f"http://127.0.0.1:{HTTP}/stats", timeout=1)
            break
        except Exception:
            time.sleep(0.1)
    s = A.scale
    for t in ["CREATE TABLE pgbench_branches (bid INT PRIMARY KEY, bbalance INT, filler VARCHAR)",
              "CREATE TABLE pgbench_tellers (tid INT PRIMARY KEY, bid INT, tbalance INT, filler VARCHAR)",
              "CREATE TABLE pgbench_accounts (aid INT PRIMARY KEY, bid INT, abalance INT, filler VARCHAR)",
              "CREATE TABLE pgbench_history (tid INT, bid INT, aid INT, delta INT, mtime TIMESTAMP, filler VARCHAR)",
              f"INSERT INTO pgbench_branches SELECT value AS bid, 0, '' FROM range(1, {s + 1})",
              f"INSERT INTO pgbench_tellers SELECT value AS tid, (value - 1) / 10 + 1, 0, '' FROM range(1, {10 * s + 1})",
              f"INSERT INTO pgbench_accounts SELECT value AS aid, (value - 1) / 100000 + 1, 0, '' FROM range(1, {100000 * s + 1})"]:
        q(t)
    out["runs"] = bench(PG, "admin", "lake")
    b = q("SELECT (SELECT sum(abalance) FROM pgbench_accounts) AS a, (SELECT sum(tbalance) FROM pgbench_tellers) AS t, (SELECT sum(bbalance) FROM pgbench_branches) AS b, "
          "(SELECT sum(delta) FROM pgbench_history) AS h, (SELECT count(*) FROM pgbench_history) AS n")[0]
    done = sum(int(x["transactions"] or 0) for x in out["runs"])
    out["balances"] = b
    out["balances_right"] = b["a"] == b["t"] == b["b"] == b["h"] and b["n"] == done
finally:
    node.terminate()
    node.wait(30)
    shutil.rmtree(lake, ignore_errors=True)
if A.postgres:
    out["postgres"] = postgres()
print(json.dumps(out))
sys.exit(0 if out.get("balances_right") and all(not x["error"] for x in out["runs"]) else 1)
