#!/usr/bin/env python3
"""The catalog at 100,000 tables (redesign step 5, piece 9: designs/redesign/redesign.md §4, "measured at
100,000 tables before it is changed"). It grows one lake's catalog in steps (1,000, 5,000, 10,000 and
20,000 tables, each made by CREATE TABLE … AS SELECT: one file each) and at each step measures:

- what one commit reads and writes: `pondra_catalog_records_total` around one INSERT into one table and one
  CREATE TABLE, the least of five tries, less what reading /metrics reads itself (its per-table rows read every
  table's record: shown as `metrics_scrape_reads`); a node's own rounds read records too, shown as the idle rate;
- the leader's resident memory;
- a follower's start (to its first answer), its resident memory once settled, and its first query's latency;
- the point lookup's p50 and p99 (GET /lookup on a keyed table of 100,000 rows), on the leader and the follower.

The write-set rule (§4) says a commit's records may not grow with the tables it doesn't touch: the commit rows
should read the same at every step. It changes nothing; it is the measure the other pieces are held to.

Today a CREATE TABLE reads every table's record, so making N tables takes time in N squared: the default steps
stop at 20,000 (about twenty minutes). Step 100,000 (about nine hours today) is run once piece 10 holds commits
to their write set.

  catalog_scale.py [--bin target/release/pondra] [--lake DIR | s3://bucket/prefix] [--steps 1000,5000,10000,20000]
                   [--workers 8] [--out results.json]

For a bucket, the node takes AWS_ENDPOINT, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY and AWS_REGION from the
environment (MinIO: AWS_ALLOW_HTTP=true)."""
import argparse, http.client, json, os, random, re, shutil, signal, statistics, subprocess, sys, tempfile, time
from concurrent.futures import ThreadPoolExecutor

HERE = os.path.dirname(os.path.abspath(__file__))
ap = argparse.ArgumentParser()
ap.add_argument("--bin", default=os.path.join(HERE, "..", "..", "target", "release", "pondra"))
ap.add_argument("--lake", default="", help="a folder (made fresh when not given) or s3://bucket/prefix")
ap.add_argument("--steps", default="1000,5000,10000,20000")
ap.add_argument("--workers", type=int, default=8, help="requests making tables at once")
ap.add_argument("--port", type=int, default=9740)
ap.add_argument("--lookups", type=int, default=2000)
ap.add_argument("--out", default="")
A = ap.parse_args()
STEPS = [int(s) for s in A.steps.split(",")]
OWNER = "catalog-scale-owner-key-0123456789"
ENV = {**os.environ, "PONDRA_OWNER_KEY": OWNER}
LOG = tempfile.mkdtemp(prefix="catalog-scale-logs-")
NODES = []


def call(port, method, path, body=None, timeout=600):
    c = http.client.HTTPConnection("127.0.0.1", port, timeout=timeout)
    c.request(method, path, body, {"x-pondra-owner": OWNER})
    r = c.getresponse()
    data = r.read()
    if r.status != 200:
        raise RuntimeError(f"{method} {path}: {r.status}: {data[:500]!r}")
    return data


def sql(port, q):
    return call(port, "POST", "/sql", q.encode())


def start(port, reader=False):
    """A node on the lake, and the seconds to its first answer."""
    args = [A.bin, "serve", "--dir", LAKE, "--addr", f"127.0.0.1:{port}"] + (["--reader"] if reader else [])
    t0 = time.perf_counter()
    p = subprocess.Popen(args, env=ENV, stdout=subprocess.DEVNULL, stderr=open(os.path.join(LOG, f"{port}.stderr"), "a"))
    NODES.append(p)
    while True:
        if p.poll() is not None:
            raise SystemExit(f"the node on {port} stopped: {open(os.path.join(LOG, f'{port}.stderr')).read()[-800:]}")
        try:
            sql(port, "SELECT 1")
            return p, time.perf_counter() - t0
        except (OSError, RuntimeError):
            time.sleep(0.02)


def stop(p):
    if p.poll() is None:
        p.send_signal(signal.SIGTERM)
        try:
            p.wait(60)
        except subprocess.TimeoutExpired:
            p.kill()
            p.wait()


def rss_mb(pid):
    return round(next(int(l.split()[1]) for l in open(f"/proc/{pid}/status") if l.startswith("VmRSS")) / 1024, 1)


def rss_settled(pid, cap=60):
    """Resident memory once it stops moving (within 2% for three seconds): the allocator gives memory back over seconds."""
    last, still, end = None, 0, time.time() + cap
    while time.time() < end:
        now = rss_mb(pid)
        still = still + 1 if last is not None and abs(now - last) * 50 <= last else 0
        if still >= 3:
            break
        last = now
        time.sleep(1)
    return rss_mb(pid)


def records(port):
    m = call(port, "GET", "/metrics").decode()
    got = dict(re.findall(r'pondra_catalog_records_total\{op="(\w+)"\} (\S+)', m))
    return float(got.get("read", 0)), float(got.get("write", 0))


def scrape(port):
    """What one read of /metrics reads itself (its per-table rows scan every table's record), with what the node
    read idle meanwhile: the least of five back-to-back reads. The rows below subtract it."""
    tries = []
    for _ in range(5):
        r0, w0 = records(port)
        r1, w1 = records(port)
        tries.append((r1 - r0, w1 - w0))
        time.sleep(0.2)
    return min(tries)


def per_commit(port, statement, base):
    """The records one commit reads and writes: the least of five, less what reading /metrics read (`scrape`)."""
    tries = []
    for i in range(5):
        r0, w0 = records(port)
        t0 = time.perf_counter()
        sql(port, statement(i))
        took = time.perf_counter() - t0
        r1, w1 = records(port)
        tries.append((r1 - r0 - base[0], w1 - w0 - base[1], took))
        time.sleep(0.2)
    best = min(tries)
    return {"reads": max(0, int(best[0])), "writes": max(0, int(best[1])), "p50_ms": round(statistics.median(t[2] for t in tries) * 1000, 2)}


def idle_rate(port, base, secs=5):
    r0, _ = records(port)
    time.sleep(secs)
    r1, _ = records(port)
    return round(max(0.0, r1 - r0 - base[0]) / secs, 1)


def lookups(port):
    random.seed(1)
    keys = [random.randint(1, 100_000) for _ in range(A.lookups)]
    h = http.client.HTTPConnection("127.0.0.1", port, timeout=60)
    ts = []
    for k in keys:
        t0 = time.perf_counter()
        h.request("GET", f"/lookup/kv/{k}", headers={"x-pondra-owner": OWNER})
        r = h.getresponse()
        r.read()
        ts.append((time.perf_counter() - t0) * 1000)
        if r.status != 200:
            return {"error": f"{r.status}"}
    ts.sort()
    return {"p50_ms": round(statistics.median(ts), 3), "p99_ms": round(ts[int(len(ts) * 0.99)], 3)}


def grow(port, have, want):
    def one(i):
        for attempt in range(5):
            try:
                return sql(port, f"CREATE TABLE IF NOT EXISTS t{i:06d} AS SELECT {i} AS x")
            except (OSError, RuntimeError):
                if attempt == 4:
                    raise
                time.sleep(0.5)
    t0 = time.perf_counter()
    with ThreadPoolExecutor(A.workers) as pool:
        list(pool.map(one, range(have, want)))
    return round((want - have) / (time.perf_counter() - t0), 1)


LAKE = A.lake or tempfile.mkdtemp(prefix="catalog-scale-")
out = {"bin": A.bin, "lake": "bucket" if LAKE.startswith("s3://") else "folder", "steps": []}
try:
    out["version"] = subprocess.run([A.bin, "--version"], capture_output=True, text=True).stdout.strip()
    leader, _ = start(A.port)
    sql(A.port, "CREATE TABLE kv (k BIGINT PRIMARY KEY, v VARCHAR)")
    sql(A.port, "INSERT INTO kv SELECT value, 'value ' || value FROM generate_series(1, 100000)")
    sql(A.port, "CHECKPOINT")
    have = 0
    for n in STEPS:
        rate = grow(A.port, have, n)
        have = n
        sql(A.port, "CHECKPOINT")
        row = {"tables": n, "tables_made_per_s": rate}
        base = scrape(A.port)
        row["metrics_scrape_reads"] = int(base[0])
        row["leader_idle_reads_per_s"] = idle_rate(A.port, base)
        row["commit_insert"] = per_commit(A.port, lambda i: f"INSERT INTO t{(i * 7919) % n:06d} VALUES (-1)", base)
        row["commit_create"] = per_commit(A.port, lambda i: f"CREATE TABLE probe_{n}_{i} (x BIGINT)", base)
        row["leader_rss_mb"] = rss_settled(leader.pid)
        row["leader_lookup"] = lookups(A.port)
        follower, row["follower_start_s"] = start(A.port + 1, reader=True)
        t0 = time.perf_counter()
        sql(A.port + 1, f"SELECT x FROM t{n // 2:06d}")
        row["follower_first_query_ms"] = round((time.perf_counter() - t0) * 1000, 1)
        row["follower_rss_mb"] = rss_settled(follower.pid)
        row["follower_lookup"] = lookups(A.port + 1)
        stop(follower)
        out["steps"].append(row)
        print(json.dumps(row), flush=True)
finally:
    for p in NODES:
        stop(p)
    if not A.lake:
        shutil.rmtree(LAKE, ignore_errors=True)
    if A.out:
        json.dump(out, open(A.out, "w"), indent=1)
    print(json.dumps(out, indent=1))
