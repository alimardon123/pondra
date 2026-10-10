#!/usr/bin/env python3
"""The bucket's limits (principle 7, C5): a node keeps within a bucket's limits at any size, and
every statement still succeeds, measured on a bucket while a load runs (stdlib + boto3).

Three nodes on one bucket, each through a proxy of its own (`faulty_s3.py`, no faults set: it only
logs every request it forwards). For `--minutes`: 48 writers insert rows into 20 tables, 8 upserters
into a keyed table, 4 readers read, and once a minute `pondra sql` from outside the cluster writes.
Then, after the load, every acknowledged row is counted on every node, and the request log is
checked against the limits:

- **refused**: fewer than 1% of the requests answered 429 or 503;
- **a key written twice in a second**: no key written successfully again within a second;
- **listings**: nothing lists the lake or the bucket whole after the first minute of load;
- **keys many nodes add start with a random part**: a folder written by two or more nodes orders its
  names at random (the share of names greater than the one before is 0.9 or less: a time or a
  counter in front gives about 1, random names about 0.5).

Against the simulator (`sim_r2.py --zero --key-writes-per-sec 1`: R2's one write a second to a key)
by default. `--real` runs against the bucket the environment names (AWS_ENDPOINT, PONDRA_BUCKET and
the keys; `.github/workflows/limits-r2.yml` runs it nightly on R2). The JSON is printed at the end
(and written with `--out`); exit 1 when a check fails.

  limits_check.py [--real] [--minutes 5] [--port 9700] [--out FILE] [--keep]
"""
import argparse, itertools, json, os, random, subprocess, sys, threading, time, traceback, urllib.parse
from collections import Counter, defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import harness
from harness import Node, call
from faulty_s3 import Faulty

TABLES = [f"t{i}" for i in range(20)]
MV = "per_writer"  # (a materialized view: the rows per writer of t0)
WRITERS, UPSERTERS, READERS = 48, 8, 4
checks, info = {}, {}


def check(name, ok, detail=None):
    checks[name] = bool(ok)
    print(f"{'ok  ' if ok else 'FAIL'} {name}" + (f": {detail}" if detail is not None and not ok else ""), flush=True)
    return ok


def until(what, secs, every=0.25):
    """`what()` once it is truthy, or None after `secs`."""
    deadline = time.time() + secs
    while time.time() < deadline:
        try:
            if got := what():
                return got
        except Exception:
            pass
        time.sleep(every)
    return None


def stats(nd):
    try:
        return call(nd.port, "GET", "/stats", timeout=3)
    except Exception:
        return None


def one_leader(nodes):
    """The leading node, when every node is up and follows it."""
    s = [stats(nd) for nd in nodes]
    leads = [nd for nd, x in zip(nodes, s) if x and x["role"] == "leader"]
    return leads[0] if len(leads) == 1 and all(x and x["leader"] == s[nodes.index(leads[0])]["leader"] for x in s) else None


def sql(port, q, timeout=120):
    return call(port, "POST", "/sql", q.encode(), timeout=timeout)


def bucket(a):
    """The simulator on --port (no added latency, R2's one write a second to a key) and a bucket in
    it, the environment set as harness.new_lake needs it; or, with --real, the bucket the environment
    names (nothing started)."""
    if a.real:
        return None, os.environ["AWS_ENDPOINT"]
    sim = subprocess.Popen([sys.executable, os.path.join(HERE, "sim_r2.py"), "--port", str(a.port), "--zero", "--key-writes-per-sec", "1"],
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    import boto3
    url = f"http://127.0.0.1:{a.port}"
    s3 = boto3.client("s3", endpoint_url=url, region_name="us-east-1", aws_access_key_id="test", aws_secret_access_key="test")
    until(lambda: s3.list_buckets() is not None, 30)
    s3.create_bucket(Bucket="limits")
    os.environ.update({"AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test", "AWS_REGION": "us-east-1", "AWS_ALLOW_HTTP": "true",
                       "PONDRA_BUCKET": "limits", "AWS_ENDPOINT": url})
    return sim, url


class Load:
    """The writers, upserters, readers and the outside writes, until `finish`. Rows acknowledged per
    table (an upsert counts under `k`); every error counted by who sent it, the first 200 characters
    of the first 20 kept. A failed write is never retried: a retry could apply a row twice."""

    def __init__(self, nodes, lake, env):
        self.nodes, self.lake, self.env = nodes, lake, env
        self.stop, self.lock, self.t0 = threading.Event(), threading.Lock(), None
        self.acked, self.keys, self.errors, self.samples, self.reads, self.outside_ok = Counter(), set(), Counter(), [], 0, 0

    def start(self):
        self.t0 = time.time()
        jobs = [self.writer] * WRITERS + [self.upserter] * UPSERTERS + [self.reader] * READERS + [self.outside]
        self.threads = [threading.Thread(target=job, args=(i,), daemon=True) for i, job in enumerate(jobs)]
        [t.start() for t in self.threads]
        return self

    def ack(self, table):
        with self.lock:
            self.acked[table] += 1

    def fail(self, who, e):
        with self.lock:
            self.errors[who] += 1
            if len(self.samples) < 20:
                self.samples.append(f"{who}: {str(e)[:200]}")

    def writer(self, w):
        n = 0
        while not self.stop.is_set():
            t = random.randrange(len(TABLES))
            try:
                sql(random.choice(self.nodes).port, f"INSERT INTO t{t} VALUES ({w}, {n}, {random.random()})")
                self.ack(f"t{t}")
            except Exception as e:
                self.fail("writer", e)
            n += 1

    def upserter(self, _):
        while not self.stop.is_set():
            key = random.randrange(1000)
            try:
                sql(random.choice(self.nodes).port, f"INSERT INTO k VALUES ({key}, {random.randrange(2 ** 31)})")
                self.ack("k")
                with self.lock:
                    self.keys.add(key)
            except Exception as e:
                self.fail("upserter", e)

    def reader(self, _):
        while not self.stop.is_set():
            port = random.choice(self.nodes).port
            try:
                sql(port, f"SELECT count(*) AS n FROM t{random.randrange(len(TABLES))}", timeout=60)
                sql(port, f"SELECT sum(c) AS n FROM {MV}", timeout=60)
                with self.lock:
                    self.reads += 1
            except Exception as e:
                self.fail("reader", e)

    def outside(self, _):
        """One write a minute from outside the cluster: `pondra sql` on this machine, with node 1's
        environment, so it goes through node 1's proxy too (its requests are in node 1's log)."""
        for k in itertools.count(1):
            if self.stop.wait(max(0.0, self.t0 + 60 * k - time.time())):
                return
            try:
                r = subprocess.run([harness.BIN, "sql", "--lake", self.lake, f"INSERT INTO t0 VALUES (-1, {k}, 0)"],
                                   env=self.env, capture_output=True, text=True, timeout=300)
                if r.returncode == 0:
                    self.ack("t0")
                    with self.lock:
                        self.outside_ok += 1
                else:
                    self.fail("outside", f"exit {r.returncode}: {r.stdout}{r.stderr}")
            except Exception as e:
                self.fail("outside", e)

    def finish(self):
        self.stop.set()
        for t in self.threads:
            t.join(300)


def read_back(nodes, load):
    """Each table, the view and the keyed table counted on every node: what does not match."""
    got = {"rows": [], "view": [], "keys": []}
    for nd in nodes:
        for t in TABLES:
            n = sql(nd.port, f"SELECT count(*) AS n FROM {t}")[0]["n"]
            if n != load.acked[t]:
                got["rows"].append({"node": nd.port, "table": t, "rows": n, "acknowledged": load.acked[t]})
        s = sql(nd.port, f"SELECT sum(c) AS n FROM {MV}")[0]["n"] or 0
        if s != load.acked["t0"]:
            got["view"].append({"node": nd.port, "sum": s, "acknowledged": load.acked["t0"]})
        n = sql(nd.port, "SELECT count(*) AS n FROM k")[0]["n"]
        if n != len(load.keys):
            got["keys"].append({"node": nd.port, "keys": n, "acknowledged": len(load.keys)})
    return got


def settled(nodes, load, secs=60):
    """Read back until the rows and the view are what was acknowledged (a follower may be a moment
    behind the leader's last commit), or `secs` have passed: the last reading either way."""
    deadline = time.time() + secs
    while True:
        got = read_back(nodes, load)
        if not any(got.values()) or time.time() > deadline:
            return got
        time.sleep(1)


def requests_of(proxies):
    """Every request the proxies forwarded, in the order they started, each with its node's number."""
    out = []
    for i, p in enumerate(proxies):
        with p.lock:
            out += [{**r, "node": i + 1} for r in p.log]
    return sorted(out, key=lambda r: r["t0"])


def query(r):
    return urllib.parse.parse_qs(r["query"], keep_blank_values=True)


def head(path, n=2):
    """The first `n` parts of a path: the bucket and the lake's first folder."""
    return "/".join(path.strip("/").split("/")[:n])


def analyse(reqs, load, lake_key):
    """The request log against the limits: the numbers in `info`, the limits in `checks`."""
    if not reqs:
        return check("the proxies logged the requests", False)
    span = max(r["t1"] for r in reqs) - reqs[0]["t0"]
    rows = sum(load.acked.values())
    by_method = Counter(r["method"] for r in reqs)
    info["requests"] = {
        "total": len(reqs), "by method": dict(by_method), "by status": dict(Counter(r["status"] for r in reqs)),
        "by node": {n: {"methods": dict(Counter(r["method"] for r in reqs if r["node"] == n)),
                        "statuses": dict(Counter(r["status"] for r in reqs if r["node"] == n))} for n in (1, 2, 3)},
        "a second, mean": round(len(reqs) / span, 1), "a second, busiest": max(Counter(int(r["t0"]) for r in reqs).values()),
        "acknowledged rows": rows,
        "per 1,000 acknowledged rows, by method": {m: round(1000 * c / rows, 1) for m, c in by_method.items()} if rows else None,
    }

    refused = [r for r in reqs if r["status"] in (429, 503)]
    info["refused"] = {"count": len(refused), "by method": dict(Counter(r["method"] for r in refused)),
                       "by path's first two parts": dict(Counter(head(r["path"]) for r in refused)),
                       "the 10 most refused paths": Counter(r["path"] for r in refused).most_common(10)}
    check("the bucket refused fewer than 1% of requests", len(refused) < 0.01 * len(reqs), f"{len(refused)} of {len(reqs)}")

    # (a key is written by a PUT of an object or a multipart upload's completion, as sim_r2.py counts
    # them: an upload's start and parts, and a bulk delete, a POST to the bucket itself, write none)
    writes = defaultdict(list)
    for r in reqs:
        q = query(r)
        keyed = (r["method"] == "PUT" and "partNumber" not in q) or (r["method"] == "POST" and "uploadId" in q)
        if keyed and 200 <= r["status"] < 300:
            writes[r["path"]].append(r["t0"])
    gaps = []
    for p, ts in writes.items():
        ts = sorted(ts)
        if len(ts) >= 2:
            gaps.append((min(b - a for a, b in zip(ts, ts[1:])), p, len(ts)))
    gaps.sort()
    info["a key written twice within a second"] = {
        "keys written 2 or more times": len(gaps),
        "the 10 smallest gaps between successful writes (s)": [{"path": p, "gap": round(g, 3), "writes": n} for g, p, n in gaps[:10]]}
    check("no key written twice within a second", not gaps or gaps[0][0] >= 1.0, gaps[:3])

    # (the lake's own folder and anything above it: a listing of either is a listing of the whole lake)
    t_after = load.t0 + 60
    listed = [r for r in reqs if "list-type" in query(r) or "prefix" in query(r)]
    prefix = lambda r: query(r).get("prefix", [""])[0]
    after = [r for r in listed if r["t0"] >= t_after]
    whole = [r for r in after if (lake_key + "/").startswith(prefix(r))]
    minutes = max(0.0, max(r["t0"] for r in reqs) - t_after) / 60
    info["listings"] = {"listings": len(listed), "after the first minute": len(after),
                        "per minute after the first minute": round(len(after) / minutes, 2) if minutes else None,
                        "the 10 most listed prefixes": Counter(prefix(r) for r in listed).most_common(10),
                        "whole-lake listings after the first minute": [{"node": r["node"], "t0": round(r["t0"] - load.t0, 1), "prefix": prefix(r)} for r in whole[:10]]}
    check("nothing lists the lake or the bucket whole after the first minute", not whole, len(whole))

    folders = defaultdict(list)
    for r in reqs:
        if r["method"] == "PUT" and "partNumber" not in query(r) and 200 <= r["status"] < 300:
            folder, _, name = r["path"].rpartition("/")
            folders[folder].append((r["t0"], r["node"], name))
    many = {}
    for folder, ws in folders.items():
        nodes_ = sorted({n for _, n, _ in ws})
        if len(nodes_) >= 2 and len(ws) >= 30:
            names = [name for _, _, name in sorted(ws)]
            ascending = sum(b > a for a, b in zip(names, names[1:]))
            many[folder] = {"puts": len(ws), "nodes": nodes_, "share of names ascending": round(ascending / (len(names) - 1), 3)}
    info["folders two or more nodes add to"] = many
    check("keys many nodes add start with a random part", all(v["share of names ascending"] <= 0.9 for v in many.values()),
          {f: v for f, v in many.items() if v["share of names ascending"] > 0.9})

    fan = {}
    for n in (1, 2, 3):
        events = sorted([(r["t0"], 1) for r in reqs if r["node"] == n] + [(r["t1"], -1) for r in reqs if r["node"] == n])
        now = top = 0
        for _, d in events:
            now += d
            top = max(top, now)
        fan[n] = top
    info["requests in flight at once, most, per node"] = fan


def run(a):
    sim, upstream = bucket(a)
    harness.A = argparse.Namespace(s3=True, keep=a.keep)
    lake = harness.new_lake()
    keys = (os.environ["AWS_ACCESS_KEY_ID"], os.environ["AWS_SECRET_ACCESS_KEY"], os.environ.get("AWS_REGION", "auto")) if a.real else None
    proxies = [Faulty(upstream, port=a.port + 10 + i, sign=keys, record=True) for i in range(3)]
    try:
        # (the nodes reach their proxies over plain HTTP on 127.0.0.1: AWS_ALLOW_HTTP is set for them;
        # AWS_ENDPOINT_URL too, as resilience_check sets it: object_store reads either)
        nodes = [Node(lake, a.port + 1 + i, env={"AWS_ALLOW_HTTP": "true", "AWS_ENDPOINT": p.url, "AWS_ENDPOINT_URL": p.url}).start()
                 for i, p in enumerate(proxies)]
        leader = until(lambda: one_leader(nodes), 120)
        check("three nodes on the bucket, one leader", leader)
        check("every node reaches the bucket through its own proxy", all(p.counts for p in proxies), [p.counts for p in proxies])
        if not leader:
            raise RuntimeError("no leader within 120 s")
        for t in TABLES:
            sql(leader.port, f"CREATE TABLE {t} (writer BIGINT, n BIGINT, v DOUBLE)")
        sql(leader.port, "CREATE TABLE k (id BIGINT PRIMARY KEY, v BIGINT)")
        sql(leader.port, f"CREATE MATERIALIZED VIEW {MV} AS SELECT writer, count(*) AS c FROM t0 GROUP BY writer")
        seen = until(lambda: all(sql(nd.port, f"SELECT count(*) AS n FROM {t}") for nd in nodes for t in TABLES + ["k", MV]), 60)
        check("every node sees the tables and the view", seen)

        load = Load(nodes, lake, nodes[0].env).start()
        t_end = load.t0 + a.minutes * 60
        while time.time() < t_end:
            time.sleep(min(60, max(0.0, t_end - time.time())))
            print(f"{round(time.time() - load.t0)} s: {sum(load.acked.values())} rows acknowledged, errors {dict(load.errors)}", flush=True)
        load.finish()
        info["load (s)"] = round(time.time() - load.t0)
        info["acknowledged rows, by table"] = dict(load.acked)
        info["outside writes acknowledged"] = load.outside_ok
        info["reads answered"] = load.reads
        info["errors, by who"] = dict(load.errors)
        info["error samples (first 200 characters)"] = load.samples

        t_drain = time.time()
        lead = lambda: one_leader(nodes) or leader
        drained = until(lambda: (stats(lead()) or {}).get("untiered_rows") == 0, 240)
        info["the log drained after the load (s)"] = round(time.time() - t_drain, 1) if drained else None
        check("the log drains into files after the load (within 240 s)", drained, stats(lead()))

        got = settled(nodes, load)
        info["keyed table: distinct keys acknowledged"] = len(load.keys)
        check("every acknowledged row is there once, on every node", not got["rows"], got["rows"][:10])
        check("per_writer's rows add up to the acknowledged rows of t0, on every node", not got["view"], got["view"][:10])
        check("the keyed table holds every key acknowledged, once, on every node", not got["keys"], got["keys"][:10])
        check("no write was refused: the writers, upserters and outside writes saw no errors",
              not (load.errors["writer"] or load.errors["upserter"] or load.errors["outside"]), load.samples[:5])
        check("the readers saw no errors", not load.errors["reader"], load.samples[:5])

        reqs = requests_of(proxies)
        if a.dump:  # (every request as a line of JSON: for reading what a check counted)
            with open(a.dump, "w") as f:
                f.writelines(json.dumps(r) + "\n" for r in reqs)
        analyse(reqs, load, lake[5:].split("/", 1)[1])
    finally:
        for nd in harness.NODES:
            nd.kill()
        for p in proxies:
            p.close()
        harness.clean_up()  # (the lake goes while the simulator still runs)
        harness.LAKES.clear()
        if sim:
            sim.kill()


def main(a):
    t0 = time.time()
    try:
        run(a)
    except Exception as e:
        traceback.print_exc()
        check("the run got to its end", False, repr(e)[-2000:])
    info["took (s)"] = round(time.time() - t0)
    ok = bool(checks) and all(checks.values())
    out = json.dumps({"limits": checks, "info": info, "ok": ok}, indent=1, default=str)
    print(out, flush=True)
    if a.out:
        with open(a.out, "w") as f:
            f.write(out + "\n")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--real", action="store_true", help="the bucket the environment names (AWS_ENDPOINT, PONDRA_BUCKET, AWS_ACCESS_KEY_ID, "
                    "AWS_SECRET_ACCESS_KEY, AWS_REGION): R2 or S3; nothing is started")
    ap.add_argument("--minutes", type=float, default=5, help="how long the load runs")
    ap.add_argument("--port", type=int, default=9700, help="the simulator's port; nodes at +1 to +3, proxies at +10 to +12")
    ap.add_argument("--out", help="also write the JSON here")
    ap.add_argument("--dump", help="also write every logged request, one JSON line each, here")
    ap.add_argument("--keep", action="store_true", help="keep the lake (and the node logs)")
    main(ap.parse_args())
