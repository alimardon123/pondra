#!/usr/bin/env python3
"""A soak (C4, ADR-039): a cluster under steady load for hours, its nodes stopped and killed now
and then, and what drifts over time measured.

  soak.py [--hours 24 | --minutes 10] [--nodes 3] [--rate 500] [--s3] [--out soak.json]
          [--resume STATE] [--save STATE]

  Steady ingest: producers append to `ev` at --rate rows a second in all, each batch once (a
  producer's sequence, retried on any live node until acknowledged); an upserting producer keeps
  `kv`, 1,000 keys, against a model; two views follow `ev` (a GROUP BY and a filter); readers ask
  every node that each producer's batches are a prefix with no gaps. Tiering, merging and
  compaction run as they do by default. Every --failover-mins one node goes, in turn: the leader
  stopped (SIGTERM: drained, stepping down), the leader killed (kill -9), a follower stopped, a
  follower killed; it starts again a moment later (by default 8 times a run, at most hourly).

  Each minute it writes a line to --timeline: every node's resident memory, the leader's rows not
  yet tiered and its commit times, the longest a batch waited for its acknowledgement, the lake's
  objects (a folder's; a bucket's every 30 minutes).

  At the end the load stops and it checks: every acknowledged batch once and `kv` as the model,
  on every node; the views equal their rows; the log drained (no rows left untiered within two
  minutes); memory flat (each node process that lived ten samples: its own memory, less its
  caches (the hot columns, and what `pondra_cache_bytes` counts), in the last tenth of its
  life within 1.5x of its second tenth's, after the warm-up, plus 64 MB). It reports the
  lake's objects (data files, log segments, the catalog's) and every stop's longest wait.
  Prints the checks as JSON (and writes them to --out) and exits 1 if one fails.

--s3: the lake is s3://$PONDRA_BUCKET/soak-<id> (AWS_* point at R2, MinIO or tools/sim_r2.py),
deleted at the end unless --keep.

Legs (a day where a machine lasts six hours: GitHub's runners, soak.yml): --save ends a leg with
its checks, stops the nodes (followers, then the leader: drained, stepping down) and writes what
the next leg needs (the lake, every producer's acknowledged batches, the model, the time so far) to
STATE, keeping the lake; --resume STATE starts the next on that lake, the load going on where it
stopped. The lake lives the whole day; each node process lives a leg at most.
"""
import argparse, collections, json, os, random, shutil, sys, tempfile, threading, time, uuid

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from upgrade_check import Load, Node, stop_all, until  # noqa: E402  (the same nodes and load)

HERE = os.path.dirname(os.path.abspath(__file__))


def rss_mb(n, kind="VmRSS"):
    """A node's resident memory (Linux), or None; `RssAnon`: its own, without the pages of the
    binary's code, which the kernel maps from the file (about 130 MB once the paths have run)."""
    try:
        with open(f"/proc/{n.p.pid}/status") as f:
            return next(int(l.split()[1]) // 1024 for l in f if l.startswith(kind + ":"))
    except (OSError, StopIteration, AttributeError):
        return None


def gauges(x):
    """A node's `/metrics`, {name with its labels: value}, or {}."""
    try:
        text = x.get("/metrics", timeout=10).decode()
        return {l.rsplit(" ", 1)[0]: float(l.rsplit(" ", 1)[1]) for l in text.splitlines() if l.startswith("pondra_")}
    except Exception:
        return {}


def objects(lake):
    """The lake's objects: {"all": n, "data": n, "log": n} (a bucket listed once)."""
    if lake.startswith("s3://"):
        import boto3
        bucket, prefix = lake[5:].split("/", 1)
        s3 = boto3.client("s3", endpoint_url=os.environ.get("AWS_ENDPOINT_URL") or os.environ.get("AWS_ENDPOINT"))
        keys = [o["Key"][len(prefix) + 1:] for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix + "/") for o in page.get("Contents", [])]
    else:
        keys = [os.path.relpath(os.path.join(d, f), lake) for d, _, fs in os.walk(lake) for f in fs]
    count = collections.Counter(k.split("/")[0] for k in keys)
    return {"all": len(keys), "data": count["data"], "log": count["log"], "catalog": count["catalog"]}


class Upserts(threading.Thread):
    """One producer upserting `kv` (1,000 keys), each batch once; the model is what was acknowledged."""

    def __init__(self, nodes, rate, model=None, seq=0):
        super().__init__(daemon=True)
        self.nodes, self.rate, self.model, self.stop_, self.seq = nodes, rate, model or {}, threading.Event(), seq

    def run(self):
        while not self.stop_.is_set():
            self.seq += 1
            rows = {random.randrange(1000): self.seq * 10 + i for i in range(10)}
            body = "".join(json.dumps({"k": k, "v": v}) + "\n" for k, v in rows.items())
            t0 = time.time()
            while True:  # (until acknowledged, as `Load`'s: a batch given up on may still have committed)
                try:
                    random.choice(self.nodes).call("POST", f"/append/kv?producer=upserts&seq={self.seq}", body, timeout=20)
                    self.model.update(rows)
                    break
                except Exception:
                    time.sleep(0.1)
            time.sleep(max(0.0, t0 + 1 / self.rate - time.time()))


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--hours", type=float, default=0)
    ap.add_argument("--minutes", type=float, default=10)
    ap.add_argument("--nodes", type=int, default=3)
    ap.add_argument("--rate", type=int, default=500, help="rows a second into `ev`, in all")
    ap.add_argument("--batch", type=int, default=20, help="rows a batch (each batch is a commit: on a bucket, about an object write)")
    ap.add_argument("--failover-mins", type=float, default=0, help="a node stopped or killed this often (default: 8 times a run, at most hourly)")
    ap.add_argument("--bin", default=os.environ.get("PONDRA_BIN", os.path.join(HERE, "..", "target", "release", "pondra")))
    ap.add_argument("--s3", action="store_true")
    ap.add_argument("--keep", action="store_true", help="keep the lake and logs")
    ap.add_argument("--port", type=int, default=9760)
    ap.add_argument("--out", default="soak.json")
    ap.add_argument("--timeline", default="soak-timeline.jsonl")
    ap.add_argument("--resume", help="a leg's STATE: go on with its lake")
    ap.add_argument("--save", help="end this leg: keep the lake, and write what the next needs here")
    A = ap.parse_args()
    was = json.load(open(A.resume)) if A.resume else {"elapsed": 0, "moves": 0, "legs": []}
    secs = A.hours * 3600 if A.hours else A.minutes * 60
    every = A.failover_mins * 60 or min(3600, secs / 8)
    work = tempfile.mkdtemp(prefix="pondra-soak-")
    lake = was.get("lake") or (f"s3://{os.environ['PONDRA_BUCKET']}/soak-{uuid.uuid4().hex[:8]}" if A.s3 else os.path.join(work, "lake"))
    binary = os.path.abspath(A.bin)
    nodes = [Node(binary, lake, A.port + i, work, tier_secs=10).start() for i in range(A.nodes)]
    first = nodes[0]
    if not A.resume:
        first.post("/tables/ev", json.dumps([["producer", "Utf8"], ["seq", "Int64"], ["i", "Int64"]]))
        first.q("CREATE TABLE kv (k BIGINT PRIMARY KEY, v BIGINT)")
        first.q("CREATE MATERIALIZED VIEW per_producer AS SELECT producer, count(*) AS n, max(seq) AS mx FROM ev GROUP BY producer")
        first.q("CREATE MATERIALIZED VIEW tens AS SELECT producer, seq, i FROM ev WHERE i = 0 AND seq % 10 = 0")
    until(lambda: all(x.q("SELECT count(*) AS n FROM ev") for x in nodes), 120)
    live = list(nodes)
    producers, size = 4, A.batch
    load = Load(live, "ev", producers=producers, readers=2, size=size, rate=max(0.1, A.rate / producers / size), acked=was.get("acked")).start()
    ups = Upserts(live, rate=2, model={int(k): v for k, v in was.get("kv", {}).items()}, seq=was.get("kv_seq", 0))
    ups.start()
    role = lambda x: x.get("/stats", timeout=5)["role"]
    timeline, moves, start = open(A.timeline, "w"), [], time.time()
    kinds = [("leader", "term"), ("leader", "kill"), ("follower", "term"), ("follower", "kill")]
    next_move, last_list, samples, wrote, mark = start + every, 0, [], {}, start
    while time.time() - start < secs:
        time.sleep(max(0, mark + (60 if secs > 600 else 10) - time.time()))
        leader = next((x for x in live if role_or(x, role) == "leader"), None)
        stats = leader.get("/stats", timeout=10) if leader else {}
        got = {x.port: gauges(x) for x in live}
        listed, now_wrote = None, {p: int(g['pondra_object_requests_total{op="write"}']) if 'pondra_object_requests_total{op="write"}' in g else None for p, g in got.items()}
        # (a node started again counts from 0: its writes since are its count)
        made = sum(w - (wrote.get(p) or 0) if w >= (wrote.get(p) or 0) else w for p, w in now_wrote.items() if w is not None)
        span, wrote = time.time() - (samples[-1]["at"] if samples else start), now_wrote
        if not A.s3 or time.time() - last_list > 1800:
            listed, last_list = objects(lake), time.time()
        line = {"t": round(was["elapsed"] + time.time() - start), "at": time.time(), "object_writes_per_s": round(made / max(span, 1), 1), "rss_mb": {x.port: rss_mb(x) for x in live}, "anon_mb": {x.port: rss_mb(x, "RssAnon") for x in live}, "hot_mb": {p: round(g.get("pondra_hot_bytes", 0) / 2**20) for p, g in got.items()}, "cache_mb": {p: round(sum(v for k, v in g.items() if k.startswith("pondra_cache_bytes")) / 2**20) for p, g in got.items()}, "pids": {x.port: x.p.pid for x in live},
                "untiered_rows": stats.get("untiered_rows"), "commit_ms_p95": stats.get("commit_ms_p95"),
                "longest_ack_s": load.longest(mark), "acked": sum(load.acked.values()),
                "torn": len(load.torn), **({"objects": listed} if listed else {})}
        for m in moves:  # (a stop's longest wait, once its batches have all been answered)
            if "window" in m and m["window"][1] < mark:
                m["longest_ack_s"] = load.longest(*m.pop("window"))
        mark = time.time()  # (the next line's waits start here: a stop below counts in it)
        samples.append(line)
        timeline.write(json.dumps(line) + "\n")
        timeline.flush()
        if time.time() >= next_move and len(live) == A.nodes:
            who, how = kinds[(was["moves"] + len(moves)) % len(kinds)]
            x = next((y for y in live if (role_or(y, role) == "leader") == (who == "leader")), None)
            if x:
                t0 = time.time()
                live.remove(x)
                x.stop(how)
                time.sleep(2)
                y = Node(binary, lake, x.port, work, tier_secs=10).start()
                until(lambda: y.plain("GET", "/ready")[0] == 200, 60)
                live.append(y)
                moves.append({"at_s": round(was["elapsed"] + t0 - start), "node": f"{who} {x.port}", "how": "stopped" if how == "term" else "killed", "window": (t0 - 0.5, time.time())})
            next_move += every
    ups.stop_.set()
    for m in moves:
        if "window" in m:
            m["longest_ack_s"] = load.longest(*m.pop("window"))
    # (asked of the leader: it holds every commit it acknowledged, a follower a moment later)
    leader = next(x for x in live if role_or(x, role) == "leader")
    result = load.finish(leader)
    ups.join(30)
    checks, info = {}, {"moves": moves, **{k: v for k, v in result.items() if k != "reads"}, "reads": result["reads"]}
    drained = until(lambda: leader.get("/stats")["untiered_rows"] == 0, 120, step=2)
    same = lambda sql: all(x.q(sql) == live[0].q(sql) for x in live)
    kv = {r["k"]: r["v"] for r in live[0].q("SELECT k, v FROM kv")}
    # (…and on every node once the log drained: a batch lost from the tail would leave no gap)
    held = lambda x: sorted((r["producer"], r["n"]) for r in x.q("SELECT producer, count(*) AS n FROM ev GROUP BY producer"))
    everywhere = all(held(x) == sorted((p, seq * size) for p, seq in load.acked.items()) for x in live)
    checks["every acknowledged batch once, no torn read"] = not result["lost or twice"] and not result["torn reads"] and everywhere
    checks["the keyed table equals its model, on every node"] = kv == ups.model and same("SELECT k, v FROM kv ORDER BY k")
    checks["the views equal their rows, on every node"] = (live[0].q("SELECT * FROM per_producer ORDER BY producer") == live[0].q("SELECT producer, count(*) AS n, max(seq) AS mx FROM ev GROUP BY producer ORDER BY producer")
                                                         and live[0].q("SELECT count(*) AS n FROM tens") == live[0].q("SELECT count(*) AS n FROM ev WHERE i = 0 AND seq % 10 = 0")
                                                         and same("SELECT count(*) AS n FROM ev") and same("SELECT * FROM per_producer ORDER BY producer"))
    checks["the log drains: no rows left untiered two minutes after the load"] = bool(drained)
    # Memory: each process's own (a node started again is another) over its life, if it lived ten
    # samples: the median of its last tenth against that of its second (the first is warm-up),
    # less its hot columns: a cache of the columns read lately, which grows with the tables up to
    # its own limit (hot.rs) and gives memory back past three fifths of the machine; and less the
    # node's other bounded caches (`pondra_cache_bytes`: objects' byte ranges, log rows, answers),
    # which fill up to their limits.
    lives = collections.defaultdict(list)
    for s in samples:
        for port, pid in s["pids"].items():
            own = s.get("anon_mb", s["rss_mb"]).get(port)
            if own:
                lives[(port, pid)].append(own - s.get("hot_mb", {}).get(port, 0) - s.get("cache_mb", {}).get(port, 0))
    median = lambda xs: sorted(xs)[len(xs) // 2]
    drift = {f"{port} (pid {pid})": {"early_mb": median(xs[len(xs) // 10:max(2, 2 * len(xs) // 10)]), "late_mb": median(xs[-max(1, len(xs) // 10):]), "samples": len(xs)}
             for (port, pid), xs in lives.items() if len(xs) >= 10}
    info["memory"] = drift
    info["hot columns at the end, MB"] = samples[-1].get("hot_mb") if samples else None
    info["caches at the end, MB"] = samples[-1].get("cache_mb") if samples else None
    checks["memory flat (its own, less its caches): each node's last tenth within 1.5x (+64 MB) of its second"] = bool(drift) and all(d["late_mb"] <= d["early_mb"] * 1.5 + 64 for d in drift.values())
    end = objects(lake)
    info["objects at the end"] = end
    info["object writes a second (every node)"] = round(sum(s["object_writes_per_s"] for s in samples[1:]) / max(1, len(samples) - 1), 1)
    info["longest wait for an acknowledgement, s"] = max((s["longest_ack_s"] for s in samples), default=0)
    info["minutes"] = round((time.time() - start) / 60, 1)
    # (followers first: the leader then drains, steps down, and the next leg's nodes lead at once)
    [x.stop() for x in sorted(live, key=lambda x: role_or(x, role) == "leader")]
    ok = all(checks.values())
    legs = was["legs"] + [{"ok": ok, "checks": checks, "minutes": info["minutes"]}]
    out = {"ok": ok and all(leg["ok"] for leg in legs), "checks": checks, "info": info, "lake": lake, **({"legs": legs} if len(legs) > 1 or A.save else {})}
    json.dump(out, open(A.out, "w"), indent=1)
    print(json.dumps(out, indent=1))
    if A.save and ok:
        json.dump({"lake": lake, "acked": dict(load.acked), "kv": ups.model, "kv_seq": ups.seq, "elapsed": was["elapsed"] + time.time() - start,
                   "moves": was["moves"] + len(moves), "legs": legs}, open(A.save, "w"))
    elif not A.keep:
        remove(lake)
        if ok:
            shutil.rmtree(work, ignore_errors=True)
    if not ok:
        print(f"(node logs kept in {work})", file=sys.stderr)
    sys.exit(0 if ok else 1)


def remove(lake):
    """The lake, gone (a bucket's prefix: every object under it)."""
    if not lake.startswith("s3://"):
        return shutil.rmtree(lake, ignore_errors=True)
    import boto3
    bucket, prefix = lake[5:].split("/", 1)
    s3 = boto3.client("s3", endpoint_url=os.environ.get("AWS_ENDPOINT_URL") or os.environ.get("AWS_ENDPOINT"))
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix + "/"):
        keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]
        if keys:
            s3.delete_objects(Bucket=bucket, Delete={"Objects": keys})


def role_or(x, role):
    try:
        return role(x)
    except Exception:
        return None


if __name__ == "__main__":
    try:
        main()
    finally:
        stop_all()
