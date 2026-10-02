#!/usr/bin/env python3
"""A soak (C4, ADR-039): a cluster under steady load for hours, its nodes stopped and killed now
and then, and what drifts over time measured.

  soak.py [--hours 24 | --minutes 10] [--nodes 3] [--rate 500] [--s3] [--out soak.json]

  Steady ingest: producers append to `ev` at --rate rows a second in all, each batch once (a
  producer's sequence, retried on any live node until acknowledged); an upserting producer keeps
  `kv`, 1,000 keys, against a model; two views follow `ev` (a GROUP BY and a filter); readers ask
  every node that each producer's batches are a prefix with no gaps. Tiering, merging and
  compaction run as they do by default. Every --failover-mins one node goes, in turn: the leader
  stopped (SIGTERM: drained, stepping down), the leader killed (kill -9), a follower stopped, a
  follower killed; it starts again a moment later.

  Each minute it writes a line to --timeline: every node's resident memory, the leader's rows not
  yet tiered and its commit times, the longest a batch waited for its acknowledgement, the lake's
  objects (a folder's; a bucket's every 30 minutes).

  At the end the load stops and it checks: every acknowledged batch once and `kv` as the model,
  on every node; the views equal their rows; the log drained (no rows left untiered within two
  minutes); memory flat (each node's in the last tenth of the run within 1.5x of the second tenth's,
  after the warm-up, plus 64 MB). It reports the lake's objects (data files, log segments, the
  catalog's) and every stop's longest wait. Prints the checks as JSON (and writes them to --out)
  and exits 1 if one fails.

--s3: the lake is s3://$PONDRA_BUCKET/soak-<id> (AWS_* point at R2, MinIO or tools/sim_r2.py),
deleted at the end unless --keep.
"""
import argparse, collections, json, os, random, shutil, sys, tempfile, threading, time, uuid

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from upgrade_check import Load, Node, stop_all, until  # noqa: E402  (the same nodes and load)

HERE = os.path.dirname(os.path.abspath(__file__))


def rss_mb(n):
    """A node's resident memory (Linux), or None."""
    try:
        with open(f"/proc/{n.p.pid}/status") as f:
            return next(int(l.split()[1]) // 1024 for l in f if l.startswith("VmRSS:"))
    except (OSError, StopIteration, AttributeError):
        return None


def writes(x):
    """The object-store writes a node made since it started (`/metrics`), or None."""
    try:
        text = x.plain("GET", "/metrics")[2]
        return int(float(next(l.split()[-1] for l in text.splitlines() if l.startswith('pondra_object_requests_total{op="write"}'))))
    except Exception:
        return None


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

    def __init__(self, nodes, rate):
        super().__init__(daemon=True)
        self.nodes, self.rate, self.model, self.stop_, self.seq = nodes, rate, {}, threading.Event(), 0

    def run(self):
        while not self.stop_.is_set():
            self.seq += 1
            rows = {random.randrange(1000): self.seq * 10 + i for i in range(10)}
            body = "".join(json.dumps({"k": k, "v": v}) + "\n" for k, v in rows.items())
            t0 = time.time()
            while not self.stop_.is_set():
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
    ap.add_argument("--failover-mins", type=float, default=0, help="a node stopped or killed this often (default: 8 times a run, at most every 30 minutes)")
    ap.add_argument("--bin", default=os.environ.get("PONDRA_BIN", os.path.join(HERE, "..", "target", "release", "pondra")))
    ap.add_argument("--s3", action="store_true")
    ap.add_argument("--keep", action="store_true", help="keep the lake and logs")
    ap.add_argument("--port", type=int, default=9760)
    ap.add_argument("--out", default="soak.json")
    ap.add_argument("--timeline", default="soak-timeline.jsonl")
    A = ap.parse_args()
    secs = A.hours * 3600 if A.hours else A.minutes * 60
    every = A.failover_mins * 60 or min(1800, secs / 8)
    work = tempfile.mkdtemp(prefix="pondra-soak-")
    lake = f"s3://{os.environ['PONDRA_BUCKET']}/soak-{uuid.uuid4().hex[:8]}" if A.s3 else os.path.join(work, "lake")
    binary = os.path.abspath(A.bin)
    nodes = [Node(binary, lake, A.port + i, work, tier_secs=10).start() for i in range(A.nodes)]
    first = nodes[0]
    first.post("/tables/ev", json.dumps([["producer", "Utf8"], ["seq", "Int64"], ["i", "Int64"]]))
    first.q("CREATE TABLE kv (k BIGINT PRIMARY KEY, v BIGINT)")
    first.q("CREATE MATERIALIZED VIEW per_producer AS SELECT producer, count(*) AS n, max(seq) AS mx FROM ev GROUP BY producer")
    first.q("CREATE MATERIALIZED VIEW tens AS SELECT producer, seq, i FROM ev WHERE i = 0 AND seq % 10 = 0")
    until(lambda: all(x.q("SELECT count(*) AS n FROM ev") for x in nodes), 30)
    live = list(nodes)
    producers, size = 4, 20
    load = Load(live, "ev", producers=producers, readers=2, size=size, rate=max(0.1, A.rate / producers / size)).start()
    ups = Upserts(live, rate=2)
    ups.start()
    role = lambda x: x.get("/stats", timeout=5)["role"]
    timeline, moves, start = open(A.timeline, "w"), [], time.time()
    kinds = [("leader", "term"), ("leader", "kill"), ("follower", "term"), ("follower", "kill")]
    next_move, last_list, samples, wrote = start + every, 0, [], {}
    while time.time() - start < secs:
        minute = time.time()
        time.sleep(max(0, 60 - (time.time() - minute)) if secs > 600 else 10)
        leader = next((x for x in live if role_or(x, role) == "leader"), None)
        stats = leader.get("/stats", timeout=10) if leader else {}
        listed, now_wrote = None, {x.port: writes(x) for x in live}
        # (a node started again counts from 0: its writes since are its count)
        made = sum(w - (wrote.get(p) or 0) if w >= (wrote.get(p) or 0) else w for p, w in now_wrote.items() if w is not None)
        span, wrote = time.time() - (samples[-1]["at"] if samples else start), now_wrote
        if not A.s3 or time.time() - last_list > 1800:
            listed, last_list = objects(lake), time.time()
        line = {"t": round(time.time() - start), "at": time.time(), "object_writes_per_s": round(made / max(span, 1), 1), "rss_mb": {x.port: rss_mb(x) for x in live}, "untiered_rows": stats.get("untiered_rows"),
                "commit_ms_p95": stats.get("commit_ms_p95"), "longest_ack_s": load.longest(minute), "acked": sum(load.acked.values()),
                "torn": len(load.torn), **({"objects": listed} if listed else {})}
        samples.append(line)
        timeline.write(json.dumps(line) + "\n")
        timeline.flush()
        if time.time() >= next_move and len(live) == A.nodes:
            who, how = kinds[len(moves) % len(kinds)]
            x = next((y for y in live if (role_or(y, role) == "leader") == (who == "leader")), None)
            if x:
                t0 = time.time()
                live.remove(x)
                x.stop(how)
                time.sleep(2)
                y = Node(binary, lake, x.port, work, tier_secs=10).start()
                until(lambda: y.plain("GET", "/ready")[0] == 200, 60)
                live.append(y)
                moves.append({"at_s": round(t0 - start), "node": f"{who} {x.port}", "how": "stopped" if how == "term" else "killed", "longest_ack_s": load.longest(t0 - 0.5)})
            next_move += every
    ups.stop_.set()
    result = load.finish(live[0])
    ups.join(30)
    checks, info = {}, {"moves": moves, **{k: v for k, v in result.items() if k != "reads"}, "reads": result["reads"]}
    leader = next(x for x in live if role(x) == "leader")
    drained = until(lambda: leader.get("/stats")["untiered_rows"] == 0, 120, step=2)
    same = lambda sql: all(x.q(sql) == live[0].q(sql) for x in live)
    kv = {r["k"]: r["v"] for r in live[0].q("SELECT k, v FROM kv")}
    checks["every acknowledged batch once, no torn read"] = not result["lost or twice"] and not result["torn reads"]
    checks["the keyed table equals its model, on every node"] = kv == ups.model and same("SELECT k, v FROM kv ORDER BY k")
    checks["the views equal their rows, on every node"] = (live[0].q("SELECT * FROM per_producer ORDER BY producer") == live[0].q("SELECT producer, count(*) AS n, max(seq) AS mx FROM ev GROUP BY producer ORDER BY producer")
                                                         and live[0].q("SELECT count(*) AS n FROM tens") == live[0].q("SELECT count(*) AS n FROM ev WHERE i = 0 AND seq % 10 = 0")
                                                         and same("SELECT count(*) AS n FROM ev") and same("SELECT * FROM per_producer ORDER BY producer"))
    checks["the log drains: no rows left untiered two minutes after the load"] = bool(drained)
    # Memory: each node's samples in the last tenth against the second tenth (the first is warm-up).
    tenth = max(1, len(samples) // 10)
    drift = {}
    for port in {p for s in samples for p in s["rss_mb"]}:
        early = [s["rss_mb"][port] for s in samples[tenth:2 * tenth] if s["rss_mb"].get(port)]
        late = [s["rss_mb"][port] for s in samples[-tenth:] if s["rss_mb"].get(port)]
        if early and late:
            drift[port] = {"early_mb": sorted(early)[len(early) // 2], "late_mb": sorted(late)[len(late) // 2]}
    info["memory"] = drift
    checks["memory flat: each node's last tenth within 1.5x (+64 MB) of its second"] = all(d["late_mb"] <= d["early_mb"] * 1.5 + 64 for d in drift.values())
    end = objects(lake)
    info["objects at the end"] = end
    info["object writes a second (every node)"] = round(sum(s["object_writes_per_s"] for s in samples[1:]) / max(1, len(samples) - 1), 1)
    info["longest wait for an acknowledgement, s"] = max((s["longest_ack_s"] for s in samples), default=0)
    info["minutes"] = round((time.time() - start) / 60, 1)
    [x.stop() for x in live]
    ok = all(checks.values())
    out = {"ok": ok, "checks": checks, "info": info, "lake": lake}
    json.dump(out, open(A.out, "w"), indent=1)
    print(json.dumps(out, indent=1))
    if not A.keep:
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
