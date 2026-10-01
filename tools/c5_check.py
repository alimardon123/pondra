#!/usr/bin/env python3
"""The bucket's limits (C5): a node keeps within them, and every statement still succeeds.

Against the simulator (`sim_r2.py`) at a low write rate, 503 SlowDown above it, and R2's one
write a second to a key (429):

- **slow down**: 16 clients send 240 single-row INSERTs at once to one node; every one succeeds,
  the node says it slowed down, and once the store first refused, the writes it is sent each
  second stay near the rate (it backs off within a second, retries and all: `budget.rs`);
- **the bell**: four `pondra sql` writers that can't reach the leader leave their INSERTs in the
  bucket's inbox at once, each ringing `inbox/bell`; a ring refused (429) counts as rung, so
  every one is answered.

  c5_check.py [--bin target/release/pondra] [--rate 30] [--port 9581]
"""
import argparse, json, os, subprocess, sys, tempfile, threading, time, urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
ap = argparse.ArgumentParser()
ap.add_argument("--bin", default=os.path.join(HERE, "..", "target", "release", "pondra"))
ap.add_argument("--rate", type=int, default=10, help="writes a second the simulated bucket takes")
ap.add_argument("--port", type=int, default=9581)
A = ap.parse_args()
SP, NP = A.port, A.port - 1000
env = {**os.environ, "AWS_ENDPOINT": f"http://127.0.0.1:{SP}", "AWS_ACCESS_KEY_ID": "k", "AWS_SECRET_ACCESS_KEY": "s", "AWS_REGION": "us-east-1", "AWS_ALLOW_HTTP": "true"}
work = tempfile.mkdtemp(prefix="pondra-c5-")
procs = []


def http(port, path, body=None, timeout=120):
    req = urllib.request.Request(f"http://127.0.0.1:{port}{path}", data=body.encode() if isinstance(body, str) else body, method="POST" if body is not None else "GET")
    return urllib.request.urlopen(req, timeout=timeout).read()


def node(lake, *flags):
    log = open(os.path.join(work, f"node-{len(procs)}.log"), "w")
    p = subprocess.Popen([A.bin, "serve", "--lake", lake, "--addr", f"127.0.0.1:{NP}", *flags], env=env, stdout=subprocess.DEVNULL, stderr=log)
    procs.append(p)
    for _ in range(600):
        try:
            http(NP, "/stats", timeout=2)
            return p, log.name
        except Exception:
            time.sleep(0.1)
    raise RuntimeError("the node didn't start: " + open(log.name).read()[-800:])


sim = subprocess.Popen([sys.executable, os.path.join(HERE, "sim_r2.py"), "--port", str(SP), "--put-p50", "40", "--get-p50", "25", "--writes-per-sec", str(A.rate), "--key-writes-per-sec", "1"],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
procs.append(sim)
checks, info = {}, {}
try:
    for _ in range(100):
        try:
            http(SP, "/__sim/stats", timeout=1)
            break
        except Exception:
            time.sleep(0.1)
    import boto3
    boto3.client("s3", endpoint_url=env["AWS_ENDPOINT"], region_name="us-east-1", aws_access_key_id="k", aws_secret_access_key="s").create_bucket(Bucket="c5check")

    # Slow down: more writes than the bucket takes, at once.
    p, log = node("s3://c5check/lake")
    http(NP, "/sql", "CREATE TABLE t (id BIGINT, at DOUBLE)")
    done, failed = [], []
    def client(c):
        for i in range(15):
            try:
                http(NP, "/sql", f"INSERT INTO t VALUES ({c * 100 + i}, {time.time()})", timeout=300)
                done.append(1)
            except Exception as e:
                failed.append(str(e)[:200])
    t0 = time.time()
    ts = [threading.Thread(target=client, args=(c,)) for c in range(16)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    took = time.time() - t0
    rows = json.loads(http(NP, "/sql", "SELECT count(*) AS n FROM t"))
    stats = json.loads(http(SP, "/__sim/stats"))
    seconds = {int(k): v for k, v in stats["second"].items()}
    first = min((s for s, v in seconds.items() if v[1] or v[2]), default=None)
    after = [v[0] for s, v in sorted(seconds.items()) if first is not None and first < s < int(t0 + took)]
    said = open(log).read()
    info["slow down"] = {"took_s": round(took, 1), "failed": failed[:3], "503": stats.get("SlowDown", 0), "429": stats.get("TooManyRequests", 0), "writes a second after the first refusal": after}
    checks["240 INSERTs at once against a bucket that takes %d writes a second: every one succeeds; the node slows down (it says so); the writes it sends stay near the rate" % A.rate] = \
        len(done) == 240 and not failed and rows == [{"n": 240}] and "slow down" in said and (not after or sum(after) / len(after) <= 3 * A.rate)
    p.terminate()
    p.wait()

    # The bell: writers that can't reach the leader ring it at once.
    p, log = node("s3://c5check/lake2", "--advertise", "127.0.0.1:9")  # (nobody reaches it there)
    http(NP, "/sql", "CREATE TABLE u (id BIGINT)")
    outs = {}
    def writer(i):
        r = subprocess.run([A.bin, "sql", "--dir", "s3://c5check/lake2", f"INSERT INTO u VALUES ({i})"], env=env, capture_output=True, text=True, timeout=180)
        outs[i] = (r.returncode, (r.stdout + r.stderr)[-300:])
    ts = [threading.Thread(target=writer, args=(i,)) for i in range(4)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    rows = json.loads(http(NP, "/sql", "SELECT count(*) AS n FROM u"))
    stats = json.loads(http(SP, "/__sim/stats"))
    info["bell"] = {"writers": outs, "429 in all": stats.get("TooManyRequests", 0)}
    checks["four writers through the bucket's inbox at once: each answered, the bell's 429s counted as rung"] = all(c == 0 for c, _ in outs.values()) and rows == [{"n": 4}]
finally:
    for p in reversed(procs):
        if p.poll() is None:
            p.terminate()
            try:
                p.wait(timeout=20)
            except subprocess.TimeoutExpired:
                p.kill()
ok = all(checks.values()) and bool(checks)
print(json.dumps({"c5": checks, "ok": ok, "info": info}, indent=1, default=str))
sys.exit(0 if ok else 1)
