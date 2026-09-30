#!/usr/bin/env python3
"""What publishing a keyed table every tier round costs (ADR-029 §4, `tier::shadow`): a 2M-row keyed
table, compacted, then rounds of upserts to random keys; each round's time (fold, positions found by
key, Iceberg and Delta published) against the same table unpublished (no positions, nothing
published), on one node on local disk.

  keyed_publish.py [path to the pondra binary]"""
import json, http.client, random, time, sys, statistics, subprocess, tempfile, os
BIN = sys.argv[1] if len(sys.argv) > 1 else os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "target", "release", "pondra")
def node(port):
    d = tempfile.mkdtemp(prefix="pondra-shadow-")
    p = subprocess.Popen([BIN, "serve", d, "--addr", f"127.0.0.1:{port}", "--tier-secs", "3600"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(2)
    return p, d
def call(port, m, path, b=b""):
    c = http.client.HTTPConnection("127.0.0.1", port, timeout=600); c.request(m, path, b); r = c.getresponse(); d = r.read()
    if r.status != 200: raise RuntimeError(d)
    return json.loads(d)
out = {}
for publish, port in (("iceberg,delta", 8801), ("", 8802)):
    p, d = node(port)
    q = lambda s: call(port, "POST", "/sql", s.encode())
    q(f"CREATE TABLE kv (k BIGINT PRIMARY KEY, v VARCHAR, n BIGINT)" + (f" WITH (publish = '{publish}')" if publish else ""))
    q("INSERT INTO kv SELECT value, 'v' || value, value FROM generate_series(0, 1999999)")
    q("CHECKPOINT")
    random.seed(7)
    times = {}
    for keys in (100, 10_000):
        ts = []
        for r in range(5):
            ks = random.sample(range(2_000_000), keys)
            q("INSERT INTO kv VALUES " + ", ".join(f"({k}, 'u{r}', {k})" for k in ks))
            t0 = time.time(); call(port, "POST", "/tier"); ts.append(time.time() - t0)
        times[keys] = round(statistics.median(ts), 3)
    out[publish or "unpublished"] = times
    n = q("SELECT count(*) AS n FROM kv")
    p.terminate(); p.wait()
    subprocess.run(["rm", "-rf", d])
print(json.dumps({"rows": 2_000_000, "median tier round (s) by keys upserted per round": out}, indent=1))
