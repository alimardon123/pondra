#!/usr/bin/env python3
"""What a flow costs (ADR-036 §1): ingest with no view, then with a flow one stage longer
each run — silver (row by row, two expectations), gold (a GROUP BY of silver), platinum (a rollup of
gold) — each run on a fresh lake, the same producers for the same time. Reports rows a second, the
append latency (an acknowledged append has every stage committed with it), and whether every stage
equals its query over the source at the moment the producers stop: no waiting, no lag.

  flow.py [--bin target/release/pondra] [--secs 15] [--producers 4] [--batch 1000]
"""
import argparse, json, os, random, shutil, statistics, subprocess, sys, tempfile, threading, time, urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
ap = argparse.ArgumentParser()
ap.add_argument("--bin", default=os.environ.get("PONDRA_BIN", os.path.join(HERE, "..", "..", "target", "release", "pondra")))
ap.add_argument("--secs", type=int, default=15)
ap.add_argument("--producers", type=int, default=4)
ap.add_argument("--batch", type=int, default=1000)
ap.add_argument("--port", type=int, default=9640)
A = ap.parse_args()

STAGES = [
    ("silver", """CREATE MATERIALIZED VIEW silver (
        CONSTRAINT positive CHECK (amount > 0) ON VIOLATION DROP ROW,
        CONSTRAINT has_user EXPECT (user_id IS NOT NULL)
      ) AS SELECT id, user_id, region, amount, amount * 2 AS doubled FROM events WHERE kind <> 'test'"""),
    ("gold", "CREATE MATERIALIZED VIEW gold AS SELECT region, user_id, count(*) AS n, sum(amount) AS total FROM silver GROUP BY region, user_id"),
    ("platinum", "CREATE MATERIALIZED VIEW platinum AS SELECT region, sum(n) AS n, sum(total) AS total FROM gold GROUP BY region"),
]
WANT = {  # (each stage, and its query over events)
    "silver": ("SELECT count(*) AS n, sum(doubled) AS s FROM silver", "SELECT count(*) AS n, sum(amount * 2) AS s FROM events WHERE kind <> 'test' AND amount > 0"),
    "gold": ("SELECT count(*) AS g, sum(n) AS n, sum(total) AS t FROM gold",
             "SELECT count(*) AS g, sum(n) AS n, sum(t) AS t FROM (SELECT region, user_id, count(*) AS n, sum(amount) AS t FROM events WHERE kind <> 'test' AND amount > 0 GROUP BY region, user_id)"),
    "platinum": ("SELECT region, n, total FROM platinum ORDER BY region", "SELECT region, count(*) AS n, sum(amount) AS total FROM events WHERE kind <> 'test' AND amount > 0 GROUP BY region ORDER BY region"),
}


def run(stages):
    lake = tempfile.mkdtemp(prefix="pondra-pipe-")
    port = A.port
    node = subprocess.Popen([A.bin, "serve", "--dir", lake, "--addr", f"127.0.0.1:{port}"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    call = lambda path, body: urllib.request.urlopen(urllib.request.Request(f"http://127.0.0.1:{port}{path}", data=body), timeout=120).read()
    q = lambda s: json.loads(call("/sql", s.encode()))
    try:
        for _ in range(300):
            try:
                urllib.request.urlopen(f"http://127.0.0.1:{port}/stats", timeout=1)
                break
            except Exception:
                time.sleep(0.1)
        q("CREATE TABLE events (id BIGINT, user_id VARCHAR, region VARCHAR, amount BIGINT, kind VARCHAR)")
        for _, sql in STAGES[:stages]:
            q(sql)
        stop, rows, lat = time.time() + A.secs, [0] * A.producers, [[] for _ in range(A.producers)]
        def produce(k):
            rng, seq = random.Random(k), 0
            while time.time() < stop:
                seq += 1
                body = "".join(json.dumps({"id": (k << 40) + seq * A.batch + i, "user_id": None if rng.random() < 0.02 else f"u{rng.randrange(1000)}", "region": f"r{rng.randrange(8)}",
                                           "amount": rng.randrange(-20, 1000), "kind": "test" if rng.random() < 0.05 else "sale"}) + "\n" for i in range(A.batch)).encode()
                t = time.perf_counter()
                call(f"/append/events?producer=p{k}&seq={seq}", body)
                lat[k].append(time.perf_counter() - t)
                rows[k] += A.batch
        ts = [threading.Thread(target=produce, args=(k,)) for k in range(A.producers)]
        t0 = time.time()
        [t.start() for t in ts]
        [t.join() for t in ts]
        took = time.time() - t0
        at_once = {name: q(WANT[name][0]) == q(WANT[name][1]) for name, _ in STAGES[:stages]}  # (right away: no waiting)
        all_lat = sorted(x for l in lat for x in l)
        return {"stages": [n for n, _ in STAGES[:stages]], "rows_per_s": round(sum(rows) / took), "append_ms_p50": round(statistics.median(all_lat) * 1000, 1),
                "append_ms_p99": round(all_lat[int(len(all_lat) * 0.99) - 1] * 1000, 1), "every_stage_right_when_acknowledged": all(at_once.values()), "per_stage": at_once}
    finally:
        node.terminate()
        node.wait(30)
        shutil.rmtree(lake, ignore_errors=True)


out = [run(n) for n in range(len(STAGES) + 1)]
for r in out:
    print(json.dumps(r), file=sys.stderr)
base = out[0]["rows_per_s"]
print(json.dumps({"producers": A.producers, "batch": A.batch, "secs": A.secs, "runs": out, "cost_vs_no_view": [round(1 - r["rows_per_s"] / base, 3) for r in out]}))
sys.exit(0 if all(r["every_stage_right_when_acknowledged"] for r in out) else 1)
