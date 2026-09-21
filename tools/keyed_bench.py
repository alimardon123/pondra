#!/usr/bin/env python3
"""What keyed-table compaction costs: a big upsert table, then many rounds of small updates, each
tiered. Reports the bytes compaction wrote (replaced files are kept, --retain-secs is long), what a
full rewrite every time 8 files pile up would have written, file counts, and that every key reads
right (SQL and /lookup) against a model.
  keyed_bench.py [--keys 2000000] [--rounds 60] [--updates 20000]"""
import argparse, io, json, os, random, sys, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import harness
from harness import Node, call, sql

A = None


def main():
    import pyarrow as pa
    lake = harness.new_lake()
    node = Node(lake, A.port, tier_secs=0, retain_secs=10 ** 6).start()
    call(A.port, "POST", "/tables/kv", json.dumps({"columns": [["id", "Int64"], ["v", "Int64"]], "key": ["id"]}).encode())
    model, rng, seq = {}, random.Random(3), 0

    def upsert(ids, v):
        nonlocal seq
        seq += 1
        b = pa.record_batch([pa.array(ids, pa.int64()), pa.array([v] * len(ids), pa.int64())], names=["id", "v"])
        buf = io.BytesIO()
        with pa.ipc.new_stream(buf, b.schema) as w:
            w.write_batch(b)
        call(A.port, "POST", f"/append/kv?producer=p&seq={seq}", buf.getvalue(), headers={"content-type": "application/vnd.apache.arrow.stream"}, timeout=600)
        model.update((i, v) for i in ids)

    for start in range(0, A.keys, 500_000):
        upsert(list(range(start, min(start + 500_000, A.keys))), 0)
    call(A.port, "POST", "/tier", timeout=3600)
    base = sum(os.path.getsize(os.path.join(lake, "data", "kv", f)) for f in os.listdir(os.path.join(lake, "data", "kv")) if f.endswith(".parquet"))
    t, tier_s = time.time(), []
    for r in range(1, A.rounds + 1):
        upsert(rng.sample(range(A.keys), A.updates), r)
        s = time.time()
        call(A.port, "POST", "/tier", timeout=3600)
        tier_s.append(time.time() - s)
    files = [f for f in os.listdir(os.path.join(lake, "data", "kv")) if f.endswith(".parquet")]
    written = sum(os.path.getsize(os.path.join(lake, "data", "kv", f)) for f in files) - base
    live = json.loads(harness.subprocess.run([harness.BIN, "catalog", "--dir", lake, "t/kv"], capture_output=True, text=True).stdout.split(" ", 1)[1])["files"]
    total = sql(A.port, "SELECT count(*) AS n, sum(v) AS s FROM kv")[0]
    ok = total == {"n": len(model), "s": sum(model.values())}
    probes = rng.sample(range(A.keys), 200)
    wrong = sum((call(A.port, "GET", f"/lookup/kv/{k}") or [{}])[0].get("v") != model[k] for k in probes)
    folds = A.rounds * A.updates * base / A.keys  # (each round's fold file is about this big)
    full = (A.rounds // 7) * base + folds  # a full rewrite whenever 8 files pile up
    print(json.dumps({"keys": A.keys, "rounds": A.rounds, "updates_per_round": A.updates, "base_mb": round(base / 1e6, 1),
                      "written_by_tiering_and_compaction_mb": round(written / 1e6, 1), "full_rewrites_would_write_mb": round(full / 1e6, 1),
                      "live_files": len(live), "tier_call_s_median": round(sorted(tier_s)[len(tier_s) // 2], 3), "tier_call_s_max": round(max(tier_s), 3),
                      "total_s": round(time.time() - t, 1), "sql_matches_model": ok, "wrong_lookups": wrong}, indent=1))
    node.kill()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--keys", type=int, default=2_000_000)
    ap.add_argument("--rounds", type=int, default=60)
    ap.add_argument("--updates", type=int, default=20_000)
    ap.add_argument("--port", type=int, default=18960)
    ap.add_argument("--s3", action="store_true")
    A = harness.A = ap.parse_args()
    main()
