#!/usr/bin/env python3
"""What keyed-table compaction costs, for five shapes of keyed table on one node: plain; published
as Delta and Iceberg; partitioned by a day; keeping the latest row by an order_by column; and a
merge table that sums a column. Each shape is its own table (kv_<shape>): a big upsert load, then
many rounds of small updates, each tiered (POST /tier). Per shape it reports the Parquet bytes its
folder holds after the first tier and how much it grew over the rounds (replaced files are kept:
--retain-secs is long), the object writes a round costs, what each tiering call and a full read
cost, the live file count, and that every key reads right (SQL and /lookup) against a model.
  keyed_bench.py [--keys 2000000] [--rounds 60] [--updates 20000] [--shapes plain,published,...]"""
import argparse, datetime, io, json, os, random, re, statistics, subprocess, sys, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import harness
from harness import Node, call, sql

A = None
TWO = [["id", "Int64"], ["v", "Int64"]]
SPECS = {  # each shape's table, as POST /tables/{name} takes it
    "plain": {"columns": TWO, "key": ["id"]},
    "published": {"columns": TWO, "key": ["id"], "publish": ["delta", "iceberg"]},
    "partitioned": {"columns": [["id", "Int64"], ["day", "Date32"], ["v", "Int64"]], "key": ["id"], "partition_by": "day"},
    "order_by": {"columns": [["id", "Int64"], ["ts", "Int64"], ["v", "Int64"]], "key": ["id"], "order_by": "ts"},
    "merge": {"columns": TWO, "key": ["id"], "merge": {"v": "sum"}},
}
DAY0 = datetime.date(2026, 1, 1)


def value(shape, r):
    """The v a round sends: a merge table adds 1 to each key a round samples (the initial load
    sets 0); every other shape sends r (round 0 is the initial load)."""
    return (1 if r else 0) if shape == "merge" else r


def stream(shape, ids, r):
    """One upsert's rows as an Arrow IPC stream: the id, the shape's own columns and v."""
    import pyarrow as pa
    cols = {"id": pa.array(ids, pa.int64())}
    if shape == "partitioned":  # each id keeps one day (id % 7 days after 2026-01-01): 7 partitions
        cols["day"] = pa.array([DAY0 + datetime.timedelta(days=i % 7) for i in ids], pa.date32())
    if shape == "order_by":  # the newest round wins
        cols["ts"] = pa.array([r] * len(ids), pa.int64())
    cols["v"] = pa.array([value(shape, r)] * len(ids), pa.int64())
    b = pa.record_batch(list(cols.values()), names=list(cols))
    buf = io.BytesIO()
    with pa.ipc.new_stream(buf, b.schema) as w:
        w.write_batch(b)
    return buf.getvalue()


def parquet_bytes(folder):
    """Bytes of a table's Parquet files in its folder and its sub-folders (a partitioned table's)."""
    return sum(os.path.getsize(os.path.join(d, f)) for d, _, fs in os.walk(folder) for f in fs if f.endswith(".parquet"))


def writes():
    """Object-store writes this node has made so far (`pondra_object_requests_total{op="write"}`)."""
    return float(re.search(rb'pondra_object_requests_total\{op="write"\} (\S+)', call(A.port, "GET", "/metrics")).group(1))


def run_shape(lake, shape):
    """One shape's table: the load, the rounds (each tiered), reads, probes and the model check."""
    table, port = f"kv_{shape}", A.port
    call(port, "POST", f"/tables/{table}", json.dumps(SPECS[shape]).encode())
    model, rng, seq = {}, random.Random(3), 0  # the same random ids in every shape

    def upsert(ids, r):
        nonlocal seq
        seq += 1  # (a producer per table: each shape's seq numbers start again at 1)
        call(port, "POST", f"/append/{table}?producer={table}&seq={seq}", stream(shape, ids, r),
             headers={"content-type": "application/vnd.apache.arrow.stream"}, timeout=600)
        v = value(shape, r)
        for i in ids:  # a merge table adds up, every other shape keeps the newest
            model[i] = model.get(i, 0) + v if shape == "merge" else v

    for start in range(0, A.keys, 500_000):
        upsert(list(range(start, min(start + 500_000, A.keys))), 0)
    call(port, "POST", "/tier", timeout=3600)
    folder = os.path.join(lake, "data", table)
    base = parquet_bytes(folder)
    before, tier_s = writes(), []
    for r in range(1, A.rounds + 1):
        upsert(rng.sample(range(A.keys), A.updates), r)
        s = time.time()
        call(port, "POST", "/tier", timeout=3600)
        tier_s.append(time.time() - s)
    objects = (writes() - before) / A.rounds
    written = parquet_bytes(folder) - base
    reads = []
    for n in range(3):  # (a comment makes each run's text new: the result cache would answer the same text again)
        s = time.time()
        sql(port, f"SELECT count(*), sum(v) FROM {table} /* run {n} */")
        reads.append(time.time() - s)
    total = sql(port, f"SELECT count(*) AS n, sum(v) AS s FROM {table}")[0]
    ok = total == {"n": len(model), "s": sum(model.values())}
    looks, wrong = [], 0
    for k in rng.sample(range(A.keys), 200):
        s = time.time()
        got = (call(port, "GET", f"/lookup/{table}/{k}") or [{}])[0].get("v")
        looks.append(time.time() - s)
        wrong += got != model[k]
    listing = subprocess.run([harness.BIN, "catalog", "--dir", lake, f"t/{table}"], capture_output=True, text=True).stdout
    out = {"base_mb": round(base / 1e6, 3), "written_mb": round(written / 1e6, 3), "objects_written_per_round": round(objects, 1),
           "tier_call_s_median": round(statistics.median(tier_s), 3), "tier_call_s_max": round(max(tier_s), 3),
           "read_s": round(statistics.median(reads), 3), "lookup_ms_median": round(statistics.median(looks) * 1000, 2),
           "live_files": len(json.loads(listing.split(" ", 1)[1])["files"]), "sql_matches_model": ok, "wrong_lookups": wrong}
    if shape == "plain":  # what a full rewrite every time 8 files pile up would have written
        folds = A.rounds * A.updates * base / A.keys  # (each round's fold file is about this big)
        out["full_rewrites_would_write_mb"] = round(((A.rounds // 7) * base + folds) / 1e6, 3)
    return out


def main():
    lake = harness.new_lake()
    node = Node(lake, A.port, tier_secs=0, retain_secs=10 ** 6).start()
    shapes = {s: run_shape(lake, s) for s in A.shapes}  # one after another: each tier call moves only its table
    print(json.dumps({"keys": A.keys, "rounds": A.rounds, "updates_per_round": A.updates, "shapes": shapes}, indent=1))
    node.kill()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--keys", type=int, default=2_000_000)
    ap.add_argument("--rounds", type=int, default=60)
    ap.add_argument("--updates", type=int, default=20_000)
    ap.add_argument("--port", type=int, default=18960)
    ap.add_argument("--shapes", default=",".join(SPECS), help=f"comma separated, of: {', '.join(SPECS)}")
    ap.add_argument("--s3", action="store_true")
    A = harness.A = ap.parse_args()
    A.shapes = list(dict.fromkeys(A.shapes.split(",")))
    if not set(A.shapes) <= set(SPECS):
        ap.error(f"--shapes takes {', '.join(SPECS)}")
    if A.s3:
        ap.error("the sizes are read from the lake's folder: run it on a local lake")
    main()
