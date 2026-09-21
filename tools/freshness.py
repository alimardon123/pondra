#!/usr/bin/env python3
"""Freshness, head to head on one lake: how long after a write is acknowledged each kind of reader
sees it. Native readers — another cluster node, a read-only node, a brand-new `pondra sql`
process — against the open formats other engines read (Delta via delta-rs, Iceberg via PyIceberg).
Each probe is one row written after a quiet spell (like a first write); all readers poll at once.
  freshness.py [--s3] [--probes 10] [--flag ack=replicated] [--tier-secs 2]"""
import argparse, concurrent.futures as cf, json, os, subprocess, sys, tempfile, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import harness
from harness import Node, call, sql
from open_check import iceberg_metadata, iceberg_props, s3_opts

A = None


COUNT = "SELECT count(*) AS n FROM events"


def visible(kind, lake, port, n):
    """Does this kind of reader see at least `n` rows?"""
    s3 = lake.startswith("s3://")
    if kind == "node":
        return sql(port, COUNT)[0]["n"] >= n
    if kind == "sql":
        env = {**os.environ, "PONDRA_CACHE_DIR": os.path.join(tempfile.gettempdir(), "pondra-freshness-cache")}
        out = subprocess.run([harness.BIN, "sql", "--dir", lake, COUNT], capture_output=True, text=True, env=env, timeout=60).stdout
        return int(out.strip().splitlines()[-2].strip("| ")) >= n  # (the one number in the printed table)
    if kind == "delta":
        import deltalake
        return deltalake.DeltaTable(f"{lake}/data/events", storage_options=s3_opts() if s3 else None).to_pyarrow_dataset().count_rows() >= n
    from pyiceberg.table import StaticTable
    return StaticTable.from_metadata(iceberg_metadata(lake, "events"), properties=iceberg_props(lake)).scan().to_arrow().num_rows >= n


def first_seen(kind, lake, port, n, t_ack, limit=60):
    """Seconds from the ack until this reader first sees row `n` (None if it never does). Each
    reader polls in its own process, so they don't slow each other down; readers of the bucket
    poll every 0.2 s there (hammering it slowed the leader's own writes to R2 down)."""
    pause = 0.2 if kind != "node" and lake.startswith("s3://") else 0.002
    error = None
    while time.time() - t_ack < limit:
        try:
            if visible(kind, lake, port, n):
                return time.time() - t_ack
        except Exception as e:
            error = e  # (e.g. the open-format metadata doesn't exist yet)
        time.sleep(pause)
    print(f"{kind}: never visible: {error}", file=sys.stderr)
    return None


def main():
    lake = harness.new_lake()
    flags = {"tier_secs": A.tier_secs, "publish": "delta,iceberg", **dict(f.split("=", 1) for f in A.flag)}
    leader = Node(lake, A.port, **flags).start()
    follower = Node(lake, A.port + 1, **flags).start()
    reader = Node(lake, A.port + 2, reader=True, **flags).start()
    call(leader.port, "POST", "/tables/events", json.dumps([["probe", "Int64"], ["note", "Utf8"]]).encode())
    time.sleep(2)  # (followers see the table; replicated acks need a follower listed)
    paths = {"another node": ("node", follower.port), "read-only node": ("node", reader.port), "pondra sql (new process)": ("sql", 0),
             "Delta (delta-rs)": ("delta", 0), "Iceberg (PyIceberg)": ("iceberg", 0)}
    acks, seen = [], {k: [] for k in paths}
    with cf.ProcessPoolExecutor(len(paths)) as pool:
        for k in range(1, A.probes + 1):
            time.sleep(A.tier_secs + 1)  # a quiet spell: tiering has nothing waiting
            t = time.time()
            call(leader.port, "POST", f"/append/events?producer=probe&seq={k}", json.dumps({"probe": k, "note": "x"}).encode())
            t_ack = time.time()
            acks.append(t_ack - t)
            futures = {name: pool.submit(first_seen, kind, lake, port, k, t_ack) for name, (kind, port) in paths.items()}
            for name, fut in futures.items():
                seen[name].append(fut.result())
    ms = lambda xs: {"p50": round(1000 * sorted(xs)[len(xs) // 2]), "max": round(1000 * max(xs))} if xs and None not in xs else xs
    out = {"lake": "object storage" if lake.startswith("s3://") else "local disk", "flags": flags, "probes": A.probes,
           "write ack": ms(acks), **{f"visible to {k} after the ack": ms(v) for k, v in seen.items()}}
    print(json.dumps(out, indent=1))
    for n in (leader, follower, reader):
        n.kill()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--probes", type=int, default=10)
    ap.add_argument("--tier-secs", type=float, default=2.0)
    ap.add_argument("--port", type=int, default=18700)
    ap.add_argument("--s3", action="store_true")
    ap.add_argument("--flag", action="append", default=[], help="extra serve flag for every node, e.g. ack=replicated")
    A = harness.A = ap.parse_args()
    main()
