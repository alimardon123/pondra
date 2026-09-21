#!/usr/bin/env python3
"""Build a small lake with one of everything — an append table, an upsert table, an aggregating
view, a streaming task, a bulk insert, small and large writes, tiered and untiered rows — and
print its folder tree, with row counts from Pondra and from the engines that read its Delta logs and
Iceberg metadata.
Run it on a local folder and on a bucket to compare the two layouts.
  demo_lake.py [--dir /tmp/demo | --dir s3://bucket/prefix] [--keep]"""
import argparse, io, json, os, sys, tempfile, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import harness
from harness import Node, call, sql


def listing(lake):
    """(path, size) of every object in the lake, relative to its root."""
    if not lake.startswith("s3://"):
        return sorted((os.path.relpath(os.path.join(r, f), lake), os.path.getsize(os.path.join(r, f))) for r, _, fs in os.walk(lake) for f in fs)
    import boto3
    bucket, prefix = lake[5:].split("/", 1)
    s3 = boto3.client("s3", endpoint_url=os.environ["AWS_ENDPOINT"], region_name="auto")
    out = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix + "/"):
        out += [(o["Key"][len(prefix) + 1:], o["Size"]) for o in page.get("Contents", [])]
    return sorted(out)


def tree(files, show=3):
    """An indented tree; long folders show their first few files and a count."""
    dirs = {}
    for path, size in files:
        d, name = os.path.split(path)
        dirs.setdefault(d, []).append((name, size))
        while d:  # every ancestor gets a line, even without files of its own
            d = os.path.dirname(d)
            dirs.setdefault(d, [])
    lines = []
    for d in sorted(dirs):
        depth = 0 if d == "" else d.count("/") + 1
        if d:
            lines.append("  " * (depth - 1) + d.split("/")[-1] + "/")
        for name, size in dirs[d][:show]:
            lines.append("  " * depth + f"{name}  ({size:,} B)")
        if len(dirs[d]) > show:
            lines.append("  " * depth + f"… {len(dirs[d]) - show} more")
    return "\n".join(lines)


def main(a):
    import pyarrow as pa, pyarrow.ipc
    lake = a.dir or tempfile.mkdtemp(prefix="pondra-demo-")
    node = Node(lake, a.port, tier_secs=0, publish="delta,iceberg").start()  # open formats: opt-in
    p = node.port
    call(p, "POST", "/tables/events", json.dumps([["user", "Utf8"], ["amount", "Int64"]]).encode())
    call(p, "POST", "/tables/customers", json.dumps({"columns": [["id", "Utf8"], ["name", "Utf8"], ["tier", "Utf8"]], "key": ["id"]}).encode())
    call(p, "POST", "/views/spend_by_user", b"SELECT user, sum(amount) AS spent, count(*) AS orders FROM events GROUP BY user")
    call(p, "POST", "/tasks/big_spenders", json.dumps({"source": "events", "target": "big_orders", "sql": "SELECT user, amount FROM events WHERE amount > 990"}).encode())
    call(p, "POST", "/insert/regions?job=regions", b"SELECT concat('u', CAST(value AS VARCHAR)) AS user, value % 5 AS region FROM range(0, 100)")
    # a big write (its own object in log/) and small ones (carried inside the catalog commit)
    t = pa.table({"user": [f"u{i % 100}" for i in range(200_000)], "amount": pa.array([i % 1000 for i in range(200_000)], pa.int64())})
    buf = io.BytesIO()
    with pa.ipc.new_stream(buf, t.schema) as w:
        w.write_table(t)
    call(p, "POST", "/append/events?producer=app&seq=1", buf.getvalue(), headers={"content-type": "application/vnd.apache.arrow.stream"})
    for i in range(3):
        call(p, "POST", f"/append/customers?producer=crm&seq={i + 1}", "".join(
            json.dumps({"id": f"c{j}", "name": f"Customer {j}", "tier": ["gold", "silver"][(i + j) % 2]}) + "\n" for j in range(20)).encode())
    time.sleep(2)
    call(p, "POST", "/tier", timeout=600)  # log -> Parquet (+ the Delta log and Iceberg metadata other engines read)
    call(p, "POST", "/append/events?producer=app&seq=2", b'{"user": "u1", "amount": 5}\n')  # left in the log, untiered
    time.sleep(1)
    counts = {tb: sql(p, f"SELECT count(*) AS n FROM {tb}")[0]["n"] for tb in ("events", "customers", "spend_by_user", "big_orders", "regions")}
    if not a.keep:
        node.kill()
    print(f"lake: {lake}\nrows per table (Pondra SQL): {counts}")
    from open_check import iceberg_readers, readers  # the same tables through Delta and Iceberg, without Pondra
    print("rows per table via Delta (delta-rs / Polars / DuckDB):", {tb: readers(lake, tb) for tb in counts})
    print("rows per table via Iceberg (PyIceberg / Polars / DuckDB):", {tb: iceberg_readers(lake, tb) for tb in counts}, "\n")
    print(tree(listing(lake)))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", help="local folder or s3://bucket/prefix (default: a new temp folder)")
    ap.add_argument("--port", type=int, default=18900)
    ap.add_argument("--keep", action="store_true", help="leave the node running")
    main(ap.parse_args())
