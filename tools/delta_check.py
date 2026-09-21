#!/usr/bin/env python3
"""Can other engines read a Pondra lake without Pondra? Builds tables, then reads their Delta logs
with three independent readers (delta-rs, Polars, DuckDB's delta extension) and compares with
Pondra's own SQL. Also crosses a Delta checkpoint and times external freshness.
  delta_check.py [--s3] [--rounds 120] [--keep]"""
import argparse, json, os, sys, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import harness
from harness import Node, call, sql

A = None


def readers(lake, table):
    """Row count of `table` per external reader (None if it can't read it)."""
    uri = f"{lake}/data/{table}"
    opts = {}
    if uri.startswith("s3://"):
        opts = {"AWS_ENDPOINT_URL": os.environ["AWS_ENDPOINT"], "AWS_ACCESS_KEY_ID": os.environ["AWS_ACCESS_KEY_ID"],
                "AWS_SECRET_ACCESS_KEY": os.environ["AWS_SECRET_ACCESS_KEY"], "AWS_REGION": "auto", "AWS_S3_ALLOW_UNSAFE_RENAME": "true"}
    out = {}
    try:
        import deltalake
        out["delta-rs"] = deltalake.DeltaTable(uri, storage_options=opts).to_pyarrow_table().num_rows
    except Exception as e:
        out["delta-rs"] = f"error: {str(e)[:120]}"
    try:
        import polars as pl
        out["polars"] = pl.read_delta(uri, storage_options=opts).height
    except Exception as e:
        out["polars"] = f"error: {str(e)[:120]}"
    try:
        import duckdb, glob
        con = duckdb.connect()
        for ext in ("delta", "httpfs"):  # from the pip packages (duckdb-extension-delta, -httpfs) when installed
            found = glob.glob(f"{sys.prefix}/**/duckdb_extension_{ext}/**/{ext}.duckdb_extension", recursive=True)
            found += glob.glob(f"/usr/local/lib/python3*/dist-packages/duckdb_extension_{ext}/**/{ext}.duckdb_extension", recursive=True)
            con.execute(f"LOAD '{found[0]}'" if found else f"LOAD {ext}")
        if uri.startswith("s3://"):
            scheme, host = os.environ["AWS_ENDPOINT"].split("://")
            con.execute(f"CREATE SECRET (TYPE s3, KEY_ID '{os.environ['AWS_ACCESS_KEY_ID']}', SECRET '{os.environ['AWS_SECRET_ACCESS_KEY']}', "
                        f"ENDPOINT '{host}', REGION 'auto', URL_STYLE 'path', USE_SSL {scheme == 'https'})")
        out["duckdb"] = con.execute(f"SELECT count(*) FROM delta_scan('{uri}')").fetchone()[0]
    except Exception as e:
        out["duckdb"] = f"error: {str(e)[:120]}"
    return out


def versions(lake, table):
    """The table's Delta log versions (JSON commits), oldest first."""
    if not lake.startswith("s3://"):
        return sorted(int(f[:20]) for f in os.listdir(f"{lake}/data/{table}/_delta_log") if f.endswith(".json"))
    import boto3
    bucket, prefix = lake[5:].split("/", 1)
    s3 = boto3.client("s3", endpoint_url=os.environ["AWS_ENDPOINT"], region_name="auto")
    pages = s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{prefix}/data/{table}/_delta_log/")
    return sorted(int(o["Key"].rsplit("/", 1)[1][:20]) for pg in pages for o in pg.get("Contents", []) if o["Key"].endswith(".json"))


def new_version(lake, table, after):
    """Whether a Delta commit newer than `after` adds a file (i.e. brings new rows)."""
    for v in (v for v in versions(lake, table) if v > after):
        path = f"data/{table}/_delta_log/{v:020}.json"
        if lake.startswith("s3://"):
            import boto3
            bucket, prefix = lake[5:].split("/", 1)
            body = boto3.client("s3", endpoint_url=os.environ["AWS_ENDPOINT"], region_name="auto").get_object(Bucket=bucket, Key=f"{prefix}/{path}")["Body"].read().decode()
        else:
            body = open(f"{lake}/{path}").read()
        if '"add"' in body:
            return True
    return False


def main():
    lake = harness.new_lake()
    node = Node(lake, A.port, tier_secs=A.tier_secs).start()
    p = node.port
    call(p, "POST", "/tables/events", json.dumps([["user", "Utf8"], ["amount", "Int64"], ["ok", "Boolean"], ["score", "Float64"]]).encode())
    call(p, "POST", "/tables/kv", json.dumps({"columns": [["id", "Int64"], ["v", "Int64"]], "key": ["id"]}).encode())
    call(p, "POST", "/views/totals", b"SELECT user, sum(amount) AS amount, count(*) AS n FROM events GROUP BY user")
    call(p, "POST", "/insert/dims?job=dims", b"SELECT concat('u', CAST(value AS VARCHAR)) AS user, value % 7 AS region FROM range(0, 100)")
    for r in range(1, A.rounds + 1):
        rows = "".join(json.dumps({"user": f"u{i % 100}", "amount": i, "ok": i % 2 == 0, "score": i / 3}) + "\n" for i in range(500))
        call(p, "POST", f"/append/events?producer=p&seq={r}", rows.encode())
        call(p, "POST", f"/append/kv?producer=k&seq={r}", "".join(json.dumps({"id": i, "v": r}) + "\n" for i in range(50)).encode())
        call(p, "POST", "/tier", timeout=600)
    time.sleep(2)
    ours = {t: sql(p, f"SELECT count(*) AS n FROM {t}")[0]["n"] for t in ("events", "kv", "totals", "dims")}
    theirs = {t: readers(lake, t) for t in ours}
    ok = all(v == ours[t] for t in ours for v in theirs[t].values())
    print(json.dumps({"lake": lake, "pondra": ours, "external": theirs, "match": ok}, indent=1))

    # freshness for an outside reader: write one row, poll delta-rs until it is there
    import deltalake
    opts = {} if not lake.startswith("s3://") else {"AWS_ENDPOINT_URL": os.environ["AWS_ENDPOINT"], "AWS_ACCESS_KEY_ID": os.environ["AWS_ACCESS_KEY_ID"],
                                                     "AWS_SECRET_ACCESS_KEY": os.environ["AWS_SECRET_ACCESS_KEY"], "AWS_REGION": "auto"}
    # Each probe comes after a quiet spell, like a real first write; under a steady stream of
    # writes, add up to --tier-secs (tiering runs at most that often).
    count = lambda: deltalake.DeltaTable(f"{lake}/data/events", storage_options=opts).to_pyarrow_dataset().count_rows()
    lags, reads, published = [], [], []
    for k in range(A.probes):
        time.sleep(A.tier_secs + 0.5)
        before, before_version = ours["events"] + k, versions(lake, "events")[-1]
        t = time.time()
        call(p, "POST", f"/append/events?producer=probe&seq={k + 1}", json.dumps({"user": "probe", "amount": 1, "ok": True, "score": 0.0}).encode())
        acked = time.time() - t
        while not new_version(lake, "events", before_version):  # Pondra's part: a Delta commit adding a file
            time.sleep(0.01)
        published.append(round((time.time() - t) * 1000))
        while True:
            r = time.time()
            if count() > before:
                break
            reads.append(time.time() - r)
        lags.append((round(acked * 1000), round((time.time() - t) * 1000)))
    lags.sort(key=lambda x: x[1])
    print(json.dumps({"external_freshness_ms": {"tier_secs": A.tier_secs, "ack_p50": sorted(a for a, _ in lags)[len(lags) // 2],
                                                "new_delta_version_p50": sorted(published)[len(published) // 2],
                                                "visible_to_delta_rs_p50": lags[len(lags) // 2][1], "max": lags[-1][1], "samples": len(lags),
                                                "one_delta_rs_read_p50": round(1000 * sorted(reads)[len(reads) // 2]) if reads else None}}))
    if not A.keep:
        node.kill()
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--rounds", type=int, default=120)
    ap.add_argument("--probes", type=int, default=10)
    ap.add_argument("--tier-secs", type=float, default=2.0)
    ap.add_argument("--port", type=int, default=18600)
    ap.add_argument("--s3", action="store_true")
    ap.add_argument("--keep", action="store_true", help="leave the node running and the lake in place")
    A = harness.A = ap.parse_args()
    main()
