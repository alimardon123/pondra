#!/usr/bin/env python3
"""Can other engines read a Pondra lake without Pondra? Builds tables published as Delta and
Iceberg, then reads them with independent readers — delta-rs, Polars and DuckDB for Delta;
PyIceberg, Polars and DuckDB for Iceberg — and compares with Pondra's own SQL. Also crosses a
Delta checkpoint and the Iceberg snapshot history limit (freshness: tools/freshness.py).
  open_check.py [--s3] [--rounds 120] [--keep]"""
import argparse, json, os, sys, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import harness
from harness import Node, call, sql

A = None


def s3_opts():
    return {"AWS_ENDPOINT_URL": os.environ["AWS_ENDPOINT"], "AWS_ACCESS_KEY_ID": os.environ["AWS_ACCESS_KEY_ID"],
            "AWS_SECRET_ACCESS_KEY": os.environ["AWS_SECRET_ACCESS_KEY"], "AWS_REGION": "auto"}


def read_object(lake, path):
    if not lake.startswith("s3://"):
        return open(f"{lake}/{path}", "rb").read()
    import boto3
    bucket, prefix = lake[5:].split("/", 1)
    return boto3.client("s3", endpoint_url=os.environ["AWS_ENDPOINT"], region_name="auto").get_object(Bucket=bucket, Key=f"{prefix}/{path}")["Body"].read()


def duck(lake):
    """A DuckDB connection with the delta, iceberg and httpfs extensions (and the bucket's secret)."""
    import duckdb, glob
    con = duckdb.connect()
    for ext in ("delta", "avro", "iceberg", "httpfs"):  # from the pip packages (duckdb-extension-…) when installed
        found = glob.glob(f"{sys.prefix}/**/duckdb_extension_{ext}/**/{ext}.duckdb_extension", recursive=True)
        found += glob.glob(f"/usr/local/lib/python3*/dist-packages/duckdb_extension_{ext}/**/{ext}.duckdb_extension", recursive=True)
        con.execute(f"LOAD '{found[0]}'" if found else f"LOAD {ext}")
    if lake.startswith("s3://"):
        scheme, host = os.environ["AWS_ENDPOINT"].split("://")
        con.execute(f"CREATE SECRET (TYPE s3, KEY_ID '{os.environ['AWS_ACCESS_KEY_ID']}', SECRET '{os.environ['AWS_SECRET_ACCESS_KEY']}', "
                    f"ENDPOINT '{host}', REGION 'auto', URL_STYLE 'path', USE_SSL {scheme == 'https'})")
    return con


def iceberg_metadata(lake, table):
    """The table's current Iceberg metadata file (from version-hint.text)."""
    v = read_object(lake, f"data/{table}/metadata/version-hint.text").decode().strip()
    return f"{lake}/data/{table}/metadata/v{v}.metadata.json"


def iceberg_props(lake):
    """PyIceberg file-IO settings. (Its default PyArrow S3 client gets 403 from R2 here; fsspec/s3fs works.)"""
    if not lake.startswith("s3://"):
        return {}
    return {"py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO", "s3.endpoint": os.environ["AWS_ENDPOINT"],
            "s3.access-key-id": os.environ["AWS_ACCESS_KEY_ID"], "s3.secret-access-key": os.environ["AWS_SECRET_ACCESS_KEY"], "s3.region": "auto"}


def iceberg_readers(lake, table):
    """Row count of `table`'s Iceberg metadata per external reader."""
    out, s3 = {}, lake.startswith("s3://")
    try:
        from pyiceberg.table import StaticTable
        out["pyiceberg"] = StaticTable.from_metadata(iceberg_metadata(lake, table), properties=iceberg_props(lake)).scan().to_arrow().num_rows
    except Exception as e:
        out["pyiceberg"] = f"error: {str(e)[:160]}"
    try:
        import polars as pl
        from pyiceberg.table import StaticTable  # (metadata through PyIceberg's fsspec IO, data through Polars' own reader)
        t = StaticTable.from_metadata(iceberg_metadata(lake, table), properties=iceberg_props(lake))
        out["polars"] = pl.scan_iceberg(t, storage_options=s3_opts() if s3 else None).select(pl.len()).collect().item()
    except Exception as e:
        out["polars"] = f"error: {str(e)[:160]}"
    try:
        out["duckdb"] = duck(lake).execute(f"SELECT count(*) FROM iceberg_scan('{iceberg_metadata(lake, table)}')").fetchone()[0]
    except Exception as e:
        out["duckdb"] = f"error: {str(e)[:160]}"
    return out


def readers(lake, table):
    """Row count of `table`'s Delta log per external reader."""
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
        out["duckdb"] = duck(lake).execute(f"SELECT count(*) FROM delta_scan('{uri}')").fetchone()[0]
    except Exception as e:
        out["duckdb"] = f"error: {str(e)[:120]}"
    return out


def main():
    lake = harness.new_lake()
    node = Node(lake, A.port, tier_secs=A.tier_secs, publish="delta,iceberg").start()
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
    theirs = {t: {**{f"delta/{k}": v for k, v in readers(lake, t).items()}, **{f"iceberg/{k}": v for k, v in iceberg_readers(lake, t).items()}} for t in ours}
    ok = all(v == ours[t] for t in ours for v in theirs[t].values())
    print(json.dumps({"lake": lake, "pondra": ours, "external": theirs, "match": ok}, indent=1))
    if not A.keep:
        node.kill()
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--rounds", type=int, default=120)
    ap.add_argument("--tier-secs", type=float, default=2.0)
    ap.add_argument("--port", type=int, default=18600)
    ap.add_argument("--s3", action="store_true")
    ap.add_argument("--keep", action="store_true", help="leave the node running and the lake in place")
    A = harness.A = ap.parse_args()
    main()
