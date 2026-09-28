#!/usr/bin/env python3
"""TPC-H from files outside the lake, against the lake's own tables (ADR-026: within 10%).

  files_tpch.py --data ~/tpch/sf1-bench [--queries tools/bench/tpch-queries] [--runs 3]
                [--stores local,s3] [--files pondra|given]

The 22 queries, each run `--runs` times (the best time counts), over two copies of one data set:

- `lake`: a lake's tables, loaded with one INSERT per table from `--data`, its in-memory columns
  off (PONDRA_HOT_GB=0) so that every query reads Parquet, as it does from the files;
- `files`: Parquet files outside the lake, as stored views (`CREATE VIEW lineitem AS SELECT * FROM
  '<folder>/*.parquet'`), listed again by every query (a file may change under its name).

Which files: `pondra` (the default) writes each table out with `COPY … TO` as LZ4 Parquet, as the
lake keeps its own, so the two differ in how they are read, not in their bytes' encoding; `given`
reads `--data`'s files as they are (tpchgen's Snappy, 122,880-row row groups), which says what
the encoding costs as well.

Each on this machine's disk (`local`: the files read by the program that started the node) and on
a local S3 (`s3`: `sim_r2.py --zero`, moto without R2's latency; the files read with a secret).
On S3 the lake's own files go through its caches, as they do in use, and the files through the
cache of files outside the lake (kept by version). Every answer is compared with the first run's.
"""
import argparse, glob, json, os, shutil, subprocess, sys, tempfile, time, uuid

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
sys.path.insert(0, HERE)
import harness
from join_order import same
from tpch import queries

TABLES = ["region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem"]


def run_all(port, qs, owner):
    """Every query `--runs` times: (best seconds, rows) each."""
    out = {}
    for i, sql in qs.items():
        times, rows = [], None
        for _ in range(A.runs):
            t = time.time()
            rows = harness.call(port, "POST", "/sql", sql.encode(), timeout=3600, headers={"x-pondra-owner": owner})
            times.append(time.time() - t)
        out[i] = (min(times), rows)
    return out


def one(name, lake, env, owner, setup, qs, before=None):
    print(f"== {name}", flush=True)
    node = harness.Node(lake, A.port, env={**env, "PONDRA_HOT_GB": "0", "PONDRA_OWNER_KEY": owner}).start()
    try:
        for s in setup:
            harness.call(A.port, "POST", "/sql", s.encode(), timeout=3600, headers={"x-pondra-owner": owner})
        harness.call(A.port, "POST", "/tier", timeout=3600)  # (merges and sealing done before timing)
        if before:
            before()
        return run_all(A.port, qs, owner)
    finally:
        node.kill()


def main():
    qs = queries(A.queries)
    harness.A = argparse.Namespace(s3=False, keep=False, port=A.port)
    owner, results = uuid.uuid4().hex, {}
    stores = A.stores.split(",")
    load = lambda lake, env: [subprocess.run([harness.BIN, "sql", "--dir", lake, f"INSERT INTO {t} SELECT * FROM '{os.path.join(A.data, t)}.parquet'"],
                                             check=True, capture_output=True, env={**os.environ, **env}) for t in TABLES]
    files = tempfile.mkdtemp(prefix="pondra-files-")
    harness.LAKES.append(files)
    if A.files == "given":
        for t in TABLES:
            os.makedirs(f"{files}/{t}")
            shutil.copy(os.path.join(A.data, f"{t}.parquet"), f"{files}/{t}/")
    write_out = lambda: [harness.call(A.port, "POST", "/sql", f"COPY {t} TO '{files}/{t}/' (FORMAT parquet, COMPRESSION lz4_raw)".encode(), timeout=3600,
                                      headers={"x-pondra-owner": owner}) for t in TABLES] if A.files == "pondra" else None
    lake = harness.new_lake()
    load(lake, {})
    results["lake, local"] = one("lake, local", lake, {}, owner, [], qs, before=write_out)
    if "local" in stores:
        views = [f"CREATE VIEW {t} AS SELECT * FROM '{files}/{t}/*.parquet'" for t in TABLES]
        results["files, local"] = one("files, local", harness.new_lake(), {}, owner, views, qs)
    if "s3" in stores:
        import boto3
        port = A.port + 50
        sim = subprocess.Popen([sys.executable, os.path.join(os.path.dirname(HERE), "sim_r2.py"), "--port", str(port), "--zero"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            s3 = boto3.client("s3", endpoint_url=f"http://127.0.0.1:{port}", region_name="us-east-1", aws_access_key_id="k", aws_secret_access_key="s")
            for _ in range(100):
                try:
                    s3.create_bucket(Bucket="lakes")
                    break
                except Exception:
                    time.sleep(0.1)
            s3.create_bucket(Bucket="tpch")
            for t in TABLES:
                for f in glob.glob(f"{files}/{t}/*.parquet"):
                    s3.upload_file(f, "tpch", f"{t}/{os.path.basename(f)}")
            env = {"AWS_ENDPOINT": f"http://127.0.0.1:{port}", "AWS_ACCESS_KEY_ID": "k", "AWS_SECRET_ACCESS_KEY": "s", "AWS_REGION": "us-east-1", "AWS_ALLOW_HTTP": "true"}
            lake = f"s3://lakes/tpch-{uuid.uuid4().hex[:8]}"
            load(lake, env)
            results["lake, s3"] = one("lake, s3", lake, env, owner, [], qs)
            secret = f"CREATE SECRET tpch (TYPE s3, KEY_ID 'k', SECRET 's', ENDPOINT 'http://127.0.0.1:{port}', SCOPE 's3://tpch')"
            views = [secret] + [f"CREATE VIEW {t} AS SELECT * FROM 's3://tpch/{t}/*.parquet'" for t in TABLES]
            results["files, s3"] = one("files, s3", f"s3://lakes/views-{uuid.uuid4().hex[:8]}", env, owner, views, qs)
        finally:
            sim.kill()
    ref = next(iter(results.values()))
    table = {name: {"total_s": round(sum(t for t, _ in r.values()), 2), "answers_match": sum(same(r[i][1], ref[i][1]) for i in qs),
                    "per_query": {i: round(r[i][0], 3) for i in qs}} for name, r in results.items()}
    print(f"\n{'':4}" + "".join(f"{n:>14}" for n in table))
    for i in qs:
        print(f"q{i:<3}" + "".join(f"{table[n]['per_query'][i]:>14.3f}" for n in table))
    print("all " + "".join(f"{table[n]['total_s']:>14.2f}" for n in table))
    print("same" + "".join(f"{str(table[n]['answers_match']) + '/22':>14}" for n in table))
    for store in stores:
        lake, files_ = table.get(f"lake, {store}"), table.get(f"files, {store}")
        if lake and files_:
            print(f"{store}: files {files_['total_s']} s against the lake's {lake['total_s']} s ({100 * (files_['total_s'] / lake['total_s'] - 1):+.1f}%)")
    if A.out:
        json.dump({"data": A.data, "files": A.files, "runs": A.runs, "cores": os.cpu_count(), "results": table}, open(A.out, "w"), indent=1)
    harness.clean_up()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", required=True)
    ap.add_argument("--queries", default=os.path.join(HERE, "tpch-queries"))
    ap.add_argument("--runs", type=int, default=3)
    ap.add_argument("--stores", default="local,s3")
    ap.add_argument("--files", choices=["pondra", "given"], default="pondra")
    ap.add_argument("--port", type=int, default=8870)
    ap.add_argument("--out")
    A = ap.parse_args()
    main()
