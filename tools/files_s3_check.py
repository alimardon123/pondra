#!/usr/bin/env python3
"""Files in a real bucket (ADR-026), read by a node as its owner (with the node's own
credentials): a glob == pyarrow, then again from the cache kept by version; a file changed under
its name and one added, read as they are at once; `COPY … TO` a folder there, read back. The
bucket is R2, S3 or MinIO: AWS_ENDPOINT, the AWS_* credentials and PONDRA_BUCKET set (and
PONDRA_TEST_PREFIX for a folder of test runs). It prints no endpoint or account, and deletes
what it wrote."""
import io, os, sys, time, uuid, argparse
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import boto3, pyarrow as pa, pyarrow.parquet as pq, harness
harness.A = argparse.Namespace(s3=False, keep=False, port=8500)
bucket, prefix = os.environ["PONDRA_BUCKET"], f"{os.environ.get('PONDRA_TEST_PREFIX', '')}files-{uuid.uuid4().hex[:8]}"
s3 = boto3.client("s3", endpoint_url=os.environ["AWS_ENDPOINT"], region_name=os.environ.get("AWS_REGION", "auto"))
buf = lambda t: (lambda b: (pq.write_table(t, b), b.getvalue())[1])(io.BytesIO())
tables = [pa.table({"id": list(range(i * 200_000, (i + 1) * 200_000)), "v": [x % 97 for x in range(200_000)]}) for i in range(3)]
for i, t in enumerate(tables):
    s3.put_object(Bucket=bucket, Key=f"{prefix}/data/part-{i}.parquet", Body=buf(t))
owner = uuid.uuid4().hex
lake = harness.new_lake()
node = harness.Node(lake, 8500, env={"PONDRA_OWNER_KEY": owner}).start()
q = lambda s: harness.call(8500, "POST", "/sql", s.encode(), headers={"x-pondra-owner": owner}, timeout=300)
glob = f"'s3://{bucket}/{prefix}/data/*.parquet'"
sql = f"SELECT count(*) AS n, sum(v) AS s FROM {glob}"
want = [{"n": 600_000, "s": sum(sum(t.column("v").to_pylist()) for t in tables)}]
checks, times = {}, []
for _ in range(3):
    t0 = time.time(); got = q(sql); times.append(round(time.time() - t0, 3))
checks["a glob on R2 == pyarrow, three times"] = got == want
print("times (cold, then kept by version):", times)
s3.put_object(Bucket=bucket, Key=f"{prefix}/data/part-0.parquet", Body=buf(pa.table({"id": list(range(10)), "v": [1] * 10})))
s3.put_object(Bucket=bucket, Key=f"{prefix}/data/part-9.parquet", Body=buf(pa.table({"id": [1], "v": [1000]})))
want2 = [{"n": 400_011, "s": want[0]["s"] - sum(tables[0].column("v").to_pylist()) + 10 + 1000}]
checks["a file changed under its name and one added: read as they are, at once"] = q(sql) == want2
copied = q(f"COPY (SELECT * FROM {glob} WHERE v < 10) TO 's3://{bucket}/{prefix}/out/' (FORMAT parquet)")
back = q(f"SELECT count(*) AS n FROM 's3://{bucket}/{prefix}/out/*.parquet'")
checks["COPY … TO a folder on R2, read back"] = back == [{"n": copied["copied"]}] and copied["copied"] > 0
node.kill()
for pg in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix + "/"):
    keys = [{"Key": o["Key"]} for o in pg.get("Contents", [])]
    keys and s3.delete_objects(Bucket=bucket, Delete={"Objects": keys, "Quiet": True})
harness.clean_up()
for k, v in checks.items():
    print(("ok  " if v else "FAIL") + " " + k)
sys.exit(0 if all(checks.values()) else 1)
