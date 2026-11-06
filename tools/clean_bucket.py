#!/usr/bin/env python3
"""Delete test lakes from a bucket, keeping the newest N (and any named with --keep). R2's free tier
is 10 GB stored and a million writes a month, so test runs clean up after themselves
(harness.new_lake) and this sweeps what a run stopped half way left (a soak day cancelled between
legs, a bench whose runner went away).
  clean_bucket.py --bucket B [--bucket B2] [--newest 3] [--keep prefix ...] [--older HOURS]
                  [--tests-only] [--report FILE] [--dry-run]
--older deletes only a lake whose newest object is that old (a lake in use is written all the
time), --tests-only only a lake named as our runs name theirs (anything else is listed, never
deleted), and --report appends a table of what the bucket holds (Markdown: a job's summary).
Unfinished multipart uploads of a deleted lake, or a day old, are aborted: their parts are stored,
and billed, until then."""
import argparse, collections, datetime, os, re
import boto3

NOT_LAKES = ("bench-bin", "bench-results")  # (the cluster benchmark's binary and results, tools/cloud/actions: kept)
# (folders only runs write: harness's PONDRA_TEST_PREFIX, limits-r2.yml's and resilience-r2.yml's, and
# files_tpch.py's cold/)
FOLDERS = re.compile(r"(round\d+|limits|cost|cold|resilience)$")
# (as the runs name their lakes at the root: harness.new_lake, soak.py, sharing_check, files_s3_check,
# environments_check, cluster-bench.yml)
TESTS = re.compile(r"((test|files|sharing|soak)-[0-9a-f]{8}|environments-\d+-\d+|bench-\d+)$")


def top(key):
    """The lake a key is in: its first part, or its first two in a folder of runs (`limits/test-…`)."""
    parts = key.split("/")
    return "/".join(parts[:2]) if FOLDERS.match(parts[0]) and len(parts) > 2 else parts[0]


def is_test(lake):
    return bool(FOLDERS.match(lake.split("/")[0]) or TESTS.match(lake))


def lakes(s3, bucket):
    """Each lake -> [newest object, keys, bytes]; NOT_LAKES' objects are counted under their own names."""
    out = collections.defaultdict(lambda: [None, [], 0])
    for pg in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket):
        for o in pg.get("Contents", []):
            lake = out[top(o["Key"])]
            lake[0] = max(lake[0] or o["LastModified"], o["LastModified"])
            lake[1].append(o["Key"])
            lake[2] += o["Size"]
    return out


def uploads(s3, bucket):
    """Multipart uploads begun and never finished or aborted: (key, upload id, begun)."""
    for pg in s3.get_paginator("list_multipart_uploads").paginate(Bucket=bucket):
        for u in pg.get("Uploads", []):
            yield u["Key"], u["UploadId"], u["Initiated"]


def delete(s3, bucket, keys):
    for i in range(0, len(keys), 1000):
        s3.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": k} for k in keys[i:i + 1000]], "Quiet": True})


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--bucket", action="append", required=True)
    ap.add_argument("--newest", type=int, default=3, help="keep this many of the newest lakes, across all buckets")
    ap.add_argument("--keep", action="append", default=[], help="bucket/prefix to keep regardless")
    ap.add_argument("--older", type=float, default=0, help="delete only a lake whose newest object is this many hours old")
    ap.add_argument("--tests-only", action="store_true", help="delete only lakes named as test runs name theirs")
    ap.add_argument("--report", help="append a Markdown table of what each bucket holds to this file")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    s3 = boto3.client("s3", endpoint_url=os.environ.get("AWS_ENDPOINT"), region_name=os.environ.get("AWS_REGION", "auto"))
    now = datetime.datetime.now(datetime.timezone.utc)
    hours = lambda t: (now - t).total_seconds() / 3600
    held = {b: lakes(s3, b) for b in a.bucket}
    found = sorted(((when, b, lake, keys) for b in a.bucket for lake, (when, keys, _) in held[b].items() if lake not in NOT_LAKES), reverse=True)
    keep = {f"{b}/{lake}" for _, b, lake, _ in found[:a.newest]} | set(a.keep)
    gone = set()
    for when, b, lake, keys in found:
        kept = f"{b}/{lake}" in keep or hours(when) < a.older or (a.tests_only and not is_test(lake))
        print(f"{'keep  ' if kept else 'delete'} {when:%m-%d %H:%M} {len(keys):6} objects  {b}/{lake}")
        if not kept:
            gone.add((b, lake))
            if not a.dry_run:
                delete(s3, b, keys)
    stuck = collections.Counter()
    for b in a.bucket:
        for key, upload, begun in uploads(s3, b):
            lake = top(key)
            if (b, lake) in gone or (hours(begun) >= max(a.older, 24) and (not a.tests_only or is_test(lake))):
                stuck[b] += 1
                print(f"abort  {begun:%m-%d %H:%M} upload of {b}/{key}")
                if not a.dry_run:
                    s3.abort_multipart_upload(Bucket=b, Key=key, UploadId=upload)
    if a.report:
        verb = "would be deleted (a dry run)" if a.dry_run else "deleted"
        with open(a.report, "a") as out:
            for b in a.bucket:
                total, objects = sum(n for _, _, n in held[b].values()), sum(len(k) for _, k, _ in held[b].values())
                print(f"### What the bucket holds: {total / 1e9:.2f} GB in {objects} objects\n", file=out)
                print("| lake | objects | GB | last written (UTC) | |\n|---|---|---|---|---|", file=out)
                for lake, (when, keys, n) in sorted(held[b].items(), key=lambda kv: -kv[1][2]):
                    print(f"| `{lake}` | {len(keys)} | {n / 1e9:.3f} | {when:%Y-%m-%d %H:%M} | {verb if (b, lake) in gone else ''} |", file=out)
                print(f"\nUnfinished uploads {'that would be ' if a.dry_run else ''}aborted: {stuck[b]}.\n", file=out)
