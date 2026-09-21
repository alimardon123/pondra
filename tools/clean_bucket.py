#!/usr/bin/env python3
"""Delete test lakes from a bucket, keeping the newest N (and any named with --keep). R2's free tier
is 10 GB, so test runs clean up after themselves (harness.new_lake) and this sweeps what's left.
  clean_bucket.py --bucket B [--bucket B2] [--newest 3] [--keep prefix ...] [--dry-run]"""
import argparse, collections, os
import boto3


def lakes(s3, bucket):
    """Top-level lakes (`test-…`, `roundN/test-…`, anything else at the root) -> (newest object, keys)."""
    out = collections.defaultdict(lambda: [None, []])
    for pg in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket):
        for o in pg.get("Contents", []):
            parts = o["Key"].split("/")
            top = "/".join(parts[:2]) if parts[0].startswith("round") and len(parts) > 2 else parts[0]
            lake = out[top]
            lake[0] = max(lake[0] or o["LastModified"], o["LastModified"])
            lake[1].append(o["Key"])
    return out


def delete(s3, bucket, keys):
    for i in range(0, len(keys), 1000):
        s3.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": k} for k in keys[i:i + 1000]], "Quiet": True})


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--bucket", action="append", required=True)
    ap.add_argument("--newest", type=int, default=3, help="keep this many of the newest lakes, across all buckets")
    ap.add_argument("--keep", action="append", default=[], help="bucket/prefix to keep regardless")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    s3 = boto3.client("s3", endpoint_url=os.environ.get("AWS_ENDPOINT"), region_name=os.environ.get("AWS_REGION", "auto"))
    found = [(when, b, top, keys) for b in a.bucket for top, (when, keys) in lakes(s3, b).items()]
    found.sort(reverse=True)
    keep = {f"{b}/{top}" for _, b, top, _ in found[:a.newest]} | set(a.keep)
    for when, b, top, keys in found:
        kept = f"{b}/{top}" in keep
        print(f"{'keep  ' if kept else 'delete'} {when:%m-%d %H:%M} {len(keys):6} objects  {b}/{top}")
        if not kept and not a.dry_run:
            delete(s3, b, keys)
