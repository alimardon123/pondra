#!/usr/bin/env bash
# Run Pondra's correctness and latency tests against a real S3-compatible bucket (Cloudflare R2,
# AWS S3, MinIO) from your own machine. Needs Rust (cargo) and Python 3; prints a short summary.
#
#   export AWS_ACCESS_KEY_ID=… AWS_SECRET_ACCESS_KEY=… AWS_REGION=auto
#   export AWS_ENDPOINT=https://<account-id>.r2.cloudflarestorage.com PONDRA_BUCKET=<bucket>
#   tools/r2_test.sh
set -eu
cd "$(dirname "$0")/.."
: "${AWS_ENDPOINT:?set AWS_ENDPOINT}" "${PONDRA_BUCKET:?set PONDRA_BUCKET}"
cargo build --release
out=$(mktemp -d)
for t in "harness.py fence" "harness.py upsert" "harness.py crash --runs 2 --batches 100" \
         "cluster.py race --nodes 3" "cluster.py latency --secs 20" "cluster.py failover --secs 45" "cluster.py users --secs 30"; do
  echo "== $t"
  python3 tools/$t --s3 2>&1 | tail -4 | tee -a "$out/summary.txt"
done
echo; echo "Summary saved to $out/summary.txt (each test deletes its lake when it finishes; tools/clean_bucket.py sweeps leftovers)."
