#!/usr/bin/env bash
# A cold node's bucket requests, one by one: the simulator (near R2's latency) traces each request,
# a node starts on a new lake and runs a first write, then starts again on the same lake (C5).
# Then what each node said of its own steps (PONDRA_TRACE_START): which step the time went to.
# usage: tools/cold_trace.sh [put_p50_ms get_p50_ms]   (the simulator only: its credentials are fake)
set -u; P=$(cd "$(dirname "$0")/.." && pwd); W=$(mktemp -d); SP=9571; NP=8571
python3 "$P/tools/sim_r2.py" --port $SP --put-p50 "${1:-300}" --get-p50 "${2:-165}" --trace > "$W/trace" 2>/dev/null & SIM=$!
export AWS_ENDPOINT=http://127.0.0.1:$SP AWS_ACCESS_KEY_ID=k AWS_SECRET_ACCESS_KEY=s AWS_REGION=us-east-1 AWS_ALLOW_HTTP=true
for i in $(seq 100); do curl -s -o /dev/null "$AWS_ENDPOINT/__sim/stats" && break; sleep 0.1; done
python3 -c "import boto3; boto3.client('s3', endpoint_url='$AWS_ENDPOINT', region_name='us-east-1', aws_access_key_id='k', aws_secret_access_key='s').create_bucket(Bucket='coldbucket')"
mark() { echo "$(date +%s.%N | cut -c1-14) $1" >> "$W/marks"; }
run() { # label, statement
  mark "$1 start"; PONDRA_TRACE_START=1 "$P/target/release/pondra" serve --lake s3://coldbucket/lake --addr 127.0.0.1:$NP > "$W/$1.log" 2>&1 & N=$!
  for i in $(seq 3000); do curl -s -o /dev/null localhost:$NP/stats && break; sleep 0.02; done; mark "$1 serving"
  curl -s -X POST localhost:$NP/sql -d "$2" > /dev/null; mark "$1 answered"; kill $N; wait $N 2>/dev/null
}
run new "CREATE TABLE people AS SELECT 1 AS id"
run existing "INSERT INTO people VALUES (2)"
kill $SIM
python3 - "$W" <<'PY'
import re, sys
w = sys.argv[1]
marks = {" ".join(l.split()[1:]): float(l.split()[0]) for l in open(f"{w}/marks")}
def name(p):  # (a listing by its prefix; ids and numbers masked)
    path, _, q = p.strip().partition("?")
    prefix = re.search(r"prefix=([^&]*)", q)
    return re.sub(r"\d{6,}", "<n>", re.sub(r"[0-9a-f]{8}-[0-9a-f-]{27}", "<uuid>", path + (f" prefix={prefix[1]}" if prefix else "")))
reqs = sorted((float(t), k, name(p)) for t, k, p in (l.split(None, 2) for l in open(f"{w}/trace") if re.match(r"^\d+\.\d+ ", l)))
for run in ["new", "existing"]:
    s, up, done = marks[f"{run} start"], marks[f"{run} serving"], marks[f"{run} answered"]
    start = [r for r in reqs if s <= r[0] <= up]
    print(f"== {run} lake: serving after {up - s:.2f} s ({len(start)} requests); the statement {done - up:.2f} s "
          f"({sum(up < r[0] <= done for r in reqs)} requests)")
    for t, k, p in start:
        print(f"  +{t - s:5.2f} {k:4} {p}")
PY
for r in new existing; do echo "== $r: the node said"; grep "^start:" "$W/$r.log"; done
rm -rf "$W"
