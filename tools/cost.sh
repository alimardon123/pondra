#!/usr/bin/env bash
# Object-storage request counts (from the simulator) for an idle and a busy writer, priced at R2 list prices.
set -u; cd "$(dirname "$0")/.."; LAKE=s3://$PONDRA_BUCKET/cost-$RANDOM; PORT=8099
stats() { curl -s 127.0.0.1:9000/__sim/stats; }
ready() { until curl -s localhost:$PORT/stats >/dev/null; do sleep 0.02; done; }
./target/release/pondra serve --dir "$LAKE" --addr 127.0.0.1:$PORT 2>/dev/null & P=$!; ready
curl -s -X POST localhost:$PORT/tables/events -d '[["producer","Utf8"],["seq","Int64"],["i","Int64"],["ts","Float64"]]' >/dev/null
a=$(stats); sleep 60; b=$(stats)
python3 tools/harness.py load --s3 --port 8095 --producers 8 --size 2500 --secs 60 >/tmp/cost-load.json 2>&1 &   # separate lake, measured below
kill $P; wait $P 2>/dev/null; c=$(stats); wait; d=$(stats)
python3 - "$a" "$b" "$c" "$d" <<'PY'
import json, sys
a, b, c, d = (json.loads(x) for x in sys.argv[1:])
def cost(x, y, secs, label):
    diff = {k: y.get(k, 0) - x.get(k, 0) for k in ("PUT", "GET", "LIST")}
    per_s = {k: v / secs for k, v in diff.items()}
    month = 30 * 86400
    usd = ((per_s["PUT"] + per_s["LIST"]) * 4.50 + per_s["GET"] * 0.36) * month / 1e6
    print(f"{label}: {', '.join(f'{k} {v:.2f}/s' for k, v in per_s.items())} -> ${usd:.2f}/month in R2 requests")
cost(a, b, 60, "idle writer")
cost(c, d, 60, "writer at the load below (incl. tiering, probes, 60 s)")
PY
tail -12 /tmp/cost-load.json
