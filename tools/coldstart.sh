#!/usr/bin/env bash
# Cold-start test: ingest N flushes into a fresh lake, then time writer restarts and one-shot reads.
# usage: coldstart.sh <lake-url> <flushes-per-producer> [tier_secs]
set -u; BIN=$(dirname "$0")/../target/release/pondra; LAKE=$1; N=$2; TIER=${3:-5}; PORT=8099
ready() { until curl -s localhost:$PORT/stats >/dev/null; do sleep 0.02; done; }
$BIN serve --dir "$LAKE" --addr 127.0.0.1:$PORT --flush-ms 20 --tier-secs "$TIER" 2>/tmp/cold.err & P=$!; ready
curl -s -X POST localhost:$PORT/tables/events -d '[["user","Utf8"],["amount","Int64"]]' >/dev/null
python3 - "$N" <<'PY'
import http.client, sys, threading
def go(p):
    c = http.client.HTTPConnection("127.0.0.1", 8099)
    for s in range(1, int(sys.argv[1]) + 1):
        c.request("POST", f"/append/events?producer={p}&seq={s}", b'{"user":"a","amount":1}\n'); c.getresponse().read()
ts = [threading.Thread(target=go, args=(f"p{k}",)) for k in range(4)]; [t.start() for t in ts]; [t.join() for t in ts]
PY
sleep $((TIER + 1)); echo "ingested: $(curl -s localhost:$PORT/stats)"; kill $P; wait $P 2>/dev/null
for i in 1 2 3; do
  t=$(date +%s%N); $BIN serve --dir "$LAKE" --addr 127.0.0.1:$PORT --tier-secs 0 2>>/tmp/cold.err & P=$!; ready
  echo "writer restart: $(( ($(date +%s%N)-t)/1000000 )) ms"; kill $P; wait $P 2>/dev/null
done
for i in 1 2 3; do
  t=$(date +%s%N); n=$($BIN sql --dir "$LAKE" "SELECT count(*) AS n FROM events" | sed -n 4p)
  echo "one-shot reader query: $(( ($(date +%s%N)-t)/1000000 )) ms, result $n"
done
