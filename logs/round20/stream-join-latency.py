# A stream join's delay: an order, then its payment; the time from the payment's ack to the pair
# in the view (polled every 2 ms), 40 times, one node on local disk. Usage: python3 stream-join-latency.py <pondra>
import json, statistics, subprocess, sys, time, urllib.request, shutil
bin_, port, lake = sys.argv[1], 8431, "/tmp/pondra-sjl"
shutil.rmtree(lake, ignore_errors=True)
p = subprocess.Popen([bin_, "serve", "--dir", lake, "--addr", f"127.0.0.1:{port}"], stderr=subprocess.DEVNULL)
q = lambda s: json.loads(urllib.request.urlopen(urllib.request.Request(f"http://127.0.0.1:{port}/sql", s.encode()), timeout=30).read())
for _ in range(100):
    try: q("SELECT 1"); break
    except Exception: time.sleep(0.1)
q("CREATE TABLE orders (id BIGINT, ts TIMESTAMP)")
q("CREATE TABLE payments (order_id BIGINT, ts TIMESTAMP)")
q("CREATE MATERIALIZED VIEW paid WITH (join = 'streams', time = 'ts', within_secs = 600) AS SELECT o.id FROM orders o JOIN payments p ON o.id = p.order_id AND p.ts BETWEEN o.ts AND o.ts + INTERVAL '10 minutes'")
lat = []
for i in range(40):
    q(f"INSERT INTO orders VALUES ({i}, TIMESTAMP '2026-09-26 10:00:00')")
    q(f"INSERT INTO payments VALUES ({i}, TIMESTAMP '2026-09-26 10:05:00')")
    t = time.time()
    while not q(f"SELECT id FROM paid WHERE id = {i} -- {time.time()}"):
        time.sleep(0.002)
    lat.append(1000 * (time.time() - t))
p.kill(); shutil.rmtree(lake, ignore_errors=True)
print(json.dumps({"pairs": len(lat), "ms_p50": round(statistics.median(lat), 1), "ms_p90": round(sorted(lat)[int(0.9 * len(lat))], 1), "ms_max": round(max(lat), 1)}))
