# How many objects a lake makes: one table streamed into (publishing Delta + Iceberg), one view, for SECS.
import os, subprocess, time, urllib.request, collections, shutil, sys, threading
BIN = os.environ.get("BIN", "/home/claude/pondra/target/release/pondra")
lake, port, SECS, RATE = "/tmp/pondra-files", 8391, int(sys.argv[1]), int(sys.argv[2])
shutil.rmtree(lake, ignore_errors=True)
node = subprocess.Popen([BIN, "serve", "--dir", lake, "--addr", f"127.0.0.1:{port}"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=dict(os.environ, **({"PONDRA_GC_SECS": sys.argv[4]} if len(sys.argv) > 4 else {})))
def sql(s):
    return urllib.request.urlopen(urllib.request.Request(f"http://127.0.0.1:{port}/sql", s.encode(), method="POST"), timeout=60).read()
for _ in range(50):
    try: sql("SELECT 1"); break
    except Exception: time.sleep(0.2)
sql("CREATE TABLE ev (user_id BIGINT, amount DOUBLE, ts TIMESTAMP) WITH (publish = 'delta,iceberg')")
seen, stop = set(), False
def watch():
    while not stop:
        for d, _, fs in os.walk(lake):
            for f in fs: seen.add(os.path.relpath(os.path.join(d, f), lake))
        time.sleep(0.25)
threading.Thread(target=watch, daemon=True).start()
t0, i = time.time(), 0
while time.time() - t0 < SECS:
    sql(f"INSERT INTO ev VALUES ({i % 100}, {i * 0.5}, now())"); i += 1
    time.sleep(max(0, t0 + i / RATE - time.time()))
time.sleep(int(sys.argv[3]))  # (replaced files are deleted after --retain-secs, 60)
stop = True; time.sleep(0.5)
def group(p):
    import re
    p = re.sub(r"[0-9a-f-]{36}", "*", p); p = re.sub(r"\d{5,}", "N", p)
    parts = p.split("/")
    return "/".join(parts[:3]) if parts[0] == "data" else "/".join(parts[:2])
now = collections.Counter(group(os.path.relpath(os.path.join(d, f), lake)) for d, _, fs in os.walk(lake) for f in fs)
ever = collections.Counter(group(p) for p in seen)
print(f"{i} inserts in {SECS}s ({RATE}/s)\n{'folder':38s} {'objects now':>12s} {'made (≥)':>9s}")
for k in sorted(ever): print(f"{k:38s} {now.get(k, 0):12d} {ever[k]:9d}")
print(f"{'total':38s} {sum(now.values()):12d} {sum(ever.values()):9d}")
node.kill(); shutil.rmtree(lake, ignore_errors=True)
