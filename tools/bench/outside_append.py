"""What another engine's append costs the node (ADR-028's G8, measured for ADR-029). PyIceberg
appends through the node's Iceberg REST catalog; the node's CPU time (/proc) and the commit's
wall time, for a tiny append (the fixed cost of a commit) and a big one (round 25 copies its
rows; ADR-029 would only commit).

    python3 tools/bench/outside_append.py"""
import http.client, json, os, subprocess, sys, tempfile, time, shutil
import pyarrow as pa, pyarrow.compute as pc
from pyiceberg.catalog import load_catalog

BIN = os.environ.get("PONDRA_BIN") or os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "target", "release", "pondra")
PORT = 18390
lake = tempfile.mkdtemp(prefix="pondra-")
p = subprocess.Popen([BIN, "serve", "--dir", lake, "--addr", f"127.0.0.1:{PORT}"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def call(method, path, body=b""):
    c = http.client.HTTPConnection("127.0.0.1", PORT, timeout=600)
    c.request(method, path, body)
    r = c.getresponse()
    d = r.read()
    assert r.status == 200, d[:500]
    return json.loads(d) if d else None


for _ in range(100):
    try:
        call("GET", "/stats"); break
    except Exception:
        time.sleep(0.1)
tick = os.sysconf("SC_CLK_TCK")
cpu = lambda: sum(int(x) for x in open(f"/proc/{p.pid}/stat").read().rsplit(")", 1)[1].split()[11:13]) / tick
call("POST", "/sql", b"CREATE TABLE events (id BIGINT, name VARCHAR, amount DOUBLE, ts BIGINT) WITH (publish = 'iceberg')")
t = load_catalog("pondra", type="rest", uri=f"http://127.0.0.1:{PORT}").load_table("default.events")


def rows(n, lo=0):
    ids = pa.array(range(lo, lo + n), pa.int64())
    return pa.table({"id": ids, "name": pc.binary_join_element_wise("user", pc.cast(pc.divide(ids, 7), pa.string()), ""),
                     "amount": pc.multiply(pc.cast(ids, pa.float64()), 0.25), "ts": pc.add(ids, 1_700_000_000_000)})


out = {}
for label, n in [("tiny", 10), ("big", 4_000_000), ("tiny again", 10)]:
    data = rows(n, 10_000_000 if label == "tiny again" else 0)
    time.sleep(3)  # (settled)
    idle0 = cpu(); time.sleep(2); idle = (cpu() - idle0) / 2  # (per second, idle)
    c0, w0 = cpu(), time.time()
    t.append(data)
    w = time.time() - w0
    t.refresh()
    size = int(t.current_snapshot().summary.additional_properties.get("added-files-size", 0) or 0)
    time.sleep(3)  # (what follows the commit: publishing)
    c = cpu() - c0 - idle * (time.time() - w0)
    out[label] = {"rows": n, "parquet_mb": round(size / 1e6, 1), "client_wall_s": round(w, 2), "node_cpu_s": round(c, 2)}
got = call("POST", "/sql", b"SELECT count(*) AS n, count(DISTINCT _row_id) AS ids FROM events")
big = out["big"]
out["node_cpu_s_per_gb"] = round((big["node_cpu_s"] - out["tiny"]["node_cpu_s"]) / (big["parquet_mb"] / 1000), 1) if big["parquet_mb"] else None
out["rows_ok"] = got
print(json.dumps(out, indent=1))
p.kill(); p.wait(); shutil.rmtree(lake, ignore_errors=True)
