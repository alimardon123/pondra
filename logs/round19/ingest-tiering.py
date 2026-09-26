# Round 19 against round 18 (both glibc 2.17 dist builds): 8 M rows appended as Arrow over HTTP
# (16 x 500k), then one /tier; files written. Usage: python3 ingest-tiering.py <pondra> <port>.
import json, os, sys, time, subprocess, shutil, urllib.request
import pyarrow as pa
bin_ = sys.argv[1]; port = int(sys.argv[2])
lake = f"/tmp/pondra-tier2-{port}"; shutil.rmtree(lake, ignore_errors=True)
p = subprocess.Popen([bin_, "serve", "--dir", lake, "--addr", f"127.0.0.1:{port}", "--tier-secs", "600", "--backlog", "100000000"], stderr=subprocess.DEVNULL)
def call(path, body=None, headers={}):
    return urllib.request.urlopen(urllib.request.Request(f"http://127.0.0.1:{port}{path}", data=body, headers=headers), timeout=600).read()
for _ in range(100):
    try: call("/stats"); break
    except Exception: time.sleep(0.1)
call("/sql", b"CREATE TABLE ev (user VARCHAR, amount BIGINT, sent BIGINT)")
R = 500_000
batch = pa.record_batch([pa.array([f"user-{j % 1000}" for j in range(R)]), pa.array(range(R), pa.int64()), pa.array([0] * R, pa.int64())], names=["user", "amount", "sent"])
sink = pa.BufferOutputStream()
with pa.ipc.new_stream(sink, batch.schema) as w:
    w.write_batch(batch)
body = sink.getvalue().to_pybytes()
t0 = time.time()
for i in range(16):
    call(f"/append/ev?producer=p&seq={i+1}", body, {"content-type": "application/vnd.apache.arrow.stream"})
put = time.time() - t0
untiered = json.loads(call("/stats")).get("untiered_rows")
t0 = time.time(); call("/tier", b""); tier = time.time() - t0
n = json.loads(call("/sql", b"SELECT count(*) AS n FROM ev"))[0]["n"]
import glob
size = sum(os.path.getsize(f) for f in glob.glob(f"{lake}/data/ev/*.parquet"))
p.kill(); p.wait()
print(json.dumps({"bin": os.path.basename(bin_), "rows": n, "append_rows_per_s": round(16 * R / put), "untiered_before_tier": untiered, "tier_s": round(tier, 2), "tier_rows_per_s": round(16 * R / tier), "parquet_mb": round(size / 1e6, 1)}))
shutil.rmtree(lake, ignore_errors=True)
