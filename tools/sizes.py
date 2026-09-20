#!/usr/bin/env python3
"""Storage bytes per event: the same 1M realistic events as NDJSON (what producers send), as Pondra's
log segments (Arrow IPC + ZSTD, one per 250 ms flush) and as Pondra's Parquet files after tiering."""
import io, json, os, random, subprocess, sys, tempfile, time
import pyarrow as pa, pyarrow.ipc
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import harness
from harness import Node, call

N, FLUSH_ROWS = 1_000_000, 25_000  # 100k events/s x 250 ms group commit
random.seed(1)
EVENTS = ["view", "click", "add_to_cart", "purchase", "search", "login", "logout", "share", "like", "comment"]
t0 = 1_760_000_000_000
rows = [{"user": f"user{random.randrange(100_000)}", "event": random.choice(EVENTS), "amount": random.randrange(10_000),
         "ts": t0 + i * 10 + random.randrange(10)} for i in range(N)]
ndjson = sum(len(json.dumps(r)) + 1 for r in rows)
table = pa.Table.from_pylist(rows, pa.schema([("user", pa.string()), ("event", pa.string()), ("amount", pa.int64()), ("ts", pa.int64())]))


def ipc(t, codec):
    buf = io.BytesIO()
    with pa.ipc.new_stream(buf, t.schema, options=pa.ipc.IpcWriteOptions(compression=codec)) as w:
        w.write_table(t)
    return buf.getbuffer().nbytes


seg_raw = sum(ipc(table.slice(o, FLUSH_ROWS), None) for o in range(0, N, FLUSH_ROWS))
seg_zstd = sum(ipc(table.slice(o, FLUSH_ROWS), "zstd") for o in range(0, N, FLUSH_ROWS))

# Real Pondra: ingest the same rows, tier them, measure the Parquet files it wrote.
harness.A = type("A", (), {"s3": False, "port": 18600})
lake = tempfile.mkdtemp(prefix="pondra-sizes-")
node = Node(lake, 18600, tier_secs=0, retain_secs=0).start()
import atexit; atexit.register(node.kill)
call(18600, "POST", "/tables/events", json.dumps([["user", "Utf8"], ["event", "Utf8"], ["amount", "Int64"], ["ts", "Int64"]]).encode())
for k, o in enumerate(range(0, N, 20_000), 1):
    buf = io.BytesIO()
    with pa.ipc.new_stream(buf, table.schema) as w:
        w.write_table(table.slice(o, 20_000))
    call(18600, "POST", f"/append/events?producer=p&seq={k}", buf.getvalue(), headers={"content-type": "application/vnd.apache.arrow.stream"})
print("tier:", call(18600, "POST", "/tier"))
node.kill()
parquet = sum(os.path.getsize(os.path.join(d, f)) for d, _, fs in os.walk(lake) for f in fs if f.endswith(".parquet"))
per = lambda b: f"{b / N:5.1f} B/event ({b / 2**20:6.1f} MB)"
print(f"1M events (user, event, amount, ts):")
print(f"  NDJSON as sent           {per(ndjson)}")
print(f"  log segments, no codec   {per(seg_raw)}")
print(f"  log segments, ZSTD      {per(seg_zstd)}")
print(f"  Parquet after tiering    {per(parquet)}   <- real Pondra output")
