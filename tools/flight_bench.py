#!/usr/bin/env python3
"""Arrow Flight throughput and latency on one node (pyarrow clients).

  flight_bench.py [--s3] [--writers 4] [--batches 200] [--rows 10000]

- DoPut: `writers` clients, each streaming `batches` Arrow batches of `rows` rows (exactly-once
  producers), all at once: rows/s and MB/s until the last ack.
- DoGet: the whole table back as Arrow (`SELECT *`): rows/s and MB/s.
- The log as a stream: a follower subscribed to one column while a producer sends a small batch
  every 20 ms; time from sending to receiving (p50/p99).
"""
import argparse, json, os, statistics, sys, threading, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import harness
from harness import Node, call

def main():
    import pyarrow as pa, pyarrow.flight as fl
    lake = harness.new_lake()
    port, fport = A.port, A.port + 30
    Node(lake, port, flight=f"127.0.0.1:{fport}", tier_secs=1).start()
    call(port, "POST", "/sql", b"CREATE TABLE ev (user VARCHAR, amount BIGINT, sent BIGINT)")
    schema = pa.schema([("user", pa.string()), ("amount", pa.int64()), ("sent", pa.int64())])
    batch = pa.record_batch([pa.array([f"user-{j % 1000}" for j in range(A.rows)]), pa.array(range(A.rows), pa.int64()), pa.array([0] * A.rows, pa.int64())], schema=schema)
    mb = batch.nbytes / 1e6
    def writer(w):
        c = fl.FlightClient(f"grpc://127.0.0.1:{fport}")
        wr, rd = c.do_put(fl.FlightDescriptor.for_path("ev", f"w{w}", "1"), schema)
        acks = [0]
        def read():
            while rd.read() is not None:
                acks[0] += 1
        t = threading.Thread(target=read); t.start()
        for _ in range(A.batches):
            wr.write_batch(batch)
        wr.done_writing(); t.join(); wr.close()
        assert acks[0] == A.batches, acks
    start = time.time()
    ts = [threading.Thread(target=writer, args=(w,)) for w in range(A.writers)]
    [t.start() for t in ts]; [t.join() for t in ts]
    put_s = time.time() - start
    n = A.writers * A.batches * A.rows
    total = call(port, "POST", "/sql", b"SELECT count(*) AS n FROM ev")[0]["n"]
    client = fl.FlightClient(f"grpc://127.0.0.1:{fport}")
    start = time.time()
    back = client.do_get(fl.Ticket(json.dumps({"sql": "SELECT * FROM ev"}))).read_all()
    get_s = time.time() - start
    # Follow latency: `sent` carries the send time (µs).
    live = client.do_get(fl.Ticket(json.dumps({"table": "ev", "columns": ["sent"]})))
    lat, stop = [], threading.Event()
    def follow():
        for chunk in live:
            if chunk.data is not None:
                now = time.time() * 1e6
                lat.extend((now - s) / 1000 for s in chunk.data.column(0).to_pylist())
            if len(lat) >= 100:
                return
    t = threading.Thread(target=follow, daemon=True); t.start()
    time.sleep(0.3)
    wr, rd = client.do_put(fl.FlightDescriptor.for_path("ev", "live", "1"), schema)
    for i in range(100):
        wr.write_batch(pa.record_batch([pa.array(["x"]), pa.array([i], pa.int64()), pa.array([int(time.time() * 1e6)], pa.int64())], schema=schema))
        time.sleep(0.02)
    wr.done_writing()
    while rd.read() is not None:
        pass
    stop.set(); t.join(10); live.cancel()
    lat.sort()
    out = {"doput": {"writers": A.writers, "rows": n, "rows_per_s": round(n / put_s), "mb_per_s": round(A.writers * A.batches * mb / put_s, 1), "all_in": total == n},
           "doget": {"rows": back.num_rows, "rows_per_s": round(back.num_rows / get_s), "mb_per_s": round(back.nbytes / 1e6 / get_s, 1)},
           "follow_ms": {"p50": round(lat[len(lat) // 2], 1), "p99": round(lat[int(len(lat) * 0.99)], 1), "samples": len(lat)} if lat else None}
    print(json.dumps(out, indent=1))

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--s3", action="store_true")
    ap.add_argument("--port", type=int, default=8096)
    ap.add_argument("--writers", type=int, default=4)
    ap.add_argument("--batches", type=int, default=200)
    ap.add_argument("--rows", type=int, default=10000)
    A = ap.parse_args()
    harness.A = A
    main()
