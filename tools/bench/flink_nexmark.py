"""Flink's side of `nexmark.py` (PyFlink, a local MiniCluster, streaming mode): the bids from a
bounded datagen source (in Flink's own process: no ingest cost), each query alone into a
blackhole sink, then all five over one source. Prints one JSON line.
  flink_nexmark.py N BID_JSON BASE_MS PER_MS"""
import json, sys, time
t0 = time.time()
from pyflink.table import EnvironmentSettings, TableEnvironment
n, bid, base, per_ms = int(sys.argv[1]), json.loads(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4])
env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
conf = env.get_config()
conf.set("parallelism.default", "2")
conf.set("taskmanager.memory.process.size", "3g")
conf.set("table.exec.mini-batch.enabled", "true")  # (Flink's advice for aggregations)
conf.set("table.exec.mini-batch.allow-latency", "200 ms")
conf.set("table.exec.mini-batch.size", "5000")
env.execute_sql("SELECT 1").wait()  # (the JVM and the MiniCluster start outside the timings)
env.execute_sql(f"""CREATE TABLE src (id BIGINT, ts AS TO_TIMESTAMP_LTZ({base} + id / {per_ms}, 3), WATERMARK FOR ts AS ts)
    WITH ('connector'='datagen', 'fields.id.kind'='sequence', 'fields.id.start'='0', 'fields.id.end'='{n - 1}',
          'number-of-rows'='{n}', 'rows-per-second'='1000000000')""")
env.execute_sql("CREATE TEMPORARY VIEW b AS SELECT " + ", ".join(f"{e} AS {c}" for c, e in bid.items()) + ", ts FROM src")
sinks = {"q1": "auction BIGINT, bidder BIGINT, euros DOUBLE, ts TIMESTAMP_LTZ(3)", "q2": "auction BIGINT, price DOUBLE",
         "q5": "w TIMESTAMP_LTZ(3), auction BIGINT, n BIGINT", "q7": "w TIMESTAMP_LTZ(3), top DOUBLE", "q11": "bidder BIGINT, s TIMESTAMP_LTZ(3), n BIGINT"}
for k, s in sinks.items():
    env.execute_sql(f"CREATE TABLE {k}_sink ({s}) WITH ('connector'='blackhole')")
queries = {
    "q1": "SELECT auction, bidder, price * 0.908, ts FROM b",
    "q2": "SELECT auction, price FROM b WHERE auction % 123 = 0",
    "q5": "SELECT window_start, auction, count(*) FROM TABLE(HOP(TABLE b, DESCRIPTOR(ts), INTERVAL '2' SECOND, INTERVAL '10' SECOND)) GROUP BY window_start, window_end, auction",
    "q7": "SELECT window_start, max(price) FROM TABLE(TUMBLE(TABLE b, DESCRIPTOR(ts), INTERVAL '10' SECOND)) GROUP BY window_start, window_end",
    "q11": "SELECT bidder, window_start, count(*) FROM TABLE(SESSION(TABLE b PARTITION BY bidder, DESCRIPTOR(ts), INTERVAL '3' SECOND)) GROUP BY bidder, window_start, window_end",
}
out = {"engine": "flink", "bids": n, "startup_s": round(time.time() - t0, 2)}
for k, sql in queries.items():
    t = time.time()
    env.execute_sql(f"INSERT INTO {k}_sink {sql}").wait()
    out[k + "_s"] = round(time.time() - t, 2)
t = time.time()
ss = env.create_statement_set()
for k, sql in queries.items():
    ss.add_insert_sql(f"INSERT INTO {k}_sink {sql}")
ss.execute().wait()
out["secs"] = round(time.time() - t, 2)
out["bids_per_s"] = round(n / out["secs"])
print(json.dumps(out), flush=True)
