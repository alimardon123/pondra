"""Flink 2.3 (PyFlink, local MiniCluster) side of the benchmark. Prints one JSON line.
  flink_bench.py batch  N DIR    generate N rows -> CSV files (no Parquet jar offline), then the shared queries
  flink_bench.py stream N KEYS   keyed running aggregation over a bounded datagen stream (streaming mode)
  flink_bench.py etl    N        stateless streaming filter/projection"""
import json, sys, time
t0 = time.time()
from pyflink.table import EnvironmentSettings, TableEnvironment
from queries import GEN, QUERIES

mode, n = sys.argv[1], int(sys.argv[2])
batch = mode == "batch"
env = TableEnvironment.create(EnvironmentSettings.in_batch_mode() if batch else EnvironmentSettings.in_streaming_mode())
conf = env.get_config()
conf.set("parallelism.default", "2")
conf.set("taskmanager.memory.process.size", "3g")
if not batch:  # Flink's recommended settings for aggregations: mini-batch + local/global aggregation
    conf.set("table.exec.mini-batch.enabled", "true")
    conf.set("table.exec.mini-batch.allow-latency", "200 ms")
    conf.set("table.exec.mini-batch.size", "5000")
env.execute_sql("SELECT 1").wait()  # start the JVM + MiniCluster once, outside the timings
out = {"engine": "flink", "mode": mode, "rows": n, "startup_s": round(time.time() - t0, 2)}
env.execute_sql(f"""CREATE TABLE gen (id BIGINT) WITH ('connector'='datagen', 'fields.id.kind'='sequence',
    'fields.id.start'='0', 'fields.id.end'='{n - 1}', 'number-of-rows'='{n}', 'rows-per-second'='1000000000')""")
cols = ", ".join(f"{e} AS {c}" for c, e in GEN.items())


def run(sql):
    t = time.time()
    r = env.execute_sql(sql)
    if sql.lstrip().upper().startswith("SELECT"):
        with r.collect() as rows:
            for _ in rows:
                pass
    else:
        r.wait()
    return round(time.time() - t, 3)


if batch:
    d = sys.argv[3]
    env.execute_sql(f"""CREATE TABLE events (user_id BIGINT, amount DOUBLE, category STRING, ts_ms BIGINT)
        WITH ('connector'='filesystem', 'path'='file://{d}/events', 'format'='csv')""")
    out["write_s"] = run(f"INSERT INTO events SELECT {cols} FROM gen")
    env.execute_sql("""CREATE TEMPORARY VIEW dims AS SELECT concat('c', CAST(id AS STRING)) AS category,
        concat('r', CAST(id % 10 AS STRING)) AS region FROM (VALUES """ + ",".join(f"({i})" for i in range(1000)) + ") AS v(id)")
    for name, q in QUERIES.items():
        out[name] = [run(q), run(q)]
else:
    env.execute_sql("CREATE TABLE sink (user_id BIGINT, n BIGINT, s DOUBLE) WITH ('connector'='blackhole')")
    env.execute_sql("CREATE TABLE sink2 (user_id BIGINT, amount DOUBLE, category STRING, ts_ms BIGINT) WITH ('connector'='blackhole')")
    if mode == "stream":
        keys = int(sys.argv[3])
        secs = run(f"""INSERT INTO sink SELECT id % {keys}, count(*), sum(CAST(id % 10000 AS DOUBLE) / 100)
                       FROM gen GROUP BY id % {keys}""")
    else:
        secs = run(f"INSERT INTO sink2 SELECT * FROM (SELECT {cols} FROM gen) WHERE amount > 50")
    out.update(processed=n, secs=secs, rows_per_s=round(n / secs))
print(json.dumps(out), flush=True)
