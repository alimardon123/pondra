"""Flink's side of `nexmark.py` (PyFlink, a local MiniCluster, streaming mode): the three streams
from bounded datagen sources (in Flink's own process: no ingest cost), each query alone into a
blackhole sink, then all of them over one set of sources. Prints one JSON line.
`check`: instead, each stream's counts and sums, in batch mode, to compare with DuckDB's rows.
  flink_nexmark.py SPEC_JSON"""
import json, os, sys, tempfile, time
t0 = time.time()
from pyflink.table import EnvironmentSettings, TableEnvironment
spec = json.load(open(sys.argv[1]))
batch = spec["check"]
env = TableEnvironment.create(EnvironmentSettings.in_batch_mode() if batch else EnvironmentSettings.in_streaming_mode())
conf = env.get_config()
conf.set("parallelism.default", "2")
# (Memory: in a local MiniCluster Flink runs inside PyFlink's gateway JVM and ignores taskmanager.memory.process.size;
# its heap is JVM_ARGS's -Xmx, which nexmark.py sets. At the JVM's default, a quarter of RAM, q9's state on the heap
# stalled garbage collection until the job's heartbeat timed out at 10M events, and with 10 GB all twenty queries
# at once did. `state` rocksdb keeps state in RocksDB, off the heap, as Flink's own Nexmark setup does.)
if spec.get("state") == "rocksdb":
    conf.set("state.backend.type", "rocksdb")
    conf.set("taskmanager.memory.managed.size", spec["managed"])  # (RocksDB's memory: local execution keeps a size it is given)
conf.set("table.local-time-zone", "UTC")
conf.set("table.exec.mini-batch.enabled", "true")  # (Flink's advice for aggregations)
conf.set("table.exec.mini-batch.allow-latency", "200 ms")
conf.set("table.exec.mini-batch.size", "5000")
env.execute_sql("SELECT 1").wait()  # (the JVM and the MiniCluster start outside the timings)
for s, cols in spec["streams"].items():
    n = spec["counts"][s]
    computed = ", ".join(f"{c} AS {sql}" for c, _, sql in cols)
    env.execute_sql(f"""CREATE TABLE {s} (k BIGINT, {computed}{'' if batch else ', WATERMARK FOR date_time AS date_time'})
        WITH ('connector'='datagen', 'fields.k.kind'='sequence', 'fields.k.start'='0', 'fields.k.end'='{max(n - 1, 0)}', 'number-of-rows'='{n}',
              'rows-per-second'='1000000000')""")
side = tempfile.mkdtemp(prefix="flink-side-")
with open(os.path.join(side, "side.csv"), "w") as f:
    f.writelines(f"{i},{label}\n" for i, label in spec["side_input"])
env.execute_sql(f"CREATE TABLE side_input (id BIGINT, label STRING) WITH ('connector'='filesystem', 'path'='{side}', 'format'='csv')")

if batch:
    out = {"engine": "flink", "check": {}}
    for s, cols in spec["streams"].items():
        parts = ["count(*)"] + [f"sum({c})" if t == "BIGINT" else f"sum(CHAR_LENGTH({c}))" for c, t, _ in cols if t not in ("TIMESTAMP",)] + \
                [f"sum(CAST(TIMESTAMPDIFF(SECOND, TO_TIMESTAMP_LTZ(0, 3), {c}) AS BIGINT))" for c, t, _ in cols if t == "TIMESTAMP"]
        with env.execute_sql(f"SELECT {', '.join(parts)} FROM {s}").collect() as rows:
            out["check"][s] = [int(v) for v in next(iter(rows))]
    print(json.dumps(out), flush=True)
    sys.exit(0)


def sink(name, sql):
    """A blackhole table with the query's columns (its time attributes as plain timestamps)."""
    schema = env.sql_query(sql).get_schema()
    cols = ", ".join(f"`{c}` {str(t).replace(' *ROWTIME*', '').replace(' *PROCTIME*', '').replace('LOCALZONEDTIMESTAMP', 'TIMESTAMP_LTZ')}" for c, t in zip(schema.get_field_names(), schema.get_field_data_types()))
    env.execute_sql(f"CREATE TABLE {name}_sink ({cols}) WITH ('connector'='blackhole')")


def why(e):
    """Java's own message, not Py4J's "An error occurred while calling …"."""
    lines = [l.strip() for l in str(e).splitlines()]
    said = [l for l in lines if "Exception: " in l and not l.startswith("py4j")]
    return (said[-1] if said else lines[0] if lines else repr(e))[:400]


queries = spec["queries"]
out = {"engine": "flink", "events": sum(spec["counts"].values()), "state": spec.get("state", "heap"), "jvm_args": os.environ.get("JVM_ARGS", ""),
       "startup_s": round(time.time() - t0, 2), "alone_s": {}, "failed": {}}
for k, sql in queries.items():
    try:
        sink(k, sql)
        t = time.time()
        env.execute_sql(f"INSERT INTO {k}_sink {sql}").wait()
        out["alone_s"][k] = round(time.time() - t, 2)
    except Exception as e:
        out["failed"][k] = why(e)
if spec.get("together", True) and out["alone_s"]:
    t = time.time()
    ss = env.create_statement_set()
    for k in out["alone_s"]:
        ss.add_insert_sql(f"INSERT INTO {k}_sink {queries[k]}")
    try:
        ss.execute().wait()
        out["secs"] = round(time.time() - t, 2)
        out["events_per_s"] = round(out["events"] / out["secs"])
    except Exception as e:
        out["together_failed"] = f"after {time.time() - t:.0f} s: {why(e)}"
print(json.dumps(out), flush=True)
