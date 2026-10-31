"""Flink's side of `cep_bench.py` (PyFlink, a local MiniCluster, streaming mode): the ticks from their CSV file (the
filesystem connector), the match query into a blackhole sink, each run timed from submit to the job's end (one run, or
three when one takes under a minute), then the read alone (`scan_s`: the same table into a blackhole with no pattern,
timed the same way: Flink's time to read the file), then the query's rows, collected into a CSV file (the matches, for
the comparison). Prints one JSON line.
  flink_cep.py SPEC_JSON"""
import csv, json, os, sys, time
t0 = time.time()
from pyflink.table import EnvironmentSettings, TableEnvironment
spec = json.load(open(sys.argv[1]))
env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
conf = env.get_config()
conf.set("parallelism.default", str(spec["parallelism"]))
# (State as cep_bench.py sets it, with nexmark.py's convention: RocksDB with its managed memory a third of the machine's,
# or the heap. The JVM's heap is JVM_ARGS's -Xmx, which cep_bench.py sets.)
conf.set("state.backend.type", spec["state"])
if spec["state"] == "rocksdb":
    conf.set("taskmanager.memory.managed.size", spec["managed"])
conf.set("table.local-time-zone", "UTC")
env.execute_sql("SELECT 1").wait()  # (the JVM and the MiniCluster start outside the timings)
# (The watermark is a millisecond behind ts: many rows share one second, and a row whose ts equals the watermark is late.
# With WATERMARK FOR ts AS ts, the rows of a second that arrive after a watermark at that second are dropped.)
env.execute_sql(f"CREATE TABLE ticks (symbol STRING, ts TIMESTAMP(3), price INT, WATERMARK FOR ts AS ts - INTERVAL '0.001' SECOND) "
                f"WITH ('connector'='filesystem', 'path'='{spec['dir']}', 'format'='csv')")


def sink(name, sql):
    """A blackhole table with the query's columns (its time attributes as plain timestamps)."""
    schema = env.sql_query(sql).get_schema()
    cols = ", ".join(f"`{c}` {str(t).replace(' *ROWTIME*', '').replace(' *PROCTIME*', '').replace('LOCALZONEDTIMESTAMP', 'TIMESTAMP_LTZ')}"
                     for c, t in zip(schema.get_field_names(), schema.get_field_data_types()))
    env.execute_sql(f"CREATE TABLE {name} ({cols}) WITH ('connector'='blackhole')")


def timed(sql):
    t = time.time()
    env.execute_sql(sql).wait()
    return round(time.time() - t, 3)


def why(e):
    """Java's own message, not Py4J's "An error occurred while calling …"."""
    lines = [l.strip() for l in str(e).splitlines()]
    said = [l for l in lines if "Exception: " in l and not l.startswith("py4j")]
    return (said[-1] if said else lines[0] if lines else repr(e))[:400]


q = spec["query"]
out = {"engine": "flink", "parallelism": spec["parallelism"], "state": spec["state"], "jvm_args": os.environ.get("JVM_ARGS", ""),
       "startup_s": round(time.time() - t0, 2), "blackhole_s": [], "scan_s": [], "failed": None}
try:
    sink("match_blackhole", q)
    first = timed(f"INSERT INTO match_blackhole {q}")
    out["blackhole_s"] = [first] + ([timed(f"INSERT INTO match_blackhole {q}") for _ in range(2)] if first < 60 else [])
except Exception as e:
    out["failed"] = f"blackhole: {why(e)}"
if out["failed"] is None:
    # The read alone: the same table, no pattern, timed the same way (how much of the query's time is reading the file)
    try:
        sink("scan_blackhole", "SELECT * FROM ticks")
        first = timed("INSERT INTO scan_blackhole SELECT * FROM ticks")
        out["scan_s"] = [first] + ([timed("INSERT INTO scan_blackhole SELECT * FROM ticks") for _ in range(2)] if first < 60 else [])
    except Exception as e:
        out["failed"] = f"scan: {why(e)}"
if out["failed"] is None:
    # (collected, not written by a filesystem sink: that sink commits its files at checkpoints, and at a 1 s interval over
    # RocksDB state the checkpoints at 1M rows never finished)
    try:
        t = time.time()
        with open(spec["out"], "w", newline="") as f:
            writer = csv.writer(f)
            for row in env.execute_sql(q).collect():
                writer.writerow(list(row))
        out["collect_s"] = round(time.time() - t, 3)
    except Exception as e:
        out["failed"] = f"collect: {why(e)}"
print(json.dumps(out), flush=True)
