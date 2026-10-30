"""Flink's side of `cep_bench.py` (PyFlink, a local MiniCluster, streaming mode): the ticks from their CSV files (the
filesystem connector), the match query into a blackhole sink, each run timed from submit to the job's end (one run, or
three when one takes under a minute), then the same query into a CSV folder, whose rows are the matches. Prints one JSON line.
  flink_cep.py SPEC_JSON"""
import json, os, sys, time
t0 = time.time()
from pyflink.table import EnvironmentSettings, TableEnvironment
spec = json.load(open(sys.argv[1]))
env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
conf = env.get_config()
conf.set("parallelism.default", str(spec["parallelism"]))
# (State as nexmark.py sets it: RocksDB, with its managed memory a third of the machine's. The JVM's heap is JVM_ARGS's
# -Xmx, which cep_bench.py sets the same way.)
conf.set("state.backend.type", "rocksdb")
conf.set("taskmanager.memory.managed.size", spec["managed"])
conf.set("table.local-time-zone", "UTC")
env.execute_sql("SELECT 1").wait()  # (the JVM and the MiniCluster start outside the timings)
env.execute_sql(f"CREATE TABLE ticks (symbol STRING, ts TIMESTAMP(3), price INT, WATERMARK FOR ts AS ts) "
                f"WITH ('connector'='filesystem', 'path'='{spec['dir']}', 'format'='csv')")


def sink(name, sql, with_clause):
    """A table with the query's columns (its time attributes as plain timestamps), written by `with_clause`."""
    schema = env.sql_query(sql).get_schema()
    cols = ", ".join(f"`{c}` {str(t).replace(' *ROWTIME*', '').replace(' *PROCTIME*', '').replace('LOCALZONEDTIMESTAMP', 'TIMESTAMP_LTZ')}"
                     for c, t in zip(schema.get_field_names(), schema.get_field_data_types()))
    env.execute_sql(f"CREATE TABLE {name} ({cols}) WITH ({with_clause})")


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
out = {"engine": "flink", "parallelism": spec["parallelism"], "state": "rocksdb", "jvm_args": os.environ.get("JVM_ARGS", ""),
       "startup_s": round(time.time() - t0, 2), "blackhole_s": [], "failed": None}
try:
    sink("match_blackhole", q, "'connector'='blackhole'")
    first = timed(f"INSERT INTO match_blackhole {q}")
    out["blackhole_s"] = [first] + ([timed(f"INSERT INTO match_blackhole {q}") for _ in range(2)] if first < 60 else [])
except Exception as e:
    out["failed"] = f"blackhole: {why(e)}"
if out["failed"] is None:
    # (the filesystem sink commits its files at a checkpoint: the bounded job's last one comes at its end)
    conf.set("execution.checkpointing.interval", "1 s")
    try:
        sink("matches", q, f"'connector'='filesystem', 'path'='{spec['out']}', 'format'='csv'")
        out["matches_csv_s"] = timed(f"INSERT INTO matches {q}")
    except Exception as e:
        out["failed"] = f"matches: {why(e)}"
print(json.dumps(out), flush=True)
