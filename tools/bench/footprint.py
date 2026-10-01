#!/usr/bin/env python3
"""What each engine costs to have and to start (the "lightweight form factor" comparison): its
install's size on disk, the time from starting it to its first answer, and its memory (resident
set) idle after that answer and after one small query over 1M rows. Pondra as a server (one
binary), DuckDB and Polars in a Python process, Spark 4 (`local[2]`) and Flink 2 (a MiniCluster)
through their Python packages, each JVM counted.

  footprint.py [--bin target/release/pondra] [--python /tmp/engines/bin/python]
"""
import argparse, json, os, shutil, subprocess, sys, tempfile, textwrap, time, urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
ap = argparse.ArgumentParser()
ap.add_argument("--bin", default=os.path.join(HERE, "..", "..", "target", "release", "pondra"))
ap.add_argument("--python", default="/tmp/engines/bin/python", help="a Python with duckdb, polars, pyspark and apache-flink")
ap.add_argument("--port", type=int, default=9660)
ap.add_argument("--only", default="pondra,duckdb,polars,spark,flink")
A = ap.parse_args()


def rss_mb(pid):
    """A process's resident memory and its children's (a JVM a Python started), in MB."""
    def one(p):
        try:
            return next(int(l.split()[1]) for l in open(f"/proc/{p}/status") if l.startswith("VmRSS")) / 1024
        except Exception:
            return 0
    kids = subprocess.run(["pgrep", "-P", str(pid)], capture_output=True, text=True).stdout.split()
    return round(one(pid) + sum(rss_mb(int(k)) for k in kids), 1)


def size_mb(path):
    return round(sum(os.path.getsize(os.path.join(d, f)) for d, _, fs in os.walk(path) for f in fs) / 2**20, 1) if os.path.isdir(path) else round(os.path.getsize(path) / 2**20, 1)


def pondra():
    lake = tempfile.mkdtemp(prefix="pondra-fp-")
    t0 = time.time()
    p = subprocess.Popen([A.bin, "serve", "--dir", lake, "--addr", f"127.0.0.1:{A.port}"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    q = lambda s: urllib.request.urlopen(urllib.request.Request(f"http://127.0.0.1:{A.port}/sql", data=s.encode()), timeout=120).read()
    while True:
        try:
            q("SELECT 1")
            break
        except Exception:
            time.sleep(0.02)
    ready = time.time() - t0
    time.sleep(2)
    idle = rss_mb(p.pid)
    q("SELECT count(*), sum(value % 7) FROM range(1000000)")
    after = rss_mb(p.pid)
    p.terminate()
    p.wait(30)
    shutil.rmtree(lake, ignore_errors=True)
    return {"engine": "pondra (server)", "install_mb": size_mb(A.bin), "first_answer_s": round(ready, 2), "idle_mb": idle, "after_query_mb": after}


def installed_mb(dists):
    """The files of these Python distributions, in MB (a JVM's own install not counted)."""
    code = f"import importlib.metadata as m; print(sum(m.distribution(d).locate_file(f).stat().st_size for d in {dists!r} for f in (m.distribution(d).files or []) if m.distribution(d).locate_file(f).exists()))"
    out = subprocess.run([A.python, "-c", code], capture_output=True, text=True).stdout.strip()
    return round(int(out) / 2**20, 1) if out.isdigit() else None


def in_python(name, setup, query, package):
    """An engine in a Python process: its own clock, and its memory read from outside."""
    code = textwrap.dedent(f"""
        import time, sys
        t0 = time.time()
        {setup}
        print("READY", round(time.time() - t0, 2), flush=True)
        sys.stdin.readline()
        {query}
        print("DONE", flush=True)
        sys.stdin.readline()
    """)
    p = subprocess.Popen([A.python, "-c", code], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
    line = p.stdout.readline()
    while line and not line.startswith("READY"):
        line = p.stdout.readline()
    if not line:
        return {"engine": name, "error": "didn't start"}
    ready = float(line.split()[1])
    time.sleep(2)
    idle = rss_mb(p.pid)
    p.stdin.write("\n")
    p.stdin.flush()
    while not p.stdout.readline().startswith("DONE"):
        pass
    after = rss_mb(p.pid)
    p.stdin.write("\n")
    p.stdin.flush()
    p.wait(60)
    return {"engine": name, "install_mb": installed_mb(package), "first_answer_s": ready, "idle_mb": idle, "after_query_mb": after}


ENGINES = {
    "pondra": pondra,
    "duckdb": lambda: in_python("duckdb (in process)", "import duckdb; c = duckdb.connect(); c.execute('SELECT 1').fetchall()",
                                "c.execute('SELECT count(*), sum(range % 7) FROM range(1000000)').fetchall()", ["duckdb"]),
    "polars": lambda: in_python("polars (in process)", "import polars as pl; pl.select(pl.lit(1)).to_dicts()",
                                "pl.select((pl.int_range(0, 1000000) % 7).sum()).to_dicts()", ["polars", "polars-runtime-32"]),
    "spark": lambda: in_python("spark 4 (local[2], JVM)", "from pyspark.sql import SparkSession; s = SparkSession.builder.master('local[2]').getOrCreate(); s.sql('SELECT 1').collect()",
                               "s.sql('SELECT count(*), sum(id % 7) FROM range(1000000)').collect()", ["pyspark"]),
    "flink": lambda: in_python("flink 2 (MiniCluster, JVM)",
                               "from pyflink.table import EnvironmentSettings, TableEnvironment; e = TableEnvironment.create(EnvironmentSettings.in_batch_mode()); e.get_config().set('parallelism.default', '2'); list(e.execute_sql('SELECT 1').collect()); "
                               "e.execute_sql(\"CREATE TABLE r (id BIGINT) WITH ('connector' = 'datagen', 'fields.id.kind' = 'sequence', 'fields.id.start' = '0', 'fields.id.end' = '999999', 'number-of-rows' = '1000000')\")",
                               "list(e.execute_sql('SELECT count(*), sum(MOD(id, 7)) FROM r').collect())", ["apache-flink", "apache-flink-libraries"]),
}

out = []
for name, f in [(n, ENGINES[n]) for n in A.only.split(",")]:
    try:
        r = f()
    except Exception as e:
        r = {"engine": name, "error": str(e)[:300]}
    print(json.dumps(r), file=sys.stderr, flush=True)
    out.append(r)
print(json.dumps({"footprint": out}))
