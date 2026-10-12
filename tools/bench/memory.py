"""A lake in memory against DuckDB's `:memory:`, each in a fresh Python process (ADR-061).

    python3 tools/bench/memory.py [--rows 10000000] [--tpch ~/tpch/sf1] [--runs 3] [--only pondra,folder,duckdb]

`pondra` is `pondra.open()` (a lake held in memory), `folder` is `pondra.open(<a new folder>)`, `duckdb` is
`duckdb.connect()`. Each engine runs in a process of its own, `--runs` times; a time is the median. What is
measured, in that process: importing and opening up to the first answer of `SELECT 1`; a table of `--rows` rows
made from a series; a grouped aggregate and a key's row over it (warm: the median of several, each a new text so
no answer is remembered); 500 one-row INSERTs; the process's memory after the load; and, with `--tpch`, its 8
tables loaded from Parquet and the 22 queries run once cold and once warm (the warm total is reported). Pondra
finds its library through PONDRA_LIBRARY (target/release/libpondra_lib.so here by default).
"""
import argparse, json, os, statistics, subprocess, sys, tempfile, time

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))

ap = argparse.ArgumentParser()
ap.add_argument("--rows", type=int, default=10_000_000)
ap.add_argument("--tpch", default=None, help="a folder of TPC-H's 8 tables as Parquet (tpchgen-cli's)")
ap.add_argument("--runs", type=int, default=3)
ap.add_argument("--only", default="pondra,folder,duckdb")
ap.add_argument("--child", default=None)
A = ap.parse_args()

TABLES = ["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"]


def child(engine):
    t0 = time.perf_counter()
    out = {}
    if engine == "duckdb":
        import duckdb
        out["import"] = time.perf_counter() - t0
        con = duckdb.connect()
        q = lambda s: con.execute(s).fetch_arrow_table()
        do = lambda s: con.execute(s)
        series = f"SELECT x AS id, x % 1000 AS k, (x * 7919 % 100003) / 100.0 AS v FROM range({A.rows}) AS g(x)"
    else:
        import pondra
        out["import"] = time.perf_counter() - t0
        folder = tempfile.mkdtemp(prefix="pondra-bench-") if engine == "folder" else None
        con = pondra.open(folder)
        q = lambda s: con.sql(s).to_arrow()
        do = lambda s: con.sql(s)
        series = f"SELECT x AS id, x % 1000 AS k, (x * 7919 % 100003) / 100.0 AS v FROM range({A.rows}) AS g(x)"
    out["open"] = time.perf_counter() - t0 - out["import"]
    q("SELECT 1")
    out["first answer"] = time.perf_counter() - t0
    t = time.perf_counter()
    do(f"CREATE TABLE t AS {series}")
    out["load series"] = time.perf_counter() - t
    assert q("SELECT count(*) AS n FROM t").column(0)[0].as_py() == A.rows
    timed = lambda make, n: statistics.median(_timed(q, make, n))
    out["aggregate (warm)"] = timed(lambda i: f"SELECT k, count(*) AS n, sum(v) AS s FROM t GROUP BY k -- {i}", 5)
    out["a key's row (warm)"] = timed(lambda i: f"SELECT * FROM t WHERE id = {i * 7919 % A.rows}", 50)
    do("CREATE TABLE s (a INT, b VARCHAR)")
    t = time.perf_counter()
    for i in range(500):
        do(f"INSERT INTO s VALUES ({i}, 'x')")
    out["one-row INSERT"] = (time.perf_counter() - t) / 500
    assert q("SELECT count(*) AS n FROM s").column(0)[0].as_py() == 500
    out["memory after load (MB)"] = rss_mb()
    if A.tpch:
        t = time.perf_counter()
        for name in TABLES:
            do(f"CREATE TABLE {name} AS SELECT * FROM '{os.path.join(A.tpch, name)}.parquet'")
        out["TPC-H load"] = time.perf_counter() - t
        qs = os.path.join(HERE, "tpch-queries")
        cold = warm = 0.0
        rows = {}
        for n in range(1, 23):
            text = open(os.path.join(qs, f"q{n}.sql")).read().strip().rstrip(";")
            t = time.perf_counter()
            rows[n] = q(f"{text}\n-- cold").num_rows
            cold += time.perf_counter() - t
            t = time.perf_counter()
            q(f"{text}\n-- warm")
            warm += time.perf_counter() - t
        out["TPC-H 22 (cold)"], out["TPC-H 22 (warm)"] = cold, warm
        out["TPC-H rows"] = rows
        out["memory after TPC-H (MB)"] = rss_mb()
    t = time.perf_counter()
    con.close()
    out["close"] = time.perf_counter() - t
    if engine == "folder":
        import shutil
        shutil.rmtree(folder, ignore_errors=True)
    print(json.dumps(out))


def _timed(q, make, n):
    each = []
    for i in range(n):
        t = time.perf_counter()
        q(make(i))
        each.append(time.perf_counter() - t)
    return each


def rss_mb():
    return next(int(l.split()[1]) for l in open("/proc/self/status") if l.startswith("VmRSS")) / 1024


def main():
    env = dict(os.environ)
    env.setdefault("PONDRA_LIBRARY", os.path.join(ROOT, "target", "release", "libpondra_lib.so"))
    env["PYTHONPATH"] = os.path.join(ROOT, "python") + os.pathsep + env.get("PYTHONPATH", "")
    results = {}
    for engine in A.only.split(","):
        runs = []
        for _ in range(A.runs):
            args = [sys.executable, __file__, "--child", engine, "--rows", str(A.rows)] + (["--tpch", A.tpch] if A.tpch else [])
            p = subprocess.run(args, env=env, capture_output=True, text=True)
            if p.returncode:
                sys.exit(f"{engine} failed:\n{p.stderr[-3000:]}")
            runs.append(json.loads(p.stdout.strip().splitlines()[-1]))
        results[engine] = {k: (statistics.median(r[k] for r in runs) if isinstance(runs[0][k], float) else runs[0][k]) for k in runs[0]}
    if A.tpch and "duckdb" in results:
        for e, r in results.items():
            if r.get("TPC-H rows") != results["duckdb"]["TPC-H rows"]:
                print(f"{e}: TPC-H row counts differ from DuckDB's: {r.get('TPC-H rows')}")
    keys = [k for k in next(iter(results.values())) if k != "TPC-H rows"]
    print(f"{'':28}" + "".join(f"{e:>14}" for e in results))
    for k in keys:
        unit = "" if "MB" in k else " s"
        print(f"{k:28}" + "".join(f"{results[e][k]:>12.4f}{unit}" if "MB" not in k else f"{results[e][k]:>14.0f}" for e in results))
    print(json.dumps(results))


if __name__ == "__main__":
    child(A.child) if A.child else main()
