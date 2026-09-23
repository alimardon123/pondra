#!/usr/bin/env python3
"""Single-node TPC-H: Pondra against DuckDB, Polars, Daft and Bodo, on one machine and one copy
of the data.

  singlenode.py prepare --data ~/tpch/sf1                  # tpchgen output -> <data>-bench/ (below)
  singlenode.py run --data ~/tpch/sf1-bench --sf 1 [--engines pondra,duckdb,polars,daft,bodo]
                [--runs 3] [--out results.json] [--daft-python …] [--bodo-python …] [--repos DIR]

The data: tpchgen-cli's Parquet with money columns as DOUBLE (not every engine computes on
DECIMAL alike) and 122,880-row row groups. Two ways to run, compared like with like:

- from files, every query: `duckdb`, `polars`, `polars-streaming`, `daft` and `bodo` read the
  Parquet files; `pondra-cold` reads its lake's (loaded with one INSERT per table), with its
  in-memory columns off (PONDRA_HOT_GB=0);
- from memory: `duckdb-native` loads the tables into DuckDB first; `pondra` runs every query twice
  first, so the columns they read are in memory (hot.rs takes a file on its second read; they get
  `--hot-gb` GB, 3 by default). Neither load is in the times.

Each query runs `--runs` times: the best time is "hot", the first "first". Answers are checked
against DuckDB's (row count, and every value; numbers to a relative 1e-6).

The queries are each project's own: SQL (DataFusion's q1-q22, the same text) for Pondra and
DuckDB; pola-rs/tpch for Polars (its in-memory and streaming engines, whichever is faster); Daft's
benchmarking/tpch (SQL, q21 in DataFrames); Bodo's benchmarks/tpch (bodo.pandas DataFrames).
`--repos` holds those three repositories (git clone; see ensure_repos).
"""
import argparse, json, os, subprocess, sys, tempfile, threading, time

HERE = os.path.dirname(os.path.abspath(__file__))
TABLES = ["region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem"]
A = None


# ---------------------------------------------------------------- data

def prepare(data):
    """tpchgen Parquet -> money as DOUBLE, 122,880-row row groups, Snappy (as tpchgen writes).
    Also a copy with upper-case column names, which Bodo's TPC-H code expects (renaming on read
    breaks its filter pushdown)."""
    import pyarrow as pa, pyarrow.parquet as pq
    out = data.rstrip("/") + "-bench"
    for folder, name in [(out, str), (out + "-upper", str.upper)]:
        os.makedirs(folder, exist_ok=True)
        for t in TABLES:
            f = pq.ParquetFile(os.path.join(data, f"{t}.parquet"))
            schema = pa.schema([pa.field(name(x.name), pa.float64() if pa.types.is_decimal(x.type) else x.type) for x in f.schema_arrow])
            with pq.ParquetWriter(os.path.join(folder, f"{t}.parquet"), schema, compression="snappy") as w:
                for b in f.iter_batches(batch_size=122_880):
                    w.write_table(pa.Table.from_batches([b.rename_columns(schema.names)]).cast(schema), row_group_size=122_880)
            print("prepared", folder, t, flush=True)
    return out


def ensure_repos(repos):
    for name, url in [("pola-tpch", "https://github.com/pola-rs/tpch.git"), ("daft-repo", "https://github.com/Eventual-Inc/Daft.git"), ("bodo-repo", "https://github.com/bodo-ai/Bodo.git")]:
        path = os.path.join(repos, name)
        if os.path.exists(path):
            continue
        sub = {"daft-repo": "benchmarking/tpch", "bodo-repo": "benchmarks/tpch"}.get(name)
        subprocess.run(["git", "clone", "--depth", "1", *(["--filter=blob:none", "--sparse"] if sub else []), url, path], check=True)
        if sub:
            subprocess.run(["git", "-C", path, "sparse-checkout", "set", sub], check=True)


# ---------------------------------------------------------------- one engine, in its own process

RUNNER = r'''
import json, os, sys, time, datetime, decimal
engine, data, sf, runs, repos, queries = sys.argv[1], sys.argv[2], float(sys.argv[3]), int(sys.argv[4]), sys.argv[5], json.loads(sys.argv[6])
T = ["region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem"]
path = lambda t: os.path.join(data, t + ".parquet")

def cell(v):
    if isinstance(v, (datetime.date, datetime.datetime)): return str(v)[:10]
    if isinstance(v, decimal.Decimal): return float(v)
    if hasattr(v, "item"): return v.item()
    return v

def rows_of(table):  # a pyarrow Table -> rows of plain values
    return [[cell(v) for v in r.values()] for r in table.to_pylist()]

def report(q, f):
    times = []
    for _ in range(runs):
        t = time.time(); result = f(); times.append(time.time() - t)
    print(json.dumps({"q": q, "times": times, "rows": rows_of(result)}, default=str), flush=True)

if engine.startswith("duckdb"):
    import duckdb
    con = duckdb.connect()
    t0 = time.time()
    for t in T:
        kind = "TABLE" if engine == "duckdb-native" else "VIEW"
        con.execute(f"CREATE {kind} {t} AS SELECT * FROM read_parquet('{path(t)}')")
    print(json.dumps({"load": time.time() - t0}), flush=True)
    qs = json.load(open(os.path.join(repos, "sql.json")))
    for q in queries:
        report(q, lambda: con.execute(qs[str(q)]).fetch_arrow_table())

elif engine.startswith("pondra"):
    import http.client, pyarrow as pa
    port = int(os.environ["PONDRA_PORT"])
    qs = json.load(open(os.path.join(repos, "sql.json")))
    def run(sql):
        c = http.client.HTTPConnection("127.0.0.1", port, timeout=3600)
        c.request("POST", "/sql?format=arrow", sql.encode())
        r = c.getresponse(); body = r.read()
        assert r.status == 200, body[:300]
        return pa.ipc.open_stream(body).read_all()
    n = [0]
    def fresh(q):  # (a new comment each run: the result cache never answers)
        n[0] += 1
        return run(qs[str(q)] + f" -- run {n[0]}")
    for q in queries:
        report(q, lambda: fresh(q))

elif engine.startswith("polars"):
    import types, polars as pl
    sys.path.insert(0, os.path.join(repos, "pola-tpch"))
    utils = types.ModuleType("queries.polars.utils")
    names = {"line_item": "lineitem", "orders": "orders", "customer": "customer", "region": "region", "nation": "nation", "supplier": "supplier", "part": "part", "part_supp": "partsupp"}
    for k, t in names.items():
        setattr(utils, f"get_{k}_ds", (lambda t: lambda: pl.scan_parquet(path(t)))(t))
    import queries.polars as qp
    sys.modules["queries.polars.utils"] = utils; qp.utils = utils
    import importlib
    how = "streaming" if engine == "polars-streaming" else "in-memory"
    for q in queries:
        m = importlib.import_module(f"queries.polars.q{q}")
        report(q, lambda: m.q().collect(engine=how).to_arrow())

elif engine == "daft":
    import daft
    sys.path.insert(0, os.path.join(repos, "daft-repo", "benchmarking", "tpch"))
    import answers_sql
    from daft import col
    def get_df(t):
        df = daft.read_parquet(path(t))
        return df.select(*[col(c).alias(c.upper()) for c in df.column_names])  # (their data is upper case; their SQL lowercases it back)
    for q in queries:
        report(q, lambda: answers_sql.get_answer(q, get_df).to_arrow())

elif engine == "bodo":
    import warnings; warnings.filterwarnings("ignore")
    import bodo.pandas as bpd, pandas, pyarrow as pa
    sys.path.insert(0, os.path.join(repos, "bodo-repo", "benchmarks", "tpch", "bodo"))
    import dataframe_queries as dq
    dq.show_output = False
    load = lambda t: bpd.read_parquet(os.path.join(data + "-upper", t + ".parquet"), dtype_backend="pyarrow")
    for q in queries:
        f = getattr(dq, f"tpch_q{q:02}")
        args = [a for a in __import__("inspect").signature(f).parameters if a not in ("pd", "scale_factor")]
        def go():
            kw = {a: load(a) for a in args}
            if q == 11: kw["scale_factor"] = sf
            res = f(**kw, pd=bpd)
            if hasattr(res, "to_pandas"): res = res.to_pandas()
            if isinstance(res, pandas.Series): res = res.to_frame()
            if not isinstance(res, pandas.DataFrame): res = pandas.DataFrame({"v": [res]})
            return pa.Table.from_pandas(res.reset_index(drop=True), preserve_index=False)
        report(q, go)
'''


def same(a, b):
    """Rows equal as multisets: numbers to a relative 1e-6, the rest exactly."""
    if len(a) != len(b):
        return False
    def key(r):
        return [round(v, 2) if isinstance(v, float) else v for v in r]
    for x, y in zip(sorted(a, key=lambda r: json.dumps(key(r), default=str)), sorted(b, key=lambda r: json.dumps(key(r), default=str))):
        if len(x) != len(y):
            return False
        for u, v in zip(x, y):
            if isinstance(u, (int, float)) and isinstance(v, (int, float)):
                if abs(u - v) > 1e-6 * max(1, abs(u), abs(v)) + 0.01:
                    return False
            elif str(u).strip() != str(v).strip():
                return False
    return True


def run_engine(engine, python, qs_left, env):
    """Run the queries in one process; a query that kills it (out of memory, a crash) is recorded
    and the rest go on in a new process."""
    out, left = {}, list(qs_left)
    while left:
        cmd = [python, "-c", RUNNER, engine, A.data, str(A.sf), str(A.runs), A.repos, json.dumps(left)]
        if engine == "bodo":  # (Bodo spawns its MPI workers; here that needs a process manager to start from)
            cmd = [os.path.join(os.path.dirname(python), "mpiexec"), "-n", "1"] + cmd
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env)
        last = [time.time()]  # a query that prints nothing for --timeout is killed (it hung)
        stop = threading.Event()
        def watchdog():
            while not stop.wait(5):
                if time.time() - last[0] > A.timeout:
                    p.kill()
                    return
        threading.Thread(target=watchdog, daemon=True).start()
        for line in p.stdout:
            last[0] = time.time()
            if line.startswith('{"load"'):
                out["load_s"] = round(json.loads(line)["load"], 1)
            elif line.startswith("{"):
                r = json.loads(line)
                out[r["q"]] = r
                left.remove(r["q"])
                print(f"  {engine:17} q{r['q']:<2} {min(r['times']):7.3f} s", flush=True)
        stop.set()
        p.wait()
        if left:  # the next query failed or took too long
            err = p.stderr.read()[-400:]
            out[left[0]] = {"q": left[0], "error": err.strip().splitlines()[-1] if err.strip() else f"exit {p.returncode}"}
            print(f"  {engine:17} q{left[0]:<2} FAILED: {out[left[0]]['error'][:120]}", flush=True)
            left = left[1:]
    return out


def pondra_lake():
    """Load the data into a new Pondra lake: one INSERT per table, as a user would."""
    sys.path.insert(0, os.path.join(HERE, ".."))
    import harness
    lake = tempfile.mkdtemp(prefix="pondra-tpch-")
    t0 = time.time()
    for t in TABLES:
        subprocess.run([harness.BIN, "sql", "--dir", lake, f"INSERT INTO {t} SELECT * FROM '{os.path.join(A.data, t)}.parquet'"], check=True, capture_output=True)
    return lake, time.time() - t0


def pondra_up(lake, hot, qs):
    """Start a node on the lake. With `hot`, run every query once and wait for the columns they
    read to be in memory; returns how long that took."""
    import harness, re
    harness.A = argparse.Namespace(s3=False, keep=False)
    env = {"PONDRA_HOT_GB": str(A.hot_gb) if hot else "0"}
    node = harness.Node(lake, A.port, env=env, **dict(f.split("=", 1) for f in A.flag)).start()
    harness.call(A.port, "POST", "/tier", timeout=3600)  # (merges and sealing done before timing)
    t0 = time.time()
    if hot:
        for run in range(2):  # (the cache takes a file the second time a scan wants it)
            for q, sql in qs.items():
                harness.call(A.port, "POST", "/sql", (sql + f" -- warm {run}").encode(), timeout=3600)
        held = lambda: float(re.search(rb"\npondra_hot_bytes (\S+)", harness.call(A.port, "GET", "/metrics")).group(1))
        last = -1
        while held() != last:  # (loading runs in the background, a file at a time)
            last = held()
            time.sleep(2)
    return node, time.time() - t0


def run():
    import re
    sys.path.insert(0, HERE)
    from tpch import queries as sql_queries
    ensure_repos(A.repos)
    json.dump({str(k): v for k, v in sql_queries(A.queries).items()}, open(os.path.join(A.repos, "sql.json"), "w"))
    qs = list(range(1, 23))
    results, info = {}, {}
    engines = A.engines.split(",")
    lake = None
    if any(e.startswith("pondra") for e in engines):
        lake, load = pondra_lake()
        info["pondra_load_s"] = round(load, 1)
    for engine in engines:
        env = dict(os.environ)
        python = {"daft": A.daft_python, "bodo": A.bodo_python}.get(engine, sys.executable)
        if engine.startswith("pondra"):
            node, warm = pondra_up(lake, engine == "pondra", sql_queries(A.queries))
            if engine == "pondra":
                info["pondra_warm_s"] = round(warm, 1)
            env["PONDRA_PORT"] = str(A.port)
        print(f"== {engine}", flush=True)
        t = time.time()
        results[engine] = run_engine(engine, python, qs, env)
        if "load_s" in results[engine]:
            info[f"{engine}_load_s"] = results[engine].pop("load_s")
        info[f"{engine}_wall_s"] = round(time.time() - t, 1)
        if engine.startswith("pondra"):
            node.kill()
    if lake:
        subprocess.run(["rm", "-rf", lake])
    ref = results.get("duckdb", {})
    table = {}
    for engine, rs in results.items():
        row = {}
        for q in qs:
            r = rs.get(q, {})
            if "times" not in r:
                row[q] = {"error": r.get("error", "not run")}
                continue
            ok = q not in ref or "rows" not in ref[q] or same(r["rows"], ref[q]["rows"])
            row[q] = {"hot": round(min(r["times"]), 3), "first": round(r["times"][0], 3), "same_as_duckdb": ok}
        good = [v for v in row.values() if "hot" in v]
        row["total_hot"] = round(sum(v["hot"] for v in good), 2) if len(good) == 22 else None
        row["answers_match"] = sum(1 for v in good if v["same_as_duckdb"])
        table[engine] = row
    out = {"data": A.data, "sf": A.sf, "cores": os.cpu_count(), "runs": A.runs, "info": info, "results": table}
    json.dump(out, open(A.out, "w"), indent=1)
    print(f"\n{'':6}" + "".join(f"{e:>12}" for e in table))
    for q in qs:
        cells = []
        for e in table:
            v = table[e][q]
            cells.append(f"{v['hot']:>11.3f}{'' if v['same_as_duckdb'] else '!'}" if "hot" in v else f"{'FAIL':>12}")
        print(f"q{q:<5}" + "".join(cells))
    print(f"{'total':6}" + "".join(f"{(table[e]['total_hot'] or float('nan')):>12.2f}" for e in table))
    print(json.dumps(info))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("what", choices=["prepare", "run"])
    ap.add_argument("--data", required=True)
    ap.add_argument("--sf", type=float, default=1)
    ap.add_argument("--engines", default="duckdb,duckdb-native,pondra,pondra-cold,polars,polars-streaming,daft,bodo")
    ap.add_argument("--runs", type=int, default=3)
    ap.add_argument("--timeout", type=int, default=600, help="seconds per query at most")
    ap.add_argument("--queries", default=os.path.expanduser("~/tpch/queries"))
    ap.add_argument("--repos", default=os.path.expanduser("~/bench-repos"))
    ap.add_argument("--daft-python", default=os.path.expanduser("~/venv-daft/bin/python"))
    ap.add_argument("--bodo-python", default=os.path.expanduser("~/venv-bodo/bin/python"))
    ap.add_argument("--port", type=int, default=8150)
    ap.add_argument("--flag", action="append", default=[], help="a pondra serve flag for the node, e.g. memory-gb=4")
    ap.add_argument("--hot-gb", type=float, default=3, help="memory for Pondra's hot columns (PONDRA_HOT_GB)")
    ap.add_argument("--out", default="singlenode.json")
    A = ap.parse_args()
    prepare(A.data) if A.what == "prepare" else run()
