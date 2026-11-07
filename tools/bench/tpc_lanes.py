#!/usr/bin/env python3
"""Pondra beside DuckDB, Polars, DataFusion and Pivot on TPC-H and TPC-DS from Parquet, under the rules of Polars' 2.0
benchmark (github.com/pola-rs/polars-2.0-benchmark): each query in a fresh process, its tables registered and one
untimed warm-up run first, then the best of 5 timed runs, each with a 60 s limit. Every engine's answer is checked
against DuckDB's (that benchmark keeps only hashes of them).

  tpc_lanes.py run tpch 10 --engine pondra --queries CLONE/queries/sql --data DATA [--iters 5] [--timeout 60] [--out DIR]
  tpc_lanes.py report --out DIR [--md FILE]

The queries and the data are that benchmark's own, read from a clone at a pinned commit (its queries/sql, and the
Parquet its tpcgen-cli writes to DATA/tpch-10 and DATA/tpcds-10): nothing of it is copied here, since its repository
has no licence, and none of its code runs. Only the engines' own Python packages do (duckdb, polars, datafusion),
Pondra's in-process node (`pondra.connect()`, its library from PONDRA_LIBRARY, its package from --pondra-python), and
Pivot's release binary (--pivot, `pivot server`: a server started fresh for each query, the tables adopted in place
from the same Parquet once per suite, the answers read over its Postgres port with psycopg).

`run` writes one JSON line per query to OUT/<benchmark>-<sf>-<engine>.jsonl and each answer to OUT/answers/; `report`
prints, for each suite, every engine's total of best times, its per-query geometric mean against DuckDB's, and on how
many queries it is faster than each other engine, counting only the queries both answered (a timeout or an error is
listed, never counted as a time).
"""
import argparse
import json
import math
import multiprocessing
import os
import queue
import sys
import time
from pathlib import Path

ENGINES = ("pondra", "duckdb", "polars", "datafusion", "pivot")
PRIME = "SELECT a, count(*) AS n, sum(b) AS s FROM (VALUES (1, 2.0), (2, 3.0), (1, 4.0)) AS t(a, b) GROUP BY a"


def tables_in(data):
    """Each table's Parquet: a file `name.parquet`, or a folder `name/` of them."""
    found = {}
    for p in sorted(Path(data).iterdir()):
        if p.is_file() and p.suffix == ".parquet":
            found[p.stem] = str(p.resolve())
        elif p.is_dir() and any(p.glob("*.parquet")):
            found[p.name] = str(p.resolve())
    return found


# Each engine: open(tables, threads) -> a handle with its tables registered; answer(handle, sql) -> a pyarrow.Table.

def open_pondra(tables, threads):
    import pondra
    db = pondra.connect(python=False, **({} if threads is None else {"cores": threads}))
    for name, path in tables.items():
        db.sql(f"CREATE EXTERNAL TABLE {name} STORED AS PARQUET LOCATION '{path}{'/' if os.path.isdir(path) else ''}'")
    return db


def open_duckdb(tables, threads):
    import duckdb
    con = duckdb.connect()
    if threads is not None:
        con.execute(f"SET threads = {threads}")
    for name, path in tables.items():
        files = f"{path}/*.parquet" if os.path.isdir(path) else path
        con.execute(f"CREATE VIEW {name} AS SELECT * FROM read_parquet('{files}')")
    return con


def open_polars(tables, threads):
    import polars as pl
    return pl.SQLContext({name: pl.scan_parquet(path) for name, path in tables.items()})


def open_datafusion(tables, threads):
    from datafusion import SessionConfig, SessionContext
    config = SessionConfig() if threads is None else SessionConfig().with_target_partitions(threads)
    ctx = SessionContext(config)
    for name, path in tables.items():
        ctx.register_parquet(name, path)
    return ctx


def open_pivot(tables, threads):
    import psycopg
    return psycopg.connect(f"host=127.0.0.1 port={os.environ['PIVOT_PORT']} user=pivot dbname=default", autocommit=True, prepare_threshold=None)


SQL_TYPES = {"int8": "TINYINT", "int16": "SMALLINT", "int32": "INTEGER", "int64": "BIGINT", "float": "REAL", "double": "DOUBLE",
             "bool": "BOOLEAN", "date32[day]": "DATE", "string": "VARCHAR", "large_string": "VARCHAR", "string_view": "VARCHAR"}


def sql_type(t):
    import pyarrow as pa
    if pa.types.is_decimal(t):
        return f"DECIMAL({t.precision}, {t.scale})"
    if pa.types.is_timestamp(t):
        return "TIMESTAMP"
    return SQL_TYPES[str(t)]


def pivot_store(args, tables):
    """The suite's Pivot datastore, made once: each table adopts its Parquet where it is (a folder of hard links to our
    files, since Pivot adopts a folder's files), with the columns the files have."""
    import pyarrow.parquet as pq
    root = Path(args.out) / f"pivot-{args.benchmark}-{args.sf}"
    files = root / "files"
    if (root / "made").exists():
        return root
    import shutil
    shutil.rmtree(root / "store", ignore_errors=True)  # (what a try that stopped part way made)
    (root / "store").mkdir(parents=True)
    for name, path in tables.items():
        (files / name).mkdir(parents=True, exist_ok=True)
        for f in ([Path(path)] if os.path.isfile(path) else sorted(Path(path).glob("*.parquet"))):
            if not (files / name / f.name).exists():
                os.link(f, files / name / f.name)
    statements = [f"CREATE TABLE {name} ({', '.join(f'{c.name} {sql_type(c.type)}' for c in pq.read_schema(path if os.path.isfile(path) else next(Path(path).glob('*.parquet'))))}) "
                  f"WITH (with_pre_existing_parquets = '{(files / name).resolve()}')" for name, path in tables.items()]
    server = pivot_server(args, root, args.pivot_port)  # (`pivot open` wants a terminal: the tables are made through a server)
    try:
        import psycopg
        with psycopg.connect(f"host=127.0.0.1 port={args.pivot_port} user=pivot dbname=default", autocommit=True, prepare_threshold=None) as c:
            for st in statements:
                c.execute(st)
    finally:
        server.kill()
        server.wait()
    script = ";\n".join(statements) + ";\n"
    (root / "made").write_text(script)
    return root


def pivot_server(args, root, port):
    """A Pivot server on the suite's datastore, started fresh: its config, then the process, once it answers."""
    import subprocess, psycopg
    config = root / f"server-{port}.yaml"
    config.write_text(f"workers: {args.threads or os.cpu_count()}\nmemory: {args.pivot_memory}\nserver:\n  bind: 127.0.0.1:{port}\n"
                      f"datastores:\n  default:\n    kind: pivotlake\n    location: {(root / 'store').resolve()}\n    default: true\n    compact: false\n"
                      f"users:\n  pivot:\n    auth:\n      method: trust\n")
    proc = subprocess.Popen([args.pivot, "server", "--config", str(config)], stdout=subprocess.DEVNULL, stderr=open(root / f"server-{port}.log", "a"))
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"pivot server exited: {(root / f'server-{port}.log').read_text()[-1000:]}")
        try:
            psycopg.connect(f"host=127.0.0.1 port={port} user=pivot dbname=default", connect_timeout=1).close()
            return proc
        except psycopg.OperationalError:
            time.sleep(0.1)
    proc.kill()
    raise RuntimeError("pivot server didn't answer in 120 s")


def from_rows(cur):
    """A Postgres answer as an Arrow table, its columns by position (`same` reads them so)."""
    import pyarrow as pa
    rows = cur.fetchall()
    names = [f"c{i}" for i in range(len(cur.description or []))]
    return pa.table({n: pa.array([r[i] for r in rows]) for i, n in enumerate(names)}) if names else pa.table({})


def answer(engine, handle, sql):
    if engine == "pivot":
        cur = handle.execute(sql)
        return from_rows(cur)
    if engine == "pondra":
        return handle.sql(sql).to_arrow()
    if engine == "polars":
        return handle.execute(sql).collect(engine="streaming").to_arrow()
    return handle.sql(sql).to_arrow_table()


def version(engine):
    if engine == "pondra":
        import pondra
        return f"{pondra.__version__} ({os.environ.get('PONDRA_COMMIT', 'unknown commit')})"
    if engine == "pivot":
        import subprocess
        return subprocess.run([os.environ["PIVOT_BIN"], "--version"], capture_output=True, text=True).stdout.strip().removeprefix("pivot ")
    return __import__(engine).__version__


def measure(engine, sql, tables, threads, runs, warm, results, pondra_python):
    """In a fresh process: register the tables, run the prime query and the warm-up untimed, then each timed run."""
    if pondra_python:
        sys.path.insert(0, pondra_python)
    if engine == "polars" and threads is not None:
        os.environ["POLARS_MAX_THREADS"] = str(threads)
    try:
        handle = globals()[f"open_{engine}"](tables, threads)
        answer(engine, handle, PRIME)
    except BaseException as e:  # (a node's panic too)
        results.put({"error": f"setup: {type(e).__name__}: {e}"})
        return
    for i in range(warm + runs):
        results.put({"started": i})
        try:
            t0 = time.perf_counter()
            table = answer(engine, handle, sql)
            took = time.perf_counter() - t0
        except BaseException as e:
            results.put({"error": f"{type(e).__name__}: {str(e)[:2000]}"})
            return
        results.put({"run": i, "warm": i < warm, "s": took, "rows": table.num_rows, "answer": serialized(table) if i == warm + runs - 1 else None})


def serialized(table):
    import pyarrow as pa
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as w:
        w.write_table(table.replace_schema_metadata(None))
    return sink.getvalue().to_pybytes()


def one_query(engine, sql, tables, args):
    """Best of `iters` timed runs in one fresh process (each run limited to `timeout` s), with what failed if any."""
    ctx = multiprocessing.get_context("spawn")
    results = ctx.Queue()
    server = None
    if engine == "pivot":  # (the engine is the server: a fresh one per query, its client in the child as the others')
        os.environ["PIVOT_PORT"] = str(args.pivot_port)
        try:
            server = pivot_server(args, pivot_store(args, tables), args.pivot_port)
        except RuntimeError as e:
            return {"status": f"setup: {e}", "best_s": None, "times_s": [], "rows": None}, None
    proc = ctx.Process(target=measure, args=(engine, sql, tables, args.threads, args.iters, 1, results, args.pondra_python))
    proc.start()
    times, rows, kept, deadline, status = [], None, None, None, "ok"
    try:
        while True:
            try:
                msg = results.get(timeout=0.1)
            except queue.Empty:
                if deadline is not None and time.monotonic() > deadline:
                    status = f"timeout ({args.timeout:g} s)"
                    break
                if not proc.is_alive():
                    try:
                        msg = results.get(timeout=1)
                    except queue.Empty:
                        status = f"process exited with code {proc.exitcode}"
                        break
                else:
                    continue
            if "started" in msg:
                deadline = time.monotonic() + args.timeout
            elif "error" in msg:
                status = msg["error"]
                break
            else:
                deadline = None
                if not msg["warm"]:
                    times.append(msg["s"])
                rows = msg["rows"]
                if msg["answer"] is not None:
                    kept = msg["answer"]
                    break
    finally:
        proc.kill()
        proc.join()
        if server is not None:
            server.kill()
            server.wait()
    return {"status": status, "best_s": min(times) if times else None, "times_s": times, "rows": rows}, kept


def run(args):
    if args.engine == "pondra":
        assert os.environ.get("PONDRA_LIBRARY"), "set PONDRA_LIBRARY to libpondra_lib.so"
    if args.engine == "pivot":
        assert args.pivot, "give --pivot, the path of Pivot's release binary"
        os.environ["PIVOT_BIN"] = args.pivot
    tables = tables_in(Path(args.data) / f"{args.benchmark}-{args.sf}")
    qdir = Path(args.queries) / args.benchmark
    queries = sorted(p.stem for p in qdir.glob("q*.sql"))
    if args.only:
        queries = [q for q in queries if q in {f"q{int(x):02d}" for x in args.only.split(",")}]
    out = Path(args.out)
    (out / "answers").mkdir(parents=True, exist_ok=True)
    lines = out / f"{args.benchmark}-{args.sf}-{args.engine}.jsonl"
    done = {json.loads(l)["query"] for l in lines.read_text().splitlines()} if lines.exists() and not args.again else set()
    with open(lines, "a" if done else "w") as f:
        for q in queries:
            if q in done:
                continue
            sql = (qdir / f"{q}.sql").read_text().strip().rstrip(";")
            r, kept = one_query(args.engine, sql, tables, args)
            r = {"benchmark": args.benchmark, "sf": args.sf, "engine": args.engine, "version": None, "query": q, **r}
            if kept is not None:
                (out / "answers" / f"{args.benchmark}-{args.sf}-{args.engine}-{q}.arrow").write_bytes(kept)
            f.write(json.dumps(r) + "\n")
            f.flush()
            best = f"{r['best_s']:.3f} s" if r["best_s"] is not None else r["status"][:120]
            print(f"{args.engine} {args.benchmark} {q}: {best}", file=sys.stderr, flush=True)
    meta = out / "versions.json"
    known = json.loads(meta.read_text()) if meta.exists() else {}
    known[args.engine] = version_in_child(args.engine, args.pondra_python)
    meta.write_text(json.dumps(known, indent=1))


def version_in_child(engine, pondra_python):
    import subprocess
    code = f"import sys; sys.path.insert(0, {pondra_python!r}) if {bool(pondra_python)} else None; import tpc_lanes; print(tpc_lanes.version({engine!r}))"
    r = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, cwd=os.path.dirname(os.path.abspath(__file__)))
    return r.stdout.strip() or r.stderr.strip()[-200:]


def same(a, b):
    """Two answers alike: the same rows in any order, columns by position, numbers equal within 1e-6 of their size
    (an average of decimals is a double in DuckDB and a decimal of scale 6 in DataFusion), text without trailing blanks."""
    def value(v):
        if v is None:
            return (0, "")
        if isinstance(v, (str, bytes)):
            return (2, str(v).rstrip())
        try:
            return (1, float(v))
        except (TypeError, ValueError):
            return (2, str(v))

    def rows(t):
        cols = [c.to_pylist() for c in t.columns]
        out = [tuple(value(c[i]) for c in cols) for i in range(t.num_rows)]
        return sorted(out, key=lambda r: tuple((k, f"{x:.4g}" if k == 1 else x) for k, x in r))

    def alike(x, y):
        if x[0] != y[0]:
            return False
        if x[0] != 1:
            return x == y
        if math.isnan(x[1]) or math.isnan(y[1]):
            return math.isnan(x[1]) and math.isnan(y[1])
        return abs(x[1] - y[1]) <= 1e-6 * (1 + max(abs(x[1]), abs(y[1])))
    if a.num_columns != b.num_columns or a.num_rows != b.num_rows:
        return False
    return all(alike(x, y) for r, o in zip(rows(a), rows(b)) for x, y in zip(r, o))


def report(args):
    import pyarrow as pa
    out = Path(args.out)
    found = {}
    for p in sorted(out.glob("*.jsonl")):
        for l in p.read_text().splitlines():
            r = json.loads(l)
            found.setdefault((r["benchmark"], r["sf"]), {}).setdefault(r["engine"], {})[r["query"]] = r
    versions = json.loads((out / "versions.json").read_text()) if (out / "versions.json").exists() else {}
    text = []
    for (bench, sf), by in sorted(found.items()):
        engines = [e for e in ENGINES if e in by]
        queries = sorted({q for e in engines for q in by[e]})
        checked = {}
        for e in engines:
            for q in queries:
                r = by[e].get(q)
                if not r or r["best_s"] is None or e == "duckdb":
                    continue
                mine, theirs = out / "answers" / f"{bench}-{sf}-{e}-{q}.arrow", out / "answers" / f"{bench}-{sf}-duckdb-{q}.arrow"
                if mine.exists() and theirs.exists():
                    checked[(e, q)] = same(pa.ipc.open_stream(mine.read_bytes()).read_all(), pa.ipc.open_stream(theirs.read_bytes()).read_all())
        text.append(f"## {bench.upper()} SF{sf}, {len(queries)} queries\n")
        text.append("| engine | version | answered | total of best times (s) | geomean vs DuckDB | answers differ from DuckDB's |")
        text.append("|---|---|---|---|---|---|")
        for e in engines:
            ok = [q for q in queries if by[e].get(q, {}).get("best_s") is not None]
            both = [q for q in ok if by.get("duckdb", {}).get(q, {}).get("best_s") is not None]
            ratio = math.exp(sum(math.log(by[e][q]["best_s"] / by["duckdb"][q]["best_s"]) for q in both) / len(both)) if both else None
            differ = sorted(q for (x, q), good in checked.items() if x == e and not good)
            text.append(f"| {e} | {versions.get(e, '')} | {len(ok)} of {len(queries)} | {sum(by[e][q]['best_s'] for q in ok):.2f} | "
                        f"{'' if ratio is None else f'{ratio:.2f}x'} | {', '.join(differ) or 'none'} |")
        text.append("")
        for e in engines:
            for o in engines:
                if o == e:
                    continue
                both = [q for q in queries if by[e].get(q, {}).get("best_s") is not None and by[o].get(q, {}).get("best_s") is not None]
                faster = sum(by[e][q]["best_s"] < by[o][q]["best_s"] for q in both)
                if e == "pondra":
                    text.append(f"- pondra is faster than {o} on {faster} of {len(both)} queries both answered")
        failed = [f"{e} {q}: {by[e][q]['status'][:160]}" for e in engines for q in queries if by[e].get(q, {}).get("best_s") is None and q in by[e]]
        if failed:
            text.append("\nNot answered:\n" + "\n".join(f"- {x}" for x in failed))
        text.append("\nPer query, best time (s):\n")
        text.append("| query | " + " | ".join(engines) + " |")
        text.append("|---|" + "---|" * len(engines))
        for q in queries:
            cells = []
            for e in engines:
                r = by[e].get(q)
                cells.append("" if r is None else (f"{r['best_s']:.3f}" if r["best_s"] is not None else "x") + ("" if checked.get((e, q), True) else " ≠"))
            text.append(f"| {q} | " + " | ".join(cells) + " |")
        text.append("")
    print("\n".join(text))
    if args.md:
        Path(args.md).write_text("\n".join(text) + "\n")


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run")
    r.add_argument("benchmark", choices=["tpch", "tpcds"])
    r.add_argument("sf")
    r.add_argument("--engine", choices=ENGINES, required=True)
    r.add_argument("--queries", required=True, help="the benchmark's queries/sql")
    r.add_argument("--data", required=True, help="the folder that holds tpch-<sf> and tpcds-<sf>")
    r.add_argument("--iters", type=int, default=5)
    r.add_argument("--timeout", type=float, default=60)
    r.add_argument("--threads", type=int)
    r.add_argument("--only", help="query numbers, e.g. 1,5,14")
    r.add_argument("--again", action="store_true", help="run every query again (default: go on where a run stopped)")
    r.add_argument("--pondra-python", help="the folder that holds the pondra package (this repository's python/)")
    r.add_argument("--pivot", help="Pivot's release binary (storage.googleapis.com/pivot-releases, its sha256 checked)")
    r.add_argument("--pivot-port", type=int, default=15432)
    r.add_argument("--pivot-memory", default="8g", help="its buffer pool: 80%% of the memory less 4 GiB is its own default")
    r.add_argument("--out", default="tpc-lanes")
    p = sub.add_parser("report")
    p.add_argument("--out", default="tpc-lanes")
    p.add_argument("--md")
    args = ap.parse_args()
    run(args) if args.cmd == "run" else report(args)


if __name__ == "__main__":
    main()
