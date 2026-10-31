#!/usr/bin/env python3
"""Run the TPC-H-shaped queries on DuckDB and Polars, one fresh process per query, under a memory cap.

  python3 others.py --data DIR [--engines duckdb,polars] [--only id,id] [--cap-gb 2]
                    [--timeout 1200] [--out FILE] [--keep]
  python3 others.py --compare FILE        # per query: do DuckDB and Polars agree, in a results file

Every query runs in its own child process (--child). The child's first action is to move itself
into the bigmem memory cgroup, so the cap applies to it alone. The parent samples the child's memory
(its RSS and its children's) and the engine's spill folder every 0.05 s, and appends one JSON line
per query to --out.
"""
import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import time
from decimal import Decimal

HERE = os.path.dirname(os.path.abspath(__file__))  # the scratchpad: queries.json, spill/, run/, results/
QUERIES = os.path.join(HERE, "queries.json")
SPILL = os.path.join(HERE, "spill")                # spill/<engine> is each engine's temp folder
RUN = os.path.join(HERE, "run")                    # each child's stdout and stderr
CGROUP = "/sys/fs/cgroup/memory" + next(l.split(":", 2)[2].strip() for l in open("/proc/self/cgroup") if ":memory:" in l) + "/bigmem"
TABLES = ("lineitem", "orders", "customer")
THREADS = 4
ENGINES = ("duckdb", "polars")
COPY = re.compile(r"^\s*COPY\s*\((.*)\)\s*TO\s*'([^']*)'\s*\(FORMAT\s+parquet\)\s*;?\s*$", re.S | re.I)
NUMBER = re.compile(r"^-?\d+(\.\d+)?([eE][-+]?\d+)?$")


# ---------------------------------------------------------------- the child: one query, one engine

def join_cgroup():
    with open(os.path.join(CGROUP, "cgroup.procs"), "w") as f:
        f.write(str(os.getpid()))


def emit(obj):
    print(json.dumps(obj), flush=True)


def shown(rows):
    """The first three rows, each cell as text cut to 60 characters."""
    return [[str(v)[:60] for v in row] for row in rows[:3]]


def cell(v):
    """A cell as the digest sees it: numbers to two places, so float formatting doesn't matter."""
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return str(v)
    if isinstance(v, (int, float, Decimal)):
        return f"{float(v):.2f}"
    return str(v)


def digest(rows):
    h = hashlib.sha1()
    for row in rows:
        h.update(("\x1f".join(cell(v) for v in row) + "\n").encode())
    return h.hexdigest()[:16]


def run_duckdb(data, sql):
    import duckdb
    con = duckdb.connect()
    con.execute(f"SET threads={THREADS}")
    con.execute(f"SET temp_directory='{os.path.join(SPILL, 'duckdb')}'")
    con.execute("SET preserve_insertion_order=false")
    for t in TABLES:
        con.execute(f"CREATE VIEW {t} AS SELECT * FROM read_parquet('{data}/{t}/*.parquet')")
    # Printed before the query runs, so a run that is killed or fails later still reports its setting.
    emit({"setting": con.execute("SELECT current_setting('memory_limit')").fetchone()[0]})
    t0 = time.perf_counter()
    rows = con.execute(sql).fetchall()  # a COPY returns one row; its answer is the file it wrote
    return rows, time.perf_counter() - t0


def polars_lazy(pl):
    """LazyFrame versions, by query id, for the queries Polars' SQL can't run. Each computes what the
    SQL says (same columns, same meaning); the SQL is used for every other query."""
    return {}


def run_polars(qid, data, sql, copy_out):
    os.environ["POLARS_TEMP_DIR"] = os.path.join(SPILL, "polars")
    os.environ["POLARS_MAX_THREADS"] = str(THREADS)
    import polars as pl
    tables = {t: pl.scan_parquet(f"{data}/{t}/*.parquet") for t in TABLES}
    emit({"setting": "streaming"})
    lazy = polars_lazy(pl)
    t0 = time.perf_counter()
    frame = lazy[qid](tables) if qid in lazy else pl.SQLContext(**tables).execute(sql, eager=False)
    if copy_out:
        frame.sink_parquet(copy_out)
        return None, time.perf_counter() - t0
    rows = frame.collect(engine="streaming").rows()
    return rows, time.perf_counter() - t0


def copy_back(out):
    """A COPY's answer: the rows its file holds (from the footer) and the first three of them."""
    import pyarrow.parquet as pq
    pf = pq.ParquetFile(out)
    batch = next(pf.iter_batches(batch_size=3), None)
    first = [tuple(r.values()) for r in batch.to_pylist()] if batch is not None else []
    return pf.metadata.num_rows, first


def child(engine, qid, data, out):
    with open(QUERIES) as f:
        sql = next(q["sql"] for q in json.load(f) if q["id"] == qid)
    sql = sql.replace("{out}", out)
    copy = COPY.match(sql)
    if engine == "duckdb":
        rows, secs = run_duckdb(data, sql)
    else:
        rows, secs = run_polars(qid, data, copy.group(1) if copy else sql, out if copy else None)
    if copy:
        n, first = copy_back(out)
    else:
        n, first = len(rows), rows
    emit({"rows": n, "result": shown(first), "digest": None if copy else digest(rows),
          "query_secs": round(secs, 3)})


# ---------------------------------------------------------------- the parent: one query at a time

def read(path):
    with open(path) as f:
        return f.read().strip()


def write(path, text):
    with open(path, "w") as f:
        f.write(text)


def set_cap(cap_gb):
    limit = int(cap_gb * 2**30)
    os.makedirs(CGROUP, exist_ok=True)  # (this shell's cgroup is new each session)
    write(os.path.join(CGROUP, "memory.limit_in_bytes"), str(limit))
    now = read(os.path.join(CGROUP, "memory.limit_in_bytes"))
    if abs(int(now) - limit) >= 4096:  # the cgroup keeps whole pages
        raise SystemExit(f"memory cap not set: wanted {limit}, cgroup says {now}")
    print(f"memory cap: {cap_gb} GiB ({now} bytes) on {CGROUP}", file=sys.stderr)


def tree_rss(pid):
    import psutil
    try:
        p = psutil.Process(pid)
        procs = [p] + p.children(recursive=True)
    except psutil.Error:
        return 0
    total = 0
    for x in procs:
        try:
            total += x.memory_info().rss
        except psutil.Error:
            pass  # it exited a moment ago
    return total


def kill_tree(pid):
    import psutil
    try:
        p = psutil.Process(pid)
        procs = p.children(recursive=True) + [p]
    except psutil.Error:
        return
    for x in procs:
        try:
            x.kill()
        except psutil.Error:
            pass


def folder_bytes(path):
    total = 0
    for root, _, files in os.walk(path):
        for name in files:
            try:
                total += os.path.getsize(os.path.join(root, name))
            except OSError:
                pass  # a spill file that was just removed
    return total


def run_one(engine, q, data, keep, timeout):
    spill = os.path.join(SPILL, engine)
    shutil.rmtree(spill, ignore_errors=True)
    os.makedirs(spill)
    out = os.path.join(SPILL, f"out-{engine}-{q['id']}.parquet")
    if os.path.exists(out):
        os.remove(out)
    base = os.path.join(RUN, f"{engine}-{q['id']}")
    failcnt_before = int(read(os.path.join(CGROUP, "memory.failcnt")))
    timed_out = False
    peak_rss = peak_spill = 0
    with open(base + ".out", "w") as so, open(base + ".err", "w") as se:
        t0 = time.monotonic()
        proc = subprocess.Popen([sys.executable, os.path.abspath(__file__), "--child", engine, q["id"], data, out],
                                stdout=so, stderr=se)
        while proc.poll() is None:
            peak_rss = max(peak_rss, tree_rss(proc.pid))
            peak_spill = max(peak_spill, folder_bytes(spill))
            if time.monotonic() - t0 > timeout:
                timed_out = True
                kill_tree(proc.pid)
                break
            time.sleep(0.05)
        rc = proc.wait()
        secs = time.monotonic() - t0
    peak_spill = max(peak_spill, folder_bytes(spill))
    failcnt = int(read(os.path.join(CGROUP, "memory.failcnt"))) - failcnt_before

    info = {}
    with open(base + ".out") as f:
        for line in f:
            if line.startswith("{"):
                try:
                    info.update(json.loads(line))
                except ValueError:
                    pass  # a line cut short by a kill
    with open(base + ".err") as f:
        err = f.read()
    if timed_out:
        status = "timeout"
    elif rc < 0:
        status = "killed"
    elif rc == 0 and "rows" in info:
        status = "ok"
    else:
        status = "error"
    ok = status == "ok"
    if not ok:
        if err.strip():
            error = err[-400:]
        elif timed_out:
            error = f"timed out after {timeout:g} s"
        else:
            error = f"died by signal {-rc}" if rc < 0 else f"exit code {rc}"
    else:
        error = ""
    if not keep and os.path.exists(out):
        os.remove(out)
    return {
        "engine": engine, "id": q["id"], "kind": q.get("kind"), "status": status,
        "secs": round(secs, 1), "peak_rss_mb": peak_rss // 2**20, "peak_spill_mb": peak_spill // 2**20,
        "rows": info.get("rows") if ok else None,
        "result": info.get("result") if ok else None,
        "digest": info.get("digest") if ok else None,
        "query_secs": info.get("query_secs") if ok else None,
        "error": error, "returncode": rc, "setting": info.get("setting", ""), "failcnt": failcnt,
    }


# ---------------------------------------------------------------- agreement between the engines

def norm(v):
    s = str(v)
    return f"{round(float(s), 2):.2f}" if NUMBER.match(s) else s


def judge(a, b, sql):
    if a is None or b is None:
        return "missing a run"
    if a["status"] != "ok" or b["status"] != "ok":
        return f"status {a['status']} / {b['status']}"
    if a["rows"] != b["rows"]:
        return f"rows differ: {a['rows']} vs {b['rows']}"
    # Rows of a query with no ORDER BY come in no fixed order: only the count is compared then.
    if "ORDER BY" not in sql.upper() and a["rows"] > 1:
        return f"rows agree: {a['rows']} (no ORDER BY: the rows themselves are not compared)"
    shape = lambda r: [[norm(c) for c in row] for row in r]
    if shape(a["result"]) != shape(b["result"]):
        return "DISAGREE: first rows differ"
    if a["digest"] and b["digest"] and a["digest"] != b["digest"]:
        return "DISAGREE: digest of all rows differs"
    return f"agree: {a['rows']} rows, first rows" + (", digest" if a["digest"] else "")


def compare(path):
    latest = {}
    with open(path) as f:
        for line in f:
            if line.strip():
                r = json.loads(line)
                latest[(r["engine"], r["id"])] = r  # the last run of each
    with open(QUERIES) as f:
        queries = json.load(f)
    disagree = 0
    for q in queries:
        if ("duckdb", q["id"]) not in latest and ("polars", q["id"]) not in latest:
            continue
        verdict = judge(latest.get(("duckdb", q["id"])), latest.get(("polars", q["id"])), q["sql"])
        disagree += verdict.startswith(("DISAGREE", "rows differ", "status", "missing"))
        print(f"{q['id']:26} {verdict}")
    print(f"{disagree} disagree" if disagree else "all agree", file=sys.stderr)
    return 1 if disagree else 0


# ---------------------------------------------------------------- main

def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--data", help="folder with lineitem/, orders/ and customer/ (Parquet)")
    ap.add_argument("--engines", default="duckdb,polars")
    ap.add_argument("--only", default="", help="comma-separated query ids")
    ap.add_argument("--cap-gb", type=float, default=2.0, help="memory cap of each query, GiB")
    ap.add_argument("--timeout", type=float, default=1200.0, help="seconds per query")
    ap.add_argument("--out", default=os.path.join(HERE, "results", "others.jsonl"))
    ap.add_argument("--keep", action="store_true", help="keep each COPY's output file in spill/")
    ap.add_argument("--compare", metavar="FILE", help="show per query whether DuckDB and Polars agree, then exit")
    args = ap.parse_args()
    if args.compare:
        return compare(args.compare)
    if not args.data:
        ap.error("--data is required")
    engines = [e.strip() for e in args.engines.split(",") if e.strip()]
    if [e for e in engines if e not in ENGINES]:
        ap.error(f"engines are {', '.join(ENGINES)}")
    with open(QUERIES) as f:
        queries = json.load(f)
    only = [s for s in args.only.split(",") if s]
    known = {q["id"] for q in queries}
    if set(only) - known:
        ap.error("unknown query ids: " + ", ".join(sorted(set(only) - known)))
    queries = [q for q in queries if not only or q["id"] in only]
    data = os.path.abspath(args.data)
    os.makedirs(RUN, exist_ok=True)
    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    set_cap(args.cap_gb)
    for engine in engines:
        for q in queries:
            rec = run_one(engine, q, data, args.keep, args.timeout)
            with open(args.out, "a") as f:
                f.write(json.dumps(rec) + "\n")
                f.flush()
            print(f"{engine:7} {q['id']:24} {rec['status']:7} {rec['secs']:6.1f}s "
                  f"rss={rec['peak_rss_mb']}MB spill={rec['peak_spill_mb']}MB rows={rec['rows']} "
                  f"failcnt+{rec['failcnt']}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    if sys.argv[1:2] == ["--child"]:
        join_cgroup()  # the child's first action: the cap applies from here on
        child(*sys.argv[2:6])
    else:
        sys.exit(main())
