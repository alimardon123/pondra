#!/usr/bin/env python3
"""TPC-DS's 99 queries through Pondra, each answer compared with DuckDB's on the same data
(roadmap D2, round 31). The data and the queries are DuckDB's: its `tpcds` extension's `dsdgen`
and `tpcds_queries()`, written out as one Parquet file a table.

  tpcds_check.py prepare --data DIR [--sf 1] [--extension tpcds.duckdb_extension]
  tpcds_check.py run --data DIR [--nodes 1|3] [--only 1,2,3] [--hot] [--out logs/round31/tpcds.json]

`prepare` needs DuckDB's extension: DuckDB fetches it itself (`INSTALL tpcds`), or give the file
(https://extensions.duckdb.org/v<version>/<platform>/tpcds.duckdb_extension.gz, unzipped).

`run` loads the tables into a lake (`CREATE TABLE t AS SELECT * FROM 't.parquet'`), starts the
nodes, and sends each query over HTTP; DuckDB answers the same query over the same files. Two
answers agree when they have the same rows: numbers within 1e-6 of each other (relative, or 1e-4
absolute: an average kept as a DECIMAL against DuckDB's DOUBLE), in the same order where the query
has an ORDER BY (rows that tie on it may come in either order).
"""
import argparse, datetime, decimal, io, json, os, re, subprocess, sys, tempfile, time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import harness

TABLES = ["call_center", "catalog_page", "catalog_returns", "catalog_sales", "customer", "customer_address", "customer_demographics", "date_dim",
          "household_demographics", "income_band", "inventory", "item", "promotion", "reason", "ship_mode", "store", "store_returns", "store_sales",
          "time_dim", "warehouse", "web_page", "web_returns", "web_sales", "web_site"]


def prepare():
    import duckdb
    os.makedirs(os.path.join(A.data, "queries"), exist_ok=True)
    con = duckdb.connect()
    con.execute(f"INSTALL '{A.extension}'" if A.extension else "INSTALL tpcds")
    con.execute("LOAD tpcds")
    con.execute(f"CALL dsdgen(sf={A.sf})")
    for t in TABLES:
        con.execute(f"COPY {t} TO '{os.path.join(A.data, t)}.parquet' (FORMAT parquet)")
    for n, q in con.execute("SELECT query_nr, query FROM tpcds_queries()").fetchall():
        open(os.path.join(A.data, "queries", f"q{n:02}.sql"), "w").write(q)
    print(f"{len(TABLES)} tables and the queries in {A.data}")


def value(v):
    """A value as both engines' answers are compared: numbers as floats, times as text."""
    if isinstance(v, (decimal.Decimal, float, int)) and not isinstance(v, bool):
        return float(v)
    if isinstance(v, (datetime.date, datetime.datetime, datetime.time)):
        return v.isoformat()
    return v


def close(a, b):
    if isinstance(a, float) and isinstance(b, float):
        return a == b or abs(a - b) <= max(1e-4, 1e-6 * max(abs(a), abs(b)))
    return a == b


def rows_close(x, y):
    return len(x) == len(y) and all(len(r) == len(s) and all(close(a, b) for a, b in zip(r, s)) for r, s in zip(x, y))


def key(r):
    return [(v is None, str(type(v)), round(v, 2) if isinstance(v, float) else v) for v in r]


def compare(mine, theirs, ordered):
    """'same', 'same rows' (in another order the query leaves open), or what differs."""
    if rows_close(mine, theirs):
        return "same"
    if len(mine) != len(theirs):
        return f"{len(mine)} rows, DuckDB {len(theirs)}"
    if rows_close(sorted(mine, key=key), sorted(theirs, key=key)):
        return "same rows" if ordered else "same"
    bad = next(i for i, (r, s) in enumerate(zip(sorted(mine, key=key), sorted(theirs, key=key))) if not rows_close([r], [s]))
    return f"row {bad} differs: {sorted(mine, key=key)[bad]} vs {sorted(theirs, key=key)[bad]}"[:400]


def outermost(sql):
    s = re.sub(r"'[^']*'", "''", sql)
    while (t := re.sub(r"\([^()]*\)", "", s)) != s:
        s = t
    return s


def run():
    import duckdb, pyarrow as pa
    folder = os.path.join(A.data, "queries")
    names = sorted(f for f in os.listdir(folder) if f.endswith(".sql"))
    if A.only:
        names = [n for n in names if int(n[1:3]) in {int(x) for x in A.only.split(",")}]
    duck = duckdb.connect()
    for t in TABLES:
        duck.execute(f"CREATE VIEW {t} AS SELECT * FROM read_parquet('{os.path.join(A.data, t)}.parquet')")
    harness.A = argparse.Namespace(s3=False, keep=False, port=A.port)
    lake = harness.new_lake()
    t0 = time.time()
    for t in TABLES:
        subprocess.run([harness.BIN, "sql", "--dir", lake, f"CREATE TABLE {t} AS SELECT * FROM '{os.path.join(os.path.abspath(A.data), t)}.parquet'"], check=True, capture_output=True)
    load = time.time() - t0
    nodes = [harness.Node(lake, A.port + i).start() for i in range(A.nodes)]
    out, t0 = {}, time.time()
    try:
        if A.nodes > 1:
            while len(harness.call(A.port, "GET", "/stats")["nodes"]) < A.nodes:
                time.sleep(0.1)
        for run in range(2 if A.hot else 0):  # (hot.rs takes a file on its second read)
            for n in names:
                try:
                    harness.call(A.port, "POST", "/sql", open(os.path.join(folder, n)).read().strip().rstrip(";").encode() + f" -- warm {run}".encode(), timeout=A.timeout)
                except Exception:
                    pass  # (its own run below says why)
        if A.hot:
            print(f"hot columns: {harness.hot_settled(A.port) / 1e9:.2f} GB", flush=True)
        for n in names:
            q = open(os.path.join(folder, n)).read().strip().rstrip(";")
            r = {}
            try:
                t = time.time()
                theirs = [tuple(value(v) for v in row) for row in duck.execute(q).fetchall()]
                r["duckdb_s"] = round(time.time() - t, 3)
            except Exception as e:
                out[n] = {"verdict": "DuckDB refused", "why": str(e)[:300]}
                continue
            try:
                t = time.time()
                path = "/sql?format=arrow" + ("&spread=1" if A.nodes > 1 else "")
                data = harness.call(A.port, "POST", path, q.encode(), timeout=A.timeout)
                r["pondra_s"] = round(time.time() - t, 3)
                table = pa.ipc.open_stream(io.BytesIO(data)).read_all()
                mine = [tuple(value(v) for v in row.values()) for row in table.to_pylist()]
                r["verdict"] = compare(mine, theirs, bool(re.search(r"\border\s+by\b", outermost(q), re.I)))
                r["rows"] = len(mine)
            except Exception as e:
                r["verdict"], r["why"] = "error", str(e)[:400]
            out[n] = r
            print(f"{n}: {r['verdict']} ({r.get('pondra_s')} s, DuckDB {r.get('duckdb_s')} s)", flush=True)
    finally:
        [node.kill() for node in nodes]
        harness.clean_up()
    same = [n for n, r in out.items() if r["verdict"] in ("same", "same rows")]
    summary = {"sf": A.sf, "nodes": A.nodes, "queries": len(out), "same": len(same), "load_s": round(load, 1), "secs": round(time.time() - t0),
               "pondra_s": round(sum(r.get("pondra_s", 0) for r in out.values()), 2), "duckdb_s": round(sum(r.get("duckdb_s", 0) for r in out.values()), 2),
               "not_same": {n: r for n, r in out.items() if n not in same}, "each": out}
    print(f"\n{len(same)} of {len(out)} queries answer as DuckDB does, {A.nodes} node(s)")
    for n, r in summary["not_same"].items():
        print(f"  {n}: {r['verdict']} {r.get('why', '')[:200]}")
    if A.out:
        json.dump(summary, open(A.out, "w"), indent=1)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("what", choices=["prepare", "run"])
    ap.add_argument("--data", required=True)
    ap.add_argument("--sf", default="1")
    ap.add_argument("--extension")
    ap.add_argument("--nodes", type=int, default=1)
    ap.add_argument("--only", default="")
    ap.add_argument("--port", type=int, default=9300)
    ap.add_argument("--timeout", type=int, default=600)
    ap.add_argument("--out")
    ap.add_argument("--hot", action="store_true", help="answer from the hot columns: every query run twice first, the columns loaded")
    A = ap.parse_args()
    {"prepare": prepare, "run": run}[A.what]()
