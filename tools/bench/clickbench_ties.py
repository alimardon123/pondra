#!/usr/bin/env python3
"""The ClickBench answers that differ from DuckDB's are ties under a LIMIT (round 32): a query that
groups and keeps the top rows may keep any of the groups tied at the cut. For each, every row Pondra
gives is a real group with the right aggregates (asked of DuckDB by its key, over the same file),
and its sort key reads as DuckDB's does at the same places.

  clickbench_ties.py [--data ~/clickbench-bench] [--bin target/release/pondra] [--lake a lake holding hits]

Without `--lake` it loads `hits.parquet` into a lake of its own (as singlenode.py does) and removes it.
"""
import argparse, duckdb, json, math, os, shutil, subprocess, sys, tempfile, time, urllib.request
ap = argparse.ArgumentParser()
ap.add_argument("--bin", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "../../target/release/pondra"))
ap.add_argument("--lake")
ap.add_argument("--data", default=os.path.expanduser("~/clickbench-bench"))
ap.add_argument("--port", type=int, default=18795)
A = ap.parse_args()
b, lake, port = A.bin, A.lake, A.port
H = f"read_parquet('{os.path.join(A.data, 'hits.parquet')}')"
if not lake:
    lake = tempfile.mkdtemp(prefix="pondra-ties-")
    subprocess.run([b, "sql", "--dir", lake, f"CREATE TABLE hits AS SELECT * FROM {H}"], check=True, capture_output=True)
Q = [l.strip().rstrip(";") for l in open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "clickbench-queries.sql")) if l.strip()]
F39 = "CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0"
SRC = "CASE WHEN (SearchEngineID = 0 AND AdvEngineID = 0) THEN Referer ELSE '' END"
spec = {  # query: (keys, aggregates, filter, the sort key's column or None)
    18: (["UserID", "SearchPhrase"], ["COUNT(*)"], "TRUE", None),
    22: (["SearchPhrase"], ["MIN(URL)", "COUNT(*)"], "URL LIKE '%google%' AND SearchPhrase <> ''", 2),
    32: (["WatchID", "ClientIP"], ["COUNT(*)", "SUM(IsRefresh)", "AVG(ResolutionWidth)"], "SearchPhrase <> ''", 2),
    33: (["WatchID", "ClientIP"], ["COUNT(*)", "SUM(IsRefresh)", "AVG(ResolutionWidth)"], "TRUE", 2),
    39: (["URL"], ["COUNT(*)"], F39 + " AND IsLink <> 0 AND IsDownload = 0", 1),
    40: (["TraficSourceID", "SearchEngineID", "AdvEngineID", SRC, "URL"], ["COUNT(*)"], F39, 5),
    41: (["URLHash", "EventDate"], ["COUNT(*)"], F39 + " AND TraficSourceID IN (-1, 6) AND RefererHash = 3594120000172545465", 2),
}
con = duckdb.connect()
lit = lambda v: "NULL" if v is None else str(v) if isinstance(v, (int, float)) else "'" + str(v).replace("'", "''") + "'"
close = lambda a, b: a == b or (isinstance(a, (int, float)) and isinstance(b, (int, float)) and math.isclose(a, b, rel_tol=1e-9))
p = subprocess.Popen([b, "serve", "--lake", lake, "--addr", f"127.0.0.1:{port}"], env={**os.environ, "PONDRA_HOT_GB": "0"}, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
def pondra(q):
    r = urllib.request.Request(f"http://127.0.0.1:{port}/sql?spread=0&format=typed", data=q.encode(), method="POST")
    return json.loads(urllib.request.urlopen(r, timeout=600).read())["rows"]
try:
    for _ in range(300):
        try: pondra("SELECT 1"); break
        except Exception: time.sleep(0.2)
    out = {}
    for q, (keys, aggs, where, sort) in spec.items():
        mine = pondra(Q[q - 1])
        duck = [list(r) for r in con.execute(Q[q - 1].replace("FROM hits", f"FROM {H}")).fetchall()]
        real = []
        for row in mine:
            cond = " AND ".join(f"({k}) = {lit(row[i])}" if row[i] is not None else f"({k}) IS NULL" for i, k in enumerate(keys))
            got = con.execute(f"SELECT {', '.join(aggs)} FROM {H} WHERE ({where}) AND {cond}").fetchone()
            want = [float(v) if isinstance(v, str) and v.replace('.', '', 1).replace('-', '', 1).isdigit() else v for v in row[len(keys):]]
            real.append(all(close(float(g) if isinstance(w, float) else g, w) or str(g) == str(w) for g, w in zip(got, want)))
        keys_same = sort is None or [float(r[sort]) for r in mine] == [float(r[sort]) for r in duck]
        out[q] = {"rows": len(mine) == len(duck), "every row a real group": all(real), "sort key as DuckDB's": keys_same}
        print(q, out[q], flush=True)
    ok = all(all(v.values()) for v in out.values())
    print(json.dumps({"duckdb": duckdb.__version__, "ok": ok}))
    sys.exit(0 if ok else 1)
finally:
    p.terminate(); p.wait()
    if not A.lake: shutil.rmtree(lake, ignore_errors=True)
