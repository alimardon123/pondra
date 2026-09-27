"""Round 22's costs: a frame against the same SQL written by hand (10 M rows, and a small table's
round trip), a macro's expansion, a CALL (SQL and Python) against the statement itself, and rows
sent with a request."""
import itertools, json, os, shutil, statistics, sys, tempfile, time
sys.path.insert(0, "/home/claude/pondra/python")
os.environ["PONDRA_BIN"] = "/home/claude/pondra/target/release/pondra"
import pondra
from pondra import col
import pandas as pd

tmp = tempfile.mkdtemp(prefix="pondra-costs22-")
con = pondra.local(os.path.join(tmp, "lake"), port=8395)
n = itertools.count()
out = {}


def best(f, k=7):
    times = []
    for _ in range(k):
        t = time.perf_counter(); f(next(n)); times.append(time.perf_counter() - t)
    return min(times)


def median(f, k=200):
    times = []
    for _ in range(k):
        t = time.perf_counter(); f(next(n)); times.append(time.perf_counter() - t)
    return statistics.median(times)


con.sql("CREATE TABLE t (id BIGINT, user VARCHAR, qty BIGINT, price DOUBLE)")
con.sql("INSERT INTO t SELECT value, 'u' || (value % 1000), value % 7, (value % 997) * 0.25 FROM generate_series(1, 10000000)")
con._call("POST", "/tier")
sql = lambda i: f"SELECT user, sum(qty * price) AS total, count(*) AS n FROM t WHERE qty > 2 AND {i} = {i} GROUP BY user ORDER BY total DESC LIMIT 10"
frame = lambda i: con.table("t").filter(col("qty") > 2).filter(f"{i} = {i}").with_column((col("qty") * col("price")).alias("total")) \
    .group_by("user").agg(col("total").sum(), pondra.len().alias("n")).sort("total", descending=True).limit(10)
f0 = frame(0)
f0.schema  # (warm: the with_column step's schema is asked once per frame)
assert con.sql(sql(0)).rows() == f0.rows()
out["10m_rows_sql_s"] = round(best(lambda i: con.sql(sql(i)).collect()), 4)
built = [frame(i) for i in range(100, 110)]  # (built beforehand: only the run is timed)
it = iter(built)
out["10m_rows_frame_run_s"] = round(best(lambda i: next(it).collect(), 7), 4)
out["frame_build_with_schema_s"] = round(best(lambda i: frame(i)), 4)  # (with_column asks the node for columns)
con.sql("CREATE TABLE small (a BIGINT, b VARCHAR)")
con.sql("INSERT INTO small VALUES (1, 'x'), (2, 'y')")
out["small_sql_ms"] = round(median(lambda i: con.sql(f"SELECT a, b FROM small WHERE a < {i}").collect()) * 1000, 2)
out["small_frame_ms"] = round(median(lambda i: con.table("small").filter(col("a") < i).select("a", "b").collect()) * 1000, 2)
con.sql("CREATE MACRO net(x, rate := 0.2) AS x * (1 - rate)")
out["small_with_macro_ms"] = round(median(lambda i: con.sql(f"SELECT net(a) AS v FROM small WHERE a < {i}").collect()) * 1000, 2)
out["small_macro_written_out_ms"] = round(median(lambda i: con.sql(f"SELECT a * (1 - 0.2) AS v FROM small WHERE a < {i}").collect()) * 1000, 2)
con.sql("CREATE PROCEDURE p_small(k BIGINT) LANGUAGE sql AS $$ SELECT a, b FROM small WHERE a < $k $$")
out["call_sql_procedure_ms"] = round(median(lambda i: con.sql(f"CALL p_small({i})").to_arrow()) * 1000, 2)
con.sql("CREATE PROCEDURE p_py(k BIGINT) LANGUAGE python AS $$\nk + 1\n$$")
out["call_python_procedure_ms"] = round(median(lambda i: con.sql(f"CALL p_py({i})").to_arrow(), 20) * 1000, 1)
con.sql("CREATE PROCEDURE p_py_frame(k BIGINT) LANGUAGE python AS $$\ncon.table('small').filter(pondra.col('a') < k)\n$$")
out["call_python_procedure_frame_ms"] = round(median(lambda i: con.sql(f"CALL p_py_frame({i})").to_arrow(), 20) * 1000, 1)
targets = pd.DataFrame({"user": [f"u{i}" for i in range(1000)], "target": list(range(1000))})
out["pandas_1000_rows_joined_with_10m_s"] = round(best(lambda i: con.sql(f"SELECT t.user, count(*) AS n, max(g.target) AS g FROM t JOIN targets g USING (user) WHERE {i} = {i} GROUP BY t.user").collect(), 3), 4)
big = pd.DataFrame({"user": [f"u{i % 1000}" for i in range(1_000_000)], "x": range(1_000_000)})
out["pandas_1m_rows_sent_and_summed_s"] = round(best(lambda i: con.sql(f"SELECT count(*) AS n, sum(x) AS s FROM big WHERE {i} = {i}").collect(), 3), 4)
con.close()
shutil.rmtree(tmp, ignore_errors=True)
print(json.dumps(out, indent=1))
