"""Round 21's costs: reads of a table with renamed and dropped columns, a view filled from 10 M
rows, and a keyed table read by event time (order_by) against one read by arrival."""
import sys, time, json, shutil, argparse
sys.path.insert(0, "/home/claude/pondra/tools")
import harness as h
h.A = argparse.Namespace(s3=False, port=8390)
lake = "/tmp/pondra-costs"
shutil.rmtree(lake, ignore_errors=True)
node = h.Node(lake, 8390, tier_secs=1).start()
q = lambda s: h.sql(8390, s)
import itertools
_n = itertools.count()
def best(f, n=5):  # (each run a new text: remembered answers would time nothing)
    out = []
    for _ in range(n):
        t = time.time(); f(next(_n)); out.append(time.time() - t)
    return min(out)
out = {}
q("CREATE TABLE t (id BIGINT, a BIGINT, b DOUBLE, c VARCHAR)")
q("INSERT INTO t SELECT value, value % 1000, value * 0.5, 'x' || (value % 100) FROM generate_series(1, 10000000)")
best(lambda i: q(f"SELECT a, sum(b) FROM t WHERE a < 500 AND {i} = {i} GROUP BY a"))  # (warm)
out["scan_10m_plain_s"] = round(best(lambda i: q(f"SELECT a, sum(b) FROM t WHERE a < 500 AND {i} = {i} GROUP BY a")), 3)
q("ALTER TABLE t RENAME COLUMN a TO k")
q("ALTER TABLE t DROP COLUMN c")
out["scan_10m_renamed_and_dropped_s"] = round(best(lambda i: q(f"SELECT k, sum(b) FROM t WHERE k < 500 AND {i} = {i} GROUP BY k")), 3)
t0 = time.time()
q("CREATE MATERIALIZED VIEW per_k AS SELECT k, count(*) AS n, sum(b) AS s FROM t GROUP BY k")
out["view_filled_from_10m_rows_s"] = round(time.time() - t0, 2)
out["view_rows_ok"] = q("SELECT sum(n) AS n FROM per_k")[0]["n"] == 10_000_000
t0 = time.time()
q("CREATE MATERIALIZED VIEW evens AS SELECT id, b * 2 AS b2 FROM t WHERE k % 2 = 0")
out["row_view_filled_from_10m_rows_s"] = round(time.time() - t0, 2)
# A keyed table read by arrival and by event time: 1.5 M rows, 5 generations of files and the log, 200 k keys.
for name, opt in (("by_arrival", ""), ("by_event_time", " WITH (order_by = 'ts')")):
    q(f"CREATE TABLE {name} (k BIGINT PRIMARY KEY, ts BIGINT, v BIGINT){opt}")
    for r in range(6):  # (6 generations of files, then rows in the log: no compaction yet)
        q(f"INSERT INTO {name} SELECT (value * 7919) % 200000, (value * 104729) % 1000000, value FROM generate_series({r * 250000}, {r * 250000 + 249999})")
        if r < 5:
            h.call(8390, "POST", "/tier")
    out[f"keyed_2m_{name}_rows"] = q(f"SELECT count(*) AS n FROM {name}")[0]["n"]
    out[f"keyed_2m_{name}_s"] = round(best(lambda i: q(f"SELECT k % 10 AS d, count(*), sum(v), max(ts) FROM {name} WHERE ts > {i} GROUP BY 1")), 3)
node.kill()
shutil.rmtree(lake, ignore_errors=True)
print(json.dumps(out, indent=1))
