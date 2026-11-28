import json, os, sys, tempfile, shutil
sys.path.insert(0, "/tmp/claude-0/wt-vortex/tools")
import harness
from harness import Node, call
harness.BIN = sys.argv[1]
fmt = sys.argv[2]
lake = tempfile.mkdtemp(prefix="vx-smoke-")
n = Node(lake, 18950, env={"PONDRA_FILE_FORMAT": fmt, "PONDRA_HOT_GB": "0"}).start()
out = {}
def q(s):
    try:
        return call(18950, "POST", "/sql", s.encode(), timeout=600)
    except Exception as e:
        return f"ERR {e}"
try:
    out["create"] = q("CREATE TABLE t AS SELECT i AS id, 'v' || i AS s, DATE '2026-01-01' + (i % 30)::int AS d, i / 3.0 AS x, (i * 1.25)::DECIMAL(12,2) AS m, TIMESTAMP '2026-01-01' + i * INTERVAL '1 second' AS ts, [i::int, 2] AS l, {'a': i::int, 'b': 'b' || i} AS st FROM range(100000) r(i)")
    call(18950, "POST", "/tier", timeout=600)
    out["files"] = sorted({os.path.splitext(f)[1] for _, _, fs in os.walk(lake) for f in fs if not f.endswith(".json")})
    out["count"] = q("SELECT count(*) AS n, sum(id) AS s, min(s) AS lo, max(d) AS hi, sum(m) AS m FROM t")
    out["filter"] = q("SELECT id, s, st, l FROM t WHERE id BETWEEN 500 AND 502 ORDER BY id")
    out["delete"] = q("DELETE FROM t WHERE id % 10 = 0")
    out["after_delete"] = q("SELECT count(*) AS n FROM t")
    out["update"] = q("UPDATE t SET s = 'u' WHERE id < 5")
    out["after_update"] = q("SELECT id, s FROM t WHERE id < 5 ORDER BY id")
    call(18950, "POST", "/tier", timeout=600)
    out["after_tier"] = q("SELECT count(*) AS n, count(DISTINCT s) AS ds FROM t")
    out["topn"] = q("SELECT * FROM t ORDER BY x DESC LIMIT 2")
    out["keyed"] = [q("CREATE TABLE k (id BIGINT PRIMARY KEY, v VARCHAR)"), q("INSERT INTO k SELECT i, 'a' FROM range(1000) r(i)"),
                    q("INSERT INTO k VALUES (5, 'b') ON CONFLICT (id) DO UPDATE SET v = excluded.v"), q("SELECT v FROM k WHERE id = 5"), q("SELECT count(*) AS n FROM k")]
    call(18950, "POST", "/tier", timeout=600)
    out["keyed_after_tier"] = [q("SELECT v FROM k WHERE id = 5"), q("SELECT count(*) AS n FROM k")]
finally:
    n.kill()
    shutil.rmtree(lake, ignore_errors=True)
print(json.dumps(out, indent=1, default=str))
