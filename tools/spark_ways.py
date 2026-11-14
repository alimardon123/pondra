#!/usr/bin/env python3
"""Spark SQL three ways: Pondra's spark_sql('...') function, Pondra's session dialect (a session that
first runs SET datafusion.sql_parser.dialect = 'spark', or SET language = 'spark', then sends the Spark
text itself), and real PySpark 4.0.1 (local[1], UTC). Runs each text below three ways and prints one
JSON line per text, then a summary line naming the texts where any of the three answers differ. A difference from
PySpark that isn't a KNOWN one (each with why) fails the run, exit 1; the KNOWN list may only shrink.

  /tmp/l6/venv-spark/bin/python tools/spark_ways.py --bin PATH [--port 8791] [--set dialect|language]
      [--out FILE] [--only SUBSTR]

Starts `BIN serve --dir <fresh temp dir> --addr 127.0.0.1:PORT`, creates Pondra table `people` and the
same rows as PySpark temp view `people`, and at the end kills the node and deletes the temp dir.
"""
import argparse
import datetime
import decimal
import http.client
import json
import math
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time

# The sandbox's TZ must not leak into the JVM (see tools/spark_check.py): pin UTC before PySpark starts.
os.environ["TZ"] = "UTC"
if hasattr(time, "tzset"):
    time.tzset()
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)

SESSION = "spark-ways"
SETS = {
    "dialect": f"SET datafusion.sql_parser.dialect = 'spark'",
    "language": "SET language = 'spark'",
}
PEOPLE_PONDRA = ("CREATE TABLE people AS SELECT id, name, xs, CAST(d AS DOUBLE) AS d FROM (VALUES "
                 "(1, 'ann', make_array(1, 2, 3), 2.5), (2, 'bob', make_array(4), -2.5), "
                 "(3, 'cy', CAST(make_array() AS BIGINT[]), 0.5), (4, 'dee', CAST(NULL AS BIGINT[]), 7.5)) "
                 "AS v(id, name, xs, d)")
PEOPLE_ROWS = [(1, "ann", [1, 2, 3], 2.5), (2, "bob", [4], -2.5), (3, "cy", [], 0.5), (4, "dee", None, 7.5)]
PEOPLE_SCHEMA = "id BIGINT, name STRING, xs ARRAY<BIGINT>, d DOUBLE"

TEXTS = [
    '''SELECT "hi" AS s, `name` FROM people WHERE name = "ann"''',
    '''SELECT id, floor(d) AS f, ceil(d) AS c FROM people ORDER BY id''',
    '''SELECT substring('Spark SQL', 5, 1) AS a, substring('Spark SQL', -3, 2) AS b, substr('Spark SQL', -3) AS c''',
    '''SELECT id, x FROM people LATERAL VIEW explode(xs) v AS x ORDER BY id, x''',
    '''SELECT p.name, v.e FROM people p LATERAL VIEW OUTER explode(p.xs) v AS e WHERE p.id >= 2 ORDER BY p.name, v.e''',
    '''SELECT explode(xs) FROM people WHERE id = 1''',
    '''SELECT 7 DIV 2 AS d, -7 DIV 2 AS e, NULL <=> NULL AS n, 'abc' RLIKE 'b' AS r, !(1 = 2) AS t''',
    '''SELECT id FROM people ORDER BY id DESC''',
    '''SELECT name FROM people ORDER BY d DESC''',
    '''SELECT floor(CAST(2.5 AS DOUBLE)) AS f''',
    '''SELECT CAST(2.7 AS INT) AS a, CAST(-2.7 AS BIGINT) AS b, CAST(9007199254740993 AS BIGINT) AS c, 7 DIV 2 AS d''',
    '''SELECT 7 / 2 AS h, 71 / 3.0 AS q, 1.0 / 3 AS r, CAST(7 / 2 AS INT) AS i''',
    '''SELECT id, d / 2 AS h FROM people ORDER BY id''',
    '''SELECT sum(d) / count(*) AS m FROM people''',
    '''SELECT 2.5 / 0.5 AS q, 1.23 * 2 AS m, 10 % 3 AS r, -7 % 3 AS n, pmod(-7, 3) AS p''',
    '''SELECT 1 / 0 AS z''',
    '''SELECT 1.0 / 0 AS z''',
    '''SELECT EXTRACT(YEAR FROM DATE '2026-11-09') AS y, date_part('month', DATE '2026-11-09') AS m''',
    '''SELECT array_contains(array(1, 2), 2) AS a, size(array(1, 2, 3)) AS s''',
    '''SELECT concat_ws('-', 'a', 'b') AS c, format_string('%s x %d', 'tea', 3) AS f''',
    '''SELECT count(*) AS n FROM people WHERE `name` <> "bob"''',
    '''SELECT id, round(d, 0) AS r, round(2.5, 0) AS h FROM people ORDER BY id''',
    '''SELECT a / b AS q FROM VALUES (CAST(10.00 AS DECIMAL(10,2)), CAST(3.00 AS DECIMAL(10,2))) AS t(a, b)''',
    '''SELECT 7 / 2.0 AS q, CAST(7 AS DOUBLE) / 2 AS d''',
    '''SELECT price / 100 AS p, price / 3 AS t, 100 / price AS u FROM VALUES (CAST(1234.56 AS DECIMAL(10,2))) AS t(price)''',
    '''SELECT -7 / 2 AS a, -7.0 / 2 AS b, CAST(-7 AS INT) / 2 AS c''',
]

# Statements a Spark session and PySpark run before a query of their own (spark_sql('…') takes a query alone, so
# these are the session's and PySpark's only).
SESSION_ONLY = [
    (["CREATE TABLE ints (a INT)", "INSERT INTO ints VALUES (2.7), (-2.7), (2.5)"], "SELECT a FROM ints ORDER BY a"),
]

# Where Pondra answers otherwise than PySpark on purpose, and why: a list that may only shrink. A difference not here
# fails the run (exit 1); one here that no longer differs is said, to take it out.
KNOWN = {
    "SELECT a FROM ints ORDER BY a": "an INSERT's cast into an INT column rounds (2.7 is 3), as Pondra's table casts it, "
                                     "Postgres's assignment; Spark cuts it (2). The statement doesn't say the column's "
                                     "type, and nothing after the parse asks the language. CAST(x AS INT) cuts.",
}

NUM = re.compile(r"^[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?$")


# ------------------------------------------------------------------ Pondra

def post(port, sql, session=None):
    """(status, SQLSTATE header, body) for one POST /sql."""
    headers = {"x-pondra-session": session} if session else {}
    c = http.client.HTTPConnection("127.0.0.1", port, timeout=300)
    try:
        c.request("POST", "/sql", sql.encode(), headers)
        r = c.getresponse()
        return r.status, r.getheader("x-pondra-sqlstate") or "?", r.read().decode("utf-8", "replace")
    finally:
        c.close()


def first_line(text):
    lines = text.strip().splitlines()
    return lines[0] if lines else ""


def pondra(port, sql, session=None):
    """The answer: rows (list of dicts), or "error: <SQLSTATE> <first line of body>"."""
    status, state, body = post(port, sql, session)
    if status != 200:
        return f"error: {state} {first_line(body)}"
    data = json.loads(body)
    return data if isinstance(data, list) else []  # (a non-query statement answers {"command": ...})


def names_of(answer):
    """Column names in order of first appearance across the rows; None for an error (no names)."""
    if isinstance(answer, str):
        return None
    names = {}
    for row in answer:
        names.update(dict.fromkeys(row))
    return list(names)


def start_node(binary, port, tmp):
    probe = http.client.HTTPConnection("127.0.0.1", port, timeout=1)
    try:
        probe.request("GET", "/stats")
        raise RuntimeError(f"port {port} is already in use by another node")
    except OSError:
        pass  # free, as it should be
    finally:
        probe.close()
    lake = os.path.join(tmp, "lake")
    os.mkdir(lake)
    log_path = os.path.join(tmp, "node.log")
    with open(log_path, "w") as log:
        node = subprocess.Popen([binary, "serve", "--dir", lake, "--addr", f"127.0.0.1:{port}"],
                                stdout=subprocess.DEVNULL, stderr=log)
    deadline = time.time() + 120
    while time.time() < deadline:
        if node.poll() is not None:
            raise RuntimeError(f"node exited while starting ({binary}): {open(log_path).read()[-500:]}")
        try:
            c = http.client.HTTPConnection("127.0.0.1", port, timeout=2)
            c.request("GET", "/stats")
            if c.getresponse().status == 200:
                return node
        except OSError:
            time.sleep(0.05)
    node.kill()
    raise RuntimeError(f"node did not answer on port {port}: {open(log_path).read()[-500:]}")


# ------------------------------------------------------------------ PySpark

def start_spark(tmp):
    from pyspark.sql import SparkSession
    spark = (SparkSession.builder.master("local[1]").config("spark.sql.warehouse.dir", os.path.join(tmp, "warehouse"))
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    spark.createDataFrame(PEOPLE_ROWS, schema=PEOPLE_SCHEMA).createOrReplaceTempView("people")
    return spark


def pyspark(spark, sql):
    """The answer: rows (list of dicts), or "error: <exception class>: <first line>"."""
    try:
        df = spark.sql(sql)
        names = list(df.columns)
        return [dict(zip(names, tuple(r))) for r in df.collect()]
    except Exception as e:  # (analysis and runtime errors alike: both are PySpark's answer)
        return f"error: {type(e).__name__}: {first_line(str(e))}"


# ------------------------------------------------------------------ comparison

def norm(v):
    """A value as the gate compares it: Decimal and numeric strings as floats, dates as ISO strings."""
    if v is None or isinstance(v, bool) or isinstance(v, int):
        return v
    if isinstance(v, (float, decimal.Decimal)):
        return float(v)
    if isinstance(v, str):
        return float(v) if NUM.match(v) else v
    if isinstance(v, (list, tuple)):
        return [norm(x) for x in v]
    if isinstance(v, dict):
        return {k: norm(x) for k, x in v.items()}
    if isinstance(v, (datetime.date, datetime.datetime)):
        return v.isoformat()
    return str(v)


def same(a, b):
    if a is None or b is None:
        return a is None and b is None
    if isinstance(a, bool) or isinstance(b, bool):
        return isinstance(a, bool) and isinstance(b, bool) and a == b
    if isinstance(a, int) and isinstance(b, int):
        return a == b  # (exact: 9007199254740993 is not 9007199254740992)
    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
        if math.isnan(a) and math.isnan(b):
            return True
        return a == b or (math.isfinite(a) and math.isfinite(b) and abs(a - b) <= 1e-9 * max(abs(a), abs(b)))
    if isinstance(a, list) and isinstance(b, list):
        return len(a) == len(b) and all(same(x, y) for x, y in zip(a, b))
    if isinstance(a, dict) and isinstance(b, dict):
        return a.keys() == b.keys() and all(same(a[k], b[k]) for k in a)
    return a == b


def sort_key(v):
    """repr with floats at 9 significant digits, so equal rows sort alike on both sides."""
    if isinstance(v, float):
        return f"{v:.9g}"
    if isinstance(v, list):
        return [sort_key(x) for x in v]
    if isinstance(v, dict):
        return {k: sort_key(x) for k, x in v.items()}
    return v


def same_answer(a, b, ordered):
    """Two answers agree: both errors (any message), or the same rows (in order when the text has ORDER BY)."""
    if isinstance(a, str) or isinstance(b, str):
        return isinstance(a, str) and isinstance(b, str)
    keys = list(dict.fromkeys(k for r in a + b for k in r))  # (Pondra's JSON leaves a NULL's key out: it is None)
    a, b = [norm({k: r.get(k) for k in keys}) for r in a], [norm({k: r.get(k) for k in keys}) for r in b]
    if not ordered:
        a, b = sorted(a, key=lambda r: repr(sort_key(r))), sorted(b, key=lambda r: repr(sort_key(r)))
    return len(a) == len(b) and all(same(x, y) for x, y in zip(a, b))


def shown(answer):
    return answer if isinstance(answer, str) else [norm(r) for r in answer]


# ------------------------------------------------------------------ main

def parse_args():
    p = argparse.ArgumentParser(description="Run Spark SQL texts three ways and list where they differ.")
    p.add_argument("--bin", required=True, help="pondra binary")
    p.add_argument("--port", type=int, default=8791)
    p.add_argument("--set", dest="set_kind", choices=sorted(SETS), default="dialect")
    p.add_argument("--out", help="JSON lines file (default stdout)")
    p.add_argument("--only", help="run only texts containing this substring")
    return p.parse_args()


def run(args):
    texts = [(i + 1, t) for i, t in enumerate(TEXTS) if not args.only or args.only in t]
    out = open(args.out, "w") if args.out else sys.stdout
    tmp, node, spark = tempfile.mkdtemp(prefix="spark-ways-"), None, None
    try:
        node = start_node(args.bin, args.port, tmp)
        created = pondra(args.port, PEOPLE_PONDRA)
        if isinstance(created, str):
            raise RuntimeError(f"CREATE TABLE people failed in Pondra: {created}")
        set_sql = SETS[args.set_kind]
        set_answer = pondra(args.port, set_sql, SESSION)
        set_error = set_answer if isinstance(set_answer, str) else None
        spark = start_spark(tmp)

        records = []
        for n, text in texts:
            ordered = bool(re.search(r"\bORDER\s+BY\b", text, re.I))
            way1 = pondra(args.port, "SELECT * FROM spark_sql('" + text.replace("'", "''") + "')")
            way2 = (set_error or "error: SET failed") if set_error else pondra(args.port, text, SESSION)
            real = pyspark(spark, text)
            names = {"spark_sql": names_of(way1), "session": names_of(way2), "pyspark": names_of(real)}
            spark_sql_is_spark = same_answer(way1, real, ordered)
            session_is_spark = same_answer(way2, real, ordered)
            same_ways = spark_sql_is_spark and session_is_spark and same_answer(way1, way2, ordered)
            rec = {"n": n, "sql": text, "spark_sql": shown(way1), "session": shown(way2),
                   "pyspark": shown(real), "same_ways": same_ways, "spark_sql_is_spark": spark_sql_is_spark,
                   "session_is_spark": session_is_spark, "names": names,
                   "names_equal": len({tuple(v) for v in names.values() if v is not None}) <= 1}
            records.append(rec)
            out.write(json.dumps(rec) + "\n")
            out.flush()

        for setup, text in SESSION_ONLY:
            if args.only and args.only not in text:
                continue
            ordered = bool(re.search(r"\bORDER\s+BY\b", text, re.I))
            done = [pondra(args.port, s, SESSION) for s in setup] + [pyspark(spark, s) for s in setup]
            way2, real = pondra(args.port, text, SESSION), pyspark(spark, text)
            failed = [d for d in done if isinstance(d, str)]
            session_is_spark = same_answer(way2, real, ordered) and not failed
            rec = {"n": len(records) + 1, "sql": text, "setup": setup, "spark_sql": None, "session": shown(way2),
                   "pyspark": shown(real), "same_ways": session_is_spark, "spark_sql_is_spark": None,
                   "session_is_spark": session_is_spark, "names": {"session": names_of(way2), "pyspark": names_of(real)},
                   "names_equal": names_of(way2) == names_of(real), **({"setup_failed": failed} if failed else {})}
            records.append(rec)
            out.write(json.dumps(rec) + "\n")
            out.flush()

        for r in records:
            r["known"] = KNOWN.get(r["sql"])
        differ = [r["n"] for r in records if not r["same_ways"]]
        unknown = [r["n"] for r in records if not r["same_ways"] and not r["known"]]
        gone = [r["sql"] for r in records if r["same_ways"] and r["known"]]
        summary = {"summary": True, "texts": len(records), "same_ways": len(records) - len(differ),
                   "spark_sql_is_spark": sum(bool(r["spark_sql_is_spark"]) for r in records if r["spark_sql"] is not None),
                   "session_is_spark": sum(r["session_is_spark"] for r in records if r["spark_sql"] is not None),
                   "session_only_is_spark": sum(r["session_is_spark"] for r in records if r["spark_sql"] is None),
                   "known": sum(1 for r in records if not r["same_ways"] and r["known"]),
                   "unknown": unknown, "known_gone": gone,
                   "names_differ": [r["n"] for r in records if not r["names_equal"]],  # (among answers that are rows)
                   "set": set_sql, "set_error": set_error, "differ": differ}
        out.write(json.dumps(summary) + "\n")
        return 1 if unknown else 0
    finally:
        if spark is not None:
            spark.stop()
        if node is not None and node.poll() is None:
            node.kill()
            node.wait()
        shutil.rmtree(tmp, ignore_errors=True)
        if args.out:
            out.close()


def main():
    try:
        sys.exit(run(parse_args()))
    except Exception as e:
        print(f"spark_ways: {type(e).__name__}: {e}", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
