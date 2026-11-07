#!/usr/bin/env python3
"""The agent benchmark: can an agent rely on Pondra? (redesign-wip/agent-benchmark-plan.md; the 1.0 scorecard's agent row)

  agent_bench.py gold [--bin PATH] [--port N] [--pg-bin DIR] [--only a01,a02]

An agent is given a question in plain words and a few tools, and its answer is the rows of the last query it names. The
same tasks, data, model and tools run on Pondra, DuckDB and Postgres, so the numbers say what Pondra adds.

This piece has no model in it. `gold` loads the same data into each engine and runs every task's gold SQL there: each
engine's rows must be DuckDB's, as the scorer reads them. That proves the tasks and the scorer, and doubles as a check
that Pondra answers like the other two. The agent loop, its costs and the replays come in the next pieces.

- **The data** (`shop`) is made here from a seed: a shop's customers, products, stores, orders, order lines and returns,
  with NULLs, web orders without a store, customers who never ordered and products never sold. It is loaded with plain
  multi-row INSERTs, which every engine takes as they are.
- **A task** (tools/agent_bench/set_a.json) is a question, the facts it may lean on (BIRD's "evidence"), its gold SQL
  (`sql`, or one per engine where they spell it differently), whether its rows' order counts, and the decimals its
  numbers are compared at.
- **The scorer** (`same`) compares rows as values, never column names, as BIRD's execution accuracy and Spider 2.0's do.
  An answer may hold more columns than the gold one, so long as the gold's columns are among them. Numbers compare as
  numbers at the task's decimals (6 significant digits if it gives none), and a timestamp at midnight compares as its
  date.

Postgres is started here as a throwaway cluster when its binaries are found (--pg-bin, PG_BIN, pg_ctl on PATH, or
/usr/lib/postgresql/*/bin); without them it is reported, not checked. Pondra is started from --bin on a fresh lake, or
reached on --port's Postgres door. One JSON line goes to stdout.
"""
import argparse
import datetime
import decimal
import glob
import itertools
import json
import os
import random
import shutil
import socket
import subprocess
import sys
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
TASKS = os.path.join(HERE, "agent_bench")
ENGINES = ("duckdb", "postgres", "pondra")

# ---------------------------------------------------------------- the data

COLUMNS = {
    "customers": [("customer_id", "INTEGER"), ("name", "VARCHAR"), ("email", "VARCHAR"), ("country", "VARCHAR"), ("segment", "VARCHAR"), ("signed_up", "DATE")],
    "products": [("product_id", "INTEGER"), ("name", "VARCHAR"), ("category", "VARCHAR"), ("list_price", "DECIMAL(10,2)"), ("discontinued", "BOOLEAN")],
    "stores": [("store_id", "INTEGER"), ("city", "VARCHAR"), ("country", "VARCHAR"), ("opened", "DATE")],
    "orders": [("order_id", "INTEGER"), ("customer_id", "INTEGER"), ("store_id", "INTEGER"), ("ordered_at", "TIMESTAMP"), ("status", "VARCHAR")],
    "order_items": [("order_id", "INTEGER"), ("line", "INTEGER"), ("product_id", "INTEGER"), ("quantity", "INTEGER"), ("unit_price", "DECIMAL(10,2)"), ("discount", "DECIMAL(4,2)")],
    "returns": [("order_id", "INTEGER"), ("line", "INTEGER"), ("returned_on", "DATE"), ("reason", "VARCHAR")],
}


def shop(seed=7):
    """The shop's rows by table, the same for a seed on every machine."""
    r = random.Random(seed)
    day = lambda lo, hi: lo + datetime.timedelta(days=r.randrange((hi - lo).days + 1))
    countries = ["US", "DE", "FR", "UZ", "JP", "BR"]
    customers = []
    for i in range(1, 301):
        country = r.choices(countries, weights=[34, 20, 14, 12, 11, 9])[0]
        email = None if r.random() < 0.12 else f"c{i}@example.com"
        customers.append((i, f"Customer {i:03d}", email, country, "business" if r.random() < 0.25 else "consumer", day(datetime.date(2023, 1, 1), datetime.date(2025, 6, 30))))
    categories = ["books", "games", "garden", "kitchen", "music", "toys"]
    products, prices = [], r.sample(range(300, 25000), 60)
    for i in range(1, 61):
        category = categories[(i - 1) % 6] if i <= 54 else r.choice(categories)
        products.append((i, f"{category.title()} item {i:02d}", category, decimal.Decimal(prices[i - 1]) / 100, r.random() < 0.1))
    stores = [(1, "Austin", "US", datetime.date(2019, 4, 1)), (2, "Boston", "US", datetime.date(2021, 9, 15)), (3, "Berlin", "DE", datetime.date(2020, 2, 3)),
              (4, "Lyon", "FR", datetime.date(2022, 6, 20)), (5, "Tashkent", "UZ", datetime.date(2023, 3, 8)), (6, "Osaka", "JP", datetime.date(2021, 1, 11)),
              (7, "Recife", "BR", datetime.date(2024, 5, 30)), (8, "Hamburg", "DE", datetime.date(2024, 10, 1))]
    buyers = [c[0] for c in customers if c[0] % 13 != 0]  # (every 13th customer never orders)
    weight = [5 if c % 17 == 0 else 1 for c in buyers]  # (and a few order often)
    popular = [p[0] for p in products if p[0] <= 57]  # (products 58 to 60 are never sold)
    pweight = [3 if p % 7 == 0 else 1 for p in popular]
    start, seconds = datetime.datetime(2025, 1, 1), 365 * 24 * 3600
    orders, items, returns = [], [], []
    for o in range(1, 4001):
        status = r.choices(["delivered", "shipped", "placed", "cancelled"], weights=[70, 15, 9, 6])[0]
        store = None if r.random() < 0.4 else r.randrange(1, 9)
        orders.append((o, r.choices(buyers, weights=weight)[0], store, start + datetime.timedelta(seconds=r.randrange(seconds)), status))
        for line in range(1, r.choices([1, 2, 3, 4, 5], weights=[30, 30, 20, 12, 8])[0] + 1):
            p = r.choices(popular, weights=pweight)[0]
            price = products[p - 1][3]
            unit = price if r.random() < 0.85 else (price * decimal.Decimal("0.9")).quantize(decimal.Decimal("0.01"))
            discount = r.choices([decimal.Decimal("0.00"), decimal.Decimal("0.05"), decimal.Decimal("0.10"), decimal.Decimal("0.20")], weights=[80, 8, 8, 4])[0]
            items.append((o, line, p, r.choices([1, 2, 3, 4, 10], weights=[50, 25, 12, 10, 3])[0], unit, discount))
            if status == "delivered" and r.random() < 0.06:
                reason = None if r.random() < 0.1 else r.choice(["damaged", "wrong item", "changed mind", "late"])
                returns.append((o, line, orders[-1][3].date() + datetime.timedelta(days=r.randrange(3, 31)), reason))
    return {"customers": customers, "products": products, "stores": stores, "orders": orders, "order_items": items, "returns": returns}


def literal(v):
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, datetime.datetime):
        return f"TIMESTAMP '{v.isoformat(sep=' ')}'"
    if isinstance(v, datetime.date):
        return f"DATE '{v.isoformat()}'"
    if isinstance(v, str):
        return "'" + v.replace("'", "''") + "'"
    return str(v)


def statements(data, batch=500):
    """The data as CREATE TABLE and multi-row INSERT statements every engine takes."""
    for table, cols in COLUMNS.items():
        yield f"CREATE TABLE {table} ({', '.join(f'{c} {t}' for c, t in cols)})"
        rows = data[table]
        for lo in range(0, len(rows), batch):
            yield f"INSERT INTO {table} VALUES " + ", ".join("(" + ", ".join(literal(v) for v in row) + ")" for row in rows[lo:lo + batch])


# ---------------------------------------------------------------- the scorer

def value(v, places):
    """A cell as the scorer compares it: numbers as floats at `places` decimals (6 significant digits if None), a
    timestamp at midnight as its date, dates and times as ISO text."""
    if isinstance(v, bool) or v is None:
        return v
    if isinstance(v, (int, float, decimal.Decimal)):
        f = float(v)
        if places is not None:
            return round(f, places) + 0.0
        return float(f"{f:.6g}") + 0.0
    if isinstance(v, datetime.datetime):
        return v.date().isoformat() if v.time() == datetime.time(0) and v.tzinfo is None else v.isoformat(sep=" ")
    if isinstance(v, datetime.date):
        return v.isoformat()
    return v


def same(gold, got, ordered=False, places=None, extra=True):
    """Whether `got`'s rows are `gold`'s, by value: some choice of `got`'s columns, one for each of gold's in gold's order,
    gives gold's rows (in gold's order when `ordered`, else as a multiset). `extra` lets `got` hold more columns."""
    g = [tuple(value(v, places) for v in row) for row in gold]
    a = [tuple(value(v, places) for v in row) for row in got]
    if len(g) != len(a):
        return False
    if not g:
        return True
    width, have = len(g[0]), len(a[0])
    if have < width or (not extra and have != width):
        return False
    column = lambda rows, i: sorted((repr(r[i]) for r in rows))
    candidates = [[j for j in range(have) if column(a, j) == column(g, i)] for i in range(width)]
    key = (lambda rows: rows) if ordered else (lambda rows: sorted(rows, key=repr))
    want = key(g)
    for pick in itertools.product(*candidates):
        if len(set(pick)) == width and key([tuple(r[j] for j in pick) for r in a]) == want:
            return True
    return False


SCORER = [  # (gold, got, ordered, places, extra) -> expected: the scorer's own cases, checked before every run
    (([(1, "a")], [("a", 1)], False, None, True), True),  # (columns in another order)
    (([(1,)], [(1, "x")], False, None, True), True),  # (an extra column)
    (([(1,)], [(1, "x")], False, None, False), False),
    (([(1,), (2,)], [(2,), (1,)], True, None, True), False),  # (order counts when asked)
    (([(1,), (2,)], [(2,), (1,)], False, None, True), True),
    (([(decimal.Decimal("12.344"),)], [(12.34,)], False, 2, True), True),  # (at the task's decimals)
    (([(decimal.Decimal("12.346"),)], [(12.34,)], False, 2, True), False),
    (([(1 / 3,)], [(0.333333333,)], False, None, True), True),
    (([(datetime.datetime(2025, 3, 1),)], [(datetime.date(2025, 3, 1),)], False, None, True), True),
    (([(None, 1)], [(None, 1)], False, None, True), True),
    (([(1, 1), (2, 2)], [(1, 2), (2, 1)], False, None, True), False),  # (columns matched one by one, never row by row)
    (([], [], False, None, True), True),
]


def scorer_checks():
    wrong = [args for args, want in SCORER if same(*args) != want]
    if wrong:
        raise SystemExit(f"the scorer gets its own cases wrong: {wrong}")


# ---------------------------------------------------------------- the engines

def free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


class DuckDB:
    name = "duckdb"

    def __init__(self):
        import duckdb
        self.con = duckdb.connect()

    def run(self, sql):
        cur = self.con.execute(sql)
        return [d[0] for d in cur.description] if cur.description else [], cur.fetchall() if cur.description else []

    def stop(self):
        self.con.close()


class Over:
    """An engine reached on a Postgres door (Postgres itself, or Pondra's)."""

    def connect(self, **kw):
        import psycopg
        deadline = time.time() + 60
        while True:
            try:
                self.con = psycopg.connect(host="127.0.0.1", autocommit=True, connect_timeout=5, **kw)
                return
            except psycopg.OperationalError:
                if time.time() > deadline:
                    raise
                time.sleep(0.2)

    def run(self, sql):
        cur = self.con.execute(sql)
        return ([d.name for d in cur.description], cur.fetchall()) if cur.description else ([], [])


def pg_bin(given):
    found = [given, os.environ.get("PG_BIN"), os.path.dirname(shutil.which("pg_ctl") or "") or None] + sorted(glob.glob("/usr/lib/postgresql/*/bin"), reverse=True)
    return next((c for c in found if c and all(os.path.isfile(os.path.join(c, b)) for b in ("initdb", "pg_ctl", "postgres"))), None)


class Postgres(Over):
    """A throwaway cluster: trust on 127.0.0.1, as the `postgres` user when this runs as root (Postgres refuses root)."""
    name = "postgres"

    def __init__(self, bin_dir):
        self.bin, self.work, self.port = bin_dir, tempfile.mkdtemp(prefix="agent-bench-pg-"), free_port()
        user = "postgres" if os.geteuid() == 0 else None
        if user:
            shutil.chown(self.work, user)
        self.as_ = {"user": user} if user else {}
        data = os.path.join(self.work, "data")
        self.sh("initdb", "-D", data, "-U", "bench", "--auth=trust", "-E", "UTF8", "--no-locale")
        self.sh("pg_ctl", "-D", data, "-l", os.path.join(self.work, "log"), "-w", "-o", f"-p {self.port} -k {self.work} -c listen_addresses=127.0.0.1", "start")
        self.data = data
        self.connect(port=self.port, user="bench", dbname="postgres")

    def sh(self, tool, *args):
        r = subprocess.run([os.path.join(self.bin, tool), *args], capture_output=True, text=True, **self.as_)
        if r.returncode:
            raise RuntimeError(f"{tool}: {(r.stderr or r.stdout)[-400:]}")

    def stop(self):
        self.con.close()
        self.sh("pg_ctl", "-D", self.data, "-m", "fast", "-w", "stop")
        shutil.rmtree(self.work, ignore_errors=True)


class Pondra(Over):
    """A node on a fresh lake from --bin, or a running one's Postgres door (--port)."""
    name = "pondra"

    def __init__(self, bin_path=None, port=None):
        self.proc, self.work = None, None
        if port is None:
            self.work, port, http = tempfile.mkdtemp(prefix="agent-bench-lake-"), free_port(), free_port()
            self.log = open(os.path.join(self.work, "stderr"), "wb")
            self.proc = subprocess.Popen([bin_path, "serve", "--dir", os.path.join(self.work, "lake"), "--addr", f"127.0.0.1:{http}", f"--pg=127.0.0.1:{port}"],
                                         stdout=subprocess.DEVNULL, stderr=self.log)
        self.connect(port=port, user="pondra", dbname="pondra")

    def stop(self):
        self.con.close()
        if self.proc:
            self.proc.kill()
            self.proc.wait()
            self.log.close()
            shutil.rmtree(self.work, ignore_errors=True)


# ---------------------------------------------------------------- the tasks

def tasks(name="a"):
    with open(os.path.join(TASKS, f"set_{name}.json")) as f:
        return json.load(f)


def gold_sql(task, engine):
    return task.get(engine) or task["sql"]


def gold(a):
    """Every task's gold SQL on each engine, scored against DuckDB's rows: the tasks and the scorer proved, and Pondra
    checked against two others on a shop's questions."""
    scorer_checks()
    todo = [t for t in tasks() if not a.only or t["id"] in a.only.split(",")]
    engines, out = [DuckDB()], {"set": "a", "tasks": len(todo), "engines": {}, "failed": {}, "ok": True}
    pg = pg_bin(a.pg_bin)
    if pg:
        engines.append(Postgres(pg))
    else:
        out["engines"]["postgres"] = "not here (no initdb and pg_ctl found)"
    if a.bin or a.port:
        engines.append(Pondra(a.bin, a.port))
    else:
        out["engines"]["pondra"] = "not here (give --bin or --port)"
    try:
        data = shop()
        for e in engines:
            t0 = time.perf_counter()
            for s in statements(data):
                e.run(s)
            out["engines"][e.name] = {"loaded_s": round(time.perf_counter() - t0, 2), "passed": 0}
        for t in todo:
            answers = {}
            for e in engines:
                try:
                    answers[e.name] = e.run(gold_sql(t, e.name))
                except Exception as err:
                    answers[e.name] = err
            want = answers["duckdb"]
            if isinstance(want, Exception):
                out["failed"].setdefault(t["id"], {})["duckdb"] = str(want)[:300]
                continue
            out["engines"]["duckdb"]["passed"] += 1
            for e in engines[1:]:
                got = answers[e.name]
                if not isinstance(got, Exception) and same(want[1], got[1], t.get("ordered", False), t.get("places"), extra=False):
                    out["engines"][e.name]["passed"] += 1
                else:
                    out["failed"].setdefault(t["id"], {})[e.name] = str(got)[:300] if isinstance(got, Exception) else {"want": [list(map(str, r)) for r in want[1][:3]], "got": [list(map(str, r)) for r in got[1][:3]], "rows": [len(want[1]), len(got[1])]}
    finally:
        for e in engines:
            e.stop()
    out["ok"] = not out["failed"]
    print(json.dumps(out, default=str))
    return 0 if out["ok"] else 1


def main():
    p = argparse.ArgumentParser()
    p.add_argument("mode", choices=["gold"])
    p.add_argument("--bin", help="a pondra binary to start on a fresh lake")
    p.add_argument("--port", type=int, help="or a running node's Postgres door")
    p.add_argument("--pg-bin", help="Postgres's initdb and pg_ctl folder")
    p.add_argument("--only", help="task ids, comma-separated")
    a = p.parse_args()
    return gold(a)


if __name__ == "__main__":
    sys.exit(main())
