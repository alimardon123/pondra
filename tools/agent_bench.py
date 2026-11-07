#!/usr/bin/env python3
"""The agent benchmark: can an agent rely on Pondra? (redesign-wip/agent-benchmark-plan.md; the 1.0 scorecard's agent row)

  agent_bench.py check  [--bin PATH | --port N] [--pg-bin DIR] [--only a01,a02]    # CI's, with no model (harness.py agents)
  agent_bench.py run    [--set a|b] [--bin PATH | --port N] [--model M] [--effort E] [--runs 3] [--jobs 6] [--out DIR]
  agent_bench.py replay FILE [--bin PATH | --port N]
  agent_bench.py serve  [--set a|b] [--bin PATH] [--model M] [--engines ...] [--out DIR]   # helper agents, no API key
  agent_bench.py --at PORT begin LANE TASK [RUN]  |  --at PORT tool TRY TOOL [VALUE]  |  --at PORT finish TRY [--tokens N
                        --tool-uses N --seconds S]  |  --at PORT stop      # (options first: Python 3.11's argparse)

An agent is given a question in plain words and a few tools, and its answer is the rows of the query it gives. The same
tasks, data, model and tools run on Pondra, DuckDB and Postgres, so the numbers say what Pondra adds.

- **The data** (`shop`) is made here from a seed: a shop's customers, products, stores, orders, order lines and returns,
  with NULLs, web orders without a store, customers who never ordered and products never sold. It is loaded with plain
  multi-row INSERTs, which every engine takes as they are.
- **A task** (tools/agent_bench/set_a.json) is a question, the facts it may lean on (BIRD's "evidence"), its gold SQL
  (`sql`, or one per engine where they spell it differently), whether its rows' order counts, and the decimals its
  numbers are compared at.
- **A job** (tools/agent_bench/set_b.json) changes data: its setup (a staging table, or the harm it asks to undo), its
  gold statements, what the harness does after the agent is done (`after`: new orders, say, that a view must show),
  and the checks whose answers are its end state. It wants the gold's end state, or with `want: before` the shop as it
  was before the setup. `only` names the engines that can do it at all (an UNDROP: Pondra), and those count apart.
  Each try at a job gets a copy of the shop of its own: a fresh DuckDB, a Postgres database made from a template, a
  Pondra node on a copy of a lake's folder.
- **The scorer** (`same`) compares rows as values, never column names, as BIRD's execution accuracy and Spider 2.0's do.
  An answer may hold more columns than the gold one, so long as the gold's columns are among them. Numbers compare as
  numbers at the task's decimals (6 significant digits if it gives none), and a timestamp at midnight compares as its
  date.
- **The agent** (`attempt`) is the Messages API's tool loop with four plain tools on every engine: `list_tables`,
  `describe`, `sql` (read-only for a question, its first 50 rows, an error as its message, 30 s a query) and `answer`
  (for a job, `sql` writes too, and `done` ends it). The system prompt is the same for every engine but its name; 15
  turns at most. A try's record holds its turns, its steps (tool calls), its tokens as the API counted them, its
  seconds, its dollars at list prices, how it ended, the statements it ran, and the whole conversation. A refusal
  ends the try as not passed: no fallback model answers in its place.
- **pondra+mcp** is Pondra a second time, with its own MCP door's tools in place of the plain ones (the ones it lists,
  only the read-only ones for a question, called through POST /mcp, its notes for agents added to the prompt), to
  show what the door adds.

`check` is what CI runs, with no key: every question's gold SQL on each engine, scored against DuckDB's rows (the tasks,
the scorer, and Pondra against two other engines); every job's gold on a copy on each engine, its end state checked;
the loop itself with a scripted model; and the answers (a job's: its statements) saved in
tools/agent_bench/replay_*.jsonl run again, so an engine change that breaks an answer an agent once gave shows. `run`
spends API dollars (ANTHROPIC_API_KEY; .github/workflows/agent-bench.yml): it writes runs.jsonl, every try's
transcript, report.md and replay.jsonl (the passed answers, to keep as a replay file) to --out. `replay` runs a saved
file's answers again.

`serve` is the same run with no API key, its model a helper agent on a subscription (a Claude Code session's): it keeps
the engines up and answers on a port of 127.0.0.1. `begin` opens a try and gives the words to brief a helper with (the
API run's system words, task and tools, as one command's); the helper calls `tool`, each call the API loop's own
(`step`), until `answer` or `done`; `finish` gives the try what its runner counted (the helper's whole tokens, all its
tool calls, its seconds), and `stop` writes the run's files and ends it. The API's turns, token meters and dollars
aren't there, and a try whose helper did anything but call its tools isn't passed. Postgres is a throwaway cluster, started when its binaries are found (--pg-bin, PG_BIN, pg_ctl on
PATH, or /usr/lib/postgresql/*/bin); Pondra is started from --bin on a fresh lake, or reached on --port's Postgres door.
Each mode prints one JSON line.
"""
import argparse
import concurrent.futures
import datetime
import decimal
import glob
import itertools
import json
import os
import random
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
import types
import urllib.request

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
    name, label = "duckdb", "DuckDB"

    def __init__(self, con=None):
        if con is None:
            import duckdb
            con = duckdb.connect()
        self.con, self.file = con, None

    def run(self, sql, limit=None):
        cur = self.con.execute(sql)
        if not cur.description:
            return [], []
        return [d[0] for d in cur.description], cur.fetchall() if limit is None else cur.fetchmany(limit)

    def session(self):
        """A connection of its own to the same database, for one worker."""
        return DuckDB(self.con.cursor())

    def copy(self, name):
        """A database of its own for one try at a job: a fresh one, the shop copied in from a file loaded once."""
        with self.lock:
            if self.file is None:
                import duckdb
                file = os.path.join(tempfile.mkdtemp(prefix="agent-bench-duck-"), "shop.duckdb")
                b = duckdb.connect(file)
                for s in statements(self.rows):
                    b.execute(s)
                b.close()
                self.file = file
        c = DuckDB()
        for s in (f"ATTACH '{self.file}' AS shop (READ_ONLY)", "COPY FROM DATABASE shop TO memory", "DETACH shop"):
            c.run(s)
        return c

    def uncopy(self, c, name):
        c.stop()

    def cancel(self):
        self.con.interrupt()

    def stop(self):
        self.con.close()
        if self.file:
            shutil.rmtree(os.path.dirname(self.file), ignore_errors=True)


class Over:
    """An engine reached on a Postgres door (Postgres itself, or Pondra's)."""

    def connect(self, **kw):
        import psycopg
        self.kw = kw
        deadline = time.time() + 60
        while True:
            try:
                self.con = psycopg.connect(host="127.0.0.1", autocommit=True, connect_timeout=5, **kw)
                return
            except psycopg.OperationalError:
                if time.time() > deadline:
                    raise
                time.sleep(0.2)

    def run(self, sql, limit=None):
        cur = self.con.execute(sql)
        if not cur.description:
            return [], []
        return [d.name for d in cur.description], cur.fetchall() if limit is None else cur.fetchmany(limit)

    def session(self, dbname=None):
        """A connection of its own to the same door, for one worker (to another database on it, if named)."""
        s = Over()
        s.name, s.label = self.name, self.label
        s.connect(**{**self.kw, **({"dbname": dbname} if dbname else {})})
        return s

    def uncopy(self, c, name):
        c.stop()
        with self.lock:
            self.run(f"DROP DATABASE {name}")

    def cancel(self):
        self.con.cancel_safe()

    def stop(self):
        self.con.close()


def pg_bin(given):
    found = [given, os.environ.get("PG_BIN"), os.path.dirname(shutil.which("pg_ctl") or "") or None] + sorted(glob.glob("/usr/lib/postgresql/*/bin"), reverse=True)
    return next((c for c in found if c and all(os.path.isfile(os.path.join(c, b)) for b in ("initdb", "pg_ctl", "postgres"))), None)


class Postgres(Over):
    """A throwaway cluster: trust on 127.0.0.1, as the `postgres` user when this runs as root (Postgres refuses root)."""
    name, label = "postgres", "PostgreSQL"

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

    def copy(self, name):
        """A database of its own for one try at a job: CREATE DATABASE … TEMPLATE of the shop, loaded once (the
        connection that holds the gold's data is on `postgres`, which can't be a template while it's open)."""
        with self.lock:
            if not getattr(self, "template", False):
                self.run("CREATE DATABASE shop")
                c = self.session("shop")
                for s in statements(self.rows):
                    c.run(s)
                c.stop()
                self.template = True
            self.run(f"CREATE DATABASE {name} TEMPLATE shop")
        return self.session(name)

    def sh(self, tool, *args):
        r = subprocess.run([os.path.join(self.bin, tool), *args], capture_output=True, text=True, **self.as_)
        if r.returncode:
            raise RuntimeError(f"{tool}: {(r.stderr or r.stdout)[-400:]}")

    def stop(self):
        self.con.close()
        self.sh("pg_ctl", "-D", self.data, "-m", "fast", "-w", "stop")
        shutil.rmtree(self.work, ignore_errors=True)


class Pondra(Over):
    """A node on a fresh lake from --bin (or on a copy of a lake's folder), or a running one's Postgres door (--port)."""
    name, label = "pondra", "Pondra"

    def __init__(self, bin_path=None, port=None, lake=None):
        self.bin_path, self.proc, self.work, self.mcp, self.base = bin_path, None, None, None, None
        if port is None:
            self.work, port, http = tempfile.mkdtemp(prefix="agent-bench-lake-"), free_port(), free_port()
            if lake:
                shutil.copytree(lake, os.path.join(self.work, "lake"))
            self.log = open(os.path.join(self.work, "stderr"), "wb")
            self.proc = subprocess.Popen([bin_path, "serve", "--dir", os.path.join(self.work, "lake"), "--addr", f"127.0.0.1:{http}", f"--pg=127.0.0.1:{port}"],
                                         stdout=subprocess.DEVNULL, stderr=self.log)
            self.mcp = f"http://127.0.0.1:{http}/mcp"
        self.connect(port=port, user="pondra", dbname="pondra")
        self.lake = self.run("SELECT current_database()")[1][0][0]

    def copy(self, name):
        """A database of its own for one try at a job: a node of its own (0.2 s to start), with its own Postgres and
        MCP doors, on a copy of a lake folder that a node loaded with the shop once and stopped. Not a branch (CREATE
        DATABASE … CLONE) yet: the Postgres door doesn't take the database a connection names, and a branch's DROP
        TABLE doesn't find its tables (both sent to their lanes, 11-07); step 6 moves an agent's tries onto branches."""
        if self.bin_path is None:
            raise SystemExit("jobs on Pondra need --bin: each try is a node of its own")
        with self.lock:
            if self.base is None:
                b = Pondra(self.bin_path)
                for s in statements(self.rows):
                    b.run(s)
                b.halt()
                self.base = b.work
        return Pondra(self.bin_path, lake=os.path.join(self.base, "lake"))

    def uncopy(self, c, name):
        c.stop()

    def halt(self):
        """The node stopped as it asks to be (SIGTERM), its folder kept."""
        self.con.close()
        self.proc.terminate()
        self.proc.wait()
        self.log.close()

    def stop(self):
        self.con.close()
        if self.proc:
            self.proc.kill()
            self.proc.wait()
            self.log.close()
            shutil.rmtree(self.work, ignore_errors=True)
        if self.base:
            shutil.rmtree(self.base, ignore_errors=True)


# ---------------------------------------------------------------- the tasks

def tasks(name="a", only=None):
    with open(os.path.join(TASKS, f"set_{name}.json")) as f:
        return [t for t in json.load(f) if not only or t["id"] in only.split(",")]


def gold_sql(task, engine):
    return task.get(engine) or task["sql"]


def runs_on(task, engine):
    return engine in task.get("only", ENGINES)


def end_state(task, c, statements_=None):
    """A job on a copy: its setup, then the given statements (its gold's on that engine, by default), then what the
    harness does after the agent is done (`after`), and the answers to its checks."""
    for s in task.get("setup", []) + (gold_sql(task, c.name) if statements_ is None else statements_) + task.get("after", []):
        c.run(s)
    return [c.run(q)[1] for q in task["checks"]]


def wanted(task, oracle):
    """The end state a job must leave, as DuckDB's answers to its checks: after its gold, or, for a job that undoes its
    setup's harm (`want: before`), as the shop was before it."""
    c = oracle.copy("want")
    try:
        if task.get("want") == "before":
            return [c.run(q)[1] for q in task["checks"]]
        return end_state(task, c)
    finally:
        oracle.uncopy(c, "want")


def gold_jobs(engines, todo, out):
    """Each job's gold on each engine it runs on, from a copy of the shop, its end state checked against the wanted one."""
    for e in engines:
        out["engines"][e.name]["jobs"] = 0
    for t in todo:
        want = wanted(t, engines[0])
        for e in engines:
            if not runs_on(t, e.name):
                continue
            c = e.copy(f"gold_{t['id']}")
            try:
                got = end_state(t, c)
                ok = all(same(w, g, False, t.get("places"), extra=False) for w, g in zip(want, got))
                why = None if ok else {"want": [list(map(str, r)) for r in want[-1][:3]], "got": [list(map(str, r)) for r in got[-1][:3]]}
            except Exception as err:
                ok, why = False, str(err)[:300]
            finally:
                e.uncopy(c, f"gold_{t['id']}")
            if ok:
                out["engines"][e.name]["jobs"] += 1
            else:
                out["failed"].setdefault(t["id"], {})[e.name] = why


def up(a, names=ENGINES):
    """DuckDB, and those of `names` that are here, each loaded with the shop; and a note for each one that isn't."""
    engines, notes = [DuckDB()], {}
    try:
        if "postgres" in names:
            pg = pg_bin(a.pg_bin)
            if pg:
                engines.append(Postgres(pg))
            else:
                notes["postgres"] = "not here (no initdb and pg_ctl found)"
        if "pondra" in names:
            if a.bin or a.port:
                engines.append(Pondra(a.bin, a.port))
            else:
                notes["pondra"] = "not here (give --bin or --port)"
        data = shop()
        for e in engines:
            e.rows, e.lock = data, threading.Lock()
            t0 = time.perf_counter()
            for s in statements(data):
                e.run(s)
            notes[e.name] = {"loaded_s": round(time.perf_counter() - t0, 2)}
    except BaseException:
        down(engines)
        raise
    return engines, notes


def down(engines):
    """Every engine stopped, even when one fails to stop (the error after the rest are down)."""
    failed = None
    for e in engines:
        try:
            e.stop()
        except Exception as err:
            failed = failed or err
    if failed:
        raise failed


def gold(engines, todo, out):
    """Every task's gold SQL on each engine, scored against DuckDB's rows: the tasks and the scorer proved, and Pondra
    checked against two others on a shop's questions."""
    for e in engines:
        out["engines"][e.name]["passed"] = 0
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


# ---------------------------------------------------------------- the agent

# Dollars per million tokens: input, output, a 5-minute cache write, a cache read (list prices, 11-07). A model not
# here is run, and its dollars are left out.
PRICES = {
    "claude-opus-5-5": (4.0, 20.0, 5.0, 0.20),
    "claude-sonnet-5-5": (2.0, 10.0, 2.50, 0.20),
    "claude-haiku-5-5": (0.10, 0.50, 0.125, 0.01),
}
TURNS, SHOWN, QUERY_S = 15, 50, 30  # the turn cap, the rows a query shows the agent, and each query's seconds
READS = {"select", "with", "values", "table", "explain", "show", "describe", "from"}

# The same words for every engine, which they only name: what differs between runs is the engine.
SYSTEM = ("You answer questions about a shop's data, which is in a {label} database. Use the tools to look at the "
          "tables and to run SQL. When you know the answer, call `answer` with the one SQL query whose result answers the "
          "question. Its rows are what is checked, so it returns the rows the question asks for, with the values it asks "
          "for among its columns. Give the answer as that query, never as text.")
JOB = ("You do jobs on a shop's data, which is in a {label} database. Use the tools to look at the tables and to run "
       "SQL, including statements that change data and tables. When the job is done, call `done`.")


def tool(name, description, **args):
    return {"name": name, "description": description, "strict": True,
            "input_schema": {"type": "object", "properties": {k: {"type": "string", "description": v} for k, v in args.items()},
                             "required": list(args), "additionalProperties": False}}


PLAIN = [
    tool("list_tables", "The tables in the database."),
    tool("describe", "A table's columns and their types.", table="the table's name"),
    tool("sql", f"Run one read-only SQL query (SELECT, WITH, VALUES, TABLE, EXPLAIN, SHOW or DESCRIBE) and see its column "
                f"names and its first {SHOWN} rows. An error comes back as its message.", query="the query"),
]
WRITES = PLAIN[:2] + [tool("sql", f"Run one SQL statement: a query, or one that changes data or tables. A query shows its "
                                  f"column names and its first {SHOWN} rows. An error comes back as its message.", query="the statement")]
ANSWER = tool("answer", "Give the final answer: the one SQL query whose rows answer the question. This ends the task.", query="the query")
DONE = tool("done", "Say that the job is done. This ends the task.", summary="what you did, in a sentence")


def first_word(sql):
    rest = re.sub(r"^(\s+|--[^\n]*(\n|$)|/\*.*?\*/|\()*", "", sql, flags=re.S)
    return (re.match(r"\w+", rest) or [""])[0].lower()


def capped(engine, sql, limit=None):
    """A query under the time cap (QUERY_S): the engine is asked to cancel it when the cap passes."""
    timer = threading.Timer(QUERY_S, engine.cancel)
    timer.start()
    try:
        return engine.run(sql, limit)
    finally:
        timer.cancel()


def shown(cols, rows):
    more = len(rows) > SHOWN
    lines = [" | ".join(cols)] + [" | ".join("NULL" if v is None else str(v) for v in r) for r in rows[:SHOWN]]
    return "\n".join(lines + [f"(the first {SHOWN} rows; there are more)" if more else f"({len(rows)} row{'' if len(rows) == 1 else 's'})"])


def use(engine, name, args, writes=False):
    """One plain tool call, as (text, is_error). Each engine's catalog is read with the same information_schema query."""
    if name == "list_tables":
        sql = "SELECT table_name FROM information_schema.tables WHERE table_schema = current_schema() ORDER BY 1"
    elif name == "describe":
        sql = ("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = current_schema() "
               f"AND table_name = '{args['table'].replace(chr(39), chr(39) * 2)}' ORDER BY ordinal_position")
    elif name == "sql" and (writes or first_word(args["query"]) in READS):
        sql = args["query"]
    elif name == "sql":
        return "only a read-only query here: SELECT, WITH, VALUES, TABLE, EXPLAIN, SHOW or DESCRIBE", True
    else:
        return f"no tool named {name}", True
    try:
        cols, rows = capped(engine, sql, SHOWN + 1 if name == "sql" else None)
    except Exception as err:
        return str(err).strip()[:2000] or type(err).__name__, True
    if name == "list_tables":
        return "\n".join(r[0] for r in rows), False
    if name == "describe":
        return ("\n".join(f"{c} {t}" for c, t in rows), False) if rows else (f"no table named {args['table']}", True)
    return (shown(cols, rows) if cols else "OK"), False


class Plain:
    """The plain tools, the same on every engine: read-only for a question, and for a job a sql that writes too."""
    suffix, notes = "", ""

    def __init__(self, engine, writes=False):
        self.engine, self.writes, self.tools = engine, writes, WRITES if writes else PLAIN

    def use(self, name, args):
        return use(self.engine, name, args, self.writes)

    def stop(self):
        pass


class Door:
    """Pondra's own MCP door (POST /mcp), as an MCP client sees it: a session, the door's notes for the model, and the
    tools it lists, each called through it. For a question only its read-only ones, as the plain sql is read-only."""
    suffix = "+mcp"

    def __init__(self, url, writes=False):
        self.url, self.sid = url, None
        self.notes = self.rpc("initialize", {"protocolVersion": "2025-06-18", "capabilities": {},
                                             "clientInfo": {"name": "agent-bench", "version": "1"}}).get("instructions", "")
        self.rpc("notifications/initialized", None)
        self.tools = [{"name": t["name"], "description": t.get("description", ""), "input_schema": t["inputSchema"]}
                      for t in self.rpc("tools/list", {})["tools"] if writes or (t.get("annotations") or {}).get("readOnlyHint")]

    def rpc(self, method, params, verb="POST"):
        body = None if verb == "DELETE" else json.dumps({"jsonrpc": "2.0", "method": method, **({"params": params} if params is not None else {}),
                                                         **({"id": 1} if not method.startswith("notifications/") else {})}).encode()
        req = urllib.request.Request(self.url, body, {"content-type": "application/json", "accept": "application/json, text/event-stream",
                                                      **({"mcp-session-id": self.sid} if self.sid else {})}, method=verb)
        with LOOPBACK.open(req, timeout=QUERY_S * 2) as r:
            self.sid = r.headers.get("mcp-session-id") or self.sid
            raw = r.read()
        if not raw:
            return {}
        out = json.loads(raw)
        if "error" in out:
            raise RuntimeError(out["error"].get("message", str(out["error"])))
        return out["result"]

    def use(self, name, args):
        try:
            r = self.rpc("tools/call", {"name": name, "arguments": args})
        except Exception as err:
            return str(err)[:2000], True
        return "\n".join(c.get("text", "") for c in r.get("content", [])), bool(r.get("isError"))

    def stop(self):
        try:
            self.rpc(None, None, "DELETE")
        except Exception:
            pass


def dump(content):
    if isinstance(content, str):
        return content
    return [b.model_dump(mode="json", exclude_none=True) if hasattr(b, "model_dump") else b for b in content]


def begin(task, engine, kit, model, effort):
    """A try's start, however its model is reached: the system's words, the task as the user puts it, the tools (the
    kit's, and `answer` or `done`), and its empty record."""
    job = "job" in task
    system = (JOB if job else SYSTEM).format(label=engine.label) + (f"\n\nThe database's own notes for agents: {kit.notes}" if kit.notes else "")
    prompt = task["job" if job else "question"] + (f"\n\nFacts you may use: {task['evidence']}" if task.get("evidence") else "")
    rec = {"id": task["id"], "engine": engine.name + kit.suffix, "model": model, "effort": effort, "turns": 0, "steps": 0,
           "tokens": {"input": 0, "cache_write": 0, "cache_read": 0, "output": 0}, "model_s": 0.0, "engine_s": 0.0,
           "sql": None, "ran": [], "end": None, "only": bool(task.get("only"))}
    return system, prompt, kit.tools + [DONE if job else ANSWER], rec


def step(rec, kit, job, name, args):
    """One tool call of a try, however its model is reached: counted as a step; `answer` (a question's) or `done` (a
    job's) ends the try, and gives None; a statement is kept for a replay; any other call is the kit's, as (text,
    is_error), its seconds the engine's."""
    rec["steps"] += 1
    if name == "answer" and not job:
        rec["sql"], rec["end"] = args["query"], "answered"
        return None
    if name == "done" and job:
        rec["end"] = "done"
        return None
    if name in ("sql", "query", "write"):  # (the plain tool's, or the MCP door's: what a replay runs again)
        rec["ran"].append(args.get("query") or args.get("sql") or "")
    t0 = time.perf_counter()
    try:
        return kit.use(name, args) if name in {t["name"] for t in kit.tools} else (f"no tool named {name}", True)
    finally:
        rec["engine_s"] += time.perf_counter() - t0


def attempt(client, model, effort, task, engine, kit=None):
    """One try at a task: the model's loop over a kit of tools (the plain ones, or Pondra's MCP door's) and `answer`,
    and its record (turns, steps, tokens, seconds, the answer's SQL and how it ended), with the whole conversation as
    its transcript. It never edits what was said: each turn only appends, so the prompt cache holds and the model's
    thinking goes back as it came."""
    job = "job" in task
    kit = kit or Plain(engine, writes=job)
    system, prompt, tools, rec = begin(task, engine, kit, model, effort)
    messages = [{"role": "user", "content": prompt}]
    while rec["end"] is None:
        if rec["turns"] == TURNS:
            rec["end"] = "turn cap"
            break
        t0 = time.perf_counter()
        try:
            r = client.messages.create(model=model, max_tokens=16000, system=system, tools=tools, messages=messages,
                                       cache_control={"type": "ephemeral"}, output_config={"effort": effort})
        except Exception as err:  # (one that outlasted the SDK's retries ends this try, and what it spent stays counted)
            if getattr(err, "status_code", None) in (401, 403):
                raise  # (a key that doesn't work stops the whole run)
            rec["end"] = f"error: {type(err).__name__}: {str(err)[:300]}"
            break
        finally:
            rec["model_s"] += time.perf_counter() - t0
        rec["turns"] += 1
        for k, f in (("input", "input_tokens"), ("cache_write", "cache_creation_input_tokens"), ("cache_read", "cache_read_input_tokens"), ("output", "output_tokens")):
            rec["tokens"][k] += getattr(r.usage, f, 0) or 0
        messages.append({"role": "assistant", "content": r.content})
        if r.stop_reason == "refusal":  # (no fallback model: that would change who answered)
            rec["end"] = f"refused: {getattr(getattr(r, 'stop_details', None), 'category', None)}"
            break
        if r.stop_reason == "max_tokens":
            rec["end"] = "max_tokens"
            break
        calls = [b for b in r.content if b.type == "tool_use"]
        if not calls:  # (a tool call is never forced: the model is asked once more, which costs it a turn)
            messages.append({"role": "user", "content": "When the job is done, call `done`." if job else "Give your answer by calling `answer` with its SQL query."})
            continue
        results = []
        for b in calls:
            said = step(rec, kit, job, b.name, b.input)
            if said is None:
                break
            results.append({"type": "tool_result", "tool_use_id": b.id, "content": said[0], **({"is_error": True} if said[1] else {})})
        if rec["end"] is None:
            messages.append({"role": "user", "content": results})
    rec["transcript"] = {"system": system, "messages": [{"role": m["role"], "content": dump(m["content"])} for m in messages]}
    return rec


def scored(rec, task, want, engine):
    """The answer's rows against the gold ones, as an agent's answer is scored: extra columns allowed. A job is scored
    by its end state, however the try ended: what the harness does after it (`after`), then its checks' answers."""
    rec["passed"] = False
    if "job" in task:
        t0 = time.perf_counter()
        try:
            for s in task.get("after", []):
                capped(engine, s)
            got = [capped(engine, q)[1] for q in task["checks"]]
            rec["passed"] = all(same(w, g, False, task.get("places")) for w, g in zip(want, got))
        except Exception as err:
            rec["end"] = f"{rec['end']}; its end state failed a check: {str(err).strip()[:300]}"
        rec["engine_s"] += time.perf_counter() - t0
        return rec
    if rec["sql"] is None:
        return rec
    t0 = time.perf_counter()
    try:
        cols, rows = capped(engine, rec["sql"])
        rec["passed"] = same(want, rows, task.get("ordered", False), task.get("places"))
        rec["rows"] = len(rows)
    except Exception as err:
        rec["end"] = f"its answer failed: {str(err).strip()[:300]}"
    rec["engine_s"] += time.perf_counter() - t0
    return rec


def dollars(rec, per_hour):
    p = PRICES.get(rec["model"])
    if p is None:
        return None
    t = rec["tokens"]
    return round((t["input"] * p[0] + t["output"] * p[1] + t["cache_write"] * p[2] + t["cache_read"] * p[3]) / 1e6 + rec["engine_s"] * per_hour / 3600, 6)


def median(xs):
    xs = sorted(x for x in xs if x is not None)
    return None if not xs else xs[len(xs) // 2] if len(xs) % 2 else (xs[len(xs) // 2 - 1] + xs[len(xs) // 2]) / 2


def tokens_in(t):
    """The tokens a try read: the API's three input meters, or a helper's whole count (`total`), in and out."""
    return t["total"] if "total" in t else t["input"] + t["cache_write"] + t["cache_read"]


def summary(recs, runs):
    """Each engine's pass rate on the tasks every engine runs (the mean of its runs', and their spread), the medians of
    such a passed task's steps, tokens, seconds and dollars, and how it did on the tasks only it can do."""
    out = {}
    for name in dict.fromkeys(r["engine"] for r in recs):
        every = [r for r in recs if r["engine"] == name]
        mine = [r for r in every if not r["only"]]
        n = max(len(mine) // runs, 1)
        rates = [sum(r["passed"] for r in mine if r["run"] == k) / n for k in range(runs)]
        ok = [r for r in mine if r["passed"]]
        alone = [r for r in every if r["only"]]
        out[name] = {"passed": round(sum(rates) / runs, 4), "spread": [round(min(rates), 4), round(max(rates), 4)],
                     "steps": median([r["steps"] for r in ok]),
                     "tokens_in": median([tokens_in(r["tokens"]) for r in ok]),
                     "tokens_out": median([r["tokens"].get("output") for r in ok]),
                     "seconds": median([round(r["model_s"] + r["engine_s"], 2) for r in ok]),
                     "dollars": median([r["dollars"] for r in ok]),
                     "spent": None if all(r["dollars"] is None for r in every) else round(sum(r["dollars"] or 0 for r in every), 4)}
        if alone:
            out[name]["only_it"] = f"{sum(r['passed'] for r in alone)} of {len(alone)}"
    return out


def num(x):
    return "" if x is None else str(int(x)) if float(x).is_integer() else f"{x:.4g}"


def report(out, recs, runs):
    lines = [f"# Agent benchmark, set {out['set'].upper()}: {out['date']}", "",
             f"Model `{out['model']}` at effort {out['effort']}; each engine with the plain tools"
             + (", and pondra+mcp with Pondra's MCP door's" if "pondra+mcp" in out["engines"] else "") + f". {out['tasks']} tasks, {runs} run{'' if runs == 1 else 's'} of each. "
             f"Commit `{out['commit']}`, {out['pondra']}. Every number is from this run.", "",
             "| Engine | Passed | Spread of runs | Steps | Tokens in | Tokens out | Seconds | Dollars | Spent |",
             "|---|---|---|---|---|---|---|---|---|"]
    for name, v in out["engines"].items():
        if isinstance(v, str):
            lines.append(f"| {name} | {v} | | | | | | | |")
        if not isinstance(v, dict) or "passed" not in v:
            continue
        lines.append(f"| {name} | {v['passed']:.1%} | {v['spread'][0]:.1%} to {v['spread'][1]:.1%} | "
                     + " | ".join(num(v[k]) for k in ("steps", "tokens_in", "tokens_out", "seconds", "dollars", "spent")) + " |")
    lines += ["", "Passed is over the tasks every engine runs; steps, tokens, seconds and dollars are the medians of such "
              "a passed task. Dollars are list prices times tokens; Spent is the whole run's."
              + (" On a subscription, each try's model is a helper agent: Tokens in is its whole count as its runner gave it "
                 "(in and out, its own prompt and tools included), the same on every engine, and there are no dollars. "
                 "A try whose helper used anything but its tools isn't passed." if out.get("helpers") else ""), ""]
    alone = [f"{name} passed {v['only_it']} tries" for name, v in out["engines"].items() if isinstance(v, dict) and v.get("only_it")]
    lines += [f"On the tasks only Pondra can do, {', '.join(alone)}."] if alone else []
    lines += [""] if lines[-1] else []
    missed = {}
    for r in recs:
        if not r["passed"]:
            missed.setdefault(r["id"], {}).setdefault(r["engine"], []).append(r["end"] if r["end"] != "answered" else "wrong rows")
    if missed:
        lines += ["| Task | Missed |", "|---|---|"]
        for tid in sorted(missed):
            lines.append(f"| {tid} | " + "; ".join(f"{e}: {len(v)} of {runs} ({', '.join(sorted(set(v)))})" for e, v in missed[tid].items()) + " |")
    return "\n".join(lines) + "\n"


def commit():
    if os.environ.get("GITHUB_SHA"):
        return os.environ["GITHUB_SHA"][:12]
    r = subprocess.run(["git", "rev-parse", "--short=12", "HEAD"], capture_output=True, text=True, cwd=HERE)
    return r.stdout.strip() or "unknown"


def version(a):
    if not a.bin:
        return "Pondra on --port" if a.port else "no Pondra"
    r = subprocess.run([a.bin, "--version"], capture_output=True, text=True)
    return r.stdout.strip() or "pondra"


def want_of(t, oracle):
    """What a try is scored against: a job's wanted end state, or a question's gold rows (DuckDB's)."""
    return wanted(t, oracle) if "job" in t else oracle.run(gold_sql(t, "duckdb"))[1]


def opened(t, e, door, name):
    """A try's own database and its kit: for a job a copy of the shop with the job's setup done, for a question a
    session; the plain tools on it, or Pondra's MCP door's (the copy's own node's, for a job)."""
    s = e.copy(name) if "job" in t else e.session()
    try:
        for st in t.get("setup", []):
            s.run(st)
        return s, Door((s if "job" in t else e).mcp, writes="job" in t) if door else Plain(s, writes="job" in t)
    except BaseException:
        closed(t, e, s, name)
        raise


def closed(t, e, s, name, kit=None):
    try:
        if kit:
            kit.stop()
    finally:
        if "job" in t:
            e.uncopy(s, name)
        else:
            s.stop()


def run_agents(client, a, oracle, engines, todo, out):
    """Every task on every engine it runs on, `runs` times, `jobs` at a time; the records, each with its transcript
    saved apart. A try at a job gets a copy of the shop of its own, with the job's setup done."""
    want = {t["id"]: want_of(t, oracle) for t in todo}
    count = itertools.count()

    def one(job):
        k, t, e, door = job
        name = f"try_{next(count)}"
        s, kit = opened(t, e, door, name)
        try:
            rec = scored(attempt(client, a.model, a.effort, t, s, kit), t, want[t["id"]], s)
        finally:
            closed(t, e, s, name, kit)
        rec["run"], rec["dollars"] = k, dollars(rec, a.per_hour)
        if a.out:
            with open(os.path.join(a.out, "transcripts", f"{rec['engine']}-{t['id']}-{k}.json"), "w") as f:
                json.dump(rec, f, indent=1, default=str)
        rec.pop("transcript", None)
        return rec

    jobs = [(k, t, e, door) for k in range(a.runs) for t in todo for e, door in engines if runs_on(t, e.name)]
    with concurrent.futures.ThreadPoolExecutor(a.jobs) as pool:
        recs = list(pool.map(one, jobs))
    for name, v in summary(recs, a.runs).items():
        out["engines"].setdefault(name, {}).update(v)
    return recs


def run(a):
    """The model's runs: a record per try (runs.jsonl), each try's transcript, the passed answers to replay
    (replay.jsonl), a report (report.md), and one JSON line to stdout."""
    import anthropic
    scorer_checks()
    client = anthropic.Anthropic(max_retries=8)
    todo = tasks(a.set, a.only)
    names = a.engines.split(",")
    if a.out:
        os.makedirs(os.path.join(a.out, "transcripts"), exist_ok=True)
    engines, notes = up(a, {n.split("+")[0] for n in names})
    out = {"set": a.set, "date": datetime.date.today().isoformat(), "model": a.model, "effort": a.effort, "tasks": len(todo),
           "runs": a.runs, "commit": commit(), "pondra": version(a), "engines": notes}
    try:  # (DuckDB is always started: its rows are the gold, whether or not an agent runs on it)
        by = {e.name: e for e in engines}
        lanes = [(by[n.split("+")[0]], n.endswith("+mcp")) for n in names if n.split("+")[0] in by]
        if any(door and not e.mcp for e, door in lanes):
            raise SystemExit("pondra+mcp needs --bin: the door's address is the node's it starts")
        recs = run_agents(client, a, engines[0], lanes, todo, out)
    finally:
        down(engines)
    if a.out:
        written(a.out, out, recs)
    print(json.dumps(out, default=str))
    return 0


def written(to, out, recs):
    """A run's files: a record per try (runs.jsonl), the passed answers to replay (replay.jsonl), and report.md."""
    with open(os.path.join(to, "runs.jsonl"), "w") as f:
        f.writelines(json.dumps(r, default=str) + "\n" for r in recs)
    seen = set()
    with open(os.path.join(to, "replay.jsonl"), "w") as f:
        for r in recs:
            said = {"sql": r["sql"]} if out["set"] == "a" else {"ran": r["ran"]}
            if r["passed"] and (r["id"], r["engine"], json.dumps(said)) not in seen:
                seen.add((r["id"], r["engine"], json.dumps(said)))
                f.write(json.dumps({"set": out["set"], "id": r["id"], "engine": r["engine"], **said}) + "\n")
    with open(os.path.join(to, "report.md"), "w") as f:
        f.write(report(out, recs, out["runs"]))


# ---------------------------------------------------------------- on a subscription, with helper agents

HELPER = """{system}

{prompt}

Your tools are one command, which you run with Bash: `{cmd} TOOL [VALUE]`. VALUE is the tool's one argument as a single
quoted shell word, or a JSON object of its arguments when it takes more than one. The tools:
{tools}

Use that command and nothing else: no other command, file or program, and no web. Call one tool at a time, at most {cap}
calls before your last one. {last} The command says when the try has ended: stop then, and reply with one line saying
how it ended."""


def brief(cmd, system, prompt, tools, job):
    """What a helper agent is told: the API run's system words and task, and its tools as one command's."""
    listed = "\n".join(f"- {t['name']}" + (f" ({', '.join(t['input_schema'].get('required', []))})" if t["input_schema"].get("required") else "")
                       + f": {t['description']}" for t in tools)
    last = "End with `done` when the job is done." if job else "End with `answer` and the one SQL query whose rows answer the question."
    return HELPER.format(system=system, prompt=prompt, cmd=cmd, tools=listed, cap=TURNS, last=last)


class Helpers:
    """Tries whose model is a helper agent on a subscription, with no API key (`serve`). A helper gets the API run's
    system words, task and tools (`begin`, `brief`) and calls the tools through one command (`tool`), each call the API
    loop's (`step`), so the tasks, the tools, the scoring and the records are the API run's. What only the API counts
    (turns, the four token meters, dollars) is left out: a helper's whole token count, its tool calls and its seconds
    come from whoever ran it (`finish`). A try ends at its answer or done, at TURNS calls before it, or at `finish`; a
    try whose helper made calls other than its tools' is not passed."""

    def __init__(self, set_, model, engines, lanes, to=None, cmd=None):
        self.set, self.model, self.to, self.cmd = set_, model, to, cmd
        self.by, self.lanes, self.oracle = {e.name: e for e in engines}, lanes, engines[0]
        self.todo = {t["id"]: t for t in tasks(set_)}
        self.want, self.tries, self.recs, self.lock, self.count = {}, {}, [], threading.Lock(), itertools.count()

    def begin(self, lane, task_id, run=0):
        if lane not in self.lanes or task_id not in self.todo:
            raise ValueError(f"no lane {lane} or no task {task_id}")
        t, e, door = self.todo[task_id], self.by[lane.split("+")[0]], lane.endswith("+mcp")
        if not runs_on(t, e.name):
            raise ValueError(f"{task_id} doesn't run on {e.name}")
        with self.lock:
            if task_id not in self.want:
                self.want[task_id] = want_of(t, self.oracle)
            name = f"try_{next(self.count)}"
        s, kit = opened(t, e, door, name)
        system, prompt, tools, rec = begin(t, s, kit, self.model, None)
        rec.update(run=run, turns=None, tokens={}, calls=[], helper=True, dollars=None)
        self.tries[name] = {"t": t, "e": e, "s": s, "kit": kit, "rec": rec, "system": system, "prompt": prompt, "tools": tools,
                            "t0": time.perf_counter(), "lock": threading.Lock(), "asked": 0}
        return {"try": name, "brief": brief(f"{self.cmd} {name}", system, prompt, tools, "job" in t)}

    def call(self, name, tool, value=None, args=None):
        tr = self.tries.get(name)
        if tr is None:
            return {"text": f"no try named {name}", "error": True, "ended": True}
        with tr["lock"]:
            rec, job = tr["rec"], "job" in tr["t"]
            tr["asked"] += 1  # (every use of the command, even after the end: what its runner's count is checked against)
            if rec["end"]:
                return {"text": f"This try has ended ({rec['end']}).", "error": True, "ended": True}
            if tool != ("done" if job else "answer") and rec["steps"] >= TURNS:
                rec["end"] = "turn cap"
                self.close(name)
                return {"text": f"This try has ended: {TURNS} calls without an answer.", "error": True, "ended": True}
            spec = next((x for x in tr["tools"] if x["name"] == tool), None)
            need = (spec or {}).get("input_schema", {}).get("required", [])
            args = dict(args or {})
            if value is not None and need and need[0] not in args:
                args[need[0]] = value
            missing = [k for k in need if k not in args]
            if missing:  # (the API's strict tools never send this; a helper's command can, and it costs a call)
                rec["steps"] += 1
                said = (f"{tool} needs {', '.join(missing)}", True)
            else:
                said = step(rec, tr["kit"], job, tool, args)
            rec["calls"].append({"tool": tool, "args": args, **({"text": said[0], "error": said[1]} if said else {})})
            if said is None:
                self.close(name)
                return {"text": f"This try has ended ({rec['end']}).", "ended": True}
            return {"text": said[0], "error": said[1], "ended": False}

    def close(self, name):
        """The try scored on its own database, which is then closed (its copy, for a job, dropped)."""
        tr = self.tries[name]
        try:
            scored(tr["rec"], tr["t"], self.want[tr["t"]["id"]], tr["s"])
        finally:
            closed(tr["t"], tr["e"], tr["s"], name, tr["kit"])

    def finish(self, name, tokens=None, tool_uses=None, seconds=None):
        """A helper's try done with: closed if it never ended, and given what its runner counted."""
        tr = self.tries[name]
        rec = tr["rec"]
        with tr["lock"]:
            if rec["end"] is None:
                rec["end"] = "no done" if "job" in tr["t"] else "no answer"
                self.close(name)
            del self.tries[name]
        rec["tokens"] = {"total": tokens} if tokens is not None else {}
        rec["model_s"] = round(max((seconds if seconds is not None else time.perf_counter() - tr["t0"]) - rec["engine_s"], 0), 2)
        rec["outside"] = max(tool_uses - tr["asked"], 0) if tool_uses is not None else None
        if rec["outside"]:
            rec["passed"], rec["end"] = False, f"{rec['end']}; {rec['outside']} calls outside its tools"
        if self.to:
            with open(os.path.join(self.to, "transcripts", f"{rec['engine']}-{rec['id']}-{rec['run']}.json"), "w") as f:
                json.dump({**rec, "system": tr["system"], "prompt": tr["prompt"]}, f, indent=1, default=str)
        out = {k: v for k, v in rec.items() if k != "calls"}
        with self.lock:
            self.recs.append(out)
        return out

    def stop(self, out):
        """What a run's report holds, from the tries finished so far; the files, when it has a folder."""
        for name in list(self.tries):
            self.finish(name)
        runs = max([r["run"] for r in self.recs] + [0]) + 1
        out.update(runs=runs, tasks=len({r["id"] for r in self.recs}))
        for name, v in summary(self.recs, runs).items():
            out["engines"].setdefault(name, {}).update(v)
        if self.to:
            written(self.to, out, self.recs)
        return out


def answering(h, out, done):
    """The helpers' HTTP door on 127.0.0.1: POST /begin, /tool, /finish and /stop, each a JSON body and answer."""
    import http.server

    class Door(http.server.BaseHTTPRequestHandler):
        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers.get("content-length") or 0)) or b"{}")
            said, code = {"error": f"no {self.path} here"}, 404
            try:
                if self.path == "/begin":
                    said, code = h.begin(body["lane"], body["task"], int(body.get("run", 0))), 200
                elif self.path == "/tool":
                    said, code = h.call(body["try"], body["tool"], body.get("value"), body.get("args")), 200
                elif self.path == "/finish":
                    said, code = h.finish(body["try"], body.get("tokens"), body.get("tool_uses"), body.get("seconds")), 200
                elif self.path == "/stop":
                    said, code = h.stop(out), 200
                    done.set()
            except Exception as err:
                said, code = {"error": f"{type(err).__name__}: {err}"}, 400
            raw = json.dumps(said, default=str).encode()
            self.send_response(code)
            self.send_header("content-type", "application/json")
            self.send_header("content-length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def log_message(self, *_):
            pass

    return http.server.ThreadingHTTPServer(("127.0.0.1", 0), Door)


LOOPBACK = urllib.request.build_opener(urllib.request.ProxyHandler({}))  # (127.0.0.1 never goes through a proxy)


def helper_cmd(at):
    """A helper's one command: this Python and this file, the door's port, then `tool`. Options go before the mode,
    since Python 3.11's argparse refuses positionals given after an option once `args` has matched."""
    return f"{sys.executable} {os.path.abspath(__file__)} --at {at} tool"


def asked(at, path, body):
    """One call to a serving run's helpers' door."""
    req = urllib.request.Request(f"http://127.0.0.1:{at}{path}", json.dumps(body).encode(), {"content-type": "application/json"})
    try:
        with LOOPBACK.open(req, timeout=QUERY_S * 3) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as err:
        return json.loads(err.read() or b"{}") or {"error": str(err)}
    except (urllib.error.URLError, OSError) as err:
        return {"error": f"the helpers' door at {at}: {getattr(err, 'reason', err)}"}


def serve(a):
    """The engines up and the helpers' door open until /stop: one JSON line with its port first, the run's after."""
    scorer_checks()
    names = a.engines.split(",")
    os.makedirs(os.path.join(a.out, "transcripts"), exist_ok=True)
    engines, notes = up(a, {n.split("+")[0] for n in names})
    out = {"set": a.set, "date": datetime.date.today().isoformat(), "model": f"{a.model} on a subscription", "effort": "its default",
           "commit": commit(), "pondra": version(a), "engines": notes, "helpers": True}
    try:
        by = {e.name for e in engines}
        done = threading.Event()
        h = Helpers(a.set, a.model, engines, [n for n in names if n.split("+")[0] in by], a.out, None)
        server = answering(h, out, done)
        at = server.server_address[1]
        h.cmd = helper_cmd(at)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        print(json.dumps({"at": at, "lanes": h.lanes, "tasks": list(h.todo)}), flush=True)
        done.wait()
        server.shutdown()
    finally:
        down(engines)
    print(json.dumps(out, default=str))
    return 0


def client(a):
    """`begin`, `tool`, `finish` and `stop` against a serving run (--at its port)."""
    if not a.at:
        raise SystemExit("--at: the serving run's port")
    x = a.args
    if a.mode == "begin":
        said = asked(a.at, "/begin", {"lane": x[0], "task": x[1], "run": int(x[2]) if len(x) > 2 else 0})
    elif a.mode == "finish":
        said = asked(a.at, "/finish", {"try": x[0], "tokens": a.tokens, "tool_uses": a.tool_uses, "seconds": a.seconds})
    elif a.mode == "stop":
        said = asked(a.at, "/stop", {})
    else:  # (a helper's: the tool's text, as the API run's tool result says it)
        value = x[2] if len(x) > 2 else None
        args = json.loads(value) if value and value.lstrip().startswith("{") else None
        said = asked(a.at, "/tool", {"try": x[0], "tool": x[1], "value": None if args else value, "args": args})
        print(("error: " if said.get("error") is True and not said.get("ended") else "") + said.get("text", str(said.get("error"))))
        return 0
    print(json.dumps(said, default=str))
    return 1 if "error" in said else 0


# ---------------------------------------------------------------- with no model

def replayed(t, e, ran):
    """A job's saved statements run again, in order, on a copy of the shop with its setup done (one that failed then
    fails again, and changes nothing), then its after and its checks."""
    c = e.copy("replay")
    try:
        for st in t.get("setup", []):
            c.run(st)
        for st in ran:
            try:
                capped(c, st)
            except Exception:
                pass
        for st in t.get("after", []):
            c.run(st)
        return [capped(c, q)[1] for q in t["checks"]]
    finally:
        e.uncopy(c, "replay")


def replay(engines, lines, out):
    """Saved answers that once passed, run again on this commit with no model call: an engine change that breaks an
    answer an agent once gave shows here. A job's are its statements, and its end state is checked."""
    by = {e.name: e for e in engines}
    gold_of = {}
    out["replay"] = {"answers": len(lines), "passed": 0, "skipped": 0, "broke": {}}
    for ln in lines:
        e = by.get(ln["engine"].split("+")[0])  # (an answer given through Pondra's MCP door is run on Pondra)
        if e is None:
            out["replay"]["skipped"] += 1
            continue
        key = (ln.get("set", "a"), ln["id"])
        if key not in gold_of:
            t = next(t for t in tasks(key[0]) if t["id"] == ln["id"])
            gold_of[key] = (t, wanted(t, engines[0]) if "job" in t else engines[0].run(gold_sql(t, "duckdb"))[1])
        t, want = gold_of[key]
        try:
            if "job" in t:
                ok = all(same(w, g, False, t.get("places")) for w, g in zip(want, replayed(t, e, ln["ran"])))
            else:
                ok = same(want, capped(e, ln["sql"])[1], t.get("ordered", False), t.get("places"))
            why = "wrong end state" if "job" in t else "wrong rows"
        except Exception as err:
            ok, why = False, str(err).strip()[:300]
        if ok:
            out["replay"]["passed"] += 1
        else:
            out["replay"]["broke"].setdefault(ln["id"], {})[ln["engine"]] = why


class Scripted:
    """A stand-in for the API client that plays a fixed script, shaped like the SDK's answers: the loop's own test,
    with no key and no model."""

    class Thing(types.SimpleNamespace):
        def model_dump(self, **_):
            return {k: v.model_dump() if hasattr(v, "model_dump") else v for k, v in vars(self).items()}

    def __init__(self, script):
        self.script, self.messages, self.seen = list(script), self, []

    def create(self, **kw):
        self.seen.append(kw)
        blocks = self.script.pop(0) if self.script else [("tool_use", "list_tables", {})]
        T = Scripted.Thing
        content = [T(type="text", text=b[1]) if b[0] == "text" else T(type="tool_use", id=f"t{len(self.seen)}_{i}", name=b[1], input=b[2])
                   for i, b in enumerate(blocks)]
        return T(content=content, stop_reason="tool_use" if any(b[0] == "tool_use" for b in blocks) else "end_turn",
                 usage=T(input_tokens=100, output_tokens=10, cache_creation_input_tokens=20, cache_read_input_tokens=300))


def loop_checks(engines, out):
    """The loop on every engine with a scripted model: two tools in one turn, a failing query whose error goes back as
    text, a turn with no call (asked once more), then the gold SQL as the answer; a model that never answers, which
    stops at the turn cap; on Pondra, the same through its MCP door's tools; and a job on a copy of the shop of its own:
    its setup's harm, the gold's statements through the sql tool that writes, then done."""
    wrong = []
    for e in engines:
        for t in tasks("a")[:2]:
            want = engines[0].run(gold_sql(t, "duckdb"))[1]
            client = Scripted([[("tool_use", "list_tables", {}), ("tool_use", "describe", {"table": "orders"})],
                               [("tool_use", "sql", {"query": "SELEC 1"})], [("text", "I think it is 42.")],
                               [("tool_use", "answer", {"query": gold_sql(t, e.name)})]])
            rec = scored(attempt(client, "claude-opus-5-5", "medium", t, e), t, want, e)
            msgs = rec["transcript"]["messages"]
            results = {r["tool_use_id"]: r for m in msgs if isinstance(m["content"], list) for r in m["content"] if r.get("type") == "tool_result"}
            checks = {
                "passed": rec["passed"], "ended": rec["end"] == "answered", "turns": rec["turns"] == 4, "steps": rec["steps"] == 4,
                "tokens": rec["tokens"] == {"input": 400, "cache_write": 80, "cache_read": 1200, "output": 40},
                "tables": results["t1_0"]["content"].split("\n") == sorted(COLUMNS) and "is_error" not in results["t1_0"],
                "describe": results["t1_1"]["content"].split("\n")[0].startswith("order_id "),
                "error as text": results["t2_0"].get("is_error") is True and len(results["t2_0"]["content"]) > 0,
                "asked again": msgs[6]["content"].startswith("Give your answer"),
                "one prompt": all(kw["system"] == client.seen[0]["system"] and kw["tools"] == PLAIN + [ANSWER] for kw in client.seen),
                "dollars": dollars(rec, 0) == round((400 * 4 + 40 * 20 + 80 * 5 + 1200 * 0.2) / 1e6, 6),
            }
            wrong += [f"{e.name} {t['id']}: {k}" for k, v in checks.items() if not v]
        rec = attempt(Scripted([]), "claude-opus-5-5", "medium", tasks("a")[0], e)
        if (rec["end"], rec["turns"], rec["steps"], rec["sql"]) != ("turn cap", TURNS, TURNS, None):
            wrong.append(f"{e.name}: the turn cap")
        write = use(e, "sql", {"query": "/* x */ DROP TABLE orders"})
        if write[1] is not True:
            wrong.append(f"{e.name}: a write went through the read-only tool")
        if getattr(e, "mcp", None):
            t, door = tasks("a")[0], Door(e.mcp)
            client = Scripted([[("tool_use", "list_tables", {}), ("tool_use", "query", {"sql": "SELEC 1"})],
                               [("tool_use", "write", {"sql": "DROP TABLE orders"})], [("tool_use", "answer", {"query": gold_sql(t, "pondra")})]])
            try:
                rec = scored(attempt(client, "claude-opus-5-5", "medium", t, e, door), t, engines[0].run(gold_sql(t, "duckdb"))[1], e)
            finally:
                door.stop()
            results = {r["tool_use_id"]: r for m in rec["transcript"]["messages"] if isinstance(m["content"], list) for r in m["content"] if r.get("type") == "tool_result"}
            names = [x["name"] for x in client.seen[0]["tools"]]
            checks = {
                "passed": rec["passed"], "named": rec["engine"] == "pondra+mcp", "steps": rec["steps"] == 4,
                "its tools": {"list_tables", "query", "find"} <= set(names) and names[-1] == "answer" and "write" not in names,
                "its notes": "own notes for agents: " in client.seen[0]["system"],
                "tables": '"customers"' in results["t1_0"]["content"] and "is_error" not in results["t1_0"],
                "error as text": results["t1_1"].get("is_error") is True and "SELEC" in results["t1_1"]["content"],
                "no write": results["t2_0"].get("is_error") is True,
            }
            wrong += [f"{e.name}+mcp: {k}" for k, v in checks.items() if not v]
        for t in [t for t in tasks("b") if t["id"] in ("b06", "b12") and runs_on(t, e.name)]:
            c = e.copy(f"loop_{t['id']}")
            try:
                for st in t["setup"]:
                    c.run(st)
                client = Scripted([[("tool_use", "sql", {"query": st})] for st in gold_sql(t, e.name)] + [[("tool_use", "done", {"summary": "fixed"})]])
                rec = scored(attempt(client, "claude-opus-5-5", "medium", t, c), t, wanted(t, engines[0]), c)
            finally:
                e.uncopy(c, f"loop_{t['id']}")
            results = [r for m in rec["transcript"]["messages"] if isinstance(m["content"], list) for r in m["content"] if r.get("type") == "tool_result"]
            checks = {"passed": rec["passed"], "done": rec["end"] == "done", "ran": rec["ran"] == gold_sql(t, e.name),
                      "wrote": all("is_error" not in r for r in results), "its words": client.seen[0]["system"].startswith("You do jobs"),
                      "its tools": [x["name"] for x in client.seen[0]["tools"]] == ["list_tables", "describe", "sql", "done"]}
            wrong += [f"{e.name} {t['id']}: {k}" for k, v in checks.items() if not v]
    wrong += helper_checks(engines)
    out["loop"] = {"ok": not wrong, "wrong": wrong}


def helper_checks(engines):
    """The helpers' door on every lane, as a helper agent would use it, with no model: a try told its task and tools,
    a tool called through the command itself, an argument left out, an error as text, the answer that ends it and a call
    after that, its runner's counts; a try with calls outside its tools, not passed; and a job on a copy of its own."""
    wrong, lanes = [], [e.name for e in engines] + ["pondra+mcp" for e in engines if getattr(e, "mcp", None)]
    h = Helpers("a", "claude-opus-5-5", engines, lanes)
    jobs = Helpers("b", "claude-opus-5-5", engines, ["duckdb"])
    done, out = threading.Event(), {"set": "a", "engines": {}, "helpers": True}
    server = answering(h, out, done)
    at = server.server_address[1]
    h.cmd = helper_cmd(at)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        t = tasks("a")[0]
        for lane in lanes:
            door = lane.endswith("+mcp")
            b = asked(at, "/begin", {"lane": lane, "task": t["id"]})
            name = b["try"]
            ran = subprocess.run(h.cmd.split() + [name, "list_tables"], capture_output=True, text=True, timeout=60)
            cli = ran.stdout
            bare = asked(at, "/tool", {"try": name, "tool": "query" if door else "describe"})
            bad = asked(at, "/tool", {"try": name, "tool": "query" if door else "sql", "value": "SELEC 1"})
            ans = asked(at, "/tool", {"try": name, "tool": "answer", "value": gold_sql(t, lane.split("+")[0])})
            after = asked(at, "/tool", {"try": name, "tool": "list_tables"})
            rec = asked(at, "/finish", {"try": name, "tokens": 1234, "tool_uses": 5, "seconds": 9.5})
            b2 = asked(at, "/begin", {"lane": lane, "task": t["id"], "run": 1})
            asked(at, "/tool", {"try": b2["try"], "tool": "list_tables"})
            rec2 = asked(at, "/finish", {"try": b2["try"], "tokens": 50, "tool_uses": 3})
            checks = {
                "brief": t["question"] in b["brief"] and f"{h.cmd} {name} TOOL" in b["brief"] and "- answer (query): " in b["brief"],
                "the command": ("customers" in cli) and not cli.startswith("error"),
                "a missing argument": bare.get("error") is True and "needs" in bare.get("text", ""),
                "error as text": bad.get("error") is True and "SELEC" in bad.get("text", ""),
                "answered": ans.get("ended") is True and after.get("ended") is True,
                "scored": rec.get("passed") is True and rec["end"] == "answered" and rec["steps"] == 4 and rec["engine"] == lane,
                "counts": rec["tokens"] == {"total": 1234} and rec["outside"] == 0 and rec["turns"] is None,
                "outside its tools": rec2.get("passed") is False and rec2["end"] == "no answer; 2 calls outside its tools",
            }
            wrong += [f"helpers {lane}: {k}" + (f" ({ran.returncode}: {(ran.stderr or cli).strip()[-300:]})" if k == "the command" else "")
                      for k, v in checks.items() if not v]
        j = next(x for x in tasks("b") if x["id"] == "b06")
        name = jobs.begin("duckdb", "b06")["try"]
        said = [jobs.call(name, "sql", st) for st in gold_sql(j, "duckdb")] + [jobs.call(name, "done", "fixed")]
        rec = jobs.finish(name, 10, len(said))
        if not (rec["passed"] and rec["end"] == "done" and rec["ran"] == gold_sql(j, "duckdb") and said[-1]["ended"]):
            wrong.append("helpers duckdb b06: a job")
        summed = asked(at, "/stop", {})
        if not (done.is_set() and summed["engines"]["duckdb"]["passed"] == 0.5 and summed["engines"]["duckdb"]["tokens_in"] == 1234
                and summed["engines"]["duckdb"]["spent"] is None):
            wrong.append(f"helpers: the summary {summed['engines'].get('duckdb')}")
    except Exception as err:
        wrong.append(f"helpers: {type(err).__name__}: {err}")
    finally:
        server.shutdown()
    return wrong


def check(a):
    """What CI runs (harness.py agents): the gold of set A's questions and set B's jobs, the loop with a scripted model,
    and the saved answers replayed."""
    scorer_checks()
    questions, jobs = tasks("a", a.only), tasks("b", a.only)
    engines, notes = up(a)
    out = {"tasks": {"a": len(questions), "b": len(jobs)}, "engines": notes, "failed": {}}
    try:
        gold(engines, questions, out)
        gold_jobs(engines, jobs, out)
        if a.mode == "check":
            loop_checks(engines, out)
            lines = []
            for f in sorted(glob.glob(os.path.join(TASKS, "replay_*.jsonl"))):
                with open(f) as fh:
                    lines += [json.loads(x) for x in fh if x.strip()]
            replay(engines, lines, out)
    finally:
        down(engines)
    out["ok"] = not out["failed"] and out.get("loop", {}).get("ok", True) and not out.get("replay", {}).get("broke")
    print(json.dumps(out, default=str))
    return 0 if out["ok"] else 1


def main():
    p = argparse.ArgumentParser()
    p.add_argument("mode", choices=["gold", "check", "run", "replay", "serve", "begin", "tool", "finish", "stop"])
    p.add_argument("args", nargs="*", help="replay: a runs.jsonl or replay.jsonl; begin: LANE TASK [RUN]; tool: TRY TOOL [VALUE]; finish: TRY")
    p.add_argument("--bin", help="a pondra binary to start on a fresh lake")
    p.add_argument("--port", type=int, help="or a running node's Postgres door")
    p.add_argument("--pg-bin", help="Postgres's initdb and pg_ctl folder")
    p.add_argument("--only", help="task ids, comma-separated")
    p.add_argument("--set", default="a")
    p.add_argument("--model", default="claude-opus-5-5")
    p.add_argument("--effort", default="medium", choices=["low", "medium", "high", "xhigh", "max"])
    p.add_argument("--runs", type=int, default=3)
    p.add_argument("--jobs", type=int, default=6, help="tries at once")
    p.add_argument("--engines", default=",".join(ENGINES + ("pondra+mcp",)), help="pondra+mcp: Pondra with its MCP door's tools")
    p.add_argument("--per-hour", type=float, default=0.0, help="the machine's dollars an hour, for the engines' seconds")
    p.add_argument("--out", default="agent-bench-out")
    p.add_argument("--at", type=int, help="begin, tool, finish, stop: the serving run's port")
    p.add_argument("--tokens", type=int, help="finish: the helper's whole token count")
    p.add_argument("--tool-uses", type=int, help="finish: the helper's tool calls, all of them")
    p.add_argument("--seconds", type=float, help="finish: the helper's seconds")
    a = p.parse_args()
    if a.mode in ("gold", "check"):
        return check(a)
    if a.mode == "run":
        return run(a)
    if a.mode == "serve":
        return serve(a)
    if a.mode in ("begin", "tool", "finish", "stop"):
        return client(a)
    with open(a.args[0]) as f:
        lines = [json.loads(x) for x in f if x.strip()]
    lines = [ln for ln in lines if ln.get("passed", True)]
    engines, notes = up(a, {ln["engine"].split("+")[0] for ln in lines})
    out = {"engines": notes}
    try:
        replay(engines, lines, out)
    finally:
        down(engines)
    print(json.dumps(out, default=str))
    return 0 if not out["replay"]["broke"] else 1


if __name__ == "__main__":
    sys.exit(main())
