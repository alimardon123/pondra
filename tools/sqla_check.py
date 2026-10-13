"""SQLAlchemy, pandas and the DB-API client over Pondra, proved (L3; `python/pondra/dbapi.py`, `dialect.py`).

    PYTHONPATH=<worktree>/python python3 tools/sqla_check.py [--bin BIN] [--lib PATH] [--postgres URL]

Starts its own node (--bin) on a new lake in a temporary folder, and runs the checks in each form:
`pondra+http` (the node's HTTP door), `pondra+psycopg` (the Postgres port), and `pondra:///folder` in this
process when --lib names a pondra_lib (without it, that form is listed under not_run). Prints one
JSON document: each check's ok and what it found, the costs (reported, not judged) and what was not run.
Exits 1 if any check is false. A check that a door can't answer is false and says what the door answered:
that is the finding, not a crash of the tool.

--postgres names a PostgreSQL 16 (a SQLAlchemy URL) where the same DDL runs in schema sqla_check, which is
dropped at the end; the inspector's answers there are compared with Pondra's. The node is stopped by its PID,
and the lake and its cache removed.
"""
import argparse
import datetime
import decimal
import json
import os
import shutil
import socket
import statistics
import subprocess
import sys
import tempfile
import time
import traceback
import urllib.request
import uuid

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "python"))

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
import pyarrow  # noqa: E402,F401  (the client's Arrow answers need it: their checks say so if it is missing)
import sqlalchemy  # noqa: E402
from sqlalchemy import (BigInteger, Boolean, Column, Date, DateTime, Float, Integer, Interval,  # noqa: E402
                        LargeBinary, Numeric, String, Table, Text, Time, Uuid, create_engine, func, insert,
                        inspect, select, text)
from sqlalchemy.exc import ArgumentError, IntegrityError  # noqa: E402
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, relationship  # noqa: E402

import pondra  # noqa: E402
from pondra import dbapi, dialect  # noqa: E402

dialect.register()  # the dialect names, as the wheel's entry points make them known (before any Table uses them)

SCHEMA = "sqla_check"
DBAPI_TABLE = "sqla_dbapi"
BIG = "sqla_big"
# The DDL of the inspector's small schema: the same text runs on Pondra and on PostgreSQL 16.
DDL = [
    f"CREATE SCHEMA {SCHEMA}",
    f"CREATE TABLE {SCHEMA}.pets (id INTEGER PRIMARY KEY, name VARCHAR(50) NOT NULL, owner_id INTEGER, "
    "weight NUMERIC(6, 2), born DATE, seen TIMESTAMP)",
    f"CREATE TABLE {SCHEMA}.owners (id INTEGER PRIMARY KEY, email TEXT NOT NULL, active BOOLEAN)",
    f"CREATE INDEX ix_pets_name ON {SCHEMA}.pets (name)",
]
# What PostgreSQL 16 answers for the DDL above (captured from PostgreSQL 16.15 by its own inspector; --postgres
# compares the whole answer live).
EXPECTED = {
    "tables": ["owners", "pets"],
    "columns": {
        "owners": [["id", "INTEGER", False], ["email", "TEXT", False], ["active", "BOOLEAN", True]],
        "pets": [["id", "INTEGER", False], ["name", "VARCHAR(50)", False], ["owner_id", "INTEGER", True],
                 ["weight", "NUMERIC(6, 2)", True], ["born", "DATE", True],
                 ["seen", "TIMESTAMP", True]],
    },
    "primary_key": {"owners": ["id"], "pets": ["id"]},
    "indexes": {"owners": [], "pets": [["ix_pets_name", ["name"], False]]},
}


class Base(DeclarativeBase):
    pass


class Parent(Base):
    __tablename__ = "sa_parent"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(40))
    # No ForeignKey: Pondra refuses a plain REFERENCES ("a reference isn't checked yet"), so the join is named.
    children: Mapped[list["Child"]] = relationship(primaryjoin="Parent.id == foreign(Child.parent_id)",
                                                   back_populates="parent")


class Child(Base):
    __tablename__ = "sa_child"
    id: Mapped[int] = mapped_column(primary_key=True)
    parent_id: Mapped[int] = mapped_column(Integer)
    label: Mapped[str] = mapped_column(String(40))
    parent: Mapped[Parent] = relationship(primaryjoin="foreign(Child.parent_id) == Parent.id",
                                          back_populates="children")


TYPES = Table(
    "sa_types", Base.metadata,
    Column("id", Integer, primary_key=True), Column("i", BigInteger), Column("f", Float), Column("n", Numeric(10, 2)),
    Column("s", String(40)), Column("tx", Text), Column("b", Boolean), Column("d", Date), Column("dt", DateTime),
    Column("dtz", DateTime(timezone=True)), Column("t", Time), Column("iv", Interval), Column("blob", LargeBinary),
    Column("u", Uuid(as_uuid=True)),
)
EVENTS = Table(  # Pondra's table options, as dialect keywords
    "sa_events", Base.metadata,
    Column("id", Integer, primary_key=True), Column("ts", DateTime), Column("region", String(20)),
    pondra_partition_by="day(ts)", pondra_cluster_by=["region", "ts"], pondra_order_by="ts",
)
MANY = Table("sa_many", Base.metadata, Column("id", Integer, primary_key=True), Column("label", String(20)))

TYPE_ROWS = [
    dict(id=1, i=2 ** 40, f=1.5, n=decimal.Decimal("12.34"), s="héllo", tx="line\nbreak 'quoted'", b=True,
         d=datetime.date(2026, 1, 5), dt=datetime.datetime(2026, 1, 5, 10, 11, 12, 345678),
         dtz=datetime.datetime(2026, 1, 5, 10, 11, 12, tzinfo=datetime.timezone(datetime.timedelta(hours=2))),
         t=datetime.time(10, 11, 12, 500), iv=datetime.timedelta(days=2, seconds=3, microseconds=4),
         blob=b"\x00\xffab", u=uuid.UUID(int=12345)),
    dict(id=2, i=-7, f=-0.25, n=decimal.Decimal("-0.50"), s="", tx=None, b=False, d=datetime.date(1999, 12, 31),
         dt=datetime.datetime(1970, 1, 1), dtz=datetime.datetime(2000, 1, 1, tzinfo=datetime.timezone.utc),
         t=datetime.time(0, 0), iv=datetime.timedelta(seconds=-5), blob=b"", u=None),
    dict(id=3, i=None, f=None, n=None, s=None, tx=None, b=None, d=None, dt=None, dtz=None, t=None, iv=None,
         blob=None, u=None),
]
PANDAS_COLUMNS = ["i", "f", "s", "b", "d", "ts", "dec"]
PANDAS_ROWS = [
    (1, 1.5, "a", True, datetime.date(2026, 1, 5), datetime.datetime(2026, 1, 5, 10, 11, 12), decimal.Decimal("1.10")),
    (2, -0.25, "b c", False, datetime.date(1999, 12, 31), datetime.datetime(2026, 1, 6), decimal.Decimal("-2.00")),
    (3, 3.0, None, True, datetime.date(2026, 2, 28), datetime.datetime(2026, 1, 7, 0, 0, 1, 500000), None),
]


# ---------------------------------------------------------------- the node and the report

class Run:
    """The checks in the order they ran (ok, and what the check found), the costs and what was not run."""

    def __init__(self):
        self.checks, self.costs, self.not_run = {}, {}, []

    def check(self, name, fn, *args):
        try:
            ok, detail = fn(*args)
        except Exception as e:  # a door's refusal is the finding: the check says what it was
            ok, detail = False, f"{type(e).__name__}: {first_line(e)}"
            if os.environ.get("SQLA_CHECK_TRACE"):  # (for whoever debugs a check: the traceback too)
                traceback.print_exc()
        self.checks[name] = {"ok": bool(ok), "detail": detail}


def first_line(e):
    text_of = str(getattr(e, "orig", None) or e).strip()
    return text_of.splitlines()[0][:300] if text_of else type(e).__name__


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def ready(port):
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/ready", timeout=2) as r:
            return r.status == 200
    except OSError:
        return False


def start_node(bin_path, root):
    """A node of its own on a new lake in `root`: (process, HTTP port, Postgres port), once it is ready."""
    http, pg = free_port(), free_port()
    env = dict(os.environ, PONDRA_CACHE_DIR=os.path.join(root, "cache"))  # its SSD cache goes with the folder
    log_path = os.path.join(root, "node.log")
    cmd = [bin_path, "serve", "--lake", os.path.join(root, "lake"), "--addr", f"127.0.0.1:{http}",
           "--pg", f"127.0.0.1:{pg}"]
    with open(log_path, "w") as log:
        node = subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT, env=env)
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline and node.poll() is None:
        if ready(http):
            return node, http, pg
        time.sleep(0.2)
    stop_node(node)
    raise SystemExit(f"the node did not start; its log's end:\n{open(log_path).read()[-800:]}")


def stop_node(node):
    """Stopped by its PID: SIGTERM lets it drain and give its term back; SIGKILL if it doesn't end in time."""
    node.terminate()
    try:
        node.wait(timeout=60)
    except subprocess.TimeoutExpired:
        node.kill()
        node.wait()


class Spy:
    """The statements a DB-API connection sends to its node, in order (the client's BEGIN, COMMIT and ROLLBACK
    among them). Counted at the one place every DB-API statement passes: the client's `_run`."""

    def __init__(self):
        self.sent = []

    def __enter__(self):
        from pondra.client import Pondra
        self.orig = Pondra._run

        def spy(client, sql, *args, **kw):
            self.sent.append(" ".join(sql.split())[:60])
            return self.orig(client, sql, *args, **kw)

        Pondra._run = spy
        return self

    def __exit__(self, *exc):
        from pondra.client import Pondra
        Pondra._run = self.orig
        return False


# ---------------------------------------------------------------- the schema and the tables

def make_schema(engine):
    with engine.begin() as c:
        for stmt in DDL:
            c.exec_driver_sql(stmt)


def schema_made(engine):
    make_schema(engine)
    return True, f"schema {SCHEMA}: its tables and index made"


def reset_made(engine):
    reset(engine)
    return True, "the test tables made, as a fresh lake has them"


def reset(engine):
    """Every test table gone and made again, as a fresh lake has them (no table is reflected: a door that
    can't answer the catalog must not stop the rest)."""
    with engine.begin() as c:
        for table in reversed(Base.metadata.sorted_tables):
            c.exec_driver_sql(f"DROP TABLE IF EXISTS {table.name}")
        c.exec_driver_sql(f"DROP TABLE IF EXISTS {DBAPI_TABLE}")
    Base.metadata.create_all(engine, checkfirst=False)
    with engine.begin() as c:
        c.exec_driver_sql(f"CREATE TABLE {DBAPI_TABLE} (id INTEGER PRIMARY KEY, v TEXT)")


def snapshot(engine):
    """The inspector's answers for schema sqla_check: tables, columns (name, type, nullable), primary keys and
    indexes."""
    insp = inspect(engine)
    names = sorted(insp.get_table_names(schema=SCHEMA))
    return {
        "tables": names,
        "columns": {t: [[c["name"], str(c["type"]), c["nullable"]] for c in insp.get_columns(t, schema=SCHEMA)]
                    for t in names},
        "primary_key": {t: insp.get_pk_constraint(t, schema=SCHEMA)["constrained_columns"] for t in names},
        "indexes": {t: sorted([i["name"], i["column_names"], i["unique"]]
                              for i in insp.get_indexes(t, schema=SCHEMA)) for t in names},
    }


def postgres16(url):
    """The same DDL on PostgreSQL 16, read by its own inspector; the schema is dropped again."""
    engine = create_engine(url)
    try:
        with engine.begin() as c:
            c.exec_driver_sql(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
        make_schema(engine)
        return snapshot(engine)
    finally:
        with engine.begin() as c:
            c.exec_driver_sql(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
        engine.dispose()


# ---------------------------------------------------------------- SQLAlchemy: Core, ORM, inspector

def core_types(engine):
    with engine.begin() as c:
        c.execute(TYPES.insert(), TYPE_ROWS)
        back = {r.id: r._asdict() for r in c.execute(select(TYPES))}
    diffs = [(row["id"], k, back[row["id"]][k], v) for row in TYPE_ROWS for k, v in row.items()
             if back[row["id"]][k] != v]
    if diffs:
        return False, f"{len(diffs)} values differ, the first: {diffs[:3]}"
    return True, f"{len(TYPE_ROWS)} rows of {len(TYPES.c)} columns (13 types and the id; NULLs too) read back equal"


def orm_flush(engine):
    with Session(engine) as s:
        parent = Parent(name="ann", children=[Child(label="a"), Child(label="b")])
        s.add(parent)
        s.flush()
        pid = parent.id
        s.commit()
    with Session(engine) as s:
        found = s.get(Parent, pid)
        labels = sorted(c.label for c in found.children)
        owner = found.children[0].parent.name
    ok = labels == ["a", "b"] and owner == "ann"
    return ok, f"parent {pid} has children {labels}; a child's parent is {owner!r}"


def orm_rollback(engine):
    with Session(engine) as s:
        s.add(Parent(name="ghost"))
        s.flush()
        s.rollback()
    with Session(engine) as s:
        n = s.scalar(select(func.count()).select_from(Parent).where(Parent.name == "ghost"))
    return n == 0, f"rows left after a rollback: {n}"


def orm_returning(engine):
    with engine.begin() as c:
        one = c.execute(insert(Parent).values(name="ret").returning(Parent.id)).scalar_one()
        many = [tuple(r) for r in c.execute(insert(Parent).values([{"name": "r1"}, {"name": "r2"}])
                                            .returning(Parent.id, Parent.name))]
    ok = isinstance(one, int) and [n for _, n in many] == ["r1", "r2"]
    return ok, f"one RETURNING id {one}; a multi-row one {many}"


def pk_conflict(engine):
    with Session(engine) as s:
        p = Parent(name="pk")
        s.add(p)
        s.commit()
        pid = p.id
    try:
        with Session(engine) as s:
            s.add(Parent(id=pid, name="again"))
            s.flush()
    except IntegrityError as e:
        code = e.orig.sqlstate
        return code == "23505", f"IntegrityError with sqlstate {code}: {first_line(e.orig)}"
    return False, "no error for a held primary key"


def inspector_part(engine, key):
    """One part of the inspector's answers, as PostgreSQL 16 gives it. The columns are checked twice: by name
    and nullability, then by type (where a VARCHAR's length is what PostgreSQL keeps and Pondra's text doesn't)."""
    if key == "columns":
        got = {t: [c[:1] + c[2:] for c in cols] for t, cols in snapshot(engine)["columns"].items()}
        want = {t: [c[:1] + c[2:] for c in cols] for t, cols in EXPECTED["columns"].items()}
    elif key == "column_types":
        got = {t: [c[:2] for c in cols] for t, cols in snapshot(engine)["columns"].items()}
        want = {t: [c[:2] for c in cols] for t, cols in EXPECTED["columns"].items()}
    else:
        got, want = snapshot(engine)[key], EXPECTED[key]
    if got == want:
        return True, "as PostgreSQL 16 answers"
    return False, f"got {json.dumps(got)}; PostgreSQL 16 says {json.dumps(want)}"


def equals_postgres(engine, pg_snap):
    mine = snapshot(engine)
    diff = {k: {"pondra": mine[k], "postgres16": pg_snap[k]} for k in pg_snap if mine[k] != pg_snap[k]}
    return not diff, "the same answers" if not diff else json.dumps(diff)[:800]


def table_options(engine):
    with engine.connect() as c:
        shown = c.exec_driver_sql("SHOW CREATE TABLE sa_events").scalar()
    got = inspect(engine).get_table_options("sa_events")
    want = {"pondra_partition_by": "day(ts)", "pondra_cluster_by": "region, ts", "pondra_order_by": "ts"}
    plain = inspect(engine).get_table_options("sa_parent")
    ok = got == want and plain == {}
    return ok, f"SHOW CREATE TABLE: {' '.join(shown.split())[:300]}; read back {got}"


def serializable_on_connection(engine, counted):
    with Spy() as spy, engine.connect() as c:
        try:
            c.execution_options(isolation_level="SERIALIZABLE")
            c.execute(text("SELECT 1"))
        except ArgumentError as e:
            sent = f"; statements sent {spy.sent}" if counted else " (statements counted on the HTTP form only)"
            return "READ COMMITTED" in str(e) and "SELECT 1" not in spy.sent, f"refused by name: {first_line(e)}{sent}"
    return False, "no error for SERIALIZABLE"


# ---------------------------------------------------------------- pandas and bulk writes

def frame_for_pandas():
    return pd.DataFrame({
        "i": [1, 2, 3], "f": [1.5, -0.25, 3.0], "s": ["a", "b c", None], "b": [True, False, True],
        "d": [datetime.date(2026, 1, 5), datetime.date(1999, 12, 31), datetime.date(2026, 2, 28)],
        "ts": pd.to_datetime([datetime.datetime(2026, 1, 5, 10, 11, 12), datetime.datetime(2026, 1, 6),
                              datetime.datetime(2026, 1, 7, 0, 0, 1, 500000)]),
        "dec": [decimal.Decimal("1.10"), decimal.Decimal("-2.00"), None],
    })


def norm(v):
    """A cell as Python reads it: NaN and NaT are None, a numpy scalar is its Python value."""
    if v is None or v is pd.NaT or (isinstance(v, float) and v != v):
        return None
    if isinstance(v, pd.Timestamp):
        return v.to_pydatetime()
    if type(v).__module__ == "numpy" and hasattr(v, "item"):
        return v.item()
    return v


def rows_of(frame):
    return [tuple(norm(v) for v in row) for row in frame.itertuples(index=False)]


def pandas_to_sql(engine):
    with engine.begin() as c:
        c.exec_driver_sql("DROP TABLE IF EXISTS sa_pandas")
    frame = frame_for_pandas()
    frame.to_sql("sa_pandas", engine, index=False, if_exists="fail", dtype={
        "i": BigInteger, "f": Float, "s": String(20), "b": Boolean, "d": Date, "ts": DateTime, "dec": Numeric(10, 2)})
    back = pd.read_sql("SELECT * FROM sa_pandas ORDER BY i", engine, coerce_float=False)
    ok = rows_of(back) == rows_of(frame) and list(back.columns) == PANDAS_COLUMNS
    return ok, "to_sql, then read_sql: the same seven columns and rows" if ok else f"rows {rows_of(back)}"


def pandas_read_sql(engine):
    with engine.begin() as c:
        c.exec_driver_sql("DROP TABLE IF EXISTS sa_pandas_read")
        c.exec_driver_sql("CREATE TABLE sa_pandas_read (i BIGINT, f DOUBLE PRECISION, s VARCHAR(20), b BOOLEAN, "
                          "d DATE, ts TIMESTAMP, dec NUMERIC(10, 2))")
        c.execute(text("INSERT INTO sa_pandas_read VALUES (:i, :f, :s, :b, :d, :ts, :dec)"),
                  [dict(zip(PANDAS_COLUMNS, row)) for row in PANDAS_ROWS])
    back = pd.read_sql("SELECT * FROM sa_pandas_read ORDER BY i", engine, coerce_float=False)
    ok = rows_of(back) == PANDAS_ROWS
    return ok, "read_sql of rows written by Core: the same values, types and NULLs" if ok else f"got {rows_of(back)}"


def pandas_read_sql_autocommit(url):
    """read_sql with the engine in AUTOCOMMIT: each statement alone. pandas' read_sql first asks has_table with
    the SQL text as a name and swallows that error; over HTTP the probe fails (no pg_catalog), and in a
    transaction the failure aborts the read that follows. Each statement alone shows the read itself."""
    engine = create_engine(url, isolation_level="AUTOCOMMIT")
    try:
        return pandas_read_sql(engine)
    finally:
        engine.dispose()


def executemany(engine, counted):
    rows = [{"id": i, "label": f"r{i}"} for i in range(1, 1001)]
    with Spy() as spy, engine.begin() as c:
        t0 = time.perf_counter()
        c.execute(MANY.insert(), rows)
        took = time.perf_counter() - t0
        n = c.execute(select(func.count()).select_from(MANY)).scalar()
    inserts = sum(1 for sent in spy.sent if sent.startswith("INSERT")) if counted else None
    counted_text = f"{len(spy.sent)} statements sent, {inserts} of them INSERT" if counted \
        else "statements not counted on this form (psycopg pipelines the rows)"
    ok = n == 1000 and (inserts is None or inserts == 1)
    return ok, f"{n} rows in {took * 1000:.0f} ms; {counted_text}"


# ---------------------------------------------------------------- the DB-API module, over HTTP

def begin_first(url):
    con = dbapi.connect(url)
    cur = con.cursor()
    with Spy() as spy:
        cur.execute("SELECT 1")
        cur.fetchall()
        con.commit()
        con.commit()
    con.close()
    return spy.sent == ["BEGIN", "SELECT 1", "COMMIT"], f"sent {spy.sent}"


def quiet_unless_begun(url):
    con = dbapi.connect(url)
    cur = con.cursor()
    with Spy() as spy:
        con.commit()
        con.rollback()
        cur.execute("SELECT 1")
        con.rollback()
        con.rollback()
    con.close()
    return spy.sent == ["BEGIN", "SELECT 1", "ROLLBACK"], f"sent {spy.sent}"


def autocommit_alone(url):
    con = dbapi.connect(url)
    con.autocommit = True
    cur = con.cursor()
    with Spy() as spy:
        cur.execute("SELECT 1")
        cur.execute("SELECT 2")
    con.close()
    return spy.sent == ["SELECT 1", "SELECT 2"], f"sent {spy.sent}"


def repeatable_read(url):
    con = dbapi.connect(url)
    con.isolation_level = "REPEATABLE READ"
    with Spy() as spy:
        con.cursor().execute("SELECT 1")
    con.close()
    return spy.sent[:1] == ["BEGIN ISOLATION LEVEL REPEATABLE READ"], f"sent {spy.sent}"


def serializable_refused(url):
    con = dbapi.connect(url)
    con.isolation_level = "SERIALIZABLE"
    cur = con.cursor()
    refused = None
    with Spy() as spy:
        try:
            cur.execute("SELECT 1")
        except dbapi.NotSupportedError as e:
            refused = str(e)
    con.close()
    return refused is not None and spy.sent == [], f"refused {refused!r}; sent {spy.sent}"


def close_rolls_back(url):
    con = dbapi.connect(url)
    con.cursor().execute(f"INSERT INTO {DBAPI_TABLE} (id, v) VALUES ($1, $2)", (900, "open"))
    con.close()
    other = dbapi.connect(url)
    cur = other.cursor()
    cur.execute(f"SELECT count(*) FROM {DBAPI_TABLE} WHERE id = $1", (900,))
    n = cur.fetchone()[0]
    other.close()
    return n == 0, f"rows of the closed, uncommitted transaction left: {n}"


def description_rowcount(url):
    con = dbapi.connect(url)
    con.autocommit = True
    cur = con.cursor()
    cur.execute("SELECT $1 + 1 AS x, $2 AS word", (41, "b"))
    names, types = [d[0] for d in cur.description], [d[1] for d in cur.description]
    read = (cur.rowcount, cur.fetchall())
    cur.execute(f"INSERT INTO {DBAPI_TABLE} (id, v) VALUES ($1, $2)", (50, "x"))
    write = (cur.description, cur.rowcount)
    cur.executemany(f"INSERT INTO {DBAPI_TABLE} (id, v) VALUES ($1, $2)", [(51, "a"), (52, "b"), (53, "c")])
    many = cur.rowcount
    cur.execute(f"DELETE FROM {DBAPI_TABLE} WHERE id = -1")
    none = cur.rowcount
    con.close()
    ok = (names, types, read, write, many, none) == (
        ["x", "word"], [dbapi.NUMBER, dbapi.STRING], (1, [(42, "b")]), (None, 1), 3, 0)
    return ok, f"names {names}, types {types}, read {read}, write {write}, executemany {many}, delete {none}"


def fetch_apis(url):
    con = dbapi.connect(url)
    con.autocommit = True
    cur = con.cursor()
    cur.execute(f"DELETE FROM {DBAPI_TABLE} WHERE id >= 50")  # (its own three rows, whatever ran before)
    cur.executemany(f"INSERT INTO {DBAPI_TABLE} (id, v) VALUES ($1, $2)", [(51, "a"), (52, "b"), (53, "c")])
    cur.execute(f"SELECT id FROM {DBAPI_TABLE} WHERE id >= 51 ORDER BY id")
    got = (cur.fetchone(), cur.fetchmany(1), cur.fetchall(), cur.fetchone(), cur.arraysize)
    cur.execute(f"SELECT id FROM {DBAPI_TABLE} WHERE id >= 51 ORDER BY id")
    iterated = list(cur)
    con.close()
    ok = got == ((51,), [(52,)], [(53,)], None, 1) and iterated == [(51,), (52,), (53,)]
    return ok, f"fetchone, fetchmany(1), fetchall, the end, arraysize: {got}; iterated {iterated}"


def live_errors(url):
    con = dbapi.connect(url)
    con.autocommit = True
    cur = con.cursor()
    cur.execute(f"INSERT INTO {DBAPI_TABLE} (id, v) VALUES (60, 'x')")
    cases = {"23505": f"INSERT INTO {DBAPI_TABLE} (id, v) VALUES (60, 'again')",
             "42P01": "SELECT * FROM no_such_table_sqla",
             "22P02": "SELECT CAST('x' AS INTEGER)",
             "0A000": f"CREATE TABLE sqla_ref (a INTEGER REFERENCES {DBAPI_TABLE} (id))"}
    got = {}
    for code, sql in cases.items():
        try:
            cur.execute(sql)
            got[code] = "no error"
        except dbapi.Error as e:
            got[code] = [type(e).__name__, e.sqlstate]
    con.close()
    want = {"23505": ["IntegrityError", "23505"], "42P01": ["ProgrammingError", "42P01"],
            "22P02": ["DataError", "22P02"], "0A000": ["NotSupportedError", "0A000"]}
    return got == want, f"{got}"


def aborted_until_rollback(url):
    con = dbapi.connect(url)
    cur = con.cursor()
    first = after = None
    cur.execute(f"INSERT INTO {DBAPI_TABLE} (id, v) VALUES (61, 'y')")
    try:
        cur.execute(f"INSERT INTO {DBAPI_TABLE} (id, v) VALUES (61, 'again')")
    except dbapi.IntegrityError as e:
        first = e.sqlstate
    try:
        cur.execute("SELECT 1")
        after = "answered"
    except dbapi.DatabaseError as e:
        after = [type(e).__name__, e.sqlstate]
    con.rollback()
    cur.execute(f"SELECT count(*) FROM {DBAPI_TABLE} WHERE id = 61")
    n = cur.fetchone()[0]
    con.close()
    ok = first == "23505" and after == ["DatabaseError", "25P02"] and n == 0
    return ok, f"duplicate {first}; the next statement {after}; rows after the rollback {n}"


def sqlstate_classes():
    want = {"23000": dbapi.IntegrityError, "22012": dbapi.DataError, "42601": dbapi.ProgrammingError,
            "3F000": dbapi.ProgrammingError, "0A000": dbapi.NotSupportedError, "40001": dbapi.OperationalError,
            "53100": dbapi.OperationalError, "55P03": dbapi.OperationalError, "57014": dbapi.OperationalError,
            "08006": dbapi.OperationalError, "25P02": dbapi.DatabaseError, "XX000": dbapi.DatabaseError}
    got = {code: dbapi._error(dbapi.PondraError("m", sqlstate=code)) for code in want}
    bad = {code: type(e).__name__ for code, e in got.items() if type(e) is not want[code]
           or e.sqlstate != code}
    return not bad, "every class as PEP 249 gives it, with its sqlstate" if not bad else f"wrong: {bad}"


def dict_params_refused(url):
    con = dbapi.connect(url)
    cur = con.cursor()
    with Spy() as spy:
        try:
            cur.execute("SELECT $1", {"a": 1})
            refused = None
        except dbapi.ProgrammingError as e:
            refused = str(e)
    con.close()
    return refused is not None and spy.sent == [], f"refused: {refused!r}; sent {spy.sent}"


def positional_params(url):
    con = dbapi.connect(url)
    con.autocommit = True
    cur = con.cursor()
    cur.execute("SELECT $2 AS b, $1 AS a", (1, 2))
    got = cur.fetchall()
    con.close()
    return got == [(2, 1)], f"SELECT $2 AS b, $1 AS a with (1, 2): {got}"


def numpy_params(url):
    con = dbapi.connect(url)
    con.autocommit = True
    cur = con.cursor()
    cur.execute("SELECT $1 + 1 AS x, $2 AS flag, $3 AS at, $4 AS took",
                (np.int64(41), np.bool_(True), np.datetime64("2024-01-02T03:04:05.123456789"), np.timedelta64(1500, "ms")))
    got = cur.fetchall()
    con.close()
    want = [(42, True, datetime.datetime(2024, 1, 2, 3, 4, 5, 123456), datetime.timedelta(seconds=1.5))]
    return got == want, f"numpy scalars as parameters (a datetime64 in nanoseconds, a timedelta64): {got}"


def non_finite_params(url):
    """NaN and the infinities as parameters: JSON has none of them, so the client sends each as SQL."""
    con = dbapi.connect(url)
    con.autocommit = True
    cur = con.cursor()
    cur.execute("SELECT $1 AS a, $2 AS b, $3 AS c", (float("nan"), float("inf"), np.float64("-inf")))
    got = cur.fetchall()
    con.close()
    a, b, c = got[0]
    return a != a and b == float("inf") and c == float("-inf"), f"NaN, inf and -inf as parameters: {got}"


# What a cursor reads, as text: each answer's column names, type codes and rows (`rows_without_pyarrow`).
ROWS_PROBE = r"""
import json, sys
if sys.argv[2] == "without":
    sys.modules["pyarrow"] = None  # (as if it weren't installed: the client's find_spec answers None)
from pondra import dbapi
con = dbapi.connect(sys.argv[1])
con.autocommit = True
cur = con.cursor()
out = []
for sql in sys.argv[3:]:
    cur.execute(sql)
    rows = cur.fetchall()
    out.append([[(d[0], getattr(d[1], "name", d[1])) for d in cur.description], len(rows), repr(rows[:3]), repr(rows[-1:])])
print(json.dumps(out))
"""


def rows_without_pyarrow(url):
    """Without pyarrow a cursor reads the node's typed JSON: the same columns (one NULL in every row too,
    and an empty answer's), types, values and row count as with it, past the first page's 10,000 rows too."""
    queries = ["SELECT NULL::INT AS a, 1 AS b, CAST('1.50' AS DECIMAL(10, 2)) AS d, DATE '2024-01-02' AS dt, "
               "TIMESTAMP '2024-01-02 03:04:05.123456' AS ts, 9007199254740993 AS big, 'x' AS s, TRUE AS t, 2.5::DOUBLE AS f",
               "SELECT * FROM (VALUES (NULL, 1), (2, 3)) AS v(a, b)",
               f"SELECT id, v FROM {DBAPI_TABLE} WHERE false",
               "SELECT value AS n FROM generate_series(1, 25000) ORDER BY value"]
    env = {**os.environ, "PYTHONPATH": os.path.join(os.path.dirname(HERE), "python")}
    said = {how: subprocess.run([sys.executable, "-c", ROWS_PROBE, url, how, *queries], capture_output=True, text=True, env=env, timeout=120)
            for how in ("with", "without")}
    if any(r.returncode for r in said.values()):
        return False, "; ".join(f"{how}: {first_line(r.stderr.strip().splitlines()[-1] if r.stderr.strip() else r.returncode)}" for how, r in said.items())
    got = {how: json.loads(r.stdout) for how, r in said.items()}
    return got["with"] == got["without"], f"without pyarrow {got['without'][:2]}" + ("" if got["with"] == got["without"] else f", with it {got['with'][:2]}")


def echo_off(url):
    con = dbapi.connect(url)
    echo = con._client.echo
    con.close()
    return echo is False, f"the client's echo is {echo}: a procedure's notices are not printed"


def dead_node(_url):
    url = f"http://127.0.0.1:{free_port()}"  # a port nothing listens on
    con = dbapi.connect(url, retry_secs=0)  # (connect doesn't ask the node: the first statement finds it gone)
    try:
        con.cursor().execute("SELECT 1")
        return False, "answered by a node that isn't there"
    except dbapi.OperationalError as e:
        return True, f"OperationalError at the first statement: {first_line(e)}"
    finally:
        con.close()


DBAPI_CHECKS = [
    ("dbapi.begin_on_first_statement", begin_first),
    ("dbapi.commit_and_rollback_send_only_when_begun", quiet_unless_begun),
    ("dbapi.autocommit_sends_each_statement_alone", autocommit_alone),
    ("dbapi.repeatable_read_begins_at_that_level", repeatable_read),
    ("dbapi.serializable_refused_before_sending", serializable_refused),
    ("dbapi.close_rolls_back", close_rolls_back),
    ("dbapi.description_and_rowcount", description_rowcount),
    ("dbapi.fetch_apis", fetch_apis),
    ("dbapi.errors_by_sqlstate_live", live_errors),
    ("dbapi.failed_statement_aborts_until_rollback", aborted_until_rollback),
    ("dbapi.errors_by_sqlstate_class", lambda _url: sqlstate_classes()),
    ("dbapi.dict_parameters_refused", dict_params_refused),
    ("dbapi.positional_parameters", positional_params),
    ("dbapi.numpy_scalars_as_parameters", numpy_params),
    ("dbapi.non_finite_floats_as_parameters", non_finite_params),
    ("dbapi.rows_without_pyarrow", rows_without_pyarrow),
    ("dbapi.echo_off_by_default", echo_off),
    ("dbapi.node_gone_is_operational_error", dead_node),
]


# ---------------------------------------------------------------- the batteries and the costs

def battery(run, label, url, pg_snap, counted):
    """Every SQLAlchemy check in one form, on the lake the form reaches."""
    engine = create_engine(url)
    try:
        run.check(f"{label}.reset_tables", reset_made, engine)
        run.check(f"{label}.core_types", core_types, engine)
        run.check(f"{label}.orm_flush_and_relationship", orm_flush, engine)
        run.check(f"{label}.orm_rollback_leaves_nothing", orm_rollback, engine)
        run.check(f"{label}.orm_insert_returning", orm_returning, engine)
        run.check(f"{label}.primary_key_conflict_23505", pk_conflict, engine)
        for key in ("tables", "columns", "column_types", "primary_key", "indexes"):
            run.check(f"{label}.inspector_{key}", inspector_part, engine, key)
        if pg_snap is not None:
            run.check(f"{label}.inspector_equals_postgres16", equals_postgres, engine, pg_snap)
        run.check(f"{label}.table_options_written_and_read", table_options, engine)
        run.check(f"{label}.serializable_refused_by_name", serializable_on_connection, engine, counted)
        run.check(f"{label}.pandas_to_sql_and_read_sql", pandas_to_sql, engine)
        run.check(f"{label}.pandas_read_sql", pandas_read_sql, engine)
        run.check(f"{label}.pandas_read_sql_in_autocommit", pandas_read_sql_autocommit, url)
        run.check(f"{label}.executemany_1000_rows", executemany, engine, counted)
    finally:
        engine.dispose()


def median_ms(fn, n=500):
    times = []
    for _ in range(n):
        t0 = time.perf_counter()
        fn()
        times.append((time.perf_counter() - t0) * 1000)
    return round(statistics.median(times), 3)


def costs(run, http_url, engine):
    """What the layers cost (reported, not judged): SELECT 1 through SQLAlchemy against the client's own
    `collect()`, and a million rows through `read_sql` (in AUTOCOMMIT, see pandas_read_sql_autocommit)
    against `to_pandas()`. Its equality is a check; an error here is reported under costs."""
    db = pondra.connect(http_url)
    with engine.connect() as c:
        run.costs["select1_sqlalchemy_ms"] = median_ms(lambda: c.execute(text("SELECT 1")).all())
    run.costs["select1_client_ms"] = median_ms(lambda: db.sql("SELECT 1").collect())
    con = dbapi.connect(http_url)
    con.autocommit = True
    cur = con.cursor()
    cur.execute(f"DROP TABLE IF EXISTS {BIG}")
    cur.execute(f"CREATE TABLE {BIG} AS SELECT CAST(i AS BIGINT) AS id, CAST(i AS DOUBLE) AS x, "
                "'row ' || CAST(i AS VARCHAR) AS label FROM unnest(range(0, 1000000)) AS t(i)")
    con.close()
    auto = create_engine(engine.url, isolation_level="AUTOCOMMIT")
    t0 = time.perf_counter()
    via_sqlalchemy = pd.read_sql(f"SELECT * FROM {BIG}", auto, coerce_float=False)
    auto.dispose()
    run.costs["read_sql_1m_rows_s"] = round(time.perf_counter() - t0, 3)
    t0 = time.perf_counter()
    via_client = db.sql(f"SELECT * FROM {BIG}").to_pandas()
    run.costs["client_to_pandas_1m_rows_s"] = round(time.perf_counter() - t0, 3)
    same = len(via_sqlalchemy) == len(via_client) == 1_000_000 and \
        int(via_sqlalchemy["id"].sum()) == int(via_client["id"].sum())
    run.check("http.read_sql_1m_rows_equal_to_client", lambda: (same, f"{len(via_sqlalchemy)} and "
              f"{len(via_client)} rows; the id sums {int(via_sqlalchemy['id'].sum())} and "
              f"{int(via_client['id'].sum())}"))


def parse():
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--bin", default=os.environ.get("PONDRA_BIN") or os.path.join(HERE, "..", "target", "release", "pondra"), help="the node binary (never built here; PONDRA_BIN, else the release build)")
    p.add_argument("--lib", help="a pondra_lib: the in-process form runs too")
    p.add_argument("--postgres", help="a PostgreSQL 16 SQLAlchemy URL to compare the inspector with")
    return p.parse_args()


def main():
    args = parse()
    if not os.path.exists(args.bin):
        raise SystemExit(f"no node binary at {args.bin}")
    if args.lib:
        os.environ["PONDRA_LIBRARY"] = os.path.abspath(args.lib)
    run = Run()
    root = tempfile.mkdtemp(prefix="pondra-sqla-")
    node = None
    try:
        node, http, pg = start_node(args.bin, root)
        pg_snap = None
        if args.postgres:
            try:
                pg_snap = postgres16(args.postgres)
                run.checks["postgres16.ran_the_same_ddl"] = {"ok": True, "detail": "the DDL ran and was read back"}
            except Exception as e:
                run.checks["postgres16.ran_the_same_ddl"] = {"ok": False, "detail": first_line(e)}
        else:
            run.not_run.append("inspector_equals_postgres16: no --postgres given")
        http_url = f"http://127.0.0.1:{http}"
        http_engine = create_engine(f"pondra+http://127.0.0.1:{http}")
        run.check("http.schema_made", schema_made, http_engine)
        run.check("http.tables_made", reset_made, http_engine)
        for name, fn in DBAPI_CHECKS:
            run.check(name, fn, http_url)
        battery(run, "http", f"pondra+http://127.0.0.1:{http}", pg_snap, counted=True)
        battery(run, "psycopg", f"pondra+psycopg://pondra@127.0.0.1:{pg}/lake", pg_snap, counted=False)
        try:
            costs(run, http_url, http_engine)
        except Exception as e:  # reported under costs: the costs don't decide the run
            run.costs["error"] = f"{type(e).__name__}: {first_line(e)}"
        if args.lib:
            lib_url = f"pondra:///{root}/lake2"
            run.check("in_process.schema_made", schema_made, create_engine(lib_url))
            battery(run, "in_process", lib_url, pg_snap, counted=False)
        else:
            run.not_run.append("in-process (pondra:///folder with the library in this process): no --lib given; "
                               "no pondra_lib is built here")
        run.not_run.append("alembic upgrade and downgrade: alembic is not installed here (not installed for this run)")
    finally:
        if node is not None:
            stop_node(node)
        shutil.rmtree(root, ignore_errors=True)
    failed = [k for k, v in run.checks.items() if not v["ok"]]
    doc = {"versions": {"sqlalchemy": sqlalchemy.__version__, "pandas": pd.__version__,
                        "pyarrow": pyarrow.__version__, "numpy": np.__version__},
           "failed": failed, "checks": run.checks, "costs": run.costs, "not_run": run.not_run}
    print(json.dumps(doc, indent=2, ensure_ascii=False, default=str))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
