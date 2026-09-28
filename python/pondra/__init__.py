"""Pondra from Python: SQL and frames over a lake, into pandas / Polars / Arrow; exactly-once
appends, change feeds and key lookups; functions and procedures in SQL or Python. Pure Python over
a node's HTTP API; pyarrow for results. `pip install pondra` also installs the `pondra` binary.

    import pondra
    from pondra import col

    con = pondra.local("lake")                                  # a node on ./lake, here; or
    con = pondra.connect("http://127.0.0.1:8080", token=None)   # one running somewhere
    con.sql("CREATE TABLE events (user VARCHAR, amount BIGINT)")
    con.append("events", [{"user": "ann", "amount": 5}])      # or a pandas / Polars / Arrow table
    top = con.table("events").group_by("user").agg(col("amount").sum()).sort("amount", descending=True)
    top.to_pandas()                                             # one SQL statement runs (top.sql)
    con.sql("SELECT * FROM top WHERE amount > $min", min=3).to_polars()   # SQL reads Python names
    for row in con.watch("events"): ...                         # new rows as they commit
    for rows in con.live("SELECT user, sum(amount) FROM events GROUP BY user"): ...   # an answer, kept current
    con.read_parquet("s3://bucket/2026/*.parquet").write_delta("out/events")      # read_* / write_* (scan_*, sink_* too)

    @con.function                                               # a Python function for SQL and frames
    def slug(title: str) -> str: ...
    @con.procedure                                              # a procedure, run on the node as its caller
    def nightly(day: date): pondra.sql("INSERT INTO daily SELECT … WHERE ts::DATE = $day", day=day)
    pondra.sql("SELECT slug(title) FROM posts")                 # the newest connection (in a procedure: its caller's)

`pondra.spark` has PySpark's names for the same frames; `%load_ext pondra` gives notebooks `%%sql`.
Also over the Postgres protocol (`pondra serve --pg 0.0.0.0:5432`) with psycopg, SQLAlchemy, etc.
"""
from .client import Pondra, Result, Run, binary, connect, current, local
from .frame import Expr, Frame, GroupBy, coalesce, col, concat, concat_str, fn, lit, sql_expr, when
from .frame import all, count, first, last, len, max, mean, median, min, n_unique, sum  # noqa: A004 (Polars' names)

__version__ = "0.25.0"
__all__ = ["connect", "local", "current", "sql", "table", "read_parquet", "read_csv", "read_json", "read_delta", "read_iceberg", "call", "secret", "fn", "Pondra", "Result", "Run", "Frame", "Expr", "GroupBy", "col", "lit", "when",
           "sql_expr", "coalesce", "concat", "concat_str",
           "all", "count", "first", "last", "len", "max", "mean", "median", "min", "n_unique", "sum"]


def sql(query, job=None, **names):
    """SQL on the current connection (`current()`: in a procedure, its caller's; elsewhere the newest
    made), as `duckdb.sql` runs on DuckDB's: code moves between a notebook and a procedure as it is."""
    import sys
    return current()._sql(query, job, names, sys._getframe(1))


def table(name):
    """A table of the current connection's lake, as a frame."""
    return current().table(name)


def read_parquet(source, **options):
    """Parquet files as a frame, on the current connection (`Pondra.read_parquet`)."""
    return current().read_parquet(source, **options)


def read_csv(source, **options):
    """CSV files as a frame, on the current connection (`Pondra.read_csv`)."""
    return current().read_csv(source, **options)


def read_json(source, **options):
    """JSON lines as a frame, on the current connection (`Pondra.read_json`)."""
    return current().read_json(source, **options)


def read_delta(source, **options):
    """A Delta table as a frame, on the current connection (`Pondra.read_delta`)."""
    return current().read_delta(source, **options)


def read_iceberg(source, **options):
    """An Iceberg table as a frame, on the current connection (`Pondra.read_iceberg`)."""
    return current().read_iceberg(source, **options)


scan_parquet, scan_csv, scan_delta, scan_iceberg = read_parquet, read_csv, read_delta, read_iceberg  # (Polars' names)
scan_ndjson = read_ndjson = read_json


def call(name, *args, **kwargs):
    """A procedure, called on the current connection (`Pondra.call`)."""
    return current().call(name, *args, **kwargs)


def secret(name):
    """A secret's values, in a procedure (`Pondra.secret`)."""
    return current().secret(name)


def load_ipython_extension(ipython):
    """`%load_ext pondra`: `%%sql` cells and `%sql` lines (see `magic.py`)."""
    from .magic import register
    register(ipython)
