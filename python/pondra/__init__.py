"""Pondra from Python: SQL and frames over a lake, into pandas / Polars / Arrow; exactly-once
appends, change feeds and key lookups; macros and procedures. Pure Python over a node's HTTP API;
pyarrow for results. `pip install pondra` also installs the `pondra` binary itself.

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

`pondra.spark` has PySpark's names for the same frames; `%load_ext pondra` gives notebooks `%%sql`.
Also over the Postgres protocol (`pondra serve --pg 0.0.0.0:5432`) with psycopg, SQLAlchemy, etc.
"""
from .client import Pondra, Result, binary, connect, local
from .frame import Expr, Frame, GroupBy, coalesce, col, concat, concat_str, lit, sql_expr, when
from .frame import all, count, first, last, len, max, mean, median, min, n_unique, sum  # noqa: A004 (Polars' names)

__version__ = "0.22.2"
__all__ = ["connect", "local", "Pondra", "Result", "Frame", "Expr", "GroupBy", "col", "lit", "when", "sql_expr", "coalesce", "concat", "concat_str",
           "all", "count", "first", "last", "len", "max", "mean", "median", "min", "n_unique", "sum"]


def load_ipython_extension(ipython):
    """`%load_ext pondra`: `%%sql` cells and `%sql` lines (see `magic.py`)."""
    from .magic import register
    register(ipython)
