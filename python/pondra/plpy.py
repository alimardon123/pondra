"""PL/Python's `plpy` in Pondra's routines (ADR-027), so a Postgres PL/Python function or procedure
runs as it is: `plpy.execute` runs SQL as the caller (in a procedure: functions have no
connection), `plpy.notice` and its kin send the caller a notice, `plpy.error` fails the call.
Every routine has it as `plpy`, with `SD` (this routine's) and `GD` (the worker's) dictionaries.

    CREATE PROCEDURE archive(days INT) LANGUAGE plpython3u AS $$
        rv = plpy.execute("SELECT count(*) AS n FROM orders WHERE ts < now() - $1 * INTERVAL '1 day'", [days])
        plpy.notice(f"{rv[0]['n']} to archive")
    $$;
"""
import sys

GD = {}  # (shared by every routine this worker runs, as PL/Python's)


class Error(Exception):
    pass


class Fatal(Error):
    pass


class SPIError(Error):
    pass


class Result(list):
    """`plpy.execute`'s answer: its rows (dicts), and PL/Python's `nrows()`, `status()`, `colnames()`."""

    def __init__(self, rows, status="SELECT", n=None):
        super().__init__(rows)
        self._status, self._n, self._cols = status, len(rows) if n is None else n, list(rows[0]) if rows else []

    def nrows(self):
        return self._n

    def status(self):
        return self._status

    def colnames(self):
        return self._cols


class Plan:
    """`plpy.prepare`'s: a statement with `$1`, `$2`… kept to run with values."""

    def __init__(self, query, types):
        self.query, self.types = query, list(types or [])

    def execute(self, args=None, limit=None):
        return execute(self, args, limit)


def prepare(query, types=None):
    return Plan(query, types)


def execute(query, args=None, limit=None):
    """Run a statement now, as the caller: a query's rows, or a write's count (`nrows()`)."""
    from .client import current
    con = current()
    sql = query.query if isinstance(query, Plan) else query
    out = con._run(sql, {str(i + 1): v for i, v in enumerate(args or [])})
    if isinstance(out, dict):
        return Result([], status=sql.split(None, 1)[0].upper() if sql.strip() else "", n=out.get("rows", 0))
    rows = out.rows()
    return Result(rows[:limit] if limit else rows)


def cursor(query, args=None):
    return iter(execute(query, args))


def _say(text):
    say = getattr(sys.stdout, "notice", None)
    say(str(text)) if say else print(text)


def notice(*msg):
    _say(" ".join(map(str, msg)))


info = warning = notice


def debug(*msg):
    print(" ".join(map(str, msg)), file=sys.stderr)  # (Postgres's DEBUG and LOG: the server's log)


log = debug


def error(*msg):
    raise Error(" ".join(map(str, msg)))


def fatal(*msg):
    raise Fatal(" ".join(map(str, msg)))


def quote_literal(s):
    return "'" + str(s).replace("'", "''") + "'"


def quote_nullable(s):
    return "NULL" if s is None else quote_literal(s)


def quote_ident(s):
    return '"' + str(s).replace('"', '""') + '"'


def subtransaction():
    raise NotImplementedError("plpy.subtransaction: Pondra commits each statement on its own, and can't roll several back together")
