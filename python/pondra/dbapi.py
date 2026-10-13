"""PEP 249 (the Python Database API, version 2.0) over `pondra.connect()`: a connection is a session
of its own on a node, and a cursor runs statements on it. SQLAlchemy (`dialect.py`) and other DB-API
tools use this. It imports only the standard library and `pondra`; rows come from pyarrow when it is
installed, as `Result` gets them, and as the node's JSON rows (names only) when it isn't.

    from pondra import dbapi
    con = dbapi.connect("http://127.0.0.1:8080", token="…")   # or a folder, or no target: a lake in memory
    cur = con.cursor()
    cur.execute("INSERT INTO t VALUES ($1, $2)", (1, "a"))
    con.commit()
    cur.execute("SELECT * FROM t WHERE id > $1", (0,))
    cur.fetchall()
"""
import datetime
import re
import time

from .client import PondraError, _code, connect as _connect

apilevel = "2.0"
threadsafety = 1  # threads may share this module, not a connection
paramstyle = "numeric_dollar"  # $1, $2, …: Postgres's own; POST /sql binds them from {"1": …} (routines::bind)


# ---------------------------------------------------------------- errors (PEP 249's classes)

class Warning(Exception):  # noqa: A001 (PEP 249's name)
    pass


class Error(Exception):
    def __init__(self, message="", sqlstate=None):
        super().__init__(message)
        self.sqlstate = self.pgcode = sqlstate  # (psycopg's .sqlstate; psycopg2's .pgcode: both kept)


class InterfaceError(Error):
    pass


class DatabaseError(Error):
    pass


class DataError(DatabaseError):
    pass


class OperationalError(DatabaseError):
    pass


class IntegrityError(DatabaseError):
    pass


class InternalError(DatabaseError):
    pass


class ProgrammingError(DatabaseError):
    pass


class NotSupportedError(DatabaseError):
    pass


# A node's SQLSTATE class decides the PEP 249 class; any other class is a plain DatabaseError.
_BY_CLASS = {"23": IntegrityError, "22": DataError, "42": ProgrammingError, "3F": ProgrammingError, "0A": NotSupportedError,
             "40": OperationalError, "53": OperationalError, "55": OperationalError, "57": OperationalError, "08": OperationalError}


def _error(e):
    """A client's error as the PEP 249 error of its SQLSTATE class (a network error is an OperationalError)."""
    if isinstance(e, PondraError):
        code = e.sqlstate or ""
        return _BY_CLASS.get(code[:2], DatabaseError)(str(e), code)
    return OperationalError(str(e))


# ---------------------------------------------------------------- type objects and constructors

class DBAPIType:
    """PEP 249's type objects: a column's type code equals the one it names (`code == NUMBER`)."""

    def __init__(self, name):
        self.name = name

    def __eq__(self, other):
        return isinstance(other, DBAPIType) and other.name == self.name

    def __hash__(self):
        return hash(self.name)

    def __repr__(self):
        return f"<dbapi type {self.name}>"


STRING, BINARY, NUMBER, DATETIME, ROWID = (DBAPIType(n) for n in ("STRING", "BINARY", "NUMBER", "DATETIME", "ROWID"))


def Date(year, month, day):  # noqa: N802 (PEP 249's names)
    return datetime.date(year, month, day)


def Time(hour, minute, second):  # noqa: N802
    return datetime.time(hour, minute, second)


def Timestamp(*parts):  # noqa: N802
    return datetime.datetime(*parts)


def DateFromTicks(ticks):  # noqa: N802
    t = time.localtime(ticks)
    return Date(t.tm_year, t.tm_mon, t.tm_mday)


def TimeFromTicks(ticks):  # noqa: N802
    t = time.localtime(ticks)
    return Time(t.tm_hour, t.tm_min, t.tm_sec)


def TimestampFromTicks(ticks):  # noqa: N802
    return Timestamp(*time.localtime(ticks)[:6])


def Binary(value):  # noqa: N802
    return bytes(value)


# ---------------------------------------------------------------- values and statements

_BEGIN = {"READ COMMITTED": "BEGIN", "REPEATABLE READ": "BEGIN ISOLATION LEVEL REPEATABLE READ"}  # what Pondra runs
_STARTS, _ENDS = {"begin", "start"}, {"commit", "end", "rollback", "abort"}


def _first_word(sql):
    m = re.match(r"\s*([a-z]+)", _code(sql), re.I)
    return m.group(1).lower() if m else ""


def _tidy(table):
    """An Arrow answer as Python reads it: nanoseconds to microseconds (a datetime holds nothing finer, and
    pyarrow would give pandas' Timestamp for them)."""
    import pyarrow as pa
    for i, field in enumerate(table.schema):
        t = field.type
        if pa.types.is_timestamp(t) and t.unit == "ns":
            target = pa.timestamp("us", t.tz)
        elif pa.types.is_time(t) and t.unit == "ns":
            target = pa.time64("us")
        else:
            continue
        table = table.set_column(i, field.with_type(target), table.column(i).cast(target, safe=False))
    return table


def _interval(v):
    """A month-day-nano interval as a timedelta. A month has no fixed length, so an interval with months
    stays as pyarrow gives it (months, days, nanoseconds)."""
    if v is None or v.months:
        return v
    return datetime.timedelta(days=v.days, microseconds=v.nanoseconds // 1000)


def _python_values(column):
    import pyarrow as pa
    if pa.types.is_interval(column.type):
        return [_interval(v) for v in column.to_pylist()]
    return column.to_pylist()


def _type_code(t):
    """PEP 249's type code for an Arrow type: one of the type objects, or the Arrow type's name for a type
    none of them covers (a list, a struct)."""
    import pyarrow as pa
    kinds = ((STRING, "is_string", "is_large_string", "is_string_view"), (BINARY, "is_binary", "is_large_binary", "is_binary_view"),
             (NUMBER, "is_boolean", "is_integer", "is_floating", "is_decimal"),
             (DATETIME, "is_date", "is_time", "is_timestamp", "is_duration", "is_interval"))
    for code, *names in kinds:
        if any(getattr(pa.types, n, lambda _t: False)(t) for n in names):
            return code
    return str(t)


def _description(schema):
    import pyarrow as pa
    out = []
    for f in schema:
        decimal = pa.types.is_decimal(f.type)
        out.append((f.name, _type_code(f.type), None, None, f.type.precision if decimal else None, f.type.scale if decimal else None, f.nullable))
    return out


_QUOTED = re.compile(r"'(?:[^']|'')*'|\$(\d+)")


def _renumber(text, offset):
    """`text` with each $n moved on by `offset`; a $ inside a string literal is left alone."""
    return _QUOTED.sub(lambda m: m.group(0) if m.group(1) is None else f"${int(m.group(1)) + offset}", text)


def _one_insert(sql):
    """(head, row, tail) of one `INSERT … VALUES (…)` with nothing after it but a RETURNING (and no
    placeholder in the head or the tail), or None. The row is the VALUES' parenthesized group, so a
    batch of rows is the head, the rows' groups side by side, and the tail."""
    text = sql.strip().rstrip(";").strip()
    masked = re.sub(r"'(?:[^']|'')*'", lambda m: "'" + " " * (len(m.group(0)) - 2) + "'", text)  # (same length: positions agree)
    if not re.match(r"(?i)insert\b", masked):
        return None
    found = re.search(r"(?i)\bvalues\s*\(", masked)
    if not found:
        return None
    start, depth, end = found.end() - 1, 0, None
    for i in range(start, len(masked)):
        if masked[i] == "(":
            depth += 1
        elif masked[i] == ")":
            depth -= 1
            if depth == 0:
                end = i
                break
    if end is None:
        return None
    tail = masked[end + 1:].strip()
    tail_ok = not tail or (re.match(r"(?i)returning\b", tail) is not None and "$" not in tail)
    if not tail_ok or "$" in masked[:found.start()]:
        return None
    return text[:found.start()], text[start:end + 1], text[end + 1:]


# ---------------------------------------------------------------- connection and cursor

class Connection:
    """A session on a node. With `autocommit` off (the default), the first statement after a commit or
    rollback is preceded by BEGIN; `commit()` and `rollback()` end the transaction, and send nothing
    when none was begun. With `autocommit` on, each statement goes alone. `isolation_level` is what
    BEGIN starts: READ COMMITTED (the default) or REPEATABLE READ (the dialect sets it)."""

    def __init__(self, client):
        self._client = client
        self.autocommit = False
        self.isolation_level = "READ COMMITTED"
        self._open = True
        self._begun = False

    def cursor(self):
        self._alive()
        return Cursor(self)

    def commit(self):
        self._alive()
        if self._begun:
            self._execute("COMMIT")

    def rollback(self):
        self._alive()
        if self._begun:
            self._execute("ROLLBACK")

    def close(self):
        """Roll back an open transaction, then end the session (and the node `connect()` started, if it
        started one). Closing never raises for the rollback: the connection is gone either way."""
        if not self._open:
            return
        try:
            self.rollback()
        except Error:
            pass
        self._open = False
        self._client.close()

    def _alive(self):
        if not self._open:
            raise InterfaceError("connection is closed")

    def _execute(self, sql, params=None):
        """One statement on this session, with its parameters. A BEGIN goes first when a transaction should
        start (autocommit off, none begun). A BEGIN, COMMIT or ROLLBACK typed by hand counts as the
        transaction's, so the commit() and rollback() that follow it agree with the node."""
        self._alive()
        word = _first_word(sql)
        try:
            if word in _ENDS:
                self._begun = False  # (a COMMIT that fails ended the transaction too)
            elif not self.autocommit and not self._begun and word not in _STARTS:
                if self.isolation_level not in _BEGIN:
                    raise NotSupportedError(f"Pondra runs READ COMMITTED and REPEATABLE READ, not {self.isolation_level}")
                self._execute(_BEGIN[self.isolation_level])
            # (catalog=postgres: pg_catalog and information_schema answered as the Postgres port answers them,
            # since a DB-API caller speaks Postgres: SQLAlchemy's inspector, has_table, pandas' to_sql)
            answer = self._client._run(sql, {str(i + 1): v for i, v in enumerate(params or ())}, catalog="postgres")
        except (PondraError, OSError) as e:
            raise _error(e) from e
        if word in _STARTS:
            self._begun = True
        return answer


class Cursor:
    arraysize = 1

    def __init__(self, con):
        self.connection = con
        self.description = None
        self.rowcount = -1
        self._answer = None  # what the last statement answered: an Arrow table, or JSON rows as a list; None: no result set
        self._pos = 0
        self._open = True

    def execute(self, sql, params=None):
        self._alive()
        self._take(self.connection._execute(sql, _values(params)))

    def executemany(self, sql, seq):
        """A single `INSERT … VALUES (…)` with parameters goes as one statement carrying every row, so it is
        one commit; any other statement is run once per row."""
        self._alive()
        batch = [_values(p) for p in seq]
        if not batch:
            self._take({"rows": 0})
            return
        width = len(batch[0])
        one = _one_insert(sql)
        if one and all(len(p) == width for p in batch):
            head, row, tail = one
            text = head + "VALUES " + ", ".join(_renumber(row, i * width) for i in range(len(batch))) + tail
            self.execute(text, [v for p in batch for v in p])
            return
        total = 0
        for p in batch:
            self.execute(sql, p)
            total += max(self.rowcount, 0)
        self.rowcount = total

    def fetchone(self):
        rows = self._next(1)
        return rows[0] if rows else None

    def fetchmany(self, size=None):
        return self._next(self.arraysize if size is None else size)

    def fetchall(self):
        return self._next(self._size())

    def setinputsizes(self, sizes):
        pass

    def setoutputsize(self, size, column=None):
        pass

    def close(self):
        self._open = False
        self._answer, self.description = None, None

    def __iter__(self):
        return self

    def __next__(self):
        row = self.fetchone()
        if row is None:
            raise StopIteration
        return row

    def _alive(self):
        if not self._open:
            raise InterfaceError("cursor is closed")
        self.connection._alive()

    def _take(self, answer):
        """Keep an answer: its rows to fetch, or for an outcome (a write's, DDL's) its row count."""
        self._pos, self._answer = 0, None
        if isinstance(answer, dict):
            self.description, self.rowcount = None, answer.get("rows", -1)
            return
        if answer._rows is not None:  # JSON rows (no pyarrow): names only, from the first row
            self._answer = answer._rows
            self.description = [(k, None, None, None, None, None, True) for k in answer._rows[0]] if answer._rows else None
        else:  # (Arrow came back, so pyarrow is here)
            table = _tidy(answer.to_arrow())
            self._answer = table
            self.description = _description(table.schema)
        self.rowcount = len(self._answer)

    def _size(self):
        if self._answer is None:
            raise ProgrammingError("the last statement returned no rows: there is nothing to fetch")
        return len(self._answer)

    def _next(self, n):
        """Up to `n` more rows of the answer, as tuples, from where the last fetch stopped."""
        self._alive()
        size = self._size()
        if isinstance(self._answer, list):
            names = [d[0] for d in self.description] if self.description else []
            rows = [tuple(r[k] for k in names) for r in self._answer[self._pos:self._pos + n]]
        else:
            part = self._answer.slice(self._pos, min(n, size - self._pos))
            columns = [_python_values(c) for c in part.columns]
            rows = list(zip(*columns)) if columns else [()] * part.num_rows
        self._pos += len(rows)
        return rows


def _values(params):
    """A statement's parameters as a list: a sequence, positional ($1 is the first). A dict is refused."""
    if params is None:
        return []
    if isinstance(params, dict):
        raise ProgrammingError("Pondra's parameters are a sequence: $1 is the first value (not a dict)")
    return list(params)


def connect(target=None, token=None, **options):
    """PEP 249's connect: `pondra.connect(target, token, **options)` as a connection. `target` is a folder,
    a bucket's URL, a node's `http://` or `https://` URL (with `token=`, or `user=` and `password=`), or
    none for a lake in this process's memory. `options` are `pondra.connect`'s."""
    # A DB-API user gets no printed notices: a procedure's notices come back on the cursor's side, not on stdout.
    options.setdefault("echo", False)
    return Connection(_connect(target, token, **options))
