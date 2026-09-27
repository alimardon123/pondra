"""Frames: queries written in Python, run as SQL (ADR-023, docs/dataframe-api.md).

A frame is a query that hasn't run yet, with Polars' names: `con.table("orders").filter(col("amount")
> 100).group_by("user").agg(col("amount").sum())`. Every method returns a new frame, each step a
CTE of one SQL statement (`frame.sql`), and `collect()` runs that statement on the node: the same
plan, speed and answer as the SQL, spread over the nodes when it pays.

SQL and Python are one language here: `con.sql(…)` gives a frame; SQL names Python frames and
pandas, Polars and Arrow data by their variable names; any expression may be a SQL snippet
(`filter("amount > 100")`, `with_columns("qty * price AS total")`); `to_view` makes a frame a view
that `.sql` files and every other client read.
"""
import builtins
import datetime as _dt
import decimal
import itertools
import math
import re

_ids = itertools.count(1)
_WORD = re.compile(r"[A-Za-z_][A-Za-z0-9_]*$")


def _quote(name):
    return '"' + str(name).replace('"', '""') + '"'


def _ident(name):
    """A column name as SQL: `a`, or `t.a` for a qualified one (each part quoted)."""
    return ".".join(_quote(p) for p in str(name).split(".")) if name != "*" else "*"


def _literal(v):
    """A Python value as a SQL literal."""
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, int):
        return str(v)
    if isinstance(v, float):  # (a float, not a decimal: Pondra reads 0.9 in SQL as DECIMAL, for TPC-H's sake)
        return f"CAST('{v!r}' AS DOUBLE)"
    if isinstance(v, decimal.Decimal):
        return f"CAST('{v}' AS DECIMAL(38, {builtins.max(0, -v.as_tuple().exponent)}))"
    if isinstance(v, _dt.datetime):
        return f"TIMESTAMP '{v.isoformat(sep=' ')}'"
    if isinstance(v, _dt.date):
        return f"DATE '{v.isoformat()}'"
    if isinstance(v, _dt.timedelta):
        return f"INTERVAL '{int(v / _dt.timedelta(microseconds=1))} microseconds'"
    if isinstance(v, (list, tuple)):
        return "[" + ", ".join(_literal(x) for x in v) + "]"
    if isinstance(v, bytes):
        return "X'" + v.hex() + "'"
    return "'" + str(v).replace("'", "''") + "'"


# ---------------------------------------------------------------- expressions

class Expr:
    """A column expression: its SQL, and the name its column gets (Polars': the first column in it)."""

    __hash__ = None

    def __init__(self, sql, name=None, over=None):
        self.sql, self.name = sql, name
        self._over = over  # how `.over(…)` places it in a window (None: `sql OVER (…)`)

    def __repr__(self):
        return f"<pondra Expr {self.sql}>"

    def __bool__(self):
        raise TypeError("a pondra expression has no truth value: combine them with & | ~, not and / or / not")

    def _op(self, op, other, right=False):
        o = expr(other)
        a, b = (o, self) if right else (self, o)
        return Expr(f"({a.sql} {op} {b.sql})", a.name or b.name)

    def _fn(self, f, *args, name=None):
        return Expr(f"{f}({', '.join([self.sql, *map(_sql, args)])})", name or self.name)

    __add__ = lambda s, o: s._op("+", o)
    __radd__ = lambda s, o: s._op("+", o, True)
    __sub__ = lambda s, o: s._op("-", o)
    __rsub__ = lambda s, o: s._op("-", o, True)
    __mul__ = lambda s, o: s._op("*", o)
    __rmul__ = lambda s, o: s._op("*", o, True)
    __mod__ = lambda s, o: s._op("%", o)
    __rmod__ = lambda s, o: s._op("%", o, True)
    __eq__ = lambda s, o: s._op("=", o)
    __ne__ = lambda s, o: s._op("<>", o)
    __lt__ = lambda s, o: s._op("<", o)
    __le__ = lambda s, o: s._op("<=", o)
    __gt__ = lambda s, o: s._op(">", o)
    __ge__ = lambda s, o: s._op(">=", o)
    __and__ = lambda s, o: s._op("AND", o)
    __rand__ = lambda s, o: s._op("AND", o, True)
    __or__ = lambda s, o: s._op("OR", o)
    __ror__ = lambda s, o: s._op("OR", o, True)
    __invert__ = lambda s: Expr(f"(NOT {s.sql})", s.name)
    __neg__ = lambda s: Expr(f"(- {s.sql})", s.name)
    __abs__ = lambda s: s.abs()
    __pow__ = lambda s, o: Expr(f"power({s.sql}, {expr(o).sql})", s.name)

    def __truediv__(self, o):  # (Polars: always a true division, as a float)
        return Expr(f"(CAST({self.sql} AS DOUBLE) / {expr(o).sql})", self.name)

    def __rtruediv__(self, o):
        return Expr(f"(CAST({expr(o).sql} AS DOUBLE) / {self.sql})", expr(o).name or self.name)

    def __floordiv__(self, o):
        return Expr(f"floor(CAST({self.sql} AS DOUBLE) / {expr(o).sql})", self.name)

    def alias(self, name):
        return Expr(self.sql, name, self._over)

    def cast(self, dtype):
        return Expr(f"CAST({self.sql} AS {sql_type(dtype)})", self.name)

    def not_(self):
        return ~self

    def is_null(self):
        return Expr(f"({self.sql} IS NULL)", self.name)

    def is_not_null(self):
        return Expr(f"({self.sql} IS NOT NULL)", self.name)

    def is_nan(self):
        return self._fn("isnan")

    def is_in(self, values):
        if isinstance(values, Frame):
            return Expr(f"({self.sql} IN ({values.sql}))", self.name)
        return Expr(f"({self.sql} IN ({', '.join(_literal(v) for v in values)}))", self.name) if values else lit(False).alias(self.name)

    def is_between(self, lower, upper, closed="both"):
        lo, hi = (">=" if closed in ("both", "left") else ">"), ("<=" if closed in ("both", "right") else "<")
        return Expr(f"({self.sql} {lo} {expr(lower).sql} AND {self.sql} {hi} {expr(upper).sql})", self.name)

    def fill_null(self, value):
        return self._fn("coalesce", value)

    def fill_nan(self, value):
        return Expr(f"CASE WHEN isnan({self.sql}) THEN {expr(value).sql} ELSE {self.sql} END", self.name)

    def abs(self):
        return self._fn("abs")

    def round(self, decimals=0, mode="half_to_even"):
        """Polars' rounding: a half to the even neighbour (SQL's round() goes away from zero)."""
        if mode != "half_to_even":
            return self._fn("round", lit(decimals))
        x, k = f"({self.sql} * {10 ** decimals})", 10 ** decimals
        return Expr(f"(CASE WHEN abs({x} - trunc({x})) = 0.5 THEN 2 * round({x} / 2) ELSE round({x}) END / {k})", self.name)

    def sqrt(self):
        return self._fn("sqrt")

    def exp(self):
        return self._fn("exp")

    def log(self, base=math.e):
        return self._fn("ln") if base == math.e else Expr(f"log({_literal(base)}, {self.sql})", self.name)

    def floor(self):
        return self._fn("floor")

    def ceil(self):
        return self._fn("ceil")

    def clip(self, lower=None, upper=None):  # (null stays null: SQL's greatest() skips nulls)
        e = self if lower is None else Expr(f"greatest({self.sql}, {expr(lower).sql})", self.name)
        e = e if upper is None else Expr(f"least({e.sql}, {expr(upper).sql})", self.name)
        return Expr(f"CASE WHEN {self.sql} IS NULL THEN NULL ELSE {e.sql} END", self.name)

    # aggregations (Polars names; `std` and `var` with ddof=1)
    def sum(self):
        return self._fn("sum")

    def mean(self):
        return self._fn("avg")

    def min(self):
        return self._fn("min")

    def max(self):
        return self._fn("max")

    def count(self):
        return self._fn("count")

    def n_unique(self):
        return Expr(f"count(DISTINCT {self.sql}) + max(CASE WHEN {self.sql} IS NULL THEN 1 ELSE 0 END)", self.name)  # (Polars counts null as a value)

    def first(self):
        return self._fn("first_value")

    def last(self):
        return self._fn("last_value")

    def median(self):
        return self._fn("median")

    def std(self, ddof=1):
        return self._fn("stddev_samp" if ddof == 1 else "stddev_pop")

    def var(self, ddof=1):
        return self._fn("var_samp" if ddof == 1 else "var_pop")

    def quantile(self, q):
        return Expr(f"approx_percentile_cont({_literal(q)}) WITHIN GROUP (ORDER BY {self.sql})", self.name)

    def any(self):
        return self._fn("bool_or")

    def all(self):
        return self._fn("bool_and")

    # windows
    def over(self, partition_by=None, order_by=None, descending=False):
        """This expression per group of rows (SQL's `OVER (PARTITION BY … ORDER BY …)`)."""
        parts = [c.sql for c in _exprs(partition_by)]
        order = [f"{c.sql} {'DESC' if descending else 'ASC'}" for c in _exprs(order_by)]
        clause = " ".join(x for x in (parts and "PARTITION BY " + ", ".join(parts), order and "ORDER BY " + ", ".join(order)) if x)
        return Expr(self._over(clause, parts) if self._over else f"{self.sql} OVER ({clause})", self.name)

    def shift(self, n=1):
        f = "lag" if n >= 0 else "lead"
        return Expr(f"{f}({self.sql}, {abs(n)})", self.name, lambda w, _: f"{f}({self.sql}, {abs(n)}) OVER ({w})")

    def cum_sum(self):
        return Expr(f"sum({self.sql})", self.name, lambda w, _: f"sum({self.sql}) OVER ({w} ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)")

    def rank(self, method="average", descending=False):
        """Rank by this column's values (Polars' methods: average, min, max, dense, ordinal)."""
        way = f"ORDER BY {self.sql} {'DESC' if descending else 'ASC'}"

        def place(_, parts):
            p = f"PARTITION BY {', '.join(parts)} " if parts else ""
            ties = f"count(*) OVER (PARTITION BY {', '.join([*parts, self.sql])})"
            r = {"min": f"rank() OVER ({p}{way})", "dense": f"dense_rank() OVER ({p}{way})", "ordinal": f"row_number() OVER ({p}{way})",
                 "max": f"(rank() OVER ({p}{way}) + {ties} - 1)", "average": f"(rank() OVER ({p}{way}) + ({ties} - 1) / 2.0)"}[method]
            return f"CASE WHEN {self.sql} IS NULL THEN NULL ELSE {r} END"
        return Expr(place("", []), self.name, place)

    @property
    def str(self):
        return _Str(self)

    @property
    def dt(self):
        return _Dt(self)


class _Str:
    """`.str`: text functions, with Polars' names."""

    def __init__(self, e):
        self.e = e

    def _f(self, f, *args):
        return self.e._fn(f, *[lit(a) if not isinstance(a, Expr) else a for a in args])

    def to_lowercase(self):
        return self._f("lower")

    def to_uppercase(self):
        return self._f("upper")

    def len_chars(self):
        return self._f("character_length")

    def len_bytes(self):
        return self._f("octet_length")

    def contains(self, pattern, literal=False):
        return Expr(f"(strpos({self.e.sql}, {_literal(pattern)}) > 0)", self.e.name) if literal else self._f("regexp_like", pattern)

    def starts_with(self, prefix):
        return self._f("starts_with", prefix)

    def ends_with(self, suffix):
        return self._f("ends_with", suffix)

    def replace(self, pattern, value, literal=False):
        return self._f("regexp_replace", re.escape(pattern) if literal else pattern, value)

    def replace_all(self, pattern, value, literal=False):
        return self._f("regexp_replace", re.escape(pattern) if literal else pattern, value, "g")

    def slice(self, offset, length=None):
        return self._f("substr", offset + 1) if length is None else self._f("substr", offset + 1, length)

    def strip_chars(self, characters=None):
        return self._f("trim") if characters is None else Expr(f"btrim({self.e.sql}, {_literal(characters)})", self.e.name)

    def split(self, by):
        return self._f("string_to_array", by)

    def to_date(self, format=None):
        return self._f("to_date") if format is None else self._f("to_date", format)

    def to_datetime(self, format=None):
        return self._f("to_timestamp") if format is None else self._f("to_timestamp", format)


class _Dt:
    """`.dt`: date and time functions, with Polars' names."""

    UNITS = {"us": "microsecond", "ms": "millisecond", "s": "second", "m": "minute", "h": "hour", "d": "day", "w": "week", "mo": "month", "q": "quarter", "y": "year"}

    def __init__(self, e):
        self.e = e

    def _part(self, part):
        return Expr(f"CAST(date_part('{part}', {self.e.sql}) AS BIGINT)", self.e.name)

    year = lambda s: s._part("year")
    month = lambda s: s._part("month")
    day = lambda s: s._part("day")
    hour = lambda s: s._part("hour")
    minute = lambda s: s._part("minute")
    second = lambda s: s._part("second")
    quarter = lambda s: s._part("quarter")
    ordinal_day = lambda s: s._part("doy")

    def weekday(self):  # (Monday 1 … Sunday 7)
        return Expr(f"CAST((date_part('dow', {self.e.sql}) + 6) % 7 + 1 AS BIGINT)", self.e.name)

    def date(self):
        return Expr(f"CAST({self.e.sql} AS DATE)", self.e.name)

    def truncate(self, every):
        """Down to the start of its `every`: "1h", "15m", "1d", "1mo"…"""
        n, unit = re.fullmatch(r"(\d+)([a-z]+)", every).groups()
        unit = self.UNITS[unit]
        if n == "1":
            return Expr(f"date_trunc('{unit}', {self.e.sql})", self.e.name)
        return Expr(f"date_bin(INTERVAL '{n} {unit}s', {self.e.sql}, TIMESTAMP '1970-01-01')", self.e.name)

    def strftime(self, format):
        return Expr(f"to_char({self.e.sql}, {_literal(format)})", self.e.name)

    def epoch(self, time_unit="us"):
        scale = {"s": 1, "ms": 1_000, "us": 1_000_000, "ns": 1_000_000_000}[time_unit]
        return Expr(f"CAST(date_part('epoch', {self.e.sql}) * {scale} AS BIGINT)", self.e.name)


def col(name):
    """A column: `col("amount")`, `col("s.amount")` (of the table called s), `col("*")`."""
    return Expr(_ident(name), None if name == "*" else str(name).split(".")[-1])


def lit(value):
    return value if isinstance(value, Expr) else Expr(_literal(value), "literal")


def sql_expr(sql):
    """A SQL expression as it is, `AS name` included: `sql_expr("CASE WHEN qty > 10 THEN 'big' END AS size")`."""
    m = re.fullmatch(r"(?is)\s*(.+?)\s+AS\s+(\"(?:[^\"]|\"\")+\"|[A-Za-z_][A-Za-z0-9_]*)\s*", sql)
    if m and _balanced(m.group(1)):
        name = m.group(2)
        return Expr(f"({m.group(1)})", name[1:-1].replace('""', '"') if name.startswith('"') else name.lower())
    return Expr(f"({sql.strip()})", sql.strip().lower() if _WORD.match(sql.strip()) else None)


def _balanced(s):
    """No `AS` inside parentheses or a CASE: the last `AS` is the alias's."""
    depth = 0
    for ch in re.sub(r"'(?:[^']|'')*'", "''", s):
        depth += {"(": 1, ")": -1}.get(ch, 0)
        if depth < 0:
            return False
    return depth == 0 and not re.search(r"(?i)\bCASE\b(?!.*\bEND\b)", s)


def len():  # noqa: A001 (Polars' name)
    """The number of rows (in each group): `count(*)`."""
    return Expr("count(*)", "len")


def when(condition):
    return _When([], expr(condition, sql=True))


class _When:
    def __init__(self, done, cond):
        self.done, self.cond = done, cond

    def then(self, value):
        return _Then(self.done + [(self.cond, expr(value))])


class _Then(Expr):
    def __init__(self, cases, otherwise=None):
        self.cases = cases
        body = " ".join(f"WHEN {c.sql} THEN {v.sql}" for c, v in cases)
        super().__init__(f"CASE {body}{' ELSE ' + otherwise.sql if otherwise is not None else ''} END", cases[0][1].name)

    def when(self, condition):
        return _When(self.cases, expr(condition, sql=True))

    def otherwise(self, value):
        return _Then(self.cases, expr(value))


def coalesce(*exprs):
    es = [expr(e) for e in exprs]
    return Expr(f"coalesce({', '.join(e.sql for e in es)})", es[0].name)


def concat_str(exprs, separator=""):
    es = [expr(e) for e in exprs]
    return Expr(f"concat_ws({_literal(separator)}, {', '.join(e.sql for e in es)})", es[0].name)


for _f in ("sum", "mean", "min", "max", "count", "n_unique", "median", "first", "last"):
    globals()[_f] = (lambda f: lambda name: getattr(col(name), f)())(_f)


def all():  # noqa: A001 (Polars' name)
    return col("*")


def expr(x, sql=False):
    """An expression from what a method was given: itself, a SQL snippet (a string, `sql`), a value."""
    if isinstance(x, Expr):
        return x
    if sql and isinstance(x, str):
        return sql_expr(x)
    return lit(x)


def _column(x):
    """A string names a column where Polars' would (`select("a")`); one that isn't a plain name is SQL."""
    if isinstance(x, str):
        return col(x) if _WORD.match(x) or x == "*" else sql_expr(x)
    return expr(x)


def _exprs(xs):
    if xs is None:
        return []
    return [_column(x) for x in (xs if isinstance(xs, (list, tuple)) else [xs])]


def _sql(x):
    return x.sql if isinstance(x, Expr) else _literal(x)


def _named(e):
    return f"{e.sql} AS {_quote(e.name)}" if e.name and e.sql != _ident(e.name) else e.sql


SQL_TYPES = {"int8": "TINYINT", "int16": "SMALLINT", "int32": "INT", "int64": "BIGINT", "uint8": "TINYINT UNSIGNED", "uint16": "SMALLINT UNSIGNED",
             "uint32": "INT UNSIGNED", "uint64": "BIGINT UNSIGNED", "float32": "REAL", "float64": "DOUBLE", "utf8": "VARCHAR", "string": "VARCHAR",
             "str": "VARCHAR", "boolean": "BOOLEAN", "bool": "BOOLEAN", "date": "DATE", "datetime": "TIMESTAMP", "int": "BIGINT", "float": "DOUBLE"}


def sql_type(t):
    """A SQL type from a Polars dtype, a Python type, or SQL itself: `pl.Int64`, `int`, "BIGINT"."""
    name = getattr(t, "__name__", None) or str(t)
    return SQL_TYPES.get(name.split("(")[0].lower(), name)


# ---------------------------------------------------------------- frames

class Frame:
    """A query that hasn't run yet: Polars' LazyFrame, over a lake. `frame.sql` is the statement."""

    def __init__(self, con, query, ctes=None, rel=None, params=None, sent=None, scopes=(), table=None, order=None):
        self._con, self._query, self._ctes, self._rel = con, query, dict(ctes or {}), rel
        self._params, self._sent, self._scopes, self._table = dict(params or {}), dict(sent or {}), scopes, table
        self._order = order  # (the sort in effect: [(SQL, the column it names)]; SQL drops a CTE's ORDER BY)
        self._schema = None

    # what it is
    @property
    def sql(self):
        """The one SQL statement this frame is: each step a CTE."""
        if not self._ctes:
            return self._query
        ours = ",\n     ".join(f"{n} AS ({q})" for n, q in self._ctes.items())
        m = re.match(r"(?is)\s*(?:(?:--[^\n]*\n|/\*.*?\*/)\s*)*WITH\s+(RECURSIVE\s+)?", self._query)
        if m:  # (SQL with a WITH of its own: ours go first in it)
            return f"WITH {m.group(1) or ''}{ours},\n     {self._query[m.end():]}"
        return f"WITH {ours}\n{self._query}"

    def __repr__(self):
        return f"<pondra Frame\n{self.sql}\n>"

    def _repr_html_(self):  # (a notebook shows the first rows, as DuckDB's relations do)
        t = self.limit(20).collect()
        return t.to_pandas()._repr_html_() if hasattr(t, "to_pandas") else None

    @property
    def schema(self):
        """Its columns and their types (a pyarrow schema): the statement planned, not run."""
        if self._schema is None:
            self._schema = self._step(lambda r: f"SELECT * FROM {r} LIMIT 0").collect().schema
        return self._schema

    @property
    def columns(self):
        return self.schema.names

    # building
    def _with(self, query, ctes=None, **kw):
        k = dict(params=self._params, sent=self._sent, scopes=self._scopes)
        k.update(kw)
        return Frame(self._con, query, self._ctes if ctes is None else ctes, **k)

    def _kept(self, names=None):
        """The sort, if every column it names is still there (`names`: the step's columns; None: all)."""
        return self._order if self._order and (names is None or all_(n in names for _, n in self._order)) else None

    def _as_rel(self, ctes):
        """A name that stands for this frame in the next step (a new CTE unless it is one already)."""
        if self._rel:
            return self._rel
        name = f"_{next(_ids)}"
        ctes[name] = self._query
        return name

    def _step(self, build, order=None, tail=""):
        """The next step: `build(rel)`, then the sort it keeps (`order`: SQL puts it in each step
        again), then `tail` (a LIMIT)."""
        ctes = dict(self._ctes)
        rel = self._as_rel(ctes)
        by = f" ORDER BY {', '.join(o for o, _ in order)}" if order else ""
        q = build(rel)
        # (a sort alone is its input read in order: the next step reads that input, in that order)
        return self._with(q + by + tail, ctes, order=order, rel=rel if q == f"SELECT * FROM {rel}" and not tail else None)

    def _joined(self, other):
        """Both frames' steps, parameters and data, for a step that reads both."""
        for k, v in other._params.items():
            if self._params.get(k, v) != v:
                raise ValueError(f"${k} has two values")
        ctes = {**self._ctes, **other._ctes}
        return ctes, dict(params={**self._params, **other._params}, sent={**self._sent, **other._sent}, scopes=self._scopes + other._scopes)

    # rows and columns
    def select(self, *exprs, **named):
        es = _exprs(_flat(exprs)) + [expr(v, sql=True).alias(k) for k, v in named.items()]
        keep = self._kept(None if any(e.sql == "*" for e in es) else {e.name for e in es})
        return self._step(lambda r: f"SELECT {', '.join(map(_named, es))} FROM {r}", keep)

    def with_columns(self, *exprs, **named):
        """New columns, or new values of old ones where they are (Polars': SQL snippets need `AS name`)."""
        es = [expr(e, sql=True) for e in _flat(exprs)] + [expr(v, sql=True).alias(k) for k, v in named.items()]
        if any(not e.name for e in es):
            raise ValueError("with_columns: each expression needs a name (.alias(…), or AS name in SQL)")
        have = {c.lower(): c for c in self.columns}
        replace = [f"{e.sql} AS {_quote(have[e.name.lower()])}" for e in es if e.name.lower() in have]
        new = [f"{e.sql} AS {_quote(e.name)}" for e in es if e.name.lower() not in have]
        star = f"* REPLACE ({', '.join(replace)})" if replace else "*"
        return self._step(lambda r: f"SELECT {', '.join([star, *new])} FROM {r}", self._kept())

    with_column = with_columns

    def filter(self, *predicates):
        conds = [expr(p, sql=True).sql for p in _flat(predicates)]
        return self._step(lambda r: f"SELECT * FROM {r} WHERE {' AND '.join(conds)}", self._kept())

    where = filter

    def drop(self, *columns):
        gone = _flat(columns)
        keep = self._order if self._order and not any_(n in gone for _, n in self._order) else None
        return self._step(lambda r: f"SELECT * EXCLUDE ({', '.join(map(_quote, gone))}) FROM {r}", keep)

    def rename(self, mapping):
        have = self.columns
        cols = [f"{_quote(c)} AS {_quote(mapping[c])}" if c in mapping else _quote(c) for c in have]
        return self._step(lambda r: f"SELECT {', '.join(cols)} FROM {r}", self._kept(set(have) - set(mapping)))

    def cast(self, dtypes):
        casts = ", ".join(f"CAST({_quote(c)} AS {sql_type(t)}) AS {_quote(c)}" for c, t in dtypes.items())
        return self._step(lambda r: f"SELECT * REPLACE ({casts}) FROM {r}", self._kept())

    def sort(self, by, *more, descending=False, nulls_last=False):
        keys = _exprs(_flat([by, *more]))
        desc = descending if isinstance(descending, (list, tuple)) else [descending] * builtins.len(keys)
        last = nulls_last if isinstance(nulls_last, (list, tuple)) else [nulls_last] * builtins.len(keys)
        order = [(f"{k.sql} {'DESC' if d else 'ASC'} NULLS {'LAST' if n else 'FIRST'}", k.name) for k, d, n in zip(keys, desc, last)]
        return self._step(lambda r: f"SELECT * FROM {r}", order)

    def limit(self, n=5):
        return self._step(lambda r: f"SELECT * FROM {r}", self._kept(), f" LIMIT {int(n)}")

    head = limit

    def unique(self, subset=None, keep="any"):
        if subset is None:
            return self._step(lambda r: f"SELECT DISTINCT * FROM {r}")
        keys = ", ".join(e.sql for e in _exprs(subset))
        return self._step(lambda r: f"SELECT DISTINCT ON ({keys}) * FROM {r}")

    def sample(self, n=None, fraction=None, seed=None):
        if n is not None:
            return self._step(lambda r: f"SELECT * FROM {r} ORDER BY random() LIMIT {int(n)}")
        return self._step(lambda r: f"SELECT * FROM {r} WHERE random() < {float(fraction)}", self._kept())

    def fill_null(self, value):
        """Nulls in every column of the value's kind (numbers, text, true/false, dates) become it."""
        import pyarrow.types as t
        kind = {bool: t.is_boolean, int: lambda x: t.is_integer(x) or t.is_floating(x) or t.is_decimal(x), float: lambda x: t.is_floating(x) or t.is_decimal(x),
                str: lambda x: t.is_string(x) or t.is_large_string(x) or t.is_string_view(x)}.get(type(value), t.is_temporal)
        cols = [f.name for f in self.schema if kind(f.type)]
        if not cols:
            return self
        fills = ", ".join(f"coalesce({_quote(c)}, {_literal(value)}) AS {_quote(c)}" for c in cols)
        return self._step(lambda r: f"SELECT * REPLACE ({fills}) FROM {r}", self._kept())

    def drop_nulls(self, subset=None):
        cols = _flat([subset]) if subset is not None else self.columns
        return self._step(lambda r: f"SELECT * FROM {r} WHERE {' AND '.join(f'{_quote(c)} IS NOT NULL' for c in cols)}", self._kept())

    def group_by(self, *keys, maintain_order=False):
        return GroupBy(self, _exprs(_flat(keys)))

    def join(self, other, on=None, how="inner", left_on=None, right_on=None, suffix="_right"):
        """Polars' join: the key once (inner, left, right), both kept (full), the right side's other
        columns with `suffix` where names clash."""
        ctes, kw = self._joined(other)
        lr, rr = self._as_rel(ctes), other._as_rel(ctes)
        lk, rk = _flat([left_on or on or []]), _flat([right_on or on or []])
        n = next(_ids)
        l, r = f"_l{n}", f"_r{n}"
        cond = " AND ".join(f"{l}.{_quote(a)} = {r}.{_quote(b)}" for a, b in zip(lk, rk)) or "TRUE"
        if how in ("semi", "anti"):
            return self._with(f"SELECT {l}.* FROM {lr} AS {l} LEFT {how.upper()} JOIN {rr} AS {r} ON {cond}", ctes, **kw)
        lcols, rcols = self.columns, other.columns
        if how == "right":  # (the right side's keys, the left's other columns first)
            left = [f"{l}.{_quote(c)}" for c in lcols if c not in lk]
            right = [f"{r}.{_quote(c)} AS {_quote(c + suffix if c in lcols and c not in rk else c)}" for c in rcols]
        else:
            left = [f"{l}.*"]
            keep = [c for c in rcols if how in ("full", "cross") or c not in rk]
            right = [f"{r}.{_quote(c)} AS {_quote(c + suffix if c in lcols else c)}" for c in keep]
        kind = {"inner": "JOIN", "left": "LEFT JOIN", "right": "RIGHT JOIN", "full": "FULL JOIN", "outer": "FULL JOIN", "cross": "CROSS JOIN"}[how]
        on_ = "" if how == "cross" else f" ON {cond}"
        return self._with(f"SELECT {', '.join(left + right)} FROM {lr} AS {l} {kind} {rr} AS {r}{on_}", ctes, **kw)

    def join_asof(self, other, on=None, left_on=None, right_on=None, by=None, by_left=None, by_right=None, strategy="backward", suffix="_right"):
        """Each row with the right side's nearest earlier (backward) or later (forward) row by `on`,
        within `by` (SQL's ASOF JOIN)."""
        ctes, kw = self._joined(other)
        lr, rr = self._as_rel(ctes), other._as_rel(ctes)
        lo, ro = left_on or on, right_on or on
        bl, br = _flat([by_left or by or []]), _flat([by_right or by or []])
        n = next(_ids)
        l, r = f"_l{n}", f"_r{n}"
        op = {"backward": ">=", "forward": "<="}[strategy]
        lcols = self.columns
        right = [f"{r}.{_quote(c)} AS {_quote(c + suffix if c in lcols else c)}" for c in other.columns if c not in br and c != ro]
        on_ = (" ON " + " AND ".join(f"{l}.{_quote(a)} = {r}.{_quote(b)}" for a, b in zip(bl, br))) if bl else ""
        return self._with(f"SELECT {', '.join([f'{l}.*'] + right)} FROM {lr} AS {l} ASOF JOIN {rr} AS {r} MATCH_CONDITION ({l}.{_quote(lo)} {op} {r}.{_quote(ro)}){on_}", ctes, **kw)

    # results
    def collect(self):
        """Run it: a pyarrow Table."""
        return self._con._frame_rows(self)

    to_arrow = collect

    def to_pandas(self):
        return self.collect().to_pandas()

    def to_polars(self):
        import polars as pl
        return pl.from_arrow(self.collect())

    def rows(self):
        return self.collect().to_pylist()

    def item(self):
        """The one value of a one-row, one-column answer."""
        t = self.collect()
        return t.column(0)[0].as_py()

    def show(self, n=20):
        print(self.limit(n).collect().to_pandas().to_string(index=False))

    def explain(self):
        """Pondra's plan for it, and whether it would spread over the nodes."""
        return "\n".join(str(r["plan"]) for r in self._with("EXPLAIN " + self.sql).rows())

    def watch(self):
        """A table's (or materialized view's) new rows as they commit."""
        if not self._table:
            raise ValueError("watch() follows a table or a materialized view: con.table(name).watch()")
        return self._con.watch(self._table)

    # back to SQL, and writes
    def to_view(self, name, temporary=False, materialized=False, replace=None, **options):
        """A view of this frame others read by name: `.sql` files, SQL clients, BI. `temporary`:
        this connection's only; `materialized`: kept up to date as rows arrive (`options` as
        `CREATE MATERIALIZED VIEW … WITH (…)` takes them: window, size_secs, …)."""
        if temporary:
            self._con._temp[name.lower()] = self
            return self._con.table(name)
        if self._sent:
            raise ValueError("a view can't keep data sent from Python: write_table it first")
        if materialized:
            opts = " WITH (" + ", ".join(f"{k} = {_literal(v)}" for k, v in options.items()) + ")" if options else ""
            self._con._run(f"CREATE MATERIALIZED VIEW {name}{opts} AS {self.sql}", self._params)
        else:
            self._con._run(f"CREATE {'OR REPLACE ' if replace is not False else ''}VIEW {name} AS {self.sql}", self._params)
        return self._con.table(name)

    def write_table(self, name, mode="create"):
        """Its rows into a table: `create` (CREATE TABLE … AS), `append` (INSERT), `overwrite` (a new table in its place)."""
        body = {"create": f"CREATE TABLE {name} AS {self.sql}", "append": f"INSERT INTO {name} {self.sql}",
                "overwrite": f"DROP TABLE IF EXISTS {name}; CREATE TABLE {name} AS {self.sql}"}[mode]
        return self._con._run(body, self._params, self._sent)

    def _target(self, what):
        if not self._table:
            raise ValueError(f"{what} changes a table: con.table(name).{what}(…)")
        return self._table

    def update(self, set, where=None):
        t = self._target("update")
        assign = ", ".join(f"{_quote(c)} = {expr(v, sql=True).sql}" for c, v in set.items())
        return self._con._run(f"UPDATE {t} SET {assign}" + (f" WHERE {expr(where, sql=True).sql}" if where is not None else ""))

    def delete(self, where=None):
        t = self._target("delete")
        return self._con._run(f"DELETE FROM {t}" + (f" WHERE {expr(where, sql=True).sql}" if where is not None else ""))

    def merge(self, source, on, source_alias="s", target_alias="t"):
        """MERGE, with Delta Lake's builder names: `.when_matched_update({…})`,
        `.when_not_matched_insert()`, `.when_not_matched_by_source_delete()`, then `.run()`."""
        return Merge(self, source, on, source_alias, target_alias)


all_, any_ = builtins.all, builtins.any


def trailing_order(sql):
    """The sort a query ends with, if it sorts by columns (`ORDER BY u.tier DESC`), for the steps
    after it: SQL drops the ORDER BY of a query that becomes a CTE."""
    masked = re.sub(r"'(?:[^']|'')*'|--[^\n]*|/\*.*?\*/|\$(\w*)\$.*?\$\1\$", lambda m: " " * builtins.len(m.group(0)), sql, flags=re.S)
    depth, at = 0, None
    for m in re.finditer(r"[()]|\bORDER\s+BY\b", masked, flags=re.I):
        depth += {"(": 1, ")": -1}.get(m.group(0), 0)
        if depth == 0 and m.group(0) not in "()":
            at = m.end()
    if at is None:
        return None
    clause = re.split(r"(?i)\b(?:LIMIT|OFFSET)\b|;", masked[at:])[0]
    out = []
    for item in clause.split(","):
        m = re.fullmatch(r'(?is)\s*(?:\w+\.)?("(?:[^"]|"")+"|\w+)\s*(ASC|DESC)?\s*(NULLS\s+(?:FIRST|LAST))?\s*', item)
        if not m or m.group(1).isdigit():
            return None  # (a position or an expression: not known after the step)
        name = m.group(1)[1:-1].replace('""', '"') if m.group(1).startswith('"') else m.group(1).lower()
        way = (m.group(2) or "ASC").upper()
        nulls = re.sub(r"\s+", " ", m.group(3).upper()) if m.group(3) else ("NULLS LAST" if way == "ASC" else "NULLS FIRST")
        out.append((f"{_quote(name)} {way} {nulls}", name))
    return out


def _flat(xs):
    out = []
    for x in xs:
        out.extend(_flat(x) if isinstance(x, (list, tuple)) else [x])
    return out


class GroupBy:
    def __init__(self, frame, keys):
        self.frame, self.keys = frame, keys

    def agg(self, *aggs, **named):
        es = [expr(a, sql=True) for a in _flat(aggs)] + [expr(v, sql=True).alias(k) for k, v in named.items()]
        cols = [_named(k) for k in self.keys] + [_named(e) for e in es]
        group = ", ".join(str(i + 1) for i in range(builtins.len(self.keys)))
        return self.frame._step(lambda r: f"SELECT {', '.join(cols)} FROM {r}" + (f" GROUP BY {group}" if group else ""))

    def len(self, name="len"):
        return self.agg(len().alias(name))


class Merge:
    """`MERGE INTO t USING (source) s ON …` built as delta-rs and Delta on Spark build it."""

    def __init__(self, target, source, on, s, t):
        self.target, self.source, self.s, self.t, self.whens = target, source, s, t, []
        if isinstance(on, Expr) or (isinstance(on, str) and not _WORD.match(on)):
            self.on = expr(on, sql=True).sql  # (a condition)
        else:
            self.on = " AND ".join(f"{t}.{_quote(k)} = {s}.{_quote(k)}" for k in _flat([on]))  # (key columns)

    def _add(self, clause):
        self.whens.append(clause)
        return self

    def _set(self, values):
        return ", ".join(f"{_quote(c)} = {expr(v, sql=True).sql}" for c, v in values.items())

    def when_matched_update(self, set=None, predicate=None):
        return self._add(f"WHEN MATCHED{_and(predicate)} THEN UPDATE SET {self._set(set)}")

    def when_matched_update_all(self, predicate=None):
        cols = [c for c in self.target.columns if c in self._source().columns]
        return self.when_matched_update({c: col(f"{self.s}.{c}") for c in cols}, predicate)

    def when_matched_delete(self, predicate=None):
        return self._add(f"WHEN MATCHED{_and(predicate)} THEN DELETE")

    def when_not_matched_insert(self, values=None, predicate=None):
        if values is None:  # (every column the source has)
            values = {c: col(f"{self.s}.{c}") for c in self.target.columns if c in self._source().columns}
        return self._add(f"WHEN NOT MATCHED{_and(predicate)} THEN INSERT ({', '.join(map(_quote, values))}) VALUES ({', '.join(expr(v, sql=True).sql for v in values.values())})")

    when_not_matched_insert_all = lambda self, predicate=None: self.when_not_matched_insert(None, predicate)

    def when_not_matched_by_source_delete(self, predicate=None):
        return self._add(f"WHEN NOT MATCHED BY SOURCE{_and(predicate)} THEN DELETE")

    def _source(self):
        return self.source if isinstance(self.source, Frame) else self.target._con.from_arrow(self.source)

    def run(self):
        src = self._source()
        sql = f"MERGE INTO {self.target._table} AS {self.t} USING ({src.sql}) AS {self.s} ON {self.on} " + " ".join(self.whens)
        return self.target._con._run(sql, src._params, src._sent)

    execute = run


def _and(predicate):
    return f" AND {expr(predicate, sql=True).sql}" if predicate is not None else ""


def concat(frames, how="vertical"):
    """Frames one after another: by position (`vertical`), or by name, missing columns null (`diagonal`)."""
    out, rels = frames[0], []
    ctes = dict(out._ctes)
    rels.append(out._as_rel(ctes))
    for f in frames[1:]:
        more, kw = out._joined(f)
        ctes.update(more)
        rels.append(f._as_rel(ctes))
        out = out._with(out._query, ctes, **kw)
    glue = " UNION ALL BY NAME " if how == "diagonal" else " UNION ALL "
    return out._with(glue.join(f"SELECT * FROM {r}" for r in rels), ctes)
