"""PySpark's names over Pondra's frames: a PySpark job moves by changing its imports (ADR-023).

    from pondra.spark import SparkSession, functions as F, Window   # was: from pyspark.sql import …
    spark = SparkSession.builder.remote("pondra://node:8080").getOrCreate()   # or .master("local[*]")
    df = spark.table("orders").where(F.col("amount") > 100).groupBy("user").agg(F.sum("amount"))
    df.show(); df.toPandas(); df.write.mode("overwrite").saveAsTable("top")

A DataFrame is a `pondra.Frame`: one SQL statement, run by the node. Where PySpark's meaning
differs from SQL's, the SQL says PySpark's: ascending sorts put nulls first, `/` divides as
doubles, columns are named as PySpark names them (`sum(amount)`, `count(1)`). What isn't here
raises `NotImplementedError` with the way round it (usually `spark.sql`), never a different answer:
RDDs, Python UDFs on the nodes, and Structured Streaming (a materialized view does that).
"""
import itertools
import re
import sys

from .. import client as _client
from ..frame import Frame, _literal, _quote, sql_expr

_ids = itertools.count(1)
_SIMPLE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*$")
TYPES = {"int": "INT", "integer": "INT", "bigint": "BIGINT", "long": "BIGINT", "short": "SMALLINT", "smallint": "SMALLINT", "byte": "TINYINT",
         "tinyint": "TINYINT", "double": "DOUBLE", "float": "REAL", "string": "VARCHAR", "boolean": "BOOLEAN", "bool": "BOOLEAN",
         "date": "DATE", "timestamp": "TIMESTAMP", "binary": "BYTEA"}


def _type(t):
    """A PySpark type (`"int"`, `IntegerType()`, `"decimal(10,2)"`) as SQL's."""
    t = t.simpleString() if hasattr(t, "simpleString") else str(t)
    return TYPES.get(t.lower(), t.upper())


def _name_sql(name):
    """A column name as SQL: plain names unquoted (so they match any case, as PySpark does),
    `t.c` qualified, others quoted whole."""
    parts = name.split(".")
    if all(_SIMPLE.match(p) for p in parts):
        return name
    return _quote(name)


# ---------------------------------------------------------------- columns

class Column:
    """A PySpark column: its SQL, and the name PySpark gives it."""

    __hash__ = None

    def __init__(self, sql, name, sort=None):
        self.sql, self.name, self._sort = sql, name, sort

    def __repr__(self):
        return f"Column<'{self.name}'>"

    def __bool__(self):
        raise ValueError("Cannot convert column into bool: please use '&' for 'and', '|' for 'or', '~' for 'not'")

    def _op(self, op, other, right=False, label=None):
        o = other if isinstance(other, Column) else _lit(other)
        a, b = (o, self) if right else (self, o)
        return Column(f"({a.sql} {op} {b.sql})", f"({a.name} {label or op} {b.name})")

    __add__ = lambda s, o: s._op("+", o)
    __radd__ = lambda s, o: s._op("+", o, True)
    __sub__ = lambda s, o: s._op("-", o)
    __rsub__ = lambda s, o: s._op("-", o, True)
    __mul__ = lambda s, o: s._op("*", o)
    __rmul__ = lambda s, o: s._op("*", o, True)
    __mod__ = lambda s, o: s._op("%", o)
    __eq__ = lambda s, o: s._op("=", o)
    __ne__ = lambda s, o: s._op("<>", o, label="!=")
    __lt__ = lambda s, o: s._op("<", o)
    __le__ = lambda s, o: s._op("<=", o)
    __gt__ = lambda s, o: s._op(">", o)
    __ge__ = lambda s, o: s._op(">=", o)
    __and__ = lambda s, o: s._op("AND", o)
    __or__ = lambda s, o: s._op("OR", o)
    __invert__ = lambda s: Column(f"(NOT {s.sql})", f"(NOT {s.name})")
    __neg__ = lambda s: Column(f"(- {s.sql})", f"(- {s.name})")

    def __truediv__(self, o):  # (PySpark: a double, whatever the operands)
        o = o if isinstance(o, Column) else _lit(o)
        return Column(f"(CAST({self.sql} AS DOUBLE) / {o.sql})", f"({self.name} / {o.name})")

    def __rtruediv__(self, o):
        o = o if isinstance(o, Column) else _lit(o)
        return Column(f"(CAST({o.sql} AS DOUBLE) / {self.sql})", f"({o.name} / {self.name})")

    def alias(self, name):
        return Column(self.sql, name)

    name_ = alias

    def cast(self, t):
        return Column(f"CAST({self.sql} AS {_type(t)})", self.name)

    astype = cast

    def isNull(self):
        return Column(f"({self.sql} IS NULL)", f"({self.name} IS NULL)")

    def isNotNull(self):
        return Column(f"({self.sql} IS NOT NULL)", f"({self.name} IS NOT NULL)")

    def isin(self, *values):
        vs = values[0] if len(values) == 1 and isinstance(values[0], (list, tuple, set)) else values
        return Column(f"({self.sql} IN ({', '.join(_literal(v) for v in vs)}))", f"({self.name} IN ({', '.join(map(str, vs))}))")

    def between(self, lo, hi):
        return Column(f"({self.sql} BETWEEN {_lit(lo).sql} AND {_lit(hi).sql})", f"(({self.name} >= {lo}) AND ({self.name} <= {hi}))")

    def like(self, pattern):
        return Column(f"({self.sql} LIKE {_literal(pattern)})", f"{self.name} LIKE {pattern}")

    def rlike(self, pattern):
        return Column(f"regexp_like({self.sql}, {_literal(pattern)})", f"RLIKE({self.name}, {pattern})")

    def contains(self, s):
        return Column(f"(strpos({self.sql}, {_literal(s)}) > 0)", f"contains({self.name}, {s})")

    def startswith(self, s):
        return Column(f"starts_with({self.sql}, {_literal(s)})", f"startswith({self.name}, {s})")

    def endswith(self, s):
        return Column(f"ends_with({self.sql}, {_literal(s)})", f"endswith({self.name}, {s})")

    def substr(self, pos, length):
        return Column(f"substr({self.sql}, {int(pos)}, {int(length)})", f"substring({self.name}, {pos}, {length})")

    # sorts: PySpark's nulls (first ascending, last descending) unless asked
    def asc(self):
        return Column(self.sql, self.name, f"{self.sql} ASC NULLS FIRST")

    def desc(self):
        return Column(self.sql, self.name, f"{self.sql} DESC NULLS LAST")

    def asc_nulls_last(self):
        return Column(self.sql, self.name, f"{self.sql} ASC NULLS LAST")

    def desc_nulls_first(self):
        return Column(self.sql, self.name, f"{self.sql} DESC NULLS FIRST")

    asc_nulls_first, desc_nulls_last = asc, desc

    def over(self, window):
        return Column(f"{self.sql} OVER ({window._clause()})", f"{self.name} OVER ({window._clause()})")

    # CASE WHEN, built as F.when(…).when(…).otherwise(…)
    @staticmethod
    def _case(cases, otherwise=None):
        body = " ".join(f"WHEN {_as_col(c).sql} THEN {_as_val(v).sql}" for c, v in cases)
        names = " ".join(f"WHEN {_as_col(c).name} THEN {_as_val(v).name}" for c, v in cases)
        e = "" if otherwise is None else f" ELSE {_as_val(otherwise).sql}"
        col = Column(f"CASE {body}{e} END", f"CASE {names}{'' if otherwise is None else ' ELSE ' + _as_val(otherwise).name} END")
        col._cases = cases
        return col

    def when(self, condition, value):
        return Column._case(self._cases + [(condition, value)])

    def otherwise(self, value):
        return Column._case(self._cases, value)

    def getItem(self, key):
        return Column(f"{self.sql}[{_literal(key if not isinstance(key, int) else key + 1)}]", f"{self.name}[{key}]")


def _col(name):
    if isinstance(name, Column):
        return name
    if name == "*":
        return Column("*", "*")
    return Column(_name_sql(name), name.split(".")[-1])


def _lit(v):
    return v if isinstance(v, Column) else Column(_literal(v), "NULL" if v is None else str(v).lower() if isinstance(v, bool) else str(v))


def _as_col(c):
    return c if isinstance(c, Column) else _col(c)


def _as_val(v):
    return v if isinstance(v, Column) else _lit(v)


def _named(c):
    """A column in a SELECT, named as PySpark names it."""
    if c.sql == "*" or c.sql.endswith(".*"):
        return c.sql
    return c.sql if c.sql == _name_sql(c.name) else f"{c.sql} AS {_quote(c.name)}"


def _cols(xs):
    out = []
    for x in xs:
        out.extend(_cols(x) if isinstance(x, (list, tuple)) else [_as_col(x)])
    return out


class Window:
    unboundedPreceding, unboundedFollowing, currentRow = -sys.maxsize, sys.maxsize, 0

    @staticmethod
    def partitionBy(*cols):
        return WindowSpec().partitionBy(*cols)

    @staticmethod
    def orderBy(*cols):
        return WindowSpec().orderBy(*cols)


class WindowSpec:
    def __init__(self, parts=(), order=(), frame=""):
        self.parts, self.order, self.frame = parts, order, frame

    def partitionBy(self, *cols):
        return WindowSpec(tuple(c.sql for c in _cols(cols)), self.order, self.frame)

    def orderBy(self, *cols):
        return WindowSpec(self.parts, tuple(c._sort or f"{c.sql} ASC NULLS FIRST" for c in _cols(cols)), self.frame)

    def rowsBetween(self, start, end):
        return WindowSpec(self.parts, self.order, f"ROWS BETWEEN {_bound(start)} AND {_bound(end)}")

    def rangeBetween(self, start, end):
        return WindowSpec(self.parts, self.order, f"RANGE BETWEEN {_bound(start)} AND {_bound(end)}")

    def _clause(self):
        p = f"PARTITION BY {', '.join(self.parts)}" if self.parts else ""
        o = f"ORDER BY {', '.join(self.order)}" if self.order else ""
        return " ".join(x for x in (p, o, self.frame) if x)


def _bound(n):
    if n <= -sys.maxsize:
        return "UNBOUNDED PRECEDING"
    if n >= sys.maxsize:
        return "UNBOUNDED FOLLOWING"
    return "CURRENT ROW" if n == 0 else f"{abs(n)} {'PRECEDING' if n < 0 else 'FOLLOWING'}"


class Row(tuple):
    """A row of an answer: by position, by name (`row.amount`, `row["amount"]`), or `asDict()`."""

    def __new__(cls, *values, **named):
        row = tuple.__new__(cls, values or tuple(named.values()))
        row.__fields__ = list(named) if named else []
        return row

    def __getattr__(self, name):
        try:
            return self[self.__fields__.index(name)]
        except ValueError:
            raise AttributeError(name) from None

    def __getitem__(self, k):
        return tuple.__getitem__(self, self.__fields__.index(k) if isinstance(k, str) else k)

    def asDict(self):
        return dict(zip(self.__fields__, self))

    def __repr__(self):
        return "Row(" + ", ".join(f"{k}={v!r}" for k, v in zip(self.__fields__, self)) + ")"


# ---------------------------------------------------------------- the session

class _Builder:
    def __init__(self):
        self.url, self.options = None, {}

    def remote(self, url):
        self.url = url.replace("pondra://", "http://").replace("sc://", "http://")
        return self

    def master(self, m):
        return self  # (`local[*]`: a lake here, `spark.pondra.lake` or ./lake)

    def appName(self, _):
        return self

    def config(self, key=None, value=None, **_):
        if key:
            self.options[key] = value
        return self

    def getOrCreate(self):
        if SparkSession._active is None:
            token = self.options.get("spark.pondra.token")
            con = _client.connect(self.url, token) if self.url else _client.local(self.options.get("spark.pondra.lake", "lake"), token=token)
            SparkSession._active = SparkSession(con)
        return SparkSession._active


class _BuilderProperty:
    def __get__(self, *_):
        return _Builder()


class SparkSession:
    builder = _BuilderProperty()
    _active = None

    def __init__(self, con):
        self.con = con
        self.catalog = _Catalog(self)
        self.version = "4.0.0 (pondra)"

    @classmethod
    def getActiveSession(cls):
        return cls._active

    def sql(self, query, args=None, **kwargs):
        """A DataFrame for a query (DataFrames by keyword as `{name}`, `args` as `:name` or
        `$name` parameters); other statements run now, as PySpark's do."""
        names = {k: (v._f if isinstance(v, DataFrame) else v) for k, v in kwargs.items()}
        params = dict(args or {})
        query = re.sub(r":(\w+)\b", lambda m: f"${m.group(1)}" if m.group(1) in params else m.group(0), query)
        caller = sys._getframe(1)
        out = _sql_from(self.con, query, caller, names, params)
        return DataFrame(self, out if isinstance(out, Frame) else self.con.sql("SELECT 1 AS done WHERE FALSE"))  # (DDL and writes: done, as PySpark's)

    def table(self, name):
        return DataFrame(self, self.con.table(name))

    def range(self, start, end=None, step=1, numPartitions=None):
        start, end = (0, start) if end is None else (start, end)
        return DataFrame(self, self.con.sql(f"SELECT value AS id FROM generate_series({int(start)}, {int(end) - 1}, {int(step)})"))

    def createDataFrame(self, data, schema=None):
        """From pandas, Polars or Arrow data, or rows (tuples, dicts, `Row`s) with a schema: column
        names, `"a int, b string"`, or a `StructType`."""
        names, types = _schema(schema)
        if isinstance(data, (list, tuple)):
            rows = [r.asDict() if isinstance(r, Row) and r.__fields__ else r for r in data]
            if rows and not isinstance(rows[0], dict):
                cols = names or [f"_{i + 1}" for i in range(len(rows[0]))]
                rows = [dict(zip(cols, r)) for r in rows]
            import pyarrow as pa
            data = pa.Table.from_pylist(rows) if rows else pa.table({n: [] for n in names})
        df = DataFrame(self, self.con.from_arrow(data))
        if types:
            df = df.select(*[Column(f"CAST({_quote(n)} AS {_type(t)})", n) for n, t in zip(names, types)])
        elif names and not isinstance(schema, str):
            df = df.toDF(*names)
        return df

    def stop(self):
        SparkSession._active = None
        self.con.close()

    @property
    def udf(self):
        """`spark.udf.register(name, f, returnType)`: a Python function for SQL and frames."""
        return _UDFs(self)

    @property
    def read(self):
        """A reader of files (each time a new one, as PySpark's: options don't carry over)."""
        return _Reader(self)

    def readStream(self, *_):
        raise NotImplementedError("Structured Streaming: a materialized view keeps a query up to date as rows arrive (df.to_view(…, materialized=True))")


class _UDFs:
    def __init__(self, spark):
        self.spark = spark

    def register(self, name, f, returnType=None):
        """PySpark's `spark.udf.register`: `f` (a function, or a `udf`) as the lake's function
        `name`, for `spark.sql` and every client; the UDF, for frames."""
        u = f if isinstance(f, UserDefinedFunction) else UserDefinedFunction(f, returnType or "string")
        u = UserDefinedFunction(u.func, returnType or u.returnType, name=name, pandas=u.pandas)
        u._make(self.spark.con)
        return u


class UserDefinedFunction:
    """PySpark's UDF (`F.udf`, `F.pandas_udf`, `spark.udf.register`): a Python function run on the
    nodes, as the lake's own (`CREATE FUNCTION … LANGUAGE python`, ADR-027), made the first time
    it is used. Its imports, helpers and constants go along (`@db.function`'s rules). A pandas UDF
    is called once a batch with pandas Series."""

    def __init__(self, func, returnType="string", name=None, pandas=False):
        import hashlib
        self.func, self.returnType, self.pandas = func, returnType, pandas
        own = func.__name__ if func.__name__ != "<lambda>" else "udf_" + hashlib.sha1(func.__code__.co_code).hexdigest()[:10]
        self.__name__ = name or own
        self._made = None

    def _make(self, con):
        if self._made is con:
            return
        from ..client import _module_of
        body = _module_of(self.func, lambda_name=self.__name__)
        entry = self.func.__name__ if self.func.__name__ != "<lambda>" else self.__name__
        if self.pandas:
            body += f"\n\ndef __pandas_entry(*cols):\n    import pyarrow as pa\n    return pa.Array.from_pandas({entry}(*[c.to_pandas() for c in cols]))\n"
            entry = "__pandas_entry"
        n = self.func.__code__.co_argcount
        params = {f"a{i + 1}": "ANY" for i in range(n)}
        con.create_function(self.__name__, body, params=params, returns=_type(self.returnType), entry=entry, vectorized=self.pandas)
        self._made = con

    def __call__(self, *cols):
        self._make(SparkSession._active.con if SparkSession._active else _client.current())
        cs = [c if isinstance(c, Column) else _col(c) for c in cols]
        return Column(f"{self.__name__}({', '.join(c.sql for c in cs)})", f"{self.__name__}({', '.join(c.name for c in cs)})")


def _sql_from(con, query, caller, names, params):
    """`con.sql` as if called where `spark.sql` was (its Python names are the caller's)."""
    return eval("__con.sql(__q, **__kw)", caller.f_globals, {**caller.f_locals, "__con": con, "__q": query, "__kw": {**names, **params}})


def _schema(schema):
    """Column names and types from what createDataFrame was given."""
    if schema is None:
        return [], []
    if isinstance(schema, str):
        pairs = [p.strip().split(None, 1) for p in re.split(r",(?![^(]*\))", schema)]
        return [p[0] for p in pairs], [p[1] if len(p) > 1 else "string" for p in pairs]
    if hasattr(schema, "fields"):
        return [f.name for f in schema.fields], [f.dataType for f in schema.fields]
    return list(schema), []


class _Catalog:
    def __init__(self, spark):
        self.spark = spark

    def listTables(self, dbName=None):
        rows = self.spark.con.sql("SELECT table_schema AS db, table_name AS name, table_type AS kind FROM information_schema.tables WHERE table_schema <> 'information_schema'").rows()
        return [Row(name=r["name"], database=r["db"], tableType=r["kind"], isTemporary=False) for r in rows if dbName is None or r["db"] == dbName]

    def tableExists(self, name, dbName=None):
        return any(t.name == name.split(".")[-1] for t in self.listTables(dbName))

    def dropTempView(self, name):
        return self.spark.con._temp.pop(name.lower(), None) is not None


class _Reader:
    """`spark.read`: files anywhere (ADR-026: `s3://`, `gs://`, `az://`, `https://`, and a
    `pondra.local` session's own machine), as SQL's `read_parquet`, `read_csv` and `read_json`
    read them, with Spark's options and defaults: a folder's `k=v` folders are columns; CSV has no
    header and every column a string unless `header` and `inferSchema` say otherwise. An option
    Pondra doesn't take is refused by name, never ignored."""

    # Spark's options (case-insensitive) → the reader's arguments; None: taken here, not passed on.
    OPTIONS = {"parquet": {"mergeschema": "union_by_name", "recursivefilelookup": None, "pathglobfilter": None},
               "csv": {"header": "header", "sep": "delim", "delimiter": "delim", "quote": "quote", "escape": "escape", "inferschema": None,
                       "encoding": None, "charset": None, "recursivefilelookup": None, "pathglobfilter": None},
               "json": {"multiline": None, "encoding": None, "recursivefilelookup": None, "pathglobfilter": None}}

    def __init__(self, spark):
        self.spark, self.fmt, self.opts, self._schema = spark, "parquet", {}, None

    def format(self, f):
        self.fmt = f.lower()
        return self

    def option(self, k, v):
        self.opts[k.lower()] = v
        return self

    def options(self, **kw):
        for k, v in kw.items():
            self.option(k, v)
        return self

    def schema(self, schema):
        self._schema = schema
        return self

    def load(self, path=None, format=None, schema=None, **options):
        if format:
            self.format(format)
        if schema is not None:
            self.schema(schema)
        if path is None:
            raise ValueError("spark.read.load(path): which files?")
        return self.options(**options)._read([path] if isinstance(path, str) else list(path))

    def parquet(self, *paths, **options):
        return self.format("parquet").options(**options)._read(list(paths))

    def csv(self, path, schema=None, **options):
        return self.format("csv").load(path, schema=schema, **{k: v for k, v in options.items() if v is not None})

    def json(self, path, schema=None, **options):
        return self.format("json").load(path, schema=schema, **{k: v for k, v in options.items() if v is not None})

    def table(self, name):
        return self.spark.table(name)

    def _read(self, paths):
        if self.fmt in ("delta", "iceberg"):
            return self._table(paths)
        known = self.OPTIONS.get(self.fmt)
        if known is None:
            raise ValueError(f"spark.read.format('{self.fmt}'): parquet, csv or json")
        o = {k: str(v).lower() == "true" if str(v).lower() in ("true", "false") else v for k, v in self.opts.items()}  # (Spark takes "true" and True)
        for k, v in o.items():
            if k not in known:
                raise ValueError(f"spark.read.option('{k}') isn't taken for {self.fmt}: {', '.join(sorted(known))}")
            if k in ("encoding", "charset") and str(v).lower().replace("-", "") != "utf8" or k == "multiline" and v is True:
                raise ValueError(f"spark.read.option('{k}', {v!r}): files are read as UTF-8, JSON as one record a line")
        glob = o.get("pathglobfilter")
        paths = [p.rstrip("/") + "/**/" + glob if glob else p for p in paths]
        args = {"hive_partitioning": False} if o.get("recursivefilelookup") is True else {}  # (else k=v folders are found)
        args.update({known[k]: v for k, v in o.items() if known[k]})
        if self.fmt == "csv":
            args.setdefault("header", False)  # (Spark's: a CSV file's first line is a row)
        fn = {"parquet": "read_parquet", "csv": "read_csv", "json": "read_json"}[self.fmt]
        where = _literal(paths[0]) if len(paths) == 1 else "[" + ", ".join(_literal(p) for p in paths) + "]"
        given = "".join(f", {k} => {_literal(v)}" for k, v in args.items())
        frame = self.spark.con.sql(f"SELECT * FROM {fn}({where}{given})")
        header = args.get("header") is True
        names, types_ = _schema(self._schema)
        if self.fmt == "csv" and (names or not header or o.get("inferschema") is not True):
            cols = frame.columns  # (a CSV file's columns by position: Spark's _c0, _c1 … without a header)
            names = names or [c if header else f"_c{i}" for i, c in enumerate(cols)]
            types_ = types_ or ["string"] * len(cols)
            if len(names) != len(cols):
                raise ValueError(f"the schema has {len(names)} columns, the files {len(cols)}")
            frame = frame.select(*[sql_expr(f"CAST({_quote(c)} AS {_type(t)}) AS {_quote(n)}") for c, n, t in zip(cols, names, types_)])
        elif names:  # (Parquet's and JSON's columns by name)
            frame = frame.select(*[sql_expr(f"CAST({_quote(n)} AS {_type(t)}) AS {_quote(n)}") for n, t in zip(names, types_ or ["string"] * len(names))])
        return DataFrame(self.spark, frame)

    # Delta's and Iceberg's options for reading an older version, as SQL's arguments.
    TABLE_OPTIONS = {"delta": {"versionasof": "version"}, "iceberg": {"snapshot-id": "snapshot_from_id", "as-of-timestamp": None}}

    def _table(self, paths):
        if len(paths) != 1:
            raise ValueError(f"spark.read.format('{self.fmt}').load(path): one table")
        known, args = self.TABLE_OPTIONS[self.fmt], {}
        for k, v in self.opts.items():
            if k not in known:
                raise ValueError(f"spark.read.option('{k}') isn't taken for {self.fmt}: {', '.join(sorted(known))}")
            if k == "as-of-timestamp":  # (Iceberg's: milliseconds since 1970)
                import datetime
                args["snapshot_from_timestamp"] = datetime.datetime.fromtimestamp(int(v) / 1000, datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")
            else:
                args[known[k]] = int(v)
        given = "".join(f", {k} => {_literal(v)}" for k, v in args.items())
        return DataFrame(self.spark, self.spark.con.sql(f"SELECT * FROM read_{self.fmt}({_literal(str(paths[0]))}{given})"))


# ---------------------------------------------------------------- data frames

class DataFrame:
    """A PySpark DataFrame: a `pondra.Frame` underneath (`df._f`), run when asked."""

    def __init__(self, spark, frame, alias=None, join=None, lineage=()):
        self.spark, self._f, self._alias = spark, frame, alias or f"_d{next(_ids)}"
        self._join = join  # (just joined: FROM clause, WHERE, SELECT — the aliases still in scope)
        self._lineage = {self._alias, *lineage}  # (the DataFrames joined into this one: `orders.o_orderkey` still means its column)

    def __repr__(self):
        return f"DataFrame[{self._f.sql}]"

    def __getitem__(self, name):
        if isinstance(name, Column):
            return self.filter(name)
        return Column(f"{self._alias}.{_name_sql(name)}", name)

    def __getattr__(self, name):
        if name.startswith("_"):
            raise AttributeError(name)
        return self[name]

    def _new(self, frame):
        return DataFrame(self.spark, frame)

    def _step(self, select, where="", tail="", order=None):
        """The next step: over the join's FROM if one was just made (its aliases in scope)."""
        if self._join and not select.startswith(("*", "DISTINCT")):
            src, conds, _ = self._join
            w = " AND ".join([*conds, *([where] if where else [])])
            by = f" ORDER BY {', '.join(o for o, _ in order)}" if order else ""
            return self._new(self._f._with(f"SELECT {select} FROM {src}" + (f" WHERE {w}" if w else "") + by + tail, self._f._ctes, order=order))
        return self._new(self._f._step(lambda r: f"SELECT {select} FROM {r}" + (f" WHERE {where}" if where else ""), order, tail))

    # what it is
    @property
    def columns(self):
        return self._f.columns

    @property
    def dtypes(self):
        return [(f.name, str(f.type)) for f in self._f.schema]

    @property
    def schema(self):
        return self._f.schema

    def printSchema(self):
        print("root\n" + "\n".join(f" |-- {f.name}: {f.type} (nullable = {str(f.nullable).lower()})" for f in self._f.schema))

    # columns
    def select(self, *cols):
        return self._step(", ".join(_named(c) for c in _cols(cols)))

    def selectExpr(self, *exprs):
        from .functions import expr
        return self.select(*[expr(e) for e in exprs])

    def withColumn(self, name, col):
        return self.withColumns({name: col})

    def withColumns(self, cols):
        have = {c.lower(): c for c in self.columns}
        replace = [f"{_as_val(c).sql} AS {_quote(have[n.lower()])}" for n, c in cols.items() if n.lower() in have]
        new = [f"{_as_val(c).sql} AS {_quote(n)}" for n, c in cols.items() if n.lower() not in have]
        return self._step(", ".join([f"* REPLACE ({', '.join(replace)})" if replace else "*", *new]))

    def withColumnRenamed(self, old, new):
        return self.select(*[Column(_quote(c), new) if c.lower() == old.lower() else Column(_quote(c), c) for c in self.columns])

    def withColumnsRenamed(self, mapping):
        low = {k.lower(): v for k, v in mapping.items()}
        return self.select(*[Column(_quote(c), low.get(c.lower(), c)) for c in self.columns])

    def toDF(self, *names):
        return self.select(*[Column(_quote(c), n) for c, n in zip(self.columns, names)])

    def drop(self, *cols):
        have = {c.lower(): c for c in self.columns}
        gone = [have[c.lower()] for c in (x.name if isinstance(x, Column) else x for x in cols) if c.lower() in have]  # (PySpark: unknown names are no error)
        return self._new(self._f.drop(*gone)) if gone else self

    # rows
    def filter(self, condition):
        c = condition if isinstance(condition, Column) else Column(f"({condition})", condition)
        if self._join:
            src, conds, sel = self._join
            conds = [*conds, c.sql]
            return DataFrame(self.spark, self._f._with(f"SELECT {sel} FROM {src} WHERE {' AND '.join(conds)}", self._f._ctes), join=(src, conds, sel))
        return self._new(self._f.filter(_strip(c.sql)))

    where = filter

    def orderBy(self, *cols, ascending=True):
        cs = _cols(cols)
        asc = ascending if isinstance(ascending, (list, tuple)) else [ascending] * len(cs)
        order = [(c._sort or (c.asc() if a else c.desc())._sort, c.name) for c, a in zip(cs, asc)]
        return self._step("*", order=[(_strip(o), n) for o, n in order])

    sort = orderBy

    def limit(self, n):
        return self._step("*", tail=f" LIMIT {int(n)}", order=self._f._kept())

    def distinct(self):
        return self._step("DISTINCT *")

    def dropDuplicates(self, subset=None):
        if not subset:
            return self.distinct()
        return self._step(f"DISTINCT ON ({', '.join(_name_sql(c) for c in subset)}) *")

    drop_duplicates = dropDuplicates

    def union(self, other):
        from ..frame import concat
        return self._new(concat([self._f, other._f]))

    unionAll = union

    def unionByName(self, other, allowMissingColumns=False):
        from ..frame import concat
        return self._new(concat([self._f, other._f], how="diagonal"))

    def sample(self, withReplacement=None, fraction=None, seed=None):
        return self._new(self._f.sample(fraction=fraction if fraction is not None else withReplacement))

    @property
    def na(self):
        return _Na(self)

    def fillna(self, value, subset=None):
        return _Na(self).fill(value, subset)

    def dropna(self, how="any", thresh=None, subset=None):
        return _Na(self).drop(how, thresh, subset)

    # joins
    def alias(self, name):
        return DataFrame(self.spark, self._f, alias=name, lineage=self._lineage)

    def join(self, other, on=None, how="inner"):
        """PySpark's join: by column names (the keys once, first), or by a condition (every column of both)."""
        how = {"outer": "full", "full_outer": "full", "fullouter": "full", "left_outer": "left", "leftouter": "left", "right_outer": "right",
               "rightouter": "right", "semi": "leftsemi", "left_semi": "leftsemi", "anti": "leftanti", "left_anti": "leftanti"}.get(how, how)
        ctes, kw = self._f._joined(other._f)
        lr, rr = self._f._as_rel(ctes), other._f._as_rel(ctes)
        l, r = self._alias, other._alias
        kind = {"inner": "JOIN", "left": "LEFT JOIN", "right": "RIGHT JOIN", "full": "FULL JOIN", "cross": "CROSS JOIN", "leftsemi": "LEFT SEMI JOIN", "leftanti": "LEFT ANTI JOIN"}[how]
        if on is None or isinstance(on, Column) or (isinstance(on, list) and on and isinstance(on[0], Column)):
            cond = " AND ".join(c.sql for c in _cols([on])) if on is not None else "TRUE"
            # (a column of a DataFrame joined in earlier is the column of the side that holds it now)
            for names, to in ((self._lineage, l), (other._lineage, r)):
                for n in names - {to}:
                    cond = re.sub(rf"\b{re.escape(n)}\.", f"{to}.", cond)
            src = f"{lr} AS {l} {kind} {rr} AS {r}" + ("" if how == "cross" else f" ON {cond}")
            sel = f"{l}.*" if how in ("leftsemi", "leftanti") else f"{l}.*, {r}.*"
        else:
            keys = [on] if isinstance(on, str) else list(on)
            src = f"{lr} AS {l} {kind} {rr} AS {r} ON " + " AND ".join(f"{l}.{_name_sql(k)} = {r}.{_name_sql(k)}" for k in keys)
            if how in ("leftsemi", "leftanti"):
                sel = f"{l}.*"
            else:
                side = {"right": r, "full": None}.get(how, l)
                key_cols = [f"{side}.{_name_sql(k)}" if side else f"coalesce({l}.{_name_sql(k)}, {r}.{_name_sql(k)}) AS {_quote(k)}" for k in keys]
                low = {k.lower() for k in keys}
                rest = [f"{l}.{_quote(c)}" for c in self.columns if c.lower() not in low] + [f"{r}.{_quote(c)}" for c in other.columns if c.lower() not in low]
                sel = ", ".join(key_cols + rest)
        frame = self._f._with(f"SELECT {sel} FROM {src}", ctes, **kw)
        # (joined by a condition, the next step may still name `a.x`; joined by names, the keys are
        # one column each: the next step reads the join's answer)
        pending = (src, [], sel) if on is None or isinstance(on, Column) or (isinstance(on, list) and on and isinstance(on[0], Column)) else None
        return DataFrame(self.spark, frame, join=pending, lineage=self._lineage | other._lineage)

    def crossJoin(self, other):
        return self.join(other, how="cross")

    # aggregations
    def groupBy(self, *cols):
        return GroupedData(self, _cols(cols))

    groupby = groupBy

    def agg(self, *exprs):
        return GroupedData(self, []).agg(*exprs)

    # results
    def toArrow(self):
        return self._settled().collect()

    def toPandas(self):
        return self.toArrow().to_pandas()

    def collect(self):
        t = self.toArrow()
        names = t.column_names
        return [Row(**dict(zip(names, r.values()))) if len(set(names)) == len(names) else Row(*r.values()) for r in t.to_pylist()]

    def count(self):
        return self._new(self._settled()).select(Column("count(*)", "count")).collect()[0][0]

    def first(self):
        rows = self.limit(1).collect()
        return rows[0] if rows else None

    def head(self, n=None):
        return self.first() if n is None else self.limit(n).collect()

    def take(self, n):
        return self.limit(n).collect()

    def show(self, n=20, truncate=True, vertical=False):
        t = self.limit(n).toPandas()
        if truncate:
            t = t.map(lambda v: (str(v)[:17] + "...") if isinstance(v, str) and len(v) > 20 else v)
        print(t.to_string(index=False))

    def explain(self, extended=False):
        print(self._settled().explain())

    def _settled(self):
        """The frame, a join made a step of its own (so its answer has every column)."""
        return self._f

    # names others read
    def createOrReplaceTempView(self, name):
        self.spark.con._temp[name.lower()] = self._f

    createTempView = createOrReplaceTempView
    registerTempTable = createOrReplaceTempView

    def cache(self):
        return self  # (the lake and its nodes' SSD tier already keep what is read)

    persist = cache

    def unpersist(self, blocking=False):
        return self

    def repartition(self, *_, **__):
        return self  # (the nodes decide how work is shared)

    coalesce = repartition

    @property
    def write(self):
        return _Writer(self)

    @property
    def rdd(self):
        raise NotImplementedError("RDDs: use DataFrame methods, or spark.sql")

    def toLocalIterator(self):
        return iter(self.collect())

    def to_view(self, name, **kw):
        """Pondra's own: this DataFrame as a view (materialized=True: kept up to date as rows arrive)."""
        return self._f.to_view(name, **kw)


def _strip(sql):
    """A column made from `df["x"]` names its DataFrame's alias; outside a join there is none."""
    return re.sub(r"\b_d\d+\.", "", sql)


class _Na:
    def __init__(self, df):
        self.df = df

    def fill(self, value, subset=None):
        cols = [c for c in (subset or self.df.columns)]
        if isinstance(value, dict):
            fills = value
        else:
            import pyarrow.types as t
            kinds = {f.name: f.type for f in self.df._f.schema}
            ok = (lambda ty: t.is_string(ty) or t.is_large_string(ty) or t.is_string_view(ty)) if isinstance(value, str) else \
                 (lambda ty: t.is_boolean(ty)) if isinstance(value, bool) else (lambda ty: t.is_integer(ty) or t.is_floating(ty) or t.is_decimal(ty))
            fills = {c: value for c in cols if c in kinds and ok(kinds[c])}
        if not fills:
            return self.df
        return self.df.withColumns({c: Column(f"coalesce({_quote(c)}, {_literal(v)})", c) for c, v in fills.items()})

    def drop(self, how="any", thresh=None, subset=None):
        cols = subset or self.df.columns
        nn = [f"{_quote(c)} IS NOT NULL" for c in cols]
        cond = " AND ".join(nn) if how == "any" else " OR ".join(nn)
        if thresh is not None:
            cond = " + ".join(f"CASE WHEN {x} THEN 1 ELSE 0 END" for x in nn) + f" >= {int(thresh)}"
        return self.df.filter(cond)


class GroupedData:
    def __init__(self, df, keys):
        self.df, self.keys = df, keys

    def agg(self, *exprs):
        if len(exprs) == 1 and isinstance(exprs[0], dict):  # ({"amount": "sum"})
            from . import functions as F
            exprs = [getattr(F, {"mean": "avg"}.get(fn, fn))(c) for c, fn in exprs[0].items()]
        cols = [*self.keys, *_cols(exprs)]
        group = f" GROUP BY {', '.join(_strip(k.sql) if not self.df._join else k.sql for k in self.keys)}" if self.keys else ""
        sel = ", ".join((_named(c) if self.df._join else _strip(_named(c))) for c in cols)
        return self.df._step(sel, tail=group)

    def count(self):
        return self.agg(Column("count(*)", "count"))

    def _each(fn):
        def go(self, *cols):
            from . import functions as F
            names = cols or [f.name for f in self.df._f.schema if str(f.type).startswith(("int", "uint", "float", "double", "decimal"))]
            return self.agg(*[getattr(F, fn)(c) for c in names])
        return go

    sum, avg, min, max = _each("sum"), _each("avg"), _each("min"), _each("max")
    mean = avg

    def pivot(self, *_):
        raise NotImplementedError("pivot: write it in SQL with CASE WHEN (spark.sql)")


class _Writer:
    """`df.write`: into the lake's tables (`saveAsTable`, `insertInto`), or into files anywhere
    (`parquet`, `csv`, `json`, `save`: SQL's `COPY … TO`, a folder of files as Spark writes, by
    `partitionBy` in `k=v` folders; `format("delta")` and `format("iceberg")` a table in the folder).
    Modes are Spark's: `error` (the default: a folder holding files is refused), `overwrite`,
    `append`, `ignore`."""

    OPTIONS = {"parquet": {"compression": "compression"}, "json": {}, "csv": {"header": "header", "sep": "delimiter", "delimiter": "delimiter"}}

    def __init__(self, df):
        self.df, self.how, self.fmt, self.opts, self.parts = df, "error", "parquet", {}, []

    def mode(self, m):
        m = m.lower()
        if m not in ("error", "errorifexists", "overwrite", "append", "ignore"):
            raise ValueError(f"mode {m!r}: error, overwrite, append or ignore")
        self.how = "error" if m == "errorifexists" else m
        return self

    def format(self, f):
        self.fmt = f.lower()
        return self

    def option(self, k, v):
        self.opts[k.lower()] = v
        return self

    def options(self, **kw):
        for k, v in kw.items():
            self.option(k, v)
        return self

    def partitionBy(self, *cols):
        self.parts = [c for cs in cols for c in ([cs] if isinstance(cs, str) else cs)]
        return self

    def saveAsTable(self, name):
        f, con = self.df._f, self.df.spark.con
        if self.parts:
            raise ValueError("saveAsTable with partitionBy: make the table with partition_by (CREATE TABLE … WITH (partition_by = '…'))")
        if self.how == "append":
            try:
                return f.write_table(name, "append")
            except RuntimeError as e:
                if "no table" not in str(e):
                    raise
        if self.how == "ignore" and self.df.spark.catalog.tableExists(name):
            return None
        return f.write_table(name, "overwrite" if self.how == "overwrite" else "create")

    def insertInto(self, name, overwrite=False):
        f = self.df._f
        if overwrite:
            self.df.spark.con._run(f"DELETE FROM {name}")
        return f.write_table(name, "append")

    def save(self, path=None, format=None, mode=None, partitionBy=None, **options):
        if format:
            self.format(format)
        if mode:
            self.mode(mode)
        if partitionBy:
            self.partitionBy(partitionBy)
        if path is None:
            raise ValueError("df.write.save(path): where to? (saveAsTable(name) for a table)")
        if self.fmt in ("delta", "iceberg"):
            if self.parts or self.opts or options:
                raise ValueError(f"df.write.format('{self.fmt}').save(path): a table as it is (partitionBy and options: not yet)")
            return self.df._f._table_to(path, self.fmt, self.how)  # (COPY … TO … (FORMAT delta): made, appended to, or replaced)
        known = self.OPTIONS.get(self.fmt)
        if known is None:
            raise ValueError(f"df.write.format('{self.fmt}'): parquet, csv, json, delta or iceberg")
        self.options(**{k: v for k, v in options.items() if v is not None})
        for k in self.opts:
            if k not in known:
                raise ValueError(f"df.write.option('{k}') isn't taken for {self.fmt}: {', '.join(sorted(known)) or 'none'}")
        given = {known[k]: v for k, v in self.opts.items()}
        if self.fmt == "csv":
            given.setdefault("header", False)  # (Spark's: no header line unless asked)
        given.update({"overwrite": True} if self.how == "overwrite" else {"append": True} if self.how == "append" else {})
        folder = path if path.endswith("/") else path + "/"  # (Spark writes a folder of files)
        cols = ", ".join(_quote(c) for c in self.parts)
        opts = [f"FORMAT {self.fmt}"] + [f"PARTITION_BY ({cols})"] * bool(self.parts) + [f"{k.upper()} {_literal(v)}" for k, v in given.items()]
        f = self.df._f
        try:
            return self.df.spark.con._run(f"COPY ({f.sql}) TO {_literal(folder)} ({', '.join(opts)})", f._params, f._sent)
        except RuntimeError as e:
            if self.how == "ignore" and "holds files already" in str(e):
                return None
            raise

    def parquet(self, path, mode=None, partitionBy=None, compression=None):
        return self.format("parquet").save(path, mode=mode, partitionBy=partitionBy, compression=compression)

    def csv(self, path, mode=None, partitionBy=None, sep=None, header=None, **options):
        return self.format("csv").save(path, mode=mode, partitionBy=partitionBy, sep=sep, header=header, **options)

    def json(self, path, mode=None, partitionBy=None, **options):
        return self.format("json").save(path, mode=mode, partitionBy=partitionBy, **options)

    def delta(self, path, mode=None):  # (Delta's own `df.write.delta(path)`, from delta-spark)
        return self.format("delta").save(path, mode=mode)


class DeltaTable:
    """`delta.tables.DeltaTable`: updates, deletes and MERGE of a table, as Delta on Spark writes them."""

    def __init__(self, spark, name, alias=None):
        self.spark, self.name, self._alias = spark, name, alias

    @classmethod
    def forName(cls, spark, name):
        return cls(spark, name)

    @classmethod
    def forPath(cls, spark, path):
        raise NotImplementedError("DeltaTable.forPath: a Pondra table goes by its name (DeltaTable.forName)")

    def alias(self, name):
        return DeltaTable(self.spark, self.name, name)

    def toDF(self):
        return self.spark.table(self.name)

    def update(self, condition=None, set=None):
        cond = f" WHERE {_as_sql(condition)}" if condition is not None else ""
        return self.spark.con._run(f"UPDATE {self.name} SET {', '.join(f'{_name_sql(k)} = {_as_sql(v)}' for k, v in (set or {}).items())}{cond}")

    def delete(self, condition=None):
        return self.spark.con._run(f"DELETE FROM {self.name}" + (f" WHERE {_as_sql(condition)}" if condition is not None else ""))

    def merge(self, source, condition):
        return _MergeBuilder(self, source, condition)


def _as_sql(v):
    return _strip(v.sql) if isinstance(v, Column) else str(v)


class _MergeBuilder:
    def __init__(self, target, source, condition):
        self.t, self.s, self.cond, self.whens = target, source, _as_sql(condition), []

    def _add(self, c):
        self.whens.append(c)
        return self

    def _set(self, d):
        return ", ".join(f"{_name_sql(k)} = {_as_sql(v)}" for k, v in d.items())

    def whenMatchedUpdate(self, condition=None, set=None):
        return self._add(f"WHEN MATCHED{_when(condition)} THEN UPDATE SET {self._set(set)}")

    def whenMatchedUpdateAll(self, condition=None):
        cols = [c for c in self.t.toDF().columns if c in self.s.columns]
        return self.whenMatchedUpdate(condition, {c: f"{self._sa()}.{_quote(c)}" for c in cols})

    def whenMatchedDelete(self, condition=None):
        return self._add(f"WHEN MATCHED{_when(condition)} THEN DELETE")

    def whenNotMatchedInsert(self, condition=None, values=None):
        return self._add(f"WHEN NOT MATCHED{_when(condition)} THEN INSERT ({', '.join(map(_name_sql, values))}) VALUES ({', '.join(_as_sql(v) for v in values.values())})")

    def whenNotMatchedInsertAll(self, condition=None):
        cols = [c for c in self.t.toDF().columns if c in self.s.columns]
        return self.whenNotMatchedInsert(condition, {c: f"{self._sa()}.{_quote(c)}" for c in cols})

    def whenNotMatchedBySourceDelete(self, condition=None):
        return self._add(f"WHEN NOT MATCHED BY SOURCE{_when(condition)} THEN DELETE")

    def _sa(self):
        return self.s._alias if not self.s._alias.startswith("_d") else "source"

    def execute(self):
        src = self.s._f
        t = self.t._alias or self.t.name.split(".")[-1]
        sql = f"MERGE INTO {self.t.name} AS {t} USING ({src.sql}) AS {self._sa()} ON {self.cond} " + " ".join(self.whens)
        return self.t.spark.con._run(sql, src._params, src._sent)


def _when(c):
    return f" AND {_as_sql(c)}" if c is not None else ""


class types:
    """`pyspark.sql.types`, enough for schemas and casts."""

    class _T:
        def __init__(self, *args):
            self.args = args

        def simpleString(self):
            name = type(self).__name__[:-4].lower()
            return {"integer": "int", "long": "bigint", "short": "smallint"}.get(name, name) + (f"({','.join(map(str, self.args))})" if self.args else "")

    class StructField:
        def __init__(self, name, dataType, nullable=True):
            self.name, self.dataType, self.nullable = name, dataType, nullable

    class StructType:
        def __init__(self, fields=None):
            self.fields = list(fields or [])

        def add(self, name, dataType, nullable=True):
            self.fields.append(types.StructField(name, dataType, nullable))
            return self


for _n in ("String", "Integer", "Long", "Short", "Byte", "Double", "Float", "Boolean", "Date", "Timestamp", "Decimal", "Binary"):
    setattr(types, f"{_n}Type", type(f"{_n}Type", (types._T,), {}))
