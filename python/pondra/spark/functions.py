"""PySpark's `functions`, for `pondra.spark`: `from pondra.spark import functions as F`.

Each builds a `Column` — its SQL, and the name PySpark would give its column (`sum(amount)`,
`(a + 1)`) — where PySpark's meaning differs from SQL's, the SQL says PySpark's (a string
concatenation with a null is null, `dayofweek` counts from Sunday = 1, …).
"""
from . import Column, _col, _lit


def col(name):
    return _col(name)


column = col


def lit(value):
    return _lit(value)


def expr(sql):
    """A SQL expression, as it is (PySpark's `expr`): `F.expr("amount * 0.9 AS net")`."""
    from ..frame import sql_expr
    e = sql_expr(sql)
    return Column(e.sql, e.name or sql.strip())


def _f(name, *cols, sql=None, label=None):
    cs = [c if isinstance(c, Column) else _col(c) for c in cols]
    inner = ", ".join(c.sql for c in cs)
    return Column(f"{sql or name}({inner})", label or f"{name}({', '.join(c.name for c in cs)})")


def when(condition, value):
    return Column._case([(condition, value)])


def coalesce(*cols):
    return _f("coalesce", *cols)


def isnull(c):
    return _col(c).isNull()


def isnan(c):
    return _f("isnan", c)


def sum(c):  # noqa: A001 (PySpark's names)
    return _f("sum", c)


def avg(c):
    return _f("avg", c)


mean = avg


def count(c):
    if isinstance(c, str) and c == "*":
        return Column("count(*)", "count(1)")
    return _f("count", c)


def countDistinct(c, *more):
    cs = [x if isinstance(x, Column) else _col(x) for x in (c, *more)]
    return Column(f"count(DISTINCT {', '.join(x.sql for x in cs)})", f"count(DISTINCT {', '.join(x.name for x in cs)})")


count_distinct = countDistinct


def approx_count_distinct(c, rsd=None):
    return _f("approx_count_distinct", c, sql="approx_distinct")


def min(c):  # noqa: A001
    return _f("min", c)


def max(c):  # noqa: A001
    return _f("max", c)


def first(c, ignorenulls=False):
    return _f("first", c, sql="first_value")


def last(c, ignorenulls=False):
    return _f("last", c, sql="last_value")


def stddev(c):
    return _f("stddev", c, sql="stddev_samp")


def variance(c):
    return _f("variance", c, sql="var_samp")


def collect_list(c):
    return _f("collect_list", c, sql="array_agg")


def abs(c):  # noqa: A001
    return _f("abs", c)


def round(c, scale=0):  # noqa: A001
    x = c if isinstance(c, Column) else _col(c)
    return Column(f"round({x.sql}, {int(scale)})", f"round({x.name}, {int(scale)})")


def sqrt(c):
    return _f("SQRT", c, sql="sqrt")


def exp(c):
    return _f("EXP", c, sql="exp")


def log(c):
    return _f("ln", c)


def pow(c, p):  # noqa: A001
    x, y = (v if isinstance(v, Column) else _lit(v) if not isinstance(v, str) else _col(v) for v in (c, p))
    return Column(f"power({x.sql}, {y.sql})", f"POWER({x.name}, {y.name})")


def floor(c):
    return _f("FLOOR", c, sql="floor")


def ceil(c):
    return _f("CEIL", c, sql="ceil")


def greatest(*cols):
    return _f("greatest", *cols)


def least(*cols):
    return _f("least", *cols)


def upper(c):
    return _f("upper", c)


def lower(c):
    return _f("lower", c)


def length(c):
    return _f("length", c, sql="character_length")


def trim(c):
    return _f("trim", c)


def ltrim(c):
    return _f("ltrim", c)


def rtrim(c):
    return _f("rtrim", c)


def substring(c, pos, length):
    x = _col(c) if isinstance(c, str) else c
    return Column(f"substr({x.sql}, {int(pos)}, {int(length)})", f"substring({x.name}, {pos}, {length})")


def concat(*cols):
    """Null if any part is (PySpark's), unlike SQL's concat()."""
    cs = [c if isinstance(c, Column) else _col(c) for c in cols]
    return Column("(" + " || ".join(c.sql for c in cs) + ")", f"concat({', '.join(c.name for c in cs)})")


def concat_ws(sep, *cols):
    cs = [c if isinstance(c, Column) else _col(c) for c in cols]
    return Column(f"concat_ws({_lit(sep).sql}, {', '.join(c.sql for c in cs)})", f"concat_ws({sep}, {', '.join(c.name for c in cs)})")


def regexp_replace(c, pattern, replacement):
    x = _col(c) if isinstance(c, str) else c
    return Column(f"regexp_replace({x.sql}, {_lit(pattern).sql}, {_lit(replacement).sql}, 'g')", f"regexp_replace({x.name}, {pattern}, {replacement}, 1)")


def regexp_extract(c, pattern, idx):
    x = _col(c) if isinstance(c, str) else c
    return Column(f"coalesce((regexp_match({x.sql}, {_lit(pattern).sql}))[{int(idx)}], '')", f"regexp_extract({x.name}, {pattern}, {idx})")


def split(c, pattern):
    x = _col(c) if isinstance(c, str) else c
    return Column(f"string_to_array({x.sql}, {_lit(pattern).sql})", f"split({x.name}, {pattern}, -1)")


def date_trunc(fmt, c):
    x = _col(c) if isinstance(c, str) else c
    return Column(f"date_trunc({_lit(fmt.lower()).sql}, {x.sql})", f"date_trunc({fmt}, {x.name})")


def to_date(c, fmt=None):
    x = _col(c) if isinstance(c, str) else c
    return Column(f"to_date({x.sql})" if fmt is None else f"to_date({x.sql}, {_lit(_chrono(fmt)).sql})", f"to_date({x.name})")


def to_timestamp(c, fmt=None):
    x = _col(c) if isinstance(c, str) else c
    return Column(f"to_timestamp({x.sql})" if fmt is None else f"to_timestamp({x.sql}, {_lit(_chrono(fmt)).sql})", f"to_timestamp({x.name})")


def _chrono(fmt):
    """A Java date pattern (`yyyy-MM-dd HH:mm:ss`) as chrono's (`%Y-%m-%d %H:%M:%S`)."""
    for java, c in (("yyyy", "%Y"), ("MM", "%m"), ("dd", "%d"), ("HH", "%H"), ("mm", "%M"), ("ss", "%S"), ("SSS", "%3f")):
        fmt = fmt.replace(java, c)
    return fmt


def _part(name, part, plus=""):
    return lambda c: Column(f"CAST(date_part('{part}', {(_col(c) if isinstance(c, str) else c).sql}){plus} AS INT)", f"{name}({(_col(c) if isinstance(c, str) else c).name})")


year, month, dayofmonth, hour, minute, second = (_part(n, p) for n, p in (("year", "year"), ("month", "month"), ("dayofmonth", "day"), ("hour", "hour"), ("minute", "minute"), ("second", "second")))
dayofweek = _part("dayofweek", "dow", " + 1")  # (Sunday = 1)


def datediff(end, start):
    e, s = (_col(x) if isinstance(x, str) else x for x in (end, start))
    return Column(f"CAST(CAST({e.sql} AS DATE) - CAST({s.sql} AS DATE) AS INT)", f"datediff({e.name}, {s.name})")


def date_add(c, days):
    x = _col(c) if isinstance(c, str) else c
    return Column(f"CAST(CAST({x.sql} AS DATE) + INTERVAL '{int(days)} days' AS DATE)", f"date_add({x.name}, {days})")


def date_sub(c, days):
    return date_add(c, -int(days))


def current_date():
    return Column("current_date()", "current_date()")


def current_timestamp():
    return Column("now()", "current_timestamp()")


def desc(c):
    return _col(c).desc()


def asc(c):
    return _col(c).asc()


def row_number():
    return Column("row_number()", "row_number()")


def rank():
    return Column("rank()", "RANK()")


def dense_rank():
    return Column("dense_rank()", "DENSE_RANK()")


def lag(c, offset=1, default=None):
    x = _col(c) if isinstance(c, str) else c
    d = "" if default is None else f", {_lit(default).sql}"
    return Column(f"lag({x.sql}, {int(offset)}{d})", f"lag({x.name}, {offset}, {default})")


def lead(c, offset=1, default=None):
    x = _col(c) if isinstance(c, str) else c
    d = "" if default is None else f", {_lit(default).sql}"
    return Column(f"lead({x.sql}, {int(offset)}{d})", f"lead({x.name}, {offset}, {default})")


def broadcast(df):
    return df  # (the planner decides)
