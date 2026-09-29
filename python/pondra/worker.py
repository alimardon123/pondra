"""A node's Python worker (ADR-027): `python -m pondra.worker`, started by a node run with
`--python`, serving one request at a time over its standard input and output until that closes.

A message is frames, each its length (4 bytes, little-endian) and its bytes: a JSON head
`{"parts": n, "head": {…}}`, then n parts (Arrow IPC streams). The node asks for one of two things:

- `apply`: a function's batch (its arguments, one column each) → its values, one column; or, for a
  table function, its rows.
- `call`: a procedure, its arguments and a token lent its caller's rights → notices (what it
  prints, `plpy.notice`) as they come, then the answer: `{"kind": "rows"}` and the rows,
  `{"kind": "sql", "sql": …}` (a frame: the node runs it) or `{"kind": "none"}`.
- `cell`: a session's code (a console cell, a DO block sent in a session) on the worker the node
  keeps for that session, in the session's namespace: what one cell makes, the next one sees.
  Answered as `call` is.

A failure answers `{"error": "…"}`, and the worker goes on. A body is compiled once per worker, and
what it imports stays imported: that is why a call takes milliseconds, not a Python start.
"""
import ast
import inspect
import io
import json
import os
import struct
import sys
import traceback

_compiled = {}  # (name, body, entry, params) -> code
_sessions = {}  # a session's namespace, kept between its cells (this worker is that session's)
_functions = {}  # the same key -> a function's callable (its module kept: constants, helpers)
_out = None  # the answers' channel


def main():
    global _out
    _out = os.fdopen(os.dup(1), "wb")
    os.dup2(2, 1)  # (anything else written to standard output goes to the node's log)
    sys.stdout = Notices()
    inp = sys.stdin.buffer
    import pyarrow  # noqa: F401 (once, now: every request needs it)
    import pondra  # noqa: F401
    while True:
        n = inp.read(4)
        if len(n) < 4:
            return  # (the node closed our input: stop)
        msg = json.loads(_exact(inp, struct.unpack("<I", n)[0]))
        head, parts = msg["head"], [_exact(inp, struct.unpack("<I", _exact(inp, 4))[0]) for _ in range(msg["parts"])]
        try:
            answer, parts = {"apply": apply, "call": call, "check": check, "cell": cell}[head["op"]](head, parts)
        except BaseException as e:  # (a routine's `sys.exit()` too: the worker stays)
            answer, parts = {"error": failure(e, head)}, []
        send(answer, parts)


def _exact(inp, n):
    b = inp.read(n)
    if len(b) < n:
        raise SystemExit(0)  # (the node went away mid-message)
    return b


def send(head, parts=()):
    frames = [json.dumps({"parts": len(parts), "head": head}).encode(), *parts]
    _out.write(b"".join(struct.pack("<I", len(f)) + f for f in frames))
    _out.flush()


class Notices(io.TextIOBase):
    """Standard output while a procedure runs: each line printed goes to its caller as a notice
    (psql shows NOTICE, the shell and the clients print it). Otherwise: the node's log."""

    def __init__(self):
        self.on, self.rest = False, ""

    def writable(self):
        return True

    def write(self, s):
        if not self.on:
            return sys.stderr.write(s)
        *lines, self.rest = (self.rest + s).split("\n")
        for line in lines:
            send({"notice": line})
        return len(s)

    def notice(self, text):
        self.flush_line()
        if self.on:
            send({"notice": str(text)})
        else:
            print(text, file=sys.stderr)

    def flush_line(self):
        if self.rest:
            rest, self.rest = self.rest, ""
            send({"notice": rest})

    def start(self):
        self.on, self.rest = True, ""

    def stop(self):
        self.flush_line()
        self.on = False


# ---------------------------------------------------------------- bodies


def code(head):
    """A routine's body compiled: a module whose `entry` function is called (the decorators' form:
    the notebook's imports, helpers and constants, then the function), or — Postgres's PL/Python
    form — the body of a function taking the parameters, whose last line, if an expression, is
    what it returns."""
    key = (head["name"], head["body"], head.get("entry") or "", tuple(head.get("params", ())))
    if key not in _compiled:
        if len(_compiled) > 512:
            _compiled.clear()
            _functions.clear()
        filename = f"<{head['name']}>"
        tree = ast.parse(_dedent(head["body"]), filename=filename)
        if not head.get("entry"):
            body = tree.body or [ast.Pass()]
            if isinstance(body[-1], ast.Expr):
                body[-1] = ast.copy_location(ast.Return(body[-1].value), body[-1])
            star = [s for s in body if isinstance(s, ast.ImportFrom) and any(a.name == "*" for a in s.names)]  # (module level only)
            names = [_arg(p, i) for i, p in enumerate(head.get("params", ()))]
            fn = ast.parse(f"def __pondra_body__({', '.join(names)}):\n    pass").body[0]
            if "args" not in names and any(isinstance(n, ast.Name) and n.id == "args" for n in ast.walk(tree)):
                body.insert(0, ast.parse(f"args = [{', '.join(names)}]").body[0])  # (PL/Python's `args`)
            fn.body = [s for s in body if s not in star]
            tree = ast.Module(body=star + [fn], type_ignores=[])
            ast.fix_missing_locations(tree)
        _compiled[key] = (compile(tree, filename, "exec"), head.get("entry") or "__pondra_body__")
    return key, _compiled[key]


def _dedent(body):
    """The body as written in `AS $$ … $$`, its lines' common indentation taken away (PL/Python's
    are usually indented); its lines keep their numbers."""
    import textwrap
    return textwrap.dedent(body)


def _arg(p, i):
    return p if p.isidentifier() else f"_{i + 1}"  # (Postgres's unnamed parameters: $1 is `_1`)


def scope(name, con=None):
    import pondra
    from pondra import plpy
    con = _NoConnection() if con is None else con
    return {"__name__": f"pondra_{name.replace('.', '_')}", "__builtins__": __builtins__, "pondra": pondra, "plpy": plpy, "con": con, "db": con,  # (`db`: a console cell's name for it)
            "SD": {}, "GD": plpy.GD}


class _NoConnection:
    def __getattr__(self, _):
        raise RuntimeError("a function has no connection to the lake (a query may run it over millions of rows on every node): read the rows as its arguments, or do it in a procedure")

    def __bool__(self):
        return False


def failure(e, head):
    """The error for the caller: what was raised, and where in the body."""
    lines = _dedent(head.get("body") or "").splitlines()
    here = f"<{head.get('name')}>"
    where = [f"  line {f.lineno}: {lines[f.lineno - 1].strip()}" for f in traceback.extract_tb(e.__traceback__) if f.filename == here and 0 < f.lineno <= len(lines)]
    traceback.print_exception(type(e), e, e.__traceback__, file=sys.stderr)  # (the whole of it: the node's log)
    return f"{type(e).__name__}: {e}" + "".join("\n" + w for w in where[-3:])


def check(head, _):
    """A body compiled, to find its mistakes when it is made."""
    code(head)
    return {}, []


# ---------------------------------------------------------------- functions


def apply(head, parts):
    """A function over a batch: per row (the default), the whole batch (`vectorized`), or, for a
    table function, once, answering rows."""
    import pyarrow as pa
    from pondra import client
    args = pa.ipc.open_stream(parts[0]).read_all()
    out = pa.ipc.open_stream(parts[1]).schema
    key, (compiled, entry) = code(head)
    f = _functions.get(key)
    if f is None:
        g = scope(head["name"])
        exec(compiled, g)
        f = _functions[key] = g[entry]
    client._inside = "function"
    try:
        wanted = head.get("json") or []
        n = args.num_rows
        if head.get("table"):
            row = [_json_in(args.column(i)[0].as_py(), i < len(wanted) and wanted[i]) for i in range(args.num_columns)] if n else []
            return {}, [_ipc(to_table(f(*row), out))]
        ret = out.field(0).type
        if head.get("vectorized"):
            values = to_array(f(*[args.column(i).combine_chunks() for i in range(args.num_columns)]), ret, n, head.get("json_returns"))
            if head.get("strict") and args.num_columns:
                import pyarrow.compute as pc
                nulls = pc.or_(*[pc.is_null(args.column(i)) for i in range(args.num_columns)]) if args.num_columns > 1 else pc.is_null(args.column(0))
                values = pc.if_else(nulls, pa.scalar(None, ret), values)
        else:
            cols = [[_json_in(v, i < len(wanted) and wanted[i]) for v in args.column(i).to_pylist()] for i in range(args.num_columns)]
            rows = zip(*cols) if cols else [()] * n
            strict = head.get("strict")
            values = to_array([None if strict and any(v is None for v in r) else f(*r) for r in rows], ret, n, head.get("json_returns"))
        return {}, [_ipc(pa.table([values], schema=out))]
    finally:
        client._inside = None


def _json_in(v, is_json):
    """A VARIANT argument as the value its JSON holds (text that isn't JSON: as it is)."""
    if not (is_json and isinstance(v, str)):
        return v
    try:
        return json.loads(v)
    except ValueError:
        return v


def to_array(v, type, n, is_json=False):
    """A function's values as an Arrow array of its type: a list, a pyarrow / pandas / Polars /
    NumPy column."""
    import pyarrow as pa
    if isinstance(v, pa.ChunkedArray):
        v = v.combine_chunks()
    if not isinstance(v, pa.Array):
        if hasattr(v, "to_arrow"):  # Polars
            v = v.to_arrow()
        elif type_of(v) == "pandas":
            v = pa.Array.from_pandas(v)
        else:
            v = list(v)
            if is_json:
                v = [None if x is None else json.dumps(x, default=str) for x in v]
            try:
                v = pa.array(v, type=type)
            except (pa.ArrowInvalid, pa.ArrowTypeError, TypeError, ValueError, OverflowError):
                if not (pa.types.is_string(type) or pa.types.is_large_string(type)):
                    raise
                v = pa.array([None if x is None else x if isinstance(x, str) else json.dumps(x, default=str) if isinstance(x, (dict, list)) else str(x) for x in v], type=type)
    if len(v) != n:
        raise ValueError(f"{len(v)} values for {n} rows")
    return v if v.type == type else v.cast(type)


def to_table(v, schema):
    """A table function's answer as its RETURNS TABLE: rows (tuples, dicts or single values, a list
    or a generator), a dict of columns, or a pandas / Polars / Arrow table. Columns are matched by
    name, or else by position."""
    import pyarrow as pa
    if v is None:
        return schema.empty_table()
    if type(v).__name__ == "LazyFrame":
        v = v.collect()
    if hasattr(v, "to_arrow") and not isinstance(v, (pa.Table, pa.RecordBatch)):
        v = v.to_arrow()
    elif type_of(v) == "pandas":
        v = pa.Table.from_pandas(v, preserve_index=False)
    if isinstance(v, pa.RecordBatch):
        v = pa.Table.from_batches([v])
    if isinstance(v, dict):
        v = pa.table(v)
    if not isinstance(v, pa.Table):
        rows = list(v)
        if rows and isinstance(rows[0], dict):
            v = pa.Table.from_pylist(rows)
        else:
            rows = [r if isinstance(r, (tuple, list)) else (r,) for r in rows]
            cols = [list(c) for c in zip(*rows)] if rows else [[] for _ in schema]
            if len(cols) != len(schema):
                raise ValueError(f"rows of {len(cols)} values: RETURNS TABLE has {len(schema)} columns ({', '.join(schema.names)})")
            return pa.table([to_array(c, f.type, len(rows)) for c, f in zip(cols, schema)], schema=schema)
    if all(name in v.column_names for name in schema.names):
        cols = [v.column(name) for name in schema.names]
    elif v.num_columns == len(schema):
        cols = v.columns
    else:
        raise ValueError(f"columns {', '.join(v.column_names)}: RETURNS TABLE ({', '.join(schema.names)})")
    return pa.table([c.cast(f.type) for c, f in zip(cols, schema)], schema=schema)


def type_of(v):
    return type(v).__module__.split(".")[0]


def _ipc(table):
    import pyarrow as pa
    buf = io.BytesIO()
    with pa.ipc.new_stream(buf, table.schema) as w:
        w.write(table) if isinstance(table, pa.RecordBatch) else w.write_table(table)
    return buf.getvalue()


# ---------------------------------------------------------------- procedures


def call(head, parts):
    """A procedure, as its caller: `pondra.sql` and `con` are a connection back to the node with
    the caller's rights, lent for as long as this runs."""
    import pyarrow as pa
    import pondra
    from pondra import client
    rows = pa.ipc.open_stream(parts[0]).read_all().to_pylist()
    args = rows[0] if rows else {}
    wanted = dict(zip(head.get("params", ()), head.get("json") or ()))
    args = {k: _json_in(v, wanted.get(k)) for k, v in args.items()}
    con = pondra.connect(head["url"], token=head["token"], headers={"x-pondra-depth": str(head["depth"])}, job=head.get("job"))
    _, (compiled, entry) = code(head)
    g = scope(head["name"], con)
    client._current = con
    sys.stdout.start()
    try:
        exec(compiled, g)
        f = g[entry]
        if head.get("entry"):
            params = list(inspect.signature(f).parameters)
            given = {k: args[k] for k in params if k in args}
            value = f(con, **given) if params[:1] == ["con"] and "con" not in args else f(**given)
        else:
            value = f(*[args.get(p) for p in head.get("params", ())])
    except SystemExit as e:
        if e.code not in (None, 0):
            raise
        value = None
    finally:
        sys.stdout.stop()
        client._current = client._last = None
    kind, data = reply(value, head["name"])
    return kind, [data] if data else []


def cell(head, parts):
    """A session's cell, in the session's namespace, kept from one cell to the next, as a notebook's
    kernel keeps it. Its last line, if an expression, is its answer. `db` (and `con`) is one
    connection back to the node for the whole session, lent each cell's rights as it runs, so a
    frame made in one cell can be collected in the next."""
    import ast as _ast
    import pondra
    from pondra import client
    g = _sessions.get(head["session"])
    if g is None:
        con = pondra.connect(head["url"], token=head["token"], echo=False)
        g = _sessions[head["session"]] = scope("cell", con)
        g["__pondra_con__"] = con
    con = g["__pondra_con__"]
    con.token, con._job = head["token"], head.get("job")
    con._headers["x-pondra-depth"] = str(head["depth"])
    for k in ("db", "con"):
        g.setdefault(k, con)
    filename = f"<{head['name']}>"
    tree = _ast.parse(_dedent(head["body"]), filename=filename)
    last = None
    if tree.body and isinstance(tree.body[-1], _ast.Expr):
        last = _ast.Expression(tree.body.pop().value)
    client._current = con
    sys.stdout.start()
    try:
        exec(compile(tree, filename, "exec"), g)
        value = eval(compile(last, filename, "eval"), g) if last is not None else None
    except SystemExit as e:
        if e.code not in (None, 0):
            raise
        value = None
    finally:
        sys.stdout.stop()
        client._current = client._last = None
    if value is not None:
        g["_"] = value  # (the last answer, as Python's shell keeps it)
    kind, data = reply(value, "cell")
    return kind, [data] if data else []


def reply(value, name):
    """A procedure's answer: nothing; a frame's SQL (the node runs it: it may spread, and nothing
    crosses twice); or rows — a table, a list of dicts, one dict (a row) or one value."""
    import pyarrow as pa
    from pondra.client import Result, _arrow
    from pondra.frame import Frame
    if value is None or isinstance(value, dict) and not value:
        return {"kind": "none"}, b""
    if isinstance(value, Frame) and not value._sent and not value._params:
        return {"kind": "sql", "sql": value.sql}, b""
    if isinstance(value, (Frame, Result)):
        value = value.collect()
    elif isinstance(value, dict) and not all(isinstance(v, (list, tuple)) for v in value.values()):
        value = [value]  # (one row)
    elif not (isinstance(value, (list, tuple, dict)) or hasattr(value, "to_arrow") or hasattr(value, "schema") or type_of(value) == "pandas"):
        value = pa.table({"value" if name in ("do", "cell") else name.split(".")[-1]: [value]})  # (one value: a procedure's by its name, a DO block's or a cell's `value`)
    table = pa.table(value) if isinstance(value, dict) else _arrow(value)
    return {"kind": "rows"}, _ipc(table)


if __name__ == "__main__":
    main()
