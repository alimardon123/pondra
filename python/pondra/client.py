"""The client: a node over HTTP (`connect`), or one started here (`local`). Queries become frames
(`frame.py`); writes, scripts and procedures run at once."""
import atexit
import base64
import importlib.util
import inspect
import io
import itertools
import json
import os
import re
import shutil
import socket
import struct
import subprocess
import sys
import sysconfig
import tempfile
import textwrap
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
import warnings

from .frame import Frame, _literal, _quote, sql_type, trailing_order

_names = itertools.count(1)
_last = None  # the newest connection (what `%%sql` cells and `pondra.sql` use)
_current = None  # inside a routine: the connection lent its caller (the worker sets it for each call)
_inside = None  # "function" while a worker runs a function: it has no connection


def current():
    """The connection `pondra.sql`, `pondra.table`, `pondra.call` and `pondra.secret` use: inside a
    procedure, the one lent its caller (their rights, for as long as it runs); elsewhere, the
    newest one made (`connect`, `local`), as `duckdb.sql` uses DuckDB's default connection."""
    if _inside == "function":
        raise RuntimeError("a function has no connection to the lake (a query may run it over millions of rows on every node): read the rows as its arguments, or do it in a procedure")
    if (_current or _last) is None:
        raise RuntimeError("no connection yet: pondra.connect(url) or pondra.local(dir) first")
    return _current or _last


def _has_arrow():
    """Whether pyarrow is here. Without it, rows come as the node's JSON (dates and times as text)
    and `rows()`, `item()`, `show()` still work; tables (`collect()`, `to_pandas()`…) need it."""
    return importlib.util.find_spec("pyarrow") is not None


def _pyarrow(what):
    try:
        import pyarrow
        return pyarrow
    except ImportError:
        raise ImportError(f"{what} needs pyarrow: pip install pyarrow (rows() works without it)") from None


def _json_rows(out):
    """The node's JSON rows (it leaves out nulls: they come back as None)."""
    rows = json.loads(out)
    if not isinstance(rows, list):
        return []  # (an outcome, not rows)
    keys = list(dict.fromkeys(k for r in rows for k in r))
    return [{k: r.get(k) for k in keys} for r in rows]


class Result:
    """Rows a statement returned at once (a script's last query, a procedure's answer): Arrow IPC,
    or JSON rows where pyarrow isn't installed."""

    def __init__(self, ipc: bytes = b"", rows=None):
        self.ipc, self._rows = ipc, rows

    def to_arrow(self):
        pa = _pyarrow("to_arrow()")
        if self._rows is not None:
            return pa.Table.from_pylist(self._rows)
        return pa.ipc.open_stream(self.ipc).read_all() if self.ipc else pa.table({})

    collect = to_arrow

    def to_pandas(self):
        return self.to_arrow().to_pandas()

    def to_polars(self):
        import polars as pl
        return pl.from_arrow(self.to_arrow())

    def rows(self):
        return self._rows if self._rows is not None else self.to_arrow().to_pylist()

    def __repr__(self):
        return repr(self._rows) if self._rows is not None else repr(self.to_arrow())


class Pondra:
    def __init__(self, url="http://127.0.0.1:8080", token=None, producer=None, timeout=300, headers=None, job=None, echo=True, user=None, password=None, ca=None):
        global _last
        self.url, self.token, self.timeout = url.rstrip("/"), token, timeout
        # (a user signs in with its name and password, or its token as the password: HTTP Basic)
        self._basic = "Basic " + base64.b64encode(f"{user}:{password}".encode()).decode() if user else None
        self.notices, self.echo = [], echo  # what the last statement's procedures printed; printed here too, unless echo=False
        self.producer = producer or f"py-{uuid.uuid4().hex[:12]}"  # exactly-once: one name, increasing seq
        self.seq = 0
        self.session = uuid.uuid4().hex  # (this connection's temporary tables and views: the node's, until close())
        self._headers, self._job, self._jobs, self._temp = dict(headers or {}), job, itertools.count(1), {}
        # a node on this machine is reached directly, whatever proxy the environment names
        local = urllib.parse.urlsplit(self.url).hostname in ("127.0.0.1", "localhost", "::1")
        handlers = [urllib.request.ProxyHandler({})] if local else []
        if ca:  # (a node's own certificate, or its authority's: a PEM file to trust besides the system's)
            import ssl
            handlers.append(urllib.request.HTTPSHandler(context=ssl.create_default_context(cafile=ca)))
        self._open = urllib.request.build_opener(*handlers).open if handlers else urllib.request.urlopen
        _last = self

    def _call(self, method, path, body=b"", headers=None, stream=False):
        h = {"x-pondra-session": self.session, **self._headers, **(headers or {})}
        if self.token:
            h["Authorization"] = f"Bearer {self.token}"
        elif self._basic:
            h["Authorization"] = self._basic
        if getattr(self, "owner", None):
            h["x-pondra-owner"] = self.owner  # (the node `local()` started: its SQL may read files here)
        req = urllib.request.Request(self.url + path, data=body if method == "POST" else None, headers=h, method=method)
        try:
            r = self._open(req, timeout=None if stream else self.timeout)
        except urllib.error.HTTPError as e:
            self._heard(e.headers)
            raise RuntimeError(f"{e.code}: {e.read().decode(errors='replace')}") from None
        if stream:
            return r
        self._heard(r.headers)
        return r.read()

    def _heard(self, headers):
        """What the statement's procedures printed (the node's `x-pondra-notices`): kept in
        `notices`, and printed, as psql shows NOTICEs (inside a procedure: to its own caller)."""
        said = headers.get("x-pondra-notices") if headers else None
        self.notices = json.loads(said) if said else []
        if self.echo:
            for n in self.notices:
                print(n)

    def _post(self, sql, params=None, sent=None, job=None, views=None, format=None):
        """Send statements: as they are; with `$name` parameters and frames by name (`views`: JSON);
        or with tables of our own too (`application/vnd.pondra.request`: the JSON's length, the
        JSON, then each table's length and Arrow IPC)."""
        format = format or ("arrow" if _has_arrow() else "json")
        path = f"/sql?format={format}" + (f"&job={urllib.parse.quote(job)}" if job else "")
        if not params and not sent and not views:
            return self._call("POST", path, sql.encode())
        head = {"sql": sql, "params": {k: _param(v) for k, v in (params or {}).items()}, "views": views or {}, "tables": list(sent or {})}
        if not sent:
            return self._call("POST", path, json.dumps(head).encode(), {"content-type": "application/json"})
        h = json.dumps(head).encode()
        body = [struct.pack("<I", len(h)), h]
        for t in sent.values():
            b = _encode(t)[0]
            body += [struct.pack("<Q", len(b)), b]
        return self._call("POST", path, b"".join(body), {"content-type": "application/vnd.pondra.request"})

    def _run(self, sql, params=None, sent=None, job=None, views=None):
        """Run statements now: the last one's rows (`Result`) or outcome (a dict)."""
        if job is None and self._job:
            job = f"{self._job}:{next(self._jobs)}"  # (a procedure's writes: its caller's job, each its own)
        out = self._post(sql, params, sent, job, views)
        if out[:1] == b"{":
            return json.loads(out)
        return Result(rows=_json_rows(out)) if out[:1] == b"[" else Result(out)

    # ------------------------------------------------------------ SQL and frames

    def sql(self, query, job=None, **names):
        """A query as a frame, run when asked (`collect()`, `to_pandas()`…); other statements
        (writes, DDL, `CALL`, several at once) run now. Python names in the SQL — frames, pandas,
        Polars and Arrow data — are found where `sql` was called, or given by keyword as `{name}`;
        other keywords are `$name` parameters: `con.sql("SELECT * FROM {r} WHERE amount > $min",
        r=recent, min=100)`. `job`: a write retried with the same job is applied once."""
        return self._sql(query, job, names, sys._getframe(1))

    def _sql(self, query, job, names, caller):
        frames = {k: v for k, v in names.items() if _data(v) or isinstance(v, Frame)}
        params = {k: v for k, v in names.items() if k not in frames}
        text = re.sub(r"\{(\w+)\}", lambda m: m.group(1) if m.group(1) in frames else m.group(0), query)
        scopes = ((frames, caller.f_locals, caller.f_globals),)
        if _is_query(text):
            f = Frame(self, text, params=params, scopes=scopes, order=trailing_order(text))
            for name, v in frames.items():
                f = _bound(f, name.lower(), v)
            return f
        views, sent = {}, {}
        for name, v in frames.items():
            _attach(name.lower(), v, views, sent, params)
        for _ in range(32):  # (a name looked up as `_frame_rows` does; one statement only: it failed before doing anything)
            try:
                return self._run(text, params, sent, job, views)
            except RuntimeError as e:
                name = _missing(e)
                value = name and ";" not in _code(text) and self._lookup(name, scopes)
                if value is None:
                    raise
                _attach(name, value, views, sent, params)
        raise RuntimeError("too many names to look up")

    def table(self, name):
        """A table (or view) as a frame."""
        return Frame(self, f"SELECT * FROM {name}", rel=name, table=name)

    def from_arrow(self, data, name=None):
        """Rows of this machine as a frame: a pyarrow, pandas or Polars table (or a list of dicts),
        sent with each query that reads it (and only there: it runs on the node it reached)."""
        name = name or f"_t{next(_names)}"
        return Frame(self, f"SELECT * FROM {name}", rel=name, sent={name: _arrow(data)})

    from_pandas = from_polars = from_rows = from_arrow

    # ------------------------------------------------------------ files anywhere (ADR-026)

    def read_parquet(self, source, hive_partitioning=None, **options):
        """Parquet files as a frame, read where the node runs each time it's asked for rows: a
        URL (`s3://`, `gs://`, `az://`, `https://`), a folder, a glob or a list of them; with
        `local()`, this machine's paths too. SQL's `read_parquet`: a URL needs a secret covering it
        (`CREATE SECRET`), unless the node is yours. (Polars' `scan_parquet` is the same.)"""
        return self._scan("read_parquet", source, dict(options, hive_partitioning=hive_partitioning))

    def read_csv(self, source, separator=None, has_header=None, hive_partitioning=None, **options):
        """CSV files as a frame (SQL's `read_csv`; Polars' `scan_csv` is the same). SQL's option
        names work too (`delim=`, `header=`); Polars' (`separator=`, `has_header=`) win if both."""
        polars = {"delim": separator, "header": has_header, "hive_partitioning": hive_partitioning}
        return self._scan("read_csv", source, {**options, **{k: v for k, v in polars.items() if v is not None}})

    def read_json(self, source, hive_partitioning=None, **options):
        """JSON lines as a frame (SQL's `read_json`; Polars' `scan_ndjson` and `read_ndjson` are the same)."""
        return self._scan("read_json", source, dict(options, hive_partitioning=hive_partitioning))

    def read_delta(self, source, version=None):
        """A Delta table as a frame (SQL's `read_delta`): its latest version, or `version`; deletion
        vectors, column mapping and partitions as Delta's own readers read them."""
        return self._scan("read_delta", source, {"version": version})

    def read_iceberg(self, source, snapshot_id=None, version=None, allow_moved_paths=None):
        """An Iceberg table as a frame (SQL's `read_iceberg`): its folder or a metadata file; its
        current snapshot, or `snapshot_id`, or its metadata `version`."""
        return self._scan("read_iceberg", source, {"snapshot_from_id": snapshot_id, "version": version, "allow_moved_paths": allow_moved_paths})

    # Polars' names for the same (ADR-028: Pondra's names first, the tools' as fallbacks).
    scan_parquet, scan_csv, scan_delta, scan_iceberg = read_parquet, read_csv, read_delta, read_iceberg
    scan_ndjson = read_ndjson = read_json

    def _scan(self, fn, source, options):
        paths = [str(source)] if isinstance(source, (str, os.PathLike)) else [str(s) for s in source]
        where = _literal(paths[0]) if len(paths) == 1 else "[" + ", ".join(_literal(p) for p in paths) + "]"
        args = "".join(f", {k} => {_literal(v)}" for k, v in options.items() if v is not None)
        return Frame(self, f"SELECT * FROM {fn}({where}{args})")

    def run(self, file, job=None, wait=True, **params):
        """A file's statements, in order, `$name` taking `name`'s value: the last one's rows or
        outcome. A `.sql` file here (or SQL itself) runs from here; otherwise a file of the lake's
        (`etl/orders.sql`, `.py`, `.ipynb`, or `notebooks/<name>`: ADR-033) runs on the node, as `CALL run(…)`, logged in
        `pondra.runs`; `wait=False` starts it there (a `Run`, as `call`'s)."""
        where = str(file)
        if where.endswith(".sql") and os.path.exists(file):
            return self._run(open(file, encoding="utf-8").read(), params, None, job)
        if not where.endswith((".sql", ".py", ".ipynb")) and not re.fullmatch(r"(files/)?notebooks/[\w.-]+", where):  # (a saved notebook, by name)
            return self._run(where, params, None, job)
        given = "".join(f", {_quote(k)} => {_literal(v)}" for k, v in params.items())  # (quoted: `$myDay` is `myDay`)
        if wait:
            return self._run(f"CALL run({_literal(where)}{given})", job=job)
        return Run(self, self._run(f"SELECT pondra.start('run', {_literal(where)}{given})", job=job).rows()[0]["run"])

    def _frame_rows(self, frame, format=None):
        """A frame's rows: a pyarrow Table, or (no pyarrow here) a list of dicts, or the text table
        `format="table"` asks for. A name the lake doesn't have is looked for among this
        connection's temporary views and the Python names where the frame's SQL was written
        (DuckDB's rule)."""
        for _ in range(32):
            try:
                out = self._post(frame.sql, frame._params, frame._sent, format=format)
                if format == "table":
                    return out.decode()
                if out[:1] == b"[" or not _has_arrow():
                    return _json_rows(out)
                import pyarrow as pa
                return pa.ipc.open_stream(out).read_all() if out[:1] != b"{" else pa.table({})
            except RuntimeError as e:
                name = _missing(e)
                value = name and self._lookup(name, frame._scopes)
                if value is None:
                    raise
                frame = _bound(frame, name, value)
        raise RuntimeError("too many names to look up")

    def _lookup(self, name, scopes):
        if name.lower() in self._temp:
            return self._temp[name.lower()]
        for where in scopes:
            for d in where:
                for k in (name, *[k for k in d if k.lower() == name.lower()]):
                    v = d.get(k)
                    if isinstance(v, Frame) or _data(v):
                        return v
        return None

    # ------------------------------------------------------------ functions and procedures (ADR-027)

    def call(self, name, *args, wait=True, **kwargs):
        """`CALL name(…)`: its rows (`Result`) or outcome. What it prints comes back as notices
        (printed here, and in `notices`). `wait=False`: started on the node, not waited for (SQL's
        `pondra.start`): a `Run`, whose row of `pondra.runs` says how it went."""
        given = ", ".join([_literal(a) for a in args] + [f"{k} => {_literal(v)}" for k, v in kwargs.items()])
        if wait:
            return self._run(f"CALL {name}({given})")
        out = self._run(f"SELECT pondra.start({_literal(name)}{', ' + given if given else ''})")
        return Run(self, out.rows()[0]["run"])

    def secret(self, name):
        """A secret's values (`CREATE SECRET name (TYPE generic, …)`), as a dict: only a procedure's
        code reads them (its lent connection), and they are kept out of what it prints."""
        return json.loads(self._call("GET", f"/secrets/{urllib.parse.quote(name)}"))

    def create_function(self, name, body=None, file=None, params=None, returns=None, language="python", entry=None, vectorized=False, strict=False,
                        volatility=None, packages=None, timeout=None, cache=None, replace=True):
        """A stored function from code (or a file of it), as `CREATE FUNCTION … LANGUAGE python`:
        `params` maps each parameter to its SQL type (or a Python one), or to (type, default);
        `returns` is a SQL type, or `TABLE (…)`, or a dict of columns and types (a table function).
        The body is a function's body (Postgres's PL/Python form), or, with `entry`, a module whose
        function of that name runs. `cache="10 minutes"`: an answer is reused that long for the
        same arguments (an API or a model called again costs nothing)."""
        body = open(file, encoding="utf-8").read() if file else body
        if isinstance(returns, dict):
            returns = "TABLE (" + ", ".join(f"{_quote(k)} {_sql_type_of(v) or 'VARIANT'}" for k, v in returns.items()) + ")"
        with_ = {"entry": entry, "vectorized": vectorized or None, "packages": packages, "timeout": timeout, "cache": cache}
        words = " ".join(w for w in ["STRICT" if strict else "", (volatility or "").upper()] if w)
        return self._run(f"CREATE {'OR REPLACE ' if replace else ''}FUNCTION {name}({_params(params)}) RETURNS {_sql_type_of(returns) or returns} "
                         f"LANGUAGE {language} {words}{_with(with_)} AS {_dollar(body)}")

    def create_procedure(self, name, body=None, file=None, params=None, language="python", entry=None, packages=None, timeout=None, replace=True):
        """A stored procedure from code (or a file of it): `params` maps each parameter to its SQL
        type, or to (type, default)."""
        body = open(file, encoding="utf-8").read() if file else body
        return self._run(f"CREATE {'OR REPLACE ' if replace else ''}PROCEDURE {name}({_params(params)}) LANGUAGE {language}"
                         f"{_with({'entry': entry, 'packages': packages, 'timeout': timeout})} AS {_dollar(body)}")

    def function(self, fn=None, *, name=None, returns=None, vectorized=False, strict=False, volatility=None, packages=None, timeout=None, cache=None, replace=True):
        """A decorator: this Python function becomes one of the lake's functions, for SQL
        (`SELECT slug(title) …`) and frames (`pondra.fn.slug(col("title"))`), on every node. Its
        types come from its annotations (`returns=` says what it returns otherwise: a SQL type, or a
        dict of columns for a table function); the imports, helper functions and constants it uses
        from where it is defined go with it. It is handed back as it was: it still runs here."""
        def make(f):
            sig = inspect.signature(f)
            params = {p.name: (_sql_type_of(p.annotation) or "ANY", p.default) for p in sig.parameters.values()}
            ret = returns or _sql_type_of(sig.return_annotation)
            if ret is None:
                raise TypeError(f"{f.__name__}: say what it returns — an annotation (-> str) or @db.function(returns='DOUBLE'), or returns={{'col': int, …}} for a table")
            self.create_function(name or f.__name__, _module_of(f), params=params, returns=ret, entry=f.__name__, vectorized=vectorized, strict=strict,
                                 volatility=volatility, packages=packages, timeout=timeout, cache=cache, replace=replace)
            return f
        return make(fn) if fn else make

    def procedure(self, fn=None, *, name=None, packages=None, timeout=None, replace=True):
        """A decorator: this Python function becomes a stored procedure, callable from SQL (`CALL`),
        from any client, and here (`db.call`), running on the node with its caller's rights:
        `pondra.sql(…)` in it is the caller's connection. Its parameters' types come from the
        annotations or the defaults (a parameter without either takes any value, as it comes); the
        imports, helper functions and constants it uses from where it is defined go with it. (A
        first parameter named `con` gets the connection, as in 0.22.) It is handed back as it was."""
        def make(f):
            ps = list(inspect.signature(f).parameters.values())
            ps = ps[1:] if ps and ps[0].name == "con" else ps
            params = {}
            for p in ps:
                t = _sql_type_of(p.annotation) or (_sql_type_of(type(p.default)) if p.default not in (inspect.Parameter.empty, None) else None)
                params[p.name] = (t or "ANY", p.default)
            self.create_procedure(name or f.__name__, _module_of(f), params=params, entry=f.__name__, packages=packages, timeout=timeout, replace=replace)
            return f
        return make(fn) if fn else make

    def routines(self):
        """This lake's functions and procedures (SQL has them as `pondra.routines`)."""
        return json.loads(self._call("GET", "/routines"))

    # ------------------------------------------------------------ rows in, rows out

    def append(self, table, data, retries=10):
        """Append rows — a list of dicts, or a pandas / Polars / Arrow table — exactly once: a
        retry after a lost answer is recognised and not applied twice."""
        self.seq += 1
        body, ctype = _encode(data)
        for attempt in range(retries):
            try:
                return json.loads(self._call("POST", f"/append/{table}?producer={self.producer}&seq={self.seq}", body, {"content-type": ctype}))
            except (OSError, urllib.error.URLError):
                if attempt == retries - 1:
                    raise
                time.sleep(min(0.1 * 2 ** attempt, 5))  # the same seq again: applied once

    def view(self, name, query, materialized=None, temporary=False, replace=None, **options):
        """A view others read by name, as SQL's `CREATE VIEW` and a frame's `to_view` make one:
        `query` (SQL or a frame) runs over the tables as they are when the view is read.
        `materialized=True`: kept up to date instead (`CREATE MATERIALIZED VIEW`), filled from the
        rows already there, then with each batch of new rows, committed with them (with GROUP BY,
        kept per key); `options` make it emit what is final: `window="w", size_secs=60,
        lateness_secs=10` (and `slide_secs=10`: sliding), `session="ts", gap_secs=1800`, or
        `join="streams", time="ts", within_secs=600`. `temporary`: this connection's only.
        The view, as a frame."""
        if materialized is None and options:  # (up to 0.22, db.view made every view a materialized one)
            warnings.warn("db.view(…) with window/session/join options: pass materialized=True (a view is a stored query unless asked)", DeprecationWarning, stacklevel=2)
            materialized = True
        frame = query if isinstance(query, Frame) else Frame(self, query)
        return frame.to_view(name, temporary=temporary, materialized=bool(materialized), replace=replace, **options)

    def write_table(self, name, data, mode="create"):
        """Rows into a table, as a frame's `write_table`: `data` is a frame, SQL, or pandas / Polars
        / Arrow data (a list of dicts too); `mode` create, append or overwrite."""
        frame = data if isinstance(data, Frame) else Frame(self, data) if isinstance(data, str) else self.from_arrow(data)
        return frame.write_table(name, mode)

    def lookup(self, table, key):
        """The current row of one key of a keyed table (None if absent)."""
        rows = json.loads(self._call("GET", f"/lookup/{table}/{key}"))
        return rows[0] if rows else None

    def watch(self, table, after=None, changes=False):
        """New rows of a table as they commit (for keyed tables, every upsert and delete). With
        `after`, a replay from that point first (as far back as the node keeps its change feed).
        With `changes`, every change: UPDATE's and DELETE's too, each row with its `_change_type`
        (insert, update_preimage, update_postimage, delete), `_row_id` and `_version`.
        Each yielded row is a dict; `self.position` is where to resume."""
        path = f"/watch/{table}?marks=true" + (f"&after={after}" if after is not None else "") + ("&changes=true" if changes else "")
        for line in self._call("GET", path, stream=True):
            row = json.loads(line)
            if "_after" in row and len(row) == 1:
                self.position = row["_after"]
                continue
            yield row

    def live(self, query, every_ms=None, **params):
        """A query's answer now, and again each time a commit changes a table it reads (ADR-028):
        each is its rows, a list of dicts; `self.position` is the commit it is as of. An answer
        that comes out the same isn't sent again, and a busy table is queried at most every
        `every_ms` (100). The query runs on the node; stopping the loop ends it there.

            for rows in db.live("SELECT region, sum(amount) AS total FROM orders GROUP BY region"):
                redraw(rows)"""
        if isinstance(query, Frame):
            if query._sent:
                raise ValueError("a live query reads the lake: a frame of rows from Python never changes (write_table them first)")
            query, params = query.sql, {**query._params, **params}
        body = json.dumps({"sql": query, "params": {k: _param(v) for k, v in params.items()}}).encode()
        r = self._call("POST", "/live" + (f"?every_ms={int(every_ms)}" if every_ms else ""), body, {"content-type": "application/json"}, stream=True)
        try:
            for line in r:
                if not line.strip():
                    continue  # (the node's keep-alive)
                answer = json.loads(line)
                if "error" in answer:
                    raise RuntimeError(answer["error"])
                self.position = answer["at"]
                yield _json_rows(json.dumps(answer["rows"]))
        finally:
            r.close()

    def close(self):
        """End this connection's session (its temporary tables and views), and stop the node
        `local()` started: closing its input stops it (it hands the lake on at once), on every OS;
        it would stop the same way if Python were killed."""
        try:
            self._call("DELETE", f"/sessions/{self.session}")
        except Exception:
            pass  # (the node is gone, or never had one: an idle session ends by itself)
        p = getattr(self, "process", None)
        if p and p.poll() is None:
            p.stdin.close()
            try:
                p.wait(15)
            except subprocess.TimeoutExpired:
                p.kill()
                p.wait()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


class Run:
    """A procedure started without waiting (`db.call(…, wait=False)`, SQL's `pondra.start`): its
    `id`, and its row of `pondra.runs` — running, ok or failed; its notices and error."""

    def __init__(self, db, id):
        self.db, self.id = db, id

    def status(self):
        rows = self.db._run("SELECT * FROM pondra.runs WHERE id = $id", {"id": self.id}).rows()
        return rows[0] if rows else {"id": self.id, "status": "starting"}

    def wait(self, timeout=None, every=0.2):
        """Its row once it has ended; a failed run raises its error."""
        deadline = None if timeout is None else time.time() + timeout
        while True:
            row = self.status()
            if row["status"] in ("ok", "failed"):
                if row["status"] == "failed":
                    raise RuntimeError(row.get("error"))
                return row
            if deadline is not None and time.time() > deadline:
                raise TimeoutError(f"run {self.id} is still {row['status']}")
            time.sleep(every)

    def __repr__(self):
        return f"<pondra Run {self.id}>"


def _params(params):
    """`name TYPE [DEFAULT value], …` from {name: type} or {name: (type, default)}."""
    out = []
    for k, t in (params or {}).items():
        t, default = t if isinstance(t, tuple) else (t, inspect.Parameter.empty)
        out.append(f"{k} {_sql_type_of(t) or t}" + ("" if default is inspect.Parameter.empty else f" DEFAULT {_literal(default)}"))
    return ", ".join(out)


def _with(options):
    given = {k: v for k, v in options.items() if v not in (None, False, "")}
    return (" WITH (" + ", ".join(f"{k} = {_literal(v if not isinstance(v, (list, tuple)) else ', '.join(v))}" for k, v in given.items()) + ")") if given else ""


def _dollar(body):
    tag = "$pondra$" if "$pondra$" not in body else f"$p{uuid.uuid4().hex[:8]}$"
    return f"{tag}{body}{tag}"


def _sql_type_of(t):
    """A Python annotation's SQL type: str VARCHAR, int BIGINT, float DOUBLE, bool BOOLEAN, date
    DATE, datetime TIMESTAMP, bytes BYTEA, Decimal DECIMAL, list[T] T[], dict VARIANT; a SQL type
    as it is. None: no annotation, or one SQL has no type for (a pyarrow array: any value)."""
    import typing
    if t is None or t is inspect.Parameter.empty or t is inspect.Signature.empty:
        return None
    if isinstance(t, str):
        return t
    origin, args = typing.get_origin(t), typing.get_args(t)
    if origin in (list, tuple, set) or t in (list, tuple, set):
        inner = _sql_type_of(args[0]) if args else None
        return f"{inner}[]" if inner else "VARIANT"
    if origin is dict or t is dict:
        return "VARIANT"
    if args and type(None) in args:  # Optional[T]
        rest = [a for a in args if a is not type(None)]
        return _sql_type_of(rest[0]) if len(rest) == 1 else None
    known = {str: "VARCHAR", int: "BIGINT", float: "DOUBLE", bool: "BOOLEAN", bytes: "BYTEA"}
    if t in known:
        return known[t]
    import datetime
    import decimal
    return {datetime.datetime: "TIMESTAMP", datetime.date: "DATE", datetime.time: "TIME", decimal.Decimal: "DECIMAL(38, 10)"}.get(t)


def _parses(src):
    try:
        import ast
        ast.parse(src)
        return True
    except SyntaxError:
        return False


def _module_of(f, lambda_name="udf"):
    """A function written in a notebook (or a file) as a module the node can run: the imports,
    helper functions and constants it uses from where it is defined, then its own source (a lambda
    as a def called `lambda_name`). Anything else it uses from there — a DataFrame, an open file, a
    connection — is refused, with the fix."""
    import ast
    import builtins
    import types
    imports, consts, sources, done = [], [], [], set()

    def const(v, depth=0):
        if isinstance(v, (bool, int, str, bytes, type(None))) or isinstance(v, float) and v == v and abs(v) != float("inf"):
            return True
        if isinstance(v, (list, tuple, set, frozenset)) and depth < 8:
            return all(const(x, depth + 1) for x in v)
        return isinstance(v, dict) and depth < 8 and all(const(k, depth + 1) and const(x, depth + 1) for k, x in v.items())

    def used(code):
        names = set(code.co_names)
        for c in code.co_consts:
            if isinstance(c, types.CodeType):
                names |= used(c)
        return names

    def visit(fn):
        if fn.__closure__:
            free = ", ".join(fn.__code__.co_freevars)
            raise TypeError(f"{fn.__name__} uses {free} from the function it is defined in, which can't go with it to the node: define it at the top level, and pass {free} as an argument")
        src = textwrap.dedent(inspect.getsource(fn))
        if fn.__name__ == "<lambda>":  # (PySpark's `F.udf(lambda s: …)`: a def of the same)
            text = src if _parses(src) else src.strip().rstrip(",").rstrip(")")
            args = list(fn.__code__.co_varnames[:fn.__code__.co_argcount])
            try:
                node = next(n for n in ast.walk(ast.parse(text)) if isinstance(n, ast.Lambda) and [a.arg for a in n.args.args] == args)
            except (StopIteration, SyntaxError):
                raise TypeError("this lambda's source can't be read: write it as a def") from None
            src = f"def {lambda_name}({ast.unparse(node.args)}):\n    return {ast.get_source_segment(text, node.body) or ast.unparse(node.body)}\n"
        node = next(n for n in ast.parse(src).body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)))
        signature = [node.args, node.returns] if node.returns else [node.args]  # (its annotations and defaults: read where it is defined)
        names = used(fn.__code__) | {n.id for part in signature for n in ast.walk(part) if isinstance(n, ast.Name)}
        for name in sorted(names):
            if name in done or name not in fn.__globals__ or name in ("pondra", "plpy", "con", fn.__name__):
                continue
            done.add(name)
            v = fn.__globals__[name]
            mod = getattr(v, "__module__", None)
            if isinstance(v, types.ModuleType):
                imports.append(f"import {v.__name__}" + (f" as {name}" if v.__name__ != name else ""))
            elif (inspect.isroutine(v) or inspect.isclass(v)) and mod and mod != fn.__module__ and getattr(sys.modules.get(mod), getattr(v, "__name__", ""), None) is v:
                imports.append(f"from {mod} import {v.__name__}" + (f" as {name}" if v.__name__ != name else ""))
            elif inspect.isfunction(v) and mod == fn.__module__:
                visit(v)  # (a helper of the notebook's: its source, and what it uses)
            elif inspect.isclass(v) and mod == fn.__module__:
                sources.append(textwrap.dedent(inspect.getsource(v)))
            elif const(v) and len(repr(v)) < 1 << 20:
                consts.append(f"{name} = {v!r}")
            elif name not in vars(builtins):
                raise TypeError(f"{fn.__name__} uses {name}, a {type(v).__name__} from where it is defined, which can't go with it to the node: "
                                f"pass it as an argument, or keep it in a table and read it there")
        sources.append("\n".join(src.splitlines()[node.lineno - 1:]))  # (without its decorators)

    visit(f)
    head = "from __future__ import annotations  # (its annotations stay text: what they name needn't come along)\n"
    return head + "\n".join(dict.fromkeys(imports)) + "\n" + "\n".join(consts) + "\n\n" + "\n\n".join(sources) + "\n"


def connect(url="http://127.0.0.1:8080", token=None, **kw):
    """A connection to a node: with a token (`token=`), or as a user (`user=`, `password=`: its
    password, or one of its tokens)."""
    return Pondra(url, token, **kw)


def local(dir="lake", port=None, token=None, flags=(), timeout=120, python=True):
    """Start a node on a lake here — a folder, or s3://bucket/prefix — and connect to it. It stops
    when Python exits, or with `close()`; the lake stays. Its SQL may read files on this machine
    (`SELECT * FROM 'jan.csv'`), as DuckDB's may, and its Python procedures run with this Python
    (`python=False`: none). `flags`: more `pondra serve` options, such as `["--memory-gb", "2"]`."""
    port, owner = port or _free_port(), uuid.uuid4().hex
    if "://" not in dir:
        os.makedirs(dir, exist_ok=True)
    log = os.path.join(tempfile.gettempdir(), f"pondra-{port}.log")
    with open(log, "ab") as err:
        args = [binary(), "serve", "--dir", dir, "--addr", f"127.0.0.1:{port}", "--stop-with-stdin", *(["--python", sys.executable] if python else []), *flags]
        here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # (so its procedures import this very package)
        env = {**os.environ, "PONDRA_OWNER_KEY": owner, "PYTHONPATH": os.pathsep.join(p for p in (here, os.environ.get("PYTHONPATH")) if p)}
        proc = subprocess.Popen(args, stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=err, env=env)
    db = Pondra(f"http://127.0.0.1:{port}", token)
    db.process, db.owner, deadline = proc, owner, time.time() + timeout
    while True:
        try:
            db._call("GET", "/stats")
            break
        except (OSError, RuntimeError):
            if proc.poll() is not None or time.time() > deadline:
                db.close()
                raise RuntimeError(f"the node didn't start; its log: {log}") from None
            time.sleep(0.05)
    atexit.register(db.close)
    return db


def binary():
    """The `pondra` executable: $PONDRA_BIN, the one pip installed with this package, or on PATH."""
    name = "pondra.exe" if os.name == "nt" else "pondra"
    places = [os.environ.get("PONDRA_BIN")]
    for scheme in (None, f"{os.name}_user", "osx_framework_user"):
        try:
            places.append(os.path.join(sysconfig.get_path("scripts", scheme) if scheme else sysconfig.get_path("scripts"), name))
        except KeyError:
            pass
    found = next((p for p in places if p and os.path.isfile(p)), None) or shutil.which(name)
    if not found:
        raise RuntimeError("no pondra binary: pip install pondra (it ships one), or set PONDRA_BIN")
    return found


def _free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _code(sql):
    """SQL without its strings and comments, and its last `;`."""
    return re.sub(r"'(?:[^']|'')*'|\"(?:[^\"]|\"\")*\"|\$(\w*)\$.*?\$\1\$|--[^\n]*|/\*.*?\*/", " ", sql, flags=re.S).strip().rstrip(";")


def _is_query(sql):
    """One statement that returns rows (so it can wait): SELECT, WITH, VALUES, SHOW, DESCRIBE…"""
    text = _code(sql)
    return ";" not in text and re.match(r"(?i)\(*\s*(select|with|values|from|table|show|describe|desc|explain)\b", text) is not None


def _missing(error):
    """The table a planning error says isn't there (its last part), if that's what it says."""
    m = re.search(r"table '([^']+)' not found", str(error))
    return m and m.group(1).split(".")[-1]


def _attach(name, value, views, sent, params):
    """A Python name for a statement that runs now: a frame is its query (the node puts it in
    place), data is sent along."""
    if isinstance(value, Frame):
        views[name] = value.sql
        sent.update(value._sent)
        params.update(value._params)
    else:
        sent[name] = _arrow(value)


def _data(v):
    """pandas, Polars or Arrow data (what SQL may name by its Python name)."""
    kind = type(v).__module__.split(".")[0]
    return kind in ("pandas", "polars", "pyarrow") and hasattr(v, "__len__") or type(v).__name__ == "LazyFrame"


def _arrow(data):
    import pyarrow as pa
    if isinstance(data, (list, tuple)):
        return pa.Table.from_pylist(list(data))
    if type(data).__name__ == "LazyFrame":
        data = data.collect()
    if hasattr(data, "to_arrow"):  # Polars
        return data.to_arrow()
    if isinstance(data, (pa.Table, pa.RecordBatch)):
        return data
    return pa.Table.from_pandas(data, preserve_index=False)


def _bound(frame, name, value):
    """`frame`, with `name` standing for a frame (a CTE before its steps) or for data (sent with it)."""
    keep = dict(rel=frame._rel, table=frame._table, order=frame._order)
    if not isinstance(value, Frame):
        return frame._with(frame._query, frame._ctes, sent={**frame._sent, name: _arrow(value)}, **keep)
    ctes = {**value._ctes, **({name: value._query} if value._query != f"SELECT * FROM {name}" else {}), **frame._ctes}  # (before the steps that read it)
    kw = dict(params={**value._params, **frame._params}, sent={**value._sent, **frame._sent}, scopes=frame._scopes + value._scopes)
    return frame._with(frame._query, ctes, **keep, **kw)


def _param(v):
    """A parameter for the node: JSON's own values as they are; dates, times and decimals as SQL."""
    return v if isinstance(v, (type(None), bool, int, float, str, list)) else {"sql": _literal(v)}


def _encode(data):
    if isinstance(data, (list, tuple)):
        return "".join(json.dumps(r) + "\n" for r in data).encode(), "application/x-ndjson"
    import pyarrow as pa
    data = _arrow(data)
    buf = io.BytesIO()
    with pa.ipc.new_stream(buf, data.schema) as w:
        w.write(data) if isinstance(data, pa.RecordBatch) else w.write_table(data)
    return buf.getvalue(), "application/vnd.apache.arrow.stream"
