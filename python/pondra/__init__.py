"""Pondra from Python: SQL into pandas / Polars / Arrow, exactly-once appends, change feeds and
key lookups, over a node's HTTP API. Pure Python; pyarrow (and pandas or Polars) only if you use
their results. `pip install pondra` also installs the `pondra` binary itself.

    import pondra
    db = pondra.local("lake")                                   # a node on ./lake, here; or
    db = pondra.connect("http://127.0.0.1:8080", token=None)    # one running somewhere
    db.sql("CREATE TABLE events (user VARCHAR, amount BIGINT)")
    db.append("events", [{"user": "ann", "amount": 5}])       # or a pandas / Polars / Arrow table
    df = db.sql("SELECT user, sum(amount) AS total FROM events GROUP BY user").to_pandas()
    for row in db.watch("events"): ...                          # new rows as they commit

Also over the Postgres protocol (`pondra serve --pg 0.0.0.0:5432`) with psycopg, SQLAlchemy, etc.
"""
import atexit
import io
import json
import os
import shutil
import socket
import subprocess
import sysconfig
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

__version__ = "0.19.0"
__all__ = ["connect", "local", "Pondra", "Result"]


class Result:
    """A query's answer, as Arrow IPC bytes: convert it to what you work with."""

    def __init__(self, ipc: bytes):
        self.ipc = ipc

    def to_arrow(self):
        import pyarrow as pa
        return pa.ipc.open_stream(self.ipc).read_all() if self.ipc else pa.table({})

    def to_pandas(self):
        return self.to_arrow().to_pandas()

    def to_polars(self):
        import polars as pl
        return pl.from_arrow(self.to_arrow())

    def rows(self):
        return self.to_arrow().to_pylist()


class Pondra:
    def __init__(self, url="http://127.0.0.1:8080", token=None, producer=None, timeout=300):
        self.url, self.token, self.timeout = url.rstrip("/"), token, timeout
        self.producer = producer or f"py-{uuid.uuid4().hex[:12]}"  # exactly-once: one name, increasing seq
        self.seq = 0
        # a node on this machine is reached directly, whatever proxy the environment names
        local = urllib.parse.urlsplit(self.url).hostname in ("127.0.0.1", "localhost", "::1")
        self._open = urllib.request.build_opener(urllib.request.ProxyHandler({})).open if local else urllib.request.urlopen

    def _call(self, method, path, body=b"", headers=None, stream=False):
        h = dict(headers or {})
        if self.token:
            h["Authorization"] = f"Bearer {self.token}"
        if getattr(self, "owner", None):
            h["x-pondra-owner"] = self.owner  # (the node `local()` started: its SQL may read files here)
        req = urllib.request.Request(self.url + path, data=body if method == "POST" else None, headers=h, method=method)
        try:
            r = self._open(req, timeout=None if stream else self.timeout)
        except urllib.error.HTTPError as e:
            raise RuntimeError(f"{e.code}: {e.read().decode(errors='replace')[:500]}") from None
        return r if stream else r.read()

    def sql(self, query, job=None):
        """A query's rows (`Result`), or for CREATE TABLE / INSERT / UPDATE / DELETE the outcome
        (a dict). `job`: a write retried with the same job id is applied once."""
        path = "/sql?format=arrow" + (f"&job={job}" if job else "")
        out = self._call("POST", path, query.encode())
        return json.loads(out) if out[:1] == b"{" else Result(out)

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

    def view(self, name, sql, **options):
        """A view: `sql` over each new batch of rows, committed with them (with GROUP BY, kept per
        key). Options make it emit what is final: `window="w", size_secs=60, lateness_secs=10`, or
        `session="ts", gap_secs=1800`. Asking again for the same view changes nothing."""
        query = urllib.parse.urlencode(options)
        return json.loads(self._call("POST", f"/views/{name}" + (f"?{query}" if query else ""), sql.encode()))

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

    def close(self):
        """Stop the node `local()` started: closing its input stops it (it hands the lake on at
        once), on every OS; it would stop the same way if Python were killed."""
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


def connect(url="http://127.0.0.1:8080", token=None, **kw):
    return Pondra(url, token, **kw)


def local(dir="lake", port=None, token=None, flags=(), timeout=120):
    """Start a node on a lake here — a folder, or s3://bucket/prefix — and connect to it. It stops
    when Python exits, or with `close()`; the lake stays. Its SQL may read files on this machine
    (`SELECT * FROM 'jan.csv'`), as DuckDB's may. `flags`: more `pondra serve` options, such as
    `["--memory-gb", "2"]`."""
    port, owner = port or _free_port(), uuid.uuid4().hex
    if "://" not in dir:
        os.makedirs(dir, exist_ok=True)
    log = os.path.join(tempfile.gettempdir(), f"pondra-{port}.log")
    with open(log, "ab") as err:
        args = [binary(), "serve", "--dir", dir, "--addr", f"127.0.0.1:{port}", "--stop-with-stdin", *flags]
        proc = subprocess.Popen(args, stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=err, env={**os.environ, "PONDRA_OWNER_KEY": owner})
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


def _encode(data):
    if isinstance(data, (list, tuple)):
        return "".join(json.dumps(r) + "\n" for r in data).encode(), "application/x-ndjson"
    import pyarrow as pa
    if hasattr(data, "to_arrow"):  # Polars
        data = data.to_arrow()
    elif not isinstance(data, (pa.Table, pa.RecordBatch)):  # pandas
        data = pa.Table.from_pandas(data, preserve_index=False)
    buf = io.BytesIO()
    with pa.ipc.new_stream(buf, data.schema) as w:
        w.write(data) if isinstance(data, pa.RecordBatch) else w.write_table(data)
    return buf.getvalue(), "application/vnd.apache.arrow.stream"
