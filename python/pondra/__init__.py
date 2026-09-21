"""Pondra from Python: SQL into pandas / Polars / Arrow, exactly-once appends, change feeds and
key lookups, over a node's HTTP API. Pure Python; pyarrow (and pandas or Polars) only if you use
their results.

    import pondra
    db = pondra.connect("http://127.0.0.1:8080", token=None)
    db.sql("CREATE TABLE events (user VARCHAR, amount BIGINT)")
    db.append("events", [{"user": "ann", "amount": 5}])       # or a pandas / Polars / Arrow table
    df = db.sql("SELECT user, sum(amount) AS total FROM events GROUP BY user").to_pandas()
    for row in db.watch("events"): ...                          # new rows as they commit

Also over the Postgres protocol (`pondra serve --pg 0.0.0.0:5432`) with psycopg, SQLAlchemy, etc.
"""
import io
import json
import time
import urllib.error
import urllib.request
import uuid

__all__ = ["connect", "Pondra", "Result"]


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

    def _call(self, method, path, body=b"", headers=None, stream=False):
        h = dict(headers or {})
        if self.token:
            h["Authorization"] = f"Bearer {self.token}"
        req = urllib.request.Request(self.url + path, data=body if method == "POST" else None, headers=h, method=method)
        try:
            r = urllib.request.urlopen(req, timeout=None if stream else self.timeout)
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

    def lookup(self, table, key):
        """The current row of one key of a keyed table (None if absent)."""
        rows = json.loads(self._call("GET", f"/lookup/{table}/{key}"))
        return rows[0] if rows else None

    def watch(self, table, after=None):
        """New rows of a table as they commit (for keyed tables, every upsert and delete). With
        `after`, a replay from that point first (as far back as the node keeps its change feed).
        Each yielded row is a dict; `self.position` is where to resume."""
        path = f"/watch/{table}?marks=true" + (f"&after={after}" if after is not None else "")
        for line in self._call("GET", path, stream=True):
            row = json.loads(line)
            if "_after" in row and len(row) == 1:
                self.position = row["_after"]
                continue
            yield row


def connect(url="http://127.0.0.1:8080", token=None, **kw):
    return Pondra(url, token, **kw)


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
