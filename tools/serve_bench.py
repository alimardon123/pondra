#!/usr/bin/env python3
"""Serving benchmark: how fast can Pondra answer point lookups and small dashboard queries,
and how many per second, while new rows keep arriving?
  serve_bench.py [--keys 2000000] [--nodes 1] [--secs 5] [--threads 1,8,32]
  serve_bench.py --users 50,100,200,400 [--events 5000000] [--secs 10]   # dashboards while writes land
  serve_bench.py --tpch ~/tpch/sf1 [--queries q6] [--nodes 3] [--writes] [--cache] [--door http|pg] [--steps 50,100,200,400] [--alone 35] [--out FILE]
      # TPC-H SF1 as Parquet: answers checked against DuckDB's, then fresh statements under load (tpch_params.py)
Prints one JSON line per measurement."""
import argparse, datetime, decimal, io, json, math, os, random, shutil, statistics, subprocess, sys, tempfile, threading, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import harness
import tpch_params
from harness import Node, call, sql

A = None
TPCH_TABLES = ["region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem"]
ARROW = {"content-type": "application/vnd.apache.arrow.stream"}
TPCH_KEYS = 10 ** 15  # loadgen's {k}: a number this big makes each request's statement new (the predicate's literal)
DOC = {"steps": []}  # what --out writes


def loadgen():
    """The Go load generator (tools/loadgen.go), built once if Go is installed; else None. Rebuilt when
    its source is newer than the build, so an old one never answers for a changed loadgen.go."""
    exe = os.path.join(tempfile.gettempdir(), "pondra-loadgen")
    src = os.path.join(os.path.dirname(os.path.abspath(__file__)), "loadgen.go")
    stale = os.path.exists(exe) and os.path.getmtime(exe) < os.path.getmtime(src)
    if (not os.path.exists(exe) or stale) and shutil.which("go"):
        subprocess.run(["go", "build", "-o", exe, src], check=True)
    return exe if os.path.exists(exe) else None


def measure(port, make_query, threads, secs, path="/sql"):
    """Run queries from `threads` clients for `secs`; return p50/p99/max ms and queries per second.
    `make_query` returns either SQL, or ("GET", path) for the /lookup fast path. With Go available
    the clients are tools/loadgen.go (the Python client tops out near 2k requests/s); then a
    `{k}` in the query becomes a random key."""
    exe = loadgen()
    if exe:
        q = make_query()
        args = ["-url", f"http://127.0.0.1:{port}{q[1]}"] if isinstance(q, tuple) else ["-url", f"http://127.0.0.1:{port}{path}", "-body", q]
        out = subprocess.run([exe, *args, "-keys", str(A.keys), "-c", str(threads), "-secs", str(secs)], capture_output=True, text=True, check=True)
        r = json.loads(out.stdout)
        return {"threads": threads, "queries": r["requests"], "qps": r["qps"], "errors": r["errors"], "p50_ms": round(r["p50_ms"], 2), "p99_ms": round(r["p99_ms"], 2), "max_ms": round(r["max_ms"], 1)}
    stop, lat, errs = threading.Event(), [], [0]

    def run():
        mine = []
        while not stop.is_set():
            q = make_query()
            t = time.time()
            try:
                call(port, "GET", q[1]) if isinstance(q, tuple) else call(port, "POST", path, q.encode())
            except Exception:
                errs[0] += 1
            mine.append((time.time() - t) * 1000)
        lat.extend(mine)

    ts = [threading.Thread(target=run, daemon=True) for _ in range(threads)]
    [t.start() for t in ts]
    time.sleep(secs)
    stop.set()
    [t.join(10) for t in ts]
    return {"threads": threads, "queries": len(lat), "qps": round(len(lat) / secs), "errors": errs[0],
            "p50_ms": round(statistics.median(lat), 1), "p99_ms": round(sorted(lat)[int(len(lat) * .99)], 1), "max_ms": round(max(lat), 1)}


# A dashboard's statements, one a line ({n}: 0-999, {k}: a key): a window of time by country, the
# top users of a window, a key's row, everything's total (the same text each time), a user's own.
MIX = """SELECT country, count(*) AS n, sum(amount) AS total FROM events WHERE ts >= {n} * 5000 AND ts < {n} * 5000 + 5000 GROUP BY country ORDER BY n DESC
SELECT user_id, sum(amount) AS s FROM events WHERE ts >= {n} * 5000 AND ts < {n} * 5000 + 50000 GROUP BY user_id ORDER BY s DESC LIMIT 10
SELECT id, name, amount FROM kv WHERE id = {k}
SELECT count(*) AS n, sum(amount) AS total FROM events
SELECT count(*) AS n, max(amount) AS top FROM events WHERE user_id = {k} AND ts >= {n} * 5000"""


def users():
    """Hundreds of clients on dashboards (`MIX`) while writes land: an append table taking 1,000
    rows every 50 ms and a keyed one taking upserts. Each step's clients, queries a second, p50 and
    p99 (each statement's too), and what the writers saw meanwhile."""
    exe = loadgen()
    assert exe, "needs Go (tools/loadgen.go)"
    lake = harness.new_lake()
    nodes = [Node(lake, A.port + i, reader=(i > 0)).start() for i in range(A.nodes)]
    p = nodes[0].port
    q = lambda s: call(p, "POST", "/sql", s.encode(), timeout=3600)
    t = time.time()
    q("CREATE TABLE events (ts BIGINT, user_id BIGINT, country VARCHAR, amount DOUBLE)")
    step = 1_000_000
    for lo in range(0, A.events, step):  # in time order, as events come: files narrow on ts
        q(f"INSERT INTO events SELECT value, (value * 7919) % {A.keys}, 'c' || (value % 40), (value % 997) * 0.25 FROM generate_series({lo}, {min(lo + step, A.events) - 1})")
    q("CREATE TABLE kv (id BIGINT PRIMARY KEY, name VARCHAR, amount BIGINT)")
    q(f"INSERT INTO kv SELECT value, 'user' || value, value % 1000 FROM generate_series(0, {A.keys - 1})")
    call(p, "POST", "/tier", timeout=3600)
    out = {"events": A.events, "keys": A.keys, "nodes": A.nodes, "load_s": round(time.time() - t, 1)}
    print(json.dumps(out), flush=True)
    mix = os.path.join(tempfile.mkdtemp(prefix="pondra-mix-"), "mix.sql")
    open(mix, "w").write(MIX)

    stop, acks, rows = threading.Event(), [], [0]

    def writer(table, make, every):
        seq = 0
        while not stop.is_set():
            seq += 1
            body = "".join(json.dumps(make()) + "\n" for _ in range(1000 if table == "events" else 100))
            t = time.time()
            try:
                call(p, "POST", f"/append/{table}?producer=w-{table}&seq={seq}", body.encode())
                acks.append((time.time() - t) * 1000)
                rows[0] += 1000 if table == "events" else 100
            except Exception:
                pass
            time.sleep(max(0, every - (time.time() - t)))

    clock = [A.events]

    def event():
        clock[0] += 1
        return {"ts": clock[0], "user_id": random.randrange(A.keys), "country": f"c{random.randrange(40)}", "amount": random.random() * 250}

    ws = [threading.Thread(target=writer, args=("events", event, 0.05), daemon=True),
          threading.Thread(target=writer, args=("kv", lambda: {"id": random.randrange(A.keys), "name": "x", "amount": random.randrange(1000)}, 0.05), daemon=True)]
    [w.start() for w in ws]
    time.sleep(2)
    for clients in [int(x) for x in A.users.split(",")]:
        acks.clear()
        before = rows[0]
        r = subprocess.run([exe, "-url", f"http://127.0.0.1:{nodes[A.at].port}/sql", "-bodies", mix, "-keys", str(A.keys), "-c", str(clients), "-secs", str(A.secs)],
                           capture_output=True, text=True, check=True)
        got = json.loads(r.stdout)
        a = sorted(acks) or [0]
        print(json.dumps({**out, "clients": clients, "qps": got["qps"], "errors": got["errors"], "p50_ms": round(got["p50_ms"], 1), "p99_ms": round(got["p99_ms"], 1),
                          "by": [{"p50_ms": round(b["p50_ms"], 1), "p99_ms": round(b["p99_ms"], 1)} for b in got["by"]],
                          "writes_rows_s": round((rows[0] - before) / A.secs), "write_ack_p50_ms": round(a[len(a) // 2], 1), "write_ack_p99_ms": round(a[int(len(a) * .99)], 1)}), flush=True)
    stop.set()
    [nd.kill() for nd in nodes]


def main():
    lake = harness.new_lake()
    nodes = [Node(lake, A.port + i, reader=(i > 0)).start() for i in range(A.nodes)]
    p = nodes[0].port
    call(p, "POST", "/tables/kv", json.dumps({"columns": [["id", "Int64"], ["name", "Utf8"], ["amount", "Int64"], ["ts", "Int64"]], "key": ["id"]}).encode())
    import pyarrow as pa, pyarrow.ipc
    t, batch = time.time(), 250_000
    for seq, lo in enumerate(range(0, A.keys, batch), 1):  # keyed table: rows go through the log
        ids = list(range(lo, min(lo + batch, A.keys)))
        tbl = pa.table({"id": pa.array(ids, pa.int64()), "name": pa.array([f"user{i}" for i in ids]),
                        "amount": pa.array([i % 1000 for i in ids], pa.int64()),
                        "ts": pa.array([1760000000000 + i for i in ids], pa.int64())})
        buf = io.BytesIO()
        with pa.ipc.new_stream(buf, tbl.schema) as w:
            w.write_table(tbl)
        call(p, "POST", f"/append/kv?producer=load&seq={seq}", buf.getvalue(),
             headers={"content-type": "application/vnd.apache.arrow.stream"}, timeout=3600)
    call(p, "POST", "/tier", timeout=3600)
    load_s = round(time.time() - t, 1)
    files = call(p, "GET", "/stats")
    out = {"keys": A.keys, "nodes": A.nodes, "load_s": load_s}
    print(json.dumps({**out, "stats": {k: files[k] for k in ("role", "hwm") if k in files}}), flush=True)

    go = loadgen() is not None  # ({k}: a random key per request, drawn by the Go client)
    key = lambda: "{k}" if go else str(random.randrange(A.keys))
    point = lambda: f"SELECT id, name, amount FROM kv WHERE id = {key()}"
    fast = lambda: ("GET", f"/lookup/kv/{key()}")
    dash = lambda: "SELECT count(*) AS n, sum(amount) AS total FROM kv WHERE amount > 900"
    fresh_dash = lambda: f"SELECT count(*) AS n, sum(amount) AS total FROM kv WHERE amount > ({key()} % 1000)"  # a new query each time
    for threads in [int(x) for x in A.threads.split(",")]:
        for name, q in (("point_lookup_sql", point), ("point_lookup", fast)):
            r = measure(nodes[-1].port, q, threads, A.secs)
            print(json.dumps({**out, "query": name, **r}), flush=True)
    for name, q, threads in (("dashboard_agg_uncached", fresh_dash, 1), ("dashboard_agg_uncached", fresh_dash, 8), ("dashboard_agg_repeated", dash, 32)):
        r = measure(nodes[-1].port, q, threads, A.secs)
        print(json.dumps({**out, "query": name, **r}), flush=True)

    # …and the same lookups while writes keep landing (the log tail is never empty)
    stop = threading.Event()

    def writer():
        seq = 0
        while not stop.is_set():
            seq += 1
            rows = "".join(json.dumps({"id": random.randrange(A.keys), "name": "x", "amount": 1, "ts": 0}) + "\n" for _ in range(200))
            try:
                call(p, "POST", f"/append/kv?producer=w&seq={seq}", rows.encode())
            except Exception:
                pass

    w = threading.Thread(target=writer, daemon=True)
    w.start()
    time.sleep(2)
    r = measure(nodes[-1].port, fast, 8, A.secs)
    print(json.dumps({**out, "query": "point_lookup_while_writing", **r}), flush=True)
    r = measure(nodes[-1].port, dash, 32, A.secs)
    print(json.dumps({**out, "query": "dashboard_agg_repeated_while_writing", **r}), flush=True)
    r = measure(nodes[-1].port, dash, 32, A.secs, path="/sql?stale_ms=1000")
    print(json.dumps({**out, "query": "dashboard_agg_repeated_while_writing_stale_1s", **r}), flush=True)
    stop.set()
    [nd.kill() for nd in nodes]


def emit(rec, step=False):
    """Prints one JSON line and keeps it for --out (a step's line in the list of steps)."""
    print(json.dumps(rec), flush=True)
    if step:
        DOC["steps"].append(rec)
    else:
        DOC.update(rec)


def quantile(xs, p):
    """The p-quantile of xs, a plain pick as the other numbers here are, to 0.01 ms (None if empty)."""
    xs = sorted(xs)
    return round(xs[min(len(xs) - 1, int(len(xs) * p))], 2) if xs else None


def arrow_bytes(part):
    """A RecordBatch or a Table as one Arrow IPC stream: the body /append takes."""
    import pyarrow as pa, pyarrow.ipc
    buf = io.BytesIO()
    with pa.ipc.new_stream(buf, part.schema) as w:
        w.write(part)
    return buf.getvalue()


def count(port, text="SELECT count(*) AS n FROM lineitem"):
    """A count from a node (one row, one column)."""
    return call(port, "POST", "/sql", text.encode(), timeout=600)[0]["n"]


def tpch_load(port, data):
    """The eight TPC-H tables into the node, loaded as tools/bench/tpch.py loads them: each Parquet
    file's columns typed as the node takes them, its batches sent to /append as Arrow IPC by this
    client (the node reads no file for it), then tiered. Each table is its own producer, so a retried
    batch is applied once."""
    import pyarrow.parquet as pq
    for t in TPCH_TABLES:
        table = pq.read_table(os.path.join(data, f"{t}.parquet"))
        cols = [[f.name, str(f.type).replace("decimal128", "Decimal128").replace("date32[day]", "Date32").replace("int64", "Int64")
                 .replace("int32", "Int32").replace("string", "Utf8")] for f in table.schema]
        call(port, "POST", f"/tables/{t}", json.dumps(cols).encode())
        for seq, batch in enumerate(table.to_batches(max_chunksize=500_000), 1):
            call(port, "POST", f"/append/{t}?producer=load-{t}&seq={seq}", arrow_bytes(batch), headers=ARROW, timeout=3600)
    call(port, "POST", "/tier", timeout=3600)


def settle(port, want, secs=120):
    """Waits until a node reads the rows the leader has: a follower sees each commit as its stream
    brings it, a moment after the leader does."""
    end, got = time.time() + secs, None
    while time.time() < end:
        try:
            got = count(port)
            if got == want:
                return
        except Exception:
            pass
        time.sleep(0.1)
    raise RuntimeError(f"node on port {port} reads {got} lineitem rows, not {want}")


def pondra_answer(port, text):
    """A statement's answer from a node: its column names and its rows, in the order it sent them."""
    rows = call(port, "POST", "/sql", text.encode(), timeout=600)
    return (list(rows[0].keys()) if rows else []), [list(r.values()) for r in rows]


def duck_answer(con, text):
    """The same statement's answer from DuckDB: its column names and rows."""
    res = con.execute(text)
    return [d[0] for d in res.description], res.fetchall()


def cell(v):
    """A value as the two engines' answers compare it: numbers as floats (a decimal the node sends as
    text is a number too), dates as their text, everything else as it is."""
    if v is None or isinstance(v, bool):
        return v
    if isinstance(v, datetime.date):
        return v.isoformat()
    if isinstance(v, (int, float, decimal.Decimal)):
        return float(v)
    if isinstance(v, str):
        try:
            return float(v)
        except ValueError:
            return v
    return v


def close(a, b):
    """Floats agree to 1e-9 relative (a sum or an average may round differently in each engine), or
    within 1e-6: the node's avg of a DECIMAL column keeps six places (its scale plus four, cut there),
    where DuckDB's is a double. The rest exactly."""
    if isinstance(a, float) and isinstance(b, float):
        return math.isclose(a, b, rel_tol=1e-9, abs_tol=1e-6)
    return a == b


def same(want, got):
    """Two answers agree when they have the same columns and the same rows in the same order (the
    ORDER BY's); a node's error or no answer (got None) agrees with nothing."""
    if got is None:
        return False
    (wn, wr), (gn, gr) = want, got
    if not wr and not gr:
        return True
    if [n.lower() for n in wn] != [n.lower() for n in gn] or len(wr) != len(gr):
        return False
    return all(len(x) == len(y) and all(close(cell(u), cell(v)) for u, v in zip(x, y)) for x, y in zip(wr, gr))


def tpch_check(ports, data, n):
    """n generated statements of each query, run on every node and on DuckDB over the same Parquet
    files, before any load: a statement counts only when every node's answer equals DuckDB's. Returns
    the statements some node got wrong (query, port, text)."""
    import duckdb
    con = duckdb.connect()
    for t in TPCH_TABLES:
        con.execute(f"CREATE VIEW {t} AS SELECT * FROM read_parquet('{os.path.join(data, t)}.parquet')")
    rng = random.Random(A.seed + 1)  # (its own draws: the load's statements stay the same whatever is checked)
    right, wrong = {}, []
    for q in tpch_params.QUERIES:
        right[q] = 0
        for _ in range(n):
            text = tpch_params.statement(q, rng, unique=not A.cache, k=rng.randrange(10 ** 12))
            want = duck_answer(con, text)
            bad = []
            for p in ports:
                try:
                    got = pondra_answer(p, text)
                except RuntimeError:
                    got = None  # (an error is a difference too)
                if not same(want, got):
                    bad.append(p)
            right[q] += not bad
            wrong += [(q, p, text) for p in bad]
    emit({"answers": {q: f"{right[q]} of {n}" for q in right}, "statements_each": n, "nodes": ports, "differ": len(wrong)})
    return wrong


def tpch_alone(port, data, n):
    """Each query alone: n statements of it, one at a time, on the node and on DuckDB with the tables in its
    memory (4 threads, as the node's cores), after a warm-up of five; the median of each. What one answer
    costs sets what a node can serve: a query that takes every core for 28 ms allows about 35 a second."""
    import duckdb
    con = duckdb.connect()
    con.execute(f"SET threads = {os.cpu_count()}")
    for t in TPCH_TABLES:
        con.execute(f"CREATE TABLE {t} AS SELECT * FROM read_parquet('{os.path.join(data, t)}.parquet')")
    rng, out = random.Random(A.seed + 2), {}
    for q in tpch_params.QUERIES:
        texts = [tpch_params.statement(q, rng, unique=not A.cache, k=rng.randrange(10 ** 12)) for _ in range(n + 5)]
        ours, theirs = [], []
        for i, text in enumerate(texts):
            t = time.perf_counter()
            pondra_answer(port, text)
            mid = time.perf_counter()
            con.execute(text).fetchall()
            if i >= 5:
                ours.append((mid - t) * 1000)
                theirs.append((time.perf_counter() - mid) * 1000)
        out[q] = {"pondra_ms": round(statistics.median(ours), 1), "duckdb_ms": round(statistics.median(theirs), 1)}
    emit({"alone": out, "statements_each": n, "duckdb": duckdb.__version__})


def write_bodies(path, queries, n, unique):
    """A file for loadgen, one statement a line and labelled by its query: `n` of each, with fresh
    parameters drawn from a seed, so a run's statements are the same each time it is run."""
    rng = random.Random(A.seed)
    with open(path, "w") as f:
        for q in queries:
            for _ in range(n):
                f.write(f"{q}\t{tpch_params.statement(q, rng, unique=unique)}\n")


class Writes:
    """A writer appending 1,000 lineitem rows every 50 ms through the leader, each batch with an
    l_orderkey of its own, above every key the table has. Each batch gets a prober that polls its
    count on the reader node (a follower when there are three nodes, else the leader) until all of its
    rows are there: freshness is from the append's send to the poll that first sees them. `step` tags
    the batches sent during a load step ("quiet" before the first, None between steps), so each step
    reports its own, and the quiet ones say how fresh a read is with no load beside it."""

    def __init__(self, ports, data):
        import pyarrow as pa, pyarrow.compute as pc, pyarrow.parquet as pq
        self.leader = ports[0]
        self.reader = ports[1] if len(ports) > 1 else ports[0]
        path = os.path.join(data, "lineitem.parquet")
        self.template = pa.Table.from_batches([next(pq.ParquetFile(path).iter_batches(batch_size=1000))])  # 1,000 rows, reused
        self.base = int(pc.max(pq.read_table(path, columns=["l_orderkey"]).column(0)).as_py())  # keys are added above it
        self.step, self.batches, self.stop = "quiet", [], threading.Event()
        self.thread = threading.Thread(target=self.run, daemon=True)

    def batch(self, key):
        """One batch's Arrow IPC body: the template's rows with l_orderkey = key."""
        import pyarrow as pa
        field = self.template.schema.field("l_orderkey")
        t = self.template.set_column(self.template.schema.get_field_index("l_orderkey"), field, pa.array([key] * self.template.num_rows, field.type))
        return arrow_bytes(t)

    def run(self):
        seq = 0
        while not self.stop.is_set():
            seq += 1
            key = self.base + seq
            body = self.batch(key)
            rec = {"step": self.step, "key": key, "sent": time.monotonic()}
            self.batches.append(rec)
            threading.Thread(target=self.probe, args=(rec,), daemon=True).start()
            try:
                call(self.leader, "POST", f"/append/lineitem?producer=fresh&seq={seq}", body, headers=ARROW, timeout=120)
                rec["acked"] = time.monotonic()
            except Exception as e:
                rec["error"] = str(e)[:200]
            time.sleep(max(0.0, 0.05 - (time.monotonic() - rec["sent"])))  # one batch every 50 ms

    def probe(self, rec):
        """Polls the batch's rows on the reader until all of them are there (a minute at most), a poll
        every 20 ms, so the probes don't load the node they measure. The time kept is when the poll
        that saw the rows came back, so it includes that poll's own wait on a busy node (`poll_ms`
        says how long such a poll took)."""
        q = f"SELECT count(*) AS n FROM lineitem WHERE l_orderkey = {rec['key']}"
        while "error" not in rec and time.monotonic() - rec["sent"] < 60:
            began = time.monotonic()
            try:
                n = call(self.reader, "POST", "/sql", q.encode(), timeout=60)[0]["n"]
            except Exception:
                n = None
            if n == self.template.num_rows:
                rec["seen"], rec["poll_ms"] = time.monotonic(), (time.monotonic() - began) * 1000
                return
            time.sleep(0.02)
        if "error" not in rec:
            rec["missing"] = True

    def settle(self, step, secs=40):
        """Waits (40 s at most) until every batch sent in `step` is seen, missing or failed."""
        end = time.monotonic() + secs
        while time.monotonic() < end and not all(done(b) for b in self.batches if b["step"] == step):
            time.sleep(0.05)

    def report(self, step, secs):
        """One step's write acks (as users() reports them) and freshness, for the batches sent in it."""
        mine = [b for b in self.batches if b["step"] == step]
        acks = [(b["acked"] - b["sent"]) * 1000 for b in mine if "acked" in b]
        seen = [(b["seen"] - b["sent"]) * 1000 for b in mine if "seen" in b]
        polls = [b["poll_ms"] for b in mine if "poll_ms" in b]
        return {"writes": {"batches": len(mine), "rows_s": round(len(acks) * self.template.num_rows / secs), "errors": sum("error" in b for b in mine),
                           "ack_p50_ms": quantile(acks, .5), "ack_p99_ms": quantile(acks, .99)},
                "freshness": {"seen": len(seen), "missing": sum("missing" in b for b in mine), "pending": sum(not done(b) for b in mine),
                              "p50_ms": quantile(seen, .5), "p99_ms": quantile(seen, .99), "max_ms": quantile(seen, 1), "poll_p50_ms": quantile(polls, .5)}}

    def start(self):
        self.thread.start()

    def close(self):
        self.stop.set()
        self.thread.join(10)


def done(b):
    """A batch whose freshness is settled: seen, missing (not in a minute) or failed to append."""
    return any(k in b for k in ("seen", "missing", "error"))


def tpch_steps(ports, bodies, writes, queries):
    """The load steps: `clients` clients for --secs, spread round-robin over the nodes by loadgen, each
    statement drawn from `bodies`. Each step prints its QPS, errors, p50 and p99, each query's own (by
    label), and with --writes the freshness and write acks of the batches sent in it."""
    if A.door == "pg":  # (the Postgres port: tools/pgload.py, a connection a client)
        dsns = ",".join(f"postgresql://u@127.0.0.1:{pg_port(p)}/lake" for p in ports)
        load = [sys.executable, os.path.join(os.path.dirname(os.path.abspath(__file__)), "pgload.py"), "--dsns", dsns, "--bodies", bodies, "--keys", str(TPCH_KEYS)]
        load_args = lambda clients: load + ["-c", str(clients), "--secs", str(A.secs)]
    else:
        exe = loadgen()
        assert exe, "needs Go (tools/loadgen.go)"
        urls = ",".join(f"http://127.0.0.1:{p}/sql" for p in ports)
        load_args = lambda clients: [exe, "-urls", urls, "-bodies", bodies, "-keys", str(TPCH_KEYS), "-c", str(clients), "-secs", str(A.secs)]
    cache = "on: as it is, parameters repeat" if A.cache else "off: every request's statement is new"
    for i, clients in enumerate(int(x) for x in A.steps.split(",")):
        if writes:
            writes.step = i
        r = subprocess.run(load_args(clients), capture_output=True, text=True, check=True)
        got = json.loads(r.stdout)
        if writes:
            writes.step = None
            writes.settle(i)
        by = [{k: round(v, 2) if k.endswith("_ms") else v for k, v in b.items()} for b in got["by"]]
        rec = {"nodes": len(ports), "door": A.door, "queries": queries, "result_cache": cache, "clients": clients, "secs": round(got["secs"], 1), "qps": got["qps"], "errors": got["errors"],
               "p50_ms": round(got["p50_ms"], 2), "p99_ms": round(got["p99_ms"], 2), "by": by, "per_url": got.get("per_url")}
        if writes:
            rec.update(writes.report(i, got["secs"]))  # (the step lasts until its last answer: so do its writes)
        emit(rec, step=True)


def pg_port(port):
    """A node's Postgres port (--door pg): its HTTP port's, 100 up."""
    return port + 100


def machine():
    """What the numbers ran on: the cores, the memory and the binary's version."""
    mem = os.sysconf("SC_PHYS_PAGES") * os.sysconf("SC_PAGE_SIZE") / 2 ** 30
    version = subprocess.run([harness.BIN, "--version"], capture_output=True, text=True).stdout.strip()
    return {"cores": os.cpu_count(), "memory_gb": round(mem, 1), "binary": version}


def tpch(data):
    """TPC-H SF1 (Parquet in `data`) on one node or three: load, then every answer of each query checked
    against DuckDB's on every node (any difference fails the run), then the load steps. The statements
    are new per request unless --cache (the result cache answers a repeated text again; see
    tpch_params.py). The writer (--writes) appends lineitem rows during the steps."""
    import pyarrow.parquet as pq
    queries = A.queries.split(",")
    assert all(q in tpch_params.QUERIES for q in queries), f"--queries: any of {','.join(tpch_params.QUERIES)}"
    DOC.update(machine())
    lake = harness.new_lake()
    pg = lambda p: {"pg": f"127.0.0.1:{pg_port(p)}"} if A.door == "pg" else {}
    nodes = [Node(lake, A.port + i, reader=(i > 0), **pg(A.port + i)).start() for i in range(A.nodes)]
    ports = [nd.port for nd in nodes]
    work = tempfile.mkdtemp(prefix="pondra-tpch-")
    try:
        t = time.time()
        tpch_load(ports[0], data)
        want = count(ports[0])
        rows = {tb: pq.read_metadata(os.path.join(data, f"{tb}.parquet")).num_rows for tb in TPCH_TABLES}
        emit({"data": {"folder": data, "rows": rows, "load_s": round(time.time() - t, 1)}})
        for p in ports[1:]:
            settle(p, want)
        for p in ports:
            harness.hot_settled(p)
        wrong = tpch_check(ports, data, A.check)
        if wrong:
            q, p, text = wrong[0]
            raise SystemExit(f"{len(wrong)} statements differ from DuckDB, the first: {q} on port {p}: {text}")
        if A.alone:
            tpch_alone(ports[0], data, A.alone)
        bodies = os.path.join(work, "mix.sql")
        write_bodies(bodies, queries, A.statements, unique=not A.cache)
        writes = Writes(ports, data) if A.writes else None
        if writes:
            writes.start()
            time.sleep(3)  # (rows landing with no load: the freshness the steps are set beside)
            writes.step = None
            writes.settle("quiet")
            emit({"quiet": writes.report("quiet", 3)})
        tpch_steps(ports, bodies, writes, queries)
        if writes:
            writes.close()
    finally:
        for nd in nodes:
            nd.kill()
        shutil.rmtree(work, ignore_errors=True)
        if A.out:
            with open(A.out, "w") as f:
                json.dump(DOC, f, indent=1)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--keys", type=int, default=2_000_000)
    ap.add_argument("--nodes", type=int, default=1)
    ap.add_argument("--secs", type=float, default=None, help="seconds per step (5; 10 with --tpch)")
    ap.add_argument("--tpch", default="", metavar="DIR", help="TPC-H SF1 as Parquet in DIR: the TPC-H mode instead")
    ap.add_argument("--queries", default=",".join(tpch_params.QUERIES), help="with --tpch: the queries, e.g. q6")
    ap.add_argument("--steps", default="50,100,200,400", help="with --tpch: clients per step")
    ap.add_argument("--writes", action="store_true", help="with --tpch: append lineitem rows during the load and measure freshness")
    ap.add_argument("--cache", action="store_true", help="with --tpch: keep the result cache as it is (statements repeat)")
    ap.add_argument("--check", type=int, default=20, help="with --tpch: statements per query checked against DuckDB")
    ap.add_argument("--statements", type=int, default=1000, help="with --tpch: statements per query in the load's file")
    ap.add_argument("--alone", type=int, default=0, help="with --tpch: time N statements of each query alone, on the node and on DuckDB")
    ap.add_argument("--door", default="http", choices=["http", "pg"], help="with --tpch: the load's clients over HTTP (tools/loadgen.go) or the Postgres port (tools/pgload.py, asyncpg)")
    ap.add_argument("--seed", type=int, default=1, help="with --tpch: the parameters' random seed")
    ap.add_argument("--out", default="", help="with --tpch: write everything as one JSON document")
    ap.add_argument("--threads", default="1,8,32,64")
    ap.add_argument("--port", type=int, default=18300)
    ap.add_argument("--s3", action="store_true")
    ap.add_argument("--users", default="", help="dashboard clients per step, e.g. 50,100,200,400: users() instead")
    ap.add_argument("--events", type=int, default=5_000_000)
    ap.add_argument("--at", type=int, default=-1, help="the node users() sends its queries to (0: the leader, which takes the writes)")
    A = harness.A = ap.parse_args()
    A.secs = A.secs or (10 if A.tpch else 5)
    if A.tpch:
        tpch(A.tpch)
    else:
        users() if A.users else main()
