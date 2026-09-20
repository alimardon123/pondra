"""Pondra side of the benchmark (one node, local disk). Prints one JSON line.
  pondra_bench.py batch  N DIR    INSERT … SELECT N generated rows -> Parquet, then the shared queries
  pondra_bench.py stream N KEYS   keyed running aggregation as an inline view: N events pushed over HTTP
                              (Arrow), stored durably and aggregated; time until the state is complete
  pondra_bench.py etl    N        the same for a filter/projection view
  pondra_bench.py live   SECS KEYS  producers push continuously; sustained events/s and the time from an
                              ack until the aggregate shows it
  (TASK=1: use a streaming task instead of an inline view, as in round 3)"""
import atexit, io, json, os, random, sys, tempfile, threading, time
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import pyarrow as pa, pyarrow.ipc
import harness
from harness import Node, call, sql, pct
from queries import GEN, QUERIES

mode, n = sys.argv[1], int(sys.argv[2])
PORT = 18500
DF = {c: e.replace("AS STRING", "AS VARCHAR") for c, e in GEN.items()}  # DataFusion dialect


def start(lake, **flags):
    if os.environ.get("BACKLOG"):  # rows allowed to wait for tiering: the memory/burst trade-off
        flags["backlog"] = int(os.environ["BACKLOG"])
    t = time.time()
    node = Node(lake, PORT, **flags).start()
    atexit.register(node.kill)
    return node, round(time.time() - t, 3)


def ipc(table):
    buf = io.BytesIO()
    with pa.ipc.new_stream(buf, table.schema) as w:
        w.write_table(table)
    return buf.getvalue()


def ingest(ids, producers=4, batch=100_000):
    """Push ids as Arrow IPC batches from several producers; returns seconds."""
    chunks = [ids[i:i + batch] for i in range(0, len(ids), batch)]
    def push(k):
        for seq, c in enumerate(chunks[k::producers], 1):
            body = ipc(pa.table({"id": pa.array(c, pa.int64())}))
            call(PORT, "POST", f"/append/src?producer=p{k}&seq={seq}", body, headers={"content-type": "application/vnd.apache.arrow.stream"}, timeout=120)
    t = time.time()
    ts = [threading.Thread(target=push, args=(k,)) for k in range(producers)]
    [x.start() for x in ts]
    [x.join() for x in ts]
    return time.time() - t


def wait(cond, secs=600):
    t = time.time()
    while not cond():
        if time.time() - t > secs:
            raise RuntimeError("timeout")
        time.sleep(0.05)
    return time.time() - t


TASK_SQL = """SELECT e.user_id, coalesce(max(t.n), 0) + count(*) AS n, coalesce(max(t.s), 0.0) + sum(e.amount) AS s
  FROM (SELECT id % {keys} AS user_id, CAST(id % 10000 AS DOUBLE) / 100 AS amount FROM src) e
  LEFT JOIN totals t ON e.user_id = t.user_id GROUP BY e.user_id"""
VIEW_SQL = "SELECT id % {keys} AS user_id, count(*) AS n, sum(CAST(id % 10000 AS DOUBLE) / 100) AS s FROM src GROUP BY id % {keys}"


def setup_stream(p, keys):
    call(p, "POST", "/tables/src", json.dumps([["id", "Int64"]]).encode())
    if os.environ.get("TASK"):
        call(p, "POST", "/tables/totals", json.dumps({"columns": [["user_id", "Int64"], ["n", "Int64"], ["s", "Float64"]], "key": ["user_id"]}).encode())
        call(p, "POST", "/tasks/agg", json.dumps({"source": "src", "target": "totals", "key": ["user_id"], "sql": TASK_SQL.format(keys=keys)}).encode())
    elif not os.environ.get("NOVIEW"):
        call(p, "POST", "/views/totals", VIEW_SQL.format(keys=keys).encode())


lake = tempfile.mkdtemp(prefix="pondra-bench-")
out = {"engine": "pondra", "mode": mode, "rows": n}
if mode == "batch":
    node, out["startup_s"] = start(lake)
    cols = ", ".join(f"{e} AS {c}" for c, e in DF.items())
    t = time.time()
    call(PORT, "POST", "/insert/events?job=gen", f"SELECT {cols} FROM (SELECT value AS id FROM range(0, {n}))".encode(), timeout=3600)
    out["write_s"] = round(time.time() - t, 2)
    call(PORT, "POST", "/insert/dims?job=dims", b"SELECT concat('c', CAST(value AS VARCHAR)) AS category, concat('r', CAST(value % 10 AS VARCHAR)) AS region FROM range(0, 1000)")
    for name, q in QUERIES.items():
        q = q.replace("AS STRING", "AS VARCHAR")
        times = []
        for _ in range(2):
            t = time.time()
            sql(PORT, q)
            times.append(round(time.time() - t, 3))
        out[name] = times
elif mode in ("stream", "etl"):
    node, out["startup_s"] = start(lake)
    if mode == "stream":
        setup_stream(PORT, int(sys.argv[3]))
    else:
        call(PORT, "POST", "/tables/src", json.dumps([["id", "Int64"]]).encode())
        cols = ", ".join(f"{e} AS {c}" for c, e in DF.items())
        call(PORT, "POST", "/views/out", f"SELECT * FROM (SELECT {cols} FROM src) WHERE amount > 50".encode())
    t = time.time()
    out["ingest_s"] = round(ingest(list(range(n))), 2)
    if mode == "stream":
        wait(lambda: (sql(PORT, "SELECT sum(n) AS n FROM totals")[0].get("n") or 0) == n)
        want = sum((i % 10000) / 100 for i in range(n))
        out["correct"] = abs(sql(PORT, "SELECT sum(s) AS s FROM totals")[0]["s"] - want) < 1e-6 * want
    else:
        expect = sum(1 for i in range(n) if (i * 48271) % 10000 > 5000)
        wait(lambda: sql(PORT, "SELECT count(*) AS n FROM out")[0]["n"] == expect)
    secs = time.time() - t
    out.update(processed=n, secs=round(secs, 2), rows_per_s=round(n / secs))
elif mode == "live":
    secs, keys = n, int(sys.argv[3])
    node, out["startup_s"] = start(lake)
    setup_stream(PORT, keys)
    stop, sent, lat = threading.Event(), [0], []

    def push(k, batch=20_000):
        seq, base = 0, k * 10**12
        while not stop.is_set():
            seq += 1
            ids = pa.array(range(base + seq * batch, base + (seq + 1) * batch), pa.int64())
            call(PORT, "POST", f"/append/src?producer=p{k}&seq={seq}", ipc(pa.table({"id": ids})), headers={"content-type": "application/vnd.apache.arrow.stream"})
            sent[0] += batch  # acked (durable)

    def probe():  # everything acked by time t must show up in the task's state: time until it does
        while not stop.is_set():
            target, t = sent[0], time.time()
            wait(lambda: (sql(PORT, "SELECT sum(n) AS n FROM totals")[0].get("n") or 0) >= target)
            lat.append(time.time() - t)
            time.sleep(random.random() / 2)

    ts = [threading.Thread(target=push, args=(k,)) for k in range(int(os.environ.get("PRODUCERS", 3)))]
    ts += [] if os.environ.get("NOVIEW") or os.environ.get("NOPROBE") else [threading.Thread(target=probe)]
    [x.start() for x in ts]
    time.sleep(secs)
    stop.set()
    [x.join() for x in ts]
    out.update(events=sent[0], rows_per_s=round(sent[0] / secs), visible_ms_p50=pct(lat, .5), visible_ms_p99=pct(lat, .99))
node.kill()
print(json.dumps(out), flush=True)
