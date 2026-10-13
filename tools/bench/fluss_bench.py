#!/usr/bin/env python3
"""Fluss and Pondra side by side, each through its own Python client, on one machine.

Apache Fluss (1.0) is a streaming store for Flink: each table a log of Arrow batches in buckets,
primary-key tables kept in RocksDB for lookups, and tiering to a lake. Pondra keeps a log too, and
the same binary answers SQL over it. Each runs as its quick start runs it: Fluss's
`local-cluster.sh` (ZooKeeper, a coordinator and a tablet server: three JVMs), one `pondra serve`.
Both are measured on what both do:

- start_s: from launching the servers to a row written and read back;
- processes, idle_mb: the servers' processes and memory once started, idle;
- append: N bids (Nexmark's seven columns, from nexmark.py) sent as Arrow batches of 10,000 until
  every one is acknowledged (Fluss: one writer over four buckets, acks=all; Pondra: four
  producers at once, each exactly once);
- ack_ms: one row at a time, written and acknowledged (p50, p99; Fluss's writer told not to wait
  for more rows, its batch timeout 0, as Kafka's linger.ms=0);
- fresh_ms: a row every 20 ms, from its write to a reader following the table (Fluss's log
  scanner, its fetches waiting 5 ms at most: at its default of 500 ms a fetch answered only when
  its wait ended, about 340 ms late; Pondra's watch);
- scan: two of the seven columns of every bid, read back as Arrow;
- group: bids per channel and the highest price (Pondra: SQL on the server; Fluss: its client
  reads the two columns and pyarrow groups them, since SQL over Fluss needs Flink);
- upsert: M people written, then each again with another city (Fluss: a primary-key table, its
  Python client's upsert a row at a time; Pondra: a keyed table, Arrow batches);
- lookup_ms: random keys one at a time, each answer checked (p50, p99). Fluss's client waits 100 ms
  for a batch of lookups to fill, which Java's client can lower and Python's can't (fluss-rust's
  lookup_batch_timeout_ms), and one lookuper does one lookup at a time;
- lookups_per_s: the same keys with 64 lookups in flight (Fluss: 64 lookupers, whose lookups the
  client batches together; Pondra: 64 connections), each answer checked.
- memory_mb: the servers' memory at the end, and memory_settled_mb 15 s later, once each has given
  back what it had freed (Pondra's allocator keeps freed pages 10 s for the next query).

  fluss_bench.py --fluss <fluss-1.0.0 folder> [--rows 10000000] [--people 1000000] [--engines pondra,fluss]
  (pip install pyfluss==1.0.0 duckdb pyarrow; Fluss needs Java 11 or newer)
"""
import argparse, asyncio, json, os, random, shutil, subprocess, sys, tempfile, threading, time
from concurrent.futures import ThreadPoolExecutor
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path[:0] = [os.path.join(HERE, ".."), os.path.join(HERE, "..", "..", "python")]
import pyarrow as pa, pyarrow.compute as pc
from nexmark import duck_rows

BATCH = 10_000
CITIES = ["Phoenix", "Boise", "Portland", "Seattle", "Kent"]


def pct(xs, p):
    return round(sorted(xs)[min(len(xs) - 1, int(len(xs) * p))] * 1000, 2) if xs else None


def spread(xs):
    return {"p50": pct(xs, 0.5), "p99": pct(xs, 0.99), "n": len(xs)}


def rss_mb(pids):
    """Resident memory of these processes and their children, in MB."""
    out = subprocess.run(["ps", "-e", "-o", "pid=,ppid=,rss="], capture_output=True, text=True).stdout.split()
    rows = [tuple(map(int, out[i:i + 3])) for i in range(0, len(out), 3)]
    keep, grew = set(pids), True
    while grew:
        more = {p for p, pp, _ in rows if pp in keep} - keep
        keep |= more
        grew = bool(more)
    return round(sum(r for p, _, r in rows if p in keep) / 1024)


def data(rows, people):
    """The bids and the people (twice: the second time each in another city), made before any clock."""
    bids = [b.cast(pa.schema([(f.name, pa.string() if pa.types.is_string(f.type) or pa.types.is_large_string(f.type) else f.type) for f in b.schema]))
            for b in duck_rows("bid", 0, rows).to_batches(BATCH)]
    first = pa.table({"id": pa.array(range(people), pa.int64()), "city": pa.array([CITIES[i % 5] for i in range(people)])})
    again = first.set_column(1, "city", pa.array([CITIES[(i + 1) % 5] for i in range(people)]))
    return bids, first, again


class Pondra:
    name = "pondra"

    def __init__(self, a):
        import harness, pondra
        harness.A = argparse.Namespace(s3=False, port=a.port)
        self.lake = tempfile.mkdtemp(prefix="pondra-fluss-")
        self.node = harness.Node(self.lake, a.port)
        self.connect = lambda: pondra.connect(f"http://127.0.0.1:{a.port}", echo=False)

    def start(self):
        self.node.start()
        self.db = self.connect()
        self.db.sql("CREATE TABLE warm (x BIGINT)")
        self.db.append("warm", [{"x": 1}])
        assert self.db.sql("SELECT count(*) AS n FROM warm").collect()["n"][0].as_py() == 1
        return [self.node.p.pid]

    def stop(self):
        self.node.kill()
        shutil.rmtree(self.lake, ignore_errors=True)

    def make(self, schema):
        cols = ", ".join(f"{f.name} {'BIGINT' if pa.types.is_integer(f.type) else 'TIMESTAMP' if pa.types.is_timestamp(f.type) else 'VARCHAR'}" for f in schema)
        self.db.sql(f"CREATE TABLE bid ({cols})")
        self.db.sql("CREATE TABLE people (id BIGINT PRIMARY KEY, city VARCHAR)")
        self.db.sql("CREATE TABLE ticks (i BIGINT, sent DOUBLE)")

    def append(self, bids, writers=4):
        def send(part):
            db = self.connect()
            for b in part:
                db.append("bid", pa.Table.from_batches([b]))
        threads = [threading.Thread(target=send, args=(bids[i::writers],)) for i in range(writers)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

    def ack(self, i):
        self.db.append("ticks", [{"i": i, "sent": time.time()}])

    def follow(self, n, seen, ready):
        """Each of the next `n` rows of `ticks` as it arrives: its delay since it was sent."""
        db = self.connect()
        rows = db.watch("ticks", after=None)
        ready.set()
        for row in rows:
            seen.append(time.time() - row["sent"])
            if len(seen) >= n:
                return

    def tick(self, i):
        self.ack(i)

    def scan(self):
        return self.db.sql("SELECT auction, price FROM bid").collect()

    def group(self):
        return self.db.sql("SELECT channel, count(*) AS n, max(price) AS top FROM bid GROUP BY channel ORDER BY channel").collect()

    def upsert(self, people):
        for b in people.to_batches(100_000):
            self.db.append("people", pa.Table.from_batches([b]))

    def lookup(self, key):
        row = self.db.lookup("people", key)
        return row and row["city"]

    def lookups(self, keys, inflight):
        def some(part):
            db = self.connect()
            return [(k, (db.lookup("people", k) or {}).get("city")) for k in part]
        with ThreadPoolExecutor(inflight) as pool:
            return dict(x for part in pool.map(some, [keys[i::inflight] for i in range(inflight)]) for x in part)


class Fluss:
    """Fluss's own quick start: `bin/local-cluster.sh start`, its data in a folder of this run's. Its client
    is asyncio's (each call made inside a running loop), so each step is a coroutine run to its end."""
    name = "fluss"

    def __init__(self, a):
        import fluss
        self.fl, self.home, self.dir = fluss, a.fluss, tempfile.mkdtemp(prefix="fluss-bench-")
        self.loop = asyncio.new_event_loop()

    def go(self, step):
        return self.loop.run_until_complete(step)

    def start(self):
        conf = os.path.join(self.dir, "conf")
        shutil.copytree(os.path.join(self.home, "conf"), conf)
        # (A tablet server refuses writes once its disk is 85% used, as `df` counts it. A machine whose writable
        # space is an allowance smaller than its disk, as a cloud sandbox's, looks full; the limit is lifted.)
        with open(os.path.join(conf, "server.yaml"), "a") as f:
            f.write(f"\ndata.dir: {self.dir}/data\nremote.data.dir: {self.dir}/remote\nserver.data-disk.write-limit-ratio: 1.0\n")
        with open(os.path.join(conf, "zookeeper.properties"), "a") as f:
            f.write(f"\ndataDir={self.dir}/zookeeper\n")
        os.makedirs(os.path.join(self.dir, "log"))
        self.env = {**os.environ, "FLUSS_CONF_DIR": conf, "FLUSS_LOG_DIR": os.path.join(self.dir, "log"), "FLUSS_PID_DIR": self.dir}
        subprocess.run([os.path.join(self.home, "bin", "local-cluster.sh"), "start"], env=self.env, check=True, capture_output=True)
        self.go(self.connect())
        return [int(p) for p in subprocess.run(["pgrep", "-f", self.dir], capture_output=True, text=True).stdout.split()]

    async def connect(self):
        fl, deadline = self.fl, time.time() + 120
        fast = fl.Config({"bootstrap.servers": "127.0.0.1:9123"})
        fast.writer_batch_timeout_ms = 0
        fast.scanner_log_fetch_wait_max_time_ms = 5
        kv = fl.Config({"bootstrap.servers": "127.0.0.1:9123"})
        # (Fluss 1.0's tablet server fails an upsert batch whose changelog passes 131,072 records, an
        # IndexOutOfBoundsException in ChangeTypeVectorWriter: an update is two, before and after, and
        # the client's 2 MB batches of rows this small hold more. 512 KB batches stay under it.)
        kv.writer_batch_size = 512 * 1024
        kv.writer_dynamic_batch_size_min = 128 * 1024
        while True:
            try:
                self.conn = await fl.FlussConnection.create(fl.Config({"bootstrap.servers": "127.0.0.1:9123"}))
                self.fast = await fl.FlussConnection.create(fast)
                self.kv = await fl.FlussConnection.create(kv)
                self.admin = self.conn.get_admin()
                await self.admin.create_database("bench", ignore_if_exists=True)
                warm = await self.table("warm", pa.schema([("x", pa.int64())]), buckets=1)
                await warm.new_append().create_writer().write_arrow_batch(pa.record_batch({"x": pa.array([1], pa.int64())})).wait()
                if (await self.read(warm)).num_rows == 1:
                    return
            except Exception:
                if time.time() > deadline:
                    raise
            await asyncio.sleep(0.1)

    def stop(self):
        subprocess.run([os.path.join(self.home, "bin", "local-cluster.sh"), "stop"], env=self.env, capture_output=True)
        for p in subprocess.run(["pgrep", "-f", self.dir], capture_output=True, text=True).stdout.split():
            subprocess.run(["kill", "-9", p])
        shutil.rmtree(self.dir, ignore_errors=True)

    async def table(self, name, schema, keys=None, buckets=4, conn=None):
        fl = self.fl
        path = fl.TablePath("bench", name)
        await self.admin.create_table(path, fl.TableDescriptor(fl.Schema(schema, primary_keys=keys), bucket_count=buckets), True)
        return await (conn or self.conn).get_table(path)

    async def read(self, table, columns=None):
        scan = table.new_scan()
        s = await (scan.project_by_name(columns) if columns else scan).create_record_batch_log_scanner()
        s.subscribe_buckets({b: self.fl.EARLIEST_OFFSET for b in range(table.get_table_info().num_buckets)})
        return await s.to_arrow()  # (up to the latest offsets)

    def make(self, schema):
        async def make():
            self.bid = await self.table("bid", schema)
            self.people = await self.table("people", pa.schema([("id", pa.int64()), ("city", pa.string())]), keys=["id"], conn=self.kv)
            self.ticks = await self.table("ticks", pa.schema([("i", pa.int64()), ("sent", pa.float64())]), buckets=1, conn=self.fast)
            self.ticker = self.ticks.new_append().create_writer()
            self.lookuper = self.people.new_lookup().create_lookuper()
        self.go(make())

    def append(self, bids):
        async def append():
            w = self.bid.new_append().create_writer()
            for b in bids:
                w.write_arrow_batch(b)
            await w.flush()
        self.go(append())

    def ack(self, i):
        async def ack():
            await self.ticker.append({"i": i, "sent": time.time()}).wait()
        self.go(ack())

    def follow(self, n, seen, ready):
        async def follow():
            s = await self.ticks.new_scan().create_record_batch_log_scanner()
            end = await self.admin.list_offsets(self.fl.TablePath("bench", "ticks"), [0], self.fl.OffsetSpec.latest())
            s.subscribe(0, end[0])
            ready.set()
            while len(seen) < n:
                t = await s.poll_arrow(100)
                now = time.time()
                seen.extend(now - x for x in t.column("sent").to_pylist())
        asyncio.new_event_loop().run_until_complete(follow())

    def tick(self, i):
        self.ack(i)

    def scan(self):
        return self.go(self.read(self.bid, ["auction", "price"]))

    def group(self):
        t = self.go(self.read(self.bid, ["channel", "price"])).group_by("channel").aggregate([("price", "count"), ("price", "max")])
        return pa.table({"channel": t["channel"], "n": t["price_count"], "top": t["price_max"]}).sort_by("channel")

    def upsert(self, people):
        # (A batch the server refuses fails every row's handle in it, and a batch holds thousands of rows:
        # a handle every 1,000 rows, awaited after the flush, sees any. Awaiting each costs Python 0.17 ms.)
        async def upsert():
            w, kept = self.people.new_upsert().create_writer(), []
            for i, row in enumerate(people.to_pylist()):
                h = w.upsert(row)
                if i % 1000 == 999 or i == people.num_rows - 1:
                    kept.append(h)
            await w.flush()
            for h in kept:
                await h.wait()
        self.go(upsert())

    def lookup(self, key):
        async def lookup():
            return await self.lookuper.lookup({"id": key})
        row = self.go(lookup())
        return row and row["city"]

    def lookups(self, keys, inflight):
        async def some(part):
            lookuper = self.people.new_lookup().create_lookuper()
            return [(k, ((await lookuper.lookup({"id": k})) or {}).get("city")) for k in part]
        async def every():
            return await asyncio.gather(*[some(keys[i::inflight]) for i in range(inflight)])
        return dict(x for part in self.go(every()) for x in part)


def bench(engine, a, bids, first, again):
    out = {"engine": engine.name}
    say = lambda what: print(f"{engine.name}: {what}", file=sys.stderr, flush=True)
    t0 = time.time()
    pids = engine.start()
    out["start_s"] = round(time.time() - t0, 2)
    time.sleep(5)
    out["processes"], out["idle_mb"] = len(pids), rss_mb(pids)
    say("started")
    engine.make(bids[0].schema)
    rows = sum(b.num_rows for b in bids)
    t = time.time()
    engine.append(bids)
    s = time.time() - t
    out["append"] = {"rows": rows, "secs": round(s, 2), "rows_per_s": round(rows / s)}
    say("appended")
    took = []
    for i in range(a.acks):
        t = time.time()
        engine.ack(i)
        took.append(time.time() - t)
    out["ack_ms"] = spread(took)
    say("acked")
    seen, ready = [], threading.Event()
    reader = threading.Thread(target=engine.follow, args=(a.ticks, seen, ready), daemon=True)
    reader.start()
    ready.wait(30)
    time.sleep(1)
    for i in range(a.ticks):
        engine.tick(a.acks + i)
        time.sleep(0.02)
    reader.join(30)
    out["fresh_ms"] = spread(seen)
    say("followed")
    t = time.time()
    got = engine.scan()
    s = time.time() - t
    out["scan"] = {"rows": got.num_rows, "secs": round(s, 2), "rows_per_s": round(got.num_rows / s), "right": got.num_rows == rows}
    t = time.time()
    got = engine.group()
    out["group"] = {"secs": round(time.time() - t, 2), "answer": [list(r.values()) for r in got.to_pylist()]}
    say("scanned and grouped")
    t = time.time()
    engine.upsert(first)
    engine.upsert(again)
    s = time.time() - t
    out["upsert"] = {"rows": 2 * first.num_rows, "secs": round(s, 2), "rows_per_s": round(2 * first.num_rows / s)}
    say("upserted")
    keys, took, wrong = random.Random(1).sample(range(first.num_rows), a.lookups), [], 0
    for k in keys:
        t = time.time()
        city = engine.lookup(k)
        took.append(time.time() - t)
        wrong += city != CITIES[(k + 1) % 5]
    out["lookup_ms"] = {**spread(took), "wrong": wrong}
    t = time.time()
    got = engine.lookups(keys, 64)
    s = time.time() - t
    out["lookups_per_s"] = {"in_flight": 64, "n": len(keys), "per_s": round(len(keys) / s), "wrong": sum(got.get(k) != CITIES[(k + 1) % 5] for k in keys)}
    out["memory_mb"] = rss_mb(pids)
    time.sleep(15)
    out["memory_settled_mb"] = rss_mb(pids)
    return out


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--fluss", default=os.path.expanduser("~/engines/fluss-1.0.0"))
    ap.add_argument("--rows", type=int, default=10_000_000)
    ap.add_argument("--people", type=int, default=1_000_000)
    ap.add_argument("--acks", type=int, default=1000)
    ap.add_argument("--ticks", type=int, default=500)
    ap.add_argument("--lookups", type=int, default=2000)
    ap.add_argument("--engines", default="pondra,fluss")
    ap.add_argument("--port", type=int, default=8295)
    a = ap.parse_args()
    bids, first, again = data(a.rows, a.people)
    for name in a.engines.split(","):
        engine = {"pondra": Pondra, "fluss": Fluss}[name](a)
        try:
            print(json.dumps(bench(engine, a, bids, first, again)), flush=True)
        finally:
            engine.stop()
