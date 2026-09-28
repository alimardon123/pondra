#!/usr/bin/env python3
"""Test harness for Pondra (the Python packages it uses: tools/requirements.txt). Every test runs on a fresh lake: a temp dir, or with
--s3 a fresh s3://$PONDRA_BUCKET/test-<id> prefix (AWS_* env vars point at R2/MinIO/the simulator).

  harness.py crash   --runs 20   kill -9 + injected crashes during commit, tiering and tasks
  harness.py upsert              random upserts/deletes + compactions vs an in-memory model
  harness.py fence               a second writer takes over; the first must stop, nothing lost
  harness.py reader              freshness as seen by a separate read-only node
  harness.py insert              bulk INSERT … SELECT, retried: applied exactly once
  harness.py sums                sum(DOUBLE) == math.fsum, in any order, on every node
  harness.py schemas             lake.schema.table, attached lakes, CREATE/DROP SCHEMA/TABLE/VIEW, CTAS
  harness.py changes             UPDATE/DELETE/MERGE on every table vs a model: row ids, views, the change feed, purges
  harness.py guard               a query spreads only when it pays (a slow link keeps it on one node)
  harness.py columns             RENAME/DROP COLUMN, a name added again, widened types, under streaming, vs a model
  harness.py fills               materialized views filled from the rows already there, every row once
  harness.py dedup               a keyed table deduplicated by event time (order_by) vs a model
  harness.py procedures          macros, procedures (SQL, Python), scripts, parameters: rights, depth, three nodes
  harness.py load    --secs 30   throughput, ack latency, freshness, catalog commit latency
  harness.py all                 quick run of everything
"""
import argparse, atexit, glob as glob_, http.client, itertools, json, os, random, shutil, signal, subprocess, sys, tempfile, threading, time, urllib.request, uuid

BIN = os.environ.get("PONDRA_BIN", os.path.join(os.path.dirname(os.path.abspath(__file__)), "../target/release/pondra"))
A = None  # parsed args


def call(port, method, path, body=b"", timeout=30, headers=None):
    c = http.client.HTTPConnection("127.0.0.1", port, timeout=timeout)
    c.request(method, path, body, headers or {})
    r = c.getresponse()
    data = r.read()
    if r.status != 200:
        raise RuntimeError(f"{r.status}: {data[:3000]!r}")
    return json.loads(data) if data[:1] in (b"{", b"[") else data


def sql(port, q):
    return call(port, "POST", "/sql", q.encode())


LAKES, NODES, S3 = [], [], []  # what this run made: removed when it exits, unless --keep or PONDRA_KEEP=1


def new_lake():
    prefix = os.environ.get("PONDRA_TEST_PREFIX", "")  # e.g. "round6/": test lakes grouped in one folder
    lake = f"s3://{os.environ['PONDRA_BUCKET']}/{prefix}test-{uuid.uuid4().hex[:8]}" if A.s3 else tempfile.mkdtemp(prefix="pondra-")
    if A.s3 and not S3:  # (made now: at exit, boto3 can no longer start the threads it needs)
        import boto3
        S3.append(boto3.client("s3", endpoint_url=os.environ.get("AWS_ENDPOINT"), region_name=os.environ.get("AWS_REGION", "auto")))
    LAKES.append(lake)
    return lake


@atexit.register
def clean_up():
    """Stop the nodes this run started and delete its lakes (buckets have size limits: R2's free tier is 10 GB)."""
    if getattr(A, "keep", False) or os.environ.get("PONDRA_KEEP") == "1":
        return
    for n in NODES:
        n.kill()
    for lake in LAKES:
        try:
            if not lake.startswith("s3://"):
                shutil.rmtree(lake, ignore_errors=True)
                continue
            bucket, prefix = lake[5:].split("/", 1)
            s3 = S3[0]
            for pg in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix + "/"):
                keys = [{"Key": o["Key"]} for o in pg.get("Contents", [])]
                if keys:
                    s3.delete_objects(Bucket=bucket, Delete={"Objects": keys, "Quiet": True})
        except Exception as e:
            print(f"(couldn't delete {lake}: {e})", file=sys.stderr)


class Node:
    def __init__(self, lake, port, reader=False, env=None, **flags):
        self.args = [BIN, "serve", "--dir", lake, "--addr", f"127.0.0.1:{port}"] + (["--reader"] if reader else [])
        self.args += [f"--{k.replace('_', '-')}" + ("" if v is True or v == "true" else f"={v}") for k, v in flags.items()]  # (a bare flag for true)
        self.port, self.env, self.p = port, {**os.environ, **(env or {})}, None
        self.log = os.path.join(tempfile.gettempdir(), f"pondra-{port}-{uuid.uuid4().hex[:6]}.stderr")

    def start(self, tries=20):
        try:
            call(self.port, "GET", "/stats", timeout=1)
            raise RuntimeError(f"port {self.port} is already in use by another node")
        except (ConnectionError, OSError):
            pass  # free, as it should be
        with open(self.log, "a") as err:
            self.p = subprocess.Popen(self.args, env=self.env, stdout=subprocess.DEVNULL, stderr=err)
        NODES.append(self)
        deadline = time.time() + 120  # opening a lake on slow object storage can take a while
        while time.time() < deadline:
            try:
                call(self.port, "GET", "/stats", timeout=2)
                return self
            except Exception:
                if self.p.poll() is not None and tries > 1:
                    return self.start(tries - 1)  # died while starting (e.g. injected crash): try again
                if self.p.poll() is not None:
                    break
                time.sleep(0.02)
        raise RuntimeError("node did not start: " + open(self.log).read()[-500:])

    def kill(self):
        if self.p and self.p.poll() is None:
            self.p.send_signal(signal.SIGKILL)
            self.p.wait()
            return True
        return False

    def alive(self):
        return self.p.poll() is None


def events_table(port):
    call(port, "POST", "/tables/events", json.dumps([["producer", "Utf8"], ["seq", "Int64"], ["i", "Int64"], ["ts", "Float64"]]).encode())


def rows(producer, seq, n):
    return "".join(f'{{"producer":"{producer}","seq":{seq},"i":{i},"ts":{time.time()}}}\n' for i in range(n)).encode()


def producer(port, name, batches, size, stop):
    """Sends batches in order, retrying each until acked (idempotent producer)."""
    seq = 1
    while seq <= batches and not stop.is_set():
        try:
            call(port, "POST", f"/append/events?producer={name}&seq={seq}", rows(name, seq, size), timeout=15)
            seq += 1
        except Exception:
            time.sleep(0.05)  # node down / crashed: retry the same seq


def pct(xs, p):
    return round(sorted(xs)[min(len(xs) - 1, int(len(xs) * p))] * 1000) if xs else None


# ---------------------------------------------------------------- tests

def crash():
    env = {"PONDRA_CRASH": "after_seg_put:0.03,after_commit:0.01,after_parquet_put:0.2"}  # commits are frequent: 1% each
    totals = {}
    for run in range(1, A.runs + 1):
        lake = new_lake()
        node = Node(lake, A.port, env=env, flush_ms=50, tier_secs=1, task_ms=200, retain_secs=0).start()
        def setup(path, body):  # (asked again after a restart: a view's fill commits, and a commit may be where the node crashes)
            for _ in range(20):
                try:
                    return call(A.port, "POST", path, body)
                except Exception:
                    if not node.alive():
                        node.start()
                    time.sleep(0.1)
            raise RuntimeError(f"{path}: the node kept crashing")
        setup("/tables/events", json.dumps([["producer", "Utf8"], ["seq", "Int64"], ["i", "Int64"], ["ts", "Float64"]]).encode())
        setup("/tasks/copy", json.dumps({"source": "events", "target": "events_copy", "sql": "SELECT producer, seq, i FROM events WHERE i % 2 = 0"}).encode())
        setup("/views/per_producer", b"SELECT producer, count(*) AS n, sum(i) AS s FROM events GROUP BY producer")
        setup("/views/thirds", b"SELECT producer, seq, i FROM events WHERE i % 3 = 0")
        stop, kills = threading.Event(), 0
        threads = [threading.Thread(target=producer, args=(A.port, f"p{k}", A.batches, A.size, stop)) for k in range(A.producers)]
        [t.start() for t in threads]
        while any(t.is_alive() for t in threads):  # chaos: random kill -9, restart after any death
            time.sleep(random.uniform(0.2, 1.5) * (5 if A.s3 else 1))  # object storage: restarts take seconds
            if random.random() < 0.5:
                kills += node.kill()
            if not node.alive():
                node.start()
        time.sleep(1.5)  # let the task catch up
        node.kill()
        crashes = {pt: open(node.log).read().count(f"aborting at {pt}") for pt in ("after_seg_put", "after_commit", "after_parquet_put")}
        clean = Node(lake, A.port, flush_ms=50, tier_secs=0, task_ms=100).start()  # verify without crash injection
        time.sleep(1)
        call(A.port, "POST", "/tier", timeout=600)
        want = A.producers * A.batches * A.size
        r = sql(A.port, "SELECT count(*) AS n, count(DISTINCT producer || ':' || seq || ':' || i) AS uniq FROM events")[0]
        c = sql(A.port, "SELECT count(*) AS n, count(DISTINCT producer || ':' || seq || ':' || i) AS uniq FROM events_copy")[0]
        v = sql(A.port, "SELECT count(*) AS k, min(n) AS lo, max(n) AS hi, min(s) AS slo, max(s) AS shi FROM per_producer")[0]
        t = sql(A.port, "SELECT count(*) AS n, count(DISTINCT producer || ':' || seq || ':' || i) AS uniq FROM thirds")[0]
        per, thirds = A.batches * A.size, A.producers * A.batches * len(range(0, A.size, 3))
        views_ok = v == {"k": A.producers, "lo": per, "hi": per, "slo": A.batches * sum(range(A.size)), "shi": A.batches * sum(range(A.size))} and t["n"] == t["uniq"] == thirds
        ok = r["n"] == r["uniq"] == want and c["n"] == c["uniq"] == want // 2 and views_ok
        for k, v in {**crashes, "kill -9": kills}.items():
            totals[k] = totals.get(k, 0) + v
        print(f"run {run:2}: kill-9={kills} injected={crashes} events={r['n']}/{want} unique={r['uniq']} "
              f"task_output={c['n']}/{want // 2} views={'exact' if views_ok else v} -> {'OK' if ok else 'FAIL'}", flush=True)
        clean.kill()
        if not ok:
            sys.exit(1)
    return f"{A.runs}/{A.runs} runs, 0 lost, 0 duplicated (events, streaming-task output, views); crashes survived: {totals}"


def lookup_mismatch(k, model):
    """How many of GET /lookup/kv/{k} and the SQL point query disagree with the model (a live
    value, or nothing if deleted)."""
    want = [model[k]] if k in model else []
    rows = [call(A.port, "GET", f"/lookup/kv/{k}"), sql(A.port, f"SELECT id, v FROM kv WHERE id = {k}")]
    return sum([r["v"] for r in rs] != want for rs in rows)


def outside():
    """Files outside the lake (ADR-026) on a local S3 (moto) and a web server, against pyarrow:
    Parquet globs, folders and lists, CSV and TSV with options, JSON lines, Hive-style folders,
    files skipped by their footers' ranges, spread over three nodes (== one node) and joined with
    a lake table, CREATE TABLE … AS and INSERT from files, a file changed under its name. Who may
    read: only with a secret covering the URL (CREATE SECRET, sealed in the catalog, listed by
    secrets() without its values), or as the program that started the node — its own machine's
    files too; a node with another key can't open a secret and says so."""
    import boto3, datetime, io, socket, threading, http.server, functools, pyarrow as pa, pyarrow.parquet as pq
    s3p, webp, owner_key = A.port + 50, A.port + 51, uuid.uuid4().hex
    for port in (s3p, webp):  # (a server left from a run that died would answer with that run's files)
        with socket.socket() as s:
            if s.connect_ex(("127.0.0.1", port)) == 0:
                raise RuntimeError(f"port {port} is already in use")
    sim = subprocess.Popen([sys.executable, os.path.join(os.path.dirname(os.path.abspath(__file__)), "sim_r2.py"), "--port", str(s3p), "--zero"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    atexit.register(sim.kill)
    s3 = boto3.client("s3", endpoint_url=f"http://127.0.0.1:{s3p}", region_name="us-east-1", aws_access_key_id="k", aws_secret_access_key="s")
    for _ in range(100):
        try:
            s3.create_bucket(Bucket="ext")
            break
        except Exception:
            time.sleep(0.1)
    put = lambda key, data: s3.put_object(Bucket="ext", Key=key, Body=data)
    parquet = lambda t: (lambda b: (pq.write_table(t, b, row_group_size=500), b.getvalue())[1])(io.BytesIO())
    rng, parts = random.Random(23), []
    for i in range(4):  # (one month a file: a filter on `day` skips the others by their footers)
        n = 2000
        t = pa.table({"id": list(range(i * n, (i + 1) * n)), "region": [rng.choice(["eu", "us", "asia"]) for _ in range(n)],
                      "amount": [rng.randrange(1000) for _ in range(n)], "note": [None if rng.random() < 0.1 else f"n{rng.randrange(50)}" for _ in range(n)],
                      "day": pa.array([datetime.date(2026, i + 1, 1 + j % 28) for j in range(n)])})
        parts.append(t)
        put(f"sales/2026/part-{i}.parquet", parquet(t))
    sales = pa.concat_tables(parts).to_pylist()
    put("drop/orders.csv", b"id;who;amount\n1;ann;5\n2;bo;7\n3;ann;1\n")
    put("drop/orders.tsv", b"id\twho\tamount\n1\tann\t5\n2\tbo\t7\n")
    put("feed/events.ndjson", b'{"k":"a","v":1}\n{"k":"b","v":2}\n{"k":"a","v":3,"x":true}\n')
    for day in ("2026-01-01", "2026-01-02"):
        put(f"hive/day={day}/data.parquet", parquet(pa.table({"v": [1, 2, 3]})))
    for k, n in (("__HIVE_DEFAULT_PARTITION__", 2), ("x", 3)):
        put(f"hive_null/k={k}/data.parquet", parquet(pa.table({"v": list(range(n))})))
    web = tempfile.mkdtemp(prefix="pondra-web-")
    open(os.path.join(web, "small.parquet"), "wb").write(parquet(pa.table({"x": [1, 2, 3, 4]})))

    class Ranged(http.server.SimpleHTTPRequestHandler):  # (object stores read a file's footer by range)
        def log_message(self, *a): pass
        def send_head(self):
            path = self.translate_path(self.path)
            if not os.path.isfile(path) or "Range" not in self.headers:
                return super().send_head()
            data = open(path, "rb").read()
            lo, hi = self.headers["Range"].split("=")[1].split("-")
            lo, hi = (len(data) - int(hi), len(data) - 1) if lo == "" else (int(lo), int(hi) if hi else len(data) - 1)
            self.send_response(206)
            self.send_header("Content-Range", f"bytes {lo}-{hi}/{len(data)}")
            self.send_header("Content-Length", str(hi - lo + 1))
            self.end_headers()
            return io.BytesIO(data[lo:hi + 1])
    server = http.server.ThreadingHTTPServer(("127.0.0.1", webp), functools.partial(Ranged, directory=web))
    threading.Thread(target=server.serve_forever, daemon=True).start()

    lake = new_lake()
    env = {"PONDRA_SECRET_KEY": "one key for the cluster", "PONDRA_OWNER_KEY": owner_key, "AWS_ENDPOINT": f"http://127.0.0.1:{s3p}",
           "AWS_ACCESS_KEY_ID": "k", "AWS_SECRET_ACCESS_KEY": "s", "AWS_REGION": "us-east-1", "AWS_ALLOW_HTTP": "true"}  # (the owner's own credentials)
    pgp = A.port + 52
    nodes = [Node(lake, A.port + i, env={**env, "PONDRA_SPREAD_MB": "0"} if i == 0 else env, **({"pg": f"127.0.0.1:{pgp}"} if i == 1 else {})).start() for i in range(3)]  # (node 0 spreads a COPY of these few files)

    def q(s, i=0, owner=False, spread=None):
        path = "/sql" + (f"?spread={spread}" if spread is not None else "")
        return call(A.port + i, "POST", path, s.encode(), headers={"x-pondra-owner": owner_key} if owner else {})

    def refused(s, i=0, owner=False):
        try:
            q(s, i, owner)
            return ""
        except RuntimeError as e:
            return str(e)

    glob = "'s3://ext/sales/2026/*.parquet'"
    per = lambda rows: sorted({r["region"] for r in rows})
    want = [{"region": g, "n": sum(1 for r in sales if r["region"] == g), "s": sum(r["amount"] for r in sales if r["region"] == g)} for g in per(sales)]
    by_region = f"SELECT region, count(*) AS n, sum(amount) AS s FROM {glob} GROUP BY region ORDER BY region"
    checks = {"without a secret, a URL is refused": "no secret covers" in refused(by_region),
              "…but the program that started the node reads it, with its own credentials": q(by_region, owner=True) == want,
              "…and a path on the node's machine only for that program": "only the program that started the node" in refused("SELECT * FROM read_csv('/etc/hostname')")
                  and q("SELECT count(*) AS n FROM read_csv('/etc/hostname', header => false)", owner=True) == [{"n": 1}]}
    q("CREATE SECRET ext_s3 (TYPE s3, KEY_ID 'k', SECRET 'pondra-sealed-7f3a', ENDPOINT 'http://127.0.0.1:%d', SCOPE 's3://ext')" % s3p)
    q("CREATE SECRET web (TYPE http, SCOPE 'http://127.0.0.1:%d')" % webp)
    sealed = not any(b"pondra-sealed-7f3a" in open(os.path.join(d, f), "rb").read() for d, _, fs in os.walk(lake) for f in fs)
    checks["CREATE SECRET: listed by secrets() without its values, sealed in the catalog"] = sealed and q("SELECT * FROM secrets() ORDER BY name", 1) == [
        {"name": "ext_s3", "type": "s3", "scope": "s3://ext"}, {"name": "web", "type": "http", "scope": "http://127.0.0.1:%d" % webp}]
    checks["one secret a bucket: another scope in it, or the same one, refused"] = all("a bucket is read with one secret" in refused(
        f"CREATE SECRET s{i} (TYPE s3, KEY_ID 'x', SECRET 'y', SCOPE '{s}')") for i, s in enumerate(["s3://ext/sales", "s3://ext"]))
    checks["a Parquet glob, a folder, a list of files == pyarrow"] = (q(by_region, 1) == want and q(by_region.replace(glob, "read_parquet('s3://ext/sales/2026/')"), 2) == want
        and q(f"SELECT count(*) AS n FROM read_parquet(['s3://ext/sales/2026/part-0.parquet', 's3://ext/sales/2026/part-3.parquet'])") == [{"n": 4000}]
        and q(f"SELECT count(*) AS n, count(note) AS notes FROM {glob}") == [{"n": len(sales), "notes": sum(r["note"] is not None for r in sales)}])
    checks["CSV with options, TSV by extension, JSON lines"] = (q("SELECT who, sum(amount) AS s FROM read_csv('s3://ext/drop/orders.csv', delim => ';') GROUP BY who ORDER BY who") == [{"who": "ann", "s": 6}, {"who": "bo", "s": 7}]
        and q("SELECT sum(amount) AS s FROM 's3://ext/drop/orders.tsv'") == [{"s": 12}]
        and q("SELECT k, sum(v) AS s, count(x) AS x FROM 's3://ext/feed/events.ndjson' GROUP BY k ORDER BY k") == [{"k": "a", "s": 4, "x": 1}, {"k": "b", "s": 2, "x": 0}])
    checks["Hive-style folders are columns, and filter (NULL's folder too)"] = (q("SELECT day, sum(v) AS s FROM read_parquet('s3://ext/hive/', hive_partitioning => true) GROUP BY day ORDER BY day") == [{"day": "2026-01-01", "s": 6}, {"day": "2026-01-02", "s": 6}]
        and q("SELECT count(*) AS n FROM read_parquet('s3://ext/hive/**/*.parquet', hive_partitioning => true) WHERE day = '2026-01-02'") == [{"n": 3}]
        and q("SELECT count(*) AS n FROM read_parquet('s3://ext/hive_null/') WHERE k IS NULL") == [{"n": 2}] and q("SELECT count(*) AS n FROM read_parquet('s3://ext/hive_null/') WHERE k = 'x'") == [{"n": 3}]
        and q("SELECT count(k) AS n, count(*) AS rows FROM read_parquet('s3://ext/hive_null/')") == [{"n": 3, "rows": 5}])
    before = metrics_of(A.port)
    march = q(f"SELECT count(*) AS n FROM {glob} WHERE day >= '2026-03-01' AND day < '2026-04-01'", spread=0)
    after = metrics_of(A.port)
    checks["a filter skips files by their footers' ranges"] = march == [{"n": 2000}] and after["pondra_files_skipped_total"] - before["pondra_files_skipped_total"] == 3
    spread = metrics_of(A.port + 1)["pondra_spread_queries_total"]
    q("CREATE TABLE eu AS SELECT * FROM %s WHERE region = 'eu'" % glob)
    joined = f"SELECT e.region, count(*) AS n, sum(l.amount) AS s FROM {glob} e JOIN eu l ON e.id = l.id GROUP BY e.region"
    while len(call(A.port + 1, "GET", "/stats")["nodes"]) < 3:
        time.sleep(0.1)  # (node 1 has heard of the others)
    checks["spread over three nodes == one node, and joined with a lake table"] = (q(by_region, 1, spread=1) == want and q(joined, 1, spread=1) == q(joined, 1, spread=0)
        and metrics_of(A.port + 1)["pondra_spread_queries_total"] - spread == 2)
    q("INSERT INTO eu SELECT * FROM %s WHERE region = 'eu' AND id < 100" % glob)
    eu = [r for r in sales if r["region"] == "eu"]
    checks["CREATE TABLE … AS and INSERT … SELECT from files"] = q("SELECT count(*) AS n FROM eu") == [{"n": len(eu) + sum(1 for r in eu if r["id"] < 100)}]
    checks["a web server's file, with its secret"] = q("SELECT sum(x) AS s FROM 'http://127.0.0.1:%d/small.parquet'" % webp) == [{"s": 10}]
    total = "SELECT sum(amount) AS s FROM read_csv('s3://ext/drop/orders.csv', delim => ';')"
    before = q(total)
    put("drop/orders.csv", b"id;who;amount\n1;ann;50\n")
    put("drop/x/1.parquet", parquet(pa.table({"x": [1, 2, 3]})))
    xs = "SELECT count(*) AS n, max(x) AS m, sum(x) AS s FROM 's3://ext/drop/x/*.parquet'"
    small = q(xs, 1)
    put("drop/x/2.parquet", parquet(pa.table({"x": list(range(5000))})))
    more = _try(lambda: q(xs, 1))
    put("drop/x/1.parquet", parquet(pa.table({"x": [7, 8, 9]})))  # (as big as it was: its bytes were read, and kept by their version)
    checks["a file changed under its name, or one more in a folder: the next query, at once, reads them as they are now"] = (before == [{"s": 13}] and q(total) == [{"s": 50}]
        and small == [{"n": 3, "m": 3, "s": 6}] and more == [{"n": 5003, "m": 4999, "s": 12497506}] and _try(lambda: q(xs, 1)) == [{"n": 5003, "m": 4999, "s": 12497524}])
    checks["refused by name: an unknown option, a compressed file, an unknown format"] = all(w in refused(s) for s, w in [
        ("SELECT * FROM read_csv('s3://ext/drop/orders.csv', bogus => 1)", "no option bogus"), ("SELECT * FROM 's3://ext/x.csv.gz'", "compressed"), ("SELECT * FROM 's3://ext/x.xlsx'", "which format")])
    # COPY … TO: files out, in each format, read back by pyarrow and by Pondra.
    got = lambda key: s3.get_object(Bucket="ext", Key=key)["Body"].read()
    keys = lambda prefix: sorted(o["Key"] for o in s3.list_objects_v2(Bucket="ext", Prefix=prefix).get("Contents", []))
    copied = q(f"COPY (SELECT * FROM {glob} WHERE region = 'eu' ORDER BY id) TO 's3://ext/out/eu.parquet'")
    checks["COPY … TO a Parquet file: pyarrow reads what the query gave"] = (copied == {"copied": len(eu), "to": "s3://ext/out/eu.parquet"}
        and pq.read_table(io.BytesIO(got("out/eu.parquet"))).to_pylist() == [r for r in sales if r["region"] == "eu"])
    q(f"COPY (SELECT id, region, amount FROM {glob}) TO 's3://ext/out/by_region/' (FORMAT parquet, PARTITION_BY (region))")
    folders = sorted({k.split("/")[2] for k in keys("out/by_region/")})
    checks["…a folder of files by PARTITION_BY, read back as one table"] = (folders == [f"region={g}" for g in per(sales)]
        and q("SELECT region, count(*) AS n, sum(amount) AS s FROM read_parquet('s3://ext/out/by_region/', hive_partitioning => true) GROUP BY region ORDER BY region", 1) == want)
    names = lambda prefix: {k.rsplit("/", 1)[1].split("_")[0] for k in keys(prefix)}  # (one write's files share the first part of their names)
    checks["…written by every node at once, each its own share's files"] = len(names("out/by_region/")) == 3
    q("CREATE TABLE spread_me (id BIGINT, amount BIGINT)")
    for i in range(3):
        q(f"INSERT INTO spread_me SELECT id, amount FROM {glob} WHERE id % 3 = {i}")
        call(A.port, "POST", "/tier", timeout=600)
    q("INSERT INTO spread_me VALUES " + ", ".join(f"({i}, {i})" for i in range(10)))  # (in the log only, for now)
    copied = q("COPY spread_me TO 's3://ext/out/lake/' (FORMAT parquet)")
    lake_rows = [{"n": len(sales) + 10, "s": sum(r["amount"] for r in sales) + sum(range(10))}]
    checks["…a lake table's files and its log tail, every node its share"] = (copied["copied"] == lake_rows[0]["n"] and len(names("out/lake/")) >= 3
        and q("SELECT count(*) AS n, sum(amount) AS s FROM read_parquet('s3://ext/out/lake/')", 1) == lake_rows)
    q(f"COPY (SELECT region, count(*) AS n FROM {glob} GROUP BY region) TO 's3://ext/out/counts/' (FORMAT parquet)")
    q(f"COPY (SELECT * FROM {glob} LIMIT 10) TO 's3://ext/out/ten/' (FORMAT parquet)")
    checks["…but an aggregate or a LIMIT, whose rows no node has alone, from one node"] = (q("SELECT region, n FROM read_parquet('s3://ext/out/counts/') ORDER BY region", 1) == [{"region": w["region"], "n": w["n"]} for w in want]
        and q("SELECT count(*) AS n FROM read_parquet('s3://ext/out/ten/')", 1) == [{"n": 10}] and len(names("out/counts/")) == len(names("out/ten/")) == 1)
    q("COPY eu TO 's3://ext/out/eu.csv' (HEADER true, DELIMITER ';')")
    q("COPY (SELECT k, v FROM 's3://ext/feed/events.ndjson' ORDER BY v) TO 's3://ext/out/events.json'")
    lines = got("out/eu.csv").decode().splitlines()
    checks["…CSV with a header and a delimiter, JSON lines"] = (lines[0].split(";")[:3] == ["id", "region", "amount"] and len(lines) == 1 + len(eu) + sum(1 for r in eu if r["id"] < 100)
        and [json.loads(l) for l in got("out/events.json").decode().splitlines()] == [{"k": "a", "v": 1}, {"k": "b", "v": 2}, {"k": "a", "v": 3}])
    q("COPY (SELECT 1 AS a) TO 's3://ext/out/eu.parquet'")
    checks["…a file there already is replaced"] = pq.read_table(io.BytesIO(got("out/eu.parquet"))).to_pylist() == [{"a": 1}]
    import psycopg
    with psycopg.connect(f"host=127.0.0.1 port={pgp} user=u dbname=lake", autocommit=True) as c:
        c.execute("COPY (SELECT 2 AS b) TO 's3://ext/out/pg.parquet'")
    checks["…from the Postgres port too"] = pq.read_table(io.BytesIO(got("out/pg.parquet"))).to_pylist() == [{"b": 2}]
    mine = tempfile.mkdtemp(prefix="pondra-copy-")
    checks["…to a bucket no secret covers: refused, but the node's owner may"] = ("no secret covers" in refused("COPY eu TO 's3://elsewhere/eu.parquet'")
        and "only the program that started the node" in refused(f"COPY eu TO '{mine}/eu.parquet'")
        and q(f"COPY eu TO '{mine}/eu.parquet'", owner=True) and pq.read_table(f"{mine}/eu.parquet").num_rows == len(lines) - 1
        and q(f"SELECT count(*) AS n FROM '{mine}/*.parquet'", owner=True) == [{"n": len(lines) - 1}])
    checks["…and never into a lake, not even by the node's owner"] = "inside the lake" in refused(f"COPY eu TO '{lake}/data/eu/x.parquet'", owner=True)
    shutil.rmtree(mine, ignore_errors=True)
    tokens = {"read_token": "r", "write_token": "w", "admin_token": "a"}
    other = Node(lake, A.port + 3, env={**env, "PONDRA_SECRET_KEY": "another key"}, **tokens).start()
    as_ = lambda s, t: call(A.port + 3, "POST", "/sql", s.encode(), headers={"authorization": f"Bearer {t}"})
    def refused_as(s, t):
        try:
            return as_(s, t) and ""
        except RuntimeError as e:
            return str(e)
    checks["…needs the admin role (it writes outside the lake)"] = "outside the lake" in refused_as("COPY eu TO 's3://ext/out/w.parquet'", "w")
    checks["a node with another key can't open the secret, and says why"] = "same PONDRA_SECRET_KEY" in refused_as(by_region, "r")
    other.kill()
    q("DROP SECRET ext_s3")
    checks["DROP SECRET: refused again"] = "no secret covers" in refused(by_region)
    [n.kill() for n in nodes]
    sim.kill()
    server.shutdown()
    shutil.rmtree(web, ignore_errors=True)
    print(json.dumps(checks, indent=1, ensure_ascii=False))
    if not all(checks.values()):
        sys.exit(1)
    return f"files outside the lake (S3, a web server, this machine), secrets: all {len(checks)} checks pass"


AZURITE_KEY = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="  # (Azurite's well-known account key)


def clouds():
    """Lakes on Google Cloud Storage and Azure, and files there (ADR-026), on emulators:
    `sim_gcs.py` (the XML API `object_store` speaks) and Azurite (Microsoft's: `npm install -g
    azurite`, or AZURITE=…/azurite-blob). For each: two nodes on a lake in the bucket, rows written
    on both, tiered to Parquet and read on both; the leader killed, the other leads (a term is a
    put-if-absent object there too) and takes writes with every row still there; then COPY … TO
    the store with a secret, and the files read back. Without Azurite, its half is skipped (said)."""
    import socket
    gport, aport = A.port + 52, A.port + 53
    for port in (gport, aport):
        with socket.socket() as s:
            if s.connect_ex(("127.0.0.1", port)) == 0:
                raise RuntimeError(f"port {port} is already in use")
    here = os.path.dirname(os.path.abspath(__file__))
    gcs = subprocess.Popen([sys.executable, os.path.join(here, "sim_gcs.py"), "--port", str(gport), "--bucket", "lakes", "--bucket", "ext"])
    atexit.register(gcs.kill)
    key = json.dumps({"gcs_base_url": f"http://127.0.0.1:{gport}", "disable_oauth": True, "client_email": "", "private_key": "", "private_key_id": ""})
    conn = f"DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey={AZURITE_KEY};BlobEndpoint=http://127.0.0.1:{aport}/devstoreaccount1;"
    owner = uuid.uuid4().hex
    env = {"GOOGLE_SERVICE_ACCOUNT_KEY": key, "GOOGLE_ALLOW_HTTP": "true", "AZURE_STORAGE_USE_EMULATOR": "true", "AZURITE_BLOB_STORAGE_URL": f"http://127.0.0.1:{aport}", "PONDRA_OWNER_KEY": owner}
    stores = [("gcs", "gs", f"CREATE SECRET ext_gcs (TYPE gcs, SERVICE_ACCOUNT_KEY '{key}', ENDPOINT 'http://127.0.0.1:{gport}', SCOPE 'gs://ext')")]
    azurite = os.environ.get("AZURITE") or shutil.which("azurite-blob") or next(iter(glob_.glob(os.path.expanduser("~/azurite/node_modules/.bin/azurite-blob"))), None)
    if azurite:
        az = subprocess.Popen([azurite, "--blobHost", "127.0.0.1", "--blobPort", str(aport), "--inMemoryPersistence", "--silent", "--skipApiVersionCheck", "--loose"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        atexit.register(az.kill)
        from azure.storage.blob import BlobServiceClient
        for _ in range(100):
            try:
                blobs = BlobServiceClient.from_connection_string(conn)
                [blobs.create_container(c) for c in ("lakes", "ext")]
                break
            except Exception:
                time.sleep(0.2)
        stores.append(("azure", "az", f"CREATE SECRET ext_az (TYPE azure, CONNECTION_STRING '{conn}', SCOPE 'az://ext')"))
    checks, run = {}, uuid.uuid4().hex[:8]
    for name, scheme, secret in stores:
        lake = f"{scheme}://lakes/test-{run}"
        a, b = Node(lake, A.port, env=env).start(), Node(lake, A.port + 1, env=env).start()
        sql(A.port, "CREATE TABLE t (id BIGINT, v VARCHAR)")
        rows = lambda lo, hi: "INSERT INTO t VALUES " + ", ".join(f"({i}, 'v{i % 7}')" for i in range(lo, hi))
        sql(A.port, rows(0, 1000))
        until(lambda: sql(A.port + 1, "SELECT count(*) AS n FROM t"), [{"n": 1000}], 10)
        sql(A.port + 1, rows(1000, 2000))  # (a follower's write, sequenced by the leader)
        call(A.port, "POST", "/tier", timeout=300)
        want = lambda n: [{"n": n, "s": n * (n - 1) // 2}]
        read = lambda port: sql(port, "SELECT count(*) AS n, sum(id) AS s FROM t")
        tiered = call(A.port, "POST", "/sql", f"SELECT count(*) AS n FROM '{lake}/data/t/*.parquet'".encode(), headers={"x-pondra-owner": owner})  # (the lake's files, as files)
        checks[f"{name}: two nodes on a lake there, rows from both, tiered to Parquet, read on both"] = (
            all(until(lambda p=p: read(p), want(2000), 20) == want(2000) for p in (A.port, A.port + 1)) and tiered == [{"n": 2000}])
        a.kill()
        def leads():
            try:
                return call(A.port + 1, "GET", "/stats", timeout=2).get("role") == "leader"
            except (OSError, RuntimeError):
                return False  # (restarting to lead)
        until(leads, True, 60)
        sql(A.port + 1, rows(2000, 2500))
        checks[f"{name}: the leader killed, the other leads (a put-if-absent term there) with every row"] = until(lambda: read(A.port + 1), want(2500), 20) == want(2500)
        # A file beside the lake in its bucket is read through the lake's reader, but never cached:
        # changed under its name (the same size), the next query reads it as it is now.
        drop = f"{scheme}://lakes/drop-{run}/n.csv"
        as_owner = lambda s: call(A.port + 1, "POST", "/sql", s.encode(), headers={"x-pondra-owner": owner})
        as_owner(f"COPY (SELECT 5 AS n) TO '{drop}'")
        first = as_owner(f"SELECT n FROM '{drop}'")
        as_owner(f"COPY (SELECT 7 AS n) TO '{drop}'")
        checks[f"{name}: a file beside the lake, changed under its name: read as it is now"] = first == [{"n": 5}] and as_owner(f"SELECT n FROM '{drop}'") == [{"n": 7}]
        try:
            as_owner(f"COPY (SELECT 1 AS n) TO '{lake}/data/t/x.parquet'")
            checks[f"{name}: COPY … TO into the lake: refused"] = False
        except RuntimeError as e:
            checks[f"{name}: COPY … TO into the lake: refused"] = "inside the lake" in str(e)
        sql(A.port + 1, secret)
        out = f"{scheme}://ext/{run}/t/"
        copied = sql(A.port + 1, f"COPY (SELECT * FROM t) TO '{out}' (FORMAT parquet, PARTITION_BY (v))")
        back = sql(A.port + 1, f"SELECT count(*) AS n, sum(id) AS s, count(DISTINCT v) AS vs FROM read_parquet('{out}')")
        checks[f"{name}: COPY … TO the store with a secret, read back by its folders"] = copied.get("copied") == 2500 and back == [{**want(2500)[0], "vs": 7}]
        b.kill()
    if not azurite:
        print("(Azurite not found: npm install -g azurite, or AZURITE=…/azurite-blob; the Azure half is skipped)")
    print(json.dumps(checks, indent=1, ensure_ascii=False))
    if not all(checks.values()):
        sys.exit(1)
    return f"lakes and files on {' and '.join(s[0] for s in stores)} (emulated): all {len(checks)} checks pass"


def start_kafka(port, root):
    """Apache Kafka (KRaft, one node), if it is here (KAFKA_HOME, or ~/kafka_2.13-*): PLAINTEXT on
    `port`, SASL SCRAM-SHA-256 (alice / alice-secret) on `port + 1`. None if it isn't."""
    home = os.environ.get("KAFKA_HOME") or next(iter(sorted(glob_.glob(os.path.expanduser("~/kafka_2.13-*")) + glob_.glob("/home/claude/kafka_2.13-*"))), None)
    if not home:
        return None
    logs = os.path.join(root, "kafka-logs")
    conf = os.path.join(root, "kafka.properties")
    open(conf, "w").write("\n".join([
        "process.roles=broker,controller", "node.id=1", f"controller.quorum.bootstrap.servers=127.0.0.1:{port + 2}",
        f"listeners=PLAINTEXT://127.0.0.1:{port},SASL_PLAINTEXT://127.0.0.1:{port + 1},CONTROLLER://127.0.0.1:{port + 2}",
        f"advertised.listeners=PLAINTEXT://127.0.0.1:{port},SASL_PLAINTEXT://127.0.0.1:{port + 1}", "controller.listener.names=CONTROLLER",
        "listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,SASL_PLAINTEXT:SASL_PLAINTEXT", "sasl.enabled.mechanisms=SCRAM-SHA-256",
        "listener.name.sasl_plaintext.scram-sha-256.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required;",
        f"log.dirs={logs}", "num.partitions=3", "offsets.topic.replication.factor=1", "transaction.state.log.replication.factor=1",
        "transaction.state.log.min.isr=1", "group.initial.rebalance.delay.ms=0", ""]))
    env = {**os.environ, "KAFKA_HEAP_OPTS": "-Xmx512m"}
    cluster = subprocess.run([f"{home}/bin/kafka-storage.sh", "random-uuid"], capture_output=True, text=True, env=env).stdout.strip().splitlines()[-1]
    subprocess.run([f"{home}/bin/kafka-storage.sh", "format", "--standalone", "-t", cluster, "-c", conf, "--add-scram", "SCRAM-SHA-256=[name=alice,password=alice-secret]"], capture_output=True, env=env, check=True)
    broker = subprocess.Popen([f"{home}/bin/kafka-server-start.sh", conf], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=env)
    atexit.register(broker.kill)
    import kafka as kp
    for _ in range(120):
        try:
            kp.KafkaAdminClient(bootstrap_servers=f"127.0.0.1:{port}").close()
            return broker
        except Exception:
            time.sleep(0.5)
    raise RuntimeError("Kafka didn't start")


def kafkas():
    """Other Kafka clusters (ADR-026), in and out: Apache Kafka 4 (if here: `start_kafka`) and a
    Pondra node's own Kafka port. A topic read as a table == what was produced, spread over three
    nodes == one node, a LIMIT reading only what it needs; COPY … TO a topic, each key in the
    partition Kafka's producers would pick; a view fed by a topic, every record once while the
    node running a partition is killed, and then the leader; SASL SCRAM-SHA-256 and PLAIN from
    secrets, a wrong password refused."""
    import kafka as kp
    from kafka.partitioner.default import murmur2
    root = tempfile.mkdtemp(prefix="pondra-kafkas-")
    kport = A.port + 60
    broker = start_kafka(kport, root)
    lake = new_lake()
    nodes = [Node(lake, A.port + i, env={"PONDRA_SECRET_KEY": "k"}).start() for i in range(3)]
    q = lambda s, i=0, spread=None: call(A.port + i, "POST", "/sql" + (f"?spread={spread}" if spread is not None else ""), s.encode())
    def refused(s):
        try:
            q(s)
            return ""
        except RuntimeError as e:
            return str(e)
    while len(call(A.port + 1, "GET", "/stats")["nodes"]) < 3:
        time.sleep(0.1)
    checks = {}
    # A Pondra node as the other cluster: a topic is its table (one partition), tokens its passwords.
    other = new_lake()
    pk = A.port + 70
    them = Node(other, A.port + 5, kafka=f"127.0.0.1:{pk}", read_token="r-tok", write_token="w-tok", admin_token="a-tok").start()
    call(A.port + 5, "POST", "/sql", b"CREATE TABLE ticks (id BIGINT, v BIGINT)", headers={"authorization": "Bearer a-tok"})
    call(A.port + 5, "POST", "/sql", ("INSERT INTO ticks VALUES " + ", ".join(f"({i}, {i * 2})" for i in range(500))).encode(), headers={"authorization": "Bearer a-tok"})
    q(f"CREATE SECRET theirs (TYPE kafka, SECURITY_PROTOCOL 'SASL_PLAINTEXT', SASL_MECHANISM 'PLAIN', USERNAME 'reader', PASSWORD 'r-tok', SCOPE 'kafka://127.0.0.1:{pk}')")
    q(f"ATTACH 'kafka://127.0.0.1:{pk}' AS pondra_k (TYPE kafka)")
    got = q("SELECT count(*) AS n, sum(CAST(value->>'v' AS BIGINT)) AS s FROM pondra_k.ticks")
    checks["a Pondra node's Kafka port, SASL PLAIN from a secret: its table as a topic"] = got == [{"n": 500, "s": 249500}]
    q(f"CREATE OR REPLACE SECRET theirs (TYPE kafka, SECURITY_PROTOCOL 'SASL_PLAINTEXT', SASL_MECHANISM 'PLAIN', USERNAME 'reader', PASSWORD 'wrong', SCOPE 'kafka://127.0.0.1:{pk}')")
    checks["…a wrong password: refused"] = "refused" in refused("SELECT count(*) AS n FROM pondra_k.ticks")
    them.kill()
    if broker:
        url = f"kafka://127.0.0.1:{kport}"
        admin = kp.KafkaAdminClient(bootstrap_servers=f"127.0.0.1:{kport}")
        admin.create_topics([kp.admin.NewTopic("orders", 3, 1), kp.admin.NewTopic("keyed", 3, 1)])
        prod = kp.KafkaProducer(bootstrap_servers=f"127.0.0.1:{kport}", key_serializer=str.encode, value_serializer=lambda v: json.dumps(v).encode(), compression_type="gzip")
        sent = 3000
        for i in range(sent):
            prod.send("orders", key=f"k{i % 17}", value={"id": i, "amount": i % 100})
        prod.flush()
        q(f"CREATE SECRET plain (TYPE kafka, SCOPE '{url}')")  # (no password there: still, a grant)
        q(f"ATTACH '{url}' AS k (TYPE kafka)")
        want = [{"n": sent, "s": sum(i % 100 for i in range(sent)), "keys": 17}]
        agg = "SELECT count(*) AS n, sum(CAST(value->>'amount' AS BIGINT)) AS s, count(DISTINCT key) AS keys FROM k.orders"
        checks["Apache Kafka: a topic as a table == what was produced"] = q(agg) == want and len(q("SELECT * FROM k.orders LIMIT 5")) == 5
        spread = metrics_of(A.port + 1)["pondra_spread_queries_total"]
        checks["…spread over three nodes (its partitions dealt) == one node"] = q(agg, 1, spread=1) == want and metrics_of(A.port + 1)["pondra_spread_queries_total"] > spread
        # COPY … TO a topic: each key where Kafka's own producers put it.
        q(f"COPY (SELECT id, 'u' || (id % 23) AS who FROM generate_series(1, 2000) AS t(id)) TO '{url}/keyed' (FORMAT json, KEY who)")
        cons = kp.KafkaConsumer("keyed", bootstrap_servers=f"127.0.0.1:{kport}", auto_offset_reset="earliest", consumer_timeout_ms=3000)
        records = list(cons)
        placed = all(r.partition == (murmur2(r.key) & 0x7fffffff) % 3 for r in records)  # (the Java client's, as kafka-python has it)
        checks["COPY … TO a topic: every row once, each key in Kafka's partition for it"] = len(records) == 2000 and placed and len({json.loads(r.value)["id"] for r in records}) == 2000
        # SCRAM-SHA-256 from a secret.
        q(f"CREATE SECRET scram (TYPE kafka, SECURITY_PROTOCOL 'SASL_PLAINTEXT', SASL_MECHANISM 'SCRAM-SHA-256', USERNAME 'alice', PASSWORD 'alice-secret', SCOPE 'kafka://127.0.0.1:{kport + 1}')")
        checks["SASL SCRAM-SHA-256 from a secret"] = q(f"SELECT count(*) AS n FROM 'kafka://127.0.0.1:{kport + 1}/orders'") == [{"n": sent}]
        q(f"CREATE OR REPLACE SECRET scram (TYPE kafka, SECURITY_PROTOCOL 'SASL_PLAINTEXT', SASL_MECHANISM 'SCRAM-SHA-256', USERNAME 'alice', PASSWORD 'nope', SCOPE 'kafka://127.0.0.1:{kport + 1}')")
        checks["…a wrong SCRAM password: refused"] = "refused" in refused(f"SELECT count(*) AS n FROM 'kafka://127.0.0.1:{kport + 1}/orders'")
        # A view fed by the topic across three nodes, one of them killed while records arrive.
        q("CREATE MATERIALIZED VIEW orders_in AS SELECT CAST(value->>'id' AS BIGINT) AS id, key, _partition, _offset FROM k.orders")
        more = 3000
        for i in range(sent, sent + more):
            prod.send("orders", key=f"k{i % 17}", value={"id": i, "amount": i % 100})
            if i == sent + more // 3:
                prod.flush()
                nodes[2].kill()  # (its partitions go to the others)
            if i == sent + 2 * more // 3:
                prod.flush()
                time.sleep(1)
                nodes[0].kill()  # (the leader: the last node leads, and runs every partition)
        prod.flush()
        total = sent + more
        count = lambda: _try(lambda: q("SELECT count(*) AS n, count(DISTINCT (_partition, _offset)) AS d, count(DISTINCT id) AS ids FROM orders_in", 1))
        got = until(count, [{"n": total, "d": total, "ids": total}], 90)
        checks["a view fed by a topic on three nodes, one killed, then the leader: every record once"] = got == [{"n": total, "d": total, "ids": total}]
        checks["…its feed kept apart from the lake's functions (`/functions` lists them)"] = _try(lambda: call(A.port + 1, "GET", "/functions")) == {}
        if got != [{"n": total, "d": total, "ids": total}]:
            print("feed:", got)
    [n.kill() for n in nodes]
    if broker:
        broker.kill()
    shutil.rmtree(root, ignore_errors=True)
    if not broker:
        print("(Apache Kafka not found: KAFKA_HOME, or ~/kafka_2.13-*; its checks are skipped)")
    print(json.dumps(checks, indent=1, ensure_ascii=False))
    if not all(checks.values()):
        sys.exit(1)
    return f"other Kafka clusters in and out{'' if broker else ' (Pondra only)'}: all {len(checks)} checks pass"


def deal():
    """A keyed table's first tiering round, dealt to three nodes: a job per third of its log, each
    writing a file. Only the job that starts where the table's files end (there are none yet) may
    drop delete markers, and an adding-up view's groups a change added nothing to the count of;
    the other jobs' older rows are in its file (invariant 93). When every job of that round took
    itself for the first, the keys deleted in the last third came back, and a view lost the part of
    an UPDATE that changed a total but not a count (`harness.py changes`' drift, now and then)."""
    lake = new_lake()
    nodes = [Node(lake, A.port + i, tier_secs=0).start() for i in range(3)]
    q = lambda s, i=0: sql(A.port + i, s)
    q("CREATE TABLE kv (id BIGINT PRIMARY KEY, v BIGINT)")
    q("CREATE TABLE acct (id BIGINT, owner VARCHAR, bal DOUBLE)")
    q("CREATE MATERIALIZED VIEW per_owner AS SELECT owner, sum(bal) AS total, count(*) AS n FROM acct GROUP BY owner")
    # Commits of about a third of each table's rows each, so the round deals a job per node: kv's
    # delete markers, and the UPDATE's rows in per_owner (a: total +10, n +0), are in the last.
    q("INSERT INTO kv VALUES " + ", ".join(f"({i}, {i})" for i in range(1, 31)))
    q("INSERT INTO acct VALUES (1, 'a', 10), (2, 'b', 20)")
    q("INSERT INTO kv VALUES " + ", ".join(f"({i}, {i})" for i in range(31, 61)))
    q("INSERT INTO acct VALUES (3, 'a', 30), (4, 'b', 40)")
    q("DELETE FROM kv WHERE id <= 10")
    q("UPDATE acct SET bal = bal + 5 WHERE owner = 'a'")
    keys, view = list(range(11, 61)), [{"owner": "a", "total": 50.0, "n": 2}, {"owner": "b", "total": 60.0, "n": 2}]
    read = lambda i: ([r["id"] for r in q("SELECT id FROM kv ORDER BY id", i)], q("SELECT owner, total, n FROM per_owner ORDER BY owner", i))
    checks = {"before tiering, every node": all(until(lambda i=i: read(i), (keys, view), 10) == (keys, view) for i in range(3))}
    call(A.port, "POST", "/tier", timeout=600)
    got = [until(lambda i=i: read(i), (keys, view), 10) for i in range(3)]
    checks["after the first round, dealt to three nodes: deleted keys stay deleted, every node"] = all(g[0] == keys for g in got)
    checks["…and the view keeps the UPDATE that changed a total, not a count"] = all(g[1] == view for g in got)
    [n.kill() for n in nodes]
    print(json.dumps(checks, indent=1, ensure_ascii=False))
    if not all(checks.values()):
        print("got:", got)
        sys.exit(1)
    return "a keyed table's and a view's first tiering round, dealt to three nodes: markers and changes kept"


def upsert():
    lake, model, tiers, bad_lookups = new_lake(), {}, [], 0
    node = Node(lake, A.port, flush_ms=50, tier_secs=0, retain_secs=0).start()
    call(A.port, "POST", "/tables/kv", json.dumps({"columns": [["id", "Int64"], ["v", "Int64"], ["_deleted", "Boolean"]], "key": ["id"]}).encode())
    for seq in range(1, 61):
        ops = []
        for _ in range(200):
            k = random.randrange(500)
            if random.random() < 0.2:
                ops.append({"id": k, "v": None, "_deleted": True}); model.pop(k, None)
            else:
                v = random.randrange(10**6); ops.append({"id": k, "v": v, "_deleted": False}); model[k] = v
        call(A.port, "POST", f"/append/kv?producer=u&seq={seq}", "".join(json.dumps(o) + "\n" for o in ops).encode())
        for k in random.sample(range(500), 20):  # the /lookup fast path agrees with the model too
            bad_lookups += lookup_mismatch(k, model)
        if seq % 10 == 0:
            t0 = time.time(); call(A.port, "POST", "/tier", timeout=600)  # compaction in between
            tiers.append(time.time() - t0)
        if seq == 30:
            node.kill(); node.start()  # restart in the middle
    got = {r["id"]: r["v"] for r in sql(A.port, "SELECT id, v FROM kv ORDER BY id")}
    live = sql(A.port, "SELECT count(*) AS n FROM kv")[0]["n"]
    bad_lookups += sum(lookup_mismatch(k, model) for k in range(500))
    node.kill()
    ok = got == model and bad_lookups == 0
    print(f"upsert: {len(model)} live keys after 12,000 random upserts/deletes, 6 compactions (s: {', '.join(f'{t:.1f}' for t in tiers)}), "
          f"1 restart, 3,400 lookups ({bad_lookups} wrong) -> {'OK' if ok else 'FAIL'}")
    if not ok:
        sys.exit(1)
    return f"12,000 random upserts/deletes with compactions and a restart match the model exactly ({live} live keys)"


def tiering():
    """Tiering has to keep working as files pile up: 12 rounds of writes + /tier over an append
    table, an upsert table and a GROUP BY view. The log must drain, the file count must stay
    bounded (merges and compactions), and every row must still be there."""
    lake, rounds, per = new_lake(), A.rounds, A.size
    node = Node(lake, A.port, tier_secs=0).start()
    call(A.port, "POST", "/tables/events", json.dumps([["user", "Utf8"], ["amount", "Int64"]]).encode())
    call(A.port, "POST", "/tables/kv", json.dumps({"columns": [["id", "Int64"], ["v", "Int64"]], "key": ["id"]}).encode())
    call(A.port, "POST", "/views/totals", b"SELECT user, sum(amount) AS amount FROM events GROUP BY user")
    for r in range(1, rounds + 1):
        call(A.port, "POST", f"/append/events?producer=p&seq={r}", "".join(
            json.dumps({"user": f"u{i % 50}", "amount": 1}) + "\n" for i in range(per)).encode(), timeout=600)
        call(A.port, "POST", f"/append/kv?producer=k&seq={r}", "".join(
            json.dumps({"id": i, "v": r}) + "\n" for i in range(200)).encode())
        call(A.port, "POST", "/tier", timeout=600)
    untiered = call(A.port, "GET", "/stats")["untiered_rows"]
    out = subprocess.run([BIN, "catalog", "--dir", lake, "t/"], capture_output=True, text=True).stdout
    files = {l.split(" ", 1)[0][2:]: len(json.loads(l.split(" ", 1)[1])["files"]) for l in out.splitlines()}
    got = {"events": sql(A.port, "SELECT count(*) AS n FROM events")[0]["n"],
           "kv": sql(A.port, "SELECT count(*) AS n, sum(v) AS v FROM kv")[0],
           "totals": sql(A.port, "SELECT sum(amount) AS n FROM totals")[0]["n"]}
    node.kill()
    ok = (got["events"] == rounds * per and got["totals"] == rounds * per
          and got["kv"] == {"n": 200, "v": 200 * rounds} and untiered == 0 and max(files.values()) <= 8)
    print(f"tiering: {rounds} rounds -> files {files}, untiered rows {untiered}, rows {got} -> {'OK' if ok else 'FAIL'}")
    if not ok:
        sys.exit(1)
    return f"{rounds} rounds of writes and tiering: log drained, files bounded ({files}), every row exact"


def fence():
    """A second node on the same lake joins as a follower. Then the leader is frozen (SIGSTOP, like
    a network partition), the follower takes over, and the old leader wakes up still believing it
    leads: its write must be rejected (fenced), and it must rejoin as a follower. Nothing lost."""
    lake = new_lake()
    a = Node(lake, A.port, flush_ms=50).start()
    events_table(A.port)
    seg = call(A.port, "POST", "/append/events?producer=a&seq=1", rows("a", 1, 100))["seg"]
    b = Node(lake, A.port + 1, flush_ms=50).start()
    joined = call(A.port + 1, "GET", "/stats")["role"] == "follower"
    while len(call(A.port + 1, "GET", "/stats")["nodes"]) < 2:  # b has heard from the leader
        time.sleep(0.1)
    via_b = call(A.port + 1, "POST", "/append/events?producer=b&seq=1", rows("b", 1, 100))["seg"]  # forwarded
    a.p.send_signal(signal.SIGSTOP)
    t = time.time()
    while time.time() - t < 60:
        try:
            if call(A.port + 1, "GET", "/stats", timeout=5)["role"] == "leader":
                break
        except Exception:
            pass  # restarting as the new leader
        time.sleep(0.2)
    takeover = time.time() - t
    call(A.port + 1, "POST", "/append/events?producer=b&seq=2", rows("b", 2, 100))
    a.p.send_signal(signal.SIGCONT)
    try:
        ack = call(A.port, "POST", "/append/events?producer=a&seq=2", rows("a", 2, 100), timeout=10)
        a_after = "accepted" if not ack.get("conflict") else "rejected"
    except Exception:
        a_after = "rejected"
    t = time.time()
    role = None
    while time.time() - t < 20 and role != "follower":
        try:
            role = call(A.port, "GET", "/stats", timeout=2)["role"]
        except Exception:
            time.sleep(0.2)
    seg = call(A.port, "POST", "/append/events?producer=a&seq=2", rows("a", 2, 100))["seg"]  # the retry, via a
    n = call(A.port, "POST", f"/sql?after={seg}", b"SELECT producer, count(*) AS n FROM events GROUP BY producer ORDER BY producer")
    a.kill(); b.kill()
    ok = joined and a_after == "rejected" and role == "follower" and n == [{"producer": "a", "n": 200}, {"producer": "b", "n": 200}]
    print(f"fence: 2nd node joined as follower={joined}; takeover after {takeover:.1f}s; stale leader's write {a_after}; "
          f"it rejoined as {role}; rows={n} -> {'OK' if ok else 'FAIL'}")
    if not ok:
        sys.exit(1)
    return "a frozen leader is replaced; when it wakes, its write is rejected (fenced) and it rejoins as a follower; nothing lost"


def reader():
    lake = new_lake()
    w = Node(lake, A.port, flush_ms=100).start()
    events_table(A.port)
    r = Node(lake, A.port + 1, reader=True).start()
    lat = []
    for k in range(1, 31):
        t = time.time()
        call(A.port, "POST", f"/append/events?producer=r&seq={k}", rows("r", k, 10))
        while sql(A.port + 1, f"SELECT count(*) AS c FROM events WHERE seq = {k}")[0]["c"] < 10:
            time.sleep(0.02)
        lat.append(time.time() - t)
        time.sleep(random.uniform(0, 0.3))
    w.kill(); r.kill()
    print(f"reader: send -> visible on a separate read-only node: p50 {pct(lat, .5)} ms, p99 {pct(lat, .99)} ms")
    return f"freshness on a separate read-only node: p50 {pct(lat, .5)} ms, p99 {pct(lat, .99)} ms"


def insert():
    lake = new_lake()
    node = Node(lake, A.port, flush_ms=50).start()
    events_table(A.port)
    for s in range(1, 6):
        call(A.port, "POST", f"/append/events?producer=i&seq={s}", rows("i", s, 1000))
    q = b"SELECT seq, count(*) AS n, sum(i) AS total FROM events GROUP BY seq"
    first = call(A.port, "POST", "/insert/summary?job=daily-1", q)
    retry = call(A.port, "POST", "/insert/summary?job=daily-1", q)
    s = sql(A.port, "SELECT count(*) AS groups, sum(n) AS n FROM summary")[0]
    node.kill()
    ok = first == {"rows": 5} and retry == {"duplicate": True} and s == {"groups": 5, "n": 5000}
    print(f"insert: first={first} retry={retry} summary={s} -> {'OK' if ok else 'FAIL'}")
    if not ok:
        sys.exit(1)
    return "bulk INSERT … SELECT writes Parquet directly; a retried job id is applied once"


def serverless():
    """Writes from any machine with the binary, with and without a running node."""
    import pyarrow as pa, pyarrow.parquet as pq
    lake, src = new_lake(), os.path.join(tempfile.mkdtemp(prefix="pondra-src-"), "jan.parquet")
    pq.write_table(pa.table({"id": list(range(1000)), "v": [i % 7 for i in range(1000)]}), src)
    insert = f"INSERT INTO sales SELECT * FROM '{src}'"

    def cli(q, job=None):
        env = {**os.environ, **({"PONDRA_JOB": job} if job else {})}
        return subprocess.Popen([BIN, "sql", "--dir", lake, q], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env)

    def done(p):
        out, err = p.communicate(timeout=300)
        if p.returncode:
            raise RuntimeError(err[-800:])
        return out.strip()

    def timed(q, job=None):
        t = time.time()
        return done(cli(q, job)), round(time.time() - t, 2)

    def leads(node):
        return "pondra leader" in open(node.log).read()

    first, alone_s = timed(insert)  # nobody running: this process records its own files
    once = [timed(insert, job="jan-1")[0], timed(insert, job="jan-1")[0]]  # a retried job counts once
    together = [done(p) for p in [cli(insert) for _ in range(4)]]  # four machines at once, nobody running
    t = time.time()
    node = Node(lake, A.port).start()  # a node starting on the idle lake leads at once
    node_start_s, node_leads = round(time.time() - t, 2), leads(node)
    via_node, via_node_s = timed(insert)  # the files go to the running leader
    node.kill()  # kill -9: its mark in the bucket goes stale within 30 s
    after_kill, after_kill_s = timed(insert)  # (no followers to take over: it waits for the stale mark)
    node2 = Node(lake, A.port + 1).start()
    n = sql(node2.port, "SELECT count(*) AS n, count(DISTINCT id) AS ids FROM sales")[0]
    node2_leads = leads(node2)
    node2.kill()
    ok = n == {"n": 8000, "ids": 1000} and '"duplicate":true' in once[1] and node_leads and node2_leads and node_start_s < 10
    print(json.dumps({"serverless": {"alone_s": alone_s, "retry": once, "four_at_once": together, "node_start_s": node_start_s, "node_leads": node_leads,
                                     "via_node_s": via_node_s, "after_leader_killed_s": after_kill_s, "rows": n, "ok": ok}}))
    if not ok:
        sys.exit(1)
    return (f"INSERT from a process with no server: {alone_s}s alone, 4 at once, retries once; a node on the idle lake leads "
            f"in {node_start_s}s; via a running leader {via_node_s}s; after the leader is killed {after_kill_s}s; {n['n']} rows, no duplicates")


def clients():
    """SQL writes, the Python client, the Postgres protocol, tokens, the bucket inbox and attached
    lakes, all against one small cluster."""
    import asyncio, asyncpg, pandas as pd, polars as pl, psycopg, psycopg2, sqlalchemy as sa
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "python"))
    import pondra
    tokens = {"read_token": "r-tok", "write_token": "w-tok", "admin_token": "a-tok"}
    lake, other = new_lake(), new_lake()
    b = Node(other, A.port + 2, **tokens).start()
    a = Node(lake, A.port, pg=f"127.0.0.1:{A.port + 10}", attach=f"sales={other}", changelog_secs=600, **tokens).start()
    admin, writer, reader = (pondra.connect(f"http://127.0.0.1:{A.port}", token=t) for t in ("a-tok", "w-tok", "r-tok"))
    checks = {}
    # tokens: nothing without one; a read token can't write; a write token can't create tables
    checks["no token -> 401"] = _raises(lambda: pondra.connect(f"http://127.0.0.1:{A.port}").sql("SELECT 1").rows())
    admin.sql("CREATE TABLE users (id BIGINT PRIMARY KEY, name VARCHAR, score DOUBLE, _deleted BOOLEAN)")
    admin.sql("CREATE TABLE events (user VARCHAR, amount BIGINT) WITH (cluster_by = 'user')")
    checks["read token can't write"] = _raises(lambda: reader.sql("INSERT INTO users VALUES (9, 'x', 0, false)"))
    checks["write token can't create"] = _raises(lambda: writer.sql("CREATE TABLE nope (a BIGINT)"))
    # SQL writes and the Python client
    writer.sql("INSERT INTO users VALUES (1, 'ann', 1.0, false), (2, 'bob', 2.0, false), (3, 'cy', 3.0, false)")
    writer.sql("UPDATE users SET score = score + 10 WHERE id = 1")
    writer.sql("DELETE FROM users WHERE id = 2")
    writer.append("events", [{"user": "ann", "amount": 5}])
    writer.append("events", pd.DataFrame({"user": ["bob"], "amount": [7]}))
    writer.append("events", pl.DataFrame({"user": ["cy"], "amount": [9]}))
    checks["users via SQL"] = reader.sql("SELECT id, score FROM users ORDER BY id").rows() == [{"id": 1, "score": 11.0}, {"id": 3, "score": 3.0}]
    checks["pandas / polars / list appends"] = reader.sql("SELECT sum(amount) AS s FROM events").rows() == [{"s": 21}]
    checks["lookup"] = reader.lookup("users", 3)["name"] == "cy"
    feed = list(itertools.islice(reader.watch("users", after=0), 5))
    checks["change feed replay (upserts + delete)"] = len(feed) == 5 and any(r.get("_deleted") for r in feed)
    # Postgres protocol: psycopg 3 (extended, text and binary), psycopg2, SQLAlchemy + pandas, asyncpg
    dsn = f"host=127.0.0.1 port={A.port + 10} dbname=pondra user=writer password=w-tok"
    with psycopg.connect(dsn, autocommit=True) as c:
        c.execute("INSERT INTO users VALUES (%s, %s, %s, false)", (4, "dee", 4.5))
        checks["psycopg 3"] = c.execute("SELECT name FROM users WHERE id = %s", (4,)).fetchone() == ("dee",)
        checks["psycopg 3 binary"] = c.cursor(binary=True).execute("SELECT score FROM users WHERE id = %s", (4,)).fetchone() == (4.5,)
    c2 = psycopg2.connect(dsn); c2.autocommit = True
    cur = c2.cursor(); cur.execute("SELECT count(*) FROM users"); checks["psycopg2"] = cur.fetchone() == (3,)
    eng = sa.create_engine(f"postgresql+psycopg2://writer:w-tok@127.0.0.1:{A.port + 10}/pondra")
    checks["SQLAlchemy + pandas"] = pd.read_sql("SELECT user, amount FROM events ORDER BY user", eng)["amount"].tolist() == [5, 7, 9]
    async def apg():
        conn = await asyncpg.connect(host="127.0.0.1", port=A.port + 10, user="reader", password="r-tok", database="pondra")
        rows = await conn.fetch("SELECT id FROM users WHERE score > $1 ORDER BY id", 4.0); await conn.close()
        return [r["id"] for r in rows]
    checks["asyncpg"] = asyncio.run(apg()) == [1, 4]
    checks["wrong password refused"] = _raises(lambda: psycopg.connect(dsn.replace("w-tok", "nope")))
    # attached lake: write into it through its own leader, join across the two
    admin.sql("CREATE TABLE sales.orders (id BIGINT PRIMARY KEY, user VARCHAR, amount BIGINT, _deleted BOOLEAN)")
    writer.sql("INSERT INTO sales.orders VALUES (1, 'ann', 10, false), (2, 'cy', 20, false)")
    time.sleep(1)
    checks["cross-lake join"] = reader.sql("SELECT u.name, o.amount FROM sales.orders o JOIN users u ON o.user = u.name ORDER BY 1").rows() == [{"name": "ann", "amount": 10}, {"name": "cy", "amount": 20}]
    # the bucket inbox: a machine that can't reach the leader still writes (exactly once)
    env = {**os.environ, "PONDRA_NO_DIRECT": "1", "PONDRA_JOB": "inbox-1"}
    run = lambda: subprocess.run([BIN, "sql", "--dir", lake, "INSERT INTO users VALUES (5, 'eve', 5.0, false)"], capture_output=True, text=True, env=env, timeout=120)
    t = time.time(); first = run(); inbox_s = time.time() - t
    again = run()
    checks["inbox write"] = '"committed":true' in first.stdout and '"duplicate":true' in again.stdout and reader.lookup("users", 5)["name"] == "eve"
    # vector search: an embedding column, nearest by cosine distance (SQL, and a Postgres array parameter);
    # DELETE on a keyed table created without a `_deleted` column
    admin.sql("CREATE TABLE docs (id BIGINT PRIMARY KEY, title VARCHAR, emb FLOAT[])")
    writer.sql("INSERT INTO docs VALUES (1, 'cats', [1.0, 0.0, 0.0]), (2, 'dogs', [0.9, 0.1, 0.0]), (3, 'cars', [0.0, 0.0, 1.0])")
    writer.sql("DELETE FROM docs WHERE id = 1")
    near = "SELECT id FROM docs ORDER BY cosine_distance(emb, {}) LIMIT 2"
    with psycopg.connect(dsn, autocommit=True) as c:
        by_pg = [r[0] for r in c.execute(near.format("%s"), ([1.0, 0.05, 0.0],)).fetchall()]
    checks["vector search (SQL + Postgres)"] = [r["id"] for r in reader.sql(near.format("[1.0, 0.05, 0.0]")).rows()] == by_pg == [2, 3]
    checks["SQL can't touch the node's files"] = _raises(lambda: reader.sql("COPY (SELECT 1) TO '/tmp/pondra-copy.csv'")) and \
        _raises(lambda: reader.sql("CREATE EXTERNAL TABLE e STORED AS CSV LOCATION '/etc/hosts'"))
    # MCP: an agent lists tables, queries, writes (with a write token only) and reads the change feed
    def mcp(token, method, params=None):
        body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params or {}}).encode()
        req = urllib.request.Request(f"http://127.0.0.1:{A.port}/mcp", data=body, headers={"content-type": "application/json", "authorization": f"Bearer {token}"})
        return json.loads(urllib.request.urlopen(req).read())["result"]
    tool = lambda token, name, **args: (lambda r: (json.loads(r["content"][0]["text"]) if not r["isError"] else None))(mcp(token, "tools/call", {"name": name, "arguments": args}))
    init = mcp("r-tok", "initialize", {"protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "harness", "version": "1"}})
    names = [t["name"] for t in mcp("r-tok", "tools/list")["tools"]]
    tables = {t["table"] for t in tool("r-tok", "list_tables")["tables"]}
    wrote = tool("w-tok", "write", sql="INSERT INTO docs VALUES (4, 'trucks', [0.0, 0.1, 0.9])", job="mcp-1")
    feed = tool("r-tok", "changes", table="docs", after=0)
    checks["MCP"] = init["serverInfo"]["name"] == "pondra" and names == ["list_tables", "query", "write", "changes"] and {"users", "docs", "sales.orders"} <= tables \
        and tool("r-tok", "query", sql="SELECT count(*) AS n FROM docs")["rows"] == [{"n": 3}] and tool("r-tok", "write", sql="DELETE FROM docs") is None \
        and wrote == {"rows": 1} and tool("w-tok", "write", sql="INSERT INTO docs VALUES (4, 'trucks', [0.0, 0.1, 0.9])", job="mcp-1") == {"duplicate": True} \
        and len(feed["rows"]) == 5 and any(r.get("_deleted") for r in feed["rows"]) and feed["position"] > 0
    a.kill(); b.kill()
    ok = all(checks.values())
    print(json.dumps({"clients": checks, "inbox_s": round(inbox_s, 2), "ok": ok}, indent=1))
    if not ok:
        sys.exit(1)
    return f"SQL writes, Python client, Postgres (4 drivers), tokens, change-feed replay, attached lake, inbox ({inbox_s:.1f}s), vector search, MCP, no file access from SQL: all {len(checks)} checks pass"


def kafka():
    """Kafka clients against a node: librdkafka (idempotent, every codec) and kafka-python
    producers, exactly-once retries, Debezium change events and tombstones, raw values, a view
    fed by Kafka, consumers reading the log back (deletes as tombstones), SASL/PLAIN tokens."""
    import confluent_kafka as ck, kafka as kp, struct, socket
    from kafka.record.default_records import DefaultRecordBatchBuilder
    tokens = {"read_token": "r-tok", "write_token": "w-tok", "admin_token": "a-tok"}
    lake, kport = new_lake(), A.port + 20
    node = Node(lake, A.port, kafka=f"127.0.0.1:{kport}", changelog_secs=600, **tokens).start()
    sql_ = lambda q, t="a-tok": call(A.port, "POST", "/sql", q.encode(), headers={"authorization": f"Bearer {t}"})
    sql_("CREATE TABLE events (user VARCHAR, amount BIGINT, _key VARCHAR, _timestamp TIMESTAMP)")
    sql_("CREATE TABLE users (id BIGINT PRIMARY KEY, name VARCHAR, score DOUBLE)")
    sql_("CREATE TABLE lines (_key VARCHAR, _value VARCHAR, _timestamp TIMESTAMP)")
    call(A.port, "POST", "/views/per_user", b"SELECT user, count(*) AS n, sum(amount) AS total FROM events GROUP BY user", headers={"authorization": "Bearer a-tok"})
    sasl = lambda user, pw: {"bootstrap.servers": f"127.0.0.1:{kport}", "security.protocol": "SASL_PLAINTEXT", "sasl.mechanisms": "PLAIN", "sasl.username": user, "sasl.password": pw}
    checks, n = {}, 20_000
    # librdkafka, idempotent, each compression codec
    t = time.time()
    for i, codec in enumerate(["none", "gzip", "snappy", "lz4", "zstd"]):
        p = ck.Producer({**sasl("writer", "w-tok"), "enable.idempotence": True, "compression.type": codec, "linger.ms": 5})
        for j in range(n // 5):
            p.produce("events", key=f"u{j % 50}", value=json.dumps({"user": f"u{j % 50}", "amount": 1}))
            p.poll(0)
        assert p.flush(60) == 0
    librdkafka_s = time.time() - t
    # kafka-python, not idempotent, gzip
    kp_prod = kp.KafkaProducer(bootstrap_servers=f"127.0.0.1:{kport}", security_protocol="SASL_PLAINTEXT", sasl_mechanism="PLAIN",
                               sasl_plain_username="writer", sasl_plain_password="w-tok", compression_type="gzip", value_serializer=lambda v: json.dumps(v).encode())
    for j in range(1000):
        kp_prod.send("events", {"user": "kp", "amount": 2})
    kp_prod.flush(); kp_prod.close()
    got = sql_("SELECT count(*) AS n, sum(amount) AS s, count(_key) AS keys, min(_timestamp) IS NOT NULL AS ts FROM events")[0]
    checks["librdkafka (5 codecs, idempotent) + kafka-python"] = got == {"n": n + 1000, "s": n + 2000, "keys": n, "ts": True}
    view = sql_("SELECT sum(n) AS n, sum(total) AS s FROM per_user")[0]
    checks["a view fed by Kafka"] = view == {"n": n + 1000, "s": n + 2000}
    # exactly-once: the same idempotent batch twice is applied once; one that skips ahead is refused
    def raw(frames, user="writer", pw="w-tok"):
        s = socket.create_connection(("127.0.0.1", kport))
        def req(key, ver, body):
            head = struct.pack(">hhih", key, ver, 7, 1) + b"t"
            s.sendall(struct.pack(">i", len(head) + len(body)) + head + body)
            size = struct.unpack(">i", s.recv(4))[0]
            data = b""
            while len(data) < size:
                data += s.recv(size - len(data))
            return data[4:]
        mech = b"PLAIN"
        req(17, 1, struct.pack(">h", len(mech)) + mech)
        auth = b"\0" + user.encode() + b"\0" + pw.encode()
        out = [req(36, 0, struct.pack(">i", len(auth)) + auth)[:2]]
        for key, ver, body in frames:
            out.append(req(key, ver, body))
        s.close()
        return out
    def batch(seq, rows, producer=424242):
        b = DefaultRecordBatchBuilder(magic=2, compression_type=0, is_transactional=False, producer_id=producer, producer_epoch=0, base_sequence=seq, batch_size=1 << 20)
        for k, r in enumerate(rows):
            b.append(k, timestamp=int(time.time() * 1000), key=None, value=json.dumps(r).encode(), headers=[])
        return bytes(b.build())
    def produce(topic, records):
        body = struct.pack(">hhi", -1, -1, 5000) + struct.pack(">i", 1) + struct.pack(">h", len(topic)) + topic.encode()
        return (0, 3, body + struct.pack(">i", 1) + struct.pack(">ii", 0, len(records)) + records)
    error = lambda resp: struct.unpack(">h", resp[4 + 2 + len("users") + 4 + 4:4 + 2 + len("users") + 4 + 4 + 2])[0]
    b0 = batch(0, [{"id": 1, "name": "ann", "score": 1.0}, {"id": 2, "name": "bob", "score": 2.0}])
    auth, first, again, ahead = raw([produce("users", b0), produce("users", b0), produce("users", batch(7, [{"id": 9, "name": "x", "score": 0}]))])
    checks["exactly-once retries (idempotent producer)"] = (auth, error(first), error(again), error(ahead)) == (b"\0\0", 0, 0, 45) and \
        sql_("SELECT count(*) AS n FROM users")[0]["n"] == 2
    # Debezium change events (with Kafka Connect's schema wrapper) and a tombstone
    dbz = lambda op, before, after: json.dumps({"schema": {}, "payload": {"op": op, "before": before, "after": after, "source": {}}})
    p = ck.Producer(sasl("writer", "w-tok"))
    p.produce("users", key=json.dumps({"id": 3}), value=dbz("c", None, {"id": 3, "name": "cy", "score": 3.0}))
    p.produce("users", key=json.dumps({"id": 1}), value=dbz("u", {"id": 1}, {"id": 1, "name": "ann", "score": 11.0}))
    p.produce("users", key=json.dumps({"id": 2}), value=dbz("d", {"id": 2, "name": "bob", "score": 2.0}, None))
    p.produce("users", key=json.dumps({"id": 2}), value=None)  # Debezium's tombstone after a delete
    p.produce("users", key=b"3", value=json.dumps({"id": 4, "name": "dee", "score": 4.0}))
    p.produce("users", key=b"4", value=None)  # a tombstone with a plain key
    p.produce("lines", key=b"k1", value=b"plain text, not JSON")
    p.produce("lines", key=b"k2", value=b'{"event": "click", "n": 3}')
    assert p.flush(30) == 0
    checks["Debezium events and tombstones"] = sql_("SELECT id, name, score FROM users ORDER BY id") == [{"id": 1, "name": "ann", "score": 11.0}, {"id": 3, "name": "cy", "score": 3.0}]
    checks["raw values (_value), queried as JSON"] = sql_("SELECT _key, _value FROM lines ORDER BY _key")[0] == {"_key": "k1", "_value": "plain text, not JSON"} and \
        sql_("SELECT _value->>'event' AS e, json_get_int(_value, 'n') AS n FROM lines WHERE _key = 'k2'") == [{"e": "click", "n": 3}]
    # consumers: kafka-python and librdkafka read the log back from the beginning
    tp = kp.TopicPartition("users", 0)
    c = kp.KafkaConsumer(bootstrap_servers=f"127.0.0.1:{kport}", security_protocol="SASL_PLAINTEXT", sasl_mechanism="PLAIN",
                         sasl_plain_username="reader", sasl_plain_password="r-tok", group_id=None, enable_auto_commit=False, consumer_timeout_ms=3000)
    c.assign([tp]); c.seek_to_beginning(tp)
    msgs = list(c); c.close()
    tombstones = [json.loads(m.key) for m in msgs if m.value is None]
    offsets = [m.offset for m in msgs]
    checks["kafka-python consumer (upserts, deletes as tombstones)"] = len(msgs) == 8 and {"id": 2} in tombstones and {"id": 4} in tombstones and offsets == sorted(offsets)
    # an offset from before the oldest segment kept (0 always is) reads from there, with no reset
    # to fall back on: "out of range" once had librdkafka retrying a stale earliest offset forever
    c = kp.KafkaConsumer(bootstrap_servers=f"127.0.0.1:{kport}", security_protocol="SASL_PLAINTEXT", sasl_mechanism="PLAIN", auto_offset_reset="none",
                         sasl_plain_username="reader", sasl_plain_password="r-tok", group_id=None, enable_auto_commit=False, consumer_timeout_ms=3000)
    c.assign([tp]); c.seek(tp, 0)
    try:
        early = len(list(c))
    except Exception as e:
        early = repr(e)
    c.close()
    checks["an offset before the oldest segment reads from there"] = early == 8
    cc = ck.Consumer({**sasl("reader", "r-tok"), "group.id": "pondra-test", "enable.auto.commit": False})
    cc.assign([ck.TopicPartition("events", 0, ck.OFFSET_BEGINNING)])
    count, deadline = 0, time.time() + 30
    while count < n + 1000 and time.time() < deadline:
        for m in cc.consume(1000, 1.0):
            count += m.error() is None
    cc.close()
    checks["librdkafka consumer"] = count == n + 1000
    # consumer groups, coordinated by the leader: a member commits and leaves; the next one, which
    # starts from a follower node, resumes exactly there. Two live members: one holds the partition.
    follower = Node(lake, A.port + 1, kafka=f"127.0.0.1:{kport + 1}", **tokens).start()
    time.sleep(1)
    kc = lambda port, group: kp.KafkaConsumer("events", bootstrap_servers=f"127.0.0.1:{port}", group_id=group, auto_offset_reset="earliest", enable_auto_commit=False,
                                               security_protocol="SASL_PLAINTEXT", sasl_mechanism="PLAIN", sasl_plain_username="reader", sasl_plain_password="r-tok")
    first, rest, deadline = [], [], time.time() + 60
    c1 = kc(kport, "g1")
    while len(first) < 10_000 and time.time() < deadline:
        for recs in c1.poll(timeout_ms=500, max_records=1000).values():
            first.extend(r.offset for r in recs)
    c1.commit(); c1.close()
    c2 = kc(kport + 1, "g1")
    while len(first) + len(rest) < n + 1000 and time.time() < deadline:
        for recs in c2.poll(timeout_ms=500).values():
            rest.extend(r.offset for r in recs)
    c2.close()
    both = first + rest
    live = [ck.Consumer({**sasl("reader", "r-tok"), "group.id": "g2", "auto.offset.reset": "earliest", "bootstrap.servers": f"127.0.0.1:{kport + k}"}) for k in (0, 1)]
    [c.subscribe(["events"]) for c in live]
    seen, t_end = set(), time.time() + 45  # (a rebalance can take a few heartbeats)
    while time.time() < t_end and not (len(seen) == n + 1000 and sorted(len(c.assignment()) for c in live) == [0, 1]):
        for c in live:
            seen.update(m.offset() for m in c.consume(1000, 0.2) if m.error() is None)
    holders = [len(c.assignment()) for c in live]
    [c.close() for c in live]
    checks["consumer groups (commit, hand-over via a follower, one holder)"] = len(set(both)) == len(both) == n + 1000 and min(rest) > max(first) and \
        sorted(holders) == [0, 1] and len(seen) == n + 1000
    if not checks["consumer groups (commit, hand-over via a follower, one holder)"]:
        print("groups:", len(first), len(rest), len(set(both)), min(rest, default=None), max(first, default=None), holders, len(seen))
    follower.kill()
    # tokens: a wrong password is refused; a read token can't produce
    failed = []
    for user, pw in [("writer", "nope"), ("reader", "r-tok")]:
        p = ck.Producer({**sasl(user, pw), "message.timeout.ms": 3000})
        p.produce("events", value=b"{}", on_delivery=lambda err, msg: failed.append(err is not None))
        p.flush(6)
    checks["wrong password / read token refused"] = failed == [True, True] and sql_("SELECT count(*) AS n FROM events")[0]["n"] == n + 1000
    node.kill()
    ok = all(checks.values())
    print(json.dumps({"kafka": checks, "librdkafka_20k_events_s": round(librdkafka_s, 2), "ok": ok}, indent=1))
    if not ok:
        sys.exit(1)
    return f"Kafka: librdkafka (5 codecs, idempotent) and kafka-python producers, exactly-once retries, Debezium, tombstones, raw values as JSON, consumers, consumer groups, SASL tokens: all {len(checks)} checks pass"


def alter():
    """ALTER TABLE … ADD COLUMN under load: producers keep writing the old columns while a column
    is added; old rows (log and Parquet) read it as null, new ones carry it; keyed tables, views,
    bulk INSERTs, Arrow appends and the Delta/Iceberg copies all follow."""
    import io, pyarrow as pa
    lake = new_lake()
    node = Node(lake, A.port, tier_secs=0.25, publish="delta,iceberg").start()
    q = lambda s: sql(A.port, s)
    q("CREATE TABLE events (user VARCHAR, amount BIGINT)")
    q("CREATE TABLE users (id BIGINT PRIMARY KEY, name VARCHAR)")
    call(A.port, "POST", "/views/per_user", b"SELECT user, count(*) AS n, sum(amount) AS total FROM events GROUP BY user")
    stop, sent = threading.Event(), [0]
    def produce():  # an old producer: never sends the new column
        seq = 0
        while not stop.is_set():
            seq += 1
            call(A.port, "POST", f"/append/events?producer=old&seq={seq}", "".join(json.dumps({"user": f"u{i % 10}", "amount": 1}) + "\n" for i in range(100)).encode())
            sent[0] += 100
    t = threading.Thread(target=produce); t.start()
    q("INSERT INTO users VALUES (1, 'ann'), (2, 'bob')")
    time.sleep(1.5)  # some rows tiered to Parquet, some still in the log
    q("ALTER TABLE events ADD COLUMN country VARCHAR")
    q("ALTER TABLE users ADD COLUMN email VARCHAR")
    again = q("ALTER TABLE users ADD COLUMN IF NOT EXISTS email VARCHAR")
    call(A.port, "POST", "/append/events?producer=new&seq=1", b'{"user": "u1", "amount": 5, "country": "UZ"}\n')
    q("INSERT INTO events VALUES ('u2', 7)")  # (bulk, the old columns only)
    b = pa.record_batch([pa.array(["u3"]), pa.array([9], pa.int64())], names=["user", "amount"])
    buf = io.BytesIO()
    with pa.ipc.new_stream(buf, b.schema) as w:
        w.write_batch(b)
    call(A.port, "POST", "/append/events?producer=arrow&seq=1", buf.getvalue(), headers={"content-type": "application/vnd.apache.arrow.stream"})
    q("UPDATE users SET email = 'ann@x.io' WHERE id = 1")
    time.sleep(1); stop.set(); t.join(); time.sleep(1.5)
    call(A.port, "POST", "/tier")
    time.sleep(1)
    total = sent[0] + 5 + 7 + 9
    checks = {
        "old and new rows": q("SELECT count(*) AS n, sum(amount) AS s, count(country) AS c FROM events")[0] == {"n": sent[0] + 3, "s": total, "c": 1},
        "the new column": q("SELECT country FROM events WHERE country IS NOT NULL") == [{"country": "UZ"}],
        "a view over the table": q("SELECT sum(n) AS n, sum(total) AS s FROM per_user")[0] == {"n": sent[0] + 3, "s": total},
        "a keyed table": q("SELECT id, name, email FROM users ORDER BY id") == [{"id": 1, "name": "ann", "email": "ann@x.io"}, {"id": 2, "name": "bob"}],
        "lookup": call(A.port, "GET", "/lookup/users/1")[0].get("email") == "ann@x.io",
        "IF NOT EXISTS": again == {"table": "users", "unchanged": True},
        "a clash is refused": _raises(lambda: q("ALTER TABLE users ADD COLUMN name VARCHAR")),
    }
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    import open_check
    theirs = {**open_check.readers(lake, "events"), **{f"iceberg/{k}": v for k, v in open_check.iceberg_readers(lake, "events").items()}}
    duck = open_check.duck(lake)
    countries = [duck.execute(f"SELECT count(country) FROM {scan}").fetchone()[0] for scan in (f"delta_scan('{lake}/data/events')", f"iceberg_scan('{open_check.iceberg_metadata(lake, 'events')}')")]
    checks["Delta and Iceberg readers"] = all(v == sent[0] + 3 for v in theirs.values()) and countries == [1, 1]
    node.kill()
    ok = all(checks.values())
    print(json.dumps({"alter": checks, "rows_written_during": sent[0], "outside_readers": theirs, "ok": ok}, indent=1))
    if not ok:
        sys.exit(1)
    return f"ALTER TABLE ADD COLUMN under load ({sent[0]:,} rows written meanwhile): old rows null, new ones set, keyed table, view, bulk and Arrow appends, 6 outside readers: all {len(checks)} checks pass"


def until(f, want, secs=60):
    """f() once it returns `want` (polled; on object storage emission takes a few round trips), or
    what it returns when the time is up."""
    deadline = time.time() + secs
    while (got := f()) != want and time.time() < deadline:
        time.sleep(0.3)
    return got


def windows():
    """Event-time windows that emit once: 1-minute windows, 10 s allowed lateness. The watermark
    is the newest event time less the lateness, so a window closes as soon as the data has moved
    10 s past its end — not when a later window starts. Rows up to 10 s out of order still count;
    each window reaches `{view}_final` once, final; a later row updates the view but not what was
    emitted; a restart of the leader emits nothing twice."""
    import datetime
    lake = new_lake()
    node = Node(lake, A.port, tier_secs=1).start()
    q = lambda s: sql(A.port, s)
    q("CREATE TABLE clicks (user VARCHAR, ts TIMESTAMP)")
    per_minute = b"SELECT date_bin(INTERVAL '1 minute', ts) AS w, user, count(*) AS n FROM clicks GROUP BY 1, 2"
    for _ in range(2):  # (asked twice, the same: the second changes nothing)
        call(A.port, "POST", "/views/per_minute?window=w&size_secs=60&lateness_secs=10", per_minute)
    other = [_raises(lambda o=o: call(A.port, "POST", f"/views/per_minute{o}", per_minute)) for o in ("", "?window=w&size_secs=30&lateness_secs=10")]
    base = 1_790_000_000 // 60 * 60
    iso = lambda s: datetime.datetime.fromtimestamp(s, datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    seq = 0
    def send(rows):
        nonlocal seq
        seq += 1
        call(A.port, "POST", f"/append/clicks?producer=p&seq={seq}", "".join(json.dumps(r) + "\n" for r in rows).encode())
    for m in range(5):  # minutes 0-4, in order: a 20 clicks and b 10 in each, in its first 30 s
        send([{"user": "a" if i % 3 else "b", "ts": iso(base + m * 60 + i)} for i in range(30)])
    minutes = lambda rows: sorted({(datetime.datetime.fromisoformat(r["w"]).replace(tzinfo=datetime.timezone.utc).timestamp() - base) // 60 for r in rows})
    emitted = lambda: minutes(q("SELECT w FROM per_minute_final"))
    first = until(emitted, [0, 1, 2, 3])  # newest 4:29, watermark 4:19: minutes 0-3 have ended
    send([{"user": "a", "ts": iso(base + 4 * 60 + 55)}])  # the watermark moves to 4:45…
    send([{"user": "a", "ts": iso(base + 4 * 60 + 40)}])  # …and a row 15 s out of order still counts: minute 4 is open
    send([{"user": "a", "ts": iso(base + 5)}])  # late, for minute 0 (already final)
    time.sleep(3)
    middle = emitted()
    send([{"user": "b", "ts": iso(base + 5 * 60 + 10)}])  # 5:10: the watermark reaches 5:00, the end of minute 4
    until(emitted, [0, 1, 2, 3, 4])
    node.kill()
    node = Node(lake, A.port, tier_secs=1).start()  # (a new leader: nothing emitted twice)
    time.sleep(3)
    final = q("SELECT w, user, n FROM per_minute_final ORDER BY w, user")
    view0 = q(f"SELECT n FROM per_minute WHERE user = 'a' AND w = '{iso(base)}'")
    node.kill()
    want = lambda r: (22 if (minutes([r]) == [4]) else 20) if r["user"] == "a" else 10
    checks = {
        "a window closes when event time passes its end, not when the next one starts": first == [0, 1, 2, 3] and middle == [0, 1, 2, 3],
        "each window once, final, with rows up to the lateness out of order": minutes(final) == [0, 1, 2, 3, 4] and len(final) == 10 and all(r["n"] == want(r) for r in final),
        "late row: in the view, not re-emitted": view0 == [{"n": 21}],
        "the same view asked for again changes nothing; with other options, it is refused": all(other),
    }
    ok = all(checks.values())
    print(json.dumps({"windows": checks, "ok": ok}, indent=1))
    if not ok:
        print(first, middle, final, view0, other)
        sys.exit(1)
    return "event-time windows: closed by the data's own time, each emitted once, final; rows out of order within the lateness count; late rows update the view only; a leader restart emits nothing twice"


def sessions():
    """Session windows: each user's clicks with no 30 s gap between them are one session, emitted
    once when the watermark (newest event time less 5 s) passes its last click plus 30 s. A session
    that spans many rounds is emitted once, whole; a row out of order within the lateness joins its
    session; a late row inside a session already emitted is left out; a leader restart emits
    nothing twice."""
    import datetime
    lake = new_lake()
    node = Node(lake, A.port, tier_secs=1).start()
    q = lambda s: sql(A.port, s)
    q("CREATE TABLE visits (user VARCHAR, ts TIMESTAMP, amount BIGINT)")
    call(A.port, "POST", "/views/user_sessions?session=ts&gap_secs=30&lateness_secs=5",
         b"SELECT user, count(*) AS n, sum(amount) AS spent FROM visits GROUP BY user")
    base = 1_790_000_000 // 60 * 60
    iso = lambda s: datetime.datetime.fromtimestamp(s, datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    seq = 0
    def send(*rows):
        nonlocal seq
        seq += 1
        call(A.port, "POST", f"/append/visits?producer=p&seq={seq}", "".join(json.dumps({"user": u, "ts": iso(base + t), "amount": 2}) + "\n" for u, t in rows).encode())
        time.sleep(1.5)
    got = lambda: sorted((r["user"], int(datetime.datetime.fromisoformat(r["session_start"]).replace(tzinfo=datetime.timezone.utc).timestamp()) - base,
                          int(datetime.datetime.fromisoformat(r["session_end"]).replace(tzinfo=datetime.timezone.utc).timestamp()) - base, r["n"], r["spent"])
                         for r in q("SELECT * FROM user_sessions"))
    send(("a", 0), ("a", 10), ("a", 20), ("b", 0), ("b", 25))  # watermark 20: nothing has closed
    send(("b", 50), ("b", 60))  # watermark 55: a's first session (0-20, ends 50) has
    first = until(got, [("a", 0, 50, 3, 6)])
    send(("a", 40), ("a", 97), ("b", 75), ("b", 100))  # a at 40: late, inside a session already emitted
    send(("a", 110), ("a", 100), ("b", 125))  # a at 100 is out of order, within the lateness
    send(("c", 300))  # watermark 295: a's second session and b's one long one close
    want = [("a", 0, 50, 3, 6), ("a", 97, 140, 3, 6), ("b", 0, 155, 7, 14)]
    until(got, want)
    node.kill()
    node = Node(lake, A.port, tier_secs=1).start()  # (a new leader: nothing emitted twice)
    time.sleep(3)
    final = got()
    node.kill()
    checks = {
        "a session closes when the watermark passes its last row plus the gap": first == [("a", 0, 50, 3, 6)],
        "each session once, whole, with rows out of order within the lateness": final == want,
    }
    ok = all(checks.values())
    print(json.dumps({"sessions": checks, "ok": ok}, indent=1))
    if not ok:
        print(first, final)
        sys.exit(1)
    return "session windows: each closed by event time, emitted once, whole; out-of-order rows within the lateness join their session; late rows inside an emitted session are left out; a leader restart emits nothing twice"


def asof():
    """Point-in-time joins over a stream: an inline view gives each trade the price its symbol had
    at the trade's own time (`ASOF JOIN … MATCH_CONDITION (t.ts >= p.ts)`), whatever the price is
    when the trade arrives: a trade that arrives after a newer price still gets the one of its
    time, and one before any price of its symbol gets NULL. The same query run ad hoc agrees (over
    HTTP, and over Postgres's extended protocol), and what can't be an as-of join is refused."""
    import datetime
    import psycopg
    lake = new_lake()
    node = Node(lake, A.port, tier_secs=1, pg=f"127.0.0.1:{A.port + 10}").start()
    q = lambda s: sql(A.port, s)
    q("CREATE TABLE prices (sym VARCHAR, ts TIMESTAMP, price DOUBLE, venue VARCHAR)")
    q("CREATE TABLE trades (id BIGINT, sym VARCHAR, ts TIMESTAMP, qty BIGINT)")
    # (`venue`: a string read from the looked-up table's files, where strings are views)
    view = "SELECT t.id, t.sym, t.qty, p.price, t.qty * p.price AS value, p.venue FROM trades t ASOF JOIN prices p MATCH_CONDITION (t.ts >= p.ts) ON t.sym = p.sym"
    call(A.port, "POST", "/views/priced", view.encode())
    base = 1_790_000_000
    iso = lambda s: datetime.datetime.fromtimestamp(base + s, datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    seq = itertools.count(1)
    send = lambda table, rows: call(A.port, "POST", f"/append/{table}?producer={table}&seq={next(seq)}", "".join(json.dumps(r) + "\n" for r in rows).encode())
    send("prices", [{"sym": "A", "ts": iso(0), "price": 10.0, "venue": "x"}, {"sym": "A", "ts": iso(60), "price": 11.0, "venue": "y"}, {"sym": "A", "ts": iso(120), "price": 12.0, "venue": "x"}, {"sym": "B", "ts": iso(30), "price": 100.0, "venue": "z"}])
    q("INSERT INTO prices VALUES ('D', '2026-01-01T00:00:00', 1.0, 'w')")  # (and a file of them, not only the log)
    send("trades", [{"id": 1, "sym": "A", "ts": iso(30), "qty": 1}, {"id": 2, "sym": "A", "ts": iso(90), "qty": 2}, {"id": 3, "sym": "B", "ts": iso(10), "qty": 1},
                    {"id": 4, "sym": "B", "ts": iso(45), "qty": 3}, {"id": 5, "sym": "C", "ts": iso(50), "qty": 1}])
    send("prices", [{"sym": "A", "ts": iso(200), "price": 13.0, "venue": "y"}])
    send("trades", [{"id": 6, "sym": "A", "ts": iso(70), "qty": 1}, {"id": 7, "sym": "A", "ts": iso(250), "qty": 1}])  # 6 arrives late: 11, not 13
    time.sleep(1.5)
    want = {1: 10.0, 2: 11.0, 3: None, 4: 100.0, 5: None, 6: 11.0, 7: 13.0}
    derived = {r["id"]: r.get("price") for r in q("SELECT id, price FROM priced")}
    values = {r["id"]: r.get("value") for r in q("SELECT id, value FROM priced")}
    venues = {r["id"]: r.get("venue") for r in q("SELECT id, venue FROM priced")}
    adhoc = {r["id"]: r.get("price") for r in q(view + " ORDER BY t.id")}
    with psycopg.connect(f"host=127.0.0.1 port={A.port + 10} user=pondra dbname=pondra", autocommit=True) as c:  # (the extended protocol: described, then run)
        postgres = dict(c.execute("SELECT t.id, p.price FROM trades t ASOF JOIN prices p MATCH_CONDITION (t.ts >= p.ts) ON t.sym = p.sym WHERE t.qty >= %s ORDER BY t.id", (0,)).fetchall())
    refused = [_raises(lambda s=s: q(s)) for s in (
        "SELECT * FROM trades t ASOF JOIN prices p MATCH_CONDITION (t.ts = p.ts) ON t.sym = p.sym",
        "SELECT * FROM trades t ASOF JOIN prices p MATCH_CONDITION (t.ts >= p.ts) USING (sym)",
        "SELECT * FROM trades t ASOF JOIN prices p MATCH_CONDITION (t.ts >= p.ts) ON t.sym = p.sym AND t.qty > p.price")]
    node.kill()
    checks = {
        "each trade priced as of its own time, in the stream": derived == want and values[2] == 22.0 and values[3] is None and venues == {1: "x", 2: "y", 3: None, 4: "z", 5: None, 6: "y", 7: "y"},
        "the same query ad hoc agrees, over HTTP and Postgres (described, then run)": adhoc == want and postgres == want,
        "what isn't an as-of join is refused": all(refused),
    }
    ok = all(checks.values())
    print(json.dumps({"asof": checks, "ok": ok}, indent=1))
    if not ok:
        print(derived, values, adhoc, postgres, refused)
        sys.exit(1)
    return "point-in-time joins: each streamed trade priced as of its own time, late ones too; ad hoc queries agree; non-as-of conditions refused"


def sums():
    """sum(DOUBLE) is the true sum rounded once, whatever order its rows are added in (`fsum.rs`):
    equal to Python's math.fsum, grouped or not, over files and the log tail, on every node. With
    DataFusion's own sum, [1e16, 1, -1e16] added up to 0, and TPC-H q15, which compares a sum with
    the max of the same sums, found its row only some of the time."""
    import math
    lake = new_lake()
    nodes = [Node(lake, A.port + i, tier_secs=0.25).start() for i in range(3)]
    q = lambda s, p=A.port: sql(p, s)
    q("CREATE TABLE m (k BIGINT, i BIGINT, v DOUBLE, d DECIMAL(12, 2))")
    rnd, want, rows = random.Random(7), {}, []
    for n in range(20_000):  # magnitudes 1e-3 to 1e12: a plain running sum depends on the order
        k, v = n % 40, rnd.uniform(-1, 1) * 10 ** rnd.randint(-3, 12)
        rows.append({"k": k, "i": n, "v": v, "d": f"{n % 1000}.25"})
        want.setdefault(k, []).append(v)
    for b in range(0, len(rows), 2_500):  # in batches: some become Parquet files, some stay in the log
        call(A.port, "POST", f"/append/m?producer=p&seq={b + 1}", "".join(json.dumps(r) + "\n" for r in rows[b:b + 2_500]).encode())
        time.sleep(0.2)
    q("CREATE TABLE edge (k BIGINT, v DOUBLE)")
    edge_rows = [(1, 1e16), (1, 1.0), (1, -1e16), (2, 1e308), (2, 1e308), (2, -1e308), (3, None)]
    call(A.port, "POST", "/append/edge?producer=e&seq=1", "".join(json.dumps({"k": k, "v": v}) + "\n" for k, v in edge_rows).encode())
    total = math.fsum(v for vs in want.values() for v in vs)
    every = [(q("SELECT sum(v) AS s FROM m", n.port)[0]["s"], {r["k"]: r["s"] for r in q("SELECT k, sum(v) AS s FROM m GROUP BY k", n.port)})
             for n in nodes for _ in range(3)]
    over = {r["k"]: r["s"] for r in q("SELECT DISTINCT k, sum(v) OVER (PARTITION BY k) AS s FROM m")}
    edge = {r["k"]: r.get("s") for r in q("SELECT k, sum(v) AS s FROM edge WHERE k = 1 OR k = 3 GROUP BY k")}
    big = q("SELECT isnan(sum(v)) AS nan, sum(v) > CAST('1e307' AS DOUBLE) AS inf FROM edge WHERE k = 2")[0]
    sliding = q("SELECT i, v, sum(v) OVER (ORDER BY i ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS s FROM m WHERE k = 0 ORDER BY i")
    types = q("SELECT arrow_typeof(sum(i)) AS i, arrow_typeof(sum(d)) AS d, arrow_typeof(sum(v)) AS v, arrow_typeof(sum(DISTINCT v)) AS dv, "
              "sum(v) FILTER (WHERE k = 3) AS f, sum(v) FILTER (WHERE k < 0) AS none FROM m")[0]
    [n.kill() for n in nodes]
    checks = {
        "sum(DOUBLE) == math.fsum, whole and per group, on every node, every time": all(s == total and g == {k: math.fsum(vs) for k, vs in want.items()} for s, g in every),
        "and in a window over each group": over == {k: math.fsum(vs) for k, vs in want.items()},
        "[1e16, 1, -1e16] adds up to 1; all NULLs to NULL; overflow to infinity, not NaN": edge == {1: 1.0, 3: None} and big == {"nan": False, "inf": True},
        "a sliding window takes values back out": all(math.isclose(r["s"], math.fsum(x["v"] for x in sliding[max(0, j - 2):j + 1]), rel_tol=1e-9, abs_tol=1e-6) for j, r in enumerate(sliding)),
        "integers, decimals and DISTINCT keep DataFusion's sum; FILTER works": types["i"] == "Int64" and types["d"].startswith("Decimal128(22, 2)") and types["v"] == types["dv"] == "Float64"
                                                                              and types["f"] == math.fsum(want[3]) and types.get("none") is None,
    }
    ok = all(checks.values())
    print(json.dumps({"sums": checks, "ok": ok}, indent=1))
    if not ok:
        print(total, every[0][0], edge, big, types)
        sys.exit(1)
    return "sum(DOUBLE): the true sum rounded once in any order (== math.fsum), grouped, windowed, on every node; NULLs, overflow, sliding windows and other types as before"


def schemas():
    """A lake is a database (ADR-019): tables live in schemas — `public` unless one is named —
    and other lakes attach as catalogs, so a table is `t`, `schema.t` or `lake.schema.t`.
    CREATE/DROP SCHEMA, DROP TABLE, CREATE TABLE … AS, CREATE [OR REPLACE] VIEW (a stored query)
    and CREATE MATERIALIZED VIEW in SQL, sent to any node (a follower hands them to the leader);
    ATTACH 'dir' AS name and DETACH, kept in the lake for every node.
    A query over a view spreads with the view's tables sliced too; a dropped table's rows never
    come back with a new table of its name; names are checked; Postgres, Flight SQL and the Iceberg
    REST catalog list the schemas."""
    import psycopg, adbc_driver_flightsql.dbapi as adbc, pyarrow as pa
    lake, other = new_lake(), new_lake()
    b = Node(other, A.port + 5).start()
    for s in ("CREATE TABLE sales (id BIGINT, amount DOUBLE)", "INSERT INTO sales VALUES (1, 10.0), (2, 20.0)",
              "CREATE SCHEMA eu", "CREATE TABLE eu.sales (id BIGINT, amount DOUBLE)", "INSERT INTO eu.sales VALUES (1, 70.0)"):
        sql(A.port + 5, s)
    nodes = [Node(lake, A.port + i, attach=f"Other={other}", tier_secs=600, **({"pg": f"127.0.0.1:{A.port + 10}", "flight": f"127.0.0.1:{A.port + 30}"} if i == 0 else {})).start() for i in range(3)]
    me = lake.rstrip("/").rsplit("/", 1)[-1].lower()  # (this lake's name: its folder's)
    q = lambda s, i=0, spread=None: call(A.port + i, "POST", "/sql" + ("" if spread is None else f"?spread={spread}"), s.encode())
    def err(s, i=0):
        try:
            q(s, i)
            return None
        except RuntimeError as e:
            return str(e)
    checks = {}
    # schemas, made on followers (the leader carries them out)
    before = err("CREATE TABLE dbo.t (k BIGINT, v DOUBLE)", 1)
    q("CREATE SCHEMA dbo", 2)
    checks["a schema must exist before its tables; CREATE SCHEMA on a follower"] = "CREATE SCHEMA dbo" in (before or "") and q("CREATE SCHEMA IF NOT EXISTS dbo", 1)["unchanged"] \
        and err("CREATE SCHEMA dbo") is not None and err("DROP SCHEMA public") is not None
    q("CREATE TABLE t (k BIGINT, v DOUBLE)", 1)
    q("CREATE TABLE dbo.t (k BIGINT, v DOUBLE)", 2)
    q("INSERT INTO t VALUES (1, 1.0), (2, 2.0)", 1)
    q("INSERT INTO dbo.t VALUES (1, 10.0), (2, 20.0), (3, 30.0)", 2)
    time.sleep(0.5)
    n = lambda name, i=0: q(f"SELECT count(*) AS n FROM {name}", i)[0]["n"]
    checks["t, public.t and lake.public.t are one table; dbo.t another"] = all(n(x, i) == 2 for x in ("t", "public.t", f'"{me}".public.t') for i in range(3)) \
        and all(n(x, i) == 3 for x in ("dbo.t", f'"{me}".dbo.t', "DBO.T") for i in range(3))
    checks["a join across schemas"] = q("SELECT a.k, a.v + b.v AS s FROM t a JOIN dbo.t b USING (k) ORDER BY k") == [{"k": 1, "s": 11.0}, {"k": 2, "s": 22.0}]
    # other lakes: catalogs by the name they were attached with (lower case); `lake.t` still works
    checks["attached lakes: other.t, other.public.t, other.eu.t, joined with this lake's"] = n("other.sales") == n("Other.public.sales") == 2 and n("other.eu.sales") == 1 \
        and q(f'SELECT o.amount + t.v AS s FROM other.eu.sales o JOIN "{me}".public.t t ON o.id = t.k') == [{"s": 71.0}] \
        and "no lake" in (err("CREATE TABLE nolake.s.t (a BIGINT)") or "") and err("SELECT * FROM nolake.public.t") is not None
    # names
    q("CREATE TABLE Mixed (Id BIGINT)")
    q("INSERT INTO MIXED VALUES (5)")
    checks["names: unquoted ones are lower case; odd characters and four parts refused"] = q("SELECT id FROM mixed") == [{"id": 5}] \
        and all(err(s) is not None for s in ('CREATE TABLE "bad name" (a BIGINT)', "CREATE TABLE a.b.c.d (a BIGINT)", 'CREATE SCHEMA "a/b"', 'CREATE TABLE "x.y" (a BIGINT)'))
    # CREATE TABLE … AS, in pieces big enough to spread
    q("CREATE TABLE dbo.big AS SELECT value AS id, value % 100 AS k, value * 0.5 AS v FROM generate_series(1, 20000)", 1)
    for i in range(1, 6):
        q(f"INSERT INTO dbo.big SELECT value + {i * 20000}, value % 100, value * 0.5 FROM generate_series(1, 20000)")
    time.sleep(0.5)
    cols = q("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'dbo' AND table_name = 'big' ORDER BY ordinal_position")
    q("CREATE TABLE dbo.named (a INT, b VARCHAR) AS VALUES (1, 'x'), (2, NULL)")
    named = q("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'dbo' AND table_name = 'named' ORDER BY ordinal_position")
    checks["CREATE TABLE … AS: the query's rows and columns, or the ones it names"] = (n("dbo.big") == 120000 and [c["column_name"] for c in cols] == ["id", "k", "v"]
        and [c["column_name"] for c in named] == ["a", "b"] and named[0]["data_type"] == "Int32" and q("SELECT a, b FROM dbo.named ORDER BY a") == [{"a": 1, "b": "x"}, {"a": 2}]
        and err("CREATE TABLE dbo.wrong (a INT) AS VALUES (1, 2)") is not None)
    q("CREATE TABLE dbo.cols (a INT, b VARCHAR, c DOUBLE)")
    q("INSERT INTO dbo.cols (c, a) VALUES (1.5, 1), (2.5, 2)")
    q("INSERT INTO dbo.cols (b, a) SELECT 'x', 3", 1)
    checks["INSERT INTO t (columns): the ones named, by position, the rest NULL"] = (q("SELECT a, b, c FROM dbo.cols ORDER BY a") == [{"a": 1, "c": 1.5}, {"a": 2, "c": 2.5}, {"a": 3, "b": "x"}]
        and err("INSERT INTO dbo.cols (nope) VALUES (1)") is not None and err("INSERT INTO dbo.cols (a, b) VALUES (1)") is not None)
    # stored views: run where they are used, over what the tables hold then
    q("CREATE VIEW dbo.small AS SELECT k, v FROM dbo.big WHERE k < 10", 2)
    q("CREATE VIEW both_t AS SELECT k, v FROM t UNION ALL SELECT k, v FROM dbo.t")
    q("CREATE VIEW dbo.nested AS SELECT k, count(*) AS n FROM dbo.small GROUP BY k", 1)
    live = n("both_t")
    q("INSERT INTO t VALUES (9, 9.0)")
    time.sleep(0.5)
    checks["a view: the query, run over the tables as they are now; a view of a view"] = live == 5 and n("both_t", 1) == 6 and n("dbo.small") == 12000 \
        and q("SELECT sum(n) AS n FROM dbo.nested", 2) == [{"n": 12000}]
    checks["CREATE VIEW over a name in use refused; OR REPLACE replaces"] = err("CREATE VIEW both_t AS SELECT 1 AS x") is not None and err("CREATE VIEW t AS SELECT 1 AS x") is not None \
        and q("CREATE OR REPLACE VIEW both_t AS SELECT k FROM t") and n("both_t") == 3 and err("CREATE TABLE both_t (a BIGINT)") is not None
    # a query over a view, spread over the three nodes: the view reads each node's share
    spread = lambda s: (lambda before: (q(s, spread=0), q(s, spread=1), metrics_of(A.port)["pondra_spread_queries_total"] + metrics_of(A.port)["pondra_shuffled_queries_total"] - before))(
        metrics_of(A.port)["pondra_spread_queries_total"] + metrics_of(A.port)["pondra_shuffled_queries_total"])
    over_view = [spread(s) for s in ("SELECT count(*) AS n, sum(v) AS s FROM dbo.small",
                                     "SELECT k, count(*) AS n FROM dbo.small GROUP BY k ORDER BY k",
                                     "SELECT count(*) AS n FROM (SELECT k FROM dbo.small UNION ALL SELECT k FROM dbo.big)",
                                     f'SELECT count(*) AS n FROM "{me}".dbo.big b JOIN dbo.small s ON b.id = s.k + 1')]
    checks["queries over views spread, with the same answers"] = all(one == many and ran >= 1 for one, many, ran in over_view)
    # materialized views in SQL
    q("CREATE MATERIALIZED VIEW dbo.per_k AS SELECT k, count(*) AS n, sum(v) AS s FROM dbo.t GROUP BY k", 1)
    q("CREATE TABLE clicks (w_ts TIMESTAMP, u VARCHAR)")
    q("CREATE MATERIALIZED VIEW per_min WITH (window = 'w', size_secs = 60) AS SELECT date_bin(INTERVAL '1 minute', w_ts) AS w, count(*) AS n FROM clicks GROUP BY 1", 2)
    q("INSERT INTO dbo.t VALUES (1, 5.0)")
    q("INSERT INTO dbo.t VALUES (1, 1.0), (3, 3.0)", 2)
    want = q("SELECT k, count(*) AS n, sum(v) AS s FROM dbo.t GROUP BY k ORDER BY k")  # (the rows already there too: ADR-022)
    got = until(lambda: q("SELECT k, n, s FROM dbo.per_k ORDER BY k"), want, 30)
    checks["CREATE MATERIALIZED VIEW: the rows already there and those written after; WITH (window …) emits to _final; bad options refused"] = got == want and len(want) == 3 \
        and n("per_min_final") == 0 and err("CREATE MATERIALIZED VIEW m2 WITH (windw = 'w') AS SELECT k FROM t") is not None
    # clients see the schemas
    with psycopg.connect(f"host=127.0.0.1 port={A.port + 10} dbname={me} user=x", autocommit=True) as c:
        spaces = {r[0] for r in c.execute("SELECT nspname FROM pg_catalog.pg_namespace").fetchall()}
        in_dbo = {r[0] for r in c.execute("SELECT c.relname FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid WHERE n.nspname = 'dbo'").fetchall()}
        by_pg = c.execute("SELECT count(*) FROM dbo.t").fetchone()[0]
    conn = adbc.connect(f"grpc://127.0.0.1:{A.port + 30}")
    cur = conn.cursor()
    cur.adbc_ingest("landed", pa.table({"a": pa.array([1, 2, 3], pa.int64())}), mode="create", db_schema_name="dbo")
    cur.close()
    objects = conn.adbc_get_objects(depth="tables").read_all().to_pylist()
    conn.close()
    by_flight = {(c["catalog_name"], s["db_schema_name"], t["table_name"]) for c in objects for s in c["catalog_db_schemas"] for t in (s["db_schema_tables"] or [])}
    rest = call(A.port, "GET", "/v1/namespaces")["namespaces"]
    checks["Postgres, Flight SQL (ADBC ingest into a schema) and Iceberg REST see the schemas"] = {"public", "dbo"} <= spaces and {"t", "big", "small", "per_k"} <= in_dbo and by_pg == 6 \
        and {(me, "dbo", "landed"), (me, "dbo", "t"), (me, "public", "t"), (me, "dbo", "small")} <= by_flight and n("dbo.landed") == 3 \
        and ["default"] in rest and ["dbo"] in rest and ["other"] in rest and ["other", "eu"] in rest
    # dropping: refused while something reads it; a new table of the name starts empty
    used = err("DROP TABLE dbo.big", 1)
    q("DROP VIEW dbo.nested")
    q("DROP VIEW dbo.small", 2)
    q("DROP TABLE dbo.big", 1)
    q("CREATE TABLE dbo.big (id BIGINT, k BIGINT, v DOUBLE)")
    q("INSERT INTO dbo.t VALUES (4, 40.0)")  # (still in the log)
    view_owned = err("DROP TABLE dbo.per_k")
    q("DROP MATERIALIZED VIEW dbo.per_k")
    q("DROP TABLE dbo.t", 2)
    q("CREATE TABLE dbo.t (k BIGINT, v DOUBLE)", 1)
    time.sleep(0.5)
    checks["DROP refused while a view reads it; after the drop, a new table of its name is empty"] = "used by" in (used or "") and view_owned is not None \
        and n("dbo.big") == 0 and all(n("dbo.t", i) == 0 for i in range(3)) and err("SELECT * FROM dbo.small") is not None and err("DROP TABLE nope") is not None \
        and q("DROP TABLE IF EXISTS nope")["dropped"] is False
    # DROP SCHEMA: refused while it holds anything, unless CASCADE
    q("CREATE VIEW dbo.again AS SELECT * FROM dbo.t")
    full = err("DROP SCHEMA dbo", 1)
    q("DROP SCHEMA dbo CASCADE", 1)
    time.sleep(0.5)
    shown = {(r["table_schema"], r["table_name"]) for r in q("SHOW TABLES", 2)}
    checks["DROP SCHEMA refused while it holds tables; CASCADE drops them all"] = "isn't empty" in (full or "") and not any(s == "dbo" for s, _ in shown) \
        and ("public", "t") in shown and err("SELECT * FROM dbo.t") is not None and err("CREATE TABLE dbo.t (a BIGINT)") is not None
    # ATTACH in SQL: kept in the lake, so every node attaches it (after a restart too, and in
    # `pondra sql`), queried and written across the two; DETACH takes it off every node
    third = new_lake()
    c = Node(third, A.port + 6).start()
    for s in ("CREATE TABLE stock (id BIGINT, qty BIGINT)", "INSERT INTO stock VALUES (1, 100), (2, 200)"):
        sql(A.port + 6, s)
    refused = [err(f"ATTACH '{d}' AS {n}", 1) for d, n in ((third, "public"), (lake, "self"), (other, f'"{me}"'))]  # a schema's name, this lake, this lake's name
    q(f"ATTACH '{third}' AS Warehouse", 1)
    reach = lambda i: not _raises(lambda: q("SELECT count(*) AS n FROM warehouse.public.stock", i))
    everywhere = [until(lambda i=i: reach(i), True, 15) for i in range(3)]
    joined = q("SELECT t.k, s.qty FROM warehouse.stock s JOIN t ON s.id = t.k ORDER BY 1", 2)
    before_insert = n("warehouse.stock", 0)
    q("INSERT INTO warehouse.stock VALUES (3, 300)", 2)
    fresh = until(lambda: n("warehouse.stock", 0), 3, 15)  # (an answer remembered from before is not)
    nodes[2].kill()
    nodes[2].start(tries=1)
    after_restart = until(lambda: _try(lambda: n("warehouse.stock", 2)), 3, 15)  # (within a second of starting)
    cli = subprocess.run([BIN, "sql", "--dir", lake, "SELECT count(*) AS n FROM warehouse.stock"], capture_output=True, text=True, timeout=120).stdout
    listed = {r["catalog_name"] for r in q("SELECT DISTINCT catalog_name FROM information_schema.schemata")}
    # DuckDB's FROM-first (`FROM t` is SELECT * FROM t): alone, as a subquery, as a view's query;
    # a change to an attached lake's table says which lake and where it runs (ADR-024)
    q("CREATE VIEW from_first AS FROM warehouse.stock")
    from_first = [q("FROM warehouse.stock ORDER BY id"), q("SELECT count(*) AS n FROM (FROM warehouse.stock)", 1), q("FROM from_first ORDER BY id", 2)]
    change = err("UPDATE warehouse.stock SET qty = 0 WHERE id = 1", 2)
    checks["FROM t is SELECT * FROM t (alone, a subquery, a view's); UPDATE on an attached lake names it and where it runs"] = \
        from_first[0] == [{"id": 1, "qty": 100}, {"id": 2, "qty": 200}, {"id": 3, "qty": 300}] and from_first[1] == [{"n": 3}] and from_first[2] == from_first[0] \
        and change is not None and "warehouse.stock is in attached lake warehouse" in change and "INSERT works from here" in change
    q("DROP VIEW from_first")
    q("DETACH warehouse")
    gone = [until(lambda i=i: reach(i), False, 15) for i in range(3)]
    checks["ATTACH 'dir' AS name on one node: every node, after a restart, pondra sql; joins and writes across; DETACH everywhere; bad ones refused"] = \
        all(refused) and all(everywhere) and joined == [{"k": 1, "qty": 100}, {"k": 2, "qty": 200}] and (before_insert, fresh) == (2, 3) and after_restart == 3 and "| 3 |" in cli \
        and {me, "other", "warehouse"} <= listed and not any(gone)
    # a new lake: ATTACH of a place with no lake makes one there; CREATE DATABASE makes one beside this lake
    fresh, beside = third + "-new", f"made_{uuid.uuid4().hex[:6]}"
    LAKES.extend([fresh, lake.rstrip("/").rsplit("/", 1)[0] + "/" + beside])
    made, db = q(f"ATTACH '{fresh}' AS fresh", 1), q(f"CREATE DATABASE {beside}", 2)
    q(f"CREATE TABLE {beside}.x (a BIGINT)")
    q(f"INSERT INTO {beside}.x VALUES (1), (2)", 1)
    checks["ATTACH of a place with no lake makes one; CREATE DATABASE makes one beside this lake, attached; twice is refused"] = made.get("created") is True and db.get("created") is True \
        and until(lambda: _try(lambda: n(f"{beside}.x", 2)), 2, 15) == 2 and err(f"CREATE DATABASE {beside}") is not None and "unchanged" in q(f"CREATE DATABASE IF NOT EXISTS {beside}")
    [x.kill() for x in nodes + [b, c]]
    ok = all(checks.values())
    print(json.dumps({"schemas": checks, "ok": ok}, indent=1))
    if not ok:
        print(before, over_view, got, spaces, in_dbo, by_flight, rest, used, view_owned, full, shown, refused, everywhere, joined, before_insert, fresh, after_restart, cli, listed, gone, from_first, change)
        sys.exit(1)
    return f"schemas: lake.schema.table, attached lakes as catalogs, CREATE/DROP SCHEMA, DROP TABLE, CTAS, stored and materialized views in SQL from any node, spread over views, clients list schemas: all {len(checks)} checks pass"


def scale():
    """Tables at scale. A partitioned table (`day(ts)`): every file holds one day, INSERTs and
    tiered log rows alike, before and after merges and an ADD COLUMN. Its files pile up past the
    catalog entry's limit and are sealed into manifests; queries skip files by min/max, on one node
    and spread over three; Delta and Iceberg readers see the sealed files too. Then queries bigger
    than a 50 MB memory limit: they spill (sorts, aggregations) or switch join strategy."""
    import datetime, io, pyarrow.parquet as pq
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    import open_check
    iso = lambda t: datetime.datetime.fromtimestamp(t, datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    lake = new_lake()
    port = A.port
    node = Node(lake, port, tier_secs=0.5).start()
    q = lambda s, p=port: sql(p, s)
    refused = [_raises(lambda s=s: q(s)) for s in (
        "CREATE TABLE bad1 (id BIGINT, ts TIMESTAMP) WITH (partition_by = 'week(ts)')",
        "CREATE TABLE bad2 (id BIGINT, ts TIMESTAMP) WITH (partition_by = 'nope')",
        "CREATE TABLE bad3 (id BIGINT PRIMARY KEY, ts TIMESTAMP) WITH (partition_by = 'day(when)')",  # (a keyed table may be partitioned, ADR-021: by a column it has)
        "CREATE TABLE bad4 (id BIGINT, name VARCHAR) WITH (partition_by = 'day(name)')")]
    q("CREATE TABLE ev (id BIGINT, ts TIMESTAMP, v DOUBLE, k VARCHAR) WITH (partition_by = 'day(ts)', publish = 'delta,iceberg')")
    base = 1_780_000_000 // 86400 * 86400
    days = {}  # day -> rows, what the table should hold
    def expect(ts, n=1):
        d = iso(ts // 86400 * 86400)
        days[d] = days.get(d, 0) + n
    # 200 INSERTs of 100 rows 10 minutes apart: each spans one or two days, ~140 days in all.
    for i in range(200):
        q(f"INSERT INTO ev SELECT value + {i * 100}, to_timestamp_seconds({base} + (value + {i * 100}) * 600), 1.0, 'k' || (value % 5) FROM generate_series(0, 99)")
        for r in range(100):
            expect(base + (r + i * 100) * 600)
    # Rows through the log: 60 batches, each spread over three days.
    for j in range(60):
        rows = [{"id": 10**6 + j * 50 + r, "ts": iso(base + (j + r % 3) * 86400 + r * 60), "v": 2.0, "k": "log"} for r in range(50)]
        for r in rows:
            expect(datetime.datetime.fromisoformat(r["ts"]).replace(tzinfo=datetime.timezone.utc).timestamp().__int__())
        call(port, "POST", f"/append/ev?producer=p&seq={j + 1}", "".join(json.dumps(r) + "\n" for r in rows).encode())
    q("ALTER TABLE ev ADD COLUMN note VARCHAR")
    q(f"INSERT INTO ev SELECT value, to_timestamp_seconds({base} + value * 3600), 1.0, 'late', 'x' FROM generate_series(0, 99)")
    for r in range(100):
        expect(base + r * 3600)
    n_rows, total = sum(days.values()), 200 * 100 * 1.0 + 60 * 50 * 2.0 + 100
    for _ in range(40):  # tiering, merges and sealing settle
        m = metrics_of(port)
        if m.get("pondra_untiered_rows", 1) == 0 and m['pondra_table_files{table="ev",where="inline"}'] <= 128:
            break
        time.sleep(0.5)
    call(port, "POST", "/tier")
    time.sleep(2)
    m = metrics_of(port)
    inline, sealed = m['pondra_table_files{table="ev",where="inline"}'], m['pondra_table_files{table="ev",where="sealed"}']
    per_day = {r["d"]: r["n"] for r in q("SELECT CAST(date_trunc('day', ts) AS VARCHAR) AS d, count(*) AS n FROM ev GROUP BY 1")}
    per_day = {d.replace(" ", "T")[:19]: n for d, n in per_day.items()}
    one_day = sorted(days)[30]
    m0 = metrics_of(port)
    day_n = q(f"SELECT count(*) AS n FROM ev WHERE ts >= TIMESTAMP '{one_day}' AND ts < TIMESTAMP '{one_day}' + INTERVAL '1 day'")[0]["n"]
    m1 = metrics_of(port)
    scanned = m1["pondra_files_scanned_total"] - m0["pondra_files_scanned_total"]
    # Every Parquet file of the table holds one day (replaced ones included, until they're deleted).
    files = [k for k in lake_objects(lake, "data/ev/") if k.endswith(".parquet") and "/" not in k[len("data/ev/"):]]
    def days_in(f):
        try:
            return len({str(t)[:10] for t in pq.read_table(io.BytesIO(open_check.read_object(lake, f)), columns=["ts"]).column("ts").to_pylist()})
        except Exception:
            return 1  # (a replaced file deleted meanwhile)
    mixed = [f for f in files if days_in(f) > 1]
    # Two more nodes: the same query spread over three.
    for p in (port + 1, port + 2):
        Node(lake, p).start()
    while len(call(port, "GET", "/stats")["nodes"]) < 3:
        time.sleep(0.2)
    spread = call(port, "POST", "/sql?spread=1", b"SELECT count(*) AS n, sum(v) AS s FROM ev WHERE k <> 'none'")[0]
    was_spread = metrics_of(port)["pondra_spread_queries_total"] >= 1
    # Each file (sealed ones too) holds one day of `ts`: a self-join on it runs by its ranges,
    # without a shuffle.
    self_join = b"SELECT count(*) AS n, sum(x.v) AS s FROM ev x JOIN ev y ON x.ts = y.ts"
    before = metrics_of(port)["pondra_ranged_queries_total"]
    by_ranges = call(port, "POST", "/sql?spread=1", self_join) == call(port, "POST", "/sql?spread=0", self_join) and metrics_of(port)["pondra_ranged_queries_total"] > before
    shuffles = shuffle_checks(port)
    theirs = {**open_check.readers(lake, "ev"), **{f"iceberg/{k}": v for k, v in open_check.iceberg_readers(lake, "ev").items()}}
    checks = {
        "bad partition specs refused": all(refused),
        "files sealed into manifests": sealed > 0 and inline <= 128,
        "every row, every day": q("SELECT count(*) AS n, sum(v) AS s FROM ev")[0] == {"n": n_rows, "s": total} and per_day == days,
        "one day reads only its files": day_n == days[one_day] and 0 < scanned <= 8,
        "every file holds one day": len(files) > 0 and not mixed,
        "three nodes: same answer": was_spread and spread == {"n": n_rows, "s": total},
        "three nodes: a self-join on sealed files by their time ranges": by_ranges,
        "shuffles (GROUP BY, joins, windows) = one node": all(same for same, *_ in shuffles.values()) and sum(sh for _, sh, *_ in shuffles.values()) >= 10,
        "outer, semi and anti joins, subqueries, CTEs and unions run across the nodes": sum(sp for _, _, sp, _ in list(shuffles.values())[14:23]) >= 9,
        "tables that share a key meet where they are, by its ranges": sum(r for *_, r in list(shuffles.values())[23:]) >= 4,
        "Delta and Iceberg readers see sealed files": all(v == n_rows for v in theirs.values()),
    }
    for n in list(NODES):
        n.kill()
    # A 50 MB memory limit: a 2M-group aggregation and a sort spill, a join of 3M rows switches
    # to sort-merge (hash joins can't spill), and the node stays up. As on a 4-core machine
    # (GitHub's runners), where each partition's sort kept 10 MB aside and the merge above the
    # sorts found the budget gone: a partition per 24 MB at most.
    node = Node(lake, port, memory_gb=0.05, env={"PONDRA_CORES": os.environ.get("PONDRA_TEST_CORES", "4")}).start()
    q("CREATE TABLE big (id BIGINT, k BIGINT, s VARCHAR)")
    q("INSERT INTO big SELECT value, value % 2000000, 'name-' || (value % 2000000) FROM generate_series(1, 3000000)")
    checks["over the memory limit: aggregation, sort, join"] = (
        q("SELECT count(*) AS g, sum(n) AS n FROM (SELECT s, count(*) AS n FROM big GROUP BY s)") == [{"g": 2000000, "n": 3000000}]
        and q("SELECT max(r) AS r FROM (SELECT row_number() OVER (ORDER BY s, id) AS r FROM big)") == [{"r": 3000000}]
        and q("SELECT count(*) AS n FROM big a JOIN big b ON a.id = b.id") == [{"n": 3000000}]
        and metrics_of(port)["pondra_memory_limit_bytes"] == int(0.05 * (1 << 30)))
    node.kill()
    ok = all(checks.values())
    shown = {q: ("same" if same else "DIFFERENT") + (", by ranges" if r else "") + (", shuffled" if sh else ", gathered" if sp else ", one node") for q, (same, sh, sp, r) in shuffles.items()}
    print(json.dumps({"scale": checks, "shuffles": shown, "rows": n_rows, "days": len(days), "files": {"inline": inline, "sealed": sealed, "parquet_objects": len(files), "one_day_scanned": scanned}, "outside_readers": theirs, "ok": ok}, indent=1))
    if not ok:
        print("mixed:", mixed[:3], "per_day diff:", {d: (per_day.get(d), n) for d, n in days.items() if per_day.get(d) != n})
        sys.exit(1)
    return f"scale: {n_rows:,} rows over {len(days)} daily partitions, {int(inline + sealed)} files ({int(sealed)} sealed), every file one day, a day's query reads {int(scanned)} files, 3 nodes, {sum(sp for _, _, sp, _ in shuffles.values())} of {len(shuffles)} queries spread, {sum(sh for _, sh, *_ in shuffles.values())} shuffled, {sum(r for *_, r in shuffles.values())} by key ranges (all equal to one node), 6 outside readers, a 50 MB memory limit: all {len(checks)} checks pass"


def flight():
    """Arrow Flight and Flight SQL. pyarrow: DoPut exactly-once (a retried stream is applied
    once), DoGet SQL, GetFlightInfo, ListFlights, a table's log as a columnar stream (chosen
    columns; following new commits), tokens and the basic-auth handshake. ADBC (Flight SQL):
    queries, a write sent as a query, bulk ingest into a new table, catalog objects. Writes to a
    follower's Flight port reach the leader."""
    import pyarrow as pa, pyarrow.flight as fl
    import adbc_driver_flightsql.dbapi as adbc
    lake = new_lake()
    port, fport = A.port, A.port + 30
    tokens = {"read_token": "r", "write_token": "w", "admin_token": "a"}
    node = Node(lake, port, flight=f"127.0.0.1:{fport}", tier_secs=0.5, **tokens).start()
    follower = Node(lake, port + 1, flight=f"127.0.0.1:{fport + 1}", **tokens).start()
    q = lambda s: call(port, "POST", "/sql", s.encode(), headers={"authorization": "Bearer a"})
    q("CREATE TABLE ev (user VARCHAR, amount BIGINT, ts TIMESTAMP)")
    opts = lambda t: fl.FlightCallOptions(headers=[(b"authorization", f"Bearer {t}".encode())])
    client = fl.FlightClient(f"grpc://127.0.0.1:{fport}")
    schema = pa.schema([("user", pa.string()), ("amount", pa.int64()), ("ts", pa.timestamp("ns"))])
    def batch(i, n=1000):
        return pa.record_batch([pa.array([f"u{j % 10}" for j in range(n)]), pa.array([i] * n, pa.int64()), pa.array([1_790_000_000_000_000_000 + j for j in range(n)], pa.timestamp("ns"))], schema=schema)
    def put(path, batches, token="w", to=client):
        w, r = to.do_put(fl.FlightDescriptor.for_path(*path), schema, options=opts(token))
        for b in batches:
            w.write_batch(b)
        w.done_writing()
        acks = []
        while (buf := r.read()) is not None:
            acks.append(json.loads(buf.to_pybytes()))
        w.close()
        return acks
    count = lambda: q("SELECT count(*) AS n, sum(amount) AS s FROM ev")[0]
    first = put(["ev", "p", "1"], [batch(i) for i in range(10)])
    again = put(["ev", "p", "1"], [batch(i) for i in range(10)])  # a retried stream
    after_retry = count()
    via_follower = put(["ev", "q", "1"], [batch(100)], to=fl.FlightClient(f"grpc://127.0.0.1:{fport + 1}"))
    try:
        put(["ev"], [batch(0)], token="r")
        read_token_refused = False
    except fl.FlightUnauthenticatedError:
        read_token_refused = True
    sql_ticket = fl.Ticket(json.dumps({"sql": "SELECT user, sum(amount) AS s FROM ev GROUP BY user ORDER BY user"}))
    by_user = client.do_get(sql_ticket, options=opts("r")).read_all()
    info = client.get_flight_info(fl.FlightDescriptor.for_command(json.dumps({"sql": "SELECT count(*) AS n FROM ev"})), opts("r"))
    via_info = client.do_get(info.endpoints[0].ticket, options=opts("r")).read_all().to_pylist()
    listed = [f.descriptor.path[0].decode() for f in client.list_flights(options=opts("r"))]
    # The log as a columnar stream: what's committed so far (two columns), then following.
    past = client.do_get(fl.Ticket(json.dumps({"table": "ev", "after": 0, "columns": ["user", "amount"], "follow": False})), options=opts("r")).read_all()
    live = client.do_get(fl.Ticket(json.dumps({"table": "ev", "columns": ["amount"]})), options=opts("r"))
    got, marks, lag = [0], [], []
    def follow():
        for chunk in live:
            if chunk.data is not None and chunk.data.num_rows:
                got[0] += chunk.data.num_rows
                lag.append(time.time())
            if chunk.app_metadata is not None:
                marks.append(json.loads(chunk.app_metadata.to_pybytes())["after"])
            if got[0] >= 3000 and marks:  # (each commit's rows, then where to resume)
                return
    t = threading.Thread(target=follow, daemon=True); t.start()
    time.sleep(0.5)
    sent_at = time.time()
    put(["ev", "p", "11"], [batch(i) for i in range(10, 13)])
    t.join(10)
    header = client.authenticate_basic_token("reader", "r")
    shaken = client.do_get(fl.Ticket(json.dumps({"sql": "SELECT 1 AS one FROM ev LIMIT 1"})), options=fl.FlightCallOptions(headers=[header])).read_all().num_rows
    # ADBC over Flight SQL.
    conn = adbc.connect(f"grpc://127.0.0.1:{fport}", db_kwargs={"adbc.flight.sql.authorization_header": "Bearer a"})
    cur = conn.cursor()
    cur.execute("SELECT count(*) AS n FROM ev")
    adbc_count = cur.fetchone()[0]
    cur.execute("INSERT INTO ev VALUES ('adbc', 5, TIMESTAMP '2026-09-22 10:00:00')")
    ingest = pa.table({"k": pa.array(range(5000), pa.int64()), "name": pa.array([f"n{i}" for i in range(5000)]), "at": pa.array([1_790_000_000_000_000 + i for i in range(5000)], pa.timestamp("us"))})
    ingested = cur.adbc_ingest("ingested", ingest, mode="create")
    cur.close(); cur = conn.cursor()  # (an ingesting statement can't run a query after)
    cur.execute("SELECT count(*) AS n, sum(k) AS s FROM ingested")
    ingest_back = cur.fetchone()
    objects = conn.adbc_get_objects(depth="tables").read_all().to_pylist()
    names = [t["table_name"] for c in objects for s in c["catalog_db_schemas"] for t in s["db_schema_tables"]]
    cur.close(); conn.close()
    total = count()
    checks = {
        "DoPut: 10 batches acked": len(first) == 10 and not any(a["duplicate"] for a in first),
        "a retried stream is applied once": len(again) == 10 and all(a["duplicate"] for a in again) and after_retry == {"n": 10000, "s": 1000 * sum(range(10))},
        "DoPut to a follower": len(via_follower) == 1 and not via_follower[0]["duplicate"],
        "a read token can't write": read_token_refused,
        "DoGet SQL": by_user.num_rows == 10 and by_user.column_names == ["user", "s"],
        "GetFlightInfo + DoGet": info.schema.names == ["n"] and via_info == [{"n": 11000}],
        "ListFlights": "ev" in listed,
        "the log, two columns": past.column_names == ["user", "amount"] and past.num_rows == 11000,
        "the log, following": got[0] == 3000 and len(marks) >= 1,
        "basic-auth handshake": shaken == 1,
        "ADBC query": adbc_count == 14000,
        "ADBC write as a query, ingest, objects": ingest_back == (5000, sum(range(5000))) and ingested == 5000 and {"ev", "ingested"} <= set(names),
        "every row once": total == {"n": 14001, "s": 1000 * (sum(range(13)) + 100) + 5},
    }
    node.kill(); follower.kill()
    ok = all(checks.values())
    follow_ms = round((lag[-1] - sent_at) * 1000) if lag else None
    print(json.dumps({"flight": checks, "follow_ms": follow_ms, "ok": ok}, indent=1))
    if not ok:
        print(first[:2], again[:2], after_retry, via_follower, by_user.num_rows, via_info, listed, past.num_rows, got, marks, shaken, adbc_count, ingest_back, ingested, names, total)
        sys.exit(1)
    return f"Arrow Flight: pyarrow DoPut exactly-once (retried stream applied once, via a follower too), DoGet SQL, FlightInfo, ListFlights, the log as a columnar stream (a subscriber has each new commit in {follow_ms} ms), tokens and handshake; ADBC queries, writes, ingest and catalog: all {len(checks)} checks pass"


def shuffle_checks(port):
    """Queries spread over the cluster (?spread=1) against the same on one node (?spread=0):
    {query: (same answer, shuffled)}. Two tables of 800,000 and 40,000 rows (the small one read
    whole by every node: broadcast), a few rows still in the log, and a keyed table (read whole).
    The self-join slices both sides: a join shuffled on its key."""
    q = lambda s, spread: call(port, "POST", f"/sql?spread={spread}", s.encode())
    q("CREATE TABLE a (id BIGINT, k BIGINT, v DOUBLE, s VARCHAR, p BIGINT) WITH (partition_by = 'p')", 0)
    q("CREATE TABLE b (k BIGINT, name VARCHAR)", 0)
    q("CREATE TABLE u (id BIGINT PRIMARY KEY, name VARCHAR)", 0)
    for i in range(8):
        q(f"INSERT INTO a SELECT value + {i * 100000}, (value * 7 + {i}) % 50000, value * 0.5, 's' || (value % 13), value % 5 FROM generate_series(1, 100000)", 0)
    for i in range(4):
        q(f"INSERT INTO b SELECT value + {i * 10000}, 'n' || value FROM generate_series(0, 9999)", 0)
    q("INSERT INTO u VALUES (0, 'zero'), (1, 'one'), (2, 'two'), (3, 'three'), (4, 'four')", 0)
    call(port, "POST", "/append/a?producer=tail&seq=1", "".join(json.dumps({"id": 10**7 + r, "k": r % 50, "v": 1.0, "s": "tail", "p": r % 5}) + "\n" for r in range(500)).encode())
    # Round 15: two tables written in the order of a key they share (orders and their lines), so
    # each file holds a narrow range of it; a few rows of each still in the log, some with no key.
    q("CREATE TABLE o (id BIGINT, c BIGINT)", 0)
    q("CREATE TABLE li (oid BIGINT, qty BIGINT)", 0)
    for i in range(4):
        q(f"INSERT INTO o SELECT value + {i * 50000}, value % 97 FROM generate_series(1, 50000)", 0)
        q(f"INSERT INTO li SELECT (value + 3) / 4 + {i * 50000}, value % 50 FROM generate_series(1, 200000)", 0)
    call(port, "POST", "/append/o?producer=tail-o&seq=1", "".join(json.dumps({"id": 200001 + r, "c": r}) + "\n" for r in range(50)).encode())
    call(port, "POST", "/append/li?producer=tail-li&seq=1", "".join(json.dumps({"oid": None if r % 10 == 0 else 200001 + r % 60, "qty": r % 50}) + "\n" for r in range(200)).encode())
    queries = {
        "many groups": "SELECT k, count(*) AS n, sum(v) AS s FROM a GROUP BY k ORDER BY k LIMIT 7",
        "groups, no order": "SELECT k % 1000 AS g, count(*) AS n FROM a GROUP BY k % 1000",
        "HAVING": "SELECT k, count(*) AS n FROM a GROUP BY k HAVING count(*) > 15 ORDER BY k",
        "string keys": "SELECT s, p, count(*) AS n, min(id) AS lo FROM a GROUP BY s, p ORDER BY s, p",
        "DISTINCT": "SELECT DISTINCT s FROM a ORDER BY s",
        "join, then many groups": "SELECT a.k, count(*) AS n, max(b.name) AS m FROM a JOIN b ON a.k = b.k GROUP BY a.k ORDER BY n DESC, a.k LIMIT 10",
        "join, filters, count": "SELECT count(*) AS n, sum(a.v) AS s FROM a JOIN b ON a.k = b.k WHERE a.v > 1000 AND b.name LIKE 'n1%'",
        "join, then another key": "SELECT a.s, count(*) AS n, sum(b.k) AS t FROM a JOIN b ON a.k = b.k GROUP BY a.s ORDER BY a.s",
        "self-join": "SELECT count(*) AS n FROM a x JOIN a y ON x.id = y.id + 1",
        "count(DISTINCT) per group": "SELECT s, count(DISTINCT k) AS d FROM a GROUP BY s ORDER BY s",
        "window, PARTITION BY": "SELECT k, v, row_number() OVER (PARTITION BY k ORDER BY v) AS r FROM a WHERE k < 3 ORDER BY k, v LIMIT 20",
        "window over all": "SELECT k, row_number() OVER (ORDER BY v, id) AS r FROM a ORDER BY r LIMIT 5",
        "global aggregate": "SELECT count(*) AS n, avg(v) AS a, count(DISTINCT k) AS d FROM a",
        "a keyed table": "SELECT u.name, count(*) AS n FROM a JOIN u ON a.p = u.id GROUP BY u.name ORDER BY u.name",
        # Round 14: shapes that used to run on one node. Each must still equal one node's answer.
        "LEFT JOIN, unmatched rows kept": "SELECT count(*) AS n, count(b.name) AS m FROM a LEFT JOIN b ON a.k = b.k + 45000",
        "small table LEFT JOIN big one": "SELECT b.k, count(a.id) AS n FROM b LEFT JOIN a ON a.k = b.k GROUP BY b.k ORDER BY n, b.k LIMIT 10",
        "FULL JOIN": "SELECT count(*) AS n, count(a.id) AS x, count(b.k) AS y FROM a FULL JOIN b ON a.id = b.k",
        "IN (a semi join)": "SELECT count(*) AS n FROM a WHERE k IN (SELECT k FROM b WHERE name LIKE 'n2%')",
        "NOT EXISTS (an anti join)": "SELECT count(*) AS n FROM b WHERE NOT EXISTS (SELECT 1 FROM a WHERE a.k = b.k)",
        "NOT IN (NULLs matter)": "SELECT count(*) AS n FROM a WHERE k NOT IN (SELECT k FROM b WHERE k < 20000)",
        "a scalar subquery": "SELECT count(*) AS n FROM a WHERE v > (SELECT avg(v) FROM a)",
        "a CTE and UNION ALL": "WITH hi AS (SELECT k FROM a WHERE v > 30000), lo AS (SELECT k FROM a WHERE v < 100) SELECT count(*) AS n, sum(k) AS s FROM (SELECT k FROM hi UNION ALL SELECT k FROM lo)",
        "a keyed table, LEFT JOIN": "SELECT a.p, count(u.name) AS n FROM a LEFT JOIN u ON a.p = u.id GROUP BY a.p ORDER BY a.p",
        # Round 15: tables sliced by the ranges of the key they share (`ranges.rs`).
        "by key ranges: join, then groups": "SELECT o.c % 10 AS g, count(*) AS n, sum(li.qty) AS q FROM o JOIN li ON o.id = li.oid GROUP BY o.c % 10",
        "by key ranges: EXISTS": "SELECT count(*) AS n FROM o WHERE EXISTS (SELECT 1 FROM li WHERE li.oid = o.id AND li.qty > 45)",
        "by key ranges: groups of the key": "SELECT oid, sum(qty) AS q FROM li GROUP BY oid HAVING sum(qty) > 180 ORDER BY oid NULLS FIRST LIMIT 20",
        "by key ranges: LEFT JOIN": "SELECT count(*) AS n, count(li.oid) AS m FROM o LEFT JOIN li ON o.id = li.oid AND li.qty = 1",
        "by key ranges: NULL keys": "SELECT count(*) AS n, sum(qty) AS q FROM li WHERE oid IS NULL OR oid > 199990",
        # (the NULLs a LEFT JOIN pads with sit wherever the unmatched rows are: not keyed by range)
        "by key ranges: grouped by a padded key": "SELECT li.oid, count(*) AS n FROM o LEFT JOIN li ON o.id = li.oid AND li.qty = 1 GROUP BY li.oid ORDER BY n DESC, li.oid LIMIT 3",
    }
    out = {}
    for name, s in queries.items():
        before = metrics_of(port)
        one, many = q(s, 0), q(s, 1)
        after = metrics_of(port)
        ordered = "ORDER BY" in s.split("OVER")[-1]
        same = one == many if ordered else sorted(map(json.dumps, one)) == sorted(map(json.dumps, many))
        ran = lambda m: int(after[f"pondra_{m}_queries_total"] - before[f"pondra_{m}_queries_total"])
        out[name] = (same and len(one) > 0, ran("shuffled"), ran("spread"), ran("ranged"))
    return out


def metrics_of(port):
    out = {}
    for line in call(port, "GET", "/metrics").decode().splitlines():
        if line and not line.startswith("#"):
            name, v = line.rsplit(" ", 1)
            out[name] = float(v)
    return out


def lake_objects(lake, prefix):
    """Keys under `prefix` in the lake (relative to it)."""
    if not lake.startswith("s3://"):
        root = os.path.join(lake, prefix)
        return [prefix + f for f in os.listdir(root)] if os.path.isdir(root) else []
    bucket, base = lake[5:].split("/", 1)
    keys = []
    for pg in S3[0].get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{base}/{prefix}"):
        keys += [o["Key"][len(base) + 1:] for o in pg.get("Contents", [])]
    return keys


def put_object(lake, key, body):
    """Write an object into a lake directly, as another program would."""
    if not lake.startswith("s3://"):
        os.makedirs(os.path.dirname(os.path.join(lake, key)), exist_ok=True)
        with open(os.path.join(lake, key), "wb") as f:
            f.write(body)
        return
    bucket, base = lake[5:].split("/", 1)
    S3[0].put_object(Bucket=bucket, Key=f"{base}/{key}", Body=body)


def changes():
    """UPDATE, DELETE and MERGE on every table (ADR-020), against a model. An append table's rows
    change by version: `_row_id` and `_created_at` kept, `_version` the change's commit, and the old
    version gone from every read on every node at once — before and after tiering, and after the
    purge that rewrites the files holding it (every round here: PONDRA_PURGE_ROWS=1), which Delta
    readers see. Changes go to any node, from `pondra sql` too; three nodes adding to the same rows
    at once lose none; a retried job applies once. A row-by-row view and an adding-up one follow
    every change; the change feed (MCP `changes`, `/watch?changes=true`) replays to the same rows,
    each old version as it was; a spread query over the changed tables == one node. Keyed tables
    keep a row's id through an UPDATE and a MERGE. What can't follow a change is refused."""
    import deltalake
    lake, rnd = new_lake(), random.Random(19)
    env = {"PONDRA_PURGE_ROWS": "1"}
    nodes = [Node(lake, A.port + i, tier_secs=1, changelog_secs=3600, env=env).start() for i in range(3)]
    # (a change waits for a tiering round in progress — with a purge in every one, on R2, seconds)
    q = lambda s, i=0, **p: call(A.port + i, "POST", "/sql" + ("?" + "&".join(f"{k}={v}" for k, v in p.items()) if p else ""), s.encode(), timeout=300)
    def err(s, i=0):
        try:
            q(s, i)
            return None
        except RuntimeError as e:
            return str(e)
    checks, model, ids = {}, {}, {}  # id -> [owner, bal]; id -> _row_id once seen
    q("CREATE TABLE acct (id BIGINT, owner VARCHAR, bal DOUBLE) WITH (publish = 'delta')")
    q("CREATE MATERIALIZED VIEW rich AS SELECT id, owner, bal FROM acct WHERE bal >= 50", 1)
    q("CREATE MATERIALIZED VIEW per_owner AS SELECT owner, sum(bal) AS total, count(*) AS n FROM acct GROUP BY owner", 2)
    owners, next_id = "abcd", [1]
    def insert(i):
        rows = []
        for _ in range(rnd.randint(1, 30)):
            model[next_id[0]] = [rnd.choice(owners), float(rnd.randint(0, 100))]
            rows.append(f"({next_id[0]}, '{model[next_id[0]][0]}', {model[next_id[0]][1]})")
            next_id[0] += 1
        q("INSERT INTO acct VALUES " + ", ".join(rows), i)
    def update(i):
        m, r, d = rnd.randint(2, 5), rnd.randint(0, 1), rnd.randint(-20, 20)
        q(f"UPDATE acct SET bal = bal + {d} WHERE id % {m} = {r}", i)
        for k, v in model.items():
            if k % m == r:
                v[1] += d
    def reown(i):
        o, x = rnd.choice(owners), rnd.randint(0, 60)
        q(f"UPDATE acct SET owner = '{o}' WHERE bal < {x}", i)
        for v in model.values():
            if v[1] < x:
                v[0] = o
    def delete(i):
        x, r = rnd.randint(40, 120), rnd.randint(0, 2)
        q(f"DELETE FROM acct WHERE bal > {x} AND id % 3 = {r}", i)
        for k in [k for k, v in model.items() if v[1] > x and k % 3 == r]:
            del model[k]
    def merge(i):
        src = {}
        for _ in range(rnd.randint(1, 8)):
            k = rnd.choice(list(model) or [1]) if rnd.random() < 0.6 else next_id[0] + rnd.randint(0, 5)
            src[k] = (rnd.choice(owners), float(rnd.randint(0, 30)))
        values = ", ".join(f"({k}, '{o}', {b})" for k, (o, b) in src.items())
        q(f"MERGE INTO acct t USING (VALUES {values}) AS s(id, owner, bal) ON t.id = s.id "
          "WHEN MATCHED AND s.bal < 5 THEN DELETE WHEN MATCHED THEN UPDATE SET bal = t.bal + s.bal WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.owner, s.bal)", i)
        for k, (o, b) in src.items():
            if k in model and b < 5:
                del model[k]
            elif k in model:
                model[k][1] += b
            else:
                model[k] = [o, b]
        next_id[0] = max(next_id[0], max(src) + 1)
    want = lambda: [{"id": k, "owner": v[0], "bal": v[1]} for k, v in sorted(model.items())]
    table = lambda i=0: q("SELECT id, owner, bal FROM acct ORDER BY id", i)
    def per_owner():
        out = {}
        for o, b in model.values():
            t = out.setdefault(o, [0.0, 0])
            t[0], t[1] = t[0] + b, t[1] + 1
        return [{"owner": o, "total": t[0], "n": t[1]} for o, t in sorted(out.items())]
    seen, kept, agree = [], [], []
    insert(0)
    for step in range(60):
        rnd.choice([insert, update, update, reown, delete, merge, merge])(rnd.randrange(3))
        if step % 6 == 5:
            now = want()
            agree.append(all(until(lambda i=i: table(i), now, 10) == now for i in range(3)))
            agree.append(until(lambda: q("SELECT id, owner, bal FROM rich ORDER BY id", 1), [r for r in now if r["bal"] >= 50], 10) == [r for r in now if r["bal"] >= 50])
            agree.append(until(lambda: q("SELECT owner, total, n FROM per_owner ORDER BY owner", 2), per_owner(), 10) == per_owner())
            rows = {r["id"]: r["_row_id"] for r in q("SELECT id, _row_id FROM acct")}
            kept.append(all(rows.get(k, v) == v for k, v in ids.items() if k in model) and len(set(rows.values())) == len(rows))
            ids.update(rows)
            seen.append(len(model))
    checks["60 random INSERT/UPDATE/DELETE/MERGE on 3 nodes == the model, every node, while tiering and purging"] = all(agree)
    checks["an UPDATE or MERGE keeps a row's _row_id; ids are unique"] = all(kept)
    # a retried job, and changes from `pondra sql` (to the running leader)
    first, again = q("UPDATE acct SET bal = bal + 1000 WHERE id % 2 = 0", 1, job="bump-1"), q("UPDATE acct SET bal = bal + 1000 WHERE id % 2 = 0", 2, job="bump-1")
    for k, v in model.items():
        v[1] += 1000 if k % 2 == 0 else 0
    cli = subprocess.run([BIN, "sql", "--dir", lake, "UPDATE acct SET bal = bal - 1000 WHERE id % 2 = 0"], capture_output=True, text=True, timeout=120)
    for k, v in model.items():
        v[1] -= 1000 if k % 2 == 0 else 0
    checks["a retried job changes rows once; `pondra sql` changes them through the leader"] = "duplicate" in again and "duplicate" not in first and cli.returncode == 0 \
        and until(table, want(), 10) == want()
    # three nodes adding to the same rows at once: none lost
    live = sorted(model)[:20]
    def bump(i):
        for _ in range(5):
            q(f"UPDATE acct SET bal = bal + 1 WHERE id IN ({', '.join(map(str, live))})", i)
    threads = [threading.Thread(target=bump, args=(i,)) for i in range(3)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    for k in live:
        model[k][1] += 15
    checks["three nodes changing the same rows at once: every change counts"] = until(table, want(), 10) == want()
    # the change feed replays to the table, each old version as it was
    state, (pos, pre_ok) = {}, (0, True)
    def mcp_changes(after):
        body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": "changes", "arguments": {"table": "acct", "after": after}}}).encode()
        r = call(A.port, "POST", "/mcp", body, headers={"content-type": "application/json"})
        return json.loads(r["result"]["content"][0]["text"])
    for _ in range(1000):
        got = mcp_changes(pos)
        for r in got["rows"]:
            key, row = r["_row_id"], (r["id"], r["owner"], r["bal"])
            if r["_change_type"] in ("insert", "update_postimage"):
                state[key] = row
            else:  # the version it replaces, as it was
                pre_ok &= state.pop(key, None) == row
        if got["position"] == pos and not got["rows"]:
            break
        pos = got["position"]
    replayed = sorted((i, o, b) for i, o, b in state.values())
    checks["the change feed replays to the table's rows; each old version as it was"] = pre_ok and replayed == [(r["id"], r["owner"], r["bal"]) for r in want()]
    watched = []
    def watch():
        c = http.client.HTTPConnection("127.0.0.1", A.port + 1, timeout=120)  # (nothing comes until the change commits: seconds, on R2)
        c.request("GET", "/watch/acct?changes=true")
        r = c.getresponse()
        while len(watched) < 2:
            watched.append(json.loads(r.readline()))
    w = threading.Thread(target=watch)
    w.start()
    time.sleep(0.5)
    one = sorted(model)[0]
    q(f"UPDATE acct SET owner = 'w' WHERE id = {one}", 2)
    model[one][0] = "w"
    w.join(60)
    checks["/watch?changes=true: an UPDATE as its old and new versions"] = [r["_change_type"] for r in watched] == ["update_preimage", "update_postimage"] \
        and watched[0]["_row_id"] == watched[1]["_row_id"] and watched[1]["owner"] == "w"
    # Delta readers, once the change is tiered and purged
    opts = {} if not A.s3 else {"AWS_ENDPOINT_URL": os.environ["AWS_ENDPOINT"], "AWS_ACCESS_KEY_ID": os.environ["AWS_ACCESS_KEY_ID"], "AWS_SECRET_ACCESS_KEY": os.environ["AWS_SECRET_ACCESS_KEY"],
                                "AWS_REGION": "auto", "AWS_ALLOW_HTTP": os.environ.get("AWS_ALLOW_HTTP", "false")}
    delta = lambda: sorted((r["id"], r["owner"], r["bal"]) for r in deltalake.DeltaTable(f"{lake}/data/acct", storage_options=opts).to_pyarrow_table().select(["id", "owner", "bal"]).to_pylist())
    checks["Delta readers see the changes once they are tiered and purged"] = until(lambda: _try(delta), [(r["id"], r["owner"], r["bal"]) for r in want()], 60) == [(r["id"], r["owner"], r["bal"]) for r in want()]
    # spread over the nodes: a changed table in pieces, its old rows in files, the log and $deleted
    q("CREATE TABLE big (id BIGINT, k BIGINT, v DOUBLE)")
    for i in range(6):
        q(f"INSERT INTO big SELECT value + {i * 20000}, value % 50, value * 0.5 FROM generate_series(1, 20000)")
    q("UPDATE big SET v = v * 2 WHERE k < 10")
    q("DELETE FROM big WHERE k >= 45")
    q("MERGE INTO big t USING (SELECT id, k FROM big WHERE k = 20) s ON t.id = s.id WHEN MATCHED THEN DELETE")
    spread = []
    for s in ("SELECT count(*) AS n, sum(v) AS s FROM big", "SELECT k, count(*) AS n, sum(v) AS s FROM big GROUP BY k ORDER BY k",
              "SELECT count(*) AS n, sum(b.v + a.bal) AS s FROM big b JOIN acct a ON b.k = a.id"):
        before = metrics_of(A.port)["pondra_spread_queries_total"]
        spread.append((q(s, spread=0), q(s, spread=1), metrics_of(A.port)["pondra_spread_queries_total"] > before))
    n_big = q("SELECT count(*) AS n, sum(v) AS s FROM big")[0]
    expect_n = sum(1 for i in range(1, 120001) if not (i % 20000 % 50 >= 45 or i % 20000 % 50 == 20))
    checks["spread over three nodes == one node, over the changed tables"] = all(a == b and ran for a, b, ran in spread) and n_big["n"] == expect_n
    # views: what can't follow is refused; a view's table isn't changed by hand
    q("CREATE MATERIALIZED VIEW top AS SELECT owner, max(bal) AS m FROM acct GROUP BY owner")
    refused = [err("UPDATE acct SET bal = 0 WHERE id = 1"), err("UPDATE rich SET bal = 0"), err("UPDATE acct SET _row_id = 1"),
               err("MERGE INTO acct t USING (VALUES (1, 'x', 1.0), (1, 'y', 2.0)) AS s(id, owner, bal) ON t.id = s.id WHEN MATCHED THEN UPDATE SET bal = s.bal")]
    q("DROP MATERIALIZED VIEW top")
    checks["refused: a change under a view keeping a max; a view's table; a system column; a MERGE matching a row twice"] = \
        "view top (keeps a min or max)" in (refused[0] or "") and "view's" in (refused[1] or "") and all(refused[2:]) and until(table, want(), 10) == want()
    # keyed tables: an UPDATE keeps the row's id; MERGE updates, inserts and deletes
    q("CREATE TABLE kv (k BIGINT, v BIGINT, _deleted BOOLEAN, PRIMARY KEY (k))")
    q("INSERT INTO kv VALUES (1, 10, false), (2, 20, false)")
    before = {r["k"]: r["_row_id"] for r in q("SELECT k, _row_id FROM kv")}
    q("UPDATE kv SET v = v + 1 WHERE k = 1", 1)
    q("MERGE INTO kv t USING (VALUES (1, 100), (3, 300), (2, 0)) AS s(k, v) ON t.k = s.k WHEN MATCHED AND s.v = 0 THEN DELETE WHEN MATCHED THEN UPDATE SET v = s.v WHEN NOT MATCHED THEN INSERT (k, v) VALUES (s.k, s.v)", 2)
    after = q("SELECT k, v, _row_id FROM kv ORDER BY k")
    checks["keyed tables: UPDATE and MERGE keep a row's _row_id; MERGE inserts and deletes"] = [(r["k"], r["v"]) for r in after] == [(1, 100), (3, 300)] and after[0]["_row_id"] == before[1] \
        and after[1]["_row_id"] not in before.values()
    # a new node, and every node after a restart, reads the same
    nodes[1].kill()
    nodes[1].start(tries=1)
    checks["after a restart, the same rows"] = until(lambda: table(1), want(), 30) == want()
    [n.kill() for n in nodes]
    ok = all(checks.values())
    print(json.dumps({"changes": checks, "ok": ok}, indent=1))
    if not ok:
        print(agree, kept, first, again, cli.stderr[-500:], replayed[:5], want()[:5], watched, spread, n_big, expect_n, refused, after, before)
        sys.exit(1)
    return f"changes: 60 random changes on 3 nodes == the model, rows keep their ids, views and the change feed follow, Delta sees them purged, spread == one node: all {len(checks)} checks pass"


def guard():
    """A query spreads over the nodes only when it pays (`guard.rs`): what it would move, at the
    link's speed, against the time it takes on one node less a node's share. The same queries
    through a node that sees a slow network (PONDRA_LINK=40,60: 40 ms, 60 MB/s, as between GitHub's
    runners) stay on it when they would shuffle, and through one that sees a fast one spread; `?spread=1` spreads anyway;
    a query nothing is known about yet stays on one node; the answers are the same every way."""
    lake = new_lake()
    small = {"PONDRA_SPREAD_MB": "1"}  # (tables this small are left on one node by size alone)
    slow = Node(lake, A.port, env={"PONDRA_LINK": "40,60", **small}).start()
    fast = Node(lake, A.port + 1, env={"PONDRA_LINK": "0.2,5000", **small}).start()
    third = Node(lake, A.port + 2, env=small).start()
    n = itertools.count()  # (a comment makes each ask new to the result cache; the guard knows it as the same query)
    q = lambda s, port, spread=None: call(port, "POST", "/sql" + ("" if spread is None else f"?spread={spread}"), f"{s} -- {next(n)}".encode(), timeout=300)
    q("CREATE TABLE f (id BIGINT, k BIGINT, v DOUBLE, p BIGINT) WITH (partition_by = 'p')", A.port)  # (a file per partition, merged or not)
    q("CREATE TABLE g (id BIGINT, w DOUBLE)", A.port)
    for i in range(8):
        q(f"INSERT INTO f SELECT value + {i * 500000}, value % 100000, value * 0.5, value % 4 FROM generate_series(1, 500000)", A.port)
        q(f"INSERT INTO g SELECT value * 7 + {i * 3500000}, value * 0.25 FROM generate_series(1, 500000)", A.port)
    time.sleep(1)
    spreads = lambda port: metrics_of(port)["pondra_spread_queries_total"]
    queries = ["SELECT k, count(*) AS n, sum(v) AS s FROM f GROUP BY k ORDER BY n DESC, k LIMIT 5",
               "SELECT count(*) AS n, sum(f.v + g.w) AS s FROM f JOIN g ON f.k = g.id",
               "SELECT count(DISTINCT v) AS n FROM f"]
    out = {}
    for first, s in zip((True, False, False), queries):
        runs = {}
        for name, node in (("slow", slow), ("fast", fast)):
            before = spreads(node.port)
            unknown = q(s, node.port)  # (the first query: nothing known yet, it stays here)
            stayed = spreads(node.port) == before or not first
            one = q(s, node.port, 0)
            before = spreads(node.port)
            auto = q(s, node.port)
            runs[name] = (auto == one == unknown, spreads(node.port) > before, stayed)
        before, shuffles = spreads(slow.port), metrics_of(slow.port)["pondra_shuffled_queries_total"]
        forced = q(s, slow.port, 1)
        runs["forced"] = (forced == q(s, fast.port, 0), spreads(slow.port) > before, metrics_of(slow.port)["pondra_shuffled_queries_total"] > shuffles)
        out[s[:40]] = runs
    # A node on the real network (no PONDRA_LINK): once a query ran both ways, the faster way wins.
    learned = []
    for s in queries:
        timed = lambda spread: (lambda t: (q(s, third.port, spread), time.time() - t)[1])(time.time())
        here, spread = min(timed(0) for _ in range(2)), min(timed(1) for _ in range(2))
        before = spreads(third.port)
        q(s, third.port)
        went = spreads(third.port) > before
        if max(here, spread) > 1.5 * min(here, spread):  # (clearly apart: else either is right)
            learned.append(went == (spread < here))
    [n.kill() for n in (slow, fast, third)]
    checks = {"the same answers every way": all(r["slow"][0] and r["fast"][0] and r["forced"][0] for r in out.values()),
              "a query nothing is known about stays on one node": all(r["slow"][2] and r["fast"][2] for r in out.values()),
              # (a query that moves nothing — its tables split by a key's ranges — may pay even there: on R2 a node's reads are slow)
              "over a slow network, queries that would shuffle stay on one node": not any(r["slow"][1] for r in out.values() if r["forced"][2]) and any(r["forced"][2] for r in out.values()),
              "over a fast one, they spread": all(r["fast"][1] for r in out.values()),
              "?spread=1 spreads anyway": all(r["forced"][1] for r in out.values()),
              "a query that ran both ways goes the faster way": all(learned)}
    ok = all(checks.values())
    print(json.dumps({"guard": checks, "ok": ok, "learned": learned}, indent=1))
    if not ok:
        print(out)
        sys.exit(1)
    return f"guard: {len(queries)} queries stay on one node over a slow network and spread over a fast one, forced ones spread, the same answers: all {len(checks)} checks pass"


def _try(f):
    """f(), or None if it raises."""
    try:
        return f()
    except Exception:
        return None


def _raises(f):
    try:
        f()
        return False
    except Exception:
        return True


def load():
    lake = new_lake()
    node = Node(lake, A.port, flush_ms=A.flush_ms, tier_secs=10).start()
    port, stop, lat, fresh, sent = A.port, threading.Event(), [], [], [0]
    events_table(port)
    call(port, "POST", "/tables/probe", json.dumps([["id", "Int64"], ["ts", "Float64"]]).encode())

    def pump(name):
        seq = 0
        while not stop.is_set():
            seq += 1
            t = time.time()
            call(port, "POST", f"/append/events?producer={name}&seq={seq}", rows(name, seq, A.size))
            lat.append(time.time() - t)
            sent[0] += A.size

    def probe():  # freshness = time from send until a SQL query sees the row
        k = 0
        while not stop.is_set():
            k += 1
            t = time.time()
            threading.Thread(target=call, args=(port, "POST", f"/append/probe?producer=probe&seq={k}", f'{{"id":{k},"ts":{t}}}\n'.encode())).start()
            while sql(port, f"SELECT count(*) AS c FROM probe WHERE id = {k}")[0]["c"] == 0:
                time.sleep(0.01)
            fresh.append(time.time() - t)
            time.sleep(random.uniform(0.05, 0.5))  # random phase, so probes don't lock onto the flush cycle

    cpu = lambda: sum(int(x) for x in open(f"/proc/{node.p.pid}/stat").read().split()[13:15]) / os.sysconf("SC_CLK_TCK")
    cpu0 = cpu()
    threads = [threading.Thread(target=pump, args=(f"load{k}",)) for k in range(A.producers)] + [threading.Thread(target=probe)]
    [t.start() for t in threads]
    time.sleep(A.secs)
    stop.set()
    [t.join() for t in threads]
    st, used = call(port, "GET", "/stats"), cpu() - cpu0
    node.kill()
    res = {"events_per_s": round(sent[0] / A.secs), "ack_ms_p50": pct(lat, .5), "ack_ms_p99": pct(lat, .99),
           "freshness_ms_p50": pct(fresh, .5), "freshness_ms_p99": pct(fresh, .99), "freshness_samples": len(fresh),
           "commits_per_s": round(st["commits"] / A.secs, 1), "commit_ms_p50": round(st["commit_ms_p50"]), "commit_ms_p95": round(st["commit_ms_p95"]),
           "server_cores_used": round(used / A.secs, 2)}
    print(json.dumps(res, indent=1))
    return res


def files():
    """Few objects (ADR-021): a bucket bills, and rate-limits, every request. An INSERT … VALUES goes
    through the log, so one-row INSERTs make a Parquet file per tiering round, not one each; the
    catalog's write-ahead log is cleared every minute (here every 2 s: PONDRA_GC_SECS), not every
    10; `/metrics` counts the writes. 200 one-row INSERTs into a table that publishes Delta."""
    lake = new_lake()
    node = Node(lake, A.port, env={"PONDRA_GC_SECS": "2"}).start()  # (tiering as by default: at most every 10 s)
    q = lambda s: sql(A.port, s)
    q("CREATE TABLE ev (user_id BIGINT, amount DOUBLE) WITH (publish = 'delta')")
    seen, stop = {"ev": set(), "big": set()}, threading.Event()
    def watch():  # (every data file that ever appears, merged away or not)
        while not stop.is_set():
            for t in seen:
                seen[t].update(k for k in lake_objects(lake, f"data/{t}/") if k.endswith(".parquet"))
            stop.wait(0.5 if lake.startswith("s3://") else 0.05)
    watcher = threading.Thread(target=watch, daemon=True)
    watcher.start()
    writes = lambda: metrics_of(A.port).get('pondra_object_requests_total{op="write"}', float("nan"))
    before, t0 = writes(), time.time()
    for i in range(200):
        q(f"INSERT INTO ev VALUES ({i}, {i * 0.5})")
    secs, made = time.time() - t0, len(seen["ev"])
    per_insert = (writes() - before) / 200  # (with what the node writes meanwhile anyway: tiering, the catalog's own files)
    got = q("SELECT count(*) AS n, sum(user_id) AS s FROM ev")
    # A bulk INSERT from another process (`pondra sql`) gets its row ids from the leader first, so
    # the leader records its files as written rather than rewriting them.
    q("CREATE TABLE big (id BIGINT, v BIGINT)")
    subprocess.run([BIN, "sql", "--dir", lake, "INSERT INTO big SELECT value, value * 2 FROM range(0, 3000000)"], check=True, capture_output=True, env=node.env)
    time.sleep(1.5)
    big = {k for k in lake_objects(lake, "data/big/") if k.endswith(".parquet")}  # (a rewrite would have put new files in place of those written)
    call(A.port, "POST", "/tier", timeout=120)
    time.sleep(8)  # (the write-ahead log's cleaner: every 2 s, for objects 2 s old)
    stop.set()
    watcher.join()
    wal = len(lake_objects(lake, "catalog/wal/"))
    node.kill()
    checks = {"all rows": got == [{"n": 200, "s": 19900}], "a Parquet file per tiering round (every 10 s), not per INSERT": made <= 3 + secs / 10,
              "about one object write per INSERT (≤ 1.2, and ≤ 2 a second meanwhile)": 200 * per_insert <= 240 + 2 * secs,
              "write-ahead log objects left (≤ 30)": wal <= 30, "a pondra sql INSERT's files recorded as written": seen["big"] == big and len(big) > 0}
    out = {"passed": all(checks.values()), "checks": checks, "secs": round(secs, 1), "parquet_files_made": made, "writes_per_insert": round(per_insert, 2),
           "wal_objects_left": wal, "cli_insert": {"files": len(big), "files_seen": len(seen["big"])}}
    print(json.dumps({"files": out}))
    if not out["passed"]:
        sys.exit(1)
    return out


def layouts():
    """A table with a PRIMARY KEY takes partition_by and cluster_by too (ADR-021), as written in the
    owner's shell: each tiering round's newest rows go into a file per day, sorted by user, then
    id, and a newer round's row for a key shadows an older round's whatever its day. Rows move
    between days (an UPDATE of ts) over 9 rounds, against a model; every file holds one day, sorted
    by user; a lookup finds a moved row where it moved to; once the table is compacted, Delta
    readers (delta-rs) see what Pondra sees."""
    lake = new_lake()
    node = Node(lake, A.port, tier_secs=0).start()
    q = lambda s: sql(A.port, s)
    q("""CREATE TABLE t ( id BIGINT PRIMARY KEY, user text, ts timestamp, v BIGINT ) WITH ( publish = 'delta,iceberg' ,cluster_by = 'user' ,partition_by = 'day(ts)' )""")
    day = lambda d: f"TIMESTAMP '2026-09-0{d} 12:00:00'"
    model = {i: (f"u{i % 7}", 1 + i % 3, i) for i in range(300)}
    q("INSERT INTO t VALUES " + ", ".join(f"({i}, '{u}', {day(d)}, {v})" for i, (u, d, v) in model.items()))
    tier = lambda: call(A.port, "POST", "/tier", timeout=300)
    now = lambda: {r["id"]: (r["u"], r["d"], r["v"]) for r in q('SELECT id, "user" AS u, CAST(date_part(\'day\', ts) AS BIGINT) AS d, v FROM t')}
    tier()
    rounds_ok = [now() == model]
    for r in range(1, 10):
        q(f"UPDATE t SET ts = {day(4)}, v = v + 1000 WHERE id % 10 = {r}")  # (to another day's files)
        model.update({i: (u, 4, v + 1000) for i, (u, d, v) in model.items() if i % 10 == r})
        q(f"INSERT INTO t VALUES ({1000 + r}, 'u{r}', {day(5)}, {r})")
        model[1000 + r] = (f"u{r}", 5, r)
        tier()
        rounds_ok.append(now() == model)
    moved = call(A.port, "GET", "/lookup/t/11")
    q("CHECKPOINT")  # (other engines see a keyed table as of its last compaction: this one, now)
    files_ok = None
    if not lake.startswith("s3://"):
        import glob, pyarrow.parquet as pq
        files_ok = True
        for f in glob.glob(os.path.join(lake, "data", "t", "*.parquet")):
            b = pq.read_table(f, columns=["user", "id", "ts"])
            users, days = b.column("user").to_pylist(), {str(t)[:10] for t in b.column("ts").to_pylist() if t is not None}
            files_ok &= users == sorted(users) and len(days) <= 1
    import deltalake
    endpoint = os.environ.get("AWS_ENDPOINT", "")
    opts = {} if not lake.startswith("s3://") else {k: v for k, v in {"AWS_ENDPOINT_URL": endpoint, "AWS_REGION": os.environ.get("AWS_REGION", "auto"), "AWS_ALLOW_HTTP": "true" if endpoint.startswith("http://") else ""}.items() if v}
    theirs = deltalake.DeltaTable(os.path.join(lake, "data", "t") if not lake.startswith("s3://") else lake + "/data/t", storage_options=opts or None).to_pyarrow_table()
    ours = q("SELECT count(*) AS n, sum(v) AS s FROM t")[0]
    node.kill()
    checks = {"every round as the model": all(rounds_ok), "files: one day each, sorted by user": files_ok is not False,
              "lookup of a moved row": moved and moved[0]["v"] == model[11][2] and str(moved[0]["ts"]).startswith("2026-09-04"),
              "delta-rs sees what Pondra sees": theirs.num_rows == ours["n"] == len(model) and sum(theirs.column("v").to_pylist()) == ours["s"]}
    out = {"passed": all(checks.values()), "checks": checks, "rounds": len(rounds_ok), "rows": len(model), "delta_rows": theirs.num_rows, "pondra": ours}
    print(json.dumps({"layouts": out}))
    if not out["passed"]:
        sys.exit(1)
    return out


def clusters():
    """cluster_by over two columns orders files along a Hilbert curve (ADR-021): each row group
    then holds a narrow range of both columns, where rows sorted by (a, b) would give every row
    group the whole of b. An append table and a keyed table, 2 M rows of two independent random
    columns each, folded from the log; every file's row groups' min and max, read back."""
    lake = new_lake()
    node = Node(lake, A.port, tier_secs=0).start()
    import pyarrow as pa, pyarrow.parquet as pq
    q = lambda s: sql(A.port, s)
    q("CREATE TABLE app (id BIGINT, a BIGINT, b DOUBLE) WITH (cluster_by = 'a, b')")
    q("CREATE TABLE kv (id BIGINT PRIMARY KEY, a BIGINT, b DOUBLE) WITH (cluster_by = 'a, b')")
    rng = random.Random(11)
    for seq in range(8):
        n = 250_000
        rb = pa.record_batch([pa.array(range(seq * n, seq * n + n), pa.int64()), pa.array([rng.randrange(10**6) for _ in range(n)], pa.int64()),
                              pa.array([rng.random() for _ in range(n)], pa.float64())], names=["id", "a", "b"])
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, rb.schema) as w:
            w.write_batch(rb)
        for t in ("app", "kv"):
            call(A.port, "POST", f"/append/{t}?producer=p-{t}&seq={seq + 1}", sink.getvalue().to_pybytes(), headers={"content-type": "application/vnd.apache.arrow.stream"}, timeout=300)
    call(A.port, "POST", "/tier", timeout=600)
    spans = {}
    for t in ("app", "kv"):
        files = [f"data/{t}/" + k.split("/")[-1] for k in lake_objects(lake, f"data/{t}/") if k.endswith(".parquet")]
        width = {}
        for f in files:
            data = open(os.path.join(lake, f), "rb").read() if not lake.startswith("s3://") else S3[0].get_object(Bucket=lake[5:].split("/", 1)[0], Key=lake[5:].split("/", 1)[1] + "/" + f)["Body"].read()
            md = pq.ParquetFile(pa.BufferReader(data)).metadata
            names = [md.schema.column(i).name for i in range(md.num_columns)]
            for c in ("a", "b"):
                col = names.index(c)
                ranges = [(md.row_group(g).column(col).statistics.min, md.row_group(g).column(col).statistics.max) for g in range(md.num_row_groups)]
                lo, hi = min(r[0] for r in ranges), max(r[1] for r in ranges)
                width.setdefault(c, []).extend((r[1] - r[0]) / (hi - lo) for r in ranges if md.num_row_groups > 1)
        spans[t] = {c: round(sum(w) / len(w), 2) for c, w in width.items() if w}
    rows = q("SELECT (SELECT count(*) FROM app) AS app, (SELECT count(*) FROM kv) AS kv")[0]
    node.kill()
    checks = {"all rows": rows == {"app": 2_000_000, "kv": 2_000_000},
              "row groups narrow in a and in b (each < 0.75 of the whole)": all(len(v) == 2 and max(v.values()) < 0.75 for v in spans.values())}
    out = {"passed": all(checks.values()), "checks": checks, "row_group_span": spans, "rows": rows}
    print(json.dumps({"clusters": out}))
    if not out["passed"]:
        sys.exit(1)
    return out


def copies():
    """COPY over the Postgres protocol (ADR-021): `COPY … FROM STDIN` loads text (psycopg's
    write_row, NULLs included) and CSV with a header and quoted commas; `COPY … TO STDOUT` sends
    text, CSV and binary, each read back as the rows went in; the ADBC Postgres driver, which reads
    every result as `COPY (query) TO STDOUT (FORMAT binary)`, gets them as Arrow."""
    import io, psycopg, adbc_driver_postgresql.dbapi as adbc
    lake, pg = new_lake(), A.port + 10
    node = Node(lake, A.port, pg=f"127.0.0.1:{pg}").start()
    dsn = f"host=127.0.0.1 port={pg} user=u dbname=lake"
    want = [(i, None if i % 10 == 0 else f"n, {i}", i * 0.5) for i in range(2000)]
    with psycopg.connect(dsn, autocommit=True) as c:
        cur = c.cursor()
        cur.execute("CREATE TABLE ev (id BIGINT, name VARCHAR, amount DOUBLE)")
        with cur.copy("COPY ev FROM STDIN") as cp:
            for r in want[:1000]:
                cp.write_row(r)
        text_in = cur.rowcount
        csv = "id,name,amount\n" + "".join(f'{i},{"" if n is None else chr(34) + n + chr(34)},{a}\n' for i, n, a in want[1000:])
        with cur.copy("COPY ev FROM STDIN WITH (FORMAT csv, HEADER true)") as cp:
            cp.write(csv)
        csv_in = cur.rowcount
        got = {}
        for fmt in ("text", "binary"):
            with cur.copy(f"COPY (SELECT id, name, amount FROM ev ORDER BY id) TO STDOUT WITH (FORMAT {fmt})") as cp:
                cp.set_types(["int8", "text", "float8"])
                got[fmt] = list(cp.rows())
        import csv as csvlib
        with cur.copy("COPY (SELECT id, name, amount FROM ev ORDER BY id) TO STDOUT WITH (FORMAT csv)") as cp:
            text = b"".join(cp).decode()
        got["csv"] = [(int(i), n or None, float(a)) for i, n, a in csvlib.reader(io.StringIO(text))]
        # DECIMAL goes as NUMERIC, text and binary alike (the ADBC driver reads NUMERIC's binary form)
        import decimal
        money = "SELECT CAST(amount - 500 AS DECIMAL(12, 3)) AS m FROM ev ORDER BY id"
        numeric = [c.execute(money).fetchall(), c.cursor(binary=True).execute(money).fetchall()]
        numeric_ok = all([r[0] for r in rows] == [decimal.Decimal(f"{a - 500:.3f}") for _, _, a in want] for rows in numeric)
    with adbc.connect(f"postgresql://u@127.0.0.1:{pg}/lake") as c, c.cursor() as cur:
        cur.execute("SELECT id, name, amount FROM ev ORDER BY id")
        arrow = cur.fetch_arrow_table()
    node.kill()
    checks = {"COPY FROM STDIN, text and CSV": (text_in, csv_in) == (1000, 1000),
              **{f"COPY TO STDOUT, {f}": got[f] == want for f in got},
              "ADBC Postgres driver (Arrow)": [tuple(r.values()) for r in arrow.to_pylist()] == want,
              "DECIMAL as NUMERIC, text and binary": numeric_ok}
    out = {"passed": all(checks.values()), "checks": checks}
    print(json.dumps({"copies": out}))
    if not out["passed"]:
        sys.exit(1)
    return out


def streams():
    """Flink's stream operators (ADR-021). A join of two streams: orders and payments arrive in any
    order, over two nodes; a payment pairs with its order whichever came first, when it's within
    10 minutes of it, each pair once (the leader keeps the view up to date right after commits,
    `join = 'streams'`), against a model — through a leader restart. Sliding windows: 5-minute
    windows every minute (`slide_secs`), each emitted once with every click it covers."""
    import datetime
    lake = new_lake()
    a = Node(lake, A.port, tier_secs=1).start()
    b = Node(lake, A.port + 1, tier_secs=1).start()
    q = lambda s, port=A.port: sql(port, s)
    base = 1_790_000_000 // 3600 * 3600
    iso = lambda s: datetime.datetime.fromtimestamp(s, datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    q("CREATE TABLE orders (id BIGINT, amount DOUBLE, ts TIMESTAMP)")
    q("CREATE TABLE payments (order_id BIGINT, paid DOUBLE, ts TIMESTAMP)")
    q("""CREATE MATERIALIZED VIEW paid WITH (join = 'streams', time = 'ts', within_secs = 600) AS
         SELECT o.id, o.amount, p.paid FROM orders o JOIN payments p ON o.id = p.order_id AND p.ts BETWEEN o.ts AND o.ts + INTERVAL '10 minutes'""")
    rng, want, orders, payments = random.Random(5), set(), [], []
    for i in range(300):
        t = base + i * 7
        orders.append((i, float(i), t))
        late = rng.choice([30, 300, 900])  # (15 minutes late: too late, no pair)
        payments.append((i, i + 0.5, t + late))
        if late <= 600:
            want.add((i, float(i), i + 0.5))
    events = [("orders", o) for o in orders] + [("payments", p) for p in payments]
    rng.shuffle(events)  # (a payment may come before its order)
    def write(s, port):  # (retried through the restart below: a job id makes a retry a no-op)
        job, deadline = uuid.uuid4().hex, time.time() + 90
        while True:
            try:
                return call(port, "POST", f"/sql?job={job}", s.encode())
            except Exception:
                if time.time() > deadline:
                    raise
                time.sleep(0.5)
    for n, chunk in enumerate(range(0, len(events), 40)):
        port = (A.port, A.port + 1)[n % 2]
        for table in ("orders", "payments"):
            rows = [r for t, r in events[chunk:chunk + 40] if t == table]
            if rows:
                write(f"INSERT INTO {table} VALUES " + ", ".join(f"({r[0]}, {r[1]}, TIMESTAMP '{iso(r[2])}')" for r in rows), port)
        if n == 7:
            a.kill(); a.start()  # (the leader again: nothing paired twice, nothing lost)
    got = lambda: sorted((r["id"], r["amount"], r["paid"]) for r in q("SELECT id, amount, paid FROM paid"))
    pairs = until(got, sorted(want), secs=90)
    # Sliding windows: 5 minutes long, one a minute; a click at minute m is in windows m-4..m.
    q("CREATE TABLE clicks (user VARCHAR, ts TIMESTAMP)")
    q("""CREATE MATERIALIZED VIEW per5 WITH (window = 'w', size_secs = 300, slide_secs = 60) AS
         SELECT date_bin(INTERVAL '1 minute', ts) AS w, user, count(*) AS n FROM clicks GROUP BY 1, 2""")
    clicks = [("a" if i % 3 else "b", base + i * 13) for i in range(100)]  # (21 minutes)
    q("INSERT INTO clicks VALUES " + ", ".join(f"('{u}', TIMESTAMP '{iso(t)}')" for u, t in clicks))
    q(f"INSERT INTO clicks VALUES ('z', TIMESTAMP '{iso(base + 3600)}')")  # (the watermark an hour on: every window closes)
    final = lambda: {(r["w"][:19].replace("T", " "), r["user"]): r["n"] for r in q("SELECT w, user, n FROM per5_final")}
    model = {}
    for u, t in clicks:
        m = (t - base) // 60
        for start in range(m - 4, m + 1):
            model[(iso(base + start * 60), u)] = model.get((iso(base + start * 60), u), 0) + 1
    emitted = until(lambda: len(final()), len(model), secs=60)
    windows = final()
    a.kill(); b.kill()
    checks = {"stream join: each pair once, whichever side came first": pairs == sorted(want),
              "sliding windows: every window, every click it covers": windows == model}
    out = {"passed": all(checks.values()), "checks": checks, "pairs": len(want), "windows": len(model)}
    print(json.dumps({"streams": out}))
    if not out["passed"]:
        print(len(pairs), len(want), sorted(set(map(tuple, pairs)) ^ want)[:5], emitted, sorted(set(windows.items()) ^ set(model.items()))[:6])
        sys.exit(1)
    return out


def columns():
    """The rest of ALTER TABLE (ADR-022): RENAME COLUMN, DROP COLUMN, a dropped name added again,
    ALTER COLUMN … TYPE (widening), while rows stream into two nodes and tiering runs; a writer
    still using a renamed column's old name has it left out, never taken for the column now stored
    under it. Against a model: every node, a query spread over three nodes, a bulk INSERT, UPDATE,
    a keyed table and its lookups, Delta and Iceberg readers. Refused: key and partition columns,
    columns a view reads, narrowing, RENAME TABLE (with the way round it)."""
    lake = new_lake()
    ports = [A.port, A.port + 1, A.port + 2]
    nodes = [Node(lake, p, tier_secs=0.3, publish="delta,iceberg").start() for p in ports]
    q = lambda s, port=A.port: sql(port, s)
    q("CREATE TABLE events (id BIGINT, user VARCHAR, amount INT, note VARCHAR)")
    model, now = {}, {"total": "amount", "note": "note", "big": False}  # (the names writers use now)
    pause, paused, stop, seq = threading.Event(), threading.Event(), threading.Event(), [0]
    def send(port, names=None, n=50):
        names = names or now
        seq[0] += 1
        rows = []
        for _ in range(n):
            i = len(model) + 1
            v = 5_000_000_000 + i if names["big"] else i
            r = {"id": i, "user": f"u{i % 7}", names["total"]: v}
            if names["note"]:
                r[names["note"]] = f"n{i}"
            rows.append(json.dumps(r) + "\n")
            # (an old name is left out: its column reads null)
            model[i] = {"total": v if names["total"] == now["total"] else None, "note": r.get(now["note"]) if names["note"] == now["note"] and now["note"] else None}
        call(port, "POST", f"/append/events?producer=p&seq={seq[0]}", "".join(rows).encode())
    def produce():
        k = 0
        while not stop.is_set():
            if pause.is_set():
                paused.set(); time.sleep(0.01); continue
            send(ports[k % 2]); k += 1
    def alter(stmt, want):  # (the writers wait: rows cross the change in the log and in files)
        pause.set(); paused.wait(); paused.clear()
        out = q(stmt, ports[1])
        until(lambda: all([c["column_name"] for c in q("DESCRIBE events", p)] == want for p in ports), True, secs=10)
        return out
    t = threading.Thread(target=produce, daemon=True); t.start()
    time.sleep(1.5)
    alter("ALTER TABLE events RENAME COLUMN amount TO total", ["id", "user", "total", "note"])
    now["total"] = "total"
    send(ports[0], {"total": "amount", "note": "note", "big": False})  # (a writer from before the rename)
    pause.clear(); time.sleep(1.2)
    alter("ALTER TABLE events DROP COLUMN note", ["id", "user", "total"])
    now["note"] = None
    for r in model.values():
        r["note"] = None  # (dropped: gone for good)
    pause.clear(); time.sleep(1)
    alter("ALTER TABLE events ADD COLUMN note VARCHAR", ["id", "user", "total", "note"])
    now["note"] = "note"
    pause.clear(); time.sleep(0.8)
    pause.set(); paused.wait(); paused.clear()
    q("INSERT INTO events SELECT id + 1000000, user, total, note FROM events WHERE id <= 20", ports[2])  # (bulk: Parquet under stored names)
    for i in range(1, 21):
        model[i + 1_000_000] = dict(model[i])
    pause.clear(); time.sleep(0.5)
    alter("ALTER TABLE events ALTER COLUMN total TYPE BIGINT", ["id", "user", "total", "note"])
    now["big"] = True
    pause.clear(); time.sleep(1)
    stop.set(); t.join()
    q("UPDATE events SET total = total + 1 WHERE id <= 5")
    for i in range(1, 6):
        model[i]["total"] = model[i]["total"] + 1 if model[i]["total"] is not None else None
    # A keyed table: renamed, updated, looked up, a column dropped.
    q("CREATE TABLE users (id BIGINT PRIMARY KEY, name VARCHAR, score INT)")
    q("INSERT INTO users VALUES (1, 'ann', 5), (2, 'bob', 7)")
    q("ALTER TABLE users RENAME COLUMN score TO points")
    q("UPDATE users SET points = points + 1 WHERE id = 1")
    lookup = call(A.port, "GET", "/lookup/users/1")
    q("ALTER TABLE users DROP COLUMN name")
    q("CREATE TABLE clicks (user VARCHAR, ts TIMESTAMP) WITH (partition_by = 'day(ts)')")
    q("CREATE MATERIALIZED VIEW per_user AS SELECT user, count(*) AS n FROM clicks GROUP BY user")
    refused = {
        "a key column": _raises(lambda: q("ALTER TABLE users DROP COLUMN id")),
        "narrowing": _raises(lambda: q("ALTER TABLE events ALTER COLUMN total TYPE INT")),
        "a column a view reads": _raises(lambda: q("ALTER TABLE clicks RENAME COLUMN user TO who")),
        "a partition column": (q("DROP MATERIALIZED VIEW per_user"), _raises(lambda: q("ALTER TABLE clicks DROP COLUMN ts")))[1],
        "a clash": _raises(lambda: q("ALTER TABLE events RENAME COLUMN user TO id")),
        "a system column": _raises(lambda: q("ALTER TABLE events RENAME COLUMN _row_id TO r")),
    }
    try:
        q("ALTER TABLE events RENAME TO events2"); refused["RENAME TABLE, with the way round it"] = False
    except Exception as e:
        refused["RENAME TABLE, with the way round it"] = "CREATE TABLE events2 AS SELECT" in str(e)
    q("CHECKPOINT")
    want = sorted((i, r["total"], r["note"]) for i, r in model.items())
    total = sum(r["total"] or 0 for r in model.values())
    got = lambda port: sorted((r["id"], r.get("total"), r.get("note")) for r in q("SELECT id, total, note FROM events", port))
    one = q("SELECT user, sum(total) AS s, count(note) AS n FROM events GROUP BY user ORDER BY user")
    spread = call(A.port, "POST", "/sql?spread=1", b"SELECT user, sum(total) AS s, count(note) AS n FROM events GROUP BY user ORDER BY user")
    joined = "SELECT count(*) AS n FROM events a JOIN events b ON a.total = b.total AND a.id <> b.id"
    checks = {
        "every node == the model": all(got(p) == want for p in ports),
        "SELECT * has SQL's columns": list(q("SELECT * FROM events WHERE id = 1")[0]) == ["id", "user", "total"] or list(q("SELECT * FROM events WHERE id = 1")[0]) == ["id", "user", "total", "note"],
        "spread over three nodes == one node": spread == one and call(A.port, "POST", "/sql?spread=1", joined.encode()) == q(joined),
        "a keyed table: renamed, updated, looked up": lookup == [{"id": 1, "name": "ann", "points": 6}] and q("SELECT * FROM users ORDER BY id") == [{"id": 1, "points": 6}, {"id": 2, "points": 7}],
        **{f"refused: {k}": v for k, v in refused.items()},
    }
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    import open_check, pyarrow as pa
    until(lambda: open_check.duck(lake).execute(f"SELECT count(*) FROM delta_scan('{lake}/data/events')").fetchone()[0], len(model), secs=30)
    duck = open_check.duck(lake)
    theirs = {}
    for name, scan in (("duckdb/delta", f"delta_scan('{lake}/data/events')"), ("duckdb/iceberg", f"iceberg_scan('{open_check.iceberg_metadata(lake, 'events')}')")):
        try:
            theirs[name] = list(duck.execute(f"SELECT count(*), sum(total), count(note) FROM {scan}").fetchone())
        except Exception as e:
            theirs[name] = f"error: {str(e)[:120]}"
    try:
        from pyiceberg.table import StaticTable
        t = StaticTable.from_metadata(open_check.iceberg_metadata(lake, "events"), properties=open_check.iceberg_props(lake) if A.s3 else {}).scan().to_arrow()
        theirs["pyiceberg"] = [t.num_rows, pa.compute.sum(t["total"]).as_py(), t.num_rows - t["note"].null_count]
    except Exception as e:
        theirs["pyiceberg"] = f"error: {str(e)[:120]}"
    try:
        import deltalake
        from deltalake import QueryBuilder
        opts = {"AWS_ENDPOINT_URL": os.environ["AWS_ENDPOINT"], "AWS_ACCESS_KEY_ID": os.environ["AWS_ACCESS_KEY_ID"], "AWS_SECRET_ACCESS_KEY": os.environ["AWS_SECRET_ACCESS_KEY"],
                "AWS_REGION": "auto", "AWS_ALLOW_HTTP": "true"} if A.s3 else None
        dt = deltalake.DeltaTable(f"{lake}/data/events", storage_options=opts)  # (its DataFusion reader maps columns; the pyarrow one refuses)
        r = pa.table(QueryBuilder().register("t", dt).execute("SELECT count(*) AS n, sum(total) AS s, count(note) AS c FROM t").read_all()).to_pylist()[0]
        theirs["delta-rs"] = [r["n"], r["s"], r["c"]]
    except Exception as e:
        theirs["delta-rs"] = f"error: {str(e)[:120]}"
    try:  # (its pyarrow reader can't map columns: it must say so, not read them by the wrong names)
        t = deltalake.DeltaTable(f"{lake}/data/events", storage_options=opts).to_pyarrow_table()
        theirs["delta-rs pyarrow"] = [t.num_rows, pa.compute.sum(t["total"]).as_py(), t.num_rows - t["note"].null_count]
    except Exception as e:
        theirs["delta-rs pyarrow"] = "refused" if "columnMapping" in str(e) else f"error: {str(e)[:120]}"
    notes = sum(1 for r in model.values() if r["note"])
    checks["Delta and Iceberg readers (by column id / physical name)"] = all(v in ([len(model), total, notes], "refused") for v in theirs.values()) and theirs["delta-rs"] != "refused"
    ok = all(checks.values())
    diff = [] if ok else sorted(set(want) ^ set(got(A.port)))[:8]
    for n in nodes:
        n.kill()
    print(json.dumps({"columns": checks, "rows": len(model), "outside_readers": theirs, "expected": [len(model), total, notes], "ok": ok}, indent=1))
    if not ok:
        print(diff)
        sys.exit(1)
    return f"RENAME/DROP/ADD again/widen under streaming ({len(model):,} rows), 3 nodes, bulk INSERT, UPDATE, a keyed table, {len(theirs)} outside readers: all {len(checks)} checks pass"


def fills():
    """Materialized views filled from the rows already there (ADR-022): made while two producers
    stream into two nodes (and a bulk INSERT writes files), dropped and made again, one made as the
    leader is killed. Each view == its query over the source, every row once: the sequencer holds
    every flush to a view from one commit on, and the view is filled with the rows before it."""
    lake = new_lake()
    a = Node(lake, A.port, tier_secs=0.5).start()
    b = Node(lake, A.port + 1, tier_secs=0.5).start()
    ports = [A.port, A.port + 1]
    q = lambda s, port=A.port: sql(port, s)
    q("CREATE TABLE events (id BIGINT, user VARCHAR, amount BIGINT)")
    stop, sent = threading.Event(), [0, 0]
    def produce(k):
        seq, rng = 0, random.Random(k)
        while not stop.is_set():
            seq += 1
            body = "".join(json.dumps({"id": k * 10**9 + seq * 100 + i, "user": f"u{rng.randrange(20)}", "amount": rng.randrange(1000)}) + "\n" for i in range(100)).encode()
            while True:  # (the same seq until it's in: exactly once through a leader restart)
                try:
                    call(ports[k], "POST", f"/append/events?producer=p{k}&seq={seq}", body, timeout=15)
                    break
                except Exception:
                    if stop.is_set():
                        return
                    time.sleep(0.1)
            sent[k] += 100
    def retry(s, port):
        deadline = time.time() + 90
        while True:
            try:
                return q(s, port)
            except Exception:
                if time.time() > deadline:
                    raise
                time.sleep(0.3)
    threads = [threading.Thread(target=produce, args=(k,), daemon=True) for k in (0, 1)]  # (daemons: a failure ends the run)
    for t in threads:
        t.start()
    time.sleep(1.5)
    views = []
    def make(n, port):
        print(f"(making views {n} on :{port})", file=sys.stderr)
        retry(f"CREATE MATERIALIZED VIEW per_user{n} AS SELECT user, count(*) AS n, sum(amount) AS s FROM events GROUP BY user", port)
        retry(f"CREATE MATERIALIZED VIEW evens{n} AS SELECT id, amount * 2 AS a2 FROM events WHERE amount % 2 = 0", port)
        views.append(n)
    make(0, ports[0])
    q("INSERT INTO events SELECT id + 5000000000, user, amount FROM events WHERE id % 7 = 0", ports[1])  # (bulk, beside a view)
    time.sleep(1)
    make(1, ports[1])
    time.sleep(0.8)
    q("DROP MATERIALIZED VIEW per_user0"); q("DROP MATERIALIZED VIEW evens0"); views.remove(0)
    make(0, ports[1])  # (again: filled afresh)
    time.sleep(0.5)
    maker = threading.Thread(target=make, args=(2, ports[1])); maker.start()
    time.sleep(0.05)
    a.kill(); a.start()  # (the leader, as the view fills)
    maker.join()
    time.sleep(1)
    stop.set()
    for t in threads:
        t.join()
    time.sleep(2)
    source = lambda port: q("SELECT user, count(*) AS n, sum(amount) AS s FROM events GROUP BY user ORDER BY user", port)
    evens = lambda port: q("SELECT count(*) AS n, sum(amount * 2) AS s FROM events WHERE amount % 2 = 0", port)
    checks = {}
    for n in sorted(views):
        for port in ports:
            checks[f"per_user{n} == its query (:{port})"] = until(lambda: q(f"SELECT user, n, s FROM per_user{n} ORDER BY user", port), source(port), secs=20) == source(port)
            checks[f"evens{n} == its query (:{port})"] = until(lambda: q(f"SELECT count(*) AS n, sum(a2) AS s FROM evens{n}", port), evens(port), secs=20) == evens(port)
    count = q("SELECT count(*) AS n FROM events")[0]["n"]
    a.kill(); b.kill()
    ok = all(checks.values())
    print(json.dumps({"fills": checks, "rows": count, "ok": ok}, indent=1))
    if not ok:
        sys.exit(1)
    return f"views filled from existing rows while {count:,} rows streamed in, made again, and through a leader restart: all {len(checks)} checks pass"


def dedup():
    """A keyed table deduplicated by event time (`order_by`, ADR-022): rows arrive out of order
    over two nodes and tiering rounds; a late row doesn't replace a newer one. Against a model:
    reads, /lookup, point queries, a DELETE and late rows after it, an UPDATE, compaction, and
    DuckDB reading its Delta copy. And a keyed table's `SELECT *` leaves `_deleted` out."""
    lake = new_lake()
    a = Node(lake, A.port, tier_secs=0.3).start()
    b = Node(lake, A.port + 1, tier_secs=0.3).start()
    q = lambda s, port=A.port: sql(port, s)
    q("CREATE TABLE latest (k BIGINT PRIMARY KEY, ts BIGINT, v BIGINT) WITH (order_by = 'ts', publish = 'delta')")
    rng, model, n = random.Random(7), {}, 0
    def put(k, ts, v, port):
        call(port, "POST", f"/append/latest?producer=d&seq={v}", (json.dumps({"k": k, "ts": ts, "v": v}) + "\n").encode())
    for r in range(16):
        batch = []
        for _ in range(150):
            n += 1
            k, ts = rng.randrange(200), rng.randrange(10_000)
            batch.append({"k": k, "ts": ts, "v": n})
            if k not in model or ts >= model[k][0]:  # (a tie: the later one)
                model[k] = (ts, n)
        call((A.port, A.port + 1)[r % 2], "POST", f"/append/latest?producer=d&seq={r + 1}", "".join(json.dumps(x) + "\n" for x in batch).encode())
        time.sleep(0.35)  # (tiering rounds: generations, then compactions)
    q("DELETE FROM latest WHERE k < 10")
    for k in range(10):
        model.pop(k, None)
    late = [{"k": 1, "ts": -5, "v": 900001}, {"k": 2, "ts": 20_000, "v": 900002}]  # (older than its delete: stays gone; newer: back)
    call(A.port, "POST", "/append/latest?producer=d&seq=100", "".join(json.dumps(x) + "\n" for x in late).encode())
    model[2] = (20_000, 900002)
    q("UPDATE latest SET v = -1 WHERE k = 20")
    if 20 in model:
        model[20] = (model[20][0], -1)
    want = sorted((k, ts, v) for k, (ts, v) in model.items())
    got = lambda port=A.port: sorted((r["k"], r["ts"], r["v"]) for r in q("SELECT k, ts, v FROM latest", port))
    reads = [got(), got(A.port + 1)]
    probe = sorted(model)[len(model) // 2]
    q("CHECKPOINT")
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    import open_check
    delta = lambda: sorted(tuple(r) for r in open_check.duck(lake).execute(f"SELECT k, ts, v FROM delta_scan('{lake}/data/latest')").fetchall())
    checks = {
        "reads == the model, both nodes": reads == [want, want],
        "after compaction": got() == want,
        "/lookup": call(A.port, "GET", f"/lookup/latest/{probe}") == [{"k": probe, "ts": model[probe][0], "v": model[probe][1]}],
        "a point query": q(f"SELECT * FROM latest WHERE k = {probe}") == [{"k": probe, "ts": model[probe][0], "v": model[probe][1]}],
        "a late row doesn't bring a deleted key back; a newer one does": q("SELECT k FROM latest WHERE k < 10 ORDER BY k") == [{"k": 2}],
        "SELECT * leaves _deleted out": list(q("SELECT * FROM latest LIMIT 1")[0]) == ["k", "ts", "v"],
        "…unless named": list(q("SELECT *, coalesce(_deleted, false) AS d FROM latest LIMIT 1")[0]) == ["k", "ts", "v", "d"] and len(q("SELECT *, _deleted FROM latest LIMIT 1")) == 1,
        "DuckDB reads the Delta copy == the model": until(delta, want, secs=30) == want,
    }
    ok = all(checks.values())
    diff = [] if ok else [sorted(set(want) ^ set(got()))[:8], sorted(set(want) ^ set(delta()))[:8]]
    a.kill(); b.kill()
    print(json.dumps({"dedup": checks, "keys": len(model), "rows": n, "ok": ok}, indent=1))
    if not ok:
        print(diff)
        sys.exit(1)
    return f"deduplication by event time: {n:,} rows out of order over {len(model)} keys, a delete and late rows, compaction, Delta: all {len(checks)} checks pass"


def procedures():
    """Macros, procedures and scripts (ADR-023) on three nodes with tokens and `--python`: macros
    expanded where SQL comes in (spread == one node, a follower uses one it just made, stored
    views read them late, materialized views as made); SQL procedures run with the caller's
    rights, arguments worked out once, retried with a job applied once, 16 deep at most; Python
    procedures lent the caller's rights for as long as they run; scripts split around `$$` bodies;
    parameters; rows sent with a request; the Postgres port, MCP and `pondra run`."""
    lake = new_lake()
    here = os.path.dirname(os.path.abspath(__file__))
    env = {"PYTHONPATH": os.path.join(here, "..", "python")}
    toks = dict(read_token="r-tok", write_token="w-tok", admin_token="a-tok")
    nodes = [Node(lake, A.port + i, env=env, python=sys.executable, pg=f"127.0.0.1:{A.port + 10 + i}", **toks).start() for i in range(3)]
    time.sleep(1)  # (the followers hear of each other: spread queries need them)
    def q(s, port=A.port, token="a-tok", path="/sql", headers=None):
        return call(port, "POST", path, s.encode() if isinstance(s, str) else s, headers={"authorization": f"Bearer {token}", **(headers or {})}, timeout=120)
    def err(s, **kw):
        try:
            q(s, **kw)
            return ""
        except Exception as e:
            return str(e)
    checks = {}
    q("CREATE TABLE orders (id BIGINT, user VARCHAR, qty BIGINT, price DOUBLE)")
    q("INSERT INTO orders SELECT value, 'u' || (value % 50), value % 7, (value % 1000) * 0.25 FROM generate_series(1, 300000)")
    q("CREATE TABLE users (user VARCHAR, tier VARCHAR)")
    q("INSERT INTO users SELECT 'u' || value, CASE WHEN value % 3 = 0 THEN 'gold' ELSE 'plain' END FROM generate_series(0, 49)")
    # macros
    q("CREATE MACRO net(x, rate := 0.2) AS x * (1 - rate)", port=A.port + 1)
    checks["a macro made on a follower is used there at once"] = q("SELECT net(10.0) AS a, net(10.0, rate := 0.5) AS b", port=A.port + 1) == [{"a": 8.0, "b": 5.0}]
    q("CREATE MACRO big(n) AS TABLE SELECT * FROM orders WHERE qty >= n")
    q("CREATE MACRO gross(x) AS net(x, rate := -0.1)")
    spread = "SELECT u.tier, count(*) AS n, round(sum(gross(b.price)), 2) AS s FROM big(3) b JOIN users u ON b.user = u.user GROUP BY u.tier ORDER BY u.tier"
    one = q(spread, path="/sql?spread=0")
    checks["a macro in a query spread over three nodes == one node == the SQL written out"] = one == q(spread, path="/sql?spread=1") == q(spread.replace("big(3) b", "(SELECT * FROM orders WHERE qty >= 3) b").replace("gross(b.price)", "b.price * 1.1"), path="/sql?spread=0")
    q("CREATE MACRO loop_a(x) AS loop_b(x)"); q("CREATE MACRO loop_b(x) AS loop_a(x)")
    checks["macros calling each other stop 16 deep"] = "16 deep" in err("SELECT loop_a(1)")
    checks["a macro can't hide SQL's own function"] = "SQL's own" in err("CREATE MACRO round(x) AS x")
    q("CREATE VIEW priced AS SELECT id, net(price) AS p FROM orders WHERE id <= 3")
    q("CREATE MATERIALIZED VIEW priced_live AS SELECT id, net(price) AS p FROM orders WHERE id > 300000")
    q("CREATE OR REPLACE MACRO net(x, rate := 0.2) AS x * 100")
    q("INSERT INTO orders VALUES (300001, 'u1', 1, 2.0)")
    checks["a stored view reads macros as they are now"] = q("SELECT p FROM priced ORDER BY id") == q("SELECT price * 100 AS p FROM orders WHERE id <= 3 ORDER BY id")
    checks["a materialized view keeps them as they were made"] = until(lambda: q("SELECT p FROM priced_live"), [{"p": 1.6}], secs=20) == [{"p": 1.6}]
    # SQL procedures
    q("CREATE TABLE log (x DOUBLE, tag VARCHAR)")
    q("""CREATE PROCEDURE twice(x DOUBLE, tag VARCHAR DEFAULT 'none') LANGUAGE sql AS $$
           INSERT INTO log VALUES ($x, $tag);   -- a ';' in a comment
           INSERT INTO log VALUES ($x, $tag || ';');
           SELECT count(*) AS n FROM log;
         $$""")
    checks["a procedure's answer is its last statement's"] = q("CALL twice(1.5)") == [{"n": 2}]
    q("CALL twice(random(), tag => 'r')")
    xs = q("SELECT x, tag FROM log WHERE tag LIKE 'r%' ORDER BY tag")
    checks["its arguments are worked out once (random() is one value)"] = len(xs) == 2 and xs[0]["x"] == xs[1]["x"] and xs[1]["tag"] == "r;"
    checks["a reader may not CALL a procedure that writes"] = "may not write" in err("CALL twice(2)", token="r-tok")
    checks["a writer may not make one"] = "may not" in err("CREATE PROCEDURE p() AS $$ SELECT 1 $$", token="w-tok")
    for _ in range(2):
        q("CALL twice(7, tag => 'job')", path="/sql?job=j-7")
    checks["a CALL retried with its job writes once"] = q("SELECT count(*) AS n FROM log WHERE tag LIKE 'job%'") == [{"n": 2}]
    q("CREATE PROCEDURE deep(n BIGINT) AS $$ CALL deep($n + 1) $$")
    checks["procedures calling procedures stop 16 deep"] = "16 deep" in err("CALL deep(0)")
    script = "CREATE TABLE s (a VARCHAR); INSERT INTO s VALUES ('x;y'); CREATE PROCEDURE s_n() LANGUAGE sql AS $$ SELECT count(*) AS n FROM s; $$; CALL s_n()"
    checks["a script: statements split around strings and $$ bodies"] = q(script, port=A.port + 2) == [{"n": 1}] and q("SELECT a FROM s") == [{"a": "x;y"}]
    body = json.dumps({"sql": "SELECT count(*) AS n FROM orders WHERE qty = $q AND user = $u AND id < $top", "params": {"q": 3, "u": "u7", "top": {"sql": "1000 + 1"}}}).encode()
    checks["$name parameters (JSON values, SQL expressions)"] = q(body, headers={"content-type": "application/json"}) == q("SELECT count(*) AS n FROM orders WHERE qty = 3 AND user = 'u7' AND id < 1001")
    checks["a parameter with no value is an error"] = "no value for $nope" in err(json.dumps({"sql": "SELECT $nope", "params": {"x": 1}}), headers={"content-type": "application/json"})
    # Python procedures
    q("""CREATE PROCEDURE py_top(k BIGINT DEFAULT 3) LANGUAGE python AS $$
print("running on", con.url)
con.table("orders").group_by("user").agg(pondra.col("qty").sum().alias("q")).sort("q", "user", descending=[True, False]).limit(k)
$$""")
    want = q("SELECT user, sum(qty) AS q FROM orders GROUP BY user ORDER BY q DESC, user LIMIT 2")
    checks["a Python procedure's frame runs here, called by a reader on a follower"] = q("CALL py_top(2)", port=A.port + 2, token="r-tok") == want
    q("""CREATE PROCEDURE py_write(tag VARCHAR) LANGUAGE python AS $$
import json, os
con.sql(f"INSERT INTO log VALUES (1, '{tag}')")
open(os.environ["LEASE_FILE"], "w").write(con.token) if "LEASE_FILE" in os.environ else None
{"token": con.token, "n": con.sql("SELECT count(*) AS n FROM log").item()}
$$""")
    checks["…and with its rights: it may not write for a reader"] = "may not write" in err("CALL py_write('r')", token="r-tok")
    got = q("CALL py_write('w')", token="w-tok")
    lent = got[0]["token"]
    checks["it writes for a writer; its lent token dies with it"] = got[0]["n"] >= 1 and _raises(lambda: q("SELECT 1", token=lent))
    q("""CREATE PROCEDURE py_fail() LANGUAGE python AS $$
raise ValueError("no such thing")
$$""")
    checks["a Python error comes back as the error"] = "ValueError: no such thing" in err("CALL py_fail()")
    q("""CREATE PROCEDURE py_deep(n BIGINT) LANGUAGE python AS $$
con.call("py_deep", n + 1)
$$""")
    checks["Python procedures calling themselves stop 16 deep"] = "16 deep" in err("CALL py_deep(0)")
    # rows sent with a request: a join with a table of the lake, forced to spread
    import pyarrow as pa, io as _io
    t = pa.table({"user": ["u1", "u2"], "target": [5, 6]})
    buf = _io.BytesIO()
    with pa.ipc.new_stream(buf, t.schema) as w:
        w.write_table(t)
    head = json.dumps({"sql": "SELECT m.user, m.target, count(*) AS n FROM orders o JOIN mine m ON o.user = m.user GROUP BY 1, 2 ORDER BY 1", "tables": ["mine"]}).encode()
    body = len(head).to_bytes(4, "little") + head + len(buf.getvalue()).to_bytes(8, "little") + buf.getvalue()
    sent = q(body, path="/sql?spread=1", port=A.port + 1, headers={"content-type": "application/vnd.pondra.request"})
    checks["rows sent with a request, joined with the lake's (asked to spread: here only)"] = sent == q("SELECT m.user, m.target, count(*) AS n FROM orders o JOIN (VALUES ('u1', 5), ('u2', 6)) AS m(user, target) ON o.user = m.user GROUP BY 1, 2 ORDER BY 1")
    # other doors
    import psycopg
    with psycopg.connect(f"host=127.0.0.1 port={A.port + 11} user=admin password=a-tok dbname=lake", autocommit=True) as c:
        checks["Postgres: a macro, a CALL"] = c.execute("SELECT net(1.0) AS v").fetchone() == (100.0,) and c.execute("CALL py_top(1)").fetchone() == tuple(want[0].values())
    mcp = call(A.port, "POST", "/mcp", json.dumps({"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": "write", "arguments": {"sql": "CALL twice(9, tag => 'mcp')"}}}).encode(), headers={"authorization": "Bearer w-tok", "content-type": "application/json"})
    checks["MCP: CALL through the write tool"] = '\\"n\\"' in json.dumps(mcp) and q("SELECT count(*) AS n FROM log WHERE tag LIKE 'mcp%'") == [{"n": 2}]
    rpc = lambda method, params, token="r-tok": call(A.port, "POST", "/mcp", json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode(), headers={"authorization": f"Bearer {token}", "content-type": "application/json"})["result"]
    listed = {t["name"]: t for t in rpc("tools/list", {})["tools"]}
    top = rpc("tools/call", {"name": "py_top", "arguments": {"k": 1}})
    checks["MCP: each procedure is a tool, its parameters the tool's"] = listed.get("py_top", {}).get("inputSchema", {}).get("properties", {}).get("k", {}).get("type") == "integer" and \
        "k" not in listed["py_top"]["inputSchema"]["required"] and not top["isError"] and json.loads(top["content"][0]["text"])["rows"] == want[:1]
    q("CREATE SCHEMA ops"); q("CREATE MACRO ops.twice_it(x) AS x * 2")
    checks["a schema's macros"] = q("SELECT ops.twice_it(21) AS v") == [{"v": 42}]
    q("DROP SCHEMA ops CASCADE")
    checks["…go with it (DROP SCHEMA … CASCADE)"] = "ops" not in json.dumps(call(A.port, "GET", "/routines", headers={"authorization": "Bearer r-tok"}))
    q("DROP MACRO big"); q("DROP PROCEDURE twice")
    checks["DROP MACRO, DROP PROCEDURE"] = err("CALL twice(1)").endswith("no procedure twice'") or "no procedure twice" in err("CALL twice(1)")
    for n in nodes:
        n.kill()
    # a node on another address with --python and no tokens refuses to start; `pondra run` runs a file
    open_node = subprocess.run([BIN, "serve", "--dir", lake, "--addr", f"0.0.0.0:{A.port + 5}", "--python", sys.executable], capture_output=True, text=True, timeout=60)
    checks["--python without tokens only on 127.0.0.1"] = open_node.returncode != 0 and "--python" in open_node.stderr
    f = os.path.join(tempfile.mkdtemp(prefix="pondra-run-"), "load.sql")
    open(f, "w").write("CREATE TABLE IF NOT EXISTS runs (d DATE, n BIGINT);\nINSERT INTO runs VALUES (CAST($day AS DATE), $n);\nSELECT count(*) AS runs FROM runs;\n")
    local = new_lake()
    ran = [subprocess.run([BIN, "run", f, local, "--day", "2026-09-27", "--n", str(i)], capture_output=True, text=True, timeout=120) for i in (1, 2)]
    checks["pondra run: a .sql file with parameters, on a node started for it"] = all(r.returncode == 0 for r in ran) and "| 2    |" in ran[1].stdout
    ok = all(checks.values())
    print(json.dumps({"procedures": checks, "ok": ok}, indent=1))
    if not ok:
        print(ran[1].stdout, ran[1].stderr, open_node.stderr[-300:])
        sys.exit(1)
    return f"macros, procedures (SQL and Python) and scripts on three nodes: all {len(checks)} checks pass"


def all_tests():
    A.runs, A.batches = min(A.runs, 5), min(A.batches, 30)
    out = {t.__name__: t() for t in (upsert, deal, outside, clouds, kafkas, tiering, fence, insert, serverless, clients, kafka, alter, windows, sessions, asof, sums, schemas, changes, guard, files, layouts, clusters, copies, streams, columns, fills, dedup, procedures, scale, flight, reader, crash)}
    A.secs = min(A.secs, 20)
    out["load"] = load()
    print(json.dumps(out, indent=1))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", choices=["crash", "upsert", "deal", "outside", "clouds", "kafkas", "tiering", "fence", "reader", "insert", "serverless", "clients", "kafka", "alter", "windows", "sessions", "asof", "sums", "schemas", "changes", "guard", "files", "layouts", "clusters", "copies", "streams", "columns", "fills", "dedup", "procedures", "scale", "flight", "load", "all"])
    ap.add_argument("--s3", action="store_true", help="use s3://$PONDRA_BUCKET/test-… instead of a temp dir")
    ap.add_argument("--port", type=int, default=8090)
    ap.add_argument("--runs", type=int, default=20)
    ap.add_argument("--producers", type=int, default=3)
    ap.add_argument("--batches", type=int, default=40)
    ap.add_argument("--size", type=int, default=100)
    ap.add_argument("--rounds", type=int, default=12, help="tiering test: write + /tier rounds")
    ap.add_argument("--secs", type=int, default=30)
    ap.add_argument("--flush-ms", type=int, default=250)
    A = ap.parse_args()
    {"crash": crash, "upsert": upsert, "deal": deal, "outside": outside, "clouds": clouds, "kafkas": kafkas, "tiering": tiering, "fence": fence, "reader": reader, "insert": insert, "serverless": serverless, "clients": clients, "kafka": kafka, "alter": alter, "windows": windows, "sessions": sessions, "asof": asof, "sums": sums, "schemas": schemas, "changes": changes, "guard": guard, "files": files, "layouts": layouts, "clusters": clusters, "copies": copies, "streams": streams, "columns": columns, "fills": fills, "dedup": dedup, "procedures": procedures, "scale": scale, "flight": flight, "load": load, "all": all_tests}[A.mode]()
