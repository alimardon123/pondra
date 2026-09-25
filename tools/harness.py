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
  harness.py load    --secs 30   throughput, ack latency, freshness, catalog commit latency
  harness.py all                 quick run of everything
"""
import argparse, atexit, http.client, itertools, json, os, random, shutil, signal, subprocess, sys, tempfile, threading, time, urllib.request, uuid

BIN = os.environ.get("PONDRA_BIN", os.path.join(os.path.dirname(os.path.abspath(__file__)), "../target/release/pondra"))
A = None  # parsed args


def call(port, method, path, body=b"", timeout=30, headers=None):
    c = http.client.HTTPConnection("127.0.0.1", port, timeout=timeout)
    c.request(method, path, body, headers or {})
    r = c.getresponse()
    data = r.read()
    if r.status != 200:
        raise RuntimeError(f"{r.status}: {data[:300]!r}")
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
        events_table(A.port)
        call(A.port, "POST", "/tasks/copy", json.dumps({"source": "events", "target": "events_copy",
                                                       "sql": "SELECT producer, seq, i FROM events WHERE i % 2 = 0"}).encode())
        call(A.port, "POST", "/views/per_producer", b"SELECT producer, count(*) AS n, sum(i) AS s FROM events GROUP BY producer")
        call(A.port, "POST", "/views/thirds", b"SELECT producer, seq, i FROM events WHERE i % 3 = 0")
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
    checks["no token -> 401"] = _raises(lambda: pondra.connect(f"http://127.0.0.1:{A.port}").sql("SELECT 1"))
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
    checks["CREATE TABLE … AS: the query's rows and columns"] = n("dbo.big") == 120000 and [c["column_name"] for c in cols] == ["id", "k", "v"]
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
    got = until(lambda: q("SELECT k, n, s FROM dbo.per_k ORDER BY k"), [{"k": 1, "n": 2, "s": 6.0}, {"k": 3, "n": 1, "s": 3.0}], 30)
    checks["CREATE MATERIALIZED VIEW follows the rows written from then on; WITH (window …) emits to _final; bad options refused"] = got == [{"k": 1, "n": 2, "s": 6.0}, {"k": 3, "n": 1, "s": 3.0}] \
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
    refused = [err(f"ATTACH '{d}' AS {n}", 1) for d, n in ((third, "public"), (lake, "self"), (third + "-nowhere", "w"), (other, f'"{me}"'))]  # a schema's name, this lake, no lake, this lake's name
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
    q("DETACH warehouse")
    gone = [until(lambda i=i: reach(i), False, 15) for i in range(3)]
    checks["ATTACH 'dir' AS name on one node: every node, after a restart, pondra sql; joins and writes across; DETACH everywhere; bad ones refused"] = \
        all(refused) and all(everywhere) and joined == [{"k": 1, "qty": 100}, {"k": 2, "qty": 200}] and (before_insert, fresh) == (2, 3) and after_restart == 3 and "| 3 |" in cli \
        and {me, "other", "warehouse"} <= listed and not any(gone)
    [x.kill() for x in nodes + [b, c]]
    ok = all(checks.values())
    print(json.dumps({"schemas": checks, "ok": ok}, indent=1))
    if not ok:
        print(before, over_view, got, spaces, in_dbo, by_flight, rest, used, view_owned, full, shown, refused, everywhere, joined, before_insert, fresh, after_restart, cli, listed, gone)
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
        "CREATE TABLE bad3 (id BIGINT PRIMARY KEY, ts TIMESTAMP) WITH (partition_by = 'day(ts)')",
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


def all_tests():
    A.runs, A.batches = min(A.runs, 5), min(A.batches, 30)
    out = {t.__name__: t() for t in (upsert, tiering, fence, insert, serverless, clients, kafka, alter, windows, sessions, asof, sums, schemas, scale, flight, reader, crash)}
    A.secs = min(A.secs, 20)
    out["load"] = load()
    print(json.dumps(out, indent=1))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", choices=["crash", "upsert", "tiering", "fence", "reader", "insert", "serverless", "clients", "kafka", "alter", "windows", "sessions", "asof", "sums", "schemas", "scale", "flight", "load", "all"])
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
    {"crash": crash, "upsert": upsert, "tiering": tiering, "fence": fence, "reader": reader, "insert": insert, "serverless": serverless, "clients": clients, "kafka": kafka, "alter": alter, "windows": windows, "sessions": sessions, "asof": asof, "sums": sums, "schemas": schemas, "scale": scale, "flight": flight, "load": load, "all": all_tests}[A.mode]()
