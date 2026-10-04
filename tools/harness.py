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
  harness.py functions           functions and procedures in SQL and Python: workers, notices, mail, secrets, run log, tasks, speed
  harness.py found               what writing the docs found (round 26), each fixed
  harness.py renames             ALTER TABLE | VIEW … RENAME TO: rows, files, copies, followers, views
  harness.py workspace           the lake's files run (CALL run): parameters, every door, files running files, the run log, a task
  harness.py followers           file commits followed as the log's rows are: views, the change feed, Kafka, tasks, a stream join
  harness.py transactions        other engines' commits over two tables at once, and carried over Pondra's merges by row id
  harness.py upserts             keyed tables published every round; other engines' upserts, deletes, equality deletes
  harness.py external            CREATE EXTERNAL TABLE: a named view of files, INSERT into a folder's, what it refuses
  harness.py server              pondra serve <folder of lakes>: each a database (Postgres, HTTP, joins, idle, restart)
  harness.py safety              panics answered as errors, TLS at every door, mutual TLS, the audit log, quotas
  harness.py versions            every file keeps its versions: listed, read, restored, kept after a delete, retention, old notebooks
  harness.py stopped             a run whose node was killed under it: stopped, not running for good
  harness.py variables           DECLARE $day / $day = … from every door, a file's parameters (DECLARE PARAMETER, a .py file's cell), runs and procedures of their own
  harness.py friendly            SQL as DuckDB's users write it (PIVOT, COLUMNS, lambdas, ASOF … ON, SUMMARIZE, …) == DuckDB's answers, spread too
  harness.py load    --secs 30   throughput, ack latency, freshness, catalog commit latency
  harness.py all                 quick run of everything
"""
import http.client as http_client
import argparse, atexit, glob as glob_, http.client, itertools, json, os, random, shutil, signal, subprocess, sys, tempfile, threading, time, traceback, urllib.request, uuid

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


def hot_settled(port):
    """Wait until the node's hot columns (hot.rs) stop loading: none being decoded and the bytes held
    the same twice running (a file's columns load in the background after its second read)."""
    import re
    def now():
        m = call(port, "GET", "/metrics")
        return [float(g.group(1)) if (g := re.search(rb"\npondra_hot_%s (\S+)" % k, m)) else 0.0 for k in (b"bytes", b"loading_files")]
    last = None
    while (held := now()) != last or held[1]:
        last = held
        time.sleep(1)
    return held[0]


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
    def __init__(self, lake, port, reader=False, env=None, cwd=None, **flags):
        self.args = [BIN, "serve", "--dir", lake, "--addr", f"127.0.0.1:{port}"] + (["--reader"] if reader else [])
        self.args += [f"--{k.replace('_', '-')}" + ("" if v is True or v == "true" else f"={v}") for k, v in flags.items()]  # (a bare flag for true)
        self.port, self.env, self.cwd, self.p = port, {**os.environ, **(env or {})}, cwd, None
        self.log = os.path.join(tempfile.gettempdir(), f"pondra-{port}-{uuid.uuid4().hex[:6]}.stderr")

    def start(self, tries=20):
        try:
            call(self.port, "GET", "/stats", timeout=1)
            raise RuntimeError(f"port {self.port} is already in use by another node")
        except (ConnectionError, OSError):
            pass  # free, as it should be
        with open(self.log, "a") as err:
            self.p = subprocess.Popen(self.args, env=self.env, cwd=self.cwd, stdout=subprocess.DEVNULL, stderr=err)
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
    def restart():  # a restart redoes the tiering round a crash cut short, before it answers, and each of
        # that round's files may abort it again: a big round died 200 times in a row (CI). After 20, one
        # start without the files' crashes gets the round through; the next start has them again.
        try:
            node.start()
        except RuntimeError:
            node.kill()
            node.env["PONDRA_CRASH"] = "after_seg_put:0.03,after_commit:0.01"
            try:
                node.start()
            finally:
                node.env.update(env)
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
                        restart()
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
                restart()
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


def external():
    """CREATE EXTERNAL TABLE (ADR-032): a stored view of files by a name, as DataFusion's statement
    reads — CSV's declared columns by position, Parquet's and JSON's by name, a folder's keys as
    declared, a folder with no files yet an empty table; INSERT into a view of a folder a new file
    in it (from a node and from `pondra sql`); DROP TABLE drops it and keeps the files; the tables
    of files its views read aren't listed as the lake's; and what it can't do, it says. And the
    lake's own files (ADR-034): replaced in place by their version (If-Match), never over another's
    change; deleted; read new by every node; never kept in the SSD tier."""
    lake, here, owner = new_lake(), tempfile.mkdtemp(prefix="pondra-ext-"), uuid.uuid4().hex
    tier = os.path.join(here, "tier")
    node = Node(lake, A.port, env={"PONDRA_OWNER_KEY": owner}, cwd=here, **({"cache_dir": tier} if A.s3 else {})).start()
    def q(s):
        return call(A.port, "POST", "/sql", s.encode(), headers={"x-pondra-owner": owner})
    def err(s):
        try:
            q(s)
            return ""
        except Exception as e:
            return str(e)
    os.makedirs(os.path.join(here, "dir"))
    open(os.path.join(here, "a.csv"), "w").write("x,y,z\n1,hello,2024-01-02\n2,world,2024-02-03\n")
    open(os.path.join(here, "j.json"), "w").write('{"a": 1, "b": "x", "extra": true}\n{"a": 2, "b": "y"}\n')
    q(f"COPY (SELECT 1 AS id, 'a' AS name, 10 AS n) TO '{here}/dir/one.parquet'")
    q(f"COPY (SELECT 2 AS id, 'b' AS name, 20 AS n) TO '{here}/dir/two.parquet'")
    checks = {}
    q("CREATE EXTERNAL TABLE c1 (a INT, b VARCHAR, c DATE) STORED AS CSV LOCATION 'a.csv' OPTIONS ('format.has_header' 'true')")
    checks["CSV: declared columns by position, typed as declared, a relative LOCATION where the node runs"] = q("SELECT a, b, CAST(c AS VARCHAR) AS c FROM c1 ORDER BY a") == [{"a": 1, "b": "hello", "c": "2024-01-02"}, {"a": 2, "b": "world", "c": "2024-02-03"}]
    q("CREATE EXTERNAL TABLE p2 (name VARCHAR, id BIGINT, missing INT, n DOUBLE) STORED AS PARQUET LOCATION 'dir'")
    checks["Parquet: a folder's files, declared columns by name, one they don't hold NULL (and IS NULL finds it)"] = q("SELECT name, id, n FROM p2 WHERE missing IS NULL AND n > 15") == [{"name": "b", "id": 2, "n": 20.0}]
    q("CREATE EXTERNAL TABLE j1 (b VARCHAR, a BIGINT) STORED AS JSON LOCATION 'j.json'")
    checks["JSON: declared columns by name, others left out"] = q("SELECT * FROM j1 ORDER BY a") == [{"b": "x", "a": 1}, {"b": "y", "a": 2}]
    q("CREATE EXTERNAL TABLE ev (id INT, name VARCHAR) STORED AS CSV LOCATION 'ev/' OPTIONS ('format.has_header' 'false')")
    empty = q("SELECT count(*) AS n FROM ev")
    q("INSERT INTO ev VALUES (1, 'one'), (2, 'two')")
    q("INSERT INTO ev (name) VALUES ('three')")
    cli = subprocess.run([BIN, "sql", "--dir", lake, "INSERT INTO ev VALUES (4, 'four')"], capture_output=True, text=True, cwd=here, env=node.env)
    files = sorted(os.listdir(os.path.join(here, "ev")))
    checks["a view of a folder: empty before its files; INSERT writes a new file there, from a node and from pondra sql"] = empty == [{"n": 0}] and len(files) == 3 \
        and q("SELECT id, name FROM ev ORDER BY id NULLS LAST") == [{"id": 1, "name": "one"}, {"id": 2, "name": "two"}, {"id": 4, "name": "four"}, {"name": "three"}] \
        and cli.returncode == 0 and '"rows":1' in cli.stdout.replace(" ", "") and not any(t["table_name"] == "ev" and t["table_type"] == "BASE TABLE" for t in q("SELECT table_name, table_type FROM information_schema.tables"))
    q("CREATE EXTERNAL TABLE pp (v INT, day INT) STORED AS PARQUET PARTITIONED BY (day) LOCATION 'pp/'")
    q("INSERT INTO pp VALUES (10, 1), (20, 2), (30, 2)")
    checks["PARTITIONED BY: its keys' folders, typed as declared, made by INSERT"] = q("SELECT day, sum(v) AS s FROM pp GROUP BY day ORDER BY day") == [{"day": 1, "s": 10}, {"day": 2, "s": 50}] \
        and sorted(os.listdir(os.path.join(here, "pp"))) == ["day=1", "day=2"]
    listed = q("SELECT table_name, table_type FROM information_schema.tables WHERE table_schema = 'public' ORDER BY 1")
    checks["information_schema lists each as a VIEW, and not the tables of files they read"] = all(t["table_type"] == "VIEW" for t in listed) and not any(t["table_name"].startswith("ext:") for t in listed) and len(listed) == 5
    same = err("CREATE EXTERNAL TABLE c1 STORED AS CSV LOCATION 'a.csv'")
    kept = q("CREATE EXTERNAL TABLE IF NOT EXISTS c1 STORED AS CSV LOCATION 'a.csv'")
    q("CREATE OR REPLACE EXTERNAL TABLE c1 (q INT, r VARCHAR, s VARCHAR) STORED AS CSV LOCATION 'a.csv'")
    checks["an existing name: refused, kept with IF NOT EXISTS, replaced with OR REPLACE"] = "already exists" in same and kept.get("unchanged") is True and q("SELECT q FROM c1 ORDER BY q") == [{"q": 1}, {"q": 2}]
    q("DROP TABLE c1")
    checks["DROP TABLE drops it; its file stays"] = "not found" in err("SELECT * FROM c1") and os.path.exists(os.path.join(here, "a.csv"))
    said = {"one file": err("INSERT INTO j1 VALUES ('z', 3)"), "update": err("UPDATE ev SET name = 'x'"), "avro": err("CREATE EXTERNAL TABLE b1 STORED AS AVRO LOCATION 'x.avro'"),
            "gzip": err("CREATE EXTERNAL TABLE b2 STORED AS CSV LOCATION 'a.csv' OPTIONS ('format.compression' 'gzip')"), "none": err("CREATE EXTERNAL TABLE b3 STORED AS CSV LOCATION 'nothing.csv'"),
            "temp": err("CREATE TEMPORARY EXTERNAL TABLE b4 STORED AS CSV LOCATION 'a.csv'"), "option": err("CREATE EXTERNAL TABLE b5 STORED AS CSV LOCATION 'a.csv' OPTIONS ('format.null_regex' 'x')"),
            "view": (q("CREATE VIEW v AS SELECT 1 AS a"), err("INSERT INTO v VALUES (2)"))[1],
            "not owner": _raises_text(lambda: call(A.port, "POST", "/sql", b"CREATE EXTERNAL TABLE z STORED AS PARQUET LOCATION 'dir/'"))}
    checks["what it can't do, it says: INSERT into one file, UPDATE, Avro, gzip, no files, TEMPORARY, an option it doesn't read, INSERT into a view; others' files need the owner"] = \
        "view of a folder" in said["one file"] and "UPDATE" in said["update"] and "avro" in said["avro"] and "gzip" in said["gzip"] and "no files" in said["none"] \
        and "TEMP VIEW" in said["temp"] and "null_regex" in said["option"] and "is a view" in said["view"] and "program that started the node" in said["not owner"]
    # The lake's own files (PUT /files/…) are the lake's: whoever reads the lake reads them as a
    # table too, and nothing else of its folder (ADR-032); /objects says what each object is.
    call(A.port, "PUT", "/files/reports/q1.csv", b"a,b\n1,x\n")
    area = call(A.port, "GET", "/objects")["files"]
    anyone = lambda s: call(A.port, "POST", "/sql", s.encode())
    mine = anyone(f"SELECT * FROM read_csv('{area}reports/q1.csv')")
    outside = [_raises_text(lambda: anyone(f"SELECT * FROM read_csv('{area}../catalog/x.csv')")), _raises_text(lambda: anyone(f"SELECT * FROM read_csv('{area[:-len('files/')]}other.csv')"))]
    kinds = {o["name"]: o["kind"] for o in call(A.port, "GET", "/objects")["objects"]}
    q("CREATE TABLE kinds_t (a BIGINT)"); q("CREATE MATERIALIZED VIEW mv_kinds AS SELECT a, count(*) AS n FROM kinds_t GROUP BY a")
    listed = {r["name"]: r["kind"] for r in call(A.port, "POST", "/sql", b"SELECT name, kind FROM pondra.tables")}
    shown = [r["name"] for r in call(A.port, "POST", "/sql", b"SHOW MATERIALIZED VIEWS")]
    q("DROP MATERIALIZED VIEW mv_kinds"); q("DROP TABLE kinds_t")
    checks["the lake's own files read as a table by any reader, and nothing else of its folder; /objects and pondra.tables say each object's kind (a materialized view's too)"] = mine == [{"a": 1, "b": "x"}] \
        and all(("program that started the node" if not A.s3 else "no secret covers") in e for e in outside) and kinds.get("ev") == "files" and kinds.get("v") == "view" \
        and listed.get("ev") == "external table" and listed.get("v") == "view" and listed.get("mv_kinds") == "materialized view" and shown == ["mv_kinds"]
    reader = Node(lake, A.port + 1, reader=True, cwd=here, **({"cache_dir": tier + "2"} if A.s3 else {})).start()
    count = lambda port: call(port, "POST", "/sql", f"SELECT count(*) AS n FROM read_csv('{area}reports/q1.csv')".encode())[0]["n"]
    etag = lambda: urllib.request.urlopen(f"http://127.0.0.1:{A.port}/files/reports/q1.csv").headers["etag"]
    before, v1 = (count(A.port), count(A.port + 1)), etag()
    blind = _raises_text(lambda: call(A.port, "PUT", "/files/reports/q1.csv", b"a,b\n9,z\n"))
    put = call(A.port, "PUT", "/files/reports/q1.csv", b"a,b\n1,x\n2,y\n3,z\n", headers={"if-match": v1})
    stale = _raises_text(lambda: call(A.port, "PUT", "/files/reports/q1.csv", b"a,b\n7,q\n", headers={"if-match": v1}))
    after = until(lambda: (count(A.port), count(A.port + 1)), (3, 3))
    checks["a lake's file is replaced by its version (If-Match); without one, or with an old one, refused (409, 412); both nodes read the new rows"] = \
        before == (1, 1) and "409" in blind and "412" in stale and put.get("version") == etag().strip('"') and after == (3, 3) \
        and call(A.port, "GET", "/files/reports/q1.csv") == b"a,b\n1,x\n2,y\n3,z\n"
    call(A.port, "PUT", "/files/tmp/x.txt", b"bye")
    gone = call(A.port, "DELETE", "/files/tmp/x.txt")
    checks["a lake's file is deleted (and is gone from files())"] = gone == {"removed": "files/tmp/x.txt"} and "404" in _raises_text(lambda: call(A.port, "GET", "/files/tmp/x.txt")) \
        and not call(A.port, "POST", "/sql", b"SELECT path FROM files('tmp/')")
    if A.s3:  # (the SSD tier: a lake on object storage; a table's files are kept there, as they never change)
        kept = lambda: [os.path.relpath(os.path.join(d, f), t) for t in (tier, tier + "2") for d, _, fs in os.walk(t) for f in fs]
        q("CREATE TABLE tiered AS SELECT 1 AS a")
        until(lambda: q("SELECT count(*) AS n FROM tiered") and any(not k.startswith("files") for k in kept()), True, 20)
        checks["the lake's files are never kept in the SSD tier (they change in place; a table's files, kept there, never do)"] = any(not k.startswith("files") for k in kept()) \
            and not any(k.startswith("files" + os.sep) or (os.sep + "files" + os.sep) in k for k in kept())
    reader.kill()
    node.kill()
    shutil.rmtree(here, ignore_errors=True)
    ok = all(checks.values())
    print(json.dumps({"external": checks, "ok": ok}, indent=1))
    return ok


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
    said = tempfile.TemporaryFile()  # (its errors, if it stops: a pipe nobody reads would fill and stop it)
    sim = subprocess.Popen([sys.executable, os.path.join(os.path.dirname(os.path.abspath(__file__)), "sim_r2.py"), "--port", str(s3p), "--zero"], stdout=subprocess.DEVNULL, stderr=said)
    atexit.register(sim.kill)
    s3 = boto3.client("s3", endpoint_url=f"http://127.0.0.1:{s3p}", region_name="us-east-1", aws_access_key_id="k", aws_secret_access_key="s")
    for _ in range(600):  # (a first import of moto on a fresh machine takes a while)
        if sim.poll() is not None:
            said.seek(0)
            raise RuntimeError(f"the S3 simulator (sim_r2.py) stopped: {said.read().decode(errors='replace')[-2000:]} (pip install -r tools/requirements.txt: moto[server])")
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
    # After a failover: the reader finds the new leader while its stream is down (every second,
    # not every fifteen), so a write the new leader acks is soon visible there too.
    f = Node(lake, A.port + 2, flush_ms=100).start()
    time.sleep(2)  # (the follower has heard the leader)
    w.kill()
    deadline, acked = time.time() + 60, None
    while acked is None and time.time() < deadline:
        try:
            call(A.port + 2, "POST", "/append/events?producer=r&seq=100", rows("r", 100, 10), timeout=5)
            acked = time.time()
        except Exception:
            time.sleep(0.2)
    seen = until(lambda: _try(lambda: sql(A.port + 1, "SELECT count(*) AS c FROM events WHERE seq = 100")[0]["c"]), 10, 30)
    after = time.time() - acked if acked and seen == 10 else None
    f.kill(); r.kill()
    print(f"reader: send -> visible on a separate read-only node: p50 {pct(lat, .5)} ms, p99 {pct(lat, .99)} ms; after a failover, {after and round(after, 1)} s after the new leader's ack")
    if after is None or after > 5:
        print("reader: after a failover, a write the new leader acked took too long to reach the reader (more than 5 s)")
        sys.exit(1)
    return f"freshness on a separate read-only node: p50 {pct(lat, .5)} ms, p99 {pct(lat, .99)} ms; after a failover {after:.1f} s"


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
    spread = insert_spread()
    return f"bulk INSERT … SELECT writes Parquet directly; a retried job id is applied once; {spread}"


def insert_spread():
    """An INSERT … SELECT or CREATE TABLE AS on three nodes is written by every node from its own
    share (`spmd::insert`), recorded in one commit: the rows the query gives, row ids unique, one
    version; a retried job writes nothing; a query whose rows don't split as they are (a GROUP BY)
    is written by one node."""
    lake = new_lake()
    ports = [A.port + 1 + i for i in range(3)]
    nodes = [Node(lake, p, env={"PONDRA_SPREAD_MB": "0"} if i == 0 else {}).start() for i, p in enumerate(ports)]
    while len(call(ports[0], "GET", "/stats")["nodes"]) < 3:
        time.sleep(0.1)
    q = lambda s, job=None: call(ports[0], "POST", "/sql" + (f"?job={job}" if job else ""), s.encode(), timeout=600)
    one = lambda s: q(s)[0]
    writes = lambda: [metrics_of(p).get('pondra_object_requests_total{op="write"}', 0) for p in ports[1:]]
    q("CREATE TABLE src (id BIGINT, k BIGINT, v DOUBLE, s VARCHAR)")
    for i in range(6):
        q(f"INSERT INTO src SELECT value + {i * 20000}, value % 50, value * 0.5, 'x' || (value % 7) FROM generate_series(1, 20000)")
    call(ports[0], "POST", "/tier", timeout=600)
    q("INSERT INTO src VALUES (1000001, 1, 1.0, 'log'), (1000002, 2, 2.0, 'log')")  # a log tail: the coordinator's
    checks = {}

    def same(name, table, query, extra=""):
        got = call(ports[0], "POST", "/sql?spread=1", f"SELECT count(*) AS n, sum(v) AS s, count(DISTINCT _row_id) AS ids, count(DISTINCT _version) AS versions FROM {table}{extra}".encode())[0]
        want = one(f"SELECT count(*) AS n, sum(v) AS s FROM ({query})")
        checks[name] = got["n"] == want["n"] > 0 and got["s"] == want["s"] and got["ids"] == got["n"] and got["versions"] == 1
        return got

    def spread(stmt, job=None):
        before = writes()
        q(stmt, job)
        checks.setdefault("every node writes its share", True)
        checks["every node writes its share"] &= all(b > a for a, b in zip(before, writes()))

    q("CREATE TABLE dst (id BIGINT, k BIGINT, v DOUBLE, s VARCHAR)")
    t = time.time()
    spread("INSERT INTO dst SELECT * FROM src WHERE k < 40", job="spread-1")
    took = round(time.time() - t, 2)
    got = same("the query's rows, row ids unique, one version", "dst", "SELECT * FROM src WHERE k < 40")
    checks["a retried job writes nothing"] = q("INSERT INTO dst SELECT * FROM src WHERE k < 40", job="spread-1") == {"duplicate": True} \
        and one("SELECT count(*) AS n FROM dst")["n"] == got["n"]
    spread("CREATE TABLE ctas AS SELECT id, v, list_transform([k], x -> x + 1) AS l FROM src")
    same("CREATE TABLE AS, a lambda in it", "ctas", "SELECT * FROM src")
    checks["… the lambda's values"] = one("SELECT count(*) AS n FROM ctas JOIN src USING (id) WHERE ctas.l[1] <> src.k + 1")["n"] == 0
    q("CREATE TABLE parts (id BIGINT, k BIGINT, v DOUBLE, s VARCHAR) WITH (partition_by = 'k')")
    spread("INSERT INTO parts SELECT * FROM src WHERE k < 5")
    same("a partitioned table", "parts", "SELECT * FROM src WHERE k < 5")
    import pyarrow.parquet as pq
    folder = os.path.join(lake, "data", "parts")
    paths = [os.path.join(r, f) for r, _, fs in os.walk(folder) for f in fs if f.endswith(".parquet")]
    checks["… each file one partition value"] = len(paths) >= 5 and all(len(set(pq.read_table(f, columns=["k"]).column("k").to_pylist())) == 1 for f in paths)
    q("CREATE TABLE followed (id BIGINT, k BIGINT, v DOUBLE, s VARCHAR)")
    q("CREATE MATERIALIZED VIEW by_k AS SELECT k, count(*) AS n, sum(v) AS v FROM followed GROUP BY k")
    spread("INSERT INTO followed SELECT * FROM src WHERE k < 20")
    same("a table a view follows", "followed", "SELECT * FROM src WHERE k < 20")
    checks["… the view follows it"] = until(lambda: one("SELECT sum(n) AS n, sum(v) AS v FROM by_k"), one("SELECT count(*) AS n, sum(v) AS v FROM followed"), 15) \
        == one("SELECT count(*) AS n, sum(v) AS v FROM followed")
    q("CREATE TABLE grouped (k BIGINT, n BIGINT, v DOUBLE)")
    before = writes()
    q("INSERT INTO grouped SELECT k, count(*), sum(v) FROM src GROUP BY k")
    checks["a GROUP BY is written by one node"] = writes() == before and one("SELECT count(*) AS n, sum(n) AS rows FROM grouped") == {"n": 50, "rows": 120002}
    log = open(nodes[0].log).read()
    checks["nothing fell back to one node (a spread query naming _row_id too)"] = "across the nodes failed" not in log and "distributed query failed" not in log
    for n in nodes:
        n.kill()
    print(json.dumps({"insert across the nodes": {"rows": got["n"], "secs": took, "checks": checks}}))
    if not all(checks.values()):
        print("insert: failed: " + ", ".join(k for k, v in checks.items() if not v))
        sys.exit(1)
    return f"on three nodes every node writes its share ({got['n']:,} rows in {took} s), one commit, ids unique; {len(checks)} checks"


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
    # Downloads (the console's, round 29): every row, not the 10,000 the console shows, in each form.
    import io, openpyxl, pyarrow.parquet as pq
    many = "SELECT value AS n, 'r' || value AS s, value % 2 = 0 AS even, value * 0.5 AS half FROM generate_series(1, 12345) ORDER BY n"
    def fetch(f):
        c = http.client.HTTPConnection("127.0.0.1", A.port, timeout=120)
        c.request("POST", f"/sql?format={f}", many.encode(), {"authorization": "Bearer r-tok"})
        r = c.getresponse()
        return r.read() if r.status == 200 else b""
    got = {f: fetch(f) for f in ("csv", "tsv", "ndjson", "parquet", "xlsx")}
    csv_lines, tsv_lines = got["csv"].decode().splitlines(), got["tsv"].decode().splitlines()
    nd = [json.loads(x) for x in got["ndjson"].decode().splitlines()]
    pt = pq.read_table(io.BytesIO(got["parquet"]))
    ws = openpyxl.load_workbook(io.BytesIO(got["xlsx"]), read_only=True).active
    rows_x = list(ws.iter_rows(values_only=True))
    checks["downloads: CSV, TSV, JSON lines, Parquet and Excel, every row (12,345, past the console's 10,000), read back"] = \
        csv_lines[0] == "n,s,even,half" and len(csv_lines) == 12346 and csv_lines[1] == "1,r1,false,0.5" and tsv_lines[1] == "1\tr1\tfalse\t0.5" and len(tsv_lines) == 12346 \
        and len(nd) == 12345 and nd[-1] == {"n": 12345, "s": "r12345", "even": False, "half": 6172.5} and pt.num_rows == 12345 and pt.column_names == ["n", "s", "even", "half"] \
        and rows_x[0] == ("n", "s", "even", "half") and len(rows_x) == 12346 and rows_x[1] == (1, "r1", False, 0.5) and rows_x[-1] == (12345, "r12345", False, 6172.5)
    if not checks["downloads: CSV, TSV, JSON lines, Parquet and Excel, every row (12,345, past the console's 10,000), read back"]:
        print("downloads:", csv_lines[:2], len(csv_lines), tsv_lines[:2], len(nd), nd[-1:], pt.num_rows, rows_x[:2], len(rows_x), rows_x[-1:])
    a.kill(); b.kill()
    ok = all(checks.values())
    print(json.dumps({"clients": checks, "inbox_s": round(inbox_s, 2), "ok": ok}, indent=1))
    if not ok:
        sys.exit(1)
    return f"SQL writes, Python client, Postgres (4 drivers), tokens, change-feed replay, attached lake, inbox ({inbox_s:.1f}s), vector search, MCP, no file access from SQL, downloads: all {len(checks)} checks pass"


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
    # The payload alone, without Kafka Connect's schema beside it; a delete for a table without a key
    # refused by name (it can't know which row went), not appended as a row.
    sql_("CREATE TABLE plain (id BIGINT, name VARCHAR)")
    sql_("CREATE TABLE people (id BIGINT PRIMARY KEY, name VARCHAR)")
    p.produce("people", key=json.dumps({"id": 5}), value=json.dumps({"payload": {"op": "c", "before": None, "after": {"id": 5, "name": "eve"}}}))
    p.produce("plain", value=json.dumps({"op": "c", "before": None, "after": {"id": 1, "name": "a"}}))
    assert p.flush(30) == 0
    failed = []
    p.produce("plain", value=json.dumps({"op": "d", "before": {"id": 1, "name": "a"}, "after": None}), on_delivery=lambda e, m: failed.append(e))
    p.flush(30)
    checks["a payload without its schema; a delete for a table without a key refused by name"] = \
        sql_("SELECT name FROM people WHERE id = 5") == [{"name": "eve"}] and sql_("SELECT id, name FROM plain") == [{"id": 1, "name": "a"}] \
        and len(failed) == 1 and failed[0] is not None and failed[0].code() == ck.KafkaError.INVALID_RECORD and "PRIMARY KEY" in open(node.log).read()
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
    # a view's column list names its columns (TPC-H q15's form), a materialized view's too
    q("CREATE TABLE vc (k BIGINT, v DOUBLE)")
    q("INSERT INTO vc VALUES (1, 1.0), (1, 2.0), (2, 5.0)")
    q("CREATE VIEW vc_sum (key, total) AS SELECT k, sum(v) FROM vc GROUP BY k", 1)
    q("CREATE VIEW vc_all (a, b) AS SELECT * FROM vc")
    q("CREATE MATERIALIZED VIEW vc_m (key, total) AS SELECT k, sum(v) AS s FROM vc GROUP BY k", 2)
    want = [{"key": 1, "total": 3.0}, {"key": 2, "total": 5.0}]
    checks["CREATE [MATERIALIZED] VIEW v (a, b): its columns named so, over * too; a count that differs refused"] = q("SELECT key, total FROM vc_sum ORDER BY key") == want \
        and q("SELECT key, total FROM vc_m ORDER BY key", 1) == want and q("SELECT sum(b) AS b FROM vc_all WHERE a = 1") == [{"b": 3.0}] \
        and err("SELECT k FROM vc_sum") is not None and (err("CREATE VIEW vc_short (a) AS SELECT k, v FROM vc") is not None or err("SELECT a FROM vc_short") is not None)
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
    # a view of a view (a flow, ADR-036): of a GROUP BY view's partial rows only a rollup; of a _final, as of a table
    over = [err("CREATE MATERIALIZED VIEW m3 AS SELECT k FROM dbo.per_k"), err("CREATE MATERIALIZED VIEW m4 AS SELECT w FROM per_min_final")]
    checks["a materialized view of a GROUP BY view that isn't a rollup is refused, saying what to do; of a _final, made"] = "GROUP BY view" in str(over[0]) and over[1] is None
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
    change = q("UPDATE warehouse.stock SET qty = qty + 1 WHERE id = 1", 2)  # (from a follower: that lake's leader carries it out, ADR-028)
    checks["FROM t is SELECT * FROM t (alone, a subquery, a view's); UPDATE on an attached lake, from any node, is its leader's"] = \
        from_first[0] == [{"id": 1, "qty": 100}, {"id": 2, "qty": 200}, {"id": 3, "qty": 300}] and from_first[1] == [{"n": 3}] and from_first[2] == from_first[0] \
        and change.get("updated") == 1 and until(lambda: sql(A.port + 6, "SELECT qty FROM stock WHERE id = 1"), [{"qty": 101}], 15) == [{"qty": 101}]
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
    except (fl.FlightUnauthenticatedError, fl.FlightUnauthorizedError):  # (signed in, not allowed: unauthorized)
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
    delta = lambda: sorted((r["id"], r["owner"], r["bal"]) for r in delta_table(f"{lake}/data/acct", opts).select(["id", "owner", "bal"]).to_pylist())
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
    fast = Node(lake, A.port + 1, env={"PONDRA_LINK": "0.2,5000", "PONDRA_DEBUG_SPREAD": "1", **small}).start()
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
    # An aggregate of a few groups moves a few rows: a slice knows its columns' distinct values
    # (the catalog's sketches), so the estimate says so. (The cluster bench's q1 stayed on one node
    # when the estimate was the whole table's rows.)
    import re
    for _ in range(2):
        q("SELECT p, count(*) AS n, sum(v) AS s FROM f GROUP BY p", fast.port)
    said = [l for l in open(fast.log) if l.startswith("spread: here")]
    few_mb = float(re.search(r"\(([\d.]+) MB", said[-1]).group(1)) if said else None
    # A node on the real network (no PONDRA_LINK): one spread run slower than here is the others'
    # first sight of the query (their caches cold), so the model decides again until a second
    # agrees; once a query ran both ways, the faster way wins.
    fresh = "SELECT count(*) AS n, sum(f.v * g.w) AS s FROM f JOIN g ON f.id = g.id"  # (the model spreads it: the tables meet by ranges of id)
    timed = lambda s, spread: (lambda t: (q(s, third.port, spread), time.time() - t)[1])(time.time())
    here = min(timed(fresh, 0) for _ in range(2))
    slower = timed(fresh, 1) > here
    before = spreads(third.port)
    q(fresh, third.port)
    again = (slower, spreads(third.port) > before)
    learned = []
    for s in queries:
        here, spread = min(timed(s, 0) for _ in range(2)), min(timed(s, 1) for _ in range(2))
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
              "one spread run slower than here doesn't decide alone: the query spreads again": again[1] or not again[0],
              "a query that ran both ways goes the faster way": all(learned),
              "an aggregate of a few groups is known to move little (under 1 MB)": few_mb is not None and few_mb < 1}
    ok = all(checks.values())
    print(json.dumps({"guard": checks, "ok": ok, "learned": learned, "again": again, "few_groups_mb": few_mb}, indent=1))
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
    theirs = delta_table(os.path.join(lake, "data", "t") if not lake.startswith("s3://") else lake + "/data/t", opts)
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
    a keyed table and its lookups, Delta and Iceberg readers; the table renamed and back. Refused:
    key and partition columns, columns a view reads, narrowing."""
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
    # (Round 26 renames tables: this one, with its renamed, dropped and widened columns, and back.)
    q("ALTER TABLE events RENAME TO events_renamed")
    want_renamed = [{"n": len(model), "s": sum(r["total"] or 0 for r in model.values())}]
    renamed = until(lambda: q("SELECT count(*) AS n, sum(total) AS s FROM events_renamed"), want_renamed, 20)  # (the last rows sent through the other nodes: in this node's view within moments)
    q("ALTER TABLE events_renamed RENAME TO events")
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
        "the table renamed and back, its columns as they were": renamed == [{"n": len(model), "s": total}],
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
        t = delta_table(f"{lake}/data/events", opts)
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
    # DO (ADR-030): a console's Python cell, from every door, as an admin. What it prints comes
    # back as notices; its last expression, a frame here, as rows; an error at the code's line.
    import urllib.request
    block = 'DO LANGUAGE python $pondra$\nfor i in range(2):\n    print("said", i)\ndb.table("orders").select("id").sort("id").limit(1)\n$pondra$'
    r = urllib.request.urlopen(urllib.request.Request(f"http://127.0.0.1:{A.port}/sql", block.encode(), headers={"authorization": "Bearer a-tok"}), timeout=120)
    said, answer = json.loads(r.headers.get("x-pondra-notices") or "[]"), json.loads(r.read())
    first = q("SELECT min(id) AS id FROM orders")[0]["id"]
    checks["DO LANGUAGE python over HTTP: what it printed as notices, its last expression (a frame) as rows"] = said == ["said 0", "said 1"] and answer == [{"id": first}]
    heard = []
    with psycopg.connect(f"host=127.0.0.1 port={A.port + 11} user=admin password=a-tok dbname=lake", autocommit=True) as c:
        c.add_notice_handler(lambda d: heard.append(d.message_primary))
        cur = c.execute("DO $$\nprint('from psql')\n$$ LANGUAGE python")
        checks["DO over Postgres: NOTICE, tag DO (the language after the code, as Postgres allows)"] = heard == ["from psql"] and cur.statusmessage == "DO"
    before = q("SELECT count(*) AS n FROM orders")[0]["n"]
    q("DO LANGUAGE sql $$ INSERT INTO orders VALUES (900, 'do', 1, 1.0); INSERT INTO orders VALUES (901, 'do', 1, 1.0) $$")
    checks["DO LANGUAGE sql: its statements, in order"] = q("SELECT count(*) AS n FROM orders")[0]["n"] == before + 2
    broken = err("DO LANGUAGE python $$\nx = 1\n1 / 0\n$$")
    checks["DO: only an admin runs one; an error at the line in its code; other languages said so"] = "admin" in err(block, token="w-tok") \
        and "ZeroDivisionError" in broken and "line 2: 1 / 0" in broken and not broken.split("ZeroDivisionError")[0].strip(" :'\"").endswith("do") \
        and "LANGUAGE python" in err("DO $$ BEGIN END $$")
    # A session's DO blocks (the console's cells) share one namespace, as a notebook's cells do
    # (ADR-032); another session's, and a block sent in none, don't; ending the session ends it.
    def cell(code, session):
        h = {"authorization": "Bearer a-tok", **({"x-pondra-session": session} if session else {})}
        try:
            return json.loads(urllib.request.urlopen(urllib.request.Request(f"http://127.0.0.1:{A.port}/sql", f"DO LANGUAGE python $$\n{code}\n$$".encode(), headers=h), timeout=120).read())
        except urllib.error.HTTPError as e:
            return e.read().decode()
    one, two = "cells-" + uuid.uuid4().hex[:8], "cells-" + uuid.uuid4().hex[:8]
    cell("x = 20\nimport math\nf = db.table('orders').select('id').sort('id').limit(1)", one)
    cell("x = x + 1\n1 / 0", one)  # (a cell that fails keeps what it did before the failing line)
    shared = [cell("x * 2", one), cell("math.floor(math.pi)", one), cell("f", one)]
    apart = [cell("x", two), cell("x", None)]
    call(A.port, "DELETE", f"/sessions/{one}", headers={"authorization": "Bearer a-tok"})
    ended = cell("x", one)
    checks["a session's Python cells share variables, imports and frames; another session's and a block in none don't; ending the session ends them"] = \
        shared == [[{"value": 42}], [{"value": 3}], [{"id": first}]] and all("NameError" in a for a in apart) and "NameError" in ended
    # Stop (round 29): an interrupt ends the running cell, its variables kept; a cell whose caller
    # stopped waiting never answers the next one.
    three = "cells-" + uuid.uuid4().hex[:8]
    admin = {"authorization": "Bearer a-tok"}
    cell("y = 5", three)
    slow = {}
    t = threading.Thread(target=lambda: slow.setdefault("a", cell("import time\ny = 6\ntime.sleep(60)", three)))
    t0 = time.time(); t.start(); time.sleep(1.5)
    said = call(A.port, "POST", f"/sessions/{three}/python", b"", headers=admin)
    t.join(30); took = time.time() - t0
    kept = cell("y", three)
    checks["Stop: an interrupt ends a session's running cell at once (KeyboardInterrupt), and its variables stay"] = \
        said == {"done": "interrupted"} and "Interrupt" in str(slow.get("a")) and took < 10 and kept == [{"value": 6}]
    h = {**admin, "x-pondra-session": three}
    try:
        urllib.request.urlopen(urllib.request.Request(f"http://127.0.0.1:{A.port}/sql", b"DO LANGUAGE python $$\nimport time\ntime.sleep(2)\n'late'\n$$", headers=h), timeout=0.5)
    except Exception:
        pass  # (the caller went away: the cell goes on)
    after = cell("'next'", three)
    checks["a cell its caller stopped waiting for is finished first: the next cell gets its own answer"] = after == [{"value": "next"}]
    info = call(A.port, "GET", "/python?all=1", headers=admin)
    checks["GET /python: the Python in use, and (?all=1) each this machine has, tried; PUT /python an admin's"] = \
        bool(info.get("python")) and info.get("worker", {}).get("version") is not None and any(p.get("ok") for p in info.get("pythons", [])) and "403" in _raises_text(lambda: call(A.port, "PUT", "/python", b'{"path": "x"}', headers={"authorization": "Bearer w-tok", "content-type": "application/json"}))  # (signed in, not allowed)
    try:
        fmt = call(A.port, "POST", "/python/format", json.dumps({"code": "x=[1,2]\nif x :  print( x )\n"}).encode(), headers={**admin, "content-type": "application/json"})
    except Exception as e:
        fmt = str(e)
    has = any(subprocess.run([sys.executable, "-c", f"import {m}"], capture_output=True, env={**os.environ, **env}).returncode == 0 for m in ("ruff", "black"))  # (as the node's workers see it)
    broken_fmt = _raises_text(lambda: call(A.port, "POST", "/python/format", b'{"code": "x = (1,"}', headers={**admin, "content-type": "application/json"}))
    checks["POST /python/format: ruff's (or black's) formatting, or how to get one; code that doesn't parse is refused, never run"] = \
        (fmt == {"code": "x = [1, 2]\nif x:\n    print(x)\n"} if has else "pip install ruff" in str(fmt)) and broken_fmt != ""
    if not checks["POST /python/format: ruff's (or black's) formatting, or how to get one; code that doesn't parse is refused, never run"]:
        print("format:", fmt, "| broken:", broken_fmt, "| has:", has)
    logged = q("SELECT args FROM pondra.runs WHERE routine = 'do' AND args LIKE '%time.sleep(60)%'")
    checks["the run log names a DO block by its code (pondra.runs.args: language and code)"] = bool(logged) and json.loads(logged[0]["args"]).get("language") == "python"
    # The ways people write them (the design review): a procedure's body after AS, no $$, its
    # parameters `$n` with `=` defaults; a function's `$x`, untyped parameters (DuckDB's macro), a
    # table function's `$k` given its value; a `$name` that isn't a parameter, refused as made.
    q("CREATE PROCEDURE plain($n BIGINT = 5) AS BEGIN\n  IF $n > 3 THEN PRINT 'big ' || $n; END IF;\n  SELECT $n * 2 AS n;\nEND;")
    q("CREATE PROCEDURE one(n BIGINT) AS SELECT $n + 1 AS n")
    q("CREATE FUNCTION twice(x) AS $x * 2")
    q("CREATE FUNCTION upto(k BIGINT) RETURNS TABLE (v BIGINT) AS $$ SELECT value AS v FROM generate_series(1, $k) $$")
    plain_said = urllib.request.urlopen(urllib.request.Request(f"http://127.0.0.1:{A.port}/sql", b"CALL plain()", headers={"authorization": "Bearer a-tok"}), timeout=120)
    written = {"default": q("CALL plain()"), "given": q("CALL plain(n => 2)"), "one statement": q("CALL one(2)"), "untyped": q("SELECT twice(21) AS a, twice(1.5) AS b"),
               "table": q("SELECT count(*) AS n FROM upto(4)"), "stray": err("CREATE FUNCTION stray(x INT) RETURNS INT RETURN x + $y"),
               "python": err("CREATE PROCEDURE py() LANGUAGE python AS BEGIN SELECT 1; END"), "notices": plain_said.headers.get("x-pondra-notices") or ""}
    checks["written as people write them: CREATE PROCEDURE p($n BIGINT = 5) AS BEGIN … END (or one statement), a function's $x, f(x) untyped, a table function's $k; $y refused as made"] = (
        written["default"] == [{"n": 10}] and written["given"] == [{"n": 4}] and written["one statement"] == [{"n": 3}]
        and written["untyped"] == [{"a": 42, "b": 3.0}] and written["table"] == [{"n": 4}] and "there is no parameter $y" in written["stray"]
        and "Python procedure's body is a string" in written["python"] and "big 5" in written["notices"])
    if not checks["written as people write them: CREATE PROCEDURE p($n BIGINT = 5) AS BEGIN … END (or one statement), a function's $x, f(x) untyped, a table function's $k; $y refused as made"]:
        print("written:", written)
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


def functions():
    """Functions and procedures in SQL and Python (ADR-027), on three nodes with tokens and
    `--python`: Postgres's CREATE FUNCTION forms; Python functions per row, vectorized and as tables,
    spread (one node == three, each node's rows through its own workers), a worker killed mid-query,
    the time limit, no connection from a function, volatile ones not remembered, table functions on
    one node; procedures that send mail through a local SMTP server from HTTP, Postgres (NOTICE),
    MCP, JavaScript and the shell, answer nothing / rows / a frame, write once with a job, read a
    secret that never shows; the run log, `pondra.start`, nested calls with few slots, idle workers
    gone; a notebook's decorated functions; a task ticking through a leader failover; speed."""
    import psycopg, statistics
    from aiosmtpd.controller import Controller
    from aiosmtpd.smtp import AuthResult
    lake = new_lake()
    here = os.path.dirname(os.path.abspath(__file__))
    py = os.path.join(here, "..", "python")
    env = {"PYTHONPATH": py, "PONDRA_WORKER_IDLE_SECS": "4", "PONDRA_PROCEDURES": "2", "PONDRA_SECRET_KEY": "harness-key-24"}
    toks = dict(read_token="r-tok", write_token="w-tok", admin_token="a-tok")
    nodes = [Node(lake, A.port + i, env=env, python=sys.executable, pg=f"127.0.0.1:{A.port + 10 + i}", **toks).start() for i in range(3)]
    time.sleep(1)
    def q(s, port=A.port, token="a-tok", path="/sql", headers=None, timeout=120):
        return call(port, "POST", path, s.encode() if isinstance(s, str) else s, headers={"authorization": f"Bearer {token}", **(headers or {})}, timeout=timeout)
    def told(s, port=A.port, token="a-tok", path="/sql"):
        c = http.client.HTTPConnection("127.0.0.1", port, timeout=120)
        c.request("POST", path, s.encode(), {"authorization": f"Bearer {token}"})
        r = c.getresponse()
        body = r.read()
        return r.status, json.loads(r.getheader("x-pondra-notices") or "[]"), json.loads(body) if body[:1] in (b"{", b"[") else body
    def err(s, **kw):
        try:
            q(s, **kw)
            return ""
        except Exception as e:
            return str(e)
    checks = {}
    q("CREATE TABLE orders (id BIGINT, user VARCHAR, qty BIGINT, price DOUBLE)")
    q("INSERT INTO orders SELECT value, 'u' || (value % 50), value % 7, (value % 1000) * 0.25 FROM generate_series(1, 300000)")
    # SQL functions: Postgres's forms, Postgres's answers
    q("CREATE FUNCTION add(integer, integer) RETURNS integer AS 'select $1 + $2;' LANGUAGE SQL IMMUTABLE")
    q("CREATE FUNCTION half(x INT) RETURNS INT STRICT RETURN x / 2")
    q("CREATE FUNCTION net(x DOUBLE PRECISION, rate DOUBLE PRECISION DEFAULT 0.2) RETURNS DOUBLE PRECISION RETURN x * (1 - rate)", port=A.port + 1)
    q("CREATE FUNCTION big(n BIGINT) RETURNS TABLE (id BIGINT, qty INT) LANGUAGE sql AS $$ SELECT id, qty FROM orders WHERE qty >= n $$")
    q("CREATE FUNCTION ids(n INT) RETURNS SETOF BIGINT LANGUAGE sql AS $$ SELECT id FROM orders WHERE id <= n ORDER BY id $$")
    q("CREATE FUNCTION top_qty() RETURNS BIGINT LANGUAGE sql STABLE AS $$ SELECT max(qty) FROM orders $$")
    checks["SQL functions: $1, STRICT, typed, defaults and named arguments, a query's value"] = \
        q("SELECT add(1, 2) AS a, half(7) AS h, half(NULL) IS NULL AS n, net(10) AS x, net(10, rate => 0.5) AS y, top_qty() AS t") == [{"a": 3, "h": 3, "n": True, "x": 8.0, "y": 5.0, "t": 6}]
    checks["RETURNS TABLE and SETOF: rows as declared, on three nodes == one == written out"] = \
        q("SELECT count(*) AS n, min(qty) AS m FROM big(5)", path="/sql?spread=1") == q("SELECT count(*) AS n, min(qty) AS m FROM big(5)", path="/sql?spread=0") == q("SELECT count(*) AS n, min(qty) AS m FROM orders WHERE qty >= 5") \
        and q("SELECT * FROM ids(3)") == [{"ids": 1}, {"ids": 2}, {"ids": 3}]
    checks["CREATE FUNCTION errors say what is wrong"] = "LANGUAGE sql or python" in err("CREATE FUNCTION f(x INT) RETURNS INT LANGUAGE plperl AS $$ 1 $$") \
        and "says what it returns" in err("CREATE FUNCTION f(x INT) LANGUAGE python AS $$ return x $$") and "SQL's own" in err("CREATE FUNCTION abs(x INT) RETURNS INT RETURN x") \
        and "no parameter $2" in err("CREATE FUNCTION f(DOUBLE) RETURNS DOUBLE RETURN $1 + $2")
    # Python functions
    q("""CREATE FUNCTION slug(title VARCHAR) RETURNS VARCHAR LANGUAGE python AS $$
    import re
    return re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-") if title is not None else "none"
$$""")
    q("CREATE FUNCTION slug_strict(title VARCHAR) RETURNS VARCHAR LANGUAGE python STRICT AS $$ return title.upper() $$")
    q("""CREATE FUNCTION twice(x BIGINT) RETURNS BIGINT LANGUAGE python WITH (vectorized = true) AS $$
    import pyarrow.compute as pc
    return pc.multiply(x, 2)
$$""")
    q("CREATE FUNCTION rates(cur VARCHAR, n INT DEFAULT 2) RETURNS TABLE (code VARCHAR, rate DOUBLE) LANGUAGE python AS $$ return [(f'{cur}{i}', i * 1.5) for i in range(n)] $$")
    checks["Python functions: per row, NULLs, STRICT, vectorized, a table"] = \
        q("SELECT slug('Hello, World!') AS a, slug(NULL) AS b, slug_strict(NULL) IS NULL AS c, slug_strict('x') AS d, twice(21) AS e") == [{"a": "hello-world", "b": "none", "c": True, "d": "X", "e": 42}] \
        and q("SELECT * FROM rates('USD') ORDER BY code") == [{"code": "USD0", "rate": 0.0}, {"code": "USD1", "rate": 1.5}] and len(q("SELECT * FROM rates('EUR', n => 5)")) == 5
    checks["SHOW USER FUNCTIONS: the lake's own, SQL and Python"] = [r["name"] for r in q("SHOW USER FUNCTIONS LIKE 's%'")] == ["slug", "slug_strict"] \
        and {r["name"]: r["kind"] for r in q("SHOW USER FUNCTIONS")}.get("big") == "table function"
    q("CREATE FUNCTION m7(x BIGINT) RETURNS BIGINT LANGUAGE python AS $$ return x % 7 $$")
    placed = "SELECT m7(id) AS k, count(*) AS n, count(DISTINCT m7(id + 1)) AS d, max(sum(qty)) OVER (ORDER BY m7(id)) AS w FROM orders GROUP BY m7(id) ORDER BY m7(id) DESC"
    written = placed.replace("m7(id)", "(id % 7)").replace("m7(id + 1)", "((id + 1) % 7)")
    checks["a Python function anywhere a SQL one goes: GROUP BY, ORDER BY, COUNT(DISTINCT), a window"] = q(placed, path="/sql?spread=0") == q(written, path="/sql?spread=0")
    q("CREATE FUNCTION slow_label(q BIGINT) RETURNS VARCHAR LANGUAGE python IMMUTABLE AS $$ import time; time.sleep(0.02); return f'q{q}' $$")
    q("CREATE FUNCTION noise(q BIGINT) RETURNS DOUBLE LANGUAGE python AS $$ import random; return q + random.random() $$")
    t0 = time.time()
    labelled = q("SELECT slow_label(qty) AS l, count(*) AS n FROM orders WHERE id <= 50000 GROUP BY 1 ORDER BY 1", path="/sql?spread=0")
    took = time.time() - t0
    checks[f"an IMMUTABLE function gets each distinct argument once a batch ({took:.1f} s for 50,000 rows at 20 ms a call); a volatile one every row"] = took < 30 \
        and labelled == q("SELECT 'q' || qty AS l, count(*) AS n FROM orders WHERE id <= 50000 GROUP BY 1 ORDER BY 1") \
        and q("SELECT count(DISTINCT noise(qty)) AS n FROM orders WHERE id <= 5000", path="/sql?spread=0") == [{"n": 5000}]
    q("CREATE FUNCTION score(qty BIGINT, price DOUBLE) RETURNS BIGINT LANGUAGE python AS $$ return qty * 10 + int(price) $$")
    q("CREATE FUNCTION whose(x BIGINT) RETURNS BIGINT LANGUAGE python AS $$ import os; return os.getppid() $$")
    spread = "SELECT user, sum(score(qty, price)) AS s, sum(twice(id)) AS t FROM orders GROUP BY user ORDER BY user"
    checks["a spread query: three nodes == one node"] = q(spread, path="/sql?spread=1") == q(spread, path="/sql?spread=0")
    checks["…each node runs its rows through its own workers"] = q("SELECT count(DISTINCT whose(id)) AS n FROM orders", path="/sql?spread=1") == [{"n": 3}]
    q("CREATE FUNCTION where_() RETURNS TABLE (pid BIGINT) LANGUAGE python AS $$ import os; return [(os.getppid(),)] $$")
    checks["a query reading a Python table function runs on one node"] = q("SELECT DISTINCT w.pid FROM orders o CROSS JOIN where_() w", path="/sql?spread=1") == [{"pid": nodes[0].p.pid}]
    q("CREATE FUNCTION clock() RETURNS DOUBLE LANGUAGE python AS $$ import time; return time.time() $$")
    q("CREATE FUNCTION clock_fixed() RETURNS DOUBLE LANGUAGE python IMMUTABLE AS $$ import time; return time.time() $$")
    a, b = q("SELECT clock() AS t"), (time.sleep(0.01), q("SELECT clock() AS t"))[1]
    c, d = q("SELECT clock_fixed() AS t"), (time.sleep(0.01), q("SELECT clock_fixed() AS t"))[1]
    checks["a volatile Python function isn't answered from the result cache (an IMMUTABLE one is)"] = a != b and c == d
    q("CREATE FUNCTION peek(x BIGINT) RETURNS BIGINT LANGUAGE python AS $$ return pondra.sql('SELECT 1 AS v').item() $$")
    checks["a function has no connection to the lake"] = "has no connection" in err("SELECT peek(1) AS v")
    q("CREATE FUNCTION nap(x BIGINT) RETURNS BIGINT LANGUAGE python WITH (timeout = 1, vectorized = true) AS $$ import time; time.sleep(30); return x $$")
    t0 = time.time()
    e = err("SELECT sum(nap(id)) AS s FROM orders WHERE id < 10", path="/sql?spread=0")
    checks["a batch past its time limit stops its worker, and fails with why"] = "longer than 1 s" in e and time.time() - t0 < 10 and q("SELECT slug('A b') AS s") == [{"s": "a-b"}]
    started = os.path.join(tempfile.mkdtemp(prefix="pondra-"), "started")
    q(f"CREATE FUNCTION stuck(x BIGINT) RETURNS BIGINT LANGUAGE python WITH (vectorized = true) AS $$ import time; open({started!r}, 'w').close(); time.sleep(60); return x $$")
    out = {}
    worker = threading.Thread(target=lambda: out.update(e=err("SELECT sum(stuck(id)) AS s FROM orders WHERE id < 10", path="/sql?spread=0")))
    worker.start()
    for _ in range(300):  # (killed once the call is in a worker: a slow machine may still be starting one)
        if os.path.exists(started):
            break
        time.sleep(0.1)
    killed = _workers(nodes[0].p.pid)
    for pid in killed:
        os.kill(pid, signal.SIGKILL)
    worker.join(60)
    # (the idle workers die too, a moment after the signal: the next query may take one the node still
    # sees running unless they are gone first, as a worker the OS kills would be. On CI's busy runner the
    # next query once got a killed worker, likeliest one not gone within the 10 s this waited, which it
    # didn't check: wait longer, and fail here if they never go)
    gone = until(lambda: all(_dead(pid) for pid in killed), True, 60)
    checks["a worker killed mid-query: that query fails with why, the node and the next query go on"] = "worker ended" in out.get("e", "") and nodes[0].alive() and gone and q("SELECT slug('C d') AS s") == [{"s": "c-d"}]
    # procedures: mail through a local SMTP server, from every door
    class Box:
        mail = []
        async def handle_DATA(self, server, session, envelope):
            self.mail.append((envelope.mail_from, list(envelope.rcpt_tos), envelope.content.decode()))
            return "250 OK"
    box = Box()
    def login(server, session, envelope, mechanism, data):
        return AuthResult(success=data.login == b"bot@example.com" and data.password == b"pw-7f3a9c")
    smtp = Controller(box, hostname="127.0.0.1", port=A.port + 40, authenticator=login, auth_require_tls=False)
    smtp.start()
    q(f"CREATE SECRET smtp (TYPE generic, host '127.0.0.1', port '{A.port + 40}', user 'bot@example.com', password 'pw-7f3a9c')")
    q("""CREATE PROCEDURE send_report(day DATE, recipients VARCHAR[]) LANGUAGE python AS $$
    import smtplib
    from email.message import EmailMessage
    top = pondra.sql("SELECT user, sum(qty) AS sold FROM orders WHERE id <= $n GROUP BY user ORDER BY sold DESC, user LIMIT 3", n=1000).to_pandas()
    s = pondra.secret("smtp")
    msg = EmailMessage()
    msg["Subject"], msg["From"], msg["To"] = f"Top items {day}", s["user"], ", ".join(recipients)
    msg.set_content(top.to_string(index=False))
    with smtplib.SMTP(s["host"], int(s["port"])) as smtp:
        smtp.login(s["user"], s["password"])
        smtp.send_message(msg)
    print(f"sent to {len(recipients)}")
$$""")
    status, heard, out = told("CALL send_report(DATE '2026-09-27', ['ann@example.com', 'bo@example.com'])", port=A.port + 2, token="r-tok")
    top = q("SELECT user, sum(qty) AS sold FROM orders WHERE id <= 1000 GROUP BY user ORDER BY sold DESC, user LIMIT 3")
    sent = box.mail[-1] if box.mail else ("", [], "")
    mailed = {"status": status, "heard": heard, "out": str(out)[:1500], "sent": [sent[1], sent[2][:600]], "top": top}
    checks["HTTP: a reader's CALL sends the mail (a secret's credentials); what it printed comes back"] = status == 200 and heard == ["sent to 2"] and out == {"called": "send_report"} \
        and sent[1] == ["ann@example.com", "bo@example.com"] and "Top items 2026-09-27" in sent[2] and top[0]["user"] in sent[2]
    said = []
    with psycopg.connect(f"host=127.0.0.1 port={A.port + 11} user=reader password=r-tok dbname=lake", autocommit=True) as c:
        c.add_notice_handler(lambda d: said.append(d.message_primary))
        c.execute("CALL send_report(DATE '2026-09-28', ['pg@example.com'])")
    checks["Postgres: the same CALL, its print a NOTICE"] = said == ["sent to 1"] and box.mail[-1][1] == ["pg@example.com"]
    mcp = call(A.port, "POST", "/mcp", json.dumps({"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": "send_report", "arguments": {"day": "2026-09-29", "recipients": ["mcp@example.com"]}}}).encode(),
               headers={"authorization": "Bearer r-tok", "content-type": "application/json"})
    checks["MCP: the procedure is a tool; its notices come with the answer"] = json.loads(mcp["result"]["content"][0]["text"]).get("notices") == ["sent to 1"] and box.mail[-1][1] == ["mcp@example.com"]
    js = os.path.join(tempfile.mkdtemp(prefix="pondra-js-"), "call.mjs")
    open(js, "w").write(f"""import {{ connect }} from {json.dumps(os.path.join(here, "..", "js", "index.js"))};
const db = connect("http://127.0.0.1:{A.port + 1}", {{ token: "r-tok", onNotice: null }});
const out = await db.call("send_report", "2026-09-30", ["js@example.com"]);
console.log(JSON.stringify({{ out, notices: db.notices }}));""")
    ran = subprocess.run(["node", js], capture_output=True, text=True, timeout=120)
    checks["JavaScript: db.call, and db.notices"] = ran.returncode == 0 and json.loads(ran.stdout) == {"out": {"called": "send_report"}, "notices": ["sent to 1"]} and box.mail[-1][1] == ["js@example.com"]
    # answers, exactly once, secrets
    q("CREATE TABLE log (tag VARCHAR)")
    q("CREATE PROCEDURE quiet() LANGUAGE python AS $$ x = 1 $$")
    q("CREATE PROCEDURE rows_() LANGUAGE python AS $$ return [{'a': 1}, {'a': 2}] $$")
    q("CREATE PROCEDURE frame_() LANGUAGE python AS $$ return pondra.table('orders').filter(pondra.col('id') <= 3).select('id').sort('id') $$")
    checks["a procedure answers nothing, rows, or a frame (run on the node)"] = q("CALL quiet()") == {"called": "quiet"} and q("CALL rows_()") == [{"a": 1}, {"a": 2}] and q("CALL frame_()") == [{"id": 1}, {"id": 2}, {"id": 3}]
    q("""CREATE PROCEDURE add_rows(tag VARCHAR) LANGUAGE python AS $$
    pondra.sql(f"INSERT INTO log VALUES ('{tag}')")
    pondra.sql(f"INSERT INTO log VALUES ('{tag}-2')")
$$""")
    for _ in range(3):
        q("CALL add_rows('j')", path="/sql?job=job-24", token="w-tok")
    checks["a CALL retried with its job: each of its writes once"] = q("SELECT count(*) AS n FROM log") == [{"n": 2}]
    q("""CREATE PROCEDURE leak() LANGUAGE python AS $$
    s = pondra.secret("smtp")
    print("using", s["password"])
    raise ValueError("refused: " + s["password"])
$$""")
    status, heard, out = told("CALL leak()")
    time.sleep(1)
    logged = json.dumps(q("SELECT * FROM pondra.runs WHERE routine = 'leak'"))
    checks["a secret read by a procedure never shows: not in its notices, its error or the run log"] = status == 500 and "pw-7f3a9c" not in json.dumps([heard, str(out), logged]) \
        and heard == ["using ***"] and "refused: ***" in str(out) and "***" in logged and "only a procedure's code" in err("SELECT 1") + str(_raises_text(lambda: call(A.port, "GET", "/secrets/smtp", headers={"authorization": "Bearer a-tok"})))
    runs = q("SELECT routine, caller, status, args FROM pondra.runs WHERE routine = 'send_report' ORDER BY started")
    checks["pondra.runs: every call, its caller, arguments and outcome"] = len(runs) == 4 and all(r["status"] == "ok" and r["caller"] == "read" for r in runs) and '"recipients":["pg@example.com"]' in runs[1]["args"]
    started = q("SELECT pondra.start('quiet') AS r")[0]["r"]
    checks["pondra.start: a run id at once, its outcome in pondra.runs"] = until(lambda: q(f"SELECT status FROM pondra.runs WHERE id = '{started}'"), [{"status": "ok"}], secs=20) == [{"status": "ok"}]
    by_statement = q("START CALL quiet()")[0]["run"]
    with psycopg.connect(f"host=127.0.0.1 port={A.port + 11} user=reader password=r-tok dbname=lake", autocommit=True) as c:
        by_pg = c.execute("start call quiet()").fetchall()
    done = lambda run: until(lambda: q(f"SELECT status FROM pondra.runs WHERE id = '{run}'"), [{"status": "ok"}], secs=20) == [{"status": "ok"}]
    checks["START CALL p(…): pondra.start as a statement, over HTTP and Postgres"] = done(by_statement) and len(by_pg) == 1 and done(by_pg[0][0])
    q("CREATE PROCEDURE deep(n BIGINT) LANGUAGE python AS $$ pondra.call('deep', n + 1) $$")
    t0 = time.time()
    checks["procedures calling procedures take no slot of their own (two here): 16 deep, not stuck"] = "16 deep" in err("CALL deep(0)", timeout=90) and time.time() - t0 < 60
    # a notebook's functions, as they are
    nb = os.path.join(tempfile.mkdtemp(prefix="pondra-nb-"), "notebook.py")
    open(nb, "w").write(f"""import re, sys, json
from datetime import date
import pondra
db = pondra.connect("http://127.0.0.1:{A.port}", token="a-tok")
TAX = 0.25
STOP = ["the", "a"]
def words(t):
    return [w for w in re.findall(r"[a-z]+", t.lower()) if w not in STOP]

@db.function
def tagline(title: str) -> str:
    return "-".join(words(title)) + f"@{{TAX}}"

@db.procedure
def weekly(day: date, top: int = 2):
    rows = pondra.sql("SELECT user, count(*) AS n FROM orders GROUP BY user ORDER BY n DESC, user LIMIT $k", k=top).rows()
    print(f"week of {{day}}: {{len(rows)}}")
    return rows

import pandas as pd
frame = pd.DataFrame({{"a": [1]}})
try:
    @db.function
    def bad(x: int) -> int:
        return x + len(frame)
    refused = ""
except TypeError as e:
    refused = str(e)
out = dict(sql=db.sql("SELECT tagline('The Quick Fox') AS t").item(), here=tagline("The Quick Fox"),
           frame=db.table("orders").filter(pondra.col("id") == 1).select(pondra.fn.tagline(pondra.col("user")).alias("t")).item(),
           call=db.call("weekly", date(2026, 9, 27)).rows(), heard=db.notices, refused=refused)
print(json.dumps(out, default=str))
""")
    ran = subprocess.run([sys.executable, nb], capture_output=True, text=True, timeout=120, env={**os.environ, "PYTHONPATH": py})
    got = json.loads(ran.stdout.strip().splitlines()[-1]) if ran.returncode == 0 else {}
    checks["@db.function / @db.procedure: the notebook's imports, helper and constant go along; SQL, frames and here agree"] = got.get("sql") == got.get("here") == "quick-fox@0.25" \
        and got.get("frame") == "u@0.25" and got.get("call") == q("SELECT user, count(*) AS n FROM orders GROUP BY user ORDER BY n DESC, user LIMIT 2") and got.get("heard") == ["week of 2026-09-27: 2"]
    checks["…a DataFrame it uses is refused, with the fix"] = "pass it as an argument" in got.get("refused", "")
    q("""CREATE FUNCTION pl_max(a INT, b INT) RETURNS INT LANGUAGE plpython3u AS $$
    if a > b:
        return a
    return b
$$""")
    q("""CREATE PROCEDURE pl_count(min_id BIGINT) LANGUAGE plpython3u AS $$
    plan = plpy.prepare("SELECT count(*) AS n FROM orders WHERE id > $1", ["bigint"])
    rv = plpy.execute(plan, [min_id])
    plpy.notice(f"{rv[0]['n']} orders")
$$""")
    checks["PL/Python as Postgres runs it: plpy.execute, plpy.notice"] = q("SELECT pl_max(3, 9) AS m") == [{"m": 9}] and told("CALL pl_count(299990)")[1] == ["10 orders"]
    # speed
    q("CREATE PROCEDURE ping(n BIGINT) LANGUAGE python AS $$ return n $$")
    q("CALL ping(0)")
    took = []
    for i in range(100):
        t0 = time.perf_counter()
        q(f"CALL ping({i})")
        took.append((time.perf_counter() - t0) * 1000)
    call_ms = statistics.median(took)
    q("CREATE FUNCTION plus1(x BIGINT) RETURNS BIGINT LANGUAGE python AS $$ return x + 1 $$")
    rates = {}
    for f in ("twice", "plus1"):
        q(f"SELECT sum({f}(value)) AS s FROM generate_series(1, 10000)", path="/sql?spread=0")
        t0 = time.perf_counter()
        q(f"SELECT sum({f}(value)) AS s FROM generate_series(1, 1000000)", path="/sql?spread=0")
        rates[f] = 1e6 / (time.perf_counter() - t0)
    checks[f"a warm CALL takes under 10 ms (median {call_ms:.1f} ms); vectorized {rates['twice'] / 1e6:.1f}M rows/s, per row {rates['plus1'] / 1e6:.2f}M rows/s"] = call_ms < 10 and min(rates.values()) > 1e6  # (a floor against a regression; shared CI runners vary a lot)
    # a task, through a leader failover
    q("CREATE TABLE ticks (at TIMESTAMP)")
    q("CREATE PROCEDURE mark() LANGUAGE sql AS $$ INSERT INTO ticks SELECT now() $$")
    q("CREATE TASK tick SCHEDULE '1 second' AS CALL mark()")
    time.sleep(4)
    before = q("SELECT count(*) AS n FROM ticks")[0]["n"]
    nodes[0].kill()
    leader = None
    deadline = time.time() + 60
    while leader is None and time.time() < deadline:
        for n in nodes[1:]:
            try:
                if call(n.port, "GET", "/stats", timeout=2)["role"] == "leader":
                    leader = n
            except Exception:
                pass
        time.sleep(0.5)
    time.sleep(6)
    q("DROP TASK tick", port=leader.port) if leader else None
    time.sleep(2)  # (a tick under way when it was dropped ends, and its line is written)
    after = q("SELECT count(*) AS n FROM ticks", port=leader.port)[0]["n"] if leader else 0
    jobs = q("SELECT count(DISTINCT job) AS n FROM pondra.runs WHERE routine = 'tick' AND status = 'ok'", port=leader.port)[0]["n"] if leader else -1
    checks[f"a task through a leader failover: each tick's writes once ({after} ticks, {jobs} runs), and on after it ({before} before)"] = leader is not None and after > before > 0 and after == jobs
    time.sleep(8)
    idle = [call(n.port, "GET", "/stats")["python_workers"] for n in nodes[1:]]
    checks["idle workers are gone after PONDRA_WORKER_IDLE_SECS"] = idle == [0, 0]
    for n in nodes:
        n.kill()
    # the shell: the same CALL, its print shown
    shell = subprocess.run([BIN, lake], input="CALL send_report(DATE '2026-10-01', ['shell@example.com']);\n", capture_output=True, text=True, timeout=180, env={**os.environ, **env})
    checks["the shell: CALL prints what the procedure printed"] = "sent to 1" in shell.stdout and box.mail[-1][1] == ["shell@example.com"]
    smtp.stop()
    ok = all(checks.values())
    print(json.dumps({"functions": checks, "ok": ok}, indent=1))
    if not ok:
        print(out, ran.stdout[-2000:], ran.stderr[-3000:], shell.stdout[-1000:], shell.stderr[-2000:], json.dumps(mailed, default=str))
        sys.exit(1)
    return f"functions and procedures in SQL and Python on three nodes: all {len(checks)} checks pass"


def _dead(pid):
    """A process gone, or a zombie its parent hasn't waited for yet. Every thread counts: a killed
    process's first thread is a zombie while the others are still ending, and until they have, its
    parent's wait says it runs (so a node could still hand that worker the next query)."""
    try:
        tasks = os.listdir(f"/proc/{pid}/task")
    except OSError:
        return True
    for t in tasks:
        try:
            if open(f"/proc/{pid}/task/{t}/stat").read().rsplit(")", 1)[1].split()[0] not in ("Z", "X"):
                return False
        except OSError:
            pass  # (that thread just ended)
    return True


def _workers(parent):
    """The Python workers a node started (`python -m pondra.worker` whose parent is it)."""
    out = []
    for d in os.listdir("/proc"):
        try:
            args = open(f"/proc/{d}/cmdline", "rb").read().split(b"\0")
            ppid = int(open(f"/proc/{d}/stat").read().rsplit(")", 1)[1].split()[1])
        except (OSError, ValueError, IndexError):
            continue
        if ppid == parent and args[1:3] == [b"-m", b"pondra.worker"]:
            out.append(int(d))
    return out


def delta_table(path, opts=None):
    """A Delta table as delta-rs reads it: through its QueryBuilder, which applies deletion vectors
    (`to_pyarrow_table` refuses a table that has them)."""
    import deltalake, pyarrow as pa
    t = deltalake.DeltaTable(path, storage_options=opts or None)
    return pa.table(deltalake.QueryBuilder().register("t", t).execute("SELECT * FROM t").read_all())


def _raises_text(f):
    try:
        f()
        return ""
    except Exception as e:
        return str(e)


# ---------------------------------------------------------------- round 25 (ADR-028)

def _client(port, owner=None):
    """A Python connection to a node (with the owner's key: this machine's files too)."""
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "python"))
    import pondra
    db = pondra.connect(f"http://127.0.0.1:{port}", echo=False)
    db.owner = owner
    return db


def names():
    """One vocabulary (ADR-028): every name in dataframe-api.md's table exists and runs; each
    fallback's answer equals its standard name's, in SQL, Python and PySpark; Delta and Iceberg
    tables written to a folder — made, APPEND, OVERWRITE, refused, ignored — read back by Pondra,
    delta-rs and PyIceberg."""
    import deltalake, glob as g, re
    from pyiceberg.table import StaticTable
    lake, out, owner = new_lake(), tempfile.mkdtemp(prefix="pondra-names-"), uuid.uuid4().hex
    node = Node(lake, A.port, env={"PONDRA_OWNER_KEY": owner}).start()
    db = _client(A.port, owner)
    import pondra
    from pondra.frame import Frame
    from pondra.spark import SparkSession
    spark = SparkSession(db)
    checks = {}
    db.sql("CREATE TABLE t AS SELECT value AS id, 'n' || value AS name, value * 0.5 AS x FROM generate_series(1, 1000)")
    paths = {f: f"{out}/{f}/" for f in ("parquet", "csv", "json")}
    for f, p in paths.items():
        db.sql(f"COPY (SELECT * FROM t) TO '{p}' (FORMAT {f})")
    db.table("t").write_delta(f"{out}/delta")
    db.table("t").write_iceberg(f"{out}/iceberg")
    paths.update(delta=f"{out}/delta", iceberg=f"{out}/iceberg")
    rows = lambda frame: sorted(frame.rows(), key=lambda r: r["id"])
    fmt = lambda name: next(f for f in ("parquet", "csv", "json", "delta", "iceberg") if f in name) if "ndjson" not in name else "json"
    sql_rows = lambda fn, f: db.sql(f"SELECT * FROM {fn}('{paths[f]}') ORDER BY id").rows()
    # the table in the docs: each name exists and runs
    doc = open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "docs", "dataframe-api.md")).read()
    table = doc.split("<!-- vocabulary -->")[1].split("<!-- /vocabulary -->")[0]
    names_ = [n for line in table.splitlines()[3:] for cell in line.split("|")[2:] for n in re.findall(r"`([^`]+)`", cell)]
    df = spark.table("t")
    def works(n):
        if n.startswith("db."):
            return callable(getattr(db, n[3:], None))
        if n.startswith("pondra."):
            return callable(getattr(pondra, n[7:], None))
        if n.startswith("frame."):
            return callable(getattr(Frame, n[6:], None))
        if n.startswith("spark.read."):
            return callable(getattr(spark.read, "load" if "format(" in n else n[11:], None))
        if n.startswith("spark."):
            return callable(getattr(spark, n[6:], None))
        if n.startswith("df.write."):
            return callable(getattr(df.write, "save" if "format(" in n else n[9:], None))
        if n.startswith("COPY"):
            f = n.split("FORMAT ")[1].rstrip(")")
            db.sql(f"COPY (SELECT * FROM t) TO '{out}/copy_{f}/' (FORMAT {f})")
            return len(db.sql(f"SELECT * FROM read_{f}('{out}/copy_{f}/')").rows()) == 1000
        if n == "FROM t":
            return len(db.sql("FROM t").rows()) == 1000
        if n.startswith("INSERT") or n.startswith("CREATE"):
            db.sql("CREATE TABLE IF NOT EXISTS t2 AS SELECT * FROM t")
            return db.sql("INSERT INTO t2 SELECT * FROM t").get("rows") == 1000
        return len(sql_rows(n, fmt(n))) == 1000  # (a SQL function)
    missing = [n for n in names_ if not _try(lambda n=n: works(n))]
    checks[f"every name in dataframe-api.md's table exists and runs ({len(names_)} names)"] = len(names_) > 40 and not missing
    # an answer's columns named as other engines name them, where DataFusion would refuse the query
    cols = lambda q: [c["name"] for c in call(A.port, "POST", "/sql?format=typed&rows=1", q.encode())["columns"]]
    named = [cols("SELECT x::int FROM t"), cols("SELECT x::int, * FROM t"), cols("SELECT id, * FROM t"), cols("SELECT id, id FROM t"), cols("SELECT CAST(x AS INT), x FROM t")]
    checks["a cast is named as its column (Postgres), or as written beside a column of that name (Snowflake, DuckDB); a column twice runs, the one named twice as id_1"] = \
        named == [["x"], ["x::INT", "id", "name", "x"], ["id_1", "id", "name", "x"], ["id", "id_1"], ["CAST(x AS INT)", "x"]]
    # each fallback equals its standard name
    sql_pairs = {"read_parquet": ["parquet_scan"], "read_csv": ["read_csv_auto"], "read_json": ["read_json_auto", "read_ndjson"], "read_delta": ["delta_scan"], "read_iceberg": ["iceberg_scan"]}
    checks["SQL: parquet_scan, read_csv_auto, read_json_auto, read_ndjson, delta_scan, iceberg_scan == Pondra's names"] = \
        all(sql_rows(o, fmt(s)) == sql_rows(s, fmt(s)) and len(sql_rows(s, fmt(s))) == 1000 for s, others in sql_pairs.items() for o in others)
    py_pairs = {"read_parquet": ["scan_parquet"], "read_csv": ["scan_csv"], "read_json": ["scan_ndjson", "read_ndjson"], "read_delta": ["scan_delta"], "read_iceberg": ["scan_iceberg"]}
    checks["Python: scan_* and read_ndjson == read_*, on the connection and the module"] = \
        all(rows(getattr(where, o)(paths[fmt(s)])) == rows(getattr(where, s)(paths[fmt(s)])) for s, others in py_pairs.items() for o in others for where in (db, pondra))
    def written(fn, f):
        p = f"{out}/w_{fn}/"
        getattr(db.table("t"), fn)(p)
        return rows(getattr(db, f"read_{f}")(p))
    checks["frames: sink_parquet, sink_csv, sink_ndjson, write_ndjson == write_parquet, write_csv, write_json"] = \
        written("sink_parquet", "parquet") == written("write_parquet", "parquet") and written("sink_csv", "csv") == written("write_csv", "csv") \
        and written("sink_ndjson", "json") == written("write_json", "json") == written("write_ndjson", "json")
    spark_rows = lambda d: sorted((r.asDict() for r in d.collect()), key=lambda r: r["id"])
    checks["PySpark: spark.read.format('delta' | 'iceberg').load == read_delta, read_iceberg"] = \
        spark_rows(spark.read.format("delta").load(paths["delta"])) == rows(db.read_delta(paths["delta"])) and spark_rows(spark.read.format("iceberg").load(paths["iceberg"])) == rows(db.read_iceberg(paths["iceberg"]))
    # Delta and Iceberg tables in a folder
    d, i = f"{out}/modes_delta", f"{out}/modes_iceberg"
    small = db.sql("SELECT * FROM t WHERE id <= 10")
    small.write_delta(d)
    small.write_iceberg(i)
    refused = [_raises_text(lambda: small.write_delta(d)), _raises_text(lambda: db.sql(f"COPY (SELECT * FROM t) TO '{i}/' (FORMAT iceberg)"))]
    ignored = [small.write_delta(d, mode="ignore"), small.write_iceberg(i, mode="ignore")]
    small.write_delta(d, mode="append")
    db.sql(f"COPY (SELECT * FROM t WHERE id <= 10) TO '{i}/' (FORMAT iceberg, APPEND)")
    appended = [len(db.read_delta(d).rows()), len(db.read_iceberg(i).rows())]
    db.sql("SELECT * FROM t WHERE id <= 3").write_delta(d, mode="overwrite")
    df.filter("id <= 3").write.format("iceberg").mode("overwrite").save(i)
    latest = sorted(g.glob(f"{i}/metadata/v*.metadata.json"), key=lambda p: int(p.rsplit("/v", 1)[1].split(".")[0]))[-1]
    theirs = [delta_table(d).num_rows, StaticTable.from_metadata(latest).scan().to_arrow().num_rows]
    checks["Delta and Iceberg folders: made, APPEND, OVERWRITE; again without a mode refused, 'ignore' leaves it; delta-rs and PyIceberg agree"] = \
        all("is there already" in r for r in refused) and ignored == [None, None] and appended == [20, 20] \
        and [len(db.read_delta(d).rows()), len(db.read_iceberg(i).rows())] == [3, 3] and theirs == [3, 3]
    # A folder named relatively is where the node runs (as a notebook's `out/x`): written and read back
    rd, ri = os.path.relpath(f"{out}/rel_delta"), os.path.relpath(f"{out}/rel_iceberg")  # (the node runs where this does)
    small.write_delta(rd)
    small.write_iceberg(ri)
    back = [len(db.read_delta(rd).rows()), len(db.read_iceberg(ri).rows()), len(db.sql(f"SELECT * FROM delta_scan('{rd}')").rows())]
    rel_meta = sorted(g.glob(f"{out}/rel_iceberg/metadata/v*.metadata.json"))[-1]
    checks["a folder named relatively: written and read back (read_delta, read_iceberg, delta_scan); Iceberg's location absolute, as PyIceberg reads it"] = \
        back == [10, 10, 10] and StaticTable.from_metadata(rel_meta).scan().to_arrow().num_rows == 10
    node.kill()
    ok = all(checks.values())
    print(json.dumps({"names": checks, "ok": ok}, indent=1))
    if not ok:
        print("missing:", missing, refused, ignored, appended, theirs, back)
        sys.exit(1)
    return f"names: {len(names_)} names in the docs' table run, fallbacks equal, Delta and Iceberg folders: all {len(checks)} checks pass"


def answers():
    """Function answers reused for a while (ADR-028, E10): a second query within the lifetime
    calls nothing; after it, it calls again; a replaced function never reuses an old answer; a
    failed call isn't kept; a table function too; the decorator; refused on SQL functions,
    procedures and schedules."""
    lake, d = new_lake(), tempfile.mkdtemp(prefix="pondra-answers-")
    node = Node(lake, A.port, env={"PYTHONPATH": os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "python")}, python=sys.executable).start()
    db = _client(A.port)
    calls = os.path.join(d, "calls")
    n = lambda: sum(1 for _ in open(calls)) if os.path.exists(calls) else 0
    q = lambda s: sql(A.port, s)
    checks = {}
    body = lambda how: f"open({calls!r}, 'a').write(city + '\\n')\nreturn city.{how}()"
    db.create_function("geo", body("upper"), params={"city": "VARCHAR"}, returns="VARCHAR", cache="10 minutes")
    q("CREATE TABLE places AS SELECT 'c' || (value % 50) AS city FROM generate_series(1, 20000)")
    first = (q("SELECT count(DISTINCT geo(city)) AS n FROM places"), n())
    second = (q("SELECT count(DISTINCT geo(city)) AS n FROM places WHERE city <> ''"), n() - first[1])
    checks[f"a second query within the lifetime calls nothing ({first[1]} calls, then {second[1]})"] = first[0] == second[0] == [{"n": 50}] and 50 <= first[1] <= 50 * (os.cpu_count() or 1) and second[1] == 0
    db.create_function("geo", body("lower"), params={"city": "VARCHAR"}, returns="VARCHAR", cache="10 minutes")
    before = n()
    checks["a replaced function never reuses an old answer"] = q("SELECT min(geo(city)) AS m FROM places") == [{"m": "c0"}] and n() - before >= 50
    q(f"CREATE FUNCTION short(x BIGINT) RETURNS BIGINT LANGUAGE python WITH (cache = '1 second') AS $$ open({calls!r}, 'a').write('s\\n'); return x * 2 $$")
    run = lambda: q("SELECT sum(short(value % 3)) AS s FROM generate_series(1, 100)")
    k = [n(), run(), n(), run(), n(), time.sleep(1.3), run(), n()]
    checks["past its lifetime, it calls again"] = k[1] == k[3] == k[6] == [{"s": 200}] and (k[2] - k[0], k[4] - k[2], k[7] - k[4]) == (3, 0, 3)
    q(f"CREATE FUNCTION rates(base VARCHAR) RETURNS TABLE (cur VARCHAR, r DOUBLE) LANGUAGE python WITH (cache = '5 minutes') AS $$ open({calls!r}, 'a').write('t\\n'); return [(base, 1.0), ('x', 2.0)] $$")
    before = n()
    both = [q("SELECT * FROM rates('eur') ORDER BY cur"), q("SELECT count(*) AS n FROM rates('eur')"), q("SELECT count(*) AS n FROM rates('usd')")]
    checks["a table function's rows too (by its arguments)"] = both[0] == [{"cur": "eur", "r": 1.0}, {"cur": "x", "r": 2.0}] and both[1] == both[2] == [{"n": 2}] and n() - before == 2
    q(f"""CREATE FUNCTION flaky(x BIGINT) RETURNS BIGINT LANGUAGE python WITH (cache = '5 minutes') AS $$
    import os
    if not os.path.exists({calls!r} + '.ok'):
        open({calls!r} + '.ok', 'w').close()
        raise ValueError('the first call fails')
    return x
$$""")
    checks["a failed call isn't kept"] = "the first call fails" in _raises_text(lambda: q("SELECT flaky(1) AS v")) and q("SELECT flaky(1) AS v") == [{"v": 1}]
    refused = [_raises_text(lambda s=s: q(s)) for s in ("CREATE FUNCTION f(x INT) RETURNS INT LANGUAGE sql WITH (cache = '1 minute') AS $$ SELECT x $$",
                                                        "CREATE PROCEDURE p() LANGUAGE python WITH (cache = '1 minute') AS $$ pass $$",
                                                        "CREATE FUNCTION g(x INT) RETURNS INT LANGUAGE python WITH (cache = 'cron 0 * * * *') AS $$ return x $$")]
    checks["refused by name: on a SQL function, a procedure, a schedule"] = all("cache:" in r for r in refused)
    import pondra
    @db.function(cache="1 minute")
    def dbl(x: int) -> int:
        return x * 2
    checks["@db.function(cache=…), in SQL and frames"] = q("SELECT dbl(21) AS v") == [{"v": 42}] and db.sql("SELECT 5 AS x").select(pondra.fn.dbl(pondra.col("x")).alias("y")).rows() == [{"y": 10}]
    node.kill()
    ok = all(checks.values())
    print(json.dumps({"answers": checks, "ok": ok}, indent=1))
    if not ok:
        print(first, second, k, both, refused)
        sys.exit(1)
    return f"answers: reused within their lifetime, by definition and arguments, only after success: all {len(checks)} checks pass"


class _Twice(__import__("http.server").server.BaseHTTPRequestHandler):
    """A proxy to a node that sends every Iceberg commit twice (a retry after a lost answer)."""
    port = 0

    def _to(self, body=None):
        c = http.client.HTTPConnection("127.0.0.1", self.port, timeout=120)
        c.request(self.command, self.path, body, {k: v for k, v in self.headers.items() if k.lower() not in ("host", "content-length")})
        r = c.getresponse()
        return r.status, r.getheader("content-type") or "application/json", r.read()

    def _reply(self, status, kind, data):
        self.send_response(status)
        self.send_header("content-type", kind)
        self.send_header("content-length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        self._reply(*self._to())

    do_HEAD = do_GET

    def do_POST(self):
        body = self.rfile.read(int(self.headers.get("content-length", 0)))
        first = self._to(body)
        self._reply(*(self._to(body) if "/tables/" in self.path else first))

    def log_message(self, *_):
        pass


def writes():
    """Other engines append to Pondra's tables through its Iceberg REST catalog (ADR-028, G8), on
    two nodes: PyIceberg through the follower; the rows are the table's own (row ids; the writer's
    file recorded where it wrote it: ADR-029), and its snapshot is found under its id; two writers
    at once, one retrying on 409, each applied once; a commit sent twice applied once; a view and
    the Delta copy follow; an attached lake's table; Pondra itself writing to another Pondra's;
    refused by name: a keyed table, files outside the table's folder. (`adopted`: what round 27
    added; `rewrites`: round 28's deletes, overwrites and schema changes.)"""
    import deltalake, glob as g, http.server, pyarrow as pa
    from pyiceberg.catalog import load_catalog
    lake, other, third = new_lake(), new_lake(), new_lake()
    owner = uuid.uuid4().hex
    a = Node(lake, A.port, tier_secs=0.5).start()
    b = Node(lake, A.port + 1, tier_secs=0.5).start()
    o = Node(other, A.port + 2, tier_secs=0.5).start()
    q = lambda s, port=A.port: sql(port, s)
    checks = {}
    q("CREATE TABLE events (id BIGINT, name VARCHAR, amount DOUBLE) WITH (publish = 'iceberg,delta')")
    q("CREATE TABLE sales (region VARCHAR, amount DOUBLE) WITH (publish = 'iceberg')")
    q("CREATE MATERIALIZED VIEW by_region AS SELECT region, sum(amount) AS total FROM sales GROUP BY region")
    q("CREATE TABLE kv (k BIGINT PRIMARY KEY, v VARCHAR) WITH (publish = 'iceberg')")
    q("INSERT INTO events VALUES (1, 'a', 1.5), (2, 'b', 2.5)")
    # (on a bucket, PyIceberg writes its files with this environment's credentials, as any writer would)
    io = {"s3.endpoint": os.environ.get("AWS_ENDPOINT"), "s3.access-key-id": os.environ.get("AWS_ACCESS_KEY_ID"), "s3.secret-access-key": os.environ.get("AWS_SECRET_ACCESS_KEY"),
          "s3.region": os.environ.get("AWS_REGION", "auto"), "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO"} if A.s3 else {}  # (fsspec: on R2, pyarrow's multipart upload is refused; lake-format.md says so)
    cat = load_catalog("pondra", type="rest", uri=f"http://127.0.0.1:{A.port + 1}", **io)  # (the follower)
    rows = lambda lo, hi: pa.table({"id": pa.array(range(lo, hi), pa.int64()), "name": [f"n{i}" for i in range(lo, hi)], "amount": [i * 0.5 for i in range(lo, hi)]})
    t = cat.load_table("default.events")
    t.append(rows(100, 1100))
    snap = t.current_snapshot().snapshot_id
    t.refresh()
    got = q("SELECT count(*) AS n, count(_row_id) AS ids, count(DISTINCT _row_id) AS distinct_ids, sum(id) AS s FROM events WHERE id >= 100")
    left = g.glob(f"{lake}/data/events/data/*") if not A.s3 else []
    checks["PyIceberg appends through a follower: the rows are the table's own (row ids), its file where it wrote it, its snapshot found by its id"] = \
        got == [{"n": 1000, "ids": 1000, "distinct_ids": 1000, "s": sum(range(100, 1100))}] and (A.s3 or len(left) == 1) and t.metadata.snapshot_by_id(snap) is not None
    one, two = cat.load_table("default.events"), cat.load_table("default.events")
    one.append(rows(2000, 2100))
    two.append(rows(3000, 3100))  # (written on the snapshot before: 409, then PyIceberg retries on top)
    checks["two writers at once: the second gets 409, retries, and each append is in once"] = \
        q("SELECT count(*) AS n FROM events WHERE id >= 2000") == [{"n": 200}]
    _Twice.port = A.port
    proxy = http.server.ThreadingHTTPServer(("127.0.0.1", A.port + 9), _Twice)
    threading.Thread(target=proxy.serve_forever, daemon=True).start()
    twice = load_catalog("twice", type="rest", uri=f"http://127.0.0.1:{A.port + 9}", **io).load_table("default.events")
    twice.append(rows(5000, 5010))
    proxy.shutdown()
    checks["a commit sent twice (a retry after a lost answer) is applied once"] = q("SELECT count(*) AS n FROM events WHERE id >= 5000") == [{"n": 10}]
    cat.load_table("default.sales").append(pa.table({"region": ["eu", "us", "eu"], "amount": [1.0, 2.0, 3.0]}))
    view = until(lambda: q("SELECT region, total FROM by_region ORDER BY region"), [{"region": "eu", "total": 4.0}, {"region": "us", "total": 2.0}], 15)
    delta = until(lambda: _try(lambda: delta_table(f"{lake}/data/events").num_rows) if not A.s3 else 1212, 1212, 20)
    checks["a view of the table follows (in the same commit), and so does its Delta copy"] = view == [{"region": "eu", "total": 4.0}, {"region": "us", "total": 2.0}] and delta == 1212
    q(f"ATTACH '{other}' AS other")
    sql(A.port + 2, "CREATE TABLE stock (id BIGINT, qty BIGINT) WITH (publish = 'iceberg')")
    until(lambda: _try(lambda: cat.load_table("other.stock")) is not None, True, 15)  # (the follower attaches it a moment later)
    cat.load_table("other.stock").append(pa.table({"id": pa.array([1, 2], pa.int64()), "qty": pa.array([10, 20], pa.int64())}))
    checks["an attached lake's table, through this lake's catalog: its leader records it"] = \
        until(lambda: sql(A.port + 2, "SELECT sum(qty) AS s FROM stock"), [{"s": 30}], 15) == [{"s": 30}]
    c = Node(third, A.port + 3, env={"PONDRA_OWNER_KEY": owner}).start()
    as_owner = lambda s: call(A.port + 3, "POST", "/sql", s.encode(), headers={"x-pondra-owner": owner})
    as_owner(f"ATTACH 'http://127.0.0.1:{A.port}' AS pondra_a (TYPE iceberg)")
    as_owner("INSERT INTO pondra_a.default.events SELECT value + 9000, 'p', 0.0 FROM generate_series(1, 5)")
    checks["Pondra itself appends to another Pondra's table through its catalog"] = q("SELECT count(*) AS n FROM events WHERE name = 'p'") == [{"n": 5}]
    q("INSERT INTO kv VALUES (1, 'a')")
    q("CHECKPOINT")
    until(lambda: _try(lambda: cat.load_table("default.kv")) is not None, True, 30)
    cat.load_table("default.kv").append(pa.table({"k": pa.array([1, 2], pa.int64()), "v": ["x", "y"]}, schema=pa.schema([pa.field("k", pa.int64(), nullable=False), ("v", pa.string())])))
    checks["a keyed table takes an append as upserts"] = q("SELECT k, v FROM kv ORDER BY k") == [{"k": 1, "v": "x"}, {"k": 2, "v": "y"}]
    refused = {}
    outside = None if A.s3 else f"{lake}/data/sales/stray.parquet"  # (in the lake, not in the table's data folder)
    if outside:
        import pyarrow.parquet as pq
        pq.write_table(pa.table({"region": ["zz"], "amount": [9.0]}), outside)
        refused["files outside the table's folder"] = _raises_text(lambda: cat.load_table("default.sales").add_files([outside]))
    said = {"files outside the table's folder": "data folder"}
    checks["refused by name: " + (", ".join(refused) or "(on R2, nothing to try)")] = all(said[k] in v for k, v in refused.items()) \
        and (outside is None or os.path.exists(outside)) and q("SELECT count(*) AS n FROM events WHERE id < 10") == [{"n": 2}]
    [x.kill() for x in (a, b, o, c)]
    ok = all(checks.values())
    print(json.dumps({"writes": checks, "ok": ok}, indent=1))
    if not ok:
        print(got, left, view, delta, {k: v[:300] for k, v in refused.items()})
        sys.exit(1)
    return f"writes: PyIceberg and Pondra append through the Iceberg REST catalog, once each, views following: all {len(checks)} checks pass"


def adopted():
    """Other engines' appends recorded as written (ADR-029 phase 1, round 27), on two nodes, the
    follower taking PyIceberg's commits: tables made through the catalog (a partition spec, a
    write order, properties); an append's files are the table's where the writer put them, its
    rows with system columns from their lineage (ids distinct and in one run, one version, one
    time), read the same by Pondra (spread over three nodes too), PyIceberg and delta-rs; it costs the node its footers and a
    commit (a quarter of a copy's CPU at most); UPDATE and DELETE keep an adopted row's id; a
    merge writes the columns out, every row keeping its id and version; a partitioned table's
    files one day each, as its published spec says, and a file of two days refused by name; the
    sort order and a key's identifier fields published; NOT NULL enforced from the footers;
    renamed and dropped through the catalog. A table with a renamed column copies (Delta readers
    go by name)."""
    import datetime, deltalake, pyarrow as pa, pyarrow.parquet as pq, re as regex
    from pyiceberg.catalog import load_catalog
    from pyiceberg.manifest import DataFile, DataFileContent, FileFormat
    from pyiceberg.partitioning import PartitionSpec, PartitionField
    from pyiceberg.schema import Schema
    from pyiceberg.table.sorting import SortOrder, SortField
    from pyiceberg.transforms import DayTransform, IdentityTransform
    from pyiceberg.typedef import Record
    from pyiceberg.types import NestedField, LongType, StringType, TimestampType, DoubleType
    lake = new_lake()
    a = Node(lake, A.port, tier_secs=0.5).start()
    b = Node(lake, A.port + 1, tier_secs=0.5).start()
    c = Node(lake, A.port + 2, tier_secs=0.5).start()  # (three: a query spreads)
    q = lambda s, port=A.port: sql(port, s)
    io = {"s3.endpoint": os.environ.get("AWS_ENDPOINT"), "s3.access-key-id": os.environ.get("AWS_ACCESS_KEY_ID"), "s3.secret-access-key": os.environ.get("AWS_SECRET_ACCESS_KEY"),
          "s3.region": os.environ.get("AWS_REGION", "auto"), "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO"} if A.s3 else {}
    cat = load_catalog("pondra", type="rest", uri=f"http://127.0.0.1:{A.port + 1}", **io)  # (the follower)
    checks = {}
    three = Schema(NestedField(1, "id", LongType()), NestedField(2, "name", StringType()), NestedField(3, "amount", DoubleType()))
    events = cat.create_table("default.events", schema=three, properties={"publish": "iceberg,delta"})
    q("CREATE TABLE copied (id BIGINT, name VARCHAR, total DOUBLE) WITH (publish = 'iceberg')")
    q("ALTER TABLE copied RENAME COLUMN total TO amount")  # (a renamed column: its appends are copied)
    until(lambda: _try(lambda: cat.load_table("default.copied")) is not None, True, 30)  # (published by the leader's next round)
    copied = cat.load_table("default.copied")
    n = 200_000 if A.s3 else 1_000_000
    rows = lambda lo, hi: pa.table({"id": pa.array(range(lo, hi), pa.int64()), "name": pa.array([f"n{i % 1000}" for i in range(lo, hi)]), "amount": pa.array([i * 0.5 for i in range(lo, hi)])})
    data = rows(0, n)
    cpu = lambda: sum(sum(map(int, open(f"/proc/{x.p.pid}/stat").read().rsplit(")", 1)[1].split()[11:13])) for x in (a, b)) / os.sysconf("SC_CLK_TCK")
    c0 = cpu()
    events.append(data)
    adopt_cpu = cpu() - c0
    c0 = cpu()
    copied.append(data)
    copy_cpu = cpu() - c0
    got = q("SELECT count(*) AS n, count(DISTINCT _row_id) AS ids, max(_row_id) - min(_row_id) + 1 AS span, count(DISTINCT _version) AS versions, count(DISTINCT _created_at) AS times FROM events")
    files = [f.file.file_path for f in cat.load_table("default.events").scan().plan_files()]
    by_writer = lambda f: regex.search(r"/data/events/data/\d{5}-\d+-[0-9a-f-]{36}\.parquet$", f)
    checks["an append is the table's where it was written (no file copied), its rows with system columns from their lineage: ids distinct and in one run, one version, one time"] = \
        got == [{"n": n, "ids": n, "span": n, "versions": 1, "times": 1}] and bool(files) and all(by_writer(f) for f in files)
    noise = 0.03  # (the nodes' own loops meanwhile, heartbeats, tiering and publishing at 0.5 s, counted in 10 ms ticks: 0.04 s against a copy's 0.10 s on CI's fast runner)
    checks[f"it costs the node footers and a commit: under a quarter of a copy's CPU ({adopt_cpu:.2f} s against {copy_cpu:.2f} s for {n:,} rows)"] = A.s3 or (adopt_cpu - noise) * 4 < copy_cpu
    delta = until(lambda: _try(lambda: delta_table(f"{lake}/data/events").num_rows), n, 20) if not A.s3 else n
    checks["other engines read the rows as written: PyIceberg and delta-rs; a filter on them skips what it can"] = cat.load_table("default.events").scan(row_filter="id >= 1000 and id < 1010").to_arrow().num_rows == 10 \
        and delta == n and q("SELECT count(*) AS n, sum(id) AS s FROM events WHERE id BETWEEN 5000 AND 5009") == [{"n": 10, "s": sum(range(5000, 5010))}] \
        and q("SELECT count(*) AS n FROM copied") == [{"n": n}]
    grouped = "SELECT name, count(*) AS n, sum(amount) AS s, count(DISTINCT _row_id) AS ids FROM events WHERE id % 97 = 0 GROUP BY name ORDER BY name"
    spread = call(A.port + 2, "POST", "/sql?spread=1", grouped.encode(), timeout=120)
    checks["spread over three nodes, the adopted rows and their ids == one node's"] = spread == q(grouped) and len(spread) > 0
    kept = q("SELECT _row_id FROM events WHERE id = 150")
    q("UPDATE events SET name = 'changed' WHERE id = 150")
    q("DELETE FROM events WHERE id = 151")
    checks["UPDATE and DELETE on adopted rows: the row keeps its id"] = q("SELECT _row_id, name FROM events WHERE id = 150") == [{"_row_id": kept[0]["_row_id"], "name": "changed"}] \
        and q("SELECT count(*) AS n FROM events") == [{"n": n - 1}]
    small = cat.create_table("default.small", schema=three)
    for i in range(8):
        small.append(rows(i * 10, i * 10 + 10))
    ids = q("SELECT id, _row_id, _version FROM small ORDER BY id")
    merged = until(lambda: len(list(cat.load_table("default.small").scan().plan_files())), 1, 40)
    checks["a merge writes their system columns out: 8 appends' files in one, every row keeping its id and version"] = merged == 1 and len({r["_row_id"] for r in ids}) == 80 \
        and q("SELECT id, _row_id, _version FROM small ORDER BY id") == ids
    daily = cat.create_table("default.daily", schema=Schema(NestedField(1, "ts", TimestampType()), NestedField(2, "region", StringType()), NestedField(3, "n", LongType())),
                             partition_spec=PartitionSpec(PartitionField(source_id=1, field_id=1000, transform=DayTransform(), name="ts_day")))
    ts = [datetime.datetime(2026, 9, d, h) for d in (27, 28, 29) for h in (1, 5)]
    daily.append(pa.table({"ts": pa.array(ts, pa.timestamp("us")), "region": ["eu", "us"] * 3, "n": pa.array(range(6), pa.int64())}))
    days = [f.file.file_path.rsplit("/", 2)[1] for f in cat.load_table("default.daily").scan().plan_files()]
    two = f"{lake}/data/daily/data/two-days.parquet"  # (on this machine only: a writer that ignores the spec)
    if not A.s3:
        pq.write_table(pa.table({"ts": pa.array([datetime.datetime(2026, 9, 1), datetime.datetime(2026, 9, 2)], pa.timestamp("us")), "region": ["x", "y"], "n": pa.array([1, 2], pa.int64())}), two)
    def two_days():
        t = cat.load_table("default.daily")
        with t.transaction() as tx:
            with tx.update_snapshot().fast_append() as fa:
                fa.append_data_file(DataFile.from_args(content=DataFileContent.DATA, file_path=two, file_format=FileFormat.PARQUET, partition=Record(20697), record_count=2, file_size_in_bytes=os.path.getsize(two)))
    refused = "" if A.s3 else _raises_text(two_days)
    checks["a partitioned table: its spec published (day), the writer's files one day each, and a file of two days refused by name"] = str(cat.load_table("default.daily").spec().fields[0].transform) == "day" \
        and sorted(days) == ["ts_day=2026-09-27", "ts_day=2026-09-28", "ts_day=2026-09-29"] and (A.s3 or "more than one partition" in refused) \
        and q("SELECT sum(n) AS n FROM daily WHERE ts >= TIMESTAMP '2026-09-28 00:00:00'") == [{"n": 14}]
    sorted_t = cat.create_table("default.sorted", schema=three, sort_order=SortOrder(SortField(source_id=1, transform=IdentityTransform())))
    q("CREATE TABLE kv (k BIGINT PRIMARY KEY, v VARCHAR) WITH (publish = 'iceberg')")
    q("INSERT INTO kv VALUES (1, 'a')")
    q("CHECKPOINT")
    kv = until(lambda: _try(lambda: cat.load_table("default.kv").schema().identifier_field_ids), [1], 30)
    checks["the layout published: a write order as cluster_by and back, a key as identifier fields"] = [f.source_id for f in cat.load_table("default.sorted").sort_order().fields] == [1] \
        and kv == [1] and sorted_t is not None
    q("CREATE TABLE strict (id BIGINT NOT NULL, v VARCHAR) WITH (publish = 'iceberg')")
    q("INSERT INTO strict VALUES (1, 'a')")
    until(lambda: _try(lambda: cat.load_table("default.strict")) is not None, True, 30)
    nulls = _raises_text(lambda: cat.load_table("default.strict").append(pa.table({"id": pa.array([2, None], pa.int64()), "v": ["b", "c"]})))
    checks["a NULL in a NOT NULL column refused by name, from the file's footer"] = "NOT NULL" in nulls and q("SELECT count(*) AS n FROM strict") == [{"n": 1}]
    cat.rename_table("default.small", "default.smaller")
    renamed = q("SELECT count(*) AS n FROM smaller")
    cat.drop_table("default.smaller")
    checks["renamed and dropped through the catalog"] = renamed == [{"n": 80}] and bool(_raises_text(lambda: q("SELECT * FROM smaller"))) \
        and "default.smaller" not in [".".join(t) for t in cat.list_tables("default")]
    [x.kill() for x in (a, b, c)]
    ok = all(checks.values())
    print(json.dumps({"adopted": checks, "ok": ok, "cpu_s": {"adopted": adopt_cpu, "copied": copy_cpu, "rows": n}}, indent=1))
    if not ok:
        print(json.dumps({"got": got, "files": files[:3], "delta": delta, "ids": ids[:3], "days": days, "refused": refused[:300], "kv": kv, "nulls": nulls[:300], "renamed": renamed}, default=str))
        sys.exit(1)
    return f"adopted: other engines' appends recorded as written ({adopt_cpu:.2f} s of the node's CPU against a copy's {copy_cpu:.2f} s for {n:,} rows), layout published, tables made through the catalog: all {len(checks)} checks pass"


def ids():
    """Row ids and log places that can't wrap (ADR-029 §11). Row ids come in blocks from a counter
    of their own, not from the commit numbers: 300 commits of a node's rows take one block, a
    restart one more, a bulk INSERT one of its own. A log row's place (`_ord`, a Kafka offset)
    is its segment's number, then its row, in 24 bits: a segment with more of a table's rows
    than that takes the numbers after it too, and a consumer that seeks into them reads on from
    there. (Locally: the big segment is 2^24 + 10 rows.)"""
    import kafka as kp, pyarrow as pa
    lake = new_lake()
    kport = A.port + 30
    node = Node(lake, A.port, kafka=f"127.0.0.1:{kport}", changelog_secs=600).start()
    q = lambda s: sql(A.port, s)
    checks = {}
    q("CREATE TABLE t (i BIGINT)")
    for i in range(300):
        q(f"INSERT INTO t VALUES ({i})")
    hwm = call(A.port, "GET", "/stats")["hwm"]
    first = q("SELECT min(_row_id >> 32) AS lo, max(_row_id >> 32) AS hi, count(DISTINCT _row_id) AS n FROM t")
    b0 = first[0]["lo"]
    node.kill()
    node.start()
    q("INSERT INTO t VALUES (300)")
    q("INSERT INTO t SELECT value FROM generate_series(1000, 1999)")  # (a bulk INSERT: files, a block of its own)
    after = q("SELECT (SELECT _row_id >> 32 FROM t WHERE i = 300) AS restarted, (SELECT min(_row_id >> 32) FROM t WHERE i >= 1000) AS bulk_lo, (SELECT max(_row_id >> 32) FROM t WHERE i >= 1000) AS bulk_hi")
    checks[f"row ids from blocks of their own: {hwm} commits took one block, a restart the next, a bulk INSERT the one after"] = hwm >= 300 and first == [{"lo": b0, "hi": b0, "n": 300}] \
        and after == [{"restarted": b0 + 1, "bulk_lo": b0 + 2, "bulk_hi": b0 + 2}]
    if not A.s3:
        big = (1 << 24) + 10
        q("CREATE TABLE wide (i BIGINT)")
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, pa.schema([("i", pa.int64())])) as w:
            w.write_table(pa.table({"i": pa.array(range(big), pa.int64())}), max_chunksize=1 << 20)
        before = call(A.port, "GET", "/stats")["hwm"]
        seg = call(A.port, "POST", "/append/wide?producer=w&seq=1", sink.getvalue().to_pybytes(), headers={"content-type": "application/vnd.apache.arrow.stream"}, timeout=600)["seg"]
        took = call(A.port, "GET", "/stats")["hwm"] - before
        q("INSERT INTO wide VALUES (-1)")  # (the next segment)
        c = kp.KafkaConsumer(bootstrap_servers=f"127.0.0.1:{kport}", consumer_timeout_ms=5000)
        tp = kp.TopicPartition("wide", 0)
        c.assign([tp])
        c.seek(tp, (seg << 24) + (1 << 24) + 5)  # (a row past the first 2^24 of its segment)
        got = []
        for r in c:
            got.append((r.offset, json.loads(r.value)["i"]))
            if got[-1][1] == -1:
                break
        c.close()
        # (at least two: the node's own commits, history's or a tiering round's, may land while the append runs)
        checks["a segment of 2^24 + 10 rows of a table takes two numbers; a consumer seeking into the second reads on from there, then the next segment"] = took >= 2 \
            and [v for _, v in got] == [(1 << 24) + k for k in range(5, 10)] + [-1] and [o for o, _ in got][:5] == [(seg << 24) + (1 << 24) + k for k in range(5, 10)] \
            and got[-1][0] >> 24 >= seg + 2
    node.kill()
    ok = all(checks.values())
    print(json.dumps({"ids": checks, "ok": ok}, indent=1))
    if not ok:
        print(json.dumps({"hwm": hwm, "first": first, "after": after, "took": locals().get("took"), "got": locals().get("got", [])[:8]}, default=str))
        sys.exit(1)
    return f"ids: row ids and log places that can't wrap: all {len(checks)} checks pass"


def rewrites():
    """Other engines' copy-on-write changes (ADR-029 phase 2, round 28), on two nodes, PyIceberg
    committing through the follower: a DELETE that drops one whole file and rewrites part of
    another, the rows as PyIceberg reads them, delta-rs too; rows in files it left alone keep their
    ids, rows it rewrote get new ones (Iceberg v2's rule: a delete and an insert); an overwrite
    with a filter (a delete's snapshot and an append's, in one commit); the files taken out gone
    after --retain-secs; the stale rule: a row still in the log when the writer read the table
    gets it 409, and its DELETE, done again, takes that row too; refused by name: a change a view
    of the table can't take back (`followers`: those it can). Schema changes (PyIceberg's update_schema): a column added, renamed, widened and
    dropped, each an ALTER TABLE; a required column added refused."""
    import deltalake, pyarrow as pa
    from pyiceberg.catalog import load_catalog
    from pyiceberg.types import LongType, StringType
    lake = new_lake()
    a = Node(lake, A.port, tier_secs=600).start()  # (tiering far off: new rows wait in the log)
    b = Node(lake, A.port + 1, tier_secs=600).start()
    q = lambda s, port=A.port: sql(port, s)
    io = {"s3.endpoint": os.environ.get("AWS_ENDPOINT"), "s3.access-key-id": os.environ.get("AWS_ACCESS_KEY_ID"), "s3.secret-access-key": os.environ.get("AWS_SECRET_ACCESS_KEY"),
          "s3.region": os.environ.get("AWS_REGION", "auto"), "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO"} if A.s3 else {}
    cat = load_catalog("pondra", type="rest", uri=f"http://127.0.0.1:{A.port + 1}", **io)
    checks = {}
    q("CREATE TABLE t (id BIGINT, v VARCHAR) WITH (publish = 'iceberg,delta')")
    q("INSERT INTO t SELECT value, 'own' FROM generate_series(1, 100)")  # (Pondra's own file: a bulk INSERT)
    for lo in (101, 151):  # (two files of PyIceberg's)
        cat.load_table("default.t").append(pa.table({"id": pa.array(range(lo, lo + 50), pa.int64()), "v": ["theirs"] * 50}))
    ids = {r["id"]: r["_row_id"] for r in q("SELECT id, _row_id FROM t")}
    own_file = [f.file.file_path for f in cat.load_table("default.t").scan().plan_files() if "/data/t/data/" not in f.file.file_path]
    cat.load_table("default.t").delete("id <= 100 or id > 190")  # (Pondra's whole file, part of PyIceberg's second, none of its first)
    after = {r["id"]: r["_row_id"] for r in q("SELECT id, _row_id FROM t")}
    theirs = sorted(cat.load_table("default.t").scan().to_arrow().column("id").to_pylist())
    delta = until(lambda: _try(lambda: sorted(delta_table(f"{lake}/data/t").column("id").to_pylist())), list(range(101, 191)), 20) if not A.s3 else theirs
    checks["a DELETE from PyIceberg: a whole file dropped, part of another rewritten; Pondra, PyIceberg and delta-rs read the same rows"] = \
        sorted(after) == list(range(101, 191)) == theirs == delta
    checks["rows in a file it left alone keep their ids; the rows it rewrote get new ones (Iceberg v2: a delete and an insert)"] = len(set(after.values())) == 90 \
        and all(after[i] == ids[i] for i in range(101, 151)) and all(after[i] != ids[i] for i in range(151, 191))
    left = [f.file.file_path for f in cat.load_table("default.t").scan().plan_files()]
    gone = bool(own_file) and own_file[0] not in left and len(left) == 2
    checks["the file it took out is the table's no more (its files: PyIceberg's first, and the rewrite of its second)"] = gone
    cat.load_table("default.t").overwrite(pa.table({"id": pa.array([150], pa.int64()), "v": ["replaced"]}), overwrite_filter="id >= 150")
    checks["an overwrite with a filter (a delete's snapshot and an append's, one commit)"] = \
        q("SELECT count(*) AS n, max(id) AS hi, count(*) FILTER (WHERE v = 'replaced') AS r FROM t") == [{"n": 50, "hi": 150, "r": 1}]
    cat.load_table("default.t").append(pa.table({"id": pa.array([5001], pa.int64()), "v": ["published"]}))
    stale = cat.load_table("default.t")  # (read before the next row)
    q("INSERT INTO t VALUES (5000, 'in the log')")  # (tiering far off: in the log, not in any file)
    first = _raises_text(lambda: stale.delete("id >= 5000"))
    cat.load_table("default.t").delete("id >= 5000")
    checks["the stale rule: a row in the log when the writer read the table: 409 (its files written at once), and the DELETE done again takes it too"] = \
        bool(first) and q("SELECT count(*) AS n FROM t WHERE id >= 5000") == [{"n": 0}] and q("SELECT count(*) AS n FROM t") == [{"n": 50}]
    q("CREATE TABLE s (region VARCHAR, amount DOUBLE) WITH (publish = 'iceberg')")
    q("INSERT INTO s VALUES ('eu', 1.0), ('us', 2.0)")
    q("CREATE MATERIALIZED VIEW per_region AS SELECT region, sum(amount) AS total FROM s GROUP BY region")
    q("CHECKPOINT")
    until(lambda: _try(lambda: cat.load_table("default.s").scan().to_arrow().num_rows), 2, 20)
    followed = _raises_text(lambda: cat.load_table("default.s").delete("region = 'eu'"))
    checks["refused by name: a change to a table a view follows that can't take it back (no count to drop the groups it empties)"] = "count(*)" in followed \
        and q("SELECT count(*) AS n FROM s") == [{"n": 2}]
    q("CREATE TABLE u (id INT, name VARCHAR) WITH (publish = 'iceberg')")
    q("INSERT INTO u VALUES (1, 'a')")
    q("CHECKPOINT")
    until(lambda: _try(lambda: cat.load_table("default.u")) is not None, True, 30)
    def change(f):
        with cat.load_table("default.u").update_schema() as s:
            f(s)
    change(lambda s: s.add_column("score", LongType()))
    added = q("SELECT id, name, score FROM u")
    change(lambda s: s.rename_column("name", "label"))
    change(lambda s: s.update_column("id", LongType()))
    change(lambda s: s.delete_column("score"))
    shape = q("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'u' ORDER BY ordinal_position")
    required = _raises_text(lambda: change(lambda s: s.add_column("must", StringType(), required=True)))
    checks["schema changes through the catalog: a column added, renamed, widened and dropped, each an ALTER TABLE; a required one refused"] = \
        added == [{"id": 1, "name": "a"}] and [r["column_name"] for r in shape] == ["id", "label"] and "Int64" in shape[0]["data_type"] + str(q("SELECT arrow_typeof(id) AS t FROM u")) \
        and bool(required) and [f.name for f in cat.load_table("default.u").schema().fields] == ["id", "label"]
    [x.kill() for x in (a, b)]
    ok = all(checks.values())
    print(json.dumps({"rewrites": checks, "ok": ok}, indent=1))
    if not ok:
        print(json.dumps({"after": sorted(after)[:5], "theirs": theirs[:5], "delta": str(delta)[:200], "own_file": own_file, "gone": gone, "first": first[:300], "followed": followed[:300], "added": added, "shape": shape, "required": required[:200]}, default=str))
        sys.exit(1)
    return f"rewrites: other engines' copy-on-write DELETE and overwrite, the stale rule, schema changes: all {len(checks)} checks pass"


def transactions():
    """Other engines' commits that span tables or cross Pondra's upkeep (ADR-029 §7, round 28), on
    two nodes, PyIceberg's commits sent as Iceberg's REST catalog takes them: a transaction over two
    tables (`/v1/transactions/commit`) is one commit — both tables' rows, one `_version`, a view of
    each following; one whose second table changed since is refused (409) and changes neither; a
    copy-on-write DELETE planned before Pondra merged the files it takes out, sent after (as a
    writer that retries does, its check passed), is carried over to the merged file by its rows'
    ids: the rows as they should be, none twice; and a change naming rows gone since is refused."""
    import pyarrow as pa
    from pyiceberg.catalog import load_catalog
    from pyiceberg.table import CommitTableRequest, TableIdentifier
    lake = new_lake()
    a = Node(lake, A.port, tier_secs=3600).start()
    b = Node(lake, A.port + 1, tier_secs=3600).start()
    q = lambda s: sql(A.port, s)
    io = {"s3.endpoint": os.environ.get("AWS_ENDPOINT"), "s3.access-key-id": os.environ.get("AWS_ACCESS_KEY_ID"), "s3.secret-access-key": os.environ.get("AWS_SECRET_ACCESS_KEY"),
          "s3.region": os.environ.get("AWS_REGION", "auto"), "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO"} if A.s3 else {}
    cat = load_catalog("pondra", type="rest", uri=f"http://127.0.0.1:{A.port + 1}", **io)  # (the follower)
    checks = {}
    for t in ("orders", "lines"):
        q(f"CREATE TABLE {t} (id BIGINT, v VARCHAR) WITH (publish = 'iceberg')")
        q(f"CREATE MATERIALIZED VIEW {t}_n AS SELECT id, v FROM {t} WHERE id >= 0")
        q(f"INSERT INTO {t} VALUES (0, 'first')")
    q("CHECKPOINT")
    until(lambda: all(_try(lambda: cat.load_table(f"default.{t}")) is not None for t in ("orders", "lines")), True, 30)
    def staged(t, f):  # (a commit PyIceberg made ready: its files written, not sent)
        tx = cat.load_table(f"default.{t}").transaction()
        f(tx)
        return tx
    def change(t, tx, main=None):
        body = json.loads(CommitTableRequest(identifier=TableIdentifier(namespace=["default"], name=t), requirements=tx._requirements, updates=tx._updates).model_dump_json())
        for r in body["requirements"]:
            if r["type"] == "assert-ref-snapshot-id" and main is not None:
                r["snapshot-id"] = main  # (as a writer that retried sends it, having checked what came since)
        return body
    def send(path, body):
        c = http.client.HTTPConnection("127.0.0.1", A.port + 1, timeout=60)
        c.request("POST", path, json.dumps(body).encode(), {"content-type": "application/json"})
        r = c.getresponse()
        return r.status, r.read().decode()[:500]
    rows = lambda lo, hi: pa.table({"id": pa.array(range(lo, hi), pa.int64()), "v": [f"r{i}" for i in range(lo, hi)]})
    both = [change("orders", staged("orders", lambda tx: tx.append(rows(1, 4)))), change("lines", staged("lines", lambda tx: tx.append(rows(10, 16))))]
    status, said = send("/v1/transactions/commit", {"table-changes": both})
    versions = q("SELECT DISTINCT _version FROM orders WHERE id > 0 UNION ALL SELECT DISTINCT _version FROM lines WHERE id > 0")
    views = until(lambda: (q("SELECT count(*) AS n, sum(id) AS s FROM orders_n"), q("SELECT count(*) AS n, sum(id) AS s FROM lines_n")), ([{"n": 4, "s": 6}], [{"n": 7, "s": 75}]), 20)
    checks["a transaction over two tables is one commit: both tables' rows, one _version, each table's view following"] = status == 204 \
        and len({r["_version"] for r in versions}) == 1 and len(versions) == 2 and views == ([{"n": 4, "s": 6}], [{"n": 7, "s": 75}]) or said
    first, second = staged("orders", lambda tx: tx.append(rows(100, 101))), staged("lines", lambda tx: tx.append(rows(200, 201)))
    q("INSERT INTO lines VALUES (300, 'meanwhile')")
    q("CHECKPOINT")
    status, said = send("/v1/transactions/commit", {"table-changes": [change("orders", first), change("lines", second)]})
    checks["one whose second table changed since is refused (409), and changes neither table"] = status == 409 \
        and q("SELECT count(*) AS n FROM orders WHERE id = 100") == [{"n": 0}] and q("SELECT count(*) AS n FROM lines WHERE id = 200") == [{"n": 0}]
    q("CREATE TABLE ev (id BIGINT, v VARCHAR) WITH (publish = 'iceberg')")
    for i in range(7):
        q(f"INSERT INTO ev VALUES ({2 * i}, 'a'), ({2 * i + 1}, 'b')")
        q("CHECKPOINT")  # (a small file each: seven)
    until(lambda: _try(lambda: len(cat.load_table("default.ev").scan().plan_files())), 7, 30)
    ids = {r["id"]: r["_row_id"] for r in q("SELECT id, _row_id FROM ev")}
    planned = staged("ev", lambda tx: tx.delete("id = 5"))  # (a copy-on-write DELETE: one file out, one in)
    gone = staged("ev", lambda tx: tx.delete("id = 6"))
    q("INSERT INTO ev VALUES (14, 'a'), (15, 'b')")
    q("CHECKPOINT")  # (the eighth small file: Pondra merges the eight)
    merged = until(lambda: _try(lambda: len(cat.load_table("default.ev").scan().plan_files())), 1, 30)
    main = cat.load_table("default.ev").metadata.current_snapshot_id
    status, said = send("/v1/namespaces/default/tables/ev", change("ev", planned, main))
    after = q("SELECT id, _row_id FROM ev ORDER BY id")
    checks["a DELETE planned before Pondra merged its file, sent after: carried over to the merged file by row id; the rows as they should be, none twice, their ids kept"] = \
        status == 200 and merged == 1 and [r["id"] for r in after] == [i for i in range(16) if i != 5] and all(r["_row_id"] == ids[r["id"]] for r in after if r["id"] // 2 not in (2, 7)) or {"status": status, "merged": merged, "after": after, "ids": ids}
    q("DELETE FROM ev WHERE id = 7")
    q("CHECKPOINT")
    main = cat.load_table("default.ev").metadata.current_snapshot_id
    status, said = send("/v1/namespaces/default/tables/ev", change("ev", gone, main))
    checks["a change naming rows deleted since (and its file merged) is refused (409), the table as it was"] = status == 409 and "changed since" in said \
        and [r["id"] for r in q("SELECT id FROM ev ORDER BY id")] == [i for i in range(16) if i not in (5, 7)]
    [x.kill() for x in (a, b)]
    ok = all(v is True for v in checks.values())
    print(json.dumps({"transactions": checks, "ok": ok}, indent=1))
    if not ok:
        raise SystemExit("transactions: FAILED")
    return f"transactions: several tables' changes as one commit, other engines' changes carried over Pondra's merges: all {len(checks)} checks pass"


def upserts():
    """Keyed tables and other engines (ADR-029 §4, §5, round 28), on two nodes: a keyed table that
    publishes is read by PyIceberg, DuckDB (Delta) and Polars as Pondra reads it after every tier
    round, its older versions and delete markers positions (not only after a compaction); another
    engine's append to it is upserts, its delete (copy-on-write) delete markers, an equality delete
    on the key a marker too; each arrives in the change feed; a table that combines its rows
    (`order_by`) refuses them by name."""
    import duckdb, glob, site, pyarrow as pa, pyarrow.parquet as pq, polars as pl
    from pyiceberg.catalog import load_catalog
    from pyiceberg.manifest import ManifestWriterV2, ManifestContent, DataFile, DataFileContent, FileFormat, ManifestEntry, ManifestEntryStatus, write_manifest_list
    from pyiceberg.typedef import Record
    lake = new_lake()
    a = Node(lake, A.port, tier_secs=1, changelog_secs=600).start()
    b = Node(lake, A.port + 1, tier_secs=1).start()
    q = lambda s: sql(A.port, s)
    io = {"s3.endpoint": os.environ.get("AWS_ENDPOINT"), "s3.access-key-id": os.environ.get("AWS_ACCESS_KEY_ID"), "s3.secret-access-key": os.environ.get("AWS_SECRET_ACCESS_KEY"),
          "s3.region": os.environ.get("AWS_REGION", "auto"), "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO"} if A.s3 else {}
    cat = load_catalog("pondra", type="rest", uri=f"http://127.0.0.1:{A.port + 1}", **io)  # (the follower)
    con = duckdb.connect()
    for ext in ("delta",):
        found = [f for d in site.getsitepackages() for f in glob.glob(f"{d}/duckdb_extension_{ext}/**/{ext}.duckdb_extension", recursive=True)]
        con.execute(f"LOAD '{found[0]}'" if found else f"INSTALL {ext}; LOAD {ext}")
    if A.s3:
        con.execute(f"CREATE SECRET (TYPE s3, KEY_ID '{os.environ['AWS_ACCESS_KEY_ID']}', SECRET '{os.environ['AWS_SECRET_ACCESS_KEY']}', ENDPOINT '{os.environ['AWS_ENDPOINT'].split('://')[-1]}', URL_STYLE 'path', REGION 'auto', USE_SSL {str(os.environ['AWS_ENDPOINT'].startswith('https')).lower()})")
    checks = {}
    q("CREATE TABLE kv (k BIGINT PRIMARY KEY, v VARCHAR) WITH (publish = 'iceberg,delta')")
    ours = lambda: sorted((r["k"], r["v"]) for r in q("SELECT k, v FROM kv"))
    def theirs():
        t = cat.load_table("default.kv").scan().to_arrow()
        where = f"{lake}/data/kv"
        opts = {"storage_options": {"aws_endpoint_url": os.environ["AWS_ENDPOINT"], "aws_access_key_id": os.environ["AWS_ACCESS_KEY_ID"], "aws_secret_access_key": os.environ["AWS_SECRET_ACCESS_KEY"], "aws_region": "auto", "aws_allow_http": "true"}} if A.s3 else {}
        return (sorted(zip(t["k"].to_pylist(), t["v"].to_pylist())), sorted(con.execute(f"SELECT k, v FROM delta_scan('{where}')").fetchall()),
                sorted(pl.read_delta(where, **opts).select(["k", "v"]).rows()))
    same = lambda want: until(lambda: _try(lambda: (ours(), *theirs())), (want,) * 4, 30)
    rounds = []
    for stmt, want in (("INSERT INTO kv VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')", [(1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e")]),
                       ("INSERT INTO kv VALUES (2, 'B'), (6, 'f')", [(1, "a"), (2, "B"), (3, "c"), (4, "d"), (5, "e"), (6, "f")]),
                       ("DELETE FROM kv WHERE k = 4", [(1, "a"), (2, "B"), (3, "c"), (5, "e"), (6, "f")])):
        q(stmt)
        rounds.append(same(want) == (want,) * 4)
    files = len(cat.load_table("default.kv").scan().plan_files())
    checks["a keyed table read by PyIceberg, DuckDB (Delta) and Polars as Pondra reads it after every tier round, before any compaction"] = all(rounds) and files >= 2 or (rounds, files)
    now = [(1, "a"), (2, "B"), (3, "c"), (5, "e"), (6, "f")]
    key = pa.schema([pa.field("k", pa.int64(), nullable=False), pa.field("v", pa.string())])  # (the key is required)
    cat.load_table("default.kv").append(pa.table({"k": [3, 7], "v": ["three", "seven"]}, schema=key))
    now = sorted(dict(now + [(3, "three"), (7, "seven")]).items())
    checks["another engine's append is upserts: a key there is replaced, a new one added"] = same(now) == (now,) * 4
    cat.load_table("default.kv").delete("k = 1")
    now = [r for r in now if r[0] != 1]
    checks["its DELETE (copy-on-write): the key gone, the file's other rows as they were"] = same(now) == (now,) * 4
    # An equality delete on the key (Flink's upserts write them), as Iceberg's Java writers commit one.
    t = cat.load_table("default.kv")
    sid, parent, seq, loc = uuid.uuid4().int >> 65, t.metadata.current_snapshot_id, t.metadata.next_sequence_number(), t.metadata.location
    field = pa.field("k", pa.int64(), nullable=False, metadata={"PARQUET:field_id": "1"})
    path = f"{loc}/data/eq-{uuid.uuid4().hex}.parquet"
    with t.io.new_output(path).create() as f:
        pq.write_table(pa.table({"k": pa.array([2], pa.int64())}, schema=pa.schema([field])), f)
    eq = DataFile.from_args(content=DataFileContent.EQUALITY_DELETES, file_path=path, file_format=FileFormat.PARQUET, partition=Record(), record_count=1,
                            file_size_in_bytes=len(t.io.new_input(path)), equality_ids=[1])
    class Deletes(ManifestWriterV2):
        def content(self):
            return ManifestContent.DELETES
    with Deletes(t.spec(), t.schema(), t.io.new_output(f"{loc}/metadata/{uuid.uuid4()}-m0.avro"), sid, "deflate") as w:
        w.add_entry(ManifestEntry.from_args(status=ManifestEntryStatus.ADDED, snapshot_id=sid, data_file=eq))
    listed = f"{loc}/metadata/snap-{sid}-{uuid.uuid4()}.avro"
    with write_manifest_list(2, t.io.new_output(listed), sid, parent, seq, "deflate") as lw:
        lw.add_manifests([w.to_manifest_file()])
    snapshot = {"snapshot-id": sid, "parent-snapshot-id": parent, "sequence-number": seq, "timestamp-ms": int(time.time() * 1000), "manifest-list": listed, "summary": {"operation": "delete"}, "schema-id": t.schema().schema_id}
    body = {"requirements": [{"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": parent}],
            "updates": [{"action": "add-snapshot", "snapshot": snapshot}, {"action": "set-snapshot-ref", "ref-name": "main", "type": "branch", "snapshot-id": sid}]}
    c = http.client.HTTPConnection("127.0.0.1", A.port + 1, timeout=60)
    c.request("POST", "/v1/namespaces/default/tables/kv", json.dumps(body).encode(), {"content-type": "application/json"})
    r = c.getresponse()
    status, said = r.status, r.read().decode()[:400]
    now = [r for r in now if r[0] != 2]
    checks["an equality delete on the key (Flink's kind): a delete marker, the key gone"] = status == 200 and same(now) == (now,) * 4 or (status, said)
    def changes(after):
        body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": "changes", "arguments": {"table": "kv", "after": after}}}).encode()
        return json.loads(call(A.port, "POST", "/mcp", body, headers={"content-type": "application/json"}, timeout=120)["result"]["content"][0]["text"])
    state, pos = {}, 0
    for _ in range(100):
        got = changes(pos)
        for r in got["rows"]:
            if r["_change_type"] == "delete":
                state.pop(r["k"], None)
            else:
                state[r["k"]] = r["v"]
        if got["position"] == pos and not got["rows"]:
            break
        pos = got["position"]
    checks["the change feed replays to the table: Pondra's writes and the other engine's, as upserts and deletes"] = sorted(state.items()) == now or sorted(state.items())
    q("CREATE TABLE latest (k BIGINT PRIMARY KEY, ts BIGINT, v VARCHAR) WITH (order_by = 'ts', publish = 'iceberg')")
    q("INSERT INTO latest VALUES (1, 10, 'a')")
    until(lambda: _try(lambda: cat.load_table("default.latest")) is not None, True, 30)
    refused = _raises_text(lambda: cat.load_table("default.latest").append(pa.table({"k": [1], "ts": [5], "v": ["old"]}, schema=pa.schema([pa.field("k", pa.int64(), nullable=False), pa.field("ts", pa.int64()), pa.field("v", pa.string())]))))
    checks["a table that combines each key's rows (order_by) refuses another engine's change by name"] = "combines each key's rows" in refused or refused
    [x.kill() for x in (a, b)]
    ok = all(v is True for v in checks.values())
    print(json.dumps({"upserts": checks, "ok": ok}, indent=1, default=str))
    if not ok:
        raise SystemExit("upserts: FAILED")
    return f"upserts: keyed tables published every round, other engines' upserts and deletes: all {len(checks)} checks pass"


def followers():
    """File commits through the log (ADR-029 §7, round 28): a bulk INSERT's files and other engines'
    commits are the table's where they were written, and whatever follows the table follows them
    as it follows the log's rows, in the same commit. On two nodes, PyIceberg committing through the
    follower: two views (one adding up with a count, one row by row) follow a bulk INSERT, a
    PyIceberg append (no copy: its file where PyIceberg wrote it) and a PyIceberg DELETE (the views
    take back its rows); every row of a commit has that commit as its `_version`; the change feed
    replays to the table (inserts, deletes); a Kafka consumer reads every appended row in order, a
    big commit a piece at a time; a streaming task takes a bulk INSERT's and PyIceberg's rows, and a
    change it can't follow is refused by name; a stream join pairs rows bulk INSERTs wrote."""
    import kafka as kp, pyarrow as pa
    from pyiceberg.catalog import load_catalog
    lake = new_lake()
    kport = A.port + 30
    a = Node(lake, A.port, tier_secs=1, kafka=f"127.0.0.1:{kport}", changelog_secs=600).start()
    b = Node(lake, A.port + 1, tier_secs=1).start()
    q = lambda s, port=A.port: sql(port, s)
    io = {"s3.endpoint": os.environ.get("AWS_ENDPOINT"), "s3.access-key-id": os.environ.get("AWS_ACCESS_KEY_ID"), "s3.secret-access-key": os.environ.get("AWS_SECRET_ACCESS_KEY"),
          "s3.region": os.environ.get("AWS_REGION", "auto"), "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO"} if A.s3 else {}
    cat = load_catalog("pondra", type="rest", uri=f"http://127.0.0.1:{A.port + 1}", **io)  # (the follower)
    checks = {}
    q("CREATE TABLE ev (id BIGINT, g VARCHAR, amount DOUBLE) WITH (publish = 'iceberg')")
    q("CREATE MATERIALIZED VIEW per_g AS SELECT g, count(*) AS n, sum(amount) AS total FROM ev GROUP BY g")
    q("CREATE MATERIALIZED VIEW big AS SELECT id, g, amount FROM ev WHERE amount >= 500")
    q("INSERT INTO ev SELECT value, 'g' || (value % 3), value * 1.0 FROM generate_series(0, 99)")  # (a bulk INSERT: files)
    n = 20_000 if A.s3 else 100_000
    rows = lambda lo, hi: pa.table({"id": pa.array(range(lo, hi), pa.int64()), "g": [f"g{i % 3}" for i in range(lo, hi)], "amount": pa.array([float(i) for i in range(lo, hi)])})
    until(lambda: _try(lambda: cat.load_table("default.ev")) is not None, True, 30)
    cat.load_table("default.ev").append(rows(100, 100 + n))
    files = [f.file.file_path for f in cat.load_table("default.ev").scan().plan_files()]
    versions = q("SELECT count(DISTINCT _version) AS v, count(*) AS n FROM ev WHERE id >= 100")
    views = lambda: (q("SELECT g, n, total FROM per_g ORDER BY g"), q("SELECT count(*) AS n, sum(id) AS s FROM big"))
    want = lambda: (q("SELECT g, count(*) AS n, sum(amount) AS total FROM ev GROUP BY g ORDER BY g"), q("SELECT count(*) AS n, sum(id) AS s FROM ev WHERE amount >= 500"))
    appended = until(views, want(), 20)
    checks["two views follow a bulk INSERT and a PyIceberg append, its file where PyIceberg wrote it, each commit's rows its _version"] = appended == want() \
        and appended[0][0]["n"] == (n + 100 + 2) // 3 and versions == [{"v": 1, "n": n}] and any("/data/ev/data/" in f for f in files)
    cat.load_table("default.ev").delete("id < 50 or id >= 1000")
    deleted = until(views, want(), 20)
    checks["and a PyIceberg DELETE: the adding-up view takes the rows back, the row-by-row one drops them"] = deleted == want() \
        and q("SELECT count(*) AS n FROM ev") == [{"n": 950}] and deleted[1] == [{"n": 500, "s": sum(range(500, 1000))}]
    state, pos = {}, 0
    def changes(after):
        body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": "changes", "arguments": {"table": "ev", "after": after}}}).encode()
        return json.loads(call(A.port, "POST", "/mcp", body, headers={"content-type": "application/json"}, timeout=120)["result"]["content"][0]["text"])
    kinds = set()
    for _ in range(1000):
        got = changes(pos)
        for r in got["rows"]:
            kinds.add(r["_change_type"])
            if r["_change_type"] == "insert":
                state[r["_row_id"]] = r["id"]
            else:
                state.pop(r["_row_id"], None)
        if got["position"] == pos and not got["rows"]:
            break
        pos = got["position"]
    checks["the change feed replays to the table: the bulk INSERT's and PyIceberg's rows as inserts, its DELETE's as deletes (and the rows it rewrote)"] = \
        sorted(state.values()) == [r["id"] for r in q("SELECT id FROM ev ORDER BY id")] and kinds == {"insert", "delete"}
    c = kp.KafkaConsumer("ev", bootstrap_servers=f"127.0.0.1:{kport}", auto_offset_reset="earliest", consumer_timeout_ms=5000, max_partition_fetch_bytes=64 << 10, fetch_max_bytes=64 << 10)
    got = [json.loads(r.value)["id"] for r in c]
    c.close()
    checks["a Kafka consumer reads every row appended, in order, a big commit a piece at a time (and the rows a DELETE rewrote)"] = got[:100 + n] == list(range(100 + n))
    q("CREATE TABLE src (id BIGINT, v VARCHAR) WITH (publish = 'iceberg')")
    q("CREATE TABLE dst (id BIGINT, v VARCHAR)")
    call(A.port, "POST", "/tasks/copy_src", json.dumps({"source": "src", "target": "dst", "sql": "SELECT id, v FROM src"}).encode())
    q("INSERT INTO src SELECT value, 'bulk' FROM generate_series(1, 10)")
    until(lambda: _try(lambda: cat.load_table("default.src")) is not None, True, 30)
    cat.load_table("default.src").append(pa.table({"id": pa.array([11, 12], pa.int64()), "v": ["iceberg", "iceberg"]}))
    task = until(lambda: q("SELECT count(*) AS n, sum(id) AS s FROM dst"), [{"n": 12, "s": 78}], 20)
    refused = _raises_text(lambda: cat.load_table("default.src").delete("id = 1"))
    checks["a streaming task takes a bulk INSERT's rows and PyIceberg's; a change it can't follow is refused by name"] = task == [{"n": 12, "s": 78}] \
        and "task copy_src" in refused and q("SELECT count(*) AS n FROM src") == [{"n": 12}]
    q("CREATE TABLE orders (id BIGINT, amount DOUBLE)")
    q("CREATE TABLE payments (order_id BIGINT, paid DOUBLE)")
    q("CREATE MATERIALIZED VIEW paid WITH (join = 'streams') AS SELECT o.id, o.amount, p.paid FROM orders o JOIN payments p ON o.id = p.order_id")
    q("INSERT INTO orders SELECT value, value * 1.0 FROM generate_series(1, 50)")
    q("INSERT INTO payments SELECT value, value + 0.5 FROM generate_series(26, 75)")
    q("INSERT INTO orders SELECT value, value * 1.0 FROM generate_series(51, 60)")
    pairs = until(lambda: q("SELECT count(*) AS n, min(id) AS lo, max(id) AS hi FROM paid"), [{"n": 35, "lo": 26, "hi": 60}], 20)
    checks["a stream join pairs the rows bulk INSERTs wrote on either side, each pair once"] = pairs == [{"n": 35, "lo": 26, "hi": 60}]
    [x.kill() for x in (a, b)]
    ok = all(checks.values())
    print(json.dumps({"followers": checks, "ok": ok}, indent=1))
    if not ok:
        print(json.dumps({"appended": appended, "versions": versions, "files": files[:3], "deleted": deleted, "want": want(), "kinds": sorted(kinds), "state": len(state), "kafka": [len(got), got[:3], got[-3:]],
                          "task": task, "refused": refused[:300], "pairs": pairs}, default=str))
        sys.exit(1)
    return f"followers: file commits followed as the log's rows are (views, the change feed, Kafka, tasks, a stream join): all {len(checks)} checks pass"


def live():
    """Live queries (ADR-028, B3): an answer at once, then one within milliseconds of a commit
    that changes the query's table (an INSERT, an UPDATE, through a view), none for commits to
    other tables or that leave the answer as it was; Python and JavaScript; closed, nothing runs."""
    lake = new_lake()
    node = Node(lake, A.port).start()
    db = _client(A.port)
    q = lambda s: sql(A.port, s)
    checks = {}
    q("CREATE TABLE orders (id BIGINT, region VARCHAR, amount DOUBLE)")
    q("CREATE TABLE other (x BIGINT)")
    q("INSERT INTO orders VALUES (1, 'eu', 10), (2, 'us', 20)")
    q("CREATE VIEW big AS SELECT region, sum(amount) AS total FROM orders GROUP BY region")
    got, stop = [], threading.Event()
    def listen():
        for rows in db.live("SELECT * FROM big ORDER BY region"):
            got.append((time.time(), rows))
            if stop.is_set():
                break
    t = threading.Thread(target=listen, daemon=True)
    t.start()
    until(lambda: len(got), 1, 10)
    q("INSERT INTO other VALUES (1)")
    q("INSERT INTO orders VALUES (3, 'eu', 0)")  # (the answer stays as it was)
    time.sleep(1)
    quiet = len(got)
    lat = []
    for s in ("INSERT INTO orders VALUES (4, 'asia', 5)", "UPDATE orders SET amount = amount + 1 WHERE id = 1"):
        n0, t0 = len(got), time.time()
        q(s)
        until(lambda: len(got), n0 + 1, 10)
        lat.append(round((got[-1][0] - t0) * 1000) if len(got) > n0 else None)
    checks[f"an answer at once, then one after each commit that changes it ({lat} ms), none for other tables or the same answer"] = \
        quiet == 1 and got[0][1] == [{"region": "eu", "total": 10.0}, {"region": "us", "total": 20.0}] \
        and got[-1][1] == [{"region": "asia", "total": 5.0}, {"region": "eu", "total": 11.0}, {"region": "us", "total": 20.0}] and all(x is not None and x < 1000 for x in lat)
    stop.set()
    q("INSERT INTO orders VALUES (5, 'eu', 1)")
    t.join(10)
    open_after = until(lambda: call(A.port, "GET", "/stats")["live_queries"], 0, 25)
    checks["closed: nothing runs for it on the node (/stats live_queries back to 0)"] = not t.is_alive() and open_after == 0
    js = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "js", "index.js")
    script = f"""import {{ connect }} from {json.dumps(js)};
const db = connect("http://127.0.0.1:{A.port}"); const seen = [];
for await (const rows of db.live("SELECT count(*) AS n FROM orders")) {{ seen.push(rows[0].n); if (seen.length === 1) await db.sql("INSERT INTO orders VALUES (6, 'eu', 1)"); else break; }}
console.log(JSON.stringify(seen));"""
    path = os.path.join(tempfile.mkdtemp(prefix="pondra-live-"), "live.mjs")
    open(path, "w").write(script)
    js_out = subprocess.run(["node", path], capture_output=True, text=True, timeout=60)
    checks["JavaScript: for await (const rows of db.live(sql))"] = js_out.stdout.strip() == "[5,6]"
    checks["refused by name: rows sent with it, and a write"] = "never change" in _raises_text(lambda: next(db.live(db.from_arrow([{"a": 1}])))) \
        and "one query" in _raises_text(lambda: next(db.live("INSERT INTO other VALUES (2)")))
    # several on one connection (the console's: a browser opens six to a host at most), each line its id's
    body = json.dumps({"queries": [{"id": "a", "sql": "SELECT count(*) AS n FROM orders"}, {"id": "b", "sql": "SELECT count(*) AS n FROM other"},
                                   {"id": "bad", "sql": "INSERT INTO other VALUES (9)"}]}).encode()
    lines, shared = [], urllib.request.urlopen(urllib.request.Request(f"http://127.0.0.1:{A.port}/live", data=body, headers={"content-type": "application/json"}), timeout=30)
    def read_shared():
        try:
            for line in shared:
                if line.strip():
                    lines.append(json.loads(line))
        except (AttributeError, OSError, ValueError):  # (closed under it, below)
            pass
    threading.Thread(target=read_shared, daemon=True).start()
    first = until(lambda: sorted((m["id"], m.get("rows") or m.get("error", "")[:9]) for m in lines), [("a", [{"n": 6}]), ("b", [{"n": 1}]), ("bad", "a live qu")], 10)
    q("INSERT INTO other VALUES (2)")
    after = until(lambda: [(m["id"], m["rows"]) for m in lines[3:]], [("b", [{"n": 2}])], 10)
    while_open = call(A.port, "GET", "/stats")["live_queries"]
    shared.close()
    q("INSERT INTO orders VALUES (7, 'eu', 1)")
    shared_after = until(lambda: call(A.port, "GET", "/stats")["live_queries"], 0, 25)
    checks["several on one connection: each line names its query, a change answers only its own, a refused one says why alone, closed ends them all"] = \
        first == [("a", [{"n": 6}]), ("b", [{"n": 1}]), ("bad", "a live qu")] and after == [("b", [{"n": 2}])] and while_open == 2 and shared_after == 0
    node.kill()
    ok = all(checks.values())
    print(json.dumps({"live": checks, "ok": ok}, indent=1))
    if not ok:
        print(got, quiet, lat, open_after, js_out.stdout, js_out.stderr[-500:], lines)
        sys.exit(1)
    return f"live: answers pushed within {max(lat)} ms of a commit that changes them, nothing when closed: all {len(checks)} checks pass"


def temps():
    """Temporary tables and views (ADR-028), on three nodes: CREATE TEMP TABLE (with columns, AS
    a query), INSERT, UPDATE (a row keeps its _row_id), DELETE, MERGE, TEMP VIEW, DROP; they shadow
    a lake table; another session doesn't see them; a spread query reading one runs on its node
    with the same answer; not answered from the result cache; a procedure sees its caller's;
    a Postgres connection's end with it; close() and idleness end a session; without one, refused."""
    import psycopg
    lake = new_lake()
    env = {"PYTHONPATH": os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "python"), "PONDRA_SESSION_IDLE_SECS": "3"}
    nodes = [Node(lake, A.port + i, env=env, python=sys.executable, pg=f"127.0.0.1:{A.port + 10 + i}").start() for i in range(3)]
    time.sleep(1)
    db, other = _client(A.port), _client(A.port)
    rows = lambda s, c=db: c.sql(s).rows()
    run = lambda s, c=db: c.sql(s)
    checks = {}
    run("CREATE TABLE orders AS SELECT value AS id, 'u' || (value % 7) AS who, value * 1.0 AS amount FROM generate_series(1, 30000)")
    run("CREATE TEMP TABLE picked (id BIGINT, note VARCHAR)")
    run("INSERT INTO picked SELECT id, 'big' FROM orders WHERE amount > 29990")
    run("INSERT INTO picked (id) VALUES (1), (2)")
    before = {r["id"]: r["_row_id"] for r in rows("SELECT id, _row_id FROM picked")}
    run("UPDATE picked SET note = 'small' WHERE id < 10")
    run("DELETE FROM picked WHERE id = 2")
    run("MERGE INTO picked p USING (SELECT 29999 AS id UNION ALL SELECT 7) s ON p.id = s.id WHEN MATCHED THEN DELETE WHEN NOT MATCHED THEN INSERT VALUES (s.id, 'merged')")
    got = rows("SELECT id, note, _row_id FROM picked ORDER BY id")
    checks["CREATE TEMP TABLE, INSERT (all columns or some), UPDATE (a row keeps its _row_id), DELETE, MERGE"] = \
        [(r["id"], r["note"]) for r in got] == [(1, "small"), (7, "merged"), (29991, "big"), (29992, "big"), (29993, "big"), (29994, "big"), (29995, "big"), (29996, "big"), (29997, "big"), (29998, "big"), (30000, "big")] \
        and next(r["_row_id"] for r in got if r["id"] == 1) == before[1]
    run("CREATE TEMP TABLE top AS SELECT who, sum(amount) AS total FROM orders GROUP BY who")
    run("CREATE TEMP VIEW mine AS SELECT o.* FROM orders o JOIN picked USING (id)")
    joined = rows("SELECT count(*) AS n FROM mine")
    spread = [call(A.port, "POST", f"/sql?spread={s}", b"SELECT count(*) AS n, sum(o.amount) AS s FROM orders o JOIN picked USING (id)", headers={"x-pondra-session": db.session}) for s in (0, 1)]
    checks["TEMP VIEW, CTAS; a query spread over three nodes reading one runs on its node, with the same answer"] = \
        joined == [{"n": 11}] and spread[0] == spread[1] and spread[0][0]["n"] == 11 and len(rows("SELECT * FROM top")) == 7
    cached = [rows("SELECT count(*) AS n FROM picked"), run("INSERT INTO picked VALUES (5, 'x')"), rows("SELECT count(*) AS n FROM picked")]
    checks["not answered from the result cache (the same query after a change to it)"] = cached[0] == [{"n": 11}] and cached[2] == [{"n": 12}]
    run("CREATE TEMP TABLE orders AS SELECT 99 AS id")
    shadow = [rows("SELECT count(*) AS n FROM orders"), rows("SELECT count(*) AS n FROM orders", other)]
    run("DROP TABLE orders")
    checks["a temporary table shadows the lake's of its name, for its session only; DROP takes it away"] = \
        shadow == [[{"n": 1}], [{"n": 30000}]] and rows("SELECT count(*) AS n FROM orders") == [{"n": 30000}]
    unseen = [_raises_text(lambda: rows("SELECT * FROM picked", other)), _raises_text(lambda: call(A.port, "POST", "/sql", b"SELECT * FROM picked"))]
    no_session = _raises_text(lambda: call(A.port, "POST", "/sql", b"CREATE TEMP TABLE x (a INT)"))
    checks["another session doesn't see them; without a session, CREATE TEMP TABLE is refused by name"] = all("picked" in u for u in unseen) and "x-pondra-session" in no_session
    lake_view = _raises_text(lambda: run("CREATE VIEW v AS SELECT * FROM picked"))
    run("CREATE PROCEDURE count_picked() LANGUAGE python AS $$ return pondra.sql('SELECT count(*) AS n FROM picked').rows()[0]['n'] $$")
    answer = db.call("count_picked")
    checks["a lake's view over one refused; a procedure sees its caller's"] = "ends with the session" in lake_view and "12" in str(answer.rows() if hasattr(answer, "rows") else answer)
    with psycopg.connect(f"host=127.0.0.1 port={A.port + 10} user=u dbname=lake", autocommit=True) as pg:
        pg.execute("CREATE TEMP TABLE t1 (a INT)")
        pg.execute("INSERT INTO t1 VALUES (1), (2)")
        mine = pg.execute("SELECT sum(a) FROM t1 WHERE a > %s", (0,)).fetchall()  # (described, then run: both in the connection's session)
    time.sleep(0.5)
    with psycopg.connect(f"host=127.0.0.1 port={A.port + 10} user=u dbname=lake", autocommit=True) as pg:
        gone = _raises_text(lambda: pg.execute("SELECT * FROM t1").fetchall())
    checks["Postgres: a connection's own, gone when it ends"] = mine == [(3,)] and "t1" in gone
    session = db.session
    db.close()
    closed = _raises_text(lambda: call(A.port, "POST", "/sql", b"SELECT * FROM picked", headers={"x-pondra-session": session}))
    idle = _client(A.port)
    idle.sql("CREATE TEMP TABLE soon (a INT)")
    time.sleep(6.5)
    idled = _raises_text(lambda: idle.sql("SELECT * FROM soon").rows())
    checks["close() ends a session; so does idleness (PONDRA_SESSION_IDLE_SECS)"] = "picked" in closed and "soon" in idled
    [n.kill() for n in nodes]
    ok = all(checks.values())
    print(json.dumps({"temps": checks, "ok": ok}, indent=1))
    if not ok:
        print(got, before, joined, spread, cached, shadow, unseen, no_session, lake_view, mine, gone, closed, idled)
        sys.exit(1)
    return f"temps: a session's own tables and views, every statement, on its node: all {len(checks)} checks pass"


def across():
    """UPDATE, DELETE and MERGE on an attached lake from another lake's follower (ADR-028): that
    lake's leader carries them out, reading what it can't — this lake's tables, a session's
    temporary table, a file on this machine — sent with them; equal to the same statements run on
    that lake's own node; a retried job applies once; with nobody leading that lake, too."""
    lake, other = new_lake(), new_lake()
    owner, files = uuid.uuid4().hex, tempfile.mkdtemp(prefix="pondra-across-")
    a = [Node(lake, A.port + i, env={"PONDRA_OWNER_KEY": owner}).start() for i in range(2)]
    b = Node(other, A.port + 2).start()
    q = lambda s, port=A.port + 1, job=None, h=None: call(port, "POST", "/sql" + (f"?job={job}" if job else ""), s.encode(), headers={"x-pondra-owner": owner, **(h or {})})
    checks = {}
    rows4 = "(1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')"
    for s in ("CREATE TABLE t (id BIGINT, v VARCHAR)", "CREATE TABLE twin (id BIGINT, v VARCHAR)", f"INSERT INTO t VALUES {rows4}", f"INSERT INTO twin VALUES {rows4}",
              "CREATE TABLE src_twin (id BIGINT, v VARCHAR)", "INSERT INTO src_twin VALUES (2, 'B'), (5, 'e')", "CREATE TABLE fix (id BIGINT, v VARCHAR)",
              "INSERT INTO fix VALUES (3, 'C'), (6, 'f')", "CREATE TABLE drop_twin AS SELECT 4 AS id",  # (that lake's own, for the same statements there)
              "CREATE TABLE src (id BIGINT, v VARCHAR)", "INSERT INTO src VALUES (2, 'WRONG')", "CREATE TABLE drop_these AS SELECT 1 AS id"):  # (same names, other rows: never read for ours)
        sql(A.port + 2, s)
    q(f"ATTACH '{other}' AS b", A.port)
    until(lambda: _try(lambda: q("SELECT count(*) AS n FROM b.t")), [{"n": 4}], 15)  # (the follower attaches it a moment later)
    q("CREATE TABLE src (id BIGINT, v VARCHAR)", A.port)
    q("INSERT INTO src VALUES (2, 'B'), (5, 'e')", A.port)
    open(f"{files}/fix.csv", "w").write("id,v\n3,C\n6,f\n")
    h = {"x-pondra-session": "across-session-1"}
    q("CREATE TEMP TABLE drop_these AS SELECT 4 AS id", h=h)
    steps = ["UPDATE {t} SET v = v || '!' WHERE id = 1",
             "DELETE FROM {t} WHERE id IN (SELECT id FROM {drop})",
             "MERGE INTO {t} AS x USING {src} ON x.id = src.id WHEN MATCHED THEN UPDATE SET v = src.v WHEN NOT MATCHED THEN INSERT VALUES (src.id, src.v)",
             "MERGE INTO {t} AS x USING {fix} AS f ON x.id = f.id WHEN MATCHED THEN UPDATE SET v = f.v WHEN NOT MATCHED THEN INSERT VALUES (f.id, f.v)"]
    outs = []
    for s in steps:
        outs.append(q(s.format(t="b.t", fix=f"'{files}/fix.csv'", src="src", drop="drop_these"), h=h, job=f"across-{len(outs)}"))  # (this lake's src, the session's drop_these, a file here)
        sql(A.port + 2, s.format(t="twin", fix="fix", src="src_twin AS src", drop="drop_twin"))  # (the same, with that lake's own tables)
    theirs = sql(A.port + 2, "SELECT id, v FROM t ORDER BY id")
    twin = sql(A.port + 2, "SELECT id, v FROM twin ORDER BY id")
    checks["from a follower of another lake: UPDATE, DELETE with a temporary table, MERGE from this lake's table and from a file here == the same on that lake's node"] = \
        theirs == twin == [{"id": 1, "v": "a!"}, {"id": 2, "v": "B"}, {"id": 3, "v": "C"}, {"id": 5, "v": "e"}, {"id": 6, "v": "f"}]
    again = q(steps[0].format(t="b.t", fix="", src="", drop=""), h=h, job="across-0")
    checks["a retried job applies once"] = again.get("duplicate") is True and sql(A.port + 2, "SELECT v FROM t WHERE id = 1") == [{"v": "a!"}]
    b.kill()
    idle = call(A.port + 1, "POST", "/sql", b"UPDATE b.t SET v = 'idle' WHERE id = 6", timeout=180)  # (once its leader's lease lapses)
    seen = until(lambda: q("SELECT v FROM b.t WHERE id = 6", A.port), [{"v": "idle"}], 30)  # (another node: once it reads that lake's catalog again)
    checks["with nobody leading that lake: this node leads it for the moment it takes"] = idle.get("updated") == 1 and seen == [{"v": "idle"}]
    [n.kill() for n in a]
    ok = all(checks.values())
    print(json.dumps({"across": checks, "ok": ok}, indent=1))
    if not ok:
        print(outs, theirs, twin, again, idle)
        sys.exit(1)
    return f"across: changes to an attached lake from any node: all {len(checks)} checks pass"


# ---------------------------------------------------------------- round 26 (ADR-030)

def hot():
    """Hot columns skip the batches a filter rules out by their ranges, and only those (round 32):
    a table whose rows came in time order, held in memory, asked for a range of its time, a top-N of
    it, a key, and NULL-sensitive filters over a column with a batch of NULLs; every answer the
    model's, and the range, the top-N and the key skip batches (`pondra_hot_batches_skipped_total`)."""
    import re
    lake = new_lake()
    node = Node(lake, A.port, env={"PONDRA_HOT_GB": "1"}).start()
    q = lambda s: sql(A.port, s)
    skipped = lambda: float(g.group(1)) if (g := re.search(rb"\npondra_hot_batches_skipped_total (\S+)", call(A.port, "GET", "/metrics"))) else 0.0
    t0, n = 1767225600, 200000
    nulls = set(range(3 * 8192, 4 * 8192)) | set(range(0, n, 7))  # (a whole batch of NULLs, and every 7th row)
    k = lambda v: None if v in nulls else v % 1000
    q(f"CREATE TABLE ev AS SELECT value AS i, to_timestamp({t0} + value) AS ts, CASE WHEN value BETWEEN {3 * 8192} AND {4 * 8192 - 1} OR value % 7 = 0 THEN NULL ELSE value % 1000 END AS k, 'r' || value AS s FROM range(0, {n})")
    ts = lambda v: f"to_timestamp({t0} + {v})"
    asks = {  # what each asks, the model's answer, and whether it must skip batches
        "a range of the time": (f"SELECT count(*) AS n, sum(i) AS s FROM ev WHERE ts >= {ts(100000)} AND ts < {ts(100500)}", [{"n": 500, "s": sum(range(100000, 100500))}], True),
        "the oldest rows (a top-N)": ("SELECT i FROM ev ORDER BY ts LIMIT 3", [{"i": 0}, {"i": 1}, {"i": 2}], True),
        "the newest rows (a top-N read from the end)": ("SELECT i FROM ev ORDER BY ts DESC LIMIT 3", [{"i": n - 1}, {"i": n - 2}, {"i": n - 3}], True),
        "the newest rows a string filter keeps (read from the end, through the filter)": ("SELECT i FROM ev WHERE s LIKE 'r1%' ORDER BY ts DESC LIMIT 3", [{"i": 199999}, {"i": 199998}, {"i": 199997}], True),
        "one key": ("SELECT count(*) AS n FROM ev WHERE i = 150000", [{"n": 1}], True),
        "the oldest rows a string filter keeps (a top-N above a filter)": ("SELECT i FROM ev WHERE s LIKE 'r1%' ORDER BY ts LIMIT 3", [{"i": 1}, {"i": 10}, {"i": 11}], True),
        "IS NULL": ("SELECT count(*) AS n FROM ev WHERE k IS NULL", [{"n": len(nulls)}], False),
        "IS DISTINCT FROM": ("SELECT count(*) AS n FROM ev WHERE k IS DISTINCT FROM 5", [{"n": sum(1 for v in range(n) if k(v) != 5)}], False),
        "NOT =": ("SELECT count(*) AS n FROM ev WHERE NOT (k = 5)", [{"n": sum(1 for v in range(n) if k(v) is not None and k(v) != 5)}], False),
        "IN": ("SELECT count(*) AS n FROM ev WHERE k IN (1, 2, 3)", [{"n": sum(1 for v in range(n) if k(v) in (1, 2, 3))}], False),
        "IS NOT NULL in the NULLs' batch": ("SELECT count(*) AS n FROM ev WHERE k IS NOT NULL AND i BETWEEN 24000 AND 33000", [{"n": sum(1 for v in range(24000, 33001) if k(v) is not None)}], False),
        "coalesce(k, -1) = -1": ("SELECT count(*) AS n FROM ev WHERE coalesce(k, -1) = -1", [{"n": len(nulls)}], False),
        "nothing": (f"SELECT count(*) AS n FROM ev WHERE i > {n}", [{"n": 0}], False),
    }
    checks, seen = {}, {}
    try:
        for run in range(2):  # (a file's columns load on its second read)
            for ask, _, _ in asks.values():
                q(ask + f" -- warm {run}")
        held = hot_settled(A.port)
        checks["the table's columns are in memory"] = held > 0
        for name, (ask, want, skips) in asks.items():
            before = skipped()
            got = q(ask)
            seen[name] = {"got": got if got != want else "== model", "skipped": skipped() - before}
            checks[f"{name}: the model's answer" + (", batches skipped" if skips else "")] = got == want and (not skips or skipped() > before)
    finally:
        node.kill()
        clean_up()
    ok = all(checks.values())
    print(json.dumps({"hot": checks, "seen": seen, "ok": ok}, indent=1))
    if not ok:
        sys.exit(1)
    return f"hot: batches skipped by their ranges, answers the model's: all {len(checks)} checks pass"


def minmax():
    """A global min/max hands the scans a filter of the rows that could still change its answer, and
    the scans skip row groups by it (round 32's fix of DataFusion's): a table of 24 files read from
    them, each file's `a` above the last's, `b` NULL in the first six and `c` in all but the last.
    Every answer the model's; without the fix `max(b + 1)` came back too low and `max(c)` NULL. And a
    top-N of 20 columns through a filter, which decodes its files filtering as it goes."""
    lake = new_lake()
    node = Node(lake, A.port, env={"PONDRA_HOT_GB": "0"}).start()
    q = lambda s: sql(A.port, s)
    files, n = 24, 100000
    q("CREATE TABLE t (a BIGINT, b BIGINT, c BIGINT, s VARCHAR)")
    for f in range(files):  # (a bulk INSERT is a file of its own)
        q(f"INSERT INTO t SELECT value + {f * n}, {'NULL' if f < 6 else f}, {f if f == files - 1 else 'NULL'}, 's' || {f} FROM range(0, {n})")
    cols = ", ".join(f"value + {i} AS c{i}" for i in range(18))
    q(f"CREATE TABLE w AS SELECT value AS a, 's' || value AS s, {cols} FROM range(0, {files * n // 2})")
    q(f"INSERT INTO w SELECT value AS a, 's' || value AS s, {cols} FROM range({files * n // 2}, {files * n})")
    asks = {  # what each asks and the model's answer
        "a min and the max of an expression": ("SELECT min(a) AS lo, max(b + 1) AS hi FROM t", [{"lo": 0, "hi": files}]),
        "a min and the max of a column NULL in the first files": ("SELECT min(a) AS lo, max(b) AS hi FROM t", [{"lo": 0, "hi": files - 1}]),
        "a min and the max of a column NULL in all files but the last": ("SELECT min(a) AS lo, max(c) AS hi FROM t", [{"lo": 0, "hi": files - 1}]),
        "a min of strings and the max of a column NULL in all files but the last": ("SELECT min(s) AS lo, max(c) AS hi FROM t", [{"lo": "s0", "hi": files - 1}]),
        "a filtered min and a max": ("SELECT min(a) FILTER (WHERE c IS NOT NULL) AS lo, max(a) AS hi FROM t", [{"lo": (files - 1) * n, "hi": files * n - 1}]),
        "the min and max of one column": ("SELECT min(a) AS lo, max(a) AS hi FROM t", [{"lo": 0, "hi": files * n - 1}]),
        "one max": ("SELECT max(c) AS hi FROM t", [{"hi": files - 1}]),
        # (a top-N of many columns decodes its files filtering as it goes: `optimize::WideTopN`)
        "a top-N of many columns through a filter": ("SELECT * FROM w WHERE s LIKE '%77%' ORDER BY a DESC LIMIT 3", [{"a": a, "s": f"s{a}", **{f"c{i}": a + i for i in range(18)}} for a in sorted((v for v in range(files * n) if "77" in f"s{v}"), reverse=True)[:3]]),
    }
    checks, seen = {}, {}
    try:
        for name, (ask, want) in asks.items():
            got = [q(ask + f" -- {run}") for run in range(3)]
            seen[name] = "== model" if all(g == want for g in got) else got
            checks[f"{name}: the model's answer, three times"] = all(g == want for g in got)
    finally:
        node.kill()
        clean_up()
    ok = all(checks.values())
    print(json.dumps({"minmax": checks, "seen": seen, "ok": ok}, indent=1))
    if not ok:
        sys.exit(1)
    return f"minmax: a global min/max skips no row it needs: all {len(checks)} checks pass"


def history():
    """Every statement a door was sent is a row of `pondra.history` (ADR-048), a second later: its
    door, user, node, session, outcome, time and rows. A slow one (`PONDRA_SLOW_MS`) keeps its plan,
    each operator with its rows, and its trace: each node's share when it ran across three nodes, and
    a line in the node's log. Past `PONDRA_HISTORY_RATE` a second, fast statements are counted in one
    row, not written; `PONDRA_HISTORY=off` writes none; an admin reads every row, a user their own."""
    import base64, psycopg
    lake = new_lake()
    a = Node(lake, A.port, env={"PONDRA_HISTORY_RATE": "40"}, pg=f"127.0.0.1:{A.port + 10}").start()
    b = Node(lake, A.port + 1, env={"PONDRA_SLOW_MS": "0"}).start()  # (every statement slow: a plan each)
    c = Node(lake, A.port + 2, env={"PONDRA_HISTORY": "off"}).start()
    q = lambda s, i=0, path="/sql", headers=None: call(A.port + i, "POST", path, s.encode(), headers=headers)
    rows_of = lambda tag: [r for r in q(f"SELECT * FROM pondra.history WHERE statement LIKE '%{tag}%' AND statement NOT LIKE '%pondra.history%'") if tag in r["statement"]]
    def until(tag, want=1, secs=15):
        deadline = time.time() + secs
        while (got := rows_of(tag)) and len(got) < want or not got:
            if time.time() > deadline:
                return got
            time.sleep(0.5)
        return got
    checks, seen = {}, {}
    try:
        q("CREATE TABLE t (k BIGINT, v VARCHAR)")
        for f in range(6):  # (a bulk INSERT is a file of its own: something to deal out)
            q(f"INSERT INTO t SELECT value, 'v' || value FROM range({f * 10000}, {(f + 1) * 10000})")
        q("SELECT count(*) AS n FROM t -- h-http", headers={"x-pondra-session": "tab-0001"})
        with psycopg.connect(f"host=127.0.0.1 port={A.port + 10} user=u dbname=lake", autocommit=True) as pg:
            pg.execute("SELECT k FROM t WHERE k < 3 -- h-pg").fetchall()
        try:
            q("SELECT * FROM nope -- h-failed")
        except RuntimeError:
            pass
        http, pgr, failed = until("h-http"), until("h-pg"), until("h-failed")
        seen.update(http=http, pg=pgr, failed=failed)
        h = http[0] if http else {}
        checks["HTTP: door, user, node, session, outcome, time and rows"] = (h.get("door"), h.get("outcome"), h.get("rows"), h.get("node"), h.get("class")) == ("http", "ok", 1, f"127.0.0.1:{A.port}", "read") \
            and str(h.get("session", "")).endswith("tab-0001") and h.get("ms") is not None and h.get("plan") is None and len(http) == 1
        checks["Postgres: its door, its rows"] = [(r["door"], r["outcome"], r["rows"]) for r in pgr] == [("postgres", "ok", 3)]
        checks["a failed statement: failed, with its error"] = [(r["outcome"], "nope" in (r["error"] or "")) for r in failed] == [("failed", True)]
        try:  # (a client that stops waiting: its statement is dropped on the node, and kept as stopped)
            urllib.request.urlopen(urllib.request.Request(f"http://127.0.0.1:{A.port}/sql", b"SELECT sum(value) AS s FROM range(0, 4000000000) -- h-stopped", method="POST"), timeout=0.5)
        except OSError:
            pass
        stopped = until("h-stopped")
        seen["stopped"] = [{k: r[k] for k in ("outcome", "door", "user", "ms")} for r in stopped]
        checks["a statement its client stopped waiting for: stopped, by its door"] = [(r["outcome"], r["door"]) for r in stopped] == [("stopped", "http")]
        # A slow statement (every one on b): its plan, its operators' rows; spread, each node's share
        q("SELECT count(*) AS n FROM t WHERE k % 7 = 0 -- h-slow", 1)
        while len(call(A.port + 1, "GET", "/stats")["nodes"]) < 3:
            time.sleep(0.2)
        call(A.port + 1, "POST", "/sql?spread=1", b"SELECT v, count(*) AS n FROM t GROUP BY v ORDER BY n DESC, v LIMIT 3 -- h-spread")
        slow, spread = until("h-slow"), until("h-spread")
        seen.update(slow=[{k: r[k] for k in ("ms", "rows", "plan")} for r in slow], spread=[{k: r[k] for k in ("nodes", "trace")} for r in spread])
        checks["a slow statement keeps its plan, each operator with its rows"] = len(slow) == 1 and "output_rows=" in (slow[0]["plan"] or "") and slow[0]["rows"] == 1
        trace = json.loads(spread[0]["trace"] or "[]") if spread else []
        shares = {s["node"] for s in trace if s["what"] in ("its share", "step 0")}
        checks["…spread over three nodes: each node's share in its trace"] = len(spread) == 1 and spread[0]["nodes"] == 3 and shares == {f"127.0.0.1:{A.port + i}" for i in range(3)}
        checks["…and a line in the node's log"] = "slow statement:" in open(b.log).read() and "h-slow" in open(b.log).read()
        # PONDRA_HISTORY=off: nothing from that node
        q("SELECT 5 AS x -- h-off", 2)
        time.sleep(2.5)
        checks["PONDRA_HISTORY=off: none of its statements"] = rows_of("h-off") == []
        # Past the rate (40 a second on a): counted in one row, not written
        time.sleep(1 - time.time() % 1)
        for i in range(120):
            q(f"SELECT {i} AS x -- h-rate-{i:03}")
        time.sleep(1.2)
        q("SELECT 1 AS x -- h-after")  # (a new second: the row counting what was skipped)
        until("h-after")
        written = rows_of("h-rate-")
        skipped = sum(r["rows"] for r in q("SELECT rows FROM pondra.history WHERE class = 'skipped'"))
        seen["rate"] = {"written": len(written), "skipped": skipped}
        checks["past PONDRA_HISTORY_RATE a second: the rest counted in one row, not written"] = 40 <= len(written) <= 80 and len(written) + skipped == 120
        # An admin reads every row; a user, their own
        basic = lambda u, p: {"Authorization": "Basic " + base64.b64encode(f"{u}:{p}".encode()).decode()}
        ann, boss = basic("ann", "ann-password-1"), basic("boss", "boss-password-1")
        q("CREATE USER boss PASSWORD 'boss-password-1' SUPERUSER")  # (then everyone signs in)
        for s in ["CREATE USER ann PASSWORD 'ann-password-1'", "GRANT SELECT ON t TO ann"]:
            q(s, headers=boss)
        q("SELECT count(*) AS n FROM t -- h-ann", headers=ann)
        time.sleep(2.5)
        mine = q("SELECT DISTINCT \"user\" FROM pondra.history", headers=ann)
        everyone = q("SELECT DISTINCT \"user\" FROM pondra.history", headers=boss)
        seen["users"] = {"ann sees": mine, "boss sees": everyone}
        checks["an admin reads every row, a user their own"] = mine == [{"user": "ann"}] and len(everyone) > 1
    finally:
        for n in (a, b, c):
            n.kill()
        clean_up()
    ok = all(checks.values())
    print(json.dumps({"history": checks, "seen": seen, "ok": ok}, indent=1, default=str))
    if not ok:
        sys.exit(1)
    return f"history: every statement a row, slow ones with plans and traces: all {len(checks)} checks pass"


def found():
    """What writing the docs found (round 26), each fixed: a filtered materialized view follows
    UPDATE and DELETE; a producer's seq 0 refused (HTTP, Flight); a merge table that leaves a
    column unmerged, an unknown WITH option and CREATE EXTERNAL TABLE refused at once; a value
    that doesn't cast refused (INSERT, DoPut), not stored as NULL; a task into a keyed table made
    in SQL; NOT NULL and DEFAULT on every door; INSERT … ON CONFLICT, UPDATE … FROM, DELETE …
    USING, TRUNCATE; BINARY, VARBINARY,
    BLOB, VARIANT, JSON columns and substr on bytes; FROM-first with WHERE; a comment before
    CREATE FUNCTION; COPY's count with a header; a write token's message; PUT /files twice
    without the node's paths; files() and file_read() with or without `files/`; a Flight function
    callable as soon as it is made, and gone as soon as it is dropped; ADBC's handshake without
    padding, its ingest modes and table types; `pondra run`'s flags anywhere. And what the console
    found: a time without seconds, BASE TABLE, exact numbers, files() after PUT."""
    import datetime as dt, psycopg, pyarrow as pa, pyarrow.flight as fl
    import adbc_driver_flightsql.dbapi as adbc
    lake, guarded = new_lake(), new_lake()
    port, fport, pgport = A.port, A.port + 30, A.port + 10
    py = {"PYTHONPATH": os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "python")}
    node = Node(lake, port, env=py, python="auto", pg=f"127.0.0.1:{pgport}", flight=f"127.0.0.1:{fport}", kafka=f"127.0.0.1:{port + 20}", tier_secs=0.5).start()
    locked = Node(guarded, port + 1, env={"PYTHONPATH": ""}, python="auto", flight=f"127.0.0.1:{fport + 1}", read_token="r", write_token="w", admin_token="a").start()
    q = lambda s: sql(port, s)
    fails = lambda s: _raises_text(lambda: q(s))
    checks, seen = {}, {}
    # A materialized view of a filter follows UPDATE and DELETE of its table (ADR-020).
    q("CREATE TABLE orders (id BIGINT, region VARCHAR, amount BIGINT)")
    q("INSERT INTO orders VALUES (1, 'eu', 10), (2, 'us', 20), (3, 'eu', 30)")
    q("CREATE MATERIALIZED VIEW eu AS SELECT id, amount FROM orders WHERE region = 'eu'")
    q("UPDATE orders SET amount = 99 WHERE id = 1")
    q("DELETE FROM orders WHERE id = 3")
    q("INSERT INTO orders VALUES (4, 'eu', 40)")
    want = [{"id": 1, "amount": 99}, {"id": 4, "amount": 40}]
    seen["view"] = until(lambda: q("SELECT id, amount FROM eu ORDER BY id"), want, 15)
    checks["a filtered materialized view follows UPDATE and DELETE of its table"] = seen["view"] == want
    # A producer's batches count from seq=1: seq 0 would be taken for one already written.
    q("CREATE TABLE ev (user VARCHAR, n BIGINT)")
    zero = _raises_text(lambda: call(port, "POST", "/append/ev?producer=p&seq=0", b'{"user": "a", "n": 1}\n'))
    call(port, "POST", "/append/ev?producer=p&seq=1", b'{"user": "a", "n": 1}\n')
    schema = pa.schema([("user", pa.string()), ("n", pa.int64())])
    client = fl.FlightClient(f"grpc://127.0.0.1:{fport}")
    def put(path, table):
        w, r = client.do_put(fl.FlightDescriptor.for_path(*path), table.schema)
        w.write_table(table)
        w.done_writing()
        while r.read() is not None:
            pass
        w.close()
    flight_zero = _raises_text(lambda: put(["ev", "f", "0"], pa.table({"user": ["b"], "n": pa.array([2], pa.int64())}, schema=schema)))
    checks["seq 0 refused, by HTTP and by Flight, and nothing written"] = "seq=1" in zero and "seq=1" in flight_zero and q("SELECT count(*) AS n FROM ev") == [{"n": 1}]
    # Refused when made, not when first used.
    seen["merge"] = fails("CREATE TABLE totals (k VARCHAR PRIMARY KEY, n BIGINT, s BIGINT) WITH (merge = 'n:sum')")
    seen["option"] = fails("CREATE TABLE w (a BIGINT) WITH (colour = 'red')")
    seen["external"] = fails("CREATE EXTERNAL TABLE x STORED AS CSV LOCATION 'x.csv'")
    checks["refused at CREATE: a merge table leaving a column unmerged, an unknown WITH option, CREATE EXTERNAL TABLE"] = \
        "s needs a merge function" in seen["merge"] and "colour" in seen["option"] and bool(seen["external"]) and "totals" not in json.dumps(q("SHOW TABLES")) \
        and call(port, "POST", "/tier", timeout=60) is not None
    # A value that doesn't cast is refused, as INSERT … SELECT and append refuse it.
    seen["cast"] = fails("INSERT INTO ev VALUES ('x', 'abc')")
    bad = pa.table({"user": ["c"], "n": ["abc"]})
    seen["flight_cast"] = _raises_text(lambda: put(["ev"], bad))
    checks["a value that doesn't cast is refused (INSERT … VALUES, DoPut), not stored as NULL"] = "abc" in seen["cast"] and "abc" in seen["flight_cast"] \
        and q("SELECT count(*) AS n FROM ev") == [{"n": 1}]
    # A task into a keyed table made in SQL (it has a hidden `_deleted` column).
    q("CREATE TABLE src (id BIGINT, v VARCHAR)")
    q("CREATE TABLE latest (id BIGINT PRIMARY KEY, v VARCHAR)")
    call(port, "POST", "/tasks/latest_of", json.dumps({"source": "src", "target": "latest", "sql": "SELECT id, v FROM src"}).encode())
    q("INSERT INTO src VALUES (1, 'a'), (2, 'b'), (1, 'c')")
    want = [{"id": 1, "v": "c"}, {"id": 2, "v": "b"}]
    checks["a task into a keyed table made in SQL"] = until(lambda: q("SELECT id, v FROM latest ORDER BY id"), want, 15) == want
    # INSERT … ON CONFLICT (Postgres), UPDATE … FROM, DELETE … USING, TRUNCATE.
    q("CREATE TABLE users (id BIGINT PRIMARY KEY, name VARCHAR, visits BIGINT)")
    q("INSERT INTO users VALUES (1, 'ann', 1), (2, 'bob', 1)")
    q("INSERT INTO users VALUES (1, 'ANN', 1), (3, 'cy', 1) ON CONFLICT (id) DO NOTHING")
    q("INSERT INTO users VALUES (2, 'bob', 1), (4, 'dee', 1) ON CONFLICT (id) DO UPDATE SET visits = users.visits + excluded.visits")
    upserted = q("SELECT id, name, visits FROM users ORDER BY id")
    q("CREATE TABLE stock (id BIGINT, qty BIGINT)")
    q("CREATE TABLE counted (id BIGINT, qty BIGINT)")
    q("INSERT INTO stock VALUES (1, 5), (2, 6), (3, 7)")
    q("INSERT INTO counted VALUES (1, 50), (2, 60), (2, 61)")
    q("UPDATE stock SET qty = c.qty FROM counted c WHERE stock.id = c.id AND c.qty < 60")
    q("DELETE FROM stock USING counted c WHERE stock.id = c.id AND c.qty >= 60")  # (id 2 matches twice: deleted once)
    moved = q("SELECT id, qty FROM stock ORDER BY id")
    q("TRUNCATE counted")
    checks["INSERT … ON CONFLICT DO NOTHING / DO UPDATE, UPDATE … FROM, DELETE … USING, TRUNCATE"] = \
        upserted == [{"id": 1, "name": "ann", "visits": 1}, {"id": 2, "name": "bob", "visits": 2}, {"id": 3, "name": "cy", "visits": 1}, {"id": 4, "name": "dee", "visits": 1}] \
        and moved == [{"id": 1, "qty": 50}, {"id": 3, "qty": 7}] and q("SELECT count(*) AS n FROM counted") == [{"n": 0}] \
        and "ON DUPLICATE KEY" in fails("INSERT INTO users VALUES (1, 'x', 1) ON DUPLICATE KEY UPDATE visits = 2")
    # The types the README names.
    q("CREATE TABLE blobs (b BINARY, v VARBINARY, l BLOB, j VARIANT, js JSON)")
    q("""INSERT INTO blobs VALUES (X'010203', X'04', X'05', '{"a": 1}', '[1, 2]')""")
    seen["bytes"] = q("SELECT byte_length(b) AS n, encode(substr(b, 2), 'hex') AS tail, encode(substr(b, 1, 1), 'hex') AS head, "
                      "encode(substring(b FROM 2 FOR 1), 'hex') AS mid, json_get_int(j, 'a') AS a, substr('hello', 2, 3) AS text FROM blobs")
    seen["named"] = q("SELECT substr(str => 'hello', start_pos => 2, length => 3) AS s")
    checks["BINARY, VARBINARY, BLOB, VARIANT and JSON columns; substr on bytes and on text (its arguments named too)"] = \
        seen["bytes"] == [{"n": 3, "tail": "0203", "head": "01", "mid": "02", "a": 1, "text": "ell"}] and seen["named"] == [{"s": "ell"}]
    # FROM first, with clauses after; a comment before CREATE FUNCTION.
    q("-- doubles a number\n/* (for the docs) */ CREATE FUNCTION twice(x BIGINT) RETURNS BIGINT AS 'x * 2'")
    checks["FROM-first with WHERE and ORDER BY; a comment before CREATE FUNCTION"] = \
        q("FROM orders WHERE region = 'eu' ORDER BY id") == [{"id": 1, "region": "eu", "amount": 99}, {"id": 4, "region": "eu", "amount": 40}] \
        and q("SELECT twice(21) AS n") == [{"n": 42}]
    # COPY … TO STDOUT with a header counts the rows, not the header.
    with psycopg.connect(f"host=127.0.0.1 port={pgport} dbname=pondra user=u", autocommit=True) as c:
        cur = c.cursor()
        with cur.copy("COPY (SELECT id FROM orders ORDER BY id) TO STDOUT WITH (FORMAT csv, HEADER)") as cp:
            text = b"".join(bytes(b) for b in cp).decode()
        checks["COPY … TO STDOUT with HEADER: the header, then the rows, counted"] = text.split() == ["id", "1", "2", "4"] and cur.rowcount == 3
    # TIMESTAMPTZ: an instant, kept and shown in UTC with its zone; TIMESTAMP: a wall-clock time.
    q("CREATE TABLE times (at TIMESTAMPTZ, local TIMESTAMP)")
    q("INSERT INTO times VALUES ('2026-09-29 10:00:00+05', '2026-09-29 10:00:00')")
    with psycopg.connect(f"host=127.0.0.1 port={pgport} dbname=pondra user=u", autocommit=True) as c:
        seen["timestamptz"] = [c.execute("SELECT at, local FROM times").fetchone(), c.cursor(binary=True).execute("SELECT at FROM times").fetchone()]
    utc = dt.datetime(2026, 9, 29, 5, tzinfo=dt.timezone.utc)
    checks["TIMESTAMPTZ is an instant, shown in UTC with its zone (JSON, Postgres text and binary); TIMESTAMP has none"] = \
        q("SELECT at, local FROM times") == [{"at": "2026-09-29T05:00:00Z", "local": "2026-09-29T10:00:00"}] \
        and seen["timestamptz"][0] == (utc, dt.datetime(2026, 9, 29, 10)) and seen["timestamptz"][1] == (utc,)
    # Tokens: what a write token may not do, said as it is.
    as_ = lambda t, s: call(port + 1, "POST", "/sql", s.encode(), headers={"authorization": f"Bearer {t}"})
    as_("a", "CREATE TABLE kept (a BIGINT)")
    seen["drop"] = _raises_text(lambda: as_("w", "DROP TABLE kept"))
    checks["a write token's DROP TABLE is refused as a change to the lake's tables"] = "may not change the lake's tables" in seen["drop"]
    # Files: a second PUT to a path says so without the node's own paths; `files/` or not.
    put_file = lambda p, b: call(port, "PUT", f"/files/{p}", b)
    put_file("photos/a.txt", b"hello")
    seen["again"] = _raises_text(lambda: put_file("photos/a.txt", b"again"))
    listed = lambda p: [r["path"] for r in q(f"SELECT path FROM files('{p}')")]
    checks["PUT /files twice: refused by name, no server path; files() and file_read() with or without files/"] = \
        "photos/a.txt is there already" in seen["again"] and lake not in seen["again"] and "/tmp" not in seen["again"] \
        and listed("photos/") == listed("files/photos/") == ["files/photos/a.txt"] \
        and q("SELECT byte_length(file_read('photos/a.txt')) AS a, byte_length(file_read('files/photos/a.txt')) AS b") == [{"a": 5, "b": 5}]
    # An async call in another's arguments (Python functions, file_read): in SELECT, WHERE, GROUP
    # BY and INSERT … VALUES. A node told `--python auto` that finds no Python with pondra says so.
    q("CREATE FUNCTION up(s VARCHAR) RETURNS VARCHAR LANGUAGE python AS $$\n    return s.upper()\n$$")
    q("CREATE FUNCTION rev(s VARCHAR) RETURNS VARCHAR LANGUAGE python AS $$\n    return s[::-1]\n$$")
    q("CREATE TABLE words (s VARCHAR)")
    q("INSERT INTO words VALUES ('ab'), ('cd')")
    q("CREATE TABLE kept_files (s VARCHAR, b BYTEA)")
    q("INSERT INTO kept_files VALUES (up('x'), file_read('photos/a.txt')), ('y', NULL)")
    seen["nested"] = [q("SELECT rev(up(s)) AS a FROM words ORDER BY a"), q("SELECT rev(up(rev(s))) AS a FROM words ORDER BY a"), q("SELECT s FROM words WHERE rev(up(s)) = 'BA'"),
                      q("SELECT rev(up(s)) AS a, count(*) AS n FROM words GROUP BY rev(up(s)) ORDER BY a"), q("SELECT up(CAST(file_read('photos/a.txt') AS VARCHAR)) AS a"),
                      q("SELECT s, byte_length(b) AS n FROM kept_files ORDER BY s")]
    as_("a", "CREATE FUNCTION up(s VARCHAR) RETURNS VARCHAR LANGUAGE python AS $$\n    return s.upper()\n$$")
    seen["no python"] = _raises_text(lambda: as_("r", "SELECT up('a') AS a"))
    checks["async calls inside async calls (Python functions, file_read) in SELECT, WHERE, GROUP BY, INSERT … VALUES; no Python found, said so"] = \
        seen["nested"] == [[{"a": "BA"}, {"a": "DC"}], [{"a": "AB"}, {"a": "CD"}], [{"s": "ab"}], [{"a": "BA", "n": 1}, {"a": "DC", "n": 1}], [{"a": "HELLO"}], [{"s": "X", "n": 5}, {"s": "y"}]] \
        and "no Python with the pondra package" in seen["no python"]
    # A function served over Flight: callable as soon as it's made, gone as soon as it's dropped.
    server = subprocess.Popen([sys.executable, os.path.join(os.path.dirname(os.path.abspath(__file__)), "udf_server.py"), "--port", str(fport + 5)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        until(lambda: _raises(lambda: fl.FlightClient(f"grpc://127.0.0.1:{fport + 5}").list_actions()), False, 20)
        q("SELECT 1")  # (the node has its functions cached now)
        call(port, "POST", "/functions/shout", json.dumps({"flight": f"http://127.0.0.1:{fport + 5}", "args": ["Utf8"], "returns": "Utf8"}).encode())
        made = _try(lambda: q("SELECT shout('hi') AS s"))
        call(port, "DELETE", "/functions/shout")
        gone = fails("SELECT shout('hi') AS s")
    finally:
        server.kill()
    checks["a Flight function is callable as soon as it is made, and gone once dropped"] = made == [{"s": "HI"}] and "shout" in gone
    # ADBC: a handshake without base64 padding ("reader:r" is 8 bytes), ingest modes, table types.
    shaken = adbc.connect(f"grpc://127.0.0.1:{fport + 1}", db_kwargs={"username": "reader", "password": "r"})
    cur = shaken.cursor(); cur.execute("SELECT count(*) AS n FROM kept"); seen["handshake"] = cur.fetchone(); cur.close(); shaken.close()
    conn = adbc.connect(f"grpc://127.0.0.1:{fport}")
    table = lambda lo, hi: pa.table({"k": pa.array(range(lo, hi), pa.int64())})
    def ingest(mode, lo, hi, name="ing"):
        c = conn.cursor()
        try:
            return c.adbc_ingest(name, table(lo, hi), mode=mode)
        finally:
            c.close()
    ingest("create", 0, 3)
    modes = {"create again": _raises_text(lambda: ingest("create", 0, 3)), "append to none": _raises_text(lambda: ingest("append", 0, 3, "none"))}
    ingest("append", 3, 5)
    ingest("create_append", 5, 6)
    ingest("create_append", 0, 2, "fresh")
    appended = q("SELECT count(*) AS n, sum(k) AS s FROM ing")
    ingest("replace", 100, 102)
    replaced = q("SELECT count(*) AS n, sum(k) AS s FROM ing")
    types = conn.adbc_get_table_types()
    conn.close()
    checks["ADBC: username/password (unpadded base64), ingest create/append/create_append/replace, table types"] = seen["handshake"] == (0,) \
        and all(modes.values()) and appended == [{"n": 6, "s": 15}] and replaced == [{"n": 2, "s": 201}] \
        and q("SELECT count(*) AS n FROM fresh") == [{"n": 2}] and sorted(types) == ["TABLE", "VIEW"]
    # NOT NULL and DEFAULT, on every door: a default for each row a write leaves the column out of.
    import confluent_kafka as ck
    q("CREATE TABLE acct (id BIGINT PRIMARY KEY, name VARCHAR NOT NULL, status VARCHAR DEFAULT 'new', made TIMESTAMP DEFAULT now(), tag VARCHAR DEFAULT uuid(), n BIGINT DEFAULT 1 + 1)")
    q("INSERT INTO acct (id, name) VALUES (1, 'ann')")
    q("INSERT INTO acct VALUES (2, 'bob', DEFAULT, DEFAULT, DEFAULT, 7)")
    q("INSERT INTO acct (id, name, status) SELECT 3, 'cy', 'old'")
    call(port, "POST", "/append/acct", b'{"id": 4, "name": "dee"}\n{"id": 5, "name": "eve", "status": null}\n')  # (no producer: at least once)
    with psycopg.connect(f"host=127.0.0.1 port={pgport} dbname=pondra user=u", autocommit=True) as c:
        with c.cursor().copy("COPY acct (id, name) FROM STDIN") as cp:
            cp.write(b"6\tfay\n")
    put(["acct"], pa.table({"id": pa.array([7], pa.int64()), "name": ["gus"]}))
    kp = ck.Producer({"bootstrap.servers": f"127.0.0.1:{port + 20}"})
    kp.produce("acct", key=b"8", value=json.dumps({"id": 8, "name": "hal"}))
    assert kp.flush(20) == 0
    kafka_refused = []
    kp.produce("acct", key=b"9", value=json.dumps({"id": 9}), on_delivery=lambda e, m: kafka_refused.append(e))
    kp.flush(20)
    null_refused = {
        "INSERT": fails("INSERT INTO acct (id) VALUES (9)"), "a NULL key": fails("INSERT INTO acct (id, name) VALUES (NULL, 'x')"),
        "UPDATE": fails("UPDATE acct SET name = NULL WHERE id = 1"), "append": _raises_text(lambda: call(port, "POST", "/append/acct", b'{"id": 9}\n')),
        "Flight": _raises_text(lambda: put(["acct"], pa.table({"id": pa.array([9], pa.int64())}))),
        "Kafka": "acct.name is NOT NULL" if kafka_refused and kafka_refused[0] is not None and "acct.name is NOT NULL" in open(node.log).read() else "",
    }
    seen["defaults"] = q("SELECT id, name, status, made IS NOT NULL AS made, length(tag) AS tag, n FROM acct ORDER BY id")
    row = lambda i, name, status="new", n=2: {"id": i, "name": name, **({"status": status} if status else {}), "made": True, "tag": 36, "n": n}
    q("CREATE TABLE big (id BIGINT, v VARCHAR DEFAULT 'd', w BIGINT NOT NULL)")
    q("INSERT INTO big SELECT value, 'x', value FROM range(0, 5)")  # (the same column twice: by position)
    q("INSERT INTO big (id, w) SELECT value, value FROM range(10, 12)")
    null_refused["bulk INSERT"] = fails("INSERT INTO big SELECT value FROM range(0, 5)")
    checks["DEFAULT for a column a write leaves out (INSERT, DEFAULT, append, COPY, Flight, Kafka, bulk INSERT), each row its own"] = \
        seen["defaults"] == [row(1, "ann"), row(2, "bob", n=7), row(3, "cy", "old"), row(4, "dee"), row(5, "eve", None), row(6, "fay"), row(7, "gus"), row(8, "hal")] \
        and q("SELECT count(DISTINCT tag) AS n FROM acct") == [{"n": 8}] and q("SELECT v, count(*) AS n FROM big GROUP BY v ORDER BY v") == [{"v": "d", "n": 2}, {"v": "x", "n": 5}]
    checks["NOT NULL refused by name on every door (a key's columns too); a bad DEFAULT refused at CREATE; ADD COLUMN … DEFAULT refused"] = \
        all("is NOT NULL" in v for v in null_refused.values()) and "acct.id is NOT NULL" in null_refused["a NULL key"] and q("SELECT count(*) AS n FROM acct") == [{"n": 8}] \
        and "Cannot cast" in fails("CREATE TABLE bad (a BIGINT DEFAULT 'abc')") and "nope" in fails("CREATE TABLE bad (a BIGINT DEFAULT nope())") \
        and "rows already there" in fails("ALTER TABLE acct ADD COLUMN z BIGINT DEFAULT 3")
    # Read your writes on a follower: each statement sees the one before it (a script's INSERT, then
    # its SELECT; an UPDATE; a table made, then filled), as on the leader.
    follower = Node(lake, port + 2).start()
    fq = lambda s: sql(port + 2, s)
    fq("CREATE TABLE ryw (i BIGINT)")
    counts, scripts = [], []
    for i in range(1, 41):
        fq(f"INSERT INTO ryw VALUES ({i})")
        counts.append(fq("SELECT count(*) AS n FROM ryw")[0]["n"] == 2 * i - 1)
        out = call(port + 2, "POST", "/sql", f"INSERT INTO ryw VALUES ({-i}); SELECT count(*) AS n FROM ryw".encode())
        scripts.append(json.dumps(out).count(f'"n": {2 * i}') == 1 or json.dumps(out).count(f'"n":{2 * i}') == 1)
    fq("UPDATE ryw SET i = 0 WHERE i < 0")
    changed = fq("SELECT count(*) AS n FROM ryw WHERE i = 0")
    follower.kill()
    seen["ryw"] = [sum(counts), sum(scripts), changed]
    checks["read your writes on a follower: an INSERT then a SELECT, one request or two, and an UPDATE (40 times)"] = all(counts) and all(scripts) and changed == [{"n": 40}]
    # `pondra run FILE`: the lake or --url, then parameters, in any order.
    work = tempfile.mkdtemp(prefix="pondra-run-")
    with open(os.path.join(work, "day.sql"), "w") as f:
        f.write("SELECT $day AS d;\n")
    run = lambda *a: subprocess.run([BIN, "run", os.path.join(work, "day.sql"), *a], capture_output=True, text=True, timeout=120)
    by_url, url_last, both = run("--day", "x1", "--url", f"http://127.0.0.1:{port}"), run("--url", f"http://127.0.0.1:{port}", "--day", "x2"), run(os.path.join(work, "l"), "--day", "x3", "--url", f"http://127.0.0.1:{port}")
    checks["pondra run: --url and parameters in any order; a lake and --url together refused"] = "x1" in by_url.stdout and "x2" in url_last.stdout \
        and both.returncode != 0 and "not both" in both.stderr
    # `pondra sql`: a folder with no lake said so (no backtrace, even with RUST_BACKTRACE set); a
    # write makes the lake; files and read_*() in CREATE TABLE … AS and INSERT, as a node takes them.
    with open(os.path.join(work, "o.ndjson"), "w") as f:
        f.write('{"a": 1, "b": "x"}\n{"a": 2, "b": "y"}\n')
    with open(os.path.join(work, "o.csv"), "w") as f:
        f.write("a,b\n3,z\n")
    cli = lambda d, s: subprocess.run([BIN, "sql", "--dir", os.path.join(work, d), s], capture_output=True, text=True, timeout=120, cwd=work, env={**os.environ, "RUST_BACKTRACE": "1"})
    empty = cli("empty", "SELECT 1")
    made = cli("fresh", "CREATE TABLE t AS SELECT * FROM 'o.ndjson'")
    more = cli("fresh", "INSERT INTO t SELECT * FROM read_csv('o.csv')")
    wrong = cli("fresh", "SELECT nope FROM t")
    seen["cli"] = [empty.stderr[-300:], made.stdout + made.stderr[-300:], more.stdout + more.stderr[-300:], wrong.stderr[-300:]]
    checks["pondra sql: no lake there said so; a write makes one; files in CTAS and INSERT; errors without backtraces"] = \
        empty.returncode == 1 and "holds no lake yet" in empty.stderr and made.returncode == 0 and more.returncode == 0 \
        and "| 3 | 6 |" in cli("fresh", "SELECT count(*) AS n, sum(a) AS s FROM t").stdout \
        and wrong.returncode == 1 and "nope" in wrong.stderr and not any("backtrace" in e.lower() for e in (empty.stderr, wrong.stderr))
    # `pondra sql` refuses a row without a NOT NULL column, as a node does (it writes to the log
    # itself), and fills a DEFAULT.
    cli("fresh", "CREATE TABLE needs (id BIGINT, email VARCHAR NOT NULL, plan VARCHAR DEFAULT 'free')")
    missing = cli("fresh", "INSERT INTO needs (id) VALUES (1)")
    given = cli("fresh", "INSERT INTO needs (id, email) VALUES (2, 'a@b')")
    checks["pondra sql: NOT NULL refused, DEFAULT filled (its own writes to the log)"] = missing.returncode == 1 and "needs.email is NOT NULL" in missing.stderr \
        and given.returncode == 0 and "| 2  | a@b   | free |" in cli("fresh", "SELECT id, email, plan FROM needs").stdout
    seen["cli_not_null"] = [missing.stdout + missing.stderr[-300:], given.stdout + given.stderr[-300:]]
    shutil.rmtree(work, ignore_errors=True)
    # What the console found (round 26): a time without seconds is a timestamp, as in Postgres
    # (INSERT, CAST, TIMESTAMP '…', a comparison); a table is a BASE TABLE to information_schema;
    # the console's answers keep decimals and big integers exact; a files() listing is never an
    # answer remembered from before the last PUT /files.
    q("CREATE TABLE meets (id BIGINT, at TIMESTAMP, tz TIMESTAMPTZ)")
    q("INSERT INTO meets VALUES (1, '2024-05-01 10:30', '2024-05-01T10:30+02'), (2, '2024-05-01 11:00:15', NULL)")
    checks["a time without seconds is a timestamp: INSERT, CAST, TIMESTAMP '…', a comparison"] = \
        q("SELECT id, at, tz FROM meets ORDER BY id") == [{"id": 1, "at": "2024-05-01T10:30:00", "tz": "2024-05-01T08:30:00Z"}, {"id": 2, "at": "2024-05-01T11:00:15"}] \
        and q("SELECT CAST('2024-05-01 10:30' AS TIMESTAMP) AS a, TIMESTAMP '2024-05-01 10:30' AS b") == [{"a": "2024-05-01T10:30:00", "b": "2024-05-01T10:30:00"}] \
        and q("SELECT count(*) AS n FROM meets WHERE at >= '2024-05-01 10:31'") == [{"n": 1}]
    q("CREATE VIEW meets_v AS SELECT id FROM meets")
    kinds = {r["table_name"]: r["table_type"] for r in q("SELECT table_name, table_type FROM information_schema.tables WHERE table_name LIKE 'meets%'")}
    checks["information_schema: a table is a BASE TABLE, a view a VIEW"] = kinds == {"meets": "BASE TABLE", "meets_v": "VIEW"}
    typed = call(port, "POST", "/sql?format=typed", b"SELECT CAST(1.5 AS DECIMAL(10,2)) AS d, 9007199254740993 AS big, 7 AS small")
    checks["format=typed: decimals and integers past 2^53 exact (as text), others as numbers"] = typed["rows"] == [["1.50", "9007199254740993", 7]] \
        and [c["type"] for c in typed["columns"]] == ["Decimal128(10, 2)", "Int64", "Int64"] and typed["total"] == 1
    # An answer of more rows than the console is sent at once: its other pages, kept on the node (pages.rs).
    big = call(port, "POST", "/sql?format=typed", b"SELECT value AS n FROM range(0, 25000)")
    page3 = call(port, "GET", f"/sql/pages/{big.get('pages')}?from=20000&rows=10000") if big.get("pages") else {}
    try:
        call(port, "GET", "/sql/pages/0123456789abcdef0123456789abcdef?from=0")
        gone = None
    except RuntimeError as e:
        gone = str(e)[:3]
    whole = call(port, "GET", f"/sql/pages/{big.get('pages')}?format=csv") if big.get("pages") else b""
    small = call(port, "GET", f"/sql/pages/{typed.get('pages')}?format=csv") if typed.get("pages") else b""
    checks["format=typed: 10,000 rows of a bigger answer, and an id its other pages, and every row as a file, are read with (the same rows, not run again); 410 once not kept"] = \
        (len(big["rows"]), big["total"], big["rows"][-1]) == (10000, 25000, [9999]) and len(page3.get("rows", [])) == 5000 and page3["rows"][0] == [20000] \
        and gone == "410" and whole.decode().split("\n")[:2] == ["n", "0"] and len(whole.decode().split()) == 25001 and small.decode().split() == ["d,big,small", "1.50,9007199254740993,7"]
    # to_timestamp over a column of text answers in its type's zone (UTC), as over a literal.
    zoned = q("SELECT to_timestamp(v) AS a, to_timestamp(d, '%Y-%m-%d') AS b, to_timestamp_millis(d, '%Y-%m-%d') AS c FROM (VALUES ('2020-09-09T00:00:00+02:00', '2020-09-08')) AS x(v, d)")
    checks["to_timestamp(column) and to_timestamp_millis(column, format): TIMESTAMP (no zone; UTC's time for text with one), as over a literal"] = zoned == [{"a": "2020-09-08T22:00:00", "b": "2020-09-08T00:00:00", "c": "2020-09-08T00:00:00"}]
    kinds = q("SELECT arrow_typeof(to_timestamp('2020-09-08 13:42:29')) AS a, arrow_typeof(to_timestamp_millis(d)) AS b, arrow_typeof(now()) AS c FROM (VALUES ('2020-09-08')) AS x(d)")
    checks["to_timestamp answers TIMESTAMP (DataFusion's, Spark's), now() TIMESTAMPTZ in UTC"] = kinds == [{"a": "Timestamp(ns)", "b": "Timestamp(ms)", "c": 'Timestamp(ns, "+00:00")'}] or kinds == [{"a": "Timestamp(Nanosecond, None)", "b": "Timestamp(Millisecond, None)", "c": 'Timestamp(Nanosecond, Some("+00:00"))'}]
    listing = lambda: [r["path"] for r in q("SELECT path FROM files('listed/')")]
    first = listing()
    call(port, "PUT", "/files/listed/a.txt", b"a")
    checks["files() after PUT /files lists the new file (never a remembered answer)"] = first == [] and listing() == ["files/listed/a.txt"]
    # A few rows of a table's go with their own strings only, not the buffers of the batch they were
    # cut from (round 32: a 10-row Arrow answer of ClickBench's was 220 MB), over HTTP and Flight.
    q("CREATE TABLE wide AS SELECT value AS i, repeat('x', 200) || value AS s FROM range(0, 50000)")
    few = "SELECT i, s FROM wide LIMIT 5"  # (a slice of a batch: 3.2 MB as Arrow before)
    sent = call(port, "POST", "/sql?format=arrow", few.encode())
    flown = client.do_get(fl.Ticket(json.dumps({"sql": few}))).read_all()
    seen["few"] = (len(sent), flown.nbytes, flown.num_rows)
    checks["a few rows of a table's, sent as Arrow (HTTP, Flight), carry only their own strings"] = len(sent) < 16 << 10 and flown.nbytes < 16 << 10 and flown.num_rows == 5
    node.kill(); locked.kill()
    ok = all(checks.values())
    print(json.dumps({"found": checks, "ok": ok}, indent=1))
    if not ok:
        print(json.dumps({k: (v if not isinstance(v, str) else v[:400]) for k, v in seen.items()}, default=str, indent=1), {k: v[:200] for k, v in null_refused.items()}, zero[:300], flight_zero[:300], upserted, moved, modes, appended, replaced, types, by_url.stdout + by_url.stderr, url_last.stdout, both.stderr[:300])
        sys.exit(1)
    return f"found: what writing the docs found, fixed: all {len(checks)} checks pass"


def workspace():
    """The workspace (ADR-033): the lake's files run, `CALL run('etl/orders.sql', day => …)`. A
    `.sql` file's `$name`s bound; a `.py` file's parameters as variables, its prints as notices,
    its last expression the answer; a notebook's `parameters` cell's values replaced, `%%sql` and
    Python cells in one run; a saved notebook by name (its newest version); files running files;
    16 deep at most; from HTTP, Postgres, MCP, Python (`db.run`, `pondra.run` in a file) and
    JavaScript; started without waiting (`pondra.start('run', …)`); on a schedule (a task); retried
    with its job, written once; each run in `pondra.runs` as `files/<path>@<version>` with its
    arguments; a writer runs SQL files but not Python; mistakes said by name."""
    import datetime, psycopg
    lake = new_lake()
    here = os.path.dirname(os.path.abspath(__file__))
    env = {"PYTHONPATH": os.path.join(here, "..", "python")}
    node = Node(lake, A.port, env=env, python=sys.executable, pg=f"127.0.0.1:{A.port + 10}", read_token="r-tok", write_token="w-tok", admin_token="a-tok").start()
    def q(s, token="a-tok", path="/sql", headers=None):
        return call(A.port, "POST", path, s.encode() if isinstance(s, str) else s, headers={"authorization": f"Bearer {token}", **(headers or {})}, timeout=120)
    def err(s, **kw):
        try:
            q(s, **kw)
            return ""
        except Exception as e:
            return str(e)
    def put(path, text):
        call(A.port, "PUT", f"/files/{path}", text.encode() if isinstance(text, str) else text, headers={"authorization": "Bearer a-tok"})
    checks = {}
    q("CREATE TABLE orders (day DATE, region VARCHAR, amount DOUBLE)")
    put("etl/orders.sql", "INSERT INTO orders VALUES ($day, $region, $amount);  -- a ';' in a comment\n"
                          "SELECT region, count(*) AS n, sum(amount) AS total FROM orders WHERE day = $day AND region = $region GROUP BY region")
    got = q("CALL run('etl/orders.sql', day => DATE '2026-09-29', region => 'west', amount => 10.5)")
    checks["a SQL file runs, its $names bound; its answer is its last statement's"] = got == [{"region": "west", "n": 1, "total": 10.5}]
    put("etl/score.py", 'print("scoring", region)\nn = db.sql(f"SELECT count(*) AS n FROM orders WHERE region = \'{region}\'").item()\n{"region": region, "n": n, "model": model, "day": str(day)}')
    r = urllib.request.urlopen(urllib.request.Request(f"http://127.0.0.1:{A.port}/sql", data=b"CALL run('files/etl/score.py', region => 'west', model => 'v3', day => DATE '2026-09-29')",
                                                      headers={"authorization": "Bearer a-tok"}))
    checks["a Python file runs with its parameters as variables, its print a notice, its last expression the answer"] = json.loads(r.read()) == [{"region": "west", "n": 1, "model": "v3", "day": "2026-09-29"}] \
        and json.loads(r.headers.get("x-pondra-notices") or "[]") == ["scoring west"]
    nb = lambda default: json.dumps({"nbformat": 4, "nbformat_minor": 5, "metadata": {}, "cells": [
        {"cell_type": "markdown", "metadata": {}, "source": ["# Weekly"]},
        {"cell_type": "code", "metadata": {"tags": ["parameters"]}, "source": [f'region = "{default}"\n', "top = 1"], "outputs": [], "execution_count": None},
        {"cell_type": "code", "metadata": {}, "source": ["%%sql\n", "SELECT count(*) AS n FROM orders WHERE region = $region"], "outputs": [], "execution_count": None},
        {"cell_type": "code", "metadata": {}, "source": ["%load_ext pondra\n", "print('region is', region)\n", "{'r': region.upper(), 'top': top}"], "outputs": [], "execution_count": None}]})
    put("reports/weekly.ipynb", nb("north"))
    checks["a notebook runs: the given values replace its parameters cell's, then %%sql and Python cells in turn"] = q("CALL run('reports/weekly.ipynb', region => 'west')") == [{"r": "WEST", "top": 1}]
    put("notebooks/weekly/2026-09-29T10-00-00-000Z.ipynb", nb("old"))
    put("notebooks/weekly/2026-09-30T10-00-00-000Z.ipynb", nb("new"))
    checks["a saved notebook by name runs its newest version"] = q("CALL run('notebooks/weekly', region => 'west')") == [{"r": "WEST", "top": 1}] \
        and until(lambda: [x["routine"].split("@")[0] for x in q("SELECT routine FROM pondra.runs WHERE routine LIKE 'files/notebooks/%'")], ["files/notebooks/weekly/2026-09-30T10-00-00-000Z.ipynb"], 10) \
        == ["files/notebooks/weekly/2026-09-30T10-00-00-000Z.ipynb"]
    put("etl/all.sql", "CALL run('etl/orders.sql', day => $day, region => 'east', amount => 1.0);\nCALL run('etl/score.py', region => 'east', model => $model, day => $day)")
    put("etl/chain.py", "import datetime\npondra.run('etl/orders.sql', day=datetime.date(2026, 9, 29), region='chain', amount=3.0)")
    checks["files run files: SQL runs Python, Python runs SQL (pondra.run)"] = q("CALL run('etl/all.sql', day => DATE '2026-09-29', model => 'v4')") == [{"region": "east", "n": 1, "model": "v4", "day": "2026-09-29"}] \
        and q("CALL run('etl/chain.py')") == [{"region": "chain", "n": 1, "total": 3.0}]
    put("etl/loop.sql", "CALL run('etl/loop.sql')")
    checks["files running themselves stop 16 deep"] = "16 deep" in err("CALL run('etl/loop.sql')")
    for _ in range(2):
        q("CALL run('etl/orders.sql', day => DATE '2026-09-28', region => 'job', amount => 2.0)", path="/sql?job=w-1")
    checks["a run retried with its job writes once"] = q("SELECT count(*) AS n FROM orders WHERE region = 'job'") == [{"n": 1}]
    with psycopg.connect(f"host=127.0.0.1 port={A.port + 10} user=admin password=a-tok dbname=lake", autocommit=True) as c:
        pg = c.execute("CALL run('etl/orders.sql', day => DATE '2026-09-29', region => 'pg', amount => 4.0)").fetchall()
    mcp = call(A.port, "POST", "/mcp", json.dumps({"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": "write", "arguments": {"sql": "CALL run('etl/orders.sql', day => DATE '2026-09-29', region => 'mcp', amount => 5.0)"}}}).encode(),
               headers={"authorization": "Bearer w-tok", "content-type": "application/json"})
    sys.path.insert(0, os.path.join(here, "..", "python"))
    import pondra
    db = pondra.connect(f"http://127.0.0.1:{A.port}", token="a-tok", echo=False)
    py = db.run("etl/orders.sql", day=datetime.date(2026, 9, 29), region="py", amount=6.0).rows()
    js = os.path.join(tempfile.mkdtemp(prefix="pondra-js-"), "run.mjs")
    open(js, "w").write(f"""import {{ connect }} from {json.dumps(os.path.join(here, "..", "js", "index.js"))};
const db = connect("http://127.0.0.1:{A.port}", {{ token: "a-tok", onNotice: null }});
console.log(JSON.stringify(await db.run("etl/orders.sql", {{ day: "2026-09-29", region: "js", amount: 7 }})));""")
    ran = subprocess.run(["node", js], capture_output=True, text=True, timeout=120)
    shutil.rmtree(os.path.dirname(js), ignore_errors=True)
    checks["every door runs a file: Postgres, MCP, Python's db.run, JavaScript's db.run"] = pg == [("pg", 1, 4.0)] and not mcp["result"]["isError"] and '\\"mcp\\"' in json.dumps(mcp) \
        and py == [{"region": "py", "n": 1, "total": 6.0}] and ran.returncode == 0 and json.loads(ran.stdout or "null") == [{"region": "js", "n": 1, "total": 7}]
    started = q("SELECT pondra.start('run', 'etl/orders.sql', day => DATE '2026-09-29', region => 'later', amount => 8.0) AS id")[0]["id"]
    done = until(lambda: q(f"SELECT status FROM pondra.runs WHERE id = '{started}'"), [{"status": "ok"}], 30)
    checks["pondra.start('run', …): started, not waited for; its row says how it went"] = done == [{"status": "ok"}] and q("SELECT count(*) AS n FROM orders WHERE region = 'later'") == [{"n": 1}]
    q("CREATE TASK nightly SCHEDULE '2 seconds' AS CALL run('etl/orders.sql', day => DATE '2026-09-29', region => 'task', amount => 1.0)")
    ticked = until(lambda: q("SELECT count(*) > 0 AS ran FROM orders WHERE region = 'task'"), [{"ran": True}], 30)
    q("DROP TASK nightly")
    by_task = until(lambda: q("SELECT caller FROM pondra.runs WHERE routine LIKE 'files/etl/orders.sql@%' AND caller LIKE 'task:%' LIMIT 1"), [{"caller": "task:nightly"}], 15)
    checks["a task runs a file on its schedule, the run log naming the task"] = ticked == [{"ran": True}] and by_task == [{"caller": "task:nightly"}]
    version = urllib.request.urlopen(urllib.request.Request(f"http://127.0.0.1:{A.port}/files/etl/orders.sql", headers={"authorization": "Bearer a-tok"})).headers["etag"].strip('"')
    logged = until(lambda: q(f"SELECT routine, args, status FROM pondra.runs WHERE routine = 'files/etl/orders.sql@{version}' AND args LIKE '%\"region\":\"west\"%'"), None, 5)
    failed = err("CALL run('etl/orders.sql', day => DATE '2026-09-29')")
    fail_row = until(lambda: q("SELECT status, error FROM pondra.runs WHERE status = 'failed' AND error LIKE '%no value for $region%'")[:1], None, 5)
    checks["each run is a row of pondra.runs: files/<path>@<version>, its arguments, how it went"] = logged and logged[0]["status"] == "ok" and '"day":"2026-09-29"' in logged[0]["args"] \
        and "no value for $region" in failed and bool(fail_row) and fail_row[0]["status"] == "failed"
    checks["a writer runs SQL files, not Python ones (they need an admin, as DO does)"] = q("CALL run('etl/orders.sql', day => DATE '2026-09-29', region => 'w', amount => 1.0)", token="w-tok") == [{"region": "w", "n": 1, "total": 1.0}] \
        and "admin token" in err("CALL run('etl/score.py', region => 'w', model => 'x', day => DATE '2026-09-29')", token="w-tok") \
        and "may not write" in err("CALL run('etl/orders.sql', day => DATE '2026-09-29', region => 'r', amount => 1.0)", token="r-tok")
    said = {"missing": err("CALL run('etl/nothing.sql')"), "kind": err("CALL run('data/x.csv')"), "unnamed": err("CALL run('etl/orders.sql', 1)"),
            "twice": err("CALL run('etl/orders.sql', a => 1, a => 2)"), "none": err("CALL run('notebooks/nothing')"), "own": err("CREATE PROCEDURE run() AS $$ SELECT 1 $$")}
    checks["mistakes said by name: no such file, not a file that runs, a value not named, a name twice, no saved notebook, run is Pondra's"] = \
        "no file files/etl/nothing.sql" in said["missing"] and ".sql, .py or .ipynb" in said["kind"] and "by name" in said["unnamed"] and "given twice" in said["twice"] \
        and "none saved" in said["none"] and "Pondra's own" in said["own"]
    imm = {"plain": q("EXECUTE IMMEDIATE FROM 'etl/orders.sql' USING (day => DATE '2026-09-29', region => 'imm', amount => 2.0)"),
           "into": q("BEGIN EXECUTE IMMEDIATE FROM 'etl/orders.sql' USING (day => DATE '2026-09-29', region => 'imm', amount => 1.0) INTO $r, $n; SELECT $r AS r, $n AS n; END"),
           "bad": err("EXECUTE IMMEDIATE FROM 'etl/orders.sql' USING day => 1")}
    checks["EXECUTE IMMEDIATE FROM 'file' USING (name => …) [INTO $a, …]: Snowflake's CALL run"] = imm["plain"] == [{"region": "imm", "n": 1, "total": 2.0}] \
        and imm["into"] == [{"r": "imm", "n": 2}] and "USING (name => value" in imm["bad"]
    if not checks["EXECUTE IMMEDIATE FROM 'file' USING (name => …) [INTO $a, …]: Snowflake's CALL run"]:
        print("immediate:", imm)
    node.kill()
    ok = all(checks.values())
    print(json.dumps({"workspace": checks, "ok": ok}, indent=1))
    if not ok:
        print(json.dumps({"got": got, "pg": pg, "py": py, "js": ran.stdout[-300:] + ran.stderr[-600:], "said": said, "logged": logged, "fail_row": fail_row, "by_task": by_task}, default=str)[:4000])
        sys.exit(1)
    return f"workspace: files run from every door, with parameters, logged, scheduled: all {len(checks)} checks pass"


def renames():
    """ALTER TABLE | VIEW … RENAME TO (ADR-030), on two nodes: rows still in the log stay the
    table's; its files stay in the folder it keeps, and its Delta and Iceberg copies stay there
    for other engines (PyIceberg through the catalog by its new name); the follower reads it by
    its new name; a stored view reads by name, so it reads whatever takes the name next (dbt's
    rename-and-replace); a new table under the old name gets a folder of its own. Refused by
    name: a name that is taken, a table a materialized view or a task follows, a materialized
    view."""
    import deltalake
    from pyiceberg.catalog import load_catalog
    lake = new_lake()
    a = Node(lake, A.port, tier_secs=600).start()  # (tiering far off: new rows wait in the log)
    b = Node(lake, A.port + 1, tier_secs=600).start()
    q = lambda s, port=A.port: sql(port, s)
    q("CREATE TABLE events (id BIGINT, v VARCHAR) WITH (publish = 'delta,iceberg')")
    q("INSERT INTO events VALUES (1, 'a'), (2, 'b')")
    q("CHECKPOINT")
    q("INSERT INTO events VALUES (3, 'c')")  # (in the log only)
    q("CREATE VIEW recent AS SELECT * FROM events WHERE id > 1")
    renamed = q("ALTER TABLE events RENAME TO events_old")
    checks = {"rows in the log before the rename are the renamed table's": q("SELECT count(*) AS n FROM events_old") == [{"n": 3}],
              "the follower reads it by its new name, and the old one is gone": until(lambda: _try(lambda: sql(A.port + 1, "SELECT count(*) AS n FROM events_old")), [{"n": 3}], 15) == [{"n": 3}]
                  and bool(_raises_text(lambda: sql(A.port + 1, "SELECT * FROM events")))}
    q("CREATE TABLE events (id BIGINT, v VARCHAR) WITH (publish = 'delta')")
    q("INSERT INTO events VALUES (10, 'z')")
    checks["a view reads by name: the new table under the old name (dbt's rename-and-replace)"] = q("SELECT id FROM recent") == [{"id": 10}]
    q("CHECKPOINT")
    if not A.s3:
        folders = sorted(os.listdir(os.path.join(lake, "data")))
        old = delta_table(os.path.join(lake, "data", "events")).num_rows
        cat = load_catalog("pondra", type="rest", uri=f"http://127.0.0.1:{A.port}")
        by_rest = cat.load_table("default.events_old").scan().to_arrow().num_rows
        checks["its files, Delta and Iceberg copies stay in its folder; a new table under the old name has a folder of its own"] = \
            {"events", "events__2"} <= set(folders) and old == 3 and by_rest == 3
    q("ALTER VIEW recent RENAME TO recent_events")
    checks["ALTER VIEW … RENAME TO"] = q("SELECT id FROM recent_events") == [{"id": 10}] and bool(_raises_text(lambda: q("SELECT * FROM recent")))
    q("CREATE MATERIALIZED VIEW per_v AS SELECT v, count(*) AS n FROM events GROUP BY v")
    refused = {"a taken name": _raises_text(lambda: q("ALTER TABLE events RENAME TO events_old")),
               "a table a materialized view follows": _raises_text(lambda: q("ALTER TABLE events RENAME TO events_new")),
               "a materialized view": _raises_text(lambda: q("ALTER VIEW per_v RENAME TO per_v2"))}
    checks["refused by name: " + ", ".join(refused)] = "exists already" in refused["a taken name"] and "materialized view per_v" in refused["a table a materialized view follows"] \
        and "materialized view" in refused["a materialized view"] and q("SELECT count(*) AS n FROM events") == [{"n": 1}]
    a.kill(); b.kill()
    ok = all(checks.values())
    print(json.dumps({"renames": checks, "ok": ok}, indent=1))
    if not ok:
        print(renamed, {k: v[:300] for k, v in refused.items()})
        sys.exit(1)
    return f"renames: ALTER TABLE | VIEW … RENAME TO, rows, files and copies kept, a follower, views by name: all {len(checks)} checks pass"


def server():
    """`pondra serve <folder>` (ADR-030, ADR-032): three lakes in a folder (with --s3, under a bucket's
    prefix), served as databases —
    found as such, or named so (`--lakes`). psql and HTTP reach each by name, and queries join
    across them; the console is at `/`; CREATE DATABASE makes one and DROP DATABASE drops one; a
    database idle for PONDRA_DATABASE_IDLE_SECS stops, and starts again when next used; each
    database's node gets `pondra serve`'s options; a connection open (or a request in flight)
    keeps its database's node running; another node joins a database's cluster through
    the server; the server killed and started again serves the same databases; `--flight` is
    refused, and a folder that holds other things than lakes isn't made one."""
    import psycopg
    folder = new_lake()  # (a folder of lakes, here)
    for name, q in [("sales", "CREATE TABLE orders AS SELECT 1 AS id, 10.5 AS amount UNION ALL SELECT 2, 20.0"),
                    ("crm", "CREATE TABLE customers AS SELECT 1 AS id, 'ann' AS name UNION ALL SELECT 2, 'bob'"), ("lake", "CREATE TABLE notes AS SELECT 'hi' AS text")]:
        subprocess.run([BIN, "sql", "--dir", os.path.join(folder, name), q], check=True, capture_output=True)
    port, pg = A.port, A.port + 10
    env = {**os.environ, "PONDRA_DATABASE_IDLE_SECS": "3"}
    def start(how=()):
        p = subprocess.Popen([BIN, "serve", *(how or [folder]), "--addr", f"127.0.0.1:{port}", "--pg", f"127.0.0.1:{pg}", "--memory-gb", "1.5"], env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        until(lambda: _try(lambda: call(port, "GET", "/databases") is not None), True, 30)
        return p
    srv = start()
    dsn = lambda db: f"host=127.0.0.1 port={pg} dbname={db} user=u password=x"
    def pq(db, s):
        with psycopg.connect(dsn(db), autocommit=True) as c:
            cur = c.execute(s)
            return cur.fetchall() if cur.description else cur.statusmessage
    running = lambda: {d["name"]: d["running"] for d in call(port, "GET", "/databases")}
    checks = {}
    checks["the folder's lakes are its databases, none running until used"] = running() == {"crm": False, "lake": False, "sales": False}
    checks["psql and HTTP reach each database by name; no /db/ means the default (lake)"] = pq("sales", "SELECT sum(amount) FROM orders") == [(30.5,)] \
        and call(port, "POST", "/db/crm/sql", b"SELECT name FROM customers ORDER BY id") == [{"name": "ann"}, {"name": "bob"}] and call(port, "POST", "/sql", b"SELECT text FROM notes") == [{"text": "hi"}]
    nodes_of = lambda: [open(f"/proc/{p}/cmdline").read().split("\0") for p in subprocess.run(["pgrep", "-x", "pondra"], capture_output=True, text=True).stdout.split()
                        if os.path.exists(f"/proc/{p}/cmdline") and "--attach-found" in open(f"/proc/{p}/cmdline").read() and folder in open(f"/proc/{p}/cmdline").read()]
    checks["each database's node gets pondra serve's options (--memory-gb 1.5 here)"] = bool(nodes_of()) and all("--memory-gb" in a and a[a.index("--memory-gb") + 1] == "1.5" for a in nodes_of())
    checks["a query joins across databases (name.schema.table)"] = pq("sales", "SELECT c.name, o.amount FROM orders o JOIN crm.customers c ON c.id = o.id ORDER BY o.id") == [("ann", 10.5), ("bob", 20.0)]
    checks["the console is at /"] = b"<html" in call(port, "GET", "/").lower() or b"<!doctype html" in call(port, "GET", "/").lower()
    pq("sales", "CREATE DATABASE hr")
    t0 = time.time()
    call(port, "POST", "/db/hr/sql", b"CREATE TABLE people AS SELECT 1 AS id")  # (a new database's first write: its node starts, takes the lease, writes)
    first_write = round(time.time() - t0, 1)
    missing = _raises_text(lambda: pq("nope", "SELECT 1"))
    pq("sales", "DROP DATABASE crm")
    checks["CREATE DATABASE makes one, DROP DATABASE drops it, folder and all; an unknown one is said so"] = "hr" in running() and "crm" not in running() \
        and not lake_objects(folder, "crm/") and 'database "nope" does not exist' in missing
    idle = until(lambda: sum(running().values()), 0, 30)
    checks["idle databases stop, and start again when used"] = idle == 0 and pq("hr", "SELECT count(*) FROM people") == [(1,)]
    with psycopg.connect(dsn("hr"), autocommit=True) as held:  # (open past the idle time: its node stays)
        held.execute("SELECT 1")
        time.sleep(7)
        kept = held.execute("SELECT count(*) FROM people").fetchall() == [(1,)] and running().get("hr") is True
    let_go = until(lambda: running().get("hr"), False, 30)
    checks["a database with a connection open isn't stopped, however long it is idle; closed, it stops"] = kept and let_go is False
    # Another node joins a database's cluster through the server (its node advertises host:port/db/sales).
    pq("sales", "SELECT 1")
    other = Node(os.path.join(folder, "sales"), port + 2).start()
    joined = until(lambda: _try(lambda: call(port + 2, "GET", "/stats")["role"]), "follower", 20)
    call(port + 2, "POST", "/sql", b"INSERT INTO orders VALUES (3, 5.0)")  # (forwarded to the leader, through the server)
    checks["another node joins a database's cluster through the server, and writes through it"] = joined == "follower" and pq("sales", "SELECT count(*) FROM orders") == [(3,)]
    other.kill()
    srv.send_signal(signal.SIGKILL)
    srv.wait()
    until(lambda: _try(lambda: sql(port, "SELECT 1")) is None, True, 10)
    srv = start(["--lakes", folder])  # (named so, nothing guessed: for services)
    checks["killed and started again, the server serves the same databases"] = sorted(running()) == ["hr", "lake", "sales"] and pq("sales", "SELECT count(*) FROM orders") == [(3,)]
    refused = subprocess.run([BIN, "serve", folder, "--addr", f"127.0.0.1:{port + 5}", "--flight", "127.0.0.1:1"], capture_output=True, text=True, timeout=30)
    checks["--flight is refused by name (a Flight port is one lake's)"] = refused.returncode != 0 and "one lake" in refused.stderr
    other = tempfile.mkdtemp(prefix="pondra-notalake-")
    open(os.path.join(other, "notes.txt"), "w").write("mine")
    kept = subprocess.run([BIN, "serve", other, "--addr", f"127.0.0.1:{port + 6}"], capture_output=True, text=True, timeout=30)
    bare = subprocess.run([BIN, "serve", "--addr", f"127.0.0.1:{port + 7}"], capture_output=True, text=True, timeout=30, cwd=other)
    mixed = [subprocess.run([BIN, "serve", "--lake", folder, "--addr", f"127.0.0.1:{port + 8}"], capture_output=True, text=True, timeout=30),
             subprocess.run([BIN, "serve", "--lakes", os.path.join(folder, "sales"), "--addr", f"127.0.0.1:{port + 9}"], capture_output=True, text=True, timeout=30)]
    checks["--lake on a folder of lakes, or --lakes on a lake, is refused and says which was meant"] = all(m.returncode != 0 for m in mixed) \
        and "--lakes" in mixed[0].stderr and "--lake " in mixed[1].stderr
    checks["a folder holding other things isn't made a lake, and nor is this folder unless named"] = kept.returncode != 0 and "other things" in kept.stderr \
        and not os.path.exists(os.path.join(other, "catalog")) and bare.returncode != 0 and "no lake in this folder" in bare.stderr
    shutil.rmtree(other, ignore_errors=True)
    srv.send_signal(signal.SIGTERM)
    srv.wait(timeout=60)
    time.sleep(1)
    left = subprocess.run(["pgrep", "-x", "pondra"], capture_output=True, text=True).stdout.split()
    checks["stopped, the server stops its databases' nodes"] = not [p for p in left if os.path.exists(f"/proc/{p}/cmdline") and folder in open(f"/proc/{p}/cmdline").read()]
    ok = all(checks.values())
    print(json.dumps({"server": checks, "ok": ok, "first_write_s": first_write}, indent=1))
    if not ok:
        print(missing[:300], idle, joined)
        sys.exit(1)
    return f"server: a folder of lakes as databases over Postgres and HTTP, created, dropped, idle-stopped, joined, restarted: all {len(checks)} checks pass"


# ---------------------------------------------------------------- round 29, part 2 (ADR-035)

def users():
    """Users, roles and grants (ADR-035 §2): made in SQL; signed in at every door — HTTP (a password,
    a token, a session), Postgres (SCRAM; a token-only user's token), Kafka (SASL/PLAIN), Flight (a
    handshake gives a session) — and held to their grants: a table, some of its columns (a scan of
    another refused, its filters' too), a schema's tables (later ones too), public's; writes by
    privilege; a listing shows what they may read; a stream of rows needs every column. Revoked and
    dropped at once, on the leader and a follower; tokens and sessions end; once a user signs in,
    nothing without a sign-in, though no token is set; the nodes call each other with the lake's
    own key."""
    import base64, psycopg
    lake = new_lake()
    a = Node(lake, A.port, pg=f"127.0.0.1:{A.port + 10}", kafka=f"127.0.0.1:{A.port + 20}", flight=f"127.0.0.1:{A.port + 30}").start()
    b = Node(lake, A.port + 1, env={"PONDRA_SESSION_HOURS": "0.0005"}).start()  # (a follower; its sessions last 1.8 s)
    checks = {}

    def as_(port, auth, q, method="POST", path="/sql"):
        try:
            return call(port, method, path, q.encode() if isinstance(q, str) else q, headers={"Authorization": auth} if auth else {})
        except Exception as e:
            return f"ERROR {e}"
    basic = lambda u, p: "Basic " + base64.b64encode(f"{u}:{p}".encode()).decode()
    boss = basic("boss", "boss-password-1")
    # Open, until a user who signs in exists; then nothing without a sign-in (no token is set)
    opened = as_(A.port, None, "SELECT 1 AS x")
    for q in ["CREATE SCHEMA sales", "CREATE TABLE sales.orders (id BIGINT, item VARCHAR, amount DOUBLE, card VARCHAR)", "INSERT INTO sales.orders VALUES (1, 'tea', 2.5, '4111'), (2, 'cake', 4.0, '5500')",
              "CREATE TABLE hr (id BIGINT, salary DOUBLE)", "INSERT INTO hr VALUES (1, 100.0)", "CREATE TABLE notes (id BIGINT, text VARCHAR)", "INSERT INTO notes VALUES (1, 'hello')",
              "CREATE USER boss PASSWORD 'boss-password-1' SUPERUSER"]:
        as_(A.port, None, q)
    closed = until(lambda: as_(A.port, None, "SELECT 1 AS x"), "ERROR 401: b\"sign in: a token, or a user's name and password\"", 10)
    checks["open until a user who signs in exists; then nothing without a sign-in (no token set)"] = opened == [{"x": 1}] and closed.startswith("ERROR 401")
    for q in ["CREATE USER ann PASSWORD 'ann-password-1'", "CREATE ROLE analyst", "GRANT SELECT (id, item, amount) ON sales.orders TO analyst", "GRANT analyst TO ann",
              "CREATE USER bob PASSWORD 'bob-password-1'", "GRANT SELECT, INSERT ON ALL TABLES IN SCHEMA sales TO bob", "GRANT SELECT ON notes TO public", "CREATE USER svc"]:
        as_(A.port, boss, q)
    ann, bob = basic("ann", "ann-password-1"), basic("bob", "bob-password-1")
    said = {
        "cols": as_(A.port, ann, "SELECT id, item FROM sales.orders ORDER BY id"),
        "star": as_(A.port, ann, "SELECT * FROM sales.orders"),
        "filter": as_(A.port, ann, "SELECT id FROM sales.orders WHERE card = '4111'"),
        "other": as_(A.port, ann, "SELECT * FROM hr"),
        "count": as_(A.port, ann, "SELECT count(*) AS n FROM sales.orders"),
        "public": as_(A.port, ann, "SELECT text FROM notes"),
        "write": as_(A.port, ann, "INSERT INTO sales.orders VALUES (3, 'pie', 1.0, '1')"),
        "listed": as_(A.port, ann, "SELECT name FROM pondra.tables ORDER BY name"),
        "info": as_(A.port, ann, "SELECT table_name FROM information_schema.tables WHERE table_schema IN ('public', 'sales') ORDER BY 1"),
        "users": as_(A.port, ann, "CREATE USER eve PASSWORD 'eve-password-1'"),
        "wrong": as_(A.port, basic("ann", "not-her-password"), "SELECT 1 AS x"),
    }
    checks["a user reads the columns granted (its roles' and public's); *, a filter on another column, another table: refused, saying why"] = \
        said["cols"] == [{"id": 1, "item": "tea"}, {"id": 2, "item": "cake"}] and "permission denied: SELECT (card) on sales.orders" in said["star"] \
        and "permission denied: SELECT (card)" in said["filter"] and "permission denied: SELECT on hr" in said["other"] and said["count"] == [{"n": 2}] and said["public"] == [{"text": "hello"}]
    checks["...writes only as granted; lists only what it may read; makes no users; a wrong password refused"] = "permission denied: INSERT on sales.orders" in said["write"] \
        and said["listed"] == [{"name": "notes"}, {"name": "orders"}] and said["info"] == [{"table_name": "notes"}, {"table_name": "orders"}] and "superuser" in said["users"] and said["wrong"].startswith("ERROR 401")
    # A schema's tables, later ones too; writes through a follower (the nodes' own key between them)
    as_(A.port, boss, "CREATE TABLE sales.refunds (id BIGINT, amount DOUBLE)")
    wrote = [as_(A.port + 1, bob, "INSERT INTO sales.refunds VALUES (1, 2.5)"), as_(A.port + 1, bob, "INSERT INTO hr VALUES (2, 1.0)"), as_(A.port, bob, "UPDATE sales.orders SET amount = 0")]
    later = until(lambda: as_(A.port + 1, bob, "SELECT id FROM sales.refunds"), [{"id": 1}], 10)
    checks["a schema's grant covers its later tables; a follower forwards a user's writes (the nodes' own key), refusing the rest"] = \
        wrote[0] == {"rows": 1} and "permission denied: INSERT on hr" in wrote[1] and "permission denied: UPDATE on sales.orders" in wrote[2] and later == [{"id": 1}]
    # Tokens and sessions
    tok = as_(A.port, boss, "CREATE TOKEN ci FOR USER svc")
    short = as_(A.port, boss, "CREATE TOKEN brief FOR USER svc EXPIRES IN '1 second'")
    as_(A.port, boss, "GRANT SELECT ON hr TO svc")
    by_token = until(lambda: as_(A.port + 1, "Bearer " + tok["token"], "SELECT salary FROM hr"), [{"salary": 100.0}], 10)  # (a follower: its grants as of a second ago)
    time.sleep(1.5)
    expired = as_(A.port, "Bearer " + short["token"], "SELECT 1 AS x")
    session = as_(A.port, None, json.dumps({"user": "ann", "password": "ann-password-1"}), path="/login")
    by_session = as_(A.port + 1, "Bearer " + session["token"], "SELECT item FROM sales.orders WHERE id = 1")
    tampered = as_(A.port, "Bearer " + session["token"][:-4] + "AAAA", "SELECT 1 AS x")
    short_session = as_(A.port + 1, None, json.dumps({"user": "ann", "password": "ann-password-1"}), path="/login")
    time.sleep(2.2)
    ended = as_(A.port, "Bearer " + short_session["token"], "SELECT 1 AS x")
    as_(A.port, boss, "DROP TOKEN ci FOR svc")
    dropped_token = as_(A.port, "Bearer " + tok["token"], "SELECT 1 AS x")
    checks["a user's token (shown once, kept as its hash) and session (signed, any node checks it) sign in; expired, tampered, dropped: refused"] = \
        tok["token"].startswith("pt_") and by_token == [{"salary": 100.0}] and expired.startswith("ERROR 401") and by_session == [{"item": "tea"}] \
        and tampered.startswith("ERROR 401") and ended.startswith("ERROR 401") and dropped_token.startswith("ERROR 401") \
        and "pt_" not in json.dumps(as_(A.port, boss, "SELECT * FROM pondra.users"))
    # Postgres: SCRAM-SHA-256 (the verifier only), a token-only user's token as its password
    pg = lambda u, p, q: psycopg.connect(f"host=127.0.0.1 port={A.port + 10} user={u} password={p} dbname=lake", connect_timeout=10).execute(q).fetchall()
    tok2 = as_(A.port, boss, "CREATE TOKEN pg FOR USER svc")["token"]
    pg_said = [_raises_text(lambda: pg("ann", "ann-password-1", "SELECT id, item FROM sales.orders ORDER BY id")), _raises_text(lambda: pg("ann", "ann-password-1", "SELECT * FROM sales.orders")),
               _raises_text(lambda: pg("ann", "wrong-password", "SELECT 1")), _raises_text(lambda: pg("svc", tok2, "SELECT salary FROM hr")), _raises_text(lambda: pg("boss", "boss-password-1", "SELECT card FROM sales.orders ORDER BY id"))]
    rows = [pg("ann", "ann-password-1", "SELECT id, item FROM sales.orders ORDER BY id"), pg("svc", tok2, "SELECT salary FROM hr"), pg("boss", "boss-password-1", "SELECT card FROM sales.orders ORDER BY id")]
    checks["Postgres: SCRAM-SHA-256 with the user's password, a token-only user's token; held to the grants; a wrong password refused"] = \
        pg_said[0] == "" and "permission denied" in pg_said[1] and "authentication failed" in pg_said[2].lower() and pg_said[3] == "" and pg_said[4] == "" \
        and rows == [[(1, "tea"), (2, "cake")], [(100.0,)], [("4111",), ("5500",)]]
    # Kafka: SASL/PLAIN; producing needs INSERT, consuming every column
    from kafka import KafkaConsumer, KafkaProducer
    from kafka.errors import TopicAuthorizationFailedError
    sasl = lambda u, p: dict(bootstrap_servers=f"127.0.0.1:{A.port + 20}", security_protocol="SASL_PLAINTEXT", sasl_mechanism="PLAIN", sasl_plain_username=u, sasl_plain_password=p)
    def produce(u, p, topic):
        pr = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode(), **sasl(u, p))
        try:
            return pr.send(topic, {"id": 7, "amount": 1.0}).get(timeout=15) and "ok"
        except TopicAuthorizationFailedError:
            return "refused"
        finally:
            pr.close()
    def consume(u, p, topic):
        c = KafkaConsumer(topic, auto_offset_reset="earliest", consumer_timeout_ms=4000, **sasl(u, p))
        try:
            return len(list(c))
        except TopicAuthorizationFailedError:
            return "refused"
        finally:
            c.close()
    kafka_said = [produce("bob", "bob-password-1", "sales.refunds"), produce("ann", "ann-password-1", "sales.refunds"), consume("bob", "bob-password-1", "sales.refunds"), consume("ann", "ann-password-1", "sales.orders")]
    checks["Kafka: SASL/PLAIN as a user; producing needs INSERT; consuming a table needs SELECT on every column"] = kafka_said == ["ok", "refused", 2, "refused"]
    # Flight: the handshake turns a user's name and password into a session
    import pyarrow.flight as fl
    client = fl.FlightClient(f"grpc://127.0.0.1:{A.port + 30}")
    bearer = client.authenticate_basic_token("ann", "ann-password-1")
    opts = fl.FlightCallOptions(headers=[bearer])
    def fsql(q):
        try:
            info = client.get_flight_info(fl.FlightDescriptor.for_command(json.dumps({"sql": q})), opts)
            return client.do_get(info.endpoints[0].ticket, opts).read_all().to_pylist()
        except Exception as e:
            return f"ERROR {e}"
    flight_said = [bearer[1].decode().startswith("Bearer ps_"), fsql("SELECT item FROM sales.orders ORDER BY id"), fsql("SELECT card FROM sales.orders")]
    checks["Flight: a handshake with a user's name and password gives a session; held to the grants"] = flight_said[0] and flight_said[1] == [{"item": "tea"}, {"item": "cake"}] and "permission denied" in flight_said[2]
    # Revoked and dropped: at once, on the leader and a follower
    as_(A.port, boss, "REVOKE SELECT (item) ON sales.orders FROM analyst")
    revoked = [until(lambda: "permission denied" in str(as_(port, ann, "SELECT item FROM sales.orders")), True, 10) for port in (A.port, A.port + 1)]
    still = as_(A.port, ann, "SELECT id FROM sales.orders ORDER BY id")
    as_(A.port, boss, "REVOKE analyst FROM ann")
    no_role = until(lambda: "permission denied" in str(as_(A.port + 1, ann, "SELECT id FROM sales.orders")), True, 10)
    grants = as_(A.port, boss, "SELECT grantee, privilege, on_name, columns FROM pondra.grants ORDER BY grantee, privilege, on_name")
    as_(A.port, boss, "DROP USER ann")
    gone = [until(lambda: str(as_(port, ann, "SELECT 1 AS x")).startswith("ERROR 401"), True, 10) for port in (A.port, A.port + 1)]
    checks["revoked (a column, a role) and dropped: at once on the leader and a follower; pondra.grants says what stands"] = revoked == [True, True] and still == [{"id": 1}, {"id": 2}] \
        and no_role is True and gone == [True, True] and {"grantee": "analyst", "privilege": "SELECT", "on_name": "sales.orders", "columns": "id, amount"} in grants
    info = {"said": said, "wrote": wrote, "pg": pg_said, "kafka": kafka_said, "flight": flight_said[1:], "grants": grants, "revoked": [revoked, still, no_role, gone],
            "tokens": [by_token, expired, by_session, tampered, ended, dropped_token]}
    a.kill(), b.kill()  # (the ports free for the next test of `all`)
    ok = all(checks.values())
    print(json.dumps({"users": checks, "ok": ok, "info": info}, indent=1, default=str))
    return ok


def secrets():
    """Secrets (ADR-035 §3): each sealed with a data key of its own, which the master key wraps
    (the values never in the clear in the catalog); a user uses one only with USAGE on it; a
    session's CREATE TEMPORARY SECRET is its own, in memory, nowhere in the lake; a new master key
    (the previous one still set) rewraps every data key when the leader starts; a key service
    (PONDRA_KMS_COMMAND) wraps them instead, and without it they don't open."""
    import base64, http.server, socketserver
    here = tempfile.mkdtemp(prefix="pondra-secrets-")
    open(os.path.join(here, "rates.csv"), "w").write("cur,rate\nEUR,1.1\nGBP,1.3\n")
    class Quiet(http.server.SimpleHTTPRequestHandler):
        def __init__(self, *a, **k):
            super().__init__(*a, directory=here, **k)
        def log_message(self, *a):
            pass
    web = socketserver.TCPServer(("127.0.0.1", 0), Quiet)
    threading.Thread(target=web.serve_forever, daemon=True).start()
    url = f"http://127.0.0.1:{web.server_address[1]}/"
    kms = os.path.join(here, "kms.sh")  # (a stand-in key service: it "wraps" by reversing, as a test can see)
    open(kms, "w").write("#!/bin/sh\nread x\nif [ \"$1\" = wrap ]; then echo \"$x\" | rev; else echo \"$x\" | rev; fi\n")
    os.chmod(kms, 0o755)
    lake = new_lake()
    keys = {"PONDRA_SECRET_KEY": "first-master-key"}
    node = Node(lake, A.port, admin_token="adm", env=keys).start()
    adm = lambda q, h=None: call(A.port, "POST", "/sql", q.encode(), headers={"Authorization": "Bearer adm", **(h or {})})
    def as_(auth, q, session=None):
        try:
            return call(A.port, "POST", "/sql", q.encode(), headers={"Authorization": auth, **({"x-pondra-session": session} if session else {})})
        except Exception as e:
            return f"ERROR {e}"
    basic = lambda u, p: "Basic " + base64.b64encode(f"{u}:{p}".encode()).decode()
    adm(f"CREATE SECRET rates (TYPE http, BEARER_TOKEN 'tok-123456', SCOPE '{url}')")
    adm("CREATE USER ann PASSWORD 'ann-password-1'")
    adm("CREATE USER bob PASSWORD 'bob-password-1'")
    adm("GRANT USAGE ON SECRET rates TO ann")
    ann, bob = basic("ann", "ann-password-1"), basic("bob", "bob-password-1")
    read = f"SELECT cur FROM read_csv('{url}rates.csv') ORDER BY cur"
    catalog = lambda: subprocess.run([BIN, "catalog", "--dir", lake, "e/"], capture_output=True, text=True, env={**os.environ, **keys}).stdout
    used = [as_(ann, read), as_(bob, read)]
    sealed = catalog()
    checks = {}
    checks["a secret is used only by who has USAGE on it (a token's or superuser's: any); its values never in the clear in the catalog, a data key of its own wrapped"] = \
        used[0] == [{"cur": "EUR"}, {"cur": "GBP"}] and "no secret" in used[1].lower() and "tok-123456" not in sealed and '"key"' in sealed and adm(read) == [{"cur": "EUR"}, {"cur": "GBP"}]
    # A session's own temporary secret: its queries only; not in the lake
    mine = as_(bob, f"CREATE TEMPORARY SECRET bobs (TYPE http, BEARER_TOKEN 'bob-own-9999', SCOPE '{url}')", session="bob-session-1")
    temp = [as_(bob, read, session="bob-session-1"), as_(bob, read, session="bob-session-2"), as_(bob, "SELECT name FROM secrets() ORDER BY name", session="bob-session-1"), as_(ann, read, session="bob-session-1")]
    checks["CREATE TEMPORARY SECRET: the session's own (another session, another user naming it: not), in memory, nowhere in the lake"] = mine == {"secret": "bobs", "temporary": True} \
        and temp[0] == [{"cur": "EUR"}, {"cur": "GBP"}] and "no secret" in temp[1].lower() and {"name": "bobs"} in temp[2] and "bob-own-9999" not in catalog() and "bobs" not in catalog() \
        and temp[3] == [{"cur": "EUR"}, {"cur": "GBP"}]  # (ann's own grant, not bob's secret: her session is her own)
    # A new master key: the previous one set, the leader rewraps every data key when it starts
    node.kill()
    keys = {"PONDRA_SECRET_KEY": "second-master-key", "PONDRA_SECRET_KEY_PREVIOUS": "first-master-key"}
    node = Node(lake, A.port, admin_token="adm", env=keys).start()
    rewrapped = until(lambda: "rewrapped 1 secret" in open(node.log).read(), True, 20)
    node.kill()
    keys = {"PONDRA_SECRET_KEY": "second-master-key"}
    node = Node(lake, A.port, admin_token="adm", env=keys).start()
    after = adm(read)
    node.kill()
    wrong = Node(lake, A.port, admin_token="adm", env={"PONDRA_SECRET_KEY": "first-master-key"}).start()
    refused = _raises_text(lambda: adm(read))
    wrong.kill()
    checks["a new master key: the leader rewraps the data keys (the previous key set), then the secret opens with the new key alone, not the old"] = rewrapped is True and after == [{"cur": "EUR"}, {"cur": "GBP"}] and "master key" in refused
    # A key service wraps them (PONDRA_KMS_COMMAND), the master key never in a file or a variable
    keys = {"PONDRA_KMS_COMMAND": kms}
    node = Node(lake, A.port, admin_token="adm", env=keys).start()
    adm(f"CREATE OR REPLACE SECRET rates (TYPE http, BEARER_TOKEN 'tok-654321', SCOPE '{url}')")
    by_kms = [adm(read), '"key":"kms:' in catalog().replace(" ", "")]
    node.kill()
    node = Node(lake, A.port, admin_token="adm", env={"PONDRA_SECRET_KEY": "second-master-key"}).start()
    without = _raises_text(lambda: adm(read))
    checks["PONDRA_KMS_COMMAND wraps a data key (kms:…); without it, it doesn't open"] = by_kms == [[{"cur": "EUR"}, {"cur": "GBP"}], True] and "PONDRA_KMS_COMMAND" in without
    web.shutdown()
    node.kill()
    ok = all(checks.values())
    print(json.dumps({"secrets": checks, "ok": ok, "info": {"used": used, "temp": temp, "refused": refused, "without": without}}, indent=1, default=str))
    return ok


def safety():
    """Safe to share, the rest of it (round 29, ADR-035 §5): a panic in a request is answered as an
    error, the node kept up (HTTP, Postgres, a query's own tasks); TLS on every door (HTTPS,
    Postgres's sslmode, Kafka's SSL, Flight's grpc+tls), plain connections only from this machine,
    nodes calling each other over HTTPS, the nodes' key only with the authority's certificate
    (mutual TLS); the audit log (what's written by default, values hidden, refusals at every door,
    a superuser's only); a user's quota (statements at once, and how long each may run)."""
    import base64, re, socket, ssl, struct, psycopg, pyarrow.flight as fl
    here = tempfile.mkdtemp(prefix="pondra-tls-")
    ca_key, ca, key, csr, crt, ext = [os.path.join(here, n) for n in ("ca.key", "ca.pem", "node.key", "node.csr", "node.pem", "ext.cnf")]
    sh = lambda *a: subprocess.run(a, check=True, capture_output=True)
    sh("openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", ca_key, "-out", ca, "-days", "2", "-subj", "/CN=pondra test authority")
    sh("openssl", "req", "-newkey", "rsa:2048", "-nodes", "-keyout", key, "-out", csr, "-subj", "/CN=pondra node")
    open(ext, "w").write("subjectAltName=IP:127.0.0.1,DNS:localhost\nextendedKeyUsage=serverAuth,clientAuth\n")
    sh("openssl", "x509", "-req", "-in", csr, "-CA", ca, "-CAkey", ca_key, "-CAcreateserial", "-out", crt, "-days", "2", "-extfile", ext)
    # Another machine, as far as the node can tell: this one's own non-loopback address, to 127.0.0.1
    away = next((ip for ip in subprocess.run(["hostname", "-I"], capture_output=True, text=True).stdout.split() if not ip.startswith("127.") and ":" not in ip), None)
    P = A.port
    lake = new_lake()
    tls = dict(tls_cert=crt, tls_key=key, tls_ca=ca)
    a = Node(lake, P, pg=f"127.0.0.1:{P + 10}", kafka=f"127.0.0.1:{P + 20}", flight=f"127.0.0.1:{P + 30}", env={"PONDRA_TEST_PANICS": "1"}, **tls).start()
    checks, info = {}, {"away": away}

    def http_(port, path, body=b"", source=None, auth=None, secure=False, cert=False, method="POST"):
        if secure:
            ctx = ssl.create_default_context(cafile=ca)
            if cert:
                ctx.load_cert_chain(crt, key)
            c = http.client.HTTPSConnection("127.0.0.1", port, context=ctx, timeout=60, source_address=(source, 0) if source else None)
        else:
            c = http.client.HTTPConnection("127.0.0.1", port, timeout=60, source_address=(source, 0) if source else None)
        try:
            c.request(method, path, body, {"Authorization": auth} if auth else {})
            r = c.getresponse()
            return r.status, r.read().decode(errors="replace")
        except Exception as e:
            return 0, f"{type(e).__name__}: {e}"

    # Panics: answered as errors; the node stays up
    panic = http_(P, "/sql", b"SELECT pondra_panic() AS x")
    spread = http_(P, "/sql", b"SELECT v % 5 AS k, sum(pondra_panic()) AS s FROM generate_series(1, 200000) g(v) GROUP BY 1")
    with psycopg.connect(f"host=127.0.0.1 port={P + 10} user=admin dbname=lake sslmode=verify-full sslrootcert={ca}", autocommit=True) as c:
        pg_panic = _raises_text(lambda: c.execute("SELECT pondra_panic()").fetchall())
        pg_after = c.execute("SELECT 1 AS x").fetchall()
        pg_ssl = c.pgconn.ssl_in_use
    info["panics"] = [panic, spread[1][:200], pg_panic]
    checks["a panic in a request is an error (HTTP 500, a query's own tasks', Postgres's), the node and the connection kept"] = \
        panic[0] == 500 and "pondra_panic() was called" in panic[1] and spread[0] == 500 and "pondra_panic() was called" in spread[1] \
        and "pondra_panic() was called" in pg_panic and pg_after == [(1,)] and a.alive() and sql(P, "SELECT 1 AS x") == [{"x": 1}]

    # TLS at every door; plain from this machine only
    said = {
        "https": http_(P, "/sql", b"SELECT 1 AS x", secure=True),
        "https away": http_(P, "/sql", b"SELECT 1 AS x", source=away, secure=True) if away else None,
        "plain here": http_(P, "/sql", b"SELECT 1 AS x"),
        "plain away": http_(P, "/sql", b"SELECT 1 AS x", source=away) if away else None,
    }
    checks["HTTPS answers; plain HTTP only from this machine, refused from another saying why"] = said["https"] == (200, '[{"x":1}]') and said["plain here"] == (200, '[{"x":1}]') \
        and (not away or (said["https away"] == (200, '[{"x":1}]') and said["plain away"][0] == 403 and "takes TLS" in said["plain away"][1]))

    def raw(port, chunks, source=None, wrap=False, first=None):
        s = socket.create_connection(("127.0.0.1", port), timeout=10, source_address=(source, 0) if source else None)
        try:
            if first:
                s.sendall(first)
                if s.recv(1) != b"S":
                    return b"no TLS"
            if wrap:
                s = ssl.create_default_context(cafile=ca).wrap_socket(s, server_hostname="127.0.0.1")
            for c in chunks:
                s.sendall(c)
            got = b""
            while True:
                d = s.recv(65536)
                if not d:
                    break
                got += d
                if len(got) > 4 and (got.endswith(b"Z\x00\x00\x00\x05I") or got[:4] == struct.pack(">i", len(got) - 4)):
                    break  # (Postgres ready, or one whole Kafka response)
            return got
        except OSError as e:
            return f"{type(e).__name__}".encode()
        finally:
            s.close()
    start = lambda: (lambda b: struct.pack(">i", len(b) + 4) + b)(struct.pack(">i", 196608) + b"user\0admin\0database\0lake\0\0")
    pg_tls = raw(P + 10, [start()], wrap=True, first=struct.pack(">ii", 8, 80877103))
    pg_away = raw(P + 10, [start()], source=away) if away else b""
    versions = struct.pack(">hhi", 18, 0, 7) + struct.pack(">h", 1) + b"t"
    kafka_tls = raw(P + 20, [struct.pack(">i", len(versions)) + versions], wrap=True)
    kafka_away = raw(P + 20, [struct.pack(">i", len(versions)) + versions], source=away) if away else b""
    client = fl.connect(f"grpc+tls://127.0.0.1:{P + 30}", tls_root_certs=open(ca, "rb").read())
    flight_tls = client.do_get(fl.Ticket(json.dumps({"sql": "SELECT 1 AS x"}).encode())).read_all().to_pylist()
    flight_away = raw(P + 30, [b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n" + bytes(9)], source=away) if away else b""
    info["doors"] = {"pg away": pg_away[:120], "kafka away": kafka_away[:40], "flight away": flight_away[:40]}
    checks["Postgres (sslmode=verify-full), Kafka (SSL) and Flight (grpc+tls) take TLS; plain from another machine refused"] = pg_ssl and b"Z\x00\x00\x00\x05I" in pg_tls \
        and kafka_tls[4:8] == struct.pack(">i", 7) and flight_tls == [{"x": 1}] \
        and (not away or (b"28000" in pg_away and b"takes TLS" in pg_away and kafka_away[:4] != struct.pack(">i", len(kafka_away) - 4) and not flight_away.startswith(b"\x00\x00")))

    # Nodes over HTTPS: a follower with the same certificate joins, and its writes reach the leader
    b = Node(lake, P + 1, **tls).start()
    sql(P, "CREATE TABLE notes (id BIGINT, text VARCHAR)")
    wrote = _raises_text(lambda: sql(P + 1, "INSERT INTO notes VALUES (1, 'over https')")) or "ok"
    seen = until(lambda: sql(P, "SELECT text FROM notes"), [{"text": "over https"}], 20)
    node_key = next(iter(re.findall(r"pn_[A-Za-z0-9_-]+", subprocess.run([BIN, "catalog", "--dir", lake, "z/"], capture_output=True, text=True).stdout)), "")
    bearer = f"Bearer {node_key}"
    keyed = {"here": http_(P, "/stats", auth=bearer, method="GET")[0],
             "away, no certificate": http_(P, "/stats", auth=bearer, source=away, secure=True, method="GET")[0] if away else None,
             "away, the nodes' certificate": http_(P, "/stats", auth=bearer, source=away, secure=True, cert=True, method="GET")[0] if away else None}
    info["nodes"] = {"wrote": wrote, "key": bool(node_key), "keyed": keyed}
    checks["nodes call each other over HTTPS (a follower's write committed by the leader); the nodes' key only with the authority's certificate"] = \
        seen == [{"text": "over https"}] and bool(node_key) and keyed["here"] == 200 and (not away or (keyed["away, no certificate"] == 401 and keyed["away, the nodes' certificate"] == 200))
    b.kill()
    a.kill()

    # The audit log, and quotas
    lake2 = new_lake()
    n = Node(lake2, P + 5, pg=f"127.0.0.1:{P + 15}").start()
    basic = lambda u, p: "Basic " + base64.b64encode(f"{u}:{p}".encode()).decode()
    def q(auth, s):
        try:
            return call(P + 5, "POST", "/sql", s.encode(), headers={"Authorization": auth} if auth else {})
        except Exception as e:
            return f"ERROR {e}"
    for s in ["CREATE TABLE t (id BIGINT, card VARCHAR)", "INSERT INTO t VALUES (1, '4111')", "CREATE USER boss PASSWORD 'boss-password-1' SUPERUSER"]:
        q(None, s)
    boss, ana = basic("boss", "boss-password-1"), basic("ana", "ana-password-1")
    for s in ["CREATE USER ana PASSWORD 'ana-password-1'", "GRANT SELECT (id) ON t TO ana", "CREATE SECRET s (TYPE s3, KEY_ID 'kid', SECRET 'very-secret-value')"]:
        q(boss, s)
    q(basic("ana", "not-her-password"), "SELECT 1")
    denied = q(ana, "SELECT card FROM t")
    q(ana, "SELECT id FROM t")
    _raises(lambda: psycopg.connect(f"host=127.0.0.1 port={P + 15} user=ana password=not-hers dbname=lake"))
    log = lambda: q(boss, "SELECT \"user\", door, \"from\", class, statement, outcome, message FROM pondra.audit ORDER BY at")
    rows = log()
    for _ in range(40):  # (written a moment later, in batches)
        if isinstance(rows, list) and sum(x["outcome"] == "refused" for x in rows) >= 3:
            break
        time.sleep(0.25)
        rows = log()
    info["audit"] = rows
    text = json.dumps(rows)
    has = lambda **want: any(all(str(r.get(k)) == v or (isinstance(v, str) and v.endswith("…") and str(r.get(k)).startswith(v[:-1])) for k, v in want.items()) for r in rows) if isinstance(rows, list) else False
    checks["the audit log: users, grants and secrets made (values '***', nowhere in the clear); refused sign-ins over HTTP and Postgres; a column refused; reads not written by default"] = \
        has(user="boss", door="http", **{"class": "role"}, statement="CREATE USER ana PASSWORD '***'", outcome="ok") and has(user="boss", statement="GRANT SELECT (id) ON t TO ana") \
        and has(statement="CREATE SECRET s (TYPE s3, KEY_ID '***', SECRET '***')") and has(user="ana", door="http", **{"class": "access"}, outcome="refused") \
        and has(user="ana", door="postgres", outcome="refused") and has(user="ana", statement="SELECT card FROM t", outcome="refused") and "permission denied" in denied \
        and not has(statement="SELECT id FROM t") and "ana-password-1" not in text and "very-secret-value" not in text and "boss-password-1" not in text \
        and all(r["from"] and r["from"].startswith("127.0.0.1:") for r in rows if r["door"] == "http")
    checks["...a superuser's only"] = "permission denied: pondra.audit" in q(ana, "SELECT * FROM pondra.audit")
    q(boss, "ALTER USER ana MAX_QUERIES 1 STATEMENT_TIMEOUT '2 seconds'")
    listed = q(boss, "SELECT max_queries, statement_timeout FROM pondra.users WHERE name = 'ana'")
    slow = "SELECT sum(v) AS s FROM generate_series(1, 4000000000) g(v)"
    took = {}
    def timed(name):
        t0 = time.time()
        took[name] = (q(ana, slow), round(time.time() - t0, 2))
    ts = [threading.Thread(target=timed, args=(i,)) for i in range(2)]
    for t in ts:
        t.start()
        time.sleep(0.2)
    for t in ts:
        t.join()
    ends = sorted(s for _, s in took.values())
    q(boss, "ALTER USER ana MAX_QUERIES 0 STATEMENT_TIMEOUT 0")
    after = q(boss, "SELECT max_queries, statement_timeout FROM pondra.users WHERE name = 'ana'")
    info["quota"] = {"listed": listed, "took": took, "after": after}
    checks["a user's quota: one statement at a time (the next waits its turn), each stopped after its STATEMENT_TIMEOUT; pondra.users says so; 0 lifts it"] = \
        listed == [{"max_queries": 1, "statement_timeout": "2 seconds"}] and all("STATEMENT_TIMEOUT" in r for r, _ in took.values()) \
        and 1.5 <= ends[0] <= 3.5 and ends[1] >= ends[0] + 1.5 and after == [{}]  # (nulls: no keys)
    n.kill()
    ok = all(checks.values())
    print(json.dumps({"safety": checks, "ok": ok, "info": info}, indent=1, default=str))
    return ok


def versions():
    """Every file keeps its versions (ADR-035 §8): each save through PUT /files kept with who made
    it and when, listed newest first, read back, restored (itself kept as the newest); a deleted
    file keeps them; files() doesn't list them; the newest PONDRA_FILE_VERSIONS stay; a notebook
    saved before versions (notebooks/<name>/<time>.ipynb) has those saves as versions of
    notebooks/<name>.ipynb, and CALL run('notebooks/<name>') runs the one file once it is saved
    again; nothing outside a file's own versions is read through ?version=."""
    import base64, urllib.parse
    lake = new_lake()
    node = Node(lake, A.port, admin_token="a-tok", python=sys.executable, env={"PONDRA_FILE_VERSIONS": "3", "PYTHONPATH": os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "python")}).start()
    admin = {"authorization": "Bearer a-tok"}
    def go(method, path, body=b"", headers=None):
        c = http.client.HTTPConnection("127.0.0.1", A.port, timeout=60)
        c.request(method, path, body.encode() if isinstance(body, str) else body, {**admin, **(headers or {})})
        r = c.getresponse()
        return r.status, r.read().decode(errors="replace"), r.getheader("etag")
    def put(path, text, headers=None):
        status, body, _ = go("PUT", f"/files/{path}", text, headers)
        return status, json.loads(body) if status == 200 else body
    def save(path, text, **h):
        etag = go("GET", f"/files/{path}")[2]
        return put(path, text, {"if-match": etag, **h} if etag else h)
    listed = lambda path: json.loads(go("GET", f"/files/{path}?versions")[1])
    read = lambda path, v=None: go("GET", f"/files/{path}" + (f"?version={urllib.parse.quote(v)}" if v else ""))[:2]
    checks, info = {}, {}
    call(A.port, "POST", "/sql", b"CREATE USER ann PASSWORD 'ann-password-1'", headers=admin)
    call(A.port, "POST", "/sql", b"GRANT INSERT ON ALL TABLES TO ann", headers=admin)
    for i, text in enumerate(["SELECT 1 AS v", "SELECT 2 AS v"]):
        save("etl/a.sql", text)
    ann = {"authorization": "Basic " + base64.b64encode(b"ann:ann-password-1").decode()}
    etag = go("GET", "/files/etl/a.sql")[2]
    c = http.client.HTTPConnection("127.0.0.1", A.port, timeout=60)
    c.request("PUT", "/files/etl/a.sql", b"SELECT 3 AS v", {**ann, "if-match": etag})
    ann_put = c.getresponse().status
    vs = listed("etl/a.sql")
    info["listed"] = vs
    oldest = vs[-1]["id"] if vs else ""
    checks["each save kept: newest first, by whom (a user, the admin token), its bytes read back"] = ann_put == 200 and [v["who"] for v in vs] == ["ann", "admin", "admin"] \
        and read("etl/a.sql", oldest) == (200, "SELECT 1 AS v") and read("etl/a.sql", vs[0]["id"]) == (200, "SELECT 3 AS v") and vs[0]["at"] >= vs[-1]["at"]
    gone = go("DELETE", "/files/etl/a.sql")[0]
    after_delete = listed("etl/a.sql")
    restored = go("POST", f"/files/etl/a.sql?restore={urllib.parse.quote(oldest)}")
    now = read("etl/a.sql")
    files_listed = call(A.port, "POST", "/sql", b"SELECT path FROM files() ORDER BY path", headers=admin)
    checks["a deleted file keeps its versions; one restored is the file again, kept as the newest; files() doesn't list them"] = gone == 200 and len(after_delete) == 3 \
        and restored[0] == 200 and now == (200, "SELECT 1 AS v") and files_listed == [{"path": "files/etl/a.sql"}]
    for i in range(4):
        save("etl/b.sql", f"SELECT {i}")
    kept = until(lambda: len(listed("etl/b.sql")), 3, 10)
    checks["the newest PONDRA_FILE_VERSIONS (3) stay"] = kept == 3 and read("etl/b.sql", listed("etl/b.sql")[0]["id"]) == (200, "SELECT 3")
    nb = lambda n: json.dumps({"cells": [{"cell_type": "code", "metadata": {}, "source": [f"%%sql\nSELECT {n} AS n"], "outputs": [], "execution_count": None}], "metadata": {}, "nbformat": 4, "nbformat_minor": 5})
    put("notebooks/old/2026-01-01T00-00-00-000Z.ipynb", nb(1))
    put("notebooks/old/2026-02-01T00-00-00-000Z.ipynb", nb(2))
    before = listed("notebooks/old.ipynb")
    ran_before = call(A.port, "POST", "/sql", b"CALL run('notebooks/old')", headers=admin)
    put("notebooks/old.ipynb", nb(3))
    ran_after = call(A.port, "POST", "/sql", b"CALL run('notebooks/old')", headers=admin)
    both = listed("notebooks/old.ipynb")
    info["notebook"] = {"before": before, "ran": [ran_before, ran_after], "both": both}
    checks["a notebook saved before versions: its saves are versions of notebooks/<name>.ipynb; run('notebooks/<name>') runs the one file once there"] = \
        [v["id"] for v in before] == ["notebooks/old/2026-02-01T00-00-00-000Z.ipynb", "notebooks/old/2026-01-01T00-00-00-000Z.ipynb"] \
        and read("notebooks/old.ipynb", before[-1]["id"])[0] == 200 and ran_before == [{"n": 2}] and ran_after == [{"n": 3}] and len(both) == 3
    refused = [read("notebooks/old.ipynb", "../../t/x")[0], read("notebooks/old.ipynb", "notebooks/other/x.ipynb")[0], read("etl/a.sql", "notebooks/old/2026-01-01T00-00-00-000Z.ipynb")[0]]
    checks["nothing but a file's own versions is read through ?version="] = all(r >= 400 for r in refused)
    info["refused"] = refused
    node.kill()
    ok = all(checks.values())
    print(json.dumps({"versions": checks, "ok": ok, "info": info}, indent=1, default=str))
    return ok


def stopped():
    """A run whose node stopped under it (round 29 part 3): a procedure started without waiting
    (pondra.start) is `running` in pondra.runs; the node is killed (-9) and started again at the
    same address; the run is then `stopped`, saying whose node, not `running` for good. A run on
    a node still up is left alone."""
    here = os.path.dirname(os.path.abspath(__file__))
    env = {"PYTHONPATH": os.path.join(here, "..", "python")}
    lake = new_lake()
    node = Node(lake, A.port, env=env, python=sys.executable).start()
    sql(A.port, "CREATE PROCEDURE slow(secs DOUBLE) LANGUAGE python AS $$\nimport time\ntime.sleep(secs)\nreturn 'slept'\n$$")
    long_run = sql(A.port, "SELECT pondra.start('slow', 600.0) AS run")[0]["run"]
    status = lambda run: (sql(A.port, f"SELECT status, error FROM pondra.runs WHERE id = '{run}'") or [{}])[0]
    running = until(lambda: status(long_run).get("status"), "running", 30)
    node.kill()
    node = Node(lake, A.port, env=env, python=sys.executable).start()
    after = until(lambda: status(long_run).get("status"), "stopped", 30)
    said = status(long_run).get("error") or ""
    short_run = sql(A.port, "SELECT pondra.start('slow', 3.0) AS run")[0]["run"]
    mid = until(lambda: status(short_run).get("status"), "running", 30)
    done = until(lambda: status(short_run).get("status"), "ok", 60)
    checks = {"a run whose node was killed and started again: stopped, saying whose node, not running for good": running == "running" and after == "stopped" and f"127.0.0.1:{A.port}" in said and "stopped while it ran" in said,
              "a run on the node that is up: running, then ok": mid == "running" and done == "ok"}
    node.kill()
    ok = all(checks.values())
    print(json.dumps({"stopped": checks, "ok": ok, "info": {"said": said, "statuses": [running, after, mid, done]}}, indent=1, default=str))
    return ok



def flows():
    """Flows of materialized views (ADR-036 §1–2), as Databricks' DLT has them: silver follows
    orders with expectations (one drops rows, one counts them), gold adds silver up, platinum rolls
    gold up, big follows silver row by row; all made while two producers stream into two nodes,
    each filled from the rows already there. Each view == its query over orders, every row once,
    also after an UPDATE and a DELETE of orders; a row a FAIL expectation refuses fails its own
    INSERT only; what can't follow is refused by name; pondra.flows and pondra.expectations."""
    lake = new_lake()
    a = Node(lake, A.port, tier_secs=0.5).start()
    b = Node(lake, A.port + 1, tier_secs=0.5).start()
    ports = [A.port, A.port + 1]
    q = lambda s, port=A.port: sql(port, s)
    def err(s, port=A.port):
        try:
            q(s, port)
            return None
        except Exception as e:
            return str(e)
    q("CREATE TABLE orders (id BIGINT, region VARCHAR, buyer VARCHAR, amount BIGINT, status VARCHAR)")
    stop, sent = threading.Event(), [0, 0]
    def produce(k):
        seq, rng = 0, random.Random(k)
        while not stop.is_set():
            seq += 1
            row = lambda i: {"id": k * 10**9 + seq * 100 + i, "region": f"r{rng.randrange(4)}", "buyer": None if rng.random() < 0.05 else f"u{rng.randrange(20)}",
                             "amount": rng.randrange(-50, 1000), "status": "test" if rng.random() < 0.1 else "paid"}
            body = "".join(json.dumps(row(i)) + "\n" for i in range(50)).encode()
            while True:
                try:
                    call(ports[k], "POST", f"/append/orders?producer=p{k}&seq={seq}", body, timeout=15)
                    break
                except Exception:
                    if stop.is_set():
                        return
                    time.sleep(0.1)
            sent[k] += 50
            time.sleep(0.02)
    threads = [threading.Thread(target=produce, args=(k,), daemon=True) for k in (0, 1)]
    for t in threads:
        t.start()
    time.sleep(1.5)
    q("""CREATE MATERIALIZED VIEW silver (
           CONSTRAINT positive CHECK (amount > 0) ON VIOLATION DROP ROW,
           CONSTRAINT has_buyer EXPECT (buyer IS NOT NULL)
         ) AS SELECT id, region, buyer, amount, amount * 2 AS doubled FROM orders WHERE status <> 'test'""")
    time.sleep(0.7)
    q("CREATE MATERIALIZED VIEW gold AS SELECT region, buyer, count(*) AS n, sum(amount) AS total FROM silver GROUP BY region, buyer", ports[1])
    q("CREATE MATERIALIZED VIEW big AS SELECT id, buyer, doubled FROM silver WHERE doubled > 1000", ports[1])
    time.sleep(0.5)
    q("CREATE MATERIALIZED VIEW platinum AS SELECT region, sum(n) AS n, sum(total) AS total FROM gold GROUP BY region")
    q("CREATE MATERIALIZED VIEW strict (CONSTRAINT small CHECK (amount < 100000)) AS SELECT id, amount FROM orders")
    time.sleep(0.5)
    checks = {}
    refused = err("INSERT INTO orders VALUES (1, 'r0', 'u1', 500000, 'paid'), (2, 'r0', 'u1', 5, 'paid')", ports[1])
    checks["a row a FAIL expectation refuses fails its INSERT, naming it"] = bool(refused) and 'violates check constraint "small"' in refused
    say = {
        "a view of a GROUP BY view's partial rows, without GROUP BY": err("CREATE MATERIALIZED VIEW bad1 AS SELECT * FROM gold WHERE total > 100"),
        "count(*) of a GROUP BY view's rows": err("CREATE MATERIALIZED VIEW bad2 AS SELECT region, count(*) AS n FROM gold GROUP BY region"),
        "a WHERE on a GROUP BY view's totals": err("CREATE MATERIALIZED VIEW bad3 AS SELECT region, sum(total) AS t FROM gold WHERE total > 5 GROUP BY region"),
        "expectations on a GROUP BY view": err("CREATE MATERIALIZED VIEW bad4 (CONSTRAINT c CHECK (n > 0)) AS SELECT region, count(*) AS n FROM orders GROUP BY region"),
        "a FAIL expectation rows already break": err("CREATE MATERIALIZED VIEW bad5 (CONSTRAINT c CHECK (amount > 0)) AS SELECT id, amount FROM orders"),
        "dropping a view others follow": err("DROP MATERIALIZED VIEW silver"),
    }
    for what, e in say.items():
        checks[f"refused: {what}"] = bool(e) and ("GROUP BY view" in e or "expectation" in e or "followed by" in e)
    time.sleep(1)
    stop.set()
    for t in threads:
        t.join()
    def same():
        out = {}
        base = "FROM orders WHERE status <> 'test' AND amount > 0"
        want = {
            "silver": (f"SELECT count(*) AS n, sum(doubled) AS d FROM silver", f"SELECT count(*) AS n, sum(amount * 2) AS d {base}"),
            "gold": ("SELECT region, buyer, n, total FROM gold ORDER BY region, buyer NULLS FIRST", f"SELECT region, buyer, count(*) AS n, sum(amount) AS total {base} GROUP BY region, buyer ORDER BY region, buyer NULLS FIRST"),
            "platinum": ("SELECT region, n, total FROM platinum ORDER BY region", f"SELECT region, count(*) AS n, sum(amount) AS total {base} GROUP BY region ORDER BY region"),
            "big": ("SELECT count(*) AS n, sum(doubled) AS d FROM big", f"SELECT count(*) AS n, sum(amount * 2) AS d {base} AND amount * 2 > 1000"),
            "strict": ("SELECT count(*) AS n, sum(amount) AS s FROM strict", "SELECT count(*) AS n, sum(amount) AS s FROM orders"),
        }
        for view, (got, expected) in want.items():
            for port in ports:
                e = q(expected, port)
                out[f"{view} == its query over orders (:{port})"] = until(lambda: q(got, port), e, secs=30) == e
        return out
    checks.update(same())
    sent_all = sum(sent)
    checks["every row sent is there once, beside the refused INSERT"] = q("SELECT count(*) AS n FROM orders")[0]["n"] == sent_all
    ex = {r["expectation"]: r for r in q("SELECT * FROM pondra.expectations ORDER BY view, expectation")}
    bad = q("SELECT count(*) FILTER (WHERE amount <= 0) AS neg, count(*) FILTER (WHERE buyer IS NULL) AS nobuyer FROM orders WHERE status <> 'test'")[0]
    checks["pondra.expectations counts each expectation's failed rows, filled and streamed"] = ex.get("positive", {}).get("failed_rows") == bad["neg"] and ex.get("has_buyer", {}).get("failed_rows") == bad["nobuyer"] and ex.get("small", {}).get("on_violation") == "fail"
    pipe = {r["name"]: r for r in q("SELECT name, follows, kind FROM pondra.flows")}
    checks["pondra.flows: orders → silver → gold → platinum, silver → big"] = [pipe.get(n, {}).get("follows") for n in ("silver", "gold", "platinum", "big")] == ["orders", "silver", "gold", "silver"] and pipe["gold"]["kind"] == "aggregate"
    # Changes of orders flow down the flow, in the same commit.
    q("UPDATE orders SET amount = amount + 7 WHERE id % 10 = 0")
    q("UPDATE orders SET amount = -amount WHERE id % 17 = 0")  # (in and out of silver's expectation)
    q("DELETE FROM orders WHERE id % 13 = 0")
    checks.update({k + ", after UPDATE and DELETE": v for k, v in same().items()})
    checks["a view of a view made again after its flow is dropped from the end"] = all(err(f"DROP MATERIALIZED VIEW {v}") is None for v in ("platinum", "big", "gold", "silver"))
    # History per key (SCD type 2, ADR-036 §8): versions in any order, a delete ending a key.
    q("CREATE TABLE customer_changes (id BIGINT, name VARCHAR, city VARCHAR, op VARCHAR, at BIGINT)")
    q("INSERT INTO customer_changes VALUES (1, 'ann', 'paris', 'U', 1), (1, 'ann', 'rome', 'U', 3), (2, 'bob', 'nyc', 'U', 1)")
    q("CREATE MATERIALIZED VIEW customers_history WITH (history = 'id', sequence_by = 'at', delete_when = 'op = ''D''') "
      "AS SELECT id, name, city, op, at FROM customer_changes", ports[1])
    q("INSERT INTO customer_changes VALUES (3, 'cy', 'lima', 'U', 2)")
    q("INSERT INTO customer_changes VALUES (1, 'ann', 'oslo', 'U', 2)", ports[1])  # (late: between paris and rome)
    q("INSERT INTO customer_changes VALUES (2, 'bob', NULL, 'D', 5)")
    want_h = [{"id": 1, "city": "paris", "__start_at": 1, "__end_at": 2}, {"id": 1, "city": "oslo", "__start_at": 2, "__end_at": 3}, {"id": 1, "city": "rome", "__start_at": 3, "__end_at": None},
              {"id": 2, "city": "nyc", "__start_at": 1, "__end_at": 5}, {"id": 3, "city": "lima", "__start_at": 2, "__end_at": None}]
    hist = lambda port: [{k: r.get(k) for k in ("id", "city", "__start_at", "__end_at")} for r in q("SELECT id, city, __start_at, __end_at FROM customers_history ORDER BY id, __start_at", port)]  # (JSON leaves out NULLs)
    checks["a history view (SCD type 2): every version with __start_at and __end_at, a late one in its place, a delete ending its key, on every node"] = \
        all(until(lambda: hist(p), want_h, secs=20) == want_h for p in ports) and q("SELECT id, city FROM customers_history WHERE __end_at IS NULL ORDER BY id") == [{"id": 1, "city": "rome"}, {"id": 3, "city": "lima"}]
    checks["refused: a materialized view of a history view (its ends are worked out as it is read)"] = "history view" in (err("CREATE MATERIALIZED VIEW h2 AS SELECT id FROM customers_history") or "")
    # A view's or a task's plan is kept from one write to the next (src/fresh.rs, invariant 200):
    # nothing of the write it was made for stays in it, neither its count (DataFusion answers a
    # count(*) from exact statistics: 1, 1, 1, 1, 1) nor its time (now() is folded as it plans).
    q("CREATE TABLE ticks (id BIGINT, v BIGINT)")
    call(A.port, "POST", "/tasks/counted", json.dumps({"source": "ticks", "target": "counted", "sql": "SELECT count(*) AS n FROM ticks"}).encode())
    q("CREATE MATERIALIZED VIEW stamped AS SELECT id, now() AS seen FROM ticks")
    for k in range(1, 6):
        q("INSERT INTO ticks VALUES " + ", ".join(f"({i}, 1)" for i in range(k)))  # (one node: each keeps its own plans)
        time.sleep(0.3)
    checks["a kept plan keeps no write's count: a task counting each write's new rows adds up to every row"] = until(lambda: q("SELECT sum(n) AS n FROM counted")[0].get("n"), 15, secs=20) == 15
    checks["…nor its time: now() in a view differs from write to write"] = q("SELECT count(DISTINCT seen) AS t FROM stamped")[0]["t"] > 1
    info = {"sent": sent_all, "refused": (refused or "")[:200], "said": {k: (v or "")[:160] for k, v in say.items()}, "expectations": ex}
    a.kill(); b.kill()
    ok = all(checks.values())
    print(json.dumps({"flows": checks, "ok": ok, "info": info}, indent=1, default=str))
    if not ok:
        sys.exit(1)


def begin():
    """Transactions (ADR-036 §5) and Postgres's error codes (§4): BEGIN … COMMIT as one commit
    from HTTP (a session), Python (`with con.transaction()`) and the Postgres port, on the leader
    and on a follower; reads see their own writes, others don't until COMMIT; ROLLBACK; a failed
    statement fails the transaction (25P02); two transactions changing one row: the second is
    refused with 40001; transfers between accounts from 8 clients at once, retried on 40001, keep the
    total; a view follows every commit; constraints and errors with their SQLSTATE on every door."""
    import psycopg
    lake = new_lake()
    pga, pgb = A.port + 2000, A.port + 2001
    a = Node(lake, A.port, tier_secs=0.5, pg=f"127.0.0.1:{pga}").start()
    b = Node(lake, A.port + 1, tier_secs=0.5, pg=f"127.0.0.1:{pgb}").start()
    q = lambda s, port=A.port: sql(port, s)
    def http(port, body, session=None):
        c = http_client.HTTPConnection("127.0.0.1", port, timeout=60)
        c.request("POST", "/sql", body.encode(), {"x-pondra-session": session} if session else {})
        r = c.getresponse()
        data = r.read()
        return r.status, r.getheader("x-pondra-sqlstate"), (json.loads(data) if data[:1] in (b"{", b"[") else data.decode())
    q("CREATE TABLE accounts (id BIGINT, balance BIGINT CHECK (balance >= -1000))")
    q("CREATE TABLE history (account BIGINT, delta BIGINT)")
    q("CREATE TABLE prices (item VARCHAR PRIMARY KEY, price BIGINT)")
    q("INSERT INTO accounts SELECT value AS id, 1000 AS balance FROM range(1, 21)")
    q("INSERT INTO prices VALUES ('tea', 3), ('cake', 5)")
    q("CREATE MATERIALIZED VIEW total AS SELECT 1 AS k, sum(balance) AS s, count(*) AS n FROM accounts GROUP BY 1")
    checks = {}
    total = lambda port=A.port: q("SELECT sum(balance) AS s, count(*) AS n FROM accounts", port)[0]
    # Read your own writes; nobody else's until COMMIT; one commit.
    for port, where in ((A.port, "leader"), (A.port + 1, "follower")):
        s = f"s-{where}-{uuid.uuid4().hex[:8]}"
        http(port, "BEGIN", s)
        http(port, "UPDATE accounts SET balance = balance - 10 WHERE id = 1", s)
        http(port, "UPDATE accounts SET balance = balance + 10 WHERE id = 2", s)
        http(port, "INSERT INTO history VALUES (1, -10), (2, 10)", s)
        http(port, "INSERT INTO prices VALUES ('tea', 4)", s)
        mine = http(port, "SELECT id, balance FROM accounts WHERE id IN (1, 2) ORDER BY id", s)[2]
        theirs = q("SELECT id, balance FROM accounts WHERE id IN (1, 2) ORDER BY id", port)
        tea = http(port, "SELECT price FROM prices WHERE item = 'tea'", s)[2]
        before = q("SELECT count(*) AS n FROM history", port)[0]["n"]
        done = http(port, "COMMIT", s)
        after = q("SELECT id, balance, _version FROM accounts WHERE id IN (1, 2) ORDER BY id", port)
        hist = q("SELECT count(*) AS n, count(DISTINCT _version) AS v FROM history", port)[0]
        checks[f"on the {where}: a transaction reads its own writes, others don't see them until COMMIT"] = mine[0]["balance"] == theirs[0]["balance"] - 10 and tea == [{"price": 4}] and before == hist["n"] - 2 and done[0] == 200
        checks[f"on the {where}: COMMIT is one commit (every row it wrote, one _version)"] = len({r["_version"] for r in after}) == 1 and after[0]["balance"] + after[1]["balance"] == 2000 and q("SELECT price FROM prices WHERE item = 'tea'", port) == [{"price": 4}]
    # ROLLBACK, and a failed statement.
    s = "s-rollback-" + uuid.uuid4().hex[:8]
    http(A.port, "BEGIN", s); http(A.port, "DELETE FROM accounts", s); http(A.port, "ROLLBACK", s)
    checks["ROLLBACK leaves nothing"] = total()["n"] == 20
    # An UPDATE then an INSERT of one table: the UPDATE's rows carry more system columns than the
    # INSERT's, and COMMIT sends them as one stream (an append table and a keyed one).
    for t, key in (("ui", ""), ("uk", " PRIMARY KEY")):
        q(f"CREATE TABLE {t} (id INT{key}, name VARCHAR)"); q(f"INSERT INTO {t} VALUES (1, 'a'), (2, 'b')")
        s = f"s-{t}-" + uuid.uuid4().hex[:8]
        http(A.port, "BEGIN", s); http(A.port, f"UPDATE {t} SET name = 'x' WHERE id = 1", s); http(A.port, f"INSERT INTO {t} (id, name) VALUES (3, 'c')", s)
        end = http(A.port, "COMMIT", s)
        checks[f"an UPDATE then an INSERT of one table commits ({'keyed' if key else 'append'})"] = end[0] == 200 and q(f"SELECT id, name FROM {t} ORDER BY id") == [{"id": 1, "name": "x"}, {"id": 2, "name": "b"}, {"id": 3, "name": "c"}]
    s = "s-failed-" + uuid.uuid4().hex[:8]
    http(A.port, "BEGIN", s)
    http(A.port, "UPDATE accounts SET balance = balance + 1 WHERE id = 3", s)
    bad = http(A.port, "SELECT * FROM nope", s)
    then = http(A.port, "SELECT 1", s)
    end = http(A.port, "COMMIT", s)
    checks["a failed statement fails the transaction: 42P01, then 25P02 until it ends; COMMIT rolls it back"] = bad[1] == "42P01" and then[1] == "25P02" and end[2].get("transaction") == "rollback" and q("SELECT balance FROM accounts WHERE id = 3") == [{"balance": 1000}]
    # Two transactions change one row: the second to commit is refused, 40001.
    s1, s2 = "s-one-" + uuid.uuid4().hex[:8], "s-two-" + uuid.uuid4().hex[:8]
    http(A.port, "BEGIN", s1); http(A.port + 1, "BEGIN", s2)
    http(A.port, "UPDATE accounts SET balance = balance + 5 WHERE id = 4", s1)
    http(A.port + 1, "UPDATE accounts SET balance = balance + 7 WHERE id = 4", s2)
    first, second = http(A.port, "COMMIT", s1), http(A.port + 1, "COMMIT", s2)
    checks["two transactions change one row: the first commits, the second gets 40001"] = first[0] == 200 and second[1] == "40001" and q("SELECT balance FROM accounts WHERE id = 4") == [{"balance": 1005}]
    # A keyed row: a one-key UPDATE in a transaction (no planning), and one changed since the snapshot.
    s1, s2 = "s-k1-" + uuid.uuid4().hex[:8], "s-k2-" + uuid.uuid4().hex[:8]
    http(A.port, "BEGIN", s1); http(A.port + 1, "BEGIN", s2)
    http(A.port, "UPDATE prices SET price = price + 1 WHERE item = 'cake'", s1)
    mine = http(A.port, "SELECT price FROM prices WHERE item = 'cake'", s1)[2]
    http(A.port, "COMMIT", s1)
    early = http(A.port + 1, "UPDATE prices SET price = price + 10 WHERE item = 'cake'", s2)
    late = http(A.port + 1, "COMMIT", s2)
    checks["a keyed row: its one-key UPDATE read back in the transaction; changed since the snapshot, 40001 (at the UPDATE or the COMMIT)"] = \
        mine == [{"price": 6}] and (early[1] == "40001" or late[1] == "40001") and q("SELECT price FROM prices WHERE item = 'cake'") == [{"price": 6}]
    with psycopg.connect(f"host=127.0.0.1 port={pgb} user=u dbname=lake", autocommit=True) as c:
        got = [c.execute("SELECT price FROM prices WHERE item = %s", ("cake",)).fetchall(), c.execute("SELECT * FROM prices WHERE item = 'none'").fetchall(),
               [d.name for d in c.execute("SELECT * FROM prices WHERE item = 'tea'").description]]
        c.execute("UPDATE prices SET price = price * 2 WHERE item = 'tea'")
        got.append(c.execute("SELECT price FROM prices WHERE item = 'tea'").fetchall())
    checks["key lookups and one-key UPDATEs through the Postgres port, without planning: right answers"] = got == [[(6,)], [], ["item", "price"], [(8,)]]
    # Transfers from 8 clients at once through both Postgres ports, retried on 40001.
    stop, done, retried, errors = time.time() + 8, [0], [0], []
    def client(k):
        rng = random.Random(k)
        with psycopg.connect(f"host=127.0.0.1 port={(pga, pgb)[k % 2]} user=u dbname=lake") as c:
            while time.time() < stop:
                x, y, d = rng.randrange(1, 21), rng.randrange(1, 21), rng.randrange(1, 50)
                for _ in range(20):
                    try:
                        with c.cursor() as cur:
                            cur.execute(f"UPDATE accounts SET balance = balance - {d} WHERE id = {x}")
                            cur.execute(f"UPDATE accounts SET balance = balance + {d} WHERE id = {y}")
                            cur.execute(f"INSERT INTO history VALUES ({x}, {-d}), ({y}, {d})")
                        c.commit()
                        done[0] += 1
                        break
                    except psycopg.errors.SerializationFailure:
                        c.rollback(); retried[0] += 1
                    except Exception as e:
                        c.rollback(); errors.append(str(e)[:200]); break
    ts = [threading.Thread(target=client, args=(k,)) for k in range(8)]
    [t.start() for t in ts]; [t.join() for t in ts]
    h = q("SELECT count(*) AS n, sum(delta) AS s FROM history")[0]
    view = until(lambda: q("SELECT s, n FROM total"), [{"s": 20005, "n": 20}], secs=20)
    checks["8 clients' transfers, retried on 40001: the total kept, every transfer's history there"] = total() == {"s": 20005, "n": 20} and h["s"] == 0 and h["n"] == 4 + 2 * done[0] and not errors
    checks["…and the view following accounts == its query, after every commit"] = view == [{"s": 20005, "n": 20}]
    # Python: with con.transaction().
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "python"))
    import pondra
    con = pondra.connect(f"http://127.0.0.1:{A.port + 1}")
    with con.transaction():
        con.sql("UPDATE accounts SET balance = balance - 1 WHERE id = 5")
        con.sql("UPDATE accounts SET balance = balance + 1 WHERE id = 6")
    try:
        with con.transaction():
            con.sql("UPDATE accounts SET balance = balance - 5000 WHERE id = 7")
        caught = None
    except pondra.PondraError as e:
        caught = e.sqlstate
    checks["Python: with con.transaction(); a CHECK refused with 23514 rolls it back"] = caught == "23514" and total()["s"] == 20005 and q("SELECT balance FROM accounts WHERE id = 7") == q("SELECT balance FROM accounts WHERE id = 7", A.port + 1)
    # Error codes on every door.
    codes = {}
    for what, stmt in {"no table": "SELECT * FROM nope", "no column": "SELECT nope FROM accounts", "syntax": "SELEC 1", "check": "INSERT INTO accounts VALUES (99, -5000)", "divide": "SELECT 1 / 0"}.items():
        st = http(A.port, stmt)[1]
        try:
            with psycopg.connect(f"host=127.0.0.1 port={pga} user=u dbname=lake", autocommit=True) as c:
                c.execute(stmt)
            pg = None
        except Exception as e:
            pg = getattr(e, "sqlstate", None)
        codes[what] = (st, pg)
    want = {"no table": "42P01", "no column": "42703", "syntax": "42601", "check": "23514", "divide": "22012"}
    checks["SQLSTATE over HTTP (x-pondra-sqlstate) and the Postgres port, as Postgres's"] = all(codes[k] == (v, v) for k, v in want.items())
    with psycopg.connect(f"host=127.0.0.1 port={pga} user=u dbname=lake") as c:
        status = [c.info.transaction_status.name]
        c.execute("SELECT 1")
        status.append(c.info.transaction_status.name)
        c.rollback()
        status.append(c.info.transaction_status.name)
    checks["the Postgres port reports the transaction's status (psycopg's IDLE, INTRANS, IDLE)"] = status == ["IDLE", "INTRANS", "IDLE"]
    a.kill(); b.kill()
    ok = all(checks.values())
    print(json.dumps({"begin": checks, "ok": ok, "info": {"transfers": done[0], "retried": retried[0], "errors": errors[:3], "codes": codes, "status": status}}, indent=1, default=str))
    if not ok:
        sys.exit(1)


def objects():
    """Round 31's SQL: `CREATE` of what is there (42P07, `IF NOT EXISTS`, `OR REPLACE`) for every
    kind of object; `ALTER MATERIALIZED VIEW … DETACH`; a session's `SET`, `SHOW`, `RESET`,
    `PREPARE`, `EXECUTE` (and a script's own session); Spark's functions, and Spark's for shared
    names in its dialect; MERGE's mistakes refused; `n * INTERVAL`; a notebook's `%%sql df <<` cell
    a frame in its Python, and a SQL cell reading a Python table by name."""
    lake = new_lake()
    here = os.path.dirname(os.path.abspath(__file__))
    nb_dir = os.path.join(lake, "files", "nb")
    node = Node(lake, A.port, env={"PYTHONPATH": os.path.join(here, "..", "python"), "PONDRA_SECRET_KEY": "objects-key"}, python=sys.executable).start()
    q = lambda s, port=A.port: sql(port, s)
    def http(body, session=None):
        c = http_client.HTTPConnection("127.0.0.1", A.port, timeout=60)
        c.request("POST", "/sql", body.encode(), {"x-pondra-session": session} if session else {})
        r = c.getresponse()
        data = r.read()
        return r.status, r.getheader("x-pondra-sqlstate"), (json.loads(data) if data[:1] in (b"{", b"[") else data.decode())
    checks, info = {}, {}
    q("CREATE TABLE t (a BIGINT)")
    q("INSERT INTO t VALUES (1), (2)")
    again = http("CREATE TABLE t (a BIGINT)")
    as_again = http("CREATE TABLE t AS SELECT 9 AS a")
    quiet = q("CREATE TABLE IF NOT EXISTS t AS SELECT 9 AS a")
    checks["CREATE TABLE of one there: refused (42P07), AS SELECT adds nothing, IF NOT EXISTS leaves it"] = again[:2] == (500, "42P07") and as_again[1] == "42P07" \
        and quiet.get("exists") is True and q("SELECT count(*) AS n FROM t") == [{"n": 2}]
    q("CREATE OR REPLACE TABLE t AS SELECT 7 AS a")
    checks["CREATE OR REPLACE TABLE makes it anew"] = q("SELECT a FROM t") == [{"a": 7}]
    q("CREATE VIEW v AS SELECT a FROM t")
    q("CREATE VIEW IF NOT EXISTS v AS SELECT a + 1 AS a FROM t")
    kept = q("SELECT a FROM v")
    q("CREATE OR REPLACE VIEW v AS SELECT a + 1 AS a FROM t")
    both = http("CREATE OR REPLACE VIEW IF NOT EXISTS v AS SELECT 1")
    checks["views: IF NOT EXISTS leaves it, OR REPLACE replaces it, both at once refused"] = kept == [{"a": 7}] and q("SELECT a FROM v") == [{"a": 8}] and both[0] == 500
    q("CREATE TABLE orders (id BIGINT, region VARCHAR, amount BIGINT)")
    q("INSERT INTO orders VALUES (1, 'eu', 10), (2, 'us', 20), (3, 'eu', 30)")
    q("CREATE MATERIALIZED VIEW eu AS SELECT id, amount FROM orders WHERE region = 'eu'")
    q("CREATE MATERIALIZED VIEW IF NOT EXISTS eu AS SELECT id FROM orders")
    same = q("SELECT count(*) AS n, sum(amount) AS s FROM eu")
    q("CREATE OR REPLACE MATERIALIZED VIEW eu AS SELECT id, amount * 2 AS amount FROM orders WHERE region = 'eu'")
    q("CREATE MATERIALIZED VIEW eu_big AS SELECT id FROM eu WHERE amount > 30")
    followed = http("CREATE OR REPLACE MATERIALIZED VIEW eu AS SELECT id, amount FROM orders")
    checks["materialized views: IF NOT EXISTS, OR REPLACE, refused while another follows"] = same == [{"n": 2, "s": 40}] \
        and q("SELECT sum(amount) AS s FROM eu") == [{"s": 80}] and followed[0] == 500 and "follow" in str(followed[2])
    q("CREATE FUNCTION twice(x BIGINT) RETURNS BIGINT RETURN x * 2")
    q("CREATE FUNCTION IF NOT EXISTS twice(x BIGINT) RETURNS BIGINT RETURN x * 3")
    q("CREATE TASK tick SCHEDULE '1 hour' AS SELECT 1")
    q("CREATE TASK IF NOT EXISTS tick SCHEDULE '1 hour' AS SELECT 2")
    checks["functions and tasks: IF NOT EXISTS leaves them"] = q("SELECT twice(5) AS x") == [{"x": 10}] and http("CREATE TASK tick SCHEDULE '1 hour' AS SELECT 3")[0] == 500
    # Every other kind alike (IF NOT EXISTS leaves one, OR REPLACE replaces it, both at once
    # refused), and OR REPLACE refused where it would drop what's inside
    q("CREATE MACRO plus1(x) AS x + 1")
    q("CREATE MACRO IF NOT EXISTS plus1(x) AS x + 2")
    macros = [q("SELECT plus1(1) AS x")]
    q("CREATE OR REPLACE MACRO plus1(x) AS x + 3")
    macros += [q("SELECT plus1(1) AS x"), http("CREATE OR REPLACE MACRO IF NOT EXISTS plus1(x) AS x")[0]]
    secret = "CREATE {}SECRET {}sec (TYPE http, BEARER_TOKEN 'b', SCOPE 'http://127.0.0.1:9/{}/')"
    q(secret.format("", "", "a"))
    secrets = [q(secret.format("", "IF NOT EXISTS ", "b")).get("unchanged"), http(secret.format("", "", "b"))[0]]
    q(secret.format("OR REPLACE ", "", "c"))
    secrets += [q("SELECT scope FROM secrets() WHERE name = 'sec'"), http(secret.format("OR REPLACE ", "IF NOT EXISTS ", "d"))[0]]
    call(A.port, "PUT", "/files/k/a.csv", b"a,b\n1,x\n")
    external = "CREATE {}EXTERNAL TABLE {}x1 {}STORED AS CSV LOCATION '" + call(A.port, "GET", "/objects")["files"] + "k/a.csv' OPTIONS ('format.has_header' 'true')"
    q(external.format("", "", "(a BIGINT, b VARCHAR) "))
    externals = [q(external.format("", "IF NOT EXISTS ", "(a VARCHAR, b VARCHAR) ")).get("unchanged"), http(external.format("", "", "(a VARCHAR, b VARCHAR) "))[0], q("SELECT a FROM x1")]
    q(external.format("OR REPLACE ", "", "(a VARCHAR, b VARCHAR) "))
    externals += [q("SELECT a FROM x1"), http(external.format("OR REPLACE ", "IF NOT EXISTS ", ""))[0]]
    t = f"objects-temp-{uuid.uuid4().hex[:8]}"
    http("CREATE TEMP TABLE tt (a BIGINT)", t)
    http("CREATE TEMP VIEW tv AS SELECT 1 AS a", t)
    http("CREATE TEMP SECRET ts (TYPE http, BEARER_TOKEN 'x', SCOPE 'http://127.0.0.1:9/t/')", t)
    temps = [http("CREATE TEMP TABLE IF NOT EXISTS tt (a BIGINT, b BIGINT)", t)[0], http("CREATE TEMP VIEW IF NOT EXISTS tv AS SELECT 2 AS a", t)[0],
             http("CREATE TEMP SECRET IF NOT EXISTS ts (TYPE http, SCOPE 'http://127.0.0.1:9/t/')", t)[2], http("SELECT a FROM tv", t)[2]]
    http("CREATE OR REPLACE TEMP VIEW tv AS SELECT 3 AS a", t)
    temps += [http("SELECT a FROM tv", t)[2]] + [http(s, t)[0] for s in ("CREATE OR REPLACE TEMP TABLE IF NOT EXISTS tt (a BIGINT)", "CREATE OR REPLACE TEMP VIEW IF NOT EXISTS tv AS SELECT 4 AS a",
                                                                    "CREATE OR REPLACE TEMP SECRET IF NOT EXISTS ts (TYPE http, SCOPE 'http://127.0.0.1:9/t/')", "CREATE TEMP SECRET ts (TYPE http, SCOPE 'http://127.0.0.1:9/t/')")]
    kept = [http(f"CREATE OR REPLACE {k}") for k in ("SCHEMA s1", "DATABASE d1", "USER u1 PASSWORD 'pw-0123456789'", "ROLE r1")]
    info.update({"macros": macros, "secrets": secrets, "externals": externals, "temps": temps, "kept": [str(k[2])[:160] for k in kept]})
    checks["macros, secrets, external tables, a session's temporary tables, views and secrets: IF NOT EXISTS, OR REPLACE, both refused"] = \
        macros == [[{"x": 2}], [{"x": 4}], 500] and secrets == [True, 500, [{"scope": "http://127.0.0.1:9/c/"}], 500] \
        and externals == [True, 500, [{"a": 1}], [{"a": "1"}], 500] and temps == [200, 200, {"secret": "ts", "temporary": True, "exists": True}, [{"a": 1}], [{"a": 3}], 500, 500, 500, 500]
    checks["OR REPLACE of a schema, a database, a user or a role: refused, saying what it would drop"] = all(k[0] == 500 for k in kept) \
        and all("everything in it" in str(k[2]) for k in kept[:2]) and all("rights given to it" in str(k[2]) for k in kept[2:])
    # DETACH: rows stay, the flow stops, what follows keeps following the table
    q("ALTER MATERIALIZED VIEW eu DETACH")
    q("INSERT INTO orders VALUES (4, 'eu', 50)")
    q("INSERT INTO eu VALUES (9, 100)")
    q("CREATE MATERIALIZED VIEW sums AS SELECT region, sum(amount) AS s FROM orders GROUP BY region")
    q("CREATE MATERIALIZED VIEW w AS SELECT region, count(*) AS n FROM orders GROUP BY region")
    q("ALTER MATERIALIZED VIEW sums DETACH")
    q("INSERT INTO orders VALUES (5, 'us', 1)")
    info["detached"] = [q("SELECT id FROM eu ORDER BY id"), q("SELECT id FROM eu_big ORDER BY id"), q("SELECT region, s FROM sums ORDER BY region"), q("SELECT region, n FROM w ORDER BY region"), str(http("ALTER MATERIALIZED VIEW t DETACH")[2])[:200]]
    checks["DETACH: its rows stay a table, the flow stops, it takes INSERTs, what follows it keeps following"] = \
        sorted(r["id"] for r in q("SELECT id FROM eu")) == [1, 3, 9] and sorted(r["id"] for r in q("SELECT id FROM eu_big")) == [3, 9] \
        and q("SELECT s FROM sums WHERE region = 'us'") == [{"s": 20}] and q("SELECT n FROM w WHERE region = 'us'") == [{"n": 2}] \
        and http("ALTER MATERIALIZED VIEW t DETACH")[0] == 500
    # A view fed by a topic (a Pondra node's Kafka port): made again by OR REPLACE it fills again
    # (its old offsets forgotten); DETACH stops its reading and keeps its rows as a table
    other, pk = new_lake(), A.port + 70
    them = Node(other, A.port + 5, kafka=f"127.0.0.1:{pk}").start()
    try:
        call(A.port + 5, "POST", "/sql", b"CREATE TABLE ticks (id BIGINT, v BIGINT)")
        call(A.port + 5, "POST", "/sql", ("INSERT INTO ticks VALUES " + ", ".join(f"({i}, {i * 2})" for i in range(10))).encode())
        topic = f"'kafka://127.0.0.1:{pk}/ticks'"
        q(f"CREATE SECRET ticks_k (TYPE kafka, SECURITY_PROTOCOL 'PLAINTEXT', SCOPE 'kafka://127.0.0.1:{pk}')")
        q(f"CREATE MATERIALIZED VIEW fed AS SELECT CAST(value->>'id' AS BIGINT) AS id FROM {topic}")
        first = until(lambda: q("SELECT count(*) AS n FROM fed"), [{"n": 10}], 30)
        q(f"CREATE OR REPLACE MATERIALIZED VIEW fed AS SELECT CAST(value->>'v' AS BIGINT) AS v FROM {topic}")
        again = until(lambda: _try(lambda: q("SELECT count(*) AS n, sum(v) AS s FROM fed")), [{"n": 10, "s": 90}], 30)
        q("ALTER MATERIALIZED VIEW fed DETACH")
        call(A.port + 5, "POST", "/sql", b"INSERT INTO ticks VALUES (10, 20), (11, 22)")
        time.sleep(8)  # (a feed takes new records within its loop's 5 s)
        q("INSERT INTO fed VALUES (1000)")
        detached = q("SELECT count(*) AS n, sum(v) AS s FROM fed")
    finally:
        them.kill()
    info["fed"] = [first, again, detached]
    checks["a view fed by a topic: OR REPLACE fills it again; DETACH stops its reading, its rows a table that takes INSERTs"] = \
        first == [{"n": 10}] and again == [{"n": 10, "s": 90}] and detached == [{"n": 11, "s": 1090}]
    # A session's settings and prepared statements; a script's own session
    s = f"objects-{uuid.uuid4().hex[:8]}"
    http("SET TIME ZONE '+08:00'", s)
    zoned = http("SELECT TIMESTAMPTZ '2026-10-01T00:00:00Z' AS t", s)[2]
    other = q("SELECT TIMESTAMPTZ '2026-10-01T00:00:00Z' AS t")
    shown = http("SHOW datafusion.execution.time_zone", s)[2]
    http("RESET ALL", s)
    http("PREPARE big (BIGINT) AS SELECT id FROM orders WHERE amount > $1 ORDER BY id", s)
    executed = http("EXECUTE big (25)", s)[2]
    script = q("SET TIME ZONE '+02:00'; SELECT TIMESTAMPTZ '2026-10-01T00:00:00Z' AS t")
    checks["SET, SHOW, RESET, PREPARE, EXECUTE: the session's; a script is a session of its own"] = zoned == [{"t": "2026-10-01T08:00:00+08:00"}] \
        and other == [{"t": "2026-10-01T00:00:00Z"}] and shown == [{"name": "datafusion.execution.time_zone", "value": "+08:00"}] \
        and executed == [{"id": 3}, {"id": 4}] and script == [{"t": "2026-10-01T02:00:00+02:00"}] and http("SET aa.bb = 1", s)[0] == 500
    spark = q("SELECT format_string('%s x %d', 'tea', 3) AS f, pmod(-7, 3) AS m, arrow_typeof(floor(CAST(1.5 AS DOUBLE))) AS fl")
    http("SET datafusion.sql_parser.dialect = 'spark'", s)
    floor_spark = http("SELECT arrow_typeof(floor(CAST(1.5 AS DOUBLE))) AS t, arrow_typeof(floor(1.5)) AS d", s)[2]  # (Spark's: a DOUBLE's floor a BIGINT, a DECIMAL's a DECIMAL of scale 0)
    info["spark"] = [spark, floor_spark]
    checks["Spark's functions; Spark's floor in its dialect (DataFusion's otherwise)"] = spark == [{"f": "tea x 3", "m": 2, "fl": "Float64"}] and floor_spark == [{"t": "Int64", "d": "Decimal128(2, 0)"}]
    refused = [http(m)[0] for m in ("MERGE INTO orders USING t ON orders.id = t.a",
                                     "MERGE INTO orders USING t ON orders.id = t.a WHEN MATCHED THEN UPDATE SET amount = 1, amount = 2",
                                     "MERGE INTO orders o USING t AS o ON o.id = o.a WHEN MATCHED THEN DELETE")]
    interval = q("SELECT 3 * INTERVAL '37 seconds' AS a, INTERVAL '1 month' * 2.5 AS b")
    checks["MERGE's mistakes refused; n * INTERVAL as Postgres"] = refused == [500, 500, 500] and interval == [{"a": "1 mins 51.000000000 secs", "b": "2 mons 15 days"}]
    # Other engines' spellings of what Pondra does, and what a write may say that it doesn't, refused
    # (each was read and then dropped: the table had no partitions, OR IGNORE replaced the row)
    q("CREATE TABLE laid (ts TIMESTAMP, user_id BIGINT, url VARCHAR) PARTITION BY days(ts) CLUSTER BY (user_id, url)")
    q("CREATE TABLE laid2 (ts TIMESTAMP, user_id BIGINT) PARTITIONED BY (user_id)")
    q("ALTER TABLE laid2 CLUSTER BY (ts)")
    shape = {o["name"]: (o.get("partition"), o.get("cluster")) for o in call(A.port, "GET", "/objects")["objects"] if o["name"] in ("laid", "laid2")}
    twice = http("CREATE TABLE laid3 (ts TIMESTAMP) WITH (cluster_by = 'ts') CLUSTER BY (ts)")
    engine = http("CREATE TABLE laid4 (ts TIMESTAMP) ENGINE = MergeTree")
    info["laid"] = [shape, twice[2], engine[2]]
    checks["PARTITION BY, PARTITIONED BY and CLUSTER BY are the table's options; given twice, or ENGINE =, refused"] = \
        shape == {"laid": ("day(ts)", ["user_id", "url"]), "laid2": ("user_id", ["ts"])} and twice[0] == 500 and engine[0] == 500
    # A table's layout as clauses, in any order and beside WITH (the SQL review's 1A, `layout.rs`)
    q("CREATE TABLE lay_s (user_id BIGINT PRIMARY KEY, seen TIMESTAMP, page VARCHAR) CLUSTER BY (page) TTL seen + INTERVAL '1 hour' "
      "PARTITION BY day(seen) SEQUENCE BY seen WITH (publish = (delta, iceberg), retention = '7 days')")
    q("CREATE TABLE lay_t (region VARCHAR PRIMARY KEY, amount DOUBLE MERGE sum, n BIGINT MERGE count)")
    q("INSERT INTO lay_t VALUES ('eu', 5.0, 1)")
    q("INSERT INTO lay_t VALUES ('eu', 3.0, 1)")
    q("ALTER TABLE lay_s TTL seen + INTERVAL '2 hours'")
    laid = {o["name"]: (o.get("partition"), o.get("cluster"), o.get("order_by"), o.get("ttl_secs"), o.get("merge"), o.get("publish"))
            for o in call(A.port, "GET", "/objects")["objects"] if o["name"] in ("lay_s", "lay_t")}
    totals = q("SELECT region, amount, n FROM lay_t")
    wrong = [http(s)[0] for s in ("CREATE TABLE lay_x (id BIGINT PRIMARY KEY, ts TIMESTAMP) TTL ts + INTERVAL '1 month'",
                                   "CREATE TABLE lay_y (id BIGINT PRIMARY KEY, ts TIMESTAMP) SEQUENCE BY ts WITH (order_by = 'ts')",
                                   "CREATE TABLE lay_z (id BIGINT PRIMARY KEY, ts TIMESTAMP) SEQUENCE BY id")]
    info["layout"] = [laid, totals, wrong]
    checks["SEQUENCE BY, TTL and MERGE sum are the table's options, in any order beside WITH; ALTER TABLE … TTL; mistakes refused"] = \
        laid == {"lay_s": ("day(seen)", ["page"], "seen", ["seen", 7200], {}, ["delta", "iceberg"]),
                 "lay_t": (None, [], None, None, {"amount": "sum", "n": "count"}, [])} \
        and totals == [{"region": "eu", "amount": 8.0, "n": 2}] and wrong == [500, 500, 500]
    q("CREATE TABLE keyed (id BIGINT PRIMARY KEY, v VARCHAR)")
    q("INSERT INTO keyed VALUES (1, 'a'), (2, 'b')")
    q("INSERT OR IGNORE INTO keyed VALUES (1, 'x'), (3, 'c')")
    q("INSERT IGNORE INTO keyed VALUES (2, 'x')")
    q("INSERT OR REPLACE INTO keyed VALUES (2, 'B'), (4, 'd')")
    upserted = q("SELECT id, v FROM keyed ORDER BY id")
    refused = [http(s)[0] for s in ("INSERT OR ABORT INTO keyed VALUES (5, 'e')", "INSERT OVERWRITE TABLE keyed SELECT 6, 'f'",
                                    "INSERT INTO keyed VALUES (7, 'g') RETURNING id", "UPDATE keyed SET v = 'h' WHERE id = 1 RETURNING id")]
    info["keyed"] = [upserted, refused]
    checks["INSERT OR IGNORE, INSERT IGNORE and INSERT OR REPLACE as ON CONFLICT; OR ABORT, OVERWRITE and RETURNING refused"] = \
        upserted == [{"id": 1, "v": "a"}, {"id": 2, "v": "B"}, {"id": 3, "v": "c"}, {"id": 4, "v": "d"}] and refused == [500, 500, 500, 500] \
        and q("SELECT count(*) AS n FROM keyed") == [{"n": 4}]
    q("CREATE TABLE past (a BIGINT)")
    q("INSERT INTO past VALUES (1)")
    first = q("SELECT max(_version) AS v FROM past")[0]["v"]
    q("INSERT INTO past VALUES (2), (3)")
    then = [q(f"SELECT count(*) AS n FROM {w}") for w in (f"past VERSION AS OF {first}", f"past VERSION AS OF ({first})",
                                                          "past TIMESTAMP AS OF (now())", "past FOR SYSTEM_TIME AS OF (now())")]
    joined = q(f"SELECT count(*) AS n FROM past n JOIN past VERSION AS OF {first} w ON n.a = w.a")
    checks["VERSION AS OF, TIMESTAMP AS OF, FOR SYSTEM_TIME AS OF: a table's past, as AT (…) reads it"] = \
        then == [[{"n": 1}], [{"n": 1}], [{"n": 3}], [{"n": 3}]] and joined == [{"n": 1}]
    refreshed = http("REFRESH MATERIALIZED VIEW w")
    not_one = [http(f"REFRESH MATERIALIZED VIEW {t}")[0] for t in ("orders", "eu")]  # (eu is a table since its DETACH)
    analyzed = [http(s)[0] for s in ("ANALYZE", "ANALYZE orders")]
    checks["REFRESH MATERIALIZED VIEW and ANALYZE taken (nothing to do); REFRESH of a table refused"] = refreshed[0] == 200 and not_one == [500, 500] and analyzed == [200, 200]
    # CREATE TABLE … LIKE takes the columns (it made a table of none); SHOW lists every kind
    q("CREATE TABLE liked LIKE orders")
    cols = lambda t: [(r["column_name"], r["data_type"]) for r in q(f"DESCRIBE {t}")]
    shown = {w: http(f"SHOW {w}")[0] for w in ("SCHEMAS", "DATABASES", "SECRETS", "USERS", "ROLES", "GRANTS")}
    checks["CREATE TABLE … LIKE has the other's columns; SHOW SCHEMAS, DATABASES, SECRETS, USERS, ROLES, GRANTS list"] = \
        cols("liked") == cols("orders") and q("SELECT count(*) AS n FROM liked") == [{"n": 0}] and set(shown.values()) == {200} \
        and any({"lake": d["name"], "name": "public"} in q("SHOW SCHEMAS") for d in q("SHOW DATABASES"))
    # A notebook run: `%%sql df <<` is a frame in its Python; a SQL cell reads a Python table
    cell = lambda src, kind="code": {"cell_type": kind, "metadata": {}, "source": src, "outputs": [], "execution_count": None}
    nb = {"cells": [cell("%%sql eu_rows <<\nSELECT id, amount FROM orders WHERE region = 'eu'"), cell("import pandas as pd\ntargets = pd.DataFrame({'region': ['eu', 'us'], 'target': [3, 1]})\nn = len(eu_rows.to_pandas())"),
                    cell("%%sql\nSELECT o.region, count(*) AS n, t.target FROM orders o JOIN targets t USING (region) GROUP BY o.region, t.target ORDER BY o.region"),
                    cell("[{'eu': n}]")], "metadata": {}, "nbformat": 4, "nbformat_minor": 5}
    call(A.port, "PUT", "/files/nb/check.ipynb", json.dumps(nb).encode())
    ran = q("CALL run('nb/check.ipynb')")
    joined = None
    try:
        nb["cells"] = nb["cells"][:3]
        call(A.port, "PUT", "/files/nb/joined.ipynb", json.dumps(nb).encode())
        joined = q("CALL run('nb/joined.ipynb')")
    except Exception as e:
        info["joined"] = str(e)[:300]
    checks["a notebook's %%sql df << cell is a frame in its Python; a SQL cell reads a Python table by name"] = ran == [{"eu": 3}] \
        and joined == [{"region": "eu", "n": 3, "target": 3}, {"region": "us", "n": 2, "target": 1}]
    info.update({"ran": ran, "joined": joined})
    node.kill()
    ok = all(checks.values())
    print(json.dumps({"objects": checks, "ok": ok, "info": info}, indent=1, default=str))
    if not ok:
        sys.exit(1)


def registry():
    """The statement registry (ADR-049): every kind of object in `pondra.objects` with its comment and
    definition; `SHOW CREATE` of each kind runs again to the same object; `COMMENT ON` every kind,
    following a rename and gone with a drop; `CREATE OR ALTER TABLE` makes, adds, widens, takes away
    options and refuses what would lose rows; `GET /kinds`."""
    lake = new_lake()
    node = Node(lake, A.port).start()
    q = lambda s: sql(A.port, s)
    def http(body):
        c = http_client.HTTPConnection("127.0.0.1", A.port, timeout=60)
        c.request("POST", "/sql", body.encode())
        r = c.getresponse()
        data = r.read()
        return r.status, data.decode()[:400]
    checks, info = {}, {}
    made = [
        ("schema", "sales", "CREATE SCHEMA sales"),
        ("table", "sales.orders", "CREATE TABLE sales.orders (id BIGINT PRIMARY KEY, region VARCHAR NOT NULL, amount DECIMAL(12, 2) DEFAULT 0, at TIMESTAMP, "
                                  "tags VARCHAR[], CONSTRAINT positive CHECK (amount >= 0)) CLUSTER BY (region) SEQUENCE BY at TTL at + INTERVAL '2 days' WITH (retention = '3 days')"),
        ("table", "clicks", "CREATE TABLE clicks (ts TIMESTAMP, page VARCHAR, \"User\" BIGINT, \"order\" INT) PARTITION BY day(ts)"),
        ("table", "totals", "CREATE TABLE totals (region VARCHAR PRIMARY KEY, amount DOUBLE MERGE sum, n BIGINT MERGE count)"),
        ("view", "eu_orders", "CREATE VIEW eu_orders AS SELECT id, amount FROM sales.orders WHERE region = 'eu'"),
        ("materialized view", "pages", "CREATE MATERIALIZED VIEW pages (CONSTRAINT some_page EXPECT (page IS NOT NULL) ON VIOLATION DROP ROW) AS SELECT ts, page FROM clicks"),
        ("materialized view", "per_page", "CREATE MATERIALIZED VIEW per_page AS SELECT page, count(*) AS n FROM clicks GROUP BY page"),
        ("materialized view", "per_minute", "CREATE MATERIALIZED VIEW per_minute WITH (window = 'minute', size_secs = 60, lateness_secs = 5) AS SELECT date_bin(INTERVAL '1 minute', ts) AS minute, count(*) AS n FROM clicks GROUP BY 1"),
        ("macro", "twice", "CREATE MACRO twice(x) AS x * 2"),
        ("function", "net", "CREATE FUNCTION net(x DOUBLE, rate DOUBLE DEFAULT 0.2) RETURNS DOUBLE IMMUTABLE RETURN x * (1 - rate)"),
        ("table function", "big_orders", "CREATE FUNCTION big_orders(least DOUBLE) RETURNS TABLE (id BIGINT, amount DOUBLE) LANGUAGE sql AS $$ SELECT id, CAST(amount AS DOUBLE) FROM sales.orders WHERE amount > least $$"),
        ("procedure", "note_click", "CREATE PROCEDURE note_click(p VARCHAR) LANGUAGE sql AS $$ INSERT INTO clicks VALUES (now(), p, 1, 1); $$"),
        ("task", "tidy", "CREATE TASK tidy SCHEDULE '1 hour' WITH (retries = 2, timeout = '10 minutes') AS CALL note_click('tidy')"),
        ("task", "after_tidy", "CREATE TASK after_tidy AFTER tidy WHEN 1 = 1 AS SELECT 1"),
        ("role", "analyst", "CREATE ROLE analyst"),
    ]
    for _, _, stmt in made:
        q(stmt)
    q("ALTER TASK tidy SUSPEND")
    word = lambda k: {"macro": "FUNCTION", "table function": "FUNCTION"}.get(k, k.upper())
    comments = {"schema": "sales", "table": "sales.orders", "view": "eu_orders", "materialized view": "per_page", "function": "net",
                "procedure": "note_click", "task": "tidy", "role": "analyst"}
    for k, n in comments.items():
        q(f"COMMENT ON {k.upper()} {n} IS 'about {n}'")
    q("COMMENT ON COLUMN sales.orders.amount IS $$in euros, it's net$$")
    q("COMMENT ON TABLE clicks IS 'gone soon'")
    q("COMMENT ON TABLE clicks IS NULL")
    listed = {(r["kind"], r.get("schema"), r["name"]): r for r in q("SELECT * FROM pondra.objects")}
    info["listed"] = sorted(f"{k[0]}:{k[1]}.{k[2]}" for k in listed)
    want = {(k, n.split(".")[0] if "." in n else (None if k in ("schema", "role") else "public"), n.split(".")[-1]) for k, n, _ in made}
    checks["pondra.objects lists every kind, each with its comment"] = want <= set(listed) \
        and all(listed[(k, n.split(".")[0] if "." in n else (None if k in ("schema", "role") else "public"), n.split(".")[-1])].get("comment") == f"about {n}" for k, n in comments.items()) \
        and listed[("table", "public", "clicks")].get("comment") is None
    # SHOW CREATE, dropped, run again: the same definition and comments
    show = lambda k, n: q(f"SHOW CREATE {word(k)} {n}")[0]["definition"]
    before = {(k, n): show(k, n) for k, n, _ in made}
    info["shown"] = {f"{k} {n}": v for (k, n), v in before.items()}
    drops = {"schema": None, "table": "DROP TABLE", "view": "DROP VIEW", "materialized view": "DROP MATERIALIZED VIEW", "macro": "DROP MACRO", "function": "DROP FUNCTION",
             "table function": "DROP FUNCTION", "procedure": "DROP PROCEDURE", "task": "DROP TASK", "role": "DROP ROLE"}
    for k, n, _ in reversed(made):
        if drops[k] and k != "table":
            q(f"{drops[k]} {n}")
    for k, n, _ in reversed(made):
        if k == "table":
            q(f"DROP TABLE {n} PURGE")
    after_drop = [r["name"] for r in q("SELECT name FROM pondra.objects WHERE comment IS NOT NULL")]
    for k, n, _ in made:
        for stmt in [s for s in before[(k, n)].split(";\n") if k != "schema" or not s.startswith("CREATE")]:
            q(stmt.rstrip(";"))
    again = {(k, n): show(k, n) for k, n, _ in made}
    info["differs"] = {f"{k} {n}": [before[(k, n)], again[(k, n)]] for k, n, _ in made if before[(k, n)] != again[(k, n)]}
    checks["SHOW CREATE of every kind, run again after a drop, makes the same object, comments and all; a drop takes its comments"] = \
        not info["differs"] and after_drop == ["sales"] and "COMMENT ON COLUMN sales.orders.amount IS 'in euros, it''s net'" in again[("table", "sales.orders")]
    q("ALTER TABLE sales.orders RENAME TO orders_2025")
    moved = q("SELECT name, comment FROM pondra.objects WHERE kind = 'table' AND schema = 'sales'")
    checks["a renamed table keeps its comments, its columns' too"] = moved == [{"name": "orders_2025", "comment": "about sales.orders"}] \
        and "IS 'in euros, it''s net'" in show("table", "sales.orders_2025")
    # CREATE OR ALTER TABLE
    first = q("CREATE OR ALTER TABLE events (id BIGINT, kind VARCHAR) CLUSTER BY (kind) WITH (retention = '1 day')")
    q("INSERT INTO events VALUES (1, 'a')")
    same = q("CREATE OR ALTER TABLE events (id BIGINT, kind VARCHAR) CLUSTER BY (kind) WITH (retention = '1 day')")
    grown = q("CREATE OR ALTER TABLE events (id BIGINT, kind VARCHAR, note VARCHAR)")
    widened = q("CREATE OR ALTER TABLE events (id BIGINT, kind VARCHAR, note VARCHAR, n INT)")
    widened2 = q("CREATE OR ALTER TABLE events (id BIGINT, kind VARCHAR, note VARCHAR, n BIGINT)")
    refused = [http(s) for s in ("CREATE OR ALTER TABLE events (id BIGINT, kind VARCHAR)",
                                 "CREATE OR ALTER TABLE events (kind VARCHAR, id BIGINT, note VARCHAR, n BIGINT)",
                                 "CREATE OR ALTER TABLE events (id BIGINT PRIMARY KEY, kind VARCHAR, note VARCHAR, n BIGINT)",
                                 "CREATE OR ALTER TABLE events (id INT, kind VARCHAR, note VARCHAR, n BIGINT)",
                                 "CREATE OR ALTER TABLE events AS SELECT 1 AS id")]
    view_again = [q("CREATE OR ALTER VIEW recent AS SELECT id FROM events"), q("CREATE OR ALTER VIEW recent AS SELECT id, kind FROM events")]
    info["or_alter"] = [first, same, grown, widened, widened2, refused, view_again]
    o = call(A.port, "GET", "/objects")
    ev = next(t for t in o["tables"] if t["name"] == "events") if isinstance(o, dict) and "tables" in o else None
    checks["CREATE OR ALTER TABLE: made, then nothing to do, a column added, options taken away, a type widened; what would lose rows refused"] = \
        first.get("created") is True and same.get("unchanged") is True and "+ note" in grown.get("altered", []) \
        and any(a.startswith("CLUSTER BY") for a in grown.get("altered", [])) and any(a.startswith("retention") for a in grown.get("altered", [])) \
        and "n BIGINT" in widened2.get("altered", []) and [r[0] for r in refused] == [500] * 5 \
        and q("SELECT id, kind, note, n FROM events") == [{"id": 1, "kind": "a"}] \
        and q("SELECT * FROM recent") == [{"id": 1, "kind": "a"}] and "CLUSTER BY" not in show("table", "events")
    q("CREATE TABLE kv (k BIGINT PRIMARY KEY, v VARCHAR)")
    q("INSERT INTO kv VALUES (1, 'a'), (2, 'b')")
    kv = [q("CREATE OR ALTER TABLE kv (k BIGINT PRIMARY KEY, v VARCHAR, w INT)"), q("CREATE OR ALTER TABLE kv (k BIGINT PRIMARY KEY, v VARCHAR, w INT)")]
    q("INSERT INTO kv VALUES (3, 'c', 3)")
    q("DELETE FROM kv WHERE k = 1")
    info["kv"] = kv + [show("table", "kv")]
    checks["a keyed table: a column added, its _deleted its own (never in its definition), DELETE still works"] = kv[0].get("altered") == ["+ w"] \
        and kv[1].get("unchanged") is True and "_deleted" not in show("table", "kv") and q("SELECT k, w FROM kv ORDER BY k") == [{"k": 2}, {"k": 3, "w": 3}]
    kinds = call(A.port, "GET", "/kinds")
    checks["GET /kinds and pondra.kinds list every kind and its statements"] = {k["kind"] for k in kinds} >= {"table", "view", "materialized view", "function", "procedure", "task", "schema"} \
        and q("SELECT statements FROM pondra.kinds WHERE kind = 'table'")[0]["statements"].startswith("CREATE, CREATE OR ALTER")
    missing = [http(s)[0] for s in ("SHOW CREATE TABLE nothing_here", "COMMENT ON TABLE nothing_here IS 'x'", "COMMENT ON COLUMN events.nothing IS 'x'")]
    quiet = q("COMMENT IF EXISTS ON TABLE nothing_here IS 'x'")
    checks["what isn't there: refused by name, or nothing with IF EXISTS"] = missing == [500, 500, 500] and quiet.get("exists") is False
    node.kill()
    ok = all(checks.values())
    print(json.dumps({"registry": checks, "ok": ok, "info": info}, indent=1, default=str))
    if not ok:
        sys.exit(1)


def sparksql():
    """Spark SQL where PySpark code sends it (H8): `spark.sql(…)` read with Spark's grammar and
    turned into Pondra's SQL (`spark_sql('…')`): `"text"`, backticks, `LATERAL VIEW [OUTER]
    explode`, `explode`, `DIV`, `<=>`, `RLIKE`, Spark's answers for names both have; frames built on
    it, its sort, `args` and `{df}`; the SQL door's `spark_sql`, a view over it, and what it refuses.
    Pondra's own SQL answers as before."""
    lake = new_lake()
    node = Node(lake, A.port).start()
    db = _client(A.port)
    from pondra.spark import SparkSession, functions as F
    spark = SparkSession(db)
    q = lambda s: sql(A.port, s)
    checks, info = {}, {}
    q("CREATE TABLE people AS SELECT id, name, xs, CAST(d AS DOUBLE) AS d FROM (VALUES (1, 'ann', make_array(1, 2, 3), 2.5), (2, 'bob', make_array(4), -2.5), "
      "(3, 'cy', CAST(make_array() AS BIGINT[]), 0.5), (4, 'dee', CAST(NULL AS BIGINT[]), 7.5)) AS v(id, name, xs, d)")  # (Spark's floor of a DOUBLE is a BIGINT; 2.5 is a DECIMAL, whose floor has scale 0)
    rows = lambda df: [r.asDict() for r in df.collect()]
    got = {
        "text": rows(spark.sql('SELECT "hi" AS s, `name` FROM people WHERE name = "ann"')),
        "floor": rows(spark.sql("SELECT id, floor(d) AS f, ceil(d) AS c FROM people ORDER BY id")),
        "substring": rows(spark.sql("SELECT substring('Spark SQL', 5, 1) AS a, substring('Spark SQL', -3, 2) AS b, substr('Spark SQL', -3) AS c")),
        "ours": q("SELECT floor(CAST(2.5 AS DOUBLE)) AS f, substring('Spark SQL', -3, 2) AS b"),
        "lateral": rows(spark.sql("SELECT id, x FROM people LATERAL VIEW explode(xs) v AS x ORDER BY id, x")),
        "outer": rows(spark.sql("SELECT p.name, v.e FROM people p LATERAL VIEW OUTER explode(p.xs) v AS e WHERE p.id >= 2 ORDER BY p.name, v.e")),
        "two": rows(spark.sql("SELECT id, a, b FROM people LATERAL VIEW explode(xs) v AS a LATERAL VIEW explode(xs) w AS b WHERE id = 1 ORDER BY a, b"))[:4],
        "explode": rows(spark.sql("SELECT explode(xs) FROM people WHERE id = 1")),
        "operators": rows(spark.sql("SELECT 7 DIV 2 AS d, -7 DIV 2 AS e, NULL <=> NULL AS n, 'abc' RLIKE 'b' AS r, !(1 = 2) AS t")),
        "composed": rows(spark.sql("SELECT id, floor(d) AS f FROM people").where(F.col("f") > 0).orderBy("id")),
        "sorted": [r["id"] for r in rows(spark.sql("SELECT id FROM people ORDER BY id DESC"))],
        "sorted out": [r["name"] for r in rows(spark.sql("SELECT name FROM people ORDER BY d DESC"))] + [r["name"] for r in sql(A.port, "SELECT * FROM spark_sql('SELECT name FROM people ORDER BY d')")],
        "args": [r["id"] for r in rows(spark.sql("SELECT id FROM people WHERE id > :m ORDER BY id", args={"m": 2}))],
        "frame": rows(spark.sql("SELECT count(*) AS n FROM {d} WHERE `name` <> \"bob\"", d=spark.table("people"))),
        "door": q("SELECT f, arrow_typeof(f) AS t FROM spark_sql('SELECT floor(CAST(2.5 AS DOUBLE)) AS f')"),
    }
    q("CREATE VIEW floors AS SELECT * FROM spark_sql('SELECT id, floor(d) AS f FROM people')")
    got["view"] = q("SELECT sum(f) AS s FROM floors")
    refused = {
        "a statement that isn't a query": _raises_text(lambda: q("SELECT * FROM spark_sql('INSERT INTO people SELECT * FROM people')")),
        "posexplode": _raises_text(lambda: q("SELECT * FROM spark_sql('SELECT id, p, x FROM people LATERAL VIEW posexplode(xs) v AS p, x')")),
        "a lateral view over a join": _raises_text(lambda: q("SELECT * FROM spark_sql('SELECT * FROM people a JOIN people b ON a.id = b.id LATERAL VIEW explode(a.xs) v AS x')")),
    }
    info.update(got=got, refused=refused)
    checks['"text" is a string, `name` a name'] = got["text"] == [{"s": "hi", "name": "ann"}]
    checks["floor and ceil answer as Spark's (whole numbers, BIGINT), and Pondra's own SQL as before (a DOUBLE)"] = \
        got["floor"] == [{"id": 1, "f": 2, "c": 3}, {"id": 2, "f": -3, "c": -2}, {"id": 3, "f": 0, "c": 1}, {"id": 4, "f": 7, "c": 8}] \
        and all(type(r["f"]) is int for r in got["floor"]) and got["ours"][0]["f"] == 2.0 and isinstance(got["ours"][0]["f"], float)
    checks["substring and substr as Spark's (a negative start counts from the end), Pondra's as before"] = \
        got["substring"] == [{"a": "k", "b": "SQ", "c": "SQL"}] and got["ours"][0]["b"] != "SQ"
    checks["LATERAL VIEW explode: a row a value, none for an empty or NULL array"] = \
        got["lateral"] == [{"id": 1, "x": 1}, {"id": 1, "x": 2}, {"id": 1, "x": 3}, {"id": 2, "x": 4}]
    checks["LATERAL VIEW OUTER: a NULL for an empty or NULL array; the view's columns by its name, the table's by its alias"] = \
        got["outer"] == [{"name": "bob", "e": 4}, {"name": "cy", "e": None}, {"name": "dee", "e": None}]
    checks["two lateral views cross, not pair"] = got["two"] == [{"id": 1, "a": 1, "b": 1}, {"id": 1, "a": 1, "b": 2}, {"id": 1, "a": 1, "b": 3}, {"id": 1, "a": 2, "b": 1}]
    checks["explode in the select list: named col, as Spark names it"] = got["explode"] == [{"col": 1}, {"col": 2}, {"col": 3}]
    checks["DIV, <=>, RLIKE, !"] = got["operators"] == [{"d": 3, "e": -3, "n": True, "r": True, "t": True}]
    checks["a frame built on spark.sql composes; its ORDER BY is kept, by a column it leaves out too; args; {df}"] = \
        got["composed"] == [{"id": 1, "f": 2}, {"id": 4, "f": 7}] and got["sorted"] == [4, 3, 2, 1] \
        and got["sorted out"] == ["dee", "ann", "cy", "bob", "bob", "cy", "ann", "dee"] and got["args"] == [3, 4] and got["frame"] == [{"n": 3}]
    checks["spark_sql('…') from any door, and a view over it"] = got["door"] == [{"f": 2, "t": "Int64"}] and got["view"] == [{"s": 6}]
    checks["refused by name: " + ", ".join(refused)] = "takes a query" in refused["a statement that isn't a query"] and "posexplode" in refused["posexplode"] \
        and "over a join" in refused["a lateral view over a join"]
    node.kill()
    ok = all(checks.values())
    print(json.dumps({"sparksql": checks, "ok": ok, "info": info}, indent=1, default=str))
    if not ok:
        sys.exit(1)



def scripts():
    """Scripts that decide (ADR-045, phases 1 and 2): blocks with handlers, IF / ELSEIF / CASE, WHILE,
    REPEAT, LOOP and FOR over a query's rows (`$r.col`), LEAVE and ITERATE by label, RETURN, RAISE
    (P0001), PRINT and RAISE NOTICE as notices, ASSERT (P0004), EXECUTE IMMEDIATE … INTO … USING,
    CALL … INTO and IDENTIFIER(), from HTTP, Python, Postgres (simple and extended protocol) and in
    procedures; a block's DECLAREs its own; a script run again with its job writes once; a file's
    parameters leave out what its blocks bind. FOR … PARALLEL n runs passes at once, ASYNC runs a
    statement beside the script and AWAIT ALL waits (in a block too), `$h = ASYNC …` and `AWAIT $h` for
    one, `AWAIT 'id'` for a run pondra.start began: each with a copy of the variables, once per job.
    VALUES with a subquery in a row (DataFusion worked the rows out before the subquery ran)."""
    import psycopg
    lake = new_lake()
    here = os.path.dirname(os.path.abspath(__file__))
    pg = A.port + 10
    node = Node(lake, A.port, pg=f"127.0.0.1:{pg}", python="auto", env={"PYTHONPATH": os.path.join(here, "..", "python")}).start()
    h = {"x-pondra-session": "harness-scripts"}
    q = lambda s, hh=h, path="/sql": call(A.port, "POST", path, s.encode(), headers=hh)
    db = _client(A.port)
    def said(script):
        out = db.sql(script)
        return list(db.notices), (out.rows() if hasattr(out, "rows") else out)
    checks, info, got = {}, {}, {}
    q("CREATE TABLE orders (id BIGINT, day DATE, amount DOUBLE)")
    q("INSERT INTO orders VALUES (1, DATE '2026-09-29', 10), (2, DATE '2026-09-30', 20), (3, DATE '2026-09-30', 5)")
    # Branches, and what a script says.
    branch = """DECLARE $n = (SELECT count(*) FROM orders WHERE day = DATE '2026-09-30');
IF $n = 0 THEN
  PRINT 'nothing';
ELSEIF $n > 1 THEN
  PRINT 'many: ' || $n;
  RAISE NOTICE 'checked % of %', $n, (SELECT count(*) FROM orders);
ELSE
  PRINT 'one';
END IF;
CASE $n WHEN 1 THEN PRINT 'case one'; WHEN 2 THEN PRINT 'case two'; ELSE PRINT 'case else'; END CASE;
IF EXISTS (SELECT 1 FROM orders WHERE amount > 15) THEN PRINT 'big'; END IF;
SELECT $n AS n"""
    got["branch"] = said(branch)
    checks["IF … ELSEIF … ELSE and CASE take the branch their conditions say (a subquery too); PRINT and RAISE NOTICE are notices; the last statement answers"] = \
        got["branch"] == (["many: 2", "checked 2 of 3", "case two", "big"], [{"n": 2}])
    # Loops.
    loops = """DECLARE $i = 0; DECLARE $odd = 0;
outer: WHILE $i < 10 DO
  $i = $i + 1;
  IF $i % 2 = 0 THEN ITERATE outer; END IF;
  IF $i > 7 THEN LEAVE outer; END IF;
  $odd = $odd + $i;
END WHILE outer;
DECLARE $r = 0;
REPEAT $r = $r + 5; UNTIL $r >= 12 END REPEAT;
DECLARE $l = 0;
LOOP $l = $l + 1; IF $l = 3 THEN LEAVE; END IF; END LOOP;
DECLARE $sum = 0;
FOR o IN (SELECT id, amount FROM orders ORDER BY id) DO
  $sum = $sum + $o.amount * $o.id;
END FOR;
SELECT $i AS i, $odd AS odd, $r AS r, $l AS l, $sum AS sum"""
    got["loops"] = said(loops)
    checks["WHILE with ITERATE and LEAVE by label, REPEAT … UNTIL, LOOP … LEAVE, FOR over a query's rows ($o.amount)"] = \
        got["loops"][1] == [{"i": 9, "odd": 16, "r": 15, "l": 3, "sum": 65.0}]
    # Handlers, errors and their codes.
    handled = """BEGIN
  SELECT 1 / 0;
  PRINT 'not here';
EXCEPTION
  WHEN unique_violation THEN PRINT 'wrong one';
  WHEN division_by_zero THEN PRINT 'caught ' || $sqlstate;
END;
BEGIN
  RAISE 'too many rows for %: %', DATE '2026-09-30', 2;
EXCEPTION WHEN OTHERS THEN PRINT $sqlstate || ' ' || $error;
END;
SELECT getvariable('error') IS NULL AS gone"""
    got["handled"] = said(handled)
    checks["EXCEPTION WHEN takes the error its name or code says ($sqlstate, $error); RAISE formats with %; a handler's variables end with it"] = \
        got["handled"] == (["caught 22012", "P0001 too many rows for 2026-09-30: 2"], [{"gone": True}])
    reraised = _raises_text(lambda: q("BEGIN SELECT 1 / 0; EXCEPTION WHEN OTHERS THEN PRINT 'x'; RAISE; END"))
    asserted = _raises_text(lambda: q("ASSERT (SELECT count(*) FROM orders) = 4, 'orders: ' || (SELECT count(*) FROM orders)"))
    unclosed = _raises_text(lambda: q("IF true THEN SELECT 1;"))
    where = _raises_text(lambda: q("SELECT 1;\nIF true THEN\n  SELECT nope FROM orders;\nEND IF"))
    info["errors"] = {"reraised": reraised, "asserted": asserted, "unclosed": unclosed, "where": where}
    checks["RAISE; in a handler raises its error again; ASSERT fails with its text; a block not closed and an error inside one say where"] = \
        "Divide by zero" in reraised and "orders: 3" in asserted and "isn't closed" in unclosed and "statement 2" in where and "line 3" in where
    # Scopes.
    got["scopes"] = said("DECLARE $x = 1; BEGIN DECLARE $x = 2; PRINT $x; $x = 3; PRINT $x; END; PRINT $x; FOR r IN (SELECT 7 AS v) DO PRINT $r.v; END FOR")
    refused = _raises_text(lambda: q("SELECT 1; BEGIN DECLARE PARAMETER $p = 1; END"))
    got["sole block"] = q("BEGIN DECLARE PARAMETER $p = 1; SELECT $p AS p; END")  # (a script that is one block: its top is the script's)
    checks["a block's DECLARE is its own (the outer one back after it); a DECLARE PARAMETER inside a block is refused, unless the block is the whole script"] = got["sole block"] == [{"p": 1}] and \
        got["scopes"][0] == ["2", "3", "1", "7"] and "declared at its top" in refused
    # RETURN, dynamic SQL, names from values, CALL … INTO.
    got["return"] = said("DECLARE $n = 41; IF $n > 40 THEN RETURN $n + 1; END IF; PRINT 'not here'")
    q("CREATE PROCEDURE total(d DATE) LANGUAGE sql AS $$ SELECT sum(amount) AS t FROM orders WHERE day = $d $$")
    q("""CREATE PROCEDURE count_to(n BIGINT) LANGUAGE sql AS $$ DECLARE $i = 0; WHILE $i < $n DO $i = $i + 1; END WHILE; RETURN $i * 10; $$""")
    dynamic = """DECLARE $big = 0;
EXECUTE IMMEDIATE 'SELECT count(*) FROM orders WHERE amount > $1 AND day = $2' INTO $big USING 6, DATE '2026-09-30';
FOR t IN (SELECT 'a' AS name UNION ALL SELECT 'b' AS name ORDER BY name) DO
  EXECUTE IMMEDIATE 'CREATE TABLE IF NOT EXISTS ' || 'made_' || $t.name || ' (x BIGINT)';
  INSERT INTO IDENTIFIER('made_' || $t.name) VALUES (1);
END FOR;
CALL total(DATE '2026-09-30') INTO $t;
CALL count_to(4) INTO $c;
SELECT $big AS big, $t AS t, $c AS c, (SELECT count(*) FROM made_a) + (SELECT count(*) FROM made_b) AS made"""
    got["dynamic"] = said(dynamic)
    checks["RETURN ends the script with its value; EXECUTE IMMEDIATE … INTO … USING; IDENTIFIER() names a table; CALL … INTO (a procedure that loops and returns)"] = \
        got["return"] == ([], [{"result": 42}]) and got["dynamic"][1] == [{"big": 1, "t": 25.0, "c": 40, "made": 2}]
    # Exactly once: the same job, run again.
    q("CREATE TABLE ticks (n BIGINT)")
    tick = "DECLARE $i = 0; WHILE $i < 3 DO $i = $i + 1; INSERT INTO ticks VALUES ($i); END WHILE"
    q(tick, path="/sql?job=harness-ticks")
    q(tick, path="/sql?job=harness-ticks")
    got["ticks"] = q("SELECT count(*) AS n, sum(n) AS s FROM ticks")
    checks["a script run again with its job writes once (each statement's part its place and its loop's pass)"] = got["ticks"] == [{"n": 3, "s": 6}]
    # Postgres: the simple protocol splits a script by its blocks; the extended one takes a block whole.
    heard = []
    with psycopg.connect(f"host=127.0.0.1 port={pg} user=pondra dbname=pondra", autocommit=True, cursor_factory=psycopg.ClientCursor) as c:
        c.add_notice_handler(lambda d: heard.append(d.message_primary))
        cur = c.execute("DECLARE $k = 2; IF $k = 2 THEN PRINT 'simple ' || $k; END IF; SELECT $k AS k")
        simple = []
        while True:
            simple += cur.fetchall() if cur.description else []
            if not cur.nextset():
                break
    with psycopg.connect(f"host=127.0.0.1 port={pg} user=pondra dbname=pondra", autocommit=True) as c:
        c.add_notice_handler(lambda d: heard.append(d.message_primary))
        c.execute("BEGIN PRINT 'extended ' || %s; END", ("too",))
        try:
            c.execute("RAISE 'refused %', 1")
            code = None
        except psycopg.Error as e:
            code = e.sqlstate
    info["postgres"] = {"simple": simple, "heard": heard, "code": code}
    checks["over Postgres: a script with blocks (simple protocol), a block alone (extended), notices as NOTICE, RAISE as P0001"] = \
        simple == [(2,)] and heard == ["simple 2", "extended too"] and code == "P0001"
    # A file's parameters: what its blocks bind isn't one.
    call(A.port, "PUT", "/files/etl/loop.sql", b"DECLARE PARAMETER $limit BIGINT = 2;\nFOR o IN (SELECT id FROM orders) DO\n  IF $o.id > $limit THEN PRINT $o.id; END IF;\nEND FOR;\nBEGIN SELECT 1; EXCEPTION WHEN OTHERS THEN PRINT $error; END;\nCALL total($day) INTO $t;\n")
    got["parameters"] = q("SELECT name, required FROM pondra.parameters('etl/loop.sql')")
    checks["a file's parameters leave out a loop's row, a handler's $error and INTO's names ($day, used unset, still one)"] = \
        got["parameters"] == [{"name": "limit", "required": False}, {"name": "day", "required": True}]
    # Phase 2: passes at once, and statements beside the script.
    q("CREATE TABLE done (n BIGINT, w TEXT)")
    got["parallel"] = q("DECLARE $total = 0; FOR d IN (SELECT value AS v FROM generate_series(1, 8)) PARALLEL 4 DO INSERT INTO done VALUES ($d.v, 'p'); $total = $total + $d.v; END FOR; SELECT $total AS total, (SELECT count(*) FROM done) AS c, (SELECT sum(n) FROM done) AS s", {})
    checks["FOR … PARALLEL 4: every pass once, each with a copy of the variables (what one sets stays its own)"] = got["parallel"] == [{"total": 0, "c": 8, "s": 36}]
    got["parallel refused"] = [_raises_text(lambda s=s: q(s, {})) for s in (
        "FOR d IN (SELECT value AS v FROM generate_series(1, 6)) PARALLEL 3 DO IF $d.v = 4 THEN RAISE 'no %', $d.v; END IF; END FOR",
        "FOR d IN (SELECT 1 AS v) PARALLEL 2 DO LEAVE; END FOR",
        "FOR d IN (SELECT 1 AS v) PARALLEL 0 DO SELECT 1; END FOR")]
    checks["a PARALLEL pass that fails fails the loop, naming the pass; LEAVE can't end it; PARALLEL 0 refused"] = \
        "pass 4: no 4" in got["parallel refused"][0] and "can't end a PARALLEL loop" in got["parallel refused"][1] and "1 to 64" in got["parallel refused"][2]
    got["async"] = q("ASYNC INSERT INTO done VALUES (100, 'a');\nASYNC BEGIN INSERT INTO done VALUES (101, 'a'); END;\nAWAIT ALL;\nSELECT count(*) AS c FROM done WHERE w = 'a'", {})
    got["async failed"] = _raises_text(lambda: q("SELECT 1;\nASYNC SELECT 1/0;\nAWAIT ALL", {}))
    q("ASYNC INSERT INTO done VALUES (102, 'e')", {})
    got["async at the end"] = q("SELECT count(*) AS c FROM done WHERE w = 'e'")
    checks["ASYNC runs beside the script and AWAIT ALL waits for it; a failure is AWAIT ALL's, at its line; a script's end waits too"] = \
        got["async"] == [{"c": 2}] and "line 2: ASYNC" in got["async failed"] and "Divide by zero" in got["async failed"] and got["async at the end"] == [{"c": 1}]
    twice = "FOR d IN (SELECT value AS v FROM generate_series(1, 5) ORDER BY 1) PARALLEL 3 DO INSERT INTO done VALUES ($d.v, 'j'); END FOR; ASYNC INSERT INTO done VALUES (9, 'j')"
    q(twice, {}, path="/sql?job=harness-parallel")
    q(twice, {}, path="/sql?job=harness-parallel")
    got["twice"] = q("SELECT count(*) AS c FROM done WHERE w = 'j'")
    checks["PARALLEL passes and ASYNC statements run again with their job write once"] = got["twice"] == [{"c": 6}]
    q("CREATE PROCEDURE nap(s DOUBLE) LANGUAGE python AS $$\nimport time\ntime.sleep(s)\n$$")
    q("CALL nap(0.01)", {})
    timed = {}
    for how in ("", " PARALLEL 8"):
        t0 = time.time()
        q(f"FOR d IN (SELECT value AS v FROM generate_series(1, 8)){how} DO CALL nap(0.3); END FOR", {})
        timed[how.strip() or "one at a time"] = round(time.time() - t0, 2)
    info["eight 0.3 s calls, s"] = timed
    checks["eight 0.3 s calls PARALLEL 8 take under 0.6 of the time one at a time does"] = timed["PARALLEL 8"] < 0.6 * timed["one at a time"]
    # ASYNC beside what follows it, AWAIT ALL in a block, handles, and runs pondra.start started.
    t0 = time.time()
    q("ASYNC CALL nap(0.4);\nCALL nap(0.4)", {})
    timed["ASYNC beside a call"] = round(time.time() - t0, 2)
    got["await in a block"], _ = said("BEGIN\n  ASYNC BEGIN CALL nap(0.2); PRINT 'async done'; END;\n  AWAIT ALL;\n  PRINT 'after AWAIT ALL';\nEND")
    got["handles"], _ = said("$a = ASYNC BEGIN CALL nap(0.2); PRINT 'a done'; END;\n$b = ASYNC SELECT 1/0;\nAWAIT $a;\nPRINT 'after AWAIT $a';\n"
                             "BEGIN\n  AWAIT $b;\nEXCEPTION WHEN OTHERS THEN\n  PRINT 'b: ' || $sqlstate;\nEND")
    checks["ASYNC runs beside what follows; AWAIT ALL in a block waits; AWAIT $h waits for its own, raising its error once"] = (
        timed["ASYNC beside a call"] < 0.7 and got["await in a block"] == ["async done", "after AWAIT ALL"]
        and got["handles"] == ["a done", "after AWAIT $a", "b: 22012"])
    q("CREATE PROCEDURE oops() LANGUAGE sql AS $$ SELECT 1/0 $$")
    ran, failed = q("SELECT pondra.start('nap', 0.3) AS id")[0]["id"], q("SELECT pondra.start('oops') AS id")[0]["id"]
    q(f"AWAIT '{ran}'", {})
    got["awaited run"] = q(f"SELECT status FROM pondra.runs WHERE id = '{ran}'")
    got["awaited failure"] = _raises_text(lambda: q(f"AWAIT '{failed}'", {}))
    checks["AWAIT 'id' waits for a run pondra.start started, with its error if it failed"] = (
        got["awaited run"] == [{"status": "ok"}] and "Divide by zero" in got["awaited failure"])
    # VALUES with a subquery in a row (DataFusion worked the rows out before the subquery ran): in a
    # loop, a transaction, over Postgres with a parameter, and as a query.
    q("CREATE TABLE tally (k BIGINT, n BIGINT)")
    q("FOR d IN (SELECT value AS v FROM generate_series(1, 3)) DO INSERT INTO tally VALUES ($d.v, (SELECT count(*) FROM tally)); END FOR", {})
    q("BEGIN; INSERT INTO tally (n, k) VALUES ((SELECT max(n) FROM tally), 4); COMMIT", {})
    with psycopg.connect(f"host=127.0.0.1 port={pg} user=pondra dbname=pondra", autocommit=True) as c:
        c.execute("INSERT INTO tally VALUES (%s, (SELECT count(*) FROM tally))", (5,))
    got["tally"] = q("SELECT k, n FROM tally ORDER BY k")
    got["values"] = q("SELECT * FROM (VALUES ((SELECT count(*) FROM tally), 'rows'), (0, 'none')) ORDER BY column1 DESC")
    checks["VALUES with a subquery: in a loop, a transaction, over Postgres, as a query"] = (
        got["tally"] == [{"k": 1, "n": 0}, {"k": 2, "n": 1}, {"k": 3, "n": 2}, {"k": 4, "n": 2}, {"k": 5, "n": 4}]
        and got["values"] == [{"column1": 5, "column2": "rows"}, {"column1": 0, "column2": "none"}])
    # The ways people write it (the design review): a bare END closes any block, FOR $r, SET $x = and
    # :=, AWAIT alone waits for all; END of another kind where a block ends is refused.
    got["forgiving"] = q("""BEGIN
  DECLARE $t INT := 0;
  FOR $r IN (SELECT value AS v FROM generate_series(1, 4)) DO
    IF $r.v % 2 = 0 THEN SET $t = $t + $r.v; END;
  END;
  ASYNC INSERT INTO tally VALUES (100, 0);
  AWAIT;
  $t := $t * 10;
  SELECT $t AS t, (SELECT count(*) FROM tally WHERE k = 100) AS async;
END""", {})
    got["wrong end"] = _raises_text(lambda: q("BEGIN IF true THEN SELECT 1; END LOOP; END", {}))
    checks["written as people write it: a bare END closes any block, FOR $r, SET $x = and :=, AWAIT alone; END LOOP where an IF ends is refused"] = (
        got["forgiving"] == [{"t": 60, "async": 1}] and "END LOOP where the IF" in got["wrong end"])
    # What deciding costs: a loop that reads only variables.
    t0 = time.time()
    q("DECLARE $i = 0; WHILE $i < 200 DO $i = $i + 1; END WHILE", {})
    info["per pass ms"] = round((time.time() - t0) * 1000 / 200, 3)
    info["got"] = got
    node.kill()
    ok = all(checks.values())
    print(json.dumps({"scripts": checks, "ok": ok, "info": info}, indent=1, default=str))
    if not ok:
        sys.exit(1)
    return f"scripts: blocks, branches, loops, handlers, RETURN, dynamic SQL, PARALLEL and ASYNC, from HTTP, Python and Postgres: all {len(checks)} checks pass"


def tasks():
    """Task graphs (ADR-045 §5) on three nodes: tasks AFTER others run in the same run of their
    graph, with `EXECUTE TASK`'s values (`$day`) and the results before them (`pondra.result`), sent
    from a follower; WHEN false skips a task and what follows it still runs; retries with the same
    job (what a try wrote lands once), refused while a run is under way; a timeout, on_failure and
    nothing after a failure; ALTER TASK SUSPEND and RESUME; refusals (a loop, two schedules, a
    missing task, DROP of a followed one); a graph through a leader failover, each write once."""
    lake = new_lake()
    py = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "python")
    nodes = [Node(lake, A.port + i, python=sys.executable, env={"PYTHONPATH": py}).start() for i in range(3)]
    time.sleep(1)
    q = lambda s, port=A.port: call(port, "POST", "/sql", s.encode(), timeout=60)
    def err(s, port=A.port):
        try:
            q(s, port)
            return ""
        except RuntimeError as e:
            return str(e)
    def status(name, port=A.port):
        r = q(f"SELECT last_status AS s FROM pondra.tasks WHERE name = '{name}'", port)
        return r[0].get("s") if r else None
    checks, got = {}, {}
    # A graph: root, then load and skipper, then report.
    q("CREATE TABLE glog (task VARCHAR, v BIGINT, day DATE)")
    q("CREATE TASK root SCHEDULE '1 hour' AS SELECT 2 AS v")
    q("CREATE TASK load AFTER root AS BEGIN DECLARE PARAMETER $day DATE = DATE '2026-01-01'; INSERT INTO glog VALUES ('load', pondra.result('root'), $day); RETURN 20; END")
    q("CREATE TASK skipper AFTER root WHEN pondra.result('root') > 5 AS INSERT INTO glog VALUES ('skipper', 0, $day)")
    q("CREATE TASK report AFTER load, skipper AS BEGIN DECLARE PARAMETER $day DATE = DATE '2026-01-02'; INSERT INTO glog VALUES ('report', pondra.result('load'), $day); END")
    ran = q("EXECUTE TASK root (day => DATE '2026-09-01')", A.port + 2)
    until(lambda: status("report"), "ok", 30)
    got["graph"] = q("SELECT task, v, CAST(day AS VARCHAR) AS day FROM glog ORDER BY task")
    got["statuses"] = {r["name"]: r.get("last_status") for r in q("SELECT name, last_status FROM pondra.tasks WHERE name IN ('root', 'load', 'skipper', 'report')")}
    got["runs"] = q("SELECT routine, status FROM pondra.runs WHERE id LIKE 'task-%' ORDER BY routine")
    at = ran.get("run", "").rsplit("-", 1)[-1] if isinstance(ran, dict) else ""
    checks["a graph from a follower's EXECUTE TASK: AFTER in order, $day and pondra.result passed on, WHEN false skipped and what follows still runs"] = (
        got["graph"] == [{"task": "load", "v": 2, "day": "2026-09-01"}, {"task": "report", "v": 20, "day": "2026-09-01"}]
        and got["statuses"] == {"root": "ok", "load": "ok", "skipper": "skipped", "report": "ok"}
        and got["runs"] == [{"routine": n, "status": s} for n, s in (("load", "ok"), ("report", "ok"), ("root", "ok"), ("skipper", "skipped"))]
        and bool(at) and q(f"SELECT count(*) AS n FROM pondra.runs WHERE id LIKE 'task-%-{at}'")[0]["n"] == 4)
    q("EXECUTE TASK root")
    until(lambda: q("SELECT count(*) AS n FROM glog")[0]["n"], 4, 30)
    got["defaults"] = q("SELECT task, CAST(day AS VARCHAR) AS day FROM glog WHERE day < DATE '2026-09-01' ORDER BY task")
    checks["a run with no values: each task that is one block takes its DECLARE PARAMETER's default"] = got["defaults"] == [{"task": "load", "day": "2026-01-01"}, {"task": "report", "day": "2026-01-02"}]
    # Retries: the same job, so what a try wrote lands once; a run under way refuses EXECUTE TASK.
    q("CREATE TABLE tries (n BIGINT)")
    ready = time.time() + 2.5
    q(f"CREATE TASK flaky SCHEDULE '1 hour' WITH (retries = 4, retry_delay = '1 second') AS BEGIN INSERT INTO tries VALUES (1); IF date_part('epoch', now()) < {ready} THEN RAISE 'not yet'; END IF; END")
    q("EXECUTE TASK flaky")
    got["again"] = err("EXECUTE TASK flaky")
    until(lambda: status("flaky"), "ok", 30)
    run = q("SELECT status, notices FROM pondra.runs WHERE routine = 'flaky'")
    got["flaky"] = run
    checks["retries: failed tries said in the run's notices, then ok, what they wrote once; EXECUTE TASK refused while it runs"] = (
        "is running" in got["again"] and len(run) == 1 and run[0]["status"] == "ok" and "try 1 failed" in (run[0].get("notices") or "")
        and q("SELECT count(*) AS n FROM tries")[0]["n"] == 1)
    # A timeout, on_failure, and nothing after a failure.
    q("CREATE TABLE failures (t VARCHAR, e VARCHAR)")
    q("CREATE PROCEDURE failed(t VARCHAR, e VARCHAR) LANGUAGE sql AS $$ INSERT INTO failures VALUES ($t, $e) $$")
    q("CREATE TASK slow SCHEDULE '1 hour' WITH (timeout = '1 second', on_failure = failed) AS BEGIN DECLARE $i = 0; LOOP $i = $i + 1; END LOOP; END")
    q("CREATE TASK after_slow AFTER slow AS INSERT INTO glog VALUES ('after_slow', 0, NULL)")
    q("EXECUTE TASK slow")
    until(lambda: status("slow"), "failed", 30)
    time.sleep(1.5)
    got["failures"] = q("SELECT t, e FROM failures")
    got["slow"] = q("SELECT status, error FROM pondra.runs WHERE routine = 'slow'")
    checks["a timeout fails the run, on_failure is called with the task and its error, and what follows doesn't run"] = (
        got["failures"] == [{"t": "slow", "e": "timed out after 1 s"}] and got["slow"][0]["status"] == "failed"
        and "timed out" in (got["slow"][0].get("error") or "") and status("after_slow") is None
        and q("SELECT count(*) AS n FROM glog WHERE task = 'after_slow'")[0]["n"] == 0)
    # SUSPEND and RESUME.
    q("CREATE TABLE beats (n BIGINT)")
    q("CREATE TASK beat SCHEDULE '1 second' AS INSERT INTO beats VALUES (1)")
    time.sleep(3)
    q("ALTER TASK beat SUSPEND")
    time.sleep(1.5)
    n1 = q("SELECT count(*) AS n FROM beats")[0]["n"]
    state = q("SELECT state FROM pondra.tasks WHERE name = 'beat'")[0]["state"]
    time.sleep(2.5)
    n2 = q("SELECT count(*) AS n FROM beats")[0]["n"]
    q("ALTER TASK beat RESUME", A.port + 1)
    time.sleep(3)
    n3 = q("SELECT count(*) AS n FROM beats")[0]["n"]
    q("DROP TASK beat")
    got["beats"] = [n1, n2, n3, state]
    checks["ALTER TASK SUSPEND stops its ticks, RESUME starts them again"] = n1 > 0 and n2 == n1 and n3 > n2 and state == "suspended"
    # A body is kept as written: sqlparser read COPY's bare OVERWRITE as a missing option's value.
    q("CREATE TASK snap SCHEDULE '1 day' AS COPY (SELECT * FROM glog) TO 'out/snap' (FORMAT delta, OVERWRITE)")
    kept = q("SELECT statement FROM pondra.tasks WHERE name = 'snap'")
    q("DROP TASK snap")
    checks["a task's statement is kept as written: COPY … (FORMAT delta, OVERWRITE)"] = bool(kept) and "(FORMAT delta, OVERWRITE)" in kept[0].get("statement", "")
    # From the clients: Python's db.task, @db.task (a procedure the task calls) and execute_task's Run;
    # JavaScript's task, executeTask and wait.
    q("CREATE TABLE clog (who VARCHAR, day DATE)")
    nb = os.path.join(tempfile.mkdtemp(prefix="pondra-tasks-"), "tasks.py")
    open(nb, "w").write(f"""import json
from datetime import date
import pondra
db = pondra.connect("http://127.0.0.1:{A.port + 1}")
db.task("croot", "INSERT INTO clog VALUES ('sql', $day)", schedule="1 hour")

@db.task(after="croot", retries=1)
def cnext():
    pondra.sql("INSERT INTO clog VALUES ('python', NULL)")

ran = db.execute_task("croot", day=date(2026, 9, 2))
print(json.dumps(ran.wait(timeout=30)["status"]))
""")
    ran = subprocess.run([sys.executable, nb], capture_output=True, text=True, timeout=120, env={**os.environ, "PYTHONPATH": py})
    until(lambda: status("cnext"), "ok", 30)
    js = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "js", "index.js")
    script = f"""import {{ connect }} from {json.dumps(js)};
const db = connect("http://127.0.0.1:{A.port + 2}");
await db.task("jroot", "INSERT INTO clog VALUES ('js', $day)", {{ schedule: "1 hour", retries: 1, timeout: "10 minutes" }});
await db.task("jnext", "INSERT INTO clog VALUES ('js after', $day)", {{ after: ["jroot"], when: "true" }});
await db.wait(await db.executeTask("jroot", {{ day: "2026-09-03" }}));
console.log("done"); await db.close();"""
    path = os.path.join(tempfile.mkdtemp(prefix="pondra-tasks-"), "tasks.mjs")
    open(path, "w").write(script)
    js_out = subprocess.run(["node", path], capture_output=True, text=True, timeout=120)
    until(lambda: status("jnext"), "ok", 30)
    got["clients"] = {"python": ran.stdout.strip() or ran.stderr[-800:], "js": js_out.stdout.strip() or js_out.stderr[-800:],
                      "rows": q("SELECT who, CAST(day AS VARCHAR) AS day FROM clog ORDER BY who"),
                      "options": q("SELECT name, after, options FROM pondra.tasks WHERE name IN ('cnext', 'jroot') ORDER BY name")}
    checks["from the clients: Python's db.task, @db.task and execute_task(…).wait(); JavaScript's task, executeTask and wait"] = (
        got["clients"]["python"] == '"ok"' and got["clients"]["js"] == "done"
        and got["clients"]["rows"] == [{"who": "js", "day": "2026-09-03"}, {"who": "js after", "day": "2026-09-03"}, {"who": "python"}, {"who": "sql", "day": "2026-09-02"}])
    # Refusals.
    got["refused"] = refused = {
        "itself": err("CREATE TASK me AFTER me AS SELECT 1"),
        "a loop": err("CREATE OR REPLACE TASK root AFTER report AS SELECT 1"),
        "two schedules": err("CREATE TASK both AFTER root, flaky AS SELECT 1"),
        "missing": err("CREATE TASK lost AFTER nothing AS SELECT 1"),
        "followed": err("DROP TASK load"),
        "no task": err("EXECUTE TASK nothing"),
        "an option": err("CREATE TASK opt SCHEDULE '1 hour' WITH (tries = 2) AS SELECT 1"),
    }
    checks["refused by name: a task after itself, a loop, two schedules, a missing task, DROP of a followed one, EXECUTE of none, an unknown option"] = (
        "follow itself" in refused["itself"] and "follow itself" in refused["a loop"] and "2 different schedules" in refused["two schedules"]
        and "no task nothing" in refused["missing"] and "runs after load" in refused["followed"] and "no task nothing" in refused["no task"]
        and "retries, retry_delay" in refused["an option"] and status("root") == "ok")
    # A graph through a leader failover: the task under way runs again on the new leader, with its job.
    q("CREATE TABLE flog (task VARCHAR, v BIGINT)")
    q("CREATE TASK froot SCHEDULE '1 hour' AS SELECT 1")
    q("CREATE TASK fload AFTER froot AS BEGIN INSERT INTO flog VALUES ('fload', 1); WHILE date_part('epoch', now()) < $until DO $i = 1; END WHILE; RETURN 7; END")
    q("CREATE TASK freport AFTER fload AS INSERT INTO flog VALUES ('freport', pondra.result('fload'))")
    q(f"EXECUTE TASK froot (until => {time.time() + 6})", A.port + 1)
    until(lambda: q("SELECT count(*) AS n FROM flog")[0]["n"], 1, 15)
    nodes[0].kill()
    leader = None
    deadline = time.time() + 60
    while leader is None and time.time() < deadline:
        for n in nodes[1:]:
            try:
                if call(n.port, "GET", "/stats", timeout=2)["role"] == "leader":
                    leader = n
            except Exception:
                pass
        time.sleep(0.5)
    port = leader.port if leader else A.port + 1
    until(lambda: status("freport", port), "ok", 60)
    got["failover"] = q("SELECT task, v FROM flog ORDER BY task", port)
    got["failover runs"] = q("SELECT routine, status FROM pondra.runs WHERE routine IN ('froot', 'fload', 'freport') ORDER BY routine", port)
    checks["a graph through a leader failover: the task under way runs again on the new leader, each write once, and what follows gets its result"] = (
        leader is not None and got["failover"] == [{"task": "fload", "v": 1}, {"task": "freport", "v": 7}]
        and [r["status"] for r in got["failover runs"]] == ["ok", "ok", "ok"])
    for n in nodes:
        n.kill()
    ok = all(checks.values())
    print(json.dumps({"tasks": checks, "ok": ok, "got": got}, indent=1, default=str))
    if not ok:
        sys.exit(1)
    return f"tasks: graphs, WHEN, results, values, retries, timeouts, on_failure, SUSPEND, refusals, a failover: all {len(checks)} checks pass"


def variables():
    """SQL variables (round 31): `DECLARE $day DATE = …` declares one (type and default optional),
    `$day = …` changes it, and every `$day` after is its value, bound, from every door (HTTP with a
    session, Postgres, Python's `db.vars`); DuckDB's `SET VARIABLE`, `getvariable` and `RESET
    VARIABLE` are the same. A file's `DECLARE PARAMETER`s are its parameters (`pondra.parameters(…)`,
    ADR-044): a run's given values replace their defaults, cast to their types; a plain `DECLARE` is
    the file's own, and a value given for it is refused; a `.py` file's are its `# %%
    tags=["parameters"]` cell's. Procedures and file runs have variables of their own; `SET` stays
    the settings'."""
    import datetime
    import psycopg
    lake = new_lake()
    here = os.path.dirname(os.path.abspath(__file__))
    pg = A.port + 10
    node = Node(lake, A.port, pg=f"127.0.0.1:{pg}", python="auto", env={"PYTHONPATH": os.path.join(here, "..", "python")}).start()
    s1, s2 = {"x-pondra-session": "harness-vars-one"}, {"x-pondra-session": "harness-vars-two"}
    q = lambda s, h=s1: call(A.port, "POST", "/sql", s.encode(), headers=h)
    checks, info = {}, {}
    q("CREATE TABLE orders (id BIGINT, day DATE, amount DOUBLE)")
    q("INSERT INTO orders VALUES (1, DATE '2026-09-29', 10), (2, DATE '2026-09-30', 20), (3, DATE '2026-09-30', 5)")
    got = {"declared": q("DECLARE $day DATE = DATE '2026-10-01' - INTERVAL '1 day'"), "used": q("SELECT sum(amount) AS total FROM orders WHERE day = $day"),
           "named": q("SELECT $day, $day + 1")}
    got["changed"] = q("$day = $day - 1")
    got["after"] = q("SELECT sum(amount) AS total FROM orders WHERE day = $day")
    got["typed"] = _raises_text(lambda: q("$day = 'not a day'"))
    got["other session"] = _raises_text(lambda: q("SELECT $day", s2))
    got["no session"] = _raises_text(lambda: q("DECLARE $x = 1", {}))
    got["one script"] = q("DECLARE $n BIGINT = 2; $n = $n * 21; SELECT $n AS n", {})
    got["duckdb"] = q("SET VARIABLE region = 'eu'; SELECT getvariable('region') AS r, $region AS s, getvariable('nope') AS none")
    q("RESET VARIABLE region")
    got["reset"] = _raises_text(lambda: q("SELECT $region"))
    q("SET datafusion.execution.batch_size = 4096")  # (a setting, not a variable)
    got["listed"] = q("SELECT name, value, type, declared FROM pondra.variables ORDER BY name")
    q("$day = DATE '2026-09-30'")
    got["listed again"] = q("SELECT name, value, type, declared FROM pondra.variables ORDER BY name")
    checks["DECLARE $day DATE = …: its value, worked out once, in WHERE and SELECT; $day = … changes it"] = \
        got["declared"] == {"variable": "day", "value": "2026-09-30", "type": "Date32"} and got["used"] == [{"total": 25.0}] \
        and got["changed"]["value"] == "2026-09-29" and got["after"] == [{"total": 10.0}]
    checks["a column of a variable is named as written ($day, $day + 1)"] = got["named"] == [{"$day": "2026-09-30", "$day + 1": "2026-10-01"}]
    checks["a declared type holds: a value that isn't one refused by name"] = "$day is DATE" in got["typed"]
    checks["a variable is its session's: another session doesn't have it; with none, a DECLARE says so; a script sent at once has its own"] = \
        "no value for $day" in got["other session"] and "session" in got["no session"] and got["one script"] == [{"n": 42}]
    checks["DuckDB's SET VARIABLE, getvariable (NULL if none) and RESET VARIABLE"] = got["duckdb"] == [{"r": "eu", "s": "eu"}] and "no value for $region" in got["reset"]
    checks["pondra.variables lists them (name, value, type, declared), never a remembered answer; SET is still a setting"] = \
        got["listed"] == [{"name": "day", "value": "2026-09-29", "type": "Date32", "declared": "DATE"}] \
        and got["listed again"] == [{"name": "day", "value": "2026-09-30", "type": "Date32", "declared": "DATE"}]
    # A file's parameters, and runs.
    daily = ("-- The day to load\nDECLARE PARAMETER $day DATE = DATE '2026-09-29';\n-- $region: where the orders come from\nDECLARE PARAMETER $min DOUBLE DEFAULT 0;\n"
             "SELECT count(*) AS n, sum(amount) AS total, $region AS region FROM orders WHERE day = $day AND amount >= $min;\n")
    call(A.port, "PUT", "/files/etl/daily.sql", daily.encode())
    got["parameters"] = q("SELECT * FROM pondra.parameters('etl/daily.sql')")
    call(A.port, "PUT", "/files/etl/sensors.sql", b"DECLARE PARAMETER $d VARCHAR = 's1'; -- the sensor\nDECLARE PARAMETER $cel INT;\n\nSELECT $d AS d, $cel AS cel;\n")
    own = b"DECLARE PARAMETER $day DATE = DATE '2026-09-29';\nDECLARE $since = $day - 1; -- the day before\nDECLARE $n BIGINT;\nSELECT $since AS since, $n AS n;\n"
    call(A.port, "PUT", "/files/etl/own.sql", own)
    got["own parameters"] = q("SELECT name FROM pondra.parameters('etl/own.sql')")
    got["own run"] = q("CALL run('etl/own.sql', day => '2026-09-30')")
    got["own refused"] = _raises_text(lambda: q("CALL run('etl/own.sql', since => '2026-09-01')"))
    got["own given"] = _raises_text(lambda: call(A.port, "POST", "/sql", json.dumps({"sql": own.decode(), "params": {"since": "2026-09-01"}}).encode(), headers={**s2, "content-type": "application/json"}))
    params_py = (b"import pondra\n\n# %% tags=[\"parameters\"]\n# the day to load\nday = \"2026-09-29\"\nlimit: int = 2  # how many\n\n"
                 b"# %%\nprint('got', day, limit)\n")
    call(A.port, "PUT", "/files/etl/params.py", params_py)
    got["py parameters"] = q("SELECT * FROM pondra.parameters('etl/params.py')")
    got["described after"] = q("SELECT name, description FROM pondra.parameters('etl/sensors.sql')")
    got["run given"] = q("CALL run('etl/daily.sql', region => 'eu', day => '2026-09-30', min => 6)")
    got["run defaults"] = q("CALL run('etl/daily.sql', region => 'us')")
    got["run missing"] = _raises_text(lambda: q("CALL run('etl/daily.sql')"))
    got["session after runs"] = q("SELECT name, value FROM pondra.variables ORDER BY name")
    checks["a file's parameters: its DECLAREs (type, default) and the $names it uses unset (required), the comment above each its description, in order"] = got["parameters"] == [
        {"name": "day", "type": "DATE", "default": "DATE '2026-09-29'", "required": False, "description": "The day to load"},
        {"name": "min", "type": "DOUBLE", "default": "0", "required": False},
        {"name": "region", "required": True, "description": "where the orders come from"}]
    checks["a comment after a DECLARE on its line describes it, and not the DECLARE after it"] = \
        got["described after"] == [{"name": "d", "description": "the sensor"}, {"name": "cel"}]
    checks["a plain DECLARE is the file's own: not a parameter, NULL with no default, and a value given for it refused (a run's, a request's)"] = \
        got["own parameters"] == [{"name": "day"}] and got["own run"] == [{"since": "2026-09-29"}] \
        and "has no parameter since (it takes $day)" in got["own refused"] and "declares as its own variable" in got["own given"]
    checks["a .py file's parameters are its # %% tags=[\"parameters\"] cell's (types from annotations and literals, comments as descriptions)"] = got["py parameters"] == [
        {"name": "day", "type": "VARCHAR", "default": '"2026-09-29"', "required": False, "description": "the day to load"},
        {"name": "limit", "type": "BIGINT", "default": "2", "required": False, "description": "how many"}]
    checks["a run's values replace the defaults, cast to their types; a required one missing is named; the run's variables stay its own"] = \
        got["run given"] == [{"n": 1, "total": 20.0, "region": "eu"}] and got["run defaults"] == [{"n": 1, "total": 10.0, "region": "us"}] \
        and "no value for $region" in got["run missing"] and got["session after runs"] == [{"name": "day", "value": "2026-09-30"}]
    # Procedures.
    q("CREATE PROCEDURE twice(n BIGINT) LANGUAGE sql AS $$ DECLARE $m = $n * 2; $m = $m + 1; SELECT $m AS m, $1 AS first $$")
    got["procedure"] = q("CALL twice(21)")
    got["procedure refused"] = _raises_text(lambda: q("CREATE PROCEDURE broken() LANGUAGE sql AS $$ SELECT $nowhere $$"))
    checks["a procedure declares variables of its own; one using a $name nothing gives it is refused when made"] = \
        got["procedure"] == [{"m": 43, "first": 21}] and "no value for $nowhere" in got["procedure refused"] and "m" not in [r["name"] for r in q("SELECT name FROM pondra.variables")]
    # Postgres: each connection its own.
    with psycopg.connect(f"host=127.0.0.1 port={pg} user=pondra dbname=pondra", autocommit=True) as c:
        cur = c.execute("DECLARE $day DATE = DATE '2026-09-29'")
        tag = cur.statusmessage
        simple = c.execute("SELECT count(*) FROM orders WHERE day = $day").fetchall()
        c.execute("$day = $day + 1")
        extended = c.execute("SELECT count(*) FROM orders WHERE day = $day AND amount > %s", (6,)).fetchall()
    checks["over Postgres: DECLARE (its tag), $day = …, and $day beside the protocol's own parameters"] = tag == "DECLARE" and simple == [(1,)] and extended == [(1,)]
    # Python: db.vars, and a file run's Python and a session's DO block.
    db = _client(A.port)
    db.sql("DECLARE $day DATE = DATE '2026-09-30'")
    first = db.vars.day
    db.vars.day = datetime.date(2026, 9, 29)
    db.vars.limit = 3
    listed = dict(db.vars)
    del db.vars.limit
    total = db.sql("SELECT sum(amount) AS t FROM orders WHERE day = $day").rows()
    call(A.port, "PUT", "/files/etl/load.py", b"import pondra\nprint('given', pondra.vars.day)\npondra.vars.seen = 41\nprint(pondra.sql('SELECT $seen + 1 AS x').rows())\n")
    db.run("etl/load.py", day="2026-10-01")
    ran = list(db.notices)
    db.run("etl/params.py", limit=5)
    params_ran = list(db.notices)
    try:
        db.run("etl/params.py", nope=1)
        params_refused = ""
    except Exception as e:
        params_refused = str(e)
    info["params.py"] = {"ran": params_ran, "refused": params_refused}
    checks["a .py file's run: its parameters cell, then the values given over its defaults, then the rest; a name it doesn't take refused"] = \
        params_ran == ["got 2026-09-29 5"] and "has no parameter nope (it takes day, limit)" in params_refused
    db.sql("DO LANGUAGE python $$\nimport pondra\nprint('do', pondra.vars.day)\n$$")
    did = list(db.notices)
    info["python"] = {"first": str(first), "listed": {k: str(v) for k, v in listed.items()}, "total": total, "ran": ran, "did": did, "after": sorted(db.vars)}
    checks["Python's db.vars reads, sets and forgets $day; a file run's Python sees its given values as pondra.vars; a session's DO block sees the session's"] = \
        first == datetime.date(2026, 9, 30) and listed == {"day": datetime.date(2026, 9, 29), "limit": 3} and total == [{"t": 10.0}] \
        and ran == ["given 2026-10-01", "[{'x': 42}]"] and did == ["do 2026-09-29"] and sorted(db.vars) == ["day"]
    js = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "js", "index.js")
    script = f"""import {{ connect }} from {json.dumps(js)};
const db = connect("http://127.0.0.1:{A.port}");
await db.sql("DECLARE $day DATE = DATE '2026-09-29'"); await db.setVariable("top", 2);
const all = await db.vars(), top = await db.getVariable("top"), rows = await db.sql("SELECT count(*) AS n, $top * 10 AS t FROM orders WHERE day = $day");
await db.resetVariable("top");
console.log(JSON.stringify([all, top, rows, await db.vars(), (await db.parameters("etl/daily.sql")).map(p => p.name)])); await db.close();"""
    path = os.path.join(tempfile.mkdtemp(prefix="pondra-vars-"), "vars.mjs")
    open(path, "w").write(script)
    js_out = subprocess.run(["node", path], capture_output=True, text=True, timeout=60)
    got["javascript"] = js_out.stdout.strip() or js_out.stderr[-1500:]
    checks["JavaScript: db.vars(), getVariable, setVariable, resetVariable, parameters"] = \
        got["javascript"] == json.dumps([{"day": "2026-09-29", "top": 2}, 2, [{"n": 1, "t": 20}], {"day": "2026-09-29"}, ["day", "min", "region"]], separators=(",", ":"))
    info["got"] = got
    node.kill()
    ok = all(checks.values())
    print(json.dumps({"variables": checks, "ok": ok, "info": info}, indent=1, default=str))
    if not ok:
        sys.exit(1)
    return f"variables: DECLARE $day, $day = …, from HTTP, Postgres and Python; a file's parameters; runs and procedures of their own: all {len(checks)} checks pass"

def doors():
    """The doors matrix (ADR-036 §7): one list of features, each through every door — SQL over HTTP
    (a session), the Python client, the Postgres port (psycopg), Flight SQL (ADBC), the JavaScript
    client and MCP. Each cell is right, or refused by name where the door can't (a transaction needs
    a session: Flight SQL and MCP have none). The table is printed; every cell must be one of the two."""
    import re, psycopg, adbc_driver_flightsql.dbapi as adbc
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "python"))
    import pondra
    lake = new_lake()
    pgp, fp = A.port + 2000, A.port + 30
    node = Node(lake, A.port, pg=f"127.0.0.1:{pgp}", flight=f"127.0.0.1:{fp}", tier_secs=0.5).start()
    q = lambda s: sql(A.port, s)
    q("CREATE TABLE d_items (id BIGINT PRIMARY KEY, qty BIGINT CHECK (qty >= 0))")
    q("CREATE TABLE d_log (door VARCHAR, n BIGINT)")
    q("INSERT INTO d_items SELECT value AS id, 100 AS qty FROM range(1, 101)")
    q("CREATE PROCEDURE d_bump(k BIGINT) LANGUAGE sql AS $$ UPDATE d_items SET qty = qty + 1000 WHERE id = $k $$")
    DOORS = ["http", "python", "postgres", "flight", "javascript", "mcp"]
    def features(i):
        k = 10 * i + 1  # (each door its own keys)
        return {
            "query": ([f"SELECT count(*) AS n FROM d_items WHERE id <= 100"], lambda r: r[-1] == [[100]]),
            "insert": ([f"INSERT INTO d_log VALUES ('door{i}', {i})"], lambda r: q(f"SELECT n FROM d_log WHERE door = 'door{i}'") == [{"n": i}]),
            "key lookup": ([f"SELECT qty FROM d_items WHERE id = {k}"], lambda r: r[-1] == [[100]]),
            "one-key update": ([f"UPDATE d_items SET qty = qty + 5 WHERE id = {k + 1}"], lambda r: q(f"SELECT qty FROM d_items WHERE id = {k + 1}") == [{"qty": 105}]),
            "update by a filter": ([f"UPDATE d_items SET qty = qty + 1 WHERE id BETWEEN {k + 2} AND {k + 3}"], lambda r: q(f"SELECT sum(qty) AS s FROM d_items WHERE id BETWEEN {k + 2} AND {k + 3}") == [{"s": 202}]),
            "a CHECK refused, 23514": (["INSERT INTO d_items VALUES (999, -1)"], lambda r: r == "23514"),
            "no table, 42P01": (["SELECT * FROM d_nope"], lambda r: r == "42P01"),
            "procedure": ([f"CALL d_bump({k + 4})"], lambda r: q(f"SELECT qty FROM d_items WHERE id = {k + 4}") == [{"qty": 1100}]),
            "transaction": (["BEGIN", f"UPDATE d_items SET qty = qty - 7 WHERE id = {k + 5}", f"UPDATE d_items SET qty = qty + 7 WHERE id = {k + 6}", f"SELECT qty FROM d_items WHERE id = {k + 5}", "COMMIT"],
                            lambda r: r[3] == [[93]] and q(f"SELECT count(DISTINCT _version) AS v, sum(qty) AS s FROM d_items WHERE id IN ({k + 5}, {k + 6})") == [{"v": 1, "s": 200}]),
        }
    def rows_of(x):
        return [list(r.values()) if isinstance(r, dict) else list(r) for r in x] if isinstance(x, list) else x
    # Each door runs a feature's statements in one session: their answers, or the error's SQLSTATE.
    def via_http(stmts):
        sid, out = "d-" + uuid.uuid4().hex[:10], []
        for st in stmts:
            c = http.client.HTTPConnection("127.0.0.1", A.port, timeout=60)
            c.request("POST", "/sql", st.encode(), {"x-pondra-session": sid})
            r = c.getresponse(); body = r.read()
            if r.status != 200:
                return r.getheader("x-pondra-sqlstate")
            out.append(rows_of(json.loads(body)) if body[:1] == b"[" else None)
        return out
    def via_python(stmts):
        con, out = pondra.connect(f"http://127.0.0.1:{A.port}"), []
        try:
            for st in stmts:
                r = con.sql(st)
                out.append(rows_of(r.rows()) if hasattr(r, "rows") else None)
        except pondra.PondraError as e:
            return e.sqlstate
        finally:
            con.close()
        return out
    def via_postgres(stmts):
        out = []
        try:
            with psycopg.connect(f"host=127.0.0.1 port={pgp} user=u dbname=lake", autocommit=True) as c:
                for st in stmts:
                    cur = c.execute(st)
                    out.append([list(r) for r in cur.fetchall()] if cur.description else None)
        except psycopg.Error as e:
            return e.sqlstate
        return out
    def via_flight(stmts):
        out = []
        try:
            with adbc.connect(f"grpc://127.0.0.1:{fp}", autocommit=True) as c:
                cur = c.cursor()
                for st in stmts:
                    cur.execute(st)
                    try:
                        out.append([list(r) for r in cur.fetchall()])
                    except Exception:
                        out.append(None)
        except Exception as e:
            meta = {(k.decode() if isinstance(k, bytes) else k): (v.decode() if isinstance(v, bytes) else v) for k, v in (getattr(e, "details", None) or [])}  # (ADBC: the status's metadata)
            return meta.get("x-pondra-sqlstate") or "refused: " + str(e).splitlines()[0][:160]
        return out
    def via_mcp(stmts):
        out = []
        for st in stmts:
            tool = "query" if st.split()[0].upper() in ("SELECT",) else "write"
            body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": tool, "arguments": {"sql": st}}}).encode()
            r = json.loads(urllib.request.urlopen(urllib.request.Request(f"http://127.0.0.1:{A.port}/mcp", data=body, headers={"content-type": "application/json"}), timeout=60).read())["result"]
            text = r["content"][0]["text"]
            if r["isError"]:
                m = re.search(r"\b(\d{2}[0-9A-Z]\d{2})\b", text)
                return m.group(1) if m and m.group(1) in ("23514", "42P01", "40001", "25P02") else "refused: " + text[:160]
            got = json.loads(text)
            out.append(rows_of(got["rows"]) if isinstance(got, dict) and "rows" in got else None)
        return out
    js_lib = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "js", "index.js")
    def via_javascript(stmts):
        script = f"""import {{ connect }} from {json.dumps(js_lib)};
const db = connect("http://127.0.0.1:{A.port}"); const out = [];
try {{ for (const s of {json.dumps(stmts)}) {{ const r = await db.sql(s); out.push(Array.isArray(r) ? r.map(x => Object.values(x)) : null); }} console.log(JSON.stringify(out)); }}
catch (e) {{ console.log(JSON.stringify(e.sqlstate || ("refused: " + e.message.slice(0, 160)))); }}
finally {{ await db.close?.(); }}"""
        path = os.path.join(tempfile.mkdtemp(prefix="pondra-doors-"), "d.mjs")
        open(path, "w").write(script)
        r = subprocess.run(["node", path], capture_output=True, text=True, timeout=60)
        return json.loads(r.stdout.strip().splitlines()[-1]) if r.stdout.strip() else "refused: " + r.stderr[-160:]
    via = {"http": via_http, "python": via_python, "postgres": via_postgres, "flight": via_flight, "javascript": via_javascript, "mcp": via_mcp}
    matrix, checks = {}, {}
    no_session = {"flight", "mcp"}  # (no session to hold a transaction: refused by name)
    for i, door in enumerate(DOORS):
        for name, (stmts, right) in features(i + 1).items():
            try:
                got = via[door](stmts)
            except Exception as e:
                got = "error: " + str(e)[:160]
            ok = False
            try:
                ok = bool(right(got))
            except Exception:
                pass
            refused = isinstance(got, str) and ("session" in got or got.startswith("refused") or got == "0A000")
            cell = "ok" if ok else ("refused" if refused and name == "transaction" and door in no_session else "WRONG")
            matrix.setdefault(name, {})[door] = cell if cell != "WRONG" else f"WRONG: {str(got)[:120]}"
    for name, row in matrix.items():
        checks[f"{name}: " + ", ".join(f"{d} {'ok' if v == 'ok' else v}" for d, v in row.items())] = all(v in ("ok", "refused") for v in row.values())
    node.kill()
    ok = all(checks.values())
    width = max(len(n) for n in matrix)
    print(f"{'':{width}}  " + "  ".join(f"{d:10}" for d in DOORS), file=sys.stderr)
    for n, row in matrix.items():
        print(f"{n:{width}}  " + "  ".join(f"{(row[d] if len(row[d]) < 10 else 'WRONG'):10}" for d in DOORS), file=sys.stderr)
    print(json.dumps({"doors": checks, "ok": ok, "matrix": matrix}, indent=1))
    if not ok:
        sys.exit(1)

def friendly():
    """SQL as DuckDB's users write it (round 34, `friendly.rs`), each form's answer equal to DuckDB's
    over the same rows: PIVOT and UNPIVOT (DuckDB's statements and the standard's), COLUMNS(…),
    `* RENAME`, ORDER BY ALL, FETCH FIRST, list comprehensions and lambdas, a struct's field,
    max_by and arg_min, string_split, json_extract, DuckDB's ASOF JOIN … ON, a select's alias in
    its WHERE, SUMMARIZE; samples that sample (TABLESAMPLE was ignored); refusals by name; the
    same answers spread over three nodes; and what `random_sql.py` found (`- -3` written again as
    a comment, a DISTINCT over a CASE refused, two IN lists of a column taken as sets of values,
    columns of one name on both sides of a join mixed up)."""
    import datetime, decimal, duckdb, pyarrow as pa
    lake = new_lake()
    import psycopg
    nodes = [Node(lake, A.port + i, tier_secs=1, **({"pg": f"127.0.0.1:{A.port + 10}"} if i == 0 else {})).start() for i in range(3)]
    time.sleep(1)
    duck = duckdb.connect()
    t0 = datetime.datetime(2026, 1, 1)
    def row(i):
        n = "NULL" if i % 5 == 0 else str(i / 2)
        l = "[]" if i % 10 == 0 else f"[{i % 5}, {i % 3}]"
        j = json.dumps({"a": {"b": i}, "s": f"v{i % 3}"}).replace("'", "''")
        return f"({i}, {i % 7}, '{'abc'[i % 3]}', {i * 1.25 + 0.5}, {n}, {l}, TIMESTAMP '{t0 + datetime.timedelta(minutes=i)}', '{j}')"
    setup = ["CREATE TABLE t (id INTEGER, k INTEGER, g VARCHAR, x DOUBLE, n DOUBLE, l INTEGER[], ts TIMESTAMP, j VARCHAR)",
             "INSERT INTO t VALUES " + ", ".join(row(i) for i in range(2000)),
             "CREATE TABLE q (k INTEGER, ts TIMESTAMP, p DOUBLE)",
             "INSERT INTO q VALUES " + ", ".join(f"({k}, TIMESTAMP '{t0 + datetime.timedelta(minutes=37 * i + k)}', {i + k / 10})" for i in range(60) for k in range(6))]
    for s_ in setup:
        sql(A.port, s_)
        duck.execute(s_)
    time.sleep(3)  # (tiered to files: a spread query slices them)

    def norm(v):
        if isinstance(v, bool) or v is None or isinstance(v, str):
            return v
        if isinstance(v, (int, float, decimal.Decimal)):
            return round(float(v), 6)
        if isinstance(v, (list, tuple)):
            return tuple(norm(x) for x in v)
        if isinstance(v, dict):
            return tuple(sorted((k, norm(x)) for k, x in v.items()))
        if isinstance(v, (datetime.datetime, datetime.date)):
            return v.isoformat()
        return str(v)

    def ours(q, port=A.port, spread=None):
        path = "/sql?format=arrow" + ("" if spread is None else f"&spread={spread}")
        t = pa.ipc.open_stream(call(port, "POST", path, q.encode(), timeout=120)).read_all()
        return t.column_names, [tuple(norm(v) for v in r.values()) for r in t.to_pylist()]

    def theirs(q):
        r = duck.execute(q)
        return [d[0] for d in r.description], [tuple(norm(v) for v in x) for x in r.fetchall()]

    same = {  # name: (query, its columns' names compared too, its order compared too)
        "PIVOT t ON g USING sum(x) GROUP BY k": ("PIVOT (SELECT k, g, x FROM t) ON g USING sum(x) GROUP BY k", True, False),
        "PIVOT with aggregates named": ("PIVOT (SELECT k % 3 AS m, g, x FROM t) ON g USING sum(x) AS s, count(*) AS n", True, False),
        "PIVOT … ON g IN (…)": ("PIVOT (SELECT k, g, x FROM t) ON g IN ('a', 'b') USING max(x) GROUP BY k", True, False),
        "the standard's PIVOT": ("SELECT * FROM (SELECT k, g, x FROM t) PIVOT (sum(x) FOR g IN ('a' AS first, 'b'))", True, False),
        "UNPIVOT … INTO NAME … VALUE …": ("UNPIVOT (SELECT id, x, n FROM t) ON x, n INTO NAME col VALUE val", True, False),
        "the standard's UNPIVOT, INCLUDE NULLS": ("SELECT * FROM (SELECT id, x, n FROM t) UNPIVOT INCLUDE NULLS (val FOR col IN (x, n))", True, False),
        "UNPIVOT … ON COLUMNS(…)": ("UNPIVOT (SELECT id, x, n FROM t) ON COLUMNS('^(x|n)$') INTO NAME col VALUE val", True, False),
        "COLUMNS('regex')": ("SELECT COLUMNS('^(id|x)$') FROM t", True, False),
        "min(COLUMNS(*))": ("SELECT min(COLUMNS(*)) FROM (SELECT id, x, n FROM t)", True, False),
        "COLUMNS([…]) in a WHERE": ("SELECT id FROM t WHERE COLUMNS(['id', 'x']) > 1000", False, False),
        "* EXCLUDE … RENAME …": ("SELECT * EXCLUDE (l, j) RENAME (g AS grp) FROM t", True, False),
        "ORDER BY ALL": ("SELECT g, k, id FROM t ORDER BY ALL", True, True),
        "ORDER BY ALL DESC over *": ("SELECT * FROM (SELECT g, id FROM t) ORDER BY ALL DESC", True, True),
        "OFFSET … FETCH NEXT … ROWS ONLY": ("SELECT id FROM t ORDER BY id DESC OFFSET 3 ROWS FETCH NEXT 5 ROWS ONLY", True, True),
        "a list comprehension": ("SELECT id, [y * 2 FOR y IN l IF y > 0] AS d FROM t", True, False),
        "lambdas (x -> …, and AND in their bodies)": ("SELECT id, list_transform(l, y -> y + 1) AS a, list_filter(l, y -> y > 0 AND y < 4) AS b FROM t", True, False),
        "LAMBDA y: …": ("SELECT id, list_transform(l, LAMBDA y: y * 10) AS a FROM t", True, False),
        "a struct's field": ("SELECT ({'a': id, 'b': g}).a AS a FROM t", True, False),
        "max_by, arg_min, min_by (named as written)": ("SELECT g, max_by(id, x), arg_min(id, x), min_by(k, x) FROM t GROUP BY g", True, False),
        "max_by over NULLs": ("SELECT k, max_by(id, n) AS a, arg_max(id, n) AS b FROM t GROUP BY k", True, False),
        "string_split": ("SELECT string_split(g || ',' || k, ',') AS s FROM t", True, False),
        "json_extract, json_extract_string": ("SELECT id, json_extract(j, '$.a.b') AS b, json_extract_string(j, '$.s') AS s FROM t", True, False),
        "DuckDB's ASOF JOIN … ON": ("SELECT t.id, q.p FROM t ASOF JOIN q ON t.k = q.k AND t.ts >= q.ts", True, False),
        "ASOF LEFT JOIN … ON": ("SELECT t.id, q.p FROM t ASOF LEFT JOIN q ON t.k = q.k AND t.ts >= q.ts", True, False),
        "a select's alias in its WHERE": ("SELECT x * 2 AS dbl FROM t WHERE dbl > 4000", True, False),
        "… but a column of that name wins": ("SELECT id + 100 AS x FROM t WHERE x > 20", True, False),
        "a minus before a minus, in a text written again (it read back as a comment)": ("SELECT id, - -k AS m, - - -x AS b FROM t WHERE m > 3 AND g IS DISTINCT FROM 'b'", True, False),
        "DISTINCT over a CASE whose WHEN shows its THEN isn't NULL (random_sql.py)": ("SELECT DISTINCT CASE WHEN k < 3 AND n = 2.5 THEN n ELSE 0 END AS c FROM t", True, False),
        "x IN (a column, …) AND x IN (…): not the lists' intersection (random_sql.py)": ("SELECT id FROM t WHERE g IN (g, 'z') AND g IN ('a', 'b')", True, False),
        "x IN (…) AND x NOT IN (NULL): no row (random_sql.py)": ("SELECT count(*) AS c FROM t WHERE g IN ('a', 'b') AND g NOT IN (NULL)", True, False),
        "columns of one name on both sides of a LEFT JOIN, filtered on its padded side (random_sql.py)": ("SELECT (t.ts - q.ts) AS d, t.k FROM t LEFT JOIN q ON t.id = q.k WHERE q.k IS DISTINCT FROM 3", True, False),
    }
    checks, failed = {}, {}
    for name, (q, names, ordered) in same.items():
        try:
            (cn, got), (dn, want) = ours(q), theirs(q)
            ok = (got == want if ordered else sorted(got, key=repr) == sorted(want, key=repr)) and (not names or cn == dn) and len(want) > 0
        except Exception as e:
            ok, cn, got, dn, want = False, str(e)[:400], [], [], []
        checks[f"{name} == DuckDB's"] = ok
        if not ok:
            failed[name] = {"ours": [cn, got[:4]], "duckdb": [dn, want[:4]]}
    # SUMMARIZE: the exact columns (approximate ones and the mean's and spread's text aside)
    cn, got = ours("SUMMARIZE t")
    dn, want = theirs("SUMMARIZE t")
    pick = lambda names, rows: sorted((r[0], r[1], r[names.index("count")], r[names.index("null_percentage")]) + ((r[2], r[3]) if r[0] in ("id", "k", "g") else ()) for r in rows)
    checks["SUMMARIZE: each column's name, type, count, NULLs and (whole and text columns) min and max == DuckDB's"] = cn == dn and pick(cn, got) == pick(dn, want)
    if not checks[list(checks)[-1]]:
        failed["SUMMARIZE"] = {"ours": [cn, pick(cn, got)], "duckdb": [dn, pick(dn, want)]}
    count = lambda q: sql(A.port, q)[0]["n"]
    ids = {r["id"] for r in sql(A.port, "SELECT id FROM t USING SAMPLE 7 ROWS")}
    share = [count("SELECT count(*) AS n FROM t USING SAMPLE 10%"), count("SELECT count(*) AS n FROM (SELECT * FROM t TABLESAMPLE SYSTEM (10)) s"),
             count("SELECT count(*) AS n FROM (SELECT * FROM t TABLESAMPLE (5 ROWS)) s")]
    checks["USING SAMPLE n ROWS / n%, TABLESAMPLE (n) / (n ROWS): samples of those sizes (TABLESAMPLE was ignored)"] = len(ids) == 7 and ids <= set(range(2000)) and 100 < share[0] < 320 and 100 < share[1] < 320 and share[2] == 5
    refused = [_raises_text(lambda q=q: sql(A.port, q)) for q in ("SELECT id FROM t FETCH FIRST 10 PERCENT ROWS ONLY", "SELECT max_by(id, x) OVER () FROM t", "SELECT json_extract(j, g) FROM t")]
    checks["refused by name: FETCH … PERCENT, max_by(…) OVER, a JSON path that isn't a literal"] = "PERCENT" in refused[0] and "first_value" in refused[1] and "literal" in refused[2]
    spread = ["PIVOT (SELECT k, g, x FROM t) ON g USING sum(x) GROUP BY k", "SELECT min(COLUMNS(*)) FROM (SELECT id, x, n FROM t)",
              "SELECT id, [y * 2 FOR y IN l IF y > 0] AS d FROM t", "SELECT g, max_by(id, x), arg_min(id, x) FROM t GROUP BY g",
              "SELECT t.id, q.p FROM t ASOF JOIN q ON t.k = q.k AND t.ts >= q.ts", "UNPIVOT (SELECT id, x, n FROM t) ON x, n INTO NAME col VALUE val"]
    apart = {q: (sorted(ours(q, spread=1)[1], key=repr), sorted(ours(q, spread=0)[1], key=repr)) for q in spread}
    checks["spread over three nodes == one node"] = all(a == b for a, b in apart.values())
    with psycopg.connect(f"host=127.0.0.1 port={A.port + 10} user=u dbname=lake", autocommit=True) as pg:  # (described, then run)
        door = [sorted(norm(tuple(r)) for r in pg.execute(q, (0,)).fetchall()) for q in (
            "SELECT id, list_filter(l, y -> y > 0 AND y < 4) AS b FROM t WHERE id >= %s", "SELECT g, max_by(id, x) FROM t WHERE k >= %s GROUP BY g")]
    want = [sorted(theirs(q)[1]) for q in ("SELECT id, list_filter(l, y -> y > 0 AND y < 4) AS b FROM t", "SELECT g, max_by(id, x) FROM t GROUP BY g")]
    checks["over Postgres (a lambda, max_by) == DuckDB's"] = door == want
    sql(A.port, "CREATE VIEW best AS SELECT g, max_by(id, x) AS best, [y + 1 FOR y IN list(k)] AS ks FROM t GROUP BY g")
    call(A.port, "POST", "/views/plus", b"SELECT id, list_transform(l, y -> y + 1) AS a FROM t")
    sql(A.port, "INSERT INTO t VALUES (5000, 1, 'a', 9999.5, NULL, [7, 8], TIMESTAMP '2026-02-01 00:00:00', '{}')")
    duck.execute("INSERT INTO t VALUES (5000, 1, 'a', 9999.5, NULL, [7, 8], TIMESTAMP '2026-02-01 00:00:00', '{}')")
    view = sorted((r["g"], r["best"]) for r in sql(A.port, "SELECT g, best FROM best"))
    plus = {r["id"]: r.get("a") for r in sql(A.port, "SELECT id, a FROM plus WHERE id IN (5000, 1)")}
    checks["a stored view and a materialized view over them, read after a write"] = view == sorted(duck.execute("SELECT g, max_by(id, x) FROM t GROUP BY g").fetchall()) and plus == {5000: [8, 9], 1: [2, 2]}
    for n in nodes:
        n.kill()
    ok = all(checks.values())
    print(json.dumps({"friendly": checks, "ok": ok}, indent=1))
    if not ok:
        print(json.dumps(failed, indent=1, default=str)[:20000])
        sys.exit(1)
    return f"SQL as DuckDB's users write it: {len(same) + 1} forms answer as DuckDB does, samples sample, refusals by name, spread == one node"


def all_tests():
    A.runs, A.batches = min(A.runs, 5), min(A.batches, 30)
    out = {t.__name__: t() for t in (upsert, deal, outside, clouds, kafkas, tiering, fence, insert, serverless, clients, kafka, alter, windows, sessions, asof, sums, schemas, changes, guard, files, layouts, clusters, copies, streams, columns, fills, dedup, procedures, functions, external, names, answers, writes, adopted, ids, rewrites, followers, transactions, upserts, live, temps, across, found, renames, workspace, server, scale, flight, users, secrets, safety, versions, stopped, flows, begin, doors, objects, registry, sparksql, variables, scripts, hot, minmax, history, friendly, reader, crash)}
    A.secs = min(A.secs, 20)
    out["load"] = load()
    print(json.dumps(out, indent=1))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", choices=["crash", "upsert", "deal", "outside", "clouds", "kafkas", "tiering", "fence", "reader", "insert", "serverless", "clients", "kafka", "alter", "windows", "sessions", "asof", "sums", "schemas", "changes", "guard", "files", "layouts", "clusters", "copies", "streams", "columns", "fills", "dedup", "procedures", "functions", "external", "names", "answers", "writes", "adopted", "ids", "rewrites", "followers", "transactions", "upserts", "live", "temps", "across", "found", "renames", "workspace", "server", "scale", "flight", "users", "secrets", "safety", "versions", "stopped", "flows", "begin", "doors", "objects", "registry", "sparksql", "variables", "scripts", "tasks", "hot", "minmax", "history", "friendly", "load", "all"])
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
    try:
        {"crash": crash, "upsert": upsert, "deal": deal, "outside": outside, "clouds": clouds, "kafkas": kafkas, "tiering": tiering, "fence": fence, "reader": reader, "insert": insert, "serverless": serverless, "clients": clients, "kafka": kafka, "alter": alter, "windows": windows, "sessions": sessions, "asof": asof, "sums": sums, "schemas": schemas, "changes": changes, "guard": guard, "files": files, "layouts": layouts, "clusters": clusters, "copies": copies, "streams": streams, "columns": columns, "fills": fills, "dedup": dedup, "procedures": procedures, "functions": functions, "external": external, "names": names, "answers": answers, "writes": writes, "adopted": adopted, "ids": ids, "rewrites": rewrites, "followers": followers, "transactions": transactions, "upserts": upserts, "live": live, "temps": temps, "across": across, "found": found, "renames": renames, "workspace": workspace, "server": server, "scale": scale, "flight": flight, "users": users, "secrets": secrets, "safety": safety, "versions": versions, "stopped": stopped, "flows": flows, "begin": begin, "doors": doors, "objects": objects, "registry": registry, "sparksql": sparksql, "variables": variables, "scripts": scripts, "tasks": tasks, "hot": hot, "minmax": minmax, "history": history, "friendly": friendly, "load": load, "all": all_tests}[A.mode]()
    except BaseException as e:  # a failure ends the run, though threads may still wait on a node (crash's producers retry for ever)
        code = e.code if isinstance(e, SystemExit) else 1
        if not isinstance(e, SystemExit):
            traceback.print_exc()
        elif not isinstance(code, int) and code is not None:
            print(code, file=sys.stderr)
        clean_up()
        sys.stdout.flush(), sys.stderr.flush()
        os._exit(code if isinstance(code, int) else 0 if code is None else 1)
