#!/usr/bin/env python3
"""Branches (ADR-047): `CREATE DATABASE dev CLONE prod` is prod as it was, copying none of its files;
its writes are its own; prod keeps the files it reads while it lives, through merges, purges and
retention; `pondra.diff` tells their rows apart; `ALTER DATABASE dev REFRESH [t]` brings a table
(a view: the tables it reads; none named: all) and what follows it up to prod's present; a branch of
a branch; `DROP DATABASE` lets go. A base that signs in on its own (a user, no token shared with the
branch): its branch's REFRESH and unpin go by the branch's own key, and a clone's REFRESH stays within
the schemas it took (`signed_in_check`). Who makes a branch and who may drop it: `owners_check` (a user
with CLONE branches a database and owns the branch; only its owner, or an admin, drops it).

  environments_check.py [--new target/release/pondra] [--work DIR] [--port 9780] [--s3]
  environments_check.py --across [--new target/release/pondra] [--work DIR] [--port 9780]

With --s3 the lakes are on s3://$PONDRA_BUCKET (AWS_* point at R2, MinIO or tools/sim_r2.py), where a
branch reads its base's files through the bucket's own store.

prod's node tiers only when asked (`POST /tier`), keeps its past one second (`--retain-secs 1`) and
purges changed rows every round (`PONDRA_PURGE_ROWS=1`), so what a branch reads would be deleted at
once if nothing kept it. Prints the checks as JSON and exits 1 if one fails (the lakes and the nodes'
logs are then kept in --work).

--across (ADR-058) runs alone, on moto (tools/sim_r2.py, no latency) with buckets acme-prod and
acme-dev: prod is a server of its own, and dev, another server, attaches it READ_ONLY with a read-only
key of prod's bucket and prod's URL, clones a schema of it into its own bucket and writes only there.
Two gates in front of moto (`Gate`) refuse what dev's own key may not reach and what the read-only key
may not write, and count it: the check asserts the counts.
"""
import argparse, base64, glob, http.client, http.server, json, os, re, shutil, signal, subprocess, sys, tempfile, threading, time, urllib.error, urllib.parse, urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from upgrade_check import Node, Failed, NODES, OWNER, stop_all, gone  # (a node started and stopped as a scheduler would)


def keys(lake):
    """Every object of a lake: a folder's files, or a bucket's keys under its prefix."""
    if not lake.startswith("s3://"):
        return sorted(glob.glob(os.path.join(lake, "**", "*"), recursive=True))
    import boto3
    bucket, prefix = lake[len("s3://"):].split("/", 1)
    s3 = boto3.client("s3", endpoint_url=os.environ.get("AWS_ENDPOINT_URL") or os.environ.get("AWS_ENDPOINT"))
    return sorted(o["Key"] for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix + "/") for o in page.get("Contents", []))


def parquet(lake):
    return [k for k in keys(lake) if "/data/" in k and k.endswith(".parquet")]


def raw(port, method, path, body, headers):
    """A request to the node on `port` with exactly these headers (no token of the test's own): (status, text)."""
    req = urllib.request.Request(f"http://127.0.0.1:{port}{path}", data=body.encode() if body else None, method=method, headers=headers)
    try:
        r = urllib.request.urlopen(req, timeout=30)
        return r.status, r.read().decode(errors="replace")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode(errors="replace")
    except OSError as e:
        return 0, str(e)


def s3_env(endpoint, key):
    """A node's bucket settings: the endpoint its buckets are reached by (moto, or a gate), its own master key."""
    return {"AWS_ENDPOINT": endpoint, "AWS_ENDPOINT_URL": endpoint, "AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test",
            "AWS_REGION": "us-east-1", "AWS_ALLOW_HTTP": "true", "PONDRA_SECRET_KEY": key, "PONDRA_PURGE_ROWS": "1"}


def moto(port, work):
    """moto's S3 (tools/sim_r2.py without its latency) on `port`, with the buckets across_check uses, once it answers."""
    import boto3
    log = os.path.join(work, f"moto-{port}.log")
    p = subprocess.Popen([sys.executable, os.path.join(HERE, "sim_r2.py"), "--port", str(port), "--zero"], stdout=open(log, "w"), stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL)
    s3 = boto3.client("s3", endpoint_url=f"http://127.0.0.1:{port}", region_name="us-east-1", aws_access_key_id="test", aws_secret_access_key="test")
    deadline = time.time() + 60
    while True:
        try:
            s3.list_buckets()
            break
        except Exception:  # (not up yet)
            if p.poll() is not None or time.time() > deadline:
                raise Failed(f"moto didn't start on {port} (pip install -r tools/requirements.txt): {open(log).read()[-1500:]}")
            time.sleep(0.5)
    for bucket in ("acme-prod", "acme-dev"):
        s3.create_bucket(Bucket=bucket)
    return p


class Gate:
    """A proxy on 127.0.0.1:`port` in front of moto, for across_check. The environment's endpoint (`env`)
    reaches acme-dev and no other bucket: a request for acme-prod is answered 403 AccessDenied and counted
    (`prod_by_env`). prod's read-only key (`prod_ro`) reads acme-prod, GET and HEAD and lists included: a
    write to it is answered 403 and counted (`prod_writes`), a read forwarded and counted (`prod_reads`)."""

    def __init__(self, upstream, port, counts, env):
        self.upstream, self.counts, self.env, self.lock = urllib.parse.urlsplit(upstream).netloc, counts, env, threading.Lock()
        gate = self

        class Handler(http.server.BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"  # (keep-alive, as the store's clients use it)

            def log_message(self, *_):
                pass

            def any(self):
                body = self.rfile.read(int(self.headers.get("Content-Length") or 0))
                bucket = urllib.parse.urlsplit(self.path).path.lstrip("/").split("/", 1)[0]
                if not gate.admit(self.command, bucket):
                    return self.answer(403, "Forbidden", [("Content-Type", "application/xml")],
                                       b"<?xml version='1.0' encoding='UTF-8'?><Error><Code>AccessDenied</Code><Message>Access Denied</Message></Error>")
                c = http.client.HTTPConnection(gate.upstream, timeout=300)
                try:
                    c.request(self.command, self.path, body, {k: v for k, v in self.headers.items() if k.lower() != "connection"})
                    r = c.getresponse()
                    data = r.read()
                finally:
                    c.close()
                headers = [(k, v) for k, v in r.getheaders() if k.lower() not in ("transfer-encoding", "connection", "content-length")]
                self.answer(r.status, r.reason, headers, data, r.getheader("Content-Length") if self.command == "HEAD" else None)

            def answer(self, status, reason, headers, data, length=None):
                self.send_response_only(status, reason)
                for k, v in headers:
                    self.send_header(k, v)
                self.send_header("Content-Length", length or str(len(data)))  # (a HEAD's is the object's)
                self.end_headers()
                if self.command != "HEAD":
                    self.wfile.write(data)

            do_GET = do_PUT = do_POST = do_DELETE = do_HEAD = any

        class Quiet(http.server.ThreadingHTTPServer):
            def handle_error(self, *_):
                pass  # (a node that drops a kept-alive connection: not the test's business)

        self.server = Quiet(("127.0.0.1", port), Handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def admit(self, method, bucket):
        """Whether a request goes on to moto; what the gate refuses or reads of prod's bucket is counted."""
        with self.lock:
            if self.env:
                if bucket == "acme-prod":
                    self.counts["prod_by_env"] += 1
                return bucket == "acme-dev"
            if bucket != "acme-prod":
                return False
            if method not in ("GET", "HEAD"):
                self.counts["prod_writes"] += 1
                return False
            self.counts["prod_reads"] += 1
            return True

    def close(self):
        self.server.shutdown()
        self.server.server_close()


class Anon(Node):
    """A node the test calls with no token of its own: it has none (token=False), so the test's admin token
    never reaches it, and what it sends on to another server carries only the TYPE pondra secret's token
    (`across_check`). The test is the program that started it (`x-pondra-owner`), so it stays an admin here
    once a user signs in, as the shell is."""

    def call(self, method, path, body=None, headers=None, timeout=120):
        data = body if isinstance(body, (bytes, type(None))) else (body if isinstance(body, str) else json.dumps(body)).encode()
        req = urllib.request.Request(f"http://127.0.0.1:{self.port}{path}", data=data, method=method, headers={"x-pondra-owner": OWNER, **(headers or {})})
        try:
            out = urllib.request.urlopen(req, timeout=timeout).read()
        except urllib.error.HTTPError as e:
            raise Failed(f"{method} {path} {body if isinstance(body, str) else ''}: {e.code} {e.read().decode(errors='replace')[:800]}") from None
        return json.loads(out) if out[:1] in (b"{", b"[") else out


def environments_check(bin, work, port, root):
    prod_dir, dev_dir = root + "/prod", root + "/dev"
    env = {"PONDRA_PURGE_ROWS": "1"}
    prod = Node(bin, prod_dir, port, work, "--retain-secs", "1", env=env).start()
    q = prod.q

    def rows(n, sql):
        return [tuple(r.values()) for r in n.q(sql)]

    def refused(n, sql):
        try:
            n.q(sql)
            return ""
        except Failed as e:
            return str(e)

    def until(f, secs=30):
        deadline = time.time() + secs
        while True:
            v = f()
            if v or time.time() > deadline:
                return v
            time.sleep(0.5)

    checks = {}
    # prod: files merged and purged, rows still in the log, changed rows, a keyed table, a view, a task,
    # a secret and a workspace file.
    q("CREATE SCHEMA sales")
    q("CREATE TABLE sales.orders (id BIGINT, amount DOUBLE)")
    for i in range(4):  # (small files, for maintenance to merge later)
        q(f"INSERT INTO sales.orders SELECT x, x * 1.5 FROM generate_series({i * 250 + 1}, {(i + 1) * 250}) AS s(x)")
        prod.post("/tier")
    q("CREATE TABLE k (id BIGINT PRIMARY KEY, v VARCHAR)")
    q("INSERT INTO k VALUES (1, 'one'), (2, 'two'), (3, 'three')")
    q("CREATE TABLE kv (id BIGINT PRIMARY KEY, v VARCHAR)")
    q("INSERT INTO kv VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    q("CREATE MATERIALIZED VIEW sales.totals AS SELECT id % 2 AS odd, count(*) AS n, sum(amount) AS s FROM sales.orders GROUP BY id % 2")
    prod.post("/tier")
    q("UPDATE sales.orders SET amount = -1 WHERE id <= 10")
    q("DELETE FROM sales.orders WHERE id BETWEEN 11 AND 20")
    q("INSERT INTO sales.orders VALUES (2001, 7.0), (2002, 8.0)")  # (in the log when branched)
    q("UPDATE k SET v = 'TWO' WHERE id = 2")
    q("CREATE TASK nightly SCHEDULE '1 hour' AS INSERT INTO k VALUES (99, 'task')")
    q("CREATE SECRET s3_prod (TYPE s3, KEY_ID 'id', SECRET 'secret', SCOPE 's3://prod-bucket/')")
    q("CREATE SHARE acme")
    q("ALTER SHARE acme ADD TABLE sales.orders")
    q("CREATE RECIPIENT acme_corp")
    q("GRANT SELECT ON SHARE acme TO RECIPIENT acme_corp")
    prod.call("PUT", "/files/etl/orders.sql", "SELECT count(*) FROM sales.orders")
    every = "SELECT _row_id, _version, id, amount FROM sales.orders ORDER BY id"
    keyed = "SELECT _row_id, id, v FROM k ORDER BY id"
    before, before_k, totals = rows(prod, every), rows(prod, keyed), rows(prod, "SELECT odd, n, s FROM sales.totals ORDER BY odd")
    files_before = len(parquet(prod_dir))

    t0 = time.time()
    made = q("CREATE DATABASE dev CLONE prod")
    took = time.time() - t0
    copied = parquet(dev_dir)
    checks["CREATE DATABASE dev CLONE prod leads dev once: one term (`pondra lead`), not a second for a statement"] = \
        len([k for k in keys(dev_dir) if "/cluster/term/" in k]) == 1
    checks["CREATE DATABASE dev CLONE prod copies no file (the log tail and the workspace only)"] = \
        len(copied) == 0 and len(keys(dev_dir)) > 0 and len(parquet(prod_dir)) == files_before
    checks["prod reads dev at once: every row as prod had it, ids and versions too (the log tail too)"] = \
        until(lambda: rows(prod, every.replace("sales.orders", "dev.sales.orders")) == before) is True \
        and rows(prod, keyed.replace(" k ", " dev.k ")) == before_k and rows(prod, "SELECT odd, n, s FROM dev.sales.totals ORDER BY odd") == totals
    listed = [(r["name"], r.get("base"), r["branches"]) for r in q("SELECT name, base, branches FROM pondra.databases ORDER BY name")]  # (a NULL is left out)
    checks["pondra.databases names dev's base, and prod's branch"] = listed == [("dev", "prod", 0), ("prod", None, 1)]

    # dev's own node: its writes are its own; what reaches the outside starts suspended.
    dev = Node(bin, dev_dir, port + 1, work, "--retain-secs", "1", env=env).start()
    checks["dev answers as prod did, from its own node"] = rows(dev, every) == before and rows(dev, keyed) == before_k
    shared = "SELECT (SELECT count(*) FROM pondra.shares) AS shares, (SELECT count(*) FROM pondra.recipients) AS recipients"
    checks["dev's task starts suspended, and no secret, share or recipient came with it"] = rows(dev, "SELECT name, state FROM pondra.tasks") == [("nightly", "suspended")] \
        and rows(dev, "SELECT count(*) FROM secrets()") == [(0,)] and rows(prod, "SELECT count(*) FROM secrets()") == [(1,)] \
        and rows(dev, shared) == [(0, 0)] and rows(prod, shared) == [(1, 1)]
    checks["dev has prod's workspace files"] = dev.get("/files/etl/orders.sql") == b"SELECT count(*) FROM sales.orders"
    # A task dev makes waits for RESUME too (an apply's, say); one it resumed stays started when made again.
    dev.q("CREATE TASK hourly SCHEDULE '1 hour' AS INSERT INTO k VALUES (98, 'dev')")
    made_suspended = rows(dev, "SELECT name, state FROM pondra.tasks ORDER BY name")
    dev.q("ALTER TASK hourly RESUME")
    dev.q("CREATE OR REPLACE TASK hourly SCHEDULE '2 hours' AS INSERT INTO k VALUES (98, 'dev')")
    checks["a task dev makes starts suspended too, one it resumed stays started when made again, and prod's runs"] = \
        made_suspended == [("hourly", "suspended"), ("nightly", "suspended")] \
        and rows(dev, "SELECT state FROM pondra.tasks WHERE name = 'hourly'") == [("started",)] \
        and rows(prod, "SELECT state FROM pondra.tasks WHERE name = 'nightly'") == [("started",)]
    dev.q("DROP TASK hourly")
    dev.q("INSERT INTO sales.orders VALUES (5001, 1.0)")
    dev.q("UPDATE sales.orders SET amount = 0 WHERE id BETWEEN 21 AND 25")
    dev.q("DELETE FROM sales.orders WHERE id BETWEEN 26 AND 30")
    dev.q("INSERT INTO k VALUES (4, 'dev')")
    dev.q("INSERT INTO kv VALUES (1, 'dev')")
    for _ in range(2):
        dev.post("/tier")
    model = {r[2]: r for r in before}
    for i in range(26, 31):
        model.pop(i)
    dev_rows = rows(dev, every)
    changed = {r[2]: r for r in dev_rows}
    checks["dev's writes are its own: dev has them, prod and its files don't"] = \
        sorted(changed) == sorted(list(model) + [5001]) and all(changed[i][3] == 0 for i in range(21, 26)) \
        and rows(prod, every) == before and rows(dev, "SELECT sum(n) FROM sales.totals") == [(len(changed),)] \
        and len(parquet(prod_dir)) == files_before and len(parquet(dev_dir)) > 0
    diff = until(lambda: (lambda d: d if d == [("delete", 5), ("insert", 1), ("update_postimage", 5), ("update_preimage", 5)] else None)(
        rows(prod, "SELECT _change_type, count(*) FROM pondra.diff('sales.orders', 'dev.sales.orders') GROUP BY 1 ORDER BY 1")))
    checks["pondra.diff('sales.orders', 'dev.sales.orders'): one inserted, five changed, five deleted"] = diff is not None \
        and rows(prod, "SELECT id, amount FROM pondra.diff('sales.orders', 'dev.sales.orders') WHERE _change_type = 'update_postimage' ORDER BY id") == [(i, 0.0) for i in range(21, 26)]

    # prod moves on: every row changed (purged into position deletes, files a tenth deleted rewritten),
    # small files merged, a table dropped, retention of a second. dev still reads what it listed.
    q("UPDATE sales.orders SET amount = amount + 1")
    q("DELETE FROM sales.orders WHERE id > 900")
    q("DROP TABLE k")
    q("UPDATE kv SET v = 'B' WHERE id = 2")
    q("DELETE FROM kv WHERE id = 3")
    q("INSERT INTO kv VALUES (5, 'e')")
    for _ in range(4):
        prod.post("/tier")
        time.sleep(3)
    time.sleep(12)  # (retention runs every 10 s at most)
    prod.post("/tier")
    dev.stop()
    dev = Node(bin, dev_dir, port + 1, work, "--retain-secs", "1", env=env).start()  # (nothing of prod's files in its memory)
    checks["while prod merges, purges, drops and lets its past go, dev's files stay: dev answers as before after a restart"] = \
        rows(dev, every) == dev_rows and rows(dev, keyed) == before_k + [(rows(dev, "SELECT _row_id FROM k WHERE id = 4")[0][0], 4, "dev")]

    # REFRESH: a table and what follows it, as prod has it now (its log tail too); dev's own rows of
    # it go; what dev writes after is newer (an upsert wins, row ids stay unique).
    q("INSERT INTO sales.orders VALUES (3001, 3.0)")
    q("INSERT INTO kv VALUES (6, 'f')")
    kv = "SELECT _row_id, _version, id, v FROM kv ORDER BY id"
    totals_sql = "SELECT odd, n, s FROM sales.totals ORDER BY odd"
    dev.q("CREATE MATERIALIZED VIEW sales.mine AS SELECT id, amount FROM sales.orders WHERE amount > 100")
    checks["REFRESH refused while dev has a view of the table prod doesn't, saying what to do"] = \
        "sales.mine" in refused(prod, "ALTER DATABASE dev REFRESH sales.orders")
    dev.q("DROP MATERIALIZED VIEW sales.mine")
    now_rows, now_totals, now_kv, own_k = rows(prod, every), rows(prod, totals_sql), rows(prod, kv), rows(dev, keyed)
    q("ALTER DATABASE dev REFRESH sales.orders")  # (from prod: dev's leader does it)
    dev.q("ALTER DATABASE dev REFRESH kv")        # (from dev itself)
    checks["ALTER DATABASE dev REFRESH t: t and its view as prod has them now (ids, versions, the log tail); other tables as they were"] = \
        rows(dev, every) == now_rows and rows(dev, totals_sql) == now_totals and rows(dev, kv) == now_kv and rows(dev, keyed) == own_k
    dev.q("INSERT INTO kv VALUES (1, 'after')")
    dev.q("INSERT INTO sales.orders VALUES (6001, 6.0)")
    dev.q("UPDATE sales.orders SET amount = 9 WHERE id = 3001")
    for _ in range(2):
        dev.post("/tier")
    checks["…what dev writes after it is newer: an upsert wins through tiering, row ids stay unique, prod unchanged"] = \
        rows(dev, "SELECT v FROM kv WHERE id = 1") == [("after",)] and rows(dev, "SELECT count(*) = count(DISTINCT _row_id) FROM sales.orders") == [(True,)] \
        and rows(dev, "SELECT amount FROM sales.orders WHERE id IN (3001, 6001) ORDER BY id") == [(9.0,), (6.0,)] \
        and rows(dev, "SELECT sum(n) FROM sales.totals") == rows(dev, "SELECT count(*) FROM sales.orders") and rows(prod, every) == now_rows
    after_kv = rows(dev, kv)
    q("ALTER DATABASE dev REFRESH sales.totals")  # (a view: the tables it reads)
    checks["…naming a view brings the tables it reads, and only those"] = \
        rows(dev, every) == now_rows and rows(dev, totals_sql) == now_totals and rows(dev, kv) == after_kv
    dev.q("ALTER DATABASE dev REFRESH")  # (none named: every table dev took from prod that prod still has)
    checks["…none named, every table dev took from prod that prod still has (prod dropped k: dev keeps its own)"] = \
        rows(dev, kv) == now_kv and rows(dev, every) == now_rows and rows(dev, keyed) == own_k
    checks["…and no REFRESH brings prod's shares or recipients, though prod shares the table it brought"] = \
        rows(dev, shared) == [(0, 0)] and rows(prod, shared) == [(1, 1)]
    checks["REFRESH refused by name: a database that isn't a branch, a table its base doesn't have"] = \
        "isn't a branch" in refused(prod, "ALTER DATABASE prod REFRESH sales.orders") and "isn't a table" in refused(prod, "ALTER DATABASE dev REFRESH k")
    dev_rows = rows(dev, every)

    # A branch of a branch reads both; its base can't go first; WITH (schemas = …) and WITH NO DATA.
    dev.stop()
    q("CREATE DATABASE dev2 CLONE dev")
    checks["a branch of a branch answers as its base"] = until(lambda: rows(prod, every.replace("sales.orders", "dev2.sales.orders")) == dev_rows) is True
    checks["a database with branches can't be dropped first"] = "branches" in refused(prod, "DROP DATABASE dev")
    q("CREATE DATABASE sales_only CLONE prod WITH (schemas = (sales))")
    q("CREATE DATABASE fixtures CLONE prod WITH NO DATA")
    checks["WITH (schemas = (sales)) takes that schema alone"] = rows(prod, "SELECT count(*) FROM sales_only.sales.orders") == rows(prod, "SELECT count(*) FROM sales.orders") \
        and "not found" in refused(prod, "SELECT * FROM sales_only.k").lower() + refused(prod, "SELECT * FROM sales_only.public.k").lower()
    checks["WITH NO DATA: the tables, empty"] = rows(prod, "SELECT count(*) FROM fixtures.sales.orders") == [(0,)]
    elsewhere = os.path.join(work, "y") if root.startswith("s3://") else "s3://b/y"
    checks["refused by name: a clone of a database not here, a branch where its base isn't (a bucket, a disk)"] = \
        "no database" in refused(prod, "CREATE DATABASE x CLONE nowhere") and "lives where its base does" in refused(prod, f"CREATE DATABASE y LOCATION '{elsewhere}' CLONE prod")

    # DROP DATABASE lets go: prod's replaced files go once nothing pins them.
    for name in ["dev2", "dev", "sales_only", "fixtures"]:
        q(f"DROP DATABASE {name}")
    held = len(parquet(prod_dir))
    for _ in range(3):
        time.sleep(11)
        prod.post("/tier")
    checks["DROP DATABASE: the branches' folders go, prod's pins with them, and prod lets the files they held go"] = \
        not keys(dev_dir) and rows(prod, "SELECT name, branches FROM pondra.databases") == [("prod", 0)] and len(parquet(prod_dir)) < held
    checks["prod answers as it should after it all"] = rows(prod, "SELECT count(*) FROM sales.orders") == [(len([r for r in before if r[2] <= 900]) + 1,)]
    checks["(cloned in {:.1f} s)".format(took)] = True
    return checks


def signed_in_check(bin, work, port, root):
    """A branch of a database that signs in on its own (a user, and no admin token shared with the
    branch): its REFRESH and unpin go by the branch's own key (`branch::keyed`), which opens nothing
    else; a clone's REFRESH stays within the schemas it took; a database next door that signs in on
    its own is cloned only by a caller who may read it (`branch::may`)."""
    prod_dir, dev_dir = root + "/sprod", root + "/sdev"  # (the folder names are the databases' names: prod is `sprod`)
    env = {"PONDRA_PURGE_ROWS": "1"}
    prod = Node(bin, prod_dir, port + 10, work, "--retain-secs", "1", env=env, token=False).start()

    def rows(n, sql):
        return [tuple(r.values()) for r in n.q(sql)]

    def refused(n, sql):
        try:
            n.q(sql)
            return ""
        except Failed as e:
            return str(e)

    basic = "Basic " + base64.b64encode(b"ann:ann-password-1").decode()

    def as_ann(sql):
        status, text = raw(port + 10, "POST", "/sql", sql, {"Authorization": basic})
        if status != 200:
            raise Failed(f"as ann, {sql}: {status} {text[:800]}")
        return [tuple(r.values()) for r in json.loads(text)]

    checks = {}
    prod.q("CREATE SCHEMA sales")
    prod.q("CREATE TABLE sales.orders (id BIGINT, amount DOUBLE)")
    prod.q("INSERT INTO sales.orders VALUES (1, 1.5), (2, 3.0), (3, 4.5)")
    prod.q("CREATE SCHEMA hr")
    prod.q("CREATE TABLE hr.pay (id BIGINT, amount DOUBLE)")
    prod.q("INSERT INTO hr.pay VALUES (1, 100.0)")
    # (made before prod has a user, so sdev has none and is open; prod signs in on its own after)
    prod.q("CREATE DATABASE sdev CLONE sprod WITH (schemas = (sales))")
    prod.q("CREATE USER ann PASSWORD 'ann-password-1' SUPERUSER")
    dev = Node(bin, dev_dir, port + 11, work, "--retain-secs", "1", env=env, token=False).start()
    prod.q("INSERT INTO sales.orders VALUES (4, 6.0), (5, 7.5)")

    dev.q("ALTER DATABASE sdev REFRESH sales.orders")  # (sdev's own node asks prod with its key)
    checks["REFRESH of a base that signs in on its own, with no token shared: by the branch's key"] = rows(dev, "SELECT count(*) FROM sales.orders") == [(5,)]

    took_hr = refused(dev, "ALTER DATABASE sdev REFRESH hr.pay")
    dev.q("CREATE SCHEMA hr")
    dev.q("CREATE TABLE hr.pay (id BIGINT, amount DOUBLE)")
    dev.q("INSERT INTO hr.pay VALUES (7, 7.0)")
    dev.q("ALTER DATABASE sdev REFRESH")
    checks["…only the schemas the clone took: one named outside them refused; none named, the branch's own table of another schema stays its own"] = \
        "took only" in took_hr and rows(dev, "SELECT id FROM hr.pay") == [(7,)]

    loc = prod.q("SELECT location FROM pondra.databases WHERE name = 'sdev'")[0]["location"]
    status, text = raw(port + 10, "POST", "/cluster/ddl", json.dumps({"op": "pin", "lake": loc, "ms": None}), {"Authorization": basic, "Content-Type": "application/json"})
    if status != 200:
        raise Failed(f"prod's pin of sdev, as ann: {status} {text[:800]}")
    key = json.loads(text)["key"]
    checks["prod's pin of sdev gives it a key of its own (pb_…)"] = key.startswith("pb_")
    bearer = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}

    def ddl(body, headers=bearer):
        return raw(port + 10, "POST", "/cluster/ddl", json.dumps(body), headers)[0]

    checks["a branch's key renews and lets go of its own pin, and opens nothing else"] = \
        ddl({"op": "pin", "lake": loc, "ms": None}) == 200 \
        and ddl({"op": "pin", "lake": loc, "ms": 0}) != 200 \
        and ddl({"op": "pin", "lake": "/elsewhere", "ms": None}) != 200 \
        and ddl({"op": "drop_table", "name": "sales.orders", "if_exists": True}) != 200 \
        and as_ann("SELECT count(*) AS n FROM sales.orders") == [(5,)] \
        and raw(port + 10, "POST", "/sql", "SELECT 1", {"Authorization": f"Bearer {key}"})[0] == 401 \
        and ddl({"op": "pin", "lake": loc, "ms": None}, {"Authorization": "Bearer pb_nope", "Content-Type": "application/json"}) != 200

    dev.q(f"ATTACH '{prod_dir}' AS prod2")  # (a database next door that signs in on its own)
    status, _, text = dev.plain("POST", "/sql", "CREATE DATABASE x CLONE prod2")
    cloning = refused(dev, "CREATE DATABASE x2 CLONE prod2")  # (the owner passes; prod refuses dev's own key)
    checks["CLONE of a database next door that signs in on its own: refused without its own rights, and saying where it is cloned"] = \
        status != 200 and "read and written through it" in text and "cloned on its own node" in cloning and "read and written through it" not in cloning and not keys(root + "/x") and not keys(root + "/x2")

    checks["…its unpin lets go, and then the key makes no new pin"] = ddl({"op": "unpin", "lake": loc}) == 200 \
        and as_ann("SELECT branches FROM pondra.databases WHERE name = 'sprod'") == [(0,)] \
        and "never makes one" in refused(dev, "ALTER DATABASE sdev REFRESH sales.orders")
    dev.stop()
    prod.stop()
    return checks


def across_check(bin, work, port):
    """Branches across servers (ADR-058): prod is a server of its own (its bucket, users and master key),
    dev another that writes only its own bucket. dev attaches prod READ_ONLY with a read-only key of prod's
    bucket and prod's URL, clones a schema of it (the branch in dev's bucket, no file copied) and writes that
    branch alone. Proves: nothing dev does writes prod's bucket, and prod's bucket is reached only with that
    key; a pin asked with a token holding CLONE takes the schemas granted and nothing more; a branch on a
    node that never saw the attachment reads prod with the key it kept, and keeps what prod pinned while
    prod merges, purges and lets its past go; REFRESH brings prod's rows; DROP lets go."""
    prod_url, env_url, ro_url, moto_url = f"http://127.0.0.1:{port}", f"http://127.0.0.1:{port + 3}", f"http://127.0.0.1:{port + 4}", f"http://127.0.0.1:{port + 5}"
    key_prod, key_dev = "across-check-prod-master-key-0123456789abcdef", "across-check-dev-master-key-0123456789abcdef"
    prod_lake, dev_lake, ali_lake = "s3://acme-prod/prod", "s3://acme-dev/dev", "s3://acme-dev/ali"
    counts = {"prod_by_env": 0, "prod_writes": 0, "prod_reads": 0}  # (what the gates refused or read of prod's bucket)
    os.environ.update(AWS_ENDPOINT=moto_url, AWS_ENDPOINT_URL=moto_url, AWS_ACCESS_KEY_ID="test", AWS_SECRET_ACCESS_KEY="test", AWS_REGION="us-east-1")  # (this process lists buckets itself)
    checks, gates, moto_proc = {}, [], None

    def rows(n, sql):
        return [tuple(r.values()) for r in n.q(sql)]

    def refused(n, sql):
        try:
            n.q(sql)
            return ""
        except Failed as e:
            return str(e)

    def until(f, secs=30):
        deadline = time.time() + secs
        while True:
            v = f()
            if v or time.time() > deadline:
                return v
            time.sleep(0.5)

    try:
        moto_proc = moto(port + 5, work)
        gates.append(Gate(moto_url, port + 3, counts, env=True))  # (dev's environment's endpoint)
        gates.append(Gate(moto_url, port + 4, counts, env=False))  # (prod_ro's endpoint)
        prod = Node(bin, prod_lake, port, work, "--retain-secs", "1", env=s3_env(moto_url, key_prod)).start()
        prod.q("CREATE SCHEMA sales")
        prod.q("CREATE SCHEMA hr")
        prod.q("CREATE TABLE sales.orders (id BIGINT, amount DOUBLE)")
        prod.q("INSERT INTO sales.orders VALUES (1, 1.5), (2, 3.0), (3, 4.5)")
        prod.post("/tier")
        prod.q("INSERT INTO sales.orders VALUES (4, 6.0), (5, 7.5)")  # (in the log: a branch reads it too)
        prod.q("CREATE TABLE sales.k (id BIGINT PRIMARY KEY, v VARCHAR)")
        prod.q("INSERT INTO sales.k VALUES (1, 'one'), (2, 'two')")
        prod.q("CREATE MATERIALIZED VIEW sales.totals AS SELECT id % 2 AS odd, count(*) AS n FROM sales.orders GROUP BY id % 2")
        prod.q("CREATE TABLE hr.pay (id BIGINT, amount DOUBLE)")
        prod.q("INSERT INTO hr.pay VALUES (1, 100.0)")
        prod.q("CREATE USER dev_server LOGIN")
        prod.q("CREATE USER nobody LOGIN")
        prod.q("GRANT CLONE ON SCHEMA sales TO dev_server")
        dev_token = prod.q("CREATE TOKEN clone FOR USER dev_server")["token"]
        nobody_token = prod.q("CREATE TOKEN plain FOR USER nobody")["token"]
        elsewhere = refused(prod, "GRANT CLONE ON DATABASE elsewhere TO nobody")
        prod.q("GRANT CLONE ON DATABASE prod TO nobody")
        shown = rows(prod, "SELECT on_kind, on_name FROM pondra.grants WHERE grantee = 'nobody'")
        prod.q("REVOKE CLONE ON DATABASE prod FROM nobody")
        checks["GRANT CLONE ON DATABASE names the database it runs on (another's refused), and pondra.grants shows it so"] = \
            "elsewhere" in elsewhere and shown == [("database", "prod")] and rows(prod, "SELECT count(*) FROM pondra.grants WHERE grantee = 'nobody'") == [(0,)]
        before = rows(prod, "SELECT id, amount FROM sales.orders ORDER BY id")
        files_before = parquet(prod_lake)

        # dev has no token of its own (Anon): nothing of the test's admin token can reach prod through it.
        dev = Anon(bin, dev_lake, port + 1, work, env=s3_env(env_url, key_dev), token=False).start()
        dev.q(f"CREATE SECRET prod_ro (TYPE s3, KEY_ID 'test', SECRET 'test', REGION 'us-east-1', ENDPOINT '{ro_url}', URL_STYLE 'path', SCOPE 's3://acme-prod/prod')")
        dev.q(f"CREATE SECRET prod_clone (TYPE pondra, TOKEN '{dev_token}', SCOPE '{prod_url}')")
        dev.q(f"ATTACH 's3://acme-prod/prod' AS prod (READ_ONLY, ENDPOINT '{prod_url}')")
        checks["dev reads prod's tables through the attachment: prod's rows, as prod has them"] = rows(dev, "SELECT id, amount FROM prod.sales.orders ORDER BY id") == before
        writes = [refused(dev, "INSERT INTO prod.sales.orders VALUES (9, 9.0)"), refused(dev, "DELETE FROM prod.sales.orders WHERE id = 1")]
        checks["a write to the attachment is refused by name: a read-only key here"] = all("read-only key" in w for w in writes)

        # A user of dev granted CLONE on prod (attached here) branches it on dev and owns it. dev has no
        # token, but a user who signs in is limited by its own grants all the same: the grant alone lets dana.
        dev.q("CREATE USER dana PASSWORD 'dana-password-1'")
        dev.q("GRANT CLONE ON DATABASE prod TO dana")
        dana = {"Authorization": "Basic " + base64.b64encode(b"dana:dana-password-1").decode()}
        made_dana = raw(port + 1, "POST", "/sql", "CREATE DATABASE dana CLONE prod", dana)[0]
        owner_dana = rows(dev, "SELECT owner FROM pondra.databases WHERE name = 'dana'")
        dropped_dana = raw(port + 1, "POST", "/sql", "DROP DATABASE dana", dana)[0]
        checks["a user of dev granted CLONE ON DATABASE prod (attached) branches it on dev, and owns it"] = \
            made_dana == 200 and owner_dana == [("dana",)] and dropped_dana == 200
        dev.q("CREATE DATABASE ali CLONE prod")
        checks["CREATE DATABASE ali CLONE prod on dev keeps its folder in acme-dev and copies no file of prod's"] = \
            len(keys(ali_lake)) > 0 and not keys("s3://acme-prod/ali") and parquet(ali_lake) == []
        checks["ali reads sales as prod has it (tables, keyed table and view), and has no hr schema (CLONE ON SCHEMA sales only)"] = \
            rows(dev, "SELECT id, amount FROM ali.sales.orders ORDER BY id") == before \
            and rows(dev, "SELECT id, v FROM ali.sales.k ORDER BY id") == rows(prod, "SELECT id, v FROM sales.k ORDER BY id") \
            and rows(dev, "SELECT odd, n FROM ali.sales.totals ORDER BY odd") == rows(prod, "SELECT odd, n FROM sales.totals ORDER BY odd") \
            and "not found" in refused(dev, "SELECT * FROM ali.hr.pay").lower()
        taken = refused(dev, "CREATE DATABASE ali2 CLONE prod WITH (schemas = (hr))")
        checks["a clone of a schema dev_server has no CLONE on is refused, naming the one it may clone"] = "sales" in taken

        dev.q("INSERT INTO ali.sales.orders VALUES (9, 9.0)")
        dev.q("UPDATE ali.sales.orders SET amount = 0 WHERE id = 1")
        ali_before = rows(dev, "SELECT id, amount FROM ali.sales.orders ORDER BY id")
        checks["ali's writes are its own: dev has them, prod's rows and files don't change"] = \
            (9, 9.0) in ali_before and (1, 0.0) in ali_before and rows(prod, "SELECT id, amount FROM sales.orders ORDER BY id") == before and parquet(prod_lake) == files_before

        ali = Anon(bin, ali_lake, port + 2, work, env=s3_env(env_url, key_dev), token=False).start()
        checks["a node on ali's folder that never saw the attachment answers as dev did: it reads prod with the key it kept"] = \
            rows(ali, "SELECT id, amount FROM sales.orders ORDER BY id") == ali_before
        metrics = ali.get("/metrics").decode()
        turns = {l.split(" ")[0]: float(l.split(" ")[1]) for l in metrics.splitlines() if l.startswith("pondra_bucket_turns") and 'lake="prod"' in l}
        quarter = max(1, int(os.environ.get("PONDRA_BUCKET_REQUESTS", "256")) // 4)
        checks["ali reads prod's files with a quarter of a node's turns at prod's bucket, and used them (/metrics)"] = \
            turns.get('pondra_bucket_turns{bucket="s3://acme-prod",lake="prod"}') == quarter \
            and 1 <= turns.get('pondra_bucket_turns_most{bucket="s3://acme-prod",lake="prod"}', 0) <= quarter

        prod.q("UPDATE sales.orders SET amount = amount + 100")  # (prod moves on: every row changed, one deleted)
        prod.q("DELETE FROM sales.orders WHERE id = 2")
        for _ in range(3):
            prod.post("/tier")
            time.sleep(3)
        time.sleep(12)  # (retention runs every 10 s at most)
        prod.post("/tier")
        checks["prod moves on (rows changed, merged, purged, its past let go): ali still answers as before, from what prod pinned"] = \
            rows(ali, "SELECT id, amount FROM sales.orders ORDER BY id") == ali_before and rows(prod, "SELECT count(*) FROM sales.orders WHERE amount > 100") == [(4,)]

        ali.q("ALTER DATABASE ali REFRESH sales.orders")  # (from ali's own node)
        checks["ALTER DATABASE ali REFRESH sales.orders, from ali's own node, brings prod's rows now"] = \
            rows(ali, "SELECT id, amount FROM sales.orders ORDER BY id") == rows(prod, "SELECT id, amount FROM sales.orders ORDER BY id")

        pin = json.dumps({"op": "pin", "lake": "s3://acme-dev/zzz", "ms": None})
        status, text = raw(port, "POST", "/cluster/ddl", pin, {"Authorization": f"Bearer {nobody_token}", "Content-Type": "application/json"})
        checks["a pin asked with the token of a user without CLONE is refused, naming CLONE"] = status == 403 and "CLONE" in text
        drop = json.dumps({"op": "drop_table", "name": "sales.orders", "if_exists": True})
        status, _ = raw(port, "POST", "/cluster/ddl", drop, {"Authorization": f"Bearer {dev_token}", "Content-Type": "application/json"})
        checks["dev_server's token, which made ali's pin, can't drop prod's table over /cluster/ddl"] = status != 200 and refused(prod, "SELECT count(*) FROM sales.orders") == ""

        ali.stop()
        dev.q("DROP DATABASE ali")
        checks["DROP DATABASE ali on dev: its folder goes from acme-dev, and prod shows no branch left"] = \
            not keys(ali_lake) and until(lambda: rows(prod, "SELECT branches FROM pondra.databases WHERE name = 'prod'") == [(0,)]) is True
        checks["the gates: nothing reached prod's bucket with a write, nothing through dev's own key, and prod_ro read it"] = \
            counts["prod_writes"] == 0 and counts["prod_by_env"] == 0 and counts["prod_reads"] > 0
        return checks
    finally:
        for n in NODES:
            n.stop()
        for g in gates:
            g.close()
        if moto_proc:
            moto_proc.terminate()
            moto_proc.wait()


def protect_check(bin, work, port, root):
    """Protected databases (ADR-058 step 3): a project's apply protects `pprod` ([env.prod] protected = true),
    after which its objects change only by an apply (an APPLY holder's, or an admin's); rows and a person's
    own objects stay free; lifting is an admin's and audited; a branch isn't protected; APPLY is a database's."""
    prod = Node(bin, root + "/pprod", port + 20, work, token=False).start()  # (the database is named by its folder)
    basic = {u: "Basic " + base64.b64encode(f"{u}:{u}-password-1".encode()).decode() for u in ["root", "ci"]}

    def rows(n, sql):
        return [tuple(r.values()) for r in n.q(sql)]

    def refused(n, sql):
        try:
            n.q(sql)
            return ""
        except Failed as e:
            return str(e)

    def until(f, secs=30):
        deadline = time.time() + secs
        while True:
            v = f()
            if v or time.time() > deadline:
                return v
            time.sleep(0.5)

    def as_(user, sql):
        """A statement as a user signs in (Basic): (status, text)."""
        return raw(port + 20, "POST", "/sql", sql, {"Authorization": basic[user]})

    def denied(user, sql):
        """What a user's statement is refused with ('' when it runs)."""
        status, text = as_(user, sql)
        return "" if status == 200 else text

    def rows_as(user, sql):
        status, text = as_(user, sql)
        if status != 200:
            raise Failed(f"as {user}, {sql}: {status} {text[:800]}")
        return [tuple(r.values()) for r in json.loads(text)]

    def post(user, path, body):
        """A JSON request (an apply, the cluster's own door) as a user signs in: (status, text)."""
        return raw(port + 20, "POST", path, json.dumps(body), {"Authorization": basic[user], "Content-Type": "application/json"})

    def state():
        """pprod's protection as pondra.databases shows it: (protected, lifted?, lifted_by)."""
        return [(r.get("protected"), r.get("lifted_at") is not None, r.get("lifted_by")) for r in prod.q("SELECT protected, lifted_at, lifted_by FROM pondra.databases WHERE name = 'pprod'")]  # (a row's JSON leaves NULLs out)

    project = {"pondra.toml": '[project]\nname = "sales"\n\n[env.prod]\nprotected = true\n',
               "objects/schemas.sql": "CREATE SCHEMA sales;\n",
               "objects/sales/orders.sql": "CREATE TABLE sales.orders (id BIGINT, amount DOUBLE);\n",
               "objects/sales/big.sql": "CREATE VIEW sales.big AS SELECT id, amount FROM sales.orders WHERE amount >= 0;\n",
               "objects/access.sql": "CREATE ROLE analyst;\nGRANT SELECT ON TABLE sales.orders TO analyst;\n"}
    with_note = {**project, "objects/sales/orders.sql": "CREATE TABLE sales.orders (id BIGINT, amount DOUBLE, note VARCHAR);\n"}
    checks = {}
    try:
        prod.q("CREATE USER root PASSWORD 'root-password-1' SUPERUSER")
        prod.q("CREATE USER ci PASSWORD 'ci-password-1'")
        prod.q("GRANT APPLY ON DATABASE pprod TO ci")
        status, text = post("ci", "/apply", {"files": project, "env": "prod"})
        if status != 200:
            raise Failed(f"ci's apply of the project: {status} {text[:800]}")
        checks["an APPLY holder who isn't an admin applies, and `[env.prod] protected = true` protects the database"] = state() == [(True, False, None)]

        frozen = ["DROP TABLE sales.orders", "CREATE OR REPLACE VIEW sales.big AS SELECT 1 AS x", "ALTER TABLE sales.orders ADD COLUMN x INT",
                  "ALTER TABLE sales.orders RENAME COLUMN amount TO a", "DROP ROLE analyst", "REVOKE SELECT ON sales.orders FROM analyst"]
        checks["an admin's DROP, CREATE OR REPLACE and ALTER of a project's table or view, and of its role's grants, are refused by name"] = \
            all("protected" in t and "project sales" in t for t in (denied("root", s) for s in frozen))

        writes = [as_("root", s)[0] for s in ["INSERT INTO sales.orders VALUES (1, 10.0), (2, 20.0)", "UPDATE sales.orders SET amount = 11.0 WHERE id = 1", "DELETE FROM sales.orders WHERE id = 2"]]
        own = ["CREATE TABLE sales.scratch (a INT)", "DROP TABLE sales.scratch", "CREATE USER ann PASSWORD 'ann-password-2'", "GRANT analyst TO ann", "GRANT SELECT ON sales.orders TO ann"]
        checks["rows still change, and a person's own objects are theirs, in the project's schema too"] = \
            writes == [200, 200, 200] and rows(prod, "SELECT id, amount FROM sales.orders ORDER BY id") == [(1, 11.0)] and all(as_("root", s)[0] == 200 for s in own)

        drop = {"op": "drop_table", "name": "sales.orders", "if_exists": False}  # (Ddl::DropTable's JSON: `op` names the variant)
        checks["the cluster's own door refuses it too: an admin's drop over /cluster/ddl is refused, and the table stays"] = \
            post("root", "/cluster/ddl", drop)[0] != 200 and refused(prod, "SELECT count(*) FROM sales.orders") == ""

        root_apply = post("root", "/apply", {"files": with_note, "env": "prod"})
        ci_apply = post("ci", "/apply", {"files": with_note, "env": "prod"})
        checks["a superuser without APPLY can't apply a protected database; ci's apply changes it"] = \
            root_apply[0] != 200 and "APPLY" in root_apply[1] and ci_apply[0] == 200 and refused(prod, "SELECT note FROM sales.orders") == ""

        ci_lift = denied("ci", "ALTER DATABASE pprod SET (protected = false)")
        root_lift = as_("root", "ALTER DATABASE pprod SET (protected = false)")[0] == 200
        lifted = state()
        audit_sql = 'SELECT "user", class FROM pondra.audit WHERE statement LIKE \'%protected = false%\''  # (audit.rs writes it a moment later)
        logged = until(lambda: ("root", "role") in rows_as("root", audit_sql))
        view = as_("root", "CREATE OR REPLACE VIEW sales.big AS SELECT * FROM sales.orders")[0] == 200
        again = as_("root", "ALTER DATABASE pprod SET (protected = true)")[0] == 200
        checks["lifting is an admin's, written in pondra.audit, and shown in pondra.databases until protected again"] = \
            ci_lift != "" and root_lift and lifted == [(False, True, "root")] and logged and view and again and state() == [(True, False, None)]

        prod.q("CREATE DATABASE pdev CLONE pprod")
        branched = rows(prod, "SELECT protected FROM pondra.databases WHERE name = 'pdev'")
        pdev = Node(bin, root + "/pdev", port + 21, work, token=False).start()  # (the branch's own node: a view's DROP is its own lake's)
        try:
            dropped = refused(pdev, "DROP VIEW sales.big")
        finally:
            pdev.stop()
        checks["a branch of a protected database isn't protected"] = branched == [(False,)] and dropped == "" and "project sales" in denied("root", "DROP VIEW sales.big")

        grants = [refused(prod, s) for s in ["GRANT APPLY ON SCHEMA sales TO ci", "GRANT APPLY ON TABLE sales.orders TO ci", "GRANT APPLY ON DATABASE other TO ci"]]
        checks["APPLY is a database's, and names the one it runs on"] = "APPLY is a database's" in grants[0] and "APPLY is a database's" in grants[1] and "pprod" in grants[2]
        return checks
    finally:
        prod.stop()


class Folder:
    """A `pondra serve --lakes` server on `port`: a folder of lakes, each a database, answering to its own admin
    token and reaching its bucket with its own settings (`env`). Stopped with the other nodes when the check ends."""

    def __init__(self, bin, folder, port, work, token, env):
        self.bin, self.folder, self.port, self.token, self.env = bin, folder, port, token, env
        self.log = os.path.join(work, f"folder-{port}.log")
        self.p = None

    def start(self):
        inherited = {k: v for k, v in os.environ.items() if k not in ("PONDRA_ADMIN_TOKEN", "PONDRA_TOKEN", "PONDRA_NODE_KEY")}
        with open(self.log, "a") as err:
            self.p = subprocess.Popen([self.bin, "serve", "--lakes", self.folder, "--addr", f"127.0.0.1:{self.port}", "--admin-token", self.token],
                                      env={**inherited, **self.env}, stdout=subprocess.DEVNULL, stderr=err, stdin=subprocess.DEVNULL)
        NODES.append(self)
        deadline = time.time() + 120
        while True:
            try:
                self.call("GET", "/databases", timeout=2)
                return self
            except Exception:  # (not up yet)
                if self.p.poll() is not None or time.time() > deadline:
                    raise Failed(f"serve --lakes {self.folder} didn't answer on {self.port}: {open(self.log).read()[-1500:]}")
                time.sleep(0.2)

    def call(self, method, path, body=None, timeout=120):
        data = None if body is None else json.dumps(body).encode()
        req = urllib.request.Request(f"http://127.0.0.1:{self.port}{path}", data=data, method=method,
                                     headers={"Authorization": f"Bearer {self.token}", "content-type": "application/json"})
        try:
            out = urllib.request.urlopen(req, timeout=timeout).read()
        except urllib.error.HTTPError as e:
            raise Failed(f"{method} {path}: {e.code} {e.read().decode(errors='replace')[:800]}") from None
        return json.loads(out) if out[:1] in (b"{", b"[") else out

    def q(self, sql, db=None):
        """A statement at the server's root (its default database) or at database `db`."""
        return self.call("POST", f"/db/{db}/sql" if db else "/sql", {"sql": sql})

    def stop(self, how="term"):
        if self.p and self.p.poll() is None:
            self.p.send_signal(signal.SIGKILL if how == "kill" else signal.SIGTERM)
            try:
                self.p.wait(timeout=60)
            except subprocess.TimeoutExpired:
                self.p.kill()
                self.p.wait()


def gh_calls(log):
    """Every call the fake gh logged (`project_check.FAKE_GH`): its arguments and its standard input."""
    return [json.loads(line) for line in open(log)] if os.path.exists(log) else []


def gh_given(log, *args):
    """What gh was given on standard input for the last call with these arguments (None: no such call)."""
    got = [c["stdin"] for c in gh_calls(log) if c["args"] == list(args)]
    return got[-1] if got else None


def layout_b_check(bin, work, port):
    """Branches on another server (Layout B, ADR-058, what `pondra ci init` sets up): prod and dev are two servers,
    folders of lakes with their own admin tokens and buckets. `pondra ci init` finds prod not attached on dev's
    server and prints what to run there; run as printed (with a read-only key of prod's bucket), dev attaches prod
    READ_ONLY, and the next `pondra ci init` makes dev's CI user. Then the branches are made on dev from prod with
    that user's token. Proves: dev reads prod's bucket through the read-only key only (the gates count what reached
    them); a branch made on dev is prod's rows and no file of prod's; ci_prod applies to prod; the dev token is
    refused on prod."""
    from project_check import FAKE_GH, put  # (the stand-in for gh that project_check uses, and its file writer)
    prod_url, dev_url = f"http://127.0.0.1:{port + 40}", f"http://127.0.0.1:{port + 41}"
    env_url, ro_url, moto_url = f"http://127.0.0.1:{port + 43}", f"http://127.0.0.1:{port + 44}", f"http://127.0.0.1:{port + 45}"
    key_prod, key_dev = "layout-b-prod-master-key-0123456789abcdef", "layout-b-dev-master-key-0123456789abcdef"
    counts = {"prod_by_env": 0, "prod_writes": 0, "prod_reads": 0}  # (what the gates refused or read of prod's bucket)
    os.environ.update(AWS_ENDPOINT=moto_url, AWS_ENDPOINT_URL=moto_url, AWS_ACCESS_KEY_ID="test", AWS_SECRET_ACCESS_KEY="test", AWS_REGION="us-east-1")
    proj, gh_log = os.path.join(work, "sales-b"), os.path.join(work, "gh-b.jsonl")
    fake = os.path.join(work, "fake-gh-b")
    put(fake, "gh", FAKE_GH.replace("@PYTHON@", sys.executable))
    os.chmod(os.path.join(fake, "gh"), 0o755)
    env = {**os.environ, "PONDRA_HOME": os.path.join(work, "home-b"), "GIT_CONFIG_GLOBAL": os.devnull,
           "PATH": fake + os.pathsep + os.environ["PATH"], "FAKE_GH_LOG": gh_log}
    for k in ("PONDRA_TOKEN", "PONDRA_ENV", "PONDRA_ADMIN_TOKEN"):
        env.pop(k, None)
    checks, gates, moto_proc, prod, dev = {}, [], None, None, None

    def cli(*args, token=None, ok=True):
        """`pondra` in the project folder, as a developer runs it: (exit code, what it printed). `ok` None: either."""
        e = {**env, "PONDRA_TOKEN": token} if token else env
        r = subprocess.run([bin, *args], cwd=proj, env=e, capture_output=True, text=True, timeout=900)
        if ok is not None and (r.returncode == 0) != ok:
            raise Failed(f"pondra {' '.join(args)}: exit {r.returncode}\n{r.stdout}{r.stderr}")
        return r.returncode, r.stdout + r.stderr

    def rows(n, sql, db=None):
        """The rows, or what refused them: a check reads False then, and the next ones still run."""
        try:
            return [tuple(r.values()) for r in n.q(sql, db)]
        except Failed as e:
            return str(e)[:400]

    try:
        moto_proc = moto(port + 45, work)
        gates.append(Gate(moto_url, port + 43, counts, env=True))  # (dev's own endpoint: its key reaches acme-dev only)
        gates.append(Gate(moto_url, port + 44, counts, env=False))  # (prod's read-only key: reads acme-prod, writes nothing)
        prod = Folder(bin, "s3://acme-prod", port + 40, work, "prod-admin", s3_env(moto_url, key_prod)).start()
        dev = Folder(bin, "s3://acme-dev", port + 41, work, "dev-admin", s3_env(env_url, key_dev)).start()
        put(proj, "pondra.toml", f'[project]\nname = "sales"\n\n[env.prod]\nserver = "{prod_url}"\n\n[env.dev]\nserver = "{dev_url}"\n')
        put(proj, "objects/schemas.sql", "CREATE SCHEMA sales;\n")
        put(proj, "objects/sales/orders.sql", "CREATE TABLE sales.orders (id BIGINT, amount DOUBLE);\n")
        put(proj, "migrations/001-first-orders.sql", "INSERT INTO sales.orders VALUES (1, 10.0), (2, 20.0);\n")
        cli("login", prod_url, "--token", "prod-admin")
        cli("login", dev_url, "--token", "dev-admin")

        _, out = cli("apply", "prod")
        checks["pondra apply prod: a new database on prod's server, applied, two rows"] = \
            "a new database" in out and rows(prod, "SELECT id, amount FROM sales.orders ORDER BY id", "prod") == [(1, 10.0), (2, 20.0)]

        # The first ci init: dev's server doesn't attach prod, so ci init says what to run there and makes no ci user on it.
        _, first = cli("ci", "init")
        printed = [s.strip().rstrip(";") for s in first.splitlines() if s.strip().startswith(("CREATE SECRET prod_", "ATTACH "))]
        attach = next((s for s in printed if s.startswith("ATTACH ")), "")
        link = next((s for s in printed if s.startswith("CREATE SECRET prod_link")), "")
        location = re.search(r"ATTACH '([^']+)'", attach)
        checks["pondra ci init on a dev server that doesn't attach prod: the statements to run there are printed, and no ci user is made there"] = \
            len(printed) == 3 and f"• {dev_url} doesn't attach prod yet" in first and "ci on " not in first

        # Run as printed, with prod's read-only key and the read-only gate's endpoint in prod_read (across_check's prod_ro).
        read = f"CREATE SECRET prod_read (TYPE s3, KEY_ID 'test', SECRET 'test', REGION 'us-east-1', ENDPOINT '{ro_url}', URL_STYLE 'path', SCOPE '{location.group(1)}')" if location else ""
        ran = True
        for stmt in [read, link, attach]:
            try:
                dev.q(stmt)
            except Failed as e:
                ran = False
                checks[f"(the notice's statement) {stmt[:40]}"] = str(e)[:400]
        checks["the statements printed for dev's server run there as printed (prod_read with the read-only key and endpoint filled in)"] = ran and bool(location)

        _, again = cli("ci", "init")
        dev_tok = gh_given(gh_log, "secret", "set", "PONDRA_DEV_TOKEN")
        checks["pondra ci init again: dev's server attaches prod, so ci is made there, and its token is PONDRA_DEV_TOKEN in gh"] = \
            f"ci on {dev_url}" in again and bool(dev_tok) and dev_tok.startswith("pt_")
        granted = [(g, p.upper(), k, n) for g, p, k, n in rows(dev, "SELECT grantee, privilege, on_kind, on_name FROM pondra.grants")]
        checks["ci holds CLONE on prod on dev's server, where prod is attached (pondra.grants)"] = ("ci", "CLONE", "database", "prod") in granted

        # A pull request adds a view. Its branch is made on dev from prod with ci's token: prod's rows, and no file of
        # prod's bucket copied. (prod's rows go into its files first: a branch takes the log rows not yet in a file
        # and tiers them into its own.)
        put(proj, "objects/sales/big.sql", "CREATE VIEW sales.big AS SELECT * FROM sales.orders WHERE amount >= 15;\n")
        prod.q("CHECKPOINT", "prod")
        made_code, made = cli("apply", "pr-1", "--fresh", token=dev_tok, ok=None)
        branch = rows(dev, "SELECT id, amount FROM sales.orders ORDER BY id", "pr_1")
        checks["pr-1 is made on dev's server from prod with ci's token: prod's rows, its folder in acme-dev with no data file, and nothing written to prod's bucket"] = \
            branch == [(1, 10.0), (2, 20.0)] and parquet("s3://acme-dev/pr_1") == [] and counts["prod_writes"] == 0 \
            or f"{branch}, files {parquet('s3://acme-dev/pr_1')}, writes {counts['prod_writes']}\n{made[-600:]}"
        checks["pondra apply pr-1 --fresh with ci's token on dev: the view added (exit 0)"] = \
            made_code == 0 and rows(dev, "SELECT id FROM sales.big ORDER BY id", "pr_1") == [(2,)] or made[-600:]
        diff_code, diffed = cli("diff", "pr-1", token=dev_tok, ok=None)
        checks["pondra diff pr-1 with ci's token on dev: the new view, against prod's objects read through its attachment there (exit 0)"] = \
            diff_code == 0 and "- `+` objects/sales/big.sql" in diffed and "Objects: prod can't" not in diffed or diffed[-800:]

        # prod: ci_prod applies the merged files; the dev token is refused there.
        prod_tok = gh_given(gh_log, "secret", "set", "PONDRA_PROD_TOKEN", "--env", "prod")
        applied, _ = cli("apply", "prod", token=prod_tok, ok=None)
        big = rows(prod, "SELECT id FROM sales.big ORDER BY id", "prod")
        checks["with PONDRA_PROD_TOKEN (ci_prod), pondra apply prod adds the view, which reads prod's rows"] = applied == 0 and big == [(2,)]
        refused, _ = cli("apply", "prod", token=dev_tok, ok=None)
        checks["with the dev token, pondra apply prod is refused"] = refused != 0

        checks["the gates: dev's own key never reached prod's bucket, nothing wrote it, and the read-only key read it"] = \
            counts["prod_by_env"] == 0 and counts["prod_writes"] == 0 and counts["prod_reads"] > 0
        return checks
    finally:
        for n in (dev, prod):
            if n:
                n.stop()
        for g in gates:
            g.close()
        if moto_proc:
            moto_proc.terminate()
            moto_proc.wait()


def owners_check(bin, work, port, root):
    """Who makes a branch and who may drop it (ADR-058): CLONE is enough for a user to branch a database,
    and the branch's owner is a superuser in it; CLONE ON SCHEMA takes those schemas only; a branch is
    dropped by its owner (or an admin), by nobody else; and on a server with another
    database attached, GRANT CLONE ON DATABASE names that one, and its grant lets a user branch it."""
    oprod = Node(bin, root + "/oprod", port + 30, work).start()  # (an admin token: a node of its own, as a server's)
    basic = {u: "Basic " + base64.b64encode(f"{u}:{u}-password-1".encode()).decode() for u in ["ali", "bob", "sam", "cleo"]}

    def rows(n, sql):
        return [tuple(r.values()) for r in n.q(sql)]

    def refused(n, sql):
        try:
            n.q(sql)
            return ""
        except Failed as e:
            return str(e)

    def as_(n, user, sql):
        """A statement as a user signs in to node `n` (Basic): (status, text)."""
        return raw(n.port, "POST", "/sql", sql, {"Authorization": basic[user]})

    def denied(n, user, sql):
        """What a user's statement is refused with on `n` ('' when it runs)."""
        status, text = as_(n, user, sql)
        return "" if status == 200 else text

    checks = {}
    try:
        oprod.q("CREATE SCHEMA sales")
        oprod.q("CREATE SCHEMA hr")
        oprod.q("CREATE TABLE sales.orders (id BIGINT, amount DOUBLE)")
        oprod.q("INSERT INTO sales.orders VALUES (1, 1.5)")
        oprod.q("CREATE TABLE hr.pay (id BIGINT, amount DOUBLE)")
        oprod.q("INSERT INTO hr.pay VALUES (1, 100.0)")
        for u in basic:
            oprod.q(f"CREATE USER {u} PASSWORD '{u}-password-1'")
        oprod.q("GRANT CLONE ON DATABASE oprod TO ali")
        oprod.q("GRANT CLONE ON DATABASE oprod TO bob")
        oprod.q("GRANT CLONE ON SCHEMA sales TO cleo")
        oprod.q("GRANT SELECT ON ALL TABLES TO sam")

        made = as_(oprod, "ali", "CREATE DATABASE ali_b CLONE oprod")[0]
        checks["a user with only CLONE makes a branch of prod, and pondra.databases names it its owner"] = \
            made == 200 and rows(oprod, "SELECT owner FROM pondra.databases WHERE name = 'ali_b'") == [("ali",)]

        loc = rows(oprod, "SELECT location FROM pondra.databases WHERE name = 'ali_b'")[0][0]
        branch = Node(bin, loc, port + 31, work).start()  # (the branch's own node, with the same admin token as oprod's)
        try:
            made_table = [as_(branch, "ali", s)[0] for s in ["CREATE TABLE mine (x INT)", "INSERT INTO mine VALUES (1)"]]
            superuser = rows(branch, "SELECT superuser FROM pondra.users WHERE name = 'ali'")
        finally:
            branch.stop()
        checks["the owner is a superuser in its branch: it makes a table there"] = made_table == [200, 200] and superuser == [(True,)]

        checks["a user without CLONE is refused, naming CLONE"] = "CLONE" in denied(oprod, "sam", "CREATE DATABASE sam_b CLONE oprod")

        cleo_b = as_(oprod, "cleo", "CREATE DATABASE cleo_b CLONE oprod")[0]
        took_sales = cleo_b == 200 and rows(oprod, "SELECT id FROM cleo_b.sales.orders") == [(1,)] and refused(oprod, "SELECT * FROM cleo_b.hr.pay") != ""
        checks["CLONE ON SCHEMA sales: the branch takes sales only, and naming hr is refused"] = \
            took_sales and "sales" in denied(oprod, "cleo", "CREATE DATABASE cleo_c CLONE oprod WITH (schemas = (hr))")

        by_sam, by_bob = denied(oprod, "sam", "DROP DATABASE ali_b"), denied(oprod, "bob", "DROP DATABASE ali_b")
        checks["another user can't drop a branch, even one who may clone its base (bob) and one who may not (sam), and the refusal names its owner"] = \
            "ali's branch" in by_sam and "ali's branch" in by_bob and "ali_b" in [r[0] for r in rows(oprod, "SELECT name FROM pondra.databases")]
        checks["the owner drops its own branch"] = as_(oprod, "cleo", "DROP DATABASE cleo_b")[0] == 200 and as_(oprod, "ali", "DROP DATABASE ali_b")[0] == 200
        checks["DROP DATABASE IF EXISTS of one that isn't there: nothing, for a user too; without IF EXISTS, refused"] = \
            as_(oprod, "bob", "DROP DATABASE IF EXISTS never_made")[0] == 200 and "no database" in denied(oprod, "bob", "DROP DATABASE never_made")

        other = Node(bin, root + "/other", port + 32, work).start()  # (another lake, made by a node, then attached here)
        other.stop()
        oprod.q(f"ATTACH '{root}/other' AS other")
        granted = refused(oprod, "GRANT CLONE ON DATABASE other TO sam") == ""
        shown = rows(oprod, "SELECT on_kind, on_name FROM pondra.grants WHERE grantee = 'sam' AND privilege = 'CLONE'")
        checks["GRANT CLONE ON DATABASE names this database or one attached here, and pondra.grants shows it"] = \
            granted and shown == [("database", "other")] and refused(oprod, "GRANT APPLY ON DATABASE other TO sam") != "" \
            and refused(oprod, "GRANT CLONE ON DATABASE nowhere TO sam") != ""
        return checks
    finally:
        oprod.stop()


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--new", default=os.path.join(HERE, "..", "target", "release", "pondra"), help="this build's binary")
    ap.add_argument("--work", default="", help="where the lakes and logs go (default: a new temporary folder, removed if every check passes)")
    ap.add_argument("--port", type=int, default=9780)
    ap.add_argument("--s3", action="store_true", help="the lakes on s3://$PONDRA_BUCKET")
    ap.add_argument("--across", action="store_true", help="branches across servers (ADR-058), alone: moto and two gates, no other check")
    a = ap.parse_args()
    work = a.work or tempfile.mkdtemp(prefix="pondra-environments-")
    os.makedirs(work, exist_ok=True)
    root = f"s3://{os.environ['PONDRA_BUCKET']}/environments-{os.getpid()}-{int(time.time())}" if a.s3 else work
    new = os.path.abspath(a.new)
    parts = [lambda: across_check(new, work, a.port)] if a.across else [
        lambda: environments_check(new, work, a.port, root), lambda: signed_in_check(new, work, a.port, root),
        lambda: protect_check(new, work, a.port, root), lambda: layout_b_check(new, work, a.port), lambda: owners_check(new, work, a.port, root)]
    checks = {}
    try:
        for i, part in enumerate(parts):
            try:
                checks.update(part())
            except Failed as e:  # (one part stopped: the others still run, and say what they found)
                checks[f"part {i + 1} ran to the end"] = str(e)[:1000]
                stop_all()
    finally:
        stop_all()
        if a.s3:
            gone(root)
    ok = all(v is True for v in checks.values())
    print(json.dumps({**checks, "ok": ok}, indent=1))
    if ok and not a.work:
        shutil.rmtree(work, ignore_errors=True)
    elif not ok:
        print(f"(the lakes and node logs kept in {work})", file=sys.stderr)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
