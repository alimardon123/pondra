#!/usr/bin/env python3
"""Postgres to Debezium Server to Pondra, checked exactly once: a Pondra table equals its Postgres source row for row
through an initial snapshot, 50,000 mixed changes, a SIGKILL of Debezium, a SIGKILL of the Pondra node, a clean
Debezium stop and start, and a TRUNCATE on the source.

  cdc_check.py [--debezium DIR] [--pg-bin DIR] [--bin PATH] [--values both|json|json-schema] [--ops N]
               [--fault-ops N] [--base-port 9930] [--pg-user nobody] [--timeout SECS] [--forward-truncate]
               [--work DIR] [--keep]

Debezium Server: --debezium DIR, or DEBEZIUM_HOME (the unpacked debezium-server-dist tarball). Postgres 16 binaries:
--pg-bin DIR, or PG_BIN, or PATH. Without either the check prints `skipped: ...` and exits 0 (CI has no Debezium).
Postgres is initdb'd and started by the check on 127.0.0.1 (as --pg-user when run as root: Postgres refuses root).
Everything it starts uses --base-port and the three ports after it (Postgres, the Pondra node's HTTP, its Kafka port,
Debezium's HTTP health), and is stopped by the PID the check started it with.

The source: shop.orders (id bigint PRIMARY KEY, customer text, amount numeric(12,2), paid boolean, placed timestamptz,
note jsonb, ref uuid) and shop.items (order_id bigint, line int, sku text, qty int, unit_price numeric(12,2), PRIMARY
KEY (order_id, line)). Pondra has the same tables with the same keys. Topics are `pg.shop.orders` and `pg.shop.items`
in Debezium, routed by its RegexRouter transform to `shop.orders` and `shop.items`, which are Pondra's tables.

The phases, each printing `ok <phase>` or `FAIL <phase>: <first differences>`:
  snapshot       10,000 rows of each table are seeded before Debezium starts: they arrive as the snapshot (op r)
  stream         --ops changes while Debezium streams: inserts, updates (some twice in a row on one key), deletes,
                 an update that changes a primary key, a transaction of 500 changes; a probe row every 100 changes
                 for the lag; the rate from the first change committed to Pondra equal
  kill-debezium  SIGKILL Debezium mid-stream (at 30% of --fault-ops), restarted 2 s later
  kill-pondra    SIGKILL the Pondra node mid-stream, restarted 3 s later on the same lake
  clean-restart  Debezium stopped cleanly (SIGTERM, offsets flushed) mid-stream, started 2 s later
  truncate       TRUNCATE shop.items on the source, last, so the phases above are not affected by it. It isn't carried:
                 Debezium's default skipped.operations is `t` (it drops TRUNCATE), and Pondra doesn't apply one it is
                 sent (--forward-truncate sets skipped.operations=none). The check proves Pondra keeps its rows, then
                 that the same TRUNCATE in Pondra makes the tables equal again
  after-truncate --fault-ops more changes: the stream goes on past the truncate, each change applied once
After each phase the check waits until the Pondra tables equal the source (fingerprints first, then every key and
every column, up to --timeout), and reports the time. Values are normalized only where one value is written two ways:
amount as text with two places (Decimal), placed as UTC microseconds since the epoch (both sides), jsonb as JSON with
sorted keys (Pondra keeps jsonb as text), uuid lower-cased. Exit 0 when every phase is ok, 1 otherwise.
"""
import argparse, collections, datetime, glob, http.client, json, math, os, pwd, random, shutil, socket, subprocess, sys, tempfile, threading, time, uuid
from decimal import Decimal

import psycopg
from psycopg.rows import dict_row

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROBE_BASE = 1_000_000_000  # probe rows' ids: the lag sample, never updated by the workload
PROBE_EVERY = 100
TXN_CHANGES = 500
SEED_ROWS = 10_000
T0 = datetime.datetime(2020, 1, 1, tzinfo=datetime.timezone.utc)
NAMES = ["ann", "bob", "cy", "dee", "eve", "fay", "gus", "hal", "Zoë", "李", "o'brien", 'x"y']
ORDER_COLS = ["customer", "amount", "paid", "placed", "note", "ref"]
ITEM_COLS = ["sku", "qty", "unit_price"]
CASTS = {"amount": "::numeric", "unit_price": "::numeric", "note": "::jsonb", "ref": "::uuid", "placed": "::timestamptz"}
DDL = [
    "CREATE SCHEMA shop",
    "CREATE TABLE shop.orders (id bigint PRIMARY KEY, customer text, amount numeric(12,2), paid boolean, placed timestamptz, note jsonb, ref uuid)",
    "CREATE TABLE shop.items (order_id bigint, line int, sku text, qty int, unit_price numeric(12,2), PRIMARY KEY (order_id, line))",
]
# the same fingerprint on both sides: counts and sums only (a cheap catch-up test; the full comparison decides)
FP = {
    "orders": "SELECT count(*) AS n, coalesce(sum(id), 0) AS ids, coalesce(sum(length(customer)), 0) AS names, "
              "coalesce(sum(CASE WHEN paid THEN 1 ELSE 0 END), 0) AS paid, coalesce(sum(CAST(amount * 100 AS BIGINT)), 0) AS cents FROM shop.orders",
    "items": "SELECT count(*) AS n, coalesce(sum(order_id), 0) AS o, coalesce(sum(line), 0) AS l, coalesce(sum(qty), 0) AS q, "
             "coalesce(sum(CAST(unit_price * 100 AS BIGINT)), 0) AS p FROM shop.items",
}
PG_SELECT = {  # Postgres's side of the full comparison: placed as microseconds since the epoch, jsonb and uuid as text
    "orders": "SELECT id, customer, amount::text AS amount, paid, (extract(epoch FROM placed) * 1000000)::bigint AS placed_us, note::text AS note, ref::text AS ref FROM shop.orders",
    "items": "SELECT order_id, line, sku, qty, unit_price::text AS unit_price FROM shop.items",
}
PD_SELECT = {  # Pondra's side: its casts to the same forms (Pondra keeps jsonb and uuid as text)
    "orders": "SELECT id, customer, CAST(amount AS VARCHAR) AS amount, paid, CAST(placed AS BIGINT) AS placed_us, note, ref FROM shop.orders",
    "items": "SELECT order_id, line, sku, qty, CAST(unit_price AS VARCHAR) AS unit_price FROM shop.items",
}


# ---------------------------------------------------------------- finding what runs

def find_debezium(path):
    """The unpacked distribution's folder, or None: it has the runner jar and lib/."""
    if not path or not os.path.isdir(path):
        return None
    if not glob.glob(os.path.join(path, "debezium-server-*-runner.jar")) or not os.path.isdir(os.path.join(path, "lib")):
        return None
    return os.path.abspath(path)


def find_pg_bin(path):
    """The folder of initdb, pg_ctl and postgres: --pg-bin, PG_BIN, else the one on PATH; None if not there."""
    for c in (path, os.environ.get("PG_BIN"), os.path.dirname(shutil.which("pg_ctl") or "") or None):
        if c and all(os.path.isfile(os.path.join(c, b)) for b in ("initdb", "pg_ctl", "postgres")):
            return os.path.abspath(c)
    return None


def port_free(port):
    s = socket.socket()
    try:
        s.bind(("127.0.0.1", port))
        return True
    except OSError:
        return False
    finally:
        s.close()


def http_status(port, path, timeout=2):
    c = http.client.HTTPConnection("127.0.0.1", port, timeout=timeout)
    try:
        c.request("GET", path)
        return c.getresponse().status
    finally:
        c.close()


def wait_for(pred, secs, what, proc=None):
    deadline = time.time() + secs
    while time.time() < deadline:
        if proc is not None and proc.poll() is not None:
            raise RuntimeError(f"{what} exited with {proc.returncode}")
        try:
            if pred():
                return
        except Exception:
            pass
        time.sleep(0.2)
    raise RuntimeError(f"{what} did not come up in {secs} s")


def pondra_sql(port, q, timeout=60):
    c = http.client.HTTPConnection("127.0.0.1", port, timeout=timeout)
    try:
        c.request("POST", "/sql", q.encode())
        r = c.getresponse()
        body = r.read()
    finally:
        c.close()
    if r.status != 200:
        raise RuntimeError(f"Pondra refused {q[:60]!r}: {r.status} {body[:300]!r}")
    return json.loads(body) if body[:1] in (b"[", b"{") else body


def popen(args, log, cwd, user=None, group=None, env=None):
    kwargs = {"user": user, "group": group} if user is not None else {}
    return subprocess.Popen(args, stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT, cwd=cwd, env=env, **kwargs)


def java_env():
    """This process's environment, minus the NO_PROXY entries with a wildcard (`*.svc.cluster.local`): the Kubernetes
    client inside Debezium Server rejects them ("NO_PROXY URL contains invalid entry") and never starts. The proxy
    settings stay as they are."""
    env = dict(os.environ)
    for key in ("NO_PROXY", "no_proxy"):
        if key in env:
            env[key] = ",".join(e for e in env[key].split(",") if e.strip() and not e.strip().startswith("*"))
    return env


def run(args, cwd, user=None, group=None):
    r = subprocess.run(args, cwd=cwd, user=user, group=group, capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(f"{os.path.basename(args[0])} failed ({r.returncode}): {(r.stderr or r.stdout)[-600:]}")
    return r


# ---------------------------------------------------------------- the processes

class Postgres:
    """A Postgres 16 cluster this check made: wal_level=logical, scram on TCP, track_commit_timestamp for the lag."""

    def __init__(self, bin_dir, work, port, uid, gid):
        self.bin, self.work, self.port, self.uid, self.gid = bin_dir, work, port, uid, gid
        self.data, self.sock, self.log = os.path.join(work, "pg-data"), os.path.join(work, "pg-sock"), os.path.join(work, "postgres.log")
        self.pw = os.urandom(12).hex()
        self.dbz_pw = os.urandom(12).hex()
        self.started = False

    def tool(self, name):
        return os.path.join(self.bin, name)

    def init(self):
        os.makedirs(self.sock)
        pwfile = os.path.join(self.work, "pg-pw")
        with open(pwfile, "w") as f:
            f.write(self.pw + "\n")
        os.chmod(pwfile, 0o600)
        if self.uid is not None:
            for p in (pwfile, self.sock):
                os.chown(p, self.uid, self.gid)
        run([self.tool("initdb"), "-D", self.data, "-U", "cdc", "--pwfile", pwfile, "--auth-host=scram-sha-256",
             "--auth-local=trust", "-E", "UTF8", "--no-locale"], self.work, self.uid, self.gid)
        with open(os.path.join(self.data, "postgresql.conf"), "a") as f:
            f.write(f"\nlisten_addresses = '127.0.0.1'\nport = {self.port}\nunix_socket_directories = '{self.sock}'\n"
                    "wal_level = logical\nmax_replication_slots = 4\nmax_wal_senders = 4\ntrack_commit_timestamp = on\n")

    def start(self):
        run([self.tool("pg_ctl"), "-D", self.data, "-l", self.log, "-w", "start"], self.work, self.uid, self.gid)
        self.started = True

    def connect(self, autocommit=True):
        return psycopg.connect(host="127.0.0.1", port=self.port, user="cdc", password=self.pw, dbname="postgres",
                               autocommit=autocommit, row_factory=dict_row)

    def stop(self):
        if self.started:
            subprocess.run([self.tool("pg_ctl"), "-D", self.data, "-m", "fast", "-w", "stop"], cwd=self.work,
                           user=self.uid, group=self.gid, capture_output=True, text=True)
            self.started = False


class Pondra:
    """A Pondra node on one lake: `serve --lake`, its HTTP port and its Kafka port, both on 127.0.0.1."""

    def __init__(self, bin_path, work, http_port, kafka_port):
        self.bin, self.work, self.http, self.kafka = bin_path, work, http_port, kafka_port
        self.lake = os.path.join(work, "lake")
        self.log = os.path.join(work, "pondra.log")
        self.proc = None

    def start(self):
        log = open(self.log, "ab")
        self.proc = popen([self.bin, "serve", "--lake", self.lake, "--addr", f"127.0.0.1:{self.http}",
                           "--kafka", f"127.0.0.1:{self.kafka}", "--changelog-secs", "86400"], log, self.work)
        wait_for(lambda: http_status(self.http, "/stats") == 200, 120, "pondra", self.proc)

    def kill(self):
        if self.proc is not None and self.proc.poll() is None:
            self.proc.kill()  # SIGKILL
            self.proc.wait()

    def sql(self, q, timeout=60):
        return pondra_sql(self.http, q, timeout)


class Debezium:
    """Debezium Server 3.7.0.Final, run from the work folder (not from its own folder) with the properties file."""

    def __init__(self, dbz_dir, work, props_path, health_port):
        self.dbz, self.work, self.conf, self.health = dbz_dir, work, props_path, health_port
        self.log = os.path.join(work, "debezium.log")
        self.proc = None

    def start(self):
        """Run from the folder that holds config/application.properties, as run.sh does (Quarkus reads it from there:
        with -Dquarkus.config.locations the Postgres source is not found as the default engine)."""
        runner = sorted(glob.glob(os.path.join(self.dbz, "debezium-server-*-runner.jar")))[0]
        java = shutil.which("java")
        if java is None:
            raise RuntimeError("java is not on PATH")
        cmd = [java, "-cp", f"{runner}:{os.path.join(self.dbz, 'config', 'lib')}:{os.path.join(self.dbz, 'lib')}/*",
               "io.debezium.server.Main"]
        self.proc = popen(cmd, open(self.log, "ab"), os.path.dirname(os.path.dirname(self.conf)), env=java_env())

    def kill(self):
        """SIGKILL: no offsets flushed, no goodbye."""
        if self.proc is not None and self.proc.poll() is None:
            self.proc.kill()
            self.proc.wait()

    def stop(self):
        """SIGTERM: a clean stop; Debezium flushes its offsets before it exits."""
        if self.proc is not None and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=120)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()

    def alive(self):
        return self.proc is not None and self.proc.poll() is None


def properties(ctx, schemas):
    """The Debezium Server application.properties for one value format (schemas: the Kafka Connect wrapper or not)."""
    s = "true" if schemas else "false"
    return "\n".join([
        "debezium.sink.type=kafka",
        f"debezium.sink.kafka.producer.bootstrap.servers=127.0.0.1:{ctx['kafka']}",
        "debezium.sink.kafka.producer.enable.idempotence=true",
        "debezium.sink.kafka.producer.acks=all",
        "debezium.sink.kafka.producer.key.serializer=org.apache.kafka.common.serialization.StringSerializer",
        "debezium.sink.kafka.producer.value.serializer=org.apache.kafka.common.serialization.StringSerializer",
        "debezium.format.key=json",
        f"debezium.format.key.schemas.enable={s}",
        "debezium.format.value=json",
        f"debezium.format.value.schemas.enable={s}",
        "debezium.source.connector.class=io.debezium.connector.postgresql.PostgresConnector",
        "debezium.source.database.hostname=127.0.0.1",
        f"debezium.source.database.port={ctx['pg']}",
        "debezium.source.database.user=dbz",
        f"debezium.source.database.password={ctx['dbz_pw']}",
        "debezium.source.database.dbname=postgres",
        "debezium.source.topic.prefix=pg",
        "debezium.source.table.include.list=shop.orders,shop.items",
        "debezium.source.plugin.name=pgoutput",
        "debezium.source.slot.name=pondra_cdc",
        "debezium.source.publication.name=pondra_pub",
        "debezium.source.publication.autocreate.mode=disabled",
        "debezium.source.snapshot.mode=initial",
        *(["debezium.source.skipped.operations=none"] if ctx["forward_truncate"] else []),
        "debezium.source.decimal.handling.mode=string",
        "debezium.source.offset.storage=org.apache.kafka.connect.storage.FileOffsetBackingStore",
        f"debezium.source.offset.storage.file.filename={os.path.join(ctx['work'], 'debezium-offsets.dat')}",
        "debezium.source.offset.flush.interval.ms=1000",
        "debezium.transforms=route",
        "debezium.transforms.route.type=org.apache.kafka.connect.transforms.RegexRouter",
        "debezium.transforms.route.regex=pg[.](.*)",
        "debezium.transforms.route.replacement=$1",
        f"quarkus.http.port={ctx['dbz_http']}",
        "quarkus.log.console.json=false",
        "",
    ])


# ---------------------------------------------------------------- the source's changes

def money(rng):
    return Decimal(rng.randrange(0, 10 ** 6)).scaleb(-2)


def gen_value(rng, col):
    """One column's value: NULL one time in twenty, otherwise a value of the column's type."""
    if rng.random() < 0.05:
        return None
    if col == "customer":
        return rng.choice(NAMES) + str(rng.randrange(1000))
    if col in ("amount", "unit_price"):
        return money(rng)
    if col == "paid":
        return rng.random() < 0.5
    if col == "placed":
        return T0 + datetime.timedelta(microseconds=rng.randrange(0, 3 * 365 * 86400 * 10 ** 6))
    if col == "note":
        return json.dumps({f"k{rng.randrange(5)}": rng.randrange(1000) for _ in range(rng.randrange(1, 4))})
    if col == "ref":
        return str(uuid.UUID(int=rng.getrandbits(128), version=4))
    if col == "sku":
        return f"SKU-{rng.randrange(100000):05d}"
    if col == "qty":
        return rng.randrange(0, 100)
    raise KeyError(col)


# how a statement is counted in the stream phase's mix: the first prefix its SQL starts with
MIX = [("INSERT INTO shop.orders", "insert order"), ("INSERT INTO shop.items", "insert item"),
       ("UPDATE shop.orders SET id", "update of a primary key"), ("UPDATE shop.orders", "update order"),
       ("UPDATE shop.items", "update item"), ("DELETE FROM shop.orders", "delete order"),
       ("DELETE FROM shop.items", "delete item")]


class Source:
    """Generates the changes. It keeps the live keys only, so every statement is valid on the source. One change in
    five on an updated row is followed by another update of the same row."""

    def __init__(self, rng):
        self.rng = rng
        self.orders, self.items, self.next_order, self.next_line, self.repeat = [], [], 1, {}, None
        self.mix = collections.Counter()  # the statements generated, by kind

    def seeded(self, n):
        self.orders = list(range(1, n + 1))
        self.next_order = n + 1
        self.items = [(i, 1) for i in range(1, n + 1)]
        self.next_line = {i: 2 for i in range(1, n + 1)}

    def change(self):
        """One statement as (sql, params)."""
        sql, params = self._change()
        kind = next(k for prefix, k in MIX if sql.startswith(prefix))
        self.mix[kind] += 1
        return sql, params

    def _change(self):
        rng = self.rng
        if self.repeat is not None:
            kind, key = self.repeat
            self.repeat = None
            return self.update_order(key) if kind == "order" else self.update_item(key)
        r = rng.random()
        if r < 0.25 or not self.orders:
            return self.insert_order()
        if r < 0.50:
            return self.update_order(self.orders[rng.randrange(len(self.orders))])
        if r < 0.52:
            return self.pk_change()
        if r < 0.60:
            return self.delete_order()
        if r < 0.75:
            return self.insert_item()
        if r < 0.90 and self.items:
            return self.update_item(self.items[rng.randrange(len(self.items))])
        if self.items:
            return self.delete_item()
        return self.insert_item()

    def _take(self, lst):
        """A random live key, removed from the list (swap with the last)."""
        i = self.rng.randrange(len(lst))
        lst[i], lst[-1] = lst[-1], lst[i]
        return lst.pop()

    def insert_order(self):
        oid = self.next_order
        self.next_order += 1
        self.orders.append(oid)
        vals = [gen_value(self.rng, c) for c in ORDER_COLS]
        cols = ["id"] + ORDER_COLS
        marks = ", ".join("%s" + CASTS.get(c, "") for c in cols)
        return f"INSERT INTO shop.orders ({', '.join(cols)}) VALUES ({marks})", [oid] + vals

    def update_order(self, oid):
        rng = self.rng
        cols = rng.sample(ORDER_COLS, rng.randrange(1, 4))
        if rng.random() < 0.2:
            self.repeat = ("order", oid)
        sets = ", ".join(f"{c} = %s{CASTS.get(c, '')}" for c in cols)
        return f"UPDATE shop.orders SET {sets} WHERE id = %s", [gen_value(rng, c) for c in cols] + [oid]

    def pk_change(self):
        """An update of the primary key: the row moves to a new id (the items keep the old order id)."""
        old = self._take(self.orders)
        new = self.next_order
        self.next_order += 1
        self.orders.append(new)
        return "UPDATE shop.orders SET id = %s WHERE id = %s", [new, old]

    def delete_order(self):
        return "DELETE FROM shop.orders WHERE id = %s", [self._take(self.orders)]

    def insert_item(self):
        if not self.orders:
            return self.insert_order()
        oid = self.orders[self.rng.randrange(len(self.orders))]
        line = self.next_line.get(oid, 1)
        self.next_line[oid] = line + 1
        self.items.append((oid, line))
        vals = [gen_value(self.rng, c) for c in ITEM_COLS]
        cols = ["order_id", "line"] + ITEM_COLS
        marks = ", ".join("%s" + CASTS.get(c, "") for c in cols)
        return f"INSERT INTO shop.items ({', '.join(cols)}) VALUES ({marks})", [oid, line] + vals

    def update_item(self, key):
        rng = self.rng
        cols = rng.sample(ITEM_COLS, rng.randrange(1, 3))
        if rng.random() < 0.2:
            self.repeat = ("item", key)
        sets = ", ".join(f"{c} = %s{CASTS.get(c, '')}" for c in cols)
        return (f"UPDATE shop.items SET {sets} WHERE order_id = %s AND line = %s",
                [gen_value(rng, c) for c in cols] + [key[0], key[1]])

    def delete_item(self):
        oid, line = self._take(self.items)
        return "DELETE FROM shop.items WHERE order_id = %s AND line = %s", [oid, line]


class Progress:
    """The changes committed so far, shared with the thread that fault injection watches."""

    def __init__(self):
        self.n, self.first, self.last, self.error = 0, None, None, None

    def bump(self, k):
        now = time.time()
        if self.first is None:
            self.first = now
        self.last = now
        self.n += k


def run_changes(conn, src, n, prog, txn_at=None, probes=None):
    """`n` changes, each statement autocommitted (a transaction's changes count one each): a probe row every
    PROBE_EVERY changes when `probes` is a list (it gets (id, commit time) of each), and one transaction of
    TXN_CHANGES changes once `txn_at` changes are done."""
    try:
        with conn.cursor() as cur:
            txn_pending = txn_at is not None
            while prog.n < n:
                if txn_pending and prog.n >= txn_at:
                    txn_pending = False
                    with conn.transaction():
                        for _ in range(TXN_CHANGES):
                            cur.execute(*src.change())
                    prog.bump(TXN_CHANGES)
                    continue
                if probes is not None and (prog.n + 1) % PROBE_EVERY == 0:
                    k = (prog.n + 1) // PROBE_EVERY
                    pid = PROBE_BASE + k
                    cur.execute("INSERT INTO shop.orders (id, customer) VALUES (%s, %s)", (pid, f"probe-{k}"))
                    probes.append((pid, time.time()))
                else:
                    cur.execute(*src.change())
                prog.bump(1)
    except Exception as e:  # reported by the phase, which stops
        prog.error = e


class Sampler(threading.Thread):
    """Reads the probe rows out of Pondra every 20 ms: when each first reads back."""

    def __init__(self, port):
        super().__init__(daemon=True)
        self.port, self.seen, self.halt = port, {}, threading.Event()

    def run(self):
        while not self.halt.is_set():
            try:
                now = time.time()
                for r in pondra_sql(self.port, f"SELECT id FROM shop.orders WHERE id >= {PROBE_BASE}"):
                    self.seen.setdefault(int(r["id"]), now)
            except Exception:
                pass  # the node is down (a fault phase): try again
            self.halt.wait(0.02)


# ---------------------------------------------------------------- comparing

def dec2(v):
    return None if v is None else str(Decimal(str(v)).quantize(Decimal("0.01")))


def jnorm(v):
    """jsonb as sorted JSON text. Pondra keeps jsonb as text, Postgres as jsonb: the same document."""
    if v is None:
        return None
    try:
        obj = json.loads(v) if isinstance(v, str) else v
    except ValueError:
        return "!" + str(v)
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def norm_order(r):
    ref = r.get("ref")
    return (r.get("customer"), dec2(r.get("amount")), None if r.get("paid") is None else bool(r["paid"]),
            None if r.get("placed_us") is None else int(r["placed_us"]), jnorm(r.get("note")),
            None if ref is None else str(ref).lower())


def norm_item(r):
    return (r.get("sku"), None if r.get("qty") is None else int(r["qty"]), dec2(r.get("unit_price")))


KEYS = {"orders": lambda r: int(r["id"]), "items": lambda r: (int(r["order_id"]), int(r["line"]))}
NORM = {"orders": norm_order, "items": norm_item}


def pg_rows(conn, table):
    return conn.execute(PG_SELECT[table]).fetchall()


def diff_table(name, pg, pdn):
    """What differs between the two sides of one table, as sentences (empty when equal)."""
    keyf, normf = KEYS[name], NORM[name]
    out = []
    pd_keys = [keyf(r) for r in pdn]
    if len(pd_keys) != len(set(pd_keys)):
        out.append(f"{name}: {len(pd_keys) - len(set(pd_keys))} key(s) in Pondra held twice")
    a = {keyf(r): normf(r) for r in pg}
    b = {keyf(r): normf(r) for r in pdn}
    missing = [k for k in a if k not in b]
    extra = [k for k in b if k not in a]
    differ = [k for k in a if k in b and a[k] != b[k]]
    if missing:
        out.append(f"{name}: {len(missing)} row(s) in Postgres not in Pondra (first {missing[:3]})")
    if extra:
        out.append(f"{name}: {len(extra)} row(s) in Pondra not in Postgres (first {extra[:3]})")
    for k in differ[:3]:
        out.append(f"{name}: key {k} differs: Postgres {a[k]} Pondra {b[k]}")
    if len(differ) > 3:
        out.append(f"{name}: {len(differ)} rows differ in all")
    return out


def compare(ctx):
    """(differences, row counts) of both tables: every key, every column, duplicates included."""
    diffs, counts = [], {}
    for name in ("orders", "items"):
        pg = pg_rows(ctx["chk"], name)
        pdn = ctx["pondra"].sql(PD_SELECT[name])
        counts[name] = (len(pg), len(pdn))
        diffs += diff_table(name, pg, pdn)
    return diffs, counts


def log_counts(kafka_port, topics):
    """The records in each topic's log, read from the start (Pondra keeps the log for --changelog-secs): (records,
    tombstones) for each topic. Reading stops after three seconds with no record."""
    from confluent_kafka import Consumer, TopicPartition, OFFSET_BEGINNING
    c = Consumer({"bootstrap.servers": f"127.0.0.1:{kafka_port}", "group.id": f"cdc-check-{uuid.uuid4().hex[:8]}",
                  "enable.auto.commit": False})
    c.assign([TopicPartition(t, 0, OFFSET_BEGINNING) for t in topics])
    counts = {t: [0, 0] for t in topics}
    idle = 0
    while idle < 3:
        m = c.poll(1.0)
        if m is None or m.error() is not None:
            idle += 1
            continue
        idle = 0
        counts[m.topic()][0] += 1
        counts[m.topic()][1] += m.value() is None
    c.close()
    return {t: tuple(v) for t, v in counts.items()}


def fingerprint(ctx):
    pg = tuple(int(v) for name in ("orders", "items") for v in ctx["chk"].execute(FP[name]).fetchone().values())
    pd = tuple(int(v) for name in ("orders", "items") for v in ctx["pondra"].sql(FP[name])[0].values())
    return pg, pd


def settle(ctx, timeout):
    """Wait until Pondra equals the source: returns (seconds to equal or None, differences, counts)."""
    t0 = time.time()
    diffs, counts = ["never compared"], {}
    while True:
        for name in ("dz", "pondra"):  # a process that has exited will not catch up: say so now
            proc = getattr(ctx.get(name), "proc", None)
            if proc is not None and proc.poll() is not None:
                raise RuntimeError(f"{name} exited with {proc.returncode}; see its log in {ctx['work']}")
        try:
            pg_fp, pd_fp = fingerprint(ctx)
            if pg_fp == pd_fp:
                diffs, counts = compare(ctx)
                if not diffs:
                    return time.time() - t0, diffs, counts
        except Exception as e:  # Pondra is down, or restarting
            diffs = [f"{type(e).__name__}: {str(e)[:200]}"]
        if time.time() - t0 > timeout:
            if not counts:
                try:
                    diffs, counts = compare(ctx)
                except Exception as e:
                    diffs = [f"{type(e).__name__}: {str(e)[:200]}"]
            return None, diffs, counts
        time.sleep(0.5)


def pct(xs, p):
    """Nearest-rank percentile of xs (p in 0..1)."""
    xs = sorted(xs)
    return xs[min(len(xs) - 1, max(0, math.ceil(p * len(xs)) - 1))]


# ---------------------------------------------------------------- phases

def say(msg):
    print(msg, flush=True)


def report(name, result):
    """Prints `ok <phase>` or `FAIL <phase>: ...` as the phase is decided; True when ok."""
    secs, diffs, counts = result
    rows = ", ".join(f"{t} {pg}/{pd}" for t, (pg, pd) in counts.items())
    if secs is not None and not diffs:
        say(f"ok {name}: rows Postgres/Pondra {rows}; caught up in {secs:.1f} s")
        return True
    say(f"FAIL {name}: " + ("; ".join(diffs[:8]) or "no difference found, but not caught up")
        + f" (rows Postgres/Pondra {rows}; {'not caught up' if secs is None else 'caught up'})")
    return False


def main():
    sys.stdout.reconfigure(line_buffering=True)  # (each phase's line as it is decided, also into a file)
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0], formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--debezium", default=os.environ.get("DEBEZIUM_HOME"), help="Debezium Server's unpacked folder (DEBEZIUM_HOME)")
    ap.add_argument("--pg-bin", default=None, help="Postgres 16's bin folder (PG_BIN, else PATH)")
    ap.add_argument("--bin", default=os.environ.get("PONDRA_BIN", os.path.join(REPO, "target/release/pondra")))
    ap.add_argument("--values", default="both", choices=["both", "json", "json-schema"], help="value format: JSON without or with Kafka Connect's schema")
    ap.add_argument("--ops", type=int, default=50_000, help="changes in the stream phase")
    ap.add_argument("--fault-ops", type=int, default=10_000, help="changes in each fault phase")
    ap.add_argument("--base-port", type=int, default=9930, help="the first of four ports on 127.0.0.1 (Postgres, Pondra HTTP, Pondra Kafka, Debezium HTTP)")
    ap.add_argument("--pg-user", default="nobody", help="the user Postgres runs as when this runs as root")
    ap.add_argument("--timeout", type=float, default=300, help="seconds to wait for a phase to catch up")
    ap.add_argument("--forward-truncate", action="store_true", help="set debezium.source.skipped.operations=none: Debezium sends TRUNCATE on (its default drops it)")
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--work", default=None, help="a folder for this run (default: a new one under the temp dir)")
    ap.add_argument("--keep", action="store_true", help="keep the work folder (logs, the lake, the offsets)")
    args = ap.parse_args()

    dbz = find_debezium(args.debezium)
    if dbz is None:
        print("skipped: Debezium Server not found (--debezium DIR or DEBEZIUM_HOME)")
        return 0
    pg_bin = find_pg_bin(args.pg_bin)
    if pg_bin is None:
        print("skipped: Postgres 16 not found (--pg-bin DIR, PG_BIN, or PATH)")
        return 0
    if shutil.which("java") is None:
        print("skipped: java not found (Debezium Server needs Java 21)")
        return 0
    if not os.path.isfile(args.bin):
        print(f"Pondra binary not found: {args.bin} (--bin PATH)")
        return 1

    ports = [args.base_port + i for i in range(4)]
    busy = [p for p in ports if not port_free(p)]
    if busy:
        print(f"ports {busy} are in use: give another --base-port")
        return 1

    if os.geteuid() == 0:
        pw = pwd.getpwnam(args.pg_user)
        uid, gid = pw.pw_uid, pw.pw_gid
    else:
        uid = gid = None
    formats = [("json", False), ("json-schema", True)] if args.values == "both" else [(args.values, args.values == "json-schema")]

    base = args.work or tempfile.mkdtemp(prefix="pondra-cdc-")
    os.makedirs(base, exist_ok=True)
    os.chmod(base, 0o755)  # (Postgres's user has to reach its folder)
    all_ok = True
    for fmt, schemas in formats:
        work = os.path.join(base, fmt)
        shutil.rmtree(work, ignore_errors=True)
        os.makedirs(work)
        if uid is not None:
            os.chown(work, uid, gid)
        os.chmod(work, 0o755)
        print(f"== value format {fmt} (JSON {'with' if schemas else 'without'} Kafka Connect's schema); work folder {work}")
        try:
            all_ok &= run_format(args, fmt, schemas, dbz, pg_bin, work, ports, uid, gid)
        finally:
            if not args.keep:
                shutil.rmtree(work, ignore_errors=True)
    if not args.keep and args.work is None:
        shutil.rmtree(base, ignore_errors=True)
    return 0 if all_ok else 1


def run_format(args, fmt, schemas, dbz, pg_bin, work, ports, uid, gid):
    pg_port, http_port, kafka_port, dbz_http = ports
    rng = random.Random(args.seed)
    src = Source(rng)
    ctx = {"work": work, "pg": pg_port, "kafka": kafka_port, "dbz_http": dbz_http, "dbz_pw": None, "forward_truncate": args.forward_truncate}
    pg = Postgres(pg_bin, work, pg_port, uid, gid)
    ctx["dbz_pw"] = pg.dbz_pw
    pondra = Pondra(args.bin, work, http_port, kafka_port)
    home = os.path.join(work, "debezium")  # Debezium's working folder: config/application.properties lives here
    os.makedirs(os.path.join(home, "config"))
    props = os.path.join(home, "config", "application.properties")
    dz = Debezium(dbz, work, props, dbz_http)
    ok = True
    try:
        pg.init()
        pg.start()
        admin = pg.connect()
        ctx["chk"] = pg.connect()
        for sql in [f"CREATE ROLE dbz LOGIN REPLICATION PASSWORD '{pg.dbz_pw}'", *DDL,
                    "GRANT USAGE ON SCHEMA shop TO dbz", "GRANT SELECT ON ALL TABLES IN SCHEMA shop TO dbz",
                    "CREATE PUBLICATION pondra_pub FOR TABLE shop.orders, shop.items"]:
            admin.execute(sql)
        seed(admin, rng, SEED_ROWS)
        src.seeded(SEED_ROWS)
        version = admin.execute("SELECT version() AS v").fetchone()["v"]
        print(f"postgres: {version.split(',')[0]} on 127.0.0.1:{pg_port} (wal_level=logical, track_commit_timestamp=on)")
        admin.close()

        pondra.start()
        ctx["pondra"] = pondra
        for sql in DDL:
            pondra.sql(sql)
        stats = json_get(http_status_body(http_port, "/stats"))
        print(f"pondra: {stats.get('version')} on 127.0.0.1:{http_port}, Kafka 127.0.0.1:{kafka_port}, lake {pondra.lake}")
        print(f"pondra tables: shop.orders, shop.items with the source's PRIMARY KEYs (the same column types: Pondra's Decimal128(12,2), Timestamp(us, UTC), text for jsonb and uuid)")

        with open(props, "w") as f:
            f.write(properties(ctx, schemas))
        print(f"debezium: {os.path.basename(dbz)} config {props}")
        dz.start()
        ctx["dz"] = dz
        print("debezium: started (snapshot.mode=initial)")

        ok &= report("snapshot", settle(ctx, args.timeout))

        # stream: the mixed workload, the lag sample, the rate
        probes = []
        sampler = Sampler(http_port)
        sampler.start()
        wl = pg.connect()
        prog = run_phase(wl, src, args.ops, txn_at=args.ops // 2, probes=probes)
        say("stream mix: " + ", ".join(f"{k} {v}" for k, v in sorted(src.mix.items())) + f"; {len(probes)} probe rows; "
            f"one transaction of {TXN_CHANGES} changes")
        result = settle(ctx, args.timeout)
        equal_at = time.time()
        sampler.halt.set()
        ok &= report("stream", result)
        if result[0] is not None:
            total = prog.n
            span = equal_at - prog.first
            src_span = prog.last - prog.first
            say(f"rate: {total} changes end to end in {span:.1f} s = {total / span:.0f} changes/s (first change committed "
                f"in Postgres to Pondra equal); Postgres committed them in {src_span:.1f} s ({total / src_span:.0f}/s), "
                f"Pondra was equal {equal_at - prog.last:.1f} s after the last one")
            lag = [sampler.seen[pid] - t for pid, t in probes if pid in sampler.seen]
            if lag:
                say(f"lag (commit to readable in Pondra, {len(lag)} of {len(probes)} probe rows, polled every 20 ms): "
                             f"p50 {pct(lag, 0.5):.3f} s, p99 {pct(lag, 0.99):.3f} s, max {max(lag):.3f} s")
            else:
                say("lag: no probe row was seen")
        wl.close()

        n_fault = args.fault_ops
        fault_at = n_fault * 3 // 10  # (3,000 of 10,000)
        # kill-debezium: SIGKILL mid-stream, restarted 2 s later; it replays from its last flushed offset
        wl = pg.connect()
        run_phase(wl, src, n_fault, faults=[(fault_at, lambda: (dz.kill(), time.sleep(2), dz.start()))], txn_at=n_fault // 2)
        ok &= report("kill-debezium", settle(ctx, args.timeout))
        wl.close()

        # kill-pondra: SIGKILL the node mid-stream, restarted 3 s later on the same lake
        wl = pg.connect()
        run_phase(wl, src, n_fault, faults=[(fault_at, lambda: (pondra.kill(), time.sleep(3), pondra.start()))], txn_at=n_fault // 2)
        ok &= report("kill-pondra", settle(ctx, args.timeout))
        wl.close()

        # clean-restart: SIGTERM (offsets flushed), started again 2 s later
        wl = pg.connect()
        run_phase(wl, src, n_fault, faults=[(fault_at, lambda: (dz.stop(), time.sleep(2), dz.start()))], txn_at=n_fault // 2)
        ok &= report("clean-restart", settle(ctx, args.timeout))
        wl.close()

        # truncate: last, so the phases above are not affected by it. A TRUNCATE isn't carried (Debezium's default
        # skipped.operations=t drops it, and Pondra doesn't apply one it is sent): the check proves Pondra keeps its rows,
        # then that the documented step, the same TRUNCATE in Pondra, makes the tables equal again.
        say("truncate: " + ("skipped.operations=none, so Debezium sends the TRUNCATE on to Pondra" if args.forward_truncate
                            else "Debezium's default skipped.operations=t, so Debezium does not send the TRUNCATE on"))
        kept = len(ctx["pondra"].sql(PD_SELECT["items"]))
        ctx["chk"].execute("TRUNCATE shop.items")
        time.sleep(5)
        after = len(ctx["pondra"].sql(PD_SELECT["items"]))
        if after != kept:
            say(f"FAIL truncate: Pondra's shop.items went from {kept} to {after} rows; a TRUNCATE isn't carried, so it keeps them")
            ok = False
        ctx["pondra"].sql("TRUNCATE shop.items")
        ok &= report(f"truncate (not carried: Pondra kept its {kept} rows until the same TRUNCATE in Pondra)", settle(ctx, args.timeout))

        # after-truncate: the stream goes on past a truncate, and every later change is applied once
        src.items, src.repeat = [], None  # (the source's items are gone: the generator starts them again)
        wl = pg.connect()
        run_phase(wl, src, n_fault, txn_at=n_fault // 2)
        ok &= report("after-truncate", settle(ctx, args.timeout))
        wl.close()

        counts = log_counts(kafka_port, ["shop.orders", "shop.items"])
        say("log (records in each topic, Pondra's log from the start, tombstones in brackets): " + "; ".join(
            f"{t} {n} ({k})" for t, (n, k) in counts.items()))

        print_log_lines("pondra", pondra.log)
        print_log_lines("debezium", dz.log)
        if not dz.alive():
            print("debezium: not running at the end")
    except Exception as e:
        ok = False
        print(f"FAIL {fmt}: {type(e).__name__}: {e}")
        print_log_lines("pondra", pondra.log)
        print_log_lines("debezium", dz.log)
    finally:
        dz.stop()
        pondra.kill()
        pg.stop()
    return ok


def run_phase(conn, src, n, faults=(), txn_at=None, probes=None):
    """Runs `n` changes on a thread; `faults` are (after this many changes, action) pairs run on this thread meanwhile."""
    prog = Progress()
    th = threading.Thread(target=run_changes, args=(conn, src, n, prog, txn_at, probes), daemon=True)
    th.start()
    pending = list(faults)
    while th.is_alive():
        while pending and prog.n >= pending[0][0]:
            pending.pop(0)[1]()
        time.sleep(0.02)
    th.join()
    if prog.error is not None:
        raise prog.error
    return prog


def seed(conn, rng, n):
    """The rows before Debezium starts: n orders, and one item for each."""
    with conn.cursor() as cur:
        with cur.copy("COPY shop.orders (id, customer, amount, paid, placed, note, ref) FROM STDIN") as cp:
            for i in range(1, n + 1):
                cp.write_row([i] + [gen_value(rng, c) for c in ORDER_COLS])
        with cur.copy("COPY shop.items (order_id, line, sku, qty, unit_price) FROM STDIN") as cp:
            for i in range(1, n + 1):
                cp.write_row([i, 1] + [gen_value(rng, c) for c in ITEM_COLS])


def http_status_body(port, path):
    c = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
    try:
        c.request("GET", path)
        return c.getresponse().read()
    finally:
        c.close()


def json_get(body):
    return json.loads(body)


def print_log_lines(name, path, limit=8):
    """The distinct lines that say a record was refused or an error was logged, as the process wrote them."""
    if not os.path.exists(path):
        return
    seen = []
    with open(path, errors="replace") as f:
        for line in f:
            s = line.strip()
            if " = " in s or "AgroalParser" in s or "log.console.json" in s:  # (Debezium's config echo, benign warnings)
                continue
            if any(w in s for w in ("refused", " ERROR ", " WARN ", "panic", "Exception")) and s not in seen:
                seen.append(s)
    for s in seen[:limit]:
        print(f"{name} log: {s[:400]}")
    if len(seen) > limit:
        print(f"{name} log: ({len(seen) - limit} more distinct lines in {path})")


if __name__ == "__main__":
    sys.exit(main())
