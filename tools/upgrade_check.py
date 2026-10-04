#!/usr/bin/env python3
"""Upgrades (ADR-039): every release's lake opens in this build, and nothing in it changes.

  upgrade_check.py lakes [--from 0.22.0] [--only 0.27.0,0.30.0] [--new target/release/pondra]
      For each release on GitHub since --from (downloaded once into --cache), a lake made by that
      release: every kind of table, view, routine and object it knows, rows tiered and rows left
      in the log, written through each of its doors (SQL, appends with a producer's sequence, bulk
      INSERTs, `pondra sql` with no node running), the node killed at the end. Its answers are
      asked before the kill; then this build opens the lake, first only reading (`pondra sql`),
      then as its node, and every answer must be the same — the rows' ids and versions too. Then
      this build writes on: a producer's last batch sent again is a duplicate, old rows are
      changed, tiered and merged, and the lake is killed and opened again. What it then answers
      must equal a lake this build made the same way from the start; other engines read the
      published tables' new versions; every row id is still unique.

  upgrade_check.py drain [--s3]
      A node told to stop drains (`drain.rs`); a leader stopped under load hands over sooner than
      one killed, and exits at once though its followers keep sending. With --s3 the cluster's
      lake is on s3://$PONDRA_BUCKET (AWS_* point at R2, MinIO or tools/sim_r2.py), where a
      follower's flush is always in flight.

Prints the checks as JSON and exits 1 if one fails (a failing release's lakes and node logs are
kept in --work). Needs the Python packages in tools/requirements.txt for other engines' reads
(deltalake, pyiceberg); without them those checks are skipped, and say so.
"""
import argparse, atexit, collections, io, json, os, platform, random, shutil, signal, socket, subprocess, sys, tarfile, tempfile, threading, time, urllib.error, urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = "alimardon123/pondra"
TOKEN = "upgrade-check-admin"  # every node's admin token: a lake with users refuses anyone else
SECRET_KEY = "upgrade-check-master-key-0123456789abcdef"  # the same on every node, old and new
OWNER = "upgrade-check-owner-key-0123456789"  # whoever started the node: files on this machine
A = None


def version(v):
    return tuple(int(x) for x in v.split("-")[0].split("."))


def since(v, first):
    return version(v) >= version(first)


# ---------------------------------------------------------------- releases

def releases(first):
    """Every release on GitHub since `first`, oldest first: [(version, URL of this platform's binary)]."""
    os_name = {"linux": "linux", "darwin": "macos", "win32": "windows"}.get(sys.platform, sys.platform)
    arch = "arm64" if platform.machine().lower() in ("arm64", "aarch64") else "x64"
    plain = {"Accept": "application/vnd.github+json"}
    headers = dict(plain)
    if os.environ.get("GITHUB_TOKEN"):  # (CI: the API's limit for a runner's shared address is low)
        headers["Authorization"] = f"Bearer {os.environ['GITHUB_TOKEN']}"
    for attempt in range(6):
        # (a 403 or 429 is the API's limit, and the token's is shared by every run in the repository
        # at once: try again a little later, every other time without it, on the runner's own limit)
        req = urllib.request.Request(f"https://api.github.com/repos/{REPO}/releases?per_page=100",
                                     headers=headers if attempt % 2 == 0 else plain)
        try:
            listed = json.load(urllib.request.urlopen(req, timeout=60))
            break
        except urllib.error.HTTPError as e:
            if e.code not in (403, 429, 500, 502, 503) or attempt == 5:
                raise
            time.sleep(min(int(e.headers.get("Retry-After") or 0) or 15 * (attempt + 1), 120))
    found = []
    for r in listed:
        v = r["tag_name"].lstrip("v")
        if r["draft"] or r["prerelease"] or not since(v, first):
            continue
        # (the npm package of this platform: every release has one, 0.22.0's first among them)
        asset = next((a for a in r["assets"] if a["name"] == f"pondra-{os_name}-{arch}-{v}.tgz"), None)
        if asset:
            found.append((v, asset["browser_download_url"]))
    return sorted(found, key=lambda r: version(r[0]))


def binary(v, url, cache):
    """Release `v`'s binary, downloaded once into the cache."""
    exe = "pondra.exe" if sys.platform == "win32" else "pondra"
    path = os.path.join(cache, v, exe)
    if not os.path.exists(path):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        data = urllib.request.urlopen(url, timeout=600).read()
        with tarfile.open(fileobj=io.BytesIO(data)) as t:
            member = next(m for m in t.getmembers() if os.path.basename(m.name) == exe and m.isfile())
            with open(path + ".part", "wb") as f:
                f.write(t.extractfile(member).read())
        os.chmod(path + ".part", 0o755)
        os.replace(path + ".part", path)
    return path


# ---------------------------------------------------------------- a node

class Failed(Exception):
    pass


NODES = []  # every node started: stopped when this ends, whatever happened


@atexit.register
def stop_all():
    for n in NODES:
        n.stop("kill")


class Node:
    """A node of `bin` serving `lake`, started and stopped as a scheduler would."""

    def __init__(self, bin, lake, port, work, *flags, env=None, tier_secs=0):
        self.bin, self.lake, self.port, self.flags, self.env, self.tier = bin, lake, port, list(flags), env or {}, tier_secs
        self.log = os.path.join(work, f"node-{port}-{os.path.basename(lake)}.log")
        self.p = None

    def start(self):
        with socket.socket() as s:  # (a node left from a run that died would answer for this one)
            if s.connect_ex(("127.0.0.1", self.port)) == 0:
                raise Failed(f"port {self.port} is in use: stop what serves it")
        env = {**os.environ, "PONDRA_SECRET_KEY": SECRET_KEY, "PONDRA_OWNER_KEY": OWNER, "PONDRA_ADMIN_TOKEN": TOKEN, **self.env}
        with open(self.log, "a") as err:
            err.write(f"\n=== {self.bin} serve {self.lake} {' '.join(self.flags)}\n")
            err.flush()
            self.mark = err.tell()
            self.p = subprocess.Popen([self.bin, "serve", "--dir", self.lake, "--addr", f"127.0.0.1:{self.port}", "--admin-token", TOKEN, "--tier-secs", str(self.tier), *self.flags],
                                      env=env, stdout=subprocess.DEVNULL, stderr=err, stdin=subprocess.DEVNULL)
        NODES.append(self)
        deadline = time.time() + 120
        while time.time() < deadline:
            try:
                self.get("/stats", timeout=2)
                return self
            except Exception:
                if self.p.poll() is not None:
                    raise Failed(f"{self.bin} didn't start on {self.lake}: {open(self.log).read()[-1500:]}")
                time.sleep(0.05)
        raise Failed(f"{self.bin} didn't answer on {self.lake}: {open(self.log).read()[-1500:]}")

    def stop(self, how="term", wait=True):
        if self.p and self.p.poll() is None:
            self.p.send_signal(signal.SIGKILL if how == "kill" else signal.SIGTERM)
            try:
                self.p.wait(timeout=60 if wait else 0.001)
            except subprocess.TimeoutExpired:
                if wait:
                    self.p.kill()
                    self.p.wait()

    def said(self):
        """What this node wrote to its standard error since it started."""
        with open(self.log) as f:
            f.seek(self.mark)
            return f.read()

    def plain(self, method, path, body=None):
        """A request without a token, as a load balancer's probe makes it: (status, headers, text)."""
        req = urllib.request.Request(f"http://127.0.0.1:{self.port}{path}", data=body.encode() if body else None, method=method)
        try:
            r = urllib.request.urlopen(req, timeout=30)
            return r.status, dict(r.headers), r.read().decode()
        except urllib.error.HTTPError as e:
            return e.code, dict(e.headers), e.read().decode(errors="replace")
        except OSError as e:
            return 0, {}, str(e)

    def call(self, method, path, body=None, headers=None, timeout=120):
        data = body if isinstance(body, (bytes, type(None))) else (body if isinstance(body, str) else json.dumps(body)).encode()
        req = urllib.request.Request(f"http://127.0.0.1:{self.port}{path}", data=data, method=method,
                                     headers={"Authorization": f"Bearer {TOKEN}", "x-pondra-owner": OWNER, **(headers or {})})
        try:
            out = urllib.request.urlopen(req, timeout=timeout).read()
        except urllib.error.HTTPError as e:
            raise Failed(f"{method} {path} {body if isinstance(body, str) else ''}: {e.code} {e.read().decode(errors='replace')[:800]}") from None
        return json.loads(out) if out[:1] in (b"{", b"[") else out

    def get(self, path, timeout=120):
        return self.call("GET", path, timeout=timeout)

    def post(self, path, body=b""):
        return self.call("POST", path, body)

    def q(self, sql):
        return self.call("POST", "/sql", sql)

    def append(self, table, producer, seq, rows):
        return self.post(f"/append/{table}?producer={producer}&seq={seq}", "".join(json.dumps(r) + "\n" for r in rows))


def cli(bin, lake, sql, ok=True):
    """`pondra sql` on the lake, as a user's machine runs it (no node needed): what it printed (with
    `ok=False`, (exit code, what it printed) whatever happened)."""
    env = {**os.environ, "PONDRA_SECRET_KEY": SECRET_KEY, "PONDRA_ADMIN_TOKEN": TOKEN}
    r = subprocess.run([bin, "sql", "--dir", lake, sql], capture_output=True, text=True, timeout=300, env=env)
    if not ok:
        return r.returncode, r.stdout + r.stderr
    if r.returncode != 0:
        raise Failed(f"pondra sql {sql!r}: {(r.stdout + r.stderr)[-1500:]}")
    return r.stdout


# ---------------------------------------------------------------- what a lake holds

def iso(second):
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(1_767_225_600 + second))  # 2026-01-01 + seconds


def build(n, v, work):
    """Everything a lake of release `v` can hold, through the doors that release had."""
    q = n.q
    # Tables of every kind: append, keyed (latest row, or by event time), partitioned, clustered,
    # published for other engines, a merge table, one in a schema; and what streams into views.
    q("CREATE TABLE ev (id BIGINT, who VARCHAR, amount DOUBLE, ts TIMESTAMP, n INT)")
    q("CREATE TABLE kv (id BIGINT PRIMARY KEY, v VARCHAR, n BIGINT)")
    q("CREATE TABLE dd (id BIGINT PRIMARY KEY, v VARCHAR, at BIGINT) WITH (order_by = 'at')")
    q("CREATE TABLE pt (id BIGINT, day DATE, v DOUBLE) WITH (partition_by = 'day')")
    q("CREATE TABLE ct (a BIGINT, b BIGINT, v DOUBLE) WITH (cluster_by = 'a,b')")
    q("CREATE TABLE pub (id BIGINT, v VARCHAR) WITH (publish = 'delta,iceberg')")
    n.post("/tables/totals", {"columns": [["k", "Utf8"], ["n", "Int64"], ["lo", "Int64"]], "key": ["k"], "merge": {"n": "sum", "lo": "min"}})
    q("CREATE SCHEMA s1")
    q("CREATE TABLE s1.t (x INT, y VARCHAR)")
    q("CREATE TABLE clicks (user_id BIGINT, page VARCHAR, ts TIMESTAMP)")
    q("CREATE TABLE trades (sym VARCHAR, px DOUBLE)")
    q("CREATE TABLE quotes (sym VARCHAR, bid DOUBLE)")
    # Views of every kind, a macro and a streaming task.
    q("CREATE VIEW sv AS SELECT who, sum(amount) AS total FROM ev GROUP BY who")
    q("CREATE MATERIALIZED VIEW by_who AS SELECT who, count(*) AS n, sum(amount) AS total FROM ev GROUP BY who")
    q("CREATE MATERIALIZED VIEW big AS SELECT id, who, amount FROM ev WHERE amount > 50")
    q("CREATE MATERIALIZED VIEW per_min WITH (window = 'w', size_secs = 60, lateness_secs = 0) AS "
      "SELECT date_bin(INTERVAL '60 seconds', ts) AS w, page, count(*) AS n FROM clicks GROUP BY 1, 2")
    n.post("/views/visits?session=ts&gap_secs=30&lateness_secs=0", "SELECT user_id, count(*) AS n FROM clicks GROUP BY user_id")
    q("CREATE MATERIALIZED VIEW tq WITH (join = 'streams') AS SELECT t.sym, t.px, q.bid FROM trades t JOIN quotes q ON t.sym = q.sym")
    n.post("/tasks/copier", {"source": "clicks", "target": "clicks_copy", "sql": "SELECT user_id, page FROM clicks"})
    q("CREATE MACRO plus1(x) AS x + 1")
    # Rows: small writes (inside a catalog commit), a big one (its own object in log/), a bulk
    # INSERT (Parquet straight away), a producer's batches (exactly-once), a merge table's parts.
    q("INSERT INTO ev VALUES (1, 'ann', 10.5, TIMESTAMP '2026-01-01 00:00:00', 1), (2, 'bob', 60.0, TIMESTAMP '2026-01-01 00:00:01', 2)")
    n.append("ev", "app", 1, [{"id": 1000 + i, "who": f"w{i % 7}", "amount": float(i % 100), "ts": iso(i), "n": i} for i in range(3000)])
    q("INSERT INTO ev SELECT value + 10000 AS id, 'bulk' AS who, CAST(value AS DOUBLE) AS amount, TIMESTAMP '2026-01-02 00:00:00' AS ts, CAST(value AS INT) AS n FROM range(0, 500)")
    q("INSERT INTO kv VALUES (1, 'a', 1), (2, 'b', 2), (3, 'c', 3), (4, 'd', 4)")
    q("INSERT INTO dd VALUES (1, 'new', 20), (1, 'old', 10), (2, 'two', 5)")
    q("INSERT INTO pt VALUES (1, DATE '2026-01-01', 1.0), (2, DATE '2026-01-02', 2.0), (3, DATE '2026-01-03', 3.0), (4, DATE '2026-01-01', 4.0)")
    q("INSERT INTO ct SELECT value % 10 AS a, value % 7 AS b, CAST(value AS DOUBLE) AS v FROM range(0, 200)")
    q("INSERT INTO pub VALUES (1, 'one'), (2, 'two'), (3, 'three')")
    n.append("totals", "parts", 1, [{"k": "a", "n": 1, "lo": 5}, {"k": "b", "n": 10, "lo": 7}])
    n.append("totals", "parts", 2, [{"k": "a", "n": 2, "lo": 3}])
    q("INSERT INTO s1.t VALUES (1, 'x'), (2, 'y')")
    clicks = [(1, "home", 0), (1, "docs", 10), (2, "home", 20), (3, "blog", 70), (1, "home", 125), (2, "docs", 130)]
    n.append("clicks", "web", 1, [{"user_id": u, "page": p, "ts": iso(s)} for u, p, s in clicks])
    q("INSERT INTO trades VALUES ('abc', 10.0), ('xyz', 20.0)")
    q("INSERT INTO quotes VALUES ('abc', 9.5), ('abc', 9.6)")
    n.post("/tier")  # log -> Parquet (and the published tables' first Delta and Iceberg versions)
    # Rows left in the log, changes of tiered rows, columns changed.
    q("INSERT INTO ev VALUES (3, 'cat', 70.0, TIMESTAMP '2026-01-01 00:00:02', 3)")
    q("UPDATE ev SET amount = amount + 1 WHERE id IN (1, 1000, 10001)")
    q("DELETE FROM ev WHERE id IN (2, 1001)")
    q("INSERT INTO kv VALUES (2, 'B', 20)")
    q("DELETE FROM kv WHERE id = 3")
    q("UPDATE kv SET n = n + 100 WHERE id = 1")
    q("INSERT INTO dd VALUES (1, 'older', 5), (2, 'newer', 6)")
    q("MERGE INTO pub USING (SELECT 2 AS id, 'TWO' AS v UNION ALL SELECT 4 AS id, 'four' AS v) s ON pub.id = s.id "
      "WHEN MATCHED THEN UPDATE SET v = s.v WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.v)")
    q("INSERT INTO quotes VALUES ('xyz', 19.0)")
    q("ALTER TABLE ev ADD COLUMN note VARCHAR")
    q("INSERT INTO ev VALUES (4, 'dan', 5.0, TIMESTAMP '2026-01-01 00:00:03', 4, 'with a note')")
    q("ALTER TABLE s1.t RENAME COLUMN y TO why")
    q("ALTER TABLE s1.t ALTER COLUMN x TYPE BIGINT")
    q("ALTER TABLE s1.t ADD COLUMN z BIGINT")
    q("INSERT INTO s1.t VALUES (3, 'z', 30)")
    q("ALTER TABLE s1.t DROP COLUMN why")
    q("CREATE TABLE ctas AS SELECT id, who FROM ev WHERE id < 10")
    other = os.path.join(work, f"other-{os.path.basename(n.lake)}")
    cli(n.bin, other, "CREATE TABLE o (a BIGINT, b VARCHAR)")
    cli(n.bin, other, "INSERT INTO o VALUES (1, 'other lake')")
    q(f"ATTACH '{other}' AS other")
    if since(v, "0.24.0"):  # functions, procedures, schedules, secrets, the lake's own files
        q("CREATE FUNCTION twice(x BIGINT) RETURNS BIGINT RETURN x * 2")
        q("CREATE PROCEDURE note_it() LANGUAGE sql AS $$ INSERT INTO s1.t (x) VALUES (42) $$")
        q("CALL note_it()")
        # (a schedule no run crosses: an hourly task ticked in one lake and not the other when a run crossed the hour)
        q("CREATE TASK yearly SCHEDULE 'cron 0 0 1 1 * UTC' AS INSERT INTO s1.t (x) VALUES (-1)")
        q("CREATE SECRET bucket_key (TYPE s3, KEY_ID 'k', SECRET 'v', SCOPE 's3://somewhere/')")
        n.call("PUT", "/files/notes/readme.md", b"# notes\n")
    if since(v, "0.26.0"):  # NOT NULL and DEFAULT, a table renamed (its folder kept), files read by name
        q("CREATE TABLE nn (id BIGINT NOT NULL, v VARCHAR DEFAULT 'none')")
        q("INSERT INTO nn (id) VALUES (1)")
        q("CREATE TABLE before_rename (a BIGINT)")
        q("INSERT INTO before_rename VALUES (1), (2)")
        n.post("/tier")
        q("ALTER TABLE before_rename RENAME TO renamed")
        q("INSERT INTO renamed VALUES (3)")
        csv = os.path.join(work, f"outside-{os.path.basename(n.lake)}.csv")
        open(csv, "w").write("a,b\n1,one\n2,two\n")
        q(f"CREATE EXTERNAL TABLE ext1 (a INT, b VARCHAR) STORED AS CSV LOCATION '{csv}' OPTIONS ('format.has_header' 'true')")
    if since(v, "0.30.0"):  # users and grants, CHECK, flows with expectations, history, files' versions
        q("CREATE USER ann PASSWORD 'correct-horse-battery'")
        q("CREATE ROLE staff")
        q("GRANT SELECT ON ev TO staff")
        q("GRANT staff TO ann")
        q("CREATE TABLE ck (id BIGINT, v BIGINT, CONSTRAINT nonneg CHECK (v >= 0))")
        q("INSERT INTO ck VALUES (1, 5)")
        q("CREATE MATERIALIZED VIEW silver (CONSTRAINT positive CHECK (amount > 60) ON VIOLATION DROP ROW) AS SELECT id, who, amount FROM big")
        q("CREATE MATERIALIZED VIEW gold AS SELECT who, count(*) AS n FROM silver GROUP BY who")
        q("CREATE TABLE changes_in (id BIGINT, name VARCHAR, op VARCHAR, at BIGINT)")
        q("CREATE MATERIALIZED VIEW people WITH (history = 'id', sequence_by = 'at', delete_when = 'op = ''D''') AS SELECT id, name, op, at FROM changes_in")
        q("INSERT INTO changes_in VALUES (1, 'Ann', 'U', 1), (1, 'Anna', 'U', 2), (2, 'Bob', 'U', 1), (2, 'Bob', 'D', 3)")
        n.call("PUT", "/files/notes/readme.md", b"# notes, saved again\n", headers={"if-match": etag(n, "/files/notes/readme.md")})


def etag(n, path):
    req = urllib.request.Request(f"http://127.0.0.1:{n.port}{path}", method="GET", headers={"Authorization": f"Bearer {TOKEN}"})
    return urllib.request.urlopen(req, timeout=30).headers["etag"]


def cli_writes(bin, lake, v):
    """Writes from a machine with no node running: `pondra sql` leads for a moment itself (before
    0.24, its INSERTs took no column list)."""
    if since(v, "0.24.0"):
        cli(bin, lake, "INSERT INTO s1.t (x, z) VALUES (7, 70)")
        cli(bin, lake, "INSERT INTO ev (id, who, amount) SELECT value + 30000 AS id, 'cli' AS who, 1.0 AS amount FROM range(0, 50)")
    else:
        cli(bin, lake, "INSERT INTO s1.t VALUES (7, 70)")
        cli(bin, lake, "INSERT INTO ev SELECT value + 30000 AS id, 'cli' AS who, 1.0 AS amount, CAST(NULL AS TIMESTAMP) AS ts, "
                       "CAST(NULL AS INT) AS n, CAST(NULL AS VARCHAR) AS note FROM range(0, 50)")


def carry_on(n, v):
    """What this build does to a lake it opened: the same, whoever made the lake (`reference`)."""
    q = n.q
    # (a producer's batch sent again after the upgrade is the duplicate it was before it)
    again = n.append("ev", "app", 1, [{"id": 1000 + i, "who": f"w{i % 7}", "amount": float(i % 100), "ts": iso(i), "n": i} for i in range(3000)])
    n.append("ev", "app", 2, [{"id": 5000 + i, "who": "late", "amount": 1.0 + i, "ts": iso(400 + i), "n": i} for i in range(10)])
    q("INSERT INTO ev (id, who, amount, ts, n, note) VALUES (5, 'eve', 55.0, TIMESTAMP '2026-01-01 00:00:04', 5, 'after the upgrade')")
    q("INSERT INTO ev (id, who, amount, ts, n) SELECT value + 20000 AS id, 'bulk2' AS who, CAST(value AS DOUBLE) AS amount, TIMESTAMP '2026-01-03 00:00:00' AS ts, CAST(value AS INT) AS n FROM range(0, 100)")
    q("UPDATE ev SET amount = amount * 2 WHERE id IN (3, 1002, 10002, 30001)")  # a row in the log, in tiered files, in a bulk file, from pondra sql
    q("DELETE FROM ev WHERE id IN (4, 1003, 30002)")
    q("INSERT INTO kv VALUES (4, 'D', 40), (5, 'e', 5)")
    q("DELETE FROM kv WHERE id = 2")
    q("INSERT INTO dd VALUES (2, 'newest', 7), (1, 'oldest', 1)")
    q("MERGE INTO pub USING (SELECT 1 AS id, 'ONE' AS v UNION ALL SELECT 5 AS id, 'five' AS v) s ON pub.id = s.id "
      "WHEN MATCHED THEN UPDATE SET v = s.v WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.v)")
    n.append("totals", "parts", 3, [{"k": "a", "n": 4, "lo": 1}, {"k": "c", "n": 1, "lo": 9}])
    n.append("clicks", "web", 2, [{"user_id": 4, "page": "home", "ts": iso(300)}, {"user_id": 1, "page": "blog", "ts": iso(310)}])
    q("INSERT INTO trades VALUES ('abc', 11.0)")
    q("INSERT INTO pt VALUES (5, DATE '2026-01-02', 5.0)")
    q("INSERT INTO s1.t (x, z) VALUES (8, 80)")
    q("ALTER TABLE ev ADD COLUMN extra BIGINT")
    q("INSERT INTO ev (id, who, amount, extra) VALUES (6, 'fay', 1.0, 7)")
    if since(v, "0.24.0"):
        q("CALL note_it()")
    if since(v, "0.26.0"):
        q("INSERT INTO renamed VALUES (4)")
        q("INSERT INTO nn (id, v) VALUES (2, 'given')")
    if since(v, "0.30.0"):
        q("INSERT INTO changes_in VALUES (1, 'Annie', 'U', 3), (3, 'Cy', 'U', 1)")
        q("INSERT INTO ck VALUES (2, 6)")
    n.post("/tier")  # old files and new rows tiered together; the published tables' next versions
    q("CHECKPOINT")
    return again


# ---------------------------------------------------------------- what it answers

SYSTEM = ["_row_id", "_version", "_created_at", "_updated_at"]


def relations(v):
    """Every table and view of a release-`v` lake (and of the lake it attached)."""
    r = ["ev", "kv", "dd", "pt", "ct", "pub", "totals", "s1.t", "clicks", "trades", "quotes", "sv", "by_who", "big",
         "per_min", "per_min_final", "visits", "tq", "clicks_copy", "ctas", "other.o"]
    if since(v, "0.24.0"):
        r += ["pondra.runs"]
    if since(v, "0.26.0"):
        r += ["nn", "renamed", "ext1"]
    if since(v, "0.30.0"):
        r += ["ck", "silver", "gold", "changes_in", "people"]
    return r


def questions(v):
    """What a lake of release `v` is asked, as {name: SQL}."""
    qs = {f"rows of {r}": f"SELECT * FROM {r}" for r in relations(v)}
    kind = ", table_type" if since(v, "0.26.0") else ""  # (before 0.26 a table was a VIEW there: invariant 132)
    qs["tables"] = f"SELECT table_schema, table_name{kind} FROM information_schema.tables WHERE table_schema NOT IN ('information_schema', 'pg_catalog')"
    qs["columns"] = "SELECT table_schema, table_name, column_name, data_type, is_nullable FROM information_schema.columns WHERE table_schema NOT IN ('information_schema', 'pg_catalog')"
    qs["a macro"] = "SELECT plus1(41) AS a"
    if since(v, "0.24.0"):
        qs["a function"] = "SELECT twice(21) AS a"
        qs["secrets"] = "SELECT * FROM secrets()"
        qs["tasks"] = "SELECT name, schedule, statement FROM pondra.tasks"
    if since(v, "0.30.0"):
        qs["users"] = "SELECT * FROM pondra.users"
        qs["grants"] = "SELECT * FROM pondra.grants"
        qs["flows"] = "SELECT * FROM pondra.flows"
        qs["expectations"] = "SELECT * FROM pondra.expectations"
    return qs


def canonical(rows):
    # (a system column named in a select beside `*` comes back as `_row_id_1`: invariant 175)
    plain = lambda c: c.rsplit("_", 1)[0] if c.rsplit("_", 1)[0] in SYSTEM and c.rsplit("_", 1)[-1].isdigit() else c
    return sorted(json.dumps({plain(c): x for c, x in r.items()} if isinstance(r, dict) else r, sort_keys=True) for r in rows)


def ask(n, v, ids=None):
    """Every question's answer, rows in a canonical order. A table's or view's rows come with their
    system columns where it has them (`ids`: which, as the old release answered first)."""
    out, ids = {}, ids if ids is not None else {}
    for name, sql in questions(v).items():
        if name.startswith("rows of "):
            r = name[len("rows of "):]
            if ids.get(r, True):
                try:
                    out[name] = canonical(n.q(f"SELECT {', '.join(SYSTEM)}, * FROM {r}"))
                    ids[r] = True
                    continue
                except Failed:
                    ids[r] = False
        out[name] = canonical(n.q(sql))
    mcp = n.post("/mcp", {"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": "changes", "arguments": {"table": "ev", "after": 0}}})
    out["the change feed"] = canonical(json.loads(mcp["result"]["content"][0]["text"])["rows"])
    if since(v, "0.24.0"):
        out["a lake's file"] = [n.get("/files/notes/readme.md").decode()]
    if since(v, "0.30.0"):  # a user signs in as before, and may do what was granted, no more
        basic = {"Authorization": "Basic " + __import__("base64").b64encode(b"ann:correct-horse-battery").decode()}
        req = lambda sql: urllib.request.urlopen(urllib.request.Request(f"http://127.0.0.1:{n.port}/sql", data=sql.encode(), method="POST", headers=basic), timeout=60)
        try:
            refused = req("SELECT * FROM kv").status
        except urllib.error.HTTPError as e:
            refused = e.code
        out["a user signs in"] = [json.dumps(json.loads(req("SELECT count(*) AS n FROM ev").read())), f"kv: {refused}"]
    return out, ids


def lookups(n):
    """Keys of `kv` looked up (`GET /lookup`, a point query, both unplanned) where they answer
    otherwise than SQL's plan: {key: answers}."""
    out = {}
    for k in range(1, 7):
        planned = canonical(n.q(f"SELECT * FROM kv WHERE id + 0 = {k}"))
        point = canonical(n.q(f"SELECT * FROM kv WHERE id = {k}"))
        found = n.get(f"/lookup/kv/{k}")
        found = canonical(found if isinstance(found, list) else [found])
        if not planned == point == found:
            out[k] = {"sql": planned, "point query": point, "lookup": found}
    return out


def settle(n, v, secs=30):
    """Wait until what runs in the background (windows, sessions, tasks, stream joins) is done."""
    probe = lambda: [n.q(f"SELECT count(*) AS n FROM {r}") for r in ("per_min_final", "visits", "clicks_copy", "tq", "by_who", "big")]
    last, deadline = None, time.time() + secs
    while time.time() < deadline:
        now = probe()
        if now == last:
            return
        last = now
        time.sleep(1.5)


def differences(old, new, columns_of_old=True):
    """Questions answered differently: {question: (old, new)} (rows as the old release named its columns)."""
    out = {}
    for k, rows in old.items():
        got = new.get(k)
        if got is not None and columns_of_old and k.startswith("rows of ") and rows:
            keep = set().union(*(json.loads(r) for r in rows))  # (a row leaves out its nulls)
            got = sorted(json.dumps({c: x for c, x in json.loads(r).items() if c in keep}, sort_keys=True) for r in got)
        if got != rows:
            gone, came = collections.Counter(rows), collections.Counter(got or [])
            out[k] = {"only before": list((gone - came).elements())[:5], "only after": list((came - gone).elements())[:5],
                      "rows": [len(rows), len(got or [])]}
    return out


def without_system(answers):
    """Answers without the system columns, and without what tells a lake's history apart (runs,
    the change feed, when a user was made): what a lake made from the start by this build must
    answer the same."""
    out = {}
    for k, rows in answers.items():
        if k in ("rows of pondra.runs", "the change feed"):
            continue
        drop = SYSTEM + (["created"] if k == "users" else [])
        out[k] = sorted(json.dumps({c: x for c, x in json.loads(r).items() if c not in drop}, sort_keys=True) for r in rows) if rows and rows[0].startswith("{") else rows
    return out


def outside_reads(lake):
    """The published table as other engines read it: (delta-rs's rows, PyIceberg's rows), or None
    for an engine whose package isn't installed."""
    got = {}
    try:
        import deltalake, pyarrow as pa
        t = deltalake.DeltaTable(os.path.join(lake, "data", "pub"))  # (its QueryBuilder applies deletion vectors)
        got["delta"] = canonical(pa.table(deltalake.QueryBuilder().register("t", t).execute("SELECT id, v FROM t").read_all()).to_pylist())
    except ImportError:
        got["delta"] = None
    try:
        from pyiceberg.table import StaticTable
        meta = os.path.join(lake, "data", "pub", "metadata")
        newest = open(os.path.join(meta, "version-hint.text")).read().strip()
        got["iceberg"] = canonical(StaticTable.from_metadata(os.path.join(meta, f"v{newest}.metadata.json")).scan(selected_fields=("id", "v")).to_arrow().to_pylist())
    except ImportError:
        got["iceberg"] = None
    return got


# ---------------------------------------------------------------- one release's lake

def lake_check(v, old_bin, new_bin, work, port):
    """A lake made by release `v`, opened by this build: {check: ok}, and what differed."""
    checks, info = {}, {}
    lake, ref = os.path.join(work, f"lake-{v}"), os.path.join(work, f"reference-{v}")
    # Made by the release: built, stopped as a scheduler stops it, written by `pondra sql` with no
    # node, opened again, asked, killed.
    n = Node(old_bin, lake, port, work).start()
    build(n, v, work)
    settle(n, v)
    n.stop()
    cli_writes(old_bin, lake, v)
    n = Node(old_bin, lake, port, work).start()
    settle(n, v)
    old, ids = ask(n, v)
    n.stop("kill")
    shutil.copytree(lake, lake + "-as-released")  # (to look at when a check fails)
    # This build, only reading it (no node: `pondra sql`), then serving it.
    read = {}
    for name in ("rows of ev", "rows of kv", "rows of pub", "rows of other.o"):
        r = name[len("rows of "):]
        cols = (", ".join(SYSTEM) + ", *") if ids.get(r) else "*"
        out = cli(new_bin, lake, f"SELECT count(*) AS n FROM (SELECT {cols} FROM {r})")
        read[name] = out.split("\n")[3].strip(" |") if out.count("\n") > 3 else out
    info["pondra sql read it"] = read
    checks["this build reads the lake with no node (pondra sql): every row"] = all(read[k] == str(len(old[k])) for k in read)
    n = Node(new_bin, lake, port, work).start()
    settle(n, v)
    opened, _ = ask(n, v, dict(ids))
    diff = differences(old, opened)
    info["answered differently once opened"] = diff
    looked = {"opened": lookups(n)}
    checks["opened by this build: every answer as the release gave it (rows, their ids and versions, the catalog, users, files)"] = not diff
    # This build writes on: everything it does to its own lakes.
    again = carry_on(n, v)
    checks["a producer's last batch sent again after the upgrade is a duplicate"] = again.get("duplicate") is True
    settle(n, v)
    after, _ = ask(n, v, dict(ids))
    looked["after this build's writes"] = lookups(n)
    unique = {r: n.q(f"SELECT count(*) AS n, count(DISTINCT _row_id) AS d FROM {r}")[0] for r, has in ids.items() if has and "." not in r}
    checks["every row id still unique, old rows' and new ones'"] = all(u["n"] == u["d"] for u in unique.values())
    if not checks["every row id still unique, old rows' and new ones'"]:
        info["row ids"] = {r: u for r, u in unique.items() if u["n"] != u["d"]}
    n.stop("kill")
    n = Node(new_bin, lake, port, work).start()
    settle(n, v)
    again_opened, _ = ask(n, v, dict(ids))
    diff = differences(after, again_opened, columns_of_old=False)
    info["answered differently after a kill"] = diff
    checks["killed and opened again: the same answers"] = not diff
    outside = outside_reads(lake)
    mine = canonical([{"id": r["id"], "v": r["v"]} for r in n.q("SELECT id, v FROM pub")])
    info["other engines"] = {k: ("not installed" if got is None else got == mine) for k, got in outside.items()}
    checks["other engines read the published table's new versions (delta-rs, PyIceberg)"] = all(got is None or got == mine for got in outside.values())
    n.stop()
    # The same, made by this build from the start.
    r = Node(new_bin, ref, port + 1, work).start()
    build(r, v, work)
    settle(r, v)
    r.stop()
    cli_writes(new_bin, ref, v)
    r = Node(new_bin, ref, port + 1, work).start()
    carry_on(r, v)
    settle(r, v)
    reference, _ = ask(r, v, dict(ids))
    looked["a lake this build made"] = lookups(r)
    r.stop()
    looked = {k: x for k, x in looked.items() if x}
    info["keys looked up otherwise than SQL reads them"] = looked
    checks["a key looked up (GET /lookup, a point query) as SQL reads it"] = not looked
    diff = differences(without_system(reference), without_system(after), columns_of_old=False)
    info["differs from a lake this build made"] = diff
    checks["after this build's writes, every answer as a lake this build made from the start"] = not diff
    return checks, info


def until(what, secs, step=0.5):
    """`what()` once it is true (errors count as not yet), or false after `secs`."""
    deadline = time.time() + secs
    while True:
        try:
            got = what()
        except Exception:
            got = False
        if got or time.time() > deadline:
            return got
        time.sleep(step)


# ---------------------------------------------------------------- the lake's format

def format_check(new_bin, work, port):
    """The lake's format (ADR-039), this build posing as a later one for a node (PONDRA_TEST_FORMAT=2)."""
    checks, info = {}, {}
    lake, later = os.path.join(work, "format-lake"), {"PONDRA_TEST_FORMAT": "2"}
    fmt = lambda n: n.get("/stats", timeout=5)["format"]
    first = Node(new_bin, lake, port, work).start()  # makes the lake
    checks["a lake this build makes is of its format from the start"] = until(lambda: fmt(first) == 1, 3, step=0.1)
    first.q("CREATE TABLE t (x INT)")
    first.q("INSERT INTO t VALUES (1), (2)")
    first.stop()
    a = Node(new_bin, lake, port, work, env=later).start()  # leads, and knows format 2
    b = Node(new_bin, lake, port + 1, work).start()  # follows, and knows format 1
    time.sleep(22)  # (the leader's first two looks: it must not go past what the follower knows)
    info["with a node of each"] = {"leader's": fmt(a), "follower's": fmt(b), "releases": a.get("/stats")["releases"]}
    checks["a lake moves on only to the newest format every node knows"] = fmt(a) == 1 and fmt(b) == 1
    b.stop()
    checks["…and on again once the node that didn't know the next one has gone"] = until(lambda: fmt(a) == 2, 40)
    refused = lambda x: x.p.poll() not in (None, 0) and "format 2" in x.said() and "run Pondra" in x.said()
    b = Node(new_bin, lake, port + 1, work)
    try:  # a node that doesn't know the lake's format: at once, or as soon as its view holds the format
        b.start()
        b.p.wait(20)
    except (Failed, subprocess.TimeoutExpired):
        pass
    info["a follower that knows format 1 said"] = b.said()[-300:]
    checks["a node that doesn't know the lake's format refuses it, saying which release it needs"] = refused(b)
    b.stop("kill")
    code, said = cli(new_bin, lake, "SELECT count(*) AS n FROM t", ok=False)
    checks["…and so does pondra sql"] = code != 0 and "format 2" in said
    a.stop()
    t0 = time.time()
    b = Node(new_bin, lake, port + 1, work)
    try:  # (nobody leads: it would, and must give the term back)
        b.start()
    except Failed:
        pass
    b.stop("kill")
    t1 = time.time()
    a = Node(new_bin, lake, port, work, env=later).start()
    info["seconds"] = {"a node of format 1 refused to lead it": round(t1 - t0, 1), "then one of format 2 led it": round(time.time() - t1, 1)}
    checks["…without holding on to the leader's term: the next node leads at once"] = refused(b) and time.time() - t1 < 10
    checks["the lake reads as it did"] = a.q("SELECT count(*) AS n FROM t") == [{"n": 2}]
    a.stop()
    return checks, info


# ---------------------------------------------------------------- stopping

class Load:
    """Producers writing without a pause, each batch once (a producer's sequence, retried on any
    live node until acknowledged), and readers asking the nodes whether each producer's batches are
    a prefix with no gaps. `nodes`: the live nodes, a list changed in place as they come and go."""

    def __init__(self, nodes, table, producers=4, readers=2, size=20, rate=None, acked=None):
        self.nodes, self.table, self.size, self.rate = nodes, table, size, rate  # (rate: a producer's batches a second; None: flat out)
        # (acked: each producer's last acknowledged batch so far, to go on from: a soak's next leg)
        self.stop_, self.acked, self.torn, self.reads = threading.Event(), collections.defaultdict(int, acked or {}), [], [0]
        self.waits = collections.deque(maxlen=500_000)  # (wait, sent at): the recent ones
        self.threads = [threading.Thread(target=self.write, args=(f"p{i}",), daemon=True) for i in range(producers)]
        self.threads += [threading.Thread(target=self.read, daemon=True) for _ in range(readers)]

    def start(self):
        [t.start() for t in self.threads]
        return self

    def write(self, producer):
        seq = self.acked[producer]
        while not self.stop_.is_set():
            seq += 1
            body = "".join(json.dumps({"producer": producer, "seq": seq, "i": i}) + "\n" for i in range(self.size))
            t0 = time.time()
            while True:
                try:
                    random.choice(self.nodes).call("POST", f"/append/{self.table}?producer={producer}&seq={seq}", body, timeout=20)
                    break
                except Exception:
                    time.sleep(0.1)
            self.waits.append((time.time() - t0, t0))
            self.acked[producer] = seq
            if self.rate:
                time.sleep(max(0.0, t0 + 1 / self.rate - time.time()))

    def read(self):
        q = f"SELECT producer, count(*) AS n, count(DISTINCT seq) AS d, max(seq) AS mx FROM {self.table} GROUP BY producer"
        while not self.stop_.is_set():
            try:
                for r in random.choice(self.nodes).q(q):
                    if (r["n"] != r["mx"] * self.size or r["d"] != r["mx"]) and len(self.torn) < 100:
                        self.torn.append(r)
                self.reads[0] += 1
            except Exception:
                pass  # (a node stopping, or starting)
            time.sleep(0.05)

    def finish(self, node):
        """Stop; then what `node` holds against what was acknowledged."""
        self.stop_.set()
        [t.join(30) for t in self.threads]
        held = {r["producer"]: r for r in node.q(f"SELECT producer, count(*) AS n, count(DISTINCT seq) AS d, max(seq) AS mx FROM {self.table} GROUP BY producer")}
        wrong = {p: held.get(p) for p, seq in self.acked.items() if not held.get(p) or held[p]["d"] != held[p]["mx"] or held[p]["n"] != held[p]["mx"] * self.size or held[p]["mx"] < seq}
        return {"batches acknowledged": sum(self.acked.values()), "lost or twice": wrong, "torn reads": self.torn[:5], "reads": self.reads[0]}

    def longest(self, since, until_=None):
        """The longest a batch waited for its acknowledgement, of those sent from `since` on (s)."""
        w = [d for d, t0 in list(self.waits) if t0 >= since and (until_ is None or t0 <= until_)]
        return round(max(w, default=0), 2)


def drain_check(new_bin, work, port):
    """A node told to stop drains first (ADR-039, `drain.rs`); a leader then steps down."""
    checks, info = {}, {}
    pg_port = port + 50
    n = Node(new_bin, os.path.join(work, "drain-lake"), port, work, "--pg", f"127.0.0.1:{pg_port}").start()
    checks["/healthz and /ready answer 200, without a token"] = n.plain("GET", "/healthz")[0] == 200 and n.plain("GET", "/ready")[0] == 200
    # A query that takes a few seconds here (each size its own: no remembered answer).
    long = lambda rows: f"SELECT sum(value % 7) AS s FROM range(0, {rows})"
    rows = 20_000_000
    for _ in range(8):
        t0 = time.time()
        n.q(long(rows))
        secs = time.time() - t0
        if secs > 2:
            break
        rows = int(rows * min(10, 3 / max(secs, 0.05)))
    rows += 1
    info["a query of"] = {"rows": rows, "seconds": round(secs, 1)}
    got = {}

    def ask(rows):
        try:
            got["rows"] = n.q(long(rows))
        except Exception as e:
            got["error"] = str(e)[:300]
        got["at"] = time.time()

    try:
        import psycopg
        pg = psycopg.connect(host="127.0.0.1", port=pg_port, user="admin", password=TOKEN, dbname="pondra", autocommit=True)
        pg.execute("SELECT 1").fetchall()
    except ImportError:
        pg = None
    th = threading.Thread(target=ask, args=(rows,))
    th.start()
    time.sleep(0.5)
    n.p.send_signal(signal.SIGTERM)
    time.sleep(0.2)
    ready, health, new = n.plain("GET", "/ready"), n.plain("GET", "/healthz"), n.plain("POST", "/sql", "SELECT 1")
    if pg:
        try:
            pg.execute("SELECT 1").fetchall()
            said = "answered"
        except psycopg.Error as e:
            said = e.sqlstate
        try:
            psycopg.connect(host="127.0.0.1", port=pg_port, user="admin", password=TOKEN, dbname="pondra", connect_timeout=5).close()
            again = "connected"
        except psycopg.Error:
            again = "refused"
        info["Postgres while stopping"] = {"a statement on a connection open before": said, "a new connection": again}
        checks["…a statement on a Postgres connection open before is answered 57P01, a new connection refused"] = said == "57P01" and again == "refused"
    else:
        info["Postgres while stopping"] = "not checked: pip install psycopg"
    th.join()
    n.p.wait(60)
    info["while stopping"] = {"/ready": ready[0], "/healthz": health[0], "a new query": [new[0], new[2][:80], {k.lower(): v for k, v in new[1].items()}.get("retry-after")]}
    checks["told to stop, a node isn't ready, and turns new requests away to be retried (503, Retry-After)"] = ready[0] == 503 and health[0] == 200 and new[0] == 503 and "retry-after" in {k.lower() for k in new[1]}
    want = (rows // 7) * 21 + sum(range(rows % 7))
    checks["…finishes the requests it has first"] = got.get("rows") == [{"s": want}]
    info["the query running"] = {k: v for k, v in got.items() if k != "at"}
    checks["…and then stops (exit 0)"] = n.p.returncode == 0 and time.time() - got["at"] < 5
    n = Node(new_bin, os.path.join(work, "drain-lake"), port, work, env={"PONDRA_DRAIN_SECS": "1"}).start()
    th = threading.Thread(target=ask, args=(rows * 4,))
    th.start()
    time.sleep(0.5)
    t0 = time.time()
    n.p.send_signal(signal.SIGTERM)
    n.p.wait(60)
    took = time.time() - t0
    th.join()
    info["seconds to stop with PONDRA_DRAIN_SECS=1 and a longer query"] = round(took, 1)
    checks["…but for PONDRA_DRAIN_SECS at most"] = took < 4
    # A leader stopped under load hands over at once; killed, its followers wait out the lease.
    lake = f"s3://{os.environ['PONDRA_BUCKET']}/drain-{os.getpid()}-{int(time.time())}" if A.s3 else os.path.join(work, "drain-cluster")
    info["the cluster's lake"] = lake
    nodes = [Node(new_bin, lake, port + i, work).start() for i in range(3)]
    nodes[0].post("/tables/ev2", json.dumps([["producer", "Utf8"], ["seq", "Int64"], ["i", "Int64"]]))
    until(lambda: all(x.q("SELECT count(*) AS n FROM ev2") for x in nodes), 20)
    live = list(nodes)
    load = Load(live, "ev2").start()
    stalls, exited = {}, None
    for how in ("term", "kill"):
        time.sleep(4)
        leader = next(x for x in live if x.get("/stats")["role"] == "leader")
        t0 = time.time()
        live.remove(leader)
        leader.stop(how)
        exited = exited or round(time.time() - t0, 2)
        until(lambda: any(x.get("/stats", timeout=2)["role"] == "leader" for x in live), 60)
        time.sleep(3)
        stalls["stopped (SIGTERM)" if how == "term" else "killed (kill -9)"] = load.longest(t0 - 0.5)
        live.append(leader.start())  # (it rejoins as a follower)
    time.sleep(3)
    result = load.finish(live[0])
    info["a leader stopped under load"] = {"longest wait for an ack, s": stalls, "stopped, it exited after, s": exited, **result}
    checks["a leader stopped under load: every acknowledged batch once, no torn read, on every node"] = not result["lost or twice"] and not result["torn reads"] and all(
        x.q("SELECT count(*) AS n FROM ev2") == live[0].q("SELECT count(*) AS n FROM ev2") for x in live)
    checks["…its followers take over sooner than when it is killed"] = stalls["stopped (SIGTERM)"] < stalls["killed (kill -9)"]
    # (its followers keep sending it flushes until it steps down; on a bucket one is always in
    # flight, and a leader that waited for none waited out PONDRA_DRAIN_SECS: run with --s3)
    checks["…and it exits within 10 s, though its followers keep sending (not after PONDRA_DRAIN_SECS)"] = exited < 10
    [x.stop() for x in live]
    if A.s3:
        gone(lake)
    return checks, info


def gone(lake):
    """A lake in a bucket deleted (the tests' own: `drain --s3`)."""
    import boto3
    bucket, prefix = lake[len("s3://"):].split("/", 1)
    s3 = boto3.client("s3", endpoint_url=os.environ.get("AWS_ENDPOINT_URL") or os.environ.get("AWS_ENDPOINT"))
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix + "/"):
        keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]
        if keys:
            s3.delete_objects(Bucket=bucket, Delete={"Objects": keys})


# ---------------------------------------------------------------- a rolling upgrade

def rolling_check(old_v, old_bin, new_bin, work, port, leader_first=False):
    """A cluster of release `old_v` upgraded to this build a node at a time, under load: the
    followers first (or the leader first), each stopped and started again on the new binary; then
    restarted a node at a time again, as the next upgrade from this build will be."""
    checks, info = {}, {}
    lake = os.path.join(work, f"rolling-{old_v}-{'leader' if leader_first else 'followers'}-first")
    nodes = [Node(old_bin, lake, port + i, work).start() for i in range(3)]
    nodes[0].post("/tables/ev2", json.dumps([["producer", "Utf8"], ["seq", "Int64"], ["i", "Int64"]]))
    nodes[0].q("CREATE TABLE kv2 (k BIGINT PRIMARY KEY, v BIGINT)")
    nodes[0].q("CREATE MATERIALIZED VIEW per_producer AS SELECT producer, count(*) AS n, max(seq) AS mx FROM ev2 GROUP BY producer")
    until(lambda: all(x.q("SELECT count(*) AS n FROM ev2") for x in nodes), 20)
    live = list(nodes)
    load = Load(live, "ev2").start()
    role = lambda x: x.get("/stats", timeout=5)["role"]
    order = sorted(nodes, key=lambda x: (role(x) == "leader") != leader_first)  # (followers first, or the leader)
    steps, k = {}, 0
    for x in order:
        time.sleep(4)
        was = role(x)
        k += 1
        x.q(f"INSERT INTO kv2 VALUES ({k}, {k}) ")
        t0 = time.time()
        live.remove(x)
        x.stop()
        if was == "leader":
            until(lambda: any(role(y) == "leader" for y in live), 60)
        y = Node(new_bin, lake, x.port, work).start()
        until(lambda: y.plain("GET", "/ready")[0] == 200, 30)
        live.append(y)
        time.sleep(2)
        steps[f"{was} {x.port} on the new binary"] = {"longest wait for an ack, s": load.longest(t0 - 0.5), "seconds": round(time.time() - t0, 1)}
    raised = until(lambda: all(y.get("/stats", timeout=5)["format"] >= 1 for y in live), 60)
    # The next upgrade, from this build: each node drained and its leader stepping down.
    since = time.time()
    for x in sorted(list(live), key=lambda x: role(x) == "leader"):
        time.sleep(3)
        live.remove(x)
        was = role(x)
        x.stop()
        if was == "leader":
            until(lambda: any(role(y) == "leader" for y in live), 60)
        y = Node(new_bin, lake, x.port, work).start()
        until(lambda: y.plain("GET", "/ready")[0] == 200, 30)
        live.append(y)
    time.sleep(2)
    restarted = load.longest(since)
    result = load.finish(live[0])
    leader = next(y for y in live if role(y) == "leader")
    info.update({"steps": steps, "then restarted from this build, longest wait for an ack, s": restarted, "releases": leader.get("/stats")["releases"], **result})
    same = lambda sql: all(y.q(sql) == live[0].q(sql) for y in live)
    checks["every acknowledged batch once, no torn read, through every step"] = not result["lost or twice"] and not result["torn reads"]
    checks["every node answers the same: rows, a keyed table, a view"] = same("SELECT count(*) AS n FROM ev2") and same("SELECT * FROM kv2 ORDER BY k") and same("SELECT * FROM per_producer ORDER BY producer")
    checks["the view equals its rows"] = live[0].q("SELECT * FROM per_producer ORDER BY producer") == live[0].q("SELECT producer, count(*) AS n, max(seq) AS mx FROM ev2 GROUP BY producer ORDER BY producer")
    checks["the lake moves to this build's format once every node runs it"] = bool(raised)
    checks["the leader hears every node say its release (none from before formats)"] = len(info["releases"]) == 3 and "older" not in info["releases"].values()
    checks["restarted a node at a time from this build, no batch waits more than 2 s (drained, the leader stepping down)"] = restarted < 2
    [y.stop() for y in live]
    return checks, info


def run(name, results, f, *args):
    """One check of this run, its nodes stopped whatever happened: True if it passed."""
    t0 = time.time()
    try:
        checks, info = f(*args)
    except Failed as e:
        checks, info = {"ran without an error": False}, {"error": str(e)[:3000]}
    finally:
        stop_all()
        NODES.clear()
    good = all(checks.values())
    results[name] = {"ok": good, "secs": round(time.time() - t0, 1), "checks": checks, **({"info": info} if not good or A.verbose else {})}
    print(f"{name}: {'ok' if good else 'FAILED'} ({results[name]['secs']} s)", file=sys.stderr, flush=True)
    return good


def main():
    global A
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("what", nargs="?", default="all", choices=["lakes", "format", "drain", "rolling", "all"])
    ap.add_argument("--new", default=os.path.join(HERE, "..", "target", "release", "pondra"), help="this build's binary")
    ap.add_argument("--from", dest="first", default="0.22.0", help="the oldest release whose lake must open")
    ap.add_argument("--only", default="", help="these releases only (comma-separated); `rolling` upgrades the newest of them")
    ap.add_argument("--cache", default=os.path.join(os.path.expanduser("~"), ".cache", "pondra-releases"))
    ap.add_argument("--work", default="", help="where lakes and logs go (default: a new temporary folder, removed if every check passes)")
    ap.add_argument("--port", type=int, default=9720)
    ap.add_argument("--verbose", action="store_true", help="what each check measured, passed or not")
    ap.add_argument("--s3", action="store_true", help="`drain`'s cluster on s3://$PONDRA_BUCKET (AWS_* point at R2, MinIO or tools/sim_r2.py)")
    A = ap.parse_args()
    new_bin = os.path.abspath(A.new)
    work = A.work or tempfile.mkdtemp(prefix="pondra-upgrade-")
    os.makedirs(work, exist_ok=True)
    wanted = [v for v in A.only.split(",") if v]
    found = [(v, url) for v, url in releases(A.first) if not wanted or v in wanted] if A.what in ("lakes", "rolling", "all") else []
    out, ok = {}, True
    if A.what in ("lakes", "all"):
        out["lakes"] = {}
        for v, url in found:
            ok &= run(v, out["lakes"], lake_check, v, binary(v, url, A.cache), new_bin, work, A.port)
        ok &= bool(found)
    if A.what in ("format", "all"):
        ok &= run("format", out, format_check, new_bin, work, A.port)
    if A.what in ("drain", "all"):
        ok &= run("drain", out, drain_check, new_bin, work, A.port)
    if A.what in ("rolling", "all"):
        v, url = found[-1]
        out["rolling"] = {}
        for first in ("followers", "leader"):
            ok &= run(f"{v}, {first} first", out["rolling"], rolling_check, v, binary(v, url, A.cache), new_bin, work, A.port, first == "leader")
    print(json.dumps({**out, "releases": [v for v, _ in found], "ok": ok}, indent=1, default=str))
    if ok and not A.work:
        shutil.rmtree(work, ignore_errors=True)
    elif not ok:
        print(f"(lakes and node logs kept in {work})", file=sys.stderr)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
