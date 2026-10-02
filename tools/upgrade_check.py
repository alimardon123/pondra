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

Prints the checks as JSON and exits 1 if one fails (a failing release's lakes and node logs are
kept in --work). Needs the Python packages in tools/requirements.txt for other engines' reads
(deltalake, pyiceberg); without them those checks are skipped, and say so.
"""
import argparse, atexit, collections, io, json, os, platform, shutil, signal, socket, subprocess, sys, tarfile, tempfile, time, urllib.error, urllib.request

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
    headers = {"Accept": "application/vnd.github+json"}
    if os.environ.get("GITHUB_TOKEN"):  # (CI: the API's limit for a runner's shared address is low)
        headers["Authorization"] = f"Bearer {os.environ['GITHUB_TOKEN']}"
    req = urllib.request.Request(f"https://api.github.com/repos/{REPO}/releases?per_page=100", headers=headers)
    found = []
    for r in json.load(urllib.request.urlopen(req, timeout=60)):
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

    def __init__(self, bin, lake, port, work, *flags):
        self.bin, self.lake, self.port, self.flags = bin, lake, port, list(flags)
        self.log = os.path.join(work, f"node-{port}-{os.path.basename(lake)}.log")
        self.p = None

    def start(self):
        with socket.socket() as s:  # (a node left from a run that died would answer for this one)
            if s.connect_ex(("127.0.0.1", self.port)) == 0:
                raise Failed(f"port {self.port} is in use: stop what serves it")
        env = {**os.environ, "PONDRA_SECRET_KEY": SECRET_KEY, "PONDRA_OWNER_KEY": OWNER, "PONDRA_ADMIN_TOKEN": TOKEN}
        with open(self.log, "a") as err:
            err.write(f"\n=== {self.bin} serve {self.lake} {' '.join(self.flags)}\n")
            err.flush()
            self.p = subprocess.Popen([self.bin, "serve", "--dir", self.lake, "--addr", f"127.0.0.1:{self.port}", "--admin-token", TOKEN, "--tier-secs", "0", *self.flags],
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

    def stop(self, how="term"):
        if self.p and self.p.poll() is None:
            self.p.send_signal(signal.SIGKILL if how == "kill" else signal.SIGTERM)
            try:
                self.p.wait(timeout=30)
            except subprocess.TimeoutExpired:
                self.p.kill()
                self.p.wait()

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


def cli(bin, lake, sql):
    """`pondra sql` on the lake, as a user's machine runs it (no node needed)."""
    env = {**os.environ, "PONDRA_SECRET_KEY": SECRET_KEY, "PONDRA_ADMIN_TOKEN": TOKEN}
    r = subprocess.run([bin, "sql", "--dir", lake, sql], capture_output=True, text=True, timeout=300, env=env)
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
        q("CREATE TASK hourly SCHEDULE '1 hour' AS INSERT INTO s1.t (x) VALUES (-1)")
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


def main():
    global A
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("what", nargs="?", default="lakes", choices=["lakes"])
    ap.add_argument("--new", default=os.path.join(HERE, "..", "target", "release", "pondra"), help="this build's binary")
    ap.add_argument("--from", dest="first", default="0.22.0", help="the oldest release whose lake must open")
    ap.add_argument("--only", default="", help="these releases only (comma-separated)")
    ap.add_argument("--cache", default=os.path.join(os.path.expanduser("~"), ".cache", "pondra-releases"))
    ap.add_argument("--work", default="", help="where lakes and logs go (default: a new temporary folder, removed if every check passes)")
    ap.add_argument("--port", type=int, default=9720)
    A = ap.parse_args()
    new_bin = os.path.abspath(A.new)
    work = A.work or tempfile.mkdtemp(prefix="pondra-upgrade-")
    os.makedirs(work, exist_ok=True)
    wanted = [v for v in A.only.split(",") if v]
    found = [(v, url) for v, url in releases(A.first) if not wanted or v in wanted]
    results, ok = {}, True
    for v, url in found:
        t0 = time.time()
        try:
            checks, info = lake_check(v, binary(v, url, A.cache), new_bin, work, A.port)
        except Failed as e:
            checks, info = {"made, opened and written without an error": False}, {"error": str(e)[:3000]}
        finally:
            stop_all()
            NODES.clear()
        good = all(checks.values())
        ok &= good
        results[v] = {"ok": good, "secs": round(time.time() - t0, 1), "checks": checks, **({"info": info} if not good else {})}
        print(f"{v}: {'ok' if good else 'FAILED'} ({results[v]['secs']} s)", file=sys.stderr, flush=True)
    ok &= bool(found)
    print(json.dumps({"lakes": results, "releases": [v for v, _ in found], "ok": ok}, indent=1, default=str))
    if ok and not A.work:
        shutil.rmtree(work, ignore_errors=True)
    elif not ok:
        print(f"(lakes and node logs kept in {work})", file=sys.stderr)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
