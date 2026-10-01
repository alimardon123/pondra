#!/usr/bin/env python3
"""Malformed input at every door: no request may stop a node (round 29, ADR-035 §5).

Starts a node with all four doors (HTTP, Postgres, Kafka, Flight), then for `--secs` a door sends
inputs made from valid ones and changed: bytes flipped, cut short or run on, lengths made huge,
zero or negative, and plain random bytes; over HTTP also generated SQL, odd and wrong. After each
door, and every 200 inputs: is the node still up, and does it still answer `SELECT 1`? A node
that stopped is a finding: the inputs sent last are kept in the scratch folder, with what the node
said, and the node is started again.

    python3 tools/fuzz_doors.py [--secs 30] [--door all|http|sql|pg|kafka|flight] [--seed N]

Exits 0 only if no node stopped. Local disk only.
"""
import argparse
import json
import os
import random
import shutil
import socket
import struct
import subprocess
import sys
import tempfile
import time
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
R = random.Random()
NODES = []


class Node:
    def __init__(self, binary, folder, port, env, n=0):
        self.binary, self.folder, self.port, self.env, self.p, self.n = binary, folder, port, env, None, n
        self.log = os.path.join(folder, f"node-{n}.stderr")

    def again(self):
        self.stop()
        return Node(self.binary, self.folder, self.port, self.env, self.n + 1).start()

    def start(self):
        args = [self.binary, "serve", "--lake", os.path.join(self.folder, "lake"), "--addr", f"127.0.0.1:{self.port}",
                "--pg", f"127.0.0.1:{self.port + 1}", "--kafka", f"127.0.0.1:{self.port + 2}", "--flight", f"127.0.0.1:{self.port + 3}"]
        self.p = subprocess.Popen(args, stdout=subprocess.DEVNULL, stderr=open(self.log, "a"), env={**os.environ, **self.env})
        NODES.append(self)
        deadline = time.time() + 120  # (a lake whose leader was killed: its lease runs out first)
        while time.time() < deadline:
            if self.answers():
                return self
            if self.p.poll() is not None:
                break
            time.sleep(0.1)
        raise RuntimeError("the node didn't start: " + open(self.log).read()[-800:])

    def healthy(self):
        """Up, and answering `SELECT 1` within a minute (a heavy input may keep it busy a while)."""
        deadline = time.time() + 60
        while self.up() and time.time() < deadline:
            if self.answers():
                return True
            time.sleep(1)
        return False

    def answers(self):
        try:
            req = urllib.request.Request(f"http://127.0.0.1:{self.port}/sql", data=b"SELECT 1 AS x", method="POST")
            return json.loads(urllib.request.urlopen(req, timeout=10).read()) == [{"x": 1}]
        except Exception:
            return False

    def up(self):
        return self.p.poll() is None

    def stop(self):
        if self.p and self.p.poll() is None:
            self.p.kill()  # (this node's own pid: nothing else)
            self.p.wait()


# ---------------------------------------------------------------- changing what is sent

def mutate(b: bytes) -> bytes:
    b = bytearray(b)
    for _ in range(R.randint(1, 4)):
        op = R.randrange(7)
        if op == 0 and b:  # flip bits
            i = R.randrange(len(b))
            b[i] ^= 1 << R.randrange(8)
        elif op == 1 and b:  # cut short
            del b[R.randrange(len(b)):]
        elif op == 2:  # run on
            b += os.urandom(R.randint(1, 64))
        elif op == 3 and len(b) >= 4:  # a length: huge, zero, negative
            i = R.randrange(len(b) - 3)
            b[i:i + 4] = struct.pack(">i", R.choice([0, -1, -2**31, 2**31 - 1, 1 << 24, R.randint(-100, 100)]))
        elif op == 4 and len(b) >= 2:
            i = R.randrange(len(b) - 1)
            b[i:i + 2] = struct.pack(">h", R.choice([0, -1, -32768, 32767, R.randint(-10, 10)]))
        elif op == 5 and b:  # a piece repeated
            i = R.randrange(len(b))
            b[i:i] = b[i:i + R.randint(1, 32)] * R.randint(2, 20)
        else:
            b += os.urandom(R.randint(0, 16))
    return bytes(b)


class Door:
    """Sends inputs to one port; keeps the last ones."""

    def __init__(self, name, port):
        self.name, self.port, self.sent = name, port, []
        self.counts = {"sent": 0, "answered": 0, "dropped": 0, "timeouts": 0}

    def keep(self, what):
        self.sent.append(what)
        del self.sent[:-50]
        self.counts["sent"] += 1

    def raw(self, chunks, timeout=5, idle=None):
        """Send bytes (each chunk in turn) on a new connection; read what comes back, until the
        node closes it (HTTP: `Connection: close`) or, with `idle`, nothing more comes for that
        long. (Not half-closed: a server may take that for a client gone, and drop the request.)"""
        self.keep(b"".join(chunks).hex())
        got = b""
        try:
            with socket.create_connection(("127.0.0.1", self.port), timeout=timeout) as s:
                for c in chunks:
                    s.sendall(c)
                if idle:
                    s.settimeout(idle)
                while len(got) < 1 << 16:
                    d = s.recv(65536)
                    if not d:
                        break
                    got += d
        except socket.timeout:
            if not idle:
                self.counts["timeouts"] += 1
                return got
        except OSError:
            pass
        self.counts["answered" if got else "dropped"] += 1
        return got


# ---------------------------------------------------------------- HTTP

PATHS = ["/sql", "/sql?format=arrow", "/sql?format=typed&rows=5", "/sql?format=csv", "/stats", "/tables", "/tables/t", "/views/v", "/tasks/x",
         "/append/t?producer=p&seq=1", "/ingest/t", "/lookup/t/1", "/watch/t", "/changes/t", "/files", "/files/a.sql", "/functions/f",
         "/login", "/whoami", "/cluster/beat", "/cluster/commit", "/cluster/shuffle?id=x", "/cluster/stage", "/cluster/copy", "/sessions/x",
         "/python/format", "/console/settings", "/mcp", "/iceberg/v1/config", "/iceberg/v1/namespaces", "/iceberg/v1/namespaces/public/tables/t",
         "/sql/pages/x?page=2", "/metrics", "/live", "/secrets/x", "/workspace/run"]


def http_one(door):
    method = R.choice(["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"])
    path = R.choice(PATHS)
    if R.random() < 0.3:
        path = mutate(path.encode()).decode("latin-1").replace("\r", "").replace("\n", "").replace(" ", "%20") or "/"
        path = path if path.startswith("/") else "/" + path
    body = R.choice([b"", b"{}", b"[]", b'{"sql": 1}', b'{"user": null, "password": []}', b"null", os.urandom(R.randint(1, 300)),
                     json.dumps({"sql": "SELECT 1", "params": {"a": [1, {"b": None}]}, "tables": {"t": [{"a": 1}]}}).encode(),
                     b'[["a", "Int64"], ["b", "Nope"]]', b'{"a": ' * 2000])
    if R.random() < 0.5:
        body = mutate(body)
    headers = {"Content-Length": str(len(body))}
    if R.random() < 0.3:
        headers["Content-Type"] = R.choice(["application/json", "text/plain", "application/vnd.apache.arrow.stream", "multipart/form-data; boundary=x", "\x00"])
    if R.random() < 0.3:
        headers["Authorization"] = R.choice(["Basic !!!", "Basic " + "QUFB" * 100, "Bearer pn_" + "A" * 50, "Bearer ps_x.y", "Bearer ", "Digest x"])
    if R.random() < 0.1:
        headers["Content-Length"] = R.choice(["-1", "99999999999999", "abc", str(len(body) + 1000)])
    if R.random() < 0.1:
        headers["Transfer-Encoding"] = "chunked"
        body = mutate(b"5\r\nhello\r\n0\r\n\r\n")
    head = f"{method} {path} HTTP/1.1\r\nHost: x\r\nConnection: close\r\n" + "".join(f"{k}: {v}\r\n" for k, v in headers.items()) + "\r\n"
    msg = head.encode("latin-1", "replace") + body
    if R.random() < 0.15:
        msg = mutate(msg)
    door.raw([msg], timeout=10, idle=0.5)


# ---------------------------------------------------------------- SQL over HTTP

VALUES = ["1", "-1", "0", "NULL", "'a'", "''", "'é😀'", "1e308", "-1e308", "9223372036854775807", "-9223372036854775808", "18446744073709551616",
          "1.5", "'2026-10-01'", "TRUE", "x'00ff'", "[1, 2, 3]", "{'a': 1}", "ARRAY[1, NULL]", "'NaN'::double", "INTERVAL '1 day'",
          "'{\"a\": [1, 2]}'", "NULL::int", "'1'::bigint", "'x'::int", "now()", "random()", "generate_series(1, 3)"]
FUNCS = ["abs", "length", "upper", "substr", "round", "coalesce", "nullif", "to_timestamp", "date_trunc", "array_length", "unnest", "sum",
         "count", "avg", "min", "max", "string_agg", "regexp_replace", "split_part", "json_get", "cosine_similarity", "md5", "sha256",
         "make_array", "struct", "named_struct", "arrow_cast", "date_part", "lpad", "repeat", "power", "log", "sqrt", "first_value",
         "row_number", "lag", "array_agg", "approx_distinct", "concat", "trim", "cast", "to_char", "pondra.start", "files", "read_csv",
         "read_parquet", "range", "secrets", "ai_complete", "file_read"]
TYPES = ["INT", "BIGINT", "DOUBLE", "DECIMAL(38, 10)", "DECIMAL(3, 2)", "VARCHAR", "DATE", "TIMESTAMP", "TIMESTAMPTZ", "BOOLEAN", "BYTEA",
         "INT[]", "INTERVAL", "TIME", "JSON", "UUID", "SMALLINT", "TINYINT", "FLOAT"]


def expr(d=0):
    if d > 4 or R.random() < 0.3:
        return R.choice(VALUES + ["a", "b", "t.a", "*"])
    k = R.randrange(8)
    if k == 0:
        return f"{expr(d + 1)} {R.choice(['+', '-', '*', '/', '%', '||', '=', '<', 'AND', 'OR', 'LIKE', 'IS NOT DISTINCT FROM', '<->', '->', '->>', '@>'])} {expr(d + 1)}"
    if k == 1:
        return f"{R.choice(FUNCS)}({', '.join(expr(d + 1) for _ in range(R.randint(0, 4)))})"
    if k == 2:
        return f"CAST({expr(d + 1)} AS {R.choice(TYPES)})"
    if k == 3:
        return f"{expr(d + 1)}::{R.choice(TYPES)}"
    if k == 4:
        return f"CASE WHEN {expr(d + 1)} THEN {expr(d + 1)} ELSE {expr(d + 1)} END"
    if k == 5:
        return f"({query(d + 1)})"
    if k == 6:
        return f"{R.choice(['sum', 'count', 'row_number', 'lag', 'avg'])}({expr(d + 1)}) OVER (PARTITION BY {expr(d + 1)} ORDER BY {expr(d + 1)} ROWS BETWEEN {R.choice(['UNBOUNDED PRECEDING', '1 PRECEDING', 'CURRENT ROW'])} AND CURRENT ROW)"
    return f"{expr(d + 1)} IN ({', '.join(expr(d + 1) for _ in range(R.randint(0, 3)))})"


def source(d):
    return R.choice(["t", "(VALUES (1, 'a'), (2, NULL)) AS t(a, b)", "generate_series(1, 5) AS t(a)", "range(3) AS t(a)", "unnest([1, 2]) AS t(a)",
                     f"({query(d + 1)}) AS t", "pondra.tables", "pondra.runs", "pondra.users", "pondra.audit", "information_schema.columns",
                     "pg_catalog.pg_class", "files('*') AS t", "read_csv('/nope.csv') AS t", "t AS OF TIMESTAMP '2020-01-01'"])


def query(d=0):
    if d > 3:
        return "SELECT 1"
    parts = [f"SELECT {R.choice(['', 'DISTINCT '])}{', '.join(expr(d + 1) for _ in range(R.randint(1, 3)))}"]
    if R.random() < 0.8:
        parts.append(f"FROM {source(d)}")
        if R.random() < 0.4:
            parts.append(f"{R.choice(['JOIN', 'LEFT JOIN', 'FULL JOIN', 'CROSS JOIN', 'ASOF JOIN', 'NATURAL JOIN', 'SEMI JOIN', 'ANTI JOIN'])} {source(d)} AS u ON {expr(d + 1)}")
    for clause in ["WHERE", "GROUP BY", "HAVING", "ORDER BY", "QUALIFY"]:
        if R.random() < 0.25:
            parts.append(f"{clause} {expr(d + 1)}")
    if R.random() < 0.2:
        parts.append(f"{R.choice(['UNION', 'UNION ALL', 'EXCEPT', 'INTERSECT'])} {query(d + 1)}")
    parts.append(f"LIMIT {R.choice(['5', '0', '-1', 'NULL', '1e3', '(SELECT 1)'])}")
    sql = " ".join(parts)
    if d == 0 and R.random() < 0.15:
        sql = f"WITH {R.choice(['', 'RECURSIVE '])}c(a) AS ({query(d + 1)}) {sql.replace('FROM t', 'FROM c AS t')}"
    if d == 0 and R.random() < 0.1:
        sql = R.choice(["EXPLAIN ", "EXPLAIN ANALYZE ", "SELECT * FROM (", "DESCRIBE "]) + sql + (")" if sql.startswith("SELECT * FROM (") else "")
    return sql


STATEMENTS = ["CREATE TABLE IF NOT EXISTS t (a BIGINT, b VARCHAR)", "INSERT INTO t VALUES (1, 'a'), (2, NULL)", "UPDATE t SET b = 'x' WHERE a = 1",
              "DELETE FROM t WHERE a > 100", "ALTER TABLE t ADD COLUMN c INT", "CREATE VIEW v AS SELECT * FROM t", "DROP VIEW IF EXISTS v",
              "MERGE INTO t USING (SELECT 1 AS a) s ON t.a = s.a WHEN MATCHED THEN UPDATE SET b = 'm'",  # (no CREATE USER: the lake stays open) "CREATE SECRET s (TYPE s3, KEY_ID 'k', SECRET 'v')", "CREATE TEMPORARY TABLE tt AS SELECT 1 AS a",
              "COPY t TO STDOUT", "SET x = 1", "BEGIN", "COMMIT", "SHOW TABLES", "SHOW VIEWS", "CALL nope()", "DO $$ SELECT 1 $$",
              "CREATE FUNCTION f(x INT) RETURNS INT AS $$ SELECT x + 1 $$", "SELECT f(1)", "CREATE MACRO m(x) AS x * 2", "SELECT m(3)",
              "CREATE TASK k SCHEDULE '5 minutes' AS CALL nope()", "DROP TASK IF EXISTS k", "CHECKPOINT", "OPTIMIZE t", "ATTACH 'nope' AS n"]


def sql_one(door):
    k = R.random()
    if k < 0.6:
        sql = query()
    elif k < 0.8:
        sql = R.choice(STATEMENTS)
        if R.random() < 0.5:
            sql = mutate(sql.encode()).decode("utf-8", "replace")
    else:  # token soup
        words = ["SELECT", "FROM", "WHERE", "(", ")", ",", "'", '"', "::", "[", "]", "{", "}", "$$", "$1", ";", "--", "/*", "*/", "\\", "E'\\x00'",
                 "OVER", "PARTITION", "JOIN", "ON", "AS", "OF", "NULL", "1", "t", "*", "CAST", "INTERVAL", "ARRAY", "STRUCT", "LATERAL", "UNNEST"]
        sql = " ".join(R.choice(words) for _ in range(R.randint(1, 40)))
    body = sql.encode()
    msg = f"POST /sql HTTP/1.1\r\nHost: x\r\nConnection: close\r\nContent-Length: {len(body)}\r\n\r\n".encode() + body
    door.raw([msg], timeout=30)
    door.sent[-1] = sql  # (kept as SQL, not as the request's bytes)


# ---------------------------------------------------------------- Postgres

def pg_msg(kind: bytes, body: bytes) -> bytes:
    return kind + struct.pack(">i", len(body) + 4) + body


def pg_startup(user="admin"):
    body = struct.pack(">i", 196608) + b"user\0" + user.encode() + b"\0database\0lake\0\0"
    return struct.pack(">i", len(body) + 4) + body


def pg_one(door):
    starts = [pg_startup(), struct.pack(">ii", 8, 80877103), struct.pack(">ii", 8, 80877104), struct.pack(">iiii", 16, 80877102, 1, 2),
              struct.pack(">ii", 8, 196608), pg_startup("nobody")]
    first = R.choice(starts)
    if R.random() < 0.4:
        first = mutate(first)
    sql = R.choice(["SELECT 1", "SELECT $1::int", query(2), "COPY t FROM STDIN", "BEGIN", ""]).encode() + b"\0"
    msgs = [pg_msg(b"Q", sql),
            pg_msg(b"P", b"s1\0" + sql + struct.pack(">h", 1) + struct.pack(">i", 23)),
            pg_msg(b"B", b"\0s1\0" + struct.pack(">hh", 1, 0) + struct.pack(">h", 1) + struct.pack(">i", 4) + b"1234" + struct.pack(">h", 0)),
            pg_msg(b"D", b"S" + b"s1\0"), pg_msg(b"D", b"P\0"), pg_msg(b"E", b"\0" + struct.pack(">i", 0)), pg_msg(b"S", b""),
            pg_msg(b"H", b""), pg_msg(b"C", b"S" + b"s1\0"), pg_msg(b"d", os.urandom(R.randint(0, 50))), pg_msg(b"c", b""),
            pg_msg(b"f", b"no\0"), pg_msg(b"p", b"x\0"), pg_msg(b"F", os.urandom(12)), pg_msg(bytes([R.randrange(256)]), os.urandom(R.randint(0, 40)))]
    then = [R.choice(msgs) for _ in range(R.randint(1, 6))]
    then = [mutate(m) if R.random() < 0.5 else m for m in then] + [pg_msg(b"X", b"")]
    door.raw([first] + then, timeout=8, idle=0.3)


# ---------------------------------------------------------------- Kafka

def kstr(s):
    return struct.pack(">h", -1) if s is None else struct.pack(">h", len(s)) + s


def kafka_frame(key, ver, body, corr=1):
    head = struct.pack(">hhi", key, ver, corr) + kstr(b"fuzz")
    if ver >= R.choice([9, 99]) and key in (0, 1, 3):  # (a flexible header's tagged fields, sometimes)
        head += b"\0"
    msg = head + body
    return struct.pack(">i", len(msg)) + msg


def record_batch():
    # A v2 record batch, one record, CRC left wrong half the time.
    rec = b"\x10\x00\x00\x00\x02k\x02v\x00"
    batch = struct.pack(">qi", 0, 0) + struct.pack(">ib", 0, 2) + struct.pack(">I", 0) + struct.pack(">hiqqqhii", R.choice([0, 1, 2, 3, 4, 7]), 0, 0, 0, -1, -1, -1, 1) + rec
    return struct.pack(">i", len(batch)) + batch


def kafka_one(door):
    produce = struct.pack(">hi", -1, 1000) + struct.pack(">i", 1) + kstr(b"t") + struct.pack(">i", 1) + struct.pack(">i", 0) + record_batch()
    fetch = struct.pack(">iii", -1, 100, 1) + struct.pack(">i", 1048576) + struct.pack(">b", 0) + struct.pack(">i", 1) + kstr(b"t") + struct.pack(">i", 1) + struct.pack(">iqi", 0, 0, 1048576)
    bodies = {18: b"", 3: struct.pack(">i", -1), 0: kstr(None) + produce, 1: fetch, 2: struct.pack(">i", -1) + struct.pack(">i", 1) + kstr(b"t") + struct.pack(">i", 1) + struct.pack(">iq", 0, -1),
              10: kstr(b"g"), 11: kstr(b"g") + struct.pack(">i", 1000) + kstr(b"") + kstr(b"consumer") + struct.pack(">i", 1) + kstr(b"range") + struct.pack(">i", 4) + b"meta",
              17: kstr(b"PLAIN"), 36: struct.pack(">i", 10) + b"\0u\0passwrd", 22: kstr(None) + struct.pack(">i", 1000), 8: kstr(b"g") + struct.pack(">i", -1),
              9: kstr(b"g") + struct.pack(">i", -1), 12: kstr(b"g") + struct.pack(">i", 1) + kstr(b"m"), 14: kstr(b"g") + struct.pack(">i", 1) + kstr(b"m") + struct.pack(">i", 0),
              19: struct.pack(">i", 1) + kstr(b"t") + struct.pack(">ih", 1, 1) + struct.pack(">ii", 0, 0), 32: struct.pack(">i", 1) + struct.pack(">b", 2) + kstr(b"t") + struct.pack(">i", -1)}
    frames = []
    for _ in range(R.randint(1, 4)):
        key = R.choice(list(bodies) + [R.randrange(-2, 80)])
        ver = R.choice([0, 1, 2, 3, 4, 7, 9, 12, 13, 99, -1])
        body = bodies.get(key, os.urandom(R.randint(0, 40)))
        f = kafka_frame(key, ver, mutate(body) if R.random() < 0.6 else body, R.randrange(1 << 31))
        frames.append(mutate(f) if R.random() < 0.2 else f)
    if R.random() < 0.1:
        frames = [struct.pack(">i", R.choice([-5, 0, 1 << 30, 129 << 20])) + os.urandom(20)]
    door.raw(frames, timeout=8, idle=0.3)


# ---------------------------------------------------------------- Flight (gRPC over HTTP/2)

def h2_frame(kind, flags, stream, payload):
    return struct.pack(">I", len(payload))[1:] + bytes([kind, flags]) + struct.pack(">I", stream) + payload


def grpc_headers(path):
    # HPACK, literal without indexing, new names (simple and valid).
    def lit(n, v):
        return b"\x00" + bytes([len(n)]) + n + bytes([len(v)]) + v
    return lit(b":method", b"POST") + lit(b":scheme", b"http") + lit(b":path", path) + lit(b":authority", b"x") + lit(b"content-type", b"application/grpc") + lit(b"te", b"trailers")


def flight_one(door):
    preface = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"
    path = R.choice([b"/arrow.flight.protocol.FlightService/" + m for m in [b"Handshake", b"ListFlights", b"GetFlightInfo", b"GetSchema", b"DoGet", b"DoPut", b"DoAction", b"ListActions", b"DoExchange", b"PollFlightInfo"]] + [b"/nope/x"])
    msg = os.urandom(R.randint(0, 60)) if R.random() < 0.5 else b"\x0a" + bytes([R.randint(0, 60)]) + os.urandom(R.randint(0, 40))
    grpc = b"\x00" + struct.pack(">I", len(msg) if R.random() < 0.8 else R.choice([0, 1 << 31, 1 << 24])) + msg
    frames = [preface, h2_frame(4, 0, 0, b""), h2_frame(1, 4, 1, grpc_headers(path)), h2_frame(0, 1, 1, grpc)]
    if R.random() < 0.5:
        i = R.randrange(1, len(frames))
        frames[i] = mutate(frames[i])
    if R.random() < 0.2:
        frames.append(h2_frame(R.randrange(12), R.randrange(256), R.choice([0, 1, 3, 2**31 - 1]), os.urandom(R.randint(0, 30))))
    if R.random() < 0.1:
        frames = [R.choice([b"GET / HTTP/1.1\r\n\r\n", os.urandom(40), preface + os.urandom(40)])]
    door.raw(frames, timeout=8, idle=0.3)


# ---------------------------------------------------------------- running

def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--bin", default=os.path.join(HERE, "..", "target", "release", "pondra"))
    ap.add_argument("--secs", type=float, default=30)
    ap.add_argument("--port", type=int, default=8700)
    ap.add_argument("--seed", type=int, default=None)
    ap.add_argument("--door", default="all")
    a = ap.parse_args()
    seed = a.seed if a.seed is not None else int(time.time())
    R.seed(seed)
    folder = tempfile.mkdtemp(prefix="pondra-fuzz-")
    node = Node(os.path.abspath(a.bin), folder, a.port, {"PONDRA_AUDIT": "off"}).start()
    run = {"http": http_one, "sql": sql_one, "pg": pg_one, "kafka": kafka_one, "flight": flight_one}
    ports = {"http": a.port, "sql": a.port, "pg": a.port + 1, "kafka": a.port + 2, "flight": a.port + 3}
    doors = list(run) if a.door == "all" else a.door.split(",")
    # (something to read and write: a table t)
    req = urllib.request.Request(f"http://127.0.0.1:{a.port}/sql", data=b"CREATE TABLE t (a BIGINT, b VARCHAR); INSERT INTO t VALUES (1, 'a'), (2, 'b')", method="POST")
    urllib.request.urlopen(req, timeout=30).read()
    report, stops = {"seed": seed, "doors": {}}, []
    try:
        fuzz(a, folder, doors, run, ports, report, stops, node)
    finally:
        for n in NODES:
            n.stop()  # (whatever happened: no node left behind)
    report["stops"] = stops
    report["ok"] = not stops
    print(json.dumps(report, indent=1))
    if not stops:
        shutil.rmtree(folder, ignore_errors=True)
    sys.exit(0 if not stops else 1)


def fuzz(a, folder, doors, run, ports, report, stops, node):
    for name in doors:
        door, end, n = Door(name, ports[name]), time.time() + a.secs, 0
        while time.time() < end:
            run[name](door)
            n += 1
            if (n % 200 == 0 or not node.up()) and not node.healthy():
                said = open(node.log, errors="replace").read()[-4000:]
                kept = os.path.join(folder, f"stopped-{name}-{len(stops)}.json")
                json.dump({"door": name, "up": node.up(), "last": door.sent, "node said": said}, open(kept, "w"), indent=1, default=str)
                stops.append({"door": name, "up": node.up(), "kept": kept, "said": said[-800:]})
                print(f"{name}: the node {'stopped answering' if node.up() else 'stopped'}; the last inputs are in {kept}", file=sys.stderr)
                node = node.again()
        alive = node.healthy()
        report["doors"][name] = {**door.counts, "node up after": alive}
        print(f"{name}: {door.counts} up: {alive}", file=sys.stderr)
        if not alive:
            stops.append({"door": name, "up": node.up(), "said": open(node.log, errors="replace").read()[-800:]})
            node = node.again()


if __name__ == "__main__":
    main()
