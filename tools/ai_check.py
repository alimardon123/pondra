#!/usr/bin/env python3
"""Models and vectors in SQL (ADR-051's phase 0): `ai_complete` and `ai_embed` against a stand-in
OpenAI-compatible endpoint that counts what it is asked, and the vector functions against numpy (and
DuckDB, when it is installed).

  ai_check.py [--new target/release/pondra] [--work DIR] [--port 9770] [--vectors 200000]

Checks that each distinct text is asked once a batch, that embeddings go many to a request (and one
at a time to an endpoint that takes no more), that a 429 or a 5xx is tried again, that a call that
fails for good is NULL and counted, that a node keeps to `PONDRA_AI_CALLS` requests in flight, that a
materialized view embeds each row once, that every vector function and its DuckDB name agree with
numpy, and how fast an exact top 10 is (DuckDB's beside it). Prints the checks as JSON and exits 1
if one fails (the lake and the node's log are then kept in --work).
"""
import argparse, http.server, json, os, shutil, sys, tempfile, threading, time

import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(HERE, "..", "python"))
from upgrade_check import Node, Failed, stop_all, TOKEN  # (a node started and stopped as a scheduler would)

WORDS = ("tea", "cake", "coffee", "lemon", "printer", "refund", "late")


class Endpoint(http.server.ThreadingHTTPServer):
    """A stand-in model: chat answers `len:<characters>`, an embedding counts WORDS. `mode` says how
    it misbehaves: `single` (refuses a list of texts), `busy` (429 for the next `n` requests), `boom`
    (500 for any text with "boom" in it); it counts requests, texts and the most at once."""

    daemon_threads = True

    def __init__(self):
        super().__init__(("127.0.0.1", 0), Handler)
        self.lock = threading.Lock()
        self.reset()

    def reset(self, mode="", n=0, delay=0.0):
        with self.lock:
            self.mode, self.n, self.delay = mode, n, delay
            self.chats, self.embeds, self.texts, self.now, self.most = 0, 0, 0, 0, 0


class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def reply(self, status, body, headers=()):
        out = json.dumps(body).encode()
        self.send_response(status)
        for k, v in [("content-type", "application/json"), ("content-length", str(len(out))), *headers]:
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(out)

    def do_POST(self):
        s = self.server
        body = json.loads(self.rfile.read(int(self.headers["content-length"])))
        with s.lock:
            s.now += 1
            s.most = max(s.most, s.now)
        try:
            time.sleep(s.delay)
            with s.lock:
                busy = s.mode == "busy" and s.n > 0
                s.n -= busy
            if busy:
                return self.reply(429, {"error": "slow down"}, [("retry-after", "0")])
            if self.path.endswith("/embeddings"):
                texts = body["input"] if isinstance(body["input"], list) else [body["input"]]
                if s.mode == "single" and isinstance(body["input"], list) and len(texts) > 1:
                    return self.reply(400, {"error": "one text a request"})
                with s.lock:
                    s.embeds += 1
                    s.texts += len(texts)
                data = [{"index": i, "embedding": [float(t.lower().split().count(w)) for w in WORDS]} for i, t in enumerate(texts)]
                return self.reply(200, {"data": data[::-1]})  # (out of order: the index says which)
            text = body["messages"][-1]["content"]
            with s.lock:
                s.chats += 1
            if s.mode == "boom" and "boom" in text:
                return self.reply(500, {"error": "boom"}, [("retry-after", "0")])
            return self.reply(200, {"choices": [{"message": {"role": "assistant", "content": f"len:{len(text)}"}}]})
        finally:
            with s.lock:
                s.now -= 1


def ai_check(bin, work, port, vectors):
    ep = Endpoint()
    threading.Thread(target=ep.serve_forever, daemon=True).start()
    env = {"PONDRA_AI_URL": f"http://127.0.0.1:{ep.server_address[1]}/v1", "PONDRA_AI_MODEL": "m", "PONDRA_EMBED_MODEL": "e",
           "PONDRA_AI_CALLS": "3", "NO_PROXY": "127.0.0.1,localhost", "no_proxy": "127.0.0.1,localhost"}
    n = Node(bin, os.path.join(work, "lake"), port, work, env=env).start()
    q = n.q
    checks = {}

    def requests():
        text = n.get("/metrics").decode()
        return {r: float(line.split()[-1]) for line in text.splitlines() for r in ("answered", "failed", "retried") if line.startswith(f'pondra_ai_requests_total{{result="{r}"}}')}

    # Each distinct text once a batch: 300 rows asking 3 questions, 3 requests.
    q("CREATE TABLE t (id BIGINT, body VARCHAR)")
    q("INSERT INTO t SELECT x, CASE x % 3 WHEN 0 THEN 'tea and cake' WHEN 1 THEN 'printer refund' ELSE 'late coffee' END FROM generate_series(1, 300) AS s(x)")
    ep.reset()
    got = q("SELECT body, ai_complete(body) AS a FROM t")
    checks["each distinct text asked once a batch"] = all(r["a"] == f"len:{len(r['body'])}" for r in got) and len(got) == 300 and ep.chats == 3
    checks["the model named in the call is the one asked"] = q("SELECT ai_complete('x', 'other') AS a")[0]["a"] == "len:1"

    # Embeddings many to a request, in order whatever order they come back in.
    q("CREATE TABLE docs (id BIGINT, body VARCHAR)")
    q("INSERT INTO docs SELECT x, 'tea ' || repeat('cake ', CAST(x % 50 AS INT)) || x FROM generate_series(1, 400) AS s(x)")
    ep.reset()
    got = q("SELECT id, ai_embed(body) AS v FROM docs ORDER BY id")
    right = all(r["v"] == [1.0, float(r["id"] % 50), 0, 0, 0, 0, 0] for r in got)
    checks["embeddings go many to a request, each row its own"] = right and len(got) == 400 and ep.texts == 400 and ep.embeds <= 400 / 64 + 8
    checks["(requests for 400 embeddings)"] = ep.embeds
    ep.reset("single")
    got = q("SELECT id, ai_embed(body) AS v FROM docs WHERE id <= 100 ORDER BY id")
    checks["an endpoint that takes one text a request: each alone"] = all(r["v"] == [1.0, float(r["id"] % 50), 0, 0, 0, 0, 0] for r in got) and len(got) == 100

    # Tried again after a 429; a 5xx that never ends is NULL, and counted.
    before = requests()
    ep.reset("busy", n=3)
    got = q("SELECT ai_complete('busy ' || id) AS a FROM t WHERE id <= 2")
    after = requests()
    checks["a 429 is tried again: every row answered"] = all(r["a"] is not None for r in got) and after["retried"] - before["retried"] >= 3
    ep.reset("boom")
    got = q("SELECT id, ai_complete(CASE WHEN id = 1 THEN 'boom' ELSE 'fine ' || id END) AS a FROM t WHERE id <= 4 ORDER BY id")
    failed = requests()["failed"] - after["failed"]
    checks["a call that fails for good is NULL, the rest answered, counted in /metrics"] = [r["a"] is None for r in got] == [True, False, False, False] and failed == 1
    checks["…and said on the node's standard error"] = "ai_complete:" in n.said()

    # The node's budget: never more than PONDRA_AI_CALLS (3) requests in flight, whatever the query.
    ep.reset(delay=0.05)
    got = q("SELECT ai_complete('q ' || id) AS a FROM t")
    checks["at most PONDRA_AI_CALLS requests in flight"] = all(r["a"] for r in got) and 1 <= ep.most <= 3
    checks["(most in flight)"] = ep.most

    # A materialized view embeds each new row once.
    q("CREATE TABLE notes (id BIGINT, body VARCHAR)")
    q("CREATE MATERIALIZED VIEW note_vectors AS SELECT id, ai_embed(body) AS v FROM notes")
    ep.reset()
    q("INSERT INTO notes VALUES (1, 'green tea'), (2, 'lemon cake'), (3, 'green tea')")
    got = q("SELECT id, v FROM note_vectors ORDER BY id")
    checks["a view embeds each row as it is written, each text once"] = [r["v"][:4] for r in got] == [[1, 0, 0, 0], [0, 1, 0, 1], [1, 0, 0, 0]] and ep.texts == 2

    # The vector functions: equal to numpy's, under every name; NULLs and lengths.
    rng = np.random.default_rng(7)
    a, b = rng.standard_normal((64, 384)).astype(np.float32), rng.standard_normal((64, 384)).astype(np.float32)
    rows = ", ".join(f"({i}, {list(map(float, a[i]))}, {list(map(float, b[i]))})" for i in range(64))
    q("CREATE TABLE vv (id BIGINT, a FLOAT[], b FLOAT[])")
    q(f"INSERT INTO vv VALUES {rows}")
    names = {"cosine_similarity": ["array_cosine_similarity", "list_cosine_similarity"], "cosine_distance": ["array_cosine_distance", "list_cosine_distance"],
             "l2_distance": ["array_distance", "list_distance"], "dot_product": ["inner_product", "array_inner_product", "list_inner_product"]}
    calls = ", ".join(f"{f}(a, b) AS {f}" for name, fs in names.items() for f in (name, *fs))
    got = q(f"SELECT id, {calls} FROM vv ORDER BY id")
    x, y = a.astype(np.float64), b.astype(np.float64)
    cos = (x * y).sum(1) / (np.linalg.norm(x, axis=1) * np.linalg.norm(y, axis=1))
    want = {"cosine_similarity": cos, "cosine_distance": 1 - cos, "l2_distance": np.linalg.norm(x - y, axis=1), "dot_product": (x * y).sum(1)}
    close = lambda u, v: abs(u - v) <= 1e-9 * max(1.0, abs(v))
    checks["every vector function == numpy, under every name"] = all(close(r[f], want[name][r["id"]]) for name, fs in names.items() for f in [name, *fs] for r in got)
    one = q("SELECT cosine_similarity([1.0, 0.0], [1.0, 0.0]) AS s, l2_distance([1, 2], [1, 2]) AS d, dot_product(CAST(NULL AS FLOAT[]), [1.0]) AS n, "
            "cosine_similarity([1.0, 2.0], [1.0]) AS m, cosine_similarity([1.0, NULL], [1.0, 2.0]) AS h, cosine_distance([0.0, 0.0], [1.0, 0.0]) AS z")[0]
    checks["literals, NULLs, a NULL inside, lengths that differ"] = one == {"s": 1.0, "d": 0.0, "n": None, "m": None, "h": None, "z": 1.0}
    checks["a query's vector against the column (sorted by distance)"] = [r["id"] for r in q(f"SELECT id FROM vv ORDER BY cosine_distance(a, {list(map(float, a[5]))}) LIMIT 1")] == [5]

    # A frame's + joins text, as Polars' does (and still adds numbers).
    import pondra
    from pondra import col, lit
    db = pondra.connect(f"http://127.0.0.1:{port}", token=TOKEN)
    got = db.table("t").filter(col("id") == 1).select((lit("ask: ") + col("body") + "?").alias("p"), (col("id") + 1).alias("n")).rows()
    checks["a frame's + joins text, as Polars' does"] = got == [{"p": "ask: printer refund?", "n": 2}]

    # How fast an exact top 10 is, beside DuckDB's.
    if vectors:
        import pyarrow as pa
        d = 384
        m = rng.standard_normal((vectors, d), dtype=np.float32)
        q("CREATE TABLE big (id BIGINT, v FLOAT[])")
        for i in range(0, vectors, 50_000):
            part = m[i:i + 50_000]
            v = pa.ListArray.from_arrays(pa.array(np.arange(len(part) + 1, dtype=np.int32) * d), pa.array(part.ravel()))
            db.append("big", pa.table({"id": pa.array(np.arange(i, i + len(part)), pa.int64()), "v": v}))
        n.post("/tier")
        probe = rng.standard_normal(d).astype(np.float32)
        top = lambda: [r["id"] for r in db.sql("SELECT id FROM big ORDER BY cosine_distance(v, $q) LIMIT 10", q=probe.tolist()).rows()]
        times = []
        for _ in range(5):
            t0 = time.time()
            ids = top()
            times.append(time.time() - t0)
        p64 = probe.astype(np.float64)
        sims = np.concatenate([(c @ p64) / (np.linalg.norm(c, axis=1) * np.linalg.norm(p64)) for c in (m[i:i + 50_000].astype(np.float64) for i in range(0, vectors, 50_000))])
        checks["an exact top 10 of the vectors == numpy's"] = ids == list(np.argsort(-sims, kind="stable")[:10])
        checks[f"(top 10 of {vectors} × {d}: best of 5, s)"] = round(min(times), 3)
        try:
            import duckdb
            con = duckdb.connect()
            fixed = pa.table({"id": pa.array(np.arange(vectors)), "v": pa.FixedSizeListArray.from_arrays(pa.array(m.ravel()), d)})
            con.execute(f"CREATE TABLE big AS SELECT id, v::FLOAT[{d}] AS v FROM fixed")
            best = min(timed(lambda: con.execute(f"SELECT id FROM big ORDER BY array_cosine_distance(v, $q::FLOAT[{d}]) LIMIT 10", {"q": probe.tolist()}).fetchall()) for _ in range(5))
            checks[f"(DuckDB {duckdb.__version__}, the same: best of 5, s)"] = round(best, 3)
        except ImportError:
            pass
    return checks


def timed(f):
    t0 = time.time()
    f()
    return time.time() - t0


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--new", default=os.path.join(HERE, "..", "target", "release", "pondra"), help="this build's binary")
    ap.add_argument("--work", default="", help="where the lake and logs go (default: a new temporary folder, removed if every check passes)")
    ap.add_argument("--port", type=int, default=9770)
    ap.add_argument("--vectors", type=int, default=200_000, help="vectors in the speed check (0: none)")
    a = ap.parse_args()
    work = a.work or tempfile.mkdtemp(prefix="pondra-ai-")
    os.makedirs(work, exist_ok=True)
    try:
        checks = ai_check(os.path.abspath(a.new), work, a.port, a.vectors)
    except Failed as e:
        checks = {"ran to the end": False, "error": str(e)}
    finally:
        stop_all()
    ok = all(v is True for k, v in checks.items() if not k.startswith("(") and k != "error")
    print(json.dumps({**checks, "ok": ok}, indent=1))
    if ok and not a.work:
        shutil.rmtree(work, ignore_errors=True)
    elif not ok:
        print(f"(the lake and node log kept in {work})", file=sys.stderr)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
