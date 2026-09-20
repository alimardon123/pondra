#!/usr/bin/env python3
"""Test harness for Pondra (stdlib only). Every test runs on a fresh lake: a temp dir, or with
--s3 a fresh s3://$PONDRA_BUCKET/test-<id> prefix (AWS_* env vars point at R2/MinIO/the simulator).

  harness.py crash   --runs 20   kill -9 + injected crashes during commit, tiering and tasks
  harness.py upsert              random upserts/deletes + compactions vs an in-memory model
  harness.py fence               a second writer takes over; the first must stop, nothing lost
  harness.py reader              freshness as seen by a separate read-only node
  harness.py insert              bulk INSERT … SELECT, retried: applied exactly once
  harness.py load    --secs 30   throughput, ack latency, freshness, catalog commit latency
  harness.py all                 quick run of everything
"""
import argparse, http.client, json, os, random, signal, subprocess, sys, tempfile, threading, time, uuid

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


def new_lake():
    return f"s3://{os.environ['PONDRA_BUCKET']}/test-{uuid.uuid4().hex[:8]}" if A.s3 else tempfile.mkdtemp(prefix="pondra-")


class Node:
    def __init__(self, lake, port, reader=False, env=None, **flags):
        self.args = [BIN, "serve", "--dir", lake, "--addr", f"127.0.0.1:{port}"] + (["--reader"] if reader else [])
        self.args += [f"--{k.replace('_', '-')}={v}" for k, v in flags.items()]
        self.port, self.env, self.p = port, {**os.environ, **(env or {})}, None
        self.log = os.path.join(tempfile.gettempdir(), f"pondra-{port}-{uuid.uuid4().hex[:6]}.stderr")

    def start(self):
        try:
            call(self.port, "GET", "/stats", timeout=1)
            raise RuntimeError(f"port {self.port} is already in use by another node")
        except (ConnectionError, OSError):
            pass  # free, as it should be
        with open(self.log, "a") as err:
            self.p = subprocess.Popen(self.args, env=self.env, stdout=subprocess.DEVNULL, stderr=err)
        deadline = time.time() + 120  # opening a lake on slow object storage can take a while
        while time.time() < deadline:
            try:
                call(self.port, "GET", "/stats", timeout=2)
                return self
            except Exception:
                if self.p.poll() is not None:
                    return self.start()  # died while starting (e.g. injected crash): try again
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


def upsert():
    lake, model, tiers = new_lake(), {}, []
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
        if seq % 10 == 0:
            t0 = time.time(); call(A.port, "POST", "/tier", timeout=600)  # compaction in between
            tiers.append(time.time() - t0)
        if seq == 30:
            node.kill(); node.start()  # restart in the middle
    got = {r["id"]: r["v"] for r in sql(A.port, "SELECT id, v FROM kv ORDER BY id")}
    live = sql(A.port, "SELECT count(*) AS n FROM kv")[0]["n"]
    node.kill()
    ok = got == model
    print(f"upsert: {len(model)} live keys after 12,000 random upserts/deletes, 6 compactions (s: {', '.join(f'{t:.1f}' for t in tiers)}), 1 restart -> {'OK' if ok else 'FAIL'}")
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
    out = {t.__name__: t() for t in (upsert, tiering, fence, insert, reader, crash)}
    A.secs = min(A.secs, 20)
    out["load"] = load()
    print(json.dumps(out, indent=1))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", choices=["crash", "upsert", "tiering", "fence", "reader", "insert", "load", "all"])
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
    {"crash": crash, "upsert": upsert, "tiering": tiering, "fence": fence, "reader": reader, "insert": insert, "load": load, "all": all_tests}[A.mode]()
