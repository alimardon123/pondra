#!/usr/bin/env python3
"""Multi-user and multi-node tests for Pondra (stdlib only; reuses harness.py's helpers).

  cluster.py users    --users 64 --readers 16 --nodes 3   many independent writers + readers at once
  cluster.py race     --nodes 5                           all nodes start together: exactly one leads
  cluster.py failover --nodes 3                           sharded stateful task; the leader is killed
                                                          twice mid-run; exactly-once + state vs a model
  cluster.py latency  --secs 20 [--load 4]                event -> view row pushed to a watcher on another
                                                          node; optionally with background producers
  cluster.py split    --secs 20                           Arrow producers write through the followers only:
                                                          throughput and each node's CPU per million events
  cluster.py spread   --rows 4000000                      distributed (SPMD) queries vs the same queries on
                                                          one node: results must match
  cluster.py isolate                                      a follower loses its link to a healthy leader:
                                                          it must not take over; a real failure must still
                                                          be taken over
"""
import argparse, http.client, json, os, random, subprocess, sys, tempfile, threading, time
from collections import defaultdict
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import harness
from harness import BIN, Node, call, pct, sql

A = None


def cluster(n, lake=None, **flags):
    """Start n nodes on one lake; return them once every follower is listed by the leader."""
    lake = lake or harness.new_lake()
    flags = {**dict(f.split("=", 1) for f in A.flag), **flags}  # --flag tier-secs=10 reaches every node
    nodes = [Node(lake, A.port + i, **flags).start() for i in range(n)]
    wait(lambda: len(leader(nodes)[1]["nodes"]) == n, 30, "followers never joined")
    return lake, nodes


def leader(nodes):
    for nd in nodes:
        try:
            s = call(nd.port, "GET", "/stats", timeout=2)
            if s["role"] == "leader":
                return nd, s
        except Exception:
            pass
    return None, {"nodes": []}


def wait(cond, secs, what):
    t = time.time()
    while time.time() - t < secs:
        try:
            if cond():
                return time.time() - t
        except Exception:
            pass
        time.sleep(0.1)
    raise RuntimeError(what)


def send(nodes, table, producer, seq, body, lat=None):
    """One batch, retried against random live nodes until acked: exactly-once from any node."""
    while True:
        nd = random.choice(nodes)
        t = time.time()
        try:
            ack = call(nd.port, "POST", f"/append/{table}?producer={producer}&seq={seq}", body, timeout=20)
            if lat is not None:
                lat.append(time.time() - t)
            return ack
        except Exception:
            time.sleep(0.2)


def users():
    """64 independent producers and 16 readers at once, spread over the nodes, plus serverless
    one-shot queries. Every read must be a consistent snapshot: each producer's batches appear
    as a gap-free prefix (seq 1..k, all rows of each), never a partial batch or a hole."""
    lake, nodes = cluster(A.nodes)
    call(nodes[0].port, "POST", "/tables/events", json.dumps([["producer", "Utf8"], ["seq", "Int64"], ["i", "Int64"]]).encode())
    for nd in nodes:  # followers see the new table within a catalog poll
        wait(lambda: sql(nd.port, "SELECT count(*) AS n FROM events") is not None, 10, "table not visible")
    stop, acked, lat, qlat, bad, reads, top = threading.Event(), defaultdict(int), [], [], [], [0], [0]

    def writer(u):
        seq = 0
        while not stop.is_set():
            seq += 1
            body = "".join(f'{{"producer":"u{u}","seq":{seq},"i":{i}}}\n' for i in range(A.size)).encode()
            top[0] = max(top[0], send(nodes, "events", f"u{u}", seq, body, lat)["seg"])
            acked[u] = seq

    def check(res, where):
        for r in res:
            if r["n"] != r["mx"] * A.size or r["d"] != r["mx"]:
                bad.append((where, r))

    q = "SELECT producer, count(*) AS n, count(DISTINCT seq) AS d, max(seq) AS mx FROM events GROUP BY producer"

    def reader(k):
        while not stop.is_set():
            nd, t = random.choice(nodes), time.time()
            try:
                res = sql(nd.port, q)
            except Exception as e:
                bad.append(("error", str(e)[:200]))
                continue
            qlat.append(time.time() - t)
            check(res, f"node {nd.port}")
            reads[0] += 1

    def serverless():  # a separate process with no server at all
        while not stop.is_set():
            out = subprocess.run([BIN, "sql", "--dir", lake, q.replace("SELECT", "SELECT 'x' AS k,")], capture_output=True, text=True)
            rows = [l.split("|")[2:6] for l in out.stdout.splitlines() if l.startswith("| x")]
            check([dict(n=int(n), d=int(d), mx=int(m)) for _, n, d, m in rows], "pondra sql")
            reads[0] += 1

    threads = [threading.Thread(target=writer, args=(u,)) for u in range(A.users)]
    threads += [threading.Thread(target=reader, args=(k,)) for k in range(A.readers)]
    threads += [threading.Thread(target=serverless) for _ in range(2)]
    t0 = time.time()
    [t.start() for t in threads]
    time.sleep(A.secs)
    stop.set()
    [t.join() for t in threads]
    secs = time.time() - t0
    # Final check: every acked batch exactly once.
    final = {r["producer"]: r for r in call(nodes[-1].port, "POST", f"/sql?after={top[0]}", q.encode())}  # read-your-writes
    lost_or_dup = [u for u in range(A.users) if final[f"u{u}"]["n"] != acked[u] * A.size or final[f"u{u}"]["d"] != acked[u]]
    events = sum(acked.values()) * A.size
    print(f"users: {A.users} writers + {A.readers} readers + 2 serverless on {A.nodes} node(s), {secs:.0f}s  lake {lake}")
    print(f"  {events} events ({events / secs:,.0f}/s), ack p50 {pct(lat, .5)} ms p99 {pct(lat, .99)} ms")
    print(f"  {reads[0]} snapshot reads, query p50 {pct(qlat, .5)} ms p99 {pct(qlat, .99)} ms")
    print(f"  inconsistent reads: {len(bad)}; producers with lost/duplicate batches: {len(lost_or_dup)}")
    from collections import Counter
    print("    by node:", dict(Counter(b[0] for b in bad)))
    for b in (bad[:3] + [b for b in bad if b[0] == "error"][:3]):
        print("   ", b)
    [nd.kill() for nd in nodes]
    return not bad and not lost_or_dup


def race():
    """Every node starts at the same moment on an empty lake: exactly one must lead."""
    lake = harness.new_lake()
    nodes = [Node(lake, A.port + i) for i in range(A.nodes)]
    threads = [threading.Thread(target=nd.start) for nd in nodes]
    [t.start() for t in threads]
    [t.join() for t in threads]
    time.sleep(2)
    stats = [call(nd.port, "GET", "/stats") for nd in nodes]
    leaders = [s for s in stats if s["role"] == "leader"]
    agree = len({s.get("leader") or f"127.0.0.1:{nd.port}" for nd, s in zip(nodes, stats)})
    print(f"race: {A.nodes} nodes started at once -> {len(leaders)} leader(s), all agree: {agree == 1}")
    [nd.kill() for nd in nodes]
    return len(leaders) == 1 and agree == 1


def failover():
    """Distributed state: running totals per user, sharded over the nodes. Producers write
    through random nodes while the leader is killed (kill -9) twice; the dead node comes back
    as a follower. Exactly-once events, and the state must equal an in-memory model."""
    lake, nodes = cluster(A.nodes, task_ms=500)
    p = nodes[0].port
    call(p, "POST", "/tables/events", json.dumps([["producer", "Utf8"], ["seq", "Int64"], ["user", "Utf8"], ["amount", "Int64"]]).encode())
    call(p, "POST", "/tables/totals", json.dumps({"columns": [["user", "Utf8"], ["total", "Int64"], ["n", "Int64"]], "key": ["user"]}).encode())
    task = {"source": "events", "target": "totals", "shards": A.shards, "shard_by": "user", "key": ["user"],
            "sql": "SELECT e.user, coalesce(max(t.total), 0) + sum(e.amount) AS total, coalesce(max(t.n), 0) + count(*) AS n "
                   "FROM events e LEFT JOIN totals t ON e.user = t.user GROUP BY e.user"}
    call(p, "POST", "/tasks/running", json.dumps(task).encode())
    call(p, "POST", "/views/totals_v", b"SELECT user, sum(amount) AS total, count(*) AS n FROM events GROUP BY user")
    stop, model, lock, sent = threading.Event(), defaultdict(lambda: [0, 0]), threading.Lock(), [0]

    def producer(k):
        seq = 0
        while not stop.is_set():
            seq += 1
            rows = [(f"user{random.randrange(A.keys)}", random.randrange(1, 100)) for _ in range(A.size)]
            body = "".join(f'{{"producer":"p{k}","seq":{seq},"user":"{u}","amount":{a}}}\n' for u, a in rows).encode()
            send(nodes, "events", f"p{k}", seq, body)
            with lock:
                for u, a in rows:
                    model[u][0] += a
                    model[u][1] += 1
                sent[0] += A.size

    threads = [threading.Thread(target=producer, args=(k,)) for k in range(A.producers)]
    [t.start() for t in threads]
    failovers = []
    time.sleep(A.secs / 6)
    victim = next(nd for nd in nodes if nd is not leader(nodes)[0])
    victim.kill()  # a follower dies mid-flush (its producers retry on other nodes), then comes back
    time.sleep(1)
    victim.start()
    for _ in range(2):
        time.sleep(A.secs / 3)
        old, _ = leader(nodes)
        before = sent[0]
        old.kill()
        t = time.time()
        failovers.append(wait(lambda: leader(nodes)[0] not in (None, old) and sent[0] > before, 60, "no new leader"))
        print(f"  killed leader :{old.port}; new leader :{leader(nodes)[0].port} accepting writes after {failovers[-1]:.1f}s")
        old.start()  # comes back as a follower
    time.sleep(A.secs / 3)
    stop.set()
    [t.join() for t in threads]
    q = lambda s: sql(leader(nodes)[0].port, s)
    try:
        caught = wait(lambda: q("SELECT sum(n) AS n FROM totals")[0]["n"] == sent[0], 60, "task never caught up")
    except RuntimeError:
        caught = float("nan")
        print(f"  task never caught up: sum(n) = {q('SELECT sum(n) AS n FROM totals')[0]['n']}, sent {sent[0]}")
    state = {r["user"]: [r["total"], r["n"]] for r in q("SELECT user, total, n FROM totals")}
    view = {r["user"]: [r["total"], r["n"]] for r in q("SELECT user, total, n FROM totals_v")}
    ev = q("SELECT count(*) AS n, count(DISTINCT producer || ':' || CAST(seq AS VARCHAR)) AS b FROM events")[0]
    runs = {nd.port: call(nd.port, "GET", "/stats").get("shard_runs") for nd in nodes}
    ok = state == dict(model) == view and ev["n"] == sent[0]
    for name, got in {"task": state, "view": view}.items():
        bad = {u: (got.get(u), model[u]) for u in set(model) | set(got) if got.get(u) != model.get(u)}
        if bad:
            print(f"  {name}: {len(bad)} users differ from the model, e.g. {list(bad.items())[:3]}")
    print(f"failover: {A.nodes} nodes, {A.shards} shards, {A.producers} producers, {sent[0]} events, 1 follower + 2 leader kills")
    print(f"  failovers: {', '.join(f'{x:.1f}s' for x in failovers)} (kill -> writes acked again); state caught up {caught:.1f}s after the last write")
    print(f"  events exactly once: {ev['n'] == sent[0]} ({ev['n']} rows); for {len(model)} users, task state == model: "
          f"{state == dict(model)}, inline view == model: {view == dict(model)}")
    print(f"  shard runs per node (since each node's last start): {runs}")
    [nd.kill() for nd in nodes]
    return ok


def latency():
    """Events go to one follower; a client watching the aggregating view on the other follower
    times each event from send until its view row arrives. Optional background load."""
    lake, nodes = cluster(3)
    call(nodes[0].port, "POST", "/tables/events", json.dumps([["user", "Utf8"], ["amount", "Int64"]]).encode())
    call(nodes[0].port, "POST", "/views/totals", b"SELECT user, sum(amount) AS total, count(*) AS n FROM events GROUP BY user")
    time.sleep(1)
    sent, got, ack, stop = {}, {}, [], threading.Event()

    def watcher():  # one long-lived HTTP response, NDJSON pushed as rows commit
        c = http.client.HTTPConnection("127.0.0.1", nodes[2].port, timeout=A.secs + 30)
        c.request("GET", "/watch/totals")
        r = c.getresponse()
        while not stop.is_set():
            line = r.readline()
            if not line:
                break
            u = json.loads(line)["user"]
            got.setdefault(u, time.time())

    def load(k):  # background producers: 5,000-row batches as fast as acks come back
        seq = 0
        while not stop.is_set():
            seq += 1
            body = "".join(f'{{"user":"bg{random.randrange(1000)}","amount":1}}\n' for _ in range(5000)).encode()
            send(nodes[:2], "events", f"bg{k}", seq, body)

    threads = [threading.Thread(target=watcher, daemon=True)] + [threading.Thread(target=load, args=(k,), daemon=True) for k in range(A.load)]
    [t.start() for t in threads]
    time.sleep(0.5)
    t_end, i = time.time() + A.secs, 0
    while time.time() < t_end:
        i += 1
        t = sent[f"probe{i}"] = time.time()
        call(nodes[1].port, "POST", f"/append/events?producer=probe&seq={i}", f'{{"user":"probe{i}","amount":1}}\n'.encode())
        ack.append(time.time() - t)
        time.sleep(0.05 + random.random() * 0.05)
    time.sleep(2)
    stop.set()
    lat = [got[u] - t for u, t in sent.items() if u in got]
    print(f"latency: 3 nodes, {A.load} background producers; {len(sent)} probe events, {len(lat)} pushed back")
    mode = "replicated" if "ack=replicated" in A.flag else "durable"
    print(f"  ack ({mode}){' ' * (26 - len(mode))}p50 {pct(ack, .5)} ms  p99 {pct(ack, .99)} ms")
    print(f"  view row pushed to another node     p50 {pct(lat, .5)} ms  p99 {pct(lat, .99)} ms")
    [nd.kill() for nd in nodes]
    return len(lat) == len(sent)


def split():
    """Where does the work happen? Producers send Arrow batches to the followers only; we count
    each node's CPU time per million events (the leader should mostly just order commits)."""
    import io, pyarrow as pa, pyarrow.ipc
    lake, nodes = cluster(3)
    call(nodes[0].port, "POST", "/tables/events", json.dumps([["id", "Int64"], ["user", "Utf8"], ["amount", "Int64"]]).encode())
    time.sleep(1)
    n = 20_000
    t = pa.table({"id": pa.array(range(n), pa.int64()), "user": pa.array([f"user{i % 5000}" for i in range(n)]), "amount": pa.array([i % 100 for i in range(n)], pa.int64())})
    buf = io.BytesIO()
    with pa.ipc.new_stream(buf, t.schema) as w:
        w.write_table(t)
    body, arrow = buf.getvalue(), {"content-type": "application/vnd.apache.arrow.stream"}
    cpu = lambda nd: sum(map(int, open(f"/proc/{nd.p.pid}/stat").read().rsplit(")", 1)[1].split()[11:13])) / os.sysconf("SC_CLK_TCK")
    before, stop, sent = [cpu(nd) for nd in nodes], threading.Event(), [0]

    def producer(k):
        seq, nd = 0, nodes[1 + k % 2]
        while not stop.is_set():
            seq += 1
            call(nd.port, "POST", f"/append/events?producer=p{k}&seq={seq}", body, headers=arrow)
            sent[0] += n

    threads = [threading.Thread(target=producer, args=(k,)) for k in range(A.producers)]
    [t.start() for t in threads]
    time.sleep(A.secs)
    stop.set()
    [t.join() for t in threads]
    used = [cpu(nd) - b for nd, b in zip(nodes, before)]
    per_m = [u / (sent[0] / 1e6) for u in used]
    print(f"split: {A.producers} Arrow producers -> followers only, {sent[0]:,} events in {A.secs:.0f}s ({sent[0] / A.secs:,.0f}/s)")
    print(f"  CPU seconds per million events: leader {per_m[0]:.2f}, followers {per_m[1]:.2f} + {per_m[2]:.2f}; "
          f"leader share {100 * used[0] / sum(used):.0f}%")
    [nd.kill() for nd in nodes]
    return True


def spread():
    """Every node runs its slice of the query's main table; the receiving node merges. Same SQL,
    spread vs local: the results must match (floats: to 9 significant digits, since sums added
    in a different order differ in the last digits)."""
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "bench"))
    from queries import QUERIES
    lake, nodes = cluster(3)
    n, p = A.rows, nodes[0].port
    cols = ("(value * 2654435761) % 1000003 AS user_id, CAST((value * 48271) % 10000 AS DOUBLE) / 100 AS amount, "
            "concat('c', CAST(value % 1000 AS VARCHAR)) AS category, 1700000000000 + value * 10 AS ts_ms")
    for k in range(8):  # 8 jobs: 8+ files to deal out
        call(p, "POST", f"/insert/events?job=g{k}", f"SELECT {cols} FROM (SELECT value + {k * n // 8} AS value FROM range(0, {n // 8}))".encode(), timeout=600)
    call(p, "POST", "/insert/dims?job=d", b"SELECT concat('c', CAST(value AS VARCHAR)) AS category, concat('r', CAST(value % 10 AS VARCHAR)) AS region FROM range(0, 1000)")
    time.sleep(1)
    extra = {"q7_avg_distinct": "SELECT category, avg(amount) AS a, count(DISTINCT user_id) AS u FROM events WHERE amount > 90 GROUP BY category ORDER BY category LIMIT 5",
             "q8_top_rows": "SELECT user_id, amount, ts_ms FROM events WHERE amount > 99.9 ORDER BY ts_ms DESC LIMIT 7",
             "q9_filter_count": "SELECT count(*) AS n FROM events WHERE category = 'c7'"}
    num = lambda v: float(f"{v:.9g}") if isinstance(v, float) else v
    ok = True
    for name, q in {**QUERIES, **extra}.items():
        q = q.replace("AS STRING", "AS VARCHAR")
        t = time.time(); a = call(nodes[1].port, "POST", "/sql?spread=0", q.encode(), timeout=600); ta = time.time() - t
        t = time.time(); b = call(nodes[1].port, "POST", "/sql?spread=1", q.encode(), timeout=600); tb = time.time() - t
        if "LIMIT" in q and "ORDER BY s DESC" in q:  # ties at the cut-off: compare the values only
            same = sorted(num(r["s"]) for r in a) == sorted(num(r["s"]) for r in b)
        else:
            same = sorted(json.dumps({k: num(v) for k, v in r.items()}, sort_keys=True) for r in a) == sorted(json.dumps({k: num(v) for k, v in r.items()}, sort_keys=True) for r in b)
        ok &= same
        print(f"  {name:20} one node {ta * 1000:6.0f} ms   3 nodes {tb * 1000:6.0f} ms   same result: {same}")
    fallbacks = open(nodes[1].log).read().count("distributed query failed")
    print(f"spread: {n:,} rows, 3 nodes; results identical: {ok}; fell back to one node: {fallbacks} times")
    [nd.kill() for nd in nodes]
    return ok and fallbacks == 0


def isolate():
    lake, drop = harness.new_lake(), os.path.join(tempfile.mkdtemp(), "drop")
    nodes = [Node(lake, A.port, env={"PONDRA_DROP_BEATS": drop}).start()]
    nodes += [Node(lake, A.port + i).start() for i in (1, 2)]
    wait(lambda: len(leader(nodes)[1]["nodes"]) == 3, 30, "followers never joined")
    cut = nodes[2]
    open(drop, "w").write(f"127.0.0.1:{cut.port}\n")  # the leader stops answering this follower
    time.sleep(12)  # > the 5 s lease: the cut-off follower asks its peers, hears the leader is fine
    ld, st = leader(nodes)
    kept = ld is nodes[0] and st["term"] == 1 and len(st["nodes"]) == 2
    call(cut.port, "POST", "/tables/t", b'[["a","Int64"]]')  # writes through it still reach the leader
    nodes[0].kill()  # now the leader really dies
    took = wait(lambda: leader(nodes)[0] in nodes[1:], 60, "no takeover")
    print(f"isolate: cut-off follower deposed a healthy leader: {not kept}; after the leader died, "
          f":{leader(nodes)[0].port} took over in {took:.1f}s (term {leader(nodes)[1]['term']})")
    [nd.kill() for nd in nodes]
    return kept


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("test", choices=["users", "race", "failover", "isolate", "latency", "split", "spread"])
    ap.add_argument("--rows", type=int, default=4_000_000)
    ap.add_argument("--load", type=int, default=0)
    ap.add_argument("--s3", action="store_true")
    ap.add_argument("--nodes", type=int, default=3)
    ap.add_argument("--users", type=int, default=64)
    ap.add_argument("--readers", type=int, default=16)
    ap.add_argument("--producers", type=int, default=8)
    ap.add_argument("--shards", type=int, default=6)
    ap.add_argument("--keys", type=int, default=1000)
    ap.add_argument("--size", type=int, default=100)
    ap.add_argument("--secs", type=float, default=20)
    ap.add_argument("--port", type=int, default=18080)
    ap.add_argument("--flag", action="append", default=[], help="extra serve flag for every node, e.g. tier-secs=10")
    A = harness.A = ap.parse_args()
    sys.exit(0 if globals()[A.test]() else 1)
