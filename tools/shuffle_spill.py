#!/usr/bin/env python3
"""A shuffle bigger than memory, and one that loses a node: answers don't change either way.

  shuffle_spill.py [--rows 4000000] [--nodes 3] [--spill-mb 1] [--memory-gb 1] [--s3]
  shuffle_spill.py --cap-gb 0.02 [...]   (the same shuffle with PONDRA_SPILL_GB too small for it: see `capped`)

Three nodes, a table of `--rows` rows with as many distinct keys, and a GROUP BY that has to
shuffle all of them. With `PONDRA_SPILL_MB` small, every bucket passes the threshold, so the rows
between the steps are written to each node's scratch disk and read back a piece at a time. What
this proves:

- the shuffled answer equals the one-node answer, row for row;
- the nodes did spill (`pondra_shuffle_spilled_bytes_total` > 0) and cleaned up after (the scratch
  folder is empty again);
- no node's memory ran away while it happened (RSS stays under `--memory-gb` plus a margin), and
  the results of each stage cross the wire a piece at a time rather than whole;
- the buckets came out even (`pondra_shuffle_skew`, the biggest against the average);
- with a node killed just before the query, the shuffle runs again without it and still answers;
- a step that fails once on a node (PONDRA_DEBUG_FAIL_STEP) is run again and answers as one node does.
"""
import argparse, http.client, itertools, json, os, subprocess, sys, threading, time

RUNS = itertools.count()

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import harness
from harness import Node, call


def metrics(port):
    out = {}
    for line in call(port, "GET", "/metrics").decode().splitlines():
        if line and not line.startswith("#"):
            name, v = line.rsplit(" ", 1)
            out[name] = float(v)
    return out


def rss_mb(node):
    try:
        return int(open(f"/proc/{node.p.pid}/statm").read().split()[1]) * 4096 / 1e6
    except Exception:
        return 0.0


def fails_once(port, rows):
    """A step that fails once on a node is run again and answers as one node does. PONDRA_DEBUG_FAIL_STEP makes
    node 1 (a follower) fail step 0, the shuffle's first step, and node 2 fail step 1, which is this query's last
    step (one exchange). Each run has a lake and three nodes of its own."""
    scratch = os.path.join(harness.tempfile.gettempdir(), f"pondra-fail-{os.getpid()}")
    q = "SELECT k, sum(v) AS s, count(*) AS n FROM ev GROUP BY k ORDER BY k LIMIT 20"
    check = "SELECT count(*) AS keys, sum(s) AS total FROM (SELECT k, sum(v) AS s FROM ev GROUP BY k)"
    once = lambda sql: (sql + f" -- {next(RUNS)}").encode()
    checks, answers, said = {}, {}, []
    runs = [(1, 0, "a step that fails once on a follower answers on its second try with the same rows as one node"),
            (2, 1, "a last step that fails once answers with the same rows")]
    for r, (failing, step, key) in enumerate(runs):
        base = port + 4 * r
        lake = harness.new_lake()
        envs = [{"PONDRA_SPILL_MB": "1", "PONDRA_DEBUG_SPREAD": "1", "PONDRA_CACHE_DIR": f"{scratch}/{r}-{i}", **({"PONDRA_DEBUG_FAIL_STEP": str(step)} if i == failing else {})} for i in range(3)]
        nodes = [Node(lake, base + i, env=envs[i], memory_gb=A.memory_gb, tier_secs=0).start() for i in range(3)]
        deadline = time.time() + 120  # (the nodes listed before the first statement, as spread_tpch.py waits for them)
        while len(call(base, "GET", "/stats").get("nodes", [])) < 3 and time.time() < deadline:
            time.sleep(0.5)
        call(base, "POST", "/sql", b"CREATE TABLE ev (id BIGINT, k VARCHAR, v DOUBLE)")
        for i in range(0, rows, 250_000):
            n = min(250_000, rows - i)
            call(base, "POST", "/sql", f"INSERT INTO ev SELECT value + {i}, 'key-' || (value + {i}), (value % 97) * 1.5 FROM generate_series(1, {n})".encode(), timeout=3600)
        call(base, "POST", "/tier", timeout=3600)

        def answer(spread, sql):
            try:
                return call(base, "POST", f"/sql?spread={spread}", once(sql), timeout=3600)
            except RuntimeError as e:
                return f"error: {e}"  # (what a run that fails answers: shown, not raised)

        shuffled = metrics(base).get("pondra_shuffled_queries_total", 0)
        one, spread_one = answer(0, check), answer(1, check)
        rows_one, rows_spread = answer(0, q), answer(1, q)
        shuffled = metrics(base).get("pondra_shuffled_queries_total", 0) - shuffled
        said.append("second try answered" in open(nodes[0].log).read())  # (the coordinator's stderr: `run`)
        checks[key] = one == spread_one and rows_one == rows_spread and shuffled >= 1
        answers[key] = {"one_node": one, "spread": spread_one, "rows_one_node": rows_one, "rows_spread": rows_spread, "shuffled": shuffled}
        for n in nodes:
            n.kill()
        if not harness.A.keep and not lake.startswith("s3://"):
            subprocess.run(["rm", "-rf", lake])
    subprocess.run(["rm", "-rf", scratch])
    checks["the coordinator says the step ran again"] = all(said)
    return checks, answers

def load(port, rows):
    """Table `ev`: `rows` rows, as many distinct keys, tiered. One INSERT per million rows: as many keys as rows, so
    the GROUP BY has nothing to reduce before the shuffle and every bucket is big."""
    call(port, "POST", "/sql", b"CREATE TABLE ev (id BIGINT, k VARCHAR, v DOUBLE)")
    for i in range(0, rows, 1_000_000):
        n = min(1_000_000, rows - i)
        call(port, "POST", "/sql", f"INSERT INTO ev SELECT value + {i}, 'key-' || (value + {i}), (value % 97) * 1.5 FROM generate_series(1, {n})".encode(), timeout=3600)
    call(port, "POST", "/tier", timeout=3600)


def answer(port, path, text):
    """(status, SQLSTATE, body) as the node sends it: a refusal is an answer too."""
    c = http.client.HTTPConnection("127.0.0.1", port, timeout=3600)
    c.request("POST", path, text.encode(), {})
    r = c.getresponse()
    return r.status, r.getheader("x-pondra-sqlstate"), r.read().decode(errors="replace")


def capped(lake, scratch):
    """The same shuffle with `PONDRA_SPILL_GB` too small for what it moves. A piece is refused as it is written
    (53100, naming the setting), and the query says so; or the shuffle finds the room and answers with the same
    rows as an uncapped run (`expected`, from the data). Either way nothing is left in the scratch folders once the
    query has answered. What the nodes counted (shuffles, bytes written to disk) is reported beside it."""
    env = lambda i: {"PONDRA_SPILL_MB": str(A.spill_mb), "PONDRA_CACHE_DIR": f"{scratch}/{i}", "PONDRA_SPILL_GB": str(A.cap_gb), "PONDRA_DEBUG_SPREAD": "1"}
    nodes = [Node(lake, A.port + i, env=env(i), memory_gb=A.memory_gb, tier_secs=0).start() for i in range(A.nodes)]
    port = A.port
    load(port, A.rows)
    # Grouped by an expression of id, not by k: a GROUP BY on a column whose files hold narrow ranges (k, in load's
    # order) runs where its rows are, with no exchange and nothing to spill (`ranges.rs`), and whether it did depended
    # on the run's files. Grouped by this expression, every run shuffles: id * 7 mod 1,000,003 has 1,000,003 values.
    check = "SELECT count(*) AS keys, sum(s) AS total FROM (SELECT (id * 7) % 1000003 AS g, sum(v) AS s FROM ev GROUP BY (id * 7) % 1000003)"
    # `load` makes each million rows from value 1..1,000,000 again, so for a whole number of millions the total is that many times one million's.
    # The keys: id * 7 mod 1,000,003 over ids 1..rows. 1,000,003 is prime, so the map is one to one on residues, and the
    # ids 1..1,000,003 already give every residue once.
    expected = [{"keys": len({(i * 7) % 1000003 for i in range(1, A.rows + 1)}), "total": float(sum(((v % 97) * 1.5 for v in range(1, 1_000_001)), 0.0)) * (A.rows // 1_000_000)}]
    counted = lambda: [metrics(n.port) for n in nodes]
    before = counted()
    # The shuffle's pieces on disk, sampled while the query runs: under the setting they never pass it (`spill_fits`).
    peak, stop = [0] * A.nodes, threading.Event()

    def watch():
        while not stop.is_set():
            for i, n in enumerate(nodes):
                peak[i] = max(peak[i], metrics(n.port).get("pondra_shuffle_disk_bytes", 0))
            time.sleep(0.05)

    sampler = threading.Thread(target=watch, daemon=True)
    sampler.start()
    status, sqlstate, body = answer(port, "/sql?spread=1", check + " -- capped")
    stop.set()
    sampler.join()
    time.sleep(1)
    after = counted()
    shuffled = sum(a.get("pondra_shuffled_queries_total", 0) - b.get("pondra_shuffled_queries_total", 0) for a, b in zip(after, before))
    spilled = [int(a.get("pondra_shuffle_spilled_bytes_total", 0) - b.get("pondra_shuffle_spilled_bytes_total", 0)) for a, b in zip(after, before)]
    deadline = time.time() + 60
    while time.time() < deadline and sum(harness.spill_files(f"{scratch}/{i}")[0] for i in range(A.nodes)) > 0:
        time.sleep(1)
    left = sum(harness.spill_files(f"{scratch}/{i}")[0] for i in range(A.nodes))
    refused = sqlstate == "53100" and "PONDRA_SPILL_GB" in body
    answered = status == 200 and json.loads(body) == expected
    # A step whose piece was refused is not run again: the coordinator says so (`run`), and the query runs here.
    piece_refused = "is out of room" in open(nodes[0].log).read()
    checks = {
        "a shuffle past PONDRA_SPILL_GB is refused by name (53100), or answers with the same rows": refused or answered,
        "the query shuffled, or was refused by a piece": shuffled >= 1 or refused or piece_refused,
        "the scratch is empty after": left == 0,
        "the shuffle's pieces on disk stayed within the setting while it ran": max(peak) <= int(A.cap_gb * (1 << 30)),
    }
    # What the coordinator said about its plan (PONDRA_DEBUG_SPREAD) last: a shuffle's exchanges, a step out of room, or why it didn't spread.
    plan = [l.strip() for l in open(nodes[0].log, errors="replace") if l.startswith(("spread:", "shuffle:"))][-4:]
    out = {"rows": A.rows, "nodes": A.nodes, "cap_gb": A.cap_gb, "spread": [status, sqlstate, body[:300]], "shuffled": shuffled, "spilled_bytes": spilled, "piece_refused": piece_refused, "peak_disk_bytes": [int(p) for p in peak],
           "left_files": left, "plan": plan, "checks": checks, "ok": all(checks.values())}
    print(json.dumps(out, indent=1))
    for n in nodes:
        n.kill()
    subprocess.run(["rm", "-rf", scratch])
    if not harness.A.keep and not lake.startswith("s3://"):
        subprocess.run(["rm", "-rf", lake])
    sys.exit(0 if out["ok"] else 1)


def main():
    lake = harness.new_lake()
    # A scratch folder each, as separate machines have (a dead node's is swept by the next process that spills there).
    scratch = os.path.join(harness.tempfile.gettempdir(), f"pondra-spill-{os.getpid()}")
    if A.cap_gb is not None:
        return capped(lake, scratch)
    env = lambda i: {"PONDRA_SPILL_MB": str(A.spill_mb), "PONDRA_CACHE_DIR": f"{scratch}/{i}"}
    nodes = [Node(lake, A.port + i, env=env(i), memory_gb=A.memory_gb, tier_secs=0).start() for i in range(A.nodes)]
    port = A.port
    t0 = time.time()
    load(port, A.rows)
    loaded = round(time.time() - t0, 1)

    q = "SELECT k, sum(v) AS s, count(*) AS n FROM ev GROUP BY k ORDER BY k LIMIT 20"
    check = "SELECT count(*) AS keys, sum(s) AS total FROM (SELECT k, sum(v) AS s FROM ev GROUP BY k)"
    # Every run gets its own text: the same query twice is answered from the cache, and a run that
    # never reached the nodes would prove nothing about spilling or about a node dying.
    once = lambda sql: (sql + f" -- {next(RUNS)}").encode()
    before = [metrics(n.port).get("pondra_shuffle_spilled_bytes_total", 0) for n in nodes]
    peak = [rss_mb(n) for n in nodes]

    def watch():
        for i, n in enumerate(nodes):
            peak[i] = max(peak[i], rss_mb(n))

    t0 = time.time()
    one = call(port, "POST", "/sql?spread=0", once(check), timeout=3600)
    watch()
    one_s = round(time.time() - t0, 1)
    t0 = time.time()
    spread = call(port, "POST", "/sql?spread=1", once(check), timeout=3600)
    watch()
    spread_s = round(time.time() - t0, 1)
    rows_one = call(port, "POST", "/sql?spread=0", once(q), timeout=3600)
    rows_spread = call(port, "POST", "/sql?spread=1", once(q), timeout=3600)
    watch()

    # A node dies with the query already on its way: its step fails twice, and the shuffle runs
    # again over the nodes that are left (the coordinator still lists it: heartbeats take a second).
    nodes[-1].kill()
    t0 = time.time()
    after_death = call(port, "POST", "/sql?spread=1", once(check), timeout=3600)
    death_s = round(time.time() - t0, 1)
    nodes = nodes[:-1]

    m = [metrics(n.port) for n in nodes]
    spilled = [int(x.get("pondra_shuffle_spilled_bytes_total", 0) - b) for x, b in zip(m, before)]
    # Each node frees what it spilled once it forgets the shuffle (a sweep every 30 s).
    deadline = time.time() + 120
    while time.time() < deadline:
        left = [int(metrics(n.port).get("pondra_shuffle_disk_bytes", 0)) for n in nodes]
        if sum(left) == 0:
            break
        time.sleep(5)
    skew = max(x.get("pondra_shuffle_skew", 0) for x in m)
    checks = {
        "the shuffled answer equals the one-node answer": one == spread and rows_one == rows_spread,
        "the query shuffled": sum(x.get("pondra_shuffled_queries_total", 0) for x in m) >= 1,
        "buckets went to disk": sum(spilled) > 0,
        "the scratch is empty again": sum(left) == 0,
        "memory stayed under the budget": max(peak) < A.memory_gb * 1024 + 1024,
        "the buckets came out even": 1 <= skew < 1.5,  # (pondra_shuffle_skew: the biggest bucket vs the average)
        "a node dying mid-query doesn't change the answer": after_death == one,
    }
    for n in nodes:
        n.kill()  # (the step-failure section below starts its own nodes)
    failed, failed_answers = fails_once(A.port + 10, min(A.rows, 1_000_000))
    checks.update(failed)
    out = {"rows": A.rows, "nodes": A.nodes, "spill_mb": A.spill_mb, "load_s": loaded, "one_node_s": one_s, "spread_s": spread_s, "after_a_node_died_s": death_s,
           "spilled_mb": [round(b / 1e6, 1) for b in spilled], "left_bytes": left, "peak_rss_mb": [round(p) for p in peak], "skew": round(skew, 2),
           "answer": one, "fails_once": failed_answers, "checks": checks, "ok": all(checks.values())}
    print(json.dumps(out, indent=1))
    for n in nodes:
        n.kill()
    subprocess.run(["rm", "-rf", scratch])
    if not harness.A.keep and not lake.startswith("s3://"):
        subprocess.run(["rm", "-rf", lake])
    sys.exit(0 if out["ok"] else 1)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=4_000_000)
    ap.add_argument("--nodes", type=int, default=3)
    ap.add_argument("--spill-mb", type=int, default=1, help="PONDRA_SPILL_MB: the piece size (small, so it spills here)")
    ap.add_argument("--memory-gb", type=float, default=1)
    ap.add_argument("--port", type=int, default=8120)
    ap.add_argument("--s3", action="store_true")
    ap.add_argument("--keep", action="store_true")
    ap.add_argument("--cap-gb", type=float, default=None, help="PONDRA_SPILL_GB for the nodes: the shuffle must then be refused or fit (see capped)")
    A = ap.parse_args()
    harness.A = A
    main()
