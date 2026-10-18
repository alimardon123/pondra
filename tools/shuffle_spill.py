#!/usr/bin/env python3
"""A shuffle bigger than memory, and one that loses a node: answers don't change either way.

  shuffle_spill.py [--rows 4000000] [--nodes 3] [--spill-mb 1] [--memory-gb 1] [--s3]

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
import argparse, itertools, json, os, subprocess, sys, time

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


def main():
    lake = harness.new_lake()
    # A scratch folder each, as separate machines have (a dead node's is swept by the next process that spills there).
    scratch = os.path.join(harness.tempfile.gettempdir(), f"pondra-spill-{os.getpid()}")
    env = lambda i: {"PONDRA_SPILL_MB": str(A.spill_mb), "PONDRA_CACHE_DIR": f"{scratch}/{i}"}
    nodes = [Node(lake, A.port + i, env=env(i), memory_gb=A.memory_gb, tier_secs=0).start() for i in range(A.nodes)]
    port = A.port
    call(port, "POST", "/sql", b"CREATE TABLE ev (id BIGINT, k VARCHAR, v DOUBLE)")
    # One INSERT per million rows: as many keys as rows, so the GROUP BY has nothing to reduce
    # before the shuffle and every bucket is big.
    t0 = time.time()
    for i in range(0, A.rows, 1_000_000):
        n = min(1_000_000, A.rows - i)
        call(port, "POST", "/sql", f"INSERT INTO ev SELECT value + {i}, 'key-' || (value + {i}), (value % 97) * 1.5 FROM generate_series(1, {n})".encode(), timeout=3600)
    call(port, "POST", "/tier", timeout=3600)
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
    A = ap.parse_args()
    harness.A = A
    main()
