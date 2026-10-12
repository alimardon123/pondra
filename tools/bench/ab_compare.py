#!/usr/bin/env python3
"""Two builds side by side on TPC-H: is B slower than A? (It imports tpch.py beside it.)

  ab_compare.py --a OLD_BIN --b NEW_BIN --data ~/tpch/sf1 [--queries tools/bench/tpch-queries] [--rounds 8]

Run one build after the other on a shared machine and the answer swings 5-10% either way with whatever
else the machine does. So each build gets its own node and lake, loaded alike, and every query runs on
one and then the other, the order flipped each round: what the machine does lands on both. The node
loaded second runs a few percent slower whichever build it is (2-7% in round 35's runs), so the whole
run is done twice with the builds' places swapped, and the two ratios are averaged (geometric mean).

Per query: the median wall time and the median of the node's own CPU time (utime + stime, from /proc),
and whether both builds gave the same answer. Prints B/A for wall and CPU; writes everything to --out.
Exit 1 if any answer differs. Linux only (/proc).
"""
import argparse, json, math, os, shutil, statistics, sys, tempfile, time
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, ".."))
sys.path.insert(0, HERE)
import harness
from harness import Node, call
from tpch import TABLES, queries


def cpu(pid):
    """The process's own CPU seconds so far (user + system)."""
    fields = open(f"/proc/{pid}/stat").read().rsplit(")", 1)[1].split()
    return (int(fields[11]) + int(fields[12])) / os.sysconf("SC_CLK_TCK")


def load(port, data):
    """The 8 tables from Parquet, appended as Arrow and tiered, as tpch.py loads them."""
    import io, pyarrow as pa, pyarrow.ipc, pyarrow.parquet as pq
    for t in TABLES:
        table = pq.read_table(os.path.join(data, f"{t}.parquet"))
        cols = [[f.name, str(f.type).replace("decimal128", "Decimal128").replace("date32[day]", "Date32").replace("int64", "Int64")
                 .replace("int32", "Int32").replace("string", "Utf8")] for f in table.schema]
        call(port, "POST", f"/tables/{t}", json.dumps(cols).encode())
        for seq, batch in enumerate(table.to_batches(max_chunksize=500_000), 1):
            buf = io.BytesIO()
            with pa.ipc.new_stream(buf, batch.schema) as w:
                w.write_batch(batch)
            call(port, "POST", f"/append/{t}?producer=load-{t}&seq={seq}", buf.getvalue(),
                 headers={"content-type": "application/vnd.apache.arrow.stream"}, timeout=3600)
    call(port, "POST", "/tier", timeout=3600)


def rounded(rows):
    """An answer to compare: floats to 6 significant digits (sums may differ in the last bits)."""
    def r(v):
        return float(f"{v:.6g}") if isinstance(v, float) else v
    return [{k: r(v) for k, v in row.items()} if isinstance(row, dict) else row for row in rows]


def one_run(first, second, A, qs, ports):
    """Both builds loaded; every query alternating between them. Returns per-query medians and answers."""
    builds = {"a": first, "b": second}
    nodes, lakes = {}, []
    try:
        for (name, binary), port in zip(builds.items(), ports):
            lake = tempfile.mkdtemp(prefix="pondra-ab-")
            lakes.append(lake)
            harness.BIN = binary  # (read when a Node is made)
            node = Node(lake, port).start()
            load(port, A.data)
            nodes[name] = (node, port)
        answers = {"a": {}, "b": {}}
        for i, q in qs.items():  # one warm run each, not counted; its answer is the one compared
            for name in "ab":
                answers[name][i] = rounded(call(nodes[name][1], "POST", "/sql", f"{q} -- warm".encode(), timeout=3600))
        wall = {n: {i: [] for i in qs} for n in "ab"}
        used = {n: {i: [] for i in qs} for n in "ab"}
        for r in range(A.rounds):
            for i, q in qs.items():
                for name in ("ab" if r % 2 == 0 else "ba"):
                    node, port = nodes[name]
                    c, t = cpu(node.p.pid), time.time()
                    call(port, "POST", "/sql", f"{q} -- round {r}".encode(), timeout=3600)  # (a new text each time: no remembered answer)
                    wall[name][i].append(time.time() - t)
                    used[name][i].append(cpu(node.p.pid) - c)
        med = lambda d: {n: {i: statistics.median(v) for i, v in d[n].items()} for n in "ab"}
        return med(wall), med(used), answers
    finally:
        for node, _ in nodes.values():
            node.kill()
        for lake in lakes:
            shutil.rmtree(lake, ignore_errors=True)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--a", required=True, help="the build to compare against (main, a release)")
    ap.add_argument("--b", required=True, help="the build being checked")
    ap.add_argument("--data", required=True, help="TPC-H Parquet: one <table>.parquet each")
    ap.add_argument("--queries", default=os.path.join(HERE, "tpch-queries"))
    ap.add_argument("--rounds", type=int, default=8)
    ap.add_argument("--no-swap", action="store_true", help="one run only, A loaded first")
    ap.add_argument("--port", type=int, default=18461, help="A's port; B's is the next")
    ap.add_argument("--out", default="ab-compare.json")
    A = harness.A = ap.parse_args()
    A.keep = False
    qs = queries(A.queries)
    runs, same = [], True
    orders = [(A.a, A.b)] + ([] if A.no_swap else [(A.b, A.a)])
    for k, (first, second) in enumerate(orders):
        wall, used, answers = one_run(first, second, A, qs, (A.port, A.port + 1))
        if k == 1:  # (B was loaded first: put it back under its own name)
            wall, used, answers = ({"a": d["b"], "b": d["a"]} for d in (wall, used, answers))
        differ = [i for i in qs if answers["a"][i] != answers["b"][i]]
        same &= not differ
        run = {"b_loaded_first": k == 1, "answers_differ": differ}
        for what, d in (("wall", wall), ("cpu", used)):
            a, b = sum(d["a"].values()), sum(d["b"].values())
            run[what] = {"a_s": round(a, 3), "b_s": round(b, 3), "b_over_a": round(b / a, 4),
                         "per_query": {i: [round(d["a"][i], 4), round(d["b"][i], 4)] for i in qs}}
        runs.append(run)
        print(f"run {k + 1} ({'B' if k else 'A'} loaded first): wall B/A {run['wall']['b_over_a']:.3f}, cpu B/A {run['cpu']['b_over_a']:.3f}"
              + (f"; answers differ: {differ}" if differ else ""), flush=True)
    summary = {what: round(math.exp(statistics.mean(math.log(r[what]["b_over_a"]) for r in runs)), 4) for what in ("wall", "cpu")}
    json.dump({"a": A.a, "b": A.b, "rounds": A.rounds, "runs": runs, "b_over_a": summary, "same_answers": same}, open(A.out, "w"), indent=1)
    print(json.dumps({"b_over_a": summary, "same_answers": same}))
    sys.exit(0 if same else 1)


if __name__ == "__main__":
    main()
