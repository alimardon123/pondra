#!/usr/bin/env python3
"""Two builds side by side on TPC-H: is B slower than A? (It imports tpch.py beside it.)

  ab_compare.py --a OLD_BIN --b NEW_BIN --data ~/tpch/sf1 [--queries tools/bench/tpch-queries] [--rounds 8]
                [--load append|ctas] [--lake folder|memory] [--a-env K=V ...] [--b-env K=V ...] [--hot-gb N]

Run one build after the other on a shared machine and the answer swings 5-10% either way with whatever
else the machine does. So each build gets its own node and lake, loaded alike, and every query runs on
one and then the other, the order flipped each round: what the machine does lands on both. The node
loaded second runs a few percent slower whichever build it is (2-7% in round 35's runs), so the whole
run is done twice with the builds' places swapped, and the two ratios are averaged (geometric mean).

Both nodes keep all of the data's columns hot (`--hot-gb`, by default four times the Parquet's uncompressed
bytes, rounded up: 3 GB for SF1). Under the hot tier's own limit (an eighth of RAM, 1.2 GB on a 15 GB machine
against SF1's 1.29 GB) each node keeps a different part hot, whichever loaded or was evicted first, and an A/B
measures that luck: q3's A/A ratio was 1.25 (reviews/q3-and-the-hot-limit.md). After the warm runs, once their
loads end, each node's hot tier is checked: one that came within 10% of its limit is named, and that run's ratios
aren't to be trusted.

Per query: the median wall time and the median of the node's own CPU time (utime + stime, from /proc),
and whether both builds gave the same answer. Prints B/A for wall and CPU; writes everything to --out,
with each build's load seconds per run.
  --load ctas     each node loads with `CREATE TABLE t AS FROM '<file>'` per table (the K2 hook's shape), over
                  HTTP with PONDRA_OWNER_KEY, so the node reads local paths; the load seconds are those CREATEs'.
                  Default append: the tables as Arrow batches over /append (load seconds include its /tier).
  --lake memory   the nodes run on `:memory:` lakes, not folders.
  --a-env/--b-env K=V  added to that build's node env, repeatable; they follow the build when the places swap.
  --hot-gb 0      the hot tier off (PONDRA_HOT_GB=0): the cold path.
Exit 1 if any answer differs. Linux only (/proc).
"""
import argparse, json, math, os, shutil, statistics, sys, tempfile, time, uuid
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, ".."))
sys.path.insert(0, HERE)
import harness
from harness import Node, call, metrics_of
from tpch import TABLES, columns, queries


def cpu(pid):
    """The process's own CPU seconds so far (user + system)."""
    fields = open(f"/proc/{pid}/stat").read().rsplit(")", 1)[1].split()
    return (int(fields[11]) + int(fields[12])) / os.sysconf("SC_CLK_TCK")


def load(port, data):
    """The 8 tables from Parquet, appended as Arrow and tiered, as tpch.py loads them."""
    import io, pyarrow as pa, pyarrow.ipc, pyarrow.parquet as pq
    for t in TABLES:
        table = pq.read_table(os.path.join(data, f"{t}.parquet"))
        cols = columns(table.schema)
        call(port, "POST", f"/tables/{t}", json.dumps(cols).encode())
        for seq, batch in enumerate(table.to_batches(max_chunksize=500_000), 1):
            buf = io.BytesIO()
            with pa.ipc.new_stream(buf, batch.schema) as w:
                w.write_batch(batch)
            call(port, "POST", f"/append/{t}?producer=load-{t}&seq={seq}", buf.getvalue(),
                 headers={"content-type": "application/vnd.apache.arrow.stream"}, timeout=3600)
    call(port, "POST", "/tier", timeout=3600)


def load_ctas(port, data, owner):
    """The 8 tables by `CREATE TABLE t AS FROM '<file>'`, one statement each, as files_tpch.py's lake loads them
    (the node reads local paths for its owner key). Tiered after the timing, as `load` is."""
    for t in TABLES:
        call(port, "POST", "/sql", f"CREATE TABLE {t} AS FROM '{os.path.join(data, t)}.parquet'".encode(), timeout=3600,
             headers={"x-pondra-owner": owner})


def hot_gb(data):
    """What the hot tier needs for every column of the data: four times the Parquet's uncompressed bytes (decoded
    strings take about twice those), in whole GB."""
    import pyarrow.parquet as pq
    def raw(t):
        m = pq.ParquetFile(os.path.join(data, f"{t}.parquet")).metadata
        return sum(m.row_group(i).total_byte_size for i in range(m.num_row_groups))
    return max(1, math.ceil(4 * sum(raw(t) for t in TABLES) / 2**30))


def hot_settled(port, hdr):
    """harness.hot_settled, with the node's owner header (a node with an owner key answers no one else)."""
    import re
    def now():
        m = call(port, "GET", "/metrics", headers=hdr)
        return [float(g.group(1)) if (g := re.search(rb"\npondra_hot_%s (\S+)" % k, m)) else 0.0 for k in (b"bytes", b"loading_files")]
    last = None
    while (held := now()) != last or held[1]:
        last = held
        time.sleep(1)
    return held[0]


def hot_full(port, hdr):
    """Whether the node's hot columns came within 10% of their limit: some then read from Parquet instead. Asked
    once the warm runs' files are loaded (in the background, after a file's second read), so no round runs half hot."""
    hot_settled(port, hdr)
    m = metrics_of(port, headers=hdr)
    return m["pondra_hot_bytes"] > 0.9 * m["pondra_hot_limit_bytes"], round(m["pondra_hot_bytes"] / 2**30, 3)


def rounded(rows):
    """An answer to compare: floats to 6 significant digits (sums may differ in the last bits)."""
    def r(v):
        return float(f"{v:.6g}") if isinstance(v, float) else v
    return [{k: r(v) for k, v in row.items()} if isinstance(row, dict) else row for row in rows]


def one_run(first, second, A, qs, ports):
    """Both builds loaded; every query alternating between them. Returns per-query medians, answers, hot tiers and
    each build's load seconds. `first` and `second` are (binary, extra node env) pairs: the env follows its build."""
    builds = {"a": first, "b": second}
    nodes, lakes, load_s = {}, [], {}
    owner = uuid.uuid4().hex  # (the CTAS's owner key, if --load ctas)
    hdr = {"x-pondra-owner": owner} if A.load == "ctas" else {}  # (a node with an owner key answers only with it)
    try:
        for (name, (binary, extra)), port in zip(builds.items(), ports):
            lake = ":memory:" if A.lake == "memory" else tempfile.mkdtemp(prefix="pondra-ab-")
            if lake != ":memory:":
                lakes.append(lake)
            harness.BIN = binary  # (read when a Node is made)
            env = {"PONDRA_HOT_GB": str(A.hot_gb), **extra, **({"PONDRA_OWNER_KEY": owner} if A.load == "ctas" else {})}
            node = Node(lake, port, env=env).start()
            t = time.time()
            if A.load == "ctas":
                load_ctas(port, A.data, owner)
                load_s[name] = round(time.time() - t, 2)
                call(port, "POST", "/tier", timeout=3600, headers=hdr)
            else:
                load(port, A.data)
                load_s[name] = round(time.time() - t, 2)
            nodes[name] = (node, port)
        answers = {"a": {}, "b": {}}
        for w in range(2):  # two warm runs each, not counted (the hot tier takes a file on its second read); the first's answer is the one compared
            for i, q in qs.items():
                for name in "ab":
                    got = call(nodes[name][1], "POST", "/sql", f"{q} -- warm {w}".encode(), timeout=3600, headers=hdr)
                    answers[name].setdefault(i, rounded(got))
        hot = {name: hot_full(port, hdr) for name, (_, port) in nodes.items()}
        for name, (full, gb) in hot.items():
            if full:
                print(f"{name}'s hot columns ({gb} GB) are near their limit ({A.hot_gb} GB): some queries read Parquet, "
                      "and which ones is luck. Give --hot-gb more.", flush=True)
        wall = {n: {i: [] for i in qs} for n in "ab"}
        used = {n: {i: [] for i in qs} for n in "ab"}
        for r in range(A.rounds):
            for i, q in qs.items():
                for name in ("ab" if r % 2 == 0 else "ba"):
                    node, port = nodes[name]
                    c, t = cpu(node.p.pid), time.time()
                    call(port, "POST", "/sql", f"{q} -- round {r}".encode(), timeout=3600, headers=hdr)  # (a new text each time: no remembered answer)
                    wall[name][i].append(time.time() - t)
                    used[name][i].append(cpu(node.p.pid) - c)
        med = lambda d: {n: {i: statistics.median(v) for i, v in d[n].items()} for n in "ab"}
        return med(wall), med(used), answers, hot, load_s
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
    ap.add_argument("--hot-gb", type=float, help="each node's PONDRA_HOT_GB (default: enough for all the data's columns; 0: the hot tier off)")
    ap.add_argument("--load", choices=["append", "ctas"], default="append", help="ctas: each node CREATE TABLE t AS FROM the file; append: Arrow over /append")
    ap.add_argument("--lake", choices=["folder", "memory"], default="folder", help="memory: each node's lake is :memory:")
    ap.add_argument("--a-env", action="append", default=[], metavar="K=V", help="A's node env, repeatable (follows A when the places swap)")
    ap.add_argument("--b-env", action="append", default=[], metavar="K=V", help="B's node env, repeatable (follows B when the places swap)")
    ap.add_argument("--no-swap", action="store_true", help="one run only, A loaded first")
    ap.add_argument("--port", type=int, default=18461, help="A's port; B's is the next")
    ap.add_argument("--out", default="ab-compare.json")
    A = harness.A = ap.parse_args()
    A.keep = False
    A.data = os.path.abspath(A.data)
    if A.hot_gb is None:
        A.hot_gb = hot_gb(A.data)
    a_env, b_env = (dict(kv.split("=", 1) for kv in e) for e in (A.a_env, A.b_env))
    qs = queries(A.queries)
    runs, same = [], True
    orders = [((A.a, a_env), (A.b, b_env))] + ([] if A.no_swap else [((A.b, b_env), (A.a, a_env))])
    for k, (first, second) in enumerate(orders):
        wall, used, answers, hot, load_s = one_run(first, second, A, qs, (A.port, A.port + 1))
        if k == 1:  # (B was loaded first: put it back under its own name)
            wall, used, answers, hot, load_s = ({"a": d["b"], "b": d["a"]} for d in (wall, used, answers, hot, load_s))
        differ = [i for i in qs if answers["a"][i] != answers["b"][i]]
        same &= not differ
        run = {"b_loaded_first": k == 1, "answers_differ": differ, "hot_gb": {n: hot[n][1] for n in "ab"}, "hot_full": [n for n in "ab" if hot[n][0]],
               "load_s": load_s}
        for what, d in (("wall", wall), ("cpu", used)):
            a, b = sum(d["a"].values()), sum(d["b"].values())
            run[what] = {"a_s": round(a, 3), "b_s": round(b, 3), "b_over_a": round(b / a, 4),
                         "per_query": {i: [round(d["a"][i], 4), round(d["b"][i], 4)] for i in qs}}
        runs.append(run)
        print(f"run {k + 1} ({'B' if k else 'A'} loaded first): wall B/A {run['wall']['b_over_a']:.3f}, cpu B/A {run['cpu']['b_over_a']:.3f}, "
              f"load A {load_s['a']} s, B {load_s['b']} s"
              + (f"; answers differ: {differ}" if differ else "") + (f"; hot tier full on {run['hot_full']}: don't trust it" if run["hot_full"] else ""), flush=True)
    summary = {what: round(math.exp(statistics.mean(math.log(r[what]["b_over_a"]) for r in runs)), 4) for what in ("wall", "cpu")}
    json.dump({"a": A.a, "b": A.b, "a_env": a_env, "b_env": b_env, "rounds": A.rounds, "hot_gb": A.hot_gb, "load": A.load, "lake": A.lake,
               "runs": runs, "b_over_a": summary, "same_answers": same}, open(A.out, "w"), indent=1)
    print(json.dumps({"b_over_a": summary, "same_answers": same}))
    sys.exit(0 if same else 1)


if __name__ == "__main__":
    main()
