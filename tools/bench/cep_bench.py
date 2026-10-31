#!/usr/bin/env python3
"""MATCH_RECOGNIZE on one stream, two engines: Pondra's view that writes each match once (`EMIT FINAL`, ADR-073 step 8)
against Flink's CEP in streaming mode, and whether they find the same matches.

The ticker: 1,000 symbols, a seeded price random walk each, one row per symbol per second, so N rows span N/1,000
seconds. The rows are written once, to `ticks-0.csv` (no header) in --dir, in time order, and both engines read that one
file. Flink reads it as one split, so one source reader reads it and no reader's watermark passes a row it has yet to
send (with several files, a reader that takes a later file first drops the earlier file's rows as late). Pondra loads
the rows with one statement before its clock starts (CREATE TABLE … AS SELECT over read_csv, run by `pondra sql` on the
lake: a node reads no local paths over HTTP), then takes them into `ticks` in 10 commits in time order. Flink reads the
file with the filesystem connector, in flink_cep.py, its own Python.

The query is the same text in both (MATCH): PATTERN (strt down+ up), the rising row last, which Flink 2.3 accepts;
Flink has no PREV, so its LAST(var, 1) stands for it. The first pattern, ending in a greedy up+, is refused by Flink
(a greedy quantifier last), and is not run here. MATCH_PREV is the PREV form, for Pondra's cross-check (--prev-check).

  pondra_query  the plain query over the whole table, counted: SELECT count(*) FROM (query), median of three. Its matches
                are the target of the view.
  pondra_view   dips: that query WITH (lateness = '0 seconds') … EMIT FINAL, fed by the 10 commits. Timed from the first
                commit until its count equals the plain query's, polled every 0.2 s, 300 s at most.
  flink         the query into a blackhole sink, submit to end (median of three), and into a CSV folder, whose rows are
                the matches: their count, and the comparison.
  flink.scan_s  Flink's read alone: the same ticks table into a blackhole with no pattern, timed the same way (the time
                Flink takes to read the file, of the flink time above).
  compare       the matches as sets of (symbol, start_ts, end_ts): the view, the plain query, a scan of the rows (model),
                and Flink. Each engine runs alone, with a node or a JVM of its own.

Runs: each timing is the median of three where one run is under a minute, else one run. Prints one JSON object.

  PONDRA_BIN=<pondra binary> cep_bench.py [--ns 100000,1000000,10000000] [--dir FOLDER] [--engines pondra,flink]
  (Flink: PyFlink 2.3 in $FLINK_PYTHON, or /home/claude/venv-flink; the ticks are written under --dir, once per N)
"""
import argparse, array, csv, datetime, itertools, json, os, random, shutil, statistics, subprocess, sys, tempfile, threading, time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

SYMBOLS = 1000
FILES = 1          # ticks-K.csv: one file in time order, so Flink's one split is read by one reader (see the docstring)
SEED = 2026
BASE = datetime.datetime(2026, 10, 30)  # (the stream's first second)
COMMITS = 10
# The pattern ends in a single row, which Flink 2.3 accepts (a greedy quantifier last is refused). Flink has no PREV: its
# LAST(var, 1) is the row before, when the variable matched it, so a run's first row is compared with strt. Both engines
# run this text. MATCH_PREV is the PREV form, for Pondra alone: a cross-check at one N.
MATCH = """SELECT * FROM ticks MATCH_RECOGNIZE (
  PARTITION BY symbol ORDER BY ts
  MEASURES FIRST(strt.ts) AS start_ts, LAST(down.ts) AS bottom_ts, LAST(up.ts) AS end_ts
  ONE ROW PER MATCH AFTER MATCH SKIP PAST LAST ROW
  PATTERN (strt down+ up)
  DEFINE down AS (LAST(down.price, 1) IS NULL AND down.price < strt.price) OR down.price < LAST(down.price, 1),
         up AS up.price > LAST(down.price))"""
MATCH_PREV = """SELECT * FROM ticks MATCH_RECOGNIZE (
  PARTITION BY symbol ORDER BY ts
  MEASURES FIRST(strt.ts) AS start_ts, LAST(down.ts) AS bottom_ts, LAST(up.ts) AS end_ts
  ONE ROW PER MATCH AFTER MATCH SKIP PAST LAST ROW
  PATTERN (strt down+ up)
  DEFINE down AS down.price < PREV(down.price), up AS up.price > PREV(up.price))"""


def ram_gb():
    return os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES") >> 30


def ticks(folder, n):
    """The rows, written once: `ticks-K.csv` (one file: FILES), in time order, every symbol's row each second. Written
    again only when the folder doesn't hold this N (`rows.txt` is written last)."""
    files = [os.path.join(folder, f"ticks-{k}.csv") for k in range(FILES)]
    marker = os.path.join(folder, ".rows.txt")  # (a dot-file: Flink's file source reads every other file in the folder)
    if os.path.exists(marker) and open(marker).read() == str(n):
        return files
    assert n % SYMBOLS == 0, "N is whole seconds: a multiple of 1,000"
    os.makedirs(folder, exist_ok=True)
    seconds = n // SYMBOLS
    walks = []
    for s in range(SYMBOLS):
        rng, price, walk = random.Random(SEED + s), 10000, array.array("i")
        for _ in range(seconds):
            price += rng.randint(-3, 3)
            walk.append(price)
        walks.append(walk)
    stamps = [(BASE + datetime.timedelta(seconds=t)).strftime("%Y-%m-%d %H:%M:%S.000") for t in range(seconds)]
    for k, path in enumerate(files):
        with open(path, "w") as out:
            for t in range(k * seconds // FILES, (k + 1) * seconds // FILES):
                out.writelines(f"SYM{s:03d},{stamps[t]},{walks[s][t]}\n" for s in range(SYMBOLS))
    open(marker, "w").write(str(n))
    return files


def status(pid):
    """(VmRSS, VmHWM) of a process, in kB: the peak is the kernel's own high-water mark."""
    kb = {"VmRSS": 0, "VmHWM": 0}
    try:
        for line in open(f"/proc/{pid}/status"):
            key = line.split(":", 1)[0]
            if key in kb:
                kb[key] = int(line.split()[1])
    except OSError:
        pass
    return kb["VmRSS"], kb["VmHWM"]


def children(pid):
    out = []
    for d in os.listdir("/proc"):
        if d.isdigit():
            try:
                stat = open(f"/proc/{d}/stat").read()
            except OSError:
                continue
            if int(stat.rsplit(")", 1)[1].split()[1]) == pid:
                out.append(int(d))
    return out


class Peak(threading.Thread):
    """The largest resident set of the JVM under a process, sampled every 0.2 s from /proc (and its VmHWM)."""
    def __init__(self, pid):
        super().__init__(daemon=True)
        self.pid, self.stop_, self.rss, self.hwm = pid, threading.Event(), 0, 0

    def run(self):
        while not self.stop_.is_set():
            todo = children(self.pid)
            while todo:
                pid = todo.pop()
                todo += children(pid)
                try:
                    if open(f"/proc/{pid}/comm").read().strip() == "java":
                        rss, hwm = status(pid)
                        self.rss, self.hwm = max(self.rss, rss), max(self.hwm, hwm)
                except OSError:
                    pass
            self.stop_.wait(0.2)

    def finish(self):
        self.stop_.set()
        self.join()
        return {"peak_rss_mb": round(self.rss / 1024, 1), "jvm_vmhwm_mb": round(self.hwm / 1024, 1)}


def median_of_three(run):
    """`run()` gives seconds: three runs where the first is under a minute, else one."""
    first = run()
    runs = [first] + ([run(), run()] if first < 60 else [])
    return runs, round(statistics.median(runs), 3)


def loadavg():
    return [round(x, 2) for x in os.getloadavg()]


def pondra_lake(files):
    """A new lake with the CSV files loaded into `ticks_src` by one statement, before any node starts. A node reads no
    local paths over HTTP, so this is `pondra sql` on the lake, as the harness's own tests load files."""
    import harness
    lake = tempfile.mkdtemp(prefix="pondra-cep-")
    stmt = (f"CREATE TABLE ticks_src AS SELECT * FROM read_csv('{os.path.dirname(files[0])}/ticks-*.csv', header => false, "
            f"columns => {{'symbol': 'VARCHAR', 'ts': 'TIMESTAMP', 'price': 'INT'}})")
    r = subprocess.run([harness.BIN, "sql", "--dir", lake, stmt], capture_output=True, text=True)
    if r.returncode != 0:
        shutil.rmtree(lake, ignore_errors=True)
        raise RuntimeError(f"load: {r.stderr[-800:]}")
    return lake


def pondra_view(n, files, port, target):
    """dips: the view fed by the commits. Its time: from the first commit until its count is `target` (the plain query's
    count), polled every 0.2 s for 300 s at most. Its node's peak RSS and the matches it wrote."""
    import harness
    q = lambda s: harness.sql(port, s)
    seq = itertools.count()
    seconds = n // SYMBOLS
    lake = pondra_lake(files)
    node = harness.Node(lake, port).start()
    try:
        q("CREATE TABLE ticks (symbol VARCHAR, ts TIMESTAMP, price INT)")
        q(f"CREATE MATERIALIZED VIEW dips WITH (lateness = '0 seconds') AS {MATCH} EMIT FINAL")
        cut = lambda k: (BASE + datetime.timedelta(seconds=round(k * seconds / COMMITS))).strftime("%Y-%m-%d %H:%M:%S")
        t0, commits = time.time(), []
        for k in range(COMMITS):
            c = time.time()
            q(f"INSERT INTO ticks SELECT symbol, ts, price FROM ticks_src WHERE ts >= TIMESTAMP '{cut(k)}' AND ts < TIMESTAMP '{cut(k + 1)}'")
            commits.append(round(time.time() - c, 3))
        reached, count, deadline = None, None, time.time() + 300
        while True:
            count = next(iter(q(f"SELECT count(*) AS n{next(seq)} FROM dips")[0].values()))  # (a new alias: not a remembered answer)
            now = time.time()
            if count == target:
                reached = now
                break
            if now > deadline:
                break
            time.sleep(0.2)
        matches = q("SELECT symbol, start_ts, end_ts FROM dips")
        peak = status(node.p.pid)[1] / 1024
        return {"secs": round(reached - t0, 3) if reached else None, "reached": reached is not None, "count": count, "target": target,
                "difference": target - count, "commit_s": commits, "peak_rss_mb": round(peak, 1), "matches": matches}
    finally:
        node.kill()
        shutil.rmtree(lake, ignore_errors=True)


def pondra_query(n, files, port, sql=MATCH):
    """The plain query over the whole table: its time (median of three), its matches, its node's peak RSS."""
    import harness
    q = lambda s: harness.sql(port, s)
    seq = itertools.count()
    lake = pondra_lake(files)
    node = harness.Node(lake, port).start()
    try:
        q("CREATE TABLE ticks (symbol VARCHAR, ts TIMESTAMP, price INT)")
        q("INSERT INTO ticks SELECT symbol, ts, price FROM ticks_src")

        def run():
            t = time.time()
            q(f"SELECT count(*) AS n{next(seq)} FROM ({sql}) m")  # (a new alias each run: the result cache answers a repeated text)
            return round(time.time() - t, 3)

        runs, med = median_of_three(run)
        matches = q(f"SELECT symbol, start_ts, end_ts FROM ({sql}) m")
        peak = status(node.p.pid)[1] / 1024
        return {"secs": runs, "median_s": med, "peak_rss_mb": round(peak, 1), "matches": matches}
    finally:
        node.kill()
        shutil.rmtree(lake, ignore_errors=True)


def flink(n, files, folder, python, state="rocksdb"):
    """flink_cep.py in its own Python, JVM outside the timings: its JSON line, and the JVM's peak RSS. Its state: RocksDB
    (nexmark.py's default: its managed memory a third of the machine's, the JVM's heap a third) or the heap (two thirds,
    as nexmark.py's --flink-state heap). JVM_ARGS overrides the heap."""
    here = os.path.join(os.path.dirname(os.path.abspath(__file__)), "flink_cep.py")
    out_file = folder + f"-flink-matches-{state}.csv"  # (beside the ticks, not in them: Flink's source reads its folder)
    if os.path.exists(out_file):
        os.unlink(out_file)
    spec = {"dir": os.path.dirname(files[0]), "out": out_file, "query": MATCH, "parallelism": os.cpu_count() or 4, "state": state,
            "managed": f"{max(ram_gb() // 3, 1)}g"}
    jvm = os.environ.get("JVM_ARGS") or f"-Xmx{max(ram_gb() * (2 if state == 'heap' else 1) // 3, 1)}g"
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
        json.dump(spec, f)
    p = subprocess.Popen([python, here, f.name], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                         env={**os.environ, "JVM_ARGS": jvm})
    peak = Peak(p.pid)
    peak.start()
    stdout, stderr = p.communicate(timeout=6 * 3600)
    mem = peak.finish()
    os.unlink(f.name)
    lines = [l for l in stdout.splitlines() if l.startswith("{")]
    res = json.loads(lines[-1]) if lines else {"engine": "flink", "failed": stderr[-2000:]}
    res.update(mem)
    if os.path.exists(out_file):
        res["matches"] = list(csv.reader(open(out_file)))
    return res


def to_us(s):
    """A timestamp as microseconds since BASE (Pondra's JSON or Flink's CSV text)."""
    return (datetime.datetime.fromisoformat(str(s).replace("T", " ").rstrip("Z")) - BASE) // datetime.timedelta(microseconds=1)


def keys(rows):
    return {(r["symbol"], to_us(r["start_ts"]), to_us(r["end_ts"])) if isinstance(r, dict) else (r[0], to_us(r[1]), to_us(r[3])) for r in rows}


def iso(k):
    return [k[0], str(BASE + datetime.timedelta(microseconds=k[1])), str(BASE + datetime.timedelta(microseconds=k[2]))]


def differ(a, b, last):
    """What `a` has that `b` lacks: its count, how many end at the last second (a symbol's tail: a later row could
    still extend the match), three examples of the rest, and two tail examples."""
    only = a - b
    tail = sorted(k for k in only if k[2] == last)
    rest = sorted(k for k in only if k[2] != last)
    return {"count": len(only), "at_tail": len(tail), "examples": [iso(k) for k in rest[:3]], "tail_examples": [iso(k) for k in tail[:2]]}


def model(files):
    """The plain query's matches by a scan of the rows, the pattern's semantics written out: from each row as strt, a run
    of falling prices (as long as it goes), then one row that rises on the run's last price; the search goes on past that
    row. Per symbol, as the query partitions it."""
    rows = {}
    for path in files:
        for sym, ts, price in csv.reader(open(path)):
            rows.setdefault(sym, []).append((to_us(ts), int(price)))
    out = set()
    for sym, seq in rows.items():
        t, p, m, i = [x[0] for x in seq], [x[1] for x in seq], len(seq), 0
        while i < m - 1:
            j = i + 1
            if p[j] < p[j - 1]:
                while j < m and p[j] < p[j - 1]:
                    j += 1
                if j < m and p[j] > p[j - 1]:
                    out.add((sym, t[i], t[j]))
                    i = j + 1
                    continue
            i += 1
    return out


def compare(view, plain, ref, last, flink_rows=None):
    """Pondra's view against its own plain query (the view should be that less the tails), the plain query against the
    scan of the rows (`model`), and Flink's matches where Flink ran."""
    V, P = keys(view), keys(plain)
    out = {"view": len(V), "plain": len(P), "scan": len(ref), "plain_is_scan": P == ref,
           "view_not_plain": differ(V, P, last), "plain_not_view": differ(P, V, last)}
    if flink_rows is not None:
        F = keys(flink_rows)
        out |= {"flink": len(F), "view_and_flink": len(V & F), "view_not_flink": differ(V, F, last), "flink_not_view": differ(F, V, last)}
    return out


def main(argv):
    ap = argparse.ArgumentParser()
    ap.add_argument("--ns", default="100000,1000000,10000000")
    ap.add_argument("--dir", default=os.path.join(tempfile.gettempdir(), "cep-bench"), help="the ticks' folder, one per N")
    ap.add_argument("--engines", default="pondra,flink")
    ap.add_argument("--flink-python", default=os.environ.get("FLINK_PYTHON", "/home/claude/venv-flink/bin/python"))
    ap.add_argument("--port", type=int, default=8391)
    ap.add_argument("--prev-check", action="store_true", help="Pondra alone: the PREV form's plain matches against the LAST form's")
    ap.add_argument("--flink-state", default="rocksdb", choices=["rocksdb", "heap"], help="where Flink keeps its state")
    a = ap.parse_args(argv)
    engines = a.engines.split(",")
    result = {"machine": {"nproc": os.cpu_count(), "ram_gb": ram_gb()}, "runs": []}
    for n in map(int, a.ns.split(",")):
        folder = os.path.join(a.dir, str(n))
        files = ticks(folder, n)
        last = (n // SYMBOLS - 1) * 1_000_000  # (the last second, in microseconds since BASE)
        one = {"n": n, "span_s": n // SYMBOLS, "rows": n, "loadavg_before": {}}
        view = query = fl = None
        if "pondra" in engines:
            # (the plain query first: its count is the view's target)
            one["loadavg_before"]["pondra_query"] = loadavg()
            query = pondra_query(n, files, a.port)
            one["loadavg_before"]["pondra_view"] = loadavg()
            view = pondra_view(n, files, a.port, target=len(query["matches"]))
            one["pondra_view"] = {k: v for k, v in view.items() if k != "matches"} | {"matches": len(view["matches"])} | (
                {"rows_per_s": round(n / view["secs"])} if view["secs"] else {})
            one["pondra_query"] = {k: v for k, v in query.items() if k != "matches"} | {"rows_per_s": round(n / query["median_s"]), "matches": len(query["matches"])}
            if a.prev_check:
                prev = pondra_query(n, files, a.port, MATCH_PREV)
                one["pondra_prev_form"] = {k: v for k, v in prev.items() if k != "matches"} | {"matches": len(prev["matches"]),
                                                                                             "same_as_last_form": keys(prev["matches"]) == keys(query["matches"])}
        if "flink" in engines:
            one["loadavg_before"]["flink"] = loadavg()
            fl = flink(n, files, folder, a.flink_python, a.flink_state)
            one["flink"] = {k: v for k, v in fl.items() if k != "matches"}
            if "matches" in fl:
                one["flink"]["matches"] = len(fl["matches"])
            if fl.get("blackhole_s"):
                med = round(statistics.median(fl["blackhole_s"]), 3)
                one["flink"] |= {"median_s": med, "rows_per_s": round(n / med)}
            if fl.get("scan_s"):
                med = round(statistics.median(fl["scan_s"]), 3)
                one["flink"] |= {"scan_median_s": med, "scan_rows_per_s": round(n / med)}
        if view is not None and query is not None:
            ref = model(files)
            one["compare"] = compare(view["matches"], query["matches"], ref, last, fl["matches"] if fl and "matches" in fl else None)
        result["runs"].append(one)
        print(f"n={n} done", file=sys.stderr, flush=True)
    print(json.dumps(result), flush=True)


if __name__ == "__main__":
    main(sys.argv[1:])
