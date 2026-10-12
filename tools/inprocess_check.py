#!/usr/bin/env python3
"""`pondra.connect()` of a lake and JavaScript's `connect()`: a node in this process, checked (ADR-061 steps 4
and 6, ADR-072; python/pondra/inprocess.py, js/index.js).

  inprocess_check.py [--lib PATH] [--bin PATH] [--bench]

--lib is the library (libpondra_lib.so, .dylib or pondra_lib.dll), as PONDRA_LIBRARY; --bin the pondra program,
as PONDRA_BIN (a node beside the process and a node's launcher use it). Each check is a child process of this
script, because a process holds one node and its settings are fixed by its first open; each Python child has
this checkout's python/ first on its path. The JavaScript children (i to n) are in inprocess_check.mjs, run by
Node, or by Bun for n (RUNNERS); o reads the library's symbols here, with nm.

a. "pondra.connect() with no path: SQL, a frame, pandas in and out, a file here, a live query; its lake held
   in memory, with nothing written in the temporary folder or here, and a new open a new lake"
b. "a lake opened, written, closed and opened again in this process: its rows"
c. "connect() while a node is open here: the same lake another connection to it (sessions apart, one closed and
   the other still answering), another lake and another memory lake a node each beside this process, a URL a
   connection; local() and open() warn that they go at 1.0"
d. "a misspelt setting is refused naming the nearest"
e. "a setting changed on a later open is refused naming it"
f. "50 opens and closes leave no threads or open files" (Linux)
g. "a fork's child: its parent's connection refused by name, connect() there a node beside it; the parent still
   answers" (Linux and macOS)
h. "the library asks for no static TLS, so any process loads it" (Linux)
i. "connect() in JavaScript with no path, not awaited: SQL, a frame, writeTable, a file here, a live query; its lake held in
   memory, with nothing written in the temporary folder or here" (Node)
j. "JavaScript: a lake opened, written, closed and opened again: its rows; the same lake another connection,
   another lake a node beside; a misspelt setting and a changed one refused by name; open() warns" (Node)
k. "JavaScript: 20 opens and closes leave no threads or open files" (Node, Linux)
l. "a Node process that calls process.exit() without close() closes its node: its lake's term given back" (Node)
m. "a Node program that ends without close() closes its node (the cleanup hook): its lake's term given back" (Node)
n. "Bun: connect() answers SQL" (Bun, when it is installed)
o. "the library defines its ABI, and needs only glibc and libgcc by version: nothing of a host binds to it" (Linux, nm)

--bench also prints, not asserted: the time to the first answer and the idle memory, against
a node beside the process (what pondra.local() was) and DuckDB, and 10 M rows into pandas. Only the standard library is needed; the checks
that use pandas, pyarrow, Polars or DuckDB are skipped when one of them isn't installed.
"""
import argparse, importlib.util, json, os, re, shutil, statistics, subprocess, sys, tempfile, time

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LINUX, FORK = sys.platform.startswith("linux"), hasattr(os, "fork")
MJS = os.path.join(os.path.dirname(os.path.abspath(__file__)), "inprocess_check.mjs")  # the JavaScript children
BUN_HOME = os.path.expanduser("~/.bun/bin/bun")  # where bun's installer puts it, when it isn't on the PATH
RUNTIMES = {"node": shutil.which("node"), "bun": shutil.which("bun") or (BUN_HOME if os.path.isfile(BUN_HOME) else None)}
RUNNERS = {"js_sql": "node", "js_calls": "node", "js_reopen": "node", "js_cycles": "node", "js_exit": "node", "js_end": "node", "js_bun": "bun"}  # (check -> runtime)


def has(module):
    return importlib.util.find_spec(module) is not None


def emit(facts):
    """A child's answer: one line the parent reads (the rest of its output is shown on failure)."""
    print("FACTS " + json.dumps(facts), flush=True)


def rss_mb(pid="self", field="VmRSS"):
    """A process's resident memory in MB, from /proc (Linux); None elsewhere."""
    try:
        with open(f"/proc/{pid}/status") as status:
            for line in status:
                if line.startswith(field + ":"):
                    return int(line.split()[1]) / 1024
    except OSError:
        return None
    return None


def refusal(*args, **settings):
    """What `pondra.connect(*args, **settings)` says when it is refused (None: it opened, and is closed)."""
    import pondra
    try:
        db = pondra.connect(*args, **settings)
    except RuntimeError as e:
        return str(e)
    db.close()
    return None


# ------------------------------------------------------------ the children: what each one found

def child_sql():
    import pondra
    db = pondra.connect()
    db.sql("CREATE TABLE t (id INT, name TEXT)")
    db.sql("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    facts = {"count": db.sql("SELECT count(*) AS n FROM t").rows()[0]["n"], "pandas": None, "polars": None}
    if has("pandas") and has("pyarrow"):
        import pandas
        db.write_table("p", pandas.DataFrame({"x": [1, 2]}))
        facts["pandas"] = int(db.sql("SELECT sum(x) AS s FROM p").to_pandas()["s"][0])
    if has("polars") and has("pyarrow"):
        facts["polars"] = int(db.sql("SELECT count(*) AS n FROM t").to_polars()["n"][0])
    scratch = tempfile.mkdtemp(prefix="inprocess-check-")
    path = os.path.join(scratch, "here.csv")
    with open(path, "w") as f:
        f.write("id,name\n1,a\n2,b\n3,c\n")
    facts["file"] = db.sql(f"SELECT count(*) AS n FROM '{path}'").rows()[0]["n"]
    shutil.rmtree(scratch, ignore_errors=True)
    live = db.live("SELECT count(*) AS n FROM t")
    facts["live"] = next(live)[0]["n"]
    live.close()
    # (its loopback door, which live queries use: only with the host's key, as a node local() starts)
    import urllib.error
    import urllib.request
    def door(headers):
        try:
            return urllib.request.urlopen(urllib.request.Request(db.url + "/sql", data=b"SELECT 1", headers=headers), timeout=30).status
        except urllib.error.HTTPError as e:
            return e.code
    facts["door"] = {"without": door({}), "with": door({"x-pondra-owner": db.owner})}
    facts["console"] = db.console_url == f"{db.url}/#key={db.owner}"
    facts["lake"], facts["while_open"] = db.lake, written_here()
    db.close()
    facts["after_close"] = written_here()
    db = pondra.connect(":memory:")
    facts["fresh"] = db.sql("SELECT count(*) AS n FROM information_schema.tables WHERE table_name = 't'").rows()[0]["n"]
    db.close()
    return facts


def wait_until(fact, secs=10):
    """The seconds until `fact()` holds, asked every 0.1 s, or None if it doesn't within `secs`."""
    started = time.monotonic()
    while time.monotonic() - started < secs:
        if fact():
            return time.monotonic() - started
        time.sleep(0.1)
    return None


def child_calls():
    """Every call through the library and none over the loopback port: a live query, a watch, a big answer in
    pieces; and Ctrl-C (a notebook's Interrupt) stopping a long query, and its work on the node."""
    import _thread
    import threading
    import pondra
    db = pondra.connect()

    def socket(*args, **kwargs):
        raise AssertionError("a call went over the loopback port")

    db._open = socket
    db.sql("CREATE TABLE t (id BIGINT)")
    db.sql("INSERT INTO t VALUES (1), (2)")
    live = db.live("SELECT count(*) AS n FROM t")
    answers = [next(live)[0]["n"]]
    db.sql("INSERT INTO t VALUES (3)")
    answers.append(next(live)[0]["n"])
    live.close()
    seen = []

    def watching():
        for row in db.watch("t"):
            seen.append(row["id"])
            break

    watcher = threading.Thread(target=watching)
    watcher.start()
    time.sleep(1)  # (the watch is on the node before the row commits)
    db.sql("INSERT INTO t VALUES (4)")
    watcher.join(30)
    stats = lambda: json.loads(db._call("GET", "/stats"))
    ended = wait_until(lambda: stats()["live_queries"] == 0)
    csv = db._call("POST", "/sql?format=csv", b"SELECT * FROM range(3000000)")  # (past 8 MB: written in pieces)

    def running():
        text = db._call("GET", "/metrics").decode()
        return next((float(line.split()[-1]) for line in text.splitlines() if line.startswith("pondra_queries_running")), None)

    threading.Timer(0.5, _thread.interrupt_main).start()
    started = time.perf_counter()
    try:
        db.sql("SELECT sum(value) AS s FROM range(100000000000)").rows()  # (minutes of work, stopped at 0.5 s)
        interrupted = None
    except KeyboardInterrupt:
        interrupted = time.perf_counter() - started
    stopped = wait_until(lambda: running() == 0)
    after = db.sql("SELECT 1 AS one").rows()[0]["one"]
    db.close()
    return {"live": answers, "watched": seen, "live_ended": ended, "csv_lines": csv.count(b"\n"),
            "interrupted": interrupted, "stopped": stopped, "after": after}


def written_here():
    """What is in the temporary folder, which is also the current one: a child that asks is given a new, empty
    one (`run_child`, HERE)."""
    root = tempfile.gettempdir()
    return sorted(os.path.relpath(os.path.join(d, n), root) for d, dirs, files in os.walk(root) for n in dirs + files)


def child_reopen():
    import pondra
    scratch = tempfile.mkdtemp(prefix="inprocess-check-")
    try:
        lake = os.path.join(scratch, "lake")
        db = pondra.connect(lake)
        db.sql("CREATE TABLE r (id INT)")
        db.sql("INSERT INTO r VALUES (1), (2), (3), (4)")
        db.close()
        db = pondra.connect(lake)
        count = db.sql("SELECT count(*) AS n FROM r").rows()[0]["n"]
        db.close()
        return {"count": count}
    finally:
        shutil.rmtree(scratch, ignore_errors=True)


def child_second():
    import warnings
    import pondra
    scratch = tempfile.mkdtemp(prefix="inprocess-check-")
    try:
        lake = os.path.join(scratch, "lake")
        a, b = pondra.connect(lake), pondra.connect(lake)  # (the same lake twice: one node, two connections)
        a.sql("CREATE TABLE s (id INT)")
        a.sql("INSERT INTO s VALUES (1), (2)")
        a.sql("CREATE TEMP TABLE mine AS SELECT 1 AS x")
        try:
            b.sql("SELECT * FROM mine").rows()
            apart = False
        except Exception:
            apart = True  # (a's temporary table is a's session's alone)
        shared = a._handle == b._handle and getattr(b, "process", None) is None
        a.close()
        left = b.sql("SELECT count(*) AS n FROM s").rows()[0]["n"]  # (b still answers after a closed)
        other = pondra.connect(os.path.join(scratch, "other"))  # (another lake: a node beside this process)
        other.sql("CREATE TABLE o (id INT)")
        other.sql("INSERT INTO o VALUES (1)")
        other_rows = other.sql("SELECT count(*) AS n FROM o").rows()[0]["n"]
        memory = pondra.connect()  # (a second lake in memory: beside it too)
        memory_one = memory.sql("SELECT 1 AS one").rows()[0]["one"]
        beside = [getattr(other, "process", None) is not None, getattr(memory, "process", None) is not None]
        for db in (memory, other, b):
            db.close()
        again = pondra.connect(lake)  # (the node let go with its last connection: this one opens it again, here)
        again_here = getattr(again, "process", None) is None and again.sql("SELECT count(*) AS n FROM s").rows()[0]["n"]
        again.close()
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            pondra.open(lake).close()
            pondra.local(os.path.join(scratch, "third")).close()
        warned = sorted(str(w.message).split(" goes")[0].split()[-1] for w in caught if issubclass(w.category, DeprecationWarning))
        url = pondra.connect("http://127.0.0.1:9")  # (a URL: a connection, nothing started)
        return {"shared": shared, "apart": apart, "left": left, "other_rows": other_rows, "memory_one": memory_one,
                "beside": beside, "again_here": again_here, "warned": warned, "url": [url.url, getattr(url, "_handle", None)]}
    finally:
        shutil.rmtree(scratch, ignore_errors=True)


def child_misspelt():
    scratch = tempfile.mkdtemp(prefix="inprocess-check-")
    message = refusal(os.path.join(scratch, "lake"), memory_gbb=2)
    shutil.rmtree(scratch, ignore_errors=True)
    return {"message": message}


def child_changed():
    import pondra
    scratch = tempfile.mkdtemp(prefix="inprocess-check-")
    try:
        pondra.connect(os.path.join(scratch, "one"), memory_gb=2).close()
        return {"message": refusal(os.path.join(scratch, "two"), memory_gb=3)}
    finally:
        shutil.rmtree(scratch, ignore_errors=True)


def child_cycles():
    import pondra

    def cycle():
        db = pondra.connect()  # (a new lake in a new folder each time: each count is 1)
        db.sql("CREATE TABLE IF NOT EXISTS t (id INT)")
        db.sql("INSERT INTO t VALUES (1)")
        n = db.sql("SELECT count(*) AS n FROM t").rows()[0]["n"]
        db.close()
        return n

    def switches():  # (every time a thread of this process was woken)
        total = 0
        for t in os.listdir("/proc/self/task"):
            try:
                total += sum(int(l.split()[1]) for l in open(f"/proc/self/task/{t}/status") if l.startswith(("voluntary_ctxt_switches:", "nonvoluntary_ctxt_switches:")))
            except OSError:
                pass  # (a thread that ended meanwhile)
        return total

    def census():
        time.sleep(2)  # (a blocking thread is let go after a second idle)
        before = switches()
        time.sleep(2)  # (a node left running, or a task of its, wakes its threads while nothing is open)
        return len(os.listdir("/proc/self/task")), len(os.listdir("/proc/self/fd")), switches() - before

    rows = [cycle()]
    first = census()
    rows += [cycle() for _ in range(49)]
    last = census()
    return {"threads": [first[0], last[0]], "files": [first[1], last[1]], "wakeups": [first[2], last[2]], "rows": rows}


def child_fork():
    import pondra
    db = pondra.connect()
    db.sql("CREATE TABLE f (id INT)")
    db.sql("INSERT INTO f VALUES (1), (2)")
    pid = os.fork()
    if pid == 0:  # (the child: its node is its parent's, so its request is refused by name; its own connect() is a node beside it)
        code = 0
        try:
            db.sql("SELECT 1").rows()
            code = 1
        except RuntimeError as e:
            print(f"child: {e}", file=sys.stderr, flush=True)
            code = 0 if "fork" in str(e) and "spawn" in str(e) else 1
        try:
            mine = pondra.connect()
            code = code or (0 if mine.sql("SELECT 7 AS n").rows()[0]["n"] == 7 and mine.process is not None else 2)
            mine.close()
        except Exception as e:
            print(f"child's connect(): {e}", file=sys.stderr, flush=True)
            code = code or 3
        os._exit(code)
    _, status = os.waitpid(pid, 0)
    parent = db.sql("SELECT count(*) AS n FROM f").rows()[0]["n"]
    db.close()
    return {"child": os.waitstatus_to_exitcode(status), "parent": parent}


def child_tls():
    """What the library asks of the loader (ELF64, little-endian). DF_STATIC_TLS puts its thread-locals in
    the block glibc keeps for the program's own, which has room for a kilobyte or two: a dlopen then fails
    ("cannot allocate memory in static TLS block") once they are more, or once other libraries took it."""
    import struct
    from pondra import inprocess
    path = os.environ.get("PONDRA_LIBRARY") or os.path.join(os.path.dirname(inprocess.__file__), "_lib", inprocess._NAME)
    with open(path, "rb") as f:
        def at(offset, size):
            f.seek(offset)
            return f.read(size)
        if at(0, 5) != b"\x7fELF\x02":
            return {"elf": False}
        (phoff,), (phentsize, phnum) = struct.unpack("<Q", at(0x20, 8)), struct.unpack("<HH", at(0x36, 4))
        flags, tls = 0, 0
        for i in range(phnum):
            kind, _, offset, _, _, size, memory, _ = struct.unpack("<IIQQQQQQ", at(phoff + i * phentsize, 56))
            if kind == 7:  # PT_TLS
                tls = memory
            elif kind == 2:  # PT_DYNAMIC
                flags = next((value for tag, value in struct.iter_unpack("<qQ", at(offset, size)) if tag == 30), 0)  # DT_FLAGS
    return {"elf": True, "static_tls": bool(flags & 0x10), "tls_bytes": tls}


def child_bench_open():
    import pondra
    started = time.perf_counter()
    db = pondra.connect()
    db.sql("SELECT 1").rows()
    first = time.perf_counter() - started
    time.sleep(2)
    facts = {"first": first, "rss": rss_mb()}
    db.close()
    return facts


def child_bench_local():
    import pondra
    scratch = tempfile.mkdtemp(prefix="inprocess-check-")
    started = time.perf_counter()
    db = pondra.client._child(os.path.join(scratch, "lake"))
    db.sql("SELECT 1").rows()
    first = time.perf_counter() - started
    time.sleep(2)
    facts = {"first": first, "rss": rss_mb(), "node_rss": rss_mb(db.process.pid)}
    db.close()
    shutil.rmtree(scratch, ignore_errors=True)
    return facts


def child_bench_duckdb():
    import duckdb
    started = time.perf_counter()
    duckdb.connect().sql("SELECT 1").fetchall()
    first = time.perf_counter() - started
    time.sleep(2)
    return {"first": first, "rss": rss_mb()}


def child_bench_pandas_open():
    import pondra
    db = pondra.connect()
    db.sql("SELECT 1").rows()
    started = time.perf_counter()
    db.sql("SELECT * FROM range(10000000)").to_pandas()
    seconds = time.perf_counter() - started
    facts = {"seconds": seconds, "peak": rss_mb(field="VmHWM")}
    db.close()
    return facts


def child_bench_pandas_local():
    import pondra
    scratch = tempfile.mkdtemp(prefix="inprocess-check-")
    db = pondra.client._child(os.path.join(scratch, "lake"))
    db.sql("SELECT 1").rows()
    started = time.perf_counter()
    db.sql("SELECT * FROM range(10000000)").to_pandas()
    seconds = time.perf_counter() - started
    mine, node = rss_mb(field="VmHWM"), rss_mb(db.process.pid, field="VmHWM")
    facts = {"seconds": seconds, "peak": None if mine is None or node is None else mine + node}  # (both processes)
    db.close()
    shutil.rmtree(scratch, ignore_errors=True)
    return facts


def child_bench_pandas_duckdb():
    import duckdb
    con = duckdb.connect()
    started = time.perf_counter()
    con.sql("SELECT * FROM range(10000000)").df()
    seconds = time.perf_counter() - started
    return {"seconds": seconds, "peak": rss_mb(field="VmHWM")}


CHILDREN = {
    "sql": child_sql, "reopen": child_reopen, "second": child_second, "misspelt": child_misspelt,
    "changed": child_changed, "cycles": child_cycles, "fork": child_fork, "calls": child_calls,
    "tls": child_tls,
    "bench_open": child_bench_open, "bench_local": child_bench_local, "bench_duckdb": child_bench_duckdb,
    "bench_pandas_open": child_bench_pandas_open, "bench_pandas_local": child_bench_pandas_local,
    "bench_pandas_duckdb": child_bench_pandas_duckdb,
}


# ------------------------------------------------------------ the parent: what each check must show

def judge_sql(f):
    yield f["count"] == 3, f"CREATE, INSERT and a frame's count: {f['count']} rows (3)"
    yield f["live"] == 3, f"a live query's first answer: {f['live']} (3)"
    yield f["file"] == 3, f"a CSV file on this machine, read by name: {f['file']} rows (3)"
    yield f["door"] == {"without": 401, "with": 200}, f"its loopback door answers only its host, with the key: {f['door']} (401 without, 200 with)"
    yield f["console"], "console_url is the loopback door with the key (#key=…)"
    if f["pandas"] is None:
        yield None, "pandas in and out: pandas or pyarrow isn't installed"
    else:
        yield f["pandas"] == 3, f"pandas in and out: a DataFrame written, read back and summed: {f['pandas']} (3)"
    if f["polars"] is None:
        yield None, "Polars: polars or pyarrow isn't installed"
    else:
        yield f["polars"] == 3, f"Polars: {f['polars']} rows (3)"
    yield f["lake"] == ":memory:" and not f["while_open"] and not f["after_close"], \
        f"its lake ({f['lake']}) is held in memory: nothing written in the temporary folder or here, open or closed: {f['while_open'] + f['after_close'] or 'none'}"
    yield f["fresh"] == 0, f"open(':memory:') after close is a new lake: {f['fresh']} tables named t (0)"


def judge_reopen(f):
    yield f["count"] == 4, f"written, closed, opened again: {f['count']} rows (4)"


def judge_second(f):
    yield f["shared"] and f["apart"], f"the same lake connected twice: one node ({f['shared']}), each connection's session its own ({f['apart']})"
    yield f["left"] == 2, f"one of them closed, the other still answers: {f['left']} rows (2)"
    yield f["beside"] == [True, True] and f["other_rows"] == 1 and f["memory_one"] == 1, \
        f"another lake and another memory lake while it is open: a node beside this process each {f['beside']}, answering"
    yield f["again_here"] == 2, f"its last connection closed, the lake connected again opens here: {f['again_here']} rows (2)"
    yield f["warned"] == ["pondra.local()", "pondra.open()"], f"local() and open() warn that they go at 1.0: {f['warned']}"
    yield f["url"] == ["http://127.0.0.1:9", None], f"a URL is a connection to a node, nothing started: {f['url']}"


def judge_misspelt(f):
    message = f["message"]
    yield message is not None and "memory_gbb isn't a setting (did you mean memory_gb?)" in message, f"a misspelt setting: {message or 'accepted'}"


def judge_changed(f):
    message = f["message"]
    yield message is not None and "memory_gb (2 now)" in message, f"a setting changed on a later open: {message or 'accepted'}"


def judge_cycles(f):
    (t0, t1), (f0, f1) = f["threads"], f["files"]
    yield t1 <= t0 + 2 and f1 <= f0 + 2, f"50 opens and closes: threads {t0} then {t1}, open files {f0} then {f1} (at most 2 more each)"
    (w0, w1) = f["wakeups"]
    yield w1 <= w0 + 20, f"nothing of the 50 runs once they are closed: {w0} then {w1} wakeups of the process's threads in 2 s idle (at most 20 more)"
    yield set(f["rows"]) == {1} and len(f["rows"]) == 50, "every cycle's lake answered its count (1)"


def judge_fork(f):
    yield f["child"] == 0, f"a fork's child: its parent's connection refused by name, its own connect() a node beside it (exit {f['child']}, 0)"
    yield f["parent"] == 2, f"the parent still answers after the fork: {f['parent']} rows (2)"


def judge_tls(f):
    if not f["elf"]:
        yield None, "not an ELF library"
        return
    yield not f["static_tls"], (f"its {f['tls_bytes']} bytes of thread-locals are given to each thread as it first uses them, "
                                "never taken from the static TLS block" if not f["static_tls"] else
                                f"it is marked STATIC_TLS ({f['tls_bytes']} bytes): build its mimalloc with local_dynamic_tls")


def judge_calls(f):
    yield f["live"] == [2, 3], f"a live query: its answer, and again after an INSERT: {f['live']} ([2, 3])"
    yield f["watched"] == [4], f"a watch: the row committed after it started: {f['watched']} ([4])"
    yield f["live_ended"] is not None, f"both ended on the node when their loops stopped (live_queries 0): {'yes' if f['live_ended'] is not None else 'no'}"
    yield f["csv_lines"] == 3000001, f"an answer past 8 MB, read in pieces as written: {f['csv_lines']} lines (3000001)"
    took = f["interrupted"]
    yield took is not None and took < 2, f"Ctrl-C 0.5 s into a long query stops the call: {'not stopped' if took is None else f'after {took:.2f} s'} (under 2 s)"
    yield f["stopped"] is not None and f["after"] == 1, \
        f"…and its work on the node (no query running {'after ' + format(f['stopped'], '.1f') + ' s' if f['stopped'] is not None else 'never'}); the node answers after: {f['after']}"


def judge_js_calls(f):
    yield f["live"] == [2, 3], f"JavaScript, with fetch refused: a live query and again after an INSERT: {f['live']} ([2, 3])"
    yield f["watched"] == 4, f"a watch: the row committed after it started: {f['watched']} (4)"
    yield f["live_left"] == 0, f"both ended on the node when their loops stopped: live_queries {f['live_left']} (0)"
    yield f["csv_lines"] == 3000001, f"an answer past 8 MB, read in pieces as written: {f['csv_lines']} lines (3000001)"
    yield f["after_cancel"] == 1, f"a big answer cancelled half-read, then the node answers: {f['after_cancel']} (1)"
    yield f["per_call_ms"] < 5, f"SELECT 1, 200 times: {f['per_call_ms']:.2f} ms a call (under 5)"


def judge_js_sql(f):
    yield f["rows"] == 3, f"CREATE, an append, and a query's rows: {f['rows']} (3)"
    yield f["frame"] == 2, f"a frame filtered to the ids above 1: {f['frame']} rows (2)"
    yield f["written"] == 2, f"writeTable makes a table from rows: {f['written']} rows (2)"
    yield f["file_rows"] == 3, f"a CSV file on this machine, read by name: {f['file_rows']} rows (3)"
    if f["live"] is None:
        yield None, "a live query: no such method"
    else:
        yield f["live"] == 3, f"a live query's first answer: {f['live']} (3)"
    yield f["lake"] == ":memory:" and not f["while_open"] and not f["after_close"], \
        f"its lake ({f['lake']}) is held in memory: nothing written in the temporary folder or here, open or closed: {f['while_open'] + f['after_close'] or 'none'}"


def judge_js_reopen(f):
    yield f["count"] == 2, f"written, closed, opened again: {f['count']} rows (2)"
    yield f["shared"] and f["same_count"] == 2 and f["left"] == 2, \
        f"the same lake again: one node ({f['shared']}), {f['same_count']} rows; closed, the other still answers: {f['left']} (2)"
    yield f["beside"] and f["other_one"] == 1, f"another lake while one is open: a node beside this process ({f['beside']}), answering"
    yield f["warned"] == ["pondra's open()"], f"open() warns that it goes at 1.0: {f['warned']}"
    misspelt, changed = f["misspelt"], f["changed"]
    # (the misspelt name holds "memory_gb" itself, so the nearest name is what is looked for, after "did you mean")
    yield misspelt is not None and re.search(r"did you mean\W+(PONDRA_)?memory_gb\b", misspelt, re.I) is not None, \
        f"a misspelt setting, the nearest named: {misspelt or 'accepted'}"
    yield changed is not None and re.search("memory_gb", changed, re.I) is not None, \
        f"a setting changed on a later open, refused naming it: {changed or 'accepted'}"


def judge_js_cycles(f):
    if f.get("skip"):
        yield None, "needs Linux's /proc"
        return
    (t0, t1), (f0, f1) = f["threads"], f["files"]
    yield t1 <= t0 + 2 and f1 <= f0 + 2, f"20 opens and closes: threads {t0} then {t1}, open files {f0} then {f1} (at most 2 more each)"


def judge_js_exit(f):
    # (the child's exit status is checked by run_child: a process that exits with an error is a FAIL)
    left = os.path.join(f["lake"], "cluster", "left")
    yield os.path.isdir(left) and bool(os.listdir(left)), f"a Node process that ends without close(): its node stepped down, giving its term back ({left})"
    shutil.rmtree(os.path.dirname(f["lake"]), ignore_errors=True)


def judge_js_bun(f):
    yield f["count"] == 3, f"Bun: CREATE, an append and a count: {f['count']} rows (3)"
    yield f["lake"] == ":memory:", f"Bun: with no folder, a lake in memory ({f['lake']})"
    yield f["live"] == 3 and f["csv_lines"] == 3000001, f"Bun: a live query ({f['live']}, 3) and an answer read in pieces ({f['csv_lines']} lines, 3000001)"


def library_symbols(args):
    """The library's dynamic symbols, as nm -D lists them (name, kind): read here, with no child."""
    path = args.lib or os.environ.get("PONDRA_LIBRARY")
    if not path:
        return {"lib": None, "defined": [], "needs": 0, "loose": []}
    path = os.path.abspath(path)
    listed = subprocess.run(["nm", "-D", path], capture_output=True, text=True, timeout=120, check=True)
    rows = [line.split()[-2:] for line in listed.stdout.splitlines() if line.strip()]  # (kind, name[@version])
    needs = {n for k, n in rows if k == "U"}
    # (what it needs is kept as a count and the unversioned ones: the full list is two hundred of glibc's)
    return {"lib": path, "defined": sorted({n for k, n in rows if k not in "Uwv"}), "needs": len(needs), "loose": sorted(n for n in needs if "@" not in n)}


# A dependency's C API that the library exports and nothing calls: object_store's checksums (crc-fast's `ffi`, on by
# its default features). No library a host has defines these names.
EXPORTED_BY_DEPENDENCIES = ("crc_fast_",)


def judge_symbols(f):
    if f["lib"] is None:
        yield None, "no library named (--lib or PONDRA_LIBRARY)"
        return
    ours = lambda n: n == "napi_register_module_v1" or n.startswith("pondra_")
    extra = [n for n in f["defined"] if not ours(n) and not n.startswith(EXPORTED_BY_DEPENDENCIES)]
    yield "napi_register_module_v1" in f["defined"] and not extra, \
        f"what it defines ({len(f['defined'])}): the ABI, Node-API's registration and crc-fast's C API; other: {extra or 'none'}"
    # (a symbol it needs without a version would bind to whatever the host defines first: Node's zlib, OpenSSL, libuv)
    yield not f["loose"], f"what it needs ({f['needs']}): only glibc's and libgcc's, by version; unversioned: {f['loose'] or 'none'}"


HERE = {"sql", "js_sql"}  # (children given a temporary folder of their own, which is their current folder too)
DIRECT = {"symbols": library_symbols}  # (checks read in this process, with no child: see run_child)

CHECKS = [  # (child, label, judge)
    ("sql", "pondra.connect() with no path: SQL, a frame, pandas in and out, a file here, a live query; its lake in memory, nothing on disk", judge_sql),
    ("calls", "every call through the library, none over the port: a live query, a watch, a big answer; Ctrl-C stops a long query and its work", judge_calls),
    ("reopen", "a lake opened, written, closed and opened again in this process: its rows", judge_reopen),
    ("second", "connect() while a node is open here: the same lake another connection, another lake (or memory) a node beside; local() and open() warn", judge_second),
    ("misspelt", "a misspelt setting is refused naming the nearest", judge_misspelt),
    ("changed", "a setting changed on a later open is refused naming it", judge_changed),
    ("cycles", "50 opens and closes leave no threads or open files", judge_cycles),
    ("fork", "a fork's child: its parent's connection refused by name, connect() there a node beside it; the parent still answers", judge_fork),
    ("tls", "the library asks for no static TLS, so any process loads it", judge_tls),
    ("js_sql", "connect() in JavaScript with no path, not awaited: SQL, a frame, writeTable, a file here, a live query; its lake in memory, nothing on disk", judge_js_sql),
    ("js_calls", "JavaScript: every call through Node-API, none over the port: a live query, a watch, a big answer in pieces, one cancelled", judge_js_calls),
    ("js_reopen", "JavaScript: a lake opened, closed and opened again: its rows; the same lake another connection, another a node beside; a misspelt and a changed setting refused; open() warns", judge_js_reopen),
    ("js_cycles", "JavaScript: 20 opens and closes leave no threads or open files", judge_js_cycles),
    ("js_exit", "a Node process that calls process.exit() without close() closes its node: its lake's term given back", judge_js_exit),
    ("js_end", "a Node program that ends without close() closes its node (the cleanup hook): its lake's term given back", judge_js_exit),
    ("js_bun", "Bun: connect() answers SQL", judge_js_bun),
    ("symbols", "the library defines its ABI, and needs only glibc and libgcc by version: nothing of a host binds to it", judge_symbols),
]
NO_NODE, NO_BUN = RUNTIMES["node"] is None, RUNTIMES["bun"] is None
SKIP_IF = {"cycles": (not LINUX, "needs Linux's /proc"), "fork": (not FORK, "needs fork (Linux or macOS)"), "tls": (not LINUX, "glibc's loader"),
           "js_sql": (NO_NODE, "needs node"), "js_calls": (NO_NODE, "needs node"), "js_reopen": (NO_NODE, "needs node"), "js_exit": (NO_NODE, "needs node"), "js_end": (NO_NODE, "needs node"),
           "js_cycles": (NO_NODE or not LINUX, "needs node and Linux's /proc"), "js_bun": (NO_BUN, "needs Bun (bun.sh, or ~/.bun/bin/bun)"),
           "symbols": (not LINUX or shutil.which("nm") is None, "needs Linux and nm")}
BENCH = [  # (child, what it is, the modules it needs)
    ("bench_open", "pondra.connect()", []),
    ("bench_local", "a node beside (local())", []),
    ("bench_duckdb", "DuckDB", ["duckdb"]),
]
BENCH_PANDAS = [
    ("bench_pandas_open", "pondra.connect()", ["pandas", "pyarrow"]),
    ("bench_pandas_local", "a node beside (local())", ["pandas", "pyarrow"]),
    ("bench_pandas_duckdb", "DuckDB", ["duckdb", "pandas"]),
]


def run_child(name, args):
    """One child's facts, or the reason it failed (its error's last words). A check in RUNNERS is a JavaScript
    child, started by its runtime on inprocess_check.mjs; one in DIRECT is read here, with no child."""
    if name in DIRECT:
        try:
            return DIRECT[name](args), None
        except (OSError, subprocess.SubprocessError) as e:
            return None, str(e)
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join(p for p in (os.path.join(REPO, "python"), env.get("PYTHONPATH")) if p)
    if args.lib:
        env["PONDRA_LIBRARY"] = os.path.abspath(args.lib)
    if args.bin:
        env["PONDRA_BIN"] = os.path.abspath(args.bin)
    runtime = RUNNERS.get(name)
    command = [RUNTIMES[runtime], MJS, name] if runtime else [sys.executable, os.path.abspath(__file__), "--child", name]
    here = tempfile.mkdtemp(prefix="inprocess-here-") if name in HERE else None  # (its temporary folder and its current one: empty)
    if here:
        env.update(TMPDIR=here, TEMP=here, TMP=here)
    try:
        done = subprocess.run(command, env=env, capture_output=True, text=True, timeout=900, cwd=here)
    except subprocess.TimeoutExpired:
        return None, "timed out after 900 s"
    finally:
        if here:
            shutil.rmtree(here, ignore_errors=True)
    line = next((ln for ln in done.stdout.splitlines() if ln.startswith("FACTS ")), None)
    if done.returncode or line is None:
        return None, done.stderr.strip()[-600:] or f"exit {done.returncode}, no answer"
    return json.loads(line[len("FACTS "):]), None


def fmt_mb(value):
    return "-" if value is None else f"{value:.0f} MB"


PANDAS_RUNS = 5


def bench(args):
    print("\nnumbers (not asserted)")
    print("  first answer to SELECT 1 (after the imports), and the memory when idle for 2 s:")
    for name, label, needs in BENCH:
        if any(not has(m) for m in needs):
            print(f"    skip {label}: needs {', '.join(needs)}")
            continue
        facts, error = run_child(name, args)
        if error:
            print(f"    FAIL {label}: {error.splitlines()[-1]}")
            continue
        # (a node beside: this process's memory plus its node's)
        idle = None if facts["rss"] is None else facts["rss"] + (facts.get("node_rss") or 0)
        note = f" (the node {fmt_mb(facts['node_rss'])})" if facts.get("node_rss") else ""
        print(f"    {label:<14} {facts['first']:.3f} s   idle memory {fmt_mb(idle)}{note}")
    print(f"  10 M rows (SELECT * FROM range(10000000)) into pandas, the median of {PANDAS_RUNS} runs taken in turn: the time,")
    print("  and the peak memory (a node beside: the two processes added up):")
    ready = [(name, label) for name, label, needs in BENCH_PANDAS if all(has(m) for m in needs)]
    for name, label, needs in BENCH_PANDAS:
        if (name, label) not in ready:
            print(f"    skip {label}: needs {', '.join(needs)}")
    runs, errors = {name: [] for name, _ in ready}, {}
    for i in range(PANDAS_RUNS):  # (in turn, each order every other time: a busy moment slows all of them alike)
        for name, _ in (ready if i % 2 == 0 else ready[::-1]):
            facts, error = run_child(name, args)
            if error:
                errors[name] = error.splitlines()[-1]
            else:
                runs[name].append(facts)
    for name, label in ready:
        if not runs[name]:
            print(f"    FAIL {label}: {errors.get(name)}")
            continue
        seconds = statistics.median(f["seconds"] for f in runs[name])
        peaks = [f["peak"] for f in runs[name] if f["peak"] is not None]  # (none where there is no /proc)
        peak = statistics.median(peaks) if peaks else None
        print(f"    {label:<14} {seconds:.2f} s   peak memory {fmt_mb(peak)}")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--lib", help="the library (libpondra_lib.so, .dylib or pondra_lib.dll), as PONDRA_LIBRARY")
    ap.add_argument("--bin", help="the pondra program, as PONDRA_BIN")
    ap.add_argument("--bench", action="store_true", help="also print the numbers (not asserted)")
    ap.add_argument("--child", help=argparse.SUPPRESS)
    args = ap.parse_args()
    if args.child:
        emit(CHILDREN[args.child]())
        return
    if args.lib and not os.path.isfile(args.lib):
        sys.exit(f"no library at {args.lib}")
    failed, summary = 0, {}
    for name, label, judge in CHECKS:
        skipped, why = SKIP_IF.get(name, (False, ""))
        if skipped:
            print(f"skip {label}: {why}")
            summary[name] = {"status": "skip"}
            continue
        facts, error = run_child(name, args)
        if error:
            print(f"FAIL {label}: {error.splitlines()[-1]}")  # (its last line: the error; the JSON has all of it)
            failed += 1
            summary[name] = {"status": "FAIL", "error": error}
            continue
        verdicts = list(judge(facts))
        status = "FAIL" if any(ok is False for ok, _ in verdicts) else "ok" if any(ok for ok, _ in verdicts) else "skip"
        print(f"{status} {label}")
        for ok, text in verdicts:
            print(f"    {'skip' if ok is None else 'ok' if ok else 'FAIL'}: {text}")
        failed += status == "FAIL"
        summary[name] = {"status": status, "facts": facts}
    if args.bench:
        bench(args)
    print(json.dumps({"failed": failed, "checks": summary}))
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
