#!/usr/bin/env python3
"""A first run of a freshly built binary, on each OS it is built for (CI's build job): the shell
takes statements on its input, a node serves SQL, schemas and views work, and the node knows the
machine's memory and its own (on Windows and macOS there is no /proc to read them from); an
extension's .wasm built once on Linux runs here; the shell attaches the lakes beside its own, for that session, and `FROM t` reads every column;
a node a Pondra process starts takes the settings it is handed on its input (`settings::Line`).

  python3 tools/smoke.py path/to/pondra[.exe]

Only the standard library: the build machines have Python but nothing installed for it."""
import json, os, subprocess, sys, tempfile, time, urllib.error, urllib.request

BIN = os.path.abspath(sys.argv[1] if len(sys.argv) > 1 else "target/release/pondra")
direct = urllib.request.build_opener(urllib.request.ProxyHandler({}))  # (this machine's own node: never a proxy)


def call(port, path, body=None):
    with direct.open(urllib.request.Request(f"http://127.0.0.1:{port}{path}", data=body), timeout=30) as r:
        return r.read().decode()


def main():
    checks, work = {}, tempfile.mkdtemp(prefix="pondra-smoke-")
    csv = os.path.join(work, "AMEX_SPY, 1(1).csv")  # (a name as downloads get them: a space, a comma, brackets)
    with open(csv, "w") as f:
        f.write("id,price\n1,10.5\n2,20.25\n3,7\n4,1\n")
    shell = subprocess.run([BIN, os.path.join(work, "shell")], input="CREATE SCHEMA s;\nCREATE TABLE s.t (x BIGINT);\nINSERT INTO s.t VALUES (1), (2);\nSELECT sum(x) AS total FROM s.t;\n"
                           f"SELECT count(*) AS rows_in_file FROM '{csv}';\nCREATE DATABASE other;\nCREATE TABLE other.p AS SELECT * FROM '{csv}';\nSELECT count(*) AS n FROM other.p;\n",
                           capture_output=True, text=True, timeout=180)
    checks["the shell runs statements from its input"] = "| 3 " in shell.stdout
    checks["the shell reads a file on this machine; CREATE DATABASE makes another lake"] = "| 4 " in shell.stdout and shell.stdout.count("| 4 ") == 2
    # An extension built once, on Linux (extensions/hello, written with the kit in extensions/kit), runs on this OS too (ADR-075 E4)
    hello = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "tests", "fixtures", "extensions", "hello-0.1.wasm")).replace("\\", "/")
    ext = subprocess.run([BIN, os.path.join(work, "ext")], input=f"INSTALL hello FROM '{hello}';\nSELECT greet('Ada') AS g, add(2, 3) AS s, checked_div(7, 2) AS d;\n"
                         "SELECT sum(square) AS sq FROM count_to(3);\n", capture_output=True, text=True, timeout=300,
                         env={**os.environ, "PONDRA_ALLOW_UNSIGNED_EXTENSIONS": "true", "PONDRA_CACHE_DIR": os.path.join(work, "cache")})
    checks["an extension's .wasm built on Linux installs and answers here: its functions and a table function"] = \
        "hello, Ada" in ext.stdout and "| 5 " in ext.stdout and "| 3 " in ext.stdout and "| 14 " in ext.stdout
    port = 8099
    log = open(os.path.join(work, "node.log"), "w")
    node = subprocess.Popen([BIN, "serve", "--dir", os.path.join(work, "lake"), "--addr", f"127.0.0.1:{port}"], stdout=subprocess.DEVNULL, stderr=log)
    try:
        for _ in range(600):
            try:
                call(port, "/stats")
                break
            except OSError:
                time.sleep(0.1)
        sql = lambda s: json.loads(call(port, "/sql", s.encode()))
        for s in ("CREATE SCHEMA sales", "CREATE TABLE sales.orders (id BIGINT, amount DOUBLE)", "INSERT INTO sales.orders VALUES (1, 2.5), (2, 4.0)",
                  "CREATE VIEW big AS SELECT * FROM sales.orders WHERE amount > 3"):
            sql(s)
        checks["SQL: a schema, a table in it, a view"] = sql("SELECT count(*) AS n FROM big") == [{"n": 1}] and sql("SELECT sum(amount) AS s FROM sales.orders") == [{"s": 6.5}]
        checks["the lake is named after its folder (lake.schema.table), on Windows too"] = sql("SELECT count(*) AS n FROM lake.sales.orders") == [{"n": 2}]
        try:
            sql(f"SELECT count(*) AS n FROM '{csv}'")
            refused = False
        except urllib.error.HTTPError:
            refused = True
        checks["a node doesn't read this machine's files for whoever asks"] = refused
        sql("UPDATE sales.orders SET amount = amount * 2 WHERE id = 1")
        checks["UPDATE: the new version, the same row"] = sql("SELECT sum(amount) AS s, count(DISTINCT _row_id) AS ids FROM sales.orders") == [{"s": 9.0, "ids": 2}]
        metrics = dict(line.rsplit(" ", 1) for line in call(port, "/metrics").splitlines() if line and not line.startswith("#"))
        resident, limit = float(metrics["pondra_resident_bytes"]), float(metrics["pondra_memory_limit_bytes"])
        checks["the node knows its resident memory"] = resident > 10 << 20
        checks["and the machine's (a third of it for queries, not the 4 GiB guess)"] = limit != 4 << 30 and limit > 256 << 20
        # kill -9 lets go of the node's term lock: the next writer on this machine finds it free and
        # writes at once, instead of waiting out the leader's 30 s mark.
        node.kill()
        node.wait(timeout=30)
        started = time.time()
        r = subprocess.run([BIN, "sql", "--dir", os.path.join(work, "lake"), "INSERT INTO sales.orders VALUES (3, 1.0)"], capture_output=True, text=True, timeout=120)
        seconds = round(time.time() - started, 2)
        print(f"the next writer after a killed node took {seconds}s", file=sys.stderr)
        checks["a node killed, the next writer on this machine writes at once (the OS let go of its term)"] = r.returncode == 0 and seconds < 15
    finally:
        node.terminate()
        node.wait(timeout=30)
    # A node a Pondra process starts (the shell's, `pondra lead`) is handed its settings on the first line of its input,
    # not in its environment (`settings::Line`): here the key that reads this machine's files, as the shell's node is
    # handed its. The rest of its input closing still stops it (`--stop-with-stdin`); a line that isn't one stops it at once.
    key, handed_port = "a-key-handed-on-the-line", 8098
    handed = subprocess.Popen([BIN, "serve", "--dir", os.path.join(work, "handed"), "--addr", f"127.0.0.1:{handed_port}", "--stop-with-stdin", "--settings-stdin"],
                              stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=log, env={k: v for k, v in os.environ.items() if not k.startswith("PONDRA_")})
    stopped = False
    try:
        handed.stdin.write(json.dumps({"PONDRA_OWNER_KEY": key}).encode() + b"\n")
        handed.stdin.flush()
        for _ in range(600):
            try:
                call(handed_port, "/stats")
                break
            except OSError:
                time.sleep(0.1)

        def files(owner):
            asked = urllib.request.Request(f"http://127.0.0.1:{handed_port}/sql", data=f"SELECT count(*) AS n FROM '{csv}'".encode(), headers={"x-pondra-owner": owner})
            try:
                with direct.open(asked, timeout=30) as r:
                    return json.loads(r.read())
            except urllib.error.HTTPError:
                return None
        checks["a node takes the settings it is handed on its input: the key it was handed reads this machine's files, another doesn't"] = \
            files(key) == [{"n": 4}] and files("not-the-key-it-was-handed") is None
        handed.stdin.close()
        stopped = handed.wait(timeout=30) == 0
    except subprocess.TimeoutExpired:
        pass
    finally:
        handed.kill()
        handed.wait(timeout=30)
    checks["…and stops when the rest of its input closes"] = stopped
    bad = subprocess.run([BIN, "serve", "--dir", os.path.join(work, "bad"), "--settings-stdin"], input="memory_gb=2\n", capture_output=True, text=True, timeout=60)
    checks["a line of settings that isn't one stops the node at once, naming why"] = bad.returncode != 0 and "--settings-stdin" in bad.stderr
    # The shell attaches the other lakes in its folder, for as long as it runs (ADR-024): here
    # `lake` (the node's, just stopped) and `other` (CREATE DATABASE's) beside `shell`.
    beside = subprocess.run([BIN, "shell"], cwd=work, input=".databases\nFROM lake.sales.orders ORDER BY id;\n", capture_output=True, text=True, timeout=180)
    elsewhere = subprocess.run([BIN, os.path.join(work, "shell")], cwd=tempfile.mkdtemp(), capture_output=True, text=True, timeout=180,
                               input=".databases\nCREATE TABLE t (a BIGINT);\nCREATE MATERIALIZED VIEW mv AS SELECT a, count(*) AS n FROM t GROUP BY a;\n.tables\n")
    cells = lambda out: {c.strip() for line in out.splitlines() if line.startswith("|") for c in line.strip("|").split("|")}
    checks["the shell attaches the lakes beside it: .databases lists them"] = {"shell", "other", "lake"} <= cells(beside.stdout)
    checks["FROM t is SELECT * FROM t (DuckDB's), on a lake found beside the shell's"] = {"id", "amount", "1", "2"} <= cells(beside.stdout)
    rows = [[c.strip() for c in line.strip("|").split("|")] for line in elsewhere.stdout.splitlines() if line.startswith("|")]
    databases = {r[0] for r in rows if len(r) == 1}  # (.databases: one column)
    checks["…for that session only: nothing saved in the shell's lake"] = "lake" not in databases and {"shell", "other"} <= databases
    checks[".tables says each one's kind: a materialized view is one, not a BASE TABLE"] = ["shell", "public", "mv", "materialized view"] in rows and ["shell", "public", "t", "table"] in rows
    # A mistyped command, or a folder of other things, is never made a lake (invariant 142, as serve).
    here, cluttered = tempfile.mkdtemp(), tempfile.mkdtemp()
    with open(os.path.join(cluttered, "notes.txt"), "w") as f:
        f.write("mine")
    typo = subprocess.run([BIN, "serv"], cwd=here, capture_output=True, text=True, timeout=60)
    full = subprocess.run([BIN, cluttered], capture_output=True, text=True, timeout=60)
    checks["a mistyped command isn't made a lake (did you mean pondra serve?), nor a folder of other things"] = \
        typo.returncode != 0 and "did you mean `pondra serve`" in typo.stderr and os.listdir(here) == [] \
        and full.returncode != 0 and "holds other things than a lake" in full.stderr and os.listdir(cluttered) == ["notes.txt"]
    ok = all(checks.values())
    print(json.dumps({"smoke": checks, "platform": sys.platform, "ok": ok}, indent=1))
    if not ok:
        print(shell.stdout, shell.stderr, ext.stdout, ext.stderr, beside.stdout, beside.stderr, elsewhere.stdout, typo.stderr, full.stderr, bad.stderr, open(log.name).read()[-2000:], file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
