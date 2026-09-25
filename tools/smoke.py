#!/usr/bin/env python3
"""A first run of a freshly built binary, on each OS it is built for (CI's build job): the shell
takes statements on its input, a node serves SQL, schemas and views work, and the node knows the
machine's memory and its own (on Windows and macOS there is no /proc to read them from).

  python3 tools/smoke.py path/to/pondra[.exe]

Only the standard library: the build machines have Python but nothing installed for it."""
import json, os, subprocess, sys, tempfile, time, urllib.request

BIN = os.path.abspath(sys.argv[1] if len(sys.argv) > 1 else "target/release/pondra")
direct = urllib.request.build_opener(urllib.request.ProxyHandler({}))  # (this machine's own node: never a proxy)


def call(port, path, body=None):
    with direct.open(urllib.request.Request(f"http://127.0.0.1:{port}{path}", data=body), timeout=30) as r:
        return r.read().decode()


def main():
    checks, work = {}, tempfile.mkdtemp(prefix="pondra-smoke-")
    shell = subprocess.run([BIN, os.path.join(work, "shell")], input="CREATE SCHEMA s;\nCREATE TABLE s.t (x BIGINT);\nINSERT INTO s.t VALUES (1), (2);\nSELECT sum(x) AS total FROM s.t;\n",
                           capture_output=True, text=True, timeout=180)
    checks["the shell runs statements from its input"] = "| 3 " in shell.stdout
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
        metrics = dict(line.rsplit(" ", 1) for line in call(port, "/metrics").splitlines() if line and not line.startswith("#"))
        resident, limit = float(metrics["pondra_resident_bytes"]), float(metrics["pondra_memory_limit_bytes"])
        checks["the node knows its resident memory"] = resident > 10 << 20
        checks["and the machine's (a third of it for queries, not the 4 GiB guess)"] = limit != 4 << 30 and limit > 256 << 20
    finally:
        node.terminate()
        node.wait(timeout=30)
    ok = all(checks.values())
    print(json.dumps({"smoke": checks, "platform": sys.platform, "ok": ok}, indent=1))
    if not ok:
        print(shell.stdout, shell.stderr, open(log.name).read()[-2000:], file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
