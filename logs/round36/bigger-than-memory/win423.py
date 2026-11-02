#!/usr/bin/env python3
"""Invariant 423's two window checks (harness.py scale), standalone: win423.py BIN LAKE [--load] [--env K=V ...]"""
import json, os, signal, subprocess, sys, time, urllib.request

BIN, LAKE = sys.argv[1], sys.argv[2]
args = sys.argv[3:]
load = "--load" in args
extra = dict(a.split("=", 1) for a in args if "=" in a)
LS = "md5(value::VARCHAR) || md5(value::VARCHAR) || md5(value::VARCHAR)"
if load:
    for s in ("CREATE TABLE ws1 (k BIGINT, s VARCHAR)", "CREATE TABLE ws3 (k BIGINT, s VARCHAR)",
              f"INSERT INTO ws1 SELECT 0, {LS} FROM generate_series(1, 9000000)",
              f"INSERT INTO ws3 SELECT value % 3, {LS} FROM generate_series(1, 6000000)"):
        subprocess.run([BIN, "sql", "--dir", LAKE, s], check=True, capture_output=True, timeout=900)


def call(port, body, timeout):
    req = urllib.request.Request(f"http://127.0.0.1:{port}/sql", data=body.encode(), method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.read().decode()[:300]


def hwm(pid):
    for line in open(f"/proc/{pid}/status"):
        if line.startswith("VmHWM:"):
            return int(line.split()[1]) >> 10
    return 0


def windowed(table, env, port=7970):
    e = {**os.environ, "PONDRA_CORES": "4", "PONDRA_MEMORY_MAX_GB": "1", **env, **extra}
    p = subprocess.Popen([BIN, "serve", "--dir", LAKE, "--addr", f"127.0.0.1:{port}", "--memory-gb=0.25"], env=e,
                         stdout=subprocess.DEVNULL, stderr=open(f"/tmp/l1/win423-{table}.stderr", "a"))
    for _ in range(600):
        try:
            urllib.request.urlopen(f"http://127.0.0.1:{port}/stats", timeout=2)
            break
        except Exception:
            time.sleep(0.1)
    t0 = time.time()
    ans = call(port, f"SELECT count(*) AS n FROM (SELECT rank() OVER (PARTITION BY k ORDER BY s) AS r FROM {table}) t WHERE r = 1", 900)
    secs = round(time.time() - t0, 1)
    peak = hwm(p.pid) if p.poll() is None else -1
    p.send_signal(signal.SIGKILL); p.wait()
    return {"table": table, "answer": ans, "peak_mb": peak, "secs": secs, "env": extra}


a = windowed("ws1", {"PONDRA_SPILL_GB": "3"})
b = windowed("ws3", {})
ok = a["answer"] == [{"n": 1}] and a["peak_mb"] < 1024 and b["answer"] == [{"n": 3}]
print(json.dumps({"ok": ok, "ws1": a, "ws3": b}))
