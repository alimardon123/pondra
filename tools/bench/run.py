#!/usr/bin/env python3
"""Run the same benchmark on Pondra, Spark and Flink, one engine at a time, and record each engine's
peak memory (RSS of its pondra/java processes). Results: bench/results.json (appended).
  run.py batch  N          e.g. run.py batch 20000000
  run.py stream N KEYS     e.g. run.py stream 5000000 100000
  run.py etl    N
  run.py live   SECS KEYS  (Pondra only: durable end-to-end)
ENGINES=pondra,spark picks engines (default: all three)."""
import json, os, subprocess, sys, tempfile, threading, time

HERE = os.path.dirname(os.path.abspath(__file__))
PY = {"pondra": "python3", "spark": "/home/claude/venv-spark/bin/python", "flink": "/home/claude/venv-flink/bin/python"}
ENGINE_PROCS = {"pondra", "java"}


def tree(pid):
    kids = {}
    for p in os.listdir("/proc"):
        if p.isdigit():
            try:
                ppid = int(open(f"/proc/{p}/stat").read().rsplit(")", 1)[1].split()[1])
                kids.setdefault(ppid, []).append(int(p))
            except Exception:
                pass
    out, todo = [], [pid]
    while todo:
        p = todo.pop()
        out.append(p)
        todo += kids.get(p, [])
    return out


def rss_mb(pids):
    total = 0
    for p in pids:
        try:
            if open(f"/proc/{p}/comm").read().strip() in ENGINE_PROCS:
                total += int(open(f"/proc/{p}/statm").read().split()[1]) * os.sysconf("SC_PAGE_SIZE")
        except Exception:
            pass
    return total / 2**20


def run(engine, args):
    script = os.path.join(HERE, f"{engine}_bench.py")
    p = subprocess.Popen([PY[engine], script, *args], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, cwd=HERE)
    peak = [0.0]
    def sample():
        while p.poll() is None:
            peak[0] = max(peak[0], rss_mb(tree(p.pid)))
            time.sleep(0.25)
    t = threading.Thread(target=sample)
    t.start()
    out = p.stdout.read()
    p.wait()
    t.join()
    lines = [l[l.index('{"engine"'):] for l in out.splitlines() if '{"engine"' in l]
    res = json.loads(lines[-1]) if lines else {"engine": engine, "error": out[-500:]}
    res["peak_rss_mb"] = round(peak[0])
    return res


if __name__ == "__main__":
    mode, rest = sys.argv[1], sys.argv[2:]
    engines = ["pondra"] if mode == "live" else os.environ.get("ENGINES", "pondra,spark,flink").split(",")
    with open(os.path.join(HERE, "results.json"), "a") as f:
        for e in engines:
            extra = [tempfile.mkdtemp(prefix=f"bench-{e}-")] if mode == "batch" else []
            r = run(e, [mode, *rest, *extra])
            print(json.dumps(r), flush=True)
            f.write(json.dumps(r) + "\n")
