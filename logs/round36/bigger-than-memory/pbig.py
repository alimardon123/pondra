#!/usr/bin/env python3
"""Pondra under a hard memory cap: each query's status, time, peak RSS and peak spill.

  pbig.py --bin pondra --data sf10 --lake lake [--cap-gb 2] [--nodes 1] [--spread 0|1] [--only a,b] [--load]
"""
import argparse, json, os, re, shutil, subprocess, sys, threading, time, urllib.request, urllib.error
import psutil

S = os.path.dirname(os.path.abspath(__file__))
CG = "/sys/fs/cgroup/memory" + next(l.split(":", 2)[2].strip() for l in open("/proc/self/cgroup") if ":memory:" in l) + ""
KEY = "bigger-than-memory-owner-key-0001"


def cgroup(i, cap):
    d = f"{CG}/bigmem-p{i}"
    os.makedirs(d, exist_ok=True)
    open(f"{d}/memory.limit_in_bytes", "w").write(str(int(cap * (1 << 30))))
    return d


def failcnt(d):
    return int(open(f"{d}/memory.failcnt").read())


class Node:
    def __init__(self, i, lake, port, cap, env):
        self.i, self.lake, self.port, self.cap = i, lake, port, cap
        self.cg = cgroup(i, cap)
        self.spill = f"{S}/spill/pondra{i}"
        self.env = {**os.environ, "TMPDIR": self.spill, "PONDRA_OWNER_KEY": KEY, "PONDRA_CACHE_DIR": f"{self.spill}/cache", **env}
        self.log = f"{S}/results/node{i}.stderr"
        self.p = None

    def start(self):
        shutil.rmtree(self.spill, ignore_errors=True)
        os.makedirs(self.spill + "/cache", exist_ok=True)
        cg = self.cg
        def into():
            open(f"{cg}/cgroup.procs", "w").write(str(os.getpid()))
        self.p = subprocess.Popen([A.bin, "serve", "--dir", self.lake, "--addr", f"127.0.0.1:{self.port}"], env=self.env, stdout=subprocess.DEVNULL, stderr=open(self.log, "a"), preexec_fn=into)
        for _ in range(600):
            try:
                call(self.port, "GET", "/stats", timeout=2)
                return self
            except Exception:
                if self.p.poll() is not None:
                    raise RuntimeError("node died at start: " + open(self.log).read()[-800:])
                time.sleep(0.1)
        raise RuntimeError("node did not start")

    def stop(self):
        if self.p and self.p.poll() is None:
            self.p.terminate()
            try:
                self.p.wait(30)
            except subprocess.TimeoutExpired:
                self.p.kill(); self.p.wait()

    def alive(self):
        return self.p.poll() is None


def call(port, method, path, body=b"", timeout=30):
    req = urllib.request.Request(f"http://127.0.0.1:{port}{path}", data=body if method == "POST" else None, method=method, headers={"x-pondra-owner": KEY})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read(), dict(r.headers)


def size(d):
    n = 0
    for root, _, files in os.walk(d):
        for f in files:
            try:
                n += os.path.getsize(os.path.join(root, f))
            except OSError:
                pass
    return n


class Watch:
    """Samples every node's RSS and spill folder until stopped; keeps the peaks."""
    def __init__(self, nodes):
        self.nodes, self.rss, self.spill, self.stop_ = nodes, 0, 0, threading.Event()
        self.t = threading.Thread(target=self.run, daemon=True)

    def run(self):
        k = 0
        while not self.stop_.is_set():
            rss = 0
            for n in self.nodes:
                try:
                    rss = max(rss, psutil.Process(n.p.pid).memory_info().rss)
                except Exception:
                    pass
            self.rss = max(self.rss, rss)
            if k % 10 == 0:  # (a walk costs more: every half second)
                self.spill = max(self.spill, max(size(n.spill) for n in self.nodes))
            k += 1
            time.sleep(0.05)

    def __enter__(self):
        self.t.start(); return self

    def __exit__(self, *a):
        self.stop_.set(); self.t.join()


def run(nodes, q, sql):
    out = f"{S}/spill/out-pondra-{q['id']}.parquet"
    if os.path.exists(out):
        shutil.rmtree(out) if os.path.isdir(out) else os.remove(out)
    sql = sql.replace("{out}", out)
    fails = [failcnt(n.cg) for n in nodes]
    rec = {"engine": "pondra" + (f"x{len(nodes)}" if len(nodes) > 1 else ""), "id": q["id"], "kind": q.get("kind", ""), "cap_gb": A.cap}
    t0 = time.time()
    path = "/sql" + ("?spread=1" if A.spread else "")
    with Watch(nodes) as w:
        try:
            body, h = call(nodes[0].port, "POST", path, (sql + f" -- run {time.time()}").encode(), timeout=A.timeout)
            rows = json.loads(body) if body[:1] in b"[{" else body[:200].decode(errors="replace")
            if "explain" in q["id"]:
                open(f"{S}/results/{q['id']}.json", "wb").write(body)
            rec.update(status="ok", rows=len(rows) if isinstance(rows, list) else None, result=json.dumps(rows)[:300])
        except urllib.error.HTTPError as e:
            rec.update(status="error", code=e.headers.get("x-pondra-sqlstate"), error=e.read().decode(errors="replace")[:400])
        except Exception as e:
            rec.update(status="error", error=repr(e)[:400])
    rec["secs"] = round(time.time() - t0, 1)
    rec["peak_rss_mb"], rec["peak_spill_mb"] = w.rss >> 20, w.spill >> 20
    rec["failcnt"] = sum(failcnt(n.cg) - f for n, f in zip(nodes, fails))
    dead = [n.i for n in nodes if not n.alive()]
    if dead:
        rec.update(status="killed", killed=dead, returncode=[n.p.returncode for n in nodes if not n.alive()])
        for n in nodes:
            n.stop()
        for n in nodes:
            n.start()
    if os.path.exists(out):
        rec["out_mb"] = size(out) >> 20 if os.path.isdir(out) else os.path.getsize(out) >> 20
        shutil.rmtree(out) if os.path.isdir(out) else os.remove(out)
    print(json.dumps(rec), flush=True, file=sys.stderr)
    with open(A.out, "a") as f:
        f.write(json.dumps(rec) + "\n")
    return rec


def main():
    os.makedirs(f"{S}/results", exist_ok=True)
    env = dict(e.split("=", 1) for e in A.env) if A.env else {}
    nodes = []
    for i in range(A.nodes):
        nodes.append(Node(i, A.lake, A.port + i, A.cap, env).start())
        time.sleep(1 if i == 0 else 0.2)
    try:
        for _ in range(300):  # (a cluster: wait until every node has joined)
            st, _ = call(A.port, "GET", "/stats")
            if len(json.loads(st)["nodes"]) >= A.nodes:
                break
            time.sleep(0.2)
        print("stats:", st[:300].decode(errors="replace"), file=sys.stderr)
        if A.load:
            for t in ("lineitem", "orders", "customer"):
                run(nodes, {"id": f"load_{t}", "kind": "load"}, f"CREATE TABLE {t} AS SELECT * FROM read_parquet('{A.data}/{t}/*.parquet')")
        qs = json.load(open(A.queries))
        for q in qs:
            if A.only and q["id"] not in A.only.split(","):
                continue
            run(nodes, q, q["sql"])
    finally:
        for n in nodes:
            n.stop()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin", default="/home/user/pondra/target/release/pondra")
    ap.add_argument("--data", default=f"{S}/sf10")
    ap.add_argument("--lake", default=f"{S}/lake")
    ap.add_argument("--queries", default=f"{S}/queries.json")
    ap.add_argument("--cap", type=float, default=2.0)
    ap.add_argument("--nodes", type=int, default=1)
    ap.add_argument("--spread", type=int, default=0)
    ap.add_argument("--port", type=int, default=7700)
    ap.add_argument("--timeout", type=float, default=1200)
    ap.add_argument("--only", default="")
    ap.add_argument("--load", action="store_true")
    ap.add_argument("--env", nargs="*", default=[])
    ap.add_argument("--out", default=f"{S}/results/pondra.jsonl")
    A = ap.parse_args()
    main()
