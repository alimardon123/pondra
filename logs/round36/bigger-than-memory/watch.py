"""One node under a hard memory cap, heavy queries back to back: each one's seconds, peak RSS and answer.

  CAPGB=2 python3 watch.py BIN LAKE 'SQL' [SLEEP3] 'SQL' …

`SLEEPn` waits n seconds and prints the node's RSS then. The node runs in a cgroup (v1) of its own, as pbig.py's do.
"""
import os, shutil, subprocess, sys, threading, time, urllib.error, urllib.request

S = os.path.dirname(os.path.abspath(__file__))
CG = "/sys/fs/cgroup/memory" + next(l.split(":", 2)[2].strip() for l in open("/proc/self/cgroup") if ":memory:" in l) + "/bigmem-p0"
os.makedirs(CG, exist_ok=True)
open(f"{CG}/memory.limit_in_bytes", "w").write(str(int(float(os.environ.get("CAPGB", "2")) * (1 << 30))))
KEY = "bigger-than-memory-owner-key-0001"
sp = f"{S}/spill/watch"
shutil.rmtree(sp, ignore_errors=True)
os.makedirs(sp + "/cache")
env = {**os.environ, "TMPDIR": sp, "PONDRA_OWNER_KEY": KEY, "PONDRA_CACHE_DIR": sp + "/cache"}
os.makedirs(f"{S}/results", exist_ok=True)
p = subprocess.Popen([sys.argv[1], "serve", "--dir", sys.argv[2], "--addr", "127.0.0.1:7760"], env=env, stdout=subprocess.DEVNULL,
                     stderr=open(f"{S}/results/watch.stderr", "w"), preexec_fn=lambda: open(f"{CG}/cgroup.procs", "w").write(str(os.getpid())))
peak = [0]


def rss():
    return int(open(f"/proc/{p.pid}/statm").read().split()[1]) * 4096 >> 20


def sample():
    while p.poll() is None:
        try:
            peak[0] = max(peak[0], rss())
        except Exception:
            pass
        time.sleep(0.05)


def q(sql):
    peak[0] = 0
    r = urllib.request.Request("http://127.0.0.1:7760/sql", data=sql.encode(), method="POST", headers={"x-pondra-owner": KEY})
    t = time.time()
    try:
        with urllib.request.urlopen(r, timeout=300) as x:
            out = x.read()[:300]
    except urllib.error.HTTPError as e:
        out = (e.headers.get("x-pondra-sqlstate"), e.read()[:200])
    except Exception as e:
        out = repr(e)
    print(round(time.time() - t, 2), "peak", peak[0], "MB", sql[:90], "->", out, flush=True)


for _ in range(100):
    try:
        urllib.request.urlopen(urllib.request.Request("http://127.0.0.1:7760/stats", headers={"x-pondra-owner": KEY}), timeout=2)
        break
    except Exception:
        time.sleep(0.2)
threading.Thread(target=sample, daemon=True).start()
for s in sys.argv[3:]:
    if s.startswith("SLEEP"):
        time.sleep(float(s[5:]))
        print("rss after sleep", rss(), "MB", flush=True)
        continue
    q(s)
p.terminate()
p.wait()
