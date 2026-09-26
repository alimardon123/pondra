# Reads of a 10M-row table as it changes, 8 runs each (round 19; hot columns off: PONDRA_HOT_GB=0).
# Output: change-reads.txt. Run from the repository root.
import subprocess, time, json, urllib.request, os, shutil, sys
BIN=os.environ.get("PONDRA_BIN", "target/release/pondra"); lake=os.path.abspath("/tmp/pondra-rdlake")
shutil.rmtree(lake, ignore_errors=True)
p=subprocess.Popen([BIN,"serve","--dir",lake,"--addr","127.0.0.1:7850","--tier-secs","3600"], stderr=subprocess.DEVNULL, env={**os.environ,"PONDRA_PURGE_ROWS":str(10**12),"PONDRA_HOT_GB":"0"})
time.sleep(1.5)
def q(s):
    r=urllib.request.urlopen(urllib.request.Request("http://127.0.0.1:7850/sql", data=f"{s} -- {time.time()}".encode()), timeout=600).read()
    return json.loads(r) if r[:1] in b"[{" else r
def t(s, n=8):
    xs=[]
    for _ in range(n):
        a=time.time(); q(s); xs.append(round(time.time()-a,3))
    return xs
try:
    q("CREATE TABLE t (id BIGINT, k BIGINT, v DOUBLE, s VARCHAR)")
    for i in range(0, 10_000_000, 2_000_000):
        q(f"INSERT INTO t SELECT value + {i}, value % 1000, value * 0.5, 'row ' || (value % 97) FROM generate_series(1, 2000000)")
    S="SELECT count(*) AS n, sum(v) AS s FROM t"
    print("before", t(S))
    q("UPDATE t SET v = v + 1 WHERE id = 12345")
    print("1 row changed, in log", t(S))
    urllib.request.urlopen(urllib.request.Request("http://127.0.0.1:7850/tier", data=b""), timeout=600).read()
    print("tiered", t(S))
    print(q("CHECKPOINT"))
    print("purged", t(S))
    q("UPDATE t SET v = v + 1 WHERE id % 100 = 1")
    print("1% changed everywhere, in log", t(S))
    urllib.request.urlopen(urllib.request.Request("http://127.0.0.1:7850/tier", data=b""), timeout=600).read()
    print("tiered", t(S))
    print(q("CHECKPOINT"))
    print("purged", t(S))
    print(len(os.listdir(os.path.join(lake, "data/t"))))
finally:
    p.kill()
