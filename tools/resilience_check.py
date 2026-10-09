#!/usr/bin/env python3
"""Pondra under failure, each way it runs, with writers and readers going the whole time. Every
part checks the same three things through its faults: every acknowledged batch is there exactly
once on every node, no read was torn or went back in time, and the cluster takes writes again
once the fault is gone (how long it took is in `info`).

- **storage**: three nodes on a bucket, each through its own `faulty_s3.py`: requests answered
  500 and 503, PUTs applied and their replies lost, requests slowed and held, the bucket down for
  longer than a leader's lease.
- **cutoff**: the leader alone cut off from the bucket while the others reach it: another leads.
- **clients**: the Python and JavaScript clients given every node's address, through a leader
  killed, a follower killed, a leader killed and back: no call fails; a transaction keeps to its
  node.
- **doors**: psycopg (a DSN of every host) and an idempotent librdkafka producer through a leader
  and a follower killed: every committed INSERT and every delivered record there once.
- **flight**: pyarrow's Flight client through a leader and a follower killed: DoPut batches
  acknowledged once, DoGet reads never torn, and the table's log followed as a stream, resumed on
  another node, seeing every batch once.
- **disk**: three nodes on a lake on local disk, and the disk full: reads go on, writes wait, and
  every one lands once when there is room.
- **cache**: three nodes on a bucket (replicated acks), their cache disk full: nothing stops.
- **server**: `pondra serve --lakes`, a database's node killed and the server killed under the
  Python client and psycopg.
- **cli**: `pondra sql` killed mid-INSERT, on local disk and a bucket, with a node and without.

  resilience_check.py [storage] [cutoff] [clients] [doors] [flight] [disk] [cache] [server] [cli] [--secs 20]
  resilience_check.py --real [storage cutoff]   # the same faults in front of a real bucket (R2, S3)

Needs moto's server, psycopg, confluent-kafka and pyarrow (tools/requirements.txt), Node.js for the
JavaScript client, and root or sudo for the small disks (tmpfs); prints ok/FAIL for each check
and a JSON summary at the end; exit 1 on a failure.
"""
import argparse, json, os, random, shutil, subprocess, sys, tempfile, threading, time
from collections import defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import harness
from harness import Node, call
from faulty_s3 import Faulty

checks = {}
A, PART = None, ""


def check(name, ok, detail=None):
    checks[f"{PART}: {name}"] = bool(ok)
    print(f"{'ok  ' if ok else 'FAIL'} {name}" + (f": {detail}" if detail is not None and not ok else ""), flush=True)
    return ok


class Info(dict):
    """`info`, each key under its part's name."""

    def __setitem__(self, k, v):
        super().__setitem__(f"{PART}: {k}", v)


info = Info()


def until(what, secs, every=0.25):
    """`what()` once it is truthy, or None after `secs`."""
    deadline = time.time() + secs
    while time.time() < deadline:
        try:
            if got := what():
                return got
        except Exception:
            pass
        time.sleep(every)
    return None


def stats(nd):
    try:
        return call(nd.port, "GET", "/stats", timeout=3)
    except Exception:
        return None


def one_leader(nodes):
    """The leading node, when every node is up and follows it."""
    s = [stats(nd) for nd in nodes]
    leads = [nd for nd, x in zip(nodes, s) if x and x["role"] == "leader"]
    return leads[0] if len(leads) == 1 and all(x and x["leader"] == s[nodes.index(leads[0])]["leader"] for x in s) else None


class Load:
    """Writers sending numbered batches to any node, each retried until acknowledged (the same
    producer and seq: applied once), and readers checking every answer is a whole prefix of each
    writer's batches that never shrinks on a node."""

    Q = "SELECT producer, count(*) AS n, count(DISTINCT seq) AS d, max(seq) AS mx FROM events GROUP BY producer"

    def __init__(self, nodes, writers=8, readers=4, size=50):
        self.nodes, self.size, self.stop, self.phase, self.t0 = nodes, size, threading.Event(), "no faults", time.time()
        self.acked, self.acks, self.bad, self.errors = defaultdict(int), [], [], defaultdict(int)
        self.reads = []  # (node, started, finished, {producer: batches}): checked for going back at the end
        self.threads = [threading.Thread(target=self.writer, args=(f"w{w}",), daemon=True) for w in range(writers)]
        self.threads += [threading.Thread(target=self.reader, daemon=True) for _ in range(readers)]

    def start(self):
        [t.start() for t in self.threads]
        return self

    def writer(self, name):
        seq = 1
        while not self.stop.is_set():
            body = "".join(f'{{"producer":"{name}","seq":{seq},"i":{i}}}\n' for i in range(self.size)).encode()
            nd = random.choice(self.nodes)
            try:
                call(nd.port, "POST", f"/append/events?producer={name}&seq={seq}", body, timeout=20)
                self.acked[name] = seq
                self.acks.append(time.time())
                seq += 1
            except Exception as e:
                self.errors[type(e).__name__] += 1
                time.sleep(0.2)

    def reader(self):
        while not self.stop.is_set():
            nd, started = random.choice(self.nodes), time.time()
            try:
                res = self.ask(nd)
            except Exception:
                time.sleep(0.2)
                continue
            when = (self.phase, round(time.time() - self.t0, 1))
            for r in res:
                if r["n"] != r["mx"] * self.size or r["d"] != r["mx"]:
                    self.bad.append(("torn", nd.port, when, r))
            self.reads.append((nd.port, started, time.time(), {r["producer"]: r["mx"] for r in res}, when))

    def ask(self, nd):
        return call(nd.port, "POST", "/sql", self.Q.encode(), timeout=20)

    def went_back(self):
        """Reads on a node that showed fewer batches than a read there that had finished before
        they started (reads at once may finish in either order)."""
        back = []
        for port in {r[0] for r in self.reads}:
            done = sorted((r for r in self.reads if r[0] == port), key=lambda r: r[2])
            best, i = defaultdict(int), 0  # the most batches shown by reads finished so far
            for r in sorted((r for r in self.reads if r[0] == port), key=lambda r: r[1]):
                while i < len(done) and done[i][2] < r[1]:
                    for p, mx in done[i][3].items():
                        best[p] = max(best[p], mx)
                    i += 1
                back += [("went back", port, r[4], p, mx, best[p]) for p, mx in r[3].items() if mx < best[p]]
        return back

    def since(self, t):
        return sum(1 for a in list(self.acks) if a >= t)

    def stall(self, t0, t1):
        """The longest time between acknowledgements within [t0, t1]."""
        marks = [t0] + [a for a in list(self.acks) if t0 <= a <= t1] + [t1]
        return round(max(b - a for a, b in zip(marks, marks[1:])), 1)

    def finish(self):
        self.stop.set()
        [t.join(30) for t in self.threads]

    def exactly_once(self, where):
        """Every acknowledged batch once, nothing else, read on every node."""
        wrong = {}
        for nd in self.nodes:
            got = {r["producer"]: r for r in call(nd.port, "POST", "/sql", self.Q.encode(), timeout=60)}
            for p, seq in self.acked.items():
                r = got.get(p, {"n": 0, "d": 0, "mx": 0})
                if r["d"] != r["mx"] or r["n"] != r["mx"] * self.size or r["mx"] < seq:
                    wrong[f"{nd.port}/{p}"] = {**r, "acked": seq}
        return check(f"{where}: every acknowledged batch is there once, on every node", not wrong, dict(list(wrong.items())[:6]))


def bucket():
    """moto's S3 (tools/sim_r2.py, no added latency) and a bucket in it, for harness.new_lake; or,
    with `--real`, the bucket the environment names (nothing started)."""
    if A.real:
        return None, os.environ["AWS_ENDPOINT"], {"AWS_ALLOW_HTTP": "true"}
    port = 9300 + os.getpid() % 500
    p = subprocess.Popen([sys.executable, os.path.join(HERE, "sim_r2.py"), "--port", str(port), "--zero"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    import boto3
    s3 = boto3.client("s3", endpoint_url=f"http://127.0.0.1:{port}", region_name="us-east-1", aws_access_key_id="test", aws_secret_access_key="test")
    until(lambda: s3.list_buckets() is not None, 30)
    s3.create_bucket(Bucket="resilience")
    env = {"AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test", "AWS_REGION": "us-east-1", "AWS_ALLOW_HTTP": "true", "PONDRA_BUCKET": "resilience"}
    os.environ.update({**env, "AWS_ENDPOINT": f"http://127.0.0.1:{port}"})
    return p, f"http://127.0.0.1:{port}", env


class Timeline:
    """Each node's role and term as they change, from /stats twice a second: when a fault's
    takeover happened, step by step (`info`)."""

    def __init__(self, nodes):
        self.nodes, self.t0, self.seen, self.events, self.stop = nodes, time.time(), {}, [], threading.Event()
        threading.Thread(target=self.watch, daemon=True).start()

    def watch(self):
        while not self.stop.is_set():
            for nd in self.nodes:
                st = stats(nd)
                now = (st["role"], st["term"]) if st else ("down", None)
                if self.seen.get(nd.port) != now:
                    self.seen[nd.port] = now
                    self.events.append((round(time.time() - self.t0, 1), nd.port, *now))
            time.sleep(0.5)

    def since(self, t):
        """The changes since `t`, timed from it."""
        return [f"{round(e[0] - (t - self.t0), 1)} s: {e[1]} {e[2]}" + (f" (term {e[3]})" if e[3] else "") for e in self.events if e[0] >= t - self.t0]


class Bucketed:
    """Three nodes on a bucket, each through a proxy of its own that fails as it's told to, with
    writers and readers going (`Load`) and their roles watched (`Timeline`)."""

    def __init__(self, a, port=None, env=None, **flags):
        self.sim, upstream, base = bucket()
        harness.A = argparse.Namespace(s3=True, keep=a.keep)
        self.lake = lake = harness.new_lake()
        keys = (os.environ["AWS_ACCESS_KEY_ID"], os.environ["AWS_SECRET_ACCESS_KEY"], os.environ.get("AWS_REGION", "auto")) if A.real else None
        self.proxies = [Faulty(upstream, sign=keys) for _ in range(3)]  # (a real bucket: each request signed again for it)
        base = {**base, "PONDRA_TRACE_START": "1", **(env or {})}  # (a new leader's steps, in its log)
        each = lambda i: {k: v.format(i=i) if isinstance(v, str) else v for k, v in flags.items()}  # ("{i}": the node's number)
        # (AWS_ENDPOINT_URL too: object_store reads either, and CI sets it to the bucket for the AWS CLI)
        self.nodes = [Node(lake, (port or a.port) + i, env={**base, "AWS_ENDPOINT": self.proxies[i].url, "AWS_ENDPOINT_URL": self.proxies[i].url}, **each(i)).start() for i in range(3)]
        first = until(lambda: one_leader(self.nodes), 60)
        check("three nodes on the bucket, one leader", first)
        # (a node that went round its proxy would pass every check below untouched by any fault)
        check("every node reaches the bucket through its own proxy", all(p.counts for p in self.proxies), [p.counts for p in self.proxies])
        until(lambda: call(first.port, "POST", "/tables/events", json.dumps([["producer", "Utf8"], ["seq", "Int64"], ["i", "Int64"]]).encode()), 30)
        until(lambda: all(call(nd.port, "POST", "/sql", b"SELECT count(*) FROM events") is not None for nd in self.nodes), 30)
        self.timeline = Timeline(self.nodes)
        self.load = Load(self.nodes).start()

    def phase(self, name, secs, **faults):
        """Every node's bucket failing as `faults` say for `secs`, then well again."""
        self.load.phase = name
        for p in self.proxies:
            p.set(**faults)
        t0 = time.time()
        time.sleep(secs)
        for p in self.proxies:
            p.set()
        self.load.phase = f"after {name}"
        info[f"{name}: acks a second"] = round(self.load.since(t0) / secs, 1)
        info[f"{name}: the longest wait for an ack (s)"] = self.load.stall(t0, time.time())
        return t0

    def end(self, settle):
        """The faults gone: every node in one cluster again, then the three checks."""
        nodes, load = self.nodes, self.load
        load.phase = "settling"
        settled = until(lambda: one_leader(nodes), 90)
        check("the bucket whole again: every node back in one cluster", settled, {nd.port: stats(nd) for nd in nodes})
        time.sleep(settle)
        load.finish()
        self.timeline.stop.set()
        info["errors the writers saw"] = dict(load.errors)
        info["faults drawn"] = [p.counts for p in self.proxies]
        bad = load.bad + load.went_back()
        info["reads checked"] = len(load.reads)
        check("no read was torn or went back in time, through every fault", not bad, bad[:6])
        load.exactly_once("after every fault")
        check("every node is still running (or restarted itself)", all(nd.alive() for nd in nodes), [nd.log for nd in nodes if not nd.alive()])
        t0, left = time.time(), []  # (how the log drained: seconds, rows still in it)

        def drained():
            st = stats(one_leader(nodes))
            n = st and st.get("untiered_rows")
            if n is not None and (not left or left[-1][1] != n):
                left.append((round(time.time() - t0, 1), n))
            return n == 0
        # A tiering job that began while the bucket failed may still be retrying a request: the
        # store retries one for up to 180 s (object_store's retry_timeout), and a PUT whose reply
        # was lost then fails as "already exists", so the round runs again. 90 s failed now and
        # then (the leader's log: a job's PUT failing after 120-170 s); a log that stops draining
        # stays undrained however long we wait.
        ok = until(drained, 240)
        info["the log after the faults (s, rows still in it)"] = left[:3] + ["…"] + left[-6:] if len(left) > 10 else left
        check("the log drains into files once the bucket is well", ok, stats(one_leader(nodes) or nodes[0]))

    def close(self):
        self.timeline.stop.set()
        self.load.stop.set()
        for nd in self.nodes:
            nd.kill()
        [p.close() for p in self.proxies]
        if self.sim:
            self.sim.kill()
            harness.LAKES.remove(self.lake)  # (moto's bucket was in its memory: gone with it)


def storage(a):
    """Every node's bucket failing alike: errors, lost replies, slowness, then gone for longer than
    a leader's lease."""
    c = Bucketed(a)
    try:
        s, load = a.secs, c.load
        time.sleep(s / 2)
        info["acks a second, no faults"] = round(load.since(time.time() - s / 2) / (s / 2))
        t0 = c.phase("errors", s, error=0.1, lost=0.05)
        check("10% of requests answered 500 or 503 and 5% applied with their replies lost: writes go on", load.since(t0) and load.stall(t0, time.time()) < 15)
        t0 = c.phase("slow", s, slow_ms=250, hang=0.02, hang_secs=20)
        # (a request held at the phase's start holds the commits behind it for as long as the phase
        # lasts: the next ack may come just after it, so wait a moment for one before judging)
        until(lambda: load.since(t0), 15)
        check("every request 250 ms slower, 2% held for 20 s: writes go on", load.since(t0) and load.stall(t0, time.time()) < 30)
        before = until(lambda: one_leader(c.nodes), 60)
        before = before and (before.port, stats(before)["term"])
        t0 = c.phase("down", 40, down=True)
        back = time.time()
        again = until(lambda: load.since(back), 90)
        info["the bucket back after 40 s down: the first ack (s)"] = round(time.time() - back, 1) if again else None
        check("the bucket down 40 s (past every lease): writes again within 30 s of its return", again and time.time() - back < 30)
        c.end(s / 2)
        after = one_leader(c.nodes)
        info["the bucket down and back: roles"] = roles = c.timeline.since(t0)
        check("…the same leader leads after it (down for everyone: nobody had to take over)", after and (after.port, stats(after)["term"]) == before, roles)
    finally:
        c.close()


def cutoff(a):
    """The leader alone cut off from the bucket, the others not: one of them leads."""
    c = Bucketed(a)
    try:
        nodes, load = c.nodes, c.load
        time.sleep(a.secs / 2)
        lead = until(lambda: one_leader(nodes), 60)
        alone = nodes.index(lead)
        c.proxies[alone].set(down=True)
        t0, load.phase = time.time(), "the leader cut off"
        others = [n for n in nodes if n is not lead]
        moved = until(lambda: load.since(t0 + 1) and one_leader(others), 90)
        took = round(time.time() - t0, 1)
        info["the leader cut off from the bucket: writes again through another leader (s)"] = took if moved else None
        info["the leader cut off: roles"] = c.timeline.since(t0)
        check("the leader alone cut off from the bucket: another leads and takes writes within 40 s", moved and took < 40,
              {nd.port: (st or {}).get("role") for nd, st in ((nd, stats(nd)) for nd in nodes)})
        # (its followers beat to the new leader now: a lease on, it looked alone to itself, and held
        # every write sent to it until the client gave up: no ack anywhere for 17 s on a slow takeover)
        time.sleep(6)
        t1, rows = time.time(), "".join(f'{{"producer":"probe","seq":1,"i":{i}}}\n' for i in range(load.size)).encode()
        try:
            answer = call(lead.port, "POST", "/append/events?producer=probe&seq=1", rows, timeout=20) and "taken"
        except Exception as e:
            answer = f"{type(e).__name__}: {str(e)[:60]}"
        check("…and the cut-off leader turns writes away once its followers follow another, never holds them",
              answer.startswith("RuntimeError: 503") and time.time() - t1 < 5, (answer, round(time.time() - t1, 1)))
        time.sleep(a.secs / 2)
        c.proxies[alone].set()
        t1 = time.time()
        c.end(a.secs / 2)
        info["the cut-off leader back: roles"] = c.timeline.since(t1)
    finally:
        c.close()


def small_disk(mb):
    """A disk of `mb` MB (tmpfs) on a new folder, or None where this machine can't mount one
    (neither root nor sudo without a password)."""
    path = tempfile.mkdtemp(prefix="pondra-disk-")
    cmd = ["mount", "-t", "tmpfs", "-o", f"size={mb}m,mode=1777", "tmpfs", path]
    if subprocess.run(cmd if os.geteuid() == 0 else ["sudo", "-n", *cmd], capture_output=True).returncode == 0:
        return path
    os.rmdir(path)
    return None


def unmount(path):
    cmd = ["umount", "-l", path]
    subprocess.run(cmd if os.geteuid() == 0 else ["sudo", "-n", *cmd], capture_output=True)
    shutil.rmtree(path, ignore_errors=True)


def fill(path):
    """The disk at `path` full to its last byte, by a file `filler` (removed to make room again)."""
    fd = os.open(os.path.join(path, "filler"), os.O_WRONLY | os.O_CREAT | os.O_APPEND)
    for size in (1 << 20, 4096, 1):
        try:
            while os.write(fd, b"\0" * size):
                pass
        except OSError:
            pass
    os.close(fd)


EVENTS = json.dumps([["producer", "Utf8"], ["seq", "Int64"], ["i", "Int64"]]).encode()


def disk(a):
    """Three nodes on a lake on local disk, and the disk full: reads go on, writes wait (nothing
    is acknowledged that isn't kept), and once there is room again every write lands once and
    the log drains into files."""
    # (64 MB: a lake on local disk clears its catalog's WAL every 5 s and the files its compactor
    # replaced a minute on, and its writes wait while less than a quarter of the disk is free, so
    # the catalog can flush and tiering write its files. It kept two minutes of WAL and those files
    # 15 minutes, and wrote until the disk was full: it filled this disk again on its own once the
    # filler went (one CI run in five), and with no room to flush the catalog or write a file,
    # nothing could clear it. On 32 MB it recovers too, its writes waiting a minute or two.)
    mnt, nodes = small_disk(64), []
    if not check("a 64 MB disk for the lake", mnt, "can't mount one: needs root, or sudo without a password"):
        return
    try:
        nodes = [Node(os.path.join(mnt, "lake"), a.port + 50 + i).start() for i in range(3)]
        first = until(lambda: one_leader(nodes), 60)
        check("three nodes on it, one leader", first)
        until(lambda: call(first.port, "POST", "/tables/events", EVENTS), 30)
        load = Load(nodes).start()
        time.sleep(a.secs / 2)
        load.phase, reads = "the disk full", len(load.reads)
        fill(mnt)
        t0 = time.time()
        time.sleep(a.secs)
        info["the disk full: acks a second"] = round(load.since(t0) / (time.time() - t0), 1)
        info["the disk full: reads answered"] = len(load.reads) - reads
        check("the disk full: reads go on", len(load.reads) > reads)
        os.remove(os.path.join(mnt, "filler"))
        back, load.phase = time.time(), "room again"
        again = until(lambda: load.since(back), 60)
        info["room again: the first ack (s)"] = round(time.time() - back, 1) if again else None
        check("room again: writes go on within 10 s", again and time.time() - back < 10)
        time.sleep(a.secs / 2)
        load.finish()
        info["errors the writers saw"] = dict(load.errors)
        bad = load.bad + load.went_back()
        check("no read was torn or went back in time", not bad, bad[:6])
        load.exactly_once("after the disk filled up")
        check("every node is still running", all(nd.alive() for nd in nodes), [nd.log for nd in nodes if not nd.alive()])
        drained = until(lambda: (st := stats(one_leader(nodes))) and st.get("untiered_rows") == 0, 90)
        check("the log drains into files once there is room", drained, stats(one_leader(nodes) or nodes[0]))
    finally:
        for nd in nodes:
            nd.kill()
        unmount(mnt)


def cache(a):
    """Three nodes on a bucket with replicated acks, their caches (the SSD tier, replicas, a
    shuffle's scratch: `--cache-dir`) on one small disk, and that disk full: writes and reads go
    on (a replica a follower can't keep is made durable instead), a query spread over the nodes
    is answered as on one node, a node restarted meanwhile comes up; once there is room,
    everything acknowledged is there once."""
    mnt = small_disk(48)
    if not check("a 48 MB disk for the nodes' caches", mnt, "can't mount one: needs root, or sudo without a password"):
        return
    c = None
    try:
        c = Bucketed(a, port=a.port + 60, env={"PONDRA_SPILL_MB": "1"}, cache_dir=os.path.join(mnt, "n{i}"), ack="replicated", replicas=2)
        nodes, load = c.nodes, c.load
        lead = one_leader(nodes)
        call(lead.port, "POST", "/sql", b"CREATE TABLE fixed AS SELECT value AS k, value * 2 AS v FROM generate_series(1, 3000000)", timeout=120)
        Q = b"SELECT count(*) AS n, sum(x.v) AS s FROM fixed x JOIN fixed y ON x.k = y.k"
        alone = call(lead.port, "POST", "/sql", Q, timeout=120)
        time.sleep(a.secs / 2)
        load.phase, reads = "the cache disk full", len(load.reads)
        fill(mnt)
        t0 = time.time()
        time.sleep(a.secs)
        info["the cache disk full: acks a second"] = round(load.since(t0) / (time.time() - t0), 1)
        info["the cache disk full: the longest wait for an ack (s)"] = load.stall(t0, time.time())
        check("the nodes' cache disk full: writes and reads go on", load.since(t0) and load.stall(t0, time.time()) < 5 and len(load.reads) > reads)
        try:
            spread = call(nodes[1].port, "POST", "/sql?spread=1", Q, timeout=180)
        except Exception as e:
            spread = repr(e)[:300]
        info["…a query spread over the nodes, spilling to the full disk"] = spread if spread != alone else "answered as on one node"
        check("…a query spread over the nodes is answered as on one node, every node up", spread == alone and all(nd.alive() for nd in nodes), spread)
        follower = next(nd for nd in nodes if nd is not one_leader(nodes))
        follower.kill()
        follower.start()
        rejoined = until(lambda: one_leader(nodes) and call(follower.port, "POST", "/sql", b"SELECT count(*) AS n FROM fixed")[0]["n"] == 3000000, 60)
        check("…a node restarted while its cache disk is full comes up and answers", rejoined)
        os.remove(os.path.join(mnt, "filler"))
        c.end(a.secs / 2)
    finally:
        if c:
            c.close()
        unmount(mnt)


JS = """
import { connect } from %s;
const [urls, size] = [process.argv[1].split(","), Number(process.argv[2])];
const db = connect(urls, { onNotice: null });
let [seq, failed, open] = [0, null, true];
process.stdin.on("data", () => {}).on("end", () => (open = false));
while (open) {
  try {
    await db.append("events", Array.from({ length: size }, (_, i) => ({ producer: "js", seq: seq + 1, i })));
    seq++;
  } catch (e) { failed = String(e); break; }
}
console.log(JSON.stringify({ acked: seq, failed }));
process.exit(0);
"""


def clients(a):
    """The Python and JavaScript clients given every node's address, writing and reading while the
    leader is killed (and stays down past its lease), a follower is killed, and the new leader is
    killed and comes straight back: no call fails, nothing is lost or doubled. A transaction keeps
    to its node, and when that node goes the next statement says so (08006) and nothing of it is
    applied."""
    sys.path.insert(0, os.path.join(os.path.dirname(HERE), "python"))
    import pondra
    harness.A = argparse.Namespace(s3=False, keep=a.keep)
    lake = harness.new_lake()
    nodes = [Node(lake, a.port + 10 + i).start() for i in range(3)]
    urls = [f"http://127.0.0.1:{nd.port}" for nd in nodes]
    size, stop, acked, failed, reads, torn = 20, threading.Event(), {}, [], [0], []
    js = None
    try:
        check("three nodes, one leader", until(lambda: one_leader(nodes), 60))
        pondra.connect(urls, echo=False).sql("CREATE TABLE events (producer VARCHAR, seq BIGINT, i BIGINT)")

        def writer(name, insert):
            con = pondra.connect(urls[hash(name) % 3:] + urls[:hash(name) % 3], echo=False)
            seq = 0
            while not stop.is_set():
                try:
                    if insert:  # (one INSERT: the client sends it with a job, so a second sending is applied once)
                        con.sql(f"INSERT INTO events VALUES " + ", ".join(f"('{name}', {seq + 1}, {i})" for i in range(size)))
                    else:
                        con.append("events", [{"producer": name, "seq": seq + 1, "i": i} for i in range(size)])
                    seq += 1
                    acked[name] = seq
                except Exception as e:
                    failed.append((name, repr(e)[:400]))
                    return

        def reader():
            con = pondra.connect(urls, echo=False)
            while not stop.is_set():
                try:
                    for r in con.sql(Load.Q).rows():
                        if r["n"] != r["mx"] * size or r["d"] != r["mx"]:
                            torn.append(r)
                    reads[0] += 1
                except Exception as e:
                    failed.append(("reader", repr(e)[:400]))
                    return

        threads = [threading.Thread(target=writer, args=(f"py{w}", w % 2 == 1), daemon=True) for w in range(6)] + [threading.Thread(target=reader, daemon=True) for _ in range(2)]
        [t.start() for t in threads]
        if shutil.which("node"):
            here = json.dumps("file://" + os.path.join(os.path.dirname(HERE), "js", "index.js"))
            js = subprocess.Popen(["node", "--input-type=module", "-e", JS % here, ",".join(urls), str(size)], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)

        def going(what, secs=30):
            before, t0 = sum(acked.values()), time.time()
            ok = until(lambda: sum(acked.values()) > before + 6, secs)
            info[f"{what}: writes again after (s)"] = round(time.time() - t0, 1) if ok else None
            return check(f"{what}: every writer's calls go on", ok and not failed, failed[:3])

        time.sleep(a.secs / 2)
        lead = one_leader(nodes)
        lead.kill()
        going("the leader killed, down past its lease")
        time.sleep(6)
        lead.start()
        check("the killed leader back, as a follower", until(lambda: one_leader(nodes), 60))
        follower = next(nd for nd in nodes if nd is not one_leader(nodes))
        follower.kill()
        going("a follower killed")
        follower.start()
        until(lambda: one_leader(nodes), 60)
        lead = one_leader(nodes)
        lead.kill()
        lead.start()
        going("the new leader killed and straight back")
        until(lambda: one_leader(nodes), 60)
        time.sleep(a.secs / 2)
        stop.set()
        [t.join(90) for t in threads]
        if js:
            out = json.loads(js.communicate(timeout=90)[0].strip().splitlines()[-1])  # (its input closed: it stops)
            acked["js"] = out["acked"]
            check("the JavaScript client's appends went on through it all", not out["failed"] and out["acked"] > 0, out)
        info["calls acknowledged"] = dict(acked)
        info["reads"] = reads[0]
        check("no call failed, no read was torn", not failed and not torn, (failed + torn)[:4])
        wrong = {}
        for nd in nodes:
            got = {r["producer"]: r for r in call(nd.port, "POST", "/sql", Load.Q.encode(), timeout=60)}
            for p, seq in acked.items():
                r = got.get(p, {"n": 0, "d": 0, "mx": 0})
                if r["d"] != r["mx"] or r["n"] != r["mx"] * size or r["mx"] < seq:
                    wrong[f"{nd.port}/{p}"] = {**r, "acked": seq}
        check("every acknowledged append and INSERT is there once, on every node", not wrong, wrong)

        con = pondra.connect(urls, echo=False)
        con.sql("BEGIN")
        con.sql("INSERT INTO events VALUES ('txn', 1, 0)")
        held = con._held
        check("a transaction keeps to its node", held in urls, held)
        holder = nodes[urls.index(held)] if held in urls else nodes[0]
        holder.kill()
        try:
            con.sql("INSERT INTO events VALUES ('txn', 2, 0)")
            said = None
        except pondra.PondraError as e:
            said = e.sqlstate
        check("its node killed, the next statement says the transaction is gone (08006)", said == "08006", said)
        n = con.sql("SELECT count(*) AS n FROM events WHERE producer = 'txn'").rows()[0]["n"]
        check("…nothing of it applied, and the connection goes on with another node", n == 0, n)
        holder.start()
    finally:
        stop.set()
        if js and js.poll() is None:
            js.kill()
        for nd in nodes:
            nd.kill()


def doors(a):
    """Postgres and Kafka clients given every node, through the leader killed (down past its
    lease) and a follower killed: psycopg with a DSN of every host reconnects to a live node, every
    INSERT it was told committed is there once (one whose answer was lost, at most once: Postgres's
    rule too); an idempotent librdkafka producer bootstrapped with every node delivers every record
    exactly once, and a consumer group reads them all back."""
    import psycopg, confluent_kafka as ck
    harness.A = argparse.Namespace(s3=False, keep=a.keep)
    lake, base = harness.new_lake(), a.port + 20
    pgs, kafkas = [base + 10 + i for i in range(3)], [base + 20 + i for i in range(3)]
    nodes = [Node(lake, base + i, pg=f"127.0.0.1:{pgs[i]}", kafka=f"127.0.0.1:{kafkas[i]}").start() for i in range(3)]
    dsn = f"host={','.join(['127.0.0.1'] * 3)} port={','.join(map(str, pgs))} user=u dbname=lake connect_timeout=3"
    stop, acked, unknown, failed, reconnects, said = threading.Event(), defaultdict(list), defaultdict(list), [], [], defaultdict(int)
    try:
        check("three nodes, one leader", until(lambda: one_leader(nodes), 60))
        call(base, "POST", "/sql", b"CREATE TABLE pgt (w VARCHAR, seq BIGINT)")
        call(base, "POST", "/sql", b"CREATE TABLE kev (k BIGINT)")
        call(base, "POST", "/sql", b"CREATE TABLE kev2 (k BIGINT)")

        def pg_writer(name):
            con, seq = None, 0
            while not stop.is_set():
                seq += 1
                try:
                    if con is None:
                        t0 = time.time()
                        con = until(lambda: psycopg.connect(dsn, autocommit=True), 60, every=0.2)
                        if con is None:
                            failed.append((name, "no node took a connection for 60 s"))
                            return
                        reconnects.append(round(time.time() - t0, 1))
                    con.execute("INSERT INTO pgt VALUES (%s, %s)", (name, seq))
                    acked[name].append(seq)
                except psycopg.Error as e:
                    unknown[name].append(seq)  # (refused, or its answer lost: applied or not, a client can't tell)
                    said[getattr(e, "sqlstate", None) or type(e).__name__] += 1
                    try:
                        con.close()
                    except Exception:
                        pass
                    con = None

        writers = [threading.Thread(target=pg_writer, args=(f"pg{w}",), daemon=True) for w in range(3)]
        [t.start() for t in writers]
        delivered, errors = [], []
        producer = ck.Producer({"bootstrap.servers": ",".join(f"127.0.0.1:{k}" for k in kafkas), "enable.idempotence": True, "linger.ms": 5, "message.timeout.ms": 120000})
        report = lambda err, msg: errors.append(str(err)) if err else delivered.append(int(json.loads(msg.value())["k"]))
        sent = [0]

        def produce():
            while not stop.is_set():
                for _ in range(50):
                    producer.produce("kev", value=json.dumps({"k": sent[0]}), callback=report)
                    sent[0] += 1
                producer.poll(0)
                time.sleep(0.02)

        # (one whose records time out while the leader is gone: librdkafka gives up on them and
        # numbers the rest again from 0 under a new epoch of the same producer id)
        hasty = ck.Producer({"bootstrap.servers": ",".join(f"127.0.0.1:{k}" for k in kafkas), "enable.idempotence": True, "linger.ms": 5, "message.timeout.ms": 2500})
        h_ok, h_failed, h_sent = [], [], [0]
        h_report = lambda err, msg: (h_failed if err else h_ok).append(int(json.loads(msg.value())["k"]))

        def produce_hasty():
            while not stop.is_set():
                for _ in range(20):
                    hasty.produce("kev2", value=json.dumps({"k": h_sent[0]}), callback=h_report)
                    h_sent[0] += 1
                hasty.poll(0)
                time.sleep(0.02)

        kt = threading.Thread(target=produce, daemon=True)
        kt.start()
        writers.append(threading.Thread(target=produce_hasty, daemon=True))
        writers[-1].start()
        time.sleep(a.secs / 2)
        lead = one_leader(nodes)
        lead.kill()
        time.sleep(8)
        lead.start()
        until(lambda: one_leader(nodes), 60)
        follower = next(nd for nd in nodes if nd is not one_leader(nodes))
        follower.kill()
        time.sleep(3)
        follower.start()
        until(lambda: one_leader(nodes), 60)
        time.sleep(a.secs / 2)
        stop.set()
        [t.join(60) for t in writers + [kt]]
        left = producer.flush(120)
        hasty.flush(60)
        info["Postgres reconnects (s)"] = sorted(reconnects)[-6:]
        info["INSERTs acknowledged, refused or answers lost"] = {w: (len(acked[w]), len(unknown[w])) for w in acked}
        info["what psycopg said (SQLSTATE or error)"] = dict(said)
        check("psycopg reconnected to a live node every time", not failed, failed)
        got = defaultdict(lambda: defaultdict(int))
        for r in call(base, "POST", "/sql", b"SELECT w, seq, count(*) AS n FROM pgt GROUP BY w, seq", timeout=60):
            got[r["w"]][r["seq"]] = r["n"]
        wrong = [(w, s_, got[w][s_]) for w in acked for s_ in acked[w] if got[w][s_] != 1] + [(w, s_, got[w][s_]) for w in unknown for s_ in unknown[w] if got[w][s_] > 1]
        extra = [(w, s_) for w in got for s_ in got[w] if s_ not in acked[w] and s_ not in unknown[w]]
        check("every INSERT psycopg was told committed is there once; one whose answer was lost at most once", not wrong and not extra, (wrong + extra)[:6])
        info["Kafka records sent, delivered, failed"] = (sent[0], len(delivered), len(errors))
        check("the idempotent Kafka producer delivered every record through it all", left == 0 and not errors and len(delivered) == sent[0], (left, errors[:3]))
        rows = call(base, "POST", "/sql", b"SELECT count(*) AS n, count(DISTINCT k) AS d, min(k) AS lo, max(k) AS hi FROM kev", timeout=60)[0]
        check("…each of them in the table exactly once", rows == {"n": sent[0], "d": sent[0], "lo": 0, "hi": sent[0] - 1}, rows)
        info["Kafka records of a producer that gave up on some: sent, delivered, given up"] = (h_sent[0], len(h_ok), len(h_failed))
        rows = {r["k"]: r["n"] for r in call(base, "POST", "/sql", b"SELECT k, count(*) AS n FROM kev2 GROUP BY k", timeout=60)}
        wrong = [(k, rows.get(k)) for k in h_ok if rows.get(k) != 1] + [(k, rows[k]) for k in h_failed if rows.get(k, 0) > 1]
        check("a producer that gave up on records and began a new epoch: each delivered one there once, none twice", h_failed and not wrong and len(rows) <= h_sent[0], (len(h_failed), wrong[:5]))
        consumer = ck.Consumer({"bootstrap.servers": ",".join(f"127.0.0.1:{k}" for k in kafkas), "group.id": "resilience", "auto.offset.reset": "earliest", "enable.auto.commit": False})
        consumer.subscribe(["kev"])
        seen, deadline = set(), time.time() + 60
        while len(seen) < sent[0] and time.time() < deadline:
            for m in consumer.consume(1000, 1.0):
                if not m.error():
                    seen.add(int(json.loads(m.value())["k"]))
        consumer.close()
        check("a consumer group reads every record back", len(seen) == sent[0], (len(seen), sent[0]))
    finally:
        stop.set()
        for nd in nodes:
            nd.kill()


class Flown(Load):
    """`Load` over Arrow Flight: a writer DoPuts a stream of ten numbered batches to any node
    (`[table, producer, first seq]`: applied once) and, when the stream breaks, sends what wasn't
    acknowledged to another; readers ask with DoGet SQL."""

    def __init__(self, nodes, fports, **kw):
        import pyarrow as pa, pyarrow.flight as fl
        super().__init__(nodes, **kw)
        self.pa, self.fl, self.fports = pa, fl, dict(zip((nd.port for nd in nodes), fports))
        self.schema = pa.schema([("producer", pa.string()), ("seq", pa.int64()), ("i", pa.int64())])

    def client(self, nd):
        return self.fl.FlightClient(f"grpc://127.0.0.1:{self.fports[nd.port]}")

    def writer(self, name):
        pa, fl, seq = self.pa, self.fl, 1
        batch = lambda s: pa.record_batch([pa.array([name] * self.size), pa.array([s] * self.size, pa.int64()), pa.array(range(self.size), pa.int64())], schema=self.schema)
        while not self.stop.is_set():
            c = self.client(random.choice(self.nodes))
            try:
                w, r = c.do_put(fl.FlightDescriptor.for_path("events", name, str(seq)), self.schema, options=fl.FlightCallOptions(timeout=20))
                for s in range(seq, seq + 10):
                    w.write_batch(batch(s))
                w.done_writing()
                while (buf := r.read()) is not None:
                    if json.loads(buf.to_pybytes()).get("conflict"):
                        raise RuntimeError("an ack of a conflict")
                    self.acked[name] = seq
                    self.acks.append(time.time())
                    seq += 1
                w.close()
            except Exception as e:
                self.errors[type(e).__name__] += 1
                time.sleep(0.2)
            finally:
                c.close()

    def ask(self, nd):
        c = self.client(nd)
        try:
            q = self.fl.Ticket(json.dumps({"sql": self.Q}))
            return c.do_get(q, options=self.fl.FlightCallOptions(timeout=20)).read_all().to_pylist()
        finally:
            c.close()


def flight(a):
    """pyarrow's Flight client through the leader killed (down past its lease) and a follower
    killed: writers DoPut numbered batches to any node and send what wasn't acknowledged again
    elsewhere, readers count them with DoGet SQL, and a reader follows the table's log as a stream,
    resuming on another node from the last commit it saw whole. Every acknowledged batch is there
    once on every node, no read was torn or went back, and the stream saw every batch once."""
    import pyarrow.flight as fl
    harness.A = argparse.Namespace(s3=False, keep=a.keep)
    lake, base = harness.new_lake(), a.port + 60
    fports = [base + 10 + i for i in range(3)]
    nodes = [Node(lake, base + i, flight=f"127.0.0.1:{fports[i]}").start() for i in range(3)]
    load, stop, seen, resumes = None, threading.Event(), defaultdict(int), [0]
    streams = []

    def follow():
        """The log from its start: a commit's rows count once its `{"after": N}` comes; rows of a
        commit cut short come again from the next node."""
        after = 0
        while not stop.is_set():
            c = fl.FlightClient(f"grpc://127.0.0.1:{random.choice(fports)}")
            try:
                rd = c.do_get(fl.Ticket(json.dumps({"table": "events", "after": after, "columns": ["producer", "seq"]})))
                streams.append(rd)
                pending = []
                while not stop.is_set():
                    chunk = rd.read_chunk()
                    if chunk.data is not None and chunk.data.num_rows:
                        pending.append(chunk.data)
                    if chunk.app_metadata is not None:
                        after = json.loads(chunk.app_metadata.to_pybytes())["after"]
                        for b in pending:
                            for p, s in zip(b.column(0).to_pylist(), b.column(1).to_pylist()):
                                seen[(p, s)] += 1
                        pending = []
            except Exception:
                if not stop.is_set():
                    resumes[0] += 1
                    time.sleep(0.2)
            finally:
                c.close()

    try:
        check("three nodes, one leader", until(lambda: one_leader(nodes), 60))
        call(base, "POST", "/sql", b"CREATE TABLE events (producer VARCHAR, seq BIGINT, i BIGINT)")
        load = Flown(nodes, fports, writers=6, readers=3).start()
        follower = threading.Thread(target=follow, daemon=True)
        follower.start()
        time.sleep(a.secs / 2)
        lead = one_leader(nodes)
        t_kill = time.time()
        load.phase = "leader killed"
        lead.kill()
        time.sleep(8)
        lead.start()
        check("…the cluster leads again", until(lambda: one_leader(nodes), 60))
        info["acks while the leader was down (8 s)"] = load.since(t_kill) - load.since(t_kill + 8)
        load.phase = "follower killed"
        other = next(nd for nd in nodes if nd is not one_leader(nodes))
        other.kill()
        time.sleep(3)
        other.start()
        until(lambda: one_leader(nodes), 60)
        load.phase = "back"
        t_back = time.time()
        time.sleep(a.secs / 2)
        check("writes go on once the nodes are back", load.since(t_back) > 0)
        load.finish()
        info["batches acknowledged"] = sum(load.acked.values())
        info["DoPut streams broken (by error)"] = dict(load.errors)
        load.exactly_once("Flight DoPut")
        bad = load.bad + load.went_back()
        check("no DoGet read was torn or went back", not bad, bad[:5])
        # The stream follows every committed batch, acknowledged or not: once the table's own
        # count is reached, it has seen each of them once.
        rows = {(r["producer"], r["seq"]): r["n"] for r in call(base, "POST", "/sql", b"SELECT producer, seq, count(*) AS n FROM events GROUP BY producer, seq", timeout=60)}
        until(lambda: sum(seen.values()) >= sum(rows.values()), 30)
        info["log stream resumed on another node"] = resumes[0]
        wrong = [(k, seen.get(k), n) for k, n in rows.items() if seen.get(k) != n] + [k for k in seen if k not in rows]
        check("the log as a stream, resumed through the kills: every batch once", rows and not wrong, wrong[:6])
    finally:
        stop.set()
        if load:
            load.stop.set()
        for rd in streams:
            try:
                rd.cancel()
            except Exception:
                pass
        for nd in nodes:
            nd.kill()


def pids(*words):
    """The pondra processes whose command line has every one of `words`."""
    found = []
    for pid in subprocess.run(["pgrep", "-x", "pondra"], capture_output=True, text=True).stdout.split():
        try:
            line = open(f"/proc/{pid}/cmdline").read()
        except OSError:
            continue
        if all(w in line for w in words):
            found.append(int(pid))
    return found


def server(a):
    """`pondra serve --lakes`: the Python client writing and reading a database through the
    server, and psycopg through its Postgres port, while the database's node is killed twice (the
    server starts it again on the next request) and then the server itself is killed (its nodes
    stop with it) and started again: no Python call fails, psycopg reconnects, and everything
    acknowledged is there once."""
    import psycopg
    sys.path.insert(0, os.path.join(os.path.dirname(HERE), "python"))
    import pondra
    harness.A = argparse.Namespace(s3=False, keep=a.keep)
    folder, port, pg = harness.new_lake(), a.port + 70, a.port + 71
    log = os.path.join(tempfile.gettempdir(), f"pondra-{port}-server.stderr")

    def start():
        with open(log, "a") as err:
            p = subprocess.Popen([harness.BIN, "serve", "--lakes", folder, "--addr", f"127.0.0.1:{port}", "--pg", f"127.0.0.1:{pg}"], stdout=subprocess.DEVNULL, stderr=err)
        until(lambda: call(port, "GET", "/databases", timeout=2) is not None, 30)
        return p

    srv, size, stop, acked, failed, torn = start(), 20, threading.Event(), {}, [], []
    pg_acked, pg_unknown, pg_said = [], [], defaultdict(int)
    url = f"http://127.0.0.1:{port}/db/shop"
    try:
        call(port, "POST", "/databases", b'{"name": "shop"}', timeout=60)
        pondra.connect(url, echo=False).sql("CREATE TABLE events (producer VARCHAR, seq BIGINT, i BIGINT)")
        pondra.connect(url, echo=False).sql("CREATE TABLE pgt (seq BIGINT)")
        node = lambda: pids("--advertise", f"127.0.0.1:{port}/db/shop")
        check("a database's node, started by the server", until(node, 30))

        def writer(name, insert):
            con, seq = pondra.connect(url, echo=False), 0
            while not stop.is_set():
                try:
                    if insert:
                        con.sql("INSERT INTO events VALUES " + ", ".join(f"('{name}', {seq + 1}, {i})" for i in range(size)))
                    else:
                        con.append("events", [{"producer": name, "seq": seq + 1, "i": i} for i in range(size)])
                    seq += 1
                    acked[name] = seq
                except Exception as e:
                    failed.append((name, repr(e)[:400]))
                    return

        def reader():
            con = pondra.connect(url, echo=False)
            while not stop.is_set():
                try:
                    torn.extend(r for r in con.sql(Load.Q).rows() if r["n"] != r["mx"] * size or r["d"] != r["mx"])
                except Exception as e:
                    failed.append(("reader", repr(e)[:400]))
                    return

        def pg_writer():
            con, seq = None, 0
            while not stop.is_set():
                seq += 1
                try:
                    con = con or psycopg.connect(f"host=127.0.0.1 port={pg} dbname=shop user=u connect_timeout=3", autocommit=True)
                    con.execute("INSERT INTO pgt VALUES (%s)", (seq,))
                    pg_acked.append(seq)
                except psycopg.Error as e:
                    pg_unknown.append(seq)
                    pg_said[getattr(e, "sqlstate", None) or type(e).__name__] += 1
                    con = None
                    time.sleep(0.1)

        threads = [threading.Thread(target=writer, args=(f"py{w}", w % 2 == 1), daemon=True) for w in range(4)]
        threads += [threading.Thread(target=reader, daemon=True), threading.Thread(target=pg_writer, daemon=True)]
        [t.start() for t in threads]

        def going(what, secs=30):
            before, t0 = sum(acked.values()), time.time()
            ok = until(lambda: sum(acked.values()) > before + 4, secs)
            info[f"{what}: writes again after (s)"] = round(time.time() - t0, 1) if ok else None
            return check(f"{what}: every Python call goes on", ok and not failed, failed[:3])

        for n in (1, 2):
            time.sleep(a.secs / 3)
            [os.kill(pid, 9) for pid in node()]
            going(f"the database's node killed ({n})")
        time.sleep(a.secs / 3)
        srv.kill()
        srv.wait()
        gone = until(lambda: not node(), 15)
        check("the server killed: its database's node stops with it", gone, node())
        srv = start()
        going("the server started again")
        time.sleep(a.secs / 3)
        stop.set()
        [t.join(90) for t in threads]
        info["calls acknowledged"] = dict(acked)
        info["Postgres INSERTs acknowledged, refused or answers lost; what psycopg said"] = (len(pg_acked), len(pg_unknown), dict(pg_said))
        check("no Python call failed, no read was torn", not failed and not torn, (failed + torn)[:4])
        got = {r["producer"]: r for r in call(port, "POST", "/db/shop/sql", Load.Q.encode(), timeout=60)}
        wrong = {p: (got.get(p), n) for p, n in acked.items() if not (got.get(p) and got[p]["d"] == got[p]["mx"] == n and got[p]["n"] == n * size)}
        check("every acknowledged append and INSERT is there once", not wrong, wrong)
        rows = {r["seq"]: r["n"] for r in call(port, "POST", "/db/shop/sql", b"SELECT seq, count(*) AS n FROM pgt GROUP BY seq", timeout=60)}
        wrong = [s_ for s_ in pg_acked if rows.get(s_) != 1] + [s_ for s_ in pg_unknown if rows.get(s_, 0) > 1]
        check("every INSERT psycopg was told committed is there once; one whose answer was lost at most once", pg_acked and not wrong, wrong[:6])
    finally:
        stop.set()
        srv.kill()
        for pid in node():
            os.kill(pid, 9)


def cli(a):
    """`pondra sql` killed in the middle of a bulk INSERT, on local disk and on a bucket, with no
    node running and with one taking writes: the INSERT is applied whole or not at all, the node's
    writers go on, and the next `pondra sql` goes in (on a bucket, once the killed writer's mark
    is stale: invariant 17)."""
    for where, s3 in (("local disk", False), ("a bucket", True)):
        sim = bucket()[0] if s3 else None
        harness.A = argparse.Namespace(s3=s3, keep=a.keep)
        lake, nd = harness.new_lake(), None
        sql = lambda q: subprocess.run([harness.BIN, "sql", "--dir", lake, q], capture_output=True, text=True, timeout=180)
        try:
            sql("CREATE TABLE t (i BIGINT, j BIGINT)")
            n = 60_000_000 if not s3 else 10_000_000  # (a few seconds' writing: killed halfway through)
            big = [harness.BIN, "sql", "--dir", lake, f"INSERT INTO t SELECT value, value * 2 FROM generate_series(1, {n})"]
            t0 = time.time()
            subprocess.run(big, capture_output=True, timeout=300, check=True)
            whole = time.time() - t0
            info[f"{where}: a pondra sql INSERT of {n:,} rows (s)"] = round(whole, 1)
            rows = lambda port: call(port, "POST", "/sql", b"SELECT count(*) AS n FROM t", timeout=60)[0]["n"]

            def killed():
                p = subprocess.Popen(big, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                time.sleep(whole / 2)
                p.kill()
                p.wait()

            killed()
            t0 = time.time()
            r = sql("INSERT INTO t VALUES (-1, -1)")
            info[f"{where}, no node: the next pondra sql INSERT after the kill (s)"] = round(time.time() - t0, 1)
            nd = Node(lake, a.port + 80 + s3).start()
            until(lambda: one_leader([nd]), 60)
            got = rows(nd.port)
            info[f"{where}, no node: of the killed INSERT"] = {n + 1: "nothing applied", 2 * n + 1: "everything applied"}.get(got, got)
            check(f"{where}, no node: pondra sql killed mid-INSERT; none of it or all of it, and the next one goes in", r.returncode == 0 and got in (n + 1, 2 * n + 1), (r.stderr[-300:], got))
            call(nd.port, "POST", "/tables/events", EVENTS)
            load = Load([nd], writers=4, readers=2).start()
            time.sleep(1)
            t0 = time.time()
            killed()
            time.sleep(1)
            load.finish()
            after = rows(nd.port)
            info[f"{where}, a node taking writes: of the killed INSERT"] = {got: "nothing applied", got + n: "everything applied"}.get(after, after)
            check(f"{where}, a node taking writes: pondra sql killed mid-INSERT; none of it or all of it, the node's writers go on",
                  after in (got, got + n) and load.since(t0) and load.stall(t0, time.time()) < 3 and nd.alive(), (after, load.stall(t0, time.time())))
            load.exactly_once(f"{where}, a node taking writes")
            if s3:  # (SlateDB tries again for as long as a bucket says no)
                t0 = time.time()
                r = subprocess.run([harness.BIN, "sql", "--dir", "s3://no-such-bucket/lake", "SELECT 1"], capture_output=True, text=True, timeout=120)
                check("pondra sql on a bucket that isn't there says so, at once", r.returncode != 0 and "NoSuchBucket" in r.stderr and time.time() - t0 < 30, r.stderr[-300:])
        finally:
            if nd:
                nd.kill()
            if sim:
                sim.kill()
                harness.LAKES.remove(lake)


PARTS = {"storage": storage, "cutoff": cutoff, "clients": clients, "doors": doors, "flight": flight, "disk": disk, "cache": cache, "server": server, "cli": cli}

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("parts", nargs="*", help=f"any of {', '.join(PARTS)} (default: all)")
    ap.add_argument("--secs", type=float, default=20, help="how long each fault lasts (an outage: 40 s)")
    ap.add_argument("--port", type=int, default=8700, help="the first node's port")
    ap.add_argument("--keep", action="store_true", help="keep the lakes (and the node logs)")
    ap.add_argument("--real", action="store_true", help="a real bucket (R2, S3) for the parts on one, named by AWS_ENDPOINT, AWS_ACCESS_KEY_ID, "
                    "AWS_SECRET_ACCESS_KEY, AWS_REGION and PONDRA_BUCKET (default parts: storage cutoff)")
    A = ap.parse_args()
    if A.real and not A.parts:
        A.parts = ["storage", "cutoff"]
    if unknown := set(A.parts) - set(PARTS):
        ap.error(f"no such part: {', '.join(unknown)}")
    for PART in A.parts or list(PARTS):
        print(f"-- {PART}", flush=True)
        try:
            PARTS[PART](A)
        except Exception as e:
            check("ran to its end", False, repr(e)[-2000:])
    ok = bool(checks) and all(checks.values())
    print(json.dumps({"resilience": checks, "ok": ok, "info": info}, indent=1, default=str))
    sys.exit(0 if ok else 1)
