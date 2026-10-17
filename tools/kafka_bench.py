#!/usr/bin/env python3
"""Kafka producers and consumers against a Pondra cluster: ingest throughput (librdkafka producers
spread over the nodes, idempotent, lz4) and latency (produce -> acked; produce -> a consumer on
another node has it). Checks every event arrived exactly once. With `--kafka DIR`, the same
producers and probes against an Apache Kafka broker (one, KRaft) from its distribution in DIR.
With `--steady MIB_S`, what a steady stream costs the lake's bucket instead: producers send that
many MiB of values a second (about 1 KB each: words drawn at random from 2,048 made-up ones, so
rows compress about as logs do) for `--warm` seconds, then `--secs` measured: the bucket's
requests a second by the class it bills them in (the nodes' `pondra_object_requests_total`, and
the simulator's own count with sim_r2.py), what they cost a month at S3's and R2's list prices,
and what a day of the stream keeps in the table's files.
  kafka_bench.py [--nodes 3] [--producers 4] [--events 500000] [--flag ack=replicated] [--s3] [--kafka ~/kafka_2.13-4.3.1]
  kafka_bench.py --s3 --steady 30 [--nodes 1] [--warm 30] [--secs 120]"""
import argparse, json, multiprocessing as mp, os, queue, random, sys, time, urllib.request
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import harness
from harness import Node, call, pct
from prices import LABEL, OPS, PRICES, dollars_a_month

A = None


def produce(port, name, n, q):
    import confluent_kafka as ck
    p = ck.Producer({"bootstrap.servers": f"127.0.0.1:{port}", "enable.idempotence": True, "compression.type": "lz4",
                     "linger.ms": 5, "batch.size": 1 << 20, "queue.buffering.max.messages": 1_000_000})
    t = time.time()
    for i in range(n):
        while True:
            try:
                p.produce("events", value=b'{"producer":"%s","i":%d,"amount":1}' % (name.encode(), i))
                break
            except BufferError:
                p.poll(0.01)
        if i % 1000 == 0:
            p.poll(0)
    left = p.flush(120)
    q.put((name, time.time() - t, left))


def steady(port, name, rate, until, q):
    """One producer sending `rate` bytes of values a second until `until`. Each value carries 1,000
    bytes of a pool of random words, read along it, so no two values of a file repeat each other."""
    import confluent_kafka as ck
    p = ck.Producer({"bootstrap.servers": f"127.0.0.1:{port}", "enable.idempotence": True, "compression.type": "lz4",
                     "linger.ms": 5, "batch.size": 1 << 20, "queue.buffering.max.messages": 1_000_000})
    vocabulary = ["".join(random.choices("abcdefghijklmnopqrstuvwxyz", k=random.randint(3, 9))) for _ in range(2048)]
    pool, size = " ".join(random.choices(vocabulary, k=4 << 20)).encode(), 1000
    at, sent, i, t0 = random.randrange(len(pool) - size), 0, 0, time.time()
    while (now := time.time()) < until:
        while sent < (now - t0) * rate:
            at = at + size + 9 if at + 2 * size + 9 < len(pool) else 0
            v = b'{"producer":"%s","i":%d,"amount":1,"payload":"%s"}' % (name.encode(), i, pool[at:at + size])
            try:
                p.produce("events", value=v)
            except BufferError:
                p.poll(0.01)
                continue
            sent, i = sent + len(v), i + 1
        p.poll(0)
        time.sleep(0.005)
    q.put((name, i, sent, p.flush(120)))


def gather(q, procs):
    """Each producer's result. A producer that died (librdkafka's fatal errors end one) stops the bench
    at once, where waiting for its result would hang until the job's time limit."""
    got = []
    while len(got) < len(procs):
        try:
            got.append(q.get(timeout=5))
        except queue.Empty:
            dead = [p.exitcode for p in procs if p.exitcode not in (None, 0)]
            if dead:
                [p.kill() for p in procs if p.is_alive()]
                sys.exit(f"kafka_bench: {len(dead)} producer(s) died (exit {dead[0]}): the error is above")
    return got


def requests(ports):
    """Every node's object-store requests so far, by class, added up."""
    got = [harness.metrics_of(p) for p in ports]
    return {op: sum(g.get(f'pondra_object_requests_total{{op="{op}"}}', 0) for g in got) for op in OPS} | \
        {"rows_in": sum(g.get("pondra_rows_in_total", 0) for g in got)}


def simulated():
    """The simulator's own count by class (sim_r2.py), or None for a real bucket."""
    try:
        return json.loads(urllib.request.urlopen(os.environ["AWS_ENDPOINT"] + "/__sim/stats", timeout=5).read())["ops"]
    except Exception:
        return None


def stored(lake):
    """The lake's bytes in its bucket (or folder)."""
    if not lake.startswith("s3://"):
        def size(path):
            try:
                return os.path.getsize(path)
            except FileNotFoundError:  # (gone while we looked: the catalog's files come and go)
                return 0
        return sum(size(os.path.join(d, f)) for d, _, fs in os.walk(lake) for f in fs)
    bucket, base = lake[5:].split("/", 1)
    return sum(o["Size"] for pg in harness.S3[0].get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=base + "/") for o in pg.get("Contents", []))


def kept():
    """The rows and bytes in the table's files."""
    return next(o for o in call(A.port, "GET", "/objects")["objects"] if o.get("name") == "events")


def cost(per_s, grew):
    """What requests at `per_s` a second, and keeping `grew` bytes a second for a day, cost a month."""
    return {where: {"requests_usd_month": round(dollars_a_month(per_s, 0, where), 2),
                    "a_day_kept_usd_month": round(dollars_a_month({}, grew * 86400 / 1e9, where), 2)} for where in PRICES}


def measure_steady(lake, ports):
    rate = A.steady * (1 << 20) / A.producers
    q, until = mp.Queue(), time.time() + A.warm + A.secs + 5
    procs = [mp.Process(target=steady, args=(A.port + 100 + k % A.nodes, f"p{k}", rate, until, q)) for k in range(A.producers)]
    [p.start() for p in procs]
    time.sleep(A.warm)
    bytes0, sim0, t0, n0 = stored(lake), simulated(), time.time(), requests(ports)
    time.sleep(A.secs)
    n1, sim1, t1 = requests(ports), simulated(), time.time()
    bytes1, took, files = stored(lake), t1 - t0, kept()
    results = gather(q, procs)
    [p.join() for p in procs]
    sent, values, unflushed = sum(r[1] for r in results), sum(r[2] for r in results), sum(r[3] for r in results)
    time.sleep(2)
    got = harness.sql(A.port, "SELECT count(*) AS n, count(DISTINCT producer || ':' || i) AS uniq FROM events")[0]
    nodes = {op: round((n1[op] - n0[op]) / took, 2) for op in OPS}
    sim = sim0 and sim1 and {op: round((sim1.get(op, 0) - sim0.get(op, 0)) / took, 2) for op in OPS}
    grew = (bytes1 - bytes0) / took
    in_rows = (n1["rows_in"] - n0["rows_in"]) / took
    kept_s = in_rows * files["bytes"] / max(files["rows"], 1)  # (bytes the table's files keep a second of the stream)
    return {"engine": "pondra", "nodes": A.nodes, "producers": A.producers, "flags": {f.split("=", 1)[0]: f.split("=", 1)[1] for f in A.flag},
            "bucket": "simulated" if sim else ("bucket" if A.s3 else "folder"), "steady_mib_s": A.steady, "measured_s": round(took),
            "in_mib_s": round(in_rows * values / max(sent, 1) / (1 << 20), 1), "in_rows_s": round(in_rows),
            "requests_per_s_nodes": nodes, "requests_per_s_bucket": sim or None,
            "bucket_grew_mb_s": round(grew / 1e6, 2), "kept_bytes_a_row": round(files["bytes"] / max(files["rows"], 1)),
            "kept_gb_a_day": round(kept_s * 86400 / 1e9, 1), "cost": cost(sim or nodes, kept_s), "prices": LABEL, "unflushed": unflushed,
            # (every acknowledged value once: one still unacknowledged when its producer gave up may have landed)
            "exactly_once": got["n"] == got["uniq"] and sent - unflushed <= got["n"] <= sent}


def broker(home, port):
    """An Apache Kafka broker and controller in one process (KRaft), its log in a folder of its own,
    with the topic `events` (one partition, as a Pondra topic has)."""
    import confluent_kafka.admin as cka, shutil, subprocess, tempfile
    d = tempfile.mkdtemp(prefix="kafka-bench-")
    cfg = os.path.join(d, "server.properties")
    open(cfg, "w").write(f"process.roles=broker,controller\nnode.id=1\ncontroller.quorum.voters=1@127.0.0.1:{port + 1}\n"
                         f"listeners=PLAINTEXT://127.0.0.1:{port},CONTROLLER://127.0.0.1:{port + 1}\ncontroller.listener.names=CONTROLLER\n"
                         f"log.dirs={d}/logs\noffsets.topic.replication.factor=1\ntransaction.state.log.replication.factor=1\ntransaction.state.log.min.isr=1\n")
    sh = lambda tool, *a: os.path.join(home, "bin", tool)
    uid = subprocess.run([sh("kafka-storage.sh"), "random-uuid"], capture_output=True, text=True, check=True).stdout.split()[-1]
    subprocess.run([sh("kafka-storage.sh"), "format", "-t", uid, "-c", cfg], capture_output=True, check=True)
    proc = subprocess.Popen([sh("kafka-server-start.sh"), cfg], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
    admin = cka.AdminClient({"bootstrap.servers": f"127.0.0.1:{port}"})
    for _ in range(120):
        try:
            admin.create_topics([cka.NewTopic("events", 1, 1)])["events"].result(5)
            break
        except Exception:
            time.sleep(0.5)

    def stop():
        os.killpg(proc.pid, 15)
        proc.wait()
        shutil.rmtree(d, ignore_errors=True)
    return stop


def main():
    import confluent_kafka as ck
    flags = dict(f.split("=", 1) for f in A.flag)
    nodes = []
    if A.kafka:
        stop = broker(os.path.expanduser(A.kafka), A.port + 100)
        A.nodes = 1
    else:
        lake = harness.new_lake()
        for i in range(A.nodes):
            nodes.append(Node(lake, A.port + i, kafka=f"127.0.0.1:{A.port + 100 + i}", **flags).start())
            if i == 0:
                call(A.port, "POST", "/sql", b"CREATE TABLE events (producer VARCHAR, i BIGINT, amount BIGINT" + (b", payload VARCHAR)" if A.steady else b")"))
                time.sleep(0.5)
        time.sleep(2)  # followers join
    if A.steady:
        out = measure_steady(lake, [A.port + i for i in range(A.nodes)])
        for n in nodes:
            n.kill()
        print(json.dumps(out, indent=1))
        return
    kport = lambda i: A.port + 100 + i % A.nodes
    # throughput: producer processes, each on its own node
    q, t = mp.Queue(), time.time()
    procs = [mp.Process(target=produce, args=(kport(k), f"p{k}", A.events, q)) for k in range(A.producers)]
    [p.start() for p in procs]
    results = gather(q, procs)
    [p.join() for p in procs]
    wall = time.time() - t
    want = A.producers * A.events
    if A.kafka:  # (idempotent producers into one partition: its end offset counts every event once)
        end = ck.Consumer({"bootstrap.servers": f"127.0.0.1:{kport(0)}", "group.id": "count"}).get_watermark_offsets(ck.TopicPartition("events", 0), 10)[1]
        got = {"n": end, "uniq": end}
    else:
        got = harness.sql(A.port, "SELECT count(*) AS n, count(DISTINCT producer || ':' || i) AS uniq FROM events")[0]
    # latency: produce one event at a time to node 0; a consumer on the last node waits for it
    c = ck.Consumer({"bootstrap.servers": f"127.0.0.1:{kport(A.nodes - 1)}", "group.id": "bench", "enable.auto.commit": False, "fetch.wait.max.ms": 100})
    c.assign([ck.TopicPartition("events", 0, ck.OFFSET_END)])
    c.consume(1, 1.0)  # (settle at the end of the log)
    p = ck.Producer({"bootstrap.servers": f"127.0.0.1:{kport(0)}", "enable.idempotence": True, "linger.ms": 0})
    acks, seen = [], []
    for i in range(A.probes):
        sent = time.time()
        p.produce("events", value=json.dumps({"producer": "probe", "i": i, "amount": 0}).encode(), on_delivery=lambda err, msg, s=sent: acks.append(time.time() - s))
        p.flush(10)
        deadline = time.time() + 10
        while time.time() < deadline:
            m = c.poll(0.05)
            if m is not None and m.error() is None and b'"probe"' in m.value() and json.loads(m.value())["i"] == i:
                seen.append(time.time() - sent)
                break
        time.sleep(0.02)
    c.close()
    for n in nodes:
        n.kill()
    if A.kafka:
        stop()
    out = {"engine": "kafka" if A.kafka else "pondra", "nodes": A.nodes, "producers": A.producers, "flags": flags, "events": want, "wall_s": round(wall, 1), "events_per_s": round(want / wall),
           "producer_s": {name: round(s, 1) for name, s, _ in results}, "unflushed": sum(l for _, _, l in results),
           "exactly_once": got == {"n": want, "uniq": want},
           "ack_ms_p50": pct(acks, .5), "ack_ms_p99": pct(acks, .99), "consumer_on_other_node_ms_p50": pct(seen, .5), "consumer_on_other_node_ms_p99": pct(seen, .99), "probes_seen": len(seen)}
    print(json.dumps(out, indent=1))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--nodes", type=int, default=3)
    ap.add_argument("--producers", type=int, default=4)
    ap.add_argument("--events", type=int, default=500_000)
    ap.add_argument("--probes", type=int, default=200)
    ap.add_argument("--flag", action="append", default=[], help="a serve flag for every node, e.g. ack=replicated")
    ap.add_argument("--port", type=int, default=18180)
    ap.add_argument("--s3", action="store_true")
    ap.add_argument("--keep", action="store_true")
    ap.add_argument("--kafka", help="an Apache Kafka distribution's folder: measure its broker instead")
    ap.add_argument("--steady", type=float, help="MiB of values a second, steadily: what the stream costs the lake's bucket")
    ap.add_argument("--warm", type=int, default=30, help="--steady: seconds before measuring")
    ap.add_argument("--secs", type=int, default=120, help="--steady: seconds measured")
    A = harness.A = ap.parse_args()
    if A.steady and A.kafka:
        ap.error("--steady measures a lake's bucket: not with --kafka")
    main()
