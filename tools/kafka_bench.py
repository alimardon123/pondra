#!/usr/bin/env python3
"""Kafka producers and consumers against a Pondra cluster: ingest throughput (librdkafka producers
spread over the nodes, idempotent, lz4) and latency (produce -> acked; produce -> a consumer on
another node has it). Checks every event arrived exactly once.
  kafka_bench.py [--nodes 3] [--producers 4] [--events 500000] [--flag ack=replicated] [--s3]"""
import argparse, json, multiprocessing as mp, os, sys, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import harness
from harness import Node, call, pct

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


def main():
    import confluent_kafka as ck
    lake = harness.new_lake()
    flags = dict(f.split("=", 1) for f in A.flag)
    nodes = []
    for i in range(A.nodes):
        nodes.append(Node(lake, A.port + i, kafka=f"127.0.0.1:{A.port + 100 + i}", **flags).start())
        if i == 0:
            call(A.port, "POST", "/sql", b"CREATE TABLE events (producer VARCHAR, i BIGINT, amount BIGINT)")
            time.sleep(0.5)
    time.sleep(2)  # followers join
    kport = lambda i: A.port + 100 + i % A.nodes
    # throughput: producer processes, each on its own node
    q, t = mp.Queue(), time.time()
    procs = [mp.Process(target=produce, args=(kport(k), f"p{k}", A.events, q)) for k in range(A.producers)]
    [p.start() for p in procs]
    results = [q.get() for _ in procs]
    [p.join() for p in procs]
    wall = time.time() - t
    want = A.producers * A.events
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
    out = {"nodes": A.nodes, "producers": A.producers, "flags": flags, "events": want, "wall_s": round(wall, 1), "events_per_s": round(want / wall),
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
    A = harness.A = ap.parse_args()
    main()
