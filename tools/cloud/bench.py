#!/usr/bin/env python3
"""A cluster benchmark: load through Arrow Flight from many writers, then a query suite on one
node and spread over all of them. Runs from any machine that reaches the nodes (pyarrow only).

  bench.py --nodes 10.0.0.1:8080:8815,10.0.0.2:8080:8815,10.0.0.3:8080:8815 \\
           --rows 100000000 --writers 12 [--token T] [--out results.json]

Each node is host:http_port:flight_port. Writes go round-robin over the nodes (every node
accepts writes); queries go to the first. Results: ingest rows/s, time until tiered, and per
query the time on one node (?spread=0) and on the cluster (?spread=1), whether they agree, and
whether the cluster shuffled. Then every node's /metrics.
"""
import argparse, json, multiprocessing, os, sys, time, urllib.request

QUERIES = {
    "filtered count": "SELECT count(*) AS n FROM events WHERE amount > 900",
    "few groups": "SELECT country, count(*) AS n, sum(amount) AS s FROM events GROUP BY country ORDER BY country",
    "many groups": "SELECT user_id, count(*) AS n, sum(amount) AS s FROM events GROUP BY user_id ORDER BY s DESC, user_id LIMIT 10",
    "distinct users": "SELECT count(DISTINCT user_id) AS d FROM events",
    "join, then groups": "SELECT u.tier, count(*) AS n, sum(e.amount) AS s FROM events e JOIN users u ON e.user_id = u.id GROUP BY u.tier ORDER BY u.tier",
    "top 10": "SELECT user_id, amount, ts FROM events ORDER BY amount DESC, user_id, ts LIMIT 10",
    "last hour": "SELECT count(*) AS n FROM events WHERE ts >= now() - INTERVAL '1 hour'",
}


def http(node, method, path, body=b"", token=None):
    host, port, _ = node.split(":")
    req = urllib.request.Request(f"http://{host}:{port}{path}", data=body if method == "POST" else None, method=method)
    if token:
        req.add_header("authorization", f"Bearer {token}")
    with urllib.request.urlopen(req, timeout=3600) as r:
        data = r.read()
    return json.loads(data) if data[:1] in (b"{", b"[") else data.decode()


def writer(args):
    """One writer: `rows` rows in batches of `batch` through one node's Flight port, exactly-once."""
    import pyarrow as pa, pyarrow.flight as fl
    node, w, rows, batch, token, users = args
    host, _, fport = node.split(":")
    client = fl.FlightClient(f"grpc://{host}:{fport}")
    opts = fl.FlightCallOptions(headers=[(b"authorization", f"Bearer {token}".encode())] if token else [])
    schema = pa.schema([("user_id", pa.int64()), ("amount", pa.int64()), ("country", pa.string()), ("ts", pa.timestamp("us"))])
    countries = ["UZ", "US", "DE", "IN", "BR", "JP", "KZ", "FR"]
    now = int(time.time() * 1e6)
    wr, rd = client.do_put(fl.FlightDescriptor.for_path("events", f"bench-{w}-{now}", "1"), schema, options=opts)
    import threading
    acks = [0]
    def read():
        while rd.read() is not None:
            acks[0] += 1
    t = threading.Thread(target=read); t.start()
    sent, i = 0, 0
    while sent < rows:
        n = min(batch, rows - sent)
        base = (w * 1_000_003 + i * 7919) % users
        wr.write_batch(pa.record_batch([pa.array([(base + j * 31) % users for j in range(n)], pa.int64()), pa.array([(i * 37 + j) % 1000 for j in range(n)], pa.int64()),
                                        pa.array([countries[(i + j) % 8] for j in range(n)]), pa.array([now - (j % 7200) * 1_000_000 for j in range(n)], pa.timestamp("us"))], schema=schema))
        sent, i = sent + n, i + 1
    wr.done_writing(); t.join(); wr.close()
    return sent


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--nodes", required=True, help="host:http_port:flight_port,…")
    ap.add_argument("--rows", type=int, default=10_000_000)
    ap.add_argument("--writers", type=int, default=4)
    ap.add_argument("--batch", type=int, default=50_000)
    ap.add_argument("--users", type=int, default=1_000_000)
    ap.add_argument("--token")
    ap.add_argument("--out", default="results.json")
    a = ap.parse_args()
    nodes = a.nodes.split(",")
    first = nodes[0]
    sql = lambda q, spread=None: http(first, "POST", "/sql" + (f"?spread={spread}" if spread is not None else ""), q.encode(), a.token)
    sql("CREATE TABLE events (user_id BIGINT, amount BIGINT, country VARCHAR, ts TIMESTAMP) WITH (partition_by = 'country')")
    sql("CREATE TABLE users (id BIGINT, tier VARCHAR)")
    sql(f"INSERT INTO users SELECT value, 'tier-' || (value % 5) FROM generate_series(0, {a.users - 1})")
    out = {"nodes": len(nodes), "rows": a.rows, "writers": a.writers}
    start = time.time()
    per = a.rows // a.writers
    with multiprocessing.Pool(a.writers) as pool:
        sent = sum(pool.map(writer, [(nodes[w % len(nodes)], w, per, a.batch, a.token, a.users) for w in range(a.writers)]))
    took = time.time() - start
    out["ingest"] = {"rows": sent, "seconds": round(took, 1), "rows_per_s": round(sent / took)}
    while True:  # until everything is Parquet
        m = metrics(first, a.token)
        if m.get("pondra_untiered_rows", 0) == 0:
            break
        time.sleep(1)
    out["ingest"]["tiered_after_s"] = round(time.time() - start, 1)
    out["queries"] = {}
    for name, q in QUERIES.items():
        row = {}
        for spread in ("0", "1"):
            before = metrics(first, a.token).get("pondra_shuffled_queries_total", 0)
            t = time.time()
            res = sql(q, spread)
            row[f"spread{spread}_s"] = round(time.time() - t, 3)
            row[f"result{spread}"] = res
            if spread == "1":
                row["shuffled"] = metrics(first, a.token).get("pondra_shuffled_queries_total", 0) > before
        row["agree"] = row.pop("result0") == row.pop("result1") or "now()" in q
        out["queries"][name] = row
        print(name, row, flush=True)
    out["metrics"] = {n: metrics(n, a.token) for n in nodes}
    json.dump(out, open(a.out, "w"), indent=1)
    print(json.dumps({k: v for k, v in out.items() if k != "metrics"}, indent=1))


def metrics(node, token):
    out = {}
    for line in http(node, "GET", "/metrics", token=token).splitlines():
        if line and not line.startswith("#"):
            k, v = line.rsplit(" ", 1)
            out[k] = float(v)
    return out


if __name__ == "__main__":
    main()
