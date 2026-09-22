#!/usr/bin/env python3
"""Table metadata at scale: does a table with a million files commit and answer as fast as one
with a handful?

  metadata_bench.py [--files 1000000] [--s3]

The files are fake: they are registered with the leader the way finished INSERT jobs are
(`/cluster/files`), each claiming 1 GiB and 10 million rows (1 PiB and 10 trillion rows in all),
with min/max statistics spread over the years 2000-2019, and they don't exist in the bucket. Real
rows, written through the log with today's timestamps, sit next to them. So:

- a query filtered to today must skip every fake file without opening it (opening one fails the
  query), and give the same answer as before they were added;
- a query over one day of 2010 plans the few hundred files that day could hold, out of a million;
- the table's catalog entry, which every commit to it rewrites, must stay small;
- tiering commits and INSERTs must take as long as before.
"""
import argparse, json, os, statistics, sys, time, uuid

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import harness
from harness import Node, call, sql

DAY = 86400
T2000 = 946684800  # 2000-01-01 in seconds


def metrics(port):
    out = {}
    for line in call(port, "GET", "/metrics").decode().splitlines():
        if line and not line.startswith("#"):
            name, v = line.rsplit(" ", 1)
            out[name] = float(v)
    return out


def iso(secs):
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(secs))


def timed(f, n=5):
    ts = []
    for i in range(n):
        t = time.time()
        f(i)
        ts.append((time.time() - t) * 1000)
    return round(statistics.median(ts), 1)


def main():
    lake = harness.new_lake()
    port = A.port
    node = Node(lake, port).start()
    sql(port, "CREATE TABLE events (id BIGINT, ts TIMESTAMP, v DOUBLE, name VARCHAR)")
    columns = [[r["column_name"], r["data_type"]] for r in sql(port, "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'events' ORDER BY ordinal_position")]
    now, appended = int(time.time()), [0]

    def append(i):  # 1,000 rows through the log, acknowledged once committed
        body = "".join(json.dumps({"id": i * 1000 + j, "ts": iso(now), "v": j * 0.5, "name": f"n{j % 7}"}) + "\n" for j in range(1000))
        call(port, "POST", f"/append/events?producer=&seq={appended[0] + 1}", body.encode())
        appended[0] += 1

    def expected():  # what the real rows add up to
        return {"n": appended[0] * 1000, "s": appended[0] * 249750.0}

    def tier(i):  # rows to Parquet: writes a file and commits the table's entry
        append(100 + i)
        call(port, "POST", "/tier")

    def insert(i):  # bulk INSERT: Parquet written directly, then its files recorded in one commit
        sql(port, f"INSERT INTO events SELECT value + {10**9 + i * 10**6}, now(), 1.0, 'bulk' FROM generate_series(1, 1000)")

    def query(i):  # rows since yesterday (a different literal each time: no cached result)
        return sql(port, f"SELECT count(*) AS n, sum(v) AS s FROM events WHERE ts >= TIMESTAMP '{iso(now - DAY - i)}' AND name <> 'bulk'")[0]

    def measure():
        return {"append_ack_ms": timed(append, 20), "tier_commit_ms": timed(tier), "insert_ms": timed(insert), "query_today_ms": timed(query)}

    for i in range(20):
        append(i)
    call(port, "POST", "/tier")
    before = measure()
    answer = query(99) == expected()
    m = metrics(port)
    before["entry_bytes"] = m['pondra_table_entry_bytes{table="events"}']
    print("before:", json.dumps(before), flush=True)

    # A million fake files, 50,000 per recorded job (as a big INSERT would be).
    span = 20 * 365 * DAY  # 2000-2019
    per = span / A.files
    t, n, batch = time.time(), 0, 50_000
    record_ms = []
    while n < A.files:
        files = []
        for i in range(n, min(n + batch, A.files)):
            lo = T2000 + int(i * per)
            files.append({"path": f"data/events/fake-{i:07d}.parquet", "rows": 10_000_000, "bytes": 1 << 30,
                          "stats": {"id": [str(-10**12 - (i + 1) * 10**7), str(-10**12 - i * 10**7 - 1)], "ts": [iso(lo), iso(lo + int(per))], "v": ["0", "100"], "name": ["a", "z"]}})
        s = time.time()
        call(port, "POST", "/cluster/files", json.dumps({"table": "events", "job": uuid.uuid4().hex, "columns": columns, "files": files}).encode(), timeout=600, headers={"content-type": "application/json"})
        record_ms.append((time.time() - s) * 1000)
        n += len(files)
    m = metrics(port)
    added = {"files": n, "seconds": round(time.time() - t, 1), "record_ms_per_50k_files": round(statistics.median(record_ms)),
             "inline": m['pondra_table_files{table="events",where="inline"}'], "sealed": m['pondra_table_files{table="events",where="sealed"}'],
             "entry_bytes": m['pondra_table_entry_bytes{table="events"}'], "table_bytes": m['pondra_table_bytes{table="events"}']}
    print("added:", json.dumps(added), flush=True)

    after = measure()
    after["entry_bytes"] = metrics(port)['pondra_table_entry_bytes{table="events"}']
    print("after:", json.dumps(after), flush=True)
    # Same answer (bulk rows added since are left out), and no fake file opened.
    same = query(99)
    m0 = metrics(port)
    query(7)
    m1 = metrics(port)
    scanned = m1["pondra_files_scanned_total"] - m0["pondra_files_scanned_total"]
    skipped = m1["pondra_files_skipped_total"] - m0["pondra_files_skipped_total"]
    # One day of 2010: plan only (the files aren't there to read).
    day = T2000 + 3652 * DAY
    s = time.time()
    sql(port, f"EXPLAIN SELECT count(*) FROM events WHERE ts >= TIMESTAMP '{iso(day)}' AND ts < TIMESTAMP '{iso(day + DAY)}'")
    plan_ms = round((time.time() - s) * 1000, 1)
    m2 = metrics(port)
    day_files = m2["pondra_files_scanned_total"] - m1["pondra_files_scanned_total"]
    # A restarted node: nothing cached.
    node.kill()
    node = Node(lake, port).start()
    s = time.time()
    cold = query(8)
    cold_ms = round((time.time() - s) * 1000, 1)
    # Two more nodes: the same query spread over three, each pruning its share of the manifests.
    for p in (port + 1, port + 2):
        Node(lake, p).start()
    while len(call(port, "GET", "/stats")["nodes"]) < 3:
        time.sleep(0.2)
    s = time.time()
    spread = call(port, "POST", "/sql?spread=1", f"SELECT count(*) AS n, sum(v) AS s FROM events WHERE ts >= TIMESTAMP '{iso(now - DAY - 9)}' AND name <> 'bulk'".encode())[0]
    spread_ms = round((time.time() - s) * 1000, 1)
    was_spread = metrics(port)["pondra_spread_queries_total"] >= 1
    checks = {
        "right answers, before and with 1M more files": answer and same == expected(),
        "today's query opened no fake file": scanned <= 128 and skipped >= n,
        "one day of 2010 plans that day's files only": 0 < day_files <= 2 * n / (20 * 365) + 64,
        "catalog entry stays small (< 256 KB)": after["entry_bytes"] < 256 << 10,
        "commits as fast (within 2x + 20 ms)": after["tier_commit_ms"] <= 2 * before["tier_commit_ms"] + 20 and after["insert_ms"] <= 2 * before["insert_ms"] + 20,
        "restarted node answers right": cold == expected(),
        "three nodes, manifests dealt out: same answer": was_spread and spread == expected(),
    }
    out = {"before": before, "added": added, "after": after, "today_scanned": scanned, "today_skipped": skipped,
           "day_2010_files": day_files, "day_2010_plan_ms": plan_ms, "cold_query_ms": cold_ms, "three_nodes_ms": spread_ms, "checks": checks, "ok": all(checks.values())}
    print(json.dumps(out, indent=1))
    sys.exit(0 if out["ok"] else 1)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--files", type=int, default=1_000_000)
    ap.add_argument("--s3", action="store_true")
    ap.add_argument("--port", type=int, default=8095)
    A = ap.parse_args()
    harness.A = A
    main()
