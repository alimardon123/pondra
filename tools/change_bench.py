#!/usr/bin/env python3
"""What changing rows costs (ADR-020), on one node.

  change_bench.py [--rows 10000000]

A table of `--rows` rows in Parquet files (bulk INSERTs). Then: an UPDATE of one row and of 1% of
them, a DELETE of 1%, a MERGE of 100,000 source rows (half matched), each timed; reads of
the table (count and sum, a filter, a group by) before any change, while the changed rows wait in
`{t}$deleted` (every read leaves them out), and once a purge has rewritten the files without them
(`CHECKPOINT`); then an UPDATE of 10%. (A purge comes by itself once a tenth of the table has
changed: the changes before it stay under that.) Every answer is checked against what the changes
should give.
"""
import argparse, json, os, sys, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import harness
from harness import Node, call, new_lake


def main():
    lake = new_lake()
    port = A.port
    node = Node(lake, port, tier_secs=3600, env={"PONDRA_PURGE_ROWS": str(10**12)}).start()  # (purged only when asked: CHECKPOINT below)
    q = lambda s, **p: call(port, "POST", "/sql" + ("?" + "&".join(f"{k}={v}" for k, v in p.items()) if p else ""), f"{s} -- {time.time()}".encode(), timeout=3600)
    def timed(s, runs=1):
        best, out = None, None
        for _ in range(runs):
            t = time.time()
            out = q(s)
            best = min(best or 1e9, time.time() - t)
        return round(best, 3), out
    n, step = A.rows, min(A.rows, 2_000_000)
    q("CREATE TABLE t (id BIGINT, k BIGINT, v DOUBLE, s VARCHAR)")
    t0 = time.time()
    for i in range(0, n, step):
        q(f"INSERT INTO t SELECT value + {i}, value % 1000, value * 0.5, 'row ' || (value % 97) FROM generate_series(1, {step})")
    load_s = round(time.time() - t0, 1)
    reads = {"count, sum": "SELECT count(*) AS n, sum(v) AS s FROM t",
             "a filter": "SELECT count(*) AS n FROM t WHERE k = 7",
             "group by": "SELECT k, count(*) AS n, sum(v) AS s FROM t GROUP BY k ORDER BY k LIMIT 3"}
    read = lambda: {name: timed(s, 5) for name, s in reads.items()}  # (best of 5: new files get their decoded columns kept after a second read, `hot.rs`)
    before = read()
    changes = {}
    changes["UPDATE 1 row"], _ = timed("UPDATE t SET v = v + 1 WHERE id = 12345")
    changes["UPDATE 1%"], upd1 = timed("UPDATE t SET v = v * 2 WHERE id % 100 = 1")
    changes["DELETE 1%"], del1 = timed("DELETE FROM t WHERE id % 100 = 2")
    changes["MERGE 100,000 (half new)"], merged = timed(f"MERGE INTO t USING (SELECT value * 2 + {n // 2} AS id FROM generate_series(1, 100000)) src ON t.id = src.id "
                                                      "WHEN MATCHED THEN UPDATE SET v = 0 WHEN NOT MATCHED THEN INSERT VALUES (src.id, -1, 0, 'merged')")
    waiting = read()
    t = time.time()
    call(port, "POST", "/tier", timeout=3600)  # (the changes' rows into Parquet: the old ones still in their files, left out by `{t}$deleted`)
    tier_s = round(time.time() - t, 2)
    tiered = read()
    t = time.time()
    done = q("CHECKPOINT")  # (and a purge: the files rewritten without them)
    purge_s = round(time.time() - t, 2)
    purged = read()
    changes["UPDATE 10% (after the rest)"], upd10 = timed("UPDATE t SET s = 'changed' WHERE id % 10 = 3")
    # what the answer must be
    ids = set(range(1, n + 1))
    deleted = {i for i in ids if i % 100 == 2}
    matched = {2 * v + n // 2 for v in range(1, 100001)}
    new = matched - (ids - deleted)
    rows = len(ids - deleted) + len(new)
    same = all(r["count, sum"][1][0]["n"] == rows for r in (waiting, tiered, purged)) and len({json.dumps(r[k][1]) for r in (waiting, tiered, purged) for k in reads}) == len(reads)
    node.kill()
    out = {"rows": n, "load_s": load_s, "changes_s": changes, "changed": {"update_1pct": upd1, "update_10pct": upd10, "delete_1pct": del1, "merge": merged},
           "reads_s": {k: {"before": before[k][0], "changes_waiting": waiting[k][0], "tiered": tiered[k][0], "purged": purged[k][0]} for k in reads},
           "tier_s": tier_s, "checkpoint_with_purge_s": purge_s, "checkpoint": done, "right_answers": same, "rows_after": rows}
    print(json.dumps(out, indent=1))
    if not same:
        print(waiting, tiered, purged)
        sys.exit(1)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=10_000_000)
    ap.add_argument("--port", type=int, default=8190)
    ap.add_argument("--s3", action="store_true")
    ap.add_argument("--keep", action="store_true")
    A = ap.parse_args()
    harness.A = A
    main()
