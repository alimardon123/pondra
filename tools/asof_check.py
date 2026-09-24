#!/usr/bin/env python3
"""Point-in-time joins (`ASOF JOIN`) against DuckDB's, on one node and across three.

  asof_check.py [--trades 1000000] [--quotes 200000] [--nodes 3] [--s3]

Trades and quotes over 1,000 symbols; quote times are all different, so each trade has one right
answer. The same queries run in DuckDB (`ASOF LEFT JOIN … ON t.sym = q.sym AND t.ts >= q.ts`) on
the same rows. What this proves:

- every direction (>=, >, <=, <) gives each trade the quote DuckDB gives it, or none where it
  gives none; so do one filtered on the quote (the latest quote, then the filter: not the latest
  quote that passes it), one with no key, one over a small table (looked up in one table rather
  than hashed by the key) and one for a few trades (they are read first, then only their
  symbols' quotes);
- across the nodes (`spread=1`) the answers are the same: with the quotes read whole where they
  are small (looked up in one table, or hashed by the symbol on every node alike), and with every
  table sliced and both sides shuffled by the symbol; and where DataFusion plans sort-merge joins
  (as Pondra's retry of a query that ran out of memory does).

It also times the main query in Pondra and in DuckDB.
"""
import argparse, io, json, os, sys, time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import duckdb, pyarrow as pa, pyarrow.ipc
import harness
from harness import Node, call
from shuffle_spill import metrics

BASE = 1_780_000_000
QUERIES = {  # name -> (Pondra's join, DuckDB's, what's selected)
    ">=": ("ASOF JOIN quotes q MATCH_CONDITION (t.ts >= q.ts) ON t.sym = q.sym", "ASOF LEFT JOIN quotes q ON t.sym = q.sym AND t.ts >= q.ts"),
    ">": ("ASOF JOIN quotes q MATCH_CONDITION (t.ts > q.ts) ON t.sym = q.sym", "ASOF LEFT JOIN quotes q ON t.sym = q.sym AND t.ts > q.ts"),
    "<=": ("ASOF JOIN quotes q MATCH_CONDITION (t.ts <= q.ts) ON t.sym = q.sym", "ASOF LEFT JOIN quotes q ON t.sym = q.sym AND t.ts <= q.ts"),
    "<": ("ASOF JOIN quotes q MATCH_CONDITION (q.ts > t.ts) ON t.sym = q.sym", "ASOF LEFT JOIN quotes q ON t.sym = q.sym AND t.ts < q.ts"),
    "no key": ("ASOF JOIN quotes q MATCH_CONDITION (t.ts >= q.ts)", "ASOF LEFT JOIN quotes q ON t.ts >= q.ts"),
    "small table": ("ASOF JOIN (SELECT * FROM quotes WHERE id % 100 = 0) q MATCH_CONDITION (t.ts >= q.ts) ON t.sym = q.sym",
                    "ASOF LEFT JOIN (SELECT * FROM quotes WHERE id % 100 = 0) q ON t.sym = q.sym AND t.ts >= q.ts"),
    "few trades": ("ASOF JOIN quotes q MATCH_CONDITION (t.ts >= q.ts) ON t.sym = q.sym WHERE t.id % 1000 = 0",
                   "ASOF LEFT JOIN quotes q ON t.sym = q.sym AND t.ts >= q.ts WHERE t.id % 1000 = 0"),
}


def pondra(port, sql, spread):
    body = call(port, "POST", f"/sql?format=arrow&spread={spread}", sql.encode(), timeout=3600)
    return pa.ipc.open_stream(io.BytesIO(body)).read_all() if body else None


def summary(sql):
    """What a query's answers add up to: per trade, the quote's id (-1 for none), summed in ways
    that differ if any trade got another quote."""
    return f"SELECT count(*) AS n, count(q_id) AS matched, sum(coalesce(q_id, -1) * (id % 1009)) AS weighted, sum(coalesce(q_id, -1)) AS total FROM ({sql}) x"


def main():
    lake = harness.new_lake()
    db = duckdb.connect()
    out, first = {}, True
    hashed = "datafusion.optimizer.hash_join_single_partition_threshold=0,datafusion.optimizer.hash_join_single_partition_threshold_rows=0"
    for name, env in [("quotes whole", {}), ("quotes whole, hashed by the key", {"PONDRA_SQL_OPTIONS": hashed}), ("every table sliced, both sides shuffled", {"PONDRA_BROADCAST_MB": "0", "PONDRA_SQL_OPTIONS": hashed}),
                      ("sort-merge joins preferred", {"PONDRA_SQL_OPTIONS": "datafusion.optimizer.prefer_hash_join=false"})]:
        scratch = os.path.join(harness.tempfile.gettempdir(), f"pondra-asof-{os.getpid()}")
        nodes = [Node(lake, A.port + i, env={**env, "PONDRA_CACHE_DIR": f"{scratch}/{i}"}, memory_gb=2, tier_secs=0).start() for i in range(A.nodes)]
        port = A.port
        deadline = time.time() + 90
        while len(call(port, "GET", "/stats").get("nodes", [])) < A.nodes and time.time() < deadline:
            time.sleep(0.5)
        if first:
            call(port, "POST", "/sql", b"CREATE TABLE trades (id BIGINT, sym VARCHAR, ts TIMESTAMP, qty BIGINT)")
            call(port, "POST", "/sql", b"CREATE TABLE quotes (id BIGINT, sym VARCHAR, ts TIMESTAMP, price DOUBLE)")
            # quotes every 13 s (all times different), trades every 7 s from a little earlier
            call(port, "POST", "/sql", f"INSERT INTO quotes SELECT value, 'S' || ((value * 7919) % 1000), to_timestamp_seconds({BASE} + value * 13), (value % 997) * 0.25 FROM generate_series(1, {A.quotes})".encode(), timeout=3600)
            span = A.quotes * 13 // 7
            for i in range(0, A.trades, 500_000):
                n = min(500_000, A.trades - i)
                call(port, "POST", "/sql", f"INSERT INTO trades SELECT value, 'S' || ((value * 104729) % 1003), to_timestamp_seconds({BASE} - 3000 + (value * 7919) % {span} * 7 + 3), value % 10 FROM generate_series({i + 1}, {i + n})".encode(), timeout=3600)
            call(port, "POST", "/tier", timeout=3600)
            for t in ("trades", "quotes"):  # (into DuckDB's own tables: it reads Arrow ones much more slowly)
                db.register("_arrow", pondra(port, f"SELECT * FROM {t}", 0))
                db.sql(f"CREATE TABLE {t} AS SELECT * FROM _arrow")
                db.unregister("_arrow")
            first = False
        before = metrics(port)
        same, plans = {}, {}
        for q, (ours, theirs) in QUERIES.items():
            mine = f"SELECT t.id, q.id AS q_id FROM trades t {ours}"
            want = db.sql(summary(f"SELECT t.id, q.id AS q_id FROM trades t {theirs}")).fetchall()[0]
            got = [tuple(pondra(port, summary(mine) + f" -- {spread}", spread).to_pylist()[0].values()) for spread in (0, 1)]
            same[q] = [g == want for g in got]
            plans[q] = [l.split(":")[1].split(",")[0].strip() + " " + l.split(",")[1].strip() for l in pondra(port, "EXPLAIN " + mine, 0).column("plan").to_pylist()[-1].splitlines() if "AsOfJoinExec" in l]
        # (a WHERE on the quote filters what the join found: it isn't pushed into the lookup)
        mine = "SELECT t.id, q.id AS q_id FROM trades t ASOF JOIN quotes q MATCH_CONDITION (t.ts >= q.ts) ON t.sym = q.sym WHERE q.price > 100"
        want = db.sql(summary("SELECT t.id, q.id AS q_id FROM trades t ASOF LEFT JOIN quotes q ON t.sym = q.sym AND t.ts >= q.ts WHERE q.price > 100")).fetchall()[0]
        same["filtered after the join"] = [tuple(pondra(port, summary(mine) + f" -- f{s}", s).to_pylist()[0].values()) == want for s in (0, 1)]
        spread = metrics(port).get("pondra_spread_queries_total", 0) - before.get("pondra_spread_queries_total", 0)
        timing = {}
        if name == "quotes whole":
            main_sql = "SELECT t.sym, sum(t.qty * q.price) AS v FROM trades t ASOF JOIN quotes q MATCH_CONDITION (t.ts >= q.ts) ON t.sym = q.sym GROUP BY t.sym"
            duck_sql = "SELECT t.sym, sum(t.qty * q.price) AS v FROM trades t ASOF LEFT JOIN quotes q ON t.sym = q.sym AND t.ts >= q.ts GROUP BY t.sym"
            best = lambda f: min((lambda s: (f(), time.time() - s)[1])(time.time()) for _ in range(3))
            k = iter(range(100))
            timing = {"pondra_one_node_s": round(best(lambda: pondra(port, main_sql + f" -- t{next(k)}", 0)), 3),
                      f"pondra_{A.nodes}_nodes_s": round(best(lambda: pondra(port, main_sql + f" -- t{next(k)}", 1)), 3),
                      "duckdb_s": round(best(lambda: db.sql(duck_sql).fetchall()), 3)}
        out[name] = {"same_as_duckdb_[one node, spread]": same, "joins": plans, "spread_queries": spread, **timing}
        for n in nodes:
            n.kill()
        harness.subprocess.run(["rm", "-rf", scratch])
    checks = {
        "one node: every answer equals DuckDB's": all(s[0] for r in out.values() for s in r["same_as_duckdb_[one node, spread]"].values()),
        "across the nodes: every answer equals DuckDB's": all(s[1] for r in out.values() for s in r["same_as_duckdb_[one node, spread]"].values()),
        "the joins ran across the nodes": all(r["spread_queries"] >= len(QUERIES) + 1 for r in out.values()),
        "every way of looking up was used": {p.split()[0] for r in out.values() for ps in r["joins"].values() for p in ps} >= {"mode=Partitioned", "mode=Collected", "mode=Keys"},
    }
    result = {"trades": A.trades, "quotes": A.quotes, "runs": out, "checks": checks, "ok": all(checks.values())}
    print(json.dumps(result, indent=1))
    sys.exit(0 if result["ok"] else 1)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--trades", type=int, default=1_000_000)
    ap.add_argument("--quotes", type=int, default=200_000)
    ap.add_argument("--nodes", type=int, default=3)
    ap.add_argument("--port", type=int, default=8180)
    ap.add_argument("--s3", action="store_true")
    ap.add_argument("--keep", action="store_true")
    A = ap.parse_args()
    harness.A = A
    main()
