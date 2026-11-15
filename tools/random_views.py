#!/usr/bin/env python3
"""Random writes under kept views (li1113/F3, DBSP's definition as an oracle): after any writes, a materialized view
equals its query run whole over the same rows. Each lake gets random views of every way Pondra keeps one (from each
write's rows, by key, whole: `pondra.flows.refresh`), then rounds of random writes: inserts (late rows among them),
updates that move rows between keys, deletes, and writes to the other side of a join. After each round every view must
come to its query's answer; a view kept after the commit gets `--wait` seconds to.

  random_views.py [--rounds 200] [--secs 0] [--seed 1] [--out random-views.json]

No write runs while views are compared, so the query's answer then is its answer at the round's last commit. Exits 1
on a view that stays different from its query, a view whose run fails, a node that stopped, or a known difference
(`KNOWN`) past its date; a write Pondra refuses is listed (`kind: refused`) and fails nothing.
"""
import argparse, datetime, json, random, sys, time

import harness
from random_sql import bag, pondra

# Known differences from the oracle, each with its evidence and a date: a hit is counted under its id, never skipped
# silently, and once an entry is past `expires` the run fails (by then it is fixed and gone from here, or dated again).
KNOWN = {
    "lookup-join": {"evidence": "A view kept from each write's rows looks its joins up as each write arrives (ADR-059 §2), so "
                                "a later write to the joined table doesn't reach rows already kept: `SELECT o.c, count(*) FROM o JOIN m "
                                "ON o.g = m.g GROUP BY o.c` after `INSERT INTO m`. F2 (li1113, L1, step 11) makes it follow both sides.",
                    "expires": "2026-12-31"},
}

GROUPS, CODES = ["east", "west", "north", "south"], ["a", "b", "c", "d", "e"]
TABLES = ("CREATE TABLE o (id BIGINT, g VARCHAR, c VARCHAR, amount DECIMAL(10,2), ts TIMESTAMP)",
          "CREATE TABLE m (g VARCHAR, who VARCHAR)")

# Each shape: (the way it is meant to be kept, its query; `{x}` a random amount, `{c}` a random code).
SHAPES = [
    ("incremental", "SELECT id, g, amount FROM o WHERE amount > {x}"),
    ("incremental", "SELECT g, count(*) AS n, sum(amount) AS s FROM o WHERE c <> '{c}' GROUP BY g"),
    ("incremental", "SELECT g, c, count(*) AS n, count(amount) AS na, sum(amount) AS s FROM o GROUP BY g, c"),
    ("by key", "SELECT g, max(amount) AS hi, min(amount) AS lo, count(DISTINCT c) AS cs FROM o GROUP BY g"),
    ("by key", "SELECT g, median(amount) AS med, count(*) AS n FROM o WHERE amount IS NOT NULL GROUP BY g"),
    ("by key", "SELECT g, c, amount, ts FROM (SELECT *, row_number() OVER (PARTITION BY g, c ORDER BY ts DESC, id DESC) AS rn FROM o) WHERE rn <= 1"),
    ("by key", "SELECT g, amount, who FROM (SELECT o.g, o.amount, m.who, row_number() OVER (PARTITION BY o.g ORDER BY o.amount DESC NULLS LAST, o.id, m.who) AS place "
               "FROM o JOIN m ON o.g = m.g) WHERE place <= 2"),
    ("full", "SELECT o.c, count(*) AS n, sum(o.amount) AS s FROM o JOIN m ON o.g = m.g GROUP BY o.c"),
]


class Gen:
    def __init__(self, rnd):
        self.rnd, self.id, self.minute = rnd, 0, 0

    def rows(self, n, late=0.0):
        out = []
        for _ in range(n):
            self.id += 1
            self.minute += self.rnd.randint(0, 3)
            at = self.minute - self.rnd.randint(30, 600) if self.rnd.random() < late else self.minute  # (a late row: before the newest)
            amount = "NULL" if self.rnd.random() < 0.08 else f"{self.rnd.randint(0, 10000) / 100:.2f}"
            out.append(f"({self.id}, '{self.rnd.choice(GROUPS)}', '{self.rnd.choice(CODES)}', {amount}, TIMESTAMP '2026-01-01 00:00:00' + INTERVAL '{max(at, 0)} minutes')")
        return "INSERT INTO o VALUES " + ", ".join(out)

    def view(self):
        way, shape = self.rnd.choice(SHAPES)
        sql = shape.format(x=self.rnd.randint(10, 80), c=self.rnd.choice(CODES))
        full = way != "full" and self.rnd.random() < 0.15  # (any shape kept whole when asked)
        return ("full" if full else way), sql, " WITH (refresh = 'full')" if full else ""

    def write(self):
        r = self.rnd
        return r.choice([
            lambda: self.rows(r.randint(1, 20), late=0.2),
            lambda: self.rows(r.randint(1, 5)),
            lambda: f"UPDATE o SET amount = amount + {r.randint(-500, 500) / 100:.2f} WHERE c = '{r.choice(CODES)}'",
            lambda: f"UPDATE o SET g = '{r.choice(GROUPS)}' WHERE id % 7 = {r.randint(0, 6)}",  # (rows move between keys)
            lambda: f"DELETE FROM o WHERE c = '{r.choice(CODES)}' AND amount < {r.randint(5, 60)}",
            lambda: f"INSERT INTO m VALUES ('{r.choice(GROUPS)}', '{r.choice('pqrst')}')",
            lambda: f"DELETE FROM m WHERE who = '{r.choice('pqrst')}'",
            lambda: f"UPDATE m SET who = '{r.choice('pqrst')}' WHERE g = '{r.choice(GROUPS)}'",
        ])()


def same(name, query, wait):
    """The view == its query, once its run has caught up; else the last two answers."""
    deadline = time.time() + wait
    while True:
        mine, whole = bag(pondra(A.port, f"SELECT * FROM {name}")), bag(pondra(A.port, query))
        if mine == whole or time.time() > deadline:
            return mine == whole, mine, whole
        time.sleep(0.3)


def main():
    rnd = random.Random(f"views {A.seed}")
    harness.A = argparse.Namespace(s3=False, keep=False, port=A.port)
    stats = {"rounds": 0, "writes": 0, "refused_writes": 0, "views": 0, "checked": 0, "ways": {}}
    known = {k: 0 for k in KNOWN}
    found, t0 = [], time.time()
    over = lambda: stats["rounds"] >= A.rounds or (A.secs and time.time() - t0 >= A.secs)
    while not over():
        g, lake = Gen(rnd), harness.new_lake()
        node = harness.Node(lake, A.port).start()
        q = lambda s: harness.call(A.port, "POST", "/sql", s.encode())
        made = []
        try:
            for s in TABLES + (g.rows(200, late=0.1), "INSERT INTO m VALUES ('east', 'p'), ('west', 'q'), ('north', 'r')"):
                q(s)
            views = {}
            for i in range(rnd.randint(4, 8)):
                way, sql, opts = g.view()
                q(f"CREATE MATERIALIZED VIEW v{i}{opts} AS {sql}")
                views[f"v{i}"] = sql
            flows = pondra(A.port, "SELECT name, refresh, reads FROM pondra.flows")
            kept, joins = {r[0]: r[1] for r in flows}, {r[0] for r in flows if r[1] == "incremental" and r[2]}
            for w in kept.values():
                stats["ways"][w] = stats["ways"].get(w, 0) + 1
            stats["views"] += len(views)
            for _ in range(A.per_lake):
                if over():
                    break
                stats["rounds"] += 1
                for _ in range(rnd.randint(1, 3)):
                    s = g.write()
                    made.append(s)
                    try:
                        q(s)
                        stats["writes"] += 1
                    except Exception as e:
                        if not node.alive():
                            raise
                        stats["refused_writes"] += 1
                        found.append({"kind": "refused", "write": s, "pondra": str(e)[:300], "views": {n: kept.get(n) for n in views}})
                for name, sql in views.items():
                    ok, mine, whole = same(name, sql, 5 if name in joins else A.wait)  # (a lookup join may never come to it: KNOWN)
                    stats["checked"] += 1
                    if not ok and name in joins:  # (ADR-059 §2's lookup: KNOWN)
                        known["lookup-join"] += 1
                    elif not ok:
                        found.append({"kind": "wrong", "view": name, "way": kept.get(name), "sql": sql, "view_rows": str(mine[:5])[:400], "query_rows": str(whole[:5])[:400], "writes": made[-30:]})
                failing = pondra(A.port, "SELECT name, error FROM pondra.flows WHERE error IS NOT NULL")
                found += [{"kind": "failing", "view": n, "way": kept.get(n), "error": str(e)[:300]} for n, e in failing]
                if any(f["kind"] in ("wrong", "failing") for f in found):
                    break  # (this lake's views have told what they had to: a new lake)
        except Exception as e:
            found.append({"kind": "stopped" if not node.alive() else "error", "error": str(e)[:300], "writes": made[-30:],
                          "said": open(node.log, errors="replace").read()[-2000:] if not node.alive() else None})
        finally:
            node.kill()
            harness.clean_up()
        print(f"{stats['rounds']} rounds, {len(found)} found, {round(time.time() - t0)} s", flush=True)
    bad = [f for f in found if f["kind"] != "refused"]
    expired = [k for k, e in KNOWN.items() if datetime.date.fromisoformat(e["expires"]) < datetime.date.today()]
    out = {**stats, "known": known, "expired": expired, "seed": A.seed, "secs": round(time.time() - t0), "wrong": sum(f["kind"] == "wrong" for f in found), "found": (bad + [f for f in found if f["kind"] == "refused"])[:200]}
    print(json.dumps({k: v for k, v in out.items() if k != "found"}))
    for f in out["found"][:20]:
        print(json.dumps({k: v for k, v in f.items() if k != "writes"})[:600])
    if A.out:
        json.dump(out, open(A.out, "w"), indent=1)
    sys.exit(1 if bad or expired else 0)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--rounds", type=int, default=200)
    ap.add_argument("--secs", type=float, default=0, help="stop once this long has passed (0: run every round)")
    ap.add_argument("--per-lake", type=int, default=25, help="rounds on one lake's views before a new lake")
    ap.add_argument("--wait", type=float, default=30, help="seconds a view kept after the commit gets to catch up")
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--port", type=int, default=9450)
    ap.add_argument("--out")
    A = ap.parse_args()
    main()
