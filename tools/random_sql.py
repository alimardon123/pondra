#!/usr/bin/env python3
"""Random queries, three ways (roadmap D2, round 31; SQLancer's oracles): each query's answer from
one Pondra node, the same query spread over three nodes, and DuckDB's on the same rows, must
agree; and each query split by a random condition `p` into `WHERE p`, `WHERE NOT p` and
`WHERE p IS NULL` must give back its rows exactly (ternary logic partitioning), on Pondra alone.

  random_sql.py [--queries 100000] [--seed 1] [--nodes 3] [--out logs/round31/random.json]

The tables are made afresh each `--every` queries (a few hundred rows of integers, decimals,
doubles, text, booleans and dates, NULLs and repeats among them). The queries keep to SQL both
engines mean the same by: no integer division or modulo, explicit NULLS FIRST/LAST, no functions
whose names or edges differ. A disagreement is kept with its query and both answers.
"""
import argparse, datetime, decimal, io, json, os, random, sys, time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import harness

TYPES = {"i": "INT", "b": "BIGINT", "d": "DECIMAL(10,2)", "f": "DOUBLE", "s": "VARCHAR", "t": "BOOLEAN", "dt": "DATE"}
WORDS = ["a", "b", "ab", "ba", "", "abc", "Z", "zz"]


class Gen:
    def __init__(self, rnd):
        self.r = rnd

    def literal(self, kind):
        r = self.r
        if r.random() < 0.15:
            return "NULL"
        return {"i": lambda: str(r.randint(-5, 5)), "b": lambda: str(r.choice([r.randint(-5, 5), r.randint(-10**12, 10**12)])),
                "d": lambda: f"{r.randint(-500, 500) / 100:.2f}", "f": lambda: repr(r.choice([0.5, -1.25, 2.0, 3.75, 0.0, -0.5])),
                "s": lambda: "'" + r.choice(WORDS) + "'", "t": lambda: r.choice(["TRUE", "FALSE"]),
                "dt": lambda: f"DATE '2026-0{r.randint(1, 9)}-{r.randint(10, 28)}'"}[kind]()

    def tables(self):
        """Two or three tables: their columns, and the SQL that makes them."""
        out, sql = {}, []
        for n in range(self.r.randint(2, 3)):
            cols = [(f"c{i}", self.r.choice(list(TYPES))) for i in range(self.r.randint(3, 6))]
            name = f"t{n}"
            out[name] = cols
            sql.append(f"CREATE TABLE {name} ({', '.join(f'{c} {TYPES[k]}' for c, k in cols)})")
            rows = [f"({', '.join(self.literal(k) for _, k in cols)})" for _ in range(self.r.randint(0, 60))]
            for i in range(0, len(rows), 30):
                sql.append(f"INSERT INTO {name} VALUES {', '.join(rows[i:i + 30])}")
        return out, sql

    def expr(self, cols, kind, depth=0):
        """An expression of `kind` over `cols` [(qualified name, kind)]."""
        r = self.r
        here = [c for c, k in cols if k == kind]
        if depth > 2 or r.random() < 0.4:
            return r.choice(here) if here and r.random() < 0.7 else self.literal(kind)
        if kind in ("i", "b", "d", "f"):
            a, b = self.expr(cols, kind, depth + 1), self.expr(cols, kind, depth + 1)
            return r.choice([f"({a} + {b})", f"({a} - {b})", f"({a} * {b})" if kind in ("d", "f") else f"({a} + {b})", f"abs({a})", f"COALESCE({a}, {b})",
                             f"CASE WHEN {self.pred(cols, depth + 1)} THEN {a} ELSE {b} END", f"(- {a})"])
        if kind == "s":
            a, b = self.expr(cols, "s", depth + 1), self.expr(cols, "s", depth + 1)
            return r.choice([f"({a} || {b})", f"upper({a})", f"lower({a})", f"COALESCE({a}, {b})", f"CASE WHEN {self.pred(cols, depth + 1)} THEN {a} ELSE {b} END"])
        if kind == "t":
            return self.pred(cols, depth + 1)
        return f"COALESCE({self.expr(cols, kind, depth + 1)}, {self.literal(kind)})"

    def pred(self, cols, depth=0):
        r = self.r
        kind = r.choice([k for _, k in cols] or ["i"])
        a, b = self.expr(cols, kind, depth + 1), self.expr(cols, kind, depth + 1)
        if depth > 2:
            return f"({a} {r.choice(['=', '<>', '<', '>=', '<=', '>'])} {b})"
        return r.choice([f"({a} {r.choice(['=', '<>', '<', '>=', '<=', '>'])} {b})", f"({a} IS NULL)", f"({a} IS NOT NULL)",
                         f"({self.pred(cols, depth + 1)} AND {self.pred(cols, depth + 1)})", f"({self.pred(cols, depth + 1)} OR {self.pred(cols, depth + 1)})",
                         f"(NOT {self.pred(cols, depth + 1)})", f"({a} IN ({b}, {self.literal(kind)}))", f"({a} IS DISTINCT FROM {b})"])

    def query(self, tables):
        """A query, and the FROM … WHERE it splits on (for the partitioning oracle)."""
        r = self.r
        names = r.sample(list(tables), r.randint(1, min(2, len(tables))))
        cols = [(f"{n}.{c}", k) for n in names for c, k in tables[n]]
        src = names[0]
        if len(names) == 2:
            a, b = names
            ka = {k for _, k in tables[a]} & {k for _, k in tables[b]}
            if ka:
                k = r.choice(sorted(ka))
                ca, cb = r.choice([c for c, x in tables[a] if x == k]), r.choice([c for c, x in tables[b] if x == k])
                src = f"{a} {r.choice(['JOIN', 'LEFT JOIN'])} {b} ON {a}.{ca} = {b}.{cb}"
            else:
                src = f"{a} CROSS JOIN {b}"
        if r.random() < 0.4:
            # grouped
            keys = r.sample(cols, r.randint(1, min(2, len(cols))))
            aggs = []
            for i in range(r.randint(1, 3)):
                c, k = r.choice(cols)
                fn = r.choice(["count", "min", "max"] + (["sum"] if k in ("i", "b", "d") else []))
                aggs.append(f"{fn}({c}) AS a{i}")
            aggs.append("count(*) AS n")
            sel = ", ".join([f"{c} AS k{i}" for i, (c, _) in enumerate(keys)] + aggs)
            group = " GROUP BY " + ", ".join(c for c, _ in keys)
            having = f" HAVING count(*) > {r.randint(0, 2)}" if r.random() < 0.3 else ""
            return f"SELECT {sel} FROM {src}", group + having, cols
        exprs = [f"{self.expr(cols, k)} AS e{i}" for i, k in enumerate(r.choice([[k for _, k in cols]] + [[r.choice(list(TYPES))] * r.randint(1, 3)]))][:4]
        distinct = "DISTINCT " if r.random() < 0.2 else ""
        return f"SELECT {distinct}{', '.join(exprs)} FROM {src}", "", cols


def value(v):
    if isinstance(v, (decimal.Decimal, float, int)) and not isinstance(v, bool):
        return round(float(v), 6)
    if isinstance(v, (datetime.date, datetime.datetime)):
        return v.isoformat()
    return v


def bag(rows):
    return sorted((tuple(value(v) for v in r) for r in rows), key=lambda r: [(v is None, str(type(v)), v if v is not None else 0) for v in r])


def pondra(port, sql, spread=False):
    import pyarrow as pa
    data = harness.call(port, "POST", "/sql?format=arrow" + ("&spread=1" if spread else ""), sql.encode(), timeout=120)
    return [tuple(r.values()) for r in pa.ipc.open_stream(io.BytesIO(data)).read_all().to_pylist()]


def main():
    import duckdb
    rnd = random.Random(A.seed)
    g = Gen(rnd)
    harness.A = argparse.Namespace(s3=False, keep=False, port=A.port)
    stats = {"queries": 0, "agree": 0, "both_refused": 0, "pondra_refused": 0, "duckdb_refused": 0, "partitions_checked": 0, "spread_checked": 0}
    found, t0 = [], time.time()
    while stats["queries"] < A.queries:
        lake = harness.new_lake()
        nodes = [harness.Node(lake, A.port + i).start() for i in range(A.nodes)]
        duck = duckdb.connect()
        try:
            if A.nodes > 1:
                while len(harness.call(A.port, "GET", "/stats")["nodes"]) < A.nodes:
                    time.sleep(0.1)
            tables, made = g.tables()
            for s in made:
                harness.call(A.port, "POST", "/sql", s.encode())
                duck.execute(s)
            for _ in range(min(A.every, A.queries - stats["queries"])):
                head, tail, cols = g.query(tables)
                where = f" WHERE {g.pred(cols)}" if rnd.random() < 0.5 else ""
                sql = head + where + tail
                stats["queries"] += 1
                try:
                    theirs = duck.execute(sql).fetchall()
                except Exception as e:
                    theirs = e
                try:
                    mine = pondra(A.port, sql)
                except Exception as e:
                    mine = e
                if isinstance(mine, Exception) or isinstance(theirs, Exception):
                    key = "both_refused" if isinstance(mine, Exception) and isinstance(theirs, Exception) else "pondra_refused" if isinstance(mine, Exception) else "duckdb_refused"
                    stats[key] += 1
                    if key == "pondra_refused":
                        found.append({"oracle": "duckdb", "sql": sql, "pondra": str(mine)[:300]})
                    continue
                if bag(mine) != bag(theirs):
                    found.append({"oracle": "duckdb", "sql": sql, "pondra": str(bag(mine)[:5])[:400], "duckdb": str(bag(theirs)[:5])[:400]})
                    continue
                stats["agree"] += 1
                if A.nodes > 1 and stats["queries"] % A.spread_every == 0:
                    stats["spread_checked"] += 1
                    spread = pondra(A.port, sql, True)
                    if bag(spread) != bag(mine):
                        found.append({"oracle": "three nodes", "sql": sql, "one": str(bag(mine)[:5])[:400], "three": str(bag(spread)[:5])[:400]})
                if not tail and "DISTINCT" not in head:  # (ternary logic partitioning: a plain query's rows, split three ways)
                    p = g.pred(cols)
                    glue = " AND " if where else " WHERE "
                    try:
                        parts = [pondra(A.port, head + where + glue + c) for c in (f"({p})", f"(NOT ({p}))", f"(({p}) IS NULL)")]
                    except Exception as e:  # (a part refused that the whole query took is a finding too)
                        found.append({"oracle": "partitions", "sql": head + where + glue + f"({p})", "pondra": str(e)[:300]})
                        continue
                    stats["partitions_checked"] += 1
                    if bag([r for part in parts for r in part]) != bag(mine):
                        found.append({"oracle": "partitions", "sql": sql, "split_on": p})
        finally:
            [n.kill() for n in nodes]
            harness.clean_up()
        print(f"{stats['queries']} queries, {len(found)} found, {round(time.time() - t0)} s", flush=True)
    out = {**stats, "seed": A.seed, "nodes": A.nodes, "secs": round(time.time() - t0), "found": found[:200]}
    print(json.dumps({k: v for k, v in out.items() if k != "found"}))
    for f in found[:20]:
        print(json.dumps(f)[:600])
    if A.out:
        json.dump(out, open(A.out, "w"), indent=1)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--queries", type=int, default=1000)
    ap.add_argument("--every", type=int, default=500, help="queries a set of tables")
    ap.add_argument("--spread-every", type=int, default=10, help="every n-th query also spread over the nodes")
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--nodes", type=int, default=3)
    ap.add_argument("--port", type=int, default=9400)
    ap.add_argument("--out")
    A = ap.parse_args()
    main()
