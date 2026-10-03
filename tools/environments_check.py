#!/usr/bin/env python3
"""Branches (ADR-047): `CREATE DATABASE dev CLONE prod` is prod as it was, copying none of its files;
its writes are its own; prod keeps the files it reads while it lives, through merges, purges and
retention; `pondra.diff` tells their rows apart; a branch of a branch; `DROP DATABASE` lets go.

  environments_check.py [--new target/release/pondra] [--work DIR] [--port 9780]

prod's node tiers only when asked (`POST /tier`), keeps its past one second (`--retain-secs 1`) and
purges changed rows every round (`PONDRA_PURGE_ROWS=1`), so what a branch reads would be deleted at
once if nothing kept it. Prints the checks as JSON and exits 1 if one fails (the lakes and the nodes'
logs are then kept in --work).
"""
import argparse, glob, json, os, shutil, sys, tempfile, time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from upgrade_check import Node, Failed, stop_all  # (a node started and stopped as a scheduler would)


def parquet(folder):
    return sorted(glob.glob(os.path.join(folder, "data", "**", "*.parquet"), recursive=True))


def environments_check(bin, work, port):
    prod_dir, dev_dir = os.path.join(work, "prod"), os.path.join(work, "dev")
    env = {"PONDRA_PURGE_ROWS": "1"}
    prod = Node(bin, prod_dir, port, work, "--retain-secs", "1", env=env).start()
    q = prod.q

    def rows(n, sql):
        return [tuple(r.values()) for r in n.q(sql)]

    def refused(n, sql):
        try:
            n.q(sql)
            return ""
        except Failed as e:
            return str(e)

    def until(f, secs=30):
        deadline = time.time() + secs
        while True:
            v = f()
            if v or time.time() > deadline:
                return v
            time.sleep(0.5)

    checks = {}
    # prod: files merged and purged, rows still in the log, changed rows, a keyed table, a view, a task,
    # a secret and a workspace file.
    q("CREATE SCHEMA sales")
    q("CREATE TABLE sales.orders (id BIGINT, amount DOUBLE)")
    for i in range(4):  # (small files, for maintenance to merge later)
        q(f"INSERT INTO sales.orders SELECT x, x * 1.5 FROM generate_series({i * 250 + 1}, {(i + 1) * 250}) AS s(x)")
        prod.post("/tier")
    q("CREATE TABLE k (id BIGINT PRIMARY KEY, v VARCHAR)")
    q("INSERT INTO k VALUES (1, 'one'), (2, 'two'), (3, 'three')")
    q("CREATE MATERIALIZED VIEW sales.totals AS SELECT id % 2 AS odd, count(*) AS n, sum(amount) AS s FROM sales.orders GROUP BY id % 2")
    prod.post("/tier")
    q("UPDATE sales.orders SET amount = -1 WHERE id <= 10")
    q("DELETE FROM sales.orders WHERE id BETWEEN 11 AND 20")
    q("INSERT INTO sales.orders VALUES (2001, 7.0), (2002, 8.0)")  # (in the log when branched)
    q("UPDATE k SET v = 'TWO' WHERE id = 2")
    q("CREATE TASK nightly SCHEDULE '1 hour' AS INSERT INTO k VALUES (99, 'task')")
    q("CREATE SECRET s3_prod (TYPE s3, KEY_ID 'id', SECRET 'secret', SCOPE 's3://prod-bucket/')")
    prod.call("PUT", "/files/etl/orders.sql", "SELECT count(*) FROM sales.orders")
    every = "SELECT _row_id, _version, id, amount FROM sales.orders ORDER BY id"
    keyed = "SELECT _row_id, id, v FROM k ORDER BY id"
    before, before_k, totals = rows(prod, every), rows(prod, keyed), rows(prod, "SELECT odd, n, s FROM sales.totals ORDER BY odd")
    files_before = len(parquet(prod_dir))

    t0 = time.time()
    made = q("CREATE DATABASE dev CLONE prod")
    took = time.time() - t0
    copied = parquet(dev_dir)
    checks["CREATE DATABASE dev CLONE prod copies no file (the log tail and the workspace only)"] = \
        len(copied) == 0 and os.path.isdir(dev_dir) and len(parquet(prod_dir)) == files_before
    checks["prod reads dev at once: every row as prod had it, ids and versions too (the log tail too)"] = \
        until(lambda: rows(prod, every.replace("sales.orders", "dev.sales.orders")) == before) is True \
        and rows(prod, keyed.replace(" k ", " dev.k ")) == before_k and rows(prod, "SELECT odd, n, s FROM dev.sales.totals ORDER BY odd") == totals
    listed = [(r["name"], r.get("base"), r["branches"]) for r in q("SELECT name, base, branches FROM pondra.databases ORDER BY name")]  # (a NULL is left out)
    checks["pondra.databases names dev's base, and prod's branch"] = listed == [("dev", "prod", 0), ("prod", None, 1)]

    # dev's own node: its writes are its own; what reaches the outside starts suspended.
    dev = Node(bin, dev_dir, port + 1, work, "--retain-secs", "1", env=env).start()
    checks["dev answers as prod did, from its own node"] = rows(dev, every) == before and rows(dev, keyed) == before_k
    checks["dev's task starts suspended, and no secret came with it"] = rows(dev, "SELECT name, state FROM pondra.tasks") == [("nightly", "suspended")] \
        and rows(dev, "SELECT count(*) FROM secrets()") == [(0,)] and rows(prod, "SELECT count(*) FROM secrets()") == [(1,)]
    checks["dev has prod's workspace files"] = dev.get("/files/etl/orders.sql") == b"SELECT count(*) FROM sales.orders"
    dev.q("INSERT INTO sales.orders VALUES (5001, 1.0)")
    dev.q("UPDATE sales.orders SET amount = 0 WHERE id BETWEEN 21 AND 25")
    dev.q("DELETE FROM sales.orders WHERE id BETWEEN 26 AND 30")
    dev.q("INSERT INTO k VALUES (4, 'dev')")
    for _ in range(2):
        dev.post("/tier")
    model = {r[2]: r for r in before}
    for i in range(26, 31):
        model.pop(i)
    dev_rows = rows(dev, every)
    changed = {r[2]: r for r in dev_rows}
    checks["dev's writes are its own: dev has them, prod and its files don't"] = \
        sorted(changed) == sorted(list(model) + [5001]) and all(changed[i][3] == 0 for i in range(21, 26)) \
        and rows(prod, every) == before and rows(dev, "SELECT sum(n) FROM sales.totals") == [(len(changed),)] \
        and len(parquet(prod_dir)) == files_before and len(parquet(dev_dir)) > 0
    diff = until(lambda: (lambda d: d if d == [("delete", 5), ("insert", 1), ("update_postimage", 5), ("update_preimage", 5)] else None)(
        rows(prod, "SELECT _change_type, count(*) FROM pondra.diff('sales.orders', 'dev.sales.orders') GROUP BY 1 ORDER BY 1")))
    checks["pondra.diff('sales.orders', 'dev.sales.orders'): one inserted, five changed, five deleted"] = diff is not None \
        and rows(prod, "SELECT id, amount FROM pondra.diff('sales.orders', 'dev.sales.orders') WHERE _change_type = 'update_postimage' ORDER BY id") == [(i, 0.0) for i in range(21, 26)]

    # prod moves on: every row changed (purged into position deletes, files a tenth deleted rewritten),
    # small files merged, a table dropped, retention of a second. dev still reads what it listed.
    q("UPDATE sales.orders SET amount = amount + 1")
    q("DELETE FROM sales.orders WHERE id > 900")
    q("DROP TABLE k")
    for _ in range(4):
        prod.post("/tier")
        time.sleep(3)
    time.sleep(12)  # (retention runs every 10 s at most)
    prod.post("/tier")
    dev.stop()
    dev = Node(bin, dev_dir, port + 1, work, "--retain-secs", "1", env=env).start()  # (nothing of prod's files in its memory)
    checks["while prod merges, purges, drops and lets its past go, dev's files stay: dev answers as before after a restart"] = \
        rows(dev, every) == dev_rows and rows(dev, keyed) == before_k + [(rows(dev, "SELECT _row_id FROM k WHERE id = 4")[0][0], 4, "dev")]

    # A branch of a branch reads both; its base can't go first; WITH (schemas = …) and WITH NO DATA.
    dev.stop()
    q("CREATE DATABASE dev2 CLONE dev")
    checks["a branch of a branch answers as its base"] = until(lambda: rows(prod, every.replace("sales.orders", "dev2.sales.orders")) == dev_rows) is True
    checks["a database with branches can't be dropped first"] = "branches" in refused(prod, "DROP DATABASE dev")
    q("CREATE DATABASE sales_only CLONE prod WITH (schemas = (sales))")
    q("CREATE DATABASE fixtures CLONE prod WITH NO DATA")
    checks["WITH (schemas = (sales)) takes that schema alone"] = rows(prod, "SELECT count(*) FROM sales_only.sales.orders") == rows(prod, "SELECT count(*) FROM sales.orders") \
        and "not found" in refused(prod, "SELECT * FROM sales_only.k").lower() + refused(prod, "SELECT * FROM sales_only.public.k").lower()
    checks["WITH NO DATA: the tables, empty"] = rows(prod, "SELECT count(*) FROM fixtures.sales.orders") == [(0,)]
    checks["refused by name: a clone of a database not here, a branch in a bucket of a lake on disk"] = \
        "no database" in refused(prod, "CREATE DATABASE x CLONE nowhere") and "lives where its base does" in refused(prod, "CREATE DATABASE y LOCATION 's3://b/y' CLONE prod")

    # DROP DATABASE lets go: prod's replaced files go once nothing pins them.
    for name in ["dev2", "dev", "sales_only", "fixtures"]:
        q(f"DROP DATABASE {name}")
    held = len(parquet(prod_dir))
    for _ in range(3):
        time.sleep(11)
        prod.post("/tier")
    checks["DROP DATABASE: the branches' folders go, prod's pins with them, and prod lets the files they held go"] = \
        not os.path.exists(dev_dir) and rows(prod, "SELECT name, branches FROM pondra.databases") == [("prod", 0)] and len(parquet(prod_dir)) < held
    checks["prod answers as it should after it all"] = rows(prod, "SELECT count(*) FROM sales.orders") == [(len([r for r in before if r[2] <= 900]),)]
    checks["(cloned in {:.1f} s)".format(took)] = True
    return checks


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--new", default=os.path.join(HERE, "..", "target", "release", "pondra"), help="this build's binary")
    ap.add_argument("--work", default="", help="where the lakes and logs go (default: a new temporary folder, removed if every check passes)")
    ap.add_argument("--port", type=int, default=9780)
    a = ap.parse_args()
    work = a.work or tempfile.mkdtemp(prefix="pondra-environments-")
    os.makedirs(work, exist_ok=True)
    try:
        checks = environments_check(os.path.abspath(a.new), work, a.port)
    except Failed as e:
        checks = {"ran to the end": False, "error": str(e)}
    finally:
        stop_all()
    ok = all(v is True for k, v in checks.items() if k != "error")
    print(json.dumps({**checks, "ok": ok}, indent=1))
    if ok and not a.work:
        shutil.rmtree(work, ignore_errors=True)
    elif not ok:
        print(f"(the lakes and node logs kept in {work})", file=sys.stderr)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
