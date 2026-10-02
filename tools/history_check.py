#!/usr/bin/env python3
"""A table's past (ADR-043): UNDROP TABLE, each table's `retention`, and `t AT (VERSION => n |
TIMESTAMP => t | OFFSET => -seconds)` against a model of every state the table went through.

  history_check.py [--new target/release/pondra] [--work DIR] [--port 9760]

A node whose tiering runs only when asked (`POST /tier`), so rows can be left in the log when a table
is dropped; `--retain-secs 1`, so the log's segments go soon after; `PONDRA_PURGE_ROWS=1`, so
changed rows leave the table's files every round and only `t$deleted` has their old versions. Prints the checks as JSON and
exits 1 if one fails (the lake and the node's log are then kept in --work).
"""
import argparse, datetime, json, os, shutil, sys, tempfile, time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from upgrade_check import Node, Failed, cli, stop_all  # (a node started and stopped as a scheduler would)


def history_check(bin, work, port):
    lake = os.path.join(work, "lake")
    n = Node(bin, lake, port, work, "--retain-secs", "1", env={"PONDRA_PURGE_ROWS": "1"}).start()
    q = n.q

    def rows(sql):
        return [tuple(r.values()) for r in q(sql)]

    def refused(sql):
        try:
            q(sql)
            return ""
        except Failed as e:
            return str(e)

    def tier():
        n.post("/tier")

    checks = {}
    # A table of every kind of row: tiered, in the log, changed (old versions in `t$deleted`).
    q("CREATE TABLE t (id BIGINT, v VARCHAR)")
    q("INSERT INTO t SELECT x, 'bulk' FROM generate_series(1, 1000) AS s(x)")
    q("INSERT INTO t VALUES (2001, 'a'), (2002, 'b')")
    tier()
    q("UPDATE t SET v = 'changed' WHERE id <= 10")
    q("DELETE FROM t WHERE id BETWEEN 11 AND 20")
    q("INSERT INTO t VALUES (3001, 'in the log'), (3002, 'in the log')")  # (not tiered when dropped)
    every = "SELECT _row_id, _version, id, v FROM t ORDER BY id"
    before = rows(every)
    q("DROP TABLE t")
    gone = refused("SELECT count(*) FROM t")
    listed = rows("SELECT name, rows_in_files FROM pondra.dropped")
    # The log moves on and its segments go: the dropped table's rows must not have stayed there.
    q("CREATE TABLE other (a BIGINT)")
    for i in range(3):
        q(f"INSERT INTO other VALUES ({i})")
        tier()
        time.sleep(11)  # (retention runs at most every 10 s)
    tier()
    q("UNDROP TABLE t")
    after = rows(every)
    checks["DROP TABLE, then UNDROP TABLE: every row as it was (ids, versions, changes), those in the log when dropped too, after the log moved on"] = \
        "not found" in gone and [r[0] for r in listed] == ["t"] and listed[0][1] >= 994 and after == before and len(before) == 994 and rows("SELECT name FROM pondra.dropped") == []
    q("INSERT INTO t VALUES (4001, 'after')")
    q("UPDATE t SET v = 'again' WHERE id = 1")
    checks["an undropped table takes writes and changes"] = rows("SELECT count(*), max(id), min(v) FROM t WHERE id IN (1, 4001)") == [(2, 4001, "after")] \
        and rows("SELECT v FROM t WHERE id = 1") == [("again",)]

    # Made again under its name while the first is kept: a table of its own, in a folder of its own.
    q("DROP TABLE t")
    q("CREATE TABLE t (id BIGINT, v VARCHAR)")
    q("INSERT INTO t VALUES (1, 'second')")
    tier()
    empty_then = rows("SELECT count(*) FROM t")
    taken = refused("UNDROP TABLE t")
    q("DROP TABLE t")
    q("UNDROP TABLE t")  # (the newest: the second)
    second = rows("SELECT id, v FROM t")
    q("ALTER TABLE t RENAME TO t_second")
    q("UNDROP TABLE t")
    first = rows("SELECT count(*), max(id) FROM t")
    folders = sorted(os.listdir(os.path.join(lake, "data")))
    checks["a table made under a kept one's name is its own (its own folder); UNDROP takes the newest, refused while the name is taken"] = \
        empty_then == [(1,)] and second == [(1, "second")] and first == [(995, 4001)] and "exists: rename it" in taken \
        and "t" in folders and "t__2" in folders

    # PURGE, retention.
    q("CREATE TABLE p (a BIGINT)")
    q("INSERT INTO p VALUES (1)")
    q("DROP TABLE p PURGE")
    purged = refused("UNDROP TABLE p")
    q("CREATE TABLE z (a BIGINT) WITH (retention = '0 seconds')")
    q("DROP TABLE z")
    q("CREATE TABLE r (a BIGINT) WITH (retention = '7 days')")
    q("ALTER TABLE r SET (retention = '2 seconds')")
    q("INSERT INTO r VALUES (1)")
    q("DROP TABLE r")
    kept = rows("SELECT name, date_part('epoch', kept_until) - date_part('epoch', dropped_at) FROM pondra.dropped ORDER BY name")
    time.sleep(11)
    tier()
    let_go = refused("UNDROP TABLE r")
    bad = refused("CREATE TABLE b (a BIGINT) WITH (retention = 'soon')")
    checks["DROP … PURGE and retention = '0 seconds' keep nothing; a table's retention is how long it's kept (ALTER TABLE … SET too), then it goes; a bad one refused"] = \
        "no dropped table p" in purged and kept == [("r", 2.0)] and "no dropped table r" in let_go and "retention" in bad

    # A keyed table, and one published for other engines.
    q("CREATE TABLE k (id BIGINT PRIMARY KEY, v VARCHAR) WITH (publish = 'delta')")
    q("INSERT INTO k VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    tier()
    q("INSERT INTO k VALUES (2, 'B')")
    q("DELETE FROM k WHERE id = 3")
    keyed = rows("SELECT id, v FROM k ORDER BY id")
    q("DROP TABLE k")
    log_gone = not os.path.exists(os.path.join(lake, "data", "k", "_delta_log", "00000000000000000000.json"))
    q("UNDROP TABLE k")
    delta = None
    try:
        import deltalake, pyarrow as pa
        t = deltalake.DeltaTable(os.path.join(lake, "data", "k"))  # (its QueryBuilder applies deletion vectors)
        delta = pa.table(deltalake.QueryBuilder().register("k", t).execute("SELECT id, v FROM k ORDER BY id").read_all()).to_pylist()
    except ImportError:
        delta = "skipped: no deltalake"
    checks["a keyed table published as Delta: dropped (its Delta log too) and undropped (other engines read it again)"] = \
        keyed == [(1, "a"), (2, "B")] and rows("SELECT id, v FROM k ORDER BY id") == keyed and log_gone \
        and delta in ([{"id": 1, "v": "a"}, {"id": 2, "v": "B"}], "skipped: no deltalake")

    # AT: every state the table was in, by its commit and by its time, against what it was then.
    q("CREATE TABLE h (id BIGINT, v VARCHAR)")
    states = []

    def snap():
        time.sleep(0.05)
        states.append((n.get("/stats")["hwm"], datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="milliseconds")[:23], rows("SELECT id, v FROM h ORDER BY id")))
        time.sleep(0.05)

    snap()
    for step in ["INSERT INTO h SELECT x, 'v0' FROM generate_series(1, 300) AS s(x)",  # (files, by a bulk INSERT)
                 "INSERT INTO h VALUES (1001, 'log'), (1002, 'log')", "UPDATE h SET v = 'u1' WHERE id % 10 = 0", "tier",
                 "DELETE FROM h WHERE id % 7 = 0", "UPDATE h SET v = 'u2' WHERE id % 10 = 0", "INSERT INTO h VALUES (1003, 'log')", "tier",
                 "MERGE INTO h USING (SELECT 5 AS id, 'm' AS v UNION ALL SELECT 2000, 'new') s ON h.id = s.id WHEN MATCHED THEN UPDATE SET v = s.v WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.v)",
                 "tier", "TRUNCATE h", "INSERT INTO h VALUES (1, 'after')"]:
        tier() if step == "tier" else q(step)
        snap()
    tier()
    by_version = [rows(f"SELECT id, v FROM h AT (VERSION => {v}) ORDER BY id") == r for v, _, r in states]
    by_time = [rows(f"SELECT id, v FROM h AT (TIMESTAMP => '{t}') ORDER BY id") == r for _, t, r in states]
    checks["AT (VERSION => n) and AT (TIMESTAMP => t): the table as it was after each write (bulk INSERT, VALUES, UPDATE twice, DELETE, MERGE, TRUNCATE), tiered and purged"] = \
        all(by_version) and all(by_time) and len(states) == 13
    v3, t3, r3 = states[3]
    sys_rows = q(f"SELECT *, _row_id, _version FROM h AT (VERSION => {v3}) WHERE id IN (10, 11) ORDER BY id")
    now_ids = {r["_row_id"] for r in q("SELECT _row_id FROM h")}
    joined = rows(f"SELECT count(*) FROM h AT (VERSION => {v3}) AS o JOIN h AT (VERSION => {states[1][0]}) AS b USING (id) WHERE o.v <> b.v")
    copied = [q(f"CREATE TABLE h3 AS SELECT * FROM h AT (VERSION => {v3})"), rows("SELECT id, v FROM h3 ORDER BY id")][1]
    q(f"CREATE VIEW h_then AS SELECT id, v FROM h AT (VERSION => {v3})")
    bound = n.call("POST", "/sql", {"sql": "SELECT count(*) FROM h AT (VERSION => $v)", "params": {"v": v3}}, headers={"content-type": "application/json"})
    recent = rows("SELECT count(*) FROM h AT (OFFSET => -0.001)")
    checks["AT with system columns, joined with itself, in CREATE TABLE … AS, a view and a parameter; OFFSET seconds back"] = \
        [list(r) for r in (tuple(x.values()) for x in sys_rows)][0][:3] == [10, "u1", sys_rows[0]["_row_id"]] and list(sys_rows[0]) == ["id", "v", "_row_id", "_version"] \
        and sys_rows[0]["_row_id"] not in now_ids and joined == [(30,)] and copied == r3 and rows("SELECT * FROM h_then ORDER BY id") == r3 \
        and bound == [{"count(*)": len(r3)}] and recent == [(1,)]
    keyed = refused("SELECT * FROM k AT (VERSION => 1)")
    q("CREATE TABLE short (a BIGINT) WITH (retention = '1 seconds')")
    q("INSERT INTO short VALUES (1)")
    first = n.get("/stats")["hwm"]
    q("UPDATE short SET a = 2")
    tier()
    time.sleep(2.5)
    q("INSERT INTO short VALUES (3)")
    tier()
    time.sleep(1.5)
    q("INSERT INTO short VALUES (4)")
    tier()
    gone = refused(f"SELECT * FROM short AT (VERSION => {first})")
    checks["AT refused by name: a keyed table, a time before the table's retention kept"] = "keyed table" in keyed and "its past is kept from version" in gone

    # From `pondra sql`, with no node running.
    n.stop()
    cli(bin, lake, "DROP TABLE t_second")
    cli(bin, lake, "UNDROP TABLE t_second")
    out = cli(bin, lake, "SELECT v FROM t_second")
    checks["pondra sql drops and undrops with no node running"] = "second" in out
    return checks


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--new", default=os.path.join(HERE, "..", "target", "release", "pondra"), help="this build's binary")
    ap.add_argument("--work", default="", help="where the lake and logs go (default: a new temporary folder, removed if every check passes)")
    ap.add_argument("--port", type=int, default=9760)
    a = ap.parse_args()
    work = a.work or tempfile.mkdtemp(prefix="pondra-history-")
    os.makedirs(work, exist_ok=True)
    try:
        checks = history_check(os.path.abspath(a.new), work, a.port)
    except Failed as e:
        checks = {"ran to the end": False, "error": str(e)}
    finally:
        stop_all()
    ok = all(v is True for k, v in checks.items() if k != "error")
    print(json.dumps({**checks, "ok": ok}, indent=1))
    if ok and not a.work:
        shutil.rmtree(work, ignore_errors=True)
    elif not ok:
        print(f"(the lake and node log kept in {work})", file=sys.stderr)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
