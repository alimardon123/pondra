#!/usr/bin/env python3
"""Backups (ADR-070): a backup is a lake, and a restore is a lake of its own.

  backup_check.py [--new target/release/pondra] [--work DIR] [--port 9890]

A lake of every kind of object (upgrade_check's `build`), with rows in files and in the log, changed
rows and a merge, is backed up with BACKUP DATABASE TO. More is written and a second backup copies
only what is new; then the source is deleted, and only the backup is read:
  - served: its answers are the second run's, as they were asked before the run;
  - writes to it are refused by name (25006) at every door, the Postgres port too;
  - restored as its newest run and as its first (`--at`), each equal to the model of that run;
  - a restore, and a backup, refuse a place that holds anything else (a file, a lake with a macro);
  - a clone of it reads its rows, with no Parquet file copied;
  - a backup of a branch reads nothing of its base, which is deleted first;
  - a run killed half way leaves the run before it whole, and the next run completes;
  - `pondra backup` and `pondra restore` work with no node running;
  - a task backs up on its schedule;
  - a task a superuser who isn't the owner made is refused a BACKUP into a folder on this machine when it
    runs: the run fails with the refusal's words, and nothing is written there;
  - two nodes on one lake: the owner's BACKUP sent to the follower goes, and a superuser's is refused there
    (42501); `pondra backup` of a lake a node serves goes;
  - a table dropped and kept for UNDROP is in the backup, and comes back in the restored lake;
  - a backup in a bucket the node has no variables for is reached with the secret whose SCOPE covers
    it, and restored from there (sim_r2.py, moto);
  - a folder on the node's disk is the owner's: a superuser that is not the owner is refused a BACKUP
    into one, a RESTORE from one, and a RESTORE LOCATION into one (42501), while the owner's BACKUP goes;
  - a backup refuses to take over a lake someone made (`pondra serve` of an empty folder: no data, its
    own keys), and a restore gives its lake none of the backup's own keys (a run catalog naming `z/auth`).

Prints the checks as JSON (progress goes to standard error) and exits 1 if one fails; the lakes and
node logs are then kept in --work.
"""
import argparse, base64, contextlib, glob, itertools, json, os, shutil, signal, subprocess, sys, tempfile, time, urllib.error, urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from upgrade_check import OWNER, SECRET_KEY, TOKEN, Node, ask, build, carry_on, cli, differences, settle, stop_all  # (the upgrade check's lake, built, asked and compared alike)

V = "0.34.0"  # the release whose lake `build` makes: every kind of object it knows


class Halt(Exception):
    """A check that the checks after it need failed: the chain stops here (its check says why)."""


def say(*parts):
    """Progress goes to standard error: standard output is the checks, as JSON."""
    print(*parts, file=sys.stderr, flush=True)


@contextlib.contextmanager
def check(checks, name, halt=False):
    """One check. The block fills the dict it is given: its "ok" says whether it held, and the other
    keys are its numbers. What the block raises is the check's own failure, with the words the system
    said. With `halt`, the checks after it are not run, since they need what it makes."""
    result = checks[name] = {}
    try:
        yield result
    except Exception as e:
        result.clear()
        result.update(ok=False, error=f"{type(e).__name__}: {e}"[-1500:])
        if halt:
            raise Halt(name) from e
    result.setdefault("ok", False)
    say(("held   " if result["ok"] is True else "FAILED ") + name)


def held(v):
    return isinstance(v, dict) and v.get("ok") is True


def answer_of(v):
    """A statement's answer as its object, whatever list of rows a query wraps it in."""
    while isinstance(v, list) and len(v) == 1:
        v = v[0]
    return v


def environment():
    """What `pondra` runs under with no node: the master key and the admin token every node here uses."""
    return {**os.environ, "PONDRA_SECRET_KEY": SECRET_KEY, "PONDRA_ADMIN_TOKEN": TOKEN}


def pondra(bin, *args):
    """`pondra …` with no node running: (exit code, standard output, standard error)."""
    r = subprocess.run([bin, *args], capture_output=True, text=True, timeout=1200, env=environment())
    return r.returncode, r.stdout, r.stderr


def printed(out):
    """What `pondra backup` and `pondra restore` print: one JSON object."""
    return json.loads(out)


def door(n, method, path, body=b"", owner=True):
    """A request as `Node.call` makes it, except that a refusal is an answer: (status, SQLSTATE, text). `owner=False`
    is a superuser's own request: the admin token, and not the node's owner's key (`server::owner`)."""
    headers = {"Authorization": f"Bearer {TOKEN}", **({"x-pondra-owner": OWNER} if owner else {})}
    req = urllib.request.Request(f"http://127.0.0.1:{n.port}{path}", data=body, method=method, headers=headers)
    try:
        r = urllib.request.urlopen(req, timeout=120)
        return r.status, r.headers.get("x-pondra-sqlstate"), r.read().decode(errors="replace")
    except urllib.error.HTTPError as e:
        return e.code, e.headers.get("x-pondra-sqlstate"), e.read().decode(errors="replace")


def pg_refused(port):
    """An INSERT over the Postgres port of a backup: refused with SQLSTATE 25006."""
    try:
        import psycopg
    except ImportError:
        subprocess.run([sys.executable, "-m", "pip", "install", "psycopg[binary]"], check=True)
        import psycopg
    dsn = f"host=127.0.0.1 port={port} dbname=pondra user=admin password={TOKEN}"
    for _ in range(100):  # (the port opens a moment after the node answers its HTTP)
        try:
            conn = psycopg.connect(dsn, autocommit=True)
            break
        except psycopg.OperationalError:
            time.sleep(0.1)
    else:
        return {"ok": False, "said": "the Postgres port never opened"}
    with conn:
        try:
            conn.execute("INSERT INTO ev (id, who) VALUES (9002, 'pg')")
        except psycopg.Error as e:
            return {"ok": e.sqlstate == "25006" and "is a backup" in str(e), "sqlstate": e.sqlstate, "said": str(e)[:200]}
    return {"ok": False, "said": "not refused"}


def cli_rows(bin, lake, sql):
    """`pondra sql` on a lake with no node: the answer's rows, each a tuple of its cells' texts."""
    lines = [line.strip() for line in cli(bin, lake, sql).splitlines() if line.strip().startswith("|")]
    return [tuple(c.strip() for c in line.strip("|").split("|")) for line in lines[1:]]  # (the first line is the header)


def count(bin, lake, table):
    return int(cli_rows(bin, lake, f"SELECT count(*) AS n FROM {table}")[0][0])


def folder_bytes(path):
    return sum(os.path.getsize(p) for p in glob.glob(os.path.join(path, "**", "*"), recursive=True) if os.path.isfile(p))


# ---------------------------------------------------------------- the chain: one backup and what reads it

def chain(bin, work, ports, checks):
    """The source is built and backed up twice; it is deleted; the backup is served, refused, restored
    (its newest run, then its first) and cloned. Each check needs the one before it."""
    src, bk, r1, r2 = (os.path.join(work, name) for name in ("src", "bk", "r1", "r2"))

    # The source: one of everything, rows in files and in the log, changed rows, a merge; backed up.
    n = Node(bin, src, next(ports), work, tier_secs=2).start()
    build(n, V, work)
    settle(n, V)
    m1, ids = ask(n, V)
    with check(checks, "a first backup copies the lake", halt=True) as c:
        raw = n.q(f"BACKUP DATABASE TO '{bk}'")
        say("the first backup answered:", repr(raw)[:600])
        first = answer_of(raw)
        c.update(ok=first["runs"] == 1 and first["copied"] > 0, copied=first["copied"], objects=first["objects"], runs=first["runs"])

    # More is written, then backed up again: only what is new is copied.
    carry_on(n, V)
    settle(n, V)
    m2, _ = ask(n, V, dict(ids))
    with check(checks, "a second backup copies only what is new", halt=True) as c:
        raw = n.q(f"BACKUP DATABASE TO '{bk}'")
        say("the second backup answered:", repr(raw)[:600])
        second = answer_of(raw)
        c.update(ok=second["runs"] == 2 and second["copied"] < second["objects"], copied=second["copied"], objects=second["objects"], runs=second["runs"])
    with check(checks, "a place inside the lake is refused") as c:
        status, code, text = door(n, "POST", "/sql", f"BACKUP DATABASE TO '{src}/inner'".encode())
        c.update(ok=status != 200 and "is inside" in text, status=status, sqlstate=code, said=text[:200])
    with check(checks, "the source deleted: only the backup is read from here") as c:
        n.stop()
        shutil.rmtree(src)
        c.update(ok=not os.path.exists(src))

    # The backup, served by a node with the Postgres port too.
    bk_port, pg_port = next(ports), next(ports)
    bn = Node(bin, bk, bk_port, work, "--pg", f"127.0.0.1:{pg_port}", tier_secs=2).start()
    with check(checks, "the backup served answers as the newest run") as c:
        settle(bn, V)
        got, _ = ask(bn, V, dict(ids))
        diff = differences(m2, got)
        c.update(ok=diff == {}, differences=diff)
    with check(checks, "writes to the backup are refused by name (25006) at every door") as c:
        ev_row = {"id": 9001, "who": "refused", "amount": 1.0, "ts": "2026-01-05T00:00:00", "n": 9}
        writes = {
            "SQL INSERT": ("POST", "/sql", b"INSERT INTO ev (id, who, amount, ts, n) VALUES (9001, 'refused', 1.0, TIMESTAMP '2026-01-05 00:00:00', 9)"),
            "SQL CREATE TABLE": ("POST", "/sql", b"CREATE TABLE zz (a INT)"),
            "SQL DELETE": ("POST", "/sql", b"DELETE FROM ev WHERE id = 1"),
            "POST /append": ("POST", "/append/ev?producer=p&seq=1", (json.dumps(ev_row) + "\n").encode()),
            "PUT /files": ("PUT", "/files/x.sql", b"SELECT 1"),
        }
        doors = {}
        for door_name, (method, path, body) in writes.items():
            status, code, text = door(bn, method, path, body)
            doors[door_name] = {"ok": status != 200 and code == "25006" and "is a backup" in text, "status": status, "sqlstate": code, "said": text[:160]}
        doors["Postgres INSERT"] = pg_refused(pg_port)
        c.update(ok=all(v["ok"] for v in doors.values()), doors=doors)

    # Restored: the newest run into a new lake, then the first run by `--at`.
    bn.stop()
    with check(checks, "restored as the newest run: every answer as the second backup's model", halt=True) as c:
        rc, out, err = pondra(bin, "restore", bk, r2)
        restored = printed(out) if rc == 0 else {}
        rn = Node(bin, r2, next(ports), work, tier_secs=2).start()
        settle(rn, V)
        got2, _ = ask(rn, V, dict(ids))
        diff = differences(m2, got2)
        c.update(ok=rc == 0 and diff == {} and restored.get("run") == second["run"], exit=rc, run=restored.get("run"), differences=diff,
                 said=err[-400:] if rc else "")
    with check(checks, "restored as the first run (--at): every answer as the first backup's model", halt=True) as c:
        rc, out, err = pondra(bin, "restore", bk, r1, "--at", first["run"])
        restored = printed(out) if rc == 0 else {}
        r1n = Node(bin, r1, next(ports), work, tier_secs=2).start()
        settle(r1n, V)
        got1, _ = ask(r1n, V, dict(ids))
        diff = differences(m1, got1)
        r1n.stop()
        c.update(ok=rc == 0 and diff == {} and restored.get("run") == first["run"], exit=rc, run=restored.get("run"), differences=diff,
                 said=err[-400:] if rc else "")
    with check(checks, "a restore refuses a place that holds anything") as c:
        rc_a, out_a, err_a = pondra(bin, "restore", bk, r2)  # (a lake is there)
        junk = os.path.join(work, "junk")
        os.makedirs(junk, exist_ok=True)
        with open(os.path.join(junk, "notes.txt"), "w") as f:
            f.write("not a lake\n")
        rc_b, out_b, err_b = pondra(bin, "restore", bk, junk)  # (a file of its own is there)
        c.update(ok=rc_a != 0 and "a lake is at" in err_a + out_a and rc_b != 0 and "holds other things" in err_b + out_b
                 and os.path.exists(os.path.join(junk, "notes.txt")), into_a_lake=(err_a + out_a)[-200:], into_a_folder=(err_b + out_b)[-200:])
    with check(checks, "a clone of the backup reads it, copying no file") as c:
        rn.q(f"ATTACH '{bk}' AS b")
        rn.q("CREATE DATABASE dev CLONE b")
        rows = rn.q("SELECT count(*) AS n FROM dev.ev")[0]["n"]
        parquet = glob.glob(os.path.join(work, "dev", "**", "*.parquet"), recursive=True)
        c.update(ok=rows == len(m2["rows of ev"]) and not parquet, rows=rows, rows_in_model=len(m2["rows of ev"]), parquet_files_in_clone=len(parquet))
    rn.stop()


# ---------------------------------------------------------------- checks on their own

def killed_run(bin, work):
    """A run killed half way (ADR-070 §3), with no node running: the run before it is what the backup
    holds, whole, and the next run completes it."""
    big, bk = os.path.join(work, "big"), os.path.join(work, "bkbig")
    mark = os.path.join(bk, "_backups", "running")
    cli(bin, big, "CREATE TABLE big (x BIGINT, s VARCHAR)")
    made = [0]

    def load(batches):  # (400,000 rows of 256 characters a batch: each batch its own files)
        for _ in range(batches):
            k, made[0] = made[0], made[0] + 1
            cli(bin, big, f"INSERT INTO big SELECT x, repeat(md5(x::varchar), 8) FROM generate_series({k * 400000 + 1}, {(k + 1) * 400000}) AS s(x)")

    load(10)
    rc, out, err = pondra(bin, "backup", big, bk)
    if rc != 0:
        return {"ok": False, "first_run": (err or out)[-800:]}
    attempts = []
    for more in (10, 20):  # (a run that finishes before it is killed: more rows, and again)
        whole = count(bin, bk, "big")
        load(more)
        with open(os.path.join(work, f"killed-run-{len(attempts) + 1}.log"), "w") as log:
            proc = subprocess.Popen([bin, "backup", big, bk], stdout=subprocess.DEVNULL, stderr=log, env=environment())
            t0 = time.time()
            while proc.poll() is None and not os.path.exists(mark) and time.time() - t0 < 900:
                time.sleep(0.01)
            mid = proc.poll() is None and os.path.exists(mark)  # (under way: its mark is written)
            if proc.poll() is None:
                proc.send_signal(signal.SIGKILL)
            proc.wait()
        attempts.append({"whole_run": whole, "killed_mid_run": mid and proc.returncode == -signal.SIGKILL})
        if mid:
            break
    last = attempts[-1]
    left = os.path.exists(mark)
    after = count(bin, bk, "big")
    rc2, out2, err2 = pondra(bin, "backup", big, bk)
    final, source = count(bin, bk, "big"), count(bin, big, "big")
    return {"ok": last["killed_mid_run"] and left and after == last["whole_run"] and rc2 == 0 and final == source and not os.path.exists(mark),
            "killed_mid_run": last["killed_mid_run"], "attempts": attempts, "rows_in_whole_run": last["whole_run"],
            "rows_after_kill": after, "mark_left_by_the_kill": left, "rows_after_next_run": final, "rows_in_source": source,
            "backup_bytes": folder_bytes(bk), "next_run_said": (err2 or out2)[-300:] if rc2 else ""}


def branch_backup(bin, work, ports):
    """A backup of a branch holds what the branch reads, so it needs none of its base: the base and the
    branch's folder are deleted before the backup is read."""
    base, dev = os.path.join(work, "branch", "base"), os.path.join(work, "branch", "dev")
    bkdev = os.path.join(work, "bkdev")
    n = Node(bin, base, next(ports), work, tier_secs=2).start()
    n.q("CREATE TABLE t (x BIGINT, v VARCHAR)")
    n.q("INSERT INTO t SELECT x, 'base' FROM generate_series(1, 5000) AS s(x)")
    n.post("/tier")
    n.q("INSERT INTO t VALUES (6001, 'base in the log')")
    n.q("CREATE DATABASE dev CLONE base")
    d = Node(bin, dev, next(ports), work, tier_secs=2).start()  # (the branch's own node writes its rows)
    d.q("INSERT INTO t VALUES (7001, 'dev'), (7002, 'dev')")
    run = answer_of(n.q(f"BACKUP DATABASE dev TO '{bkdev}'"))
    d.stop()
    n.stop()
    shutil.rmtree(base)
    shutil.rmtree(dev)
    got = cli_rows(bin, bkdev, "SELECT x, v FROM t ORDER BY x")
    expect = [(str(i), "base") for i in range(1, 5001)] + [("6001", "base in the log"), ("7001", "dev"), ("7002", "dev")]
    return {"ok": got == expect, "rows": len(got), "expected_rows": len(expect), "copied": run.get("copied"), "objects": run.get("objects")}


def cli_only(bin, work):
    """`pondra backup` and `pondra restore` with no node running: the restored lake reads the same rows,
    and a time it can't read names the forms it takes."""
    lake, bkc = os.path.join(work, "plain"), os.path.join(work, "bkplain")
    back, yesterday = os.path.join(work, "plain-restored"), os.path.join(work, "plain-yesterday")
    cli(bin, lake, "CREATE TABLE c (a BIGINT, b VARCHAR)")
    cli(bin, lake, "INSERT INTO c VALUES (1, 'one'), (2, 'two')")
    cli(bin, lake, "INSERT INTO c SELECT x, 'many' FROM generate_series(3, 2000) AS s(x)")
    rc1, out1, err1 = pondra(bin, "backup", lake, bkc)
    rc2, out2, err2 = pondra(bin, "restore", bkc, back)
    rc3, out3, err3 = pondra(bin, "restore", bkc, yesterday, "--at", "yesterday")
    rows = cli_rows(bin, back, "SELECT a, b FROM c ORDER BY a") if rc2 == 0 else []
    same = rc2 == 0 and rows == cli_rows(bin, lake, "SELECT a, b FROM c ORDER BY a")
    b = printed(out1) if rc1 == 0 else {}
    message = err3 + out3
    return {"ok": rc1 == 0 and b.get("runs") == 1 and b.get("copied", 0) > 0 and rc2 == 0 and same and len(rows) == 2000
            and rc3 != 0 and "RFC 3339" in message and "milliseconds" in message and not os.path.exists(yesterday),
            "backup": b, "backup_said": (err1 or "")[-300:], "restore_exit": rc2, "rows_restored": len(rows), "same_as_source": same,
            "--at yesterday": message[-300:]}


def scheduled_backup(bin, work, ports):
    """A task that backs its database up on its schedule: a run's catalog appears in the backup's folder."""
    lake, bkt = os.path.join(work, "tasks", "lake"), os.path.join(work, "bktask")
    n = Node(bin, lake, next(ports), work).start()
    try:
        n.q("CREATE TABLE tk (a BIGINT)")
        n.q("INSERT INTO tk VALUES (1), (2), (3)")
        made = door(n, "POST", "/sql", f"CREATE TASK nightly SCHEDULE '1 second' AS BACKUP DATABASE TO '{bkt}'".encode())
        t0, runs = time.time(), []
        while time.time() - t0 < 120 and not runs:
            runs = glob.glob(os.path.join(bkt, "_backups", "*.json.zst"))
            time.sleep(0.5)
        seconds = round(time.time() - t0, 1)
        n.q("DROP TASK nightly")
        return {"ok": made[0] == 200 and bool(runs), "created": made[0], "seconds": seconds, "run_files": len(runs)}
    finally:
        n.stop()


def task_not_the_owners(bin, work, port):
    """A task a superuser that isn't the node's owner made (`by_owner` false) runs as that superuser: its BACKUP into a folder on
    this machine is refused 42501 when it runs (`backup::owners_only`), the run fails with the refusal's words, and nothing is
    written there."""
    lake, bkt = os.path.join(work, "task-theirs", "lake"), os.path.join(work, "task-theirs", "bk")
    n = Node(bin, lake, port, work).start()
    try:
        n.q("CREATE TABLE tk (a BIGINT)")
        n.q("INSERT INTO tk VALUES (1)")
        made = door(n, "POST", "/sql", f"CREATE TASK theirs SCHEDULE '1 second' AS BACKUP DATABASE TO '{bkt}'".encode(), owner=False)
        t0, failed = time.time(), []
        while time.time() - t0 < 120 and not failed:
            failed = [r for r in n.q("SELECT status, error FROM pondra.runs WHERE routine = 'theirs'") if r.get("status") == "failed"]
            time.sleep(0.5)
        n.q("DROP TASK theirs")
    finally:
        n.stop()
    error = failed[0].get("error") or "" if failed else ""
    return {"ok": made[0] == 200 and bool(failed) and "server's own disk" in error and not os.path.exists(bkt),
            "created": made[0], "run_error": error[-300:], "folder_written": os.path.exists(bkt)}


def owner_through_a_follower(bin, work, port, follower_port):
    """Two nodes on one lake: a BACKUP into a folder on this machine, sent to the FOLLOWER, runs on the follower as the owner's
    request does (the statement's node decides the place): the owner's goes, and the folder is a backup of the lake; a superuser
    that isn't the owner is refused there (42501), and its folder is not made."""
    lake = os.path.join(work, "two-nodes", "shared")
    bk, refused_bk = os.path.join(work, "two-nodes", "owner-bk"), os.path.join(work, "two-nodes", "superuser-bk")
    lead = Node(bin, lake, port, work).start()
    fol = None
    try:
        lead.q("CREATE TABLE t (a INT)")
        lead.q("INSERT INTO t VALUES (1), (2)")
        fol = Node(bin, lake, follower_port, work).start()
        time.sleep(3)  # (the follower joins the lake, and reads what the leader wrote)
        leaders = {}  # (what each node says of its leader: evidence, not a condition)
        for name, node in (("leader", lead), ("follower", fol)):
            try:
                leaders[name] = node.get("/cluster/leader")
            except Exception as e:
                leaders[name] = f"{type(e).__name__}: {e}"[-120:]
        owners = door(fol, "POST", "/sql", f"BACKUP DATABASE TO '{bk}'".encode())
        refused = door(fol, "POST", "/sql", f"BACKUP DATABASE TO '{refused_bk}'".encode(), owner=False)
    finally:
        if fol:
            fol.stop()
        lead.stop()
    rows = cli_rows(bin, bk, "SELECT a FROM t ORDER BY a") if owners[0] == 200 else []
    return {"ok": owners[0] == 200 and rows == [("1",), ("2",)] and refused[1] == "42501" and "server's own disk" in refused[2]
            and not os.path.exists(refused_bk),
            "owners_backup_via_follower": owners[0], "rows_in_the_backup": len(rows), "superuser_refused": refused[1],
            "superuser_said": refused[2][-200:], "cluster_leader": leaders}


def backup_while_serving(bin, work, port):
    """`pondra backup` of a lake a node serves goes: the lake's leader is that node, and the statement runs where `pondra` runs it.
    (Refused through the leader before the statement's node decided the place.)"""
    lake, bk = os.path.join(work, "serving", "lake"), os.path.join(work, "serving", "bk")
    n = Node(bin, lake, port, work).start()
    try:
        n.q("CREATE TABLE t (a INT)")
        n.q("INSERT INTO t VALUES (1), (2), (3)")
        rc, out, err = pondra(bin, "backup", lake, bk)
    finally:
        n.stop()
    b = printed(out) if rc == 0 else {}
    rows = cli_rows(bin, bk, "SELECT count(*) AS n FROM t") if rc == 0 else []
    return {"ok": rc == 0 and b.get("runs") == 1 and rows == [("3",)], "backup": b, "backup_said": (err + out)[-300:], "rows_in_the_backup": rows}


def places_refused(bin, work):
    """A backup goes only where nothing else is: a folder holding a file, and a lake that isn't a
    backup (one with only a macro: no table), are refused, and what they hold stays."""
    lake, folder, other = (os.path.join(work, name) for name in ("plain", "notes-folder", "only-a-macro"))
    cli(bin, lake, "CREATE TABLE t (a INT)")
    os.makedirs(folder, exist_ok=True)
    with open(os.path.join(folder, "notes.txt"), "w") as f:
        f.write("mine\n")
    rc1, out1, err1 = pondra(bin, "backup", lake, folder)
    cli(bin, other, "CREATE MACRO twice(x) AS x * 2")
    rc2, out2, err2 = pondra(bin, "backup", lake, other)
    kept = cli_rows(bin, other, "SELECT twice(21) AS n") == [("42",)]
    return {"ok": rc1 != 0 and "holds other things" in err1 + out1 and os.listdir(folder) == ["notes.txt"]
            and rc2 != 0 and "isn't a backup" in err2 + out2 and kept,
            "into_a_folder": (err1 + out1)[-200:], "into_a_lake": (err2 + out2)[-200:], "macro_kept": kept}


def owner_only_places(bin, work, port):
    """A folder on the node's own disk is the owner's alone (COPY's rule, `ext::on_disk`): a superuser that is not the
    owner is refused a BACKUP into one, a RESTORE from one and a RESTORE LOCATION into one, each before anything is
    opened. The owner's own BACKUP into one still goes."""
    refused = lambda r, words: r[1] == "42501" and words in r[2]
    shop = Node(bin, os.path.join(work, "owner-only", "shop"), port, work).start()
    try:
        shop.q("CREATE TABLE t (a INT)")
        shop.q("INSERT INTO t VALUES (1)")
        bk = os.path.join(work, "owner-only", "owner-bk")
        owners = door(shop, "POST", "/sql", f"BACKUP DATABASE TO '{bk}'".encode())
        into = door(shop, "POST", "/sql", f"BACKUP DATABASE TO '{os.path.join(work, 'owner-only', 'remote-bk')}'".encode(), owner=False)
        restore_from = door(shop, "POST", "/sql", f"RESTORE DATABASE copied FROM '{bk}'".encode(), owner=False)
        # (from a bucket, so that only the LOCATION is on this machine: the bucket is not reached, the place is refused first)
        restore_into = door(shop, "POST", "/sql", f"RESTORE DATABASE copied2 FROM 's3://backups/none' LOCATION '{os.path.join(work, 'owner-only', 'remote-copy')}'".encode(), owner=False)
    finally:
        shop.stop()
    ok = owners[0] == 200 and refused(into, "server's own disk") and refused(restore_from, "server's own disk") and refused(restore_into, "LOCATION")
    return {"ok": ok, "owners_backup": owners[0], "into_a_folder": into[2][-200:], "restore_from_a_folder": restore_from[2][-200:],
            "restore_location_a_folder": restore_into[2][-200:]}


def zstd_raw(data):
    """A zstd frame of raw blocks, uncompressed: a run's catalog as a stand-in for one a backup's writer made (zstd reads it)."""
    out = bytearray(b"\x28\xb5\x2f\xfd\xe0")  # the magic number; one segment, an 8-byte content size
    out += len(data).to_bytes(8, "little")
    pieces = [data[i:i + 131072] for i in range(0, len(data), 131072)] or [b""]
    for i, piece in enumerate(pieces):
        out += ((len(piece) << 3) | int(i == len(pieces) - 1)).to_bytes(3, "little")  # a raw block (type 0), the last flagged
        out += piece
    return bytes(out)


def lake_not_taken_over(bin, work, port):
    """A lake someone made (`pondra serve` of an empty folder with no tokens: its own keys `c`, `format`, `z/auth` and no
    data) is not a place a backup takes over: refused by name, and it is still a lake of its own, where a table can be made."""
    src, made = os.path.join(work, "taken-over", "source"), os.path.join(work, "taken-over", "made")
    cli(bin, src, "CREATE TABLE t (a INT)")
    Node(bin, made, port, work, token=False).start().stop()
    rc, out, err = pondra(bin, "backup", src, made)
    rc2, said = cli(bin, made, "CREATE TABLE after (a INT)", ok=False)
    return {"ok": rc != 0 and "isn't a backup" in err + out and rc2 == 0 and not os.path.exists(os.path.join(made, "_backups")),
            "backup_said": (err + out)[-300:], "table_after": said[-200:] if rc2 else "made"}


def restored_keys_are_its_own(bin, work):
    """A restore copies the run catalog it is given, and none of the keys a lake keeps of its own (`backup.rs`'s `own`):
    a backup whose newest run names the source's signing key (`z/auth`) restores a lake without that key."""
    src, bk, back = (os.path.join(work, "own-keys", name) for name in ("source", "bk", "restored"))
    cli(bin, src, "CREATE TABLE t (a INT)")
    cli(bin, src, "INSERT INTO t VALUES (1)")
    rc0, out0, err0 = pondra(bin, "backup", src, bk)
    if rc0:
        return {"ok": False, "backup_said": (err0 or out0)[-300:]}
    newest = sorted(glob.glob(os.path.join(bk, "_backups", "*.json.zst")))[-1]
    planted = "KEY-PLANTED-IN-A-RUN-7f3a"
    with open(newest, "wb") as f:
        f.write(zstd_raw(json.dumps({"z/auth": base64.b64encode(f'{{"sign":"{planted}"}}'.encode()).decode()}).encode()))
    rc1, out1, err1 = pondra(bin, "restore", bk, back)
    rc2, out2, _ = pondra(bin, "catalog", "--dir", back, "z/")
    return {"ok": rc1 == 0 and rc2 == 0 and planted not in out2, "restore_said": (err1 or out1)[-300:], "restored_z_keys": out2[:300]}


def dropped_kept(bin, work):
    """A table dropped and kept (ADR-043) is the database's too: restored, UNDROP brings its rows back."""
    lake, bkd, back = (os.path.join(work, name) for name in ("dropped", "bkdropped", "dropped-restored"))
    cli(bin, lake, "CREATE TABLE gone (a BIGINT)")
    cli(bin, lake, "INSERT INTO gone SELECT x FROM generate_series(1, 500) AS s(x)")
    cli(bin, lake, "DROP TABLE gone")
    cli(bin, lake, "CREATE TABLE stays (a BIGINT)")
    rc1, out1, err1 = pondra(bin, "backup", lake, bkd)
    rc2, out2, err2 = pondra(bin, "restore", bkd, back)
    if rc1 or rc2:
        return {"ok": False, "backup_said": (err1 or out1)[-400:], "restore_said": (err2 or out2)[-400:]}
    rc3, said = cli(bin, back, "UNDROP TABLE gone", ok=False)
    rows = count(bin, back, "gone") if rc3 == 0 else None
    return {"ok": rc3 == 0 and rows == 500, "undrop_said": said[-300:] if rc3 else "", "rows_after_undrop": rows}


def in_a_bucket(bin, work, ports):
    """A backup in a bucket: the node has no variables for it (no AWS_ENDPOINT), so only the secret
    whose SCOPE covers the place reaches it, here and in the `pondra lead` the run is asked of; then
    RESTORE DATABASE reads it back from there."""
    import boto3
    s3p = next(ports)
    with open(os.path.join(work, "sim_r2.log"), "w") as said:
        sim = subprocess.Popen([sys.executable, os.path.join(HERE, "sim_r2.py"), "--port", str(s3p), "--zero"], stdout=subprocess.DEVNULL, stderr=said)
    n = None
    try:
        s3 = boto3.client("s3", endpoint_url=f"http://127.0.0.1:{s3p}", region_name="us-east-1", aws_access_key_id="k", aws_secret_access_key="s")
        for _ in range(200):
            try:
                s3.create_bucket(Bucket="backups")
                break
            except Exception:
                if sim.poll() is not None:
                    raise RuntimeError("the S3 simulator (sim_r2.py) stopped: see sim_r2.log (pip install -r tools/requirements.txt: moto[server])")
                time.sleep(0.1)
        n = Node(bin, os.path.join(work, "bucket", "shop"), next(ports), work, tier_secs=2).start()
        n.q("CREATE TABLE t (x BIGINT, v VARCHAR)")
        n.q("INSERT INTO t SELECT x, 'v' || x FROM generate_series(1, 3000) AS s(x)")
        n.post("/tier")
        n.q("INSERT INTO t VALUES (3001, 'in the log')")
        n.q(f"CREATE SECRET backups_s3 (TYPE s3, KEY_ID 'k', SECRET 's', ENDPOINT 'http://127.0.0.1:{s3p}', SCOPE 's3://backups')")
        run = answer_of(n.q("BACKUP DATABASE TO 's3://backups/shop'"))
        keys = [o["Key"] for page in s3.get_paginator("list_objects_v2").paginate(Bucket="backups", Prefix="shop/") for o in page.get("Contents", [])]
        runs = [k for k in keys if k.startswith("shop/_backups/") and k.endswith(".json.zst")]
        restored = answer_of(n.q("RESTORE DATABASE shop2 FROM 's3://backups/shop'"))
        got = n.q("SELECT count(*) AS n, sum(x) AS total FROM shop2.t")[0]
        local = os.path.isdir(os.path.join(work, "bucket", "shop2"))
        return {"ok": run.get("runs") == 1 and len(runs) == 1 and got == {"n": 3001, "total": 3001 * 3002 // 2} and local,
                "copied": run.get("copied"), "objects_in_bucket": len(keys), "run_catalogs": len(runs), "restored": restored, "rows": got,
                "restored_beside_the_lake": local}
    finally:
        if n:
            n.stop()
        sim.terminate()
        sim.wait()


def backup_check(bin, work, port, checks):
    ports = itertools.count(port)  # (each node takes the next port: none is reused)
    try:
        chain(bin, work, ports, checks)
    except Halt:
        pass  # (its check says what failed)
    except Exception as e:
        checks["the chain"] = {"ok": False, "error": f"{type(e).__name__}: {e}"[-1500:]}
    with check(checks, "a run killed half way leaves the last run whole, and the next run completes") as c:
        c.update(killed_run(bin, work))
    with check(checks, "a backup of a branch reads nothing of its base") as c:
        c.update(branch_backup(bin, work, ports))
    with check(checks, "pondra backup and restore work with no node, and a time they can't read names its forms") as c:
        c.update(cli_only(bin, work))
    with check(checks, "a backup refuses a place that holds anything but a backup of its database") as c:
        c.update(places_refused(bin, work))
    with check(checks, "a folder on the node's disk is the owner's: a superuser that is not the owner is refused it") as c:
        c.update(owner_only_places(bin, work, next(ports)))
    with check(checks, "a backup doesn't take over a lake someone made") as c:
        c.update(lake_not_taken_over(bin, work, next(ports)))
    with check(checks, "a restore gives its lake none of the backup's own keys") as c:
        c.update(restored_keys_are_its_own(bin, work))
    with check(checks, "a task backs up on its schedule") as c:
        c.update(scheduled_backup(bin, work, ports))
    with check(checks, "a task a superuser who isn't the owner made is refused a BACKUP into a folder on this machine") as c:
        c.update(task_not_the_owners(bin, work, next(ports)))
    with check(checks, "two nodes on one lake: the owner's BACKUP sent to the follower goes, a superuser's is refused") as c:
        c.update(owner_through_a_follower(bin, work, next(ports), next(ports)))
    with check(checks, "pondra backup of a lake a node serves goes") as c:
        c.update(backup_while_serving(bin, work, next(ports)))
    with check(checks, "a table dropped and kept for UNDROP comes back in the restored lake") as c:
        c.update(dropped_kept(bin, work))
    with check(checks, "a backup in a bucket is reached with the secret that covers it, and restored from there") as c:
        c.update(in_a_bucket(bin, work, ports))


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--new", default=os.path.join(HERE, "..", "target", "release", "pondra"), help="this build's binary")
    ap.add_argument("--work", default="", help="where the lakes and logs go (default: a new temporary folder, removed if every check passes)")
    ap.add_argument("--port", type=int, default=9890)
    a = ap.parse_args()
    work = a.work or tempfile.mkdtemp(prefix="pondra-backup-")
    os.makedirs(work, exist_ok=True)
    checks, started = {}, time.time()
    try:
        backup_check(os.path.abspath(a.new), work, a.port, checks)
    except Exception as e:
        checks["error"] = f"{type(e).__name__}: {e}"[-1500:]
    finally:
        stop_all()
    ok = all(held(v) for v in checks.values())
    print(json.dumps({**checks, "ok": ok, "seconds": round(time.time() - started)}, indent=1))
    if ok and not a.work:
        shutil.rmtree(work, ignore_errors=True)
    elif not ok:
        print(f"(the lakes and node logs kept in {work})", file=sys.stderr)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
