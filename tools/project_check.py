#!/usr/bin/env python3
"""A project made true in its databases (ADR-047 §4): `pondra init`, `plan`, `deploy`, `test`,
`export`, `branch` and `diff` against a folder of lakes (`pondra serve --lakes`), as a team would.

- the first deploy makes every object, runs the migration once and the tests; a second finds nothing;
- a column added and a view changed are ALTER TABLE and a replacement; drift is put back;
- a renamed column is refused with the migration to write, and passes with it;
- a materialized view changed is made again with what follows it;
- a failed test fails the deploy; a plan shown before someone else deployed is refused;
- another project can't change this one's objects;
- `pondra export` of prod, deployed into an empty database, exports the same;
- `pondra branch` and `pondra diff`: a branch's new view and rows, its task suspended;
- `CALL plan(…)` and `CALL deploy(…)` on a project kept in the workspace; `pondra.deploys`;
- the developer's day: `pondra ci init`, `pondra branch --if-missing` and `--hook`, `pondra dev`.

  project_check.py [--new target/release/pondra] [--work DIR] [--port 9790]

Prints the checks as JSON and exits 1 if one fails (the lakes and the project are then kept in --work).
"""
import argparse, json, os, re, shutil, subprocess, sys, tempfile, time, urllib.request, urllib.error
import yaml

HERE = os.path.dirname(os.path.abspath(__file__))


def project_check(bin, work, port):
    lakes, proj = os.path.join(work, "lakes"), os.path.join(work, "sales")
    os.makedirs(lakes, exist_ok=True)
    for name in ["prod", "empty"]:
        subprocess.run([bin, "sql", "--dir", os.path.join(lakes, name), "CREATE SCHEMA scratch"], check=True, capture_output=True)
    log = open(os.path.join(work, "server.log"), "w")
    server = subprocess.Popen([bin, "serve", "--lakes", lakes, "--addr", f"127.0.0.1:{port}", "--pg", f"127.0.0.1:{port + 1}"], stdout=log, stderr=log)
    base = f"http://127.0.0.1:{port}"
    env = {**os.environ, "PONDRA_HOME": os.path.join(work, "home"), "GIT_CONFIG_GLOBAL": os.devnull}
    env.pop("PONDRA_ENV", None)

    def call(method, path, body=None):
        data = None if body is None else (body if isinstance(body, bytes) else json.dumps(body).encode())
        req = urllib.request.Request(base + path, data=data, method=method, headers={"content-type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=120) as r:
                text = r.read()
        except urllib.error.HTTPError as e:
            raise RuntimeError(e.read().decode())
        try:
            return json.loads(text)
        except ValueError:
            return text

    def q(db, sql):
        return call("POST", f"/db/{db}/sql", {"sql": sql})

    def rows(db, sql):
        return [tuple(r.values()) for r in q(db, sql)]

    def refused(db, sql):
        try:
            q(db, sql)
            return ""
        except RuntimeError as e:
            return str(e)

    def pondra(*args, ok=True):
        r = subprocess.run([bin, *args], cwd=proj, env=env, capture_output=True, text=True, timeout=300)
        if ok is not None and (r.returncode == 0) != ok:
            raise RuntimeError(f"pondra {' '.join(args)}: exit {r.returncode}\n{r.stdout}{r.stderr}")
        return r.stdout + r.stderr

    def write(rel, text):
        path = os.path.join(proj, rel)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:
            f.write(text)

    deadline = time.time() + 30
    while True:
        try:
            call("GET", "/databases")
            break
        except Exception:
            if time.time() > deadline:
                raise
            time.sleep(0.3)

    checks = {}
    os.makedirs(proj)
    subprocess.run(["git", "init", "-q", "-b", "main"], cwd=proj, env=env, check=True)
    pondra("init")
    with open(os.path.join(proj, "pondra.toml")) as f:
        toml = f.read()
    checks["pondra init: pondra.toml (named after the folder), objects/, migrations/, tests/"] = 'name = "sales"' in toml and all(os.path.isdir(os.path.join(proj, d)) for d in ["objects", "migrations", "tests"])
    write("pondra.toml", f'[project]\nname = "sales"\nserver = "{base}"\n\n[env.prod]\nvalues = {{ big = 100 }}\n\n[env.dev]\nvalues = {{ big = 50 }}\n')
    write("objects/schemas.sql", "CREATE SCHEMA sales;\n")
    write("objects/sales/orders.sql", "-- every order\nCREATE TABLE sales.orders (\n  id BIGINT,\n  amount DOUBLE\n);\n")
    write("objects/sales/views.sql", "CREATE VIEW sales.big AS SELECT * FROM sales.orders WHERE amount >= $big;\n"
          "CREATE MATERIALIZED VIEW sales.totals AS SELECT id % 2 AS odd, count(*) AS n, sum(amount) AS s FROM sales.orders GROUP BY id % 2;\n"
          "CREATE MATERIALIZED VIEW sales.rollup AS SELECT odd, sum(n) AS n FROM sales.totals GROUP BY odd;\n")
    write("objects/sales/code.sql", "CREATE MACRO sales.double(x) AS x * 2;\n"
          "CREATE PROCEDURE sales.add(n BIGINT) LANGUAGE sql AS $$ INSERT INTO sales.orders VALUES (n, n * 10.0) $$;\n"
          "CREATE TASK sales.nightly SCHEDULE '1 hour' AS CALL sales.add(999);\n")
    write("objects/access.sql", "CREATE ROLE analyst;\nGRANT SELECT ON TABLE sales.orders TO analyst;\n")
    write("migrations/001-first-orders.sql", "INSERT INTO sales.orders SELECT x, x * 10.0 FROM generate_series(1, 20) AS s(x);\n")
    write("tests/no_negative.sql", "SELECT * FROM sales.orders WHERE amount < 0\n")
    subprocess.run(["git", "add", "-A"], cwd=proj, env=env, check=True)
    subprocess.run(["git", "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-qm", "first"], cwd=proj, env=env, check=True)

    plan = pondra("plan", "--env", "prod")
    checks["pondra plan: each object made, the migration once, nothing done"] = all(f"+ {k}" in plan for k in ["table", "view", "materialized view", "macro", "procedure", "task", "role", "grant"]) \
        and "▶ migration" in plan and "first deploy" in plan and refused("prod", "SELECT * FROM sales.orders") != ""
    out = pondra("deploy", "--env", "prod", "--test")
    checks["pondra deploy --test: every object made, the migration's rows, the view's value, the tests passed"] = \
        rows("prod", "SELECT count(*) FROM sales.orders") == [(20,)] and rows("prod", "SELECT count(*) FROM sales.big") == [(11,)] \
        and rows("prod", "SELECT sum(n) FROM sales.rollup") == [(20,)] and rows("prod", "SELECT sales.double(21) AS d") == [(42,)] \
        and "✓ tests      1 passed" in out and "deploy 1" in out
    d = q("prod", "SELECT id, project, env, commit, status FROM pondra.deploys ORDER BY id")
    head = subprocess.run(["git", "rev-parse", "--short", "HEAD"], cwd=proj, env=env, capture_output=True, text=True).stdout.strip()
    checks["pondra.deploys: the deploy, its project, environment, commit and status"] = [tuple(r.values()) for r in d] == [(1, "sales", "prod", head, "ok")]
    again = pondra("deploy", "--env", "prod")
    checks["deployed again: nothing to change, the migration not run again"] = "nothing to change" in again and rows("prod", "SELECT count(*) FROM sales.orders") == [(20,)]
    plans = [pondra("plan", "--env", "prod") for _ in range(2)]
    checks["a plan made twice is the same plan (nothing to change)"] = plans[0] == plans[1] and "nothing to change" in plans[0]

    # A column added, a view changed: ALTER TABLE and a replacement.
    write("objects/sales/orders.sql", "CREATE TABLE sales.orders (\n  id BIGINT,\n  amount DOUBLE,\n  channel VARCHAR\n);\n")
    write("objects/sales/views.sql", open(os.path.join(proj, "objects/sales/views.sql")).read().replace("amount >= $big", "amount > $big"))
    plan = pondra("plan", "--env", "prod")
    pondra("deploy", "--env", "prod")
    checks["a column added and a view changed: ADD COLUMN, the view replaced"] = "ADD COLUMN channel VARCHAR" in plan and "~ view" in plan and "replaced" in plan \
        and rows("prod", "SELECT count(*) FROM sales.big") == [(10,)] and rows("prod", "SELECT count(channel) FROM sales.orders") == [(0,)]
    if not checks["a column added and a view changed: ADD COLUMN, the view replaced"]:
        checks["(added: the plan, big, a row)"] = [plan, rows("prod", "SELECT count(*) FROM sales.big"), refused("prod", "SELECT channel FROM sales.orders")]
    q("prod", "CREATE OR REPLACE VIEW sales.big AS SELECT * FROM sales.orders")
    plan = pondra("plan", "--env", "prod")
    pondra("deploy", "--env", "prod")
    checks["a view changed outside a deploy: shown as drift, put back"] = "changed outside a deploy" in plan and rows("prod", "SELECT count(*) FROM sales.big") == [(10,)]

    # A rename is a migration's.
    write("objects/sales/orders.sql", "CREATE TABLE sales.orders (\n  id BIGINT,\n  total DOUBLE,\n  channel VARCHAR\n);\n")
    write("objects/sales/views.sql", open(os.path.join(proj, "objects/sales/views.sql")).read().replace("amount", "total"))
    no = pondra("plan", "--env", "prod", ok=False)
    write("tests/no_negative.sql", "SELECT * FROM sales.orders WHERE total < 0\n")
    write("migrations/002-rename.sql", "DROP MATERIALIZED VIEW sales.rollup;\nDROP MATERIALIZED VIEW sales.totals;\nDROP VIEW sales.big;\n"
          "ALTER TABLE sales.orders RENAME COLUMN amount TO total;\n")
    yes = pondra("deploy", "--env", "prod", "--test")
    checks["a renamed column: refused, saying the migration and what reads it; with it, deployed, the views made again"] = "refused" in no and "RENAME COLUMN" in no \
        and "materialized view sales.totals" in no and rows("prod", "SELECT sum(total) FROM sales.orders") == [(2100.0,)] \
        and rows("prod", "SELECT s FROM sales.totals ORDER BY odd") == [(1100.0,), (1000.0,)] and rows("prod", "SELECT sum(n) FROM sales.rollup") == [(20,)]
    write("objects/sales/views.sql", open(os.path.join(proj, "objects/sales/views.sql")).read().replace("sum(total) AS s", "sum(total) AS s, sum(total * 2) AS twice"))
    plan = pondra("plan", "--env", "prod")
    pondra("deploy", "--env", "prod")
    checks["a materialized view changed: made again from the rows already there, with what follows it (↻)"] = "↻ materialized view sales.totals" in plan.replace("  ", " ") \
        and "sales.rollup" in plan and rows("prod", "SELECT twice FROM sales.totals ORDER BY odd") == [(2200.0,), (2000.0,)] and rows("prod", "SELECT sum(n) FROM sales.rollup") == [(20,)]

    # A failed test fails the deploy; a plan someone deployed after is refused.
    write("tests/at_most_ten.sql", "SELECT count(*) FROM sales.orders HAVING count(*) > 10\n")
    failed = pondra("deploy", "--env", "prod", "--test", ok=False)
    last = q("prod", "SELECT status, tests FROM pondra.deploys ORDER BY id DESC LIMIT 1")[0]
    checks["a failed test: the deploy exits 1, its row says tests failed, and which"] = "1 of 2 failed" in failed and "at_most_ten.sql" in failed and last["status"] == "tests_failed"
    os.remove(os.path.join(proj, "tests/at_most_ten.sql"))
    files = {}
    for root, _, names in os.walk(proj):
        for n in names:
            rel = os.path.relpath(os.path.join(root, n), proj)
            if n.endswith(".sql") or n == "pondra.toml":
                files[rel.replace(os.sep, "/")] = open(os.path.join(root, n)).read()
    shown = call("POST", "/db/prod/plan", {"files": files, "env": "prod"})["plan"]["id"]
    write("objects/sales/more.sql", "CREATE VIEW sales.small AS SELECT * FROM sales.orders WHERE total < 50;\n")
    pondra("deploy", "--env", "prod")
    try:
        call("POST", "/db/prod/deploy", {"files": files, "env": "prod", "plan": shown})
        stale = ""
    except RuntimeError as e:
        stale = str(e)
    checks["a plan shown before someone else deployed: refused, plan again"] = "plan again" in stale

    # Another project can't change this one's objects.
    other = {"pondra.toml": '[project]\nname = "crm"\n', "objects/o.sql": "CREATE VIEW sales.big AS SELECT 1 AS x;\n"}
    p = call("POST", "/db/prod/plan", {"files": other})
    checks["another project declaring this one's view: refused, naming its owner"] = any("project sales's" in r for r in p["plan"]["refused"])

    # Shares and recipients are each database's own: a plan never drops prod's, and a project can't
    # declare them (a branch would carry a partner's grant into dev).
    for sql in ["CREATE SHARE acme", "ALTER SHARE acme ADD TABLE sales.orders", "CREATE RECIPIENT acme_corp", "GRANT SELECT ON SHARE acme TO RECIPIENT acme_corp"]:
        q("prod", sql)
    pruned = pondra("plan", "--env", "prod", "--prune")
    try:
        call("POST", "/db/prod/plan", {"files": {**files, "objects/share.sql": "CREATE SHARE acme;\nGRANT SELECT ON SHARE acme TO RECIPIENT acme_corp;\n"}, "env": "prod"})
        declared = ""
    except RuntimeError as e:
        declared = str(e)
    checks["prod's shares and recipients: never in a plan, even pruning; a project declaring one refused by name"] = \
        "acme" not in pruned and "shares and recipients are each environment's" in declared and rows("prod", "SELECT count(*) FROM pondra.shares") == [(1,)]

    # Export, then deploy the export into an empty database: it exports the same.
    out_dir = os.path.join(work, "exported")
    pondra("export", out_dir, "--env", "prod")
    exported = {os.path.relpath(os.path.join(r, n), out_dir): open(os.path.join(r, n)).read() for r, _, ns in os.walk(out_dir) for n in ns}
    with open(os.path.join(out_dir, "pondra.toml"), "w") as f:
        f.write(f'[project]\nname = "copy"\nserver = "{base}"\n\n[env.prod]\nvalues = {{ big = 100 }}\n')
    r = subprocess.run([bin, "deploy", "--env", "empty", "--project", out_dir], env=env, capture_output=True, text=True, timeout=300)
    again = call("GET", "/db/empty/export")
    same = {k: v for k, v in again.items() if k != "pondra.toml"} == {k: v for k, v in exported.items() if k.startswith("objects")}
    checks["pondra export of prod, deployed into an empty database, exports the same (no share or recipient)"] = r.returncode == 0 and same and "objects/sales/orders.sql" in exported \
        and not any("acme" in v for v in exported.values())
    if not same:
        checks["(export differs)"] = sorted(set(exported.items()) ^ set(again.items()))[:6]

    # A branch: its own view and rows, told apart from prod; its task suspended.
    subprocess.run(["git", "switch", "-q", "-c", "orders-by-channel"], cwd=proj, env=env, check=True)
    made = pondra("branch")
    write("objects/sales/channels.sql", "CREATE VIEW sales.by_channel AS SELECT channel, count(*) AS n FROM sales.orders GROUP BY channel;\n")
    deployed = pondra("deploy", "--test")
    q("orders_by_channel", "INSERT INTO sales.orders VALUES (100, 5.0, 'web')")
    q("orders_by_channel", "DELETE FROM sales.orders WHERE id = 1")
    diff = pondra("diff")
    checks["pondra branch: a database named after the git branch, prod as it is"] = "orders_by_channel" in made and rows("orders_by_channel", "SELECT sum(n) FROM sales.rollup") == [(20,)]
    checks["…deployed to with no --env: its view made, its values [env.dev]'s"] = "orders_by_channel: deploy" in deployed and rows("orders_by_channel", "SELECT count(*) FROM sales.big") == [(15,)]
    checks["…its task suspended"] = rows("orders_by_channel", "SELECT state FROM pondra.tasks WHERE name = 'sales.nightly'") == [("suspended",)]
    checks["pondra diff: the branch's new object and its rows apart from prod's"] = "`+` objects/sales/by_channel.sql" in diff and "| sales.orders | 1 | 0 | 1 |" in diff
    if not checks["pondra diff: the branch's new object and its rows apart from prod's"]:
        checks["(diff)"] = diff
    pondra("branch", "--drop")
    checks["pondra branch --drop: gone"] = "orders_by_channel" not in [d["name"] for d in call("GET", "/databases")]

    # CALL plan and deploy on a project kept in the workspace.
    for rel, text in files.items():
        call("PUT", f"/db/prod/files/projects/sales/{rel}", text.encode())
    call("PUT", "/db/prod/files/projects/sales/objects/sales/more.sql", b"CREATE VIEW sales.small AS SELECT * FROM sales.orders WHERE total < 30;\n")
    planned = q("prod", "CALL plan('projects/sales', env => 'prod')")
    deployed = q("prod", "CALL deploy('projects/sales', env => 'prod', test => true)")
    checks["CALL plan and CALL deploy on a project in the workspace: its rows, then done"] = any(r["name"] == "sales.small" and r["mark"] == "~" and r["change"] == "replace" for r in planned) \
        and any(r["name"] == "sales.small" for r in deployed) and rows("prod", "SELECT count(*) FROM sales.small") == [(2,)]
    checks["each deploy's files are kept, out of files()"] = call("GET", "/db/prod/files/.deploys/1/pondra.toml") is not None \
        and not any(".deploys" in r["path"] for r in q("prod", "SELECT path FROM files()"))

    # The developer's day: the CI workflow, branches made by hand and by the git hook, and pondra dev.
    subprocess.run(["git", "switch", "-q", "main"], cwd=proj, env=env, check=True)
    wf_path = os.path.join(proj, ".github", "workflows", "pondra.yml")
    ci_toml = open(os.path.join(proj, "pondra.toml")).read()
    said = pondra("ci", "init")
    jobs = yaml.safe_load(open(wf_path).read())["jobs"]  # (PyYAML reads the key `on` as True: the jobs are read under `jobs`)
    checks["pondra ci init: the pull-request, closed and prod jobs (no test: pondra.toml has no [env.test]); the four things to do"] = \
        set(jobs) == {"pull-request", "closed", "prod"} and jobs["prod"]["environment"] == "prod" and "needs" not in jobs["prod"] \
        and "GRANT DEPLOY ON DATABASE prod TO ci" in said and "protected = true" in said
    again = pondra("ci", "init", ok=False)
    checks["pondra ci init again: refused without --force, saying so"] = "--force" in again
    write("pondra.toml", ci_toml + '\n[env.test]\nclone = "prod"\n')
    pondra("ci", "init", "--force")
    jobs = yaml.safe_load(open(wf_path).read())["jobs"]
    checks["pondra ci init --force with [env.test]: a test job, and prod needs it"] = jobs.get("test", {}).get("if") == "github.event_name == 'push'" and jobs["prod"].get("needs") == "test"
    write("pondra.toml", ci_toml)

    subprocess.run(["git", "switch", "-q", "-c", "hooked-by-hand"], cwd=proj, env=env, check=True)
    pondra("branch", "--if-missing")
    second = pondra("branch", "--if-missing")
    checks["pondra branch --if-missing twice on a new git branch: both exit 0, the database there once, the second saying so"] = \
        [d["name"] for d in call("GET", "/databases")].count("hooked_by_hand") == 1 and "there already" in second

    pondra("branch", "--hook")
    hook_env = {**env, "PATH": os.path.dirname(bin) + os.pathsep + env["PATH"]}  # (the hook finds pondra on the PATH)
    subprocess.run(["git", "switch", "-q", "-c", "via-hook"], cwd=proj, env=hook_env, check=True)
    checks["pondra branch --hook: git switch -c via-hook made its database (the hook ran pondra)"] = "via_hook" in [d["name"] for d in call("GET", "/databases")]
    os.remove(os.path.join(proj, ".git", "hooks", "post-checkout"))  # (the next switch is pondra dev's own: no hook makes its database first)

    subprocess.run(["git", "switch", "-q", "main"], cwd=proj, env=env, check=True)
    refused_main = pondra("dev", ok=False)
    checks["pondra dev on main: refused, saying git switch"] = "git switch" in refused_main

    subprocess.run(["git", "switch", "-q", "-c", "dev-loop"], cwd=proj, env=env, check=True)
    dev_log = os.path.join(work, "dev.log")
    with open(dev_log, "w") as log:
        dev = subprocess.Popen([bin, "dev", "--deploys", "2"], cwd=proj, env=env, stdout=log, stderr=subprocess.STDOUT)
    try:
        deadline = time.time() + 120
        while time.time() < deadline and not ("dev_loop: deploy" in open(dev_log).read() and "dev_loop" in [d["name"] for d in call("GET", "/databases")]):
            time.sleep(0.5)
        write("objects/sales/dev_loop.sql", "CREATE VIEW sales.dev_loop AS SELECT count(*) AS n FROM sales.orders;\n")
        code = dev.wait(timeout=120)
    finally:
        if dev.poll() is None:
            dev.kill()
    dev_out = open(dev_log).read()
    checks["pondra dev --deploys 2 on git branch dev-loop: its database made and deployed, a save deployed again (timed), exit 0"] = \
        code == 0 and dev_out.count("dev_loop: deploy") == 2 and re.search(r"\d\d:\d\d:\d\d changed: objects/sales/dev_loop.sql", dev_out) is not None
    if not checks["pondra dev --deploys 2 on git branch dev-loop: its database made and deployed, a save deployed again (timed), exit 0"]:
        checks["(dev)"] = [code, dev_out[-800:]]
    checks["…the view saved is answered by the branch"] = rows("dev_loop", "SELECT n FROM sales.dev_loop") == [(20,)]
    os.remove(os.path.join(proj, "objects", "sales", "dev_loop.sql"))
    pondra("branch", "--drop")
    subprocess.run(["git", "switch", "-q", "main"], cwd=proj, env=env, check=True)

    server.terminate()
    server.wait(10)
    return checks


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--new", default=os.path.join(HERE, "..", "target", "release", "pondra"), help="this build's binary")
    ap.add_argument("--work", default="", help="where the lakes and the project go (default: a new temporary folder, removed if every check passes)")
    ap.add_argument("--port", type=int, default=9790)
    a = ap.parse_args()
    work = a.work or tempfile.mkdtemp(prefix="pondra-project-")
    os.makedirs(work, exist_ok=True)
    try:
        checks = project_check(os.path.abspath(a.new), work, a.port)
    except Exception as e:
        checks = {"ran to the end": False, "error": str(e)}
    finally:
        # (by the binary's own name: a copy under another name escapes `pgrep -x pondra`)
        for p in subprocess.run(["pgrep", "-x", os.path.basename(a.new)[:15]], capture_output=True, text=True).stdout.split():
            try:
                if work in open(f"/proc/{p}/cmdline").read():
                    os.kill(int(p), 15)
            except OSError:
                pass
    ok = all(v is True for k, v in checks.items() if k not in ("error",) and not k.startswith("("))
    print(json.dumps({**checks, "ok": ok}, indent=1, ensure_ascii=False))
    if ok and not a.work:
        shutil.rmtree(work, ignore_errors=True)
    elif not ok:
        print(f"(the lakes and the project kept in {work})", file=sys.stderr)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
