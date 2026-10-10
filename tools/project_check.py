#!/usr/bin/env python3
"""A project made true in its databases (ADR-047 §4): `pondra init`, `plan`, `apply`, `test`,
`export`, `branch` and `diff` against a folder of lakes (`pondra serve --lakes`), as a team would.

- the first apply makes every object, runs the migration once and the tests; a second finds nothing;
- a column added and a view changed are ALTER TABLE and a replacement; drift is put back;
- a renamed column is refused with the migration to write, and passes with it;
- a materialized view changed is made again with what follows it;
- a failed test fails the apply; a plan shown before someone else applied is refused;
- another project can't change this one's objects;
- `pondra export` of prod, applied into an empty database, exports the same;
- `pondra login` with no URL: the project's server;
- `pondra apply test`: a clone made once and kept; `--fresh` makes it again; a name nobody declared is a branch of prod;
- `pondra apply fresh_prod`: a database nobody made is made on its server, and applied;
- `pondra branch NAME`, `pondra branch` (the list), `pondra branch -d NAME`, `pondra diff`: a branch's view and rows;
- `pondra apply --watch --applies 2`: each save applied and tested; `--watch` on prod is refused;
- `CALL plan(…)` and `CALL apply(…)` on a project kept in the workspace; `pondra.applies`;
- `pondra switch NAME`: a branch that exists is checked out with its database made; one that doesn't is refused;
- the developer's day: `pondra ci init`: the pull-request, closed and prod jobs.

  project_check.py [--new target/release/pondra] [--work DIR] [--port 9790]

Prints the checks as JSON and exits 1 if one fails (the lakes and the project are then kept in --work).
"""
import argparse, json, os, re, shutil, subprocess, sys, tempfile, time, urllib.request, urllib.error
import yaml

HERE = os.path.dirname(os.path.abspath(__file__))


def runs(job):
    """The `run:` lines of a workflow job's steps."""
    return [s.get("run", "") for s in job.get("steps", [])]


def put(root, rel, text):
    """Writes a file under `root`, making its folders."""
    path = os.path.join(root, rel)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.write(text)


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
        put(proj, rel, text)

    def git_now():
        return subprocess.run(["git", "rev-parse", "--abbrev-ref", "HEAD"], cwd=proj, env=env, capture_output=True, text=True).stdout.strip()

    def db_names():
        return [d["name"] for d in call("GET", "/databases")]

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
    checks["pondra init: pondra.toml (named after the folder), objects/, migrations/, tests/, a .gitignore leaving out /lake/"] = 'name = "sales"' in toml \
        and all(os.path.isdir(os.path.join(proj, d)) for d in ["objects", "migrations", "tests"]) and "/lake/" in open(os.path.join(proj, ".gitignore")).read().split()
    base_toml = f'[project]\nname = "sales"\nserver = "{base}"\n\n[env.prod]\nvalues = {{ big = 100 }}\n\n[env.dev]\nvalues = {{ big = 50 }}\n'
    write("pondra.toml", base_toml)
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

    plan = pondra("plan", "prod")
    checks["pondra plan: each object made, the migration once, nothing done"] = all(f"+ {k}" in plan for k in ["table", "view", "materialized view", "macro", "procedure", "task", "role", "grant"]) \
        and "▶ migration" in plan and "first apply" in plan and refused("prod", "SELECT * FROM sales.orders") != ""
    out = pondra("apply", "prod")
    checks["pondra apply prod: every object made, the migration's rows, the view's value, the tests passed"] = \
        rows("prod", "SELECT count(*) FROM sales.orders") == [(20,)] and rows("prod", "SELECT count(*) FROM sales.big") == [(11,)] \
        and rows("prod", "SELECT sum(n) FROM sales.rollup") == [(20,)] and rows("prod", "SELECT sales.double(21) AS d") == [(42,)] \
        and "✓ tests      1 passed" in out and "apply 1" in out
    d = q("prod", "SELECT id, project, env, commit, status FROM pondra.applies ORDER BY id")
    head = subprocess.run(["git", "rev-parse", "--short", "HEAD"], cwd=proj, env=env, capture_output=True, text=True).stdout.strip()
    checks["pondra.applies: the apply, its project, environment, commit and status"] = [tuple(r.values()) for r in d] == [(1, "sales", "prod", head, "ok")]
    again = pondra("apply", "prod")
    checks["applied again: nothing to change, the migration not run again"] = "nothing to change" in again and rows("prod", "SELECT count(*) FROM sales.orders") == [(20,)]
    plans = [pondra("plan", "prod") for _ in range(2)]
    checks["a plan made twice is the same plan (nothing to change)"] = plans[0] == plans[1] and "nothing to change" in plans[0]

    # A column added, a view changed: ALTER TABLE and a replacement.
    write("objects/sales/orders.sql", "CREATE TABLE sales.orders (\n  id BIGINT,\n  amount DOUBLE,\n  channel VARCHAR\n);\n")
    write("objects/sales/views.sql", open(os.path.join(proj, "objects/sales/views.sql")).read().replace("amount >= $big", "amount > $big"))
    plan = pondra("plan", "prod")
    pondra("apply", "prod")
    checks["a column added and a view changed: ADD COLUMN, the view replaced"] = "ADD COLUMN channel VARCHAR" in plan and "~ view" in plan and "replaced" in plan \
        and rows("prod", "SELECT count(*) FROM sales.big") == [(10,)] and rows("prod", "SELECT count(channel) FROM sales.orders") == [(0,)]
    if not checks["a column added and a view changed: ADD COLUMN, the view replaced"]:
        checks["(added: the plan, big, a row)"] = [plan, rows("prod", "SELECT count(*) FROM sales.big"), refused("prod", "SELECT channel FROM sales.orders")]
    q("prod", "CREATE OR REPLACE VIEW sales.big AS SELECT * FROM sales.orders")
    plan = pondra("plan", "prod")
    pondra("apply", "prod")
    checks["a view changed outside an apply: shown as drift, put back"] = "changed outside an apply" in plan and rows("prod", "SELECT count(*) FROM sales.big") == [(10,)]

    # pondra apply local: this machine's own database, the lake in the project's lake/ (where pondra opens its
    # shell), with no server; --watch keeps its node up, and git leaves it out.
    local_log = os.path.join(work, "local.log")
    with open(local_log, "w") as log:
        lw = subprocess.Popen([bin, "apply", "local", "--watch", "--applies", "2"], cwd=proj, env=env, stdout=log, stderr=subprocess.STDOUT)
    try:
        deadline = time.time() + 120
        while time.time() < deadline and "local: apply" not in open(local_log).read():
            time.sleep(0.5)
        write("objects/sales/local_loop.sql", "CREATE VIEW sales.local_loop AS SELECT count(*) AS n FROM sales.orders;\n")
        code = lw.wait(timeout=120)
    finally:
        if lw.poll() is None:
            lw.kill()
    local_out = open(local_log).read()
    consoles = set(re.findall(r"console: (http://127\.0\.0\.1:\d+)/", local_out))
    name = "pondra apply local --watch --applies 2: the project's lake/ made and applied with no server, a save applied again on the same node (one console), exit 0"
    checks[name] = code == 0 and local_out.count("local: apply") == 2 and os.path.isdir(os.path.join(proj, "lake")) and len(consoles) == 1 \
        and "local" not in db_names()
    if not checks[name]:
        checks["(local)"] = [code, local_out[-800:]]
    os.remove(os.path.join(proj, "objects", "sales", "local_loop.sql"))
    shell = subprocess.run([bin], cwd=proj, env=env, input="SELECT n AS local_rows FROM sales.local_loop;\n", capture_output=True, text=True, timeout=120)
    ignored = subprocess.run(["git", "status", "--porcelain"], cwd=proj, env=env, capture_output=True, text=True).stdout
    checks["…pondra in the project's folder opens that lake (the view saved: the migration's 20 rows), and git leaves lake/ out"] = \
        "local_rows" in shell.stdout and re.search(r"\b20\b", shell.stdout) is not None and "lake" not in ignored
    if not checks["…pondra in the project's folder opens that lake (the view saved: the migration's 20 rows), and git leaves lake/ out"]:
        checks["(shell)"] = [shell.stdout[-400:], shell.stderr[-400:], ignored]

    # A rename is a migration's.
    write("objects/sales/orders.sql", "CREATE TABLE sales.orders (\n  id BIGINT,\n  total DOUBLE,\n  channel VARCHAR\n);\n")
    write("objects/sales/views.sql", open(os.path.join(proj, "objects/sales/views.sql")).read().replace("amount", "total"))
    no = pondra("plan", "prod", ok=False)
    write("tests/no_negative.sql", "SELECT * FROM sales.orders WHERE total < 0\n")
    write("migrations/002-rename.sql", "DROP MATERIALIZED VIEW sales.rollup;\nDROP MATERIALIZED VIEW sales.totals;\nDROP VIEW sales.big;\n"
          "ALTER TABLE sales.orders RENAME COLUMN amount TO total;\n")
    yes = pondra("apply", "prod")
    checks["a renamed column: refused, saying the migration and what reads it; with it, applied, the views made again"] = "refused" in no and "RENAME COLUMN" in no \
        and "materialized view sales.totals" in no and rows("prod", "SELECT sum(total) FROM sales.orders") == [(2100.0,)] \
        and rows("prod", "SELECT s FROM sales.totals ORDER BY odd") == [(1100.0,), (1000.0,)] and rows("prod", "SELECT sum(n) FROM sales.rollup") == [(20,)]
    write("objects/sales/views.sql", open(os.path.join(proj, "objects/sales/views.sql")).read().replace("sum(total) AS s", "sum(total) AS s, sum(total * 2) AS twice"))
    plan = pondra("plan", "prod")
    pondra("apply", "prod")
    checks["a materialized view changed: made again from the rows already there, with what follows it (↻)"] = "↻ materialized view sales.totals" in plan.replace("  ", " ") \
        and "sales.rollup" in plan and rows("prod", "SELECT twice FROM sales.totals ORDER BY odd") == [(2200.0,), (2000.0,)] and rows("prod", "SELECT sum(n) FROM sales.rollup") == [(20,)]

    # A failed test fails the apply; a plan someone applied after is refused.
    write("tests/at_most_ten.sql", "SELECT count(*) FROM sales.orders HAVING count(*) > 10\n")
    failed = pondra("apply", "prod", ok=False)
    last = q("prod", "SELECT status, tests FROM pondra.applies ORDER BY id DESC LIMIT 1")[0]
    checks["a failed test: the apply exits 1, its row says tests failed, and which"] = "1 of 2 failed" in failed and "at_most_ten.sql" in failed and last["status"] == "tests_failed"
    os.remove(os.path.join(proj, "tests/at_most_ten.sql"))
    files = {}
    for root, dirs, names in os.walk(proj):
        dirs[:] = [d for d in dirs if d != "lake"]  # (apply local's lake: rows, not the project's files)
        for n in names:
            rel = os.path.relpath(os.path.join(root, n), proj)
            if n.endswith(".sql") or n == "pondra.toml":
                files[rel.replace(os.sep, "/")] = open(os.path.join(root, n)).read()
    shown = call("POST", "/db/prod/plan", {"files": files, "env": "prod"})["plan"]["id"]
    write("objects/sales/more.sql", "CREATE VIEW sales.small AS SELECT * FROM sales.orders WHERE total < 50;\n")
    pondra("apply", "prod")
    try:
        call("POST", "/db/prod/apply", {"files": files, "env": "prod", "plan": shown})
        stale = ""
    except RuntimeError as e:
        stale = str(e)
    checks["a plan shown before someone else applied: refused, plan again"] = "plan again" in stale

    # Another project can't change this one's objects.
    other = {"pondra.toml": '[project]\nname = "crm"\n', "objects/o.sql": "CREATE VIEW sales.big AS SELECT 1 AS x;\n"}
    p = call("POST", "/db/prod/plan", {"files": other})
    checks["another project declaring this one's view: refused, naming its owner"] = any("project sales's" in r for r in p["plan"]["refused"])

    # Shares and recipients are each database's own: a plan never drops prod's, and a project can't
    # declare them (a branch would carry a partner's grant into dev).
    for sql in ["CREATE SHARE acme", "ALTER SHARE acme ADD TABLE sales.orders", "CREATE RECIPIENT acme_corp", "GRANT SELECT ON SHARE acme TO RECIPIENT acme_corp"]:
        q("prod", sql)
    pruned = pondra("plan", "prod", "--prune")
    try:
        call("POST", "/db/prod/plan", {"files": {**files, "objects/share.sql": "CREATE SHARE acme;\nGRANT SELECT ON SHARE acme TO RECIPIENT acme_corp;\n"}, "env": "prod"})
        declared = ""
    except RuntimeError as e:
        declared = str(e)
    checks["prod's shares and recipients: never in a plan, even pruning; a project declaring one refused by name"] = \
        "acme" not in pruned and "shares and recipients are each environment's" in declared and rows("prod", "SELECT count(*) FROM pondra.shares") == [(1,)]

    # Export, then apply the export into an empty database: it exports the same.
    out_dir = os.path.join(work, "exported")
    pondra("export", out_dir, "--env", "prod")
    exported = {os.path.relpath(os.path.join(r, n), out_dir): open(os.path.join(r, n)).read() for r, _, ns in os.walk(out_dir) for n in ns}
    put(out_dir, "pondra.toml", f'[project]\nname = "copy"\nserver = "{base}"\n\n[env.prod]\nvalues = {{ big = 100 }}\n\n[env.empty]\nvalues = {{ big = 100 }}\n')
    r = subprocess.run([bin, "apply", "empty", "--project", out_dir], env=env, capture_output=True, text=True, timeout=300)
    again = call("GET", "/db/empty/export")
    same = {k: v for k, v in again.items() if k != "pondra.toml"} == {k: v for k, v in exported.items() if k.startswith("objects")}
    checks["pondra export of prod, applied into an empty database, exports the same (no share or recipient)"] = r.returncode == 0 and same and "objects/sales/orders.sql" in exported \
        and not any("acme" in v for v in exported.values())
    if not same:
        checks["(export differs)"] = sorted(set(exported.items()) ^ set(again.items()))[:6]

    # On main nothing is named: refused, and the message says what to do. prod is not a branch, and
    # --watch never applies to it.
    no_env = pondra("apply", ok=False)
    checks["pondra apply on main: refused, naming test, prod and pondra branch"] = all(s in no_env for s in ["pondra apply test", "pondra apply prod", "pondra branch"])
    checks["pondra apply prod --watch: refused"] = "not to prod" in pondra("apply", "prod", "--watch", ok=False)
    checks["pondra branch prod: refused, it's an environment"] = "is an environment" in pondra("branch", "prod", ok=False)

    # A name nobody declared is a branch of prod, and says so; its database is dropped again.
    prdo = pondra("apply", "prdo")
    checks["pondra apply prdo (a name nobody declared): a branch of prod, saying so"] = "prdo: made from prod" in prdo and rows("prdo", "SELECT sum(n) FROM sales.rollup") == [(20,)]
    pondra("branch", "-d", "prdo")

    # A clone environment: made once from prod and kept; --fresh makes it again.
    write("pondra.toml", base_toml + '\n[env.test]\nclone = "prod"\nvalues = { big = 100 }\n')
    first = pondra("apply", "test")
    q("test", "INSERT INTO sales.orders (id, total, channel) VALUES (500, 5.0, 'web')")
    again = pondra("apply", "test")
    kept = rows("test", "SELECT count(*) FROM sales.orders WHERE id = 500") == [(1,)]
    fresh_out = pondra("apply", "test", "--fresh")
    gone = rows("test", "SELECT count(*) FROM sales.orders WHERE id = 500") == [(0,)]
    checks["pondra apply test with [env.test] clone = \"prod\": made once from prod and kept (a row written there survives a second apply); --fresh makes it again (the row gone)"] = \
        "made from prod" in first and "made from" not in again and kept and "made from prod" in fresh_out and gone
    write("pondra.toml", base_toml)

    # The first apply to a database nobody made: a project of its own, whose environment has no clone.
    fresh = os.path.join(work, "fresh")
    put(fresh, "pondra.toml", f'[project]\nname = "fresh"\nserver = "{base}"\n\n[env.fresh_prod]\nvalues = {{ big = 100 }}\n')
    put(fresh, "objects/schemas.sql", "CREATE SCHEMA sales;\n")
    put(fresh, "objects/sales/orders.sql", "CREATE TABLE sales.orders (\n  id BIGINT,\n  amount DOUBLE\n);\n")
    made_out = pondra("apply", "fresh_prod", "--project", fresh)
    checks["pondra apply fresh_prod (no clone, not there yet): a new database on the server, applied, its table there"] = \
        "a new database on" in made_out and "fresh_prod: apply" in made_out and rows("fresh_prod", "SELECT count(*) FROM sales.orders") == [(0,)]

    # A branch: a Git branch and a database of its name, prod as it is; its own view and rows.
    made = pondra("branch", "orders-by-channel")
    checks["pondra branch: a git branch and a database named after it, prod as it is"] = "a branch with prod's data" in made \
        and git_now() == "orders-by-channel" and rows("orders_by_channel", "SELECT sum(n) FROM sales.rollup") == [(20,)]
    write("objects/sales/channels.sql", "CREATE VIEW sales.by_channel AS SELECT channel, count(*) AS n FROM sales.orders GROUP BY channel;\n")
    applied = pondra("apply")
    checks["…applied with no ENV: its view made, its values [env.dev]'s"] = "orders_by_channel: apply" in applied and rows("orders_by_channel", "SELECT count(*) FROM sales.big") == [(15,)]
    checks["…its task suspended"] = rows("orders_by_channel", "SELECT state FROM pondra.tasks WHERE name = 'sales.nightly'") == [("suspended",)]
    q("orders_by_channel", "INSERT INTO sales.orders VALUES (100, 5.0, 'web')")
    q("orders_by_channel", "DELETE FROM sales.orders WHERE id = 1")
    diff = pondra("diff")
    checks["pondra diff: the branch's new object and its rows apart from prod's"] = "`+` objects/sales/by_channel.sql" in diff and "| sales.orders | 1 | 0 | 1 |" in diff
    if not checks["pondra diff: the branch's new object and its rows apart from prod's"]:
        checks["(diff)"] = diff
    os.remove(os.path.join(proj, "objects/sales/channels.sql"))

    # A second branch, then the list: * on the current git branch's database.
    pondra("branch", "returns-report")
    listing = pondra("branch")
    checks["pondra branch with no name: the branches, * on the current one"] = "* returns_report" in listing and "  orders_by_channel" in listing \
        and "from prod" in listing and "no branches yet" not in listing
    subprocess.run(["git", "switch", "-q", "orders-by-channel"], cwd=proj, env=env, check=True)
    dropped = pondra("branch", "-d", "orders-by-channel")
    checks["pondra branch -d: its database dropped; its git branch stays while it is checked out, and the output says so"] = \
        "orders_by_channel" not in db_names() and "its database dropped" in dropped and "the git branch stays" in dropped
    subprocess.run(["git", "switch", "-q", "main"], cwd=proj, env=env, check=True)
    dropped = pondra("branch", "-d", "returns-report")
    checks["pondra branch -d: a merged git branch goes with its database"] = "and its git branch" in dropped and "returns_report" not in db_names() \
        and subprocess.run(["git", "rev-parse", "--verify", "--quiet", "refs/heads/returns-report"], cwd=proj, env=env).returncode != 0

    # pondra switch: a branch that exists is checked out with its database made; one that doesn't is refused.
    subprocess.run(["git", "branch", "other"], cwd=proj, env=env, check=True)
    was_there = "other" in db_names()
    switched = pondra("switch", "other")
    checks["pondra switch to a branch that exists: git's branch is the current one, its database made if it was missing"] = \
        not was_there and "other: switched to it" in switched and "made from prod" in switched and "next: pondra apply" in switched \
        and git_now() == "other" and "other" in db_names()
    nope = pondra("switch", "nobody-yet", ok=False)
    checks["pondra switch to a branch that doesn't exist: refused, saying pondra branch"] = "pondra branch nobody-yet" in nope and git_now() == "other"
    on_main = pondra("switch", "main")
    checks["pondra switch main: git's main line, no database made, the environments named"] = \
        git_now() == "main" and "pondra apply test or pondra apply prod" in on_main and "main" not in db_names()
    pondra("branch", "-d", "other")  # (cleanup: its database and git branch)
    # CALL plan and apply on a project kept in the workspace.
    for rel, text in files.items():
        call("PUT", f"/db/prod/files/projects/sales/{rel}", text.encode())
    call("PUT", "/db/prod/files/projects/sales/objects/sales/more.sql", b"CREATE VIEW sales.small AS SELECT * FROM sales.orders WHERE total < 30;\n")
    planned = q("prod", "CALL plan('projects/sales', env => 'prod')")
    applied = q("prod", "CALL apply('projects/sales', env => 'prod', test => true)")
    checks["CALL plan and CALL apply on a project in the workspace: its rows, then done"] = any(r["name"] == "sales.small" and r["mark"] == "~" and r["change"] == "replace" for r in planned) \
        and any(r["name"] == "sales.small" for r in applied) and rows("prod", "SELECT count(*) FROM sales.small") == [(2,)]
    # (the folder keeps its stored name, files/.deploys/: a path the database has, not a word of the command)
    checks["each apply's files are kept, out of files()"] = call("GET", "/db/prod/files/.deploys/1/pondra.toml") is not None \
        and not any(".deploys" in r["path"] for r in q("prod", "SELECT path FROM files()"))

    # pondra apply --watch: each save is applied and tested, on the git branch's database.
    subprocess.run(["git", "switch", "-q", "-c", "dev-loop"], cwd=proj, env=env, check=True)
    dev_log = os.path.join(work, "dev.log")
    with open(dev_log, "w") as log:
        dev = subprocess.Popen([bin, "apply", "--watch", "--applies", "2"], cwd=proj, env=env, stdout=log, stderr=subprocess.STDOUT)
    try:
        deadline = time.time() + 120
        while time.time() < deadline and not ("dev_loop: apply" in open(dev_log).read() and "dev_loop" in db_names()):
            time.sleep(0.5)
        write("objects/sales/dev_loop.sql", "CREATE VIEW sales.dev_loop AS SELECT count(*) AS n FROM sales.orders;\n")
        code = dev.wait(timeout=120)
    finally:
        if dev.poll() is None:
            dev.kill()
    dev_out = open(dev_log).read()
    checks["pondra apply --watch --applies 2 on git branch dev-loop: its database made and applied, a save applied again (timed), exit 0"] = \
        code == 0 and dev_out.count("dev_loop: apply") == 2 and re.search(r"\d\d:\d\d:\d\d changed: objects/sales/dev_loop.sql", dev_out) is not None
    if not checks["pondra apply --watch --applies 2 on git branch dev-loop: its database made and applied, a save applied again (timed), exit 0"]:
        checks["(dev)"] = [code, dev_out[-800:]]
    checks["…the view saved is answered by the branch"] = rows("dev_loop", "SELECT n FROM sales.dev_loop") == [(20,)]
    os.remove(os.path.join(proj, "objects", "sales", "dev_loop.sql"))
    pondra("branch", "-d", "dev-loop")  # (checked out: its git branch stays)
    subprocess.run(["git", "switch", "-q", "main"], cwd=proj, env=env, check=True)

    # The developer's day: the CI workflow for a project with and without [env.test].
    wf_path = os.path.join(proj, ".github", "workflows", "pondra.yml")
    ci_toml = open(os.path.join(proj, "pondra.toml")).read()
    said = pondra("ci", "init")
    jobs = yaml.safe_load(open(wf_path).read())["jobs"]  # (PyYAML reads the key `on` as True: the jobs are read under `jobs`)
    checks["pondra ci init: the pull-request, closed and prod jobs (no test: pondra.toml has no [env.test]); the four things to do"] = \
        set(jobs) == {"pull-request", "closed", "prod"} and jobs["prod"]["environment"] == "prod" and "needs" not in jobs["prod"] \
        and "CREATE USER ci_prod" in said and "GRANT APPLY ON DATABASE prod TO ci_prod" in said and "protected = true" in said
    pr_runs, closed_runs = runs(jobs["pull-request"]), runs(jobs["closed"])
    checks["pondra ci init: a pull request applies pr-N --fresh and diffs it; closed drops only pr-N's database (no BRANCH env, no branch -d of the head ref)"] = \
        "pondra apply pr-${{ github.event.number }} --fresh" in pr_runs and "pondra diff pr-${{ github.event.number }} >> \"$GITHUB_STEP_SUMMARY\"" in pr_runs \
        and "BRANCH" not in jobs["closed"]["env"] and not any("$BRANCH" in r for r in closed_runs) and "pondra branch -d pr-${{ github.event.number }}" in closed_runs
    checks["pondra ci init: prod applies with PONDRA_PROD_TOKEN; the pull request with PONDRA_DEV_TOKEN"] = \
        jobs["prod"]["env"]["PONDRA_TOKEN"] == "${{ secrets.PONDRA_PROD_TOKEN }}" and jobs["pull-request"]["env"]["PONDRA_TOKEN"] == "${{ secrets.PONDRA_DEV_TOKEN }}" \
        and "pondra apply prod" in runs(jobs["prod"])
    again = pondra("ci", "init", ok=False)
    checks["pondra ci init again: refused without --force, saying so"] = "--force" in again
    write("pondra.toml", ci_toml + '\n[env.test]\nclone = "prod"\n')
    pondra("ci", "init", "--force")
    jobs = yaml.safe_load(open(wf_path).read())["jobs"]
    checks["pondra ci init --force with [env.test]: a test job with PONDRA_TEST_TOKEN, and prod needs it"] = \
        jobs.get("test", {}).get("if") == "github.event_name == 'push'" and jobs["test"]["env"]["PONDRA_TOKEN"] == "${{ secrets.PONDRA_TEST_TOKEN }}" \
        and jobs["prod"].get("needs") == "test"
    write("pondra.toml", ci_toml)

    # Signed in with no URL: the project's server (the open server answers at its database prod).
    login_out = pondra("login", "--token", "x")
    try:
        saved = json.load(open(os.path.join(work, "home", "login.json"))).get(base)
    except (OSError, ValueError):
        saved = None
    checks["pondra login with no URL: signed in to the project's server"] = f"signed in to {base}" in login_out and saved == "x"

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
