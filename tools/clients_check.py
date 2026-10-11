#!/usr/bin/env python3
"""Pondra as dbt and BI tools use it (ADR-030): its Postgres port, with Postgres's catalog.

  clients_check.py [dbt,psql,sqlalchemy,psycopg,jdbc,odbc,adbc,npgsql,params] [--port 8870] [--pg16 PORT]

  (params takes a Postgres 16 running on PORT with --pg16, else starts one under the work folder.)

- dbt: a project with seeds, a view, a table, incremental models (delete+insert, merge,
  append), a snapshot and data tests, run twice — the second time with changed seeds, so views
  and tables are replaced by renames and the incremental models take their incremental path —
  then `dbt docs generate`; the same project against Postgres 16, and every model's rows alike.
- psql: `\\dt`, `\\d`, `\\dv`, `\\dn`, `\\df`, `\\l`, `\\du` show the lake's tables, columns (types,
  NOT NULL, defaults, keys), views, schemas, functions and databases; `\\dT+` lists an enum type
  with its labels in order, and `\\d` shows a column of it by the type's name; a cursor in a
  transaction; Ctrl-C (SIGINT) on a running statement cancels it.
- sqlalchemy: the inspector (schemas, tables, views, columns, primary keys, a view's definition,
  an enum type's labels), with psycopg 2 and 3; pandas' `to_sql` and `read_sql`.
- psycopg: psycopg 3's named cursor (a server-side cursor, read a part at a time), its cursor
  statements by hand (tags, MOVE, FETCH, CLOSE), and `conn.cancel()` on a running statement.
- jdbc: pgjdbc's DatabaseMetaData (what DBeaver, Metabase and Tableau's JDBC connector call):
  catalogs, schemas, tables, columns, primary keys, then a query.
- odbc: psqlODBC's catalog functions through pyodbc (what Tableau and Excel call): tables,
  columns, primary keys, then a query.
- adbc: ADBC's Postgres driver (Arrow straight from the port): it reads pg_type first, then
  fetches rows of every kind (integers, text, doubles, timestamps with a zone, lists, decimals,
  booleans, dates) as Arrow.
- npgsql: Npgsql 4.0 (what Power BI Desktop's PostgreSQL connector carries) and 8: it loads the
  server's types on open (pg_type joined to pg_proc), then GetSchema's databases, tables, views
  and columns, and queries with a parameter.
- params: psycopg 3 sends typed parameters (numeric, bytea, dates, times, timestamps with and
  without a zone, uuid, json, jsonb, interval, text, boolean, float8), as text (`%t`) and binary
  (`%b`): `SELECT %s` and an INSERT of each into a column of its type, read back, give Postgres
  16's values and type codes, and `SELECT 70 + %s` with a Decimal gives the same. What Pondra
  can't match is checked by name, as what it does: its results have no text, uuid, json or jsonb
  type of their own (a text of the text sent), a uuid column is refused. A float8, infinities and
  NaN included, is a float8 again, and `5 - $1` subtracts a negative one. A TIME and an INTERVAL print
  as Postgres prints them.

Needs: dbt-postgres (`PONDRA_DBT`, else `dbt` on PATH), Postgres 16's binaries
(`PONDRA_PG_BIN`, else /usr/lib/postgresql/16/bin), java and javac with pgjdbc's jar
(`PGJDBC_JAR`, else fetched once), psqlODBC with pyodbc, the .NET SDK (`dotnet`, with NuGet),
psycopg 3 (`params` is skipped by name without it).
"""
import argparse, contextlib, datetime as dt, decimal, json, math, os, re, shutil, subprocess, sys, tempfile, time, uuid

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import harness
from harness import Node, sql

DBT = os.environ.get("PONDRA_DBT") or shutil.which("dbt") or "/home/claude/venv-dbt/bin/dbt"
PG_BIN = os.environ.get("PONDRA_PG_BIN", "/usr/lib/postgresql/16/bin")
PASSWORD = "x"  # (a node without tokens takes any)


def psql(port, *commands, db="lake"):
    args = ["psql", "-h", "127.0.0.1", "-p", str(port), "-U", "u", "-d", db, "-X", "-A", "-F", "|"]
    for c in commands:
        args += ["-c", c]
    r = subprocess.run(args, capture_output=True, text=True, env={**os.environ, "PGPASSWORD": PASSWORD}, timeout=60)
    return r.stdout + r.stderr


# ---------------------------------------------------------------- dbt

SEEDS = [
    {"orders.csv": "id,customer,amount,day\n1,ann,10.5,2026-09-01\n2,bob,20,2026-09-01\n3,ann,7.25,2026-09-02\n",
     "customers.csv": "id,name,tier\n1,ann,gold\n2,bob,new\n"},
    {"orders.csv": "id,customer,amount,day\n1,ann,10.5,2026-09-01\n2,bob,20,2026-09-01\n3,ann,7.25,2026-09-02\n4,cy,3,2026-09-03\n5,bob,1.5,2026-09-03\n",
     "customers.csv": "id,name,tier\n1,ann,gold\n2,bob,gold\n3,cy,new\n"},
]
MODELS = {
    "order_totals.sql": "{{ config(materialized='view') }}\nselect customer, sum(amount) as total, count(*) as n from {{ ref('orders') }} group by customer\n",
    "customer_totals.sql": "{{ config(materialized='table') }}\nselect c.name, c.tier, t.total, t.n from {{ ref('customers') }} c join {{ ref('order_totals') }} t on t.customer = c.name\n",
    "daily.sql": "{{ config(materialized='incremental', unique_key='day', incremental_strategy='delete+insert') }}\n"
                 "select day, sum(amount) as total, count(*) as n from {{ ref('orders') }}\n"
                 "{% if is_incremental() %} where day >= (select max(day) from {{ this }}) {% endif %}\ngroup by day\n",
    "merged.sql": "{{ config(materialized='incremental', unique_key='id', incremental_strategy='merge') }}\n"
                  "select o.id, o.customer, o.amount, c.tier from {{ ref('orders') }} o left join {{ ref('customers') }} c on c.name = o.customer\n"
                  "{% if is_incremental() %} where o.id >= (select max(id) from {{ this }}) - 2 {% endif %}\n",
    "appended.sql": "{{ config(materialized='incremental', incremental_strategy='append') }}\n"
                    "select id, amount from {{ ref('orders') }}\n{% if is_incremental() %} where id > (select max(id) from {{ this }}) {% endif %}\n",
    "schema.yml": "version: 2\nmodels:\n  - name: customer_totals\n    columns:\n      - name: name\n        data_tests: [unique, not_null]\n"
                  "  - name: daily\n    columns:\n      - name: day\n        data_tests: [unique, not_null]\n  - name: merged\n    columns:\n      - name: id\n        data_tests: [unique]\n",
}
SNAPSHOT = "{% snapshot customers_snap %}\n{{ config(target_schema='snapshots', unique_key='id', strategy='check', check_cols=['tier']) }}\nselect * from {{ ref('customers') }}\n{% endsnapshot %}\n"
TABLES = {"analytics.order_totals": "customer", "analytics.customer_totals": "name", "analytics.daily": "day", "analytics.merged": "id", "analytics.appended": "id",
          "analytics.orders": "id", "analytics.customers": "id"}


def project(root, port, dbname):
    os.makedirs(root, exist_ok=True)
    for d in ("models", "seeds", "snapshots"):
        os.makedirs(os.path.join(root, d), exist_ok=True)
    with open(os.path.join(root, "dbt_project.yml"), "w") as f:
        f.write("name: shop\nversion: '1.0'\nprofile: check\nmodel-paths: [models]\nseed-paths: [seeds]\nsnapshot-paths: [snapshots]\n")
    with open(os.path.join(root, "profiles.yml"), "w") as f:
        f.write(f"check:\n  target: t\n  outputs:\n    t:\n      type: postgres\n      host: 127.0.0.1\n      port: {port}\n      user: {'postgres' if dbname == 'postgres' else 'u'}\n"
                f"      password: {PASSWORD}\n      dbname: {dbname}\n      schema: analytics\n      threads: 1\n")
    for name, body in MODELS.items():
        with open(os.path.join(root, "models", name), "w") as f:
            f.write(body)
    with open(os.path.join(root, "snapshots", "customers_snap.sql"), "w") as f:
        f.write(SNAPSHOT)


def dbt(root, *args):
    r = subprocess.run([DBT, *args, "--profiles-dir", root, "--project-dir", root], capture_output=True, text=True, timeout=600)
    ok = r.returncode == 0
    return ok, (r.stdout + r.stderr)[-3000:]


def rows_of(port, table, order, user):
    out = subprocess.run(["psql", "-h", "127.0.0.1", "-p", str(port), "-U", user, "-d", "lake" if user == "u" else "postgres", "-X", "-A", "-t", "-F", "|", "-c",
                          f"SELECT * FROM {table} ORDER BY {order}"], capture_output=True, text=True, env={**os.environ, "PGPASSWORD": PASSWORD}, timeout=60)
    norm = lambda v: str(float(v)) if re.fullmatch(r"-?\d+(\.\d*)?", v) else v  # (20, 20.0 and 20.00 alike: DOUBLE here, NUMERIC there)
    return [tuple(norm(v) for v in line.split("|")) for line in out.stdout.strip().splitlines()] if out.returncode == 0 else out.stderr.strip()


@contextlib.contextmanager
def postgres16(work, port):
    """Postgres 16 on `port` for the length of the block, its data under `work` (run as `claude`
    when this is root: initdb refuses root)."""
    pg_dir = os.path.join(work, f"pg16-{port}")
    os.makedirs(pg_dir)
    owner = "claude" if os.geteuid() == 0 else None
    run_as = ["runuser", "-u", owner, "--"] if owner else []
    if owner:
        shutil.chown(pg_dir, owner)
    subprocess.run([*run_as, f"{PG_BIN}/initdb", "-D", f"{pg_dir}/data", "-A", "trust", "-U", "postgres"], check=True, capture_output=True)
    subprocess.run([*run_as, f"{PG_BIN}/pg_ctl", "-D", f"{pg_dir}/data", "-o", f"-p {port} -k {pg_dir} -c listen_addresses=127.0.0.1", "-l", f"{pg_dir}/log", "-w", "start"], check=True, capture_output=True)
    try:
        yield
    finally:
        subprocess.run([*run_as, f"{PG_BIN}/pg_ctl", "-D", f"{pg_dir}/data", "-m", "immediate", "stop"], capture_output=True)


def dbt_check(node_pg, work):
    """The project against Pondra and against Postgres 16, run twice: the same rows."""
    pg_port = node_pg + 1
    with postgres16(work, pg_port):
        roots = {"pondra": (os.path.join(work, "dbt-pondra"), node_pg, "u"), "postgres": (os.path.join(work, "dbt-pg"), pg_port, "postgres")}
        steps, got = {}, {}
        for who, (root, port, user) in roots.items():
            project(root, port, "lake" if who == "pondra" else "postgres")
            for i, seeds in enumerate(SEEDS):
                for name, body in seeds.items():
                    with open(os.path.join(root, "seeds", name), "w") as f:
                        f.write(body)
                for step in ("seed", "run", "snapshot", "test"):
                    steps[f"{who}: dbt {step} ({'first' if i == 0 else 'second'} run)"] = dbt(root, step)
            steps[f"{who}: dbt docs generate"] = dbt(root, "docs", "generate")
            got[who] = {t: rows_of(port, t, o, user) for t, o in TABLES.items()}
            got[who]["snapshots.customers_snap"] = rows_of(port, "snapshots.customers_snap", "id, dbt_valid_from", user)
        # (a snapshot's timestamps and ids differ between runs: its rows' values and which are current)
        snap = lambda rows: rows if isinstance(rows, str) else [(r[0], r[1], r[2], r[-1] == "") for r in rows]
        got["pondra"]["snapshots.customers_snap"] = snap(got["pondra"]["snapshots.customers_snap"])
        got["postgres"]["snapshots.customers_snap"] = snap(got["postgres"]["snapshots.customers_snap"])
        catalog = os.path.join(roots["pondra"][0], "target", "catalog.json")
        documented = json.load(open(catalog))["nodes"] if os.path.exists(catalog) else {}
        checks = {
            "dbt seed, run, snapshot and test, twice, and docs generate: every step succeeds on Pondra": all(ok for k, (ok, _) in steps.items() if k.startswith("pondra")),
            "…and on Postgres 16 (the reference)": all(ok for k, (ok, _) in steps.items() if k.startswith("postgres")),
            "every model's and seed's rows as Postgres has them (views, tables, delete+insert, merge, append)": all(got["pondra"][t] == got["postgres"][t] for t in TABLES),
            "the snapshot's history as Postgres has it (each version, which is current)": got["pondra"]["snapshots.customers_snap"] == got["postgres"]["snapshots.customers_snap"],
            "docs generate's catalog has every model with its columns": all(any(n.endswith(m) and len(v["columns"]) > 0 for n, v in documented.items()) for m in ("customer_totals", "order_totals", "daily", "merged", "appended")),
        }
        failed = {k: v[1] for k, v in steps.items() if not v[0]}
        diffs = {t: {"pondra": got["pondra"][t], "postgres": got["postgres"][t]} for t in got["pondra"] if got["pondra"][t] != got["postgres"].get(t)}
        return checks, {"failed steps": failed, "differences": diffs}


# ---------------------------------------------------------------- psql, SQLAlchemy, JDBC, ODBC

SETUP = ["CREATE TABLE users (id BIGINT PRIMARY KEY, name VARCHAR NOT NULL, score DOUBLE DEFAULT 0, joined TIMESTAMPTZ)",
         "CREATE TABLE events (user_id BIGINT, amount DECIMAL(10,2), at TIMESTAMP, tags VARCHAR[])",
         "CREATE VIEW big AS SELECT * FROM events WHERE amount > 10",
         "CREATE SCHEMA sales", "CREATE TABLE sales.orders (id BIGINT, total DOUBLE)",
         "CREATE FUNCTION twice(x BIGINT) RETURNS BIGINT AS 'x * 2'",
         "INSERT INTO users VALUES (1, 'ann', 1.5, TIMESTAMPTZ '2026-09-01 10:00:00+00'), (2, 'bob', 2.5, NULL)",
         "INSERT INTO events VALUES (1, 12.5, TIMESTAMP '2026-09-01 10:00:00', ['a', 'b'])",
         "CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')", "CREATE TABLE moods (id BIGINT PRIMARY KEY, feeling mood)", "INSERT INTO moods VALUES (1, 'ok')"]


def psql_check(pg):
    out = {c: psql(pg, c) for c in ("\\dt", "\\dt sales.*", "\\d users", "\\dv", "\\dn", "\\df", "\\l", "\\du", "\\d sales.orders", "\\d+ big", "\\dT+", "\\d moods")}
    # A cursor in a transaction, and Ctrl-C (SIGINT, which psql answers with a cancel request) on a statement
    # that runs until it is cancelled: both over psql's simple query protocol.
    cursor = psql(pg, "BEGIN", "DECLARE c CURSOR FOR SELECT name FROM users ORDER BY id", "FETCH 1 FROM c", "COMMIT")
    stop = subprocess.run(["timeout", "-s", "INT", "1", "psql", "-h", "127.0.0.1", "-p", str(pg), "-U", "u", "-d", "lake", "-X", "-c",
                           "SELECT count(*) FROM range(1, 40000) a CROSS JOIN range(1, 40000) b WHERE a.value * b.value % 1000003 = 7"],
                          capture_output=True, text=True, env={**os.environ, "PGPASSWORD": PASSWORD}, timeout=120)
    checks = {
        "psql: a cursor in a transaction (DECLARE, FETCH, COMMIT)": "DECLARE CURSOR" in cursor and "ann" in cursor and "(1 row)" in cursor,
        "psql: Ctrl-C cancels a running statement: 57014 in Postgres's words": "canceling statement due to user request" in stop.stderr,
        "\\dt lists the tables on the search path (public), \\dt sales.* another schema's": all(x in out["\\dt"] for x in ("public|events|table", "public|users|table"))
            and "sales|" not in out["\\dt"] and "sales|orders|table" in out["\\dt sales.*"],
        # (a string column is `text` here, as the catalog reports every string: Postgres 16 says `character varying`
        # for a VARCHAR, and this is the one place the answers differ on purpose)
        "\\d shows columns, Postgres's types, NOT NULL, defaults and the key": all(x in out["\\d users"] for x in
            ("id|bigint||not null|", "name|text||not null|", "score|double precision|||0", "joined|timestamp with time zone|||", '"users_pkey" PRIMARY KEY, btree (id)')),
        "\\dv, \\dn, \\df, \\l, \\du": "public|big|view" in out["\\dv"] and "sales|" in out["\\dn"] and "twice|" in out["\\df"] and "lake|" in out["\\l"] and "u|" in out["\\du"],
        "\\d of a table in a schema, and \\d+ of a view with its definition": "tags|text[]" in psql(pg, "\\d events") and "total|double precision" in out["\\d sales.orders"]
            and "SELECT * FROM events WHERE amount > 10" in out["\\d+ big"],
        "\\dT+ lists the enum type mood with its labels in order; \\d shows a column's type as mood": re.search(r"public\s*\|\s*mood\s*\|\s*mood\s*\|\s*4\s*\|\s*sad\s*\n\s*ok\s*\n\s*happy\s*\|", out["\\dT+"]) is not None
            and out["\\dT+"].index("sad") < out["\\dT+"].index("ok") < out["\\dT+"].index("happy") and re.search(r"feeling\s*\|\s*mood\s*\|", out["\\d moods"]) is not None,
    }
    return checks, {k: v[:600] for k, v in out.items() if "ERROR" in v}


def sqlalchemy_check(pg):
    import pandas as pd, sqlalchemy as sa
    checks, said = {}, {}
    for drv in ("psycopg2", "psycopg"):
        eng = sa.create_engine(f"postgresql+{drv}://u:{PASSWORD}@127.0.0.1:{pg}/lake")
        try:
            insp = sa.inspect(eng)
            cols = {c["name"]: (str(c["type"]), c["nullable"], c["default"]) for c in insp.get_columns("users")}
            listed = {"schemas": insp.get_schema_names(), "tables": insp.get_table_names(), "sales": insp.get_table_names(schema="sales"), "views": insp.get_view_names()}
            checks[f"{drv}: schemas, tables, views"] = {"public", "sales"} <= set(listed["schemas"]) and {"events", "users"} <= set(listed["tables"]) \
                and listed["sales"] == ["orders"] and listed["views"] == ["big"]
            if not checks[f"{drv}: schemas, tables, views"]:
                said[f"{drv} listed"] = listed
            checks[f"{drv}: columns (types, nullable, defaults), the primary key, has_table"] = cols == {"id": ("BIGINT", False, None), "name": ("TEXT", False, None),  # (a string is TEXT here: see psql's \d)
                "score": ("DOUBLE PRECISION", True, "0"), "joined": ("TIMESTAMP", True, None)} and insp.get_pk_constraint("users")["constrained_columns"] == ["id"] \
                and insp.has_table("users") and not insp.has_table("nope") and insp.get_view_definition("big").startswith("SELECT")
            from sqlalchemy.dialects import postgresql
            feeling = {c["name"]: c["type"] for c in insp.get_columns("moods")}["feeling"]
            enums = [(e["name"], e["schema"], e["labels"]) for e in insp.get_enums()]
            checks[f"{drv}: get_enums() lists mood with its labels; a column of it is an ENUM of them"] = enums == [("mood", "public", ["sad", "ok", "happy"])] \
                and isinstance(feeling, postgresql.ENUM) and feeling.enums == ["sad", "ok", "happy"]
            pd.DataFrame({"a": [1, 2], "b": ["x", "y"]}).to_sql(f"from_{drv}", eng, index=False, if_exists="replace")
            pd.DataFrame({"a": [3], "b": ["z"]}).to_sql(f"from_{drv}", eng, index=False, if_exists="append")
            checks[f"{drv}: pandas to_sql (replace, append) and read_sql"] = pd.read_sql(f"SELECT * FROM from_{drv} ORDER BY a", eng).to_dict("records") == [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}, {"a": 3, "b": "z"}]
        except Exception as e:  # noqa: BLE001 (reported)
            checks[f"{drv}: the inspector"] = False
            said[drv] = str(e)[:800]
    return checks, said


def psycopg_check(pg):
    """psycopg 3 (what SQLAlchemy's psycopg driver and most Python code use): a named cursor reads a table
    a part at a time, the cursor's statements by hand with their tags, and conn.cancel() stops a running
    statement (57014) with the connection usable after it."""
    import psycopg, threading
    url = f"host=127.0.0.1 port={pg} user=u dbname=lake password={PASSWORD}"
    checks = {}
    with psycopg.connect(url, autocommit=True) as c:
        c.execute("CREATE TABLE ten_k (id BIGINT)")
        c.execute("INSERT INTO ten_k SELECT value FROM range(1, 10001)")
    with psycopg.connect(url) as c:
        with c.cursor(name="ten") as cur:
            cur.itersize = 100
            cur.execute("SELECT id FROM ten_k ORDER BY id")
            ids = [row[0] for row in cur]
        checks["psycopg: a named cursor reads 10,000 rows, 100 a part, in order"] = ids == list(range(1, 10001))
        cur = c.cursor()
        cur.execute("DECLARE d CURSOR FOR SELECT id FROM ten_k ORDER BY id")
        cur.execute("MOVE 9995 FROM d")
        moved = cur.statusmessage
        cur.execute("FETCH FORWARD 5 FROM d")
        got, tag = [r[0] for r in cur.fetchall()], cur.statusmessage
        cur.execute("CLOSE d")
        checks["psycopg: DECLARE, MOVE, FETCH, CLOSE by hand: the tags (MOVE 9995, FETCH 5, CLOSE CURSOR) and the rows"] = \
            moved == "MOVE 9995" and tag == "FETCH 5" and got == list(range(9996, 10001)) and cur.statusmessage == "CLOSE CURSOR"
    with psycopg.connect(url, autocommit=True) as c:
        out = {}

        def run():  # (a thread: the statement runs until conn.cancel() stops it)
            try:
                c.execute("SELECT count(*) FROM range(1, 40000) a CROSS JOIN range(1, 40000) b WHERE a.value * b.value % 1000003 = 7")
                out["answer"] = "finished"
            except psycopg.errors.QueryCanceled as e:
                out["sqlstate"] = e.sqlstate

        t = threading.Thread(target=run)
        t.start()
        time.sleep(0.5)
        c.cancel()
        t.join(60)
        checks["psycopg: conn.cancel() stops a running statement (57014), and the connection carries on"] = out.get("sqlstate") == "57014" and c.execute("SELECT 1").fetchone() == (1,)
    return checks, {}


CURSORS_JAVA = r"""
import java.sql.*;
public class Cursors {
  static final String LONG = "SELECT count(*) FROM range(1, 40000) a CROSS JOIN range(1, 40000) b WHERE a.value * b.value % 1000003 = 7"; // runs until cancelled (the filter stops count(*) from being answered from statistics)
  // Statement.cancel() from another thread: the statement answers 57014, and the connection carries on.
  static void cancel(String url, String name) throws Exception {
    try (Connection c = DriverManager.getConnection(url, "u", "x")) {
      Statement st = c.createStatement();
      Thread t = new Thread(() -> { try { Thread.sleep(500); st.cancel(); } catch (Exception e) { System.out.println(name + "-cancel-threw=" + e); } });
      t.start();
      try { st.executeQuery(LONG); System.out.println(name + "-cancel=none"); }
      catch (SQLException e) { System.out.println(name + "-cancel=" + e.getSQLState()); }
      t.join();
      ResultSet r = c.createStatement().executeQuery("SELECT 1");
      r.next();
      System.out.println(name + "-after=" + r.getInt(1));
    }
  }
  public static void main(String[] a) throws Exception {
    // getTransactionIsolation() sends SHOW TRANSACTION ISOLATION LEVEL (what Metabase asks before it adds a database).
    try (Connection c = DriverManager.getConnection(a[0], "u", "x")) {
      System.out.println("isolation=" + c.getTransactionIsolation());
    }
    cancel(a[0], "extended");                  // pgjdbc's default: Parse, Bind, Execute
    cancel(a[0] + "?preferQueryMode=simple", "simple");  // the simple query protocol
    try (Connection w = DriverManager.getConnection(a[0], "u", "x")) {
      w.createStatement().execute("CREATE TABLE fetch_t (id BIGINT)");
      w.createStatement().execute("INSERT INTO fetch_t SELECT value FROM range(1, 10001)");
    }
    // setFetchSize(100) with autocommit off: pgjdbc asks for 100 rows at a time through a portal (its log, on stderr).
    try (Connection c = DriverManager.getConnection(a[0] + "?loggerLevel=DEBUG", "u", "x")) {
      c.setAutoCommit(false);
      Statement st = c.createStatement();
      st.setFetchSize(100);
      ResultSet r = st.executeQuery("SELECT id FROM fetch_t ORDER BY id");
      long n = 0, sum = 0;
      while (r.next()) { n++; sum += r.getLong(1); }
      c.commit();
      System.out.println("fetch=" + n + "|" + sum);
    }
  }
}
"""


JAVA = r"""
import java.sql.*;
public class Meta {
  static String rows(ResultSet r, String... cols) throws SQLException {
    StringBuilder b = new StringBuilder();
    while (r.next()) { for (String c : cols) b.append(r.getString(c)).append('|'); b.append(';'); }
    return b.toString();
  }
  public static void main(String[] a) throws Exception {
    Connection c = DriverManager.getConnection(a[0], "u", "x");
    DatabaseMetaData m = c.getMetaData();
    System.out.println("product=" + m.getDatabaseProductName() + " " + m.getDatabaseProductVersion());
    System.out.println("catalogs=" + rows(m.getCatalogs(), "TABLE_CAT"));
    System.out.println("schemas=" + rows(m.getSchemas(), "TABLE_SCHEM"));
    System.out.println("tables=" + rows(m.getTables(null, "public", "%", new String[]{"TABLE", "VIEW"}), "TABLE_NAME", "TABLE_TYPE"));
    System.out.println("columns=" + rows(m.getColumns(null, "public", "users", "%"), "COLUMN_NAME", "TYPE_NAME", "NULLABLE", "COLUMN_DEF"));
    System.out.println("keys=" + rows(m.getPrimaryKeys(null, "public", "users"), "COLUMN_NAME", "KEY_SEQ"));
    System.out.println("functions=" + rows(m.getFunctions(null, "public", "%"), "FUNCTION_NAME"));
    PreparedStatement p = c.prepareStatement("SELECT name, score FROM users WHERE id = ?");
    p.setLong(1, 2);
    ResultSet r = p.executeQuery();
    r.next();
    System.out.println("query=" + r.getString(1) + "|" + r.getDouble(2) + "|" + r.getMetaData().getColumnTypeName(2));
  }
}
"""


def jdbc_check(pg, work):
    jar = os.environ.get("PGJDBC_JAR") or os.path.join(os.path.expanduser("~"), ".cache", "pondra", "postgresql-42.7.4.jar")
    if not os.path.exists(jar):
        os.makedirs(os.path.dirname(jar), exist_ok=True)
        subprocess.run(["curl", "-sSLf", "-o", jar, "https://jdbc.postgresql.org/download/postgresql-42.7.4.jar"], check=True)
    sources = []
    for name, text in (("Meta", JAVA), ("Cursors", CURSORS_JAVA)):
        sources.append(os.path.join(work, f"{name}.java"))
        with open(sources[-1], "w") as f:
            f.write(text)
    subprocess.run(["javac", "-d", work, *sources], check=True, capture_output=True)
    url = f"jdbc:postgresql://127.0.0.1:{pg}/lake"
    r = subprocess.run(["java", "-cp", f"{work}:{jar}", "Meta", url], capture_output=True, text=True, timeout=120)
    got = dict(line.split("=", 1) for line in r.stdout.splitlines() if "=" in line)
    # Cancels (both protocols), and a cursor-style read of 10,000 rows: each row once, in parts. pgjdbc logs
    # what it sends and gets (its DEBUG log, through java.util.logging at ALL: the JDK's default drops it).
    logging_conf = os.path.join(work, "logging.properties")
    with open(logging_conf, "w") as f:
        f.write("handlers=java.util.logging.ConsoleHandler\n.level=ALL\norg.postgresql.level=ALL\njava.util.logging.ConsoleHandler.level=ALL\n"
                "java.util.logging.SimpleFormatter.format=%5$s%n\n")
    r2 = subprocess.run(["java", f"-Djava.util.logging.config.file={logging_conf}", "-cp", f"{work}:{jar}", "Cursors", url],
                        capture_output=True, text=True, timeout=300)
    got2 = dict(line.split("=", 1) for line in r2.stdout.splitlines() if "=" in line)
    parts = r2.stderr.count("PortalSuspended")  # (one per part the server sent: 100 for 10,000 rows at 100 a fetch, then the empty fetch)
    print(json.dumps({"pgjdbc_portal_suspended": parts, "fetch": got2.get("fetch")}), flush=True)
    checks = {
        "pgjdbc: catalogs, schemas, tables and views": "lake|" in got.get("catalogs", "") and "sales|" in got.get("schemas", "") and "users|TABLE|" in got.get("tables", "") and "big|VIEW|" in got.get("tables", ""),
        "pgjdbc: columns (types, nullable, defaults), primary keys, functions": "id|int8|0|null|" in got.get("columns", "") and "name|varchar|0|" in got.get("columns", "")
            and "score|float8|1|0|" in got.get("columns", "") and got.get("keys") == "id|1|;" and "twice|" in got.get("functions", ""),
        "pgjdbc: a prepared query": got.get("query") == "bob|2.5|float8",
        "pgjdbc: Statement.cancel() stops a running statement (57014), extended and simple protocol; the connection carries on":
            got2.get("extended-cancel") == "57014" and got2.get("simple-cancel") == "57014" and got2.get("extended-after") == "1" and got2.get("simple-after") == "1",
        "pgjdbc: setFetchSize(100) in a transaction over 10,000 rows: each row once": got2.get("fetch") == "10000|50005000",
        "pgjdbc: setFetchSize(100): the answer comes in parts (PortalSuspended)": parts > 0,
        "pgjdbc: getTransactionIsolation() (SHOW TRANSACTION ISOLATION LEVEL) answers read committed (2)": got2.get("isolation") == "2",
    }
    said = {} if all(checks.values()) else {"out": r.stdout[-1500:], "err": r.stderr[-1500:], "cursors": (r2.stdout + r2.stderr)[-1500:]}
    return checks, said


def odbc_check(pg):
    import pyodbc
    c = pyodbc.connect(f"DRIVER={{PostgreSQL Unicode}};SERVER=127.0.0.1;PORT={pg};DATABASE=lake;UID=u;PWD={PASSWORD}", autocommit=True)
    cur = c.cursor()
    tables = [(r.table_schem, r.table_name, r.table_type) for r in cur.tables()]
    cols = [(r.column_name, r.type_name, r.nullable) for r in cur.columns(table="users")]
    keys = [(r.column_name, r.key_seq) for r in cur.primaryKeys("users")]
    one = cur.execute("SELECT name, score FROM users WHERE id = ?", 1).fetchone()
    c.close()
    checks = {
        "psqlODBC: tables and views": ("public", "users", "TABLE") in tables and ("public", "big", "VIEW") in tables and ("sales", "orders", "TABLE") in tables,
        "psqlODBC: columns and the primary key": ("id", "int8", 0) in cols and ("score", "float8", 1) in cols and keys == [("id", 1)],
        "psqlODBC: a query with a parameter": tuple(one) == ("ann", 1.5),
    }
    return checks, ({} if all(checks.values()) else {"tables": tables, "columns": cols, "keys": keys, "one": one})


def adbc_check(pg):
    """ADBC's Postgres driver: it reads pg_type first (a type's binary format by its receive
    function's name), then fetches Arrow through COPY … TO STDOUT (FORMAT binary). NUMERIC comes
    as its digits (text), as the driver gives it from Postgres."""
    import adbc_driver_postgresql.dbapi as adbc
    with adbc.connect(f"postgresql://u:{PASSWORD}@127.0.0.1:{pg}/lake") as conn:
        cur = conn.cursor()
        cur.execute("SELECT id, name, score, joined FROM users ORDER BY id")
        t = cur.fetch_arrow_table()
        cur.execute("SELECT user_id, amount, at, tags, CAST(amount AS DECIMAL(10, 2)) AS exact, amount > 10 AS big, CAST(at AS DATE) AS day FROM events")
        e = cur.fetch_arrow_table()
    got = {"users": t.to_pylist(), "types": [str(f.type) for f in t.schema], "events": e.to_pylist(), "event_types": [str(f.type) for f in e.schema]}
    import datetime as dt
    checks = {
        "ADBC (Postgres driver): Arrow rows with their types": got["users"][0]["name"] == "ann" and got["users"][1]["score"] == 2.5 and got["types"][:3] == ["int64", "string", "double"]
            and got["users"][0]["joined"] == dt.datetime(2026, 9, 1, 10, tzinfo=dt.timezone.utc) and got["events"][0]["tags"] == ["a", "b"]
            and got["events"][0]["exact"] == "12.50" and got["events"][0]["big"] is True and got["events"][0]["day"] == dt.date(2026, 9, 1),
    }
    return checks, ({} if all(checks.values()) else {k: str(v)[:800] for k, v in got.items()})


CSHARP = r"""
using System;
using System.Data;
using Npgsql;

// What Power BI's PostgreSQL connector does through Npgsql: open (Npgsql loads the server's types
// from pg_type, joined to pg_proc), list the databases, tables, views and columns, and query.
class P {
    static readonly IFormatProvider Inv = System.Globalization.CultureInfo.InvariantCulture;
    static string Rows(DataTable t, params string[] cols) {
        var s = "";
        foreach (DataRow r in t.Rows) { foreach (var c in cols) s += r[c] + "|"; s += ";"; }
        return s;
    }
    static void Main(string[] a) {
        using var c = new NpgsqlConnection(a[0]);
        c.Open();
        Console.WriteLine("databases=" + Rows(c.GetSchema("Databases"), "database_name"));
        Console.WriteLine("tables=" + Rows(c.GetSchema("Tables"), "table_schema", "table_name", "table_type"));
        Console.WriteLine("views=" + Rows(c.GetSchema("Views"), "table_schema", "table_name"));
        Console.WriteLine("columns=" + Rows(c.GetSchema("Columns", new[] { null, null, "users" }), "column_name", "is_nullable"));
        using (var cmd = new NpgsqlCommand("SELECT name, score, joined FROM users WHERE id = @id", c)) {
            cmd.Parameters.AddWithValue("id", 1L);
            using var rd = cmd.ExecuteReader();
            rd.Read();
            Console.WriteLine("row=" + rd.GetString(0) + "|" + rd.GetDouble(1).ToString(Inv) + "|" + rd.GetDateTime(2).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm", Inv));
        }
        using (var cmd = new NpgsqlCommand("SELECT tags, amount FROM events", c)) {
            using var rd = cmd.ExecuteReader();
            rd.Read();
            Console.WriteLine("event=" + string.Join(",", (string[])rd.GetValue(0)) + "|" + rd.GetDecimal(1).ToString(Inv));
        }
    }
}
"""


def npgsql_check(pg, work):
    """Npgsql, the .NET driver: 4.0 (what Power BI Desktop's PostgreSQL connector carries) and 8.
    Needs the .NET SDK (`dotnet`) and NuGet."""
    checks, said = {}, {}
    env = {**os.environ, "DOTNET_CLI_TELEMETRY_OPTOUT": "1", "DOTNET_NOLOGO": "1"}
    for version in ("4.0.12", "8.0.8"):
        proj = os.path.join(work, f"pgcheck{version.split('.')[0]}")
        os.makedirs(proj, exist_ok=True)
        with open(os.path.join(proj, "pgcheck.csproj"), "w") as f:
            f.write('<Project Sdk="Microsoft.NET.Sdk"><PropertyGroup><OutputType>Exe</OutputType><TargetFramework>net8.0</TargetFramework><Nullable>disable</Nullable>'
                    f'<NoWarn>NU1903;NU1902</NoWarn></PropertyGroup><ItemGroup><PackageReference Include="Npgsql" Version="{version}" /></ItemGroup></Project>')
        with open(os.path.join(proj, "Program.cs"), "w") as f:
            f.write(CSHARP)
        built = subprocess.run(["dotnet", "build", "-c", "Release", "-o", "out"], cwd=proj, capture_output=True, text=True, env=env, timeout=600)
        r = subprocess.run(["dotnet", "out/pgcheck.dll", f"Host=127.0.0.1;Port={pg};Username=u;Password={PASSWORD};Database=lake"], cwd=proj, capture_output=True, text=True, env=env, timeout=120)
        got = dict(line.split("=", 1) for line in r.stdout.splitlines() if "=" in line)
        checks[f"Npgsql {version}: types loaded, databases, tables, views, columns; a query with a parameter; arrays and decimals"] = built.returncode == 0 \
            and got.get("databases") == "lake|;" and "public|users|BASE TABLE|;" in got.get("tables", "") and "public|big|;" in got.get("views", "") \
            and "name|NO|;" in got.get("columns", "") and got.get("row") == "ann|1.5|2026-09-01T10:00" and got.get("event") == "a,b|12.50"
        if not checks[f"Npgsql {version}: types loaded, databases, tables, views, columns; a query with a parameter; arrays and decimals"]:
            said[version] = {"build": built.stdout[-800:], "out": r.stdout[-1500:], "err": r.stderr[-1500:]}
    return checks, said


# ---------------------------------------------------------------- params (psycopg 3)

def param_cases():
    """(name, the value psycopg 3 sends, the column it is written to, what Pondra answers to a SELECT,
    what it answers for a value stored and read back). Where those are None, Pondra answers as
    Postgres 16 does. Where not, it is a named difference, held to exactly: Pondra has no text, uuid,
    json or jsonb type of its own, so a value of one comes back as a text (25) of the text it was
    sent."""
    from psycopg.types.json import Json, Jsonb
    utc = dt.timezone.utc
    as_text = lambda s: ("value", s, 25)
    js = '{"a": [1, 2]}'
    return [
        ("numeric", decimal.Decimal("15.7563"), "NUMERIC", None, None),
        ("numeric, negative", decimal.Decimal("-4.24"), "NUMERIC", None, None),
        ("bytea", b"\xff\x00\x01", "BYTEA", None, None),
        ("date", dt.date(2020, 1, 1), "DATE", None, None),
        ("time", dt.time(12, 34, 56, 789000), "TIME", None, None),
        ("timestamp", dt.datetime(2020, 1, 1, 12, 0), "TIMESTAMP", None, None),
        ("timestamptz", dt.datetime(2020, 1, 1, 12, 0, tzinfo=utc), "TIMESTAMPTZ", None, None),
        ("interval", dt.timedelta(days=1, seconds=3, microseconds=4), "INTERVAL", None, None),
        ("uuid", uuid.UUID("12345678-1234-5678-1234-567812345678"), "UUID", as_text("12345678-1234-5678-1234-567812345678"), None),
        ("json", Json({"a": [1, 2]}), "JSON", as_text(js), as_text(js)),
        ("jsonb", Jsonb({"a": [1, 2]}), "JSONB", as_text(js), as_text(js)),
        ("text", "it's", "TEXT", None, None),
        ("boolean", True, "BOOLEAN", None, None),
        ("double precision", 2.5, "DOUBLE PRECISION", None, None),
        ("double precision, negative", -2.5, "DOUBLE PRECISION", None, None),
        ("double precision, infinity", float("inf"), "DOUBLE PRECISION", None, None),
        ("double precision, -infinity", float("-inf"), "DOUBLE PRECISION", None, None),
        ("double precision, NaN", float("nan"), "DOUBLE PRECISION", None, None),
    ]


def _psycopg(port, db, user):
    """A psycopg 3 connection that reads text as text: Pondra speaks UTF-8 only, and a Postgres made
    without an encoding (SQL_ASCII) would otherwise hand psycopg bytes for every text value."""
    import psycopg
    return psycopg.connect(f"host=127.0.0.1 port={port} dbname={db} user={user} password={PASSWORD} client_encoding=UTF8",
                           autocommit=True, prepare_threshold=None)


def _pg_answer(conn, sql, args=None):
    """What the server answers one statement: ("value", its first value, that column's type code), or
    ("error", SQLSTATE, the message's first line)."""
    try:
        cur = conn.execute(sql, args)
        return ("value", cur.fetchone()[0], cur.description[0].type_code)
    except Exception as e:  # noqa: BLE001 (a refusal is an answer too)
        return ("error", getattr(e, "sqlstate", None) or type(e).__name__, str(e).splitlines()[0][:200])


def _stored(conn, table, coltype, ph, value):
    """INSERT the value into a column of its type, then SELECT it back. A column the server refuses
    is an answer too: ("refused", SQLSTATE, the message's first line)."""
    try:
        conn.execute(f"DROP TABLE IF EXISTS {table}")
        conn.execute(f"CREATE TABLE {table} (v {coltype})")
    except Exception as e:  # noqa: BLE001
        return ("refused", getattr(e, "sqlstate", None) or type(e).__name__, str(e).splitlines()[0][:200])
    try:
        conn.execute(f"INSERT INTO {table} (v) VALUES ({ph})", [value])
    except Exception as e:  # noqa: BLE001
        return ("error", getattr(e, "sqlstate", None) or type(e).__name__, str(e).splitlines()[0][:200])
    return _pg_answer(conn, f"SELECT v FROM {table}")


def _key(answer):
    """An answer as it is compared: a value with its Python type and its type code (NaN is equal to
    NaN here, which Python's own == is not); an error or a refusal by its SQLSTATE alone (the messages
    differ by server)."""
    if answer[0] != "value":
        return answer[:2]
    value = "NaN" if isinstance(answer[1], float) and math.isnan(answer[1]) else answer[1]
    return (answer[0], type(answer[1]), value, *answer[2:])


def _note(differences, label, got, want):
    if _key(got) != _key(want):
        differences.append(f"{label}: Pondra's answer {got!r} / Postgres's answer {want!r}")


def _psql_value(port, user, db, sql):
    r = subprocess.run(["psql", "-h", "127.0.0.1", "-p", str(port), "-U", user, "-d", db, "-X", "-A", "-t", "-c", sql],
                       capture_output=True, text=True, env={**os.environ, "PGPASSWORD": PASSWORD}, timeout=60)
    return r.stdout.strip() if r.returncode == 0 else "ERROR " + r.stderr.strip()


def params_check(pg, pg16):
    """psycopg 3 against Pondra and against Postgres 16: each type as a parameter of SELECT and of an
    INSERT, text (%t) and binary (%b), with Postgres 16's values and type codes. Pondra's own answers
    where it has no type of Postgres's are held to by name; every other difference is listed."""
    try:
        import psycopg  # noqa: F401 (the part is skipped by name without it)
    except ImportError:
        return {}, {"skipped": "psycopg 3 isn't installed (pip install 'psycopg[binary]'): params wasn't run"}
    pondra, postgres = _psycopg(pg, "lake", "u"), _psycopg(pg16, "postgres", "postgres")
    checks, differences = {}, []
    cases = param_cases()
    how = lambda named: "as Pondra answers it (a named difference)" if named else "as Postgres 16 has it"
    for i, (name, value, coltype, select_as, stored_as) in enumerate(cases):
        for ph in ("%t", "%b"):
            want = _pg_answer(postgres, f"SELECT {ph}", [value])
            got = _pg_answer(pondra, f"SELECT {ph}", [value])
            checks[f"{name} {ph}: SELECT {ph}, {how(select_as)}"] = _key(got) == _key(select_as or want)
            _note(differences, f"SELECT {ph} of {name}", got, want)
            table = f"pm_{i:02d}_{ph[1]}"
            want = _stored(postgres, table, coltype, ph, value)
            got = _stored(pondra, table, coltype, ph, value)
            if got[0] == "refused":  # (a column Pondra has no type for: named, and never written)
                checks[f"{name} {ph}: a {coltype} column is refused by name (Pondra)"] = coltype in got[2]
                if ph == "%t":
                    differences.append(f"CREATE TABLE … (v {coltype}): Pondra's answer {got[2]!r} / Postgres's answer created the column")
                continue
            checks[f"{name} {ph}: INSERT {ph}, then SELECT, {how(stored_as)}"] = _key(got) == _key(stored_as or want)
            _note(differences, f"INSERT {ph} of {name}, then SELECT", got, want)
    for ph in ("%t", "%b"):  # a numeric parameter's arithmetic: the same result on both
        got = _pg_answer(pondra, f"SELECT 70 + {ph}", [decimal.Decimal("4.24")])
        want = _pg_answer(postgres, f"SELECT 70 + {ph}", [decimal.Decimal("4.24")])
        checks[f"SELECT 70 + {ph} with Decimal('4.24') is Decimal('74.24') with type 1700, as Postgres 16 has it"] = \
            _key(got) == _key(want) == _key(("value", decimal.Decimal("74.24"), 1700))
        _note(differences, f"SELECT 70 + {ph} with Decimal('4.24')", got, want)
    for ph in ("%t", "%b"):  # a float parameter after a minus still subtracts, as it does on Postgres 16
        got = _pg_answer(pondra, f"SELECT 5 - {ph}", [-2.5])
        want = _pg_answer(postgres, f"SELECT 5 - {ph}", [-2.5])
        checks[f"SELECT 5 - {ph} with -2.5 is 7.5 with type 701, as Postgres 16 has it"] = _key(got) == _key(want) == _key(("value", 7.5, 701))
        _note(differences, f"SELECT 5 - {ph} with -2.5", got, want)
    for conn in (postgres, pondra):  # a numeric parameter that isn't a number: refused, and nothing it says runs
        conn.execute("DROP TABLE IF EXISTS pm_inj")
        conn.execute("CREATE TABLE pm_inj (v TEXT)")
        conn.execute("INSERT INTO pm_inj VALUES ('kept')")
    injection = "1; DROP TABLE pm_inj"
    got = _pg_answer(pondra, "SELECT %s::numeric", [injection])
    want = _pg_answer(postgres, "SELECT %s::numeric", [injection])
    kept = _pg_answer(pondra, "SELECT v FROM pm_inj")
    checks["a numeric parameter that isn't a number is refused (22P02), and the table it names stays"] = \
        got[:2] == ("error", "22P02") and kept[0] == "value" and kept[1] == "kept"
    _note(differences, "SELECT %s::numeric with '1; DROP TABLE pm_inj'", got, want)
    names = {name: i for i, (name, *_rest) in enumerate(cases)}  # (a TIME and an INTERVAL as psql prints them)
    for name in ("time", "interval"):
        table = f"pm_{names[name]:02d}_b"
        got = _psql_value(pg, "u", "lake", f"SELECT v FROM {table}")
        want = _psql_value(pg16, "postgres", "postgres", f"SELECT v FROM {table}")
        checks[f"psql prints a {name} as Postgres 16 does"] = got == want
        if got != want:
            differences.append(f"psql, SELECT v FROM a {name}: Pondra's answer {got!r} / Postgres's answer {want!r}")
    pondra.close()
    postgres.close()
    return checks, {"differences": differences}


def params_part(pg, work, pg16):
    """params against the Postgres 16 given by --pg16, else one started for it here."""
    if pg16:
        return params_check(pg, pg16)
    with postgres16(work, pg + 1):
        return params_check(pg, pg + 1)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("parts", nargs="?", default="dbt,psql,sqlalchemy,psycopg,jdbc,odbc,adbc,npgsql,params")
    ap.add_argument("--port", type=int, default=8870)
    ap.add_argument("--pg16", type=int, default=None, help="a Postgres 16 already running on this port, for params (else one is started)")
    A = harness.A = ap.parse_args()
    A.s3, A.keep = False, False
    work = tempfile.mkdtemp(prefix="pondra-clients-")
    os.chmod(work, 0o755)  # (Postgres runs as another user, and reads its folder here)
    lake = os.path.join(work, "lake")
    pg = A.port + 10
    node = Node(lake, A.port, pg=f"127.0.0.1:{pg}").start()
    results, said = {}, {}
    try:
        for s in SETUP:
            sql(A.port, s)
        for part in A.parts.split(","):
            try:
                checks, info = {"dbt": lambda: dbt_check(pg, work), "psql": lambda: psql_check(pg), "sqlalchemy": lambda: sqlalchemy_check(pg), "psycopg": lambda: psycopg_check(pg),
                                "jdbc": lambda: jdbc_check(pg, work), "odbc": lambda: odbc_check(pg), "adbc": lambda: adbc_check(pg), "npgsql": lambda: npgsql_check(pg, work),
                                "params": lambda: params_part(pg, work, A.pg16)}[part]()
            except Exception as e:  # noqa: BLE001 (a part that couldn't run fails)
                checks, info = {f"{part}: ran": False}, {"error": f"{type(e).__name__}: {str(e)[:1500]}"}
            results.update(checks)
            if info:
                said[part] = info
            print(json.dumps({part: checks}, indent=1), flush=True)
    finally:
        node.kill()
        shutil.rmtree(work, ignore_errors=True)
    ok = all(results.values())
    print(json.dumps(said, indent=1, default=str))  # (what each part said: its differences from the reference, its failures)
    print(json.dumps({"checks": len(results), "passed": sum(results.values()), "ok": ok}))
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
