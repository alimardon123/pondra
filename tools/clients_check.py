#!/usr/bin/env python3
"""Pondra as dbt and BI tools use it (ADR-030): its Postgres port, with Postgres's catalog.

  clients_check.py [dbt,psql,sqlalchemy,jdbc,odbc] [--port 8870]

- dbt: a project with seeds, a view, a table, incremental models (delete+insert, merge,
  append), a snapshot and data tests, run twice — the second time with changed seeds, so views
  and tables are replaced by renames and the incremental models take their incremental path —
  then `dbt docs generate`; the same project against Postgres 16, and every model's rows alike.
- psql: `\\dt`, `\\d`, `\\dv`, `\\dn`, `\\df`, `\\l`, `\\du` show the lake's tables, columns (types,
  NOT NULL, defaults, keys), views, schemas, functions and databases.
- sqlalchemy: the inspector (schemas, tables, views, columns, primary keys, a view's definition),
  with psycopg 2 and 3; pandas' `to_sql` and `read_sql`.
- jdbc: pgjdbc's DatabaseMetaData (what DBeaver, Metabase and Tableau's JDBC connector call):
  catalogs, schemas, tables, columns, primary keys, then a query.
- odbc: psqlODBC's catalog functions through pyodbc (what Tableau and Excel call): tables,
  columns, primary keys, then a query.

Needs: dbt-postgres (`PONDRA_DBT`, else `dbt` on PATH), Postgres 16's binaries
(`PONDRA_PG_BIN`, else /usr/lib/postgresql/16/bin), java and javac with pgjdbc's jar
(`PGJDBC_JAR`, else fetched once), psqlODBC with pyodbc.
"""
import argparse, json, os, re, shutil, subprocess, sys, tempfile, time

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


def dbt_check(node_pg, work):
    """The project against Pondra and against Postgres 16, run twice: the same rows."""
    pg_dir = os.path.join(work, "pg16")
    os.makedirs(pg_dir)
    owner = "claude" if os.geteuid() == 0 else None  # (initdb refuses root)
    run_as = ["runuser", "-u", owner, "--"] if owner else []
    if owner:
        shutil.chown(pg_dir, owner)
    subprocess.run([*run_as, f"{PG_BIN}/initdb", "-D", f"{pg_dir}/data", "-A", "trust", "-U", "postgres"], check=True, capture_output=True)
    pg_port = node_pg + 1
    subprocess.run([*run_as, f"{PG_BIN}/pg_ctl", "-D", f"{pg_dir}/data", "-o", f"-p {pg_port} -k {pg_dir} -c listen_addresses=127.0.0.1", "-l", f"{pg_dir}/log", "-w", "start"], check=True, capture_output=True)
    try:
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
    finally:
        subprocess.run([*run_as, f"{PG_BIN}/pg_ctl", "-D", f"{pg_dir}/data", "-m", "immediate", "stop"], capture_output=True)


# ---------------------------------------------------------------- psql, SQLAlchemy, JDBC, ODBC

SETUP = ["CREATE TABLE users (id BIGINT PRIMARY KEY, name VARCHAR NOT NULL, score DOUBLE DEFAULT 0, joined TIMESTAMPTZ)",
         "CREATE TABLE events (user_id BIGINT, amount DECIMAL(10,2), at TIMESTAMP, tags VARCHAR[])",
         "CREATE VIEW big AS SELECT * FROM events WHERE amount > 10",
         "CREATE SCHEMA sales", "CREATE TABLE sales.orders (id BIGINT, total DOUBLE)",
         "CREATE FUNCTION twice(x BIGINT) RETURNS BIGINT AS 'x * 2'",
         "INSERT INTO users VALUES (1, 'ann', 1.5, TIMESTAMPTZ '2026-09-01 10:00:00+00'), (2, 'bob', 2.5, NULL)",
         "INSERT INTO events VALUES (1, 12.5, TIMESTAMP '2026-09-01 10:00:00', ['a', 'b'])"]


def psql_check(pg):
    out = {c: psql(pg, c) for c in ("\\dt", "\\d users", "\\dv", "\\dn", "\\df", "\\l", "\\du", "\\d sales.orders", "\\d+ big")}
    checks = {
        "\\dt lists every table, in its schema": all(x in out["\\dt"] for x in ("public|events|table", "public|users|table", "sales|orders|table")),
        "\\d shows columns, Postgres's types, NOT NULL, defaults and the key": all(x in out["\\d users"] for x in
            ("id|bigint||not null|", "name|character varying||not null|", "score|double precision|||0", "joined|timestamp with time zone|||", '"users_pkey" PRIMARY KEY, btree (id)')),
        "\\dv, \\dn, \\df, \\l, \\du": "public|big|view" in out["\\dv"] and "sales|" in out["\\dn"] and "twice|" in out["\\df"] and "lake|" in out["\\l"] and "u|" in out["\\du"],
        "\\d of a table in a schema, and \\d+ of a view with its definition": "tags|character varying[]" in psql(pg, "\\d events") and "total|double precision" in out["\\d sales.orders"]
            and "SELECT * FROM events WHERE amount > 10" in out["\\d+ big"],
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
            checks[f"{drv}: schemas, tables, views"] = {"public", "sales"} <= set(insp.get_schema_names()) and {"events", "users"} <= set(insp.get_table_names()) \
                and insp.get_table_names(schema="sales") == ["orders"] and insp.get_view_names() == ["big"]
            checks[f"{drv}: columns (types, nullable, defaults), the primary key, has_table"] = cols == {"id": ("BIGINT", False, None), "name": ("VARCHAR", False, None),
                "score": ("DOUBLE PRECISION", True, "0"), "joined": ("TIMESTAMP", True, None)} and insp.get_pk_constraint("users")["constrained_columns"] == ["id"] \
                and insp.has_table("users") and not insp.has_table("nope") and insp.get_view_definition("big").startswith("SELECT")
            pd.DataFrame({"a": [1, 2], "b": ["x", "y"]}).to_sql(f"from_{drv}", eng, index=False, if_exists="replace")
            pd.DataFrame({"a": [3], "b": ["z"]}).to_sql(f"from_{drv}", eng, index=False, if_exists="append")
            checks[f"{drv}: pandas to_sql (replace, append) and read_sql"] = pd.read_sql(f"SELECT * FROM from_{drv} ORDER BY a", eng).to_dict("records") == [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}, {"a": 3, "b": "z"}]
        except Exception as e:  # noqa: BLE001 (reported)
            checks[f"{drv}: the inspector"] = False
            said[drv] = str(e)[:800]
    return checks, said


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
    src = os.path.join(work, "Meta.java")
    with open(src, "w") as f:
        f.write(JAVA)
    subprocess.run(["javac", "-d", work, src], check=True, capture_output=True)
    r = subprocess.run(["java", "-cp", f"{work}:{jar}", "Meta", f"jdbc:postgresql://127.0.0.1:{pg}/lake"], capture_output=True, text=True, timeout=120)
    got = dict(line.split("=", 1) for line in r.stdout.splitlines() if "=" in line)
    checks = {
        "pgjdbc: catalogs, schemas, tables and views": "lake|" in got.get("catalogs", "") and "sales|" in got.get("schemas", "") and "users|TABLE|" in got.get("tables", "") and "big|VIEW|" in got.get("tables", ""),
        "pgjdbc: columns (types, nullable, defaults), primary keys, functions": "id|int8|0|null|" in got.get("columns", "") and "name|varchar|0|" in got.get("columns", "")
            and "score|float8|1|0|" in got.get("columns", "") and got.get("keys") == "id|1|;" and "twice|" in got.get("functions", ""),
        "pgjdbc: a prepared query": got.get("query") == "bob|2.5|float8",
    }
    return checks, ({} if all(checks.values()) else {"out": r.stdout[-1500:], "err": r.stderr[-1500:]})


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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("parts", nargs="?", default="dbt,psql,sqlalchemy,jdbc,odbc")
    ap.add_argument("--port", type=int, default=8870)
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
                checks, info = {"dbt": lambda: dbt_check(pg, work), "psql": lambda: psql_check(pg), "sqlalchemy": lambda: sqlalchemy_check(pg),
                                "jdbc": lambda: jdbc_check(pg, work), "odbc": lambda: odbc_check(pg)}[part]()
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
    if not ok:
        print(json.dumps(said, indent=1, default=str)[:12000])
    print(json.dumps({"checks": len(results), "passed": sum(results.values()), "ok": ok}))
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
