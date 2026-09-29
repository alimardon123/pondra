# ADR-030: The console, the server and the docs (round 26)

**Date:** 2026-09-29 · **Status:** accepted and built (the owner's choice: all three in one round) · **Follows:** ADR-024 (nothing to set up), ADR-027 (SQL and Python as one), ADR-028 (one vocabulary)

## Context

Round 26 was planned as the console, `--server`, dbt and BI tools (roadmap A4, E1, E2). After
round 25 the owner added one more item, and chose to take it in the same round:

> "We need really beautiful and easy to read/understand and full doc on everything about our
> product. How to use it, what features it has… Now even I can't know exactly what things we have
> and how to use the product full power."

Where things stand:

- **A node serves one lake.** The shell attaches the lakes beside it (ADR-024). But no command
  serves a folder of lakes as a database server does, so a Postgres client can't pick a
  database by name.
- **Nothing is at `/`.** A new user's first five minutes are the shell or a notebook.
- **dbt stops at its first catalog query.** `dbt seed` fails with "table 'pg_tables' not found".
  Probing the SQL that dbt-postgres runs finds five more gaps:
  - `DELETE … USING`;
  - `UPDATE … FROM`;
  - `TRUNCATE`;
  - `ALTER TABLE … RENAME TO` (refused since round 21: a table's name is where its files live);
  - `ALTER VIEW … RENAME TO`.

  BI tools read far more of `pg_catalog` than the five views emulated today, and
  `pg_attribute`, which lists every table's columns, is empty.
- **The docs are written for builders.** There are 29 ADRs, `AGENTS.md`, the README and the notebook.
  Nobody can find out from them what Pondra does, or how to use it, without reading the design.

## Decision

### 1. `pondra server`: a folder of lakes, served as databases

```bash
pondra server ./data --pg 0.0.0.0:5432      # every lake in ./data is a database
psql -h host -d sales                       # the lake ./data/sales
curl host:8080/db/sales/sql -d 'SELECT 1'   # the same over HTTP; http://host:8080/ is the console
```

- **A command of its own.** The owner asked for new modes behind an explicit command.
  `pondra serve --dir lake` stays what it is: one lake, one node.
- **Each database is a node of its own**, started as a child process the first time a
  connection or request names it. It stops once nothing has used it for
  `PONDRA_DATABASE_IDLE_SECS` (600) and it has nothing left to tier. A database nobody uses costs
  nothing, and each lake keeps a node's whole life: leading, failover, restart, tiering.
- **The server process routes and holds no lake itself:**
  - Postgres: by the startup message's `database`, then it passes the bytes through;
  - HTTP: `/db/{name}/…` is that database's whole API;
  - `/` is the console;
  - `/databases` lists the databases.

  Requests without `/db/` go to the default database (`--default`, else `lake`, else the only
  one).
- **Every database sees the others as `name.schema.table`,** as the shell's lakes do.
  - `CREATE DATABASE x` makes `./data/x` and serves it.
  - `DROP DATABASE x` (admin only, server mode only) stops its node and deletes the folder, as
    Postgres does. It is refused while another process leads that lake.
- **Other nodes can join a database's cluster.** Its node advertises `host:port/db/name`, and
  the server passes `/db/name/cluster/…` through.
- **Flight and Kafka stay on single-lake nodes** (`pondra serve`), where a port means one lake.
  The server refuses `--flight` and `--kafka` by name, and says so.

### 2. The console at `/`

One page, embedded in the binary, is served by every node and by the server. It needs no install
and no CDN, and it works offline.

- **A sidebar:** databases, then schemas, then tables and views (with their row counts), then
  columns (with their types). Click a table to see its first rows.
- **Cells, as in a notebook:**
  - **SQL cells** run with Ctrl+Enter. They show a table with types, the row count, the time, and
    errors in plain words.
  - **Python cells** run on the node (below), with `db` (the connection) and frames. They show
    what the code printed and what its last expression is.
- **Live results:** a cell's "live" switch shows its answer as it changes (`/live`, round 25).
- **Notebooks:**
  - saved in the lake as `.ipynb` under `files/notebooks/`, and opened again from the sidebar;
  - the same format Jupyter reads, with SQL cells kept as `%%sql` cells;
  - download and upload.
- **Light and dark**, following the system, and keyboard first.

**`DO LANGUAGE python $$ … $$`**, Postgres's anonymous code block, is what a Python cell runs, from
every door (psql too).

- It works as a procedure that is made, called once and forgotten, with the caller's rights.
- It needs `--python` and an admin, as making a procedure does (invariant 85).
- What it prints comes back as notices. Its last expression comes back as rows when it is a frame
  or a table.

### 3. Postgres as dbt and BI tools use it

- **`pg_catalog` from the lake's catalog,** with the columns clients read:
  - `pg_namespace`, `pg_class` (tables, views, materialized views), `pg_attribute` (every
    column, with type, number, not-null and default), `pg_type` (with arrays);
  - `pg_index` and `pg_constraint` (a keyed table's primary key);
  - `pg_tables`, `pg_views` (with definitions), `pg_matviews`, `pg_proc` (the lake's functions
    and procedures), `pg_database` (every database the server holds);
  - `pg_roles` and `pg_user`, `pg_settings`, `pg_description`;
  - empty but present: `pg_depend`, `pg_rewrite`, `pg_inherits`, `pg_attrdef`, `pg_enum`,
    `pg_collation`, `pg_extension`, `pg_am`.
- **The functions clients call:**
  - `format_type`, `pg_get_viewdef`, `pg_get_expr`, `pg_get_constraintdef`;
  - `obj_description`, `col_description`;
  - `pg_table_is_visible`, `has_table_privilege` and `has_schema_privilege`;
  - `pg_get_userbyid`, `current_user` and `session_user`, `pg_encoding_to_char`,
    `pg_backend_pid`.
- **SQL that dbt emits:**
  - `ALTER TABLE | VIEW … RENAME TO`;
  - `TRUNCATE`;
  - `DELETE … USING`;
  - `UPDATE … FROM`;
  - `CREATE TEMPORARY TABLE … AS` (round 25).
- **Renaming a table:**
  - **The name is decoupled from the table's storage folder.** A table's entry records its
    `folder` (by default its name, so every existing lake reads as before). Every path to its
    files, manifests and Delta and Iceberg copies goes through it.
  - **How a rename runs:** the leader first tiers the table, so no log row still names it. Then,
    under the lake's lock and in one commit, it moves the entry, the table's `$deleted`
    companion, its Delta and Iceberg state and its published name.
  - **Refused** while a materialized view or a task follows the table, as a dropped column is.
    Stored views read by name, so after dbt's rename-and-replace they read the new table.
- **Transactions stay as they are.** `BEGIN` and `COMMIT` are accepted, and each statement
  commits on its own, as today. dbt's rename-and-replace is three statements, and each one
  holds. The docs say so, and say that `ROLLBACK` undoes nothing.

### 4. The documentation website

- **Starlight** (Astro's docs framework; the owner's choice), in `site/`. It is published to
  GitHub Pages by `pages.yml` on every release tag, at `https://alimardon123.github.io/pondra/`.
  The owner turns Pages on once (Settings → Pages → Source: GitHub Actions).
- **What's in it:**
  - **Start:** install, a first lake, a first query in each client, the console.
  - **Guides by task:**
    - load data;
    - query files and other lakes;
    - stream, with views and windows;
    - change rows;
    - functions and procedures in SQL and Python;
    - share with other engines;
    - run a cluster, and run on S3 or R2;
    - dbt and BI tools;
    - AI and files.
  - **Reference, a page per feature:**
    - SQL statements and functions;
    - the Python connection, frames and the PySpark layer;
    - JavaScript;
    - the HTTP API;
    - the Postgres, Kafka, Flight and Iceberg doors;
    - the command line and its settings;
    - system columns and tables.
  - **Concepts:** the lake, the log, tiering, keyed tables, views, the cluster, and the design
    notes (the ADRs).
- **Every example is shown the same way in each language.** Tabs give SQL, Python, PySpark and
  JavaScript side by side wherever a feature has them, and the reader's choice is remembered.
- **Every example runs in CI.** `tools/docs_check.py` runs each page's code blocks in order,
  against a fresh node:
  - SQL through the connection;
  - Python in one interpreter per page;
  - JavaScript under Node;
  - shell blocks in bash.

  A block marked `norun` is exempt. It is kept for what can't run here (Windows, a real cluster).
  A page whose examples fail fails the build, so the docs can't fall behind the code.
- **The README gets shorter** and points to the site. The ADRs stay in `docs/` as the design
  record.

## Rejected

- **Several lakes inside one node process.** Leadership, failover and restart are per process
  today: `cluster::restart` replaces the process, and settings are process-wide. A child per
  database keeps each lake's life as it is.
- **Adding pg_catalog through `datafusion-pg-catalog`.** It is 8,000 lines, it is built for
  DataFusion 54 (Pondra is on 55), and it would put a second DataFusion in the binary. What
  clients read is a few hundred lines over Pondra's own catalog. Its list of tables and functions
  is a useful checklist.
- **Real transactions for dbt.** They are a design of their own (multi-statement snapshots,
  rollback). No dbt materialization needs them, because each of its steps holds by itself.
- **A console loaded from a website** (as DuckDB's UI loads its assets): it would need the
  internet, and a version to match. This one is in the binary.
- **MkDocs or Material:** Material is in maintenance mode, and Starlight was the owner's choice.

## Tests (the plan)

- **The server** (`harness.py server`):
  - three lakes in a folder: psql and HTTP reach each by name, and joins across them work;
  - `CREATE DATABASE` and `DROP DATABASE`;
  - a database's node starts on first use and stops when idle;
  - a second node joins a database's cluster through the server;
  - the server killed and started again.
- **The console** (`console_check.py`, headless Chromium through Playwright):
  - the tree lists what the catalog has;
  - a SQL cell and a Python cell run;
  - a live cell updates after an `INSERT`;
  - a notebook saved and opened again equals what was saved, and Jupyter's `nbformat` reads it;
  - no request leaves the node.
- **dbt** (`clients_check.py dbt`): a project with seeds, a view, a table, an incremental model
  (`delete+insert`, `merge` and `append`), a snapshot, data tests and `dbt docs generate`. Run
  twice (the second run renames and replaces), with the same rows as Postgres 16 gives.
- **BI drivers** (`clients_check.py drivers`):
  - pgjdbc's `DatabaseMetaData`: catalogs, schemas, tables, columns, primary keys (what DBeaver
    and DataGrip call);
  - psqlODBC's catalog functions through pyodbc (what Tableau and Excel call);
  - Npgsql (the driver in Power BI's PostgreSQL connector), if its SDK fits on the sandbox's
    disk;
  - ADBC Flight SQL.
  
  For Power BI Desktop on Windows, a script and a checklist for the owner.
- **Renaming a table:** under streaming ingest, with a Delta and an Iceberg reader, the change
  feed and a stored view; refused while a materialized view follows it.
- **The docs:** `docs_check.py` passes for every page, and the site builds in CI.
- **Every new invariant** gets a test in `tools/` that fails without it.

## As built (round 26)

Built as decided, with these differences and details:

- **The server:**
  - `pondra serve --advertise host:port/db/name` is how a database's node names itself to its
    cluster behind the server.
  - The server's folder is on its own disk. A folder in a bucket (listing its lakes, making and
    dropping them there) is left for a later round; until then, `pondra serve` per lake.
  - `DROP DATABASE` runs from another database, as in Postgres. On a plain node it says to use
    `DETACH`.
  - A stopped database answered its first query in about 70 ms on local disk.
- **The console** (`src/console.html`, about 1,000 lines, one file):
  - SQL cells use `POST /sql?format=typed`: the columns with their types, rows as lists (a
    join's repeated names survive), the first 10,000 rows, decimals and integers past 2^53 as
    text.
  - Python cells are `DO LANGUAGE python` blocks. Each runs on its own, so cells don't share
    variables; a temporary table (the page's session) carries data between them.
  - Notebooks are saved as `files/notebooks/<name>/<time>.ipynb`, a version per save, because a
    file in the lake is never replaced. SQL cells are `%%sql` cells, and each answer keeps its
    first 100 rows in the cell's outputs (`application/vnd.pondra.rows+json`, and a text table
    for Jupyter and GitHub).
  - Jupyter's keys. Not built: completion, charts.
- **Postgres's catalog** (`src/pg_catalog.rs`, 1,100 lines): the tables and functions listed
  above, `information_schema`'s `table_constraints` and `key_column_usage`, and a rewriter (on
  the Postgres dialect's AST) for what DataFusion lacks: `COLLATE`, `LIKE … ESCAPE`,
  `OPERATOR(…)`, casts to `reg*` types, `ARRAY(subquery)`, the correlated subqueries psql writes,
  `generate_subscripts`, `_pg_expandarray` (pgjdbc) and `int2vector` subscripts (psqlODBC).
  Tested: dbt (the same rows as Postgres 16, run twice), psql, SQLAlchemy with psycopg 2 and 3,
  pgjdbc, psqlODBC, ADBC's Postgres driver, and Npgsql 4.0 (what Power BI Desktop's PostgreSQL
  connector carries) and 8, through the .NET SDK. Npgsql loads the server's types by joining
  `pg_proc` on a type's `typreceive`, so `pg_proc` lists the type functions and such a join is
  made by name. Power BI Desktop itself (Windows only) is not run here.
- **Renaming:** a table's entry records its `folder`, and a new table under a used name gets
  `name__N` (object stores percent-encode `~`).
- **The website:** 49 pages, 456 examples, all run by `docs_check.py` in CI. `pages.yml`
  publishes it with each release tag (and by hand). A `python cell` block runs as the console
  runs it. The README points to the site at its top and keeps its overview: GitHub's front page
  is where most people land first.
- **Writing the site found 37 bugs**, all fixed with checks (`harness.py found` and others).
  Finishing it found six more: `pondra sql` didn't check `NOT NULL`, ADBC's Postgres driver
  couldn't read `pg_type`, Npgsql knew none of the types, a time without seconds wasn't a
  timestamp, tables were views to `information_schema`, and a `files()` listing could be a
  remembered answer. AGENTS.md
  invariants 129–138.
