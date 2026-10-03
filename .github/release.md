## What's new in 0.32.0 (since 0.30.0)

0.31.0 and 0.31.1 were never released, so this covers both.

- **Faster.** The join order is tried from every input: TPC-DS q72 drops from 81 s to 0.17 s, and all 99 queries from 18.2 s to 12.1 s. Small queries cost half what they did. Flows stay within 5% of ingest. In-memory scans skip batches by their ranges and read a top-N in key order. A `SELECT *` top-N filters while it decodes Parquet: ClickBench from files beats DuckDB 1.5.5 (10.47 s against 11.10 s), and from memory takes 6.58 s. In-memory columns now hold what they count. Key lookups while writes land are 16× faster (27,600 a second).
- **A wrong answer fixed.** A global `min` and `max` of two things could skip rows it needed (wrong since before 0.30.0).
- **Runs for years.** Lakes have a format version. Every release's lake since 0.22 opens and answers as before. Rolling upgrades work. A stopped node drains (`/healthz`, `/ready`), and a leader steps down cleanly. A key lookup that could miss a live key is fixed.
- **Survives failures.** Every way Pondra runs is now checked under a failing bucket, a leader cut off, killed nodes and full disks. That check found five faults, all fixed: idempotent Kafka producers could lose acknowledged records across a failover, and a leader cut off from its bucket held every write. The Python and JavaScript clients now take every node's address and carry on when one goes down.
- **A table's past.** `UNDROP TABLE` brings back a dropped table within its retention (a day unless the table says otherwise). `SELECT … FROM t AT (VERSION => n | TIMESTAMP => '…' | OFFSET => -3600)` reads an append table as it was. `RESTORE TABLE t TO VERSION AS OF n` puts it back in one change, and `CREATE TABLE c CLONE t` copies it without copying a file.
- **Deployed your way.** There is a container image, a compose cluster, a Helm chart and `pondra service install` (systemd, launchd, Windows). A node sizes itself to its container's memory.
- **SQL.** `CREATE VIEW v (a, b)` names the view's columns. A transaction's `UPDATE` followed by an `INSERT` into the same table commits.
- **Scripts that decide.** A SQL script can branch, loop, handle errors and return a value (`IF`, `CASE`, `WHILE`, `REPEAT`, `LOOP`, `FOR r IN (query)`, `BEGIN … EXCEPTION … END`, `RETURN`, `RAISE`, `PRINT`, `ASSERT`, `EXECUTE IMMEDIATE`, `CALL … INTO`, `IDENTIFIER`), from every client: the console, files, `POST /sql`, psql and every Postgres driver, MCP, procedures and tasks.
- **Changed: a file's parameters are marked.** Only `DECLARE PARAMETER $day DATE = …` makes a parameter of a SQL file; a plain `DECLARE` is now the file's own variable, and a run given a name that isn't a parameter is refused. A file whose plain `DECLARE`s were its parameters needs `PARAMETER` added. A `.py` file's parameters are its `# %% tags=["parameters"]` cell.
- **The console.** Live cells share one connection. There is one grid everywhere, with undo, paste and filters. The Data tree has details, drag and drop and uploads. There is a schedule editor, users, roles and access, and a table's own tab that saves edits as one transaction.

The details are in [`docs/prototype-status.md`](https://github.com/alimardon123/pondra/blob/main/docs/prototype-status.md) (round 32) and on the [performance page](https://alimardon123.github.io/pondra/concepts/performance/).

## Install

| You have | Run | Then |
|---|---|---|
| Windows | `irm https://github.com/alimardon123/pondra/releases/latest/download/install.ps1 \| iex` | `pondra` |
| Linux, macOS | `curl -fsSL https://github.com/alimardon123/pondra/releases/latest/download/install.sh \| sh` | `pondra` |
| Python | `pip install pondra` (add `pyarrow` for pandas, Polars and Arrow) | `pondra`, `python -m pondra`, or `import pondra` |
| Node | `npm install -g pondra` | `pondra`, or `npx pondra` with no install |

The files below: the binary alone for each platform (`pondra-<platform>.tar.gz` / `.zip`, what the
installers download), the Python wheels and the npm packages. What changed is in the commits
below and in [`docs/prototype-status.md`](https://github.com/alimardon123/pondra/blob/main/docs/prototype-status.md).
