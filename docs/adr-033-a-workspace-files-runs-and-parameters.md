# ADR-033: A workspace: files, runs and parameters

**Date:** 2026-09-29 · **Status:** accepted and built, 2026-09-30 (the owner moved it before round 27; the open questions were decided by Claude, marked below, for the owner's review) · **Builds on:** ADR-027 (procedures, schedules and the run log), ADR-030 and ADR-032 (the console, notebooks in the lake), ADR-034 (files edited in place)

## Context

The owner, 2026-09-29: "what do you think if we are able to… open, create SQL and Python files too,
and be able to run them… our SQL and Python seamless integrations… run each other or
parametrized. And in the future, even in the platform or in this open product, we can have ETL
platforms, dashboards, reports, etc."

What exists:

- **Notebooks** are saved in the lake as `.ipynb` versions (`files/notebooks/<name>/<time>.ipynb`).
- **Procedures** in SQL and Python (ADR-027), `DO` blocks, schedules, and a run log.
- **The console** edits and runs cells, and has an extension API (ADR-032).

Nothing runs a *file*, passes it parameters, or lets one file call another.

How the others do it:

- **Databricks:** workspace files, `%run ./other`, `dbutils.notebook.run(path, timeout,
  params)`, widgets as parameters.
- **Snowflake:** `EXECUTE IMMEDIATE FROM '@stage/file.sql' USING (name => value)`, with templating.
- **Jupyter:** papermill's "parameters" cell.

Duckle (an ETL studio on DuckDB) keeps its workspaces as plain files in a folder, which makes
them git-friendly, and advances a watermark only when a run fully succeeds.

## Decision (as proposed; Built, below, says what changed)

### 1. Files in the lake, edited in the console

`.sql`, `.py` and `.ipynb` files live under the lake's `files/` (say `files/workspace/…`).

- The console's Files section opens, creates, renames and edits them in the middle of the page,
  with the cells' editor, completion and Run.
- Each save is a version, as notebooks are, so a run records exactly what it ran and nothing is
  overwritten.
- The same files sync with a git folder (`pondra workspace pull | push <dir>`), so they can be
  reviewed like code.

### 2. One way to run anything, from every door (ADR-025)

```sql
CALL run('etl/orders.sql', day => DATE '2026-09-29');
CALL run('etl/score.py', model => 'v3');
CALL run('reports/weekly.ipynb', week => 39);
```

```python
db.run("etl/orders.sql", day=date(2026, 9, 29))
```

- **Parameters.** A `.sql` file names them as `$day` (bound, never pasted into the text). A `.py`
  file gets them as variables, as papermill's parameters cell does. A notebook takes them in its
  cell tagged `parameters`.
- **Files call each other** the same way, so SQL and Python chain freely. A run's answer is its
  last statement's rows, or its last expression's.
- **Rights.** A run has its caller's rights. Running Python needs what `DO` needs.

### 3. Runs are recorded; schedules run files

Each run writes the run log that schedules already use (ADR-027):

- what ran: the file and its version;
- the parameters;
- who ran it;
- when, and how long it took;
- rows, errors and notices.

A schedule can run a file or a notebook, and that is an ETL pipeline. Dependencies between runs
(a DAG), and watermarks that advance only on success, come after.

### 4. Later: dashboards and reports

A notebook with parameters, shown read-only as a page (its text, answers and figures), with its
parameters as inputs. The open product gets that. A platform adds more through the console's
extension API.

## What it costs

No new service and nothing always on: files in the lake, one procedure (`run`), and the run log.
The console grows by an editor view for files, reusing the cell's editor.

## Open questions

- Where the files live: under `files/`, or a `workspace/` prefix of their own with the catalog
  listing it.
- Versions per save (like notebooks), or git as the history.
- Whether `run` spreads a SQL file's statements over the cluster as any query spreads (yes by
  default), and where a Python file runs (the session's worker, or a fresh one).

## Built (2026-09-30)

What was built, and how the open questions were settled. **Decided by Claude, for the owner's
review**, where marked.

- **Where the files live:** under the lake's `files/`, as the console's Workspace shows them
  (ADR-034). No prefix of their own: a file anywhere there runs. *(Decided by Claude.)*
- **Versions:** a file is replaced in place when saved (ADR-034's `If-Match`), not kept per save;
  a run records the version that ran (`files/<path>@<etag>` in `pondra.runs`), and git is the
  history (the console downloads files; a `pondra` command that syncs a folder is left for later).
  Notebooks keep their versions as before, and `run('notebooks/<name>')` runs the newest.
  *(Decided by Claude: two kinds of history for text files would be one too many.)*
- **`CALL run(path, name => value, …)`** (`workspace.rs`): `run` is Pondra's own procedure
  (`CREATE PROCEDURE run` is refused). Its values are worked out once, as the caller, like a
  procedure's arguments.
  - `.sql`: `routines::script`, `$name` bound (never pasted in); the last statement's answer.
  - `.py`: a namespace of the run's own (a session kernel, ended with the run), the values as
    variables, prints as notices, the last expression as the answer. *(Decided by Claude: a fresh
    namespace per run, not the caller's session, so a job never depends on what a page ran.)*
  - `.ipynb`: code cells in order, `%%sql` ones as SQL (`$name`) and the rest as Python in one
    namespace; the values given are set after the cell tagged `parameters` (papermill's rule);
    Jupyter's `%` and `!` lines are skipped.
- **Every door:** SQL over HTTP and Postgres, MCP's `write` tool, a task, `pondra.start('run', …)`,
  and the clients: Python's `db.run(path, **values)` (a `.sql` file on the machine if there is one,
  else the lake's) and `pondra.run` inside a run, JavaScript's `db.run(path, values)`.
- **Rights:** each statement of a SQL file has the caller's rights; a file that runs Python needs
  an admin token, as `DO` does. Runs inside runs stop 16 deep.
- **The job:** a run retried with its job writes once; each statement (and each file it runs) gets
  its part of the job.
- **The console:** a SQL file's `$name`s get inputs above the editor, bound on the node; a file's
  and a notebook's ⋯ has **Run as a job** (`pondra.start('run', …)`) and **Schedule…** (a task);
  **Runs** lists the node's runs and the schedules (dropped from there).
- **Mistakes by name:** no such file, a file that doesn't run (`.csv`), a value not named, a name
  given twice, no saved notebook; a file missing values names all of them (`no value for $region,
  $amount`).
- **The console's budget** (ADR-034 §7): the parameters bar, the jobs and Runs took it to 72.4 KB;
  the served code now leaves out a comment after code too (`console::lean`, when no quote or `/`
  follows its `//`), which brings it to 71,632 bytes of the 71,680. The next change to the console
  makes room first.
- **Tests:** `harness.py workspace` (every door, parameters, notebooks, files running files, the
  run log, a task, the job, rights, mistakes by name) and `console_check.py files`.

Left for later: a command that syncs files with a git folder, dependencies between runs (a DAG),
watermarks that advance on success, and dashboards (§4).
