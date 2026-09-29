# ADR-033: A workspace: files, runs and parameters

**Date:** 2026-09-29 · **Status:** proposed (the owner's idea; to be built after the base-binary rounds the owner put first) · **Builds on:** ADR-027 (procedures, schedules and the run log), ADR-030 and ADR-032 (the console, notebooks in the lake)

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

## Decision (proposed)

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
