# ADR-043: A file's parameters are declared as such; its other variables are its own

**Date:** 2026-10-03 · **Status:** accepted (the owner chose `DECLARE PARAMETER` on 2026-10-03) ·
**Changes:** ADR-037 §4 (a file's `DECLARE`s were all its parameters) · **Builds on:** ADR-033 (files
run with parameters), papermill's and jupytext's parameters cell

## Context

ADR-037 made every `DECLARE` in a file a parameter. A file that works with variables of its own (a
date derived from another, a switch an `IF` reads, a counter) then showed each of them in the bar
above it, in `pondra.parameters(…)`, in `pondra run --help` and in MCP's tools, and any caller could
set them. Everywhere else `DECLARE` means a local variable (T-SQL, BigQuery, Snowflake and
Databricks scripting, SQL/PSM), and parameters are the few values a caller gives. The owner asked
for one way that keeps the bar short, works the same in SQL files, Python files and notebooks, and
works outside the console.

## Decision

1. **`DECLARE PARAMETER $day DATE = current_date - 1;`** declares a parameter: a variable a run, a
   request's `params` or the console's bar may give a value for. Its type, default and comment are
   what tools show. A `$name` used before anything sets it is still a required parameter.
2. **A plain `DECLARE $since = $day - 7;` is the script's own variable.** It is never listed as a
   parameter. A value given for it is refused by name, at the `DECLARE` and, for a file run, before
   anything runs (`run: … has no parameter since (it takes $day)`). Without a default it is NULL
   (cast to its type, if it has one), as SQL's `DECLARE` is.
3. **A `.py` file's parameters are its `# %% tags=["parameters"]` cell** (jupytext's percent format,
   which papermill reads too): each `name = value` or `name: type = value` at its top level, the
   type from the annotation or the literal, the comment after it or above it as its description. A
   run runs the file up to and including that cell, sets the values given over its defaults, then
   runs the rest (its lines numbered as in the file). A name the cell doesn't set is refused. A file
   without that cell takes any value, set as a variable before it runs, as before.
4. **A notebook's parameters** are its cell tagged `parameters` (papermill's) and the `DECLARE
   PARAMETER`s of its SQL cells.
5. **One rule, on the node** (`workspace::parameters`): `pondra.parameters(file)`, `CALL run(…)`,
   `pondra run FILE --help`, the clients' `parameters` and the console read it alike.

## Consequences

- A file can use as many variables as it needs and still show its callers only what they may set.
- `IF`, dynamic SQL, functions and procedures use variables and parameters alike: both are `$name`,
  bound the same way (ADR-037 §2 stands).
- Files written for ADR-037, whose plain `DECLARE`s were parameters, need `PARAMETER` added. Before
  1.0 this is allowed; the release notes say it.
- `harness.py variables` (own variables, refusals, a `.py` file's cell), `console_check.py` (the bar
  lists only `DECLARE PARAMETER`s).
