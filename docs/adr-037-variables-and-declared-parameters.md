# ADR-037: Variables, and a file's parameters declared

**Date:** 2026-10-01 · **Status:** accepted (the owner chose the syntax on 2026-10-01) · **Builds on:**
ADR-027 (procedures, `$name` parameters), ADR-033 (the workspace: files run with parameters),
round 31's session settings (`settings.rs`)

## Context

A `.sql` file of the lake takes parameters as `$day`, bound when it runs (`CALL run('f.sql', day =>
…)`). Nothing said which parameters a file has, of what type, with what default, or what each
means: a person had to read the file, and the console's bar above it and other tools could only
guess from the `$name`s. And a session had no way to hold a value for its statements: DuckDB has
`SET VARIABLE`, Snowflake `SET x = …` with `$x`, Postgres nothing outside psql's `\set`.

The owner's conditions (2026-10-01): one familiar thing, not a `DECLARE` plus a `SET` pair; a
declaration people and tools can see, with a type and a default; and `SET` stays the settings'
(`SET datafusion.…`, Postgres's names), so a variable never collides with one.

## Decision

1. **`DECLARE $day DATE = current_date - 1;`** declares a variable. Its type and its value (`=` or
   `DEFAULT`) are both optional. **`$day = $day + 1;`** changes it: no `SET`. The `$` keeps both
   apart from Postgres's `DECLARE` (a cursor) and from settings. DuckDB's `SET VARIABLE x = …`,
   `RESET VARIABLE x` and `getvariable('x')` are other names for the same (ADR-028's rule: the
   tools' names as fallbacks for one implementation).
2. **A value is worked out once, as its caller, when it is set**, and bound wherever `$day` is
   used, as a parameter is: a typed literal in the syntax tree, never text pasted in. A declared
   type casts every value the variable takes; one that can't be cast is refused by name.
3. **Where a variable lives.** A session's (a Postgres connection, a client's `x-pondra-session`,
   a console tab, a script sent at once), as settings are. A procedure and a file run have their
   own (`vars::own`), shared by the connection their Python is lent (`auth::lend` carries them), so
   Python's `db.vars.day` and SQL's `$day` are one value. With no session, a `DECLARE` says it
   needs one rather than keeping a value nobody can read.
4. **A run's given values** (`CALL run(…, day => …)`, the console's bar, a request's `params`) are
   its run's: `$day` is the value given until something sets it, and a `DECLARE` takes the value
   given in place of its default, cast to its type (declared, else its default's). So in a file a
   `DECLARE` *is* a parameter with a default, a `DECLARE` without one a required parameter, and a
   `$name` used but never set a required one too.
5. **Discoverable:** `SELECT * FROM pondra.parameters('etl/orders.sql')` lists a file's parameters
   in order (name, type, default, required, description: the comment line above the `DECLARE`,
   or a `-- $name: …` line); `db.parameters(file)` in Python; `pondra.variables` lists a session's
   or a run's variables (never answered from the result cache, run on its node).
6. **A column that uses a variable and has no name is named as written** (`$day + 1`), as
   Snowflake names `$x`, not after the literal put in its place.

## Consequences

- A file says what it takes; the console, MCP and `pondra run --help` can read it instead of
  guessing. The bar above a SQL file can show types, defaults and descriptions.
- Variables cost nothing to statements that don't use them: a statement is parsed for binding only
  when it holds a `$name` outside strings and comments, or says `getvariable`.
- A variable holds one value (a number, a string, a date, …), not a table: a table is a temporary
  table or a CTE.
- Invariant 198 (AGENTS.md); `harness.py variables`.
