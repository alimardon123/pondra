# ADR-025: One name, one meaning (0.23.0)

**Date:** 2026-09-28 · **Status:** built · **Follows:** ADR-022 (frames), ADR-023 (frames and procedures)

## Context

The owner made a view from Python and was told it was materialized, though they had asked for a
view. Python's `db.view(name, sql)` (and JavaScript's, and `POST /views/{name}`) made a
*materialized* view, filled at once and kept up to date as rows arrive, while SQL's `CREATE VIEW`
and a frame's `to_view(name)` make a *stored query*, run when read. One word, two meanings,
depending on where you typed it. The owner's rule (2026-09-27): everything works the same from
every API, and the names are easy to remember; and frames and the connection should follow the
same ways.

Two more of the same kind:

- A connection had no way to write a table from Python data or a frame (`frame.write_table`
  existed, `db.write_table` didn't; `db.from_pandas(df).write_table(…)` was the way round).
- A stored procedure is `con.call(…)` in Python and `db.callProcedure(…)` in JavaScript (`call`
  was the JavaScript client's name for one HTTP request).

## Decision

**SQL's words are the names, everywhere.** `CREATE VIEW` is a view; `CREATE MATERIALIZED VIEW`
is a view with `materialized`:

| | a stored query | kept up to date |
|---|---|---|
| SQL | `CREATE [OR REPLACE] VIEW v AS …` | `CREATE MATERIALIZED VIEW v [WITH (window = 'w', …)] AS …` |
| Python, the connection | `db.view("v", sql_or_frame)` | `db.view("v", sql_or_frame, materialized=True, window="w", …)` |
| Python, a frame | `frame.to_view("v")` | `frame.to_view("v", materialized=True, window="w", …)` |
| PySpark's names | `df.to_view("v")` (and `createOrReplaceTempView` for the session) | `df.to_view("v", materialized=True, …)` |
| JavaScript | `db.view("v", sql)` | `db.view("v", sql, { materialized: true, window: "w", … })` |

- `db.view(name, x, …)` *is* `x.to_view(name, …)`: a connection's method takes what a frame's
  method is called on as its second argument. `db.write_table(name, data, mode)` is
  `frame.write_table(name, mode)` the same way; `data` may be a frame, SQL, or pandas / Polars /
  Arrow data. Both return the view or table as a frame.
- Window, session and join options belong to materialized views: given without
  `materialized=True`, they are refused (a frame's `to_view` used to drop them silently).
- **One release of grace:** `db.view(…)` with such options and no `materialized` still makes a
  materialized view, with a `DeprecationWarning` (JavaScript: `console.warn`). Without options it
  is now a stored query, which answers the same, computed when read.
- JavaScript: `db.call(name, …args)` calls a procedure, as in Python; the HTTP request is
  `db.request(method, path, …)`; `callProcedure` stays as another name for `call`.
- `POST /views/{name}` stays what it was (a materialized view), since clients up to 0.22 send
  their views there. The clients now send SQL.

## Rejected

- **`db.materialized_view(…)` beside `db.view(…)`.** Two methods where SQL has one word and a
  keyword; `materialized=True` reads like the SQL and matches `to_view`, which already had it.
- **Keeping `db.view` materialized and changing SQL.** SQL's `CREATE VIEW` means a stored query
  in every database; Pondra's clients follow SQL, not the other way round.

## Still open

- A listing can't tell the two apart yet: `information_schema.tables` (and the shell's `.tables`)
  says `VIEW` for both, since DataFusion's table types have no materialized view.

## Tests

`frames_check.py`, section 5 ("names"): `db.view` without `materialized` is a stored query, as
`CREATE VIEW` and `to_view` make; with `materialized=True`, from SQL or a frame, a materialized
one; all three answer alike; options without `materialized` are refused; 0.22's call with
`window=` is still materialized, with a warning; `db.write_table` takes pandas data, then a
frame. With 0.22's client the section fails (its `db.view` sends a frame as text). Packages:
`package_check.py` and `package_check.mjs` make a materialized view and a stored one, and
JavaScript calls a procedure with `db.call`.
