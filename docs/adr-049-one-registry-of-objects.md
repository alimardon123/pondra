# ADR-049: One registry of objects, the same words for every kind

**Date:** 2026-10-03 · **Status:** accepted (Alimardon's card, 16:04: "next, with the registry") ·
**Builds on:** the SQL review (designs/sql-statement-design-review.md, finding 3), invariant 104
(a catalog prefix per kind), invariant 173 (one listing of what a lake holds), ADR-034's third list
(the console's actions are SQL)

## Context

Each kind of object grew its own listing and its own words: `pondra.tables`, `pondra.routines`,
`pondra.tasks`, `SHOW USERS`, `SHOW SECRETS`, `/objects`. There was no `COMMENT ON`, `SHOW CREATE`
gave column names instead of a statement, and nothing made a table *or* brought the one there to a
definition, which is what a project's files (dbt, environments, ADR-047) run again and again. Every
new kind (sharing's shares and recipients, environments' databases, types and sequences later)
would have added one more of each. Snowflake, Databricks, BigQuery and Postgres all have the same
three: a catalog view of every object, a comment on any object, and the statement that makes it.

## Decision

1. **One registry** (`src/objects.rs`). A kind is a `Kind` entry in `KINDS` (its SQL name, its
   family, the statements it takes), and a family has one lister in `FAMILIES` that reads its
   catalog entries into `Object`s (kind, lake, schema, name, definition). A new kind adds an
   entry and, if it is a new family, a lister. Nothing else is edited for it to be listed,
   commented and shown. Kinds of one family share names (a table and a view can't both be `orders`).
2. **`pondra.objects`** lists every object of every kind, this lake's and the attached lakes',
   with its comment and its definition; `pondra.kinds` and `GET /kinds` list the kinds. A user
   given grants sees what they may read, and no secrets, users or roles.
3. **`SHOW CREATE <kind> name`** answers the statements that make it again, comments included,
   as one `definition`: a table with its layout clauses (PR #24), constraints and
   options; a materialized view with its expectations and window; a function, macro, procedure or
   task as written in Pondra's forms. Running it after a drop makes the same object. A secret's
   values and a user's password are never shown, and are refused by name.
4. **`COMMENT [IF EXISTS] ON <kind> name IS '…' | $$…$$ | NULL`**, `COLUMN t.c` too. Comments are
   kept apart from what they describe (`cm/{family}/{name}`, `cm/column/{table}/{stored column}`),
   so no kind's entry changes shape. A rename moves them and a drop removes them, in one place
   (`objects::follow`, from `ddl::apply`). A column's comment is keyed by its stored name, so it
   survives `RENAME COLUMN` (invariant 74).
5. **`CREATE OR ALTER TABLE`** (Snowflake's words) makes the table, or brings the one there to the
   definition without losing a row. Columns are added at the end, types widened, and the layout and
   options set as written, with one left out going back to its default. Anything that would lose or
   reinterpret rows is refused by name: a column taken away, moved or narrowed, a changed key,
   partition, merge function, `NOT NULL`, `DEFAULT` or `CHECK`. It answers `created`,
   `unchanged`, or `altered` with each change. `CREATE OR ALTER VIEW` is `CREATE OR REPLACE
   VIEW`. Other kinds refuse it, naming `CREATE OR REPLACE`.

## Consequences

- Sharing (ADR-046) and environments (ADR-047) add their kinds as entries, once they merge.
  `CREATE TYPE`, `SEQUENCE` and `COMMENT` on them follow the same way (the main thread left
  them to the registry).
- The console's Data tree and its "Script as" can read `pondra.objects` instead of a listing per kind.
- `pondra.tables`, `SHOW …` and `/objects` stay as they are. They answer what tools already read.
