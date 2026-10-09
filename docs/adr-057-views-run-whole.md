# ADR-057: Views run whole again

**Date:** 2026-10-04 · **Status:** proposed, built on the recommended answer to the open card
"Keep every materialized view current: incremental, by key, or refreshed?" (All three ways) ·
**Builds on:** ADR-055 (views that finish their answers), ADR-056 (views kept by key)

## Context

ADR-055 keeps a `GROUP BY` of plain aggregates from each write's rows, and ADR-056 keeps one whose
aggregates need every row of a group by working out again the groups a commit touched. What was
left either was refused or, worse, was taken and answered wrongly. A view without a `GROUP BY` of
its own was derived from each write's rows alone, whatever it did with them: `ORDER BY … LIMIT 5`
kept each write's top five, `DISTINCT` each write's distinct rows, a window function each write's
ranks, and `WHERE amount > (SELECT avg(amount) FROM t)` each write's own average. One of them
(`ORDER BY … LIMIT`) failed its fill and held `CREATE` until it timed out. A median joined to
another table, a global median, a median of a keyed table, a view of an `avg` view and anything
calling `now()` were refused.

Alimardon's question on 2026-10-03 16:40 was whether every query can be a live materialized view.
Snowflake's dynamic tables and Databricks' materialized views answer what they can't keep
incrementally by running the query whole on a schedule (a target lag, or a refresh asked for).

## Decision

1. **The third way, chosen last.** A view is kept from each write's rows when it can be (ADR-055),
   by key when it can be (ADR-056), and otherwise its query is **run whole** again: no query that
   is a valid `SELECT` over this lake's tables is refused for how it is kept. `WITH (refresh =
   'full')` asks for it, and `WITH (refresh = 'incremental')` still refuses rather than fall back.
   `pondra.flows` says `full`, why (`ORDER BY or LIMIT over orders looks across its rows`), and
   every table it follows.
2. **Row by row means row by row.** A view without a `GROUP BY` of its own, or below that
   `GROUP BY`, is kept from each write's rows only if nothing in it looks across its source's rows:
   no `ORDER BY` or `LIMIT`, window, `DISTINCT`, `GROUP BY` or subquery over the source, and the
   source read once (`rerun::across`). Joins with other tables stay as they were (looked up as each
   write arrives).
3. **When it runs.** After a commit that changes any table of this lake it reads (found through
   stored views too), on the leader, beside views kept by key, at most so often that its runs take
   a tenth of the time (a run of 50 ms runs at most twice a second; one of 10 s every 100 s) and
   never more often than its `lag`. A query calling `now()`, or reading another lake's table or a
   file, also runs every `lag` (a minute unless given) with nothing changed here; it commits only
   if its answer moved.
4. **What it writes** is ADR-056's run, every row a group: the new rows that aren't among the old
   ones go in, the old ones that aren't among the new go to `{view}$deleted`, in one commit with
   its progress (`rerun:{view}`, the same producer as a view kept by key). A row that came out the
   same stays as it was. A `GROUP BY` under the `ORDER BY` keeps a group's row id when its answer
   changed (an update in the change feed); other rows are matched whole.
5. **Flows.** What follows it takes its rows back, as what follows a view kept by key does. A view
   over a view that runs again, which the rows alone can't keep, runs again itself: by key when it
   can be, else whole.
6. **`lag` belongs to runs.** On a view kept from each write's rows it is refused by name, since that
   view is current in each write's commit.

## Consequences

- Every query can be a materialized view, and the three ways are one statement with no options:
  Pondra picks the cheapest that is right, and says which.
- A full run costs the whole query each time. Pacing by its own length keeps the leader's share
  bounded with no setting; a `lag` bounds it further for a big view.
- Views without a `GROUP BY` that used `ORDER BY`, `LIMIT`, `DISTINCT`, windows or a subquery over
  their source were wrong before; made again, they are right. Existing ones keep their old way
  until made again (`CREATE OR REPLACE MATERIALIZED VIEW`).
- Next: `REFRESH MATERIALIZED VIEW v` running one now and waiting for it (today it is accepted and
  does nothing, as every view is current), and joins kept by key on their join key.
