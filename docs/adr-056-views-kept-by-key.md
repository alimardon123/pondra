# ADR-056: Views kept current by key

**Date:** 2026-10-04 · **Status:** proposed, built on the recommended answer to the open card
"Keep every materialized view current: incremental, by key, or refreshed?" (All three ways) ·
**Builds on:** ADR-020 (rows that change: take-backs), ADR-036 (flows), ADR-055 (views that finish
their answers)

## Context

ADR-055 made every grouped view of plain aggregates incremental: a write's rows add their partial
rows in the write's own commit. What needs every row of a group was still refused: `median`,
percentiles, `count(DISTINCT …)`, `string_agg`, `array_agg`. Alimardon asked on 2026-10-03 16:40
whether every query could be a live materialized view, "smart and efficient and performant and
simple and lightweight", and the design he was shown (designs/sql-decisions-in-examples.md) answers
with three ways, picked per view and shown in `pondra.flows`: incremental (ADR-055), **by key**
(this ADR), and refreshed (next). Snowflake's dynamic tables and Databricks' materialized views
refresh whole when they can't go incrementally; Materialize and RisingWave keep every group's rows
in memory. Keeping by key sits between: nothing is held, and a write costs only the groups it
touches.

## Decision

1. **Chosen, not declared.** A `GROUP BY` the rows alone can't keep is kept by key when it can be:
   one table of this lake read alone (no join, no subquery), only `HAVING` above the `GROUP BY`,
   every `GROUP BY` expression among its columns, and no function that answers otherwise as time
   passes (`now()`, `random()`, a Python function not declared `IMMUTABLE`). Anything else is
   refused as before, with why. `WITH (refresh = 'by key')` asks for it on any such view (a sum
   too), `WITH (refresh = 'incremental')` refuses rather than fall back, and `WITH (lag = '1
   minute')` runs it at most once a lag. `pondra.flows` gains `refresh` and `reason`.
2. **A run, after commits, on the leader** (`rerun::run_all`, beside stream joins). The groups the
   commits since its last run touched are the view's `GROUP BY` over the rows they changed: what
   they wrote (`_version`), the old versions they replaced or deleted (`{t}$deleted`), and the rows
   other engines' file commits took out. Those groups are worked out again from the source as of
   the run's commit: the view's own query, its `GROUP BY` reading only their rows (a semi join on
   the keys, `IS NOT DISTINCT FROM`, so a NULL key is a group). The first run works out every group,
   and so does a run whose changes the source no longer holds whole (`past::kept_since`).
3. **In place of the old rows, in one commit with the progress.** The view's table is an append
   table that changes as `UPDATE` changes one (ADR-020): the old rows of the touched groups go to
   `{view}$deleted`. A row that came out the same stays; a group that came out different keeps its
   row's id, so the change feed shows an update; an emptied group's row goes. Both parts are
   committed under the producer `rerun:{view}` (and `…:deleted`), seq the run's commit and `prev`
   the last, so a run another leader already made is refused whole.
4. **Light on the leader.** A run is a task of its own and never holds up the leader's other work.
   A view runs again only once as long as its last run took has passed (and its `lag`). A run
   with nothing of its source changed does nothing, and the log is kept for a view kept by key until
   its runs have read it (`tier::expire`).
5. **Flows.** A view of a view kept by key takes its rows back, as a view of a changed table does:
   row by row, or a `GROUP BY` of sums and counts, or one kept by key itself. Anything that can't
   (a `min`, windows, sessions, stream joins) is refused by name.
6. **Format 2**, as ADR-055: release 0.32 would take such a view for one derived from each write.

## Consequences

- A median or a distinct count over a stream is a live view, kept with no state in memory: a
  write costs a read of its touched groups' rows, not of the table.
- It is current a moment after the commit, not in it: a run follows the commit. Read the source
  when you need the answer as of your own write.
- A run reads its touched groups' rows whole; a group of millions of rows that changes every second
  is better served incrementally (`approx_distinct`) or refreshed with a `lag`.
- Next: refreshed views for what can't be kept by key (`ORDER BY … LIMIT` across groups, joins
  whose other table changes, `now()`), then joins kept by key on their join key.
