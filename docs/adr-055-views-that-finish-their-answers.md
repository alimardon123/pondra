# ADR-055: Views that finish their answers as they are read

**Date:** 2026-10-04 · **Status:** accepted (Alimardon, 2026-10-03 16:40: "please fix all the
drawbacks first … are we able to support all the sqls in materialized views as incremental and
live?") · **Builds on:** ADR-005 (every node writes), ADR-020 (rows that change: take-backs),
ADR-036 (flows), ADR-052 (windows as SQL)

## Context

A materialized view with `GROUP BY` is a merge table: every node adds partial rows (a `sum`, a
`count`, a `min`, a `max` per group) in the same commit as the rows it read, and reads combine them.
That is what keeps a grouped view current at any write rate, from every node, with no state held
anywhere. But only those four aggregates could be combined, so the view refused what people write
first. That included `avg(x)`, `stddev(x)`, `HAVING count(*) > 10`, `sum(a) / sum(b)` and
`ORDER BY total DESC LIMIT 10`, and it also refused a key left out of the `SELECT`. The docs told
people to keep `sum` and `count` and divide when they read. That is the kind of drawback Alimardon
asked to fix before calling `EMIT FINAL` done.

## Decision

1. **A grouped view's query is split in two where SQL comes in** (`finish::split`). The partial
   query keeps, per group, what can be added up: `count(*)` (always, as `__count`), and per column
   the `sum`, `count`, `min`, `max` or moments it needs. The finishing query works the answers
   out from those columns, as written: `avg` is the sum over the count, an expression over
   aggregates is that expression over their columns, `HAVING` becomes its `WHERE`, and `ORDER BY`,
   `LIMIT` and `QUALIFY` stay. A query that needs no finishing step (plain `count`, `sum`, `min`,
   `max`) is kept exactly as before, so nothing already made changes.
2. **A variance is kept as moments** (`pondra_moments`: a struct of the count, the mean and the sum
   of squared deviations). Rows are folded in one pass (Welford) and partial rows are combined
   with Chan's formula, so `stddev`, `variance` and their `_samp` and `_pop` forms stay exact to the
   last few bits however the rows were split across nodes, files and commits. A row taken back
   (`UPDATE`, `DELETE`: ADR-020) is its moments with the count and the squares negated, which the
   same formula subtracts. `bool_and` and `bool_or` are a `min` and a `max`.
3. **Every read finishes it** (`TableMeta::finish`, applied in `query::table_view`'s merge branch).
   That is the one place the merge table is read. So a query, a spread query's whole tables
   (invariant 34) and `EMIT FINAL`'s emission (ADR-052) all see finished answers. Each
   answer column is cast to the type the query gives it when run by hand. An `avg` of a `DECIMAL`
   is the decimal division, so it is exact.
4. **The view is shown as written.** `View.written` keeps the query, `SHOW CREATE` and the
   listings give it, and `CREATE … IF NOT EXISTS` compares with it. `SELECT *`,
   `information_schema`, `pg_catalog` and MCP show the columns it answers, not its partial ones
   (`TableMeta::described`).
5. **Refused by name, never wrong.** The following are refused:
   - `count(DISTINCT …)`, `median` and every other aggregate that needs every row: work them out
     when reading, with a stored view;
   - a view of such a view, because there are no rows to follow: use its source;
   - `DETACH` of one: its partial rows mean nothing without the finishing query;
   - a window in the options form: write it as `EMIT FINAL`;
   - `EMIT FINAL` with `ORDER BY` or `LIMIT`: sort when you read.
6. **Format 2.** Release 0.32 would read such a table's partial columns as the view. So the leader
   calls `format::require(lake, 2, …)` before it makes the first one, and a lake that never has one
   still opens in 0.32.

## Consequences

- To Alimardon's question: every grouped view of plain aggregates is incremental and live with no
  full load. That covers expressions over aggregates, `HAVING`, `ORDER BY … LIMIT`, and keys not
  selected. A new view fills once from the rows already there, as before (invariant 76), and from
  then on each write adds its partial rows in its own commit.
- What still needs every row is refused, never recomputed in secret. The next step on the same
  shape is sketches, which add up as moments do: `approx_distinct` as a HyperLogLog
  (`sketch.rs` has one), and `approx_percentile_cont` as a t-digest. Each is a kind of partial
  column plus a line in `Parts::of`.
- Reads do a little more work: one projection, and a filter for `HAVING`, over rows already
  combined per group. A view without a finishing step reads exactly as before.
- `harness.py finishes` checks all of this, and `finish.rs`'s unit tests check the split and the
  moments.
