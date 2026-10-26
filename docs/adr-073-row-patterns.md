# ADR-073: Row patterns (`MATCH_RECOGNIZE`)

**Date:** 2026-10-12 · **Status:** accepted (Alimardon chose "Before 1.0", 2026-10-12 21:07); phase 1 building (ONE ROW PER MATCH, every operator and skip rule, #231; ALL ROWS PER MATCH and its options, #234); phase 2 built (`WITHIN`, #240; conditions that read the match so far, 2026-10-26) · **Builds on:**
ADR-017 (watermarks), ADR-052 (`EMIT FINAL`), ADR-056 and ADR-059 (views that run their query
again, by key), ADR-072 (one engine in every form) · **Lane:** the coordinator's call (L6 owns the
SQL front; L1 owns `rerun.rs`, `views.rs`, `store.rs`)

## Context

Finding a shape in a sequence of rows (a price that falls, then rises; a login that fails three
times, then succeeds; a sensor above a limit for five readings in a row) is what Flink's CEP and
its `MATCH_RECOGNIZE` are used for, and what Trino, Snowflake and Oracle answer in batch. Pondra
has no way to say it today: people write it with `lag`, `lead` and running counts, which works for
fixed shapes and fails for "one or more" and "either of".

The standard (SQL:2016, ISO/IEC 19075-5) is the clause to take. It is already people's word for
it, and sqlparser 0.62 parses it with the dialect Pondra uses (`TableFactor::MatchRecognize`:
`PARTITION BY`, `ORDER BY`, `MEASURES`, `ONE ROW | ALL ROWS PER MATCH`, `AFTER MATCH SKIP`,
`PATTERN`, `DEFINE`). DataFusion refuses it. What each rival takes:

| | Flink 2.3 | Trino | Snowflake | Pondra (this ADR) |
|---|---|---|---|---|
| `ONE ROW PER MATCH` | yes | yes | yes | yes |
| `ALL ROWS PER MATCH` | no | yes | yes | yes |
| `*` `+` `?` `{n,m}`, alternation, groups | yes | yes | yes | yes |
| reluctant (`+?`), `PERMUTE`, `{- A -}`, `^` `$` | partly | yes | yes | yes |
| `DEFINE` reading the match so far (`A.price`, `LAST(A.price, 1)`, `avg(A.price)`) | yes | yes | yes | yes |
| `WITHIN INTERVAL` | yes | no | no | yes |
| over a stream, each match once | yes | no | no | yes (`EMIT FINAL`) |
| live (the view always equals the query) | no | no | no | yes |
| spread over machines | by key | by key | by key | by key |

## Decision

1. **`MATCH_RECOGNIZE` is rewritten where SQL comes in into plain SQL around one window
   function**, in a module of its own (`src/pattern.rs`, called from `front::expand` after the
   macros, as `types::rewrite` is). The query after it is ordinary SQL, so the result cache, a
   spread query, a view's stored SQL and history see nothing new. A stored view keeps the clause as
   written and is rewritten as it is read (`expand_stored`).

   ```sql
   SELECT * FROM ticks MATCH_RECOGNIZE (
     PARTITION BY symbol ORDER BY ts
     MEASURES FIRST(down.ts) AS fell_at, LAST(up.ts) AS rose_at, min(down.price) AS low
     ONE ROW PER MATCH
     PATTERN (strt down+ up+)
     DEFINE down AS price < PREV(price), up AS price > PREV(price))
   ```
   becomes three steps:
   - **The conditions.** Each `DEFINE` that reads only the current row and its neighbours is a
     column: `PREV(x, n)` is `lag(x, n)`, `NEXT(x, n)` is `lead(x, n)` over `(PARTITION BY …
     ORDER BY …)`, a NULL is false, and a symbol with no `DEFINE` is true.
   - **The match.** `pondra_match(spec, d_strt, d_down, d_up, …) OVER (PARTITION BY symbol ORDER BY
     ts)`, a window function, answers for each row the match it is in, its symbol, its place in the
     match and whether it is the match's last row (NULL when in none). `spec` holds the pattern,
     the symbols and the skip rule. Where matches may overlap (`AFTER MATCH SKIP TO NEXT ROW`, `TO
     FIRST | LAST v`) it answers a list per row, unnested after.
   - **The measures**, as SQL over the matched rows. `ONE ROW PER MATCH` is a `GROUP BY` of the
     partition columns and the match number: `FIRST(down.ts)` is `first_value(ts ORDER BY __seq)
     FILTER (WHERE __symbol = 'down')`, `LAST(v.x, n)` is `nth_value` from the end, `v.x` alone is
     `LAST(v.x)`, an aggregate of `v.x` is that aggregate filtered to `v`, a bare column is the
     match's last row's, `CLASSIFIER()` the last row's symbol, `MATCH_NUMBER()` the match.
     `ALL ROWS PER MATCH` is every matched row with its measures as windows over `(PARTITION BY
     key, match ORDER BY __seq)`: `RUNNING` (the default) frames to the current row, `FINAL` to the
     match's end. A navigation there is `pondra_nav(x, keep, m, first, final)`, a window function that reads
     the m-th kept row from the first or the last, up to the row or over the whole match, in one pass
     (DataFusion's `first_value` and `last_value` take no `FILTER` in a window); an aggregate is the
     same aggregate, its `FILTER` kept, as a window framed to the row or to the match's end.
     `SHOW EMPTY MATCHES`, `OMIT EMPTY MATCHES` and `WITH UNMATCHED ROWS` are filters on what the
     matcher answered. `RUNNING` and `FINAL` before a measure, which sqlparser doesn't read, are
     taken in the text first, as `friendly.rs` takes DuckDB's spellings. `PATTERN (…)` is read by Pondra's own
   parser, from the standard's grammar, taken out of the text the same way: sqlparser 0.62 reads `a {- b -} c`'s `{` as a
   count, and takes only names in an exclusion and in `PERMUTE`.

2. **The matcher runs the standard's preference order in linear time where it can.** The pattern
   is compiled to an automaton (bounded repetitions copied out, `PERMUTE` to its alternatives in
   the order the standard prefers, reluctant `+?` read from sqlparser's nested repetition). Each
   match starts where the skip rule says and is the most preferred one from there: the leftmost
   alternative, greedy taking more and reluctant fewer. While every condition reads only its own
   row and neighbours, the matcher is a Pike VM: threads in preference order, one per state, so a
   partition takes time in proportion to its rows (`SKIP TO NEXT ROW` re-reads, as the standard
   asks). A pattern compiled past 10,000 states is refused by name (54000).

3. **A condition that reads the match so far is worked out inside the matcher.** `B AS B.price <
   A.price`, `LAST(down.price, 1)`, `avg(A.price) < 15` depend on which rows the match gave to which
   symbol, so they can't be columns. The rewrite splits such a `DEFINE` at its top-level `AND`s: the
   conjuncts that read only the row stay the symbol's column, as before. In the others, each part that
   reads only the row (columns, `PREV`/`NEXT` of them, `CLASSIFIER()` as the symbol's name, any
   function over those) becomes a column too, planned with the statement. What is left, the glue,
   names those columns and one slot per navigation (`FIRST`, `LAST`, another symbol's column,
   `PREV`/`NEXT` of them), aggregate (`count`, `sum`, `avg`, `min`, `max`) or `MATCH_NUMBER()`.
   The matcher plans each glue once, as SQL over those names, against a snapshot of the engine's
   session (its functions, analyzer rules and simplifier), so it is DataFusion's own expression.
   Each thread keeps its slots' state: a navigation's row numbers, a count, a sum as MEASURES sums
   it (wrapping integers, exact decimals, fsum's two-part DOUBLE), an average, a min or max. A row
   is taken in three steps: every thread at a symbol takes it and checks the row-only conjuncts; each
   glue runs once over all those threads; the passing threads go on in their order of preference.
   Threads at one state merge only when their slots are equal too (and their starts, under
   `WITHIN`). Past 100,000 live threads the search is refused (54000): the worst case is quadratic
   in a match's length, and the cap bounds it. Still refused by name (0A000), each saying what to
   write: an aggregate's own `FILTER`, `ORDER BY` or `DISTINCT`, aggregates other than the five, `avg`
   of a decimal, `CLASSIFIER()` inside a navigation or an aggregate, a navigation of a navigation
   deeper than `PREV`/`NEXT` of `FIRST`/`LAST`, a subquery.

4. **Rows tied on the `ORDER BY` are taken in `_row_id` order** when the input is a lake table
   (the rewrite adds it as the last key), so every run and every node finds the same matches, and a
   view that runs again takes nothing back for a tie. Other inputs (files, a CTE) match ties in the
   order the engine reads them, as Trino and Oracle do; the reference page says to add a column.

5. **Flink's `WITHIN INTERVAL '…'`** after `PATTERN (…)` is taken in the text and given to the
   matcher: a match must end within that time of its first row (the `ORDER BY`'s first column,
   ascending, a date or a timestamp). The bound is inclusive, as Flink's documented example has it
   (12:00 to 13:00 is within an hour). A thread that would take a row too late dies and the others go
   on, so a shorter repetition or a later start can still match; threads at one state are kept per
   start, at most 1,000,000 at once (54000). The interval must be an interval literal, written back as
   sqlparser reads it. Over a stream it also decides a match once the watermark passes it.

6. **Spread by key, as any window.** `pondra_match` partitions by the `PARTITION BY`, so a spread
   query hashes by it (a Hash exchange) and each node matches its keys whole. With no `PARTITION
   BY`, the rows are one sequence and run on one node, as any window over all rows does.

7. **A view of a pattern is live by key.** `CREATE MATERIALIZED VIEW v AS SELECT … MATCH_RECOGNIZE
   (…)` is a view of windows partitioned by its keys (invariant 284), so each run matches only the
   keys whose rows moved, and the view always equals the query, late rows too. For `ONE ROW PER
   MATCH`'s `GROUP BY` and the unnest, `rerun::window` learns to pass an aggregation grouped by every
   key and an unnest (the key still traces through as a group column).

8. **A pattern view with `EMIT FINAL` keeps each match once, when it is decided**, as Flink's CEP
   does, at a cost that doesn't grow with each key's history:
   ```sql
   CREATE MATERIALIZED VIEW dips WITH (lateness = '10 seconds') AS
     SELECT * FROM ticks MATCH_RECOGNIZE (…) EMIT FINAL;
   ```
   - A match is decided when no thread could still extend or replace it, or `WITHIN` has passed.
   - Only rows up to the watermark are matched (ADR-052).
   - Each key keeps a resume point, in a hidden keyed table `{v}$resume`: the first row an
     undecided match could start at, less the rows `PREV` looks back.
   - A run reads each touched key's rows from its resume point, appends the matches decided since,
     and moves the resume points, in one commit with the producer `match:{v}` (seq the watermark,
     `prev` the last one: invariant 24's mechanism).
   - A row behind its key's resume point is late: it stays in its table, and counts as the view's
     `$late` (ADR-052 §4).

9. **Per-partition watermarks** come with L4's Kafka partitions (round 36). A feed commits each
   partition's newest event time with its offsets (`feeds::shard`), and `views::newest` takes the
   least over the partitions that had a row within `idle`. Windows, sessions and pattern views all
   read the watermark there, so all three gain it at once.

## Phases (all before the freeze)

1. Steps 1, 2, 4 and 6: conditions on the row and its neighbours, every pattern operator, both
   `ROWS PER MATCH`, every skip rule, the measures, spread. A live view needs `rerun::window`'s
   change (step 7).
2. Steps 3 and 5: conditions reading the match so far, and `WITHIN`.
3. Step 8 with L1's view runs (dealt to every node, roadmap task "view runs on every node"), then
   step 9 with L4.

## Proof

- **Answers.** `harness.py patterns`:
  - A reference matcher in Python, written from the standard's text and kept apart from the Rust,
    against Pondra on random sequences and random patterns (every operator, every skip rule, both
    row modes, empty matches), as `random_sql.py` checks against DuckDB.
  - Every example in Trino's and Oracle's documentation, with their published answers.
  - The same on three nodes == one node, over Postgres, in a stored view, a live view through
    changes and a restart, and an `EMIT FINAL` view through a leader's kill: each match once.
- **Against Flink 2.3** (`tools/bench/patterns.py`, Flink's own CEP examples and a stock ticker of
  10M rows over 1,000 symbols):
  - the same matches;
  - batch time on one node and three;
  - over a stream: events a second in, and how long after the row that decides a match it is in
    the view.
- **Costs nothing unused**: TPC-H, TPC-DS and the gates unchanged (the rewrite runs only on
  statements that contain the word).

## Refused by name

- A `DEFINE` naming a symbol not in the pattern (42601).
- A navigation of a navigation outside `PREV(LAST(…))`'s standard forms (0A000).
- `MATCH_RECOGNIZE` over a stream's join or an outer join's padded side in a view kept by key
  (it runs whole, as any such window view does).
