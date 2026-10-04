# ADR-052: Windows as SQL: `EMIT FINAL`

**Date:** 2026-10-03 · **Status:** accepted (Alimardon, 16:40: "I am fine with simple version
then. But please fix all the drawbacks first") · **Builds on:** ADR-017 (streams on their own
time: watermarks, windows, sessions), ADR-036 (flows and expectations), the SQL review
(designs/sql-decisions-in-examples.md, decision 2)

## Context

Windows and sessions were options of a materialized view: `WITH (window = 'w', size_secs = 60,
lateness_secs = 10)` kept every window, open or not, in the view and each one once in a second table,
`{view}_final`. `WITH (session = 'ts', gap_secs = 30)` grouped by a key and added the session
columns. Three things about it read badly. The window's size was said twice, once in `date_bin` and
once in `size_secs`. The table people want, the finished windows, had a name they didn't choose. And
every option was a number of seconds. Flink, RisingWave and Materialize spell windows as table
functions (`TUMBLE(…)`, `HOP(…)`), which Alimardon found neither simple nor beautiful (16:05).

## Decision

1. **A window is the GROUP BY the query already has, and `EMIT FINAL` says to keep each group once,
   when it is over.**
   ```sql
   CREATE MATERIALIZED VIEW per_minute WITH (lateness = '10 seconds') AS
     SELECT date_bin(INTERVAL '1 minute', ts) AS minute, user, count(*) AS n
     FROM clicks GROUP BY 1, 2
     EMIT FINAL;
   ```
   The query is one people run by hand, and running it by hand answers the open groups too. The
   view is named what it holds.
2. **Any bucket that only grows with time is a window.** One GROUP BY expression must be a bucket
   of one timestamp column of the source (`once::grows`): the column itself, `date_bin`,
   `date_trunc`, `time_bucket`, a date, a year, seconds since the epoch, `floor`/`ceil`/`round`/
   `trunc` of one, a cast to a date, timestamp or number, plus or minus a constant, times or divided
   by a positive one. A bucket that comes back round (`hour(ts)`, `extract(minute …)`) is refused by
   name. The bucket is planned once, as DataFusion evaluates it (`once::planned`). A group is over
   when its bucket is before the watermark's.
3. **The view's partial rows are kept in `{view}$open`**, a hidden merge table the GROUP BY view
   machinery already maintains (one running row per group, from every node, in the same commit as
   the rows). The leader moves the groups that are over to `{view}` twice a second, with the
   watermark as the producer `emit:{view}$open`'s seq, so each group is kept exactly once through
   restarts and failovers (invariant 24's mechanism). A view made over rows waits for its fill to
   commit before keeping anything.
4. **Late rows are counted, not lost from sight.** A row for a group already kept stays in its
   table, and the view keeps what it had. Each one counts in `pondra$expectations` as the view's
   `$late`, and `pondra.flows` shows it as `late_rows`.
5. **Time is written as time.** `lateness = '10 seconds'` and `idle = '1 minute'` take a time.
   `idle` moves the watermark on with the clock once the source has had no newer row for that long,
   so a quiet stream's last group is kept too. `emit = 'final'` is the same as the clause, for
   clients that build the statement from options (Python's and JavaScript's `db.view`).
6. **Sessions are `GROUP BY key, SESSION(ts, INTERVAL '30 minutes') EMIT FINAL`**, the same view
   `WITH (session = 'ts', gap_secs = 1800)` made (`views::Sessions`, `session_start`,
   `session_end`), and `SHOW CREATE` writes a session view this way.
7. **Sliding windows are a window frame over the kept groups** (`sum(n) OVER (ORDER BY minute RANGE
   BETWEEN INTERVAL '4 minutes' PRECEDING AND CURRENT ROW)`): each row counted once, in its bucket.
8. **The options form stays** (`window`, `size_secs`, `slide_secs`, `session`, `gap_secs`,
   `lateness_secs`), so existing views and clients keep working. The docs teach `EMIT FINAL`.

## Consequences

- `SHOW CREATE`, `pondra.objects`, `pondra.tables`, `pondra.flows` and `DROP MATERIALIZED VIEW` name
  the view, never `$open`; `DROP TABLE`, `ALTER TABLE` and `DETACH` on it are refused, naming
  `DROP MATERIALIZED VIEW`.
- `View.once` is a new optional field, so the lake's format doesn't move: an older build reads such
  a view as a plain GROUP BY view of `{view}$open` and stops keeping groups until it is upgraded.
- What it doesn't do: a window over a join (a stream join's view is the source to group instead), a
  bucket of an expression over two columns, per-key or per-partition watermarks (ADR-017's limit
  stands), and a side output of the late rows themselves.
