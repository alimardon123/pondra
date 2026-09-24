# ADR-017: Streams on their own time — watermarks from the data, session windows, point-in-time joins

**Status:** Accepted, built and tested (round 16) · **Date:** 2026-09-26 · **Builds on:** ADR-005 (inline views), ADR-011 (windows emitted once)

## Context

Round 10 gave Pondra event-time windows that emit each window once, final (ADR-011). Three things
still separated its streaming from Flink's:

- **The watermark came from the windows, not from the data.** A window closed once a *later
  window* had rows and started its size plus the lateness after it. With 10 s lateness, the minute
  from 12:00 closed only when a click of 12:02 or later arrived. Flink closes it at the first
  click past 12:01:10.
- **No session windows.** "A user's visit", "a machine's run", "a trip": groups of rows with no
  long gap between them, whose bounds come from the data. Flink and Spark both have them.
- **No point-in-time joins.** Each trade priced at the quote of its moment, each order at the
  exchange rate of its day, each event enriched with the customer's tier as it was then. Joining
  to the current table gives today's price to yesterday's trade, and the answer changes when the
  same stream is replayed. Flink has temporal joins; Snowflake and DuckDB have `ASOF JOIN`.

## Decisions

### 1. The watermark is the newest event time, less the lateness

As Flink's bounded out-of-orderness: the newest event time a stream's rows hold, less the lateness
its rows may arrive with. A window closes when the watermark reaches its end; a row that arrives
before that counts, however out of order.

- **Where it comes from.** The source's event-time column: the one the window's `date_bin(…)`
  reads, found in the view's plan when it is created and stored with it (`Emit::time`).
- **How it's known cheaply.** The leader keeps each source's newest event time in memory
  (`views::newest`): what the files' column ranges say, then each log segment after them, read once.
  After a restart it is worked out again the same way, and it only grows.
- **Exactly once, as before.** Emission's progress is a producer's sequence number, committed with
  what it emits, so a new leader emits nothing twice.

Minutes of clicks in their first 30 s, 10 s lateness: a minute now closes when a click 10 s past
its end arrives, where before it waited for the next minute but one. The harness's `windows` test
checks it both ways, with rows 15 s out of order still counted.

### 2. Session windows (`POST /views/{v}?session=ts&gap_secs=30&lateness_secs=5`)

The view's SQL is an ordinary `SELECT user, count(*) … FROM clicks GROUP BY user`; its GROUP BY
columns are what sessions are kept per. Pondra adds `session_start` and `session_end` to its
`SELECT` and `GROUP BY`, and runs it over each closed session's rows. Each session is emitted to
`{v}` once, whole, when the watermark passes its last row plus the gap.

On each round the leader:

1. **reads the rows that may still be in an open session.** These are the source's rows from the
   earliest start of any session left open last time (a bound kept in the catalog) up to the
   watermark;
2. **leaves out late rows.** A row that falls inside a key's session already emitted, before that
   session's end, is late and is left out. The harness's `sessions` test fails without this: a
   late row would re-emit its session, longer;
3. **cuts each key's rows where a gap of `gap_secs` falls.** This is two window functions (a `lag`,
   then a running sum) in SQL;
4. **runs the view's SQL over the closed sessions' rows.** It appends the result with the watermark
   as the producer's seq, exactly once, as windows do.

The bound it starts from is only a lower bound: written after the append, it can be stale after a
crash, which reads more rows but never different sessions. A session that spans many rounds is
emitted once, whole.

### 3. Point-in-time joins (`ASOF JOIN`)

```sql
SELECT t.id, q.price
FROM trades t ASOF JOIN quotes q MATCH_CONDITION (t.ts >= q.ts) ON t.sym = q.sym
```

gives every trade the latest quote of its symbol at or before the trade. This is Snowflake's
syntax (which the SQL parser already reads); DuckDB's `ASOF LEFT JOIN` means the same.

- **The four directions.** `>=`, `>` (strictly before), `<=` (the first at or after) and `<`
  (strictly after).
- **Rows with no match.** A trade with no quote gets NULLs. The left operand of the match
  condition is the left table's; written the other way round, it is turned.

DataFusion has no such join (`asof.rs`):

- **The rewrite.** The SQL becomes a `LEFT JOIN` on the keys whose condition carries a marker,
  `pondra_asof(t.ts >= q.ts)`. This happens wherever SQL comes in: HTTP, Postgres (simple and
  extended), Flight, views, tasks and INSERTs.
- **The join that replaces it.** DataFusion plans a hash join (or a sort-merge or nested-loop join)
  for it. A physical rule replaces that with `AsOfJoinExec`, which builds the looked-up side per
  key in time order and finds each row's match by binary search.
- **Big or small.** DataFusion sizes the two sides as for any join, and the join follows:
  - where it would have collected the looked-up side, that side is one table;
  - where it would have hashed both sides by the key, there is a table per partition, so neither
    side has to fit in one piece;
  - where it would have collected the *kept* side, as for a stream's new rows, the kept rows are
    read first and only their keys' rows of the other side are kept (`Mode::Keys`). This doubled
    the rate of a stream enriched by an as-of view (272k to 584k clicks a second).
- **Ties.** Rows of one key at the same time are equally "as of": any one of them is the answer,
  as in DuckDB and Snowflake.
- **Safe if the rule misses.** The marker fails if it is ever run, so a plan the rule missed errs
  instead of returning every earlier quote.
- **A WHERE stays above the join.** A WHERE on the quote must not be pushed into the lookup, or it
  would find the latest quote that passes the filter rather than the latest quote. DataFusion
  turns an outer join inner when a WHERE drops its NULL rows, and then pushes the WHERE down.
  `KeepOuter` stops that for as-of joins. `asof_check.py` caught this against DuckDB.

**A view's rows go into its table by position, cast** (`query::cast_as`). Strings a query reads from
a table's files are views; a view's table holds plain strings. A view that took a string from
the table it looks up in failed every flush until this (`stream_check.py` found it; the harness's
`asof` test fails without it). Tasks write their rows the same way.

**Across the nodes (`spmd::asof`).** Each row must see all of its key's rows on the other side:

- if the other side is read whole on every node, the join runs where the rows are;
- if both sides are hashed by the key, it runs in each bucket (a node's partition *i* holds the
  keys a whole copy's partition *i* does, as the buckets are cut);
- otherwise the looked-up side is sent to every node.

**Over a stream.** In an inline view each new event joins the table as it is when the event
arrives, and finds the row of the event's own time in it. A trade that arrives after a newer
price still gets the price of its time (the `asof` test). A price that arrives after a trade of
its time, though, doesn't reach that trade.

## What it measures

One 2-vCPU box; the three-node rows ran as three processes on it (`logs/round16/`).

| | |
|---|---|
| `ASOF JOIN`, 1 M trades × 200 k quotes, one node, best of 3 | **0.18–0.32 s** over four runs (DuckDB 1.5.5: 0.24–0.26 s) |
| …on three nodes | 0.34–0.39 s (three processes on the same box) |
| Answers equal to DuckDB's: 4 directions, no key, a small table, a few trades, a filter after the join; one node and three; four ways of planning it | **8 of 8, every way** |
| Window closed after its end (1-minute windows, 10 s lateness) | when event time is 10 s past its end (before: a minute or more) |
| One stream of 2 M clicks (20 k users) in 4 Arrow producers: no views | 2.4 M clicks/s |
| …with a window view / a session view / an as-of view, each alone | 1.03 M / 1.08 M / 0.58 M clicks/s |
| …with all three | 0.37 M clicks/s; every click in one window and one session, each enriched right |
| Last window / session out, after the click that closed them | 0.51 s / 0.73 s (the leader emits every 500 ms) |
| The same on real R2 (400 k clicks, durable acks) | every click once; 22 k clicks/s with or without the views (the acks bound it); last window / session out 2.5 s / 3.0 s after the closing click |
| TPC-H SF1 on one node, from memory / from Parquet (nothing here should change it) | 1.90–2.01 s / 2.99–3.17 s; round 15's binary run beside it: 1.91 s / 2.97 s (DuckDB 3.18–3.26 s) |

## What this costs

- **Emission.** The leader emits every 500 ms. Windows cost one read of the view per round, as
  before. Sessions re-read the rows of every session still open, so a key that never goes quiet
  keeps its rows being read; Flink keeps them in state instead. On two cores that work halved the
  ingest rate beside it (2.4 M to 1.08 M clicks a second).
- **The watermark.** It is one number per source in the leader's memory, updated from each new
  log segment once.
- **`ASOF JOIN`.** The looked-up side is held in memory (per partition when both sides are
  hashed), counted against the query's memory budget, and it doesn't spill. Past the budget, the
  query runs again with sort-merge joins, where the table is built per partition; past that too,
  it fails cleanly (a million quotes under a 30 MB budget: `Resources exhausted`). In a view,
  each flush still reads the whole looked-up table, keeping only its keys' rows. A query's SQL
  goes through the rewrite only if it contains "asof".

## What is still open

- **One watermark per source.** It isn't per partition or per node, so a node that falls behind
  looks like late data. A source that goes quiet holds its watermark: its last windows and
  sessions wait for more rows. Flink's idleness only stops a quiet input holding the others back,
  so it would wait too.
- **Late rows are left out, not counted.** A late row still updates a window view's live table,
  but no side output collects late rows.
- **An as-of join in a view doesn't wait.** Flink's temporal join holds each event until the
  table's own watermark passes the event's time, so a price that arrives late still reaches the
  trades of its time. Here an event joins what the table has when the event arrives.
- **Keyed tables keep only their latest row.** An as-of join against one gives the latest, not the
  one of that moment; a table's history has to be kept as rows for it. Flink's temporal join over
  a changelog does this with versioned state.
- **Sliding windows, timers, pattern matching (CEP).**
- **A head-to-head with Flink on Nexmark.**
