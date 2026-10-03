# ADR-048: Every statement remembered, slow ones with their plans

**Date:** 2026-10-03 · **Status:** accepted (round 33, the main thread) · **Builds on:** ADR-035 §6
(`audit::statement`, the one place every door's statement passes), ADR-021 (few objects), C5 (the
bucket's limits), invariant 14 (a remembered answer is good for one catalog version)

## Context

Round 33's roadmap asks for observability: a query history, plans and times across the nodes, a
slow-query log and traces, with the console's History showing each statement's plan and profile.
Until now a node kept none of it: `/metrics` counts, the audit log keeps writes and refusals, and a
slow query left no trace once its answer was sent. Snowflake's `QUERY_HISTORY`, Databricks' query
history and Postgres's `pg_stat_statements` are what users expect.

## Decision

1. **`pondra.history` is a table of the lake's own** (`pondra$history`, hidden, kept
   `PONDRA_HISTORY_DAYS`, 7): a row per statement a door was sent, with when, its id, the user,
   the door and the client's address, the node, the session, its class (read, write, ddl, …), the
   statement (secrets as `'***'`, cut at 4 KB), how it ended and its error, its time, the rows it
   answered, how many nodes it ran on, and for a slow one its plan and trace. A procedure's or a
   script's own statements are their call's (as in the audit log). SQL reads it like any table.
2. **It is written where every statement already passes** (`audit::statement`), never on the
   statement's path: each node's writer appends what came in a second through the log, one
   producer and a seq a batch (exactly once), the table made on first use. At most
   `PONDRA_HISTORY_RATE` (500) rows a second a node; past that, statements that went well and fast
   are counted in one row (`class = 'skipped'`, `rows` the count). Slow or failed ones are always
   written. `PONDRA_HISTORY=off` writes none.
3. **A slow statement (`PONDRA_SLOW_MS`, 1000) keeps its plan and its trace.** The plan is the one
   that ran, each operator with its rows, time and bytes (DataFusion's metrics, 64 KB at most);
   the trace is each node's share, the log tail and the finish when it ran across the nodes, a
   step at a time for a shuffle (`spmd::timed`). It also writes one line to the node's log, the
   slow-query log.
4. **An admin reads every row; anyone else their own** (`history::visible`).
5. **History's commits are quiet** (`store::quiet`): a commit that only adds history rows, moves
   its producer or its table's entry, or lets segments go (retention's mark moves, marked
   `QUIET`), doesn't move the version remembered answers are keyed by (`Catalog::version`). Every
   read but the history's is the same across it, and queries naming the history are never
   remembered. Without this, a lake nobody wrote to forgot every remembered answer each second
   (a repeated query 0.30 ms → 3.6 ms).

## Consequences

- A small query costs what it did: point lookups within 1% with history on (0.414 ms off, 0.418
  ms on, three runs each), remembered answers kept.
- One commit a second a node while statements come, an object only when a batch passes 64 KB;
  tiering folds the rows into Parquet as any table's.
- Left: the console's History reading `pondra.history` (the console's thread); OpenTelemetry
  export of traces, if users ask; a statement's fingerprint (its text without literals) for
  grouping, once the history shows what people need.
