# ADR-036: Pipelines, and a database's behaviour from every door

**Date:** 2026-10-01 · **Status:** built (round 30), but identity columns (§3), UNIQUE (decision 10) and SCD type 2 (§8) · **Builds on:** ADR-017 (streams on their own time), ADR-020 (change any row), ADR-022 (views filled from existing rows), ADR-035 (trust anywhere)

## Context

Round 30 in the roadmap is "behaves like a database, from every door": transactions, Postgres's
error codes, the key lookup's fast path from every door, constraints, and a test of every feature
through every door. The owner added pipelines to it on 2026-10-01, as Databricks' Delta Live Tables
(now Lakeflow Declarative Pipelines) has them: a materialized view following another, in the same
commit (bronze → silver → gold); expectations; history kept per key (SCD type 2); and the
pipeline's graph in the console.

Until now a materialized view of a materialized view was refused: a view's rows are written in the
commit that derives them, not taken in as a table's are, so a view of them would have stayed empty.
Every error left the Postgres port as `XX000`. `BEGIN` and `COMMIT` were accepted and did nothing.

What Databricks and the others do, for comparison:

- **Databricks pipelines:** streaming tables (append, each row once) and materialized views
  (recomputed, incrementally where Enzyme can); expectations `CONSTRAINT c EXPECT (cond) [ON
  VIOLATION DROP ROW | FAIL UPDATE]`, counted in the pipeline's event log; `APPLY CHANGES INTO … STORED
  AS SCD TYPE 2`; a graph of the datasets. A pipeline runs on its own compute, triggered or
  continuous: each table lags the one before it by at least one micro-batch.
- **Snowflake:** dynamic tables with a target lag, each refreshed after the ones it reads.
- **Flink, RisingWave, Materialize:** a view of a view is a longer dataflow; consistent across views
  in Materialize (one timestamp), eventually in Flink.

## Decision

### 1. A pipeline is views following views, all in one commit (built)

A materialized view may follow another's table. When a flush is packed, its views run in an order
where each comes after the view it follows (`views::in_order`), each taking the rows the one before
it derived in this flush (`views::derive`). So bronze, silver and gold move in **the same commit**:
no lag between them, no state to track, exactly once, and a query never sees silver ahead of gold.
That is Materialize's consistency at Pondra's inline cost, where Databricks' and Snowflake's
pipelines each lag the stage before.

- **A view of a row-by-row view** (no GROUP BY) takes its rows as a view of a table does, and follows
  their changes: a row keeps its first source row's `_row_id`, so an `UPDATE` or `DELETE` of
  `orders` takes back silver's row and then gold's, in that one commit.
- **A view of a GROUP BY view** sees that view's partial rows (a few per key, combined as they are
  read), not its totals, so it must combine them the same way: a **rollup**, `GROUP BY` some of the
  view's keys, a `WHERE` on those keys only, and `sum()` of its sums and counts (a sum of counts is a
  count), `min()` of its mins, `max()` of its maxes. Anything else is refused, saying so and pointing
  to a stored view (`CREATE VIEW`), which reads the totals as they are.
- **Filled like any view** (ADR-022): a view made over a view that is still filling gets the
  other's filled rows either in its own filling or as they arrive, never both and never neither.
- **Dropped from its end:** dropping a view another follows is refused, naming the followers.
- **The sequencer's check** (`log::commit`) now also owes a view's rows to the views that follow it:
  a flush packed without a downstream view (made meanwhile) is packed again.
- `pondra.pipelines` lists each materialized view: what it follows, its kind (rows, aggregate,
  window, sessions, stream join), what else it reads, its expectations, its definition.

### 2. Expectations (built)

`CREATE MATERIALIZED VIEW silver (CONSTRAINT positive CHECK (amount > 0) ON VIOLATION DROP ROW,
CONSTRAINT has_buyer EXPECT (buyer IS NOT NULL)) AS SELECT …`

- A condition over the view's columns, checked as its rows are written. A row breaks it when it is
  false (NULL passes, as Postgres's `CHECK` does).
- `CHECK (…)` alone fails the write that brings such a row (Postgres's meaning of CHECK);
  `EXPECT (…)` alone keeps the row and counts it (Databricks' default); `ON VIOLATION DROP ROW` leaves
  it out and counts it; `ON VIOLATION FAIL` fails the write.
- **A failed write is only the writer's:** a flush carries many producers' rows; when one of them
  breaks a `FAIL` expectation, each producer's rows are checked alone and only the ones that break
  it are refused (`log::send`), as Postgres would refuse that one INSERT.
- **Counted with the rows:** each flush that had failures adds a row per expectation to
  `pondra$expectations` (a merge table) in the same commit, so the counts are exact.
  `pondra.expectations` shows each expectation with its condition, what a violation does, and the
  rows that broke it.
- **Taken back consistently:** an `UPDATE` of a row a `DROP ROW` expectation had left out takes back
  nothing downstream (`views::let_in`), and a row that comes to meet it enters.
- A view whose `FAIL` expectation the rows already there would break isn't made (it would be filled
  with them). Expectations go on row-by-row views: a GROUP BY view's rows are partial sums, so they
  are refused there, pointing to the view before it.

### 3. Constraints on tables (CHECK built; identity proposed)

- `CHECK (…)` on a column or the table, named as Postgres names them when unnamed
  (`{table}_{column}_check`, `{table}_check`), enforced on every write from every door where NOT NULL
  is (`defaults::check`): INSERT, append, Kafka, Flight, COPY, a bulk INSERT's stream, UPDATE, MERGE.
  A refusal is `23514 check_violation`, as Postgres sends it.
- **Identity columns** (`GENERATED ALWAYS AS IDENTITY`, `SERIAL`): proposed. Values reserved by each
  node in blocks from the leader, as row ids are, so they are unique and grow, with gaps, as
  Postgres's do with a cache.
- **UNIQUE** beyond a key: waits for decision 10 (what an `INSERT` of an existing key does).

### 4. Postgres's error codes, from every door (built)

`codes.rs` gives every error a SQLSTATE: a typed error says its own (`Violation` is 23514, a
serialization conflict 40001), anything else is known by its words (no table 42P01, no column 42703,
no function 42883, syntax 42601, permission 42501, NOT NULL 23502, division by zero 22012, a cast
22P02, a timeout 57014, memory 53200, not supported 0A000…), else `XX000` as in Postgres.

- Postgres port: the code, as Postgres sends it.
- HTTP: `x-pondra-sqlstate` on every error (the status stays 500, the body the message, so no client
  breaks); the Python client raises `PondraError` (a `RuntimeError`) with `.sqlstate`, the
  JavaScript client's error has `.sqlstate`.
- Flight SQL: the nearest gRPC code (NotFound, InvalidArgument, PermissionDenied, Aborted for 40001,
  FailedPrecondition for a constraint…), and `x-pondra-sqlstate` in its metadata.

### 5. Transactions: `BEGIN` … `COMMIT` as one commit (built)

- **Where it runs:** on the node the session talks to. `BEGIN` fixes a snapshot (the version the
  node sees). Every statement in it reads that snapshot with the transaction's own writes over it
  (an overlay, as temporary tables are registered: `temp.rs`), so it reads its own writes.
- **Writes are kept, not sent:** an INSERT's rows, an UPDATE's or DELETE's old and new versions
  (worked out by `change::rows_of` over the snapshot and the overlay), per table, in the session's
  memory.
- **`COMMIT`** sends them to the leader in one request. Under the lake's lock it checks that no row
  the transaction changed (by `_row_id`; a keyed table's by key) was changed by a commit after the
  snapshot, then writes everything as **one flush, one commit**, its views following in it. A row
  changed meanwhile refuses the commit with `40001 could not serialize access due to concurrent
  update`, as Postgres's REPEATABLE READ does; the client tries again (pgbench's `--max-tries`).
  That is snapshot isolation, first committer wins.
- A statement that fails inside a transaction fails the transaction: until `ROLLBACK`, every
  statement is refused with `25P02`, as in Postgres. An idle transaction ends after
  `PONDRA_TXN_IDLE_SECS`.
- Every door with a session: the Postgres port (its connection is the session, and its status
  says when a transaction is open, as drivers expect), HTTP and the clients (`x-pondra-session`;
  `with db.transaction():` in Python). Flight SQL's own transactions and MCP (no session) refuse
  it by name (0A000).
- **Measured:** pgbench's own TPC-B script runs with its balances right (`tools/bench/pgbench.py`,
  a gate): 133 tps on one client (7.5 ms a transaction, from 35 ms when it first ran), 91 tps on
  four; Postgres 997 and 1,793 on the same machine. At scale 1 every transaction updates the one
  branch row, so four clients mostly retry: first committer wins where Postgres queues on the
  row's lock. The commit (a round trip to the leader and the log's group commit) is a third of
  the transaction.

### 6. The point path from every door (built)

A key lookup was 0.17 ms through `/lookup` and 6.5 ms through the Postgres port, where it was
planned as any query. Now `SELECT … FROM t WHERE key = $1` (a literal or a parameter) is recognised
before anything else (`serve::point`, worked out once per catalog version) and answered from the
serving path: on HTTP as before, on the Postgres port first thing in a statement, its `Describe`
and parameter types from it too. The log's segments are decoded once per node and a lookup reads
only those committed since the last (`Lake::segments_after`); a big log batch not yet tiered is
looked up through an index of its keys made once per node. A psycopg lookup (`vs_postgres.py`):
p50 6.5 → 0.29 ms, Postgres 0.09 ms: the gate's ≤ 0.5 ms. In a
transaction it reads the transaction's own version of the row, or the snapshot's
(`txn::point_read`, 6.2 → 0.4 ms). A one-key `UPDATE` of a keyed table finds its row the same way
and computes the new one without planning (`txn::point_change`): in a transaction 6.5 → 0.7 ms,
on its own 10.8 → 2.5 ms.

### 7. The doors matrix (built)

One list of features — a query, an INSERT, a key lookup, a one-key UPDATE, an UPDATE by a filter,
a CHECK refused with 23514, no table with 42P01, a procedure, a transaction — each run through SQL
over HTTP, the Python client, the Postgres port, Flight SQL (ADBC), the JavaScript client and MCP,
in one test (`harness.py doors`). Every cell is right, or refused by name: a transaction on Flight
SQL and MCP. Its first run found three gaps, fixed: Flight SQL ran no `CALL` and its planning
errors lost their codes; MCP's errors had no code.

### 8. History per key, SCD type 2 (proposed)

`CREATE MATERIALIZED VIEW dim WITH (history = 'id', sequence_by = 'updated_at') AS SELECT … FROM
changes`: every version of each key kept as it arrives (an append view: concurrent writers need no
coordination), its `__start_at` the row's `sequence_by`, and `__end_at` the next version's, worked
out when read (a window over the key), so rows that arrive out of order still make the right
history. Databricks' `APPLY CHANGES … STORED AS SCD TYPE 2`, without its ordering constraints.

## Open: decision 10

An `INSERT` of a key that exists replaces the row (an upsert), as Fluss's and Paimon's key tables
do; Postgres raises `23505`. The recommendation stands: keep the upsert, add `WITH (on_duplicate =
'error')` for Postgres's behaviour, `ON CONFLICT` working in both. UNIQUE beyond the key follows it.

## Consequences

- Pipelines cost what views cost: each stage's SQL over the flush's new rows, in the node that
  received them. A deep pipeline makes each flush's packing longer, not the commit. Measured
  (`tools/bench/pipeline.py`): three stages take 29% off ingest (183,000 → 131,000 rows/s on two
  vCPUs), and every stage is right the moment its rows are acknowledged.
- Partial rows are explicit: a rollup is the only view a GROUP BY view may have. A view needing
  totals of totals with a filter on them is a stored view, read when asked.
- A `FAIL` expectation turns a view into a constraint on its source: a row it refuses refuses the
  INSERT into the base table, which is what Databricks' `FAIL UPDATE` does to a pipeline update.
