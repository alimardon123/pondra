# ADR-067: Transactions that wait (READ COMMITTED and row locks)

**Date:** 2026-10-11 · **Status:** accepted (the 1.0 month, lane L3: the Postgres port) ·
**Builds on:** ADR-036 §5 (a transaction is a session's, and one commit), §6 (the point path from
every door), ADR-020 (a change is the leader's, under the lake's lock: invariant 56), invariant 3
(only committed data is visible)

## Context

A transaction today is snapshot isolation, first committer wins: `BEGIN` fixes a snapshot, and a
`COMMIT` that finds a row it changed changed since refuses with 40001. That is Postgres's
REPEATABLE READ. Postgres's default is READ COMMITTED, where two transactions updating one row
queue on the row's lock: the second waits, then updates the row as the first left it. Applications
written for Postgres count on that. A counter, a balance or a stock level updated in a transaction
from several connections fails with 40001 here, and most applications don't retry.

pgbench's TPC-B script shows it (`tools/bench/pgbench.py`, scale 1, 15 s, this 4-vCPU sandbox,
2026-10-11):

| clients | Pondra tps | retried | Postgres 16 tps |
|---|---|---|---|
| 1 | 171 | 0 | 1,262 |
| 2 | 155 | 416 | 2,304 |
| 4 | 148 | 668 | 2,153 |
| 8 | 125 | 657 (6 failed after 100 tries) | 1,925 |

More clients make it slower: every transaction updates the one branch row, so all but one of any
that overlap start again. And `SHOW transaction_isolation` says `read committed`, which it isn't.

Where a transaction's 3 ms in the node go (`PONDRA_TRACE_PG=1`): `BEGIN` 0.05 ms, a one-key
`UPDATE` 0.34, a key lookup 0.05, an `INSERT` 0.52, `COMMIT` 2.0, of which the catalog's commit is
1.45 (`/stats` `commit_ms_p50`).

## Decision

1. **READ COMMITTED is the default**, as in Postgres. A level runs as asked or stronger, never
   weaker (the standard allows stronger; Postgres runs READ UNCOMMITTED as READ COMMITTED):
   READ UNCOMMITTED and READ COMMITTED are READ COMMITTED; REPEATABLE READ is today's snapshot
   isolation. SERIALIZABLE isn't built, so it is refused by name (0A000), saying that REPEATABLE
   READ is snapshot isolation and allows write skew: a program that asked for it is told, not
   given less. `BEGIN … ISOLATION LEVEL`, `SET TRANSACTION` (before the transaction's first
   statement, 25001 after) and `SET SESSION CHARACTERISTICS AS TRANSACTION` (the session's
   `default_transaction_isolation`) go through one function, `txn::modes`. `READ ONLY` is
   enforced: a write in such a transaction is refused with 25006. `SHOW transaction_isolation`,
   `transaction_read_only` and their defaults say what runs. REPEATABLE READ takes its snapshot at
   the transaction's first statement that isn't a setting, as Postgres does, not at BEGIN: a commit
   made between BEGIN and that statement is seen, one made after it isn't (`txn::gate`).

2. **In READ COMMITTED each statement reads the newest commit its node sees when the statement
   starts**, with the transaction's own writes over it (the overlay, as now). A row it read and
   didn't change may have changed by the next statement; that is READ COMMITTED.

3. **A write locks the rows it changes, and holds them to the end of the transaction.** A row is
   named by its table and its key (a keyed table) or its `_row_id` (an append table). An `UPDATE`
   or `DELETE` of one key (the point path, ADR-036 §6) locks the key, then reads the key's newest
   committed version and works out the new one from it. An `INSERT` into a keyed table locks the
   keys it writes. A plain `INSERT` of a key the table already holds is refused 23505 (the owner's
   decision of 2026-10-11, built in its own PR, not in this round: until then it upserts, as it does
   now). `ON CONFLICT`, `MERGE`, `INSERT OR REPLACE`, the loading doors (Kafka, Flight, `COPY`, the bulk
   INSERT) and merge tables keep upserting or adding, whatever the rule says for a plain `INSERT`. An
   `INSERT` into an append table locks nothing:
   new rows can't meet anyone else's. Any other `UPDATE`, `DELETE` or `MERGE` works out its rows
   from its snapshot, locks the ones it would change, and runs again from a new snapshot if any of
   them changed in between (Postgres re-checks each changed row's `WHERE`; running the statement
   again gives the same answer more simply), again under READ COMMITTED for as long as a pass locks a row it hadn't held (`LOCK_MAX` is the only cap; under REPEATABLE READ the first changed row is 40001) (a job queue's
   workers take the next row after each other's commit: a few collisions in a row are normal). A statement that
   would lock more than 10,000 rows locks none: rule 6 alone keeps it right, as today. A `MERGE` and an
   `INSERT … ON CONFLICT` lock the rows they change as an UPDATE does, so a counter's upsert waits for the
   transaction holding its row and is worked out again at the grant (Postgres's behaviour, which the
   check of 40001 at COMMIT would otherwise break). On an append table an `INSERT … ON CONFLICT` also locks the values of its conflict target, as the key `ON`, then each column and its value (`txn::conflict_keys`): a second transaction inserting the same value waits for the first's COMMIT, then reads its row and does nothing. Such a key is never a row id's, which has one part. Any write to a table with an enforced UNIQUE
   (`constraints.rs`) takes no row locks at all, in a transaction or not: its rows are worked out from
   the transaction's snapshot and its COMMIT refuses a row it touched that changed since (40001). Such a
   table's writes go through the leader's `change::run` outside a transaction (invariant 236). A `DELETE`
   takes the same path as an `UPDATE`. An `UPDATE` of a key column, a write to a view's table, and an `UPDATE`, `DELETE` or `MERGE` of a merge table are refused by name (0A000) by `txn::refuse_change`, from a transaction, from an autocommitted write and from the leader's change alike (a key column's row would move to another key: delete it and insert it with its new key). A `SELECT … FOR UPDATE` over more than 10,000 rows is refused
   (0A000): it can't keep that many locks, and a locking read has no snapshot path to fall back on.
   A key is named by the stored column's type: a literal is cast to it and read as text, so
   `'2026-01-01 00:00:00'` on a TIMESTAMP key, and `1.5` and `1.50` on a DECIMAL(10,2) one, name one
   row whichever form writes it, and a one-key UPDATE and an UPDATE with a condition lock the same
   row (`key_text`). An `INSERT` whose rows are read from the table (`INSERT … SELECT n + 100`) has
   them worked out again at the commit its locks were granted at, as a locking read's rows are.
   A locking read (`SELECT … FOR UPDATE`, and `FOR SHARE` as an exclusive lock, which it is here)
   returns the rows it locked: the table's keys, or its `_row_id`s, are added to its SELECT and taken
   off the answer, so `ORDER BY 1` and aliases mean what they say, and an empty answer keeps its
   columns. Under READ COMMITTED a row another transaction changed while the lock waited is read
   again at the commit the lock was granted at, and again for as long as its rows keep moving (each
   time it locks a row it didn't hold, so `LOCK_MAX` ends them; no 40001: a job queue's workers that
   take the next row after each other's commit wait their turn, as Postgres's FOR UPDATE makes them);
   a locking read
   needs UPDATE on its table (`auth::may_lock`: 42501 otherwise).

4. **Locks live on the leader**, in memory, one table per lake (`locks.rs`). A follower asks it
   (`POST /cluster/lock`). Waiters are served in the order they asked. A lock is released when its
   transaction commits (once the commit is visible on the leader, so whoever takes the lock next
   reads it; a follower waits until it sees that commit, as `write::seen_here` does), rolls back,
   or its session ends. A follower's transaction takes the leader's address at each statement
   (`txn::next`). A leader is fixed for the life of a process (a new one restarts the node:
   `cluster.leader`), so within a process the address can't go stale. A transaction whose node
   restarted is gone: its COMMIT says there is no transaction in progress, and its writes never land
   (rule 6). A lock call waits at most the statement's `lock_timeout` plus a margin of 2 s (the lease,
   with none); a call with no answer in time is asked again, and the place the first one may have
   left is replaced (by owner), not doubled. A release or a renewal is asked once: the lease ends what
   doesn't land. A follower told to stop ends its waits at once (57P01), and a node told to stop hands its transactions' locks to their leaders (`locks::release_all`, from `drain.rs`). A waiter renews the keys it holds at each tick of its wait, so a long wait doesn't lapse them. The client side (`locks::passes`, `take_on`, `release_place`, `renew_places`, `Hold`, `close`) is in `locks.rs`, beside the server's table. A leader's SQLSTATE comes
   back through a follower's answer (`x-pondra-sqlstate`), and so do the notices.

5. **Deadlocks are refused at once.** A transaction waits for one lock at a time, so the waits form
   chains; a wait that would close a loop is refused with 40P01, as Postgres refuses it (after its
   `deadlock_timeout`). `SET lock_timeout` bounds a wait (55P03), as in Postgres.

6. **Locks are a schedule, never the authority.** `COMMIT` still checks, under the lake's lock,
   that every row the transaction changed is still at the version it read. A keyed row: no commit
   after the snapshot its statement read the key at holds the key. An append row: no
   `{t}$deleted` row since `BEGIN` holds its `_row_id` with `_old_version` the version it read
   (exact: a row another transaction changed before this one read it is no conflict). Under
   REPEATABLE READ every row was read at `BEGIN`'s snapshot, so this is today's check. A lock can
   be lost (a lease of `PONDRA_LOCK_LEASE_SECS`, 60, renewed by each lock its transaction asks
   for, lapses; the leader changes), and some writes take none: an append table's change outside a
   transaction, a write to a UNIQUE table (rule 3), and the loading doors (Kafka, Flight,
   `COPY`, the bulk INSERT: "Departures from Postgres"). Any of those can still make a `COMMIT` refuse
   with 40001; none can lose an update. So nothing about the locks is written down, and a leader that
   takes over starts with none. The check reads the log since the read point: for a keyed table the
   keys it changed (`conflicts`, a scan of the tail); for an append table the `{t}$deleted` rows and
   the files taken out. A read of one key is a lookup (`current`). A transaction's locks are renewed
   every lease/3 (at most every 30 s, `upkeep`), so an idle session keeps them; a lapsed lease's
   locks are swept when a lock is taken and when `/stats` reads `row_locks`.

7. **Outside a transaction, an `UPDATE` or `DELETE` of a keyed table is a transaction of its own.**
   It runs as `txn::autocommit`: one statement, READ COMMITTED, under an owner of its own (`auto-…`).
   It is no session's transaction: it has no entry in the book, takes no session and no lake lock.
   It works its rows out, locks their keys (rule 3's passes, so a row that moved while it waited is
   worked out again at the commit its lock was granted at), and then appends its change through the
   log under the producer `sql:{job}`, so a retry of the job is a duplicate and applies once. Its locks
   go once the append is acked, or refused. A change over 10,000 rows locks none, as in a transaction.
   It waits for a transaction holding one of its rows, as a statement inside a transaction does.
   A `MERGE` outside a transaction and a write to a UNIQUE table keep the leader's `change::run`, under
   the lake's lock (rule 3). An
   append table's change (the leader's, under the lake's lock from one snapshot, invariant 56) and an
   `INSERT` (new rows can't meet anyone else's) take no locks either. An autocommitted write registers as appending to its table in the same request that grants its locks (the flight in `locks::take`), and a writer that changes rows under the lake's lock closes that table first (the gate, `locks::close`): it waits for the appends in flight, the autocommitted writes that come meanwhile wait at the gate, and the writer's commit is made before they go on. The writers that close the gate are a COMMIT (`txn::commit_here`), a change outside a transaction (`change::run`), an outside commit that takes files or rows out (`iceberg::record`) and a branch's REFRESH (`branch::refresh`). Without the gate, an autocommitted write's append lands over a change made between its working-out and its append: `harness.py round7` with `PONDRA_TEST_NO_GATE=1` shows the lost update. Still ungated, and so named: blind keyed INSERTs through the log, bulk file INSERTs, the leader's appends of a UNIQUE table's rows (`constraints.rs`) and of a view's fill (`views.rs`). An autocommitted write holds its locks until its append is answered, or lets them go at once if it never sends the append (`Hold`), and only if it took any; when the answer is lost (a request that fails in transit, a reply that never comes), they are kept to the lease (`Hold::lost`), since the append may still land. Over HTTP, a 4xx from the leader is its refusal, made before its sequencer took the flush; a 5xx, or an answer whose body never arrives, counts as lost. A closer waits at the gate 5 s at most (`GATE_WAIT`): it holds the lake's lock meanwhile, and an append whose answer was lost keeps its table busy to its lease, so past 5 s the closer is refused (40001) and the lake goes on. No writer marked appending takes the lake's lock or waits for it: it waits for its row locks before it is marked (`Table::leave`), and the sequencer's backpressure pauses commits only while the lake's lock is free or held by tiering (invariant 283), never while a closer holds it. `PONDRA_TEST_AUTOCOMMIT_PAUSE_MS` holds an autocommitted write between its working-out and its append, for the check.

## DDL and savepoints in a transaction

dbt-postgres sends `BEGIN; CREATE TABLE …; INSERT …; COMMIT` for seeds and models, pandas' `to_sql`
and SQLAlchemy 2's `create_all` create tables inside a transaction, and `begin_nested()` and
psycopg's nested `conn.transaction()` need savepoints. Refusing either broke them (until this
round, DDL in a transaction was refused by name).

8. **DDL runs at once.** A DDL statement in an open transaction (CREATE, DROP, ALTER TABLE and VIEW,
   and the rest of the catalog's DDL) runs as a commit of its own, and the transaction goes on. It
   isn't part of the transaction: ROLLBACK doesn't undo it. The client is told by a notice, "CREATE
   TABLE ran at once: DDL isn't part of a transaction here, and ROLLBACK won't undo it", sent once
   the statement has run (a failed DDL sends its error alone). A rolled-back seed leaves its table
   there, empty, as the notice says. Rows the transaction writes to a table it made are kept and
   commit with it, as any write's. A DDL that moves a table's entry (a rename, a swap, a kept drop, a clone) doesn't close the gate: the sequencer holds the table's appends until its commit (invariant 283).
   - `CREATE TABLE … AS SELECT` runs both its steps at once, so dbt's incremental model
     (`CREATE TEMP TABLE tmp AS …; DELETE … USING tmp; INSERT … SELECT FROM tmp; COMMIT`) has its
     DELETE and INSERT in one commit.
   - `CREATE TEMP TABLE` runs at once as it always did (the session's own, in memory); it gets the
     same notice and refusals. `IF NOT EXISTS` on a table that is there runs nothing, so no notice.
   - Refused by name (0A000), each saying what to do instead: DDL on a table this transaction has
     written (its kept writes were made against the old shape: "COMMIT first, then run the DDL");
     any DDL under REPEATABLE READ (its snapshot was taken before the DDL made what it would read:
     "run it before BEGIN or after COMMIT, or use READ COMMITTED"). Under READ ONLY it is 25006 and
     in a failed transaction 25P02, as any write.
9. **Savepoints.** `SAVEPOINT name`, `RELEASE [SAVEPOINT] name` and `ROLLBACK [WORK | TRANSACTION] TO
   [SAVEPOINT] name`. Over the Postgres port the status says what Postgres says: a SAVEPOINT or a
   RELEASE leaves the transaction open for the driver (ReadyForQuery's status stays in a
   transaction), and a ROLLBACK TO reopens one after a failure (psycopg's nested transactions). A savepoint records the transaction's kept writes and reads (`Mark`). `ROLLBACK TO` puts
   them back, keeps the savepoint, and clears a failed state (25P02), so the transaction goes on.
   `RELEASE` forgets the savepoint and those taken after it; their writes stay. An unknown name is
   3B001; a savepoint statement outside a transaction is 25P01. DDL after a savepoint is not undone
   by `ROLLBACK TO` (rule 8: it ran at once, with the same notice).
   - Row locks taken after a savepoint **stay held until the transaction ends**, even after
     `ROLLBACK TO` has undone the writes that took them. Postgres lets a rolled-back subtransaction's
     row locks go; here they are kept, which is safe (rule 6: COMMIT still checks every row it
     changed) but can make another transaction wait longer than in Postgres.
10. **An idle transaction ends.** After `PONDRA_TXN_IDLE_SECS` (600) without a statement, the
    transaction's kept writes are dropped and its locks let go (`upkeep`). The session's next
    statements, savepoint statements included, are refused 25P03 (Postgres's code for an idle
    transaction ended by `idle_in_transaction_session_timeout`) until BEGIN, COMMIT or ROLLBACK ends
    the refusal: a statement is never autocommitted in its place. A COMMIT is refused the same way, with
    25P03 and the words "the transaction was ended after N s idle (PONDRA_TXN_IDLE_SECS) and rolled back:
    nothing was committed": it is never a success. The same holds on the Postgres port, which answers 25P03 with those words
    (`harness.py waits`: HTTP on a follower, the port on the leader; a COMMIT after a failed UPDATE that then idled answers 25P03 too). A ROLLBACK answers ROLLBACK, and a BEGIN starts
    another transaction. Over every door, the code travels as the door carries SQLSTATEs (the Postgres
    port's ErrorResponse, HTTP's `x-pondra-sqlstate`, Flight's status). The marker is kept an hour at most.
    It is per node: a session that moves to another node meanwhile isn't refused (a known limit).
    A statement in progress keeps its transaction from being idle, a lock wait included (`Busy`, the
    guard `txn::next` returns). A session name (`x-pondra-session`) that isn't 8 to 128 letters, digits,
    - or _ is refused (22023) with what a name is, and never ignored: `temp::of`. A lock wait that ends without a grant fails the transaction as any
    statement error does: 55P03 (lock_timeout, NOWAIT), 40P01 (a deadlock) or 57P01 (the node stops).

## Departures from Postgres

- Row locks taken after a savepoint stay held until the transaction ends (rule 9).
- A `WHERE` subquery under `FOR UPDATE` is allowed: it reads at the snapshot and its table is not
  locked, as in Postgres. A locking read over a join, GROUP BY, DISTINCT, a WITH or a
  subquery's lock clause is refused by name.
- Temporary tables created under REPEATABLE READ are refused (0A000) as DDL is: their snapshot can't
  see what the statement makes.
- `CREATE … IF NOT EXISTS` of a table that is there runs nothing and sends no notice.
- A `FOR UPDATE` of an attached lake's table is refused (0A000): its leader takes the locks.
- Jobs keep the unlocked path of rule 7, so a retried change applies once and does not wait.
- The gate of rule 7 covers the writers it names and no others: an autocommitted write's append and a change made under the lake's lock. The loading doors and the writers listed there as still ungated aren't ordered by it.
- Not gated: blind keyed INSERTs through the log, bulk file INSERTs and `constraints::append` (a UNIQUE table's rows). A plain INSERT of a key takes that key's lock (the primary-key work), but it doesn't close the gate, so an INSERT landing during a closer's check isn't ordered by it.
- An append still in flight past the 60 s lease (`PONDRA_LOCK_LEASE_SECS`) no longer holds the gate, nor its locks: its lease has lapsed (`Table::appending`, `settle`).
- The loading doors (the Postgres port's `COPY`, Kafka, Flight, the bulk INSERT and the JSON and Arrow
  appends) upsert without waiting for row locks. A change outside a transaction
  may overwrite a row that landed between its working-out and its append, and says nothing; inside a
  transaction its COMMIT catches it as 40001 (rule 6). Postgres's own writers would wait instead.
- `COPY … FROM STDIN` in a transaction runs at once, as DDL does (rule 8): ROLLBACK doesn't undo it, and
  a notice says so. A READ ONLY transaction refuses it (25006), a failed one answers 25P02.

## What it costs

A lock on the leader is a map entry (no I/O); a follower's is one round trip to the leader per
write statement in a transaction. An autocommitted `UPDATE` or `DELETE` of a keyed table takes the
same round trips and a lock of its own, then the append it made anyway; its owner's record is one
entry, which the append's answer ends (the lease ends it if none comes). Its registration as appending rides on the lock request, so it costs no round trip, and a writer under the lake's lock waits at the gate only for the appends in flight (a few milliseconds here), nothing when there are none. A UNIQUE table's write and an append's change take no lock,
so they cost what they did. Nothing is spent on a lake with no transactions and no keyed writes
(principle 6). The commit's check reads the log since the earliest read point of the rows it changed
(`conflicts`): a keyed table's tail, or for an append table the `{t}$deleted` rows and the files
taken out. That is what it reads today, not a key lookup per row.

## What it should show

pgbench's TPC-B at scale 1: four clients faster than one, with no retries. The transactions that
update the branch row then run one after another, so the rate is bounded by the time from that
`UPDATE` to the end of its `COMMIT`, which is mostly the catalog's commit. A faster commit (the log
side, ADR-005's sequencer and the catalog's flush interval) raises it further; that is the main
thread's.

## Not now

- `SKIP LOCKED` (refused by name, in a transaction or not), `FOR SHARE` as a shared lock (here it is exclusive, as `FOR UPDATE`
  is), and locking reads over a join, GROUP BY or DISTINCT (refused by name). A single table's
  `SELECT … FOR UPDATE` locks the rows it reads (§3, `txn::locked_read`); a subquery in its WHERE is
  read at the transaction's snapshot and its table isn't locked, as in Postgres. Before this round
  the clause was silently ignored.
- SERIALIZABLE (Postgres's SSI), refused by name until it is built (rule 1).
