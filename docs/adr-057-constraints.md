# ADR-057: UNIQUE kept on the leader, other keys kept as facts

**Date:** 2026-10-04 · **Status:** accepted (round 34, "SQL as people write it") ·
**Builds on:** ADR-020 (changes are the leader's, under the lake's lock, from one snapshot:
invariant 56), ADR-036 §2 and §5 (a refusal is the writer's alone; a transaction is one commit),
ADR-049 (one registry: `SHOW CREATE`, `pondra.objects`), invariant 130 (NOT NULL and DEFAULT on
every door)

## Context

`CREATE TABLE t (email TEXT UNIQUE, team INT REFERENCES teams (id))` ran, and both words were
dropped: nothing kept them, nothing checked them, and `SHOW CREATE` gave the table back without
them. A schema copied from Postgres, an ORM's migration or dbt's contract then said something the
lake didn't do. Three things are asked of these words, and they cost very different amounts:

- **UNIQUE** is what applications lean on (an email, a handle, an external id). It has to hold
  when two writers on two nodes add the same value at once.
- **PRIMARY KEY** already means something here: a keyed table, where a new row replaces the old one
  of its key. Snowflake, BigQuery and Databricks also accept a key that is only a fact
  (`NOT ENFORCED`, `RELY`), which BI tools and planners read.
- **FOREIGN KEY** checked on every write means reading the other table for each row and refusing a
  delete that leaves rows pointing nowhere: a cost on two tables for one rule. Snowflake, BigQuery
  and Databricks keep it as a fact and check nothing.

## Decision

1. **A UNIQUE constraint is kept in its table's entry** (`TableMeta::constraints`, stored column
   names, so a rename keeps it and a dropped column takes it with it) and **checked by the leader,
   under the lake's lock**, for every write: the change path (`change::appends`, which
   `INSERT … VALUES`, `INSERT … SELECT`, `UPDATE`, `MERGE` and a transaction's `COMMIT` all reach)
   refuses new rows that give its columns a value twice among themselves or with a row the table
   keeps (every row but the ones the statement replaces; in a keyed table, every key but the ones it
   writes). One statement is checked as a whole, as the SQL standard has it (an `UPDATE` that swaps
   two values holds; Postgres checks row by row unless the constraint is deferrable). NULLs are
   distinct. The error is Postgres's: `23505`, `duplicate key value violates unique constraint
   "users_email_key"`, `Key (email)=(a@x) already exists`. A table with one sends `INSERT` to the
   change path (`write::changes`), so its rows are checked from every SQL door, `pondra sql` included.
2. **The sequencer takes a UNIQUE table's rows only from that check** (`Flush::checked`, set by
   `change::submit` and never sent between nodes; `constraints::sequenced`, a lookup a table, the
   entry read again only when a commit wrote it). The doors that write rows without SQL
   (`POST /append`, `COPY … FROM STDIN`, Kafka, Flight, another engine's commit) refuse the table by
   name, and a node that hasn't seen an `ALTER TABLE … ADD CONSTRAINT` yet is refused at the
   sequencer instead of slipping a row past it.
3. **`ALTER TABLE … ADD CONSTRAINT … UNIQUE` keeps it first, then checks the rows already there**:
   the entry is committed (from then on every write is checked, or refused), one flush through the
   sequencer makes sure those sequenced before it are in, and a `GROUP BY … HAVING count(*) > 1`
   over the table decides; two rows of a value put the entry back and answer `23505`. All of it under
   the lake's lock, which tiering also takes, so nothing else rewrites the entry meanwhile.
4. **`NOT ENFORCED` is a fact, checked nowhere**: a `UNIQUE`, a `PRIMARY KEY` (which then makes no
   key: the table takes rows as they come) and a `FOREIGN KEY` (which must say `NOT ENFORCED`; without
   it, it is refused, by name, rather than kept and not checked). They are listed where tools read
   them: `information_schema.table_constraints` (`enforced` YES or NO), `key_column_usage`,
   `referential_constraints`, `constraint_column_usage`, `pg_constraint` (with Postgres 18's
   `conenforced`), `pg_get_constraintdef`, and an enforced UNIQUE's index in `pg_index` and
   `pg_indexes`, as Postgres makes one.
5. **Refused by name**: `UNIQUE NULLS NOT DISTINCT`, `DEFERRABLE`, `ALTER TABLE … ADD PRIMARY KEY`
   (a table's key is made with it), two primary keys, an enforced UNIQUE on a temporary table, and
   a change of constraints through `CREATE OR ALTER TABLE` (`ALTER TABLE … ADD | DROP CONSTRAINT`
   says it). `ALTER TABLE … ADD CHECK` checks the rows there (`23514`) and `DROP CONSTRAINT` drops a
   CHECK too.

## Consequences

- A UNIQUE table's writes cost what a change costs: they go to the leader and its check reads the
  table once a statement (a join against the new rows; files skipped by the columns' ranges). That
  is the price of the guarantee, paid only by tables that ask for it; every other table's appends
  are as before, and the sequencer's lookup is a map's.
- A UNIQUE table can't be fed by Kafka, Flight or `POST /append` until those doors learn to send
  rows through the check (a later step: the same `change::appends`, batched).
- FOREIGN KEY checks, `NULLS NOT DISTINCT` and deferred constraints stay open; the catalog already
  carries what they would need.
