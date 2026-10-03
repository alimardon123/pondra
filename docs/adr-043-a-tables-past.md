# ADR-043: A table's past (UNDROP, retention, time travel, RESTORE, CLONE)

Status: accepted, being built (round 33, the second part of ADR-039's data half).

## Context

Snowflake, Databricks and Delta let a user get back a table dropped by mistake and read a table as it
was. Pondra had neither. A dropped table left the catalog at once and its files were swept a day
later. Nothing could read the table as of an earlier commit. The data was mostly still there:

- files are immutable (invariant 6);
- every row carries its commit (`_version`, ADR-020);
- an `UPDATE` or `DELETE` keeps the old versions in `{t}$deleted` (invariant 55).

## Decision

**1. Retention per table.** `WITH (retention = '7 days')` sets how long a table's past is kept, and
`ALTER TABLE … SET (retention = …)` changes it (`TableMeta::retention_secs`). The default is a day,
as Snowflake's. `'0 seconds'` keeps nothing.

**2. `DROP TABLE` keeps the table; `UNDROP TABLE` brings it back.**

- `DROP TABLE` first sends the table's rows still in the log to files, as a rename does (invariant
  129). Its entries, and its `{t}$deleted`'s, then move to `dt/{name}/{ms}` in one commit (`ddl::Dropped`).
  The log moves on and expires, so nothing of the table may be left in it.
- `UNDROP TABLE t` puts the newest entry for the name back. The table reads the log from the end,
  as a new table does (invariant 50): the log since the drop may hold another table's rows under
  the same name.
- `UNDROP` is refused while the name is taken. Rename that table first (Snowflake's rule).
- While a dropped table is kept, its folder is taken (`ddl::free_folder`), and its files are in
  use for the orphan sweep (`tier::collect_orphans`).
- `tier::expire` lets the entry go after its retention. Its files then become orphans, swept a day on.
- `DROP … PURGE`, or a retention of 0, works as before. The table's Delta or Iceberg copy is taken
  away when it is dropped, and published again when it comes back.
- `pondra.dropped` lists what can still come back.

**3. `t AT (VERSION => n | TIMESTAMP => t | OFFSET => -s)` reads an append table as it was**
(`past.rs`). It needs no history in the catalog, because the rows carry it. The table as of `n` is:

- the current rows whose `_version ≤ n`;
- plus `{t}$deleted`'s old rows that were alive then: `_old_version ≤ n < _version`.

As of a time, `_updated_at` (the commit's time) takes `_version`'s place: of a row's old versions,
the one its first change after that time took out. `AT (…)` is rewritten where SQL comes in, on
the tokens (sqlparser takes no `AT` after a table), to a name `"at:<spec>"` the session registers,
so a join, a view, a CTAS or an `INSERT … SELECT` reads it as any table. Such a query runs on its node.

A purge drops `{t}$deleted` files only once they are older than the table's retention, not after
`--retain-secs` alone (`tier::settle`, on every purge call, so the past goes even when no change
follows), and records from where the table is still whole (`TableMeta::past_from`). An earlier
moment is refused by name. Keyed tables are refused, since compaction keeps only each key's newest
version.

**4. `RESTORE TABLE t TO VERSION AS OF n` (or `TIMESTAMP AS OF`, Delta's words) is one MERGE**
from `t AT (…)` (`past::restore`, where SQL comes in, so every door has it): matched by `_row_id`, a
row whose values differ gets the old ones (and keeps its id), a row made since goes (`WHEN NOT
MATCHED BY SOURCE THEN DELETE`), a row deleted since comes back as a new row. It is a change like
any other, so its own past is kept and another `RESTORE` undoes it.

**5. `CREATE TABLE c [SHALLOW] CLONE t` copies no file.** The source's log rows go to files first
(as a rename's do); then `c`'s entry (and its `{c}$deleted`'s) is `t`'s, in a folder of its own,
reading the log from its end, with `t`'s folder in `TableMeta::shares`. A file in a folder some
table shares is never deleted as one table's garbage (`tier::expire`): only the orphan sweep deletes
it, and it counts every table that lists files in a folder (a table, those kept to be undropped, and
every clone sharing it). So `t`'s merges, `c`'s merges and `DROP TABLE t` delete nothing the other
reads; the cost is that a shared folder's replaced files stay until the sweep (a day). A clone isn't
published (Delta and Iceberg name a table's files under its folder) and its past starts when it was
made (`past_from`). An `AT` clone is `CREATE TABLE c AS SELECT * FROM t AT (…)`, a copy.

## Consequences

- A `DROP TABLE` costs a tiering of the table's log rows (usually none: tiering runs every 10 s).
- A dropped table's files stay a day longer than before, as they do in Snowflake.
- A table made under the name of one still kept gets a folder of its own (`name__2`), as a table
  made under a renamed one's old name already did.
- Old lakes need nothing new. A dropped table's entry is a new catalog prefix, `dt/`, which older
  releases never read (ADR-039: no format change). An older release leading the lake does not know
  the kept tables either: its orphan sweep may take their files once they are a day old.
- Tests: `tools/history_check.py` (every part; the orphan sweep's counting of clones can't be shown
  in a test, as it deletes only what is a day old).
