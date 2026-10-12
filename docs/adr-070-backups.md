# ADR-070: A backup is a lake

**Date:** 2026-10-11 · **Status:** accepted (phase 1 built in round 34) · **Builds on:** ADR-001 (the files where they are),
ADR-043 (a table's past: `AT`, `UNDROP`, `RESTORE`), ADR-047 and ADR-058 (`CLONE`: a lake made
from another's snapshot, held there by a pin) · **For:** the adoption review's P0-5

## Context

A lake's catalog is the only home of its tables' entries, the rows not yet in files, its views,
routines, tasks, users and secrets (ADR-001: "the Parquet files alone are not enough"). Lose the
`catalog/` prefix, or the bucket, and the lake is gone; a format move is one-way. There is no
`pondra backup`, no page on it and no backup step in the upgrade guide. For a store of record that
is a reason not to adopt it, and the adoption review ranks it P0.

`AT`, `UNDROP`, `RESTORE` and zero-copy `CLONE` already undo a wrong statement. They don't help
when the objects themselves go: a deleted prefix, a bucket emptied by a lifecycle rule, an
account closed, a region down. That needs a copy somewhere else. Copying the prefix with `aws s3
sync` while the lake runs isn't one: the catalog's manifest and the files it names are copied at
different moments, and a merge or the catalog's compactor can delete a file the copy then misses.

Snowflake's answer is Time Travel plus Fail-safe (kept by Snowflake, not the user); Databricks'
is `DEEP CLONE` into another location, run again for increments; DuckDB's is `EXPORT DATABASE` or
copying the file. `DEEP CLONE` is the shape that fits Pondra: it already has the shallow one.

## Decision

1. **A backup is a lake.** `BACKUP DATABASE [name] TO 'place' [WITH (retention_days = 7)]` (an
   admin's, at every door: it is SQL) and `pondra backup LAKE PLACE [--retention-days 7]` (with no
   node running) make a lake at `place` that is the database as of one commit. The statement's
   node pins the database (a pin, as `CLONE` takes one, ADR-047, renewed while the run goes on and
   let go after it); the backup's own leader (a `pondra lead` of its own when nobody leads it,
   invariant 62) reads the database's catalog at a version that holds the pin, copies every object
   that version names into the backup's own folders, and writes the whole catalog in one commit.
   Nothing names an object before it is copied (invariant 290). A branch's base's files, under
   `_base/<id>/` in the branch, go under `data/_bases/<id>/` in the backup, so a backup of a branch
   holds what the branch reads and needs no base (manifests naming the base's paths are sealed again
   under the backup's). The place is empty the first time, and a backup of this same database
   after: a folder holding anything else, or a lake that isn't this database's backup, is refused
   before anything is written (a run would delete what that lake names and the backup doesn't).
2. **Every kind of object goes, but what is each lake's own.** Tables (kept dropped ones too), the
   log tail and the segments the change feed still reads, views, routines, tasks, users and grants,
   sealed secrets, shares and recipients, comments, sequences, indexes and types all go, whatever the
   registry's `on_clone` says, so a new kind is backed up with no change. Not carried: the counters,
   format and members; the lake's own keys (`z/auth`: a restored lake makes new ones, so a backup
   holds no key its source's nodes sign each other in with); a key a branch was lent to read its base
   with; the pins others hold in it; an apply under way; and what it published to Delta and Iceberg (a
   restored lake publishes anew). Secrets stay sealed: restoring them needs the same master key,
   which a backup never holds, so keep it apart from the bucket.
3. **Run again, it copies what is new.** A second `BACKUP` to the same place copies only the objects
   its catalog doesn't name yet (an object never changes under its name, invariant 6). The log's
   objects go first, while surely there (a pin holds files, and tiering lets segments go once their
   rows are in files). A mark (`_backups/running`) says a run is under way; one stopped half way
   leaves the previous run whole and readable, and the next run looks before it copies what the
   stopped one may have. The workspace's files (`files/`, their kept versions too) are copied as they
   are when a run gets to them, those changed or new since the last run, and those the source deleted
   are deleted: a restore takes the newest run's.
4. **Its past is its runs.** Each run keeps its catalog as an object (`_backups/{ms}-{commit}.json.zst`)
   and is listed in the backup's `bk` entry; objects a run no longer names are kept for
   `retention_days` (7 the first time; a run keeps the last one given), and longer while a clone of
   the backup may read them. A run past the retention goes with its catalog. Within a backup, `AT
   (…)` reads a table's past as its source had it at the newest run.
5. **A backup is read-only to everything but its next run** (invariant 291). It opens as any lake
   (`pondra serve PLACE`, `ATTACH 'place' AS b`), and every write to it is refused by name with
   25006 at every door: SQL, the row doors (`/append`, `/insert`, Kafka, Flight, `COPY`), its files
   and the catalog itself (`Catalog.frozen`, which only a run's commit passes). Its nodes do no
   tiering, emission, streaming tasks, feeds, task ticks or history; its leader only keeps its
   clones' pins. `CREATE DATABASE dev CLONE b` (b attached) makes a working database from it with
   nothing copied.
6. **Restoring copies one run into a lake of its own.** `RESTORE DATABASE name FROM 'place' [LOCATION
   'dir'] [AT (TIMESTAMP => '2026-10-12 03:00:00')]` makes the newest run at or before that time a new
   lake (beside this one unless `LOCATION` says) and attaches it as `name`; `pondra restore PLACE NEW
   [--at …]` does the same with no node. A restore only reads the backup, refuses a place that holds
   anything, takes away what it made if it fails, and suspends the restored lake's tasks (`ALTER TASK
   … RESUME` once that copy should run them: nothing in a copy reaches the outside on its own). The
   restored lake's format is the backup's (the newest of its runs').
7. **On a schedule like anything else:** `CREATE TASK nightly SCHEDULE 'cron 0 3 * * * UTC' AS BACKUP DATABASE
   TO 's3://other-bucket/prod'`.
8. **Within the bucket's limits** (principle 7): the catalog names every object, so nothing of the
   lake is listed (the workspace's `files/` is, once a run); copies go eight at a time through the
   node's request budget, an object over 8 MB in parts, never whole in memory. Copies go through the
   node, not server-side: a backup's point is often another bucket or provider. A place there is
   reached with the bucket secret whose `SCOPE` covers it (`ext::reach_place`), else the environment's
   credentials; a `pondra lead` the node starts for the backup reaches it the same way
   (`store::REACH_VAR`).
9. **Before an upgrade, a backup.** The upgrade guide's first step for a release that raises the
   lake's format is `BACKUP DATABASE TO …` with the release still running. A backup keeps the format
   its runs were written at, so the release being left can still serve it if the upgrade goes wrong
   (a release refuses only a lake newer than itself, invariant 207).
10. **Repair, for a lake with no backup** (phase 2): `pondra repair LAKE` rebuilds tables from what
   `data/` still holds: a published table from its newest Delta or Iceberg version (schema, files,
   deletes), an unpublished append table from its files' footers (system columns included). It says
   what it rebuilt and what it couldn't (rows only in the log tail, views, routines, users, secrets).
   A lake with backups never needs it, so it comes after.

## Consequences

- Losing a bucket loses at most what was written since the last backup; a nightly task is the
  default the guide shows.
- A backup costs a second copy of the lake's files in storage, and egress when it is in another
  provider; incremental runs keep the copies to what changed.
- A backup ignores `on_clone`: a new kind is backed up with no change.
- Invariants 290 (a run copies one snapshot, and its commit names nothing it hasn't copied) and 291
  (a backup is written only by its runs, and keeps every object of the runs it keeps).
- Left for later: a restore of one table from a backup (today: `CLONE` the backup and `INSERT …
  SELECT`), server-side copies within one store, and a listing of the runs in SQL (today: the
  backup's answer, and its `bk` entry).

## Checks (phase 1)

`tools/backup_check.py`: a lake of every kind of object (the upgrade check's), with rows in files
and in the log, changed rows and a merge; a backup; more writes; a second backup that copies only
what is new; the source deleted; the backup served, a restore of its newest run and one `--at` the
first run, each == a model of that run; writes to the backup refused (25006) at every door; a clone
of the backup; a backup of a branch whose base is then deleted; a run killed half way leaves the
run before it readable and the next completes; `pondra backup` and `pondra restore` with no node; a
restore refuses a place that holds anything; a task's scheduled backup; a dropped table kept for
`UNDROP` comes back in the restored lake; a backup in a bucket (moto) the node has no variables
for, reached by the secret that covers it and restored from there. `backup::tests`: the lake's own
keys (`z/auth` among them) never go, a branch's base files' place, the times a restore takes.
