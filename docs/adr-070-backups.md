# ADR-070: A backup is a lake

**Date:** 2026-10-11 · **Status:** proposed · **Builds on:** ADR-001 (the files where they are),
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

1. **A backup is a lake.** `BACKUP DATABASE [name] TO 'url'` (and `pondra backup [LAKE] URL`)
   makes a lake at `url` that is the database as of one commit: made as `CLONE` makes a branch (a
   pin, then the source's catalog read at a version that holds it, ADR-047), except that every
   file it lists is **copied** into the backup's own `data/`, never listed under `_base/`, and
   every kind of object is copied, whatever the registry's `on_clone` says (a backup takes its
   secrets, shares and recipients; a branch doesn't). Secrets stay sealed as they are: restoring
   them needs the same master key, which a backup never holds (keep it apart from the bucket).
   The workspace's files go too, with their kept versions.
2. **Run again, it copies what is new.** A second `BACKUP` to the same place pins the source
   again and copies only the files the backup doesn't list yet (a file is never changed under its
   name, invariant 6), then writes every entry in one commit and lets the pin go. The first run
   costs the lake's size in copies; each later one what was written and merged since. A file is
   copied before any commit names it, so a backup stopped half way leaves the previous one whole
   and readable, and the next run finishes it.
3. **Its past is the backups.** Each run is a commit of the backup lake, which keeps its replaced
   files for `retention_days` (7 unless the statement says, `WITH (retention_days = 30)`), so
   `AT (TIMESTAMP => …)` reads any table as of an earlier run, and `UNDROP` brings back a table a
   later run no longer had.
4. **Restoring is reading it.** A backup opens as any lake (`pondra serve URL`), read-only to
   everything but the next `BACKUP` (its `bk/` entry: writes refused by name, 25006). `CREATE
   DATABASE prod CLONE 'url' [AT (TIMESTAMP => …)]` makes a working database from it without
   copying; `pondra restore URL TO 'new url' [--at …]` copies it into a place of its own, with its
   tasks as they were (a backup keeps them suspended: nothing in a backup reaches the outside).
   A restore refuses a place that holds a lake.
5. **On a schedule like anything else:** `CREATE TASK nightly SCHEDULE '0 3 * * *' AS BACKUP
   DATABASE TO 's3://other-bucket/prod'`. Every door takes it as SQL; the command line's `pondra
   backup` and `pondra restore` are the same statements.
6. **Within the bucket's limits** (principle 7): the catalog names every file, so nothing is
   listed; copies go through the node's request budget (`budget.rs`), server-side within one
   store; a backup's commit is one write. It runs where a statement runs, as a one-off leader of
   the backup lake (invariant 62: `pondra lead`, as `CREATE DATABASE` does).
7. **Before an upgrade, a backup.** The upgrade guide's first step for a release that raises the
   lake's format is `BACKUP DATABASE TO …` with the release still running. A backup keeps the
   format it was written at, so the release being left can still serve it if the upgrade goes
   wrong (a release refuses only a lake newer than itself, invariant 207).
8. **Repair, for a lake with no backup** (phase 2): `pondra repair LAKE` rebuilds tables from what
   `data/` still holds: a published table from its newest Delta or Iceberg version (schema, files,
   deletes), an unpublished append table from its files' footers (system columns included). It
   says what it rebuilt and what it couldn't (rows only in the log tail, views, routines, users,
   secrets). A lake with backups never needs it, so it comes after.

## Consequences

- Losing a bucket loses at most what was written since the last backup; a nightly task is the
  default the guide shows.
- A backup costs a second copy of the lake's files in storage, and egress when it is in another
  provider; incremental runs keep the copies to what changed.
- `on_clone` gains a third reader (`backup`), which ignores it: a new kind is backed up with no
  change, and `harness.py registry` checks that every kind round-trips through a backup.
- Invariants 290–291 (L1's numbers): one snapshot per backup run, and no commit naming a file not
  yet copied.

## Checks (phase 1, 0.36.0)

`tools/backup_check.py`: a lake holding one of every kind (the demo lake), a backup, writes,
merges and a purge, a second backup; the source prefix deleted; the backup read at both runs ==
a model of each; a restore and a `CLONE` of it answer the same; a backup killed half way leaves
the first readable, and its next run completes; `--s3` on simulated R2 with the copies counted.
