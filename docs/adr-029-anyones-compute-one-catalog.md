# ADR-029: Anyone's compute, one catalog

**Date:** 2026-09-29 · **Status:** accepted; phase 1 built in round 27 (2026-09-30, below); phase 2 begun in round 28 (copy-on-write changes, below), its rest next · **Builds on:** ADR-001 (readers who need only the bucket), ADR-002 (the streamhouse), ADR-003 (serverless) · **Follows:** ADR-028 (other engines append, G8), ADR-020 (system columns)

## Context

The owner, 2026-09-29, on hearing that other engines read Pondra's tables with their own compute
but that their appends cost Pondra a second read and write of every row:

> "We might somehow design architecture beautifully to allow external engines to read/write using
> their own compute. So it will be total serverless and compute/storage separation."

And, the same day: "Don't forget about our streamhouse features too. Like Flink, Fluss, Spark,
DuckDB comparison."

**What the first step already decided** (2026-09-19). This ADR keeps each decision and doesn't
reopen any of them:

- **ADR-001: readers need only the bucket.** Open-format metadata points at the lake's own files
  in place, and data is never copied to be read. Pondra has published Delta and Iceberg this way
  since round 6 (ADR-007).
- **ADR-002: the streamhouse.**
  - Its six requirements: the log, keyed tables, one read of hot and cold data, exactly-once
    tiering, incremental SQL, and retention.
  - The Fluss verdict: borrow its ideas, not its servers.
  - The capability matrix against Databricks and Snowflake.
  
  `comparison-spark-flink-fluss.md` keeps the head-to-head with Flink, Fluss, Spark and DuckDB
  current.
- **ADR-003: serverless.** Nothing is always on. Every process does its own work and hands its
  commit to whoever holds the lease. A bulk `INSERT` from `pondra sql` already works this way:
  that machine writes the files, and the leader only records them.

What is new here is one step on from those: ADR-003's rule (your compute, the leader's commit) and
ADR-001's rule (files in place, never copied) now apply to other engines' **writes**. Until now
they applied to Pondra's own processes and to other engines' reads.

Where it stands after round 25:

| When another engine… | Whose compute | What Pondra does besides |
|---|---|---|
| reads a table | its own | publishes Delta and Iceberg metadata every tier round (2 s); rewrites every file with a changed row, every round, so readers see the change (ADR-020's purge); a keyed table only after a full compaction |
| appends (round 25) | its own, then Pondra's | reads every row the writer wrote and writes it again (below) |
| deletes, overwrites, upserts, alters or creates a table | none | refused by name |

**Round 25's copy, measured.** PyIceberg appended through a node on local disk, on this 2-vCPU
sandbox:

- **4 M rows** (24.5 MB of the writer's Parquet): 1.1 CPU-seconds on the node, about 0.27 s per
  million rows, or 45 s per GB of Parquet as compressed as this.
- **A 10-row commit:** no CPU the node's counters can see, and 30–70 ms end to end.

So the copy costs the node what the writer spent writing, and grows with the data. The commit
itself costs almost nothing.

The copy buys three things (ADR-028): row ids, the table's layout, and whatever follows the table
(views, tasks, the change feed). None of them needs it:

- **Row ids.** Iceberg v3 (row lineage) and Delta (row tracking) define a row's id the same way:
  its file's first row id plus its position in the file. Rows that keep their id through an
  update carry it in a column. Pondra's ids (ADR-020) already follow this rule.
- **Layout.** Writers follow a layout when the table publishes one (Iceberg's partition spec and
  sort order). Small or unsorted files are merged later, as Pondra's own are.
- **Followers.** A view needs the new rows once, read by whoever keeps the view. It never needs a
  second copy of the table.

## Decision

**Storage is the lake, the catalog is the one coordinator, and compute is anyone's.**

Every engine reads and writes the table's Parquet files itself: Pondra's nodes, `pondra sql`,
Spark, Trino, Flink, DuckDB, PyIceberg, Snowflake. The catalog only decides which files make up
each version, in one order, under one set of rules.

| Work | Whose compute | Pondra's cost |
|---|---|---|
| A query, from any engine | the engine's | none: the metadata is in the bucket, or one catalog call away |
| An append, delete, update, merge or overwrite, from any engine | the engine's | the commit: one small read per new file, one catalog commit, one publish |
| A view or task that follows the table | Pondra's, on any node | the changed rows, read once; nothing if nothing follows |
| The change feed, `/watch`, Kafka topics, live queries | the reader's | serving it |
| Merging small files, clustering | Pondra's, in the background | as today, and a table can turn it off |
| Small, frequent writes (streaming) | Pondra's log (HTTP, Kafka, Flight, Postgres) | as today: this is what the log is for |

Batch writes go straight to storage with any engine's compute. Streaming writes go through
Pondra's log, which gives them millisecond latency.

### 1. A file is a file, whoever wrote it

- **Each file's lineage is recorded in the catalog.** The leader records a file with three values:
  - `first_row_id`: a block of row ids;
  - `version`: the commit that recorded the file;
  - `at`: when that commit happened.
- **Reads fill in missing system columns.** When a file has no system columns, a read takes them
  from its lineage:
  - `_row_id` = `first_row_id` + position (Parquet's row number, DataFusion's virtual column,
    which stays right under row selections);
  - `_version` = `version`;
  - `_created_at` = `_updated_at` = `at`.
- **A column in the file wins.** For example, a v3 writer copies a row's `_row_id` through an
  update.
- **Merging makes the columns explicit.** When maintenance merges such a file, it writes the
  columns into the new file (as tiering does), so ids survive every rewrite.
- **Ids come in blocks.** One commit's files share one block, each file taking the next range.
- **Statistics come from each file's Parquet footer.** Reading it is one ranged read per file, and
  it checks four things:
  - the file is there;
  - its row count and size match what the writer's manifest says;
  - its columns fit the table;
  - its column ranges, which are used for skipping only.
  
  This is the only data I/O a commit does. A footer without statistics means the file is never
  skipped: slower, still correct.
- **Columns are matched by field id where the file has them** (Spark and PyIceberg write them),
  otherwise by name. This is `scan.rs`'s adapter, which attached Iceberg tables already use.

### 2. The leader records files and never reads rows

- **What a commit carries** (`POST /v1/namespaces/{ns}/tables/{t}`):
  - added data files, with their lineage stamped;
  - removed files;
  - delete files, each attached to its data file (`scan.rs`'s `Delete`: position deletes,
    deletion vectors), which reads apply as row selections.
- **Every snapshot kind is taken:** append, overwrite, delete, and replace (another engine's
  compaction).
- **One outside commit is one Pondra commit.** The Iceberg snapshot published for it has that
  commit number as its sequence number. So Iceberg's `_last_updated_sequence_number` and Pondra's
  `_version` are the same number.
- **Files stay where the writer put them** (`data/<t>/data/…`): no copy, no rename. A file outside
  the table's folder is refused, as now.
- **Retention treats them like Pondra's own files:**
  - replaced files are deleted after `--retain-secs`;
  - files no commit ever named are deleted after a day.

### 3. Nobody commits on a stale table

- **Changes are checked against the table as Pondra knows it**, not only as last published. A
  delete, overwrite or replace can arrive while Pondra has commits to the table that aren't
  published yet (the log tail, at most a tier round old). Then the leader tiers and publishes at
  once, and answers 409. The writer refreshes, runs Iceberg's own conflict checks against what
  changed, and retries.
- **The effect:** an outside `DELETE … WHERE x = 1` never misses a row Pondra committed before it.
- **Appends keep round 25's rule** (checked against the last published version): an append
  commutes with anything.
- **Pondra's own changes are serialized with outside commits.** `UPDATE`, `DELETE` and `MERGE` read
  the recorded files like any others, under the same lock.

### 4. Deletes are positions, and are published as positions

- **Other engines' deletes become per-file positions.** They arrive as position-delete files
  (Iceberg v2), as deletion vectors (Iceberg v3), or as rewritten files (copy-on-write). Each
  becomes its data file's deleted positions, and reads skip those rows without decoding them.
- **Pondra's own changes are published the same way.** A tier round turns the `{t}$deleted` rows
  it covers into positions. It reads only the `_row_id` and `_version` columns, and only of the
  files whose ranges hold them. Then it publishes the positions:
  - Iceberg v2: position-delete files;
  - Iceberg v3 and Delta: deletion vectors.

  Pondra's own reads can use the same positions instead of today's anti-join.
- **ADR-020's per-round purge becomes maintenance.** Today every file with a changed row is
  rewritten every round. Instead, a file is rewritten once a tenth of it is deleted.
- **Keyed tables that publish do so every tier round.** The older versions a round replaces become
  positions, found by key (key-sorted files, bloom filters). Other engines then see a keyed table
  as of the last tier round, not the last full compaction.
- **The change feed:** a file's deleted positions are its `delete` rows, read from the file when
  the feed is read. An update shows up in one of two ways:
  - v3 writers: the same `_row_id` in the added file, so the feed shows the update;
  - v2 writers: a delete and an insert, as Iceberg's own changelog shows it.

### 5. Keyed tables take upserts

- **An outside append into a keyed table is upserts.** Each row becomes its key's newest version,
  by commit and then by position in the file (a read's `_ord` becomes that pair).
- **Equality deletes on the key columns become delete markers.** Equality deletes on any other
  columns are refused by name.
- **A position delete depends on which version it names:**
  - the key's current version: the key is deleted (a marker);
  - an older version: nothing happens (it is already shadowed).
- **Some tables refuse outside writes:** `order_by` tables and merge tables (a view's `GROUP BY`).
  Their rows are Pondra's to combine.

### 6. Layout is published for writers to follow

- **`partition_by` becomes Iceberg's partition spec:** identity, and year, month, day or hour of a
  timestamp.
- **`cluster_by` becomes the sort order**, for one column. Two or more columns use a Hilbert
  curve, which Iceberg can't express, so no sort order is published and maintenance clusters the
  files.
- **The key becomes the schema's `identifier-field-ids`.**
- **A partitioned table takes a file only if it holds one partition value** (the manifest's
  partition tuple under the current spec). Writers that follow the spec do this; other files are
  refused by name.
- **File sizes and sort order are the writer's choice.** Maintenance merges and sorts later, if the
  table asks.

### 7. Followers are fed from the files

- **A table that nothing follows:** the commit is all Pondra does.
- **A table that views or tasks follow:**
  - the node taking the commit reads the new files, and the deleted rows, once;
  - it derives each follower's change;
  - it commits those changes together with the files, as one commit (today's flush does this with
    log rows, so a view never sees half a commit);
  - nothing is rewritten, and the read is the followers' cost.
- **Everything that reads the change feed sees a file commit as its rows**, read from the files by
  whoever reads the feed. That covers `/watch`, Kafka topics and live queries.

### 8. The door: Iceberg's REST catalog, complete

- **Tables:**
  - create: schema, partition spec, sort order, identifier fields (a key), and properties
    (`publish`);
  - drop and rename;
  - schema updates that Pondra's `ALTER TABLE` can express: add, rename, drop, widen;
  - multi-table transactions (`/v1/transactions/commit`), all or nothing, because Pondra's catalog
    commit already spans tables.
- **Storage access for writers** (`X-Iceberg-Access-Delegation`):
  - where the store can mint credentials (S3's session policies, R2's temporary credentials),
    vended credentials scoped to the table's folder;
  - otherwise, Iceberg's remote signing;
  - on a local lake, nothing.
- **Scan planning** (the REST spec's plan endpoints):
  - a reader that asks for a plan gets the table's files as of now, with their deletes; the leader
    tiers first if the table is behind;
  - a reader that doesn't ask reads the last published version.
- **Delta writers go through a catalog, once one can be tested here.** The route would be Delta's
  catalog-managed commits, which Unity Catalog's open API serves; Pondra would serve that API as
  it serves Iceberg's. Until then, Delta stays read-only for others. Delta's own log commits
  (put-if-absent) can't refuse a conflicting commit, and Pondra's catalog must be able to.

### 9. Serverless, as ADR-003 decided

ADR-003's levels hold: nothing is always on, and a process starts when work comes. For other
engines this means:

- **Reading needs no Pondra process**, as ADR-001 decided: the metadata is in the bucket, or behind
  any node's REST catalog.
- **Writing needs one coordinator**, which is ADR-003's lease holder. Any node can take the commit
  and forward it. The coordinator holds no data, and on a cold start it opens the catalog in about
  20 requests (30 ms on local disk, 3–6 s on R2 from this sandbox).

### 10. The streamhouse stays as ADR-002 decided

The log, keyed tables, views and tasks, the Kafka, Flight and Postgres doors, and millisecond
freshness between nodes are all unchanged. Outside commits join the same order:

- **In the change feed and the topics.** An outside commit is in the change feed, `/watch`, Kafka
  topics and live queries as its rows (decision 7). So Flink or Spark reading a table through the
  Kafka port (round 10) sees other engines' writes too.
- **In the views.** Views take outside commits in commit order. Event-time windows take an outside
  commit's rows by their timestamps, on time or late, as they take the log's.
- **In the hot and cold read.** ADR-002's hot and cold read (R3) reaches other engines through
  scan planning (decision 8): the leader tiers first, and the engine reads plain files.

### 11. Found on the way: ids and Kafka offsets are built on commit numbers

- **Row ids are `(commit number << 32) + n`, and Kafka offsets are `(segment << 32) + row`, both
  64-bit signed.** With the default `--flush-ms 0`, a steady trickle of writes to a lake on local
  disk commits hundreds of times a second. At 500 commits a second:
  - after 2³¹ commits (50 days), Kafka offsets turn negative;
  - after 2³² commits (100 days), row ids repeat.

  On R2, at about 10 commits a second, the same limits are years away.
- **The fix, in phase 1:**
  - row-id blocks come from a counter of their own, which moves only when a node or a bulk write
    reserves a block, starting above every commit number already used;
  - Kafka offsets get an encoding that can't wrap in a table's lifetime.
- **Until then, prototype-status lists it as a known limit.**

## Rejected

- **Keeping round 25's copy:** Pondra's compute grows with every byte others write. That is the
  opposite of separating compute from storage.
- **Taking in commits written straight into the metadata** (Delta's put-if-absent, Iceberg's file
  catalogs) and adopting them afterwards: Pondra would learn of a commit after it happened, and
  could undo a conflicting one but never refuse it.
- **Row ids in a sidecar file per data file:** one more object per file, which no engine reads. The
  formats' own rule (a first id plus a position) needs nothing extra.
- **Publishing keyed tables with equality deletes:** every reader would pay a join on every read. A
  position is a skipped row.
- **Letting each engine commit with its own locking** (a file-system catalog, a DynamoDB lock):
  there would be no single order, and so no views, no row ids and no change feed.

## Phases

| Phase | What | Proof |
|---|---|---|
| 1 (a round) | Appends as written: lineage per file, footers read, layout published (partition spec, sort order, identifier fields), followers fed from the files, the feed and its readers seeing file commits; create, drop and rename through the catalog; the id fix (decision 11) | An outside append of 1 GB costs the node its footers and a commit, against round 25's copy; Spark, PyIceberg and another Pondra; the rows' ids stable through a merge and an `UPDATE` |
| 2 (a round) | Changes as written: deletes, overwrites and merges from Spark (copy-on-write and merge-on-read) and PyIceberg; the rule against stale tables; positions published for Pondra's own changes and for keyed tables (purges become maintenance); keyed tables taking upserts and equality deletes; schema changes; multi-table transactions | Pondra's answers equal Spark's after each; the change feed shows them; views follow; an outside `DELETE` racing an untiered `INSERT` gets 409 and deletes the row on retry; PyIceberg and delta-rs see a keyed table after each tier round |
| 3 (when each can be tested) | Iceberg v3 as an option per table (row ids kept through other engines' updates, deletion vectors, `next-row-id` from Pondra's blocks), vended credentials and remote signing, scan planning, Delta commits through a catalog | Spark's v3 `UPDATE` keeps `_row_id`; a writer with only a catalog URL and a token |

## Tests (the plan)

- **Cost:** the node's CPU time and bytes read for an outside append, before and after. The
  commit's work grows with the number of files, not with the rows.
- **Rows and ids:** every engine's rows equal what was written. `_row_id` is distinct, and stable
  through a merge, a Pondra `UPDATE` and a compaction. The change feed shows each commit once.
- **Isolation:** two outside writers and a Pondra `UPDATE` on the same rows; each lands once or
  retries; nothing is lost.
- **Layout:** a partitioned table takes a Spark write and refuses an unpartitioned file by name.
- **Serverless:** with every node stopped, reads through the static metadata still work, and a
  node started cold takes a commit.
- **Every new invariant** gets a test in `tools/` that fails without it.

## Built in round 27 (phase 1, 2026-09-30)

What was built, what was measured, and what moved. **Decided by Claude, for the owner's review**,
where marked.

- **Appends as written** (`adopt.rs`). The node taking the commit reads each added file's footer
  once (sixteen at a time) and checks it: the manifest's row count, each column's type as the
  table's (strings, binaries and lists however they are laid out; matched by field id, else by
  name), no NULL in a NOT NULL column (a footer that doesn't say is refused too), and one partition
  value in a partitioned table (by the footer's range of the partition column, put through the
  table's own `partition_by` expression). A file that fails refuses the whole commit by name,
  before anything is recorded. The leader records the files with their lineage and the columns'
  ranges and NULLs the footers gave, and publishes. The writer's files stay where they are.
- **Lineage per file** (`store::Lineage`: first row id, commit, time). Reads give the system
  columns from it (`scan::adopted`): `_row_id` is the first id plus Parquet's row number, a
  virtual column of DataFusion 55's Parquet reader, which stays right under skipped row groups;
  `_version` and the times are the commit's. Every read goes through it — queries, spread queries'
  slices, `UPDATE`/`DELETE`/`MERGE`, purges and merges (`query::files_once`) — and a merge writes
  the columns into its file. The ranges of the system columns are recorded too, so a purge skips
  the files that hold no changed row.
- **Measured:** a million rows appended by PyIceberg (one file): the two nodes' CPU for the commit
  0.01 s recorded as written, against 0.24 s copied (round 25's path, still taken below). The
  commit's work grows with the files, not the rows (`harness.py adopted`).
- **Still copied** (round 25's path, *decided by Claude*): a table that views or tasks follow (its
  rows go through the log for them, as a bulk `INSERT`'s do), and a table with a renamed or dropped
  column. **The Delta question is settled that way:** Pondra publishes Delta with column mapping, so
  a Delta reader looks a column up by its physical (stored) name, which another engine's file
  written after a rename doesn't have. Copying those appends keeps every Delta reader right; a
  table altered this way is the exception, and nothing is lost but the copy's cost.
- **Pondra's own files written without a leader** (`pondra sql`, the inbox) now take lineage when
  they are recorded instead of being rewritten with their system columns. *(Decided by Claude: one
  rule for every file that doesn't hold them.)*
- **Layout published** (`iceberg::Layout`): `partition_by` as partition spec 1 — identity (of an
  integer, string, boolean, date or timestamp), or year, month, day or hour — each manifest entry
  with its file's partition record; spec 0, no partitions, stays in the metadata for manifests
  written before. *(Decided by Claude: a new spec id rather than changing spec 0, so no manifest
  already written is misread.)* A `cluster_by` on one column is the sort order; a key is the
  schema's identifier fields, which Iceberg asks to be required, so a key's columns are published
  as required (they are NOT NULL). PyIceberg now writes a partitioned table a file per partition
  value, and Pondra takes them as written.
- **Found on the way:** a column rename reached the Iceberg metadata only with the next files, and
  PyIceberg (which matches an Arrow table's columns by the name mapping) refused appends to a
  renamed table. Now a version is published when the schema or layout changes (`Published::shape`),
  at once after `ALTER TABLE … COLUMN`, and the name mapping holds a renamed column's both names.
- **Tables through the catalog:** create (the types, required fields as NOT NULL, identifier fields
  as the key, one partition field, a write order of ascending columns as `cluster_by`, `publish`
  from the properties), drop and rename (within a namespace), each as the SQL statement run as the
  caller, so DDL needs an admin token as in SQL. A create answers the table's first published
  version. Struct, map, uuid, time and fixed columns, specs by bucket or truncate or of two
  fields, descending orders and staged creates are refused by name.
- **The id fix (§11):** row-id blocks come from their own counter (catalog key `b`, starting at the
  commit number the first time, so no block before it is reused); a node's block lasts it 2^32
  rows, and a bulk `INSERT` takes one. A log row's place (`_ord`, a Kafka offset) is now its
  segment's number shifted by 24 bits, not 32, then its row: 40 bits of segment numbers, 34 years at
  a thousand commits a second. A segment with more of one table's rows than 2^24 takes the numbers
  after it too (`log::span`), and a Kafka fetch looks back over them. *(Decided by Claude: offsets
  a consumer saved before this upgrade point elsewhere now; there is no promise of that before
  1.0, ADR-032 §8.)*
- **Tests:** `harness.py adopted` (10 checks: spread over three nodes too), `ids` (a 2^24 + 10-row segment, a consumer seeking
  into its second number), `writes` updated; everything else in `harness.py all` as before; Spark
  4 with Iceberg 1.10 appending through the catalog (`formats_check.py --only commits`), its rows
  and their ids as it wrote them.

**Moved to round 28, with phase 2** (*decided by Claude*, to finish phase 1 well rather than all of
it at once):

- **Followers fed from the files (§7):** a followed table still takes an append through the log.
  Deriving each follower's change from the files, in the same commit, needs the file records to
  travel with the sequencer's commit; phase 2's changes need the same.
- **The feed seeing file commits:** `/watch`, the change feed and Kafka topics read the log, so
  they carry neither an outside append recorded as written nor a bulk `INSERT` (which never did).
  The plan: a file commit leaves a marker segment in the log naming its files, which the feed's
  readers read the rows of.
- **Hot columns:** files recorded as written are read from Parquet, not the hot columns, until a
  merge rewrites them.

## Built in round 28, in part (phase 2's first step, 2026-09-30)

Other engines' **copy-on-write changes** (§2, §3), decided by the owner's plan; how far this round
went was *decided by Claude, for the owner's review*: the part every engine uses by default, done
fully, rather than all of phase 2 at once.

- **What a commit may do now:** its snapshots, one after the other with the last made `main`
  (PyIceberg's `overwrite` sends a delete's and an append's), with operations `append`,
  `overwrite` and `delete`; files added (recorded as written, as round 27's), and the table's
  files taken out — Pondra's own or another engine's, sealed ones too (their manifests unsealed).
  One catalog commit, published under the writer's last snapshot id. Spark's `DELETE`, `UPDATE`
  and `MERGE` (Iceberg's default, copy-on-write) and PyIceberg's `delete` and `overwrite` work.
- **Row ids:** rows in a file the change left alone keep theirs; the rows it rewrote get new ones
  (Iceberg v2's model, a delete and an insert, as §4 says). v3's kept ids are phase 3.
- **The stale rule (§3):** a commit that takes files out while the table holds rows its last
  published version doesn't (rows in the log, or `UPDATE`s not yet purged) is answered 409 after
  the leader tiers, purges and publishes the table; the writer reads it again and runs Iceberg's own
  conflict checks (PyIceberg's found the new row, in the test). A file taken out that isn't the
  table's any more (merged since) is a 409 too. *(Found: a change that matches no published file
  commits nothing, so a row only in the log escapes it; the answer is scan planning, phase 3 —
  documented.)*
- **Refused by name:** delete files (merge-on-read), a `replace` snapshot (another engine's
  compaction: Pondra merges its tables itself), and a change to a table that views or tasks follow
  or that has a renamed column (its appends are copied; its changes go through Pondra's SQL).
- **Tests:** `harness.py rewrites` (6 checks: a whole file dropped and part of another rewritten,
  ids kept and new, an overwrite of two snapshots, the stale rule, a followed table refused);
  `formats_check.py --spark … --only commits` (Spark 4 with Iceberg 1.10: INSERT, append, then
  DELETE, UPDATE and MERGE, Pondra reading what Spark reads).

**Still to do in phase 2:** merge-on-read (position deletes and deletion vectors, read as row
selections), Pondra's own changes published as positions and purges as maintenance, keyed tables
taking upserts and equality deletes, schema changes Pondra can express, multi-table transactions,
and what round 27 moved here: followers fed from the files in one commit, and the feed carrying
file commits.

## Open

- **Delta readers and renamed columns.** Say another engine writes a file after a column was
  renamed. Delta readers match columns by physical name, and the file uses the new name. Either
  maintenance rewrites those files before they are published to Delta, or Delta is published
  with column mapping by id. *(Settled in round 27, for now: such a table's appends are copied, as
  above. Rewriting them in maintenance, or Delta's mapping by id, stay the ways to record them as
  written if renamed tables turn out to take many.)*
- **Which round: decided.** Phase 1 is round 27 and phase 2 is round 28, after round 26's console,
  server and docs (the owner, 2026-09-29).
