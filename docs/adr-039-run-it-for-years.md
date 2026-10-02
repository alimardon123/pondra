# ADR-039: Run it for years: a lake's format, every release's lake, rolling upgrades, drains

**Date:** 2026-10-02 · **Status:** accepted (round 33, the data half; decided by Claude under the
owner's standing direction to work through the roadmap) · **Builds on:** ADR-004 (the cluster),
ADR-005 (every node writes), ADR-029 §11 (log places and row ids), ADR-032 §8 (compatibility from
1.0), invariants 17 and 46

## Context

Round 33 of the roadmap is "run it for years": a lake format version, every release's lake since
0.22 opening in the new one (in CI), a rolling upgrade of a mixed-version cluster, drain before
stop, and a 24-hour soak (C4). Where things stood:

- **Nothing in a lake said which Pondra's rules wrote it.** Catalog entries are JSON with
  `#[serde(default)]` for fields added later, so an older binary reads a newer lake without an
  error, and silently: a field it doesn't know is dropped. That is fine for a field that only adds
  (a statistic, a name), and wrong for one that changes what the rows are: round 28's deleted rows
  as positions (`DataFile::deletes`), read by a 0.27 node, come back. ADR-029 §11 changed what a
  log place means in 0.27 and could only say "no promise before 1.0".
- **No test opened an older release's lake.** Every suite makes its lakes with the binary it
  tests.
- **Stopping a node dropped what it was doing.** SIGTERM checkpointed a leader's catalog and
  exited at once: requests in flight got a reset, a load balancer kept sending work until its
  probe failed (there was no probe but `/stats`), and the followers of a stopped leader waited out
  the 5 s lease before one took over, as if it had died.
- **No long run.** Leaks and slow drifts (memory, objects, the log) show only over hours.

## Decision

1. **A lake has a format: a number in its catalog** (`format`: `{"format": n, "by": "<release>"}`;
   none is format 0, every lake made before this ADR). A build knows the formats up to its
   `FORMAT` (`format.rs`; 1 now, which is the mark itself and changes nothing else). Every way into
   a lake opens it through `Lake::open`, which **refuses a newer lake by name** before its tables
   are read: "… is a lake of format 2, written by Pondra X; this is Pondra Y, which knows formats
   up to 1: run Pondra X or newer here". A node that would lead one gives the term back
   (`cluster::release`) and exits, so the next node leads at once instead of after 30 s; a running
   node whose lake moves past it stops (`format::watch`).
2. **The leader moves the format, once every node knows it.** Every node says its release and the
   newest format it knows on every call to another (`x-pondra-version`, `x-pondra-format` on
   `cluster::http`'s client). The leader keeps what each follower's heartbeat said and counts the
   commit streams open by format (read-only nodes don't heartbeat), waits two leases after it
   starts leading, and then every 10 s raises the lake to the newest format every live node knows,
   its own at most (`format::raise`). A node from before formats says nothing: format 0, so the
   lake stays where it is until the last one has gone. A lake a build makes (nothing committed
   before its leader opened it) is of that build's format from its first commit: no older node
   has read it. `/stats` shows the node's release and the
   lake's format; the leader's lists every live node's release (`releases`): a rolling upgrade's
   progress.
3. **The rule for what comes next.** A change an older release would read *wrongly* (not just
   ignore) gets the next format, and this build writes it only once the lake is at that format: the
   older nodes are gone by then. A migration, when one is needed, runs in the commit that raises
   the format. A field an older release can safely ignore needs no format. From 1.0 on, ADR-032
   §8's promise is this mechanism's: a release opens every older format.
4. **Every release's lake opens, in CI.** `tools/upgrade_check.py lakes` downloads each release
   since 0.22 (its npm package's binary) and makes a lake with it holding everything that release
   knows: append, keyed, deduplicated, partitioned, clustered, published and merge tables, a
   schema, renamed, dropped and widened columns, materialized, window, session and stream-join
   views, a flow with an expectation, a history view, a task, a macro, functions and procedures, a
   secret, users and grants, a lake file, an attached lake, an external table; rows tiered and in
   the log, written over HTTP appends with a producer's sequence, SQL, bulk INSERTs and `pondra sql`
   with no node. This build reads it with `pondra sql`, then serves it, and every answer must be the
   release's, row ids and versions included; then it writes on (a producer's last batch again is a
   duplicate, old rows changed, tiered, merged, killed and opened again), and its answers must equal
   a lake this build made the same way, with Delta and Iceberg readers following. `upgrade.yml`
   runs it on every pull request. Where a release itself answered wrongly and a later one fixed it,
   the check compares only what that release got right, and says so (0.24 and 0.25 named tables
   `VIEW`s in `information_schema`, invariant 132; 0.22's `pondra sql` took no column list).
5. **A rolling upgrade is a node at a time, followers first.** Stop a follower, start the new
   binary on its address, wait for `/ready`, go on; the leader last. The cluster serves and takes
   writes throughout, a mixed one included (`upgrade_check.py rolling`: from the newest release,
   followers first and leader first, under load, every acknowledged batch once and no torn read on
   any node). Leader first works too; it costs a lease more for the older followers.
6. **A node drains before it stops** (`drain.rs`). SIGTERM, Ctrl-C or its starter's end: `/ready`
   answers 503 (`/healthz` stays 200 while the process runs), it waits
   `PONDRA_DRAIN_GRACE_SECS` (0) for a load balancer to notice, new requests get 503 with
   `Retry-After: 1` and a new Postgres connection is refused, except the cluster's own calls; the
   requests in flight at HTTP and each Postgres statement (`panics::door` counts them) finish, for
   `PONDRA_DRAIN_SECS` (30) at most. A second signal stops at once. Then a leader waits until what
   it committed is in the bucket (with `--ack replicated` too), checkpoints its catalog, **steps
   down** (`cluster/left/{term}`, put-if-absent) and releases its mark; a follower whose leader
   stops answering and has stepped down claims the next term at once, without the lease or asking
   its peers. Kafka and Flight connections end with the process; their clients retry, as they do
   when a broker goes. `/ready` is also 503 while a node that just started catches up with its
   leader (invariant 197).
7. **The soak** (`tools/soak.py`, C4): a cluster under steady ingest (appends with a producer's
   sequence, an upserting producer against a model, two views, readers checking every node), a node
   stopped or killed in turn (by default 8 times a run, at most hourly), each minute every node's
   memory, the leader's untiered rows and commit times, the longest wait for an acknowledgement and
   the object writes a second on a timeline; at the end every batch once, the keyed table as its
   model, the views as their rows, the log drained, memory flat (judged per process, since a node
   started again starts small). `soak.yml` runs it on a GitHub runner (5.5 hours at most, local
   disk or R2). A 24-hour soak needs a machine that runs that long: the owner's. On a bucket each
   commit is about an object write (a durable acknowledgement waits for one), so a soak's cost is
   its commits: `--rate / --batch` a second.

## Measured (round 33, this build, the 4-core sandbox)

- Every release's lake (0.22.0, 0.22.1, 0.22.2, 0.24.0, 0.25.0, 0.26.0, 0.27.0, 0.30.0) opens and
  carries on: 8 of 8, 20–24 s each.
- The check found that **a key lookup could miss a live key**, in every release from 0.22 to 0.30
  and on main: `GET /lookup` and a point query (`WHERE key = …`, answered without planning since
  ADR-036 §6) read a row's `_deleted` without checking it was NULL, so a live row whose NULL's
  value bit the Parquet decoder left set read as deleted. Which keys missed depended on the files'
  layout (a key ahead of a deleted one in the same file). Fixed in `serve::lookup`; the check
  compares every key's lookup, point query and planned query on every lake, and fails on main.
- A leader stopped with SIGTERM under load (3 nodes, 4 producers): the longest any batch waited
  for its acknowledgement was 0.6 s; killed with `kill -9`, 5.2 s (the lease). Every batch once.
- A 0.30.0 cluster upgraded a node at a time under load: a follower's step 0.05–0.2 s of waiting,
  the leader's about 5 s (0.30.0 doesn't step down); then restarted a node at a time from this
  build, 1.0 s at most. The lake moved to format 1 once the last node ran this build.

- The soak, 5 minutes on 3 nodes at 500 rows a second in batches of 20 (a leader and a follower
  each stopped and killed): every batch once, no torn read, the keyed table as its model, the views
  as their rows, the log drained. A stopped leader kept batches waiting 0.13–0.45 s, a killed one
  2.2 s. About 27 object writes a second for 27 commits a second.

## Not decided here, or later

- **Kafka consumer groups' offsets saved by 0.26 or before** point elsewhere since 0.27 (ADR-029
  §11) and aren't migrated: nothing records which numbering saved one. They are the one known
  break in an upgrade from before 0.27; a consumer group starts again from its reset policy.
- **A binary from before this ADR can't refuse a newer lake**: it doesn't know the key. The first
  format change that matters should come after the releases before it are out of use; until then
  the format only moves when every node says it knows it, and a node from before formats keeps it
  at 0.
- **Time travel (`AT`), retention per table, `UNDROP`, `CLONE`, `RESTORE`**: the round's second
  part, after round 32's changes to the same files have merged.

## Invariants (for AGENTS.md, numbered when they go in)

- **A lake newer than this build is refused before its tables are read** (`format::check` in
  `Lake::open`), by name, at every door; a node that would lead it gives the term back first.
  `upgrade_check.py format`.
- **The lake's format moves only to what every live node knows** (`format::raise`: followers'
  heartbeats and the commit streams' `x-pondra-format`, after two leases), and a change an
  older release would read wrongly is written only at its format. `upgrade_check.py format`
  ("…no further") fails if the leader raises past a follower.
- **Every release's lake since 0.22 opens in this build and answers as it did**
  (`upgrade_check.py lakes`, `upgrade.yml` on every pull request).
- **A node told to stop drains** (`drain.rs`): not ready, new requests turned away to be
  retried, those in flight finished (bounded); a leader then waits for durability, checkpoints
  and steps down (`cluster/left/{n}`), and its followers take over without the lease.
  `upgrade_check.py drain` ("…sooner than when it is killed").
- **A key lookup reads `_deleted` only where it is valid** (`serve::lookup`): a NULL's value bit
  means nothing. `upgrade_check.py`'s "a key looked up … as SQL reads it".
