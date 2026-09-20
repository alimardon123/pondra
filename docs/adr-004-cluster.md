# ADR-004: Cluster mode and distributed state: one binary, one bucket

**Status:** Accepted, prototype built and tested (round 3) · **Date:** 2026-09-20 · **Builds on:** ADR-002, ADR-003

## Context

Round 2 had one writer and any number of read-only nodes. Three questions were still open:

- What happens when many independent users write and read at the same time?
- How does streaming state (running totals, per-key aggregates) spread over several machines?
- How do nodes find each other and replace a dead writer?

The distribution mechanism had to stay simple: the same binary, pointed at the same bucket, with no ZooKeeper, etcd, Raft group, JobManager or coordinator service.

## Decision

### 1. Nodes form a cluster through the bucket they already share

Every node runs `lh serve --dir s3://bucket/lake --addr host:port`, and a node is known by that address. Nothing else is configured.

| Mechanism | How | Why it's safe |
|---|---|---|
| **Leader election** | A leader "term" is an object `cluster/term/{n}`, written with put-if-absent (S3/R2 `If-None-Match: *`) | Exactly one node can create each term. Tested with 5 nodes started at the same instant: 1 leader, and all agree |
| **Old leaders can't write** | The new leader opens the catalog (SlateDB), which fences the previous writer | A stale leader's next commit fails. It restarts itself and rejoins as a follower |
| **Liveness** | Followers send an HTTP heartbeat to the leader every second and get back the list of live nodes | No gossip or quorum. The leader's answer is the membership |
| **Failover** | If the leader has been unreachable for 5 s, the follower first asks the other members whether *they* still hear it. Only if none do does it claim term n+1 and restart as the leader | A follower with a bad link can't depose a healthy leader. If two followers race, put-if-absent picks one and the other follows it |

Roles:

- **Leader:** ingest (group commit), tiering, compaction, retention, catalog commits.
- **Followers:** answer SQL straight from the bucket, run their share of streaming-task shards, and forward writes to the leader unchanged. A client can talk to any node.
- **Readers (`--reader`):** SQL only. They never lead, so read-only bucket keys are enough.

### 2. Distributed state = sharded tasks whose state is a table

A streaming task gets `shards` and `shard_by`:

```json
{"source": "events", "target": "totals", "key": ["user"], "shards": 6, "shard_by": "user",
 "sql": "SELECT e.user, coalesce(max(t.total),0) + sum(e.amount) AS total FROM events e LEFT JOIN totals t ON e.user = t.user GROUP BY e.user"}
```

- **Routing:** rows are routed by a fixed-seed hash of `shard_by`. Shard `s` runs on the `s % n`-th live node, in sorted node-id order. Membership changes move shards automatically.
- **Where state lives:** in the target upsert table, in the bucket, not on a node's disk. There are no RocksDB checkpoints to ship around. A new owner of a shard just reads the table.
- **Exactly-once:**
  - Each shard's progress is a producer sequence (`task:{name}:{s}`).
  - A run over segments (done, hwm] commits its output and progress together, and only if progress is still `done` (compare-and-swap).
  - If two nodes run the same shard during a membership change, one commit wins and the other is rejected and discarded.
- **State is queryable:** it's a normal table, so `SELECT * FROM totals` works on any node, and so does any external engine reading the Parquet files.

### 3. Many independent users

- **Writers:**
  - Any number of producers can write through any node at the same time.
  - The leader's group commit folds every producer's batch into one catalog write per flush. There are no locks between users and no write-write conflicts. Throughput grows with batch size, not with coordination.
  - Each producer is deduplicated on its own `(producer, seq)`. Producers must have distinct names; `prev` (compare-and-swap) catches two clients that share one.
- **Readers:**
  - Every query sees one consistent snapshot: immutable objects plus one catalog view. No partial batch is ever visible.
  - Any number of nodes, `lh sql` processes and outside engines can read at the same time without coordinating.
- **Read-your-writes:** every ack carries its segment number. `POST /sql?after=<seg>` makes any node wait until it has seen that segment.

## Test results (2 vCPU, local disk and a simulated R2 with R2-like latency)

| Test | Local disk | Simulated R2 |
|---|---|---|
| **64 independent writers + 16 SQL readers + 2 serverless `lh sql` processes**, over 3 nodes, 20–38 s | 407k events, 20k/s (the Python clients are the limit), 1,985 reads: **0 inconsistent reads, 0 lost or duplicated batches** | 400k events, 10.6k/s, 4,314 reads: **0 inconsistent, 0 lost or duplicated** |
| **Leader race:** 5 nodes started at once on an empty lake | 1 leader, all agree | 1 leader, all agree |
| **Distributed state + failover:** 3 nodes, 6 shards, 8 producers writing through random nodes, leader `kill -9` twice | Writes resume after 5.3 s / 5.3 s. **Events exactly once. State = in-memory model for all 1,000 keys.** Shard runs spread 35/76/28 | Writes resume after 12.6 s / 14.8 s. **Events exactly once. State = model.** Terms 1→2→3, no flapping |
| **Cut-off follower:** the leader stops answering one follower's heartbeats, but the other follower still hears it | The cut-off follower checks with its peer and does **not** take over (term stays 1). When the leader is then killed, a follower takes over in 5.0 s | Same; takeover in 8.2 s |
| **Split brain:** the leader is frozen (SIGSTOP), a follower takes over, then the old leader resumes | Takeover 6.0 s. The stale leader's write is rejected and it rejoins as a follower. Nothing lost | Takeover 10.2 s, same outcome |
| **Regressions:** crash/exactly-once, upsert, insert, reader | All pass | Crash 4/4 runs clean (70 crashes) |

## Consequences

**Good:**

- One mechanism covers every deployment: a laptop, one server, or N nodes, all reading the same bucket.
- Nothing to operate besides the binary. The bucket is the only durable state, so a node's disk can be lost at any time.
- State moves between nodes for free, because it was never on a node.

**Costs and limits:**

- **One leader commits everything.** Writes scale up, not out. Measured: 585k events/s end to end on 2 vCPUs, and more with bigger batches. Past one machine's ingest, the plan is one leader per lake (lakes per domain or tenant), or partitioned logs later.
- **Failover takes about 5 s plus the time to open the catalog:** 5–6 s on local disk, 8–15 s on simulated R2. Writes retry through that window, and reads keep working on the followers. A brand-new leader gets up to 30 s to open the catalog before followers give up on it. Without that grace, followers deposed leaders that were still starting, and leadership flapped on slow storage (found and fixed in this round's tests).
- **Followers lag the leader** by one catalog poll plus object-store latency (≈0.3–1 s). Use `?after=` when a client must read its own write.
- **Task state is re-read from the table every run.** That's fine for up to millions of keys. For very large state, partitioned upsert compaction and key-range pruning are next (ADR-003 item 4).
- **The lease is time-based.** Safety doesn't depend on clocks, only on put-if-absent and catalog fencing. Liveness does: a leader paused for more than 5 s is replaced.
- **Membership is the leader's view.** A follower cut off from the leader but not from its peers stays a follower and runs no shards until its link heals. Nodes must run under a supervisor (systemd, Kubernetes) so a node that exits, for example because it can't reach the bucket, comes back.

## Alternatives rejected

| Option | Why not |
|---|---|
| ZooKeeper / etcd / Consul | Another cluster to run; the bucket already gives us put-if-absent |
| Raft among lh nodes | Needs a quorum and per-node disks; durability is already the bucket's job |
| Flink-style keyed state (RocksDB + checkpoints) | Moving state on rescale is the hard part; a table in the bucket needs no moving |
| Gossip membership (SWIM) | Heartbeats to the leader are enough when the leader already decides everything |
