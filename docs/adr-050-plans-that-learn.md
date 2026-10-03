# ADR-050 (proposed): Every plan as estimated and as it ran, and a lake that learns from its runs

**Date:** 2026-10-03 · **Status:** proposed; part 1 built (expected rows in every plan, the history's new columns) · **Asked:** Alimardon, 2026-10-03 23:33
("estimated and actual plans kept in the history, adaptive execution, and the platform learning
from them so it needs less maintenance") · **Builds on:** ADR-048 (`pondra.history`), ADR-020
(`guard.rs`: a query spreads when it pays, learned from its own times), round 32's join order from
the catalog's statistics (`optimize::JoinOrder`), ADR-016 (`skew.rs`: hot keys shared out between
steps), ADR-043 (a table's past: `AT (VERSION => n)`)

Numbers in the examples are made up to show the shape.

## What Pondra does today (read from the code on main, 81c1cc9)

| | Today | Where |
|---|---|---|
| The plan before it runs | `EXPLAIN`: the operators, no rows expected. The console: "The plan, before it runs" | DataFusion's EXPLAIN |
| The plan as it ran | `EXPLAIN ANALYZE`: each operator's rows and time. The console: "Query profile" | `console/plan.js` |
| Plans kept | `pondra.history.plan`: the plan that ran, with its metrics, for a statement over 1 s that ran on one node. A spread one keeps its trace (each node's share), not a plan. A fast one keeps nothing of its plan | `history::planned`, `server.rs` |
| Learned | each query's last three times, here and spread, in one node's memory (4,096 queries): gone at a restart, and each node learns alone | `guard.rs` |
| Estimates | row counts, column ranges and distinct-count sketches from the catalog. They are bounds, so the join order needs a 2× margin to overrule the order written, and nothing a run shows ever corrects them | `optimize::JoinOrder` |
| Adapting while it runs | dynamic filters (a join's build side and a top-N's bound filter the scans as they go, hot batches skipped: invariant 203); a hot key shared out between shuffle steps (`skew.rs`, Spark's adaptive skew join); a query out of memory run again with sort-merge joins; scalar subqueries answered before what uses them | several |

### Why a plan can look wrong today

Read from the code, not reproduced:

1. **On a cluster, the plan shown is one node's.** The Plan tab runs `EXPLAIN` and its Query profile
   `EXPLAIN ANALYZE`; neither spreads (the spread analysis refuses an EXPLAIN's operators), but the
   query itself may run across the nodes when the guard says it pays.
2. **A query out of memory runs again with other joins** (sort-merge instead of hash); the plan shown
   has hash joins.
3. **The same query can plan differently a moment later**: once the hot columns hold a table, its scan
   and the gather above it change (invariant 34).
4. **The estimate is invisible.** No row counts appear before it runs, so a wrong guess shows only as
   a slow run, and the next run guesses the same way.

## Decision

### 1. Two plans, named as people know them

**Estimated plan** (before it runs) and **Actual plan** (what ran): SQL Server's names, which say
which is which. Snowflake and Databricks call the second a "query profile". Every operator shows the
rows it expects, and the actual plan shows the rows it got beside them, so a bad guess stands out:

```sql
EXPLAIN
SELECT c.region, sum(o.total) AS sales
FROM orders o JOIN customers c ON c.id = o.customer_id
WHERE o.day >= DATE '2026-09-01' AND c.country = 'FR' AND c.city = 'Paris'
GROUP BY c.region;
```

```
AggregateExec                    rows≈ 12
  HashJoinExec (c.id = o.customer_id)   rows≈ 41,000
    FilterExec (country, city)   rows≈ 30          (country and city taken as independent)
      DataSourceExec customers   rows≈ 2,000,000
    DataSourceExec orders        rows≈ 1,400,000
```

```sql
EXPLAIN ANALYZE SELECT …;   -- the same statement, run
```

```
AggregateExec                    rows 12          expected 12          1.1 ms
  HashJoinExec (c.id = o.customer_id)   rows 960,000     expected 41,000    ×23   210 ms
    FilterExec (country, city)   rows 21,400      expected 30        ×713    18 ms   (Paris is in France)
      DataSourceExec customers   rows 2,000,000   expected 2,000,000          40 ms
    DataSourceExec orders        rows 1,400,000   expected 1,400,000         160 ms
```

- `EXPLAIN ANALYZE` runs the statement the way it would run: across the nodes when the guard spreads
  it, each step's plan with each node's rows and time; frugally, if it had to.
- The console says "Estimated plan" and "Actual plan" in place of "The plan, before it runs" and
  "Query profile". Per Alimardon's note of 23:28, the console's half waits for the UI round at the end;
  the engine's half (the numbers, the plan that really ran) comes first.

### 2. The history keeps what each statement ran

New columns of `pondra.history` (names for the SQL review thread):

| Column | What | Kept for |
|---|---|---|
| `fingerprint` | the statement with its literals taken out: one query shape's runs together (ADR-048 left it for later) | every statement |
| `plan_id` | the shape of the plan that ran: its operators, the join order, here or spread and how | every query |
| `version` | the commit it read at: its answer again, from the lake's past | every statement |
| `reads`, `writes` | the tables and views it read; the tables it changed | every statement |
| `misestimate` | a factor: how many times more or fewer rows its worst join gave than expected (`40`: forty times off) | queries over 100 ms |
| `plan` (the actual plan; `actual_plan` if the SQL review renames it) | the plan that ran, every operator's rows beside what it expected, its time and bytes; each step and node for a spread query | slow statements, and any with a `misestimate` of 10 or more |
| `estimated_plan` | the plan before it ran, kept only when it differs from the one that ran (it spread, ran frugally, or changed between steps) | as above |

```sql
-- Yesterday's slowest statements, with both plans
SELECT at, ms, statement, plan   -- (and estimated_plan, once the plan can differ: part 3)
FROM pondra.history
WHERE at > now() - INTERVAL '1 day'
ORDER BY ms DESC LIMIT 20;

-- A dashboard's query got slower: did its plan change?
SELECT plan_id, count(*) AS runs, median(ms) AS ms, max(at) AS last
FROM pondra.history
WHERE fingerprint = 'a41f9c0e'
GROUP BY plan_id ORDER BY last;

-- Everything that read orders today
SELECT at, "user", ms, statement
FROM pondra.history
WHERE array_has(reads, 'orders') AND at >= current_date;

-- Where the planner guessed worst this week
SELECT fingerprint, max(misestimate) AS worst, count(*) AS runs
FROM pondra.history
WHERE misestimate >= 10 AND at > now() - INTERVAL '7 days'
GROUP BY fingerprint ORDER BY worst DESC;
```

**Answers aren't kept.** Keeping every answer costs as much as the queries read. `version` gives the
answer again for nothing, from the lake's own past:

```sql
-- the statement above, as it read the lake then (version from its history row)
SELECT region, sum(total) FROM orders AT (VERSION => 18234) GROUP BY region;
```

That works for append tables within their retention; a keyed table is refused by name (invariant
217). The console's History can offer "Run as it was" in the UI round. Answers are already kept where
it pays: the console's pages for 20 minutes (invariant 171), remembered answers for a catalog version
(invariant 14), `WITH (cache = …)`; to keep one for good, `CREATE TABLE … AS`.

### 3. The lake learns, and every node learns the same

After a query of 100 ms or more runs (`PONDRA_LEARN_MS`), its node compares each scan's, filter's and
join's rows with what the planner expected, and sends the leader those off by 2× or more, once a
second. Faster queries teach nothing worth the cost.

The leader keeps them as **facts**, catalog entries under a prefix of their own (`l/`, invariant 104),
in at most one commit a second, quiet (invariant 224: they change no answer, so remembered answers
stay). Every node holds them through the commit stream, as it holds the catalog. At most 20,000; the
least recently used go first, and a table's go with it.

| Fact | Kept as | Used by |
|---|---|---|
| rows a scan returns under a filter | a share of the table's rows, so it holds as the table grows; keyed with the filter's literals (a dashboard asks the same again), and also without them once every run agreed (`day >= now() - INTERVAL '7 days'`) | the join order, the build side |
| rows a join returns per row of its inputs | its fan-out | the join order |
| bytes each exchange moved | per exchange | the spread guard, broadcast or shuffle |
| a query's time here and spread | `guard.rs`'s times, moved here | the spread guard, now shared by every node and kept through restarts |

```sql
SELECT * FROM pondra.learned WHERE object = 'customers';
```

```
object     kind    about                                   expected   actual   runs  updated_at
customers  filter  country = 'FR' AND city = 'Paris'      0.0015%    1.07%    14    2026-10-03 23:10
customers  join    orders.customer_id = customers.id      1.0        1.0      14    2026-10-03 23:10
```

- **The planner takes a fact in place of its estimate** where one fits. The join order trusts counts
  more than bounds: where every input's size is learned its margin drops from 2 to 1.2.
- **Every node plans alike** (invariant 27): a spread query's coordinator sends the facts it planned
  with in each slice, as a slice carries its file listing (`Slice::ext`), so a node whose commit
  stream is a moment behind plans the same way.
- **Never worse**: the plan with the facts and the plan before them are both timed, and the faster
  wins, as the spread guard does today (invariant 72). A fact that made a query slower twice is set
  aside for that query. (SQL Server calls this automatic plan correction.)
- `PONDRA_LEARN=off` turns it off; nothing else is to set.

### 4. Adapting while it runs, where a run pauses

A spread query runs a step at a time, and between steps the coordinator knows how many bytes every
exchange moved (`skew.rs` already shares out hot keys by them). Two more of Spark's adaptive ideas
fit there, without a driver:

- **Broadcast after all**: a join side that came out small is sent whole to every node, so the other
  side isn't shuffled.
- **Fewer, fuller partitions** when a step's buckets come out small.

Each change shows in the actual plan ("broadcast after its step: 3 MB, expected 2.4 GB").

On one node a query's operators run at once, as a stream: there is no point to stop and re-plan
short of starting over. There, dynamic filters adapt the scans as it runs (built), and the facts
make the next run's plan right. Oracle's adaptive join (rows held back until the join method is
chosen) is not planned; it is the answer for a query asked only once, if a check shows one that
learning can't fix.

### 5. Maintenance from what ran

In order, each a step on the one before:

1. **Maintenance goes where the reads are** (nothing to set): the leader merges and compacts first
   the tables and files read most (history's `reads`, the scans' facts), not in catalog order.
2. **Advice** (a name for the SQL review thread):

   ```sql
   SELECT object, advice, because, statement FROM pondra.advice;
   ```

   ```
   object          advice          because                                                   statement
   orders          cluster by day  82% of 3,400 reads filter on day; 9% of files hold it      ALTER TABLE orders CLUSTER BY (day)
                                   narrowly
   sales_by_store  materialize     read 4,100 times today, 1.2 s each, one query shape       CREATE MATERIALIZED VIEW sales_by_store_m AS …
   clicks_2024     unread          no statement read it in 30 days                           (none)
   ```

3. **Automatic, when asked**: `ALTER TABLE orders CLUSTER BY AUTO` (Databricks' words; `CLUSTER BY NONE`
   turns it off; underneath, `cluster_by = 'auto'`; Redshift's automatic table optimization does the same): the leader picks the clustering from
   the reads, changes it rarely, and applies it in its merges (two columns along a Hilbert curve, as
   today). Materialized views stay advice: a view is something people see and pay to keep.

### 6. Tables and views

- **A table** has no plan. What it has is its statistics (what the planner expects: rows, bytes,
  ranges, distinct counts) and what reads of it actually returned (its facts), and its reads (history's
  `reads`). The console's Details shows them side by side, in the UI round.
- **A view** is planned when it is read: its estimated plan is `EXPLAIN SELECT * FROM v`, and its
  actual plans are its readers':

  ```sql
  SELECT at, ms, plan FROM pondra.history
  WHERE array_has(reads, 'sales_by_store') ORDER BY at DESC LIMIT 1;
  ```

- **A materialized view** runs its plan at every write (kept from one write to the next, invariant
  200). A slow refresh becomes a history row of its own, its class `refresh` and its statement the
  view's name, with its actual plan, so it is found the same way:

  ```sql
  SELECT at, ms, plan FROM pondra.history
  WHERE class = 'refresh' AND statement = 'sales_by_store';
  ```

### 7. Room for an AI advisor later (Alimardon, 23:34: around 1.0, not now)

He plans to let an AI model's API help run the platform from the history: clustering, indexes,
maintenance, later the ETL and reporting tools. Nothing above has to change for it, because each
part is a small surface another part can plug into (principle 9):

- **What it reads is SQL**: `pondra.history`, `pondra.learned`, the tables' statistics. Any model
  reaches them over MCP (`POST /mcp` already serves `list_tables` and `query`) or the clients.
- **What it proposes is rows**: `pondra.advice` gets a `source` column (`rules` for the built-in
  advisor, `ai:<name>` for another); every row carries the statement it suggests and why.
- **What it does is a statement**, checked as any user's (invariant 176), applied only when an admin
  takes it or the table opted in to `auto`. A model never gets a door of its own.
- **Where it runs** already exists: a Python procedure on a schedule (`CREATE TASK … SCHEDULE`)
  calling the model's API with a `CREATE SECRET`, or `ai_complete` in SQL. An advisor is one
  registered source of advice, and the built-in rules are another, so either can be replaced.

## Costs (principles 6 and 7)

- Every statement: a fingerprint, a plan id, a version and the tables it read, a few microseconds.
  The bar is ADR-048's: point lookups within 1%.
- Facts come only from queries of 100 ms or more, reach the leader once a second, and commit at most
  once a second, quietly; 20,000 at most, a few MB on each node. A spread query's slice carries a few
  KB more.
- Nothing runs in the background that doesn't run today.

## Checks

- `tools/learn_check.py` (new): correlated filters (country and city), a join that fans out, a
  skewed date range. First run against second: the plan changes, the answer doesn't, the second is
  no slower; on three nodes every node plans alike (no fall back to one node); a fact that made a
  plan slower is set aside after two runs; the facts survive a leader's restart.
- `harness.py history`: the new columns; both plans of a spread query; `version` gives the answer
  as it was.
- The gates, once per engine PR: TPC-H's 22 and TPC-DS's 99 answers equal DuckDB's and no query is
  slower, first run and second; `join_order.py`; `spread_tpch.py`.

## Phases and owners

1. **Plans you can trust** (no new syntax): expected rows in every plan, `EXPLAIN ANALYZE` as it
   runs, history's columns, a spread query's plan kept. `history.rs` and `server.rs` in this thread;
   the `spmd.rs` part to the main thread as paste-ready text.
2. **Facts** (`pondra.learned` the only new name): used by the join order and the spread guard
   (`optimize.rs`, `guard.rs`: the main thread).
3. **Adapting between steps** (`spmd.rs`: the main thread).
4. **Maintenance where the reads are, advice, `CLUSTER BY AUTO`** (`tier.rs`; the names through
   the SQL review thread). An AI advisor, near 1.0, plugs in as another source of advice (§7).
5. **The console**: the two names, History's two plans and "Run as it was", Details' expected against
   actual. In the UI round at the end.

## What others do (from their documentation as I know it; nothing here was run)

- **SQL Server**: estimated and actual execution plans; the Query Store keeps every query's plans and
  run times; automatic plan correction goes back to a plan that was faster; cardinality estimation
  feedback.
- **Oracle**: adaptive plans (a join method chosen while it runs), statistics feedback, SQL plan
  baselines.
- **Spark**: adaptive query execution at stage boundaries (partitions coalesced, a sort-merge join
  turned into a broadcast, skewed joins split).
- **IBM Db2**: LEO, the learning optimizer (2001): actual cardinalities fed back into estimates.
- **Databricks**: query profile; predictive optimization (OPTIMIZE, VACUUM and ANALYZE run when they
  pay); automatic liquid clustering keys from the queries.
- **Snowflake**: query profile; automatic clustering (keys chosen by people).
- **Redshift**: automatic table optimization (sort and distribution keys from the workload).
