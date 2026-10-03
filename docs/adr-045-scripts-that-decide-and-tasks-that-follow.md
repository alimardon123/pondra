# ADR-045: Scripts that decide, and tasks that follow each other

**Date:** 2026-10-03 · **Status:** proposed (the owner asked for it on 2026-10-03: "easy to use,
really powerful, performant, distributed, extensible", "lightweight and efficient", "used by other languages") · **Builds on:** ADR-027 (procedures, tasks,
the run log), ADR-033 (files run as jobs), ADR-036 §4–5 (Postgres's error codes, transactions),
ADR-037 and ADR-044 (variables and parameters), ADR-028's rule (other tools' names are fallbacks for
one implementation)

## Context

A script, a file, a SQL procedure and a task are straight lists of statements today. Anything that
decides (skip a load when there is nothing new, stop when a check fails, go over a list of tables,
retry a conflict) needs Python. Tasks run one statement on a schedule, with nothing to order them,
no condition, and no way to pass a value on. Every engine Pondra replaces has this:

| | Branches, loops | Errors handled | Dynamic SQL | Parallel | Task graphs |
|---|---|---|---|---|---|
| Postgres (PL/pgSQL) | yes | `EXCEPTION WHEN` | `EXECUTE` | no | no (pg_cron) |
| Snowflake Scripting | yes | `EXCEPTION WHEN` | `EXECUTE IMMEDIATE`, `IDENTIFIER()` | `ASYNC` / `AWAIT` | `AFTER`, `WHEN`, return values |
| BigQuery scripting | yes | `EXCEPTION WHEN ERROR` | `EXECUTE IMMEDIATE` | no | no (scheduled queries) |
| Databricks SQL scripting (SQL/PSM) | yes | handlers | `EXECUTE IMMEDIATE`, `IDENTIFIER()` | Jobs' for-each | Jobs: depends on, if/else, task values |
| DuckDB | no | no | no | no | no |

The SQL standard's procedural part (SQL/PSM) is what Databricks and BigQuery follow closely and
Snowflake nearly so. Pondra takes that shape, with its `$` variables, so what people know works.

## Decision

### 1. One script language, everywhere a script runs

A script is statements and blocks. It runs the same in the console, a `.sql` file, `POST /sql`,
Postgres (simple and extended protocol: a block is one statement), MCP, `pondra sql`, a SQL
procedure's body and a task. Every statement Pondra has works inside a block unchanged.

```sql
DECLARE PARAMETER $day DATE = current_date - 1;      -- ADR-044
DECLARE $n = (SELECT count(*) FROM staged WHERE day = $day);

IF $n = 0 THEN
  PRINT 'nothing for ' || $day;
  RETURN;
ELSEIF $n > 10000000 THEN
  RAISE 'too many rows for %: %', $day, $n;          -- an error: the script stops
END IF;

BEGIN
  MERGE INTO orders USING (SELECT * FROM staged WHERE day = $day) s ON orders.id = s.id
    WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *;
EXCEPTION
  WHEN serialization_failure THEN                    -- 40001: another writer won; try once more
    CALL run('etl/merge_day.sql', day => $day);
  WHEN OTHERS THEN
    PRINT 'failed: ' || $error;
    RAISE;                                           -- the same error, to the caller
END;

ASSERT (SELECT count(*) FROM orders WHERE id IS NULL) = 0, 'orders without an id';

FOR t IN (SELECT name FROM pondra.tables WHERE schema = 'raw') DO
  OPTIMIZE IDENTIFIER('raw.' || $t.name);            -- a name, bound: never text pasted in
END FOR;

RETURN $n;                                           -- the run's result
```

| Statement | What it does |
|---|---|
| `BEGIN … [EXCEPTION WHEN … THEN …] END` | a block: its own `DECLARE`s, and its handlers |
| `IF … THEN … ELSEIF … ELSE … END IF` | branches |
| `CASE [$x] WHEN … THEN … ELSE … END CASE` | branches by value or condition |
| `WHILE cond DO … END WHILE`, `REPEAT … UNTIL cond END REPEAT`, `LOOP … END LOOP` | loops |
| `FOR r IN (query) DO … END FOR` | a loop over a query's rows: `$r.column` |
| `LEAVE [label]`, `ITERATE [label]`; `label: WHILE …` | leave or go on with a loop |
| `RETURN [value]` | ends the script; the value is its answer and its run's result |
| `CALL p(…) INTO $x` | a procedure's or file's result into a variable |
| `RAISE 'text %', $x` · `RAISE` · `RAISE NOTICE …` | an error (`P0001`), the current one again, or a notice |
| `PRINT expr` | a notice: the console's Messages, psql's NOTICE, the clients' notices, the run log |
| `ASSERT cond [, 'text']` | an error (`P0004`) unless the condition holds: a data check in one line |
| `EXECUTE IMMEDIATE 'sql' [USING $a, $b]` | a statement built as text, its values bound |
| `IDENTIFIER($name)` | a table's or column's name from a value, in any statement, quoted as one name |

- **Conditions and values are SQL expressions**, worked out once, as the caller, with `$` bound
  (ADR-037 §2). A subquery is fine: `IF EXISTS (SELECT 1 FROM t WHERE …) THEN`.
- **Handlers** name errors by Postgres's codes or condition names (`'40001'`,
  `serialization_failure`, `unique_violation`, `OTHERS`; the names join the codes in `codes.rs`).
  Inside one, `$error` is the message and `$sqlstate` its code.
- **`BEGIN;` is still a transaction.** `BEGIN` followed by `;`, `TRANSACTION` or `WORK`, and `START
  TRANSACTION`, start one (ADR-036 §5); `BEGIN` followed by a statement starts a block. A
  transaction may sit inside a block, and a block inside a transaction.
- **The fallbacks** (ADR-028): Postgres's `RAISE NOTICE` and `ELSIF`, T-SQL's `PRINT`, BigQuery's
  `ELSEIF` and `ASSERT`, Snowflake's and Databricks' `IDENTIFIER()` and `EXECUTE IMMEDIATE`.

### 2. Variables and parameters: one kind of name, scopes that nest

Every value a script works with is a `$name`, bound as a typed literal (ADR-037 §2), whoever set it.
What differs is only who may set it and how long it lives:

| Scope | Made by | Lives | Who may set it |
|---|---|---|---|
| The script's parameters | `DECLARE PARAMETER $day DATE = …` at its top level (ADR-044); a procedure's arguments (`p(day DATE)`); a `.py` file's parameters cell | the run | the caller: a run's values, a request's `params`, the console's bar, a task, `EXECUTE TASK … (day => …)` |
| The script's own | `DECLARE $n = …`, `$n = …` | the run | the script |
| A block's own | `DECLARE` inside `BEGIN … END` | the block (it hides an outer `$n` of the same name) | the block |
| A loop's row | `FOR r IN (…)`: `$r.column` | one pass | the loop |
| A handler's | `$error`, `$sqlstate` | the handler | Pondra |
| A session's | `DECLARE` or `$x = …` sent on its own in a console tab, psql or a client's session | the session | its statements |

- **A parameter is the script's interface, so it is declared at the top**, never inside a block or a
  loop (refused there by name). Everything a tool shows about a script (the bar, `pondra.parameters`,
  `pondra run --help`, MCP's tool for a procedure) comes from that one list (`workspace::parameters`).
  A `$name` used before anything sets it is still a required parameter (ADR-044 §1), but names a
  loop, a handler, `INTO` or a block's `DECLARE` binds are never taken for one: the scan walks the
  blocks.
- **A script run in a session sees the session's variables**; its top-level `DECLARE`s go into the
  session (so a console tab runs a file a statement at a time), its blocks' stay in the blocks. A
  file run, a procedure and a task hold their own (`vars::own`), lent to their Python (`db.vars`).
- **Out, as well as in.** `RETURN $value` is the run's result: a number, text, or a struct for several
  (`RETURN {'rows': $n, 'day': $day}`); the clients get it as the answer, and `pondra.result('t')`
  reads a task's (§5). A procedure's or a file's result goes into a variable with `CALL load_day(day
  => $day) INTO $n` (Snowflake's form).
- **Tasks pass values down.** A graph run has the first task's parameters; every task in it whose
  script declares a parameter of the same name gets that value (`EXECUTE TASK nightly (day => DATE
  '2026-09-01')` backfills one day through the whole graph). A task may also give values itself:
  `AS CALL run('etl/orders.sql', day => $day)`.
- **The same names in Python.** A `.py` file's or notebook's parameters cell is its `DECLARE
  PARAMETER`s; `db.vars` reads and sets the session's or run's variables; a Python procedure's
  arguments are its parameters. So a SQL script, a Python file and a notebook take the same values
  the same way, from any client.

### 3. Light, and the same from every language

- **Nothing new runs until a script uses it.** The runner is a walk over the parsed blocks inside
  the request that sent them: no service, no thread, no memory when unused (principle 6). A
  script's state is its variables and where it is; a straight list of statements runs as today.
- **A condition that reads only variables never plans a query.** `IF $n = 0` is folded as an
  expression (DataFusion's simplifier over bound literals), with no session or physical plan: a
  `WHILE` pass with a condition and an assignment takes 0.4 ms (1.2 ms when each was a query); one
  with a subquery is a query like any other. A script is parsed once per text, not once per loop
  pass.
- **Every client, unchanged.** A script is SQL text, so whatever sends SQL runs one: the console,
  `.sql` files, `pondra sql -f`, `POST /sql`, psql and every Postgres driver (JDBC, ODBC, Npgsql,
  asyncpg, R's and Go's), Flight SQL and ADBC, MCP (an agent sends a script as one call), and
  Python's and JavaScript's `db.sql`. Values go in as parameters (`params`, `$name`, ADR-044) and
  come out as the answer (the last query's rows, or `RETURN`'s value), the notices (`PRINT`,
  through each door's own channel: invariant 116) and `pondra.runs`.
- **Every language from a script.** `CALL` runs a Python procedure, `CALL run(…)` a `.sql`, `.py`
  or notebook file, sharing its values; Python runs a script with `db.sql`. A new language is a
  `LANGUAGE` entry for procedures (ADR-027), not a change to scripting.
- **Errors say where.** An error names the script's line and statement (the console marks it, as
  it does a SQL file's now); Stop ends a script between statements and stops the one running.

### 4. Fast, and spread over the cluster

- **Each statement in a script is a statement as today:** a query spreads across the nodes when
  that is faster (`guard.rs`), a write goes to the leader as it does now. The script's own
  decisions run on the node running it.
- **Set-based first.** A `FOR` over rows is for lists (tables, days, files), not for row-by-row
  work: its query is answered before the first pass.
- **Parallel branches:** `FOR d IN (…) PARALLEL 8 DO … END FOR` runs up to 8 passes at once (at
  most 64: a fan-out is bounded), each with a copy of the script's variables, so what one sets stays
  its own; once a pass fails no new one starts, and the loop fails with that pass's error after the
  others end. `ASYNC <statement>` starts any statement (a block too) beside the script, with a copy
  of the variables, and `AWAIT ALL` waits for every one started, failing with the first failure's
  error at its line; a script's end waits too. Neither runs inside a transaction. Their passes and
  statements keep their places in the job, so a retried script writes once (phase 2, built: eight
  0.3-second Python calls take 0.74 s with `PARALLEL 8`, 2.48 s one at a time).
- **Waiting for one:** `$h = ASYNC <statement>` puts a handle in `$h`, and `AWAIT $h` waits for that
  one alone, failing with its error (which `AWAIT ALL` then leaves out). A handle is a text id, as a
  run's is, so `AWAIT` takes either: `AWAIT 'id'` waits for a procedure or file that `pondra.start`
  began, from any session or client, reading `pondra.runs` as its node writes it (built).
- **Dealt to the nodes** (phase 2b, next): passes and `ASYNC` statements sent to the nodes with
  room, so a backfill of 365 days uses the whole cluster. It needs a node to run statements as the
  script's caller on another's word (signed with the nodes' key), which is its own change; until
  then each pass's statements spread as any statement does. A dealt statement is a run of its own,
  so its handle is that run's id and `AWAIT` needs nothing new.
- **Exactly once through a retry.** A task's tick that runs again after a failover runs its script
  from the start with the same job; each statement's part is its place in the script and its
  loops' counts (`task:nightly:42:3.2#5`), so a write already made is not made twice.

### 5. Tasks that follow each other

```sql
CREATE TASK nightly SCHEDULE 'cron 0 2 * * * UTC' AS CALL run('etl/extract.sql');
CREATE TASK load_orders AFTER nightly WHEN (SELECT count(*) FROM staged) > 0
  WITH (retries = 2, timeout = '1 hour')
  AS CALL run('etl/orders.sql', rows => pondra.result('nightly'));
CREATE TASK report AFTER load_orders, load_customers AS BEGIN
  IF pondra.result('load_orders') > 0 THEN CALL run('reports/daily.ipynb'); END IF;
END;
EXECUTE TASK nightly;                                -- now, once, with everything after it
EXECUTE TASK nightly (day => DATE '2026-09-01');    -- one day again, through the whole graph
ALTER TASK report SUSPEND;  ALTER TASK report RESUME;
```

- **`AFTER a, b`:** a task runs when all of them finished well in the same run of the graph (the
  first task's tick). A task has `SCHEDULE` or `AFTER`, not both.
- **`WHEN cond`:** checked before it runs; false is a run marked `skipped`, which counts as done
  for what follows.
- **Results:** a script's `RETURN` value is its run's result in `pondra.runs`; `pondra.result('t')`
  is task `t`'s in the same graph run.
- **`WITH (…)`** holds a task's options by name: `retries`, `retry_delay`, `timeout`, `on_failure`
  (a procedure to call: mail, Slack, anything Python can do). An unknown name is refused, and a
  new option is a new name, not new grammar.
- **Names in the console:** a **task** is the object (the Data tree's Tasks, New task…), its
  **schedule** is when it runs, each time it runs is a **run** (History, `pondra.runs`). The Jobs
  view becomes Tasks, with its graph drawn, and flows beside them.
- **No separate schedule objects:** one task holds the schedule and the rest follow it. `CREATE
  SCHEDULE` shared by name can come later without changing anything here.

**Built (phase 3).** As above, with these settled while building it:
- **A graph has one start.** A task that would follow tasks of two schedules, or itself, is
  refused, and so is dropping a task others follow. A run of the graph is the first task's tick,
  and each task's run in it is `task-<name>-<tick>` in `pondra.runs`.
- **What follows a failure doesn't run.** A failed task (after its `retries`) calls `on_failure`
  with its name and error, and the tasks after it wait for the next run of the graph. A suspended
  task, and so what follows it, doesn't run in the graph's runs. `EXECUTE TASK` still runs it.
- **Results** are kept with the task's last run (`pondra.tasks.last_result`): its `RETURN`, or the
  first value of its last query. `pondra.result('t')` is put in as that value, and is NULL when
  `t` gave none in this run.
- **Values passed down the graph** are `EXECUTE TASK`'s, bound as `$name` in every task, its
  `WHEN` too. A scheduled run gives none, so a task gives its own default with `DECLARE PARAMETER`
  at its top. A task that is one `BEGIN … END` block has that block's top as its top, and so does a
  file that is one block.
- **`EXECUTE TASK`** claims a tick that the leader's scheduler runs at once. It is refused while
  the task's last run is still going.
- **Every pass of a loop yields.** A loop that only read variables never waited, so it held its
  worker thread: a task's `timeout` never fired, and after a failover the new leader ran it again
  and lost a thread to it.

### 6. Built to grow

- **A registry of statements** (`script.rs`): each kind of block or statement is one entry, the
  words that open and close it, how its head is read, and how it runs. `REPEAT` or a future
  `FOREACH FILE` is an entry, not a branch through the runner (principle 9). The splitter that keeps
  a block whole (the server's `routines::statements`, the console's `statements`) reads the same
  list of opening and closing words.
- **New functions, not new grammar:** what a script asks about the platform is a `pondra.*`
  function (`pondra.result`, `pondra.run_status($id)`, `pondra.files('…')`), usable in any query
  too.
- **Options by name** (`WITH (…)`) for tasks, and later for blocks (`BEGIN WITH (timeout = …)`).
- **Any language through `CALL`:** a script calls Python procedures, files and notebooks, and they
  share its variables (`db.vars`); a Python file can run a script with `db.sql`.

## Phases

1. **Scripting**: blocks, branches, loops, handlers, `PRINT`, `RAISE`, `ASSERT`, `RETURN`,
   `CALL … INTO`, `EXECUTE IMMEDIATE`, `IDENTIFIER()`; the scopes of §2; in every door, procedures and tasks; the console's
   highlighting and Run at the caret taking the whole block.
2. **Parallel**: `PARALLEL n` loops, `ASYNC`, `AWAIT ALL` and `AWAIT $h` or a run's id (built); then
   passes dealt to the nodes, each a run of its own in `pondra.runs`.
3. **Task graphs**: `AFTER`, `WHEN`, results, values passed down the graph, `WITH (…)` options, `EXECUTE TASK`, `SUSPEND` and
   `RESUME` (built); then the console's Tasks view with the graph and the renaming.

## Consequences

- What needed Python to decide can be SQL, in the console, a file, a procedure or a task, and it
  reads like Databricks', BigQuery's and Snowflake's scripts.
- One new file (`script.rs`) and a splitter that knows blocks; the engine is untouched.
- Each phase gets its checks in `harness.py` (a script's branches, loops and handlers from every
  door; a block over Postgres's extended protocol; a retried tick writing once; a graph through a
  leader failover).
